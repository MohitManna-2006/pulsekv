#define _GNU_SOURCE  /* SO_REUSEPORT under strict -std=c11 */

/*
 * PulseKV -- build step 5: thread-per-core epoll over a sharded hash table.
 *
 * Each of N_THREADS workers owns a complete stack of its own: its own listening
 * socket on the shared port via SO_REUSEPORT, its own epoll instance, and its
 * own set of connections. Nothing about the event loop is shared, so there is
 * no cross-thread epoll contention and no thundering herd on a common listener
 * -- the kernel hashes each incoming connection to exactly one worker's accept
 * queue. The design doc picks this over a shared epoll fd with EPOLLEXCLUSIVE
 * because it scales more predictably under the 25K req/sec target.
 *
 * The one thing every worker shares is the logical store: one pk_table_t with
 * 1,024 bucket chains striped across 256 mutex shards. A request locks only the
 * shard selected by its key hash, so unrelated keys can execute concurrently
 * without changing the worker-owned event-loop model established in step 4.
 *
 * Shutdown is a real mechanism rather than signal timing. A signal handler can
 * set a flag, but a thread parked in epoll_wait(-1) will not look at it, so the
 * handler also writes to an eventfd that is registered in every worker's epoll
 * set. That is what actually wakes all sixteen.
 *
 * The trigger mode from step 2 is still a compile-time choice, edge-triggered
 * by default:
 *
 *   level-triggered (-DPULSEKV_LEVEL_TRIGGERED)
 *       epoll keeps re-reporting a fd for as long as data sits on it, so one
 *       read per event is enough -- what we miss now comes back next turn.
 *
 *   edge-triggered (default)
 *       epoll reports only the *transition* to readable. Bytes we leave in the
 *       kernel produce no further notification, so a read that stops early
 *       strands them until the client happens to send more. We must drain to
 *       EAGAIN ourselves.
 */

#include "hashtable.h"
#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#define PULSEKV_PORT    9999
#define N_THREADS       16
#define LISTEN_BACKLOG  512
#define MAX_EVENTS      64

#ifdef PULSEKV_LEVEL_TRIGGERED
#  define TRIGGER_FLAG  0
#  define TRIGGER_NAME  "level-triggered"
#else
#  define TRIGGER_FLAG  EPOLLET
#  define TRIGGER_NAME  "edge-triggered"
#endif

typedef struct conn {
    int  fd;
    char desc[INET_ADDRSTRLEN + 8];

    /* Bytes read off the socket but not yet parsed into a whole frame. */
    uint8_t rbuf[PK_MAX_REQ_LEN];
    size_t  rhave;

    /* Responses generated but not yet accepted by the kernel. wsent..wfill is
     * the part still owed to the client. */
    uint8_t wbuf[PK_MAX_RESP_LEN];
    size_t  wsent;
    size_t  wfill;

    bool want_write;    /* EPOLLOUT currently in this fd's interest set */
    bool read_stalled;  /* stopped reading because rbuf had no room */

    /* Intrusive list of this worker's live connections, so shutdown can free
     * them. Otherwise the only pointer to a conn_t lives in kernel epoll state,
     * where a leak checker cannot see it. */
    struct conn  *all_next;
    struct conn **all_prev;
} conn_t;

typedef struct {
    int         index;
    int         lfd;    /* this worker's own listening socket */
    int         epfd;   /* this worker's own epoll instance */
    conn_t     *conns;  /* this worker's own connections */
    pk_table_t *table;  /* shared with every other worker */

    /* Touched only by the owning thread; read by main after pthread_join, which
     * is itself the synchronisation point. */
    unsigned long accepted;
    unsigned long served;
} worker_t;

/*
 * Stop signalling. The flag alone is not enough: a worker blocked in
 * epoll_wait(-1) never gets around to reading it, so the handler also writes to
 * this eventfd, which is registered in every worker's epoll set. It is
 * deliberately registered level-triggered and never drained, so a worker that
 * had not yet reached epoll_wait when the signal arrived still finds it ready.
 */
static int        g_stopfd = -1;
static atomic_int g_stop;  /* lock-free on every target we build for */
_Static_assert(ATOMIC_INT_LOCK_FREE == 2,
               "the signal-handler stop flag must always be lock-free");

/* Startup is reported back to main so it only announces readiness once all
 * sixteen workers have created and registered their own resources. This
 * mutex is never touched on the request path; the table still has the only
 * data-plane mutex in this step. */
static pthread_mutex_t g_start_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_start_cond = PTHREAD_COND_INITIALIZER;
static int             g_started_ok;
static int             g_started_failed;
static atomic_int      g_next_worker_index;

/* Each index has one writer, and main reads these only after joining all
 * workers. They make the SO_REUSEPORT distribution visible at shutdown. */
static unsigned long g_accepted[N_THREADS];
static unsigned long g_served[N_THREADS];

/* Set once before any thread starts, read-only thereafter. */
static bool g_quiet = false;

/*
 * Distinguishing what an epoll event refers to. A conn_t pointer means a
 * client; these two addresses are just unique tags for the other two cases.
 */
static char g_listener_tag;
static char g_stopfd_tag;
#define TAG_LISTENER ((void *)&g_listener_tag)
#define TAG_STOPFD   ((void *)&g_stopfd_tag)

static void request_stop(void)
{
    atomic_store_explicit(&g_stop, 1, memory_order_relaxed);

    if (g_stopfd >= 0) {
        uint64_t one = 1;
        /* write() is async-signal-safe; there is nothing useful to do if this
         * fails, and the flag above still covers threads that wake anyway. */
        ssize_t rc = write(g_stopfd, &one, sizeof(one));
        (void)rc;
    }
}

static void on_signal(int sig)
{
    int saved_errno = errno;
    (void)sig;
    request_stop();
    errno = saved_errno;
}

static int install_signal_handlers(void)
{
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    sigemptyset(&action.sa_mask);

    action.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &action, NULL) < 0)
        return -1;

    action.sa_handler = on_signal;
    if (sigaction(SIGINT, &action, NULL) < 0
        || sigaction(SIGTERM, &action, NULL) < 0)
        return -1;
    return 0;
}

static bool stopping(void)
{
    return atomic_load_explicit(&g_stop, memory_order_relaxed) != 0;
}

static int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        perror("fcntl(F_GETFL)");
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("fcntl(F_SETFL)");
        return -1;
    }
    return 0;
}

/*
 * Run one request against the shared store. A GET hit is copied into val_out,
 * which belongs to the caller -- the table never lends out a pointer into a
 * node, since another thread's DEL could free it the moment the lock drops.
 */
static pk_status_t dispatch(worker_t *w, const pk_request_t *req, uint8_t *val_out,
                            size_t val_cap, uint32_t *val_len_out)
{
    *val_len_out = 0;

    switch (req->opcode) {
    case PK_OP_GET:
        switch (pk_table_get(w->table, req->key, req->key_len,
                             val_out, val_cap, val_len_out)) {
        case PK_TABLE_OK:        return PK_STATUS_OK;
        case PK_TABLE_NOT_FOUND: return PK_STATUS_NOT_FOUND;
        default:                 return PK_STATUS_ERROR;
        }

    case PK_OP_SET:
        return pk_table_set(w->table, req->key, req->key_len,
                            req->val, req->val_len) == PK_TABLE_OK
                   ? PK_STATUS_OK : PK_STATUS_ERROR;

    case PK_OP_DEL:
        /*
         * The wire protocol defines DEL as answering OK whenever the frame
         * parsed, so a delete of an absent key is not reported differently.
         * pk_table_del does distinguish the two if that is ever wanted.
         */
        switch (pk_table_del(w->table, req->key, req->key_len)) {
        case PK_TABLE_OK:
        case PK_TABLE_NOT_FOUND: return PK_STATUS_OK;
        default:                 return PK_STATUS_ERROR;
        }

    default:
        return PK_STATUS_ERROR;
    }
}

/*
 * Built into one buffer and emitted with a single call. stdio locks per call,
 * not per line, so the piecewise printfs this replaced would have interleaved
 * into nonsense once sixteen threads were logging.
 */
static void log_request(const worker_t *w, const conn_t *c, const pk_request_t *req,
                        pk_status_t status, uint32_t val_len)
{
    if (g_quiet)
        return;

    char line[256];
    int  klen = (int)(req->key_len > 48 ? 48 : req->key_len);
    int  n    = snprintf(line, sizeof(line), "[t%02d %s] %s key=\"%.*s\"",
                         w->index, c->desc, pk_opcode_name(req->opcode),
                         klen, (const char *)req->key);

    if (n >= 0 && req->opcode == PK_OP_SET && (size_t)n < sizeof(line))
        n += snprintf(line + n, sizeof(line) - (size_t)n, " val=%uB", req->val_len);
    if (n >= 0 && (size_t)n < sizeof(line))
        n += snprintf(line + n, sizeof(line) - (size_t)n, " -> %s", pk_status_name(status));
    if (n >= 0 && req->opcode == PK_OP_GET && status == PK_STATUS_OK
        && (size_t)n < sizeof(line))
        n += snprintf(line + n, sizeof(line) - (size_t)n, " val=%uB", val_len);
    if (n >= 0 && (size_t)n < sizeof(line))
        snprintf(line + n, sizeof(line) - (size_t)n, "\n");

    fputs(line, stdout);
}

/* ---------------------------------------------------------------- write side */

static size_t wbuf_room(const conn_t *c)
{
    return sizeof(c->wbuf) - c->wfill;
}

/* Slide the still-unsent bytes back to the front to reclaim the head of wbuf. */
static void wbuf_compact(conn_t *c)
{
    if (c->wsent == 0)
        return;
    memmove(c->wbuf, c->wbuf + c->wsent, c->wfill - c->wsent);
    c->wfill -= c->wsent;
    c->wsent = 0;
}

/* Returns false when there is no room, which the caller reads as backpressure. */
static bool queue_response(conn_t *c, pk_status_t status,
                           const uint8_t *val, uint32_t val_len)
{
    size_t need = PK_RESP_HEADER_LEN + val_len;

    if (wbuf_room(c) < need)
        wbuf_compact(c);
    if (wbuf_room(c) < need)
        return false;

    pk_response_t resp = {
        .status  = (uint8_t)status,
        .val_len = val_len,
        .val     = val_len > 0 ? val : NULL,
    };
    int n = pk_response_encode(&resp, c->wbuf + c->wfill, wbuf_room(c));
    if (n < 0) {
        fprintf(stderr, "[%s] failed to encode response (status=%d)\n", c->desc, status);
        return false;
    }
    c->wfill += (size_t)n;
    return true;
}

/* Hand buffered bytes to the kernel until it takes them all or pushes back. */
static bool conn_flush(conn_t *c)
{
    while (c->wsent < c->wfill) {
        ssize_t n = write(c->fd, c->wbuf + c->wsent, c->wfill - c->wsent);
        if (n > 0) {
            c->wsent += (size_t)n;  /* short write: come round again for the rest */
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            return true;  /* socket buffer is full; EPOLLOUT will wake us */
        fprintf(stderr, "[%s] write: %s\n", c->desc, strerror(errno));
        return false;
    }
    c->wsent = c->wfill = 0;  /* fully drained -- refill from the front */
    return true;
}

/* ----------------------------------------------------------------- read side */

/* Turn whatever whole frames are in rbuf into queued responses. */
static bool conn_process(worker_t *w, conn_t *c)
{
    for (;;) {
        pk_request_t req;
        size_t consumed = 0;
        pk_decode_result_t r = pk_request_decode(c->rbuf, c->rhave, &req, &consumed);

        if (r == PK_DECODE_INCOMPLETE)
            return true;

        if (r == PK_DECODE_ERROR) {
            fprintf(stderr, "[t%02d %s] malformed frame, closing\n", w->index, c->desc);
            queue_response(c, PK_STATUS_ERROR, NULL, 0);
            return false;
        }

        /*
         * A frame whose reply will not fit stays in rbuf and is decoded again
         * on a later pass, so anything that mutates must not have run yet.
         * SET and DEL always reply with a bare header, so the room for it can
         * be checked before executing. GET is read-only and safe to repeat,
         * which is what lets its variable-length reply be sized afterwards.
         * (Today both mutations are idempotent so a repeat is invisible; once
         * step 6 appends a WAL record per write, it would not be.)
         */
        if (req.opcode != PK_OP_GET) {
            if (wbuf_room(c) < PK_RESP_HEADER_LEN)
                wbuf_compact(c);
            if (wbuf_room(c) < PK_RESP_HEADER_LEN)
                return true;  /* backpressure, nothing executed */
        }

        /* Staging for a GET hit. The value has to land somewhere the table can
         * copy into while it holds its lock, before the protocol encoder frames
         * it into wbuf. Per-call, so each thread has its own. */
        uint8_t  val[PK_MAX_VAL_LEN];
        uint32_t val_len = 0;

        pk_status_t status = dispatch(w, &req, val, sizeof(val), &val_len);

        /* Queue the reply before consuming the request: if the write buffer is
         * full we leave the frame in rbuf and pick it up again once the socket
         * drains, rather than answering a request we've already thrown away. */
        if (!queue_response(c, status, val, val_len))
            return true;

        log_request(w, c, &req, status, val_len);
        w->served++;

        c->rhave -= consumed;
        memmove(c->rbuf, c->rbuf + consumed, c->rhave);
    }
}

static bool conn_on_readable(worker_t *w, conn_t *c)
{
    for (;;) {
        if (stopping())
            return false;

        if (c->rhave == sizeof(c->rbuf)) {
            /* Nothing decodable left and no room to read into, so the write
             * side is the thing that's behind. Resume once it drains. */
            c->read_stalled = true;
            return true;
        }

        ssize_t n = read(c->fd, c->rbuf + c->rhave, sizeof(c->rbuf) - c->rhave);

        if (n > 0) {
            c->rhave += (size_t)n;
            if (!conn_process(w, c))
                return false;
#ifdef PULSEKV_LEVEL_TRIGGERED
            /* Anything still buffered gets reported again on the next
             * epoll_wait, so stopping here costs nothing. */
            return true;
#else
            /* This is the only notification for bytes that have already
             * arrived, so keep going until the kernel says there are none. */
            continue;
#endif
        }

        if (n == 0) {
            if (c->rhave > 0)
                fprintf(stderr, "[t%02d %s] disconnected mid-frame (%zu stray bytes)\n",
                        w->index, c->desc, c->rhave);
            else if (!g_quiet)
                printf("[t%02d %s] disconnected\n", w->index, c->desc);
            return false;
        }

        if (errno == EINTR)
            continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return true;  /* drained */

        fprintf(stderr, "[t%02d %s] read: %s\n", w->index, c->desc, strerror(errno));
        return false;
    }
}

static bool conn_on_writable(worker_t *w, conn_t *c)
{
    if (!conn_flush(c))
        return false;

    /* Space freed up: finish any frame that was waiting on it. */
    if (!conn_process(w, c))
        return false;

    if (c->read_stalled && c->rhave < sizeof(c->rbuf)) {
        c->read_stalled = false;
        return conn_on_readable(w, c);
    }
    return true;
}

/* ------------------------------------------------------------- epoll plumbing */

/*
 * Only ask for EPOLLOUT while bytes are actually owed. A socket is writable
 * almost all the time, so leaving it armed would spin the loop at full tilt
 * under level-triggered for no reason.
 */
static bool conn_update_interest(worker_t *w, conn_t *c)
{
    bool need_write = (c->wsent < c->wfill);
    if (need_write == c->want_write)
        return true;

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events   = EPOLLIN | TRIGGER_FLAG | (need_write ? EPOLLOUT : 0u);
    ev.data.ptr = c;

    if (epoll_ctl(w->epfd, EPOLL_CTL_MOD, c->fd, &ev) < 0) {
        fprintf(stderr, "[t%02d %s] epoll_ctl(MOD): %s\n",
                w->index, c->desc, strerror(errno));
        return false;
    }
    c->want_write = need_write;
    return true;
}

static void conns_insert(worker_t *w, conn_t *c)
{
    c->all_next = w->conns;
    c->all_prev = &w->conns;
    if (w->conns != NULL)
        w->conns->all_prev = &c->all_next;
    w->conns = c;
}

static void conns_remove(conn_t *c)
{
    *c->all_prev = c->all_next;
    if (c->all_next != NULL)
        c->all_next->all_prev = c->all_prev;
}

static void conn_close(worker_t *w, conn_t *c)
{
    conn_flush(c);  /* best effort: the last reply may still be buffered */
    epoll_ctl(w->epfd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    conns_remove(c);
    free(c);
}

/* Drain this worker's accept queue: one event can cover several pending peers. */
static void accept_ready(worker_t *w)
{
    for (;;) {
        if (stopping())
            return;

        struct sockaddr_in peer;
        socklen_t peer_len = sizeof(peer);

        int cfd = accept(w->lfd, (struct sockaddr *)&peer, &peer_len);
        if (cfd < 0) {
            if (errno == EINTR || errno == ECONNABORTED)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;  /* queue empty */
            fprintf(stderr, "[t%02d] accept: %s\n", w->index, strerror(errno));
            return;
        }

        if (set_nonblocking(cfd) < 0) {
            close(cfd);
            continue;
        }

        conn_t *c = calloc(1, sizeof(*c));
        if (c == NULL) {
            fprintf(stderr, "[t%02d] out of memory, dropping connection\n", w->index);
            close(cfd);
            continue;
        }
        c->fd = cfd;

        char ip[INET_ADDRSTRLEN];
        if (inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip)) == NULL)
            strcpy(ip, "?");
        snprintf(c->desc, sizeof(c->desc), "%s:%u", ip, ntohs(peer.sin_port));

        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events   = EPOLLIN | TRIGGER_FLAG;
        ev.data.ptr = c;

        if (epoll_ctl(w->epfd, EPOLL_CTL_ADD, cfd, &ev) < 0) {
            fprintf(stderr, "[t%02d %s] epoll_ctl(ADD): %s\n",
                    w->index, c->desc, strerror(errno));
            close(cfd);
            free(c);
            continue;
        }

        conns_insert(w, c);
        w->accepted++;
        if (!g_quiet)
            printf("[t%02d %s] connected\n", w->index, c->desc);
    }
}

/*
 * Every worker binds the same port. SO_REUSEPORT is what makes that legal and
 * useful: the kernel hashes each incoming connection to exactly one of the
 * listening sockets, giving every thread a private accept queue instead of
 * sixteen threads racing on one.
 */
static int listen_socket(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
        perror("setsockopt(SO_REUSEADDR)");
        close(fd);
        return -1;
    }
    /* Must be set before bind, or binding the second socket fails. */
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0) {
        perror("setsockopt(SO_REUSEPORT)");
        close(fd);
        return -1;
    }

    /* Non-blocking: with a full accept queue drained in a loop, the last
     * accept() must report EAGAIN instead of parking the whole event loop. */
    if (set_nonblocking(fd) < 0) {
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, LISTEN_BACKLOG) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }
    return fd;
}

/* Open this worker's listener and epoll set. Cleans up after itself on failure. */
static int worker_setup(worker_t *w)
{
    w->lfd = listen_socket(PULSEKV_PORT);
    if (w->lfd < 0)
        return -1;

    w->epfd = epoll_create1(EPOLL_CLOEXEC);
    if (w->epfd < 0) {
        perror("epoll_create1");
        close(w->lfd);
        w->lfd = -1;
        return -1;
    }

    struct epoll_event ev;

    memset(&ev, 0, sizeof(ev));
    ev.events   = EPOLLIN | TRIGGER_FLAG;
    ev.data.ptr = TAG_LISTENER;
    if (epoll_ctl(w->epfd, EPOLL_CTL_ADD, w->lfd, &ev) < 0) {
        perror("epoll_ctl(ADD listener)");
        goto fail;
    }

    /* Level-triggered on purpose, and never drained: whoever gets here after
     * the signal has already fired must still see it as ready. */
    memset(&ev, 0, sizeof(ev));
    ev.events   = EPOLLIN;
    ev.data.ptr = TAG_STOPFD;
    if (epoll_ctl(w->epfd, EPOLL_CTL_ADD, g_stopfd, &ev) < 0) {
        perror("epoll_ctl(ADD stopfd)");
        goto fail;
    }
    return 0;

fail:
    close(w->epfd);
    close(w->lfd);
    w->epfd = -1;
    w->lfd  = -1;
    return -1;
}

static void report_worker_start(bool ok)
{
    pthread_mutex_lock(&g_start_lock);
    if (ok)
        g_started_ok++;
    else
        g_started_failed++;
    pthread_cond_broadcast(&g_start_cond);
    pthread_mutex_unlock(&g_start_lock);
}

static void *worker_main(void *arg)
{
    pk_table_t *table = arg;
    worker_t worker = {
        .index = atomic_fetch_add_explicit(&g_next_worker_index, 1,
                                           memory_order_relaxed),
        .lfd   = -1,
        .epfd  = -1,
        .table = table,
    };
    worker_t *w = &worker;
    struct epoll_event events[MAX_EVENTS];
    bool failed = false;

    /* The listener and epoll instance are created here, in the thread that
     * owns and will eventually close them. The only object passed through
     * pthread_create is the one shared table. */
    if (worker_setup(w) != 0) {
        report_worker_start(false);
        request_stop();
        return (void *)(intptr_t)1;
    }
    report_worker_start(true);

    while (!stopping()) {
        int n = epoll_wait(w->epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            fprintf(stderr, "[t%02d] epoll_wait: %s\n", w->index, strerror(errno));
            failed = true;
            request_stop();
            break;
        }

        /* The signal may have arrived just after epoll_wait returned a batch
         * without the stopfd in it. Honour the flag before doing more work. */
        if (stopping())
            break;

        for (int i = 0; i < n; i++) {
            void    *ptr = events[i].data.ptr;
            uint32_t e   = events[i].events;

            if (ptr == TAG_STOPFD || stopping())
                goto done;
            if (ptr == TAG_LISTENER) {
                accept_ready(w);
                continue;
            }

            conn_t *c = ptr;
            bool alive = true;

            if (e & (EPOLLERR | EPOLLHUP)) {
                alive = false;
            } else {
                /* Writable first: draining frees buffer space, which may let
                 * the read side finish a frame it had to leave parked. */
                if (e & EPOLLOUT)
                    alive = conn_on_writable(w, c);
                if (alive && (e & EPOLLIN))
                    alive = conn_on_readable(w, c);
                if (alive)
                    alive = conn_flush(c);
                if (alive)
                    alive = conn_update_interest(w, c);
            }

            if (!alive)
                conn_close(w, c);
        }
    }

done:
    /* A worker tears down only what it owns. The shared table outlives every
     * thread and is destroyed once, by main, after all the joins. */
    close(w->lfd);
    w->lfd = -1;
    while (w->conns != NULL)
        conn_close(w, w->conns);
    close(w->epfd);
    w->epfd = -1;
    g_accepted[w->index] = w->accepted;
    g_served[w->index]   = w->served;
    return failed ? (void *)(intptr_t)1 : NULL;
}

int main(void)
{
    /* Line buffering keeps the interleaved per-connection log readable when
     * stdout is a pipe rather than a terminal. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    /* The per-request log is the bulk of the output under load; the stress test
     * turns it off so it is measuring the server rather than stdio. */
    g_quiet = (getenv("PULSEKV_QUIET") != NULL);

    atomic_init(&g_stop, 0);
    atomic_init(&g_next_worker_index, 0);

    /* Non-blocking is important in a signal handler: even the pathological
     * eventfd-counter-full case must not park the interrupted thread. */
    g_stopfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (g_stopfd < 0) {
        perror("eventfd");
        return EXIT_FAILURE;
    }

    /* Installed only once the eventfd exists, so the handler always has
     * something to write to. SIGPIPE is ignored in the same operation: a
     * client that vanishes mid-write must not take the server down with it. */
    if (install_signal_handlers() < 0) {
        perror("sigaction");
        close(g_stopfd);
        return EXIT_FAILURE;
    }

    pk_table_t table;
    int rc = pk_table_init(&table);
    if (rc != 0) {
        fprintf(stderr, "pk_table_init: %s\n", strerror(rc));
        close(g_stopfd);
        return EXIT_FAILURE;
    }

    pthread_t threads[N_THREADS];
    int created = 0;
    bool failed = false;

    for (int i = 0; i < N_THREADS; i++) {
        rc = pthread_create(&threads[i], NULL, worker_main, &table);
        if (rc != 0) {
            fprintf(stderr, "pthread_create: %s\n", strerror(rc));
            failed = true;
            request_stop();
            break;
        }
        created++;
    }

    if (created == N_THREADS) {
        pthread_mutex_lock(&g_start_lock);
        while (g_started_ok + g_started_failed < N_THREADS)
            pthread_cond_wait(&g_start_cond, &g_start_lock);
        if (g_started_failed != 0)
            failed = true;
        pthread_mutex_unlock(&g_start_lock);

        if (failed)
            request_stop();
    } else {
        fprintf(stderr, "only %d of %d workers created, shutting down\n",
                created, N_THREADS);
    }

    if (!failed && !stopping())
        printf("pulsekv listening on 0.0.0.0:%d (%d threads, thread-per-core via "
               "SO_REUSEPORT, %s, %u lock shards / %u buckets)\n",
               PULSEKV_PORT, N_THREADS, TRIGGER_NAME,
               PK_TABLE_SHARDS, PK_TABLE_BUCKETS);

    for (int i = 0; i < created; i++) {
        void *worker_result = NULL;
        rc = pthread_join(threads[i], &worker_result);
        if (rc != 0) {
            fprintf(stderr, "pthread_join: %s\n", strerror(rc));
            failed = true;
        } else if (worker_result != NULL) {
            failed = true;
        }
    }

    /* Past every join, so the counters below need no synchronisation and the
     * table has no users left. */
    unsigned long total_conns = 0;
    unsigned long total_reqs  = 0;
    for (int i = 0; i < N_THREADS; i++) {
        total_conns += g_accepted[i];
        total_reqs  += g_served[i];
    }

    printf("shutdown: %lu connections, %lu requests, %zu keys resident\n",
           total_conns, total_reqs, pk_table_count(&table));
    printf("per-thread connections:");
    for (int i = 0; i < N_THREADS; i++)
        printf(" t%02d=%lu", i, g_accepted[i]);
    printf("\n");

    pk_table_destroy(&table);
    close(g_stopfd);
    return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
