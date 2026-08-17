#define _GNU_SOURCE  /* SO_REUSEPORT under strict -std=c11 */

/*
 * PulseKV -- build step 8: measured and tuned concurrent durable server.
 *
 * Each worker owns a complete stack of its own: its own listening
 * socket on the shared port via SO_REUSEPORT, its own epoll instance, and its
 * own set of connections. Nothing about the event loop is shared, so there is
 * no cross-thread epoll contention and no thundering herd on a common listener
 * -- the kernel hashes each incoming connection to exactly one worker's accept
 * queue. The design doc picks this over a shared epoll fd with EPOLLEXCLUSIVE
 * because it scales more predictably under the 25K req/sec target.
 *
 * Workers share the logical store and one append-only WAL service. GET stays on
 * the worker. SET/DEL is copied into an owned WAL request and submitted to a
 * dedicated writer, which orders requests, batches them, writes them, and calls
 * fdatasync once per batch. Only its completion lets the owning worker update
 * the table and answer the client. No filesystem call blocks an epoll loop.
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
 *
 * Step 8 retains the same architecture but tunes its hot paths from measured
 * 500-connection workloads: completion delivery is a lock-free SPSC handoff,
 * eventfd and condition-variable wakeups are coalesced, accepted sockets use
 * accept4/TCP_NODELAY, WAL buffers grow to actual demand, and the durable
 * default is a 256-record/1 ms group-commit window.
 */

#include "hashtable.h"
#include "protocol.h"
#include "wal.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
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
#include <time.h>
#include <unistd.h>

#define PULSEKV_PORT    9999
#define MAX_WORKERS     16
#define LISTEN_BACKLOG  512
#define MAX_EVENTS      64

#ifdef PULSEKV_LEVEL_TRIGGERED
#  define TRIGGER_FLAG  0
#  define TRIGGER_NAME  "level-triggered"
#else
#  define TRIGGER_FLAG  EPOLLET
#  define TRIGGER_NAME  "edge-triggered"
#endif

typedef struct worker worker_t;

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
    bool want_read;     /* EPOLLIN currently in this fd's interest set */
    bool read_stalled;  /* stopped reading because rbuf had no room */
    bool close_after_pending;

    /* At most one mutation per connection may be in the WAL. Holding later
     * pipelined frames in rbuf preserves program order (SET then GET, etc.). */
    pk_wal_request_t *pending;
    int               wal_error;
    struct conn      *completion_next;

    /* Intrusive list of this worker's live connections, so shutdown can free
     * them. Otherwise the only pointer to a conn_t lives in kernel epoll state,
     * where a leak checker cannot see it. */
    struct conn  *all_next;
    struct conn **all_prev;
} conn_t;

typedef struct {
    pk_table_t table;
    pk_wal_t  *wal;
} server_t;

struct worker {
    int         index;
    int         lfd;    /* this worker's own listening socket */
    int         epfd;   /* this worker's own epoll instance */
    int         completion_fd;
    conn_t     *conns;  /* this worker's own connections */
    server_t   *server; /* table and ordered WAL shared by every worker */

    /* The WAL writer is the sole producer and this worker is the sole
     * consumer. Completions use a release/acquire atomic stack; the worker
     * reverses each detached batch to restore FIFO order. */
    _Atomic(conn_t *) completion_stack;
    atomic_bool       completion_signaled;
    size_t          pending_count;
    bool            draining;

    /* Touched only by the owning thread; read by main after pthread_join, which
     * is itself the synchronisation point. */
    unsigned long accepted;
    unsigned long served;
};

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
static unsigned long g_accepted[MAX_WORKERS];
static unsigned long g_served[MAX_WORKERS];

/* Set once before any thread starts, read-only thereafter. */
static bool g_quiet = false;

/*
 * Distinguishing what an epoll event refers to. A conn_t pointer means a
 * client; these two addresses are just unique tags for the other two cases.
 */
static char g_listener_tag;
static char g_stopfd_tag;
static char g_completion_tag;
#define TAG_LISTENER ((void *)&g_listener_tag)
#define TAG_STOPFD   ((void *)&g_stopfd_tag)
#define TAG_COMPLETION ((void *)&g_completion_tag)

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

/* GET never touches the WAL. The table copies a hit into worker-owned staging
 * because another worker could delete the node as soon as its shard unlocks. */
static pk_status_t dispatch_get(worker_t *w, const pk_request_t *req,
                                uint8_t *val_out, size_t val_cap,
                                uint32_t *val_len_out)
{
    *val_len_out = 0;
    switch (pk_table_get(&w->server->table, req->key, req->key_len,
                         val_out, val_cap, val_len_out)) {
    case PK_TABLE_OK:        return PK_STATUS_OK;
    case PK_TABLE_NOT_FOUND: return PK_STATUS_NOT_FOUND;
    default:                 return PK_STATUS_ERROR;
    }
}

/* Called only after the WAL writer reports a successful fdatasync. */
static pk_status_t apply_durable_mutation(worker_t *w,
                                          const pk_wal_record_t *record)
{
    if (record->opcode == PK_OP_SET) {
        return pk_table_set(&w->server->table, record->key, record->key_len,
                            record->val, record->val_len) == PK_TABLE_OK
                   ? PK_STATUS_OK : PK_STATUS_ERROR;
    }
    if (record->opcode == PK_OP_DEL) {
        switch (pk_table_del(&w->server->table, record->key, record->key_len)) {
        case PK_TABLE_OK:
        case PK_TABLE_NOT_FOUND: return PK_STATUS_OK;
        default:                 return PK_STATUS_ERROR;
        }
    }
    return PK_STATUS_ERROR;
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

static void wal_complete(pk_wal_request_t *request, int error,
                         void *completion_ctx);

static void conn_consume(conn_t *c, size_t consumed)
{
    c->rhave -= consumed;
    if (c->rhave > 0)
        memmove(c->rbuf, c->rbuf + consumed, c->rhave);
}

/* Turn whatever whole frames are in rbuf into queued responses. */
static bool conn_process(worker_t *w, conn_t *c)
{
    if (c->pending != NULL)
        return true;

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

        /* A mutation is submitted exactly once. Reserve its fixed-size reply
         * before consuming the frame; otherwise backpressure could force a
         * second WAL append when this buffer is decoded again. */
        if (req.opcode != PK_OP_GET) {
            if (wbuf_room(c) < PK_RESP_HEADER_LEN)
                wbuf_compact(c);
            if (wbuf_room(c) < PK_RESP_HEADER_LEN)
                return true;  /* backpressure, nothing executed */
        }

        if (req.opcode == PK_OP_GET) {
            uint8_t  val[PK_MAX_VAL_LEN];
            uint32_t val_len = 0;
            pk_status_t status = dispatch_get(w, &req, val, sizeof(val), &val_len);

            /* GET is read-only and safe to decode again if its variable-sized
             * response does not fit yet. */
            if (!queue_response(c, status, val, val_len))
                return true;

            log_request(w, c, &req, status, val_len);
            w->served++;
            conn_consume(c, consumed);
            continue;
        }

        pk_wal_request_t *pending =
            pk_wal_request_create(req.opcode, req.key, req.key_len,
                                  req.val, req.val_len, c);
        if (pending == NULL) {
            if (!queue_response(c, PK_STATUS_ERROR, NULL, 0))
                return true;
            log_request(w, c, &req, PK_STATUS_ERROR, 0);
            w->served++;
            conn_consume(c, consumed);
            continue;
        }

        /* Set connection state before publish. The callback may run as soon as
         * submit unlocks the queue, but it can only enqueue a completion; this
         * worker remains the sole owner of the connection state machine. */
        c->pending = pending;
        w->pending_count++;
        int submit_error = pk_wal_submit(w->server->wal, pending,
                                         wal_complete, w);
        if (submit_error != 0) {
            c->pending = NULL;
            w->pending_count--;
            pk_wal_request_destroy(pending);
            if (!queue_response(c, PK_STATUS_ERROR, NULL, 0))
                return true;
            log_request(w, c, &req, PK_STATUS_ERROR, 0);
            w->served++;
            conn_consume(c, consumed);
            continue;
        }

        /* The WAL request owns its copies now. Later pipelined bytes remain in
         * rbuf and are deliberately not interpreted until this completion. */
        conn_consume(c, consumed);
        return true;
    }
}

static bool conn_on_readable(worker_t *w, conn_t *c)
{
    if (c->pending != NULL)
        return true;

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
            if (c->pending != NULL)
                return true;
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

    if (c->pending == NULL && c->read_stalled && c->rhave < sizeof(c->rbuf)) {
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
    if (c->fd < 0)
        return true;

    bool need_write = (c->wsent < c->wfill);
    bool need_read  = (c->pending == NULL && !w->draining);
    if (need_write == c->want_write && need_read == c->want_read)
        return true;

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events   = TRIGGER_FLAG | EPOLLRDHUP
                | (need_read ? EPOLLIN : 0u)
                | (need_write ? EPOLLOUT : 0u);
    ev.data.ptr = c;

    if (epoll_ctl(w->epfd, EPOLL_CTL_MOD, c->fd, &ev) < 0) {
        fprintf(stderr, "[t%02d %s] epoll_ctl(MOD): %s\n",
                w->index, c->desc, strerror(errno));
        return false;
    }
    c->want_write = need_write;
    c->want_read  = need_read;
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

static void conn_detach_socket(worker_t *w, conn_t *c)
{
    if (c->fd < 0)
        return;
    epoll_ctl(w->epfd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    c->fd = -1;
    c->want_read = false;
    c->want_write = false;
}

static void conn_close(worker_t *w, conn_t *c)
{
    if (c->pending != NULL) {
        /* The writer callback still carries this pointer. Drop the socket but
         * retain the small connection object until that completion arrives. */
        c->close_after_pending = true;
        conn_detach_socket(w, c);
        return;
    }

    if (c->fd >= 0)
        conn_flush(c);  /* best effort: the last reply may still be buffered */
    conn_detach_socket(w, c);
    conns_remove(c);
    free(c);
}

/* Runs on the WAL writer thread. It never touches epoll or the table; it only
 * hands the owned connection back to its worker and rings that worker's fd. */
static void wal_complete(pk_wal_request_t *request, int error,
                         void *completion_ctx)
{
    worker_t *w = completion_ctx;
    conn_t *c = pk_wal_request_user_data(request);

    c->wal_error = error;
    conn_t *head = atomic_load_explicit(&w->completion_stack,
                                        memory_order_relaxed);
    do {
        c->completion_next = head;
    } while (!atomic_compare_exchange_weak_explicit(
                 &w->completion_stack, &head, c,
                 memory_order_release, memory_order_relaxed));

    bool already_signaled = atomic_exchange_explicit(
        &w->completion_signaled, true, memory_order_acq_rel);
    if (!already_signaled) {
        uint64_t one = 1;
        ssize_t rc = write(w->completion_fd, &one, sizeof(one));
        (void)rc; /* EAGAIN means it is already readable; the queue is authoritative. */
    }
}

static void drain_completion_fd(worker_t *w)
{
    uint64_t value;
    for (;;) {
        ssize_t n = read(w->completion_fd, &value, sizeof(value));
        if (n == (ssize_t)sizeof(value))
            continue;
        if (n < 0 && errno == EINTR)
            continue;
        return;
    }
}

/* Runs on the owning epoll worker. Completion therefore applies the table
 * mutation and advances the connection state machine without cross-thread
 * socket ownership. */
static bool process_wal_completions(worker_t *w)
{
    drain_completion_fd(w);

    /* Clear the notification state before detaching. A producer racing before
     * the exchange is included here (and may leave a harmless extra eventfd
     * wake); one racing after it writes the wakeup for the next batch. */
    atomic_store_explicit(&w->completion_signaled, false, memory_order_release);
    conn_t *stack = atomic_exchange_explicit(&w->completion_stack, NULL,
                                              memory_order_acquire);
    conn_t *completed = NULL;
    while (stack != NULL) {
        conn_t *next = stack->completion_next;
        stack->completion_next = completed;
        completed = stack;
        stack = next;
    }

    bool healthy = true;
    while (completed != NULL) {
        conn_t *c = completed;
        completed = c->completion_next;
        c->completion_next = NULL;

        pk_wal_request_t *pending = c->pending;
        const pk_wal_record_t *record = pk_wal_request_record(pending);
        pk_status_t status = PK_STATUS_ERROR;
        if (pending != NULL && c->wal_error == 0) {
            status = apply_durable_mutation(w, record);
            if (status != PK_STATUS_OK) {
                /* A durable record that cannot be represented in memory must
                 * not let this process continue as if disk and RAM agree. */
                fprintf(stderr, "[t%02d %s] durable mutation could not be applied\n",
                        w->index, c->desc);
                healthy = false;
                request_stop();
            }
        }

        if (record != NULL) {
            pk_request_t req = {
                .opcode  = record->opcode,
                .key_len = record->key_len,
                .key     = record->key,
                .val_len = record->val_len,
                .val     = record->val,
            };
            log_request(w, c, &req, status, 0);
        }
        w->served++;

        c->pending = NULL;
        c->wal_error = 0;
        if (w->pending_count > 0)
            w->pending_count--;

        bool can_reply = c->fd >= 0 && !w->draining && !c->close_after_pending;
        if (can_reply && !queue_response(c, status, NULL, 0))
            c->close_after_pending = true;

        pk_wal_request_destroy(pending);

        if (c->close_after_pending || w->draining) {
            conn_close(w, c);
            continue;
        }

        bool alive = conn_process(w, c);
        if (alive)
            alive = conn_flush(c);
        if (alive)
            alive = conn_update_interest(w, c);
        if (!alive)
            conn_close(w, c);
    }
    return healthy;
}

/* Drain this worker's accept queue: one event can cover several pending peers. */
static void accept_ready(worker_t *w)
{
    for (;;) {
        if (stopping())
            return;

        struct sockaddr_in peer;
        socklen_t peer_len = sizeof(peer);

        int cfd = accept4(w->lfd, (struct sockaddr *)&peer, &peer_len,
                          SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (cfd < 0) {
            if (errno == EINTR || errno == ECONNABORTED)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;  /* queue empty */
            fprintf(stderr, "[t%02d] accept: %s\n", w->index, strerror(errno));
            return;
        }

        int one = 1;
        if (setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
            fprintf(stderr, "[t%02d] setsockopt(TCP_NODELAY): %s\n",
                    w->index, strerror(errno));
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
        c->want_read = true;

        char ip[INET_ADDRSTRLEN];
        if (inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip)) == NULL)
            strcpy(ip, "?");
        snprintf(c->desc, sizeof(c->desc), "%s:%u", ip, ntohs(peer.sin_port));

        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events   = EPOLLIN | EPOLLRDHUP | TRIGGER_FLAG;
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

    w->completion_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (w->completion_fd < 0) {
        perror("eventfd(completion)");
        close(w->epfd);
        close(w->lfd);
        w->epfd = -1;
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

    memset(&ev, 0, sizeof(ev));
    ev.events   = EPOLLIN;
    ev.data.ptr = TAG_COMPLETION;
    if (epoll_ctl(w->epfd, EPOLL_CTL_ADD, w->completion_fd, &ev) < 0) {
        perror("epoll_ctl(ADD completion_fd)");
        goto fail;
    }
    return 0;

fail:
    close(w->completion_fd);
    close(w->epfd);
    close(w->lfd);
    w->completion_fd = -1;
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

static void worker_begin_draining(worker_t *w)
{
    if (w->draining)
        return;
    w->draining = true;

    /* Stop taking new work, then close every socket. Pending WAL requests keep
     * only their conn_t shell alive until the writer hands them back. */
    if (w->lfd >= 0) {
        epoll_ctl(w->epfd, EPOLL_CTL_DEL, w->lfd, NULL);
        close(w->lfd);
        w->lfd = -1;
    }
    epoll_ctl(w->epfd, EPOLL_CTL_DEL, g_stopfd, NULL);

    conn_t *c = w->conns;
    while (c != NULL) {
        conn_t *next = c->all_next;
        conn_close(w, c);
        c = next;
    }
}

static void *worker_main(void *arg)
{
    server_t *server = arg;
    worker_t worker = {
        .index         = atomic_fetch_add_explicit(&g_next_worker_index, 1,
                                                   memory_order_relaxed),
        .lfd           = -1,
        .epfd          = -1,
        .completion_fd = -1,
        .server        = server,
    };
    worker_t *w = &worker;
    struct epoll_event events[MAX_EVENTS];
    bool failed = false;

    atomic_init(&w->completion_stack, NULL);
    atomic_init(&w->completion_signaled, false);
    /* The listener, epoll set, and completion eventfd are all created and
     * eventually closed by their owning worker. */
    if (worker_setup(w) != 0) {
        report_worker_start(false);
        request_stop();
        return (void *)(intptr_t)1;
    }
    report_worker_start(true);

    for (;;) {
        if (w->draining && w->pending_count == 0)
            break;

        int n = epoll_wait(w->epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            fprintf(stderr, "[t%02d] epoll_wait: %s\n", w->index, strerror(errno));
            failed = true;
            request_stop();
            worker_begin_draining(w);
            continue;
        }

        bool saw_stop = stopping();
        bool completions_ready = false;
        for (int i = 0; i < n; i++) {
            if (events[i].data.ptr == TAG_STOPFD)
                saw_stop = true;
            else if (events[i].data.ptr == TAG_COMPLETION)
                completions_ready = true;
        }

        /* If shutdown and socket events arrived together, ignore those socket
         * pointers and retire the connections once, after examining the batch.
         * This avoids freeing an object still referenced later in events[]. */
        if (saw_stop)
            worker_begin_draining(w);

        if (!w->draining) {
            for (int i = 0; i < n; i++) {
                void    *ptr = events[i].data.ptr;
                uint32_t e   = events[i].events;

                if (ptr == TAG_STOPFD || ptr == TAG_COMPLETION)
                    continue;
                if (ptr == TAG_LISTENER) {
                    accept_ready(w);
                    continue;
                }

                conn_t *c = ptr;
                bool alive = true;

                if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    alive = false;
                } else {
                    /* Writable first: draining frees buffer space, which may
                     * let the read side finish a parked frame. */
                    if (e & EPOLLOUT)
                        alive = conn_on_writable(w, c);
                    if (alive && (e & EPOLLIN) && c->pending == NULL)
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

        if (stopping())
            worker_begin_draining(w);

        /* Completion processing comes after socket events because it can free
         * a disconnected pending connection whose fd also appeared above. */
        if (completions_ready && !process_wal_completions(w))
            failed = true;
    }

    /* A worker tears down only what it owns. The shared table outlives every
     * thread, and the WAL has no callbacks left for this worker once its
     * pending count reaches zero. */
    worker_begin_draining(w);
    while (w->conns != NULL)
        conn_close(w, w->conns);
    close(w->completion_fd);
    w->completion_fd = -1;
    close(w->epfd);
    w->epfd = -1;
    g_accepted[w->index] = w->accepted;
    g_served[w->index]   = w->served;
    return failed ? (void *)(intptr_t)1 : NULL;
}

static int replay_record(const pk_wal_record_t *record, void *ctx)
{
    pk_table_t *table = ctx;

    if (record->opcode == PK_OP_SET) {
        switch (pk_table_set(table, record->key, record->key_len,
                             record->val, record->val_len)) {
        case PK_TABLE_OK:      return 0;
        case PK_TABLE_NOMEM:   return ENOMEM;
        default:               return EINVAL;
        }
    }
    if (record->opcode == PK_OP_DEL) {
        switch (pk_table_del(table, record->key, record->key_len)) {
        case PK_TABLE_OK:
        case PK_TABLE_NOT_FOUND: return 0;
        case PK_TABLE_NOMEM:     return ENOMEM;
        default:                 return EINVAL;
        }
    }
    return EINVAL;
}

static double elapsed_ms(struct timespec start, struct timespec end)
{
    time_t seconds = end.tv_sec - start.tv_sec;
    long nanoseconds = end.tv_nsec - start.tv_nsec;
    return (double)seconds * 1000.0 + (double)nanoseconds / 1000000.0;
}

static size_t env_size(const char *name, size_t fallback, size_t maximum)
{
    const char *text = getenv(name);
    if (text == NULL || text[0] == '\0')
        return fallback;

    errno = 0;
    char *end = NULL;
    unsigned long value = strtoul(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || value == 0 || value > maximum) {
        fprintf(stderr, "%s must be an integer from 1 to %zu (using %zu)\n",
                name, maximum, fallback);
        return fallback;
    }
    return (size_t)value;
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

    server_t server;
    memset(&server, 0, sizeof(server));

    int rc = pk_table_init(&server.table);
    if (rc != 0) {
        fprintf(stderr, "pk_table_init: %s\n", strerror(rc));
        close(g_stopfd);
        return EXIT_FAILURE;
    }

    const char *wal_path = getenv("PULSEKV_WAL_PATH");
    if (wal_path == NULL || wal_path[0] == '\0')
        wal_path = PK_WAL_DEFAULT_PATH;
    size_t wal_batch = env_size("PULSEKV_WAL_BATCH_MAX",
                                PK_WAL_DEFAULT_BATCH_MAX, 4096);
    size_t wal_delay_us = env_size("PULSEKV_WAL_DELAY_US",
                                   PK_WAL_DEFAULT_DELAY_US, 1000000);
    size_t recovery_chunk = env_size("PULSEKV_RECOVERY_CHUNK",
                                     PK_WAL_DEFAULT_RECOVERY_CHUNK,
                                     16u * 1024u * 1024u);
    size_t worker_count = env_size("PULSEKV_THREADS", MAX_WORKERS, MAX_WORKERS);

    struct timespec recovery_start;
    struct timespec recovery_end;
    clock_gettime(CLOCK_MONOTONIC, &recovery_start);
    pk_wal_recovery_stats_t recovery = {0};
    bool skip_recovery = getenv("PULSEKV_SKIP_RECOVERY") != NULL;
    rc = skip_recovery
       ? 0
       : pk_wal_recover(wal_path, recovery_chunk, replay_record,
                        &server.table, &recovery);
    clock_gettime(CLOCK_MONOTONIC, &recovery_end);
    if (rc != 0) {
        fprintf(stderr, "pk_wal_recover(%s): %s\n", wal_path, strerror(rc));
        pk_table_destroy(&server.table);
        close(g_stopfd);
        return EXIT_FAILURE;
    }
    if (recovery.last_sequence == UINT64_MAX) {
        fprintf(stderr, "pk_wal_recover(%s): sequence space exhausted\n", wal_path);
        pk_table_destroy(&server.table);
        close(g_stopfd);
        return EXIT_FAILURE;
    }

    printf("recovery: %s%llu records, %llu valid/%llu original bytes, %llu read "
           "calls, %zu keys, %.3f ms",
           skip_recovery ? "SKIPPED (fault injection), " : "",
           (unsigned long long)recovery.records,
           (unsigned long long)recovery.valid_bytes,
           (unsigned long long)recovery.original_bytes,
           (unsigned long long)recovery.read_calls,
           pk_table_count(&server.table),
           elapsed_ms(recovery_start, recovery_end));
    if (recovery.repair != PK_WAL_REPAIR_NONE) {
        printf("; repaired %s (%llu bytes discarded)",
               pk_wal_repair_name(recovery.repair),
               (unsigned long long)recovery.discarded_bytes);
    }
    printf("\n");

    rc = pk_wal_init(&server.wal, wal_path, wal_batch,
                     (uint32_t)wal_delay_us, recovery.last_sequence + 1u);
    if (rc != 0) {
        fprintf(stderr, "pk_wal_init(%s): %s\n", wal_path, strerror(rc));
        pk_table_destroy(&server.table);
        close(g_stopfd);
        return EXIT_FAILURE;
    }

    pthread_t threads[MAX_WORKERS];
    int created = 0;
    bool failed = false;

    for (size_t i = 0; i < worker_count; i++) {
        rc = pthread_create(&threads[i], NULL, worker_main, &server);
        if (rc != 0) {
            fprintf(stderr, "pthread_create: %s\n", strerror(rc));
            failed = true;
            request_stop();
            break;
        }
        created++;
    }

    if ((size_t)created == worker_count) {
        pthread_mutex_lock(&g_start_lock);
        while ((size_t)(g_started_ok + g_started_failed) < worker_count)
            pthread_cond_wait(&g_start_cond, &g_start_lock);
        if (g_started_failed != 0)
            failed = true;
        pthread_mutex_unlock(&g_start_lock);

        if (failed)
            request_stop();
    } else {
        fprintf(stderr, "only %d of %d workers created, shutting down\n",
                created, (int)worker_count);
    }

    if (!failed && !stopping())
        printf("pulsekv listening on 0.0.0.0:%d (%d threads, thread-per-core via "
               "SO_REUSEPORT, %s, %u lock shards / %u buckets, async WAL "
               "%s: batch %zu or %zuus)\n",
               PULSEKV_PORT, (int)worker_count, TRIGGER_NAME,
               PK_TABLE_SHARDS, PK_TABLE_BUCKETS, wal_path,
               wal_batch, wal_delay_us);

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

    /* Every worker has observed all of its completions, so no request remains
     * in the WAL queue. Stop/join the writer before reading its statistics. */
    rc = pk_wal_stop(server.wal);
    if (rc != 0) {
        fprintf(stderr, "WAL stopped with error: %s\n", strerror(rc));
        failed = true;
    }
    pk_wal_stats_t wal_stats = pk_wal_stats(server.wal);

    /* Past every join, so the counters below need no synchronisation and the
     * table has no users left. */
    unsigned long total_conns = 0;
    unsigned long total_reqs  = 0;
    for (size_t i = 0; i < worker_count; i++) {
        total_conns += g_accepted[i];
        total_reqs  += g_served[i];
    }

    printf("shutdown: %lu connections, %lu requests, %zu keys resident\n",
           total_conns, total_reqs, pk_table_count(&server.table));
    printf("WAL: %llu records, %llu batches/fsyncs, %llu bytes, largest batch %zu\n",
           (unsigned long long)wal_stats.records,
           (unsigned long long)wal_stats.batches,
           (unsigned long long)wal_stats.bytes,
           wal_stats.largest_batch);
    printf("per-thread connections:");
    for (size_t i = 0; i < worker_count; i++)
        printf(" t%02zu=%lu", i, g_accepted[i]);
    printf("\n");

    pk_wal_destroy(server.wal);
    pk_table_destroy(&server.table);
    close(g_stopfd);
    return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
