/*
 * PulseKV -- build step 2: single-threaded epoll event loop.
 *
 * One thread, one epoll instance, every socket non-blocking. Connections make
 * progress independently: a client that stalls mid-frame parks its partial
 * bytes in its own read buffer and everyone else keeps being served. Storage is
 * still stubbed -- SET/DEL answer OK, GET answers NOT_FOUND -- until step 3.
 *
 * Step 2 lands in two stages, selected at compile time:
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

#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#define PULSEKV_PORT    9999
#define LISTEN_BACKLOG  512
#define MAX_EVENTS      64

#ifdef PULSEKV_LEVEL_TRIGGERED
#  define TRIGGER_FLAG  0
#  define TRIGGER_NAME  "level-triggered"
#else
#  define TRIGGER_FLAG  EPOLLET
#  define TRIGGER_NAME  "edge-triggered"
#endif

typedef struct {
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
} conn_t;

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
 * Step 3 replaces this with a real hash table lookup. Until then every SET and
 * DEL claims success and every GET misses.
 */
static pk_status_t dispatch(const pk_request_t *req)
{
    switch (req->opcode) {
    case PK_OP_GET:
        return PK_STATUS_NOT_FOUND;
    case PK_OP_SET:
    case PK_OP_DEL:
        return PK_STATUS_OK;
    default:
        return PK_STATUS_ERROR;
    }
}

static void log_request(const conn_t *c, const pk_request_t *req, pk_status_t status)
{
    printf("[%s] %s key=\"%.*s\"", c->desc, pk_opcode_name(req->opcode),
           (int)req->key_len, (const char *)req->key);
    if (req->opcode == PK_OP_SET)
        printf(" val=\"%.*s\"", (int)req->val_len, (const char *)req->val);
    printf(" -> %s\n", pk_status_name(status));
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
static bool queue_status(conn_t *c, pk_status_t status)
{
    if (wbuf_room(c) < PK_RESP_HEADER_LEN)
        wbuf_compact(c);
    if (wbuf_room(c) < PK_RESP_HEADER_LEN)
        return false;

    pk_response_t resp = { .status = (uint8_t)status, .val_len = 0, .val = NULL };
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
static bool conn_process(conn_t *c)
{
    for (;;) {
        pk_request_t req;
        size_t consumed = 0;
        pk_decode_result_t r = pk_request_decode(c->rbuf, c->rhave, &req, &consumed);

        if (r == PK_DECODE_INCOMPLETE)
            return true;

        if (r == PK_DECODE_ERROR) {
            fprintf(stderr, "[%s] malformed frame, closing\n", c->desc);
            queue_status(c, PK_STATUS_ERROR);
            return false;
        }

        pk_status_t status = dispatch(&req);

        /* Queue the reply before consuming the request: if the write buffer is
         * full we leave the frame in rbuf and pick it up again once the socket
         * drains, rather than answering a request we've already thrown away. */
        if (!queue_status(c, status))
            return true;

        log_request(c, &req, status);

        c->rhave -= consumed;
        memmove(c->rbuf, c->rbuf + consumed, c->rhave);
    }
}

static bool conn_on_readable(conn_t *c)
{
    for (;;) {
        if (c->rhave == sizeof(c->rbuf)) {
            /* Nothing decodable left and no room to read into, so the write
             * side is the thing that's behind. Resume once it drains. */
            c->read_stalled = true;
            return true;
        }

        ssize_t n = read(c->fd, c->rbuf + c->rhave, sizeof(c->rbuf) - c->rhave);

        if (n > 0) {
            c->rhave += (size_t)n;
            if (!conn_process(c))
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
                fprintf(stderr, "[%s] disconnected mid-frame (%zu stray bytes)\n",
                        c->desc, c->rhave);
            else
                printf("[%s] disconnected\n", c->desc);
            return false;
        }

        if (errno == EINTR)
            continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return true;  /* drained */

        fprintf(stderr, "[%s] read: %s\n", c->desc, strerror(errno));
        return false;
    }
}

static bool conn_on_writable(conn_t *c)
{
    if (!conn_flush(c))
        return false;

    /* Space freed up: finish any frame that was waiting on it. */
    if (!conn_process(c))
        return false;

    if (c->read_stalled && c->rhave < sizeof(c->rbuf)) {
        c->read_stalled = false;
        return conn_on_readable(c);
    }
    return true;
}

/* ------------------------------------------------------------- epoll plumbing */

/*
 * Only ask for EPOLLOUT while bytes are actually owed. A socket is writable
 * almost all the time, so leaving it armed would spin the loop at full tilt
 * under level-triggered for no reason.
 */
static bool conn_update_interest(int epfd, conn_t *c)
{
    bool need_write = (c->wsent < c->wfill);
    if (need_write == c->want_write)
        return true;

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events   = EPOLLIN | TRIGGER_FLAG | (need_write ? EPOLLOUT : 0u);
    ev.data.ptr = c;

    if (epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd, &ev) < 0) {
        fprintf(stderr, "[%s] epoll_ctl(MOD): %s\n", c->desc, strerror(errno));
        return false;
    }
    c->want_write = need_write;
    return true;
}

static void conn_close(int epfd, conn_t *c)
{
    conn_flush(c);  /* best effort: the last reply may still be buffered */
    epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    free(c);
}

/* Drain the accept queue: one readable event can cover several pending peers. */
static void accept_ready(int epfd, int lfd)
{
    for (;;) {
        struct sockaddr_in peer;
        socklen_t peer_len = sizeof(peer);

        int cfd = accept(lfd, (struct sockaddr *)&peer, &peer_len);
        if (cfd < 0) {
            if (errno == EINTR || errno == ECONNABORTED)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;  /* queue empty */
            perror("accept");
            return;
        }

        if (set_nonblocking(cfd) < 0) {
            close(cfd);
            continue;
        }

        conn_t *c = calloc(1, sizeof(*c));
        if (c == NULL) {
            fprintf(stderr, "out of memory, dropping connection\n");
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

        if (epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &ev) < 0) {
            fprintf(stderr, "[%s] epoll_ctl(ADD): %s\n", c->desc, strerror(errno));
            close(cfd);
            free(c);
            continue;
        }

        printf("[%s] connected\n", c->desc);
    }
}

static int listen_socket(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    /* Without this a restart trips over the previous socket's TIME_WAIT. */
    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
        perror("setsockopt(SO_REUSEADDR)");
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

int main(void)
{
    /* A client that vanishes mid-write must not take the server down with it. */
    signal(SIGPIPE, SIG_IGN);

    /* Line buffering keeps the interleaved per-connection log readable when
     * stdout is a pipe rather than a terminal. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    int lfd = listen_socket(PULSEKV_PORT);
    if (lfd < 0)
        return EXIT_FAILURE;

    int epfd = epoll_create1(0);
    if (epfd < 0) {
        perror("epoll_create1");
        close(lfd);
        return EXIT_FAILURE;
    }

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events   = EPOLLIN | TRIGGER_FLAG;
    ev.data.ptr = NULL;  /* NULL in data.ptr marks the listener */
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &ev) < 0) {
        perror("epoll_ctl(ADD listener)");
        close(epfd);
        close(lfd);
        return EXIT_FAILURE;
    }

    printf("pulsekv listening on 0.0.0.0:%d (single-threaded epoll, %s)\n",
           PULSEKV_PORT, TRIGGER_NAME);

    struct epoll_event events[MAX_EVENTS];
    for (;;) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < n; i++) {
            conn_t  *c = events[i].data.ptr;
            uint32_t e = events[i].events;

            if (c == NULL) {
                accept_ready(epfd, lfd);
                continue;
            }

            bool alive = true;
            if (e & (EPOLLERR | EPOLLHUP)) {
                alive = false;
            } else {
                /* Writable first: draining frees buffer space, which may let
                 * the read side finish a frame it had to leave parked. */
                if (e & EPOLLOUT)
                    alive = conn_on_writable(c);
                if (alive && (e & EPOLLIN))
                    alive = conn_on_readable(c);
                if (alive)
                    alive = conn_flush(c);
                if (alive)
                    alive = conn_update_interest(epfd, c);
            }

            if (!alive)
                conn_close(epfd, c);
        }
    }

    close(epfd);
    close(lfd);
    return EXIT_FAILURE;
}
