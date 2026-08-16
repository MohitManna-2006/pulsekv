/*
 * PulseKV -- build step 1: blocking TCP skeleton.
 *
 * One connection at a time, fully sequential: accept, serve frames until the
 * client goes away, close, accept again. No event loop, no threads, no store
 * behind it yet -- the handlers are stubs. This exists to prove the wire
 * protocol frames and de-frames correctly over a real socket.
 */

#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define PULSEKV_PORT    9999
#define LISTEN_BACKLOG  128

static bool write_all(int fd, const uint8_t *buf, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, buf + sent, len - sent);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("write");
            return false;
        }
        sent += (size_t)n;
    }
    return true;
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

static bool send_status(int fd, pk_status_t status)
{
    uint8_t out[PK_RESP_HEADER_LEN];  /* stub replies never carry a value */
    pk_response_t resp = { .status = (uint8_t)status, .val_len = 0, .val = NULL };

    int n = pk_response_encode(&resp, out, sizeof(out));
    if (n < 0) {
        fprintf(stderr, "failed to encode response (status=%d)\n", status);
        return false;
    }
    return write_all(fd, out, (size_t)n);
}

static void log_request(const char *peer, const pk_request_t *req, pk_status_t status)
{
    printf("[%s] %s key=\"%.*s\"", peer, pk_opcode_name(req->opcode),
           (int)req->key_len, (const char *)req->key);
    if (req->opcode == PK_OP_SET)
        printf(" val=\"%.*s\"", (int)req->val_len, (const char *)req->val);
    printf(" -> %s\n", pk_status_name(status));
    fflush(stdout);
}

/* Serve one client until it disconnects or sends something unparseable. */
static void handle_conn(int fd, const char *peer)
{
    uint8_t buf[PK_MAX_REQ_LEN];
    size_t have = 0;

    for (;;) {
        pk_request_t req;
        size_t consumed = 0;
        pk_decode_result_t r = pk_request_decode(buf, have, &req, &consumed);

        if (r == PK_DECODE_OK) {
            pk_status_t status = dispatch(&req);
            log_request(peer, &req, status);
            if (!send_status(fd, status))
                return;

            /* Slide any bytes of the next frame down to the front. */
            have -= consumed;
            memmove(buf, buf + consumed, have);
            continue;
        }

        if (r == PK_DECODE_ERROR) {
            fprintf(stderr, "[%s] malformed frame, closing\n", peer);
            send_status(fd, PK_STATUS_ERROR);
            return;
        }

        /* PK_DECODE_INCOMPLETE: pull more bytes off the socket. Any frame the
         * decoder accepts fits in buf, so a full buffer means a decoder bug
         * rather than a legitimately oversized request. */
        if (have == sizeof(buf)) {
            fprintf(stderr, "[%s] buffer full with no complete frame, closing\n", peer);
            send_status(fd, PK_STATUS_ERROR);
            return;
        }

        ssize_t n = read(fd, buf + have, sizeof(buf) - have);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("read");
            return;
        }
        if (n == 0) {
            if (have > 0)
                fprintf(stderr, "[%s] disconnected mid-frame (%zu stray bytes)\n", peer, have);
            else
                printf("[%s] disconnected\n", peer);
            fflush(stdout);
            return;
        }
        have += (size_t)n;
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

    int lfd = listen_socket(PULSEKV_PORT);
    if (lfd < 0)
        return EXIT_FAILURE;

    printf("pulsekv listening on 0.0.0.0:%d\n", PULSEKV_PORT);
    fflush(stdout);

    for (;;) {
        struct sockaddr_in peer;
        socklen_t peer_len = sizeof(peer);

        int cfd = accept(lfd, (struct sockaddr *)&peer, &peer_len);
        if (cfd < 0) {
            if (errno == EINTR || errno == ECONNABORTED)
                continue;
            perror("accept");
            break;
        }

        char peer_desc[INET_ADDRSTRLEN + 8];
        char ip[INET_ADDRSTRLEN];
        if (inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip)) == NULL)
            strcpy(ip, "?");
        snprintf(peer_desc, sizeof(peer_desc), "%s:%u", ip, ntohs(peer.sin_port));

        printf("[%s] connected\n", peer_desc);
        fflush(stdout);

        handle_conn(cfd, peer_desc);
        close(cfd);
    }

    close(lfd);
    return EXIT_FAILURE;
}
