/*
 * Concurrency check for build step 2.
 *
 * The blocking server from step 1 would fail this: it serves one connection to
 * completion before accepting the next, so a client that connects and then goes
 * quiet mid-frame stalls everyone behind it. Four phases:
 *
 *   1. open 6 connections at once, one of which sends a deliberately truncated
 *      frame (split across the key_len field) and then says nothing more
 *   2. three rounds of interleaved SET/GET/DEL across the 5 active connections,
 *      all requests sent before any response is read
 *   3. a pipelined burst -- three frames written back-to-back, which tends to
 *      arrive as a single read and shakes out decoders that assume one frame
 *      per wakeup
 *   4. the stalled connection finally completes its frame, proving its partial
 *      bytes survived intact while other connections were served
 *   5. a burst too large for one read() to drain, exercising frame reassembly
 *      across several reads (see the note there on what it does not prove)
 *
 * Storage is still stubbed, so the expected answers are OK for SET and DEL and
 * NOT_FOUND for GET.
 */

#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define SERVER_HOST      "127.0.0.1"
#define SERVER_PORT      9999
#define NUM_CLIENTS      5
#define ROUNDS           3
#define PIPELINE_DEPTH   3
#define BURST_FRAMES     80
#define RECV_TIMEOUT_SEC 5

/*
 * A response buffer has to persist across reads for the same reason the server's
 * does: one read() can return several frames, and whatever is left over belongs
 * to the next call. Keeping it on the stack of the read helper would silently
 * drop the tail of a pipelined burst.
 */
typedef struct {
    int     fd;
    uint8_t rbuf[PK_MAX_RESP_LEN];
    size_t  rhave;
} client_t;

static int g_sent = 0;
static int g_failed = 0;

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

static int build_request(uint8_t *buf, size_t cap, pk_opcode_t opcode,
                         const char *key, const char *val)
{
    pk_request_t req = {
        .opcode  = (uint8_t)opcode,
        .key_len = (uint32_t)strlen(key),
        .key     = (const uint8_t *)key,
        .val_len = val ? (uint32_t)strlen(val) : 0,
        .val     = (const uint8_t *)val,
    };
    return pk_request_encode(&req, buf, cap);
}

static bool send_req(client_t *c, pk_opcode_t opcode, const char *key, const char *val)
{
    uint8_t buf[PK_MAX_REQ_LEN];
    int n = build_request(buf, sizeof(buf), opcode, key, val);
    if (n < 0) {
        fprintf(stderr, "encode failed for %s %s\n", pk_opcode_name(opcode), key);
        return false;
    }
    g_sent++;
    return write_all(c->fd, buf, (size_t)n);
}

/* Take one response frame off the connection. Returns false on timeout, EOF or
 * garbage. Bytes beyond that frame stay buffered for the next call. */
static bool recv_status(client_t *c, uint8_t *status_out)
{
    for (;;) {
        pk_response_t resp;
        size_t consumed = 0;
        pk_decode_result_t r = pk_response_decode(c->rbuf, c->rhave, &resp, &consumed);

        if (r == PK_DECODE_OK) {
            *status_out = resp.status;
            c->rhave -= consumed;
            memmove(c->rbuf, c->rbuf + consumed, c->rhave);
            return true;
        }
        if (r == PK_DECODE_ERROR) {
            fprintf(stderr, "  malformed response frame\n");
            return false;
        }

        if (c->rhave == sizeof(c->rbuf)) {
            fprintf(stderr, "  response buffer full with no complete frame\n");
            return false;
        }

        ssize_t n = read(c->fd, c->rbuf + c->rhave, sizeof(c->rbuf) - c->rhave);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                fprintf(stderr, "  TIMEOUT after %ds -- server never answered\n",
                        RECV_TIMEOUT_SEC);
            else
                perror("  read");
            return false;
        }
        if (n == 0) {
            fprintf(stderr, "  server closed the connection early\n");
            return false;
        }
        c->rhave += (size_t)n;
    }
}

static uint8_t expected_status(pk_opcode_t opcode)
{
    /* No storage until step 3: writes always succeed, reads always miss. */
    return (opcode == PK_OP_GET) ? PK_STATUS_NOT_FOUND : PK_STATUS_OK;
}

/* Read one response and check it against what the stub handler owes us. */
static bool expect(client_t *c, pk_opcode_t opcode, const char *what)
{
    uint8_t got = 0;
    if (!recv_status(c, &got)) {
        fprintf(stderr, "  FAIL %s: no usable response\n", what);
        g_failed++;
        return false;
    }
    uint8_t want = expected_status(opcode);
    if (got != want) {
        fprintf(stderr, "  FAIL %s: expected %s, got %s\n", what,
                pk_status_name(want), pk_status_name(got));
        g_failed++;
        return false;
    }
    return true;
}

static bool connect_to_server(client_t *c)
{
    memset(c, 0, sizeof(*c));

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return false;
    }

    /* Without this a serializing server would hang the test instead of failing it. */
    struct timeval tv = { .tv_sec = RECV_TIMEOUT_SEC, .tv_usec = 0 };
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("setsockopt(SO_RCVTIMEO)");
        close(fd);
        return false;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_HOST, &addr.sin_addr) != 1) {
        fprintf(stderr, "bad server address %s\n", SERVER_HOST);
        close(fd);
        return false;
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return false;
    }

    c->fd = fd;
    return true;
}

int main(void)
{
    static const pk_opcode_t ops[3] = { PK_OP_SET, PK_OP_GET, PK_OP_DEL };

    static client_t clients[NUM_CLIENTS];
    static client_t slow;

    printf("=== phase 1: open %d concurrent connections (%d active + 1 stalled) ===\n",
           NUM_CLIENTS + 1, NUM_CLIENTS);

    for (int i = 0; i < NUM_CLIENTS; i++) {
        if (!connect_to_server(&clients[i]))
            return EXIT_FAILURE;
        printf("  client %d connected\n", i);
    }

    if (!connect_to_server(&slow))
        return EXIT_FAILURE;

    /* Truncate inside the 4-byte key_len field, so the server is left holding a
     * length it cannot even finish reading. */
    uint8_t slow_frame[PK_MAX_REQ_LEN];
    int slow_len = build_request(slow_frame, sizeof(slow_frame), PK_OP_SET,
                                 "slowkey", "slowval");
    if (slow_len < 0)
        return EXIT_FAILURE;
    if (!write_all(slow.fd, slow_frame, 3))
        return EXIT_FAILURE;
    g_sent++;
    printf("  slow client connected, sent 3 of %d bytes, now silent\n", slow_len);

    printf("\n=== phase 2: %d rounds, all sends before any reads ===\n", ROUNDS);
    for (int r = 0; r < ROUNDS; r++) {
        pk_opcode_t round_ops[NUM_CLIENTS];
        char keys[NUM_CLIENTS][32];
        char vals[NUM_CLIENTS][32];

        for (int i = 0; i < NUM_CLIENTS; i++) {
            round_ops[i] = ops[(r + i) % 3];
            snprintf(keys[i], sizeof(keys[i]), "key-c%d-r%d", i, r);
            snprintf(vals[i], sizeof(vals[i]), "val-c%d-r%d", i, r);
            const char *val = (round_ops[i] == PK_OP_SET) ? vals[i] : NULL;
            if (!send_req(&clients[i], round_ops[i], keys[i], val))
                return EXIT_FAILURE;
        }

        printf("  round %d:", r + 1);
        for (int i = 0; i < NUM_CLIENTS; i++) {
            char what[64];
            snprintf(what, sizeof(what), "round %d client %d", r + 1, i);
            bool ok = expect(&clients[i], round_ops[i], what);
            printf("  c%d %s->%s", i, pk_opcode_name((uint8_t)round_ops[i]),
                   ok ? pk_status_name(expected_status(round_ops[i])) : "MISMATCH");
        }
        printf("\n");
    }

    printf("\n=== phase 3: %d pipelined frames in one burst on client 0 ===\n",
           PIPELINE_DEPTH);
    {
        uint8_t burst[PK_MAX_REQ_LEN];
        size_t  burst_len = 0;
        pk_opcode_t burst_ops[PIPELINE_DEPTH];

        for (int i = 0; i < PIPELINE_DEPTH; i++) {
            char key[32];
            snprintf(key, sizeof(key), "pipe-%d", i);
            burst_ops[i] = ops[i % 3];
            const char *val = (burst_ops[i] == PK_OP_SET) ? "pipeval" : NULL;
            int n = build_request(burst + burst_len, sizeof(burst) - burst_len,
                                  burst_ops[i], key, val);
            if (n < 0)
                return EXIT_FAILURE;
            burst_len += (size_t)n;
            g_sent++;
        }

        if (!write_all(clients[0].fd, burst, burst_len))
            return EXIT_FAILURE;
        printf("  wrote %zu bytes covering %d frames in one write()\n",
               burst_len, PIPELINE_DEPTH);

        printf("  responses:");
        for (int i = 0; i < PIPELINE_DEPTH; i++) {
            char what[64];
            snprintf(what, sizeof(what), "pipelined frame %d", i);
            bool ok = expect(&clients[0], burst_ops[i], what);
            printf("  %s->%s", pk_opcode_name((uint8_t)burst_ops[i]),
                   ok ? pk_status_name(expected_status(burst_ops[i])) : "MISMATCH");
        }
        printf("\n");
    }

    printf("\n=== phase 4: stalled client completes its frame ===\n");
    if (!write_all(slow.fd, slow_frame + 3, (size_t)slow_len - 3))
        return EXIT_FAILURE;
    if (expect(&slow, PK_OP_SET, "stalled client"))
        printf("  sent remaining %d bytes, SET->OK (partial frame survived)\n",
               slow_len - 3);

    printf("\n=== phase 5: burst larger than the server's read buffer ===\n");
    {
        /*
         * One read() takes at most PK_MAX_REQ_LEN bytes, so a burst past that
         * size spans several reads. That covers frame reassembly across read
         * boundaries and reuse of the read buffer as it is drained and refilled.
         *
         * What it deliberately does not claim: this does not catch an
         * edge-triggered server that reads once per notification. Edge-triggered
         * epoll re-arms on every new arrival, so a client still streaming keeps
         * handing the server fresh notifications and it catches up regardless.
         * Stranding needs the peer to fall silent while more than one read's
         * worth is already queued -- which is why that bug shows up as rare
         * intermittent stalls in production rather than an outright failure.
         * Confirmed by patching the drain loop to read-once: this test still
         * passes against it, so treat it as coverage, not as proof.
         */
        char bigval[1000];
        memset(bigval, 'x', sizeof(bigval) - 1);
        bigval[sizeof(bigval) - 1] = '\0';

        size_t   cap = (size_t)BURST_FRAMES * (PK_REQ_HEADER_LEN + 32 + sizeof(bigval));
        uint8_t *big = malloc(cap);
        if (big == NULL) {
            perror("malloc");
            return EXIT_FAILURE;
        }

        size_t len = 0;
        for (int i = 0; i < BURST_FRAMES; i++) {
            char key[32];
            snprintf(key, sizeof(key), "burst-%04d", i);
            int n = build_request(big + len, cap - len, PK_OP_SET, key, bigval);
            if (n < 0) {
                free(big);
                return EXIT_FAILURE;
            }
            len += (size_t)n;
            g_sent++;
        }

        printf("  %d frames = %zu bytes in one write(), read buffer is %u bytes"
               " (needs >= %zu reads)\n",
               BURST_FRAMES, len, PK_MAX_REQ_LEN,
               (len + PK_MAX_REQ_LEN - 1) / PK_MAX_REQ_LEN);

        if (!write_all(clients[1].fd, big, len)) {
            free(big);
            return EXIT_FAILURE;
        }
        free(big);

        int good = 0;
        for (int i = 0; i < BURST_FRAMES; i++) {
            uint8_t got = 0;
            if (!recv_status(&clients[1], &got)) {
                g_failed++;
                fprintf(stderr, "  FAIL burst: stalled after %d of %d responses"
                        " -- tail left unread in the kernel\n", good, BURST_FRAMES);
                break;
            }
            if (got != PK_STATUS_OK) {
                g_failed++;
                fprintf(stderr, "  FAIL burst frame %d: expected OK, got %s\n",
                        i, pk_status_name(got));
                break;
            }
            good++;
        }
        if (good == BURST_FRAMES)
            printf("  %d/%d responses correct (whole burst drained)\n",
                   good, BURST_FRAMES);
    }

    for (int i = 0; i < NUM_CLIENTS; i++)
        close(clients[i].fd);
    close(slow.fd);

    printf("\nRESULT: %s (%d requests, %d mismatches)\n",
           g_failed == 0 ? "PASS" : "FAIL", g_sent, g_failed);
    return g_failed == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
