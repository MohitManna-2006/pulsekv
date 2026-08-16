/*
 * Concurrency check for build step 2.
 *
 * The blocking server from step 1 would fail this: it serves one connection to
 * completion before accepting the next, so a client that connects and then goes
 * quiet mid-frame stalls everyone behind it. Four phases:
 *
 *   1. open 6 connections at once, one of which sends a deliberately truncated
 *      frame (split across the key_len field) and then says nothing more
 *   2. a full storage lifecycle -- SET, GET, cross-connection read, overwrite,
 *      DEL, GET -- with all five requests of each step issued before any reply
 *      is read
 *   3. SET/GET/DEL/GET on one key pipelined into a single write, where the
 *      replies are only correct if the frames were applied in order
 *   4. the stalled connection finally completes its frame, proving its partial
 *      bytes survived intact while other connections were served
 *   5. a burst too large for one read() to drain, exercising frame reassembly
 *      across several reads (see the note there on what it does not prove)
 *
 * Storage is real as of step 3, so a GET after a SET must return the stored
 * bytes; only a genuinely absent key answers NOT_FOUND.
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
static bool recv_response(client_t *c, uint8_t *status_out,
                          uint8_t *val_out, size_t val_cap, uint32_t *val_len_out)
{
    for (;;) {
        pk_response_t resp;
        size_t consumed = 0;
        pk_decode_result_t r = pk_response_decode(c->rbuf, c->rhave, &resp, &consumed);

        if (r == PK_DECODE_OK) {
            *status_out  = resp.status;
            *val_len_out = resp.val_len;
            if (resp.val_len > val_cap) {
                fprintf(stderr, "  response value of %u bytes exceeds the test buffer\n",
                        resp.val_len);
                return false;
            }
            if (resp.val_len > 0)
                memcpy(val_out, resp.val, resp.val_len);

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

/*
 * Read one response and check it. want_val is the value a hit must carry, or
 * NULL when the reply is expected to be a bare status.
 */
static bool expect(client_t *c, uint8_t want_status, const char *want_val, const char *what)
{
    uint8_t  got      = 0;
    uint8_t  val[PK_MAX_VAL_LEN];
    uint32_t val_len  = 0;

    if (!recv_response(c, &got, val, sizeof(val), &val_len)) {
        fprintf(stderr, "  FAIL %s: no usable response\n", what);
        g_failed++;
        return false;
    }
    if (got != want_status) {
        fprintf(stderr, "  FAIL %s: expected %s, got %s\n", what,
                pk_status_name(want_status), pk_status_name(got));
        g_failed++;
        return false;
    }

    size_t want_len = want_val ? strlen(want_val) : 0;
    if (val_len != want_len || (want_len > 0 && memcmp(val, want_val, want_len) != 0)) {
        fprintf(stderr, "  FAIL %s: expected value \"%s\" (%zu bytes), got %u bytes \"%.*s\"\n",
                what, want_val ? want_val : "", want_len, val_len,
                (int)(val_len > 40 ? 40 : val_len), (const char *)val);
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

    printf("\n=== phase 2: storage lifecycle, interleaved across %d connections ===\n",
           NUM_CLIENTS);
    {
        char keys[NUM_CLIENTS][32];
        char first[NUM_CLIENTS][48];
        char second[NUM_CLIENTS][48];
        /* Generous: gcc cannot bound keys[i] through the 2-D array and assumes
         * it may run to the end of the whole thing. */
        char what[224];

        for (int i = 0; i < NUM_CLIENTS; i++) {
            snprintf(keys[i],   sizeof(keys[i]),   "key-c%d", i);
            snprintf(first[i],  sizeof(first[i]),  "first-value-from-client-%d", i);
            snprintf(second[i], sizeof(second[i]), "overwritten-by-client-%d", i);
        }

        /* Every step issues all five requests before reading any reply, which
         * is the part a server that serialised connections would fail. */
        for (int i = 0; i < NUM_CLIENTS; i++)
            if (!send_req(&clients[i], PK_OP_SET, keys[i], first[i]))
                return EXIT_FAILURE;
        for (int i = 0; i < NUM_CLIENTS; i++) {
            snprintf(what, sizeof(what), "SET %s", keys[i]);
            expect(&clients[i], PK_STATUS_OK, NULL, what);
        }
        printf("  SET on all %d connections            -> OK\n", NUM_CLIENTS);

        for (int i = 0; i < NUM_CLIENTS; i++)
            if (!send_req(&clients[i], PK_OP_GET, keys[i], NULL))
                return EXIT_FAILURE;
        for (int i = 0; i < NUM_CLIENTS; i++) {
            snprintf(what, sizeof(what), "GET %s", keys[i]);
            expect(&clients[i], PK_STATUS_OK, first[i], what);
        }
        printf("  GET returns the stored value         -> OK + value\n");

        /* Each connection reads the key its neighbour wrote: one shared table,
         * not per-connection state. */
        for (int i = 0; i < NUM_CLIENTS; i++)
            if (!send_req(&clients[i], PK_OP_GET, keys[(i + 1) % NUM_CLIENTS], NULL))
                return EXIT_FAILURE;
        for (int i = 0; i < NUM_CLIENTS; i++) {
            int owner = (i + 1) % NUM_CLIENTS;
            snprintf(what, sizeof(what), "client %d reads %s", i, keys[owner]);
            expect(&clients[i], PK_STATUS_OK, first[owner], what);
        }
        printf("  each connection reads a neighbour's  -> one shared table\n");

        for (int i = 0; i < NUM_CLIENTS; i++)
            if (!send_req(&clients[i], PK_OP_SET, keys[i], second[i]))
                return EXIT_FAILURE;
        for (int i = 0; i < NUM_CLIENTS; i++) {
            snprintf(what, sizeof(what), "overwrite %s", keys[i]);
            expect(&clients[i], PK_STATUS_OK, NULL, what);
        }
        for (int i = 0; i < NUM_CLIENTS; i++)
            if (!send_req(&clients[i], PK_OP_GET, keys[i], NULL))
                return EXIT_FAILURE;
        for (int i = 0; i < NUM_CLIENTS; i++) {
            snprintf(what, sizeof(what), "GET %s after overwrite", keys[i]);
            expect(&clients[i], PK_STATUS_OK, second[i], what);
        }
        printf("  SET again then GET                   -> replaced value\n");

        for (int i = 0; i < NUM_CLIENTS; i++)
            if (!send_req(&clients[i], PK_OP_DEL, keys[i], NULL))
                return EXIT_FAILURE;
        for (int i = 0; i < NUM_CLIENTS; i++) {
            snprintf(what, sizeof(what), "DEL %s", keys[i]);
            expect(&clients[i], PK_STATUS_OK, NULL, what);
        }
        for (int i = 0; i < NUM_CLIENTS; i++)
            if (!send_req(&clients[i], PK_OP_GET, keys[i], NULL))
                return EXIT_FAILURE;
        for (int i = 0; i < NUM_CLIENTS; i++) {
            snprintf(what, sizeof(what), "GET %s after DEL", keys[i]);
            expect(&clients[i], PK_STATUS_NOT_FOUND, NULL, what);
        }
        printf("  DEL then GET                         -> NOT_FOUND\n");
    }

    printf("\n=== phase 3: a whole key lifecycle pipelined into one write ===\n");
    {
        /* SET, GET, DEL, GET on one key, written back-to-back. These are not
         * independent: each reply is only correct if the server applied the
         * frames in order rather than merely parsing them all. */
        static const pk_opcode_t seq[]  = { PK_OP_SET, PK_OP_GET, PK_OP_DEL, PK_OP_GET };
        static const uint8_t     want[] = { PK_STATUS_OK, PK_STATUS_OK,
                                            PK_STATUS_OK, PK_STATUS_NOT_FOUND };
        const int   nseq   = (int)(sizeof(seq) / sizeof(seq[0]));
        const char *pipeval = "pipelined-value";

        uint8_t burst[PK_MAX_REQ_LEN];
        size_t  burst_len = 0;

        for (int i = 0; i < nseq; i++) {
            const char *val = (seq[i] == PK_OP_SET) ? pipeval : NULL;
            int n = build_request(burst + burst_len, sizeof(burst) - burst_len,
                                  seq[i], "pipe-key", val);
            if (n < 0)
                return EXIT_FAILURE;
            burst_len += (size_t)n;
            g_sent++;
        }

        if (!write_all(clients[0].fd, burst, burst_len))
            return EXIT_FAILURE;
        printf("  wrote %zu bytes covering %d frames in one write()\n", burst_len, nseq);

        for (int i = 0; i < nseq; i++) {
            char what[64];
            snprintf(what, sizeof(what), "pipelined %s", pk_opcode_name((uint8_t)seq[i]));
            /* only the GET between SET and DEL carries a value back */
            const char *want_val = (i == 1) ? pipeval : NULL;
            if (expect(&clients[0], want[i], want_val, what))
                printf("  %-3s -> %s%s\n", pk_opcode_name((uint8_t)seq[i]),
                       pk_status_name(want[i]), want_val ? " + value" : "");
        }
    }

    printf("\n=== phase 4: stalled client completes its frame ===\n");
    if (!write_all(slow.fd, slow_frame + 3, (size_t)slow_len - 3))
        return EXIT_FAILURE;
    if (expect(&slow, PK_STATUS_OK, NULL, "stalled client SET"))
        printf("  sent remaining %d bytes, SET->OK (partial frame survived)\n",
               slow_len - 3);
    /* And the value it wrote is really in the table, not just acknowledged. */
    if (!send_req(&slow, PK_OP_GET, "slowkey", NULL))
        return EXIT_FAILURE;
    if (expect(&slow, PK_STATUS_OK, "slowval", "stalled client GET"))
        printf("  GET slowkey -> OK + \"slowval\"\n");

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
            uint8_t  got     = 0;
            uint8_t  val[PK_MAX_VAL_LEN];
            uint32_t val_len = 0;

            if (!recv_response(&clients[1], &got, val, sizeof(val), &val_len)) {
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
            printf("  %d/%d SETs acknowledged (whole burst drained)\n",
                   good, BURST_FRAMES);

        /* Spot-check that the burst was stored, not just acknowledged. */
        static const int probes[] = { 0, BURST_FRAMES / 2, BURST_FRAMES - 1 };
        for (size_t p = 0; p < sizeof(probes) / sizeof(probes[0]); p++) {
            char key[32], what[64];
            snprintf(key,  sizeof(key),  "burst-%04d", probes[p]);
            snprintf(what, sizeof(what), "GET %s", key);
            if (!send_req(&clients[1], PK_OP_GET, key, NULL))
                return EXIT_FAILURE;
            expect(&clients[1], PK_STATUS_OK, bigval, what);
        }
        printf("  spot-checked %zu keys, each returning its %zu-byte value\n",
               sizeof(probes) / sizeof(probes[0]), strlen(bigval));
    }

    for (int i = 0; i < NUM_CLIENTS; i++)
        close(clients[i].fd);
    close(slow.fd);

    printf("\nRESULT: %s (%d requests, %d mismatches)\n",
           g_failed == 0 ? "PASS" : "FAIL", g_sent, g_failed);
    return g_failed == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
