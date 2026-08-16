/*
 * Concurrency stress for build step 4.
 *
 * 64 client threads, each on its own connection, hammering the server in
 * parallel. More clients than server threads on purpose: the kernel spreads
 * connections across the 16 SO_REUSEPORT listeners, so several land on the same
 * worker and the shared table is genuinely hit from every direction at once.
 *
 * Two invariants do the real work here:
 *
 *   own keys     Only one thread ever touches "t<id>-own-<j>", so a GET
 *                immediately after that thread's own SET must return exactly
 *                what it wrote. Keys from different threads still collide into
 *                shared buckets, so a chain corrupted by concurrent inserts
 *                shows up here as a lost or wrong value.
 *
 *   shared keys  Every thread writes "shared-<j>", so the winner is a race and
 *                the exact value cannot be asserted. What can be asserted is
 *                that the value is *one whole value* -- each thread fills its
 *                value with a single repeated character, so any interleaving of
 *                two writers, or a read that catches a value mid-replacement,
 *                comes back as a mix of characters or a length nobody wrote.
 *
 * Usage: test_thread_stress [iterations]   (default ITERATIONS)
 */

#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define SERVER_HOST      "127.0.0.1"
#define SERVER_PORT      9999
#define NUM_CLIENTS      64
#define ITERATIONS       300
#define OWN_KEYS         4
#define SHARED_KEYS      8
#define SHARED_CHURN_KEYS 4
#define SHARED_MIN_LEN   64
#define SHARED_MAX_LEN   256
#define VAL_CAP          1024
#define RECV_TIMEOUT_SEC 30

typedef struct {
    int  id;
    int  iterations;
    int  fd;

    /* Responses can arrive several to a read, so leftovers must persist. */
    uint8_t rbuf[PK_MAX_RESP_LEN];
    size_t  rhave;

    long failures;
    long requests;
    char first_failure[256];
} client_t;

/* All 64 connections are established before this gate opens, so the client
 * threads begin issuing requests together instead of merely being created in
 * a loop and hoping their schedules overlap. */
typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    bool            open;
    bool            abort;
} start_gate_t;

static start_gate_t g_start_gate = {
    .lock = PTHREAD_MUTEX_INITIALIZER,
    .cond = PTHREAD_COND_INITIALIZER,
};

static void fail(client_t *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

static void fail(client_t *c, const char *fmt, ...)
{
    c->failures++;
    if (c->first_failure[0] == '\0') {
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(c->first_failure, sizeof(c->first_failure), fmt, ap);
        va_end(ap);
    }
}

static bool write_all(int fd, const uint8_t *buf, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, buf + sent, len - sent);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return false;
        }
        if (n == 0)
            return false;
        sent += (size_t)n;
    }
    return true;
}

static bool send_req(client_t *c, pk_opcode_t opcode,
                     const char *key, const uint8_t *val, uint32_t val_len)
{
    pk_request_t req = {
        .opcode  = (uint8_t)opcode,
        .key_len = (uint32_t)strlen(key),
        .key     = (const uint8_t *)key,
        .val_len = val_len,
        .val     = val,
    };
    uint8_t buf[PK_MAX_REQ_LEN];

    int n = pk_request_encode(&req, buf, sizeof(buf));
    if (n < 0)
        return false;
    c->requests++;
    return write_all(c->fd, buf, (size_t)n);
}

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
            if (resp.val_len > val_cap)
                return false;  /* longer than anything this test ever stores */
            if (resp.val_len > 0)
                memcpy(val_out, resp.val, resp.val_len);

            c->rhave -= consumed;
            memmove(c->rbuf, c->rbuf + consumed, c->rhave);
            return true;
        }
        if (r == PK_DECODE_ERROR || c->rhave == sizeof(c->rbuf))
            return false;

        ssize_t n = read(c->fd, c->rbuf + c->rhave, sizeof(c->rbuf) - c->rhave);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return false;
        }
        if (n == 0)
            return false;
        c->rhave += (size_t)n;
    }
}

/* Send one request and read its reply, checking only the status. */
static bool op_expect(client_t *c, pk_opcode_t opcode, const char *key,
                      const uint8_t *val, uint32_t val_len,
                      uint8_t want_status, const char *what)
{
    if (!send_req(c, opcode, key, val, val_len)) {
        fail(c, "%s: send failed on %s", what, key);
        return false;
    }

    uint8_t  got = 0;
    uint8_t  rv[VAL_CAP];
    uint32_t rvlen = 0;

    if (!recv_response(c, &got, rv, sizeof(rv), &rvlen)) {
        fail(c, "%s: no usable response for %s", what, key);
        return false;
    }
    if (got != want_status) {
        fail(c, "%s on %s: expected %s, got %s", what, key,
             pk_status_name(want_status), pk_status_name(got));
        return false;
    }
    return true;
}

static void own_key(char *out, size_t cap, int id, int j)
{
    snprintf(out, cap, "t%02d-own-%d", id, j);
}

static void own_value(char *out, size_t cap, int id, int j, int iter)
{
    snprintf(out, cap, "thread-%02d-key-%d-iter-%06d", id, j, iter);
}

/* Every thread fills its shared value with one repeated letter, so a value
 * assembled from two writers is detectable on sight. */
static uint8_t shared_fill(int id)
{
    return (uint8_t)('A' + (id % 26));
}

static uint32_t shared_len(int iter)
{
    return (uint32_t)(SHARED_MIN_LEN + (iter % (SHARED_MAX_LEN - SHARED_MIN_LEN)));
}

static bool shared_value_is_whole(const uint8_t *v, uint32_t len)
{
    if (len < SHARED_MIN_LEN || len > SHARED_MAX_LEN)
        return false;
    if (v[0] < 'A' || v[0] > 'Z')
        return false;
    for (uint32_t i = 1; i < len; i++)
        if (v[i] != v[0])
            return false;
    return true;
}

static int connect_to_server(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    struct timeval tv = { .tv_sec = RECV_TIMEOUT_SEC, .tv_usec = 0 };
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_HOST, &addr.sin_addr) != 1
        || connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static bool wait_for_start(void)
{
    pthread_mutex_lock(&g_start_gate.lock);
    while (!g_start_gate.open)
        pthread_cond_wait(&g_start_gate.cond, &g_start_gate.lock);
    bool run = !g_start_gate.abort;
    pthread_mutex_unlock(&g_start_gate.lock);
    return run;
}

static void release_clients(bool abort)
{
    pthread_mutex_lock(&g_start_gate.lock);
    g_start_gate.abort = abort;
    g_start_gate.open  = true;
    pthread_cond_broadcast(&g_start_gate.cond);
    pthread_mutex_unlock(&g_start_gate.lock);
}

/* A shared-key GET can legitimately lose a race with another client's DEL or
 * SET. It may miss or return a whole value from one writer, but it must never
 * return a torn/corrupt value or any other status. */
static bool get_shared_after_churn(client_t *c, const char *key)
{
    if (!send_req(c, PK_OP_GET, key, NULL, 0)) {
        fail(c, "GET churn: send failed on %s", key);
        return false;
    }

    uint8_t  got = 0, rv[VAL_CAP];
    uint32_t rvlen = 0;
    if (!recv_response(c, &got, rv, sizeof(rv), &rvlen)) {
        fail(c, "GET churn: no usable response for %s", key);
        return false;
    }
    if (got == PK_STATUS_NOT_FOUND)
        return true;
    if (got != PK_STATUS_OK || !shared_value_is_whole(rv, rvlen)) {
        fail(c, "GET churn %s: status %s, invalid %u-byte value",
             key, pk_status_name(got), rvlen);
        return false;
    }
    return true;
}

static void *client_main(void *arg)
{
    client_t *c = arg;

    if (!wait_for_start())
        return NULL;

    for (int iter = 0; iter < c->iterations; iter++) {
        char key[64], val[64];
        int  j = iter % OWN_KEYS;

        own_key(key, sizeof(key), c->id, j);
        own_value(val, sizeof(val), c->id, j, iter);

        /* Nobody else writes this key, so the read-back must be exact. */
        if (!op_expect(c, PK_OP_SET, key, (const uint8_t *)val,
                       (uint32_t)strlen(val), PK_STATUS_OK, "SET own"))
            return NULL;

        if (!send_req(c, PK_OP_GET, key, NULL, 0)) {
            fail(c, "GET own: send failed on %s", key);
            return NULL;
        }
        {
            uint8_t  got = 0, rv[VAL_CAP];
            uint32_t rvlen = 0;

            if (!recv_response(c, &got, rv, sizeof(rv), &rvlen)) {
                fail(c, "GET own: no usable response for %s", key);
                return NULL;
            }
            if (got != PK_STATUS_OK) {
                fail(c, "GET own %s: expected OK, got %s", key, pk_status_name(got));
                return NULL;
            }
            if (rvlen != strlen(val) || memcmp(rv, val, rvlen) != 0) {
                fail(c, "LOST UPDATE on %s: wrote %zu bytes, read back %u",
                     key, strlen(val), rvlen);
                return NULL;
            }
        }

        /* Contended: every thread writes these. */
        {
            char     skey[32];
            uint8_t  sval[SHARED_MAX_LEN];
            uint32_t slen = shared_len(iter);

            snprintf(skey, sizeof(skey), "shared-%d", iter % SHARED_KEYS);
            memset(sval, shared_fill(c->id), slen);

            if (!op_expect(c, PK_OP_SET, skey, sval, slen, PK_STATUS_OK, "SET shared"))
                return NULL;

            if (!send_req(c, PK_OP_GET, skey, NULL, 0)) {
                fail(c, "GET shared: send failed on %s", skey);
                return NULL;
            }

            uint8_t  got = 0, rv[VAL_CAP];
            uint32_t rvlen = 0;
            if (!recv_response(c, &got, rv, sizeof(rv), &rvlen)) {
                fail(c, "GET shared: no usable response for %s", skey);
                return NULL;
            }
            /* A concurrent DEL is not in play for shared keys, so a miss here
             * would itself be wrong. */
            if (got != PK_STATUS_OK) {
                fail(c, "GET shared %s: expected OK, got %s", skey, pk_status_name(got));
                return NULL;
            }
            if (!shared_value_is_whole(rv, rvlen)) {
                fail(c, "CORRUPT VALUE on %s: %u bytes, first=0x%02x", skey, rvlen,
                     rvlen ? rv[0] : 0);
                return NULL;
            }
        }

        /* Periodically churn the chain so deletes race inserts. */
        if (iter % 32 == 31) {
            char     shared_key[32];
            uint8_t  shared_val[SHARED_MAX_LEN];
            uint32_t shared_vlen = shared_len(iter);

            snprintf(shared_key, sizeof(shared_key), "shared-churn-%d",
                     (iter / 32) % SHARED_CHURN_KEYS);
            memset(shared_val, shared_fill(c->id), shared_vlen);
            if (!op_expect(c, PK_OP_SET, shared_key, shared_val, shared_vlen,
                           PK_STATUS_OK, "SET shared churn"))
                return NULL;
            if (!op_expect(c, PK_OP_DEL, shared_key, NULL, 0,
                           PK_STATUS_OK, "DEL shared churn"))
                return NULL;
            if (!get_shared_after_churn(c, shared_key))
                return NULL;

            if (!op_expect(c, PK_OP_DEL, key, NULL, 0, PK_STATUS_OK, "DEL own"))
                return NULL;
            if (!op_expect(c, PK_OP_GET, key, NULL, 0, PK_STATUS_NOT_FOUND,
                           "GET after DEL"))
                return NULL;
            if (!op_expect(c, PK_OP_SET, key, (const uint8_t *)val,
                           (uint32_t)strlen(val), PK_STATUS_OK, "re-SET own"))
                return NULL;
        }
    }

    /* Leave every own key at a value the verification pass can predict, and
     * one deleted so the final state exercises both outcomes. */
    for (int j = 0; j < OWN_KEYS; j++) {
        char key[64], val[64];
        own_key(key, sizeof(key), c->id, j);

        if (j == 0) {
            if (!op_expect(c, PK_OP_DEL, key, NULL, 0, PK_STATUS_OK, "final DEL"))
                return NULL;
            continue;
        }
        own_value(val, sizeof(val), c->id, j, -1);
        if (!op_expect(c, PK_OP_SET, key, (const uint8_t *)val,
                       (uint32_t)strlen(val), PK_STATUS_OK, "final SET"))
            return NULL;
    }
    return NULL;
}

int main(int argc, char **argv)
{
    int iterations = ITERATIONS;
    if (argc > 1) {
        iterations = atoi(argv[1]);
        if (iterations <= 0)
            iterations = ITERATIONS;
    }

    client_t *clients = calloc(NUM_CLIENTS, sizeof(*clients));
    if (clients == NULL) {
        perror("calloc");
        return EXIT_FAILURE;
    }

    printf("=== %d client threads x %d iterations against %d server threads ===\n",
           NUM_CLIENTS, iterations, 16);

    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients[i].id         = i;
        clients[i].iterations = iterations;
        clients[i].fd         = connect_to_server();
        if (clients[i].fd < 0) {
            fprintf(stderr, "client %d could not connect\n", i);
            for (int j = 0; j < i; j++)
                close(clients[j].fd);
            free(clients);
            return EXIT_FAILURE;
        }
    }
    printf("  %d connections established\n", NUM_CLIENTS);

    pthread_t threads[NUM_CLIENTS];
    int created = 0;
    bool launch_failed = false;
    for (int i = 0; i < NUM_CLIENTS; i++) {
        int rc = pthread_create(&threads[i], NULL, client_main, &clients[i]);
        if (rc != 0) {
            fprintf(stderr, "pthread_create: %s\n", strerror(rc));
            launch_failed = true;
            break;
        }
        created++;
    }
    release_clients(launch_failed);
    for (int i = 0; i < created; i++)
        pthread_join(threads[i], NULL);

    long failures = launch_failed ? 1 : 0;
    long requests = 0;
    for (int i = 0; i < NUM_CLIENTS; i++) {
        failures += clients[i].failures;
        requests += clients[i].requests;
        if (clients[i].first_failure[0] != '\0')
            fprintf(stderr, "  client %d: %s\n", i, clients[i].first_failure);
    }
    printf("  %ld requests issued across %d threads, %ld failures during the run\n",
           requests, NUM_CLIENTS, failures);

    if (launch_failed) {
        for (int i = 0; i < NUM_CLIENTS; i++)
            close(clients[i].fd);
        pthread_cond_destroy(&g_start_gate.cond);
        pthread_mutex_destroy(&g_start_gate.lock);
        free(clients);
        return EXIT_FAILURE;
    }

    /* Verification pass on one fresh connection, with nothing else running. */
    printf("\n=== final state verification (single connection, quiesced) ===\n");
    client_t v;
    memset(&v, 0, sizeof(v));
    v.fd = connect_to_server();
    if (v.fd < 0) {
        fprintf(stderr, "verification connection failed\n");
        free(clients);
        return EXIT_FAILURE;
    }

    int checked = 0;
    for (int i = 0; i < NUM_CLIENTS; i++) {
        for (int j = 0; j < OWN_KEYS; j++) {
            char key[64], val[64];
            own_key(key, sizeof(key), i, j);

            if (j == 0) {
                op_expect(&v, PK_OP_GET, key, NULL, 0, PK_STATUS_NOT_FOUND,
                          "verify deleted own key");
                checked++;
                continue;
            }

            own_value(val, sizeof(val), i, j, -1);
            if (!send_req(&v, PK_OP_GET, key, NULL, 0)) {
                fail(&v, "verify: send failed on %s", key);
                break;
            }
            uint8_t  got = 0, rv[VAL_CAP];
            uint32_t rvlen = 0;
            if (!recv_response(&v, &got, rv, sizeof(rv), &rvlen)) {
                fail(&v, "verify: no response for %s", key);
                break;
            }
            if (got != PK_STATUS_OK || rvlen != strlen(val)
                || memcmp(rv, val, rvlen) != 0) {
                fail(&v, "verify %s: final value wrong (status %s, %u bytes)",
                     key, pk_status_name(got), rvlen);
            }
            checked++;
        }
    }
    printf("  %d own keys verified (%d threads x %d keys)\n",
           checked, NUM_CLIENTS, OWN_KEYS);

    int shared_ok = 0;
    for (int j = 0; j < SHARED_KEYS; j++) {
        char skey[32];
        snprintf(skey, sizeof(skey), "shared-%d", j);

        if (!send_req(&v, PK_OP_GET, skey, NULL, 0))
            break;
        uint8_t  got = 0, rv[VAL_CAP];
        uint32_t rvlen = 0;
        if (!recv_response(&v, &got, rv, sizeof(rv), &rvlen)) {
            fail(&v, "verify: no response for %s", skey);
            break;
        }
        if (got != PK_STATUS_OK || !shared_value_is_whole(rv, rvlen)) {
            fail(&v, "verify %s: not a whole value (status %s, %u bytes)",
                 skey, pk_status_name(got), rvlen);
            continue;
        }
        shared_ok++;
    }
    printf("  %d/%d contended keys hold one intact value from a single writer\n",
           shared_ok, SHARED_KEYS);

    int churn_deleted = 0;
    for (int j = 0; j < SHARED_CHURN_KEYS; j++) {
        char key[32];
        snprintf(key, sizeof(key), "shared-churn-%d", j);

        if (!send_req(&v, PK_OP_GET, key, NULL, 0)) {
            fail(&v, "verify: send failed on %s", key);
            break;
        }
        uint8_t  got = 0, rv[VAL_CAP];
        uint32_t rvlen = 0;
        if (!recv_response(&v, &got, rv, sizeof(rv), &rvlen)) {
            fail(&v, "verify: no response for %s", key);
            break;
        }
        if (got != PK_STATUS_NOT_FOUND || rvlen != 0) {
            fail(&v, "verify %s: expected final delete, got %s/%u bytes",
                 key, pk_status_name(got), rvlen);
            continue;
        }
        churn_deleted++;
    }
    printf("  %d/%d contended churn keys finish deleted\n",
           churn_deleted, SHARED_CHURN_KEYS);

    if (v.first_failure[0] != '\0')
        fprintf(stderr, "  verification: %s\n", v.first_failure);

    close(v.fd);
    for (int i = 0; i < NUM_CLIENTS; i++)
        close(clients[i].fd);

    long total_failures = failures + v.failures;
    printf("\nRESULT: %s (%ld requests, %ld failures)\n",
           total_failures == 0 ? "PASS" : "FAIL", requests + v.requests, total_failures);

    pthread_cond_destroy(&g_start_gate.cond);
    pthread_mutex_destroy(&g_start_gate.lock);
    free(clients);
    return total_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
