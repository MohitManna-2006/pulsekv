/*
 * Unit test for the store, with no network in the picture.
 *
 * The interesting cases are the ones that separate a sharded hash table that
 * works from one that only appears to: two keys sharing a lock but not a chain,
 * two keys chained in the same bucket, concurrent shared-key churn, an
 * overwrite that has to free what it replaces, and whether the table actually
 * copies what it is given. The server hands it pointers into a connection's
 * read buffer, which is overwritten by the next frame, so anything held by
 * reference would come back as garbage a moment later.
 *
 * Run under valgrind: several of these assertions are about lifetime, and a
 * leak checker is the only thing that sees the difference.
 */

#include "hashtable.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define VALBUF 256
#define DIRECT_THREADS     32
#define DIRECT_KEYS         4
#define DIRECT_SHARED_KEYS  8
#define DIRECT_ITERATIONS 1000

static int g_checks = 0;
static int g_failed = 0;

static void ok(bool cond, const char *what)
{
    g_checks++;
    if (cond) {
        printf("  ok    %s\n", what);
    } else {
        printf("  FAIL  %s\n", what);
        g_failed++;
    }
}

static const uint8_t *B(const char *s)
{
    return (const uint8_t *)s;
}

static uint32_t L(const char *s)
{
    return (uint32_t)strlen(s);
}

/* Fetch and compare against an expected C string. */
static bool get_is(pk_table_t *t, const char *key, const char *want)
{
    uint8_t  buf[VALBUF];
    uint32_t len = 0;

    if (pk_table_get(t, B(key), L(key), buf, sizeof(buf), &len) != PK_TABLE_OK)
        return false;
    return len == L(want) && memcmp(buf, want, len) == 0;
}

static bool get_misses(pk_table_t *t, const char *key)
{
    uint8_t  buf[VALBUF];
    uint32_t len = 0;

    return pk_table_get(t, B(key), L(key), buf, sizeof(buf), &len) == PK_TABLE_NOT_FOUND;
}

static size_t bucket_of(const char *key)
{
    return (size_t)(pk_table_hash(B(key), L(key)) & (PK_TABLE_BUCKETS - 1u));
}

static size_t shard_of(const char *key)
{
    return bucket_of(key) & (PK_TABLE_SHARDS - 1u);
}

static size_t bucket_in_shard_of(const char *key)
{
    return bucket_of(key) / PK_TABLE_SHARDS;
}

/*
 * Search for a second key landing in the same bucket as the first, rather than
 * hardcoding a pair that would quietly stop colliding if PK_TABLE_BUCKETS or
 * the hash ever changed.
 */
static bool find_collision(const char *anchor, char *out, size_t out_cap, size_t *bucket)
{
    size_t target = bucket_of(anchor);

    for (int i = 0; i < 1000000; i++) {
        snprintf(out, out_cap, "probe-%d", i);
        if (bucket_of(out) == target && strcmp(out, anchor) != 0) {
            *bucket = target;
            return true;
        }
    }
    return false;
}

static bool find_same_shard_other_bucket(const char *anchor, char *out,
                                         size_t out_cap)
{
    size_t shard  = shard_of(anchor);
    size_t bucket = bucket_in_shard_of(anchor);

    for (int i = 0; i < 1000000; i++) {
        snprintf(out, out_cap, "stripe-%d", i);
        if (shard_of(out) == shard && bucket_in_shard_of(out) != bucket)
            return true;
    }
    return false;
}

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    bool            go;
    bool            abort;
} direct_gate_t;

typedef struct {
    pk_table_t   *table;
    direct_gate_t *gate;
    int           id;
    int           failures;
    char          first_failure[160];
} direct_client_t;

static void direct_fail(direct_client_t *c, const char *what, const char *key)
{
    c->failures++;
    if (c->first_failure[0] == '\0')
        snprintf(c->first_failure, sizeof(c->first_failure), "%s: %s", what, key);
}

static bool direct_shared_value_is_whole(const uint8_t *value, uint32_t len)
{
    if (len < 32 || len > 63 || value[0] < 'A' || value[0] > 'Z')
        return false;
    for (uint32_t i = 1; i < len; i++)
        if (value[i] != value[0])
            return false;
    return true;
}

static void *direct_client_main(void *arg)
{
    direct_client_t *c = arg;

    pthread_mutex_lock(&c->gate->lock);
    while (!c->gate->go)
        pthread_cond_wait(&c->gate->cond, &c->gate->lock);
    bool run = !c->gate->abort;
    pthread_mutex_unlock(&c->gate->lock);
    if (!run)
        return NULL;

    for (int iter = 0; iter < DIRECT_ITERATIONS; iter++) {
        char key[48], value[64];
        int key_index = iter % DIRECT_KEYS;

        snprintf(key, sizeof(key), "direct-%02d-%d", c->id, key_index);
        snprintf(value, sizeof(value), "thread-%02d-key-%d-iter-%04d",
                 c->id, key_index, iter);

        if (pk_table_set(c->table, B(key), L(key), B(value), L(value)) != PK_TABLE_OK) {
            direct_fail(c, "SET unique failed", key);
            return NULL;
        }

        uint8_t  got[VALBUF];
        uint32_t got_len = 0;
        if (pk_table_get(c->table, B(key), L(key), got, sizeof(got), &got_len)
                != PK_TABLE_OK
            || got_len != L(value) || memcmp(got, value, got_len) != 0) {
            direct_fail(c, "GET unique lost/corrupted value", key);
            return NULL;
        }

        char shared_key[48];
        uint8_t shared_value[63];
        uint32_t shared_len = (uint32_t)(32 + (iter % 32));
        snprintf(shared_key, sizeof(shared_key), "direct-shared-%d",
                 iter % DIRECT_SHARED_KEYS);
        memset(shared_value, 'A' + (c->id % 26), shared_len);

        if (pk_table_set(c->table, B(shared_key), L(shared_key),
                         shared_value, shared_len) != PK_TABLE_OK) {
            direct_fail(c, "SET shared failed", shared_key);
            return NULL;
        }
        got_len = 0;
        if (pk_table_get(c->table, B(shared_key), L(shared_key),
                         got, sizeof(got), &got_len) != PK_TABLE_OK
            || !direct_shared_value_is_whole(got, got_len)) {
            direct_fail(c, "GET shared returned torn value", shared_key);
            return NULL;
        }

        if (iter % 97 == 0) {
            if (pk_table_del(c->table, B(key), L(key)) != PK_TABLE_OK
                || !get_misses(c->table, key)
                || pk_table_set(c->table, B(key), L(key), B(value), L(value))
                       != PK_TABLE_OK) {
                direct_fail(c, "DEL/re-SET unique failed", key);
                return NULL;
            }
        }
    }

    /* Give the verification pass a deterministic quiescent final state. */
    for (int j = 0; j < DIRECT_KEYS; j++) {
        char key[48], value[64];
        snprintf(key, sizeof(key), "direct-%02d-%d", c->id, j);
        if (j == 0) {
            if (pk_table_del(c->table, B(key), L(key)) != PK_TABLE_OK) {
                direct_fail(c, "final DEL failed", key);
                return NULL;
            }
            continue;
        }
        snprintf(value, sizeof(value), "final-thread-%02d-key-%d", c->id, j);
        if (pk_table_set(c->table, B(key), L(key), B(value), L(value)) != PK_TABLE_OK) {
            direct_fail(c, "final SET failed", key);
            return NULL;
        }
    }
    return NULL;
}

static bool run_direct_concurrency_test(void)
{
    pk_table_t table;
    if (pk_table_init(&table) != 0)
        return false;

    direct_gate_t gate;
    memset(&gate, 0, sizeof(gate));
    if (pthread_mutex_init(&gate.lock, NULL) != 0) {
        pk_table_destroy(&table);
        return false;
    }
    if (pthread_cond_init(&gate.cond, NULL) != 0) {
        pthread_mutex_destroy(&gate.lock);
        pk_table_destroy(&table);
        return false;
    }

    pthread_t threads[DIRECT_THREADS];
    direct_client_t clients[DIRECT_THREADS];
    memset(clients, 0, sizeof(clients));

    int created = 0;
    bool launch_failed = false;
    for (int i = 0; i < DIRECT_THREADS; i++) {
        clients[i].table = &table;
        clients[i].gate  = &gate;
        clients[i].id    = i;
        int rc = pthread_create(&threads[i], NULL, direct_client_main, &clients[i]);
        if (rc != 0) {
            launch_failed = true;
            break;
        }
        created++;
    }

    pthread_mutex_lock(&gate.lock);
    gate.abort = launch_failed;
    gate.go    = true;
    pthread_cond_broadcast(&gate.cond);
    pthread_mutex_unlock(&gate.lock);

    for (int i = 0; i < created; i++)
        pthread_join(threads[i], NULL);

    bool clean = !launch_failed;
    for (int i = 0; i < created; i++) {
        if (clients[i].failures != 0) {
            printf("  thread %d: %s\n", i, clients[i].first_failure);
            clean = false;
        }
    }

    int checked = 0;
    if (clean) {
        for (int i = 0; i < DIRECT_THREADS; i++) {
            for (int j = 0; j < DIRECT_KEYS; j++) {
                char key[48], value[64];
                snprintf(key, sizeof(key), "direct-%02d-%d", i, j);
                if (j == 0) {
                    clean = clean && get_misses(&table, key);
                } else {
                    snprintf(value, sizeof(value), "final-thread-%02d-key-%d", i, j);
                    clean = clean && get_is(&table, key, value);
                }
                checked++;
            }
        }

        for (int j = 0; j < DIRECT_SHARED_KEYS; j++) {
            char key[48];
            uint8_t value[VALBUF];
            uint32_t len = 0;
            snprintf(key, sizeof(key), "direct-shared-%d", j);
            clean = clean
                && pk_table_get(&table, B(key), L(key), value, sizeof(value), &len)
                       == PK_TABLE_OK
                && direct_shared_value_is_whole(value, len);
        }

        size_t expected = DIRECT_THREADS * (DIRECT_KEYS - 1) + DIRECT_SHARED_KEYS;
        clean = clean && pk_table_count(&table) == expected;
    }

    printf("  %d threads x %d iterations; %d unique-key states checked\n",
           DIRECT_THREADS, DIRECT_ITERATIONS, checked);

    pthread_cond_destroy(&gate.cond);
    pthread_mutex_destroy(&gate.lock);
    pk_table_destroy(&table);
    return clean;
}

int main(void)
{
    pk_table_t t;

    if (pk_table_init(&t) != 0) {
        fprintf(stderr, "pk_table_init failed\n");
        return 1;
    }

    printf("=== empty table ===\n");
    ok(pk_table_count(&t) == 0, "a fresh table holds nothing");
    ok(get_misses(&t, "never-set"), "GET on a key never set reports not-found");
    ok(pk_table_del(&t, B("never-set"), L("never-set")) == PK_TABLE_NOT_FOUND,
       "DEL on a key never set reports not-found");

    printf("\n=== two-level shard routing ===\n");
    ok(PK_TABLE_BUCKETS == 1024u, "the table retains 1,024 physical buckets");
    ok(PK_TABLE_SHARDS == 256u, "256 mutex shards set lock granularity");
    ok(PK_TABLE_BUCKETS_PER_SHARD == 4u, "each lock shard owns four bucket chains");
    {
        const char *anchor = "shard-anchor";
        char peer[32];
        bool found = find_same_shard_other_bucket(anchor, peer, sizeof(peer));

        ok(found, "construct two keys sharing a mutex but not a chain");
        if (found) {
            ok(shard_of(anchor) == shard_of(peer), "both keys route to one lock shard");
            ok(bucket_in_shard_of(anchor) != bucket_in_shard_of(peer),
               "the keys occupy different chains inside that shard");
            ok(pk_table_set(&t, B(anchor), L(anchor), B("anchor"), 6) == PK_TABLE_OK,
               "SET the first striped key");
            ok(pk_table_set(&t, B(peer), L(peer), B("peer"), 4) == PK_TABLE_OK,
               "SET the second striped key");
            ok(get_is(&t, anchor, "anchor") && get_is(&t, peer, "peer"),
               "both chains remain independently addressable");
            ok(pk_table_del(&t, B(anchor), L(anchor)) == PK_TABLE_OK
                   && pk_table_del(&t, B(peer), L(peer)) == PK_TABLE_OK
                   && pk_table_count(&t) == 0,
               "striped test keys cleanly unlink from their own chains");
        }
    }

    printf("\n=== basic set/get ===\n");
    ok(pk_table_set(&t, B("alpha"), L("alpha"), B("one"), 3) == PK_TABLE_OK, "SET alpha");
    ok(pk_table_set(&t, B("beta"),  L("beta"),  B("two"), 3) == PK_TABLE_OK, "SET beta");
    ok(pk_table_set(&t, B("gamma"), L("gamma"), B("three"), 5) == PK_TABLE_OK, "SET gamma");
    ok(pk_table_count(&t) == 3, "count reflects three distinct keys");
    ok(get_is(&t, "alpha", "one"),   "GET alpha returns its own value");
    ok(get_is(&t, "beta",  "two"),   "GET beta returns its own value");
    ok(get_is(&t, "gamma", "three"), "GET gamma returns its own value");

    printf("\n=== the table owns its copies ===\n");
    {
        /*
         * Insert from buffers, then scribble over them. Anything the table kept
         * by reference now reads as the scribble.
         */
        char key[32];
        char val[32];
        memcpy(key, "borrowed-key", sizeof("borrowed-key"));
        memcpy(val, "borrowed-value", sizeof("borrowed-value"));

        ok(pk_table_set(&t, B(key), L(key), B(val), L(val)) == PK_TABLE_OK,
           "SET from caller-owned buffers");

        memset(key, 'Z', sizeof(key));
        memset(val, 'Z', sizeof(val));

        ok(get_is(&t, "borrowed-key", "borrowed-value"),
           "value survives the caller's buffer being overwritten");
    }

    printf("\n=== overwrite ===\n");
    ok(pk_table_set(&t, B("alpha"), L("alpha"),
                    B("a-much-longer-replacement"), 25) == PK_TABLE_OK,
       "SET over an existing key");
    ok(get_is(&t, "alpha", "a-much-longer-replacement"),
       "GET returns the replacement, not the original");
    ok(pk_table_count(&t) == 4, "overwrite does not add an entry");
    /* The original "one" allocation is unreachable now; valgrind is what
     * actually proves it was freed rather than dropped. */

    printf("\n=== bucket collisions ===\n");
    {
        const char *anchor = "collide-anchor";
        char        other[32];
        size_t      bucket = 0;

        if (!find_collision(anchor, other, sizeof(other), &bucket)) {
            printf("  FAIL  could not construct a collision\n");
            g_failed++;
        } else {
            printf("  (\"%s\" and \"%s\" both hash to bucket %zu)\n",
                   anchor, other, bucket);
            ok(bucket_of(anchor) == bucket_of(other), "two keys share one bucket");

            ok(pk_table_set(&t, B(anchor), L(anchor), B("anchor-value"), 12) == PK_TABLE_OK,
               "SET the first colliding key");
            ok(pk_table_set(&t, B(other), L(other), B("other-value"), 11) == PK_TABLE_OK,
               "SET the second colliding key");
            ok(pk_table_count(&t) == 6, "both colliding keys are stored separately");

            ok(get_is(&t, anchor, "anchor-value"), "chained lookup finds the first key");
            ok(get_is(&t, other,  "other-value"),  "chained lookup finds the second key");

            /* "other" was inserted last, so it sits at the head and "anchor"
             * behind it. Deleting the head must leave the tail reachable. */
            ok(pk_table_del(&t, B(other), L(other)) == PK_TABLE_OK,
               "DEL the key at the head of the chain");
            ok(get_is(&t, anchor, "anchor-value"),
               "the other key in that bucket is still reachable");
            ok(get_misses(&t, other), "the deleted key is gone");

            ok(pk_table_del(&t, B(anchor), L(anchor)) == PK_TABLE_OK,
               "DEL the last key in the chain");
            ok(get_misses(&t, anchor), "the bucket is now empty");
        }
    }

    printf("\n=== delete ===\n");
    ok(pk_table_del(&t, B("beta"), L("beta")) == PK_TABLE_OK, "DEL an existing key");
    ok(get_misses(&t, "beta"), "GET after DEL reports not-found");
    ok(pk_table_del(&t, B("beta"), L("beta")) == PK_TABLE_NOT_FOUND,
       "a second DEL of the same key reports not-found");
    ok(get_is(&t, "gamma", "three"), "an unrelated key is untouched by the delete");

    printf("\n=== edge cases ===\n");
    ok(pk_table_set(&t, B("empty"), L("empty"), NULL, 0) == PK_TABLE_OK,
       "SET with a zero-length value");
    {
        uint8_t  buf[VALBUF];
        uint32_t len = 99;
        ok(pk_table_get(&t, B("empty"), L("empty"), buf, sizeof(buf), &len) == PK_TABLE_OK
               && len == 0,
           "GET returns a zero-length value as a hit, not a miss");
    }
    {
        /* Keys are raw bytes, so an embedded NUL must not truncate them. */
        const uint8_t k1[] = { 'n', 'u', 'l', 0, 'a' };
        const uint8_t k2[] = { 'n', 'u', 'l', 0, 'b' };
        uint8_t  buf[VALBUF];
        uint32_t len = 0;

        ok(pk_table_set(&t, k1, sizeof(k1), B("first"), 5) == PK_TABLE_OK,
           "SET a key containing a NUL byte");
        ok(pk_table_set(&t, k2, sizeof(k2), B("second"), 6) == PK_TABLE_OK,
           "SET another differing only after the NUL");
        ok(pk_table_get(&t, k1, sizeof(k1), buf, sizeof(buf), &len) == PK_TABLE_OK
               && len == 5 && memcmp(buf, "first", 5) == 0,
           "keys compare over their full length, not up to the NUL");
    }
    {
        uint8_t  small[4];
        uint32_t len = 0;
        ok(pk_table_get(&t, B("gamma"), L("gamma"), small, sizeof(small), &len)
               == PK_TABLE_TOO_BIG,
           "GET into an undersized buffer is refused rather than overflowing");
    }
    ok(pk_table_set(&t, B(""), 0, B("x"), 1) == PK_TABLE_INVALID, "SET with an empty key is rejected");
    ok(pk_table_get(&t, B(""), 0, NULL, 0, NULL) == PK_TABLE_INVALID, "GET with an empty key is rejected");

    printf("\n=== teardown ===\n");
    printf("  destroying a table holding %zu keys\n", pk_table_count(&t));
    pk_table_destroy(&t);

    printf("\n=== direct concurrent shard stress ===\n");
    ok(run_direct_concurrency_test(),
       "concurrent shard SET/GET/DEL preserves all final state");

    printf("\nRESULT: %s (%d checks, %d failed)\n",
           g_failed == 0 ? "PASS" : "FAIL", g_checks, g_failed);
    return g_failed == 0 ? 0 : 1;
}
