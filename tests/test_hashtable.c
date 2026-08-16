/*
 * Unit test for the store, with no network in the picture.
 *
 * The interesting cases are the ones that separate a hash table that works from
 * one that only appears to: two keys chained in the same bucket, an overwrite
 * that has to free what it replaces, an unlink from the middle of a chain, and
 * -- the likeliest real bug here -- whether the table actually copies what it
 * is given. The server hands it pointers into a connection's read buffer, which
 * is overwritten by the very next frame, so anything held by reference would
 * come back as garbage a moment later.
 *
 * Run under valgrind: several of these assertions are about lifetime, and a
 * leak checker is the only thing that sees the difference.
 */

#include "hashtable.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define VALBUF 256

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

    printf("\nRESULT: %s (%d checks, %d failed)\n",
           g_failed == 0 ? "PASS" : "FAIL", g_checks, g_failed);
    return g_failed == 0 ? 0 : 1;
}
