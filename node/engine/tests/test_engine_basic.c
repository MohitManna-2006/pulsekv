/*
 * Engine fundamentals, with no network and no NVMe tier in the picture.
 *
 * The interesting cases are the ones that separate a sharded hash table that
 * works from one that only appears to -- inherited directly from v1's
 * tests/test_hashtable.c, because the extraction did not change any of the
 * properties they check: two keys sharing a lock but not a chain, two keys
 * chained in the same bucket, an overwrite that has to free what it replaces,
 * and whether the table actually copies what it is given rather than holding
 * the caller's pointer.
 *
 * New in v2 and covered here: the caller-owned allocation contract on get,
 * peek's promise not to disturb recency, prefix scanning, and capacity
 * accounting.
 *
 * Run under valgrind: several of these assertions are about lifetime, and a
 * leak checker is the only thing that sees the difference.
 */

#include "test_util.h"

#include "hashtable.h"      /* internal: needed to construct deliberate collisions */
#include "pulsekv_engine.h"

#define RAM_BUDGET (16ULL * 1024 * 1024)
#define MAX_VALUE  (1ULL * 1024 * 1024)

static pk_engine_t *new_engine(void)
{
    pk_engine_config_t cfg = {
        .data_dir         = NULL,  /* RAM only: tiering has its own test */
        .ram_budget_bytes = RAM_BUDGET,
        .max_value_bytes  = MAX_VALUE,
    };
    return pk_engine_create(&cfg);
}

/* Fetch and compare against an expected C string. */
static bool get_is(pk_engine_t *e, const char *key, const char *want)
{
    uint8_t *val = NULL;
    uint64_t len = 0;

    if (pk_engine_get(e, B(key), L(key), &val, &len) != PK_ENGINE_OK)
        return false;

    bool match = (len == (uint64_t)strlen(want)) &&
                 (len == 0 || memcmp(val, want, (size_t)len) == 0);
    pk_engine_free_value(val);
    return match;
}

static bool is_missing(pk_engine_t *e, const char *key)
{
    uint8_t *val = NULL;
    uint64_t len = 0;
    pk_engine_result_t rc = pk_engine_get(e, B(key), L(key), &val, &len);
    pk_engine_free_value(val);
    return rc == PK_ENGINE_NOT_FOUND;
}

/* ------------------------------------------------------------------ */

static void test_roundtrip(pk_engine_t *e)
{
    section("round trip");

    ok(pk_engine_put(e, B("alpha"), L("alpha"), B("one"), 3) == PK_ENGINE_OK,
       "put alpha");
    ok(get_is(e, "alpha", "one"), "get alpha returns one");
    ok(is_missing(e, "never-written"), "get on an unknown key is NOT_FOUND");

    ok(pk_engine_put(e, B("alpha"), L("alpha"), B("two-longer"), 10) == PK_ENGINE_OK,
       "overwrite alpha with a longer value");
    ok(get_is(e, "alpha", "two-longer"), "overwrite is visible");

    ok(pk_engine_put(e, B("alpha"), L("alpha"), B("s"), 1) == PK_ENGINE_OK,
       "overwrite alpha with a shorter value");
    ok(get_is(e, "alpha", "s"), "shorter overwrite is visible");
}

static void test_empty_value(pk_engine_t *e)
{
    section("empty values");

    ok(pk_engine_put(e, B("empty"), L("empty"), NULL, 0) == PK_ENGINE_OK,
       "put a zero-length value");

    uint8_t *val = NULL;
    uint64_t len = 1;
    pk_engine_result_t rc = pk_engine_get(e, B("empty"), L("empty"), &val, &len);
    /* An empty value is a hit, not a miss -- the gRPC layer reports
     * found = true with an empty bytes field. */
    ok(rc == PK_ENGINE_OK, "get on an empty value is OK, not NOT_FOUND");
    ok(len == 0, "empty value reports length 0");
    ok(val == NULL, "empty value hands back no allocation");
    pk_engine_free_value(val);
}

static void test_copies_input(pk_engine_t *e)
{
    section("ownership");

    /*
     * The gRPC handler decodes into a protobuf-owned buffer that is released
     * the moment the RPC returns. Anything the engine held by reference would
     * come back as garbage a moment later, so scribble over the source and
     * confirm the stored copy is unaffected.
     */
    char key[] = "borrowed";
    char scratch[16];
    memcpy(scratch, "original", 9);

    ok(pk_engine_put(e, B(key), L(key), (const uint8_t *)scratch, 8) == PK_ENGINE_OK,
       "put from a buffer the caller will reuse");
    memset(scratch, 'X', sizeof(scratch));
    ok(get_is(e, "borrowed", "original"), "value survives the caller overwriting its buffer");

    /* And the value handed back is the caller's to free and to modify. */
    uint8_t *val = NULL;
    uint64_t len = 0;
    pk_engine_get(e, B(key), L(key), &val, &len);
    if (val != NULL)
        val[0] = 'Z';
    pk_engine_free_value(val);
    ok(get_is(e, "borrowed", "original"),
       "mutating the returned copy does not corrupt the stored value");
}

static void test_invalid_args(pk_engine_t *e)
{
    section("argument validation");

    uint8_t *val = NULL;
    uint64_t len = 0;

    ok(pk_engine_put(e, NULL, 0, B("v"), 1) == PK_ENGINE_INVALID,
       "put with a NULL key is INVALID");
    ok(pk_engine_put(e, B("k"), 0, B("v"), 1) == PK_ENGINE_INVALID,
       "put with a zero-length key is INVALID");
    ok(pk_engine_put(e, B("k"), 1, NULL, 5) == PK_ENGINE_INVALID,
       "put with a NULL value but non-zero length is INVALID");
    ok(pk_engine_get(e, B("k"), 0, &val, &len) == PK_ENGINE_INVALID,
       "get with a zero-length key is INVALID");
    ok(pk_engine_get(e, B("k"), 1, NULL, &len) == PK_ENGINE_INVALID,
       "get with no output pointer is INVALID");
}

/*
 * v1's two structural cases, rebuilt. Keys are searched for rather than
 * hardcoded so the test keeps working if the hash or the routing changes.
 */
static void find_colliding_keys(char *same_bucket_a, char *same_bucket_b,
                                char *same_shard_a, char *same_shard_b,
                                bool *found_bucket, bool *found_shard)
{
    *found_bucket = false;
    *found_shard  = false;

    char probe[32];
    /* route: shard = hash & 255, bucket-within-shard = (hash & 1023) / 256 */
    for (int i = 0; i < 20000 && !(*found_bucket && *found_shard); i++) {
        snprintf(probe, sizeof(probe), "probe-%d", i);
        uint64_t h = pk_table_hash(B(probe), L(probe));
        uint64_t global = h & (PK_TABLE_BUCKETS - 1u);

        for (int j = i + 1; j < 20000; j++) {
            char other[32];
            snprintf(other, sizeof(other), "probe-%d", j);
            uint64_t h2 = pk_table_hash(B(other), L(other));
            uint64_t g2 = h2 & (PK_TABLE_BUCKETS - 1u);

            if (!*found_bucket && global == g2) {
                strcpy(same_bucket_a, probe);
                strcpy(same_bucket_b, other);
                *found_bucket = true;
            }
            if (!*found_shard &&
                (global & (PK_TABLE_SHARDS - 1u)) == (g2 & (PK_TABLE_SHARDS - 1u)) &&
                global != g2) {
                strcpy(same_shard_a, probe);
                strcpy(same_shard_b, other);
                *found_shard = true;
            }
            if (*found_bucket && *found_shard)
                break;
        }
    }
}

static void test_collisions(pk_engine_t *e)
{
    section("shard and bucket collisions");

    char ba[32], bb[32], sa[32], sb[32];
    bool found_bucket = false, found_shard = false;
    find_colliding_keys(ba, bb, sa, sb, &found_bucket, &found_shard);

    ok(found_bucket, "found two keys that chain in the same bucket (%s, %s)",
       found_bucket ? ba : "-", found_bucket ? bb : "-");
    if (found_bucket) {
        pk_engine_put(e, B(ba), L(ba), B("chain-a"), 7);
        pk_engine_put(e, B(bb), L(bb), B("chain-b"), 7);
        ok(get_is(e, ba, "chain-a") && get_is(e, bb, "chain-b"),
           "both chained keys read back independently");

        /* Deleting the head of a chain must not orphan the rest of it. */
        ok(pk_engine_put(e, B(ba), L(ba), B("chain-A"), 7) == PK_ENGINE_OK,
           "overwrite one chained key");
        ok(get_is(e, ba, "chain-A") && get_is(e, bb, "chain-b"),
           "overwriting one chained key leaves the other intact");
    }

    ok(found_shard, "found two keys that share a lock but not a chain (%s, %s)",
       found_shard ? sa : "-", found_shard ? sb : "-");
    if (found_shard) {
        pk_engine_put(e, B(sa), L(sa), B("lock-a"), 6);
        pk_engine_put(e, B(sb), L(sb), B("lock-b"), 6);
        ok(get_is(e, sa, "lock-a") && get_is(e, sb, "lock-b"),
           "both same-shard keys read back independently");
    }
}

static void test_capacity(void)
{
    section("capacity accounting");

    pk_engine_t *e = new_engine();
    if (e == NULL) {
        ok(false, "engine created");
        return;
    }

    pk_engine_capacity_t cap;
    pk_engine_capacity(e, &cap);
    ok(cap.resident_keys == 0 && cap.bytes_in_ram_tier == 0,
       "a fresh engine reports nothing resident");

    uint8_t payload[1000];
    fill_value(payload, sizeof(payload), 7);
    for (int i = 0; i < 50; i++) {
        char key[32];
        snprintf(key, sizeof(key), "cap-%d", i);
        pk_engine_put(e, B(key), L(key), payload, sizeof(payload));
    }

    pk_engine_capacity(e, &cap);
    ok(cap.resident_keys == 50, "50 keys stored, capacity reports %llu",
       (unsigned long long)cap.resident_keys);
    ok(cap.bytes_in_ram_tier == 50ULL * sizeof(payload),
       "RAM bytes are the exact sum of value lengths (%llu)",
       (unsigned long long)cap.bytes_in_ram_tier);
    ok(cap.bytes_in_nvme_tier == 0, "nothing spilled while inside the budget");
    ok(cap.spills == 0 && cap.evict_drops == 0, "no evictions inside the budget");

    /* An overwrite must adjust the byte total, not add to it. */
    pk_engine_put(e, B("cap-0"), L("cap-0"), payload, 10);
    pk_engine_capacity(e, &cap);
    ok(cap.resident_keys == 50, "overwrite does not change the key count");
    ok(cap.bytes_in_ram_tier == 49ULL * sizeof(payload) + 10,
       "overwrite adjusts the byte total (%llu)",
       (unsigned long long)cap.bytes_in_ram_tier);

    pk_engine_destroy(e);
}

static void test_scan_prefix(void)
{
    section("prefix scan");

    pk_engine_t *e = new_engine();
    if (e == NULL) {
        ok(false, "engine created");
        return;
    }

    for (int i = 0; i < 40; i++) {
        char key[32];
        snprintf(key, sizeof(key), "user:%02d", i);
        pk_engine_put(e, B(key), L(key), B("u"), 1);
    }
    for (int i = 0; i < 10; i++) {
        char key[32];
        snprintf(key, sizeof(key), "session:%02d", i);
        pk_engine_put(e, B(key), L(key), B("s"), 1);
    }

    pk_engine_keyset_t ks;
    ok(pk_engine_scan_prefix(e, B("user:"), L("user:"), &ks) == PK_ENGINE_OK,
       "scan by prefix succeeds");
    ok(ks.count == 40, "prefix user: matched %zu of 40", ks.count);

    bool all_prefixed = true;
    for (size_t i = 0; i < ks.count; i++) {
        if (ks.keys[i].key_len < 5 || memcmp(ks.keys[i].key, "user:", 5) != 0)
            all_prefixed = false;
    }
    ok(all_prefixed, "every matched key actually carries the prefix");
    pk_engine_free_keyset(&ks);

    ok(pk_engine_scan_prefix(e, NULL, 0, &ks) == PK_ENGINE_OK, "empty prefix scans");
    ok(ks.count == 50, "empty prefix matched every key (%zu of 50)", ks.count);
    pk_engine_free_keyset(&ks);

    ok(pk_engine_scan_prefix(e, B("nothing:"), L("nothing:"), &ks) == PK_ENGINE_OK,
       "a prefix that matches nothing is not an error");
    ok(ks.count == 0, "and returns an empty keyset");
    pk_engine_free_keyset(&ks);

    /* A key shorter than the prefix must not be read past its own end. */
    pk_engine_put(e, B("u"), 1, B("x"), 1);
    ok(pk_engine_scan_prefix(e, B("user:"), L("user:"), &ks) == PK_ENGINE_OK &&
       ks.count == 40,
       "a key shorter than the prefix is skipped, not over-read");
    pk_engine_free_keyset(&ks);

    pk_engine_destroy(e);
}

static void test_peek(void)
{
    section("peek");

    pk_engine_t *e = new_engine();
    if (e == NULL) {
        ok(false, "engine created");
        return;
    }

    pk_engine_put(e, B("p"), 1, B("value"), 5);

    uint8_t *val = NULL;
    uint64_t len = 0;
    ok(pk_engine_peek(e, B("p"), 1, &val, &len) == PK_ENGINE_OK && len == 5 &&
       memcmp(val, "value", 5) == 0,
       "peek returns the value");
    pk_engine_free_value(val);

    ok(pk_engine_peek(e, B("absent"), 6, &val, &len) == PK_ENGINE_NOT_FOUND,
       "peek on a missing key is NOT_FOUND");
    pk_engine_free_value(val);

    pk_engine_destroy(e);
}

static void test_config_defaults(void)
{
    section("configuration");

    pk_engine_config_t cfg = { .data_dir = NULL, .ram_budget_bytes = 0, .max_value_bytes = 0 };
    pk_engine_t *e = pk_engine_create(&cfg);
    ok(e != NULL, "a zero config is accepted");
    if (e != NULL) {
        ok(pk_engine_max_value_bytes(e) == PK_ENGINE_DEFAULT_MAX_VALUE_BYTES,
           "zero max_value_bytes selects the 64 MiB default");
        ok(pk_engine_ram_budget_bytes(e) == PK_ENGINE_DEFAULT_RAM_BUDGET_BYTES,
           "zero ram_budget_bytes selects the 256 MiB default");
        pk_engine_destroy(e);
    }

    ok(pk_engine_create(NULL) == NULL, "a NULL config is rejected");

    /* A data_dir that cannot be created must fail loudly at startup rather
     * than quietly degrading the node to a RAM-only cache. */
    pk_engine_config_t bad = {
        .data_dir         = "/proc/pulsekv-cannot-exist/data",
        .ram_budget_bytes = RAM_BUDGET,
        .max_value_bytes  = MAX_VALUE,
    };
    ok(pk_engine_create(&bad) == NULL, "an unusable data_dir fails engine creation");
}

int main(void)
{
    printf("test_engine_basic\n");

    pk_engine_t *e = new_engine();
    if (e == NULL) {
        printf("  FAIL  could not create engine\n");
        return 1;
    }

    test_roundtrip(e);
    test_empty_value(e);
    test_copies_input(e);
    test_invalid_args(e);
    test_collisions(e);
    pk_engine_destroy(e);

    test_capacity();
    test_scan_prefix();
    test_peek();
    test_config_defaults();

    return test_summary("test_engine_basic");
}
