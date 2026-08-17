/*
 * Step 1.3: the NVMe spill tier.
 *
 * Nothing in v1 does this, so nothing about it is inherited-and-trusted. The
 * properties that matter, in order of how badly it goes if they are wrong:
 *
 *   1. A working set several times the RAM budget is FULLY CORRECT, just
 *      slower. Every key written is readable, byte for byte, whichever tier it
 *      happens to be in.
 *   2. Repeated promote/demote cycles do not truncate or corrupt a value. A
 *      value that survives one round trip but degrades over ten is worse than
 *      one that never worked.
 *   3. The bookkeeping matches reality. keys_in_nvme_tier is checked against an
 *      actual count of files on disk, so a leaked or orphaned spill file shows
 *      up as a failure rather than as slow disk growth nobody notices.
 *   4. Losing the tier degrades the cache; it never corrupts it.
 */

#include "test_util.h"

#include <dirent.h>

#include "hashtable.h"      /* internal: to place keys in a chosen shard on purpose */
#include "pulsekv_engine.h"

#define VALUE_LEN    512u
#define SHARD_BUDGET 1024ULL                  /* 512-byte values: ~2 per shard */
#define RAM_BUDGET   (SHARD_BUDGET * 256ULL)  /* 256 KiB total */
#define MAX_VALUE    (16ULL * 1024 * 1024)

static pk_engine_t *new_engine(const char *dir, uint64_t ram_budget)
{
    pk_engine_config_t cfg = {
        .data_dir         = dir,
        .ram_budget_bytes = ram_budget,
        .max_value_bytes  = MAX_VALUE,
    };
    return pk_engine_create(&cfg);
}

/* Counts *.val files two levels under <dir>/spill -- the layout tiering.c
 * creates. This is the independent check on the engine's own accounting. */
static size_t count_spill_files(const char *dir)
{
    char root[4096];
    snprintf(root, sizeof(root), "%s/spill", dir);

    size_t total = 0;
    DIR *l1 = opendir(root);
    if (l1 == NULL)
        return 0;

    struct dirent *e1;
    while ((e1 = readdir(l1)) != NULL) {
        if (e1->d_name[0] == '.')
            continue;
        char p1[4096 + 256];
        if (snprintf(p1, sizeof(p1), "%s/%s", root, e1->d_name) >= (int)sizeof(p1))
            continue;

        DIR *l2 = opendir(p1);
        if (l2 == NULL)
            continue;
        struct dirent *e2;
        while ((e2 = readdir(l2)) != NULL) {
            if (e2->d_name[0] == '.')
                continue;
            char p2[4096 + 512];
            if (snprintf(p2, sizeof(p2), "%s/%s", p1, e2->d_name) >= (int)sizeof(p2))
                continue;

            DIR *l3 = opendir(p2);
            if (l3 == NULL)
                continue;
            struct dirent *e3;
            while ((e3 = readdir(l3)) != NULL) {
                size_t n = strlen(e3->d_name);
                if (n > 4 && strcmp(e3->d_name + n - 4, ".val") == 0)
                    total++;
            }
            closedir(l3);
        }
        closedir(l2);
    }
    closedir(l1);
    return total;
}

static void key_for(char *buf, size_t cap, int i)
{
    snprintf(buf, cap, "tier-key-%06d", i);
}

/* Writes value #i and returns true on success. */
static bool put_indexed(pk_engine_t *e, int i)
{
    uint8_t val[VALUE_LEN];
    char    key[32];
    key_for(key, sizeof(key), i);
    fill_value(val, sizeof(val), (uint64_t)i + 1);
    return pk_engine_put(e, B(key), L(key), val, sizeof(val)) == PK_ENGINE_OK;
}

/* Reads value #i back and verifies it byte for byte. */
static bool get_indexed_ok(pk_engine_t *e, int i)
{
    char key[32];
    key_for(key, sizeof(key), i);

    uint8_t *out = NULL;
    uint64_t len = 0;
    if (pk_engine_get(e, B(key), L(key), &out, &len) != PK_ENGINE_OK) {
        pk_engine_free_value(out);
        return false;
    }
    bool good = (len == VALUE_LEN) && value_matches(out, VALUE_LEN, (uint64_t)i + 1);
    pk_engine_free_value(out);
    return good;
}

/* ------------------------------------------------------------------ */

static void test_spill_and_read_back(const char *dir)
{
    section("a working set several times the RAM budget");

    pk_engine_t *e = new_engine(dir, RAM_BUDGET);
    if (e == NULL) {
        ok(false, "engine created with a data_dir");
        return;
    }

    /* 2000 * 512 B = 1 MiB of values against a 256 KiB budget: 4x oversubscribed. */
    const int N = 2000;
    bool all_written = true;
    for (int i = 0; i < N; i++)
        all_written &= put_indexed(e, i);
    ok(all_written, "wrote %d values totalling %d KiB against a %llu KiB budget",
       N, (N * (int)VALUE_LEN) / 1024, (unsigned long long)(RAM_BUDGET / 1024));

    pk_engine_capacity_t cap;
    pk_engine_capacity(e, &cap);

    ok(cap.spills > 0, "values spilled to NVMe (%llu spills)",
       (unsigned long long)cap.spills);
    ok(cap.bytes_in_nvme_tier > 0, "the NVMe tier reports %llu bytes",
       (unsigned long long)cap.bytes_in_nvme_tier);
    ok(cap.spill_errors == 0, "no spill errors");
    ok(cap.evict_drops == 0, "nothing was dropped -- with a tier, eviction spills");
    ok(cap.resident_keys == (uint64_t)N,
       "all %d keys are still indexed (%llu)", N, (unsigned long long)cap.resident_keys);
    ok(cap.keys_in_ram_tier + cap.keys_in_nvme_tier == cap.resident_keys,
       "per-tier key counts sum to the total");
    ok(cap.bytes_in_ram_tier + cap.bytes_in_nvme_tier == (uint64_t)N * VALUE_LEN,
       "per-tier byte counts sum to exactly what was written (%llu of %llu)",
       (unsigned long long)(cap.bytes_in_ram_tier + cap.bytes_in_nvme_tier),
       (unsigned long long)((uint64_t)N * VALUE_LEN));

    size_t on_disk = count_spill_files(dir);
    ok(on_disk == cap.keys_in_nvme_tier,
       "spill files on disk (%zu) match keys_in_nvme_tier (%llu)",
       on_disk, (unsigned long long)cap.keys_in_nvme_tier);

    /* The headline property: every single key is correct, whichever tier it is
     * in and regardless of the promotions this loop itself causes. */
    int bad = 0;
    for (int i = 0; i < N; i++) {
        if (!get_indexed_ok(e, i))
            bad++;
    }
    ok(bad == 0, "all %d values read back byte for byte (%d wrong)", N, bad);

    pk_engine_capacity(e, &cap);
    ok(cap.promotions > 0, "reads promoted spilled values back to RAM (%llu)",
       (unsigned long long)cap.promotions);
    ok(cap.spill_errors == 0, "still no spill errors after the read pass");

    pk_engine_destroy(e);
    ok(count_spill_files(dir) == 0, "destroy removed every spill file");
}

static void test_promote_demote_cycles(const char *dir)
{
    section("repeated promote/demote cycles");

    pk_engine_t *e = new_engine(dir, RAM_BUDGET);
    if (e == NULL) {
        ok(false, "engine created");
        return;
    }

    /* Enough keys that the budget cannot hold them, so every pass forces the
     * previous pass's promotions back out to disk. */
    const int N = 600;
    for (int i = 0; i < N; i++)
        put_indexed(e, i);

    int bad = 0;
    for (int pass = 0; pass < 10; pass++) {
        for (int i = 0; i < N; i++) {
            if (!get_indexed_ok(e, i))
                bad++;
        }
    }
    ok(bad == 0, "10 full passes over %d keys, every read exact (%d wrong)", N, bad);

    pk_engine_capacity_t cap;
    pk_engine_capacity(e, &cap);
    ok(cap.promotions > (uint64_t)N,
       "the passes really did thrash the tiers (%llu promotions, %llu spills)",
       (unsigned long long)cap.promotions, (unsigned long long)cap.spills);
    ok(cap.resident_keys == (uint64_t)N, "no key was lost across the cycles");
    ok(count_spill_files(dir) == cap.keys_in_nvme_tier,
       "accounting still matches the filesystem after thrashing");

    pk_engine_destroy(e);
}

static void test_overwrite_spilled(const char *dir)
{
    section("overwriting and re-reading a spilled key");

    pk_engine_t *e = new_engine(dir, RAM_BUDGET);
    if (e == NULL) {
        ok(false, "engine created");
        return;
    }

    const int N = 400;
    for (int i = 0; i < N; i++)
        put_indexed(e, i);

    pk_engine_capacity_t before;
    pk_engine_capacity(e, &before);
    ok(before.keys_in_nvme_tier > 0, "some keys are spilled to overwrite");

    /* Rewrite every key with a different value. Whichever tier each one was in,
     * the new value must win and the old spill file must go. */
    for (int i = 0; i < N; i++) {
        uint8_t val[VALUE_LEN];
        char    key[32];
        key_for(key, sizeof(key), i);
        fill_value(val, sizeof(val), (uint64_t)i + 900000);
        pk_engine_put(e, B(key), L(key), val, sizeof(val));
    }

    int bad = 0;
    for (int i = 0; i < N; i++) {
        char key[32];
        key_for(key, sizeof(key), i);
        uint8_t *out = NULL;
        uint64_t len = 0;
        if (pk_engine_get(e, B(key), L(key), &out, &len) != PK_ENGINE_OK ||
            len != VALUE_LEN ||
            !value_matches(out, VALUE_LEN, (uint64_t)i + 900000)) {
            bad++;
        }
        pk_engine_free_value(out);
    }
    ok(bad == 0, "every overwritten value reads back as the NEW value (%d wrong)", bad);

    pk_engine_capacity_t after;
    pk_engine_capacity(e, &after);
    ok(after.resident_keys == (uint64_t)N, "overwrite did not change the key count");
    ok(count_spill_files(dir) == after.keys_in_nvme_tier,
       "no orphaned spill files left by the overwrites (%zu on disk, %llu tracked)",
       count_spill_files(dir), (unsigned long long)after.keys_in_nvme_tier);

    pk_engine_destroy(e);
}

/*
 * Fills `out` with `want` distinct keys that all route to the same shard.
 *
 * Shard selection is hash & (PK_TABLE_SHARDS - 1) -- see route_of_hash in
 * hashtable.c. Searching for keys instead of hardcoding them keeps this working
 * if the hash or the striping ever changes.
 */
static int keys_in_one_shard(char out[][32], int want)
{
    int found = 0;
    for (int i = 0; i < 500000 && found < want; i++) {
        char probe[32];
        snprintf(probe, sizeof(probe), "shardpin-%d", i);
        if ((pk_table_hash(B(probe), L(probe)) & (PK_TABLE_SHARDS - 1u)) == 0u) {
            snprintf(out[found], 32, "%s", probe);
            found++;
        }
    }
    return found;
}

static void test_large_value_through_the_tier(const char *dir)
{
    section("multi-megabyte values through the tier");

    /*
     * Six 2 MiB values deliberately placed in ONE shard, against a 256 MiB
     * budget whose per-shard share is 1 MiB. That guarantees the shard is over
     * budget by 11 MiB and must spill, rather than leaving it to whether the
     * hash happened to collide -- see test_per_shard_budget below for why
     * spreading the same 12 MiB across 12 shards spills nothing at all.
     */
    pk_engine_t *e = new_engine(dir, 256ULL * 1024 * 1024);
    if (e == NULL) {
        ok(false, "engine created");
        return;
    }

    char keys[6][32];
    int  n = keys_in_one_shard(keys, 6);
    ok(n == 6, "found %d keys that share a shard", n);
    if (n < 6) {
        pk_engine_destroy(e);
        return;
    }

    const size_t big = 2 * 1024 * 1024;
    uint8_t *src = malloc(big);
    if (src == NULL) {
        ok(false, "allocated a 2 MiB source buffer");
        pk_engine_destroy(e);
        return;
    }
    for (int i = 0; i < n; i++) {
        fill_value(src, big, (uint64_t)i + 5000);
        pk_engine_put(e, B(keys[i]), L(keys[i]), src, big);
    }
    free(src);

    pk_engine_capacity_t cap;
    pk_engine_capacity(e, &cap);
    ok(cap.bytes_in_nvme_tier > 0,
       "12 MiB of 2 MiB values in a 1 MiB shard spilled (%llu MiB on NVMe)",
       (unsigned long long)(cap.bytes_in_nvme_tier / (1024 * 1024)));
    ok(cap.keys_in_ram_tier == 1,
       "exactly the most-recently-written one stayed in RAM (%llu)",
       (unsigned long long)cap.keys_in_ram_tier);
    ok(count_spill_files(dir) == cap.keys_in_nvme_tier,
       "%zu multi-megabyte spill files on disk match the accounting",
       count_spill_files(dir));

    int bad = 0;
    for (int i = 0; i < n; i++) {
        uint8_t *out = NULL;
        uint64_t len = 0;
        if (pk_engine_get(e, B(keys[i]), L(keys[i]), &out, &len) != PK_ENGINE_OK ||
            len != big || !value_matches(out, big, (uint64_t)i + 5000)) {
            bad++;
        }
        pk_engine_free_value(out);
    }
    ok(bad == 0, "every 2 MiB value survived the round trip through disk (%d wrong)", bad);

    pk_engine_destroy(e);
}

/*
 * Pins down a real and initially surprising consequence of the design, so it is
 * an asserted property rather than something the next person rediscovers.
 *
 * The budget is divided per shard rather than tracked globally: a global byte
 * counter would be one contended cache line touched by every write on every
 * core, which is exactly the bottleneck v1's 256-way striping exists to avoid.
 * The price is that occupancy is only as even as the hash, and a shard holding
 * a single entry never evicts it (see shard_enforce_budget's `protect`).
 *
 * So total bytes resident can legitimately exceed ram_budget_bytes, bounded by
 * budget + PK_TABLE_SHARDS * max_value_bytes in the worst case. Operators sizing
 * a node need to know that; it is documented in
 * docs/pulsekv-v2-phase1-summary.md and asserted here.
 */
static void test_per_shard_budget(const char *dir)
{
    section("the budget is per shard, not global");

    /* 8 MiB budget => 32 KiB per shard. Twelve 2 MiB values, one per shard. */
    pk_engine_t *e = new_engine(dir, 8ULL * 1024 * 1024);
    if (e == NULL) {
        ok(false, "engine created");
        return;
    }

    const size_t big = 2 * 1024 * 1024;
    uint8_t *src = malloc(big);
    if (src == NULL) {
        ok(false, "allocated source buffer");
        pk_engine_destroy(e);
        return;
    }

    /* Pick keys that land in distinct shards, so no shard ever holds two. */
    int placed = 0;
    unsigned char used[PK_TABLE_SHARDS] = {0};
    for (int i = 0; i < 100000 && placed < 12; i++) {
        char key[32];
        snprintf(key, sizeof(key), "spread-%d", i);
        size_t shard = (size_t)(pk_table_hash(B(key), L(key)) & (PK_TABLE_SHARDS - 1u));
        if (used[shard])
            continue;
        used[shard] = 1;
        fill_value(src, big, (uint64_t)placed + 7000);
        pk_engine_put(e, B(key), L(key), src, big);
        placed++;
    }
    free(src);

    ok(placed == 12, "placed %d large values in 12 distinct shards", placed);

    pk_engine_capacity_t cap;
    pk_engine_capacity(e, &cap);
    ok(cap.bytes_in_nvme_tier == 0 && cap.spills == 0,
       "24 MiB against an 8 MiB budget spilled NOTHING, because no single shard "
       "held more than one value");
    ok(cap.bytes_in_ram_tier == (uint64_t)placed * big,
       "resident bytes (%llu MiB) exceed the configured budget (8 MiB) by design",
       (unsigned long long)(cap.bytes_in_ram_tier / (1024 * 1024)));

    pk_engine_destroy(e);
}

static void test_mru_protection(const char *dir)
{
    section("the just-written entry is not spilled by its own insert");

    /* 256 shards * 4 KiB = 1 MiB budget, so one shard's share is 4 KiB and a
     * 256 KiB value is 64x past it. Without protecting the entry the insert
     * just created, every large write would be flushed straight to disk by the
     * very operation that stored it, and every large read would be a disk read. */
    pk_engine_t *e = new_engine(dir, 256ULL * 4096ULL);
    if (e == NULL) {
        ok(false, "engine created");
        return;
    }

    const size_t len = 256 * 1024;
    uint8_t *src = malloc(len);
    if (src == NULL) {
        ok(false, "allocated source buffer");
        pk_engine_destroy(e);
        return;
    }
    fill_value(src, len, 31337);
    ok(pk_engine_put(e, B("hot"), L("hot"), src, len) == PK_ENGINE_OK,
       "store a value 64x its shard's budget");
    free(src);

    pk_engine_capacity_t cap;
    pk_engine_capacity(e, &cap);
    ok(cap.keys_in_ram_tier == 1 && cap.keys_in_nvme_tier == 0,
       "it stayed in RAM (ram=%llu nvme=%llu)",
       (unsigned long long)cap.keys_in_ram_tier,
       (unsigned long long)cap.keys_in_nvme_tier);
    ok(cap.spills == 0, "no spill happened at all");

    pk_engine_destroy(e);
}

static void test_tiering_disabled(void)
{
    section("no data_dir: a plain bounded RAM cache");

    /* Same tiny budget, no tier. Eviction has nowhere to put anything, so it
     * drops -- which is legitimate for a cache, and must be reported rather
     * than hidden. */
    pk_engine_t *e = new_engine(NULL, RAM_BUDGET);
    if (e == NULL) {
        ok(false, "engine created without a data_dir");
        return;
    }

    const int N = 2000;
    for (int i = 0; i < N; i++)
        put_indexed(e, i);

    pk_engine_capacity_t cap;
    pk_engine_capacity(e, &cap);
    ok(cap.evict_drops > 0, "entries were dropped rather than spilled (%llu)",
       (unsigned long long)cap.evict_drops);
    ok(cap.bytes_in_nvme_tier == 0 && cap.keys_in_nvme_tier == 0,
       "nothing claims to be on NVMe");
    ok(cap.resident_keys < (uint64_t)N,
       "the cache is smaller than the working set (%llu of %d keys)",
       (unsigned long long)cap.resident_keys, N);
    ok(cap.bytes_in_ram_tier <= RAM_BUDGET + 256ULL * VALUE_LEN,
       "RAM stayed inside the budget, allowing one protected entry per shard "
       "(%llu bytes)", (unsigned long long)cap.bytes_in_ram_tier);

    /*
     * The part that matters: whatever survived is CORRECT. A cache is allowed
     * to forget; it is not allowed to lie.
     */
    int found = 0, wrong = 0;
    for (int i = 0; i < N; i++) {
        char key[32];
        key_for(key, sizeof(key), i);
        uint8_t *out = NULL;
        uint64_t len = 0;
        pk_engine_result_t rc = pk_engine_get(e, B(key), L(key), &out, &len);
        if (rc == PK_ENGINE_OK) {
            found++;
            if (len != VALUE_LEN || !value_matches(out, VALUE_LEN, (uint64_t)i + 1))
                wrong++;
        }
        pk_engine_free_value(out);
    }
    ok(wrong == 0, "every surviving value is exact (%d survived, %d wrong)", found, wrong);

    pk_engine_destroy(e);
}

int main(void)
{
    printf("test_engine_tiering\n");

    char *dir = make_temp_dir();
    if (dir == NULL) {
        printf("  FAIL  could not create a temp data dir\n");
        return 1;
    }
    printf("  data_dir: %s\n", dir);

    test_spill_and_read_back(dir);
    test_promote_demote_cycles(dir);
    test_overwrite_spilled(dir);
    test_large_value_through_the_tier(dir);
    test_per_shard_budget(dir);
    test_mru_protection(dir);
    test_tiering_disabled();

    ok(remove_temp_dir(dir), "the engine left the data dir empty for removal");
    free(dir);

    return test_summary("test_engine_tiering");
}
