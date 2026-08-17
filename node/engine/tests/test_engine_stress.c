/*
 * Concurrency, in the spirit of v1's tests/test_thread_stress.c, against a RAM
 * budget small enough that eviction is running constantly underneath the
 * workload.
 *
 * v2 gives each shard more shared mutable state than v1's did -- an intrusive
 * LRU list and four counters on top of the chains -- and eviction mutates a
 * node that a concurrent reader may be about to promote. So the assertions are
 * chosen to catch the specific ways that goes wrong:
 *
 *   Phase A, disjoint keys: the final state is fully determined, so every key
 *     must hold exactly its last written value. Catches lost updates and
 *     accounting drift.
 *   Phase B, shared keys: the final value is a race by design, so instead every
 *     value read is checked for SELF-consistency -- its payload must match the
 *     seed stamped in its own first eight bytes. A torn write, or a value
 *     spliced from two different writers, fails this even though no single
 *     expected value exists.
 *   Phase C, scans concurrent with writes: a prefix scan walks all 256 shards
 *     while they are being mutated. It must not crash, and every value it
 *     returns must still be self-consistent.
 *
 * Build with -DPULSEKV_SANITIZE=thread and run this to check the locking
 * itself; the assertions above only see races that happen to corrupt data on
 * the day, and ThreadSanitizer sees the ones that did not.
 */

#include "test_util.h"

#include <pthread.h>
#include <stdatomic.h>

#include "pulsekv_engine.h"

#define THREADS      8
#define ITERATIONS   2000
#define VALUE_LEN    512u
#define SHARED_KEYS  16

/* 512-byte values, ~2 per shard: eviction never stops. */
#define RAM_BUDGET   (256ULL * 1024)
#define MAX_VALUE    (1ULL * 1024 * 1024)

/* A value carries the seed that generated it in its first eight bytes, so any
 * value can be checked on its own without knowing who wrote it. */
static void stamp_value(uint8_t *buf, size_t len, uint64_t seed)
{
    fill_value(buf, len, seed);
    if (len >= 8)
        memcpy(buf, &seed, 8);
}

static bool value_self_consistent(const uint8_t *buf, size_t len)
{
    if (len < 8)
        return true;

    uint64_t seed;
    memcpy(&seed, buf, 8);

    uint8_t *expect = malloc(len);
    if (expect == NULL)
        return true;  /* cannot check; do not invent a failure */
    fill_value(expect, len, seed);
    memcpy(expect, &seed, 8);

    bool good = memcmp(expect, buf, len) == 0;
    free(expect);
    return good;
}

typedef struct {
    pk_engine_t *engine;
    int          thread_id;
    /* results */
    int          write_failures;
    int          inconsistent;
    int          io_errors;
    long         reads_hit;
    long         scans;
} worker_t;

static uint64_t seed_for(int thread_id, int iteration)
{
    return ((uint64_t)(thread_id + 1) << 32) | (uint64_t)(iteration + 1);
}

/* ------------------------------------------------------------------ */
/* Phase A: disjoint keys -- the final state is knowable exactly. */

static void *disjoint_worker(void *arg)
{
    worker_t *w = arg;
    uint8_t   val[VALUE_LEN];

    for (int i = 0; i < ITERATIONS; i++) {
        char key[64];
        snprintf(key, sizeof(key), "t%02d-key-%06d", w->thread_id, i);

        stamp_value(val, sizeof(val), seed_for(w->thread_id, i));
        if (pk_engine_put(w->engine, B(key), L(key), val, sizeof(val)) != PK_ENGINE_OK)
            w->write_failures++;

        /* Read something written earlier by this thread: with eviction running
         * it may well have been spilled and have to come back off disk. */
        if (i > 0) {
            int      target = i / 2;
            char     prev[64];
            uint8_t *out = NULL;
            uint64_t len = 0;
            snprintf(prev, sizeof(prev), "t%02d-key-%06d", w->thread_id, target);

            pk_engine_result_t rc = pk_engine_get(w->engine, B(prev), L(prev), &out, &len);
            if (rc == PK_ENGINE_OK) {
                w->reads_hit++;
                if (len != VALUE_LEN || !value_self_consistent(out, (size_t)len))
                    w->inconsistent++;
            } else if (rc == PK_ENGINE_IO_ERROR) {
                w->io_errors++;
            }
            pk_engine_free_value(out);
        }
    }
    return NULL;
}

static void test_disjoint(pk_engine_t *e)
{
    section("phase A: concurrent writers on disjoint keys");

    pthread_t threads[THREADS];
    worker_t  workers[THREADS];
    memset(workers, 0, sizeof(workers));

    for (int t = 0; t < THREADS; t++) {
        workers[t].engine    = e;
        workers[t].thread_id = t;
        if (pthread_create(&threads[t], NULL, disjoint_worker, &workers[t]) != 0) {
            ok(false, "spawned thread %d", t);
            return;
        }
    }
    for (int t = 0; t < THREADS; t++)
        pthread_join(threads[t], NULL);

    int  write_failures = 0, inconsistent = 0, io_errors = 0;
    long reads_hit = 0;
    for (int t = 0; t < THREADS; t++) {
        write_failures += workers[t].write_failures;
        inconsistent   += workers[t].inconsistent;
        io_errors      += workers[t].io_errors;
        reads_hit      += workers[t].reads_hit;
    }

    ok(write_failures == 0, "%d threads x %d writes, none failed", THREADS, ITERATIONS);
    ok(io_errors == 0, "no NVMe I/O errors during the run");
    ok(inconsistent == 0, "every one of %ld concurrent reads was self-consistent", reads_hit);

    /* The determinism check. Disjoint key ranges mean there is exactly one
     * correct final value per key, whatever order the threads ran in. */
    int missing = 0, wrong = 0;
    uint8_t expect[VALUE_LEN];
    for (int t = 0; t < THREADS; t++) {
        for (int i = 0; i < ITERATIONS; i++) {
            char key[64];
            snprintf(key, sizeof(key), "t%02d-key-%06d", t, i);

            uint8_t *out = NULL;
            uint64_t len = 0;
            if (pk_engine_get(e, B(key), L(key), &out, &len) != PK_ENGINE_OK) {
                missing++;
            } else {
                stamp_value(expect, sizeof(expect), seed_for(t, i));
                if (len != VALUE_LEN || memcmp(out, expect, VALUE_LEN) != 0)
                    wrong++;
            }
            pk_engine_free_value(out);
        }
    }
    ok(missing == 0, "all %d keys survived (missing: %d)", THREADS * ITERATIONS, missing);
    ok(wrong == 0, "every key holds exactly its last written value (wrong: %d)", wrong);

    pk_engine_capacity_t cap;
    pk_engine_capacity(e, &cap);
    ok(cap.resident_keys == (uint64_t)(THREADS * ITERATIONS),
       "capacity agrees: %llu keys", (unsigned long long)cap.resident_keys);
    ok(cap.bytes_in_ram_tier + cap.bytes_in_nvme_tier ==
           (uint64_t)THREADS * ITERATIONS * VALUE_LEN,
       "byte accounting survived %d concurrent evicting writes",
       THREADS * ITERATIONS);
    ok(cap.spills > 0, "eviction was genuinely active (%llu spills, %llu promotions)",
       (unsigned long long)cap.spills, (unsigned long long)cap.promotions);
    ok(cap.spill_errors == 0 && cap.evict_drops == 0,
       "no spill errors and no drops");
}

/* ------------------------------------------------------------------ */
/* Phase B: shared keys -- the winner is a race, self-consistency is not. */

static void *shared_worker(void *arg)
{
    worker_t *w = arg;
    uint8_t   val[VALUE_LEN];

    for (int i = 0; i < ITERATIONS; i++) {
        char key[32];
        snprintf(key, sizeof(key), "shared-%02d", i % SHARED_KEYS);

        stamp_value(val, sizeof(val), seed_for(w->thread_id, i));
        if (pk_engine_put(w->engine, B(key), L(key), val, sizeof(val)) != PK_ENGINE_OK)
            w->write_failures++;

        uint8_t *out = NULL;
        uint64_t len = 0;
        pk_engine_result_t rc = pk_engine_get(w->engine, B(key), L(key), &out, &len);
        if (rc == PK_ENGINE_OK) {
            w->reads_hit++;
            /* Whoever won, the bytes must be one writer's value in full --
             * never a splice of two, never half-overwritten. */
            if (len != VALUE_LEN || !value_self_consistent(out, (size_t)len))
                w->inconsistent++;
        } else if (rc == PK_ENGINE_IO_ERROR) {
            w->io_errors++;
        }
        pk_engine_free_value(out);
    }
    return NULL;
}

static void test_shared(pk_engine_t *e)
{
    section("phase B: concurrent churn on shared keys");

    pthread_t threads[THREADS];
    worker_t  workers[THREADS];
    memset(workers, 0, sizeof(workers));

    for (int t = 0; t < THREADS; t++) {
        workers[t].engine    = e;
        workers[t].thread_id = t;
        pthread_create(&threads[t], NULL, shared_worker, &workers[t]);
    }
    for (int t = 0; t < THREADS; t++)
        pthread_join(threads[t], NULL);

    int  inconsistent = 0, write_failures = 0, io_errors = 0;
    long hits = 0;
    for (int t = 0; t < THREADS; t++) {
        inconsistent   += workers[t].inconsistent;
        write_failures += workers[t].write_failures;
        io_errors      += workers[t].io_errors;
        hits           += workers[t].reads_hit;
    }

    ok(write_failures == 0, "no write failures under shared-key contention");
    ok(io_errors == 0, "no NVMe I/O errors under shared-key contention");
    ok(inconsistent == 0,
       "%ld reads of %d hotly contested keys, every one a complete single "
       "writer's value (%d torn)", hits, SHARED_KEYS, inconsistent);
}

/* ------------------------------------------------------------------ */
/* Phase C: prefix scan running against live mutation. */

/*
 * atomic, not volatile. `volatile` orders nothing and synchronizes nothing in
 * C; it only stops the compiler caching the load. ThreadSanitizer reports the
 * plain-int version as a data race, and it is right to -- in a file whose whole
 * purpose is to prove the engine's locking, the harness has to be correct
 * first, or every future run starts by triaging a race that was never in the
 * engine.
 */
static atomic_int g_scanning;

static void *scan_worker(void *arg)
{
    worker_t *w = arg;

    while (atomic_load_explicit(&g_scanning, memory_order_relaxed)) {
        pk_engine_keyset_t ks;
        if (pk_engine_scan_prefix(w->engine, B("shared-"), L("shared-"), &ks) != PK_ENGINE_OK)
            continue;
        w->scans++;

        for (size_t i = 0; i < ks.count; i++) {
            uint8_t *out = NULL;
            uint64_t len = 0;
            /* peek, not get: a scan must not promote and must not reorder. */
            pk_engine_result_t rc = pk_engine_peek(w->engine, ks.keys[i].key,
                                                   ks.keys[i].key_len, &out, &len);
            if (rc == PK_ENGINE_OK) {
                if (len != VALUE_LEN || !value_self_consistent(out, (size_t)len))
                    w->inconsistent++;
            } else if (rc == PK_ENGINE_IO_ERROR) {
                w->io_errors++;
            }
            /* NOT_FOUND is expected and fine: the key may have been evicted
             * between the scan and the fetch. */
            pk_engine_free_value(out);
        }
        pk_engine_free_keyset(&ks);
    }
    return NULL;
}

static void test_scan_under_mutation(pk_engine_t *e)
{
    section("phase C: prefix scans concurrent with writes");

    pthread_t writers[THREADS];
    worker_t  wworkers[THREADS];
    pthread_t scanner;
    worker_t  sworker;

    memset(wworkers, 0, sizeof(wworkers));
    memset(&sworker, 0, sizeof(sworker));
    sworker.engine = e;

    atomic_store_explicit(&g_scanning, 1, memory_order_relaxed);
    pthread_create(&scanner, NULL, scan_worker, &sworker);

    for (int t = 0; t < THREADS; t++) {
        wworkers[t].engine    = e;
        wworkers[t].thread_id = t;
        pthread_create(&writers[t], NULL, shared_worker, &wworkers[t]);
    }
    for (int t = 0; t < THREADS; t++)
        pthread_join(writers[t], NULL);

    atomic_store_explicit(&g_scanning, 0, memory_order_relaxed);
    pthread_join(scanner, NULL);

    ok(sworker.scans > 0, "the scanner completed %ld scans during the writes",
       sworker.scans);
    ok(sworker.inconsistent == 0,
       "every value a concurrent scan returned was self-consistent (%d torn)",
       sworker.inconsistent);
    ok(sworker.io_errors == 0, "no NVMe I/O errors seen by the scanner");
}

int main(void)
{
    printf("test_engine_stress\n");

    char *dir = make_temp_dir();
    if (dir == NULL) {
        printf("  FAIL  could not create a temp data dir\n");
        return 1;
    }
    printf("  data_dir: %s\n", dir);

    pk_engine_config_t cfg = {
        .data_dir         = dir,
        .ram_budget_bytes = RAM_BUDGET,
        .max_value_bytes  = MAX_VALUE,
    };
    pk_engine_t *e = pk_engine_create(&cfg);
    if (e == NULL) {
        printf("  FAIL  could not create engine\n");
        free(dir);
        return 1;
    }

    test_disjoint(e);
    test_shared(e);
    test_scan_under_mutation(e);

    pk_engine_destroy(e);
    ok(remove_temp_dir(dir), "the engine left the data dir empty for removal");
    free(dir);

    return test_summary("test_engine_stress");
}
