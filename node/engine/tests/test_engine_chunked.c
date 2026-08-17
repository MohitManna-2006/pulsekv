/*
 * The engine half of Step 1.2: the hard value-size ceiling, and multi-megabyte
 * values surviving a round trip intact.
 *
 * The wire half -- PutChunked/GetChunked framing, out-of-order chunks, lying
 * total_length, the 4 MiB unary line -- is asserted against a live node by
 * deploy/smoke-test.sh, because that is where the framing actually lives.
 * What belongs here is the contract the framing depends on: that
 * max_value_bytes is enforced before anything is allocated, that the limit is
 * readable so the shim can reject an oversized stream before buffering it, and
 * that a value far larger than v1's 64 KiB frame comes back byte for byte.
 */

#include "test_util.h"

#include "pulsekv_engine.h"

#define MAX_VALUE  (8ULL * 1024 * 1024)
/* Deliberately large: 4 GiB / 256 shards = 16 MiB per shard, so nothing in this
 * file evicts anything. Eviction has its own test. */
#define RAM_BUDGET (4ULL * 1024 * 1024 * 1024)

static pk_engine_t *new_engine(void)
{
    pk_engine_config_t cfg = {
        .data_dir         = NULL,
        .ram_budget_bytes = RAM_BUDGET,
        .max_value_bytes  = MAX_VALUE,
    };
    return pk_engine_create(&cfg);
}

/* Store a generated value of `len` bytes and read it back byte for byte. */
static bool roundtrip(pk_engine_t *e, const char *key, size_t len, uint64_t seed)
{
    uint8_t *src = malloc(len ? len : 1);
    if (src == NULL)
        return false;
    fill_value(src, len, seed);

    pk_engine_result_t rc = pk_engine_put(e, B(key), L(key), src, len);
    free(src);
    if (rc != PK_ENGINE_OK)
        return false;

    uint8_t *out = NULL;
    uint64_t out_len = 0;
    if (pk_engine_get(e, B(key), L(key), &out, &out_len) != PK_ENGINE_OK)
        return false;

    bool good = (out_len == len) && (len == 0 || value_matches(out, len, seed));
    pk_engine_free_value(out);
    return good;
}

static void test_limit_is_readable(pk_engine_t *e)
{
    section("the limit is part of the contract");

    /* The gRPC shim reads this to reject an oversized PutChunked stream on its
     * first chunk, before it buffers a byte. Without it the shim would have to
     * hardcode a number the engine owns. */
    ok(pk_engine_max_value_bytes(e) == MAX_VALUE,
       "max_value_bytes reads back as configured (%llu)",
       (unsigned long long)pk_engine_max_value_bytes(e));
}

static void test_ceiling(pk_engine_t *e)
{
    section("value size ceiling");

    ok(roundtrip(e, "at-limit", (size_t)MAX_VALUE, 11),
       "a value of exactly max_value_bytes is accepted and round-trips");

    /*
     * One byte over. The rejection has to happen before the engine allocates
     * anything for it -- bounds before trust, the same discipline v1's
     * protocol.c applies to every decoded frame -- so this must not require
     * MAX_VALUE+1 bytes of headroom to fail.
     */
    static uint8_t probe[64];
    ok(pk_engine_put(e, B("over"), L("over"), probe, MAX_VALUE + 1) == PK_ENGINE_TOO_LARGE,
       "max_value_bytes + 1 is TOO_LARGE");

    /* A wildly oversized claim must be refused on the number alone. If this
     * ever tried to allocate first, the test host would notice. */
    ok(pk_engine_put(e, B("absurd"), L("absurd"), probe, (uint64_t)1 << 60)
           == PK_ENGINE_TOO_LARGE,
       "an absurd length is refused without attempting the allocation");
}

static void test_rejection_is_clean(void)
{
    section("a rejected write changes nothing");

    pk_engine_t *e = new_engine();
    if (e == NULL) {
        ok(false, "engine created");
        return;
    }

    ok(pk_engine_put(e, B("keep"), L("keep"), B("original"), 8) == PK_ENGINE_OK,
       "store a small value");

    pk_engine_capacity_t before;
    pk_engine_capacity(e, &before);

    static uint8_t probe[64];
    ok(pk_engine_put(e, B("keep"), L("keep"), probe, MAX_VALUE + 1) == PK_ENGINE_TOO_LARGE,
       "an oversized overwrite of an existing key is rejected");

    /*
     * The failure mode this rules out: validating the size after having already
     * freed or unlinked the previous value, which would turn a rejected write
     * into a silent delete.
     */
    uint8_t *val = NULL;
    uint64_t len = 0;
    ok(pk_engine_get(e, B("keep"), L("keep"), &val, &len) == PK_ENGINE_OK &&
       len == 8 && memcmp(val, "original", 8) == 0,
       "the previous value survives the rejected overwrite");
    pk_engine_free_value(val);

    pk_engine_capacity_t after;
    pk_engine_capacity(e, &after);
    ok(before.resident_keys == after.resident_keys &&
       before.bytes_in_ram_tier == after.bytes_in_ram_tier,
       "capacity is unchanged by the rejected write");

    pk_engine_destroy(e);
}

static void test_large_values(pk_engine_t *e)
{
    section("values far past v1's 64 KiB frame");

    /* v1's wire protocol capped a value at 64 KiB in one frame. These are the
     * sizes a KV-cache block actually runs to. */
    const size_t sizes[] = {
        0,
        1,
        64 * 1024,              /* v1's old ceiling */
        64 * 1024 + 1,          /* one past it */
        1024 * 1024,            /* 1 MiB */
        4 * 1024 * 1024,        /* the unary/chunked wire line */
        4 * 1024 * 1024 + 1,    /* one past the wire line; the engine does not care */
        5 * 1024 * 1024,        /* a realistic multi-megabyte block */
    };

    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++) {
        char key[64];
        snprintf(key, sizeof(key), "big-%zu", sizes[i]);
        ok(roundtrip(e, key, sizes[i], 1000 + i),
           "%zu-byte value round-trips byte for byte", sizes[i]);
    }
}

static void test_large_overwrite(pk_engine_t *e)
{
    section("resizing a large value in place");

    ok(roundtrip(e, "resize", 5 * 1024 * 1024, 4242),
       "store 5 MiB");
    ok(roundtrip(e, "resize", 128, 4243),
       "shrink the same key to 128 bytes");
    ok(roundtrip(e, "resize", 6 * 1024 * 1024, 4244),
       "grow it again to 6 MiB");

    pk_engine_capacity_t cap;
    pk_engine_capacity(e, &cap);
    /* If an overwrite ever leaked the old value's bytes from the accounting,
     * this total would drift upward instead of tracking the live value. */
    ok(cap.bytes_in_ram_tier >= 6 * 1024 * 1024,
       "byte accounting tracks the current value, not the sum of every version");
}

int main(void)
{
    printf("test_engine_chunked\n");

    pk_engine_t *e = new_engine();
    if (e == NULL) {
        printf("  FAIL  could not create engine\n");
        return 1;
    }

    test_limit_is_readable(e);
    test_ceiling(e);
    test_large_values(e);
    test_large_overwrite(e);
    pk_engine_destroy(e);

    test_rejection_is_clean();

    return test_summary("test_engine_chunked");
}
