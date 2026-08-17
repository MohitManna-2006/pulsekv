#ifndef PULSEKV_ENGINE_H
#define PULSEKV_ENGINE_H

#include <stddef.h>
#include <stdint.h>

/*
 * PulseKV v2 storage engine -- the whole public surface.
 *
 * THIS IS THE ONLY HEADER node/grpc_shim/ MAY INCLUDE FROM node/engine/.
 * hashtable.h, tiering.h, and every other internal live under src/ and are not
 * installed anywhere the shim can reach. The rule runs both ways: no protobuf,
 * no gRPC, and no C++ type ever appears on this side of the line either. The
 * engine is testable, and benchmarkable, with no network stack in the picture
 * at all. See node/README.md for why the boundary sits exactly here.
 *
 * WHAT THE ENGINE IS: a two-tier, bounded, loss-tolerant cache for one node.
 * Hot values live in a 256-way sharded in-memory table (extracted from v1);
 * cold values spill to local NVMe; a read promotes them back. It is explicitly
 * not a system of record -- there is no WAL, nothing survives a restart, and
 * eviction may drop an entry outright if the disk refuses it. Losing a value
 * costs a recompute, which is the trade docs/pulsekv-v2-distributed-design.md
 * section 4.3 makes on purpose.
 *
 * THREAD SAFETY: every function below is safe to call concurrently from any
 * number of threads, which is the point -- gRPC's C++ server calls in from its
 * own handler pool. The one exception is pk_engine_destroy, which requires that
 * no other call is in flight.
 */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pk_engine pk_engine_t;

/* Hard ceiling on a single stored value. Values above this are refused before
 * anything is allocated for them. */
#define PK_ENGINE_DEFAULT_MAX_VALUE_BYTES  (64ULL * 1024 * 1024)

/* Total RAM-tier budget across all shards. */
#define PK_ENGINE_DEFAULT_RAM_BUDGET_BYTES (256ULL * 1024 * 1024)

typedef struct {
    /*
     * NVMe spill directory for this node. The engine creates and exclusively
     * owns <data_dir>/spill/, and PURGES IT AT STARTUP AND SHUTDOWN -- spill
     * files are unreachable once the in-RAM index naming them is gone, so
     * anything found there is garbage from a previous run. Nothing outside that
     * subdirectory is ever touched.
     *
     * NULL or "" disables the NVMe tier entirely. The engine is then a plain
     * bounded RAM cache and eviction drops entries instead of spilling them.
     * Tests use that; nodes should not.
     */
    const char *data_dir;

    /* 0 selects the defaults above. */
    uint64_t    ram_budget_bytes;
    uint64_t    max_value_bytes;
} pk_engine_config_t;

typedef enum {
    PK_ENGINE_OK        = 0,
    PK_ENGINE_NOT_FOUND = 1,
    PK_ENGINE_TOO_LARGE = 2,  /* value exceeds max_value_bytes */
    PK_ENGINE_INVALID   = 3,  /* empty key, or a length/pointer pair that disagree */
    PK_ENGINE_IO_ERROR  = 4,  /* a spilled value could not be read back */
    PK_ENGINE_NOMEM     = 5
} pk_engine_result_t;

/* Stable, human-readable name for a result. Never NULL. */
const char *pk_engine_strerror(pk_engine_result_t r);

/* Returns NULL if the config is unusable or the spill directory cannot be
 * created. */
pk_engine_t *pk_engine_create(const pk_engine_config_t *cfg);
void         pk_engine_destroy(pk_engine_t *e);

/* The effective limits, after defaults were applied. The gRPC shim needs
 * max_value_bytes to reject an oversized chunked write before it buffers a
 * single byte of it. */
uint64_t pk_engine_max_value_bytes(const pk_engine_t *e);
uint64_t pk_engine_ram_budget_bytes(const pk_engine_t *e);

/* Copies the value in; the caller's buffer is not retained. Replaces any
 * previous value for the key, in either tier. */
pk_engine_result_t pk_engine_put(pk_engine_t *e,
                                 const uint8_t *key, uint32_t key_len,
                                 const uint8_t *val, uint64_t val_len);

/*
 * On PK_ENGINE_OK, *out_val is heap-allocated and owned by the caller; free it
 * with pk_engine_free_value. A zero-length value yields *out_val == NULL with
 * *out_len == 0, which is still PK_ENGINE_OK -- an empty value is a hit.
 *
 * A hit on a spilled value promotes it back into the RAM tier, which may evict
 * something colder from the same shard.
 */
pk_engine_result_t pk_engine_get(pk_engine_t *e,
                                 const uint8_t *key, uint32_t key_len,
                                 uint8_t **out_val, uint64_t *out_len);

/*
 * Same as pk_engine_get, but does not promote a spilled value and does not
 * touch recency order. This is what a prefix scan reads through: walking the
 * whole keyspace must not evict the hot working set as a side effect.
 */
pk_engine_result_t pk_engine_peek(pk_engine_t *e,
                                  const uint8_t *key, uint32_t key_len,
                                  uint8_t **out_val, uint64_t *out_len);

/* Tolerates NULL. */
void pk_engine_free_value(uint8_t *val);

typedef struct {
    uint8_t  *key;
    uint32_t  key_len;
} pk_engine_key_t;

typedef struct {
    pk_engine_key_t *keys;
    size_t           count;
} pk_engine_keyset_t;

/*
 * Every key carrying the given prefix. prefix_len == 0 matches everything.
 *
 * Keys only, deliberately: the caller fetches values afterwards one at a time
 * (with pk_engine_peek), so no shard lock is ever held while a multi-megabyte
 * value is copied or streamed to a client.
 *
 * NOT A SNAPSHOT. Shards are visited one at a time under their own locks, so a
 * concurrent write to a shard already passed is not reflected and one to a
 * shard not yet reached is. For a cache index this is the right trade; a
 * consistent scan would mean freezing all 256 shards at once. Keys can also
 * disappear between the scan and the fetch, so treat a subsequent NOT_FOUND as
 * normal.
 *
 * Cost is O(total keys) -- see the PrefixMatch note in
 * docs/pulsekv-v2-phase1-summary.md.
 */
pk_engine_result_t pk_engine_scan_prefix(pk_engine_t *e,
                                         const uint8_t *prefix, uint32_t prefix_len,
                                         pk_engine_keyset_t *out);
void pk_engine_free_keyset(pk_engine_keyset_t *ks);

typedef struct {
    /* These three map field-for-field onto CapacityResponse in
     * proto/node.proto. */
    uint64_t resident_keys;       /* keys this node answers for, across both tiers */
    uint64_t bytes_in_ram_tier;   /* value bytes only; the index is not counted */
    uint64_t bytes_in_nvme_tier;  /* value bytes only; file headers are not counted */

    /* Diagnostics beyond the proto contract: logs, tests, and the benchmark. */
    uint64_t keys_in_ram_tier;
    uint64_t keys_in_nvme_tier;
    uint64_t spills;         /* values moved RAM -> NVMe */
    uint64_t promotions;     /* values moved NVMe -> RAM */
    uint64_t spill_errors;   /* spill writes that failed; the entry was dropped */
    uint64_t evict_drops;    /* entries dropped rather than spilled */
} pk_engine_capacity_t;

void pk_engine_capacity(const pk_engine_t *e, pk_engine_capacity_t *out);

#ifdef __cplusplus
}
#endif

#endif /* PULSEKV_ENGINE_H */
