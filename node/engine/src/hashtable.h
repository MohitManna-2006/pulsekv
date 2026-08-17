#ifndef PULSEKV_ENGINE_HASHTABLE_H
#define PULSEKV_ENGINE_HASHTABLE_H

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#include "tiering.h"

/*
 * PulseKV v2 RAM tier -- extracted from v1's src/hashtable.c and extended.
 *
 * COPIED FROM v1, UNCHANGED IN SPIRIT:
 *   - 1,024 separate-chaining buckets striped across 256 independently locked
 *     shards. A request takes exactly one shard mutex, so keys in different
 *     shards proceed concurrently while four bucket chains share each lock.
 *   - FNV-1a over the raw key bytes for routing.
 *   - The table owns every key and value it is given; nothing is borrowed by
 *     pointer, because the caller's buffers do not outlive the request.
 *
 * That design needed no correcting. Concurrent gRPC handler threads calling in
 * is precisely the access pattern the 256-way striping was built for -- which
 * is why Phase 1 extracts the storage logic and drops v1's hand-rolled epoll
 * loop entirely: gRPC C++ already owns the sockets and the thread pool, so the
 * event loop would be dead code with no caller.
 *
 * ADDED IN v2:
 *   - uint64 value lengths. v1 capped values at 64 KiB; KV-cache blocks run to
 *     megabytes.
 *   - A per-shard LRU list, intrusive and doubly linked, protected by the
 *     shard mutex that already exists. Deliberately not a second lock: a
 *     tiering lock and a table lock acquired in two orders is how tiering bugs
 *     and hash-table bugs become deadlocks together.
 *   - Per-shard byte and key accounting per tier, so eviction decisions and
 *     CapacityResponse both read from the same numbers.
 *   - Spill to, and promotion from, the NVMe tier (tiering.h).
 *
 * INHERITED LIMITATION, still true: the bucket array is a fixed size. Well past
 * PK_TABLE_BUCKETS live keys the chains lengthen and lookups drift toward O(n).
 * Growth-and-rehash under the lock remains out of scope; see
 * docs/pulsekv-v2-phase1-summary.md.
 */

#define PK_TABLE_BUCKETS           1024u
#define PK_TABLE_SHARDS             256u
#define PK_TABLE_BUCKETS_PER_SHARD (PK_TABLE_BUCKETS / PK_TABLE_SHARDS)

_Static_assert((PK_TABLE_BUCKETS & (PK_TABLE_BUCKETS - 1u)) == 0,
               "bucket count must be a power of two");
_Static_assert((PK_TABLE_SHARDS & (PK_TABLE_SHARDS - 1u)) == 0,
               "shard count must be a power of two");
_Static_assert(PK_TABLE_BUCKETS % PK_TABLE_SHARDS == 0,
               "buckets must divide evenly across shards");

typedef struct pk_node pk_node_t;  /* chain node; layout is private to the .c */

typedef struct {
    pk_node_t      *buckets[PK_TABLE_BUCKETS_PER_SHARD];

    /* LRU order for this shard only. head is most-recently-used; eviction
     * takes from tail. */
    pk_node_t      *lru_head;
    pk_node_t      *lru_tail;

    /* Value bytes and key counts, split by tier. Maintained under `lock`, so
     * they are exact rather than eventually-consistent. */
    uint64_t        ram_bytes;
    uint64_t        nvme_bytes;
    uint64_t        ram_keys;
    uint64_t        nvme_keys;

    pthread_mutex_t lock;
} pk_table_shard_t;

typedef struct {
    pk_table_shard_t shards[PK_TABLE_SHARDS];
    atomic_size_t    count;

    pk_tier_t       *tier;                /* NULL => tiering disabled */
    uint64_t         shard_budget_bytes;  /* ram_budget_bytes / PK_TABLE_SHARDS */
    uint64_t         max_value_bytes;

    /* Diagnostics. Relaxed atomics: they inform logs, tests, and the
     * benchmark, and never gate a decision. */
    atomic_uint_least64_t spills;
    atomic_uint_least64_t promotions;
    atomic_uint_least64_t spill_errors;
    atomic_uint_least64_t evict_drops;
} pk_table_t;

typedef enum {
    PK_TABLE_OK        =  0,
    PK_TABLE_NOT_FOUND =  1,
    PK_TABLE_NOMEM     = -1,
    PK_TABLE_TOO_BIG   = -2,  /* value exceeds max_value_bytes */
    PK_TABLE_INVALID   = -3,  /* empty key, or a length/pointer pair that disagree */
    PK_TABLE_IO_ERROR  = -4   /* the spilled value could not be read back */
} pk_table_result_t;

/* Returns 0, or an errno from pthread_mutex_init. A partial initialization is
 * rolled back before an error is returned. Does not take ownership of `tier`. */
int pk_table_init(pk_table_t *t, pk_tier_t *tier,
                  uint64_t ram_budget_bytes, uint64_t max_value_bytes);

/* Frees every node and unlinks every spill file it owns. The caller must
 * ensure nothing else is using the table. */
void pk_table_destroy(pk_table_t *t);

/* Copies the value in, replacing any previous one (including a spilled one).
 * May evict colder entries in the same shard to stay inside the budget. */
pk_table_result_t pk_table_set(pk_table_t *t, const uint8_t *key, uint32_t klen,
                               const uint8_t *val, uint64_t vlen);

/* On a hit, hands back a malloc()'d copy the caller owns and frees.
 * Promotes a spilled value back into RAM and marks the entry most-recently-used. */
pk_table_result_t pk_table_get(pk_table_t *t, const uint8_t *key, uint32_t klen,
                               uint8_t **out_val, uint64_t *out_vlen);

/* Like pk_table_get, but never promotes and never reorders the LRU. For scans,
 * which must not evict hot data just by looking at it. */
pk_table_result_t pk_table_peek(pk_table_t *t, const uint8_t *key, uint32_t klen,
                                uint8_t **out_val, uint64_t *out_vlen);

pk_table_result_t pk_table_del(pk_table_t *t, const uint8_t *key, uint32_t klen);

/* Collects every key with the given prefix. prefix_len == 0 matches everything.
 * Keys only: values are fetched afterwards, one at a time, so a scan never
 * holds a shard lock while a value is copied or streamed.
 *
 * *out_keys is an array of *out_count malloc()'d key buffers; free the whole
 * thing with pk_table_free_keys. */
pk_table_result_t pk_table_scan_prefix(pk_table_t *t,
                                       const uint8_t *prefix, uint32_t prefix_len,
                                       uint8_t ***out_keys, uint32_t **out_klens,
                                       size_t *out_count);
void pk_table_free_keys(uint8_t **keys, uint32_t *klens, size_t count);

typedef struct {
    uint64_t total_keys;
    uint64_t ram_keys;
    uint64_t nvme_keys;
    uint64_t ram_bytes;
    uint64_t nvme_bytes;
    uint64_t spills;
    uint64_t promotions;
    uint64_t spill_errors;
    uint64_t evict_drops;
} pk_table_stats_t;

/* Walks all 256 shards under their locks, so the numbers agree with each other
 * per shard but are not a single global instant. That is the right trade: a
 * stop-the-world snapshot would serialize the whole table for a metrics read. */
void pk_table_stats(pk_table_t *t, pk_table_stats_t *out);

/* Exact atomic snapshot without taking any shard lock. */
size_t pk_table_count(pk_table_t *t);

/* FNV-1a over the raw key bytes. Public so tests can construct deliberate
 * shard and bucket collisions. */
uint64_t pk_table_hash(const uint8_t *key, uint32_t klen);

#endif /* PULSEKV_ENGINE_HASHTABLE_H */
