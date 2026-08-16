#ifndef PULSEKV_HASHTABLE_H
#define PULSEKV_HASHTABLE_H

#include <pthread.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdint.h>

/*
 * PulseKV in-memory store: 1,024 separate-chaining buckets striped across 256
 * independently locked shards. A request takes exactly one shard mutex, so
 * keys in different shards proceed concurrently while four bucket chains share
 * each lock. Keeping bucket count separate from lock count lets capacity and
 * contention be tuned independently.
 *
 * Ownership is the thing to get right here. The table copies every key and
 * value it is given, because the caller's pointers are decode views into a
 * connection's read buffer and that buffer is overwritten by the next frame.
 * A hit is copied out the same way, into a caller-provided buffer, rather than
 * borrowed by pointer: the node it came from can be freed by the next DEL the
 * instant the lock is released.
 *
 * TODO(later): the bucket array is a fixed size. Much past PK_TABLE_BUCKETS
 * live keys the chains lengthen and lookups drift towards O(n); the design's
 * 1M-key target would want growth-and-rehash under the lock. Deliberately out
 * of scope for this correctness baseline.
 */

#define PK_TABLE_BUCKETS          1024u
#define PK_TABLE_SHARDS            256u
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
    pthread_mutex_t lock;
} pk_table_shard_t;

typedef struct {
    pk_table_shard_t shards[PK_TABLE_SHARDS];
    atomic_size_t    count;
} pk_table_t;

typedef enum {
    PK_TABLE_OK        =  0,
    PK_TABLE_NOT_FOUND =  1,
    PK_TABLE_NOMEM     = -1,
    PK_TABLE_TOO_BIG   = -2,  /* the value does not fit the caller's buffer */
    PK_TABLE_INVALID   = -3   /* empty key, or a length/pointer pair that disagree */
} pk_table_result_t;

/* Returns 0, or an errno from pthread_mutex_init. A partial initialization is
 * rolled back before an error is returned. */
int  pk_table_init(pk_table_t *t);

/* Frees every node. The caller must ensure nothing else is using the table. */
void pk_table_destroy(pk_table_t *t);

/* Copies key and value in. Frees the previous value when overwriting. */
pk_table_result_t pk_table_set(pk_table_t *t, const uint8_t *key, uint32_t klen,
                               const uint8_t *val, uint32_t vlen);

/* On a hit, copies the value into out_val and writes its length to out_vlen.
 * Returns PK_TABLE_TOO_BIG, without copying, if the value exceeds out_cap. */
pk_table_result_t pk_table_get(pk_table_t *t, const uint8_t *key, uint32_t klen,
                               uint8_t *out_val, size_t out_cap, uint32_t *out_vlen);

pk_table_result_t pk_table_del(pk_table_t *t, const uint8_t *key, uint32_t klen);

/* Exact atomic snapshot without taking any shard lock. */
size_t pk_table_count(pk_table_t *t);

/* FNV-1a over the raw key bytes. Public so tests and future benchmarks can
 * construct deliberate shard and bucket collisions. */
uint64_t pk_table_hash(const uint8_t *key, uint32_t klen);

#endif /* PULSEKV_HASHTABLE_H */
