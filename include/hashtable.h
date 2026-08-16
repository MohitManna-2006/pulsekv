#ifndef PULSEKV_HASHTABLE_H
#define PULSEKV_HASHTABLE_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

/*
 * PulseKV in-memory store: separate chaining, one lock for the whole table.
 *
 * Ownership is the thing to get right here. The table copies every key and
 * value it is given, because the caller's pointers are decode views into a
 * connection's read buffer and that buffer is overwritten by the next frame.
 * A hit is copied out the same way, into a caller-provided buffer, rather than
 * borrowed by pointer: the node it came from can be freed by the next DEL the
 * instant the lock is released.
 *
 * Nothing here is a singleton. Step 5 shards the store by making an array of
 * these and routing on the hash, with no change to the operations themselves.
 *
 * TODO(later): the bucket array is a fixed size. Much past PK_TABLE_BUCKETS
 * live keys the chains lengthen and lookups drift towards O(n); the design's
 * 1M-key target would want growth-and-rehash under the lock. Deliberately out
 * of scope for this correctness baseline.
 */

#define PK_TABLE_BUCKETS 1024u  /* power of two: the index is a mask, not a modulo */

typedef struct pk_node pk_node_t;  /* chain node; layout is private to the .c */

typedef struct {
    pk_node_t      *buckets[PK_TABLE_BUCKETS];
    pthread_mutex_t lock;
    size_t          count;
} pk_table_t;

typedef enum {
    PK_TABLE_OK        =  0,
    PK_TABLE_NOT_FOUND =  1,
    PK_TABLE_NOMEM     = -1,
    PK_TABLE_TOO_BIG   = -2,  /* the value does not fit the caller's buffer */
    PK_TABLE_INVALID   = -3   /* empty key, or a length/pointer pair that disagree */
} pk_table_result_t;

/* Returns 0, or an errno from pthread_mutex_init. */
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

size_t pk_table_count(pk_table_t *t);

/* FNV-1a over the raw key bytes. Public because step 5's shard router needs the
 * same hash, and because tests construct deliberate bucket collisions with it. */
uint64_t pk_table_hash(const uint8_t *key, uint32_t klen);

#endif /* PULSEKV_HASHTABLE_H */
