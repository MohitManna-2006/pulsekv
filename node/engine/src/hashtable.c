#include "hashtable.h"

#include <stdlib.h>
#include <string.h>

#define FNV1A_OFFSET_BASIS 0xcbf29ce484222325ULL
#define FNV1A_PRIME        0x00000100000001b3ULL

/*
 * The key rides along in the node's own allocation: it is fixed for the node's
 * lifetime, so there is no reason to chase a second pointer for it. The value
 * is separate because an overwrite replaces it with one of a different size,
 * and because spilling frees the value while the node stays put.
 *
 * A spilled node keeps its place in the hash chain. Only the value moves to
 * disk; the index stays in RAM. That is what makes a miss on a key this node
 * has never seen cost zero filesystem work, keeps CapacityResponse exact
 * without walking the spill tree, and lets a prefix scan see spilled keys.
 * The cost is one node plus the key bytes per spilled entry, which for
 * megabyte-scale KV-cache values is noise.
 *
 * The LRU list threads only RESIDENT nodes. A spilled node has no RAM left to
 * reclaim, so leaving it in the list would just make eviction walk past it
 * forever.
 */
struct pk_node {
    struct pk_node *next;      /* hash chain */
    struct pk_node *lru_prev;  /* toward lru_head (hotter) */
    struct pk_node *lru_next;  /* toward lru_tail (colder) */
    uint8_t        *val;       /* NULL when spilled, or when vlen == 0 */
    uint64_t        vlen;      /* valid in either tier */
    uint64_t        hash;      /* cached: routing, spill paths, fast chain reject */
    uint64_t        spill_id;  /* tier file id; meaningful only when !resident */
    uint32_t        klen;
    uint8_t         resident;  /* 1 = value in RAM, 0 = value on NVMe */
    uint8_t         key[];     /* klen bytes */
};

uint64_t pk_table_hash(const uint8_t *key, uint32_t klen)
{
    uint64_t h = FNV1A_OFFSET_BASIS;
    for (uint32_t i = 0; i < klen; i++) {
        h ^= (uint64_t)key[i];
        h *= FNV1A_PRIME;
    }
    return h;
}

typedef struct {
    size_t shard;
    size_t bucket;
} pk_route_t;

/*
 * The low ten hash bits still address 1,024 physical buckets. The low eight
 * choose one of 256 lock shards; the remaining two choose one of the four
 * chains inside it. In other words, shard s owns global buckets
 * s, s+256, s+512 and s+768. Unchanged from v1.
 */
static pk_route_t route_of_hash(uint64_t hash)
{
    size_t global_bucket = (size_t)(hash & (uint64_t)(PK_TABLE_BUCKETS - 1u));
    pk_route_t route = {
        .shard  = global_bucket & (PK_TABLE_SHARDS - 1u),
        .bucket = global_bucket / PK_TABLE_SHARDS,
    };
    return route;
}

static size_t bucket_of_hash(uint64_t hash)
{
    return (size_t)(hash & (uint64_t)(PK_TABLE_BUCKETS - 1u)) / PK_TABLE_SHARDS;
}

/*
 * Caller holds shard->lock. prev_out receives the address of the link that
 * points at the node found, so a delete can unlink it without walking the chain
 * twice and without special-casing the head.
 *
 * The cached hash is compared first: it rejects a non-match in one integer
 * compare, before touching the key bytes at all.
 */
static pk_node_t *find(pk_table_shard_t *shard, size_t bucket, uint64_t hash,
                       const uint8_t *key, uint32_t klen,
                       pk_node_t ***prev_out)
{
    pk_node_t **prev = &shard->buckets[bucket];

    for (pk_node_t *n = *prev; n != NULL; prev = &n->next, n = n->next) {
        if (n->hash == hash && n->klen == klen &&
            memcmp(n->key, key, klen) == 0) {
            if (prev_out != NULL)
                *prev_out = prev;
            return n;
        }
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* LRU list. All of these require shard->lock. */

static void lru_unlink(pk_table_shard_t *shard, pk_node_t *n)
{
    if (n->lru_prev != NULL)
        n->lru_prev->lru_next = n->lru_next;
    else if (shard->lru_head == n)
        shard->lru_head = n->lru_next;

    if (n->lru_next != NULL)
        n->lru_next->lru_prev = n->lru_prev;
    else if (shard->lru_tail == n)
        shard->lru_tail = n->lru_prev;

    n->lru_prev = NULL;
    n->lru_next = NULL;
}

static void lru_push_front(pk_table_shard_t *shard, pk_node_t *n)
{
    n->lru_prev = NULL;
    n->lru_next = shard->lru_head;
    if (shard->lru_head != NULL)
        shard->lru_head->lru_prev = n;
    shard->lru_head = n;
    if (shard->lru_tail == NULL)
        shard->lru_tail = n;
}

static void lru_touch(pk_table_shard_t *shard, pk_node_t *n)
{
    if (shard->lru_head == n)
        return;
    lru_unlink(shard, n);
    lru_push_front(shard, n);
}

/* ------------------------------------------------------------------ */
/* eviction. All of these require shard->lock. */

/* Removes a node from the table entirely, from whichever tier it is in. */
static void shard_drop_node(pk_table_t *t, pk_table_shard_t *shard, pk_node_t *n)
{
    pk_node_t **prev = &shard->buckets[bucket_of_hash(n->hash)];
    while (*prev != NULL && *prev != n)
        prev = &(*prev)->next;
    if (*prev == n)
        *prev = n->next;

    if (n->resident) {
        lru_unlink(shard, n);
        shard->ram_bytes -= n->vlen;
        shard->ram_keys--;
        free(n->val);
    } else {
        shard->nvme_bytes -= n->vlen;
        shard->nvme_keys--;
        pk_tier_remove(t->tier, n->hash, n->spill_id);
    }

    atomic_fetch_sub_explicit(&t->count, 1, memory_order_relaxed);
    free(n);
}

/*
 * Moves one resident node's value to the NVMe tier, or -- if there is no tier,
 * or the write fails -- drops it.
 *
 * Dropping on a spill failure is the deliberate choice. This tier is a cache:
 * the design doc's whole point is that a miss costs a recompute, not data loss
 * of record. A full or failing disk therefore degrades the node into a smaller
 * cache, which is survivable, rather than growing RAM past its budget, which is
 * not. The failure is counted in spill_errors so it is visible rather than
 * silent.
 *
 * NOTE: the spill write happens with shard->lock held. That serializes one
 * shard behind a disk write, which is a real contention cost and is measured in
 * docs/pulsekv-v2-phase1-summary.md. Dropping the lock around the I/O means
 * handling a concurrent overwrite or delete of the very node being spilled;
 * that complexity belongs with Phase 6's transport work, not here.
 */
static void evict_one(pk_table_t *t, pk_table_shard_t *shard, pk_node_t *n)
{
    if (t->tier != NULL) {
        uint64_t id = pk_tier_next_id(t->tier);
        if (pk_tier_write(t->tier, n->hash, id, n->key, n->klen,
                          n->val, n->vlen) == 0) {
            free(n->val);
            n->val      = NULL;
            n->resident = 0;
            n->spill_id = id;

            lru_unlink(shard, n);
            shard->ram_bytes -= n->vlen;
            shard->ram_keys--;
            shard->nvme_bytes += n->vlen;
            shard->nvme_keys++;

            atomic_fetch_add_explicit(&t->spills, 1, memory_order_relaxed);
            return;
        }
        atomic_fetch_add_explicit(&t->spill_errors, 1, memory_order_relaxed);
    }

    shard_drop_node(t, shard, n);
    atomic_fetch_add_explicit(&t->evict_drops, 1, memory_order_relaxed);
}

/*
 * Evicts coldest-first until this shard is back inside its share of the RAM
 * budget.
 *
 * `protect` is the entry the caller just wrote or promoted -- the hottest thing
 * in the shard by definition. Skipping it matters because the budget is
 * per-shard: with 256 shards and, say, a 256 MiB budget, one shard gets 1 MiB,
 * so a 4 MiB value would otherwise be spilled to disk by the very insert that
 * created it, and every large write would round-trip through the filesystem for
 * nothing. Holding one over-budget entry is the better of the two wrongs.
 *
 * Consequence worth knowing: a shard can exceed its budget by up to one value.
 * Across 256 shards the ceiling is budget + 256 * max_value_bytes in the worst
 * case. See the summary doc for why the per-shard split is still the right
 * call -- a global byte counter would be a single contended cache line on every
 * write.
 */
static void shard_enforce_budget(pk_table_t *t, pk_table_shard_t *shard,
                                 pk_node_t *protect)
{
    while (shard->ram_bytes > t->shard_budget_bytes) {
        pk_node_t *victim = shard->lru_tail;
        if (victim == NULL)
            break;
        if (victim == protect) {
            victim = victim->lru_prev;
            if (victim == NULL)
                break;  /* the protected entry is all that is left */
        }
        evict_one(t, shard, victim);
    }
}

/* ------------------------------------------------------------------ */
/* lifecycle */

int pk_table_init(pk_table_t *t, pk_tier_t *tier,
                  uint64_t ram_budget_bytes, uint64_t max_value_bytes)
{
    memset(t, 0, sizeof(*t));
    atomic_init(&t->count, 0);
    atomic_init(&t->spills, 0);
    atomic_init(&t->promotions, 0);
    atomic_init(&t->spill_errors, 0);
    atomic_init(&t->evict_drops, 0);

    t->tier               = tier;
    t->max_value_bytes    = max_value_bytes;
    t->shard_budget_bytes = ram_budget_bytes / PK_TABLE_SHARDS;

    for (size_t s = 0; s < PK_TABLE_SHARDS; s++) {
        int rc = pthread_mutex_init(&t->shards[s].lock, NULL);
        if (rc != 0) {
            while (s > 0) {
                s--;
                pthread_mutex_destroy(&t->shards[s].lock);
            }
            return rc;
        }
    }
    return 0;
}

void pk_table_destroy(pk_table_t *t)
{
    for (size_t s = 0; s < PK_TABLE_SHARDS; s++) {
        pk_table_shard_t *shard = &t->shards[s];

        for (size_t b = 0; b < PK_TABLE_BUCKETS_PER_SHARD; b++) {
            pk_node_t *n = shard->buckets[b];
            while (n != NULL) {
                pk_node_t *next = n->next;
                if (n->resident)
                    free(n->val);
                else
                    pk_tier_remove(t->tier, n->hash, n->spill_id);
                free(n);
                n = next;
            }
            shard->buckets[b] = NULL;
        }
        shard->lru_head   = NULL;
        shard->lru_tail   = NULL;
        shard->ram_bytes  = 0;
        shard->nvme_bytes = 0;
        shard->ram_keys   = 0;
        shard->nvme_keys  = 0;
        pthread_mutex_destroy(&shard->lock);
    }
    atomic_store_explicit(&t->count, 0, memory_order_relaxed);
}

/* ------------------------------------------------------------------ */
/* set */

pk_table_result_t pk_table_set(pk_table_t *t, const uint8_t *key, uint32_t klen,
                               const uint8_t *val, uint64_t vlen)
{
    if (key == NULL || klen == 0)
        return PK_TABLE_INVALID;
    if (vlen > 0 && val == NULL)
        return PK_TABLE_INVALID;
    /* Bounds before trust, and before any allocation -- the same discipline
     * v1's protocol.c applies to every decoded frame. */
    if (vlen > t->max_value_bytes)
        return PK_TABLE_TOO_BIG;

    uint8_t *copy = NULL;
    if (vlen > 0) {
        copy = malloc((size_t)vlen);
        if (copy == NULL)
            return PK_TABLE_NOMEM;
        memcpy(copy, val, (size_t)vlen);
    }

    uint64_t   hash  = pk_table_hash(key, klen);
    pk_route_t route = route_of_hash(hash);
    pk_table_shard_t *shard = &t->shards[route.shard];

    pthread_mutex_lock(&shard->lock);

    pk_node_t *n = find(shard, route.bucket, hash, key, klen, NULL);
    if (n != NULL) {
        /* Retire whatever the previous value was, in whichever tier it lived. */
        if (n->resident) {
            free(n->val);
            shard->ram_bytes -= n->vlen;
            shard->ram_keys--;
            lru_unlink(shard, n);
        } else {
            pk_tier_remove(t->tier, n->hash, n->spill_id);
            shard->nvme_bytes -= n->vlen;
            shard->nvme_keys--;
            n->spill_id = 0;
        }

        n->val      = copy;
        n->vlen     = vlen;
        n->resident = 1;
        shard->ram_bytes += vlen;
        shard->ram_keys++;
        lru_push_front(shard, n);

        shard_enforce_budget(t, shard, n);
        pthread_mutex_unlock(&shard->lock);
        return PK_TABLE_OK;
    }

    pk_node_t *fresh = malloc(sizeof(*fresh) + klen);
    if (fresh == NULL) {
        pthread_mutex_unlock(&shard->lock);
        free(copy);
        return PK_TABLE_NOMEM;
    }

    memcpy(fresh->key, key, klen);
    fresh->klen     = klen;
    fresh->hash     = hash;
    fresh->val      = copy;
    fresh->vlen     = vlen;
    fresh->resident = 1;
    fresh->spill_id = 0;
    fresh->lru_prev = NULL;
    fresh->lru_next = NULL;

    fresh->next                  = shard->buckets[route.bucket];
    shard->buckets[route.bucket] = fresh;
    lru_push_front(shard, fresh);
    shard->ram_bytes += vlen;
    shard->ram_keys++;
    atomic_fetch_add_explicit(&t->count, 1, memory_order_relaxed);

    shard_enforce_budget(t, shard, fresh);
    pthread_mutex_unlock(&shard->lock);
    return PK_TABLE_OK;
}

/* ------------------------------------------------------------------ */
/* get / peek */

/* Caller holds the lock. Hands back a fresh copy of a resident value. */
static pk_table_result_t copy_out(const pk_node_t *n,
                                  uint8_t **out_val, uint64_t *out_vlen)
{
    if (n->vlen == 0) {
        *out_val  = NULL;
        *out_vlen = 0;
        return PK_TABLE_OK;
    }
    uint8_t *copy = malloc((size_t)n->vlen);
    if (copy == NULL)
        return PK_TABLE_NOMEM;
    memcpy(copy, n->val, (size_t)n->vlen);
    *out_val  = copy;
    *out_vlen = n->vlen;
    return PK_TABLE_OK;
}

/*
 * Reads a spilled value back. Caller holds the lock.
 *
 * A spilled value that will not read back is unrecoverable -- the only copy is
 * the file. Dropping the index entry rather than leaving it means the next get
 * on that key is an honest miss instead of a repeat of the same failed I/O.
 */
static pk_table_result_t read_spilled(pk_table_t *t, pk_table_shard_t *shard,
                                      pk_node_t *n, uint8_t **out_val,
                                      uint64_t *out_len)
{
    uint8_t *from_disk = NULL;
    uint64_t len       = 0;

    if (pk_tier_read(t->tier, n->hash, n->spill_id, n->key, n->klen,
                     &from_disk, &len) != 0) {
        shard_drop_node(t, shard, n);
        return PK_TABLE_IO_ERROR;
    }
    if (len != n->vlen) {
        /* The file disagrees with the index about its own length. Refuse it
         * rather than hand back something of the wrong size. */
        free(from_disk);
        shard_drop_node(t, shard, n);
        return PK_TABLE_IO_ERROR;
    }

    *out_val = from_disk;
    *out_len = len;
    return PK_TABLE_OK;
}

pk_table_result_t pk_table_get(pk_table_t *t, const uint8_t *key, uint32_t klen,
                               uint8_t **out_val, uint64_t *out_vlen)
{
    if (out_val != NULL)
        *out_val = NULL;
    if (out_vlen != NULL)
        *out_vlen = 0;
    if (key == NULL || klen == 0 || out_val == NULL || out_vlen == NULL)
        return PK_TABLE_INVALID;

    uint64_t   hash  = pk_table_hash(key, klen);
    pk_route_t route = route_of_hash(hash);
    pk_table_shard_t *shard = &t->shards[route.shard];

    pthread_mutex_lock(&shard->lock);

    pk_node_t *n = find(shard, route.bucket, hash, key, klen, NULL);
    if (n == NULL) {
        pthread_mutex_unlock(&shard->lock);
        return PK_TABLE_NOT_FOUND;
    }

    if (n->resident) {
        pk_table_result_t rc = copy_out(n, out_val, out_vlen);
        if (rc == PK_TABLE_OK)
            lru_touch(shard, n);
        pthread_mutex_unlock(&shard->lock);
        return rc;
    }

    /* Spilled: read it, promote it, then hand the caller its own copy. */
    uint8_t *from_disk = NULL;
    uint64_t len       = 0;
    pk_table_result_t rc = read_spilled(t, shard, n, &from_disk, &len);
    if (rc != PK_TABLE_OK) {
        pthread_mutex_unlock(&shard->lock);
        return rc;
    }

    uint8_t *caller_copy = NULL;
    if (len > 0) {
        caller_copy = malloc((size_t)len);
        if (caller_copy == NULL) {
            free(from_disk);
            pthread_mutex_unlock(&shard->lock);
            return PK_TABLE_NOMEM;
        }
        memcpy(caller_copy, from_disk, (size_t)len);
    }

    n->val      = from_disk;
    n->resident = 1;
    shard->nvme_bytes -= n->vlen;
    shard->nvme_keys--;
    shard->ram_bytes += n->vlen;
    shard->ram_keys++;
    lru_push_front(shard, n);

    /* The file is redundant now that the value is back in RAM. */
    pk_tier_remove(t->tier, n->hash, n->spill_id);
    n->spill_id = 0;
    atomic_fetch_add_explicit(&t->promotions, 1, memory_order_relaxed);

    /* Promoting may have pushed this shard over budget; make room by spilling
     * something colder, never the entry we just promoted. */
    shard_enforce_budget(t, shard, n);

    pthread_mutex_unlock(&shard->lock);
    *out_val  = caller_copy;
    *out_vlen = len;
    return PK_TABLE_OK;
}

pk_table_result_t pk_table_peek(pk_table_t *t, const uint8_t *key, uint32_t klen,
                                uint8_t **out_val, uint64_t *out_vlen)
{
    if (out_val != NULL)
        *out_val = NULL;
    if (out_vlen != NULL)
        *out_vlen = 0;
    if (key == NULL || klen == 0 || out_val == NULL || out_vlen == NULL)
        return PK_TABLE_INVALID;

    uint64_t   hash  = pk_table_hash(key, klen);
    pk_route_t route = route_of_hash(hash);
    pk_table_shard_t *shard = &t->shards[route.shard];

    pthread_mutex_lock(&shard->lock);

    pk_node_t *n = find(shard, route.bucket, hash, key, klen, NULL);
    if (n == NULL) {
        pthread_mutex_unlock(&shard->lock);
        return PK_TABLE_NOT_FOUND;
    }

    pk_table_result_t rc;
    if (n->resident) {
        /* Deliberately no lru_touch: a scan must not reorder the cache. */
        rc = copy_out(n, out_val, out_vlen);
    } else {
        rc = read_spilled(t, shard, n, out_val, out_vlen);
    }

    pthread_mutex_unlock(&shard->lock);
    return rc;
}

/* ------------------------------------------------------------------ */
/* delete */

pk_table_result_t pk_table_del(pk_table_t *t, const uint8_t *key, uint32_t klen)
{
    if (key == NULL || klen == 0)
        return PK_TABLE_INVALID;

    uint64_t   hash  = pk_table_hash(key, klen);
    pk_route_t route = route_of_hash(hash);
    pk_table_shard_t *shard = &t->shards[route.shard];

    pthread_mutex_lock(&shard->lock);

    pk_node_t *n = find(shard, route.bucket, hash, key, klen, NULL);
    if (n == NULL) {
        pthread_mutex_unlock(&shard->lock);
        return PK_TABLE_NOT_FOUND;
    }
    shard_drop_node(t, shard, n);

    pthread_mutex_unlock(&shard->lock);
    return PK_TABLE_OK;
}

/* ------------------------------------------------------------------ */
/* scan */

pk_table_result_t pk_table_scan_prefix(pk_table_t *t,
                                       const uint8_t *prefix, uint32_t prefix_len,
                                       uint8_t ***out_keys, uint32_t **out_klens,
                                       size_t *out_count)
{
    if (out_keys == NULL || out_klens == NULL || out_count == NULL)
        return PK_TABLE_INVALID;
    if (prefix_len > 0 && prefix == NULL)
        return PK_TABLE_INVALID;

    *out_keys  = NULL;
    *out_klens = NULL;
    *out_count = 0;

    uint8_t **keys  = NULL;
    uint32_t *klens = NULL;
    size_t    count = 0;
    size_t    cap   = 0;

    for (size_t s = 0; s < PK_TABLE_SHARDS; s++) {
        pk_table_shard_t *shard = &t->shards[s];
        pthread_mutex_lock(&shard->lock);

        for (size_t b = 0; b < PK_TABLE_BUCKETS_PER_SHARD; b++) {
            for (pk_node_t *n = shard->buckets[b]; n != NULL; n = n->next) {
                if (n->klen < prefix_len)
                    continue;
                if (prefix_len > 0 && memcmp(n->key, prefix, prefix_len) != 0)
                    continue;

                if (count == cap) {
                    size_t new_cap = (cap == 0) ? 32 : cap * 2;
                    uint8_t **nk = realloc(keys, new_cap * sizeof(*nk));
                    if (nk == NULL)
                        goto oom;
                    keys = nk;
                    uint32_t *nl = realloc(klens, new_cap * sizeof(*nl));
                    if (nl == NULL)
                        goto oom;
                    klens = nl;
                    cap = new_cap;
                }

                uint8_t *k = malloc(n->klen);
                if (k == NULL)
                    goto oom;
                memcpy(k, n->key, n->klen);
                keys[count]  = k;
                klens[count] = n->klen;
                count++;
            }
        }
        pthread_mutex_unlock(&shard->lock);
        continue;

    oom:
        pthread_mutex_unlock(&shard->lock);
        pk_table_free_keys(keys, klens, count);
        return PK_TABLE_NOMEM;
    }

    *out_keys  = keys;
    *out_klens = klens;
    *out_count = count;
    return PK_TABLE_OK;
}

void pk_table_free_keys(uint8_t **keys, uint32_t *klens, size_t count)
{
    if (keys != NULL) {
        for (size_t i = 0; i < count; i++)
            free(keys[i]);
        free(keys);
    }
    free(klens);
}

/* ------------------------------------------------------------------ */
/* stats */

void pk_table_stats(pk_table_t *t, pk_table_stats_t *out)
{
    memset(out, 0, sizeof(*out));

    for (size_t s = 0; s < PK_TABLE_SHARDS; s++) {
        pk_table_shard_t *shard = &t->shards[s];
        pthread_mutex_lock(&shard->lock);
        out->ram_bytes  += shard->ram_bytes;
        out->nvme_bytes += shard->nvme_bytes;
        out->ram_keys   += shard->ram_keys;
        out->nvme_keys  += shard->nvme_keys;
        pthread_mutex_unlock(&shard->lock);
    }

    out->total_keys   = out->ram_keys + out->nvme_keys;
    out->spills       = atomic_load_explicit(&t->spills, memory_order_relaxed);
    out->promotions   = atomic_load_explicit(&t->promotions, memory_order_relaxed);
    out->spill_errors = atomic_load_explicit(&t->spill_errors, memory_order_relaxed);
    out->evict_drops  = atomic_load_explicit(&t->evict_drops, memory_order_relaxed);
}

size_t pk_table_count(pk_table_t *t)
{
    return atomic_load_explicit(&t->count, memory_order_relaxed);
}
