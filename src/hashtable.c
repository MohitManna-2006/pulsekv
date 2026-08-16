#include "hashtable.h"

#include <stdlib.h>
#include <string.h>

#define FNV1A_OFFSET_BASIS 0xcbf29ce484222325ULL
#define FNV1A_PRIME        0x00000100000001b3ULL

/*
 * The key rides along in the node's own allocation: it is fixed for the node's
 * lifetime, so there is no reason to chase a second pointer for it. The value
 * is separate because an overwrite replaces it with one of a different size.
 */
struct pk_node {
    struct pk_node *next;
    uint8_t        *val;   /* NULL when vlen == 0 */
    uint32_t        vlen;
    uint32_t        klen;
    uint8_t         key[]; /* klen bytes */
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

static size_t bucket_of(const uint8_t *key, uint32_t klen)
{
    return (size_t)(pk_table_hash(key, klen) & (uint64_t)(PK_TABLE_BUCKETS - 1u));
}

/*
 * Caller holds t->lock. prev_out receives the address of the link that points
 * at the node found, so a delete can unlink it without walking the chain twice
 * and without special-casing the head.
 */
static pk_node_t *find(pk_table_t *t, size_t b, const uint8_t *key, uint32_t klen,
                       pk_node_t ***prev_out)
{
    pk_node_t **prev = &t->buckets[b];

    for (pk_node_t *n = *prev; n != NULL; prev = &n->next, n = n->next) {
        if (n->klen == klen && memcmp(n->key, key, klen) == 0) {
            if (prev_out != NULL)
                *prev_out = prev;
            return n;
        }
    }
    return NULL;
}

int pk_table_init(pk_table_t *t)
{
    memset(t->buckets, 0, sizeof(t->buckets));
    t->count = 0;
    return pthread_mutex_init(&t->lock, NULL);
}

void pk_table_destroy(pk_table_t *t)
{
    for (size_t b = 0; b < PK_TABLE_BUCKETS; b++) {
        pk_node_t *n = t->buckets[b];
        while (n != NULL) {
            pk_node_t *next = n->next;
            free(n->val);
            free(n);
            n = next;
        }
        t->buckets[b] = NULL;
    }
    t->count = 0;
    pthread_mutex_destroy(&t->lock);
}

pk_table_result_t pk_table_set(pk_table_t *t, const uint8_t *key, uint32_t klen,
                               const uint8_t *val, uint32_t vlen)
{
    if (key == NULL || klen == 0)
        return PK_TABLE_INVALID;
    if (vlen > 0 && val == NULL)
        return PK_TABLE_INVALID;

    /*
     * Copy the value before taking the lock, so the allocation happens outside
     * the critical section and a failure here cannot leave a live entry with
     * its old value already freed.
     */
    uint8_t *copy = NULL;
    if (vlen > 0) {
        copy = malloc(vlen);
        if (copy == NULL)
            return PK_TABLE_NOMEM;
        memcpy(copy, val, vlen);
    }

    size_t b = bucket_of(key, klen);
    pthread_mutex_lock(&t->lock);

    pk_node_t *n = find(t, b, key, klen, NULL);
    if (n != NULL) {
        uint8_t *old = n->val;  /* freed after the swap, never before */
        n->val  = copy;
        n->vlen = vlen;
        pthread_mutex_unlock(&t->lock);
        free(old);
        return PK_TABLE_OK;
    }

    pk_node_t *fresh = malloc(sizeof(*fresh) + klen);
    if (fresh == NULL) {
        pthread_mutex_unlock(&t->lock);
        free(copy);
        return PK_TABLE_NOMEM;
    }

    memcpy(fresh->key, key, klen);
    fresh->klen = klen;
    fresh->val  = copy;
    fresh->vlen = vlen;

    fresh->next   = t->buckets[b];  /* prepend: newest first, O(1) */
    t->buckets[b] = fresh;
    t->count++;

    pthread_mutex_unlock(&t->lock);
    return PK_TABLE_OK;
}

pk_table_result_t pk_table_get(pk_table_t *t, const uint8_t *key, uint32_t klen,
                               uint8_t *out_val, size_t out_cap, uint32_t *out_vlen)
{
    if (out_vlen != NULL)
        *out_vlen = 0;
    if (key == NULL || klen == 0)
        return PK_TABLE_INVALID;

    size_t b = bucket_of(key, klen);
    pthread_mutex_lock(&t->lock);

    pk_node_t *n = find(t, b, key, klen, NULL);
    if (n == NULL) {
        pthread_mutex_unlock(&t->lock);
        return PK_TABLE_NOT_FOUND;
    }
    if (n->vlen > out_cap) {
        pthread_mutex_unlock(&t->lock);
        return PK_TABLE_TOO_BIG;
    }

    /* Copied out while still holding the lock. Handing back n->val instead
     * would dangle the moment a DEL freed the node. */
    if (n->vlen > 0)
        memcpy(out_val, n->val, n->vlen);
    if (out_vlen != NULL)
        *out_vlen = n->vlen;

    pthread_mutex_unlock(&t->lock);
    return PK_TABLE_OK;
}

pk_table_result_t pk_table_del(pk_table_t *t, const uint8_t *key, uint32_t klen)
{
    if (key == NULL || klen == 0)
        return PK_TABLE_INVALID;

    size_t b = bucket_of(key, klen);
    pthread_mutex_lock(&t->lock);

    pk_node_t **prev = NULL;
    pk_node_t  *n    = find(t, b, key, klen, &prev);
    if (n == NULL) {
        pthread_mutex_unlock(&t->lock);
        return PK_TABLE_NOT_FOUND;
    }

    *prev = n->next;  /* head or mid-chain, same operation */
    t->count--;

    pthread_mutex_unlock(&t->lock);

    /* Unlinked already, so nothing can reach it and the frees need no lock. */
    free(n->val);
    free(n);
    return PK_TABLE_OK;
}

size_t pk_table_count(pk_table_t *t)
{
    pthread_mutex_lock(&t->lock);
    size_t c = t->count;
    pthread_mutex_unlock(&t->lock);
    return c;
}
