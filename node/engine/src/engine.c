/*
 * The extern "C" surface. Everything here is glue: it owns the lifetime of the
 * table and the tier, translates internal result codes to public ones, and
 * nothing else. Any decision worth making belongs in hashtable.c or tiering.c.
 */

#include "pulsekv_engine.h"

#include <stdlib.h>
#include <string.h>

#include "hashtable.h"
#include "tiering.h"

struct pk_engine {
    pk_table_t table;
    pk_tier_t *tier;
    uint64_t   ram_budget_bytes;
    uint64_t   max_value_bytes;
};

const char *pk_engine_strerror(pk_engine_result_t r)
{
    switch (r) {
    case PK_ENGINE_OK:        return "ok";
    case PK_ENGINE_NOT_FOUND: return "not found";
    case PK_ENGINE_TOO_LARGE: return "value exceeds max_value_bytes";
    case PK_ENGINE_INVALID:   return "invalid argument";
    case PK_ENGINE_IO_ERROR:  return "nvme tier I/O error";
    case PK_ENGINE_NOMEM:     return "out of memory";
    }
    return "unknown engine result";
}

static pk_engine_result_t translate(pk_table_result_t r)
{
    switch (r) {
    case PK_TABLE_OK:        return PK_ENGINE_OK;
    case PK_TABLE_NOT_FOUND: return PK_ENGINE_NOT_FOUND;
    case PK_TABLE_NOMEM:     return PK_ENGINE_NOMEM;
    case PK_TABLE_TOO_BIG:   return PK_ENGINE_TOO_LARGE;
    case PK_TABLE_INVALID:   return PK_ENGINE_INVALID;
    case PK_TABLE_IO_ERROR:  return PK_ENGINE_IO_ERROR;
    }
    return PK_ENGINE_INVALID;
}

pk_engine_t *pk_engine_create(const pk_engine_config_t *cfg)
{
    if (cfg == NULL)
        return NULL;

    uint64_t ram_budget = cfg->ram_budget_bytes ? cfg->ram_budget_bytes
                                                : PK_ENGINE_DEFAULT_RAM_BUDGET_BYTES;
    uint64_t max_value  = cfg->max_value_bytes ? cfg->max_value_bytes
                                               : PK_ENGINE_DEFAULT_MAX_VALUE_BYTES;

    pk_engine_t *e = calloc(1, sizeof(*e));
    if (e == NULL)
        return NULL;

    e->ram_budget_bytes = ram_budget;
    e->max_value_bytes  = max_value;

    /*
     * A data_dir that was asked for but cannot be opened is a hard failure:
     * silently continuing as a RAM-only cache would give a node a fraction of
     * its configured capacity while reporting success, which is exactly the
     * kind of quiet degradation that gets found in production instead of at
     * startup. A data_dir that was not asked for is a legitimate RAM-only
     * configuration.
     */
    int tier_failed = 0;
    e->tier = pk_tier_open(cfg->data_dir, &tier_failed);
    if (tier_failed) {
        free(e);
        return NULL;
    }

    if (pk_table_init(&e->table, e->tier, ram_budget, max_value) != 0) {
        pk_tier_close(e->tier);
        free(e);
        return NULL;
    }
    return e;
}

void pk_engine_destroy(pk_engine_t *e)
{
    if (e == NULL)
        return;
    /* Table first: it unlinks the spill file behind every spilled entry, so
     * the tier's own purge afterwards has little left to do. */
    pk_table_destroy(&e->table);
    pk_tier_close(e->tier);
    free(e);
}

uint64_t pk_engine_max_value_bytes(const pk_engine_t *e)
{
    return (e == NULL) ? 0 : e->max_value_bytes;
}

uint64_t pk_engine_ram_budget_bytes(const pk_engine_t *e)
{
    return (e == NULL) ? 0 : e->ram_budget_bytes;
}

pk_engine_result_t pk_engine_put(pk_engine_t *e,
                                 const uint8_t *key, uint32_t key_len,
                                 const uint8_t *val, uint64_t val_len)
{
    if (e == NULL)
        return PK_ENGINE_INVALID;
    return translate(pk_table_set(&e->table, key, key_len, val, val_len));
}

pk_engine_result_t pk_engine_get(pk_engine_t *e,
                                 const uint8_t *key, uint32_t key_len,
                                 uint8_t **out_val, uint64_t *out_len)
{
    if (e == NULL)
        return PK_ENGINE_INVALID;
    return translate(pk_table_get(&e->table, key, key_len, out_val, out_len));
}

pk_engine_result_t pk_engine_peek(pk_engine_t *e,
                                  const uint8_t *key, uint32_t key_len,
                                  uint8_t **out_val, uint64_t *out_len)
{
    if (e == NULL)
        return PK_ENGINE_INVALID;
    return translate(pk_table_peek(&e->table, key, key_len, out_val, out_len));
}

void pk_engine_free_value(uint8_t *val)
{
    free(val);
}

pk_engine_result_t pk_engine_scan_prefix(pk_engine_t *e,
                                         const uint8_t *prefix, uint32_t prefix_len,
                                         pk_engine_keyset_t *out)
{
    if (e == NULL || out == NULL)
        return PK_ENGINE_INVALID;

    out->keys  = NULL;
    out->count = 0;

    uint8_t **keys  = NULL;
    uint32_t *klens = NULL;
    size_t    count = 0;

    pk_table_result_t rc =
        pk_table_scan_prefix(&e->table, prefix, prefix_len, &keys, &klens, &count);
    if (rc != PK_TABLE_OK)
        return translate(rc);

    if (count == 0) {
        pk_table_free_keys(keys, klens, count);
        return PK_ENGINE_OK;
    }

    /* Repack the table's two parallel arrays into the one array of pairs the
     * public API exposes, so callers do not have to keep them in step. */
    pk_engine_key_t *packed = malloc(count * sizeof(*packed));
    if (packed == NULL) {
        pk_table_free_keys(keys, klens, count);
        return PK_ENGINE_NOMEM;
    }
    for (size_t i = 0; i < count; i++) {
        packed[i].key     = keys[i];  /* ownership moves to the keyset */
        packed[i].key_len = klens[i];
    }

    /* The key buffers now belong to `packed`, so release only the spines. */
    free(keys);
    free(klens);

    out->keys  = packed;
    out->count = count;
    return PK_ENGINE_OK;
}

void pk_engine_free_keyset(pk_engine_keyset_t *ks)
{
    if (ks == NULL || ks->keys == NULL) {
        if (ks != NULL) {
            ks->keys  = NULL;
            ks->count = 0;
        }
        return;
    }
    for (size_t i = 0; i < ks->count; i++)
        free(ks->keys[i].key);
    free(ks->keys);
    ks->keys  = NULL;
    ks->count = 0;
}

void pk_engine_capacity(const pk_engine_t *e, pk_engine_capacity_t *out)
{
    if (out == NULL)
        return;
    memset(out, 0, sizeof(*out));
    if (e == NULL)
        return;

    pk_table_stats_t st;
    /* pk_table_stats takes shard locks, so it needs a non-const table. The
     * engine is logically const here -- reading occupancy changes nothing --
     * and this cast keeps that promise visible in the public signature. */
    pk_table_stats(&((pk_engine_t *)e)->table, &st);

    out->resident_keys      = st.total_keys;
    out->bytes_in_ram_tier  = st.ram_bytes;
    out->bytes_in_nvme_tier = st.nvme_bytes;

    out->keys_in_ram_tier  = st.ram_keys;
    out->keys_in_nvme_tier = st.nvme_keys;
    out->spills            = st.spills;
    out->promotions        = st.promotions;
    out->spill_errors      = st.spill_errors;
    out->evict_drops       = st.evict_drops;
}
