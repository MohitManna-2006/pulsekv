#ifndef PULSEKV_ENGINE_TIERING_H
#define PULSEKV_ENGINE_TIERING_H

#include <stdint.h>

/*
 * The NVMe spill tier: the "warm" level below the in-memory shard table.
 *
 * This file owns the *mechanism* only -- where a spilled value lives on disk,
 * how it gets there atomically, and how it comes back. The *policy* (which
 * entry is coldest, when the budget is exceeded) lives in hashtable.c, which
 * is the only caller. The dependency runs one way, hashtable -> tiering, and
 * nothing here knows the table exists.
 *
 * WHAT THIS IS NOT: a write-ahead log, and not persistence. v2's cache tier is
 * explicitly loss-tolerant -- worst case is a recompute, per
 * docs/pulsekv-v2-distributed-design.md section 4.3. Spill files are unreachable
 * the moment the process exits, because the index that names them lives only in
 * RAM, so pk_tier_open() purges the whole tree at startup. That also cleans up
 * after a kill -9.
 *
 * The one guarantee that does matter: a read never returns bytes that are not
 * exactly what was written for exactly that key. Losing a spilled value on
 * crash is fine; returning a truncated or mismatched one is not. Hence the
 * write-to-temp-then-rename() and the self-describing file header below.
 */

typedef struct pk_tier pk_tier_t;

/*
 * Opens <data_dir>/spill/ as the tier root, creating it if needed, and purges
 * anything already in it.
 *
 * data_dir == NULL or "" returns NULL, which is a valid "tiering disabled"
 * tier -- every function below tolerates a NULL tier. With tiering disabled the
 * engine is a plain bounded RAM cache: eviction drops entries instead of
 * spilling them. Tests use that; nodes do not.
 *
 * Returns NULL on failure too; use pk_tier_open_failed() to tell the cases
 * apart, since NULL alone is ambiguous.
 */
pk_tier_t *pk_tier_open(const char *data_dir, int *out_failed);

/* Purges the tree and frees the tier. Tolerates NULL. */
void pk_tier_close(pk_tier_t *t);

/* Monotonic, thread-safe. Every spilled value gets its own id, so a filename is
 * unique by construction -- two distinct keys that collide on `hash` still get
 * distinct files, and an overwrite never races with its own predecessor. */
uint64_t pk_tier_next_id(pk_tier_t *t);

/*
 * Writes one value. Returns 0 on success, -1 on any failure (caller treats a
 * failure as "this entry could not be spilled", never as data loss it must
 * hide).
 *
 * Atomic: written to a unique temp path in the destination directory, then
 * rename()d into place. A crash mid-write leaves a .tmp file that the next
 * pk_tier_open() purges; it can never leave a half-written value under the
 * real name. Deliberately not fsync()ed -- see the loss-tolerance note above.
 */
int pk_tier_write(pk_tier_t *t, uint64_t hash, uint64_t id,
                  const uint8_t *key, uint32_t klen,
                  const uint8_t *val, uint64_t vlen);

/*
 * Reads one value back. On success returns 0 and hands the caller a
 * malloc()'d buffer of exactly the stored length, which the caller frees.
 *
 * Verifies the magic, the exact file size, and the full key stored alongside
 * the value before returning a byte of it. The key check is what makes a
 * 64-bit hash collision harmless rather than a silent wrong answer.
 */
int pk_tier_read(pk_tier_t *t, uint64_t hash, uint64_t id,
                 const uint8_t *key, uint32_t klen,
                 uint8_t **out_val, uint64_t *out_len);

/* Best-effort unlink. A failure here leaks a file until the next purge, which
 * is not worth failing an eviction or a promotion over. */
void pk_tier_remove(pk_tier_t *t, uint64_t hash, uint64_t id);

/* Removes every *.val and *.tmp under the tier root and prunes empty
 * directories. Called at open and at close. */
void pk_tier_purge(pk_tier_t *t);

/* For tests and diagnostics. */
const char *pk_tier_root(const pk_tier_t *t);

#endif /* PULSEKV_ENGINE_TIERING_H */
