#define _POSIX_C_SOURCE 200809L

#include "tiering.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/*
 * On-disk format. Self-describing on purpose: a spill file read back must be
 * provably the value for the key being asked about, or be rejected. The
 * alternative -- trusting the filename -- turns a 64-bit hash collision, a
 * stale file, or a truncated write into a silently wrong answer, which is the
 * one failure mode this tier is not allowed to have.
 *
 *   magic[8]  "PKV1SPL\0"
 *   klen      uint32 little-endian
 *   reserved  uint32 (zero; keeps the key 8-byte aligned in the file)
 *   vlen      uint64 little-endian
 *   key       klen bytes
 *   value     vlen bytes
 *
 * There is no checksum over the value, and that is a considered choice rather
 * than an omission: write() + rename() cannot publish a file that is longer
 * than what was written, so a torn write always shows up as a short file, and
 * the exact-size check below already catches that. A checksum would only add
 * value against bit rot, which is Phase 9 hardening territory and would cost a
 * full pass over every multi-megabyte value on every read.
 */
#define PK_TIER_MAGIC "PKV1SPL"
#define PK_TIER_MAGIC_LEN 8u
#define PK_TIER_HEADER_LEN 24u

#define PK_TIER_PATH_MAX 4096

struct pk_tier {
    char             *root;   /* <data_dir>/spill */
    atomic_uint_least64_t next_id;
};

/* ------------------------------------------------------------------ */
/* little-endian helpers -- the format is fixed regardless of host order */

static void put_u32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v);
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static void put_u64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

static uint32_t get_u32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t get_u64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v |= (uint64_t)p[i] << (8 * i);
    return v;
}

/* ------------------------------------------------------------------ */
/* paths */

/*
 * <root>/<aa>/<bb>/<hash:016x>_<id:016x>.val
 *
 * aa and bb are the top two bytes of the hash. The *low* ten bits already
 * select the table's bucket, so taking from the top keeps directory placement
 * independent of bucket placement. Two levels of 256 gives 65,536 leaf
 * directories, created lazily -- enough that no single directory collects a
 * pathological number of entries, which is the same reason v1 sized its bucket
 * array the way it did.
 */
static int spill_dir(const pk_tier_t *t, uint64_t hash, char *out, size_t cap)
{
    int n = snprintf(out, cap, "%s/%02x/%02x", t->root,
                     (unsigned)((hash >> 56) & 0xffu),
                     (unsigned)((hash >> 48) & 0xffu));
    return (n > 0 && (size_t)n < cap) ? 0 : -1;
}

static int spill_path(const pk_tier_t *t, uint64_t hash, uint64_t id,
                      const char *suffix, char *out, size_t cap)
{
    int n = snprintf(out, cap, "%s/%02x/%02x/%016llx_%016llx%s", t->root,
                     (unsigned)((hash >> 56) & 0xffu),
                     (unsigned)((hash >> 48) & 0xffu),
                     (unsigned long long)hash, (unsigned long long)id, suffix);
    return (n > 0 && (size_t)n < cap) ? 0 : -1;
}

/* mkdir that treats "already there" as success. */
static int mkdir_ok(const char *path)
{
    if (mkdir(path, 0700) == 0)
        return 0;
    return (errno == EEXIST) ? 0 : -1;
}

/* Creates <root>/aa and <root>/aa/bb. The root itself is made at open. */
static int ensure_spill_dir(const pk_tier_t *t, uint64_t hash)
{
    char path[PK_TIER_PATH_MAX];

    int n = snprintf(path, sizeof(path), "%s/%02x", t->root,
                     (unsigned)((hash >> 56) & 0xffu));
    if (n <= 0 || (size_t)n >= sizeof(path))
        return -1;
    if (mkdir_ok(path) != 0)
        return -1;

    if (spill_dir(t, hash, path, sizeof(path)) != 0)
        return -1;
    return mkdir_ok(path);
}

/* ------------------------------------------------------------------ */
/* full-buffer I/O -- write(2) and read(2) are both allowed to do less than
 * asked, and a partial write that nobody noticed is exactly how a cache starts
 * returning truncated values. */

static int write_all(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        p += (size_t)n;
        len -= (size_t)n;
    }
    return 0;
}

static int read_all(int fd, void *buf, size_t len)
{
    uint8_t *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;  /* short file */
        p += (size_t)n;
        len -= (size_t)n;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* open / close / purge */

pk_tier_t *pk_tier_open(const char *data_dir, int *out_failed)
{
    if (out_failed != NULL)
        *out_failed = 0;

    if (data_dir == NULL || data_dir[0] == '\0')
        return NULL;  /* tiering deliberately disabled */

    if (mkdir_ok(data_dir) != 0) {
        if (out_failed != NULL)
            *out_failed = 1;
        return NULL;
    }

    pk_tier_t *t = calloc(1, sizeof(*t));
    if (t == NULL) {
        if (out_failed != NULL)
            *out_failed = 1;
        return NULL;
    }

    size_t need = strlen(data_dir) + sizeof("/spill");
    t->root = malloc(need);
    if (t->root == NULL) {
        free(t);
        if (out_failed != NULL)
            *out_failed = 1;
        return NULL;
    }
    snprintf(t->root, need, "%s/spill", data_dir);

    if (mkdir_ok(t->root) != 0) {
        free(t->root);
        free(t);
        if (out_failed != NULL)
            *out_failed = 1;
        return NULL;
    }

    atomic_init(&t->next_id, 1);

    /* Anything already here is unreachable: the index that names spill files
     * lives only in RAM. Clean it up rather than leaking it forever. */
    pk_tier_purge(t);
    return t;
}

void pk_tier_close(pk_tier_t *t)
{
    if (t == NULL)
        return;
    pk_tier_purge(t);
    free(t->root);
    free(t);
}

const char *pk_tier_root(const pk_tier_t *t)
{
    return (t == NULL) ? NULL : t->root;
}

uint64_t pk_tier_next_id(pk_tier_t *t)
{
    if (t == NULL)
        return 0;
    return atomic_fetch_add_explicit(&t->next_id, 1, memory_order_relaxed);
}

static int is_spill_file(const char *name)
{
    size_t len = strlen(name);
    if (len > 4 && strcmp(name + len - 4, ".val") == 0)
        return 1;
    if (len > 4 && strcmp(name + len - 4, ".tmp") == 0)
        return 1;
    return 0;
}

/*
 * Two levels deep, and only files this tier could have written. Scoped
 * deliberately narrowly: this walks a directory the operator handed us on the
 * command line, so it unlinks only names matching our own layout and never
 * recurses past the depth we create.
 */
void pk_tier_purge(pk_tier_t *t)
{
    if (t == NULL)
        return;

    DIR *l1 = opendir(t->root);
    if (l1 == NULL)
        return;

    struct dirent *e1;
    while ((e1 = readdir(l1)) != NULL) {
        if (e1->d_name[0] == '.')
            continue;

        char p1[PK_TIER_PATH_MAX];
        if (snprintf(p1, sizeof(p1), "%s/%s", t->root, e1->d_name) >= (int)sizeof(p1))
            continue;

        DIR *l2 = opendir(p1);
        if (l2 == NULL)
            continue;

        struct dirent *e2;
        while ((e2 = readdir(l2)) != NULL) {
            if (e2->d_name[0] == '.')
                continue;

            char p2[PK_TIER_PATH_MAX];
            if (snprintf(p2, sizeof(p2), "%s/%s", p1, e2->d_name) >= (int)sizeof(p2))
                continue;

            DIR *l3 = opendir(p2);
            if (l3 == NULL)
                continue;

            struct dirent *e3;
            while ((e3 = readdir(l3)) != NULL) {
                if (e3->d_name[0] == '.' || !is_spill_file(e3->d_name))
                    continue;
                char p3[PK_TIER_PATH_MAX];
                if (snprintf(p3, sizeof(p3), "%s/%s", p2, e3->d_name) < (int)sizeof(p3))
                    unlink(p3);
            }
            closedir(l3);
            rmdir(p2);  /* only succeeds if we emptied it */
        }
        closedir(l2);
        rmdir(p1);
    }
    closedir(l1);
}

/* ------------------------------------------------------------------ */
/* write / read / remove */

int pk_tier_write(pk_tier_t *t, uint64_t hash, uint64_t id,
                  const uint8_t *key, uint32_t klen,
                  const uint8_t *val, uint64_t vlen)
{
    if (t == NULL || key == NULL || klen == 0)
        return -1;
    if (vlen > 0 && val == NULL)
        return -1;

    if (ensure_spill_dir(t, hash) != 0)
        return -1;

    char tmp[PK_TIER_PATH_MAX];
    char final[PK_TIER_PATH_MAX];
    if (spill_path(t, hash, id, ".tmp", tmp, sizeof(tmp)) != 0)
        return -1;
    if (spill_path(t, hash, id, ".val", final, sizeof(final)) != 0)
        return -1;

    /* O_EXCL: the id is unique, so an existing temp path means something is
     * badly wrong and silently overwriting it would hide it. */
    int fd = open(tmp, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0)
        return -1;

    uint8_t header[PK_TIER_HEADER_LEN];
    memset(header, 0, sizeof(header));
    memcpy(header, PK_TIER_MAGIC, PK_TIER_MAGIC_LEN);
    put_u32(header + 8, klen);
    put_u32(header + 12, 0);
    put_u64(header + 16, vlen);

    int rc = write_all(fd, header, sizeof(header));
    if (rc == 0)
        rc = write_all(fd, key, klen);
    if (rc == 0 && vlen > 0)
        rc = write_all(fd, val, (size_t)vlen);

    if (close(fd) != 0)
        rc = -1;

    if (rc != 0) {
        unlink(tmp);
        return -1;
    }

    /* The publish step. Until this succeeds the value is not visible under a
     * name any reader will ever construct. */
    if (rename(tmp, final) != 0) {
        unlink(tmp);
        return -1;
    }
    return 0;
}

int pk_tier_read(pk_tier_t *t, uint64_t hash, uint64_t id,
                 const uint8_t *key, uint32_t klen,
                 uint8_t **out_val, uint64_t *out_len)
{
    if (out_val != NULL)
        *out_val = NULL;
    if (out_len != NULL)
        *out_len = 0;
    if (t == NULL || key == NULL || klen == 0 || out_val == NULL || out_len == NULL)
        return -1;

    char path[PK_TIER_PATH_MAX];
    if (spill_path(t, hash, id, ".val", path, sizeof(path)) != 0)
        return -1;

    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;

    int rc = -1;
    uint8_t *value = NULL;
    uint8_t *stored_key = NULL;

    do {
        uint8_t header[PK_TIER_HEADER_LEN];
        if (read_all(fd, header, sizeof(header)) != 0)
            break;
        if (memcmp(header, PK_TIER_MAGIC, PK_TIER_MAGIC_LEN) != 0)
            break;

        uint32_t file_klen = get_u32(header + 8);
        uint64_t file_vlen = get_u64(header + 16);
        if (file_klen != klen)
            break;

        /* Exact size check. write()+rename() cannot publish a file longer than
         * what was written, so this is what catches a torn write. */
        struct stat st;
        if (fstat(fd, &st) != 0)
            break;
        uint64_t expect = (uint64_t)PK_TIER_HEADER_LEN + file_klen + file_vlen;
        if (st.st_size < 0 || (uint64_t)st.st_size != expect)
            break;

        stored_key = malloc(file_klen);
        if (stored_key == NULL)
            break;
        if (read_all(fd, stored_key, file_klen) != 0)
            break;
        /* The check that makes a hash collision harmless. */
        if (memcmp(stored_key, key, file_klen) != 0)
            break;

        if (file_vlen > 0) {
            value = malloc((size_t)file_vlen);
            if (value == NULL)
                break;
            if (read_all(fd, value, (size_t)file_vlen) != 0)
                break;
        }

        *out_val = value;
        *out_len = file_vlen;
        value = NULL;  /* ownership handed over */
        rc = 0;
    } while (0);

    free(stored_key);
    free(value);
    close(fd);
    return rc;
}

void pk_tier_remove(pk_tier_t *t, uint64_t hash, uint64_t id)
{
    if (t == NULL)
        return;
    char path[PK_TIER_PATH_MAX];
    if (spill_path(t, hash, id, ".val", path, sizeof(path)) == 0)
        unlink(path);
}
