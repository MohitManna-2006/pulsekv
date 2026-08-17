#ifndef PULSEKV_ENGINE_TEST_UTIL_H
#define PULSEKV_ENGINE_TEST_UTIL_H

/*
 * Shared scaffolding for the engine tests, in the same shape as v1's
 * tests/test_hashtable.c: a counted `ok()` per assertion, a printed line per
 * check so a failure says which case broke, and a summary that decides the exit
 * code. No framework.
 */

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int g_checks = 0;
static int g_failed = 0;

__attribute__((format(printf, 2, 3)))
static void ok(bool cond, const char *fmt, ...)
{
    va_list ap;
    g_checks++;
    printf(cond ? "  ok    " : "  FAIL  ");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    if (!cond)
        g_failed++;
    fflush(stdout);
}

static void section(const char *name)
{
    printf("\n%s\n", name);
}

static int test_summary(const char *suite)
{
    printf("\n%s: %d check(s), %d failed\n", suite, g_checks, g_failed);
    return g_failed == 0 ? 0 : 1;
}

static const uint8_t *B(const char *s) { return (const uint8_t *)s; }
static uint32_t       L(const char *s) { return (uint32_t)strlen(s); }

/* ------------------------------------------------------------------ */
/* temp data directories */

/* Returns a malloc'd path to a fresh directory, or NULL. */
static char *make_temp_dir(void)
{
    const char *base = getenv("TMPDIR");
    if (base == NULL || base[0] == '\0')
        base = "/tmp";

    size_t need = strlen(base) + sizeof("/pulsekv-engine-XXXXXX");
    char  *path = malloc(need);
    if (path == NULL)
        return NULL;
    snprintf(path, need, "%s/pulsekv-engine-XXXXXX", base);

    if (mkdtemp(path) == NULL) {
        free(path);
        return NULL;
    }
    return path;
}

/*
 * Call only after pk_engine_destroy: the engine's own shutdown purges every
 * spill file and prunes the two directory levels it created, so all that is
 * left here is the two empty directories on top. If either rmdir fails the
 * engine left something behind, which is itself worth knowing -- so this
 * reports rather than ignoring it.
 */
static bool remove_temp_dir(const char *dir)
{
    char spill[4096];
    snprintf(spill, sizeof(spill), "%s/spill", dir);

    bool clean = true;
    if (rmdir(spill) != 0 && access(spill, F_OK) == 0)
        clean = false;
    if (rmdir(dir) != 0 && access(dir, F_OK) == 0)
        clean = false;
    return clean;
}

/* ------------------------------------------------------------------ */
/* deterministic values
 *
 * Values are generated from the key's seed rather than stored, so a test can
 * verify a multi-megabyte round trip byte for byte without holding a second
 * copy of it. xorshift64* because it is a few lines, has no library
 * dependency, and produces bytes that no filesystem or allocator will
 * accidentally reproduce.
 */

static uint64_t xorshift64s(uint64_t *state)
{
    uint64_t x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    return x * 0x2545F4914F6CDD1DULL;
}

static void fill_value(uint8_t *buf, size_t len, uint64_t seed)
{
    uint64_t state = seed | 1u;
    size_t   i     = 0;
    while (i < len) {
        uint64_t r = xorshift64s(&state);
        size_t   n = (len - i < 8) ? len - i : 8;
        memcpy(buf + i, &r, n);
        i += n;
    }
}

static bool value_matches(const uint8_t *buf, size_t len, uint64_t seed)
{
    uint64_t state = seed | 1u;
    size_t   i     = 0;
    while (i < len) {
        uint64_t r = xorshift64s(&state);
        size_t   n = (len - i < 8) ? len - i : 8;
        if (memcmp(buf + i, &r, n) != 0)
            return false;
        i += n;
    }
    return true;
}

/* Suppress unused-function warnings in tests that use only part of this. */
static void test_util_keep_alive(void) __attribute__((unused));
static void test_util_keep_alive(void)
{
    (void)section;
    (void)B;
    (void)L;
    (void)make_temp_dir;
    (void)remove_temp_dir;
    (void)fill_value;
    (void)value_matches;
    (void)test_summary;
}

#endif /* PULSEKV_ENGINE_TEST_UTIL_H */
