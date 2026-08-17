#define _POSIX_C_SOURCE 200809L

#include "hashtable.h"
#include "protocol.h"
#include "wal.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define BENCH_RECORDS 20000u
#define BENCH_KEYS     1024u

static int g_checks;
static int g_failed;

static void check(bool condition, const char *what)
{
    g_checks++;
    if (condition)
        printf("  ok    %s\n", what);
    else {
        printf("  FAIL  %s\n", what);
        g_failed++;
    }
}

static void section(const char *name)
{
    printf("\n=== %s ===\n", name);
}

static bool write_all(int fd, const uint8_t *data, size_t len)
{
    size_t written = 0;
    while (written < len) {
        ssize_t n = write(fd, data + written, len - written);
        if (n > 0) {
            written += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        return false;
    }
    return true;
}

static int temp_file(char path[], size_t path_size)
{
    const char *pattern = "/tmp/pulsekv-recovery-XXXXXX";
    if (strlen(pattern) + 1 > path_size)
        return -1;
    strcpy(path, pattern);
    return mkstemp(path);
}

static int append_record(int fd, uint64_t sequence, uint8_t opcode,
                         const uint8_t *key, uint32_t key_len,
                         const uint8_t *val, uint32_t val_len,
                         size_t only_bytes)
{
    uint8_t encoded[PK_WAL_MAX_RECORD_LEN];
    pk_wal_record_t record = {
        .opcode   = opcode,
        .sequence = sequence,
        .key_len  = key_len,
        .key      = key,
        .val_len  = val_len,
        .val      = val_len > 0 ? val : NULL,
    };
    int n = pk_wal_record_encode(&record, encoded, sizeof(encoded));
    if (n < 0)
        return -1;
    size_t write_len = only_bytes == 0 || only_bytes > (size_t)n
                     ? (size_t)n : only_bytes;
    return write_all(fd, encoded, write_len) ? n : -1;
}

static uint64_t file_size(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0 && st.st_size >= 0 ? (uint64_t)st.st_size : 0;
}

static int replay_to_table(const pk_wal_record_t *record, void *ctx)
{
    pk_table_t *table = ctx;
    if (record->opcode == PK_OP_SET) {
        pk_table_result_t rc =
            pk_table_set(table, record->key, record->key_len,
                         record->val, record->val_len);
        return rc == PK_TABLE_OK ? 0 : (rc == PK_TABLE_NOMEM ? ENOMEM : EINVAL);
    }
    if (record->opcode == PK_OP_DEL) {
        pk_table_result_t rc = pk_table_del(table, record->key, record->key_len);
        return rc == PK_TABLE_OK || rc == PK_TABLE_NOT_FOUND ? 0 : EINVAL;
    }
    return EINVAL;
}

static bool table_value(pk_table_t *table, const uint8_t *key, uint32_t key_len,
                        const uint8_t *want, uint32_t want_len)
{
    uint8_t value[PK_MAX_VAL_LEN];
    uint32_t value_len = 0;
    return pk_table_get(table, key, key_len, value, sizeof(value), &value_len)
               == PK_TABLE_OK
        && value_len == want_len
        && (want_len == 0 || memcmp(value, want, want_len) == 0);
}

static bool table_missing(pk_table_t *table, const uint8_t *key, uint32_t key_len)
{
    uint8_t value[16];
    uint32_t value_len = 0;
    return pk_table_get(table, key, key_len, value, sizeof(value), &value_len)
           == PK_TABLE_NOT_FOUND;
}

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    bool            done;
    int             error;
} completion_t;

static void writer_completion(pk_wal_request_t *request, int error, void *ctx)
{
    completion_t *completion = ctx;
    pthread_mutex_lock(&completion->lock);
    completion->done = true;
    completion->error = error;
    pthread_cond_broadcast(&completion->cond);
    pthread_mutex_unlock(&completion->lock);
    pk_wal_request_destroy(request);
}

static bool wait_completion(completion_t *completion)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 5;

    pthread_mutex_lock(&completion->lock);
    while (!completion->done) {
        int rc = pthread_cond_timedwait(&completion->cond, &completion->lock,
                                        &deadline);
        if (rc == ETIMEDOUT)
            break;
    }
    bool ok = completion->done && completion->error == 0;
    pthread_mutex_unlock(&completion->lock);
    return ok;
}

static void test_lifecycle_and_sequence_handoff(void)
{
    section("batched replay and sequence handoff");

    char path[64];
    int fd = temp_file(path, sizeof(path));
    check(fd >= 0, "temporary WAL created");
    if (fd < 0)
        return;

    static const uint8_t alpha[] = "alpha";
    static const uint8_t one[] = "one";
    static const uint8_t two[] = "two";
    static const uint8_t binary_key[] = {'b', 0, 'k'};
    static const uint8_t binary_val[] = {0, 0xff, 7};
    static const uint8_t gone[] = "gone";

    bool built = append_record(fd, 1, PK_OP_SET, alpha, 5, one, 3, 0) > 0
              && append_record(fd, 2, PK_OP_SET, alpha, 5, two, 3, 0) > 0
              && append_record(fd, 3, PK_OP_SET, binary_key, sizeof(binary_key),
                               binary_val, sizeof(binary_val), 0) > 0
              && append_record(fd, 4, PK_OP_SET, gone, 4, one, 3, 0) > 0
              && append_record(fd, 5, PK_OP_DEL, gone, 4, NULL, 0, 0) > 0;
    close(fd);
    check(built, "SET/overwrite/binary/DEL history encoded");

    pk_table_t table;
    int rc = pk_table_init(&table);
    check(rc == 0, "fresh recovery table initialized");
    pk_wal_recovery_stats_t stats;
    rc = pk_wal_recover(path, 17, replay_to_table, &table, &stats);
    check(rc == 0 && stats.records == 5 && stats.last_sequence == 5,
          "five records replay across deliberately tiny read boundaries");
    check(stats.repair == PK_WAL_REPAIR_NONE
          && stats.valid_bytes == stats.original_bytes,
          "clean log needs no repair");
    check(table_value(&table, alpha, 5, two, 3),
          "later SET overwrites the earlier recovered value");
    check(table_value(&table, binary_key, sizeof(binary_key),
                      binary_val, sizeof(binary_val)),
          "binary key and value survive replay");
    check(table_missing(&table, gone, 4) && pk_table_count(&table) == 2,
          "DEL replays and final key count is correct");
    pk_table_destroy(&table);

    pk_wal_t *wal = NULL;
    rc = pk_wal_init(&wal, path, 8, 1000, stats.last_sequence + 1);
    check(rc == 0, "writer starts at recovered sequence + 1");
    if (rc == 0) {
        static const uint8_t continued[] = "continued";
        pk_wal_request_t *request =
            pk_wal_request_create(PK_OP_SET, continued, sizeof(continued) - 1,
                                  one, 3, NULL);
        completion_t completion = {
            .lock = PTHREAD_MUTEX_INITIALIZER,
            .cond = PTHREAD_COND_INITIALIZER,
        };
        rc = request == NULL ? ENOMEM
                             : pk_wal_submit(wal, request, writer_completion,
                                             &completion);
        check(rc == 0 && wait_completion(&completion),
              "first post-recovery mutation becomes durable");
        rc = pk_wal_stop(wal);
        check(rc == 0, "continued writer stops cleanly");
        pk_wal_destroy(wal);
        pthread_cond_destroy(&completion.cond);
        pthread_mutex_destroy(&completion.lock);

        pk_table_t verify;
        pk_table_init(&verify);
        rc = pk_wal_recover(path, 4096, replay_to_table, &verify, &stats);
        check(rc == 0 && stats.records == 6 && stats.last_sequence == 6,
              "rescan confirms sequence 6 followed recovered sequence 5");
        check(table_value(&verify, continued, sizeof(continued) - 1, one, 3),
              "post-recovery mutation is replayable too");
        pk_table_destroy(&verify);
    }

    unlink(path);
}

static void test_truncated_tail(void)
{
    section("crash-truncated tail repair");

    char path[64];
    int fd = temp_file(path, sizeof(path));
    if (fd < 0) {
        check(false, "temporary truncated WAL created");
        return;
    }
    static const uint8_t key1[] = "safe-1";
    static const uint8_t key2[] = "safe-2";
    static const uint8_t key3[] = "partial";
    static const uint8_t value[] = "value";
    int n1 = append_record(fd, 1, PK_OP_SET, key1, 6, value, 5, 0);
    int n2 = append_record(fd, 2, PK_OP_SET, key2, 6, value, 5, 0);
    int n3 = append_record(fd, 3, PK_OP_SET, key3, 7, value, 5,
                           PK_WAL_HEADER_LEN + 2);
    close(fd);
    check(n1 > 0 && n2 > 0 && n3 > 0, "two records plus half-written tail created");
    uint64_t original = file_size(path);

    pk_table_t table;
    pk_table_init(&table);
    pk_wal_recovery_stats_t stats;
    int rc = pk_wal_recover(path, 31, replay_to_table, &table, &stats);
    uint64_t valid = (uint64_t)n1 + (uint64_t)n2;
    check(rc == 0 && stats.records == 2
          && stats.repair == PK_WAL_REPAIR_TRUNCATED,
          "recovery stops at the incomplete record");
    check(stats.original_bytes == original && stats.valid_bytes == valid
          && stats.discarded_bytes == original - valid,
          "repair reports the exact discarded byte count");
    check(file_size(path) == valid, "physical file truncates to last valid boundary");
    check(table_value(&table, key1, 6, value, 5)
          && table_value(&table, key2, 6, value, 5)
          && table_missing(&table, key3, 7),
          "only fully durable records reach the table");
    pk_table_destroy(&table);

    pk_table_init(&table);
    rc = pk_wal_recover(path, 4096, replay_to_table, &table, &stats);
    check(rc == 0 && stats.records == 2 && stats.repair == PK_WAL_REPAIR_NONE,
          "a second startup sees the repaired log as clean");
    pk_table_destroy(&table);
    unlink(path);
}

static void test_corruption_and_sequence(void)
{
    section("corrupt CRC and invalid sequence repair");

    static const uint8_t value[] = "v";
    static const uint8_t keys[][4] = {"one", "two", "bad"};
    char path[64];
    int fd = temp_file(path, sizeof(path));
    if (fd < 0) {
        check(false, "temporary corrupt WAL created");
        return;
    }
    int n1 = append_record(fd, 1, PK_OP_SET, keys[0], 3, value, 1, 0);
    int n2 = append_record(fd, 2, PK_OP_SET, keys[1], 3, value, 1, 0);
    int n3 = append_record(fd, 3, PK_OP_SET, keys[2], 3, value, 1, 0);
    off_t corrupt_at = (off_t)n1 + n2 + PK_WAL_HEADER_LEN;
    uint8_t byte;
    pread(fd, &byte, 1, corrupt_at);
    byte ^= 0x40;
    pwrite(fd, &byte, 1, corrupt_at);
    close(fd);

    pk_table_t table;
    pk_table_init(&table);
    pk_wal_recovery_stats_t stats;
    int rc = pk_wal_recover(path, 4096, replay_to_table, &table, &stats);
    check(n1 > 0 && n2 > 0 && n3 > 0 && rc == 0
          && stats.records == 2 && stats.repair == PK_WAL_REPAIR_CORRUPT,
          "CRC corruption discards the damaged record");
    check(table_value(&table, keys[0], 3, value, 1)
          && table_value(&table, keys[1], 3, value, 1)
          && table_missing(&table, keys[2], 3),
          "records before corruption remain recoverable");
    pk_table_destroy(&table);
    unlink(path);

    fd = temp_file(path, sizeof(path));
    if (fd < 0) {
        check(false, "temporary sequence WAL created");
        return;
    }
    n1 = append_record(fd, 1, PK_OP_SET, keys[0], 3, value, 1, 0);
    n2 = append_record(fd, 3, PK_OP_SET, keys[1], 3, value, 1, 0);
    close(fd);
    pk_table_init(&table);
    rc = pk_wal_recover(path, 4096, replay_to_table, &table, &stats);
    check(n1 > 0 && n2 > 0 && rc == 0 && stats.records == 1
          && stats.last_sequence == 1
          && stats.repair == PK_WAL_REPAIR_SEQUENCE,
          "sequence gap is treated as an invalid tail");
    check(file_size(path) == (uint64_t)n1,
          "sequence repair truncates before the out-of-order record");
    pk_table_destroy(&table);
    unlink(path);
}

static void test_non_regular_path(void)
{
    section("recovery path safety");

    pk_wal_recovery_stats_t stats;
    int rc = pk_wal_recover("/dev/null", 4096, NULL, NULL, &stats);
    check(rc == EINVAL,
          "recovery rejects devices instead of treating them as WAL files");
}

static uint32_t header_total(const uint8_t header[PK_WAL_HEADER_LEN])
{
    return ((uint32_t)header[8] << 24) | ((uint32_t)header[9] << 16)
         | ((uint32_t)header[10] << 8) | (uint32_t)header[11];
}

static int read_exact_counted(int fd, uint8_t *buf, size_t len,
                              uint64_t *read_calls, bool allow_clean_eof)
{
    size_t have = 0;
    while (have < len) {
        ssize_t n = read(fd, buf + have, len - have);
        (*read_calls)++;
        if (n > 0) {
            have += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        if (n == 0 && allow_clean_eof && have == 0)
            return 1;
        return -1;
    }
    return 0;
}

static int naive_recover(const char *path, pk_wal_replay_fn replay, void *ctx,
                         uint64_t *records_out, uint64_t *read_calls_out)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno;
    uint8_t record_buf[PK_WAL_MAX_RECORD_LEN];
    uint64_t records = 0;
    uint64_t calls = 0;
    int error = 0;

    for (;;) {
        int rc = read_exact_counted(fd, record_buf, PK_WAL_HEADER_LEN,
                                    &calls, true);
        if (rc == 1)
            break;
        if (rc < 0) {
            error = EIO;
            break;
        }
        uint32_t total = header_total(record_buf);
        if (total < PK_WAL_MIN_RECORD_LEN || total > PK_WAL_MAX_RECORD_LEN) {
            error = EINVAL;
            break;
        }
        rc = read_exact_counted(fd, record_buf + PK_WAL_HEADER_LEN,
                                total - PK_WAL_HEADER_LEN, &calls, false);
        if (rc != 0) {
            error = EIO;
            break;
        }
        pk_wal_record_t record;
        size_t consumed = 0;
        if (pk_wal_record_decode(record_buf, total, &record, &consumed)
                != PK_WAL_DECODE_OK || consumed != total
            || record.sequence != records + 1) {
            error = EINVAL;
            break;
        }
        if (replay != NULL && (error = replay(&record, ctx)) != 0)
            break;
        records++;
    }
    close(fd);
    *records_out = records;
    *read_calls_out = calls;
    return error;
}

static double milliseconds(struct timespec start, struct timespec end)
{
    return (double)(end.tv_sec - start.tv_sec) * 1000.0
         + (double)(end.tv_nsec - start.tv_nsec) / 1000000.0;
}

static void test_batched_vs_naive(void)
{
    section("batched recovery versus record-at-a-time baseline");

    char path[64];
    int fd = temp_file(path, sizeof(path));
    if (fd < 0) {
        check(false, "benchmark WAL created");
        return;
    }
    bool built = true;
    for (uint64_t i = 1; i <= BENCH_RECORDS; i++) {
        char key[32];
        char value[48];
        int key_len = snprintf(key, sizeof(key), "bench-%04llu",
                               (unsigned long long)((i - 1) % BENCH_KEYS));
        int value_len = snprintf(value, sizeof(value), "value-%08llu",
                                 (unsigned long long)i);
        if (append_record(fd, i, PK_OP_SET, (const uint8_t *)key,
                          (uint32_t)key_len, (const uint8_t *)value,
                          (uint32_t)value_len, 0) < 0) {
            built = false;
            break;
        }
    }
    close(fd);
    check(built, "20,000-record benchmark log encoded");

    pk_table_t batched_table;
    pk_table_t naive_table;
    pk_table_init(&batched_table);
    pk_table_init(&naive_table);
    struct timespec start;
    struct timespec end;

    pk_wal_recovery_stats_t batched;
    clock_gettime(CLOCK_MONOTONIC, &start);
    int batched_rc = pk_wal_recover(path, PK_WAL_DEFAULT_RECOVERY_CHUNK,
                                    replay_to_table, &batched_table, &batched);
    clock_gettime(CLOCK_MONOTONIC, &end);
    double batched_ms = milliseconds(start, end);

    uint64_t naive_records = 0;
    uint64_t naive_calls = 0;
    clock_gettime(CLOCK_MONOTONIC, &start);
    int naive_rc = naive_recover(path, replay_to_table, &naive_table,
                                 &naive_records, &naive_calls);
    clock_gettime(CLOCK_MONOTONIC, &end);
    double naive_ms = milliseconds(start, end);

    check(batched_rc == 0 && naive_rc == 0
          && batched.records == BENCH_RECORDS
          && naive_records == BENCH_RECORDS,
          "both recovery strategies replay every record");
    check(pk_table_count(&batched_table) == BENCH_KEYS
          && pk_table_count(&naive_table) == BENCH_KEYS,
          "both strategies reconstruct the same final key count");
    check(batched.read_calls * 100u <= naive_calls * 40u,
          "batched scanner cuts read syscalls by at least 60 percent");

    printf("  benchmark: batched %.3f ms / %llu reads; naive %.3f ms / %llu reads\n",
           batched_ms, (unsigned long long)batched.read_calls,
           naive_ms, (unsigned long long)naive_calls);

    pk_table_destroy(&batched_table);
    pk_table_destroy(&naive_table);
    unlink(path);
}

int main(void)
{
    test_lifecycle_and_sequence_handoff();
    test_truncated_tail();
    test_corruption_and_sequence();
    test_non_regular_path();
    test_batched_vs_naive();

    printf("\nRESULT: %s (%d checks, %d failed)\n",
           g_failed == 0 ? "PASS" : "FAIL", g_checks, g_failed);
    return g_failed == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
