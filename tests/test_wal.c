#define _POSIX_C_SOURCE 200809L

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

#define PRODUCERS       8
#define PER_PRODUCER   32
#define TOTAL_REQUESTS (PRODUCERS * PER_PRODUCER)

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

static void test_record_codec(void)
{
    section("versioned record codec");

    static const uint8_t key[] = {'b', 'i', 'n', 0, 'k', 'e', 'y'};
    static const uint8_t val[] = {0, 1, 2, 0xfe, 0xff};
    pk_wal_record_t source = {
        .opcode   = PK_OP_SET,
        .sequence = 0x0102030405060708ULL,
        .key_len  = sizeof(key),
        .key      = key,
        .val_len  = sizeof(val),
        .val      = val,
    };
    uint8_t encoded[PK_WAL_MAX_RECORD_LEN + 3];
    memset(encoded, 0xa5, sizeof(encoded));

    int n = pk_wal_record_encode(&source, encoded, sizeof(encoded));
    size_t want = PK_WAL_MIN_RECORD_LEN + sizeof(key) + sizeof(val);
    check(n == (int)want, "encoded size includes header, payload, and CRC");
    check(encoded[0] == 'P' && encoded[1] == 'K'
          && encoded[2] == 'W' && encoded[3] == 'L',
          "record starts with the PKWL magic");

    pk_wal_record_t decoded;
    size_t consumed = 0;
    pk_wal_decode_result_t result =
        pk_wal_record_decode(encoded, (size_t)n + 3, &decoded, &consumed);
    check(result == PK_WAL_DECODE_OK && consumed == (size_t)n,
          "decoder consumes one record and leaves trailing bytes");
    check(decoded.opcode == PK_OP_SET && decoded.sequence == source.sequence,
          "opcode and 64-bit sequence round-trip");
    check(decoded.key_len == sizeof(key)
          && memcmp(decoded.key, key, sizeof(key)) == 0,
          "binary key round-trips without C-string assumptions");
    check(decoded.val_len == sizeof(val)
          && memcmp(decoded.val, val, sizeof(val)) == 0,
          "binary value round-trips without C-string assumptions");

    bool all_incomplete = true;
    for (size_t prefix = 0; prefix < (size_t)n; prefix++) {
        consumed = 999;
        if (pk_wal_record_decode(encoded, prefix, &decoded, &consumed)
                != PK_WAL_DECODE_INCOMPLETE || consumed != 0) {
            all_incomplete = false;
            break;
        }
    }
    check(all_incomplete, "every valid truncated prefix is reported incomplete");

    encoded[PK_WAL_HEADER_LEN + 1] ^= 0x80;
    check(pk_wal_record_decode(encoded, (size_t)n, &decoded, &consumed)
              == PK_WAL_DECODE_ERROR,
          "one corrupted payload byte is rejected by CRC32");
    encoded[PK_WAL_HEADER_LEN + 1] ^= 0x80;

    static const uint8_t crc_vector[] = "123456789";
    check(pk_wal_crc32(crc_vector, sizeof(crc_vector) - 1) == 0xcbf43926u,
          "CRC32 matches the IEEE reference vector");

    pk_wal_record_t bad = source;
    bad.opcode = PK_OP_GET;
    check(pk_wal_record_encode(&bad, encoded, sizeof(encoded)) < 0,
          "GET is rejected because reads never belong in the WAL");
    bad = source;
    bad.opcode = PK_OP_DEL;
    check(pk_wal_record_encode(&bad, encoded, sizeof(encoded)) < 0,
          "DEL with a value is rejected");
}

typedef struct completion_state completion_state_t;

typedef struct {
    completion_state_t *state;
    int                 id;
} request_tag_t;

struct completion_state {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    size_t          completed;
    size_t          errors;
    bool            seen[TOTAL_REQUESTS];
    uint64_t        sequences[TOTAL_REQUESTS];
};

static void record_completion(pk_wal_request_t *request, int error, void *ctx)
{
    completion_state_t *state = ctx;
    request_tag_t *tag = pk_wal_request_user_data(request);
    const pk_wal_record_t *record = pk_wal_request_record(request);

    pthread_mutex_lock(&state->lock);
    size_t slot = state->completed;
    if (slot < TOTAL_REQUESTS)
        state->sequences[slot] = record->sequence;
    if (tag->id >= 0 && tag->id < TOTAL_REQUESTS)
        state->seen[tag->id] = true;
    if (error != 0)
        state->errors++;
    state->completed++;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->lock);

    pk_wal_request_destroy(request);
}

typedef struct {
    int                 producer;
    pk_wal_t           *wal;
    completion_state_t *state;
    pthread_barrier_t  *barrier;
    request_tag_t      *tags;
    int                 submit_errors;
} producer_arg_t;

static void *producer_main(void *opaque)
{
    producer_arg_t *arg = opaque;
    pthread_barrier_wait(arg->barrier);

    for (int i = 0; i < PER_PRODUCER; i++) {
        int id = arg->producer * PER_PRODUCER + i;
        char key[64];
        char value[96];
        int klen = snprintf(key, sizeof(key), "producer-%02d-key-%03d",
                            arg->producer, i);
        int vlen = snprintf(value, sizeof(value), "value-from-%02d-for-%03d",
                            arg->producer, i);
        arg->tags[id].state = arg->state;
        arg->tags[id].id = id;

        pk_wal_request_t *request =
            pk_wal_request_create(PK_OP_SET, (const uint8_t *)key, (uint32_t)klen,
                                  (const uint8_t *)value, (uint32_t)vlen,
                                  &arg->tags[id]);
        if (request == NULL) {
            arg->submit_errors++;
            continue;
        }
        int rc = pk_wal_submit(arg->wal, request, record_completion, arg->state);
        if (rc != 0) {
            arg->submit_errors++;
            pk_wal_request_destroy(request);
        }
    }
    return NULL;
}

static bool wait_for_completions(completion_state_t *state, size_t want)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 15;

    pthread_mutex_lock(&state->lock);
    while (state->completed < want) {
        int rc = pthread_cond_timedwait(&state->cond, &state->lock, &deadline);
        if (rc == ETIMEDOUT)
            break;
    }
    bool done = state->completed == want;
    pthread_mutex_unlock(&state->lock);
    return done;
}

static uint8_t *read_file(const char *path, size_t *len_out)
{
    *len_out = 0;
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return NULL;

    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size < 0) {
        close(fd);
        return NULL;
    }
    size_t len = (size_t)st.st_size;
    uint8_t *buf = malloc(len > 0 ? len : 1);
    if (buf == NULL) {
        close(fd);
        return NULL;
    }

    size_t have = 0;
    while (have < len) {
        ssize_t n = read(fd, buf + have, len - have);
        if (n > 0) {
            have += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        free(buf);
        close(fd);
        return NULL;
    }
    close(fd);
    *len_out = len;
    return buf;
}

static void test_async_group_commit(void)
{
    section("concurrent producers and group commit");

    char path[] = "/tmp/pulsekv-wal-test-XXXXXX";
    int temp_fd = mkstemp(path);
    check(temp_fd >= 0, "temporary WAL file created");
    if (temp_fd < 0)
        return;
    close(temp_fd);

    pk_wal_t *wal = NULL;
    int rc = pk_wal_init(&wal, path, 64, 20000, 1);
    check(rc == 0, "dedicated WAL writer started");
    if (rc != 0) {
        unlink(path);
        return;
    }

    completion_state_t state = {
        .lock = PTHREAD_MUTEX_INITIALIZER,
        .cond = PTHREAD_COND_INITIALIZER,
    };
    pthread_barrier_t barrier;
    rc = pthread_barrier_init(&barrier, NULL, PRODUCERS);
    check(rc == 0, "producer start barrier initialized");

    pthread_t threads[PRODUCERS];
    producer_arg_t args[PRODUCERS];
    request_tag_t tags[TOTAL_REQUESTS];
    memset(tags, 0, sizeof(tags));

    int created = 0;
    for (int i = 0; i < PRODUCERS; i++) {
        args[i] = (producer_arg_t){
            .producer = i,
            .wal      = wal,
            .state    = &state,
            .barrier  = &barrier,
            .tags     = tags,
        };
        rc = pthread_create(&threads[i], NULL, producer_main, &args[i]);
        if (rc != 0)
            break;
        created++;
    }
    check(created == PRODUCERS, "all concurrent producer threads started");

    for (int i = 0; i < created; i++)
        pthread_join(threads[i], NULL);

    int submit_errors = 0;
    for (int i = 0; i < created; i++)
        submit_errors += args[i].submit_errors;
    check(submit_errors == 0, "all 256 mutations entered the WAL queue");
    check(wait_for_completions(&state, TOTAL_REQUESTS),
          "all completions arrived after durable batch syncs");

    rc = pk_wal_stop(wal);
    check(rc == 0, "writer drained and stopped without an I/O error");
    pk_wal_stats_t stats = pk_wal_stats(wal);
    check(stats.records == TOTAL_REQUESTS, "writer reports all records durable");
    check(stats.batches == stats.syncs && stats.batches > 0,
          "each group-commit batch performed exactly one fdatasync");
    check(stats.batches < stats.records && stats.largest_batch > 1,
          "concurrent requests were combined into multi-record batches");

    pthread_mutex_lock(&state.lock);
    bool callbacks_clean = state.completed == TOTAL_REQUESTS && state.errors == 0;
    bool all_seen = true;
    bool ordered = true;
    for (size_t i = 0; i < TOTAL_REQUESTS; i++) {
        if (!state.seen[i])
            all_seen = false;
        if (state.sequences[i] != i + 1)
            ordered = false;
    }
    pthread_mutex_unlock(&state.lock);
    check(callbacks_clean && all_seen, "every submitted request completed once");
    check(ordered, "completion order follows contiguous WAL sequence order");

    size_t file_len = 0;
    uint8_t *file = read_file(path, &file_len);
    check(file != NULL && file_len == stats.bytes,
          "durable file length matches writer byte accounting");

    size_t offset = 0;
    size_t records = 0;
    bool file_valid = file != NULL;
    while (file_valid && offset < file_len) {
        pk_wal_record_t record;
        size_t consumed = 0;
        pk_wal_decode_result_t decoded =
            pk_wal_record_decode(file + offset, file_len - offset,
                                 &record, &consumed);
        if (decoded != PK_WAL_DECODE_OK || consumed == 0
            || record.sequence != records + 1) {
            file_valid = false;
            break;
        }
        offset += consumed;
        records++;
    }
    check(file_valid && offset == file_len && records == TOTAL_REQUESTS,
          "the append stream is CRC-valid and ordered on disk");

    free(file);
    pk_wal_destroy(wal);
    pthread_barrier_destroy(&barrier);
    pthread_cond_destroy(&state.cond);
    pthread_mutex_destroy(&state.lock);
    unlink(path);
}

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    bool            done;
    int             error;
} error_completion_t;

static void error_completion(pk_wal_request_t *request, int error, void *ctx)
{
    error_completion_t *state = ctx;
    pthread_mutex_lock(&state->lock);
    state->done = true;
    state->error = error;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->lock);
    pk_wal_request_destroy(request);
}

static void test_disk_failure(void)
{
    section("disk failure containment");

    pk_wal_t *wal = NULL;
    int rc = pk_wal_init(&wal, "/dev/full", 8, 1000, 1);
    check(rc == 0, "/dev/full opens so failure happens on append");
    if (rc != 0)
        return;

    error_completion_t state = {
        .lock = PTHREAD_MUTEX_INITIALIZER,
        .cond = PTHREAD_COND_INITIALIZER,
    };
    static const uint8_t key[] = "must-not-apply";
    static const uint8_t val[] = "value";
    pk_wal_request_t *request =
        pk_wal_request_create(PK_OP_SET, key, sizeof(key) - 1,
                              val, sizeof(val) - 1, NULL);
    check(request != NULL, "failure-path mutation allocated");
    if (request != NULL) {
        rc = pk_wal_submit(wal, request, error_completion, &state);
        check(rc == 0, "failure-path mutation submitted asynchronously");

        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += 5;
        pthread_mutex_lock(&state.lock);
        while (!state.done) {
            rc = pthread_cond_timedwait(&state.cond, &state.lock, &deadline);
            if (rc == ETIMEDOUT)
                break;
        }
        bool failed_as_expected = state.done && state.error != 0;
        pthread_mutex_unlock(&state.lock);
        check(failed_as_expected, "write error reaches completion instead of success");
    }

    rc = pk_wal_stop(wal);
    check(rc != 0, "WAL retains the disk error as a sticky service error");
    pk_wal_stats_t stats = pk_wal_stats(wal);
    check(stats.records == 0 && stats.syncs == 0,
          "failed batch is never counted as durable");

    pk_wal_destroy(wal);
    pthread_cond_destroy(&state.cond);
    pthread_mutex_destroy(&state.lock);
}

int main(void)
{
    test_record_codec();
    test_async_group_commit();
    test_disk_failure();

    printf("\nRESULT: %s (%d checks, %d failed)\n",
           g_failed == 0 ? "PASS" : "FAIL", g_checks, g_failed);
    return g_failed == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
