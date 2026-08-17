#define _POSIX_C_SOURCE 200809L

/* Versioned WAL codec, asynchronous group-commit writer, and batched replay. */

#include "protocol.h"
#include "wal.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

struct pk_wal_request {
    pk_wal_record_t       record;
    void                 *user_data;
    pk_wal_completion_fn  completion;
    void                 *completion_ctx;
    struct pk_wal_request *next;
    uint8_t               data[];
};

struct pk_wal {
    int                    fd;
    pthread_t              thread;
    pthread_mutex_t        lock;
    pthread_cond_t         cond;
    pk_wal_request_t      *head;
    pk_wal_request_t      *tail;
    size_t                 queued;
    size_t                 batch_max;
    uint64_t               next_sequence;
    uint64_t               delay_ns;
    bool                   stopping;
    bool                   thread_started;
    bool                   sync_initialized;
    int                    sticky_error;
    pk_wal_request_t     **batch;
    uint8_t               *batch_buf;
    size_t                 batch_buf_cap;
    pk_wal_stats_t         stats;
};

static uint16_t get_u16(const uint8_t *p)
{
    return (uint16_t)(((uint16_t)p[0] << 8) | (uint16_t)p[1]);
}

static uint32_t get_u32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16)
         | ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static uint64_t get_u64(const uint8_t *p)
{
    return ((uint64_t)get_u32(p) << 32) | (uint64_t)get_u32(p + 4);
}

static void put_u16(uint8_t *p, uint16_t value)
{
    p[0] = (uint8_t)(value >> 8);
    p[1] = (uint8_t)value;
}

static void put_u32(uint8_t *p, uint32_t value)
{
    p[0] = (uint8_t)(value >> 24);
    p[1] = (uint8_t)(value >> 16);
    p[2] = (uint8_t)(value >> 8);
    p[3] = (uint8_t)value;
}

static void put_u64(uint8_t *p, uint64_t value)
{
    put_u32(p, (uint32_t)(value >> 32));
    put_u32(p + 4, (uint32_t)value);
}

static pthread_once_t g_crc_once = PTHREAD_ONCE_INIT;
static uint32_t g_crc_table[256];

static void init_crc_table(void)
{
    for (uint32_t value = 0; value < 256; value++) {
        uint32_t crc = value;
        for (unsigned bit = 0; bit < 8; bit++) {
            uint32_t mask = (uint32_t)-(int32_t)(crc & 1u);
            crc = (crc >> 1) ^ (0xedb88320u & mask);
        }
        g_crc_table[value] = crc;
    }
}

uint32_t pk_wal_crc32(const uint8_t *data, size_t len)
{
    pthread_once(&g_crc_once, init_crc_table);

    uint32_t crc = UINT32_MAX;
    for (size_t i = 0; i < len; i++)
        crc = (crc >> 8) ^ g_crc_table[(crc ^ data[i]) & 0xffu];
    return ~crc;
}

size_t pk_wal_record_size(uint32_t key_len, uint32_t val_len)
{
    if (key_len == 0 || key_len > PK_MAX_KEY_LEN || val_len > PK_MAX_VAL_LEN)
        return 0;
    return PK_WAL_MIN_RECORD_LEN + (size_t)key_len + (size_t)val_len;
}

static bool valid_record(const pk_wal_record_t *record)
{
    if (record == NULL || record->key == NULL || record->key_len == 0
        || record->key_len > PK_MAX_KEY_LEN || record->val_len > PK_MAX_VAL_LEN)
        return false;
    if (record->opcode != PK_OP_SET && record->opcode != PK_OP_DEL)
        return false;
    if (record->opcode == PK_OP_DEL && record->val_len != 0)
        return false;
    return record->val_len == 0 || record->val != NULL;
}

int pk_wal_record_encode(const pk_wal_record_t *record,
                         uint8_t *buf, size_t cap)
{
    if (!valid_record(record) || buf == NULL)
        return -1;

    size_t total = pk_wal_record_size(record->key_len, record->val_len);
    if (total == 0 || total > cap || total > UINT32_MAX)
        return -1;

    put_u32(buf, PK_WAL_MAGIC);
    put_u16(buf + 4, PK_WAL_VERSION);
    buf[6] = record->opcode;
    buf[7] = 0;
    put_u32(buf + 8, (uint32_t)total);
    put_u64(buf + 12, record->sequence);
    put_u32(buf + 20, record->key_len);
    put_u32(buf + 24, record->val_len);
    memcpy(buf + PK_WAL_HEADER_LEN, record->key, record->key_len);
    if (record->val_len > 0) {
        memcpy(buf + PK_WAL_HEADER_LEN + record->key_len,
               record->val, record->val_len);
    }
    put_u32(buf + total - PK_WAL_CRC_LEN,
            pk_wal_crc32(buf, total - PK_WAL_CRC_LEN));
    return (int)total;
}

pk_wal_decode_result_t pk_wal_record_decode(const uint8_t *buf, size_t len,
                                            pk_wal_record_t *out,
                                            size_t *consumed)
{
    if (consumed != NULL)
        *consumed = 0;
    if (buf == NULL || out == NULL)
        return PK_WAL_DECODE_ERROR;
    if (len < PK_WAL_HEADER_LEN)
        return PK_WAL_DECODE_INCOMPLETE;

    uint32_t total = get_u32(buf + 8);
    uint32_t klen  = get_u32(buf + 20);
    uint32_t vlen  = get_u32(buf + 24);
    size_t expected = pk_wal_record_size(klen, vlen);

    if (get_u32(buf) != PK_WAL_MAGIC || get_u16(buf + 4) != PK_WAL_VERSION
        || buf[7] != 0 || expected == 0 || total != expected
        || total > PK_WAL_MAX_RECORD_LEN)
        return PK_WAL_DECODE_ERROR;
    if (len < total)
        return PK_WAL_DECODE_INCOMPLETE;

    uint8_t opcode = buf[6];
    if ((opcode != PK_OP_SET && opcode != PK_OP_DEL)
        || (opcode == PK_OP_DEL && vlen != 0))
        return PK_WAL_DECODE_ERROR;

    uint32_t stored_crc = get_u32(buf + total - PK_WAL_CRC_LEN);
    uint32_t actual_crc = pk_wal_crc32(buf, total - PK_WAL_CRC_LEN);
    if (stored_crc != actual_crc)
        return PK_WAL_DECODE_ERROR;

    out->opcode   = opcode;
    out->sequence = get_u64(buf + 12);
    out->key_len  = klen;
    out->key      = buf + PK_WAL_HEADER_LEN;
    out->val_len  = vlen;
    out->val      = vlen > 0 ? out->key + klen : NULL;
    if (consumed != NULL)
        *consumed = total;
    return PK_WAL_DECODE_OK;
}

const char *pk_wal_repair_name(pk_wal_repair_t repair)
{
    switch (repair) {
    case PK_WAL_REPAIR_NONE:      return "none";
    case PK_WAL_REPAIR_TRUNCATED: return "truncated tail";
    case PK_WAL_REPAIR_CORRUPT:   return "corrupt tail";
    case PK_WAL_REPAIR_SEQUENCE:  return "invalid sequence";
    default:                      return "unknown";
    }
}

static int repair_tail(int fd, pk_wal_recovery_stats_t *stats,
                       pk_wal_repair_t repair)
{
    stats->repair = repair;
    if (stats->original_bytes >= stats->valid_bytes)
        stats->discarded_bytes = stats->original_bytes - stats->valid_bytes;

    if (ftruncate(fd, (off_t)stats->valid_bytes) < 0)
        return errno;
    if (fdatasync(fd) < 0)
        return errno;
    return 0;
}

int pk_wal_recover(const char *path, size_t read_chunk,
                   pk_wal_replay_fn replay, void *replay_ctx,
                   pk_wal_recovery_stats_t *stats_out)
{
    if (path == NULL || path[0] == '\0' || read_chunk == 0 || stats_out == NULL)
        return EINVAL;
    if (read_chunk > SIZE_MAX - PK_WAL_MAX_RECORD_LEN)
        return EOVERFLOW;

    pk_wal_recovery_stats_t stats = {0};
    int fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (fd < 0)
        return errno;

    struct stat file_stat;
    if (fstat(fd, &file_stat) < 0) {
        int error = errno;
        close(fd);
        return error;
    }
    if (!S_ISREG(file_stat.st_mode)) {
        close(fd);
        return EINVAL;
    }
    if (file_stat.st_size < 0) {
        close(fd);
        return EOVERFLOW;
    }
    stats.original_bytes = (uint64_t)file_stat.st_size;

    size_t capacity = read_chunk + PK_WAL_MAX_RECORD_LEN;
    uint8_t *buffer = malloc(capacity);
    if (buffer == NULL) {
        close(fd);
        return ENOMEM;
    }

    size_t have = 0;
    bool eof = false;
    int error = 0;

    while (!eof || have > 0) {
        if (!eof) {
            ssize_t n;
            do {
                n = read(fd, buffer + have, read_chunk);
                stats.read_calls++;
            } while (n < 0 && errno == EINTR);

            if (n < 0) {
                error = errno;
                break;
            }
            if (n == 0)
                eof = true;
            else {
                have += (size_t)n;
                stats.bytes_read += (uint64_t)n;
            }
        }

        size_t consumed_from_buffer = 0;
        while (consumed_from_buffer < have) {
            pk_wal_record_t record;
            size_t consumed = 0;
            pk_wal_decode_result_t decoded =
                pk_wal_record_decode(buffer + consumed_from_buffer,
                                     have - consumed_from_buffer,
                                     &record, &consumed);
            if (decoded == PK_WAL_DECODE_INCOMPLETE)
                break;
            if (decoded == PK_WAL_DECODE_ERROR) {
                error = repair_tail(fd, &stats, PK_WAL_REPAIR_CORRUPT);
                goto done;
            }

            uint64_t expected = stats.last_sequence + 1u;
            if (stats.last_sequence == UINT64_MAX || record.sequence != expected) {
                error = repair_tail(fd, &stats, PK_WAL_REPAIR_SEQUENCE);
                goto done;
            }

            if (replay != NULL) {
                error = replay(&record, replay_ctx);
                if (error != 0)
                    goto done;
            }

            consumed_from_buffer += consumed;
            stats.records++;
            stats.valid_bytes += consumed;
            stats.last_sequence = record.sequence;
        }

        if (consumed_from_buffer > 0) {
            have -= consumed_from_buffer;
            memmove(buffer, buffer + consumed_from_buffer, have);
        }

        if (eof && have > 0) {
            error = repair_tail(fd, &stats, PK_WAL_REPAIR_TRUNCATED);
            goto done;
        }
    }

done:
    free(buffer);
    if (close(fd) < 0 && error == 0)
        error = errno;
    *stats_out = stats;
    return error;
}

pk_wal_request_t *pk_wal_request_create(uint8_t opcode,
                                        const uint8_t *key, uint32_t key_len,
                                        const uint8_t *val, uint32_t val_len,
                                        void *user_data)
{
    pk_wal_record_t candidate = {
        .opcode  = opcode,
        .key_len = key_len,
        .key     = key,
        .val_len = val_len,
        .val     = val,
    };
    if (!valid_record(&candidate))
        return NULL;

    pk_wal_request_t *request = malloc(sizeof(*request) + key_len + val_len);
    if (request == NULL)
        return NULL;

    memset(request, 0, sizeof(*request));
    request->record.opcode  = opcode;
    request->record.key_len = key_len;
    request->record.key     = request->data;
    request->record.val_len = val_len;
    request->record.val     = val_len > 0 ? request->data + key_len : NULL;
    request->user_data      = user_data;
    memcpy(request->data, key, key_len);
    if (val_len > 0)
        memcpy(request->data + key_len, val, val_len);
    return request;
}

void pk_wal_request_destroy(pk_wal_request_t *request)
{
    free(request);
}

const pk_wal_record_t *pk_wal_request_record(const pk_wal_request_t *request)
{
    return request == NULL ? NULL : &request->record;
}

void *pk_wal_request_user_data(const pk_wal_request_t *request)
{
    return request == NULL ? NULL : request->user_data;
}

static struct timespec add_ns(struct timespec time, uint64_t ns)
{
    time.tv_sec += (time_t)(ns / 1000000000u);
    time.tv_nsec += (long)(ns % 1000000000u);
    if (time.tv_nsec >= 1000000000L) {
        time.tv_sec++;
        time.tv_nsec -= 1000000000L;
    }
    return time;
}

static int write_all(int fd, const uint8_t *buf, size_t len)
{
    size_t written = 0;
    while (written < len) {
        ssize_t n = write(fd, buf + written, len - written);
        if (n > 0) {
            written += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        return n == 0 ? EIO : errno;
    }
    return 0;
}

static size_t take_batch(pk_wal_t *wal)
{
    size_t count = 0;
    while (count < wal->batch_max && wal->head != NULL) {
        pk_wal_request_t *request = wal->head;
        wal->head = request->next;
        request->next = NULL;
        wal->batch[count++] = request;
        wal->queued--;
    }
    if (wal->head == NULL)
        wal->tail = NULL;
    return count;
}

static int encode_batch(pk_wal_t *wal, size_t count, size_t *bytes_out)
{
    size_t needed = 0;
    for (size_t i = 0; i < count; i++) {
        const pk_wal_record_t *record = &wal->batch[i]->record;
        size_t record_size = pk_wal_record_size(record->key_len, record->val_len);
        if (record_size == 0 || needed > SIZE_MAX - record_size)
            return EOVERFLOW;
        needed += record_size;
    }

    if (needed > wal->batch_buf_cap) {
        size_t capacity = wal->batch_buf_cap > 0 ? wal->batch_buf_cap : 4096u;
        while (capacity < needed) {
            if (capacity > SIZE_MAX / 2u) {
                capacity = needed;
                break;
            }
            capacity *= 2u;
        }
        uint8_t *larger = realloc(wal->batch_buf, capacity);
        if (larger == NULL)
            return ENOMEM;
        wal->batch_buf = larger;
        wal->batch_buf_cap = capacity;
    }

    size_t used = 0;
    for (size_t i = 0; i < count; i++) {
        const pk_wal_record_t *record = &wal->batch[i]->record;
        int n = pk_wal_record_encode(record, wal->batch_buf + used,
                                     wal->batch_buf_cap - used);
        if (n < 0)
            return EINVAL;
        used += (size_t)n;
    }
    *bytes_out = used;
    return 0;
}

static void complete_batch(pk_wal_t *wal, size_t count, int error)
{
    for (size_t i = 0; i < count; i++) {
        pk_wal_request_t *request = wal->batch[i];
        request->completion(request, error, request->completion_ctx);
        wal->batch[i] = NULL;
    }
}

static void *writer_main(void *arg)
{
    pk_wal_t *wal = arg;

    for (;;) {
        pthread_mutex_lock(&wal->lock);
        while (wal->head == NULL && !wal->stopping)
            pthread_cond_wait(&wal->cond, &wal->lock);

        if (wal->head == NULL && wal->stopping) {
            pthread_mutex_unlock(&wal->lock);
            break;
        }

        if (!wal->stopping && wal->queued < wal->batch_max
            && wal->sticky_error == 0 && wal->delay_ns > 0) {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            struct timespec deadline = add_ns(now, wal->delay_ns);
            while (wal->queued < wal->batch_max && !wal->stopping) {
                int rc = pthread_cond_timedwait(&wal->cond, &wal->lock, &deadline);
                if (rc == ETIMEDOUT)
                    break;
            }
        }

        size_t count = take_batch(wal);
        int error = wal->sticky_error;
        pthread_mutex_unlock(&wal->lock);

        size_t bytes = 0;
        if (error == 0)
            error = encode_batch(wal, count, &bytes);
        if (error == 0)
            error = write_all(wal->fd, wal->batch_buf, bytes);
        if (error == 0 && fdatasync(wal->fd) < 0)
            error = errno;

        if (error != 0) {
            pthread_mutex_lock(&wal->lock);
            if (wal->sticky_error == 0)
                wal->sticky_error = error;
            error = wal->sticky_error;
            pthread_mutex_unlock(&wal->lock);
        } else {
            wal->stats.records += count;
            wal->stats.batches++;
            wal->stats.syncs++;
            wal->stats.bytes += bytes;
            if (count > wal->stats.largest_batch)
                wal->stats.largest_batch = count;
        }

        complete_batch(wal, count, error);
    }
    return NULL;
}

int pk_wal_init(pk_wal_t **wal_out, const char *path,
                size_t batch_max, uint32_t max_delay_us,
                uint64_t next_sequence)
{
    if (wal_out == NULL || path == NULL || path[0] == '\0' || batch_max == 0
        || next_sequence == 0)
        return EINVAL;
    *wal_out = NULL;
    if (batch_max > SIZE_MAX / sizeof(pk_wal_request_t *))
        return EOVERFLOW;

    pk_wal_t *wal = calloc(1, sizeof(*wal));
    if (wal == NULL)
        return ENOMEM;
    wal->fd = -1;
    wal->batch_max = batch_max;
    wal->next_sequence = next_sequence;
    wal->delay_ns = (uint64_t)max_delay_us * 1000u;

    wal->batch = calloc(batch_max, sizeof(*wal->batch));
    if (wal->batch == NULL) {
        free(wal->batch);
        free(wal);
        return ENOMEM;
    }

    int rc = pthread_mutex_init(&wal->lock, NULL);
    if (rc != 0)
        goto fail_storage;

    pthread_condattr_t attr;
    rc = pthread_condattr_init(&attr);
    if (rc != 0) {
        pthread_mutex_destroy(&wal->lock);
        goto fail_storage;
    }
    rc = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    if (rc == 0)
        rc = pthread_cond_init(&wal->cond, &attr);
    pthread_condattr_destroy(&attr);
    if (rc != 0) {
        pthread_mutex_destroy(&wal->lock);
        goto fail_storage;
    }
    wal->sync_initialized = true;

    wal->fd = open(path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0600);
    if (wal->fd < 0) {
        rc = errno;
        goto fail_sync;
    }

    rc = pthread_create(&wal->thread, NULL, writer_main, wal);
    if (rc != 0) {
        close(wal->fd);
        wal->fd = -1;
        goto fail_sync;
    }
    wal->thread_started = true;
    *wal_out = wal;
    return 0;

fail_sync:
    pthread_cond_destroy(&wal->cond);
    pthread_mutex_destroy(&wal->lock);
    wal->sync_initialized = false;
fail_storage:
    free(wal->batch);
    free(wal->batch_buf);
    wal->batch = NULL;
    wal->batch_buf = NULL;
    free(wal);
    return rc;
}

int pk_wal_submit(pk_wal_t *wal, pk_wal_request_t *request,
                  pk_wal_completion_fn completion, void *completion_ctx)
{
    if (wal == NULL || request == NULL || completion == NULL)
        return EINVAL;

    pthread_mutex_lock(&wal->lock);
    if (wal->stopping || !wal->thread_started) {
        pthread_mutex_unlock(&wal->lock);
        return ESHUTDOWN;
    }
    if (wal->next_sequence == 0) {
        pthread_mutex_unlock(&wal->lock);
        return EOVERFLOW;
    }

    bool wake_writer = wal->head == NULL;
    request->record.sequence = wal->next_sequence++;
    request->completion = completion;
    request->completion_ctx = completion_ctx;
    request->next = NULL;
    if (wal->tail == NULL)
        wal->head = request;
    else
        wal->tail->next = request;
    wal->tail = request;
    wal->queued++;
    if (wake_writer || wal->queued >= wal->batch_max)
        pthread_cond_signal(&wal->cond);
    pthread_mutex_unlock(&wal->lock);
    return 0;
}

int pk_wal_stop(pk_wal_t *wal)
{
    if (wal == NULL || !wal->sync_initialized)
        return EINVAL;

    if (wal->thread_started) {
        pthread_mutex_lock(&wal->lock);
        wal->stopping = true;
        pthread_cond_broadcast(&wal->cond);
        pthread_mutex_unlock(&wal->lock);

        int rc = pthread_join(wal->thread, NULL);
        if (rc != 0)
            return rc;
        wal->thread_started = false;
    }

    if (wal->fd >= 0) {
        if (close(wal->fd) < 0 && wal->sticky_error == 0)
            wal->sticky_error = errno;
        wal->fd = -1;
    }
    return wal->sticky_error;
}

void pk_wal_destroy(pk_wal_t *wal)
{
    if (wal == NULL)
        return;
    if (wal->thread_started)
        (void)pk_wal_stop(wal);
    if (wal->fd >= 0)
        close(wal->fd);
    if (wal->sync_initialized) {
        pthread_cond_destroy(&wal->cond);
        pthread_mutex_destroy(&wal->lock);
    }
    free(wal->batch);
    free(wal->batch_buf);
    free(wal);
}

pk_wal_stats_t pk_wal_stats(const pk_wal_t *wal)
{
    pk_wal_stats_t empty = {0};
    return wal == NULL ? empty : wal->stats;
}
