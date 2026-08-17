#ifndef PULSEKV_WAL_H
#define PULSEKV_WAL_H

#include "protocol.h"

#include <stddef.h>
#include <stdint.h>

/*
 * Version-one append-only WAL record, all integer fields in big-endian order:
 *
 *   [4B magic][2B version][1B opcode][1B flags]
 *   [4B record_len][8B sequence][4B key_len][4B value_len]
 *   [key][value][4B CRC32]
 *
 * record_len covers the whole record. The CRC is IEEE CRC32 over every byte
 * before the CRC itself, including the header. Length and version fields make
 * malformed data rejectable without trusting payload bytes.
 */
#define PK_WAL_MAGIC            0x504b574cu /* "PKWL" */
#define PK_WAL_VERSION          1u
#define PK_WAL_HEADER_LEN       28u
#define PK_WAL_CRC_LEN           4u
#define PK_WAL_MIN_RECORD_LEN   (PK_WAL_HEADER_LEN + PK_WAL_CRC_LEN)
#define PK_WAL_MAX_RECORD_LEN   (PK_WAL_MIN_RECORD_LEN + PK_MAX_KEY_LEN + PK_MAX_VAL_LEN)

#define PK_WAL_DEFAULT_PATH          "pulsekv.log"
#define PK_WAL_DEFAULT_BATCH_MAX    256u
#define PK_WAL_DEFAULT_DELAY_US    1000u
#define PK_WAL_DEFAULT_RECOVERY_CHUNK (256u * 1024u)

typedef struct {
    uint8_t        opcode;
    uint64_t       sequence;
    uint32_t       key_len;
    const uint8_t *key;
    uint32_t       val_len;
    const uint8_t *val;
} pk_wal_record_t;

typedef enum {
    PK_WAL_DECODE_OK         =  0,
    PK_WAL_DECODE_INCOMPLETE =  1,
    PK_WAL_DECODE_ERROR      = -1
} pk_wal_decode_result_t;

size_t pk_wal_record_size(uint32_t key_len, uint32_t val_len);
int pk_wal_record_encode(const pk_wal_record_t *record,
                         uint8_t *buf, size_t cap);
pk_wal_decode_result_t pk_wal_record_decode(const uint8_t *buf, size_t len,
                                            pk_wal_record_t *out,
                                            size_t *consumed);
uint32_t pk_wal_crc32(const uint8_t *data, size_t len);

typedef enum {
    PK_WAL_REPAIR_NONE = 0,
    PK_WAL_REPAIR_TRUNCATED,
    PK_WAL_REPAIR_CORRUPT,
    PK_WAL_REPAIR_SEQUENCE
} pk_wal_repair_t;

typedef struct {
    uint64_t        records;
    uint64_t        original_bytes;
    uint64_t        valid_bytes;
    uint64_t        discarded_bytes;
    uint64_t        bytes_read;
    uint64_t        read_calls;
    uint64_t        last_sequence;
    pk_wal_repair_t repair;
} pk_wal_recovery_stats_t;

/* Return zero to accept a decoded record or an errno-style value to abort.
 * key/val are temporary decode views valid only for the callback duration. */
typedef int (*pk_wal_replay_fn)(const pk_wal_record_t *record, void *ctx);

/*
 * Sequentially scans a regular file at path in read_chunk-sized reads,
 * validates record framing, sequence, and CRC, and invokes replay in log
 * order. A corrupt or incomplete tail is truncated back to the last valid
 * record and synced before return. replay may be NULL for validation-only
 * scans.
 */
int pk_wal_recover(const char *path, size_t read_chunk,
                   pk_wal_replay_fn replay, void *replay_ctx,
                   pk_wal_recovery_stats_t *stats_out);
const char *pk_wal_repair_name(pk_wal_repair_t repair);

typedef struct pk_wal_request pk_wal_request_t;
typedef void (*pk_wal_completion_fn)(pk_wal_request_t *request,
                                     int error, void *completion_ctx);

/*
 * A request owns copies of its key and value from creation until destroy.
 * That lets an epoll worker consume and compact its socket read buffer while
 * the WAL writer is using the mutation on another thread.
 */
pk_wal_request_t *pk_wal_request_create(uint8_t opcode,
                                        const uint8_t *key, uint32_t key_len,
                                        const uint8_t *val, uint32_t val_len,
                                        void *user_data);
void pk_wal_request_destroy(pk_wal_request_t *request);
const pk_wal_record_t *pk_wal_request_record(const pk_wal_request_t *request);
void *pk_wal_request_user_data(const pk_wal_request_t *request);

typedef struct {
    uint64_t records;
    uint64_t batches;
    uint64_t syncs;
    uint64_t bytes;
    size_t   largest_batch;
} pk_wal_stats_t;

typedef struct pk_wal pk_wal_t; /* queue/thread/file internals stay private */

/* Opens path for append and starts the dedicated writer thread. next_sequence
 * is normally recovery.last_sequence + 1, or 1 for an empty log. */
int pk_wal_init(pk_wal_t **wal_out, const char *path,
                size_t batch_max, uint32_t max_delay_us,
                uint64_t next_sequence);

/*
 * Linearizes the request into the global WAL order and transfers ownership to
 * the writer. Completion always runs on the writer thread, after the batch's
 * fdatasync succeeds or with a nonzero errno-style error. The completion
 * callback becomes the request owner and must eventually call
 * pk_wal_request_destroy. If submit itself fails, ownership remains with the
 * caller and no callback runs.
 */
int pk_wal_submit(pk_wal_t *wal, pk_wal_request_t *request,
                  pk_wal_completion_fn completion, void *completion_ctx);

/* Drain queued work, join the writer, and close the file. Safe to call once. */
int pk_wal_stop(pk_wal_t *wal);

/* Call after pk_wal_stop; releases synchronization and the opaque service. */
void pk_wal_destroy(pk_wal_t *wal);

/* Stable after pk_wal_stop. */
pk_wal_stats_t pk_wal_stats(const pk_wal_t *wal);

#endif /* PULSEKV_WAL_H */
