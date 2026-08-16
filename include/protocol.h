#ifndef PULSEKV_PROTOCOL_H
#define PULSEKV_PROTOCOL_H

#include <stddef.h>
#include <stdint.h>

/*
 * PulseKV wire protocol -- binary, fixed layout, no allocation on parse.
 *
 *   request:  [1B opcode][4B key_len][key][4B val_len][val]
 *   response: [1B status][4B val_len][val]
 *
 * key_len and val_len are uint32_t in network byte order. The val_len field is
 * always present; GET and DEL carry val_len == 0 and no value bytes, so only
 * SET actually puts a payload on the wire.
 */

typedef enum {
    PK_OP_GET = 0x01,
    PK_OP_SET = 0x02,
    PK_OP_DEL = 0x03
} pk_opcode_t;

typedef enum {
    PK_STATUS_OK        = 0x00,
    PK_STATUS_NOT_FOUND = 0x01,
    PK_STATUS_ERROR     = 0x02
} pk_status_t;

/* Fixed per-frame overhead, i.e. everything that is not key or value bytes. */
#define PK_REQ_HEADER_LEN   9u  /* opcode + key_len + val_len */
#define PK_RESP_HEADER_LEN  5u  /* status + val_len */

/* Bounds, so a bogus length field can't make us buffer gigabytes. */
#define PK_MAX_KEY_LEN      1024u
#define PK_MAX_VAL_LEN      (64u * 1024u)

#define PK_MAX_REQ_LEN      (PK_REQ_HEADER_LEN + PK_MAX_KEY_LEN + PK_MAX_VAL_LEN)
#define PK_MAX_RESP_LEN     (PK_RESP_HEADER_LEN + PK_MAX_VAL_LEN)

/*
 * Decoded frames are views, not copies: key and val point into the caller's
 * buffer and stay valid only as long as that buffer holds the frame. Nothing
 * here owns memory.
 */
typedef struct {
    uint8_t        opcode;
    uint32_t       key_len;
    const uint8_t *key;
    uint32_t       val_len;
    const uint8_t *val;  /* NULL when val_len == 0 */
} pk_request_t;

typedef struct {
    uint8_t        status;
    uint32_t       val_len;
    const uint8_t *val;  /* NULL when val_len == 0 */
} pk_response_t;

typedef enum {
    PK_DECODE_OK         =  0,  /* one whole frame decoded */
    PK_DECODE_INCOMPLETE =  1,  /* need more bytes; buffer is still good */
    PK_DECODE_ERROR      = -1   /* unparseable; answer ERROR and drop the connection */
} pk_decode_result_t;

/*
 * Decode one frame from the front of buf. On PK_DECODE_OK, *consumed is set to
 * that frame's length so the caller can slide any trailing bytes down.
 */
pk_decode_result_t pk_request_decode(const uint8_t *buf, size_t len,
                                     pk_request_t *out, size_t *consumed);
pk_decode_result_t pk_response_decode(const uint8_t *buf, size_t len,
                                      pk_response_t *out, size_t *consumed);

/* Serialize into a caller-provided buffer. Returns bytes written, or -1 if the
 * frame is invalid or cap is too small. */
int pk_request_encode(const pk_request_t *req, uint8_t *buf, size_t cap);
int pk_response_encode(const pk_response_t *resp, uint8_t *buf, size_t cap);

const char *pk_opcode_name(uint8_t opcode);
const char *pk_status_name(uint8_t status);

#endif /* PULSEKV_PROTOCOL_H */
