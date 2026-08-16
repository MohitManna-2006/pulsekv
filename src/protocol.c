#include "protocol.h"

#include <arpa/inet.h>
#include <string.h>

#define LEN_FIELD_LEN   4u
#define REQ_PREFIX_LEN  5u  /* opcode + key_len, i.e. the offset of the key */

/* The length fields are never guaranteed aligned in the read buffer, so go
 * through memcpy rather than casting to uint32_t *. */
static uint32_t rd_u32(const uint8_t *p)
{
    uint32_t n;
    memcpy(&n, p, sizeof(n));
    return ntohl(n);
}

static void wr_u32(uint8_t *p, uint32_t v)
{
    uint32_t n = htonl(v);
    memcpy(p, &n, sizeof(n));
}

static int valid_opcode(uint8_t opcode)
{
    return opcode == PK_OP_GET || opcode == PK_OP_SET || opcode == PK_OP_DEL;
}

static int valid_status(uint8_t status)
{
    return status == PK_STATUS_OK || status == PK_STATUS_NOT_FOUND ||
           status == PK_STATUS_ERROR;
}

pk_decode_result_t pk_request_decode(const uint8_t *buf, size_t len,
                                     pk_request_t *out, size_t *consumed)
{
    if (len < REQ_PREFIX_LEN)
        return PK_DECODE_INCOMPLETE;

    uint8_t opcode = buf[0];
    if (!valid_opcode(opcode))
        return PK_DECODE_ERROR;

    uint32_t key_len = rd_u32(buf + 1);
    if (key_len == 0 || key_len > PK_MAX_KEY_LEN)
        return PK_DECODE_ERROR;

    /* val_len sits after the key, so the key has to be here before we can read it */
    size_t need = REQ_PREFIX_LEN + key_len + LEN_FIELD_LEN;
    if (len < need)
        return PK_DECODE_INCOMPLETE;

    uint32_t val_len = rd_u32(buf + REQ_PREFIX_LEN + key_len);
    if (val_len > PK_MAX_VAL_LEN)
        return PK_DECODE_ERROR;
    if (opcode != PK_OP_SET && val_len != 0)
        return PK_DECODE_ERROR;  /* GET/DEL never carry a value */

    need += val_len;
    if (len < need)
        return PK_DECODE_INCOMPLETE;

    out->opcode  = opcode;
    out->key_len = key_len;
    out->key     = buf + REQ_PREFIX_LEN;
    out->val_len = val_len;
    out->val     = val_len ? buf + REQ_PREFIX_LEN + key_len + LEN_FIELD_LEN : NULL;

    if (consumed)
        *consumed = need;
    return PK_DECODE_OK;
}

pk_decode_result_t pk_response_decode(const uint8_t *buf, size_t len,
                                      pk_response_t *out, size_t *consumed)
{
    if (len < PK_RESP_HEADER_LEN)
        return PK_DECODE_INCOMPLETE;

    uint8_t status = buf[0];
    if (!valid_status(status))
        return PK_DECODE_ERROR;

    uint32_t val_len = rd_u32(buf + 1);
    if (val_len > PK_MAX_VAL_LEN)
        return PK_DECODE_ERROR;
    if (status != PK_STATUS_OK && val_len != 0)
        return PK_DECODE_ERROR;  /* only a hit carries bytes back */

    size_t need = PK_RESP_HEADER_LEN + val_len;
    if (len < need)
        return PK_DECODE_INCOMPLETE;

    out->status  = status;
    out->val_len = val_len;
    out->val     = val_len ? buf + PK_RESP_HEADER_LEN : NULL;

    if (consumed)
        *consumed = need;
    return PK_DECODE_OK;
}

int pk_request_encode(const pk_request_t *req, uint8_t *buf, size_t cap)
{
    if (!valid_opcode(req->opcode))
        return -1;
    if (req->key_len == 0 || req->key_len > PK_MAX_KEY_LEN || req->key == NULL)
        return -1;
    if (req->val_len > PK_MAX_VAL_LEN)
        return -1;
    if (req->val_len > 0 && (req->val == NULL || req->opcode != PK_OP_SET))
        return -1;

    size_t total = PK_REQ_HEADER_LEN + req->key_len + req->val_len;
    if (cap < total)
        return -1;

    buf[0] = req->opcode;
    wr_u32(buf + 1, req->key_len);
    memcpy(buf + REQ_PREFIX_LEN, req->key, req->key_len);
    wr_u32(buf + REQ_PREFIX_LEN + req->key_len, req->val_len);
    if (req->val_len > 0)
        memcpy(buf + PK_REQ_HEADER_LEN + req->key_len, req->val, req->val_len);

    return (int)total;
}

int pk_response_encode(const pk_response_t *resp, uint8_t *buf, size_t cap)
{
    if (!valid_status(resp->status))
        return -1;
    if (resp->val_len > PK_MAX_VAL_LEN)
        return -1;
    if (resp->val_len > 0 && (resp->val == NULL || resp->status != PK_STATUS_OK))
        return -1;

    size_t total = PK_RESP_HEADER_LEN + resp->val_len;
    if (cap < total)
        return -1;

    buf[0] = resp->status;
    wr_u32(buf + 1, resp->val_len);
    if (resp->val_len > 0)
        memcpy(buf + PK_RESP_HEADER_LEN, resp->val, resp->val_len);

    return (int)total;
}

const char *pk_opcode_name(uint8_t opcode)
{
    switch (opcode) {
    case PK_OP_GET: return "GET";
    case PK_OP_SET: return "SET";
    case PK_OP_DEL: return "DEL";
    default:        return "??";
    }
}

const char *pk_status_name(uint8_t status)
{
    switch (status) {
    case PK_STATUS_OK:        return "OK";
    case PK_STATUS_NOT_FOUND: return "NOT_FOUND";
    case PK_STATUS_ERROR:     return "ERROR";
    default:                  return "??";
    }
}
