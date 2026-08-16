/*
 * Framing round-trip check for build step 1.
 *
 * Sends SET foo=bar then GET foo and prints what comes back. The server has no
 * storage yet, so the expected answers are OK for the SET and NOT_FOUND for the
 * GET -- what this proves is that frames encode, travel, and decode intact, not
 * that anything was stored.
 */

#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define SERVER_HOST "127.0.0.1"
#define SERVER_PORT 9999

static bool write_all(int fd, const uint8_t *buf, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, buf + sent, len - sent);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("write");
            return false;
        }
        sent += (size_t)n;
    }
    return true;
}

static bool send_request(int fd, pk_opcode_t opcode, const char *key, const char *val)
{
    pk_request_t req = {
        .opcode  = (uint8_t)opcode,
        .key_len = (uint32_t)strlen(key),
        .key     = (const uint8_t *)key,
        .val_len = val ? (uint32_t)strlen(val) : 0,
        .val     = (const uint8_t *)val,
    };

    uint8_t buf[PK_MAX_REQ_LEN];
    int n = pk_request_encode(&req, buf, sizeof(buf));
    if (n < 0) {
        fprintf(stderr, "encode failed for %s\n", pk_opcode_name(req.opcode));
        return false;
    }

    printf("-> %s key=\"%s\"", pk_opcode_name(req.opcode), key);
    if (val)
        printf(" val=\"%s\"", val);
    printf("  (%d bytes on the wire)\n", n);

    return write_all(fd, buf, (size_t)n);
}

/* Read until one whole response frame has arrived, then print it. */
static bool recv_response(int fd)
{
    uint8_t buf[PK_MAX_RESP_LEN];
    size_t have = 0;

    for (;;) {
        pk_response_t resp;
        size_t consumed = 0;
        pk_decode_result_t r = pk_response_decode(buf, have, &resp, &consumed);

        if (r == PK_DECODE_OK) {
            printf("<- status=%s val_len=%u", pk_status_name(resp.status), resp.val_len);
            if (resp.val_len > 0)
                printf(" val=\"%.*s\"", (int)resp.val_len, (const char *)resp.val);
            printf("  (%zu bytes on the wire)\n", consumed);
            return true;
        }
        if (r == PK_DECODE_ERROR) {
            fprintf(stderr, "<- malformed response frame\n");
            return false;
        }

        ssize_t n = read(fd, buf + have, sizeof(buf) - have);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("read");
            return false;
        }
        if (n == 0) {
            fprintf(stderr, "<- server closed the connection mid-response\n");
            return false;
        }
        have += (size_t)n;
    }
}

static int connect_to_server(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_HOST, &addr.sin_addr) != 1) {
        fprintf(stderr, "bad server address %s\n", SERVER_HOST);
        close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    return fd;
}

int main(void)
{
    int fd = connect_to_server();
    if (fd < 0)
        return EXIT_FAILURE;

    printf("connected to %s:%d\n", SERVER_HOST, SERVER_PORT);

    bool ok = send_request(fd, PK_OP_SET, "foo", "bar") && recv_response(fd) &&
              send_request(fd, PK_OP_GET, "foo", NULL) && recv_response(fd);

    close(fd);

    if (!ok) {
        printf("round-trip FAILED\n");
        return EXIT_FAILURE;
    }
    printf("round-trip OK (framing verified; storage lands in step 3)\n");
    return EXIT_SUCCESS;
}
