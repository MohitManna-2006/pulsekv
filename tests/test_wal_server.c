#define _POSIX_C_SOURCE 200809L

/*
 * End-to-end durability ordering check. Run beside the server with both
 * processes pointing PULSEKV_WAL_PATH at the same test file. A mutation reply
 * is not accepted until its exact checksummed record is visible in that file;
 * the following GET then verifies that WAL completion preceded table apply.
 */

#include "protocol.h"
#include "wal.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#define SERVER_HOST "127.0.0.1"
#define SERVER_PORT 9999

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

static bool write_all(int fd, const uint8_t *buf, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, buf + sent, len - sent);
        if (n > 0) {
            sent += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        return false;
    }
    return true;
}

static bool read_exact(int fd, uint8_t *buf, size_t len)
{
    size_t have = 0;
    while (have < len) {
        ssize_t n = read(fd, buf + have, len - have);
        if (n > 0) {
            have += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        return false;
    }
    return true;
}

static bool request(int fd, uint8_t opcode,
                    const uint8_t *key, uint32_t key_len,
                    const uint8_t *val, uint32_t val_len,
                    uint8_t want_status, const uint8_t *want_val,
                    uint32_t want_val_len)
{
    pk_request_t req = {
        .opcode  = opcode,
        .key_len = key_len,
        .key     = key,
        .val_len = val_len,
        .val     = val_len > 0 ? val : NULL,
    };
    uint8_t request_buf[PK_MAX_REQ_LEN];
    int n = pk_request_encode(&req, request_buf, sizeof(request_buf));
    if (n < 0 || !write_all(fd, request_buf, (size_t)n))
        return false;

    uint8_t header[PK_RESP_HEADER_LEN];
    if (!read_exact(fd, header, sizeof(header)))
        return false;

    /* Read the length without depending on host byte order. */
    uint32_t got_len = ((uint32_t)header[1] << 24) | ((uint32_t)header[2] << 16)
                     | ((uint32_t)header[3] << 8) | (uint32_t)header[4];
    if (header[0] != want_status || got_len != want_val_len
        || got_len > PK_MAX_VAL_LEN)
        return false;

    uint8_t response_val[PK_MAX_VAL_LEN];
    if (got_len > 0 && !read_exact(fd, response_val, got_len))
        return false;
    return got_len == 0 || memcmp(response_val, want_val, got_len) == 0;
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
    uint8_t *data = malloc(len > 0 ? len : 1);
    if (data == NULL) {
        close(fd);
        return NULL;
    }

    size_t have = 0;
    while (have < len) {
        ssize_t n = read(fd, data + have, len - have);
        if (n > 0) {
            have += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        free(data);
        close(fd);
        return NULL;
    }
    close(fd);
    *len_out = len;
    return data;
}

static bool wal_contains(const char *path, uint8_t opcode,
                         const uint8_t *key, uint32_t key_len,
                         const uint8_t *val, uint32_t val_len)
{
    size_t len = 0;
    uint8_t *data = read_file(path, &len);
    if (data == NULL)
        return false;

    bool found = false;
    size_t offset = 0;
    while (offset < len) {
        pk_wal_record_t record;
        size_t consumed = 0;
        if (pk_wal_record_decode(data + offset, len - offset,
                                 &record, &consumed) != PK_WAL_DECODE_OK) {
            found = false;
            break;
        }
        if (record.opcode == opcode && record.key_len == key_len
            && memcmp(record.key, key, key_len) == 0
            && record.val_len == val_len
            && (val_len == 0 || memcmp(record.val, val, val_len) == 0))
            found = true;
        offset += consumed;
    }
    free(data);
    return found;
}

static int connect_server(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    struct timeval timeout = {.tv_sec = 5};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_HOST, &addr.sin_addr) != 1
        || connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int main(int argc, char **argv)
{
    const char *path = getenv("PULSEKV_WAL_PATH");
    if (path == NULL || path[0] == '\0')
        path = PK_WAL_DEFAULT_PATH;

    printf("=== server WAL-before-apply contract ===\n");
    int fd = connect_server();
    check(fd >= 0, "connected to the epoll server");
    if (fd < 0)
        goto done;

    if (argc == 2 && strcmp(argv[1], "--seed-recovery") == 0) {
        static const uint8_t restart_key[] = "step7-restart-key";
        static const uint8_t restart_val[] = "survives-process-restart";
        check(request(fd, PK_OP_SET, restart_key, sizeof(restart_key) - 1,
                      restart_val, sizeof(restart_val) - 1,
                      PK_STATUS_OK, NULL, 0),
              "seed mutation is durable before the first process exits");
        close(fd);
        goto done;
    }

    if (argc == 2 && strcmp(argv[1], "--verify-recovery") == 0) {
        static const uint8_t restart_key[] = "step7-restart-key";
        static const uint8_t restart_val[] = "survives-process-restart";
        static const uint8_t continued_key[] = "step7-after-restart";
        static const uint8_t continued_val[] = "sequence-continued";
        check(request(fd, PK_OP_GET, restart_key, sizeof(restart_key) - 1,
                      NULL, 0, PK_STATUS_OK,
                      restart_val, sizeof(restart_val) - 1),
              "new process serves a value rebuilt from the WAL");
        check(request(fd, PK_OP_SET, continued_key, sizeof(continued_key) - 1,
                      continued_val, sizeof(continued_val) - 1,
                      PK_STATUS_OK, NULL, 0),
              "new mutation appends after the recovered sequence");
        check(request(fd, PK_OP_GET, continued_key, sizeof(continued_key) - 1,
                      NULL, 0, PK_STATUS_OK,
                      continued_val, sizeof(continued_val) - 1),
              "continued mutation is visible in the rebuilt table");
        close(fd);
        goto done;
    }

    if (argc != 1) {
        fprintf(stderr, "usage: %s [--seed-recovery|--verify-recovery]\n", argv[0]);
        g_failed++;
        close(fd);
        goto done;
    }

    char key_text[64];
    char val_text[64];
    int key_len = snprintf(key_text, sizeof(key_text), "wal-e2e-key-%ld", (long)getpid());
    int val_len = snprintf(val_text, sizeof(val_text), "durable-value-%ld", (long)getpid());
    const uint8_t *key = (const uint8_t *)key_text;
    const uint8_t *val = (const uint8_t *)val_text;

    if (getenv("PULSEKV_EXPECT_WAL_FAILURE") != NULL) {
        check(request(fd, PK_OP_SET, key, (uint32_t)key_len,
                      val, (uint32_t)val_len, PK_STATUS_ERROR, NULL, 0),
              "SET reports ERROR when WAL append fails");
        check(request(fd, PK_OP_GET, key, (uint32_t)key_len, NULL, 0,
                      PK_STATUS_NOT_FOUND, NULL, 0),
              "failed WAL append did not mutate the in-memory table");
        check(request(fd, PK_OP_DEL, key, (uint32_t)key_len, NULL, 0,
                      PK_STATUS_ERROR, NULL, 0),
              "sticky WAL failure rejects later mutations too");
        close(fd);
        goto done;
    }

    check(request(fd, PK_OP_SET, key, (uint32_t)key_len, val, (uint32_t)val_len,
                  PK_STATUS_OK, NULL, 0),
          "SET receives OK after asynchronous completion");
    check(wal_contains(path, PK_OP_SET, key, (uint32_t)key_len,
                       val, (uint32_t)val_len),
          "SET record is complete and CRC-valid when OK is observed");
    check(request(fd, PK_OP_GET, key, (uint32_t)key_len, NULL, 0,
                  PK_STATUS_OK, val, (uint32_t)val_len),
          "GET observes the value applied after WAL durability");

    check(request(fd, PK_OP_DEL, key, (uint32_t)key_len, NULL, 0,
                  PK_STATUS_OK, NULL, 0),
          "DEL receives OK after asynchronous completion");
    check(wal_contains(path, PK_OP_DEL, key, (uint32_t)key_len, NULL, 0),
          "DEL record is complete and CRC-valid when OK is observed");
    check(request(fd, PK_OP_GET, key, (uint32_t)key_len, NULL, 0,
                  PK_STATUS_NOT_FOUND, NULL, 0),
          "GET observes the delete applied after WAL durability");
    close(fd);

done:
    printf("\nRESULT: %s (%d checks, %d failed)\n",
           g_failed == 0 ? "PASS" : "FAIL", g_checks, g_failed);
    return g_failed == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
