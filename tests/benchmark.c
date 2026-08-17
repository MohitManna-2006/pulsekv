#define _GNU_SOURCE

/*
 * PulseKV build step 8: 500-connection latency and throughput benchmark.
 *
 * The load generator is itself event-driven. One epoll loop owns every client
 * socket, keeping one request outstanding per connection. This models 500
 * concurrent closed-loop clients without adding the scheduler noise and 500
 * large stacks of a thread-per-client generator to the measured tail latency.
 *
 * Usage:
 *   benchmark [--clients N] [--requests N] [--warmup N]
 *             [--workload read|mixed|write]
 */

#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define SERVER_HOST              "127.0.0.1"
#define SERVER_PORT              9999
#define DEFAULT_CLIENTS          500u
#define DEFAULT_REQUESTS         1000u
#define DEFAULT_WARMUP           50u
#define MAX_CLIENTS              4096u
#define MAX_REQUESTS_PER_CLIENT  1000000u
#define MAX_EVENTS               1024
#define VALUE_CAP                96u
#define REQUEST_CAP              256u
#define RESPONSE_CAP             (PK_RESP_HEADER_LEN + VALUE_CAP)

#define TARGET_REQUESTS_PER_SEC  25000.0
#define TARGET_P99_NS            5000000u

typedef enum {
    WORKLOAD_READ,
    WORKLOAD_MIXED,
    WORKLOAD_WRITE
} workload_t;

typedef enum {
    STAGE_SEED,
    STAGE_WARMUP,
    STAGE_WAITING,
    STAGE_MEASURED,
    STAGE_DONE
} stage_t;

typedef struct {
    uint64_t ns;
    uint8_t  opcode;
} sample_t;

typedef struct {
    size_t     clients;
    size_t     requests;
    size_t     warmup;
    workload_t workload;
} config_t;

typedef struct benchmark benchmark_t;

typedef struct {
    benchmark_t *benchmark;
    size_t       id;
    int          fd;
    stage_t      stage;
    char         key[48];

    uint8_t  expected_value[VALUE_CAP];
    uint32_t expected_value_len;
    bool     present;

    uint8_t  pending_value[VALUE_CAP];
    uint32_t pending_value_len;
    uint8_t  current_opcode;
    uint8_t  expected_status;

    uint8_t request_buf[REQUEST_CAP];
    size_t  request_len;
    size_t  request_sent;
    bool    want_write;

    uint8_t response_buf[RESPONSE_CAP];
    size_t  response_have;
    size_t  response_need;

    size_t   warmup_completed;
    size_t   measured_completed;
    uint64_t operation_start_ns;
    char     failure[192];
} client_t;

struct benchmark {
    config_t  config;
    int       epfd;
    client_t *clients;
    sample_t *samples;
    size_t    ready;
    size_t    done;
    size_t    completed;
    uint64_t  start_ns;
    uint64_t  finish_ns;
    bool      failed;
};

static uint64_t monotonic_ns(void)
{
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
        return 0;
    return (uint64_t)now.tv_sec * 1000000000u + (uint64_t)now.tv_nsec;
}

static const char *workload_name(workload_t workload)
{
    switch (workload) {
    case WORKLOAD_READ:  return "read (100% GET)";
    case WORKLOAD_MIXED: return "mixed (90% GET / 8% SET / 2% DEL)";
    case WORKLOAD_WRITE: return "write (100% durable SET)";
    default:             return "unknown";
    }
}

static bool parse_count(const char *text, size_t minimum, size_t maximum,
                        size_t *out)
{
    if (text == NULL || text[0] == '\0')
        return false;
    errno = 0;
    char *end = NULL;
    unsigned long long value = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0'
        || value < minimum || value > maximum)
        return false;
    *out = (size_t)value;
    return true;
}

static void usage(const char *program)
{
    fprintf(stderr,
            "usage: %s [--clients 1-%u] [--requests 1-%u] "
            "[--warmup 0-%u] [--workload read|mixed|write]\n",
            program, MAX_CLIENTS, MAX_REQUESTS_PER_CLIENT,
            MAX_REQUESTS_PER_CLIENT);
}

static bool parse_args(int argc, char **argv, config_t *config)
{
    *config = (config_t){
        .clients  = DEFAULT_CLIENTS,
        .requests = DEFAULT_REQUESTS,
        .warmup   = DEFAULT_WARMUP,
        .workload = WORKLOAD_MIXED,
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--clients") == 0 && i + 1 < argc) {
            if (!parse_count(argv[++i], 1, MAX_CLIENTS, &config->clients))
                return false;
        } else if (strcmp(argv[i], "--requests") == 0 && i + 1 < argc) {
            if (!parse_count(argv[++i], 1, MAX_REQUESTS_PER_CLIENT,
                             &config->requests))
                return false;
        } else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc) {
            if (!parse_count(argv[++i], 0, MAX_REQUESTS_PER_CLIENT,
                             &config->warmup))
                return false;
        } else if (strcmp(argv[i], "--workload") == 0 && i + 1 < argc) {
            const char *name = argv[++i];
            if (strcmp(name, "read") == 0)
                config->workload = WORKLOAD_READ;
            else if (strcmp(name, "mixed") == 0)
                config->workload = WORKLOAD_MIXED;
            else if (strcmp(name, "write") == 0)
                config->workload = WORKLOAD_WRITE;
            else
                return false;
        } else {
            return false;
        }
    }
    return true;
}

static bool fail_client(client_t *client, const char *message)
{
    if (client->failure[0] == '\0')
        snprintf(client->failure, sizeof(client->failure), "%s", message);
    client->benchmark->failed = true;
    return false;
}

static uint32_t make_value(client_t *client, uint64_t sequence,
                           uint8_t value[VALUE_CAP])
{
    int n = snprintf((char *)value, VALUE_CAP, "value-client-%04zu-op-%010llu",
                     client->id, (unsigned long long)sequence);
    return n < 0 || (size_t)n >= VALUE_CAP ? 0 : (uint32_t)n;
}

static int connect_to_server(void)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0)
        return -1;

    int enabled = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) < 0) {
        close(fd);
        return -1;
    }

    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_HOST, &address.sin_addr) != 1
        || connect(fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        close(fd);
        return -1;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static bool update_interest(client_t *client, bool want_write)
{
    if (client->fd < 0 || client->want_write == want_write)
        return true;

    struct epoll_event event = {
        .events = EPOLLIN | EPOLLRDHUP | (want_write ? EPOLLOUT : 0u),
        .data.ptr = client,
    };
    if (epoll_ctl(client->benchmark->epfd, EPOLL_CTL_MOD,
                  client->fd, &event) < 0)
        return fail_client(client, "epoll interest update failed");
    client->want_write = want_write;
    return true;
}

static bool flush_request(client_t *client)
{
    if (client->operation_start_ns == 0 && client->stage == STAGE_MEASURED)
        client->operation_start_ns = monotonic_ns();

    while (client->request_sent < client->request_len) {
        ssize_t n = write(client->fd,
                          client->request_buf + client->request_sent,
                          client->request_len - client->request_sent);
        if (n > 0) {
            client->request_sent += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            return update_interest(client, true);
        return fail_client(client, "request write failed");
    }
    return update_interest(client, false);
}

static bool prepare_request(client_t *client, uint8_t opcode,
                            const uint8_t *value, uint32_t value_len,
                            uint8_t expected_status)
{
    pk_request_t request = {
        .opcode  = opcode,
        .key_len = (uint32_t)strlen(client->key),
        .key     = (const uint8_t *)client->key,
        .val_len = value_len,
        .val     = value_len > 0 ? value : NULL,
    };
    int encoded = pk_request_encode(&request, client->request_buf,
                                    sizeof(client->request_buf));
    if (encoded < 0)
        return fail_client(client, "request encoding failed");

    client->current_opcode = opcode;
    client->expected_status = expected_status;
    client->request_len = (size_t)encoded;
    client->request_sent = 0;
    client->response_have = 0;
    client->response_need = PK_RESP_HEADER_LEN;
    client->operation_start_ns = 0;
    return flush_request(client);
}

static bool prepare_set(client_t *client, uint64_t sequence)
{
    client->pending_value_len = make_value(client, sequence,
                                           client->pending_value);
    if (client->pending_value_len == 0)
        return fail_client(client, "value formatting failed");
    return prepare_request(client, PK_OP_SET, client->pending_value,
                           client->pending_value_len, PK_STATUS_OK);
}

static bool prepare_get(client_t *client)
{
    return prepare_request(client, PK_OP_GET, NULL, 0,
                           client->present ? PK_STATUS_OK
                                           : PK_STATUS_NOT_FOUND);
}

static bool prepare_delete(client_t *client)
{
    return prepare_request(client, PK_OP_DEL, NULL, 0, PK_STATUS_OK);
}

static bool prepare_operation(client_t *client, uint64_t sequence)
{
    workload_t workload = client->benchmark->config.workload;
    if (workload == WORKLOAD_READ)
        return prepare_get(client);
    if (workload == WORKLOAD_WRITE)
        return prepare_set(client, sequence);

    unsigned slot = (unsigned)((sequence + client->id * 37u) % 100u);
    if (slot < 2)
        return prepare_delete(client);
    if (slot < 10)
        return prepare_set(client, sequence);
    return prepare_get(client);
}

static bool start_measurement(benchmark_t *benchmark)
{
    benchmark->start_ns = monotonic_ns();
    for (size_t i = 0; i < benchmark->config.clients; i++) {
        client_t *client = &benchmark->clients[i];
        if (client->stage != STAGE_WAITING)
            return fail_client(client, "client missed synchronized start");
        client->stage = STAGE_MEASURED;
        if (!prepare_operation(client, benchmark->config.warmup + 1u))
            return false;
    }
    return true;
}

static bool verify_response(client_t *client, uint8_t status,
                            const uint8_t *value, uint32_t value_len)
{
    if (status != client->expected_status)
        return fail_client(client, "response status was incorrect");

    uint32_t expected_len = 0;
    const uint8_t *expected = NULL;
    if (client->current_opcode == PK_OP_GET && client->present) {
        expected_len = client->expected_value_len;
        expected = client->expected_value;
    }
    if (value_len != expected_len
        || (value_len > 0 && memcmp(value, expected, value_len) != 0))
        return fail_client(client, "response value was incorrect");
    return true;
}

static void apply_completed_operation(client_t *client)
{
    if (client->current_opcode == PK_OP_SET) {
        memcpy(client->expected_value, client->pending_value,
               client->pending_value_len);
        client->expected_value_len = client->pending_value_len;
        client->present = true;
    } else if (client->current_opcode == PK_OP_DEL) {
        client->expected_value_len = 0;
        client->present = false;
    }
}

static bool finish_response(client_t *client)
{
    benchmark_t *benchmark = client->benchmark;
    uint8_t status = client->response_buf[0];
    uint32_t network_len;
    memcpy(&network_len, client->response_buf + 1, sizeof(network_len));
    uint32_t value_len = ntohl(network_len);
    const uint8_t *value = value_len > 0
                         ? client->response_buf + PK_RESP_HEADER_LEN : NULL;

    uint64_t finish = client->stage == STAGE_MEASURED ? monotonic_ns() : 0;
    if (!verify_response(client, status, value, value_len))
        return false;
    apply_completed_operation(client);

    if (client->stage == STAGE_SEED) {
        if (benchmark->config.warmup == 0) {
            client->stage = STAGE_WAITING;
            benchmark->ready++;
        } else {
            client->stage = STAGE_WARMUP;
            if (!prepare_operation(client, 1u))
                return false;
        }
    } else if (client->stage == STAGE_WARMUP) {
        client->warmup_completed++;
        if (client->warmup_completed == benchmark->config.warmup) {
            client->stage = STAGE_WAITING;
            benchmark->ready++;
        } else if (!prepare_operation(client, client->warmup_completed + 1u)) {
            return false;
        }
    } else if (client->stage == STAGE_MEASURED) {
        sample_t *sample = benchmark->samples
                         + client->id * benchmark->config.requests
                         + client->measured_completed;
        sample->ns = finish - client->operation_start_ns;
        sample->opcode = client->current_opcode;
        client->measured_completed++;
        benchmark->completed++;

        if (client->measured_completed == benchmark->config.requests) {
            client->stage = STAGE_DONE;
            benchmark->done++;
            if (benchmark->done == benchmark->config.clients)
                benchmark->finish_ns = finish;
            epoll_ctl(benchmark->epfd, EPOLL_CTL_DEL, client->fd, NULL);
            close(client->fd);
            client->fd = -1;
        } else if (!prepare_operation(
                       client, benchmark->config.warmup
                             + client->measured_completed + 1u)) {
            return false;
        }
    } else {
        return fail_client(client, "response arrived in an invalid phase");
    }

    if (benchmark->ready == benchmark->config.clients
        && benchmark->start_ns == 0)
        return start_measurement(benchmark);
    return true;
}

static bool read_responses(client_t *client)
{
    while (client->fd >= 0) {
        while (client->response_have < client->response_need) {
            ssize_t n = read(client->fd,
                             client->response_buf + client->response_have,
                             client->response_need - client->response_have);
            if (n > 0) {
                client->response_have += (size_t)n;
                continue;
            }
            if (n < 0 && errno == EINTR)
                continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
                return true;
            return fail_client(client, n == 0 ? "server closed the connection"
                                              : "response read failed");
        }

        if (client->response_need == PK_RESP_HEADER_LEN) {
            uint32_t network_len;
            memcpy(&network_len, client->response_buf + 1,
                   sizeof(network_len));
            uint32_t value_len = ntohl(network_len);
            if (value_len > VALUE_CAP)
                return fail_client(client, "response exceeded benchmark capacity");
            client->response_need = PK_RESP_HEADER_LEN + value_len;
            if (client->response_have < client->response_need)
                continue;
        }

        if (!finish_response(client))
            return false;
        if (client->stage == STAGE_WAITING || client->stage == STAGE_DONE)
            return true;
        /* finish_response may have sent the next request immediately. Try to
         * consume an already-arrived response before returning to epoll. */
    }
    return true;
}

static int compare_sample(const void *left, const void *right)
{
    const sample_t *a = left;
    const sample_t *b = right;
    return a->ns < b->ns ? -1 : a->ns > b->ns ? 1 : 0;
}

static size_t operation_count(const sample_t *samples, size_t count, int opcode)
{
    if (opcode < 0)
        return count;
    size_t matches = 0;
    for (size_t i = 0; i < count; i++)
        if (samples[i].opcode == (uint8_t)opcode)
            matches++;
    return matches;
}

static uint64_t percentile(const sample_t *samples, size_t count, int opcode,
                           size_t numerator, size_t denominator)
{
    size_t matches = operation_count(samples, count, opcode);
    if (matches == 0)
        return 0;
    size_t rank = (matches * numerator + denominator - 1u) / denominator;
    if (rank == 0)
        rank = 1;

    size_t seen = 0;
    for (size_t i = 0; i < count; i++) {
        if (opcode >= 0 && samples[i].opcode != (uint8_t)opcode)
            continue;
        if (++seen == rank)
            return samples[i].ns;
    }
    return samples[count - 1].ns;
}

static void print_latency(const char *label, const sample_t *samples,
                          size_t count, int opcode)
{
    size_t matches = operation_count(samples, count, opcode);
    if (matches == 0)
        return;
    uint64_t minimum = percentile(samples, count, opcode, 1, matches);
    uint64_t p50 = percentile(samples, count, opcode, 50, 100);
    uint64_t p99 = percentile(samples, count, opcode, 99, 100);
    uint64_t p999 = percentile(samples, count, opcode, 999, 1000);
    uint64_t maximum = percentile(samples, count, opcode, 1, 1);
    long double total = 0;
    for (size_t i = 0; i < count; i++)
        if (opcode < 0 || samples[i].opcode == (uint8_t)opcode)
            total += samples[i].ns;

    printf("  %-7s %8zu ops  min %8.3f ms  mean %8.3Lf ms  "
           "p50 %8.3f ms  p99 %8.3f ms  p999 %8.3f ms  max %8.3f ms\n",
           label, matches, (double)minimum / 1e6,
           total / (long double)matches / 1e6L,
           (double)p50 / 1e6, (double)p99 / 1e6,
           (double)p999 / 1e6, (double)maximum / 1e6);
}

static void close_clients(client_t *clients, size_t count)
{
    for (size_t i = 0; i < count; i++)
        if (clients[i].fd >= 0)
            close(clients[i].fd);
}

int main(int argc, char **argv)
{
    config_t config;
    if (!parse_args(argc, argv, &config)) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }
    if (config.clients > SIZE_MAX / config.requests
        || config.clients * config.requests > SIZE_MAX / sizeof(sample_t)) {
        fprintf(stderr, "sample count is too large\n");
        return EXIT_FAILURE;
    }

    benchmark_t benchmark = {
        .config = config,
        .epfd = -1,
    };
    size_t sample_count = config.clients * config.requests;
    benchmark.clients = calloc(config.clients, sizeof(*benchmark.clients));
    benchmark.samples = calloc(sample_count, sizeof(*benchmark.samples));
    struct epoll_event *events = calloc(
        config.clients < MAX_EVENTS ? config.clients : MAX_EVENTS,
        sizeof(*events));
    if (benchmark.clients == NULL || benchmark.samples == NULL || events == NULL) {
        perror("calloc");
        free(events);
        free(benchmark.samples);
        free(benchmark.clients);
        return EXIT_FAILURE;
    }
    for (size_t i = 0; i < config.clients; i++)
        benchmark.clients[i].fd = -1;

    benchmark.epfd = epoll_create1(EPOLL_CLOEXEC);
    if (benchmark.epfd < 0) {
        perror("epoll_create1");
        free(events);
        free(benchmark.samples);
        free(benchmark.clients);
        return EXIT_FAILURE;
    }

    printf("=== PulseKV step 8 load benchmark ===\n");
    printf("  driver: one epoll loop, one in-flight request per connection\n");
    long online_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    if (online_cpus > 0)
        printf("  environment: %ld online CPU%s visible\n", online_cpus,
               online_cpus == 1 ? "" : "s");
    printf("  workload: %s\n", workload_name(config.workload));
    printf("  clients: %zu, warmup: %zu/client, measured: %zu/client (%zu total)\n",
           config.clients, config.warmup, config.requests, sample_count);

    for (size_t i = 0; i < config.clients && !benchmark.failed; i++) {
        client_t *client = &benchmark.clients[i];
        client->benchmark = &benchmark;
        client->id = i;
        client->stage = STAGE_SEED;
        snprintf(client->key, sizeof(client->key), "benchmark-client-%04zu", i);
        client->fd = connect_to_server();
        if (client->fd < 0) {
            fprintf(stderr, "client %zu could not connect: %s\n", i, strerror(errno));
            benchmark.failed = true;
            break;
        }

        struct epoll_event event = {
            .events = EPOLLIN | EPOLLRDHUP,
            .data.ptr = client,
        };
        if (epoll_ctl(benchmark.epfd, EPOLL_CTL_ADD, client->fd, &event) < 0) {
            fail_client(client, "epoll add failed");
            break;
        }
    }
    if (!benchmark.failed)
        printf("  connections: %zu/%zu established\n", config.clients, config.clients);

    for (size_t i = 0; i < config.clients && !benchmark.failed; i++)
        if (!prepare_set(&benchmark.clients[i], 0))
            break;

    int event_capacity = (int)(config.clients < MAX_EVENTS
                             ? config.clients : MAX_EVENTS);
    while (!benchmark.failed && benchmark.done < config.clients) {
        int ready = epoll_wait(benchmark.epfd, events, event_capacity, -1);
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            benchmark.failed = true;
            break;
        }
        for (int i = 0; i < ready && !benchmark.failed; i++) {
            client_t *client = events[i].data.ptr;
            uint32_t flags = events[i].events;
            if (flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                fail_client(client, "connection closed during benchmark");
                break;
            }
            if ((flags & EPOLLOUT) && !flush_request(client))
                break;
            if ((flags & EPOLLIN) && !read_responses(client))
                break;
        }
    }

    if (benchmark.failed) {
        for (size_t i = 0; i < config.clients; i++)
            if (benchmark.clients[i].failure[0] != '\0') {
                fprintf(stderr, "client %zu: %s after %zu measured requests\n",
                        i, benchmark.clients[i].failure,
                        benchmark.clients[i].measured_completed);
                break;
            }
    }

    bool correct = !benchmark.failed && benchmark.completed == sample_count;
    if (correct) {
        qsort(benchmark.samples, sample_count,
              sizeof(*benchmark.samples), compare_sample);
        double elapsed = (double)(benchmark.finish_ns - benchmark.start_ns) / 1e9;
        double throughput = elapsed > 0 ? (double)sample_count / elapsed : 0;

        printf("\n=== measured results ===\n");
        printf("  completed: %zu/%zu correct responses in %.3f seconds\n",
               benchmark.completed, sample_count, elapsed);
        printf("  throughput: %.0f requests/second\n", throughput);
        print_latency("overall", benchmark.samples, sample_count, -1);
        print_latency("GET", benchmark.samples, sample_count, PK_OP_GET);
        print_latency("SET", benchmark.samples, sample_count, PK_OP_SET);
        print_latency("DEL", benchmark.samples, sample_count, PK_OP_DEL);

        uint64_t p99 = percentile(benchmark.samples, sample_count, -1, 99, 100);
        printf("\n=== design targets (scorecard, not correctness gates) ===\n");
        printf("  throughput >= 25,000 req/s: %s (%.0f)\n",
               throughput >= TARGET_REQUESTS_PER_SEC ? "MET" : "MISS", throughput);
        printf("  overall p99 < 5.000 ms:   %s (%.3f ms)\n",
               p99 < TARGET_P99_NS ? "MET" : "MISS", (double)p99 / 1e6);
    }

    printf("\nRESULT: %s (%zu measured requests, %zu clients)\n",
           correct ? "PASS" : "FAIL", benchmark.completed, config.clients);

    close_clients(benchmark.clients, config.clients);
    close(benchmark.epfd);
    free(events);
    free(benchmark.samples);
    free(benchmark.clients);
    return correct ? EXIT_SUCCESS : EXIT_FAILURE;
}
