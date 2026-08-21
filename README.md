# PulseKV

Distributed, tiered KV cache store for LLM serving engines with SGLang HiCache and vLLM KVConnector integrations and single-node C storage core.

> **PulseKV v2 Status: Phase 9 (Complete — Final Milestone)** — Production-ready distributed KV-cache for LLM inference serving. Features a 3-replica Raft consensus control plane in Go, C data nodes with RAM/NVMe tiering & zero-copy bulk transport, SGLang HiCache and vLLM KVConnector v1 adapters (100% verified cross-replica hits), Prometheus exporter (179 metrics), Zipf load generator, and long-duration chaos soak harness.

> **Semantic Context Gateway: Phase 10.6 complete** — The fail-open gateway is
> proven against two real SGLang 0.5.15 serving processes with concrete
> cross-replica PulseKV key correlation. See [`gateway/README.md`](gateway/README.md)
> and the [post-hoc Phase 10.6 evidence reconstruction](docs/pulsekv-semantic-context-phase10.6-summary.md),
> which was written after—not included in—the original Phase 10.6 merge.

---

## Quickstart (PulseKV v2 Commands)

Type `make` or any of the following commands directly from your terminal (Docker container is auto-detected and wrapped automatically on macOS):

| Command | Action |
| :--- | :--- |
| **`make test`** | Run full test suite (smoke test + engine tests + Python adapter tests) |
| **`make test-adapter`** | Run Python client SDK, SGLang HiCache, and vLLM adapter tests |
| **`make demo`** | Run SGLang and vLLM cross-replica prefix cache hit demos |
| **`make demo-sglang`** | Run SGLang HiCache cross-replica demo (10 trials @ 512 tokens) |
| **`make demo-vllm`** | Run vLLM KVConnector cross-replica demo (10 trials @ 512 tokens, 16 layers) |
| **`make bench`** | Run bulk transport zero-copy vs socket benchmarks |
| **`make chaos`** | Run node crash & Raft leader failover fault-injection tests |
| **`make soak`** | Run sustained Zipf load + interleaved chaos soak test harness |
| **`make start`** / **`make up`** | Boot 4-node local cluster with 3-replica Raft control plane |
| **`make stop`** / **`make down`** | Stop all cluster processes cleanly |
| **`make status`** | Show cluster leader and service status |
| **`make shell`** | Open interactive bash shell inside dev container |
| **`make help`** | Show all available commands |


---

## Overview

PulseKV is a TCP key-value store built from scratch to hit **25K+ req/sec, 500 concurrent clients, <5ms p99** on a single Linux box. Every layer — wire framing, hash table, event loop, threading — is hand-rolled in C11 on `epoll` + `pthreads`.

```
Client(s) --TCP--> 16 SO_REUSEPORT listeners (one epoll loop per worker)
                                      |
                         parse frame + dispatch
                          /                 \
                       GET                 SET / DEL
                        |                      |
              sharded hash table       synchronized FIFO
            (256 locks / 1,024 buckets)        |
                        ^              dedicated WAL writer
                        |                      |
                 durable completion     write + fdatasync
                        |                      |
                 worker eventfd          pulsekv.log
```

---

## Build Progress

| Step | Description | State |
|------|-------------|-------|
| 1 | Blocking TCP skeleton + wire protocol | Done |
| 2 | Single-threaded epoll (level-triggered → edge-triggered) | Done |
| 3 | In-memory hash table, single global mutex | Done |
| 4 | Thread pool over epoll (16 threads, thread-per-core) | Done |
| 5 | Sharded table (256 striped mutexes over 1,024 buckets) | Done |
| 6 | Append-only WAL with checksummed async group commit | Done |
| 7 | Crash recovery with batched replay and tail repair | Done |
| 8 | Load test, latency benchmark, and measured hot-path optimization | **Done — current** |

The complete design and benchmark methodology are in [`docs/system-design.md`](docs/system-design.md).
For a shorter visual explanation of how the pieces fit together, see
[`docs/architecture-guide.md`](docs/architecture-guide.md).

---

## Architecture

### Wire Protocol — `include/protocol.h` / `src/protocol.c`

Binary, fixed-layout, zero-copy on decode. Decoded frames are **views** into the caller's buffer — no allocation.

```
request:  [1B opcode][4B key_len][key][4B val_len][val]       (network byte order for lengths)
response: [1B status][4B val_len][val]
```

| Field | Values |
|-------|--------|
| Opcode | `GET 0x01`, `SET 0x02`, `DEL 0x03` |
| Status | `OK 0x00`, `NOT_FOUND 0x01`, `ERROR 0x02` |

- `key_len` always present; `val_len` always present. Only `SET` carries value bytes — `GET`/`DEL` send `val_len == 0`.
- Header sizes: `PK_REQ_HEADER_LEN = 9`, `PK_RESP_HEADER_LEN = 5`.
- Bounds: `PK_MAX_KEY_LEN = 1024`, `PK_MAX_VAL_LEN = 64 KiB`. A bogus length field cannot force a large allocation.
- `PK_DECODE_OK / INCOMPLETE / ERROR` — incomplete means "need more bytes, buffer still valid"; error means "answer ERROR and drop the connection".
- Encode/decode helpers: `pk_request_decode`, `pk_response_decode`, `pk_request_encode`, `pk_response_encode`, `pk_opcode_name`, `pk_status_name`.

### Storage Engine — `include/hashtable.h` / `src/hashtable.c`

Separate-chaining hash table with 1,024 physical buckets striped across 256 lock shards.

- **Hash:** FNV-1a over raw key bytes. `global_bucket = hash & 1023`, `shard = global_bucket & 255`, and `bucket_in_shard = global_bucket / 256`.
- **Concurrency:** each shard owns one `pthread_mutex_t` and four bucket chains. Every request takes exactly one of 256 locks, so unrelated shards proceed concurrently while bucket capacity and lock granularity remain independently tunable.
- **Count:** one relaxed atomic entry counter provides exact statistics without taking every shard lock or serializing key/value access.
- **Ownership:** `pk_table_set` **copies** key and value in. `pk_table_get` **copies out** into a caller buffer (never hands out an interior pointer — another thread's `DEL` could free the node the instant the lock drops).
- **Node layout:** `key[]` flex array rides inside the node allocation; `val` is a separate allocation so overwrites of different sizes don't require reallocating the node.
- **API:**

  ```c
  int                pk_table_init(pk_table_t *t);
  void               pk_table_destroy(pk_table_t *t);
  pk_table_result_t  pk_table_set(t, key,klen, val,vlen);   // OK / NOMEM / INVALID
  pk_table_result_t  pk_table_get(t, key,klen, out,cap, out_len); // OK / NOT_FOUND / TOO_BIG / INVALID
  pk_table_result_t  pk_table_del(t, key,klen);              // OK / NOT_FOUND / INVALID
  size_t             pk_table_count(t);
  uint64_t           pk_table_hash(key, klen);
  ```
- **Limitation:** fixed bucket array, no growth/rehash. Past ~1K live keys chains lengthen toward O(n) — intentionally deferred.

### Concurrency Model — `src/main.c`

**Thread-per-core via `SO_REUSEPORT`** — 16 workers, each owns:

- its own listening socket on port `9999` (kernel hashes each incoming connection to exactly one worker's accept queue)
- its own `epoll` instance
- its own intrusive linked list of `conn_t`

Nothing about the event loop is shared. No cross-thread epoll contention, no thundering herd. The one shared logical store is a `pk_table_t`; requests contend only when their keys map to the same one of its 256 lock shards.

**Per-connection state (`conn_t`):**

- `rbuf[PK_MAX_REQ_LEN]` — bytes read but not yet parsed; `rhave` tracks fill.
- `wbuf[PK_MAX_RESP_LEN]` — queued responses; `wsent..wfill` is the unsent window.
- `want_write` / `read_stalled` — drive `EPOLLOUT` interest and backpressure.
- one optional owned WAL request — pauses later pipelined frames until that mutation is durable.

**Event loop highlights:**

- **Edge-triggered by default** — drains to `EAGAIN` on every `EPOLLIN`. `PULSEKV_LEVEL_TRIGGERED` compile flag keeps the level-triggered variant buildable for comparison.
- **Backpressure:** if `wbuf` has no room, the request stays in `rbuf` and is re-decoded after the socket drains. `SET`/`DEL` reserve reply space before being submitted exactly once; `GET` is idempotent and safe to re-run.
- **`EPOLLOUT` only while bytes are owed** — a writable socket would otherwise spin the loop.
- **Write coalescing:** `queue_response` appends to `wbuf`; `conn_flush` loops `write()` until `EAGAIN`; `wbuf_compact` reclaims head space.
- **Shutdown:** `SIGINT`/`SIGTERM` → `atomic_int g_stop` + shared `eventfd`. Workers close listeners and sockets, retain connection shells with outstanding WAL requests, drain their completion eventfds, and only then exit. Main joins workers before stopping the writer and destroying the table. `SIGPIPE` is ignored.
- **Startup barrier:** `g_start_lock`/`g_start_cond` gate the "listening" announcement until every configured worker has registered its epoll set. Sixteen remains the default and maximum; `PULSEKV_THREADS` lets constrained environments preserve the intended thread-per-core ratio. Per-thread counters are published after `pthread_join`.

### Persistence — `include/wal.h` / `src/wal.c`

SET and DEL use an asynchronous write-ahead path; GET remains memory-only:

1. The epoll worker copies a mutation into a `pk_wal_request_t`, consumes its wire frame once, disables `EPOLLIN` for that connection, and submits it to the global FIFO.
2. The dedicated writer assigns a 64-bit sequence, waits for either 256 records or the 1 ms deadline, encodes a contiguous batch, appends it, and calls `fdatasync` once.
3. The writer pushes completions onto each originating worker's lock-free atomic handoff and coalesces its `eventfd` wakeups.
4. On success, the worker applies the table mutation and responds `OK`; on failure it responds `ERROR` without touching memory. It then resumes later frames from the same connection in order.

Version-one record layout, with all integers big-endian:

```
[4B "PKWL"][2B version][1B opcode][1B flags]
[4B record_len][8B sequence][4B key_len][4B value_len]
[key][value][4B IEEE CRC32]
```

CRC32 covers the complete header and payload. Magic, version, flags, opcode, total length, key/value bounds, and checksum are all validated. The first disk error becomes sticky, so no later request can bypass a failed append. A client disconnect does not cancel a submitted mutation: if it becomes durable, the worker applies it but has no socket to answer.

### Crash Recovery — `pk_wal_recover` in `src/wal.c`

Before any listener opens, startup reads the WAL in 256 KiB chunks and rebuilds the sharded table in
record order. Records split across read boundaries remain buffered until complete. Each record must
pass magic, version, bounds, opcode, CRC32, and contiguous-sequence validation before replay.

The first invalid or incomplete record is treated as a crash boundary. PulseKV truncates the file
to the last valid byte and syncs the repair before starting the writer. New mutations continue at
`last_sequence + 1`. Startup prints record/byte counts, read calls, restored keys, repairs, and
elapsed time. A 20,000-record comparison takes 6 batched reads instead of 40,001 naive reads;
representative optimized builds recover in roughly 4 ms versus 20 ms for that baseline.

### Step 8 Benchmark and Optimization

`build/benchmark` is an epoll-based closed-loop load generator. It owns 500 persistent sockets,
keeps one request outstanding on each, validates every response, discards a configurable warmup,
then reports throughput plus min/mean/p50/p99/p999/max latency overall and per operation. Available
profiles are `read` (100% GET), `mixed` (90% GET, 8% SET, 2% DEL), and `write` (100% durable SET).

The benchmark drove evidence-backed hot-path changes without weakening durability: a 256-record
default group, dynamically sized WAL encode buffers, coalesced writer wakeups, lock-free
single-producer/single-consumer completion handoff, coalesced worker `eventfd` notifications,
`accept4`, `TCP_NODELAY`, and configurable worker count. `tests/run_benchmarks.sh` reserves one CPU
for the co-located load generator by default; `PULSEKV_BENCH_WORKERS` overrides that choice.

A representative Docker Desktop run exposed only two CPUs and used one server worker plus one load
generator loop. It sustained roughly 192K read, 178K mixed, and 113K fully durable write requests
per second across 500 connections. Throughput cleared the 25K goal in every profile. Tail latency
remains hardware and workload sensitive—especially when `fdatasync` is in the measured path—so the
benchmark prints the `<5 ms p99` scorecard but never hides a miss behind a correctness `PASS`.

---

## Getting Started

### Prerequisites

Linux is required (`epoll`, `eventfd`, `SO_REUSEPORT`). On macOS, build inside Docker:

```sh
docker build -t pulsekv-dev .
docker run --rm -v "$PWD:/src" -w /src pulsekv-dev make        # build
docker run --rm -v "$PWD:/src" -w /src pulsekv-dev make tsan   # ThreadSanitizer builds
```

Native Linux:

```sh
make          # all targets
make tsan     # TSAN-instrumented variants under build/tsan/
make clean
```

### Build Targets

| Binary | Description |
|--------|-------------|
| `build/pulsekv` | Server — edge-triggered (default) |
| `build/pulsekv_lt` | Server — level-triggered (`-DPULSEKV_LEVEL_TRIGGERED`) |
| `build/test_client` | Framing round-trip smoke test |
| `build/test_multi_client` | Concurrency / pipelining / reassembly test |
| `build/test_hashtable` | Direct hash table unit tests (no network) |
| `build/test_thread_stress` | Multi-threaded stress test |
| `build/test_wal` | WAL codec, CRC, concurrent producers, group commit, and disk-failure tests |
| `build/test_wal_server` | End-to-end WAL-before-table contract against a running server |
| `build/test_recovery` | Replay, restart sequence, tail repair, and batched-vs-naive recovery tests |
| `build/benchmark` | 500-connection epoll load generator with read/mixed/write workloads and latency percentiles |

Compiler: `cc`, `-Wall -Wextra -std=c11 -O2 -pthread`. TSAN: `-O1 -g -fsanitize=thread`.

### Running the Server

```sh
./build/pulsekv                          # 0.0.0.0:9999, 16 threads, edge-triggered
./build/pulsekv_lt                       # same, level-triggered
PULSEKV_QUIET=1 ./build/pulsekv          # suppress per-request log (for benchmarks)
PULSEKV_WAL_PATH=/tmp/test.wal ./build/pulsekv
PULSEKV_THREADS=4 ./build/pulsekv         # tune 1..16 workers to available CPUs
```

Logs each connection and request to stdout (single `fputs` per request to avoid interleaving). On `SIGINT`/`SIGTERM`, prints:

```
recovery: <records>, <valid/original bytes>, <read calls>, <keys>, <milliseconds>
shutdown: <conns> connections, <reqs> requests, <keys> keys resident
WAL: <records> records, <batches> batches/fsyncs, <bytes> bytes, largest batch <n>
per-thread connections: t00=.. t01=.. ...
```

### Manual Interaction

```sh
python3 tests/manual_cli.py
# pulsekv> SET foo bar
# OK
# pulsekv> GET foo
# OK -> bar
# pulsekv> DEL foo
# OK
```

The CLI speaks the binary protocol directly (`struct.pack("!BI", opcode, len)` + key + val_len + val).

---

## Testing

| Test | What it covers |
|------|----------------|
| `test_hashtable` | Sharded table in isolation: two-level routing, same-shard/different-chain keys, collision chains, ownership/error cases, and a 32-thread direct SET/GET/DEL stress test |
| `test_client` | Single-connection framing round-trip (step 1 — proves encode/decode + TCP transport) |
| `test_multi_client` | 5 concurrent clients: truncated-frame stall, pipelined lifecycle (SET→GET→cross-read→overwrite→DEL), in-order pipelining, stalled-connection resume, 80-frame burst reassembly |
| `test_thread_stress` | Multi-threaded load against the 16-worker server |
| `test_wal` | Record/version/CRC validation, every truncated prefix, binary keys and values, 8 concurrent producers, FIFO sequences, group-commit accounting, and `/dev/full` sticky failure |
| `test_wal_server` | Successful replies have complete CRC-valid WAL records before table visibility; failure mode proves an append error never changes memory |
| `test_recovery` | SET/overwrite/DEL and binary replay, sequence handoff, truncated/CRC/sequence tail repair, and 20,000-record batched-vs-naive comparison |
| `benchmark` | 500 persistent connections, verified responses, configurable warmup/workload, throughput, and min/mean/p50/p99/p999/max latency |

Run all three benchmark profiles in an isolated temporary WAL:

```sh
# native Linux, or inside the Docker image
make bench

# macOS host: the server and load generator run together inside Linux
docker run --rm -v "$PWD:/src" -w /src pulsekv-dev make bench

# force a worker count or shorten an instrumentation run
PULSEKV_BENCH_WORKERS=4 PULSEKV_BENCH_REQUESTS=200 make bench
```

Run under valgrind/TSAN for heap and race coverage:

```sh
# inside Docker (valgrind available)
valgrind ./build/test_hashtable
valgrind ./build/test_wal
valgrind ./build/test_recovery
./build/tsan/test_thread_stress
```

---

## Project Structure

```
pulsekv/
├── src/
│   ├── main.c          # measured/tuned epoll + WAL state machines (step 8)
│   ├── hashtable.c     # 1,024 buckets / 256 lock shards + FNV-1a
│   ├── protocol.c      # binary frame encode/decode
│   └── wal.c           # async group-commit writer + v1 record codec
├── include/
│   ├── hashtable.h
│   ├── protocol.h
│   └── wal.h
├── tests/
│   ├── test_hashtable.c
│   ├── test_client.c
│   ├── test_multi_client.c
│   ├── test_thread_stress.c
│   ├── test_wal.c
│   ├── test_wal_server.c
│   ├── test_recovery.c
│   ├── benchmark.c     # 500-socket epoll latency/throughput driver
│   ├── run_benchmarks.sh
│   └── manual_cli.py   # interactive REPL over the wire protocol
├── docs/
│   ├── architecture-guide.md
│   └── system-design.md
├── Dockerfile          # Debian bookworm + gcc/make/valgrind (glibc, not musl)
├── Makefile
└── build/              # gitignored — binaries + build/tsan/
```

~7K lines total (C + headers + tests + docs).

---

## Configuration

| Knob | Default | Where |
|------|---------|-------|
| Listen port | `9999` | `PULSEKV_PORT` in `src/main.c` |
| Worker threads | `16` (range `1..16`) | `PULSEKV_THREADS` env var |
| Listen backlog | `512` | `LISTEN_BACKLOG` |
| Buckets | `1024` | `PK_TABLE_BUCKETS` in `include/hashtable.h` |
| Lock shards | `256` | `PK_TABLE_SHARDS` in `include/hashtable.h` |
| Max key / val | `1 KiB / 64 KiB` | `PK_MAX_KEY_LEN / PK_MAX_VAL_LEN` in `include/protocol.h` |
| Trigger mode | edge-triggered | `PULSEKV_LEVEL_TRIGGERED` compile flag |
| Quiet log | off | `PULSEKV_QUIET` env var |
| WAL path | `pulsekv.log` | `PULSEKV_WAL_PATH` env var |
| WAL batch size | `256` records | `PULSEKV_WAL_BATCH_MAX` env var |
| WAL batch delay | `1000` µs | `PULSEKV_WAL_DELAY_US` env var |
| Recovery read chunk | `256 KiB` | `PULSEKV_RECOVERY_CHUNK` env var |
| Skip recovery | unset | `PULSEKV_SKIP_RECOVERY`; fault injection only, never normal operation |

---

## Roadmap

- **Step 5 — Sharded table (complete):** 1,024 bucket chains striped across 256 mutex shards; one atomic entry counter; removes unrelated-key contention without multiplying the table into 256 oversized copies.
- **Step 6 — WAL (complete):** versioned, length-delimited, sequenced, CRC32-protected records; dedicated writer; measured 256-record/1 ms group commit; per-worker completion eventfds; WAL-before-table ordering; sticky disk errors; two-phase shutdown drain.
- **Step 7 — Recovery (complete):** 256 KiB sequential reads, ordered table rebuild, contiguous sequence handoff, CRC/truncation detection, physical tail repair, and a deterministic read-syscall comparison against naive replay.
- **Step 8 — Benchmark and optimization (complete):** one epoll load generator drives 500 verified connections across read/mixed/write profiles; reports throughput and min/mean/p50/p99/p999/max; tuning doubled durable throughput versus the original 64-record batch in local comparison runs.

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Threading | Up to 16 thread-per-core workers, each with its own epoll and `SO_REUSEPORT` listener | Predictable scaling without cross-thread epoll contention; worker count can match constrained CPU allocations |
| Store lock | 256 striped mutex shards over 1,024 buckets | Removes unrelated-key contention while keeping bucket capacity and lock granularity independently tunable |
| Durability | Group-commit over per-write fsync | Throughput target requires batching; same trade-off as Kafka/Postgres |
| WAL execution | Dedicated writer + per-worker completion eventfds instead of filesystem calls in epoll workers | Keeps filesystem latency off every networking loop while retaining deterministic append order |
| Recovery I/O | 256 KiB sequential reads over record-at-a-time reads | Rebuilds the same state with dramatically fewer syscalls and repairs the physical tail before append resumes |
| Load generation | One epoll loop over 500 sockets | Measures server latency without adding 500 co-located client threads and their scheduler noise |
| Protocol | Binary fixed-layout, views not copies | No allocation on parse; table copies only after validation |

See [`docs/system-design.md`](docs/system-design.md) for the full trade-off analysis and failure-mode handling.
