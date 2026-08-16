# PulseKV

Concurrent, sharded, epoll-based key-value store in C — single-node, Linux-native, zero external dependencies.

> **Status: Step 4 of 8 complete** — thread-per-core server with shared in-memory store over epoll. Sharding, WAL, and crash recovery are designed but not yet implemented.

---

## Overview

PulseKV is a TCP key-value store built from scratch to hit **25K+ req/sec, 500 concurrent clients, <5ms p99** on a single Linux box. Every layer — wire framing, hash table, event loop, threading — is hand-rolled in C11 on `epoll` + `pthreads`.

```
Client(s) --TCP--> epoll event loop --dispatch--> worker thread pool (16 x thread-per-core)
                                                          |
                                          shard router: hash(key) % N  (planned)
                                                          |
                                    -------------------------------------
                                    |                                   |
                          sharded hash table                      WAL writer (planned)
                          (per-bucket mutex)                   (append-only log)
                                                                        |
                                                                  disk: pulsekv.log
```

---

## Build Progress

| Step | Description | State |
|------|-------------|-------|
| 1 | Blocking TCP skeleton + wire protocol | Done |
| 2 | Single-threaded epoll (level-triggered → edge-triggered) | Done |
| 3 | In-memory hash table, single global mutex | Done |
| 4 | Thread pool over epoll (16 threads, thread-per-core) | **Done — current** |
| 5 | Shard the hash table (per-bucket mutex) | Planned |
| 6 | Append-only WAL with checksummed records | Planned |
| 7 | Crash recovery with batched replay | Planned |
| 8 | Load test & benchmark (500 clients, p50/p99/p999) | Planned |

Full design for steps 5–8 is in [`docs/system-design.md`](docs/system-design.md).

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

Separate-chaining hash table, 1024 buckets (power of two, masked not modulo'd).

- **Hash:** FNV-1a over raw key bytes (`pk_table_hash` — public so the future shard router can reuse it).
- **Concurrency:** single `pthread_mutex_t` guarding the whole table. Correctness baseline for step 4; step 5 shards this into per-bucket locks.
- **Ownership:** `pk_table_set` **copies** key and value in. `pk_table_get` **copies out** into a caller buffer (never hands out an interior pointer — another thread's `DEL` could free the node the instant the lock drops). No singleton — step 5 creates an array of tables and routes on the hash.
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

### Concurrency Model — `src/main.c` (867 lines)

**Thread-per-core via `SO_REUSEPORT`** — 16 workers, each owns:

- its own listening socket on port `9999` (kernel hashes each incoming connection to exactly one worker's accept queue)
- its own `epoll` instance
- its own intrusive linked list of `conn_t`

Nothing about the event loop is shared. No cross-thread epoll contention, no thundering herd. The one shared object is the `pk_table_t` behind a single mutex.

**Per-connection state (`conn_t`):**

- `rbuf[PK_MAX_REQ_LEN]` — bytes read but not yet parsed; `rhave` tracks fill.
- `wbuf[PK_MAX_RESP_LEN]` — queued responses; `wsent..wfill` is the unsent window.
- `want_write` / `read_stalled` — drive `EPOLLOUT` interest and backpressure.

**Event loop highlights:**

- **Edge-triggered by default** — drains to `EAGAIN` on every `EPOLLIN`. `PULSEKV_LEVEL_TRIGGERED` compile flag keeps the level-triggered variant buildable for comparison.
- **Backpressure:** if `wbuf` has no room, the request stays in `rbuf` and is re-decoded after the socket drains. `SET`/`DEL` check room *before* executing; `GET` is idempotent and safe to re-run.
- **`EPOLLOUT` only while bytes are owed** — a writable socket would otherwise spin the loop.
- **Write coalescing:** `queue_response` appends to `wbuf`; `conn_flush` loops `write()` until `EAGAIN`; `wbuf_compact` reclaims head space.
- **Shutdown:** `SIGINT`/`SIGTERM` → `atomic_int g_stop` + `eventfd` write. Every worker watches `g_stopfd` level-triggered and never drains it, so late-arriving workers still see the wakeup. `SIGPIPE` is ignored.
- **Startup barrier:** `g_start_lock`/`g_start_cond` gate the "listening" announcement until all 16 workers have registered their epoll sets. Per-thread `accepted`/`served` counters are flushed to `g_accepted[]`/`g_served[]` after `pthread_join`.

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

Compiler: `cc`, `-Wall -Wextra -std=c11 -O2 -pthread`. TSAN: `-O1 -g -fsanitize=thread`.

### Running the Server

```sh
./build/pulsekv                          # 0.0.0.0:9999, 16 threads, edge-triggered
./build/pulsekv_lt                       # same, level-triggered
PULSEKV_QUIET=1 ./build/pulsekv          # suppress per-request log (for benchmarks)
```

Logs each connection and request to stdout (single `fputs` per request to avoid interleaving). On `SIGINT`/`SIGTERM`, prints:

```
shutdown: <conns> connections, <reqs> requests, <keys> keys resident
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
| `test_hashtable` | Hash table in isolation: set/get/del, overwrites, collision chains, `TOO_BIG`, `INVALID`, `NOT_FOUND` |
| `test_client` | Single-connection framing round-trip (step 1 — proves encode/decode + TCP transport) |
| `test_multi_client` | 5 concurrent clients: truncated-frame stall, pipelined lifecycle (SET→GET→cross-read→overwrite→DEL), in-order pipelining, stalled-connection resume, 80-frame burst reassembly |
| `test_thread_stress` | Multi-threaded load against the 16-worker server |

Run under valgrind/TSAN for heap and race coverage:

```sh
# inside Docker (valgrind available)
valgrind ./build/test_hashtable
./build/tsan/test_thread_stress
```

---

## Project Structure

```
pulsekv/
├── src/
│   ├── main.c          # thread-per-core epoll server (step 4)
│   ├── hashtable.c     # separate-chaining store + FNV-1a
│   └── protocol.c      # binary frame encode/decode
├── include/
│   ├── hashtable.h
│   └── protocol.h
├── tests/
│   ├── test_hashtable.c
│   ├── test_client.c
│   ├── test_multi_client.c
│   ├── test_thread_stress.c
│   └── manual_cli.py   # interactive REPL over the wire protocol
├── docs/
│   └── system-design.md
├── Dockerfile          # Debian bookworm + gcc/make/valgrind (glibc, not musl)
├── Makefile
└── build/              # gitignored — binaries + build/tsan/
```

~3K lines total (C + headers + tests + docs).

---

## Configuration

| Knob | Default | Where |
|------|---------|-------|
| Listen port | `9999` | `PULSEKV_PORT` in `src/main.c` |
| Worker threads | `16` | `N_THREADS` in `src/main.c` |
| Listen backlog | `512` | `LISTEN_BACKLOG` |
| Buckets | `1024` | `PK_TABLE_BUCKETS` in `include/hashtable.h` |
| Max key / val | `1 KiB / 64 KiB` | `PK_MAX_KEY_LEN / PK_MAX_VAL_LEN` in `include/protocol.h` |
| Trigger mode | edge-triggered | `PULSEKV_LEVEL_TRIGGERED` compile flag |
| Quiet log | off | `PULSEKV_QUIET` env var |

---

## Roadmap

- **Step 5 — Sharded table:** array of `N_SHARDS` buckets each with its own mutex; `shard = hash(key) % N_SHARDS`; removes the single-lock bottleneck.
- **Step 6 — WAL:** `[len][opcode][key][val][CRC32]` append-only log, group-commit fsync (batch N or T ms) to sustain 25K req/sec. Durability point is *before* the in-memory mutation; disk-full must not apply the write.
- **Step 7 — Recovery:** sequential batched reads, checksum each record, discard truncated tail (crash-mid-write), rebuild shards. Benchmark vs. naive record-by-record replay for the 60%+ restart-time cut.
- **Step 8 — Load test:** 500 concurrent clients, report p50/p99/p999.

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Threading | Thread-per-core + `SO_REUSEPORT` over shared epoll + `EPOLLEXCLUSIVE` | Predictable scaling, no cross-thread epoll contention; matches the 25K req/sec target |
| Store lock | Single global mutex (now) → per-bucket sharding (next) | Correctness baseline first, then measure contention reduction |
| Durability | Group-commit over per-write fsync | Throughput target requires batching; same trade-off as Kafka/Postgres |
| Protocol | Binary fixed-layout, views not copies | No allocation on parse; table copies only after validation |

See [`docs/system-design.md`](docs/system-design.md) for the full trade-off analysis and failure-mode handling.
