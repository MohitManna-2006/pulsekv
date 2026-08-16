# PulseKV — System Design
Concurrent, sharded, epoll-based key-value store in C with append-only persistence and crash recovery.
## 1. Requirements
**Functional**
- GET / SET / DEL over TCP against an in-memory store
- Durable persistence — survive a process crash/restart without losing committed data
- Fast crash recovery via log replay
**Non-functional** (the numbers the design has to actually earn):
- 25K+ req/sec sustained
- 500 concurrent client connections
- <5ms p99 latency
- 1M+ keys resident
- 60%+ cut in restart time vs. naive replay
**Constraints**
- Single-node, single-process C on Linux (POSIX epoll/pthreads)
- No external dependencies — the point is to build the primitives yourself
## 2. High-Level Design
```
Client(s) --TCP--> epoll event loop --dispatch--> worker thread pool (1..16)
                                                          |
                                          shard router: hash(key) % N_SHARDS
                                                          |
                                    -------------------------------------
                                    |                                   |
                          sharded hash table                      WAL writer
                          (per-bucket mutex)                   (append-only log)
                                                                        |
                                                                  disk: pulsekv.log
Startup (separate path from requests):
disk log --sequential batched read--> recovery replay --rebuild--> sharded hash table
```
**SET request flow:** epoll fires readable → worker parses frame → computes shard = hash(key) % N →
appends checksummed record to WAL (durability point, before the in-memory mutation) →
takes that shard's mutex, applies the write → responds.
**Wire protocol** (binary, fixed layout — no allocation on parse):
```
[1B opcode][4B key_len][key][4B val_len][val]
```
## 3. Deep Dive
**Data model:** array of N_SHARDS buckets (power of 2, well above thread count — 256 to start),
each with its own `pthread_mutex_t`. N_SHARDS is a real tunable to benchmark, not guess.
**Threading model — pick one deliberately:**
- Shared epoll fd + `EPOLLEXCLUSIVE`: kernel wakes one thread per event, avoids thundering herd, less code.
- Thread-per-core + `SO_REUSEPORT`: each thread owns its own epoll instance and listening socket,
  scales more predictably, no cross-thread epoll contention. Closer to production systems
  (Seastar/ScyllaDB); the more defensible choice if 25K+ req/sec is the headline number.
**WAL format:** `[len][opcode][key][val][CRC32]` per record, appended sequentially.
Fsync policy is the real decision:
- Sync every write — durable, throughput capped at disk fsync latency
- Group commit (batch N writes or T ms, one fsync) — this is what gets you to 25K+ req/sec
**Recovery:** sequential, large-chunk reads of the log (not record-by-record `read()`), checksum
each record, stop at the first corrupt/truncated one (handles crash-mid-write), rebuild shards
from what's valid. Benchmark this against naive record-by-record replay to get a real
"restart time cut" number.
## 4. Scale & Reliability
Load check: 25K req/sec ÷ 16 threads ≈ 1,560 req/sec/thread. A mutex-guarded bucket access is a
few hundred ns, so the bottleneck is almost certainly syscalls (one read/write per request) or
fsync batching — not the lock itself.
**Failure modes to design for:**
- Crash mid-WAL-write → truncated trailing record → recovery must detect and discard it
- Client disconnects mid-frame → partial read must not corrupt state for other clients
- Disk full → WAL write fails → do NOT apply the in-memory mutation (WAL-then-apply order matters)
## 5. Trade-offs
| Decision | Chose | Over | Why |
|---|---|---|---|
| Concurrency | Mutex-sharded | Lock-free (CAS) | Correct and fast enough at 16 threads; lock-free buys little here, easy to get subtly wrong |
| Durability | Group-commit fsync | Per-write fsync | Throughput target requires it; same knob Kafka/Postgres expose |
| Event model | Thread-per-core | Shared epoll fd | More predictable scaling under load, matches throughput target |
**Revisit later if profiling justifies it:** lock-free structure on a specifically hot shard,
io_uring instead of epoll, NUMA-aware shard placement.
## Build Order
1. Blocking TCP skeleton + protocol definition
2. Single-threaded epoll event loop (level-triggered → edge-triggered)
3. In-memory hash table, single global mutex (correctness baseline)
4. Thread pool over epoll (16 threads)
5. Shard the hash table (per-bucket mutex)
6. Append-only WAL with checksummed records
7. Crash recovery with batched replay
8. Load test and benchmark (500 clients, measure p50/p99/p999)
