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
Client(s) --TCP--> 16 SO_REUSEPORT listeners (one epoll loop per worker)
                                      |
                         parse frame + dispatch
                                      |
                  FNV-1a route: shard = hash & 255
                    bucket-in-shard = (hash >> 8) & 3
                                      |
                    -------------------------------------
                    |                                   |
          sharded hash table                      WAL writer
       (256 locks / 1,024 buckets)             (append-only log)
                                                        |
                                                  disk: pulsekv.log
Startup (separate path from requests):
disk log --sequential batched read--> recovery replay --rebuild--> sharded hash table
```
**SET request flow:** a worker's epoll fires readable → the worker parses a frame → computes the
1,024-way bucket and its 256-way lock shard from the same hash → appends a checksummed record to
the WAL (durability point, before the in-memory mutation) → takes exactly that shard's mutex,
applies the write to one of its four bucket chains → responds.
**Wire protocol** (binary, fixed layout — no allocation on parse):
```
[1B opcode][4B key_len][key][4B val_len][val]
```
## 3. Deep Dive
**Data model:** bucket capacity and lock granularity are deliberately separate tunables:

- `PK_TABLE_BUCKETS = 1024` separate-chaining buckets preserve the step-3 table's distribution.
- `PK_TABLE_SHARDS = 256` lock shards, each with one `pthread_mutex_t` and four bucket chains.
- `global_bucket = hash & 1023`, `shard = global_bucket & 255`, and
  `bucket_in_shard = global_bucket / 256` (equivalent to the next two hash bits).
- One atomic table-wide entry counter provides exact statistics without acquiring all 256 locks;
  it does not protect keys or values and is not a data-plane serialization point.

A request holds exactly one shard lock. Keys in different shards execute concurrently; keys in
the same shard serialize even when they occupy different chains. This striped layout avoids the
memory and initialization cost of 1,024 mutexes while retaining 1,024-way hash distribution.
Both constants are powers of two so routing uses masks, and both remain benchmark tunables rather
than guessed universal values.
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
Load check: 25K req/sec ÷ 16 threads ≈ 1,560 req/sec/thread. With uniform keys, 16 workers spread
over 256 locks, so unrelated requests rarely serialize. A deliberately hot shard can still become
a bottleneck; that is measurable and easier to diagnose than whole-table contention. Syscalls and,
later, fsync batching are still expected to dominate before uniformly distributed shard locks do.
**Failure modes to design for:**
- Crash mid-WAL-write → truncated trailing record → recovery must detect and discard it
- Client disconnects mid-frame → partial read must not corrupt state for other clients
- Disk full → WAL write fails → do NOT apply the in-memory mutation (WAL-then-apply order matters)
## 5. Trade-offs
| Decision | Chose | Over | Why |
|---|---|---|---|
| Concurrency | 256 striped mutex shards over 1,024 buckets | One global lock or lock-free CAS | Removes unrelated-key contention without taking on lock-free reclamation complexity; capacity and lock count tune independently |
| Durability | Group-commit fsync | Per-write fsync | Throughput target requires it; same knob Kafka/Postgres expose |
| Event model | Thread-per-core | Shared epoll fd | More predictable scaling under load, matches throughput target |
**Revisit later if profiling justifies it:** lock-free structure on a specifically hot shard,
io_uring instead of epoll, NUMA-aware shard placement.
## Build Order
1. Blocking TCP skeleton + protocol definition
2. Single-threaded epoll event loop (level-triggered → edge-triggered)
3. In-memory hash table, single global mutex (correctness baseline)
4. Thread pool over epoll (16 threads)
5. Shard the hash table (256 striped mutexes over 1,024 buckets)
6. Append-only WAL with checksummed records
7. Crash recovery with batched replay
8. Load test and benchmark (500 clients, measure p50/p99/p999)
