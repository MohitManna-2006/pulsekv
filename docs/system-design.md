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
                    GET ------------------> sharded hash table
                    SET/DEL                           ^
                       |                              |
                synchronized FIFO                    | durable completion
                       |                              |
                dedicated WAL writer --eventfd--> owning epoll worker
                       |
              group write + fdatasync
                       |
                disk: pulsekv.log
Startup (separate path from requests):
disk log --sequential batched read--> recovery replay --rebuild--> sharded hash table
```
**SET/DEL request flow:** a worker parses a frame → copies it into an owned WAL request → removes
that frame from the socket buffer exactly once → pauses later frames on that connection → enqueues
the mutation in the global FIFO. The dedicated writer assigns its sequence, combines up to 256
records or waits at most 1 ms, appends the encoded batch, and calls `fdatasync`. It then places the
result on the originating worker's atomic completion stack and coalesces that worker's `eventfd`
wakeups. Only a
successful durable completion lets the epoll worker take the target table shard lock, apply the
mutation, and queue `OK`. A WAL failure queues `ERROR` and leaves the table unchanged.

**GET request flow:** GET bypasses the WAL and reads the sharded table immediately. If it follows
a mutation on the same pipelined connection, it remains buffered behind that connection's one
pending WAL request, preserving client-visible request order.
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
**Threading model:** thread-per-core with `SO_REUSEPORT`. Up to 16 workers each own a listener,
epoll instance, live connections, and a WAL-completion `eventfd`. One additional dedicated thread
owns the WAL fd and is the only code allowed to append or call `fdatasync`. A mutex/condition FIFO
linearizes submissions from all workers. The writer is the single producer for each worker's
atomic completion stack; the worker is its single consumer and reverses each detached batch back
to FIFO order. This returns ownership without letting the writer touch sockets, epoll state, or
the hash table. `PULSEKV_THREADS` selects 1–16 workers so deployed threads can match available CPUs.

**WAL record v1** (all integers big-endian):
```
[4B "PKWL"][2B version][1B opcode][1B flags]
[4B record_len][8B sequence][4B key_len][4B value_len]
[key][value][4B IEEE CRC32]
```
`record_len` covers the entire record and CRC32 covers every preceding byte. Only SET and DEL are
valid WAL opcodes; DEL must have a zero-length value. Header/version/length validation happens
before any payload pointer is trusted. The writer encodes a whole group into one contiguous buffer,
appends it in order, and issues one `fdatasync` before completing any member. Default group-commit
policy is 256 records or 1 ms, configurable with `PULSEKV_WAL_BATCH_MAX` and
`PULSEKV_WAL_DELAY_US`; `PULSEKV_WAL_PATH` selects the file.

The first writer error is sticky: that batch and all later mutations complete with an error and
do not change memory. Successfully durable earlier batches remain readable. An unacknowledged
operation may still be present after a process crash between `fdatasync` and the client reply;
clients must treat a lost connection as an unknown commit result and use idempotent retry logic.

**Shutdown:** SIGINT/SIGTERM begins a two-phase drain. Workers close their listeners and client
sockets immediately, but retain connection shells referenced by pending WAL requests. They stay
in epoll on their completion eventfds until every submitted mutation is synced and applied, then
exit. Main joins all workers, stops/joins the empty WAL writer, destroys the shared table once, and
prints WAL batch statistics. This also prevents the writer callback from targeting a dead worker.

**Recovery:** before opening listeners or starting the WAL writer, startup requires a regular WAL
file, opens it read/write, and scans it with 256 KiB reads. An incomplete record is retained across
chunk boundaries, so a record split between reads is decoded only after enough bytes arrive. Every
record must pass magic, version, length, opcode, CRC32, and contiguous-sequence validation before
its SET/DEL is applied to
the table. The writer then starts at `last_sequence + 1`.

The first invalid or incomplete record is the crash boundary. Recovery truncates the physical file
to the last valid byte, calls `fdatasync`, and only then allows startup to continue; subsequent
starts see a clean log rather than rediscovering the same damaged tail. Startup reports original
and valid bytes, discarded bytes, read syscall count, restored key count, and elapsed time. The
recovery test's 20,000-record comparison uses 6 batched reads versus 40,001 record-at-a-time reads.
A table-driven CRC32 implementation keeps checksum validation from dominating replay; representative
normal runs are about 4 ms batched versus 20 ms naive, an approximately 80% restart-time reduction.
The deterministic syscall reduction is asserted while wall-clock timing is reported but not asserted,
because filesystem cache, scheduling, and host load make timing tests nondeterministic.

**Load benchmark:** Step 8 uses one client-side epoll loop to drive 500 persistent sockets with one
outstanding request per connection. All keys are seeded and warmed before the measured interval.
Every reply is checked against per-connection expected state, so corrupt or missing results cannot
be counted as throughput. The driver reports min, mean, p50, p99, p999, and max for three profiles:
100% GET, 90/8/2 GET/SET/DEL, and 100% durable SET.

The measure–change–remeasure loop retained only hot-path changes supported by the benchmark:

- 256-record batches roughly doubled fully durable throughput versus 64-record batches locally.
- The WAL encode buffer now grows to actual batch bytes rather than reserving the worst case.
- Queue condition signals and per-worker `eventfd` notifications are coalesced.
- WAL-to-worker completion delivery is a lock-free SPSC atomic handoff.
- `accept4` removes setup fcntls and accepted sockets enable `TCP_NODELAY`.
- `PULSEKV_THREADS` prevents oversubscribing machines with fewer than 16 available CPUs.

Representative Docker Desktop measurements saw two virtual CPUs and reserved one for the
co-located load generator. Across 500 connections, read/mixed/durable-write profiles sustained
approximately 192K/178K/113K requests per second. The 25K throughput goal was exceeded throughout.
The `<5 ms p99` result is printed as a scorecard rather than asserted: it was met in some read runs,
while mixed and durable-write tails remained above it because real `fdatasync` latency is included.
Production numbers require dedicated client CPUs and the intended server core count.

## 4. Scale & Reliability

Load check: 25K req/sec ÷ 16 threads ≈ 1,560 req/sec/thread. With uniform keys, 16 workers spread
over 256 locks, so unrelated requests rarely serialize. A deliberately hot shard can still become
a bottleneck. Mutations also share one ordered log by definition, so batching—not extra WAL writer
threads—is the throughput lever. At 256 mutations per sync, 25K writes/sec requires roughly 98
syncs/sec rather than 25K.
**Failure modes to design for:**
- Crash mid-WAL-write → truncated trailing record → recovery must detect and discard it
- Client disconnects mid-frame → partial read must not corrupt state for other clients
- Disk full → WAL write fails → do NOT apply the in-memory mutation (WAL-then-apply order matters)
- Signal with WAL requests in flight → drain completions before worker or table destruction
- Client disconnect after submit → retain request ownership, apply if durable, omit the response
## 5. Trade-offs

| Decision | Chose | Over | Why |
|---|---|---|---|
| Concurrency | 256 striped mutex shards over 1,024 buckets | One global lock or lock-free CAS | Removes unrelated-key contention without taking on lock-free reclamation complexity; capacity and lock count tune independently |
| Durability | Group-commit fsync | Per-write fsync | Throughput target requires it; same knob Kafka/Postgres expose |
| Event model | Thread-per-core | Shared epoll fd | More predictable scaling under load, matches throughput target |
| WAL integration | Dedicated async writer + per-worker completion eventfds | Blocking worker writes | Preserves event-loop responsiveness while retaining one deterministic append order |
| Benchmark driver | One epoll loop over 500 sockets | 500 co-located client threads | Preserves 500 concurrent connections without polluting tail latency with client scheduler contention |
**Revisit later if profiling justifies it:** lock-free structure on a specifically hot shard,
io_uring instead of epoll, NUMA-aware shard placement.
## Build Order

1. Blocking TCP skeleton + protocol definition
2. Single-threaded epoll event loop (level-triggered → edge-triggered)
3. In-memory hash table, single global mutex (correctness baseline)
4. Thread pool over epoll (16 threads)
5. Shard the hash table (256 striped mutexes over 1,024 buckets)
6. Append-only WAL with checksummed records and async group commit (complete)
7. Crash recovery with batched replay and tail repair (complete)
8. Load test, measured optimization, and benchmark (500 clients, p50/p99/p999) (complete)
