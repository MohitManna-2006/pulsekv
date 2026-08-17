# PulseKV v2 — Phase 1 Summary

**Status: complete.** Read this first if you are picking up Phase 2.

Phase 1 turned the Phase 0 skeleton into a real data-plane node: v1's sharded
hash table extracted into `node/engine/`, an NVMe spill tier built underneath
it, chunked framing for multi-megabyte values, and every `NodeService` RPC wired
through to it. Nothing returns `UNIMPLEMENTED` any more.

Companion docs: `pulsekv-v2-distributed-design.md` (what v2 is and why),
`pulsekv-v2-implementation-plan.md` (the phase order),
`pulsekv-v2-phase0-summary.md` (the frozen contract and the toolchain, still
accurate).

v1 is untouched. `src/`, `include/`, `tests/`, the root `Makefile`, the root
`Dockerfile`, and `README.md` are exactly as they were, and v1's own suite still
builds and passes — verified, not assumed (§5).

---

## 1. What was added

```
node/engine/                          # was a placeholder, now the engine
├── include/pulsekv_engine.h          #   the entire public surface, extern "C"
├── src/
│   ├── engine.c                      #   public API: glue and result translation only
│   ├── hashtable.[ch]                #   RAM tier: v1's table + LRU + accounting
│   └── tiering.[ch]                  #   NVMe tier: paths, atomic writes, verified reads
├── tests/
│   ├── test_util.h                   #   shared scaffolding, in v1's tests/ style
│   ├── test_engine_basic.c           #   47 checks
│   ├── test_engine_chunked.c         #   20 checks
│   ├── test_engine_tiering.c         #   38 checks
│   └── test_engine_stress.c          #   16 checks, 8 threads under constant eviction
├── CMakeLists.txt
└── README.md                         # updated

node/grpc_shim/
├── main.cpp                          # every RPC real; engine config CLI flags
└── CMakeLists.txt                    # the Phase 0 seam, now closed

proto/node.proto                      # + PutChunked/GetChunked, PutChunk/GetChunk,
                                      #   UnaryLimit, PrefixMatchResponse.value_omitted

control/
├── cmd/pulsekv-node-bench/main.go    # NEW — the node benchmark
├── cmd/pulsekv-smoke/main.go         # UNIMPLEMENTED assertions -> real behaviour
├── cmd/controlplane/main.go          # + --print-engine, config warnings
└── internal/config/config.go         # + engine section, Warnings()

deploy/
├── cluster.config.yaml               # + engine: budgets and per-node data_root
├── run-local-cluster.sh              # per-node --data-dir, engine flags
├── test-engine.sh                    # NEW — release / TSan / Valgrind
├── bench-node.sh                     # NEW — the two benchmark scenarios
├── Dockerfile                        # + valgrind
└── README.md                         # updated
```

---

## 2. The engine

### The boundary

`node/engine/include/pulsekv_engine.h` is the **only** header the shim can
include from the engine, and that is enforced by CMake rather than by
discipline: the `pulsekv_engine` target exports `include/` as PUBLIC and `src/`
as PRIVATE, so `hashtable.h` and `tiering.h` are not on `main.cpp`'s include
path at all. A stray `#include "hashtable.h"` in the shim fails to compile.

No protobuf, gRPC, or C++ type appears anywhere in `node/engine/`. The engine
builds and its full suite runs with `cmake -S node/engine` and no gRPC toolchain
present.

### RAM tier — what came from v1

`src/hashtable.c` was copied, not moved: 1,024 buckets over 256 mutex-striped
shards, FNV-1a routing, copy-in/copy-out ownership. None of that needed
correcting — concurrent gRPC handler threads calling in is precisely the access
pattern the striping was built for.

Added on top:

- `uint64` value lengths (v1 capped a value at 64 KiB in one frame).
- A per-shard **intrusive LRU list**, protected by the shard mutex that already
  exists. Deliberately not a second lock: a tiering lock and a table lock taken
  in two orders is how tiering bugs and hash-table bugs become deadlocks
  together.
- Per-shard, per-tier byte and key accounting, so eviction and
  `CapacityResponse` read from the same numbers.
- A cached hash per node, which rejects a chain non-match in one integer compare
  before touching key bytes, and gives eviction the spill path without
  recomputing.

### NVMe tier

Spill files live at `<data_dir>/spill/<aa>/<bb>/<hash>_<id>.val` — two levels of
256 directories, created lazily, keyed off the *top* bytes of the hash so
directory placement stays independent of the bucket placement the low bits
decide.

Each file is self-describing: magic, key length, value length, the full key,
then the value. A read verifies all of it plus the exact file size before
returning a byte. That is what makes a 64-bit hash collision harmless rather
than a silent wrong answer, and what catches a torn write. Writes go to a unique
temp path and are published with `rename()`.

Deliberately **not** `fsync`ed, and the tree is **purged at startup and
shutdown**. Spill files are unreachable the moment the in-RAM index naming them
is gone, so anything found in the directory is garbage from a previous run.
This is a cache tier, not persistence — the design doc's §4.3 trade.

---

## 3. Deviations from the Phase 1 prompt

Everything below is either something the prompt asked to be recorded, an
addition (never a rename or removal), or a decision the prompt explicitly left
open.

### 3.1 The epoll scope correction — recorded, as asked

The implementation plan described Phase 1.1 as extracting v1's hash table **and**
`main.c`'s epoll worker model. Only the storage logic was extracted. gRPC C++'s
server owns its own sockets and thread pool, so v1's hand-rolled event loop
would have been dead code with no caller. This is a correction to what
"extraction" means, not a weakening of it: the 256-way mutex striping — the part
that actually makes concurrent access work — came across intact and is exercised
by `test_engine_stress.c` under eight concurrent threads.

### 3.2 Proto additions

Additive only; no RPC or field was renamed or removed.

| Added | Why |
|---|---|
| `rpc PutChunked(stream PutChunk) returns (PutResponse)` | Step 1.2 |
| `rpc GetChunked(GetRequest) returns (stream GetChunk)` | Step 1.2 |
| `message PutChunk`, `message GetChunk` | Step 1.2 |
| `enum UnaryLimit { UNARY_VALUE_LIMIT_BYTES = 4194304 }` | So clients and the smoke test read the 4 MiB line from the contract instead of hardcoding it. |
| `PrefixMatchResponse.value_omitted` | See §3.4. |

`PutResponse.error` is left **unused**. gRPC discards the response message when
the status is non-OK, so an in-band error string would be unreadable; gRPC
status codes are the sole error channel. The field is kept because the contract
is frozen, and it is the natural place for Phase 4's "committed locally, replica
lagging" — an OK status with a caveat, which is a different thing from an error.

### 3.3 Framing conventions — the prompt said "pick one and document it"

- **Out-of-order chunks are rejected, not reassembled.** gRPC already guarantees
  stream ordering, so a gap or a repeat means a broken client, not a network
  reordering. Buffering around it would cost memory to paper over a bug.
- **`key` is required on chunk 0**; later chunks may repeat it identically or
  omit it. A *different* non-empty key aborts the stream — that is two writes
  spliced into one value.
- **`total_length` is validated against `--max-value-bytes` before a single byte
  is buffered or reserved**, so a corrupt or hostile length cannot turn into an
  allocation. A running check also cuts a lying stream off at the point it
  starts lying rather than after buffering everything it wanted to send.
- **`GetChunked` on a miss is an empty stream** (zero messages, status OK),
  mirroring `Get`'s `found = false`. A zero-length *value* is still a hit, so it
  gets one empty chunk — the two cases stay distinguishable.

### 3.4 `PrefixMatch` — scope and implementation, stated honestly

The table has no ordered iteration, so this is a **full scan of all 256 shards,
O(total keys)**. No cheaper approach exists without a second index, which is not
Phase 1 work.

Three decisions on top of that:

- **Two passes.** The engine collects matching *keys* under the shard locks and
  releases them; values are fetched one at a time afterwards. No shard lock is
  ever held while a multi-megabyte value is copied or written to a
  possibly-slow client.
- **It reads through `pk_engine_peek`, not `pk_engine_get`.** A scan must not
  promote every spilled value it touches and evict the actual working set as a
  side effect.
- **It is not a snapshot, and does not claim to be.** Shards are visited one at
  a time, so a concurrent write to an already-passed shard is not reflected.
  Keys can also vanish between the scan and the fetch; those are skipped.
  Freezing all 256 shards to produce a consistent scan is the wrong trade for a
  cache index.

`value_omitted` exists because a scan over a cache of multi-megabyte KV blocks
otherwise has only bad options: fail the whole stream on the first large value,
or send an empty `value` that is indistinguishable from a key whose value
genuinely is empty. The flag makes the distinction explicit.

### 3.5 Engine API additions beyond the prompt's sketch

The prompt's header sketch invited adjustment. Added: `pk_engine_peek` (scan
reads), `pk_engine_scan_prefix` / `pk_engine_free_keyset` (PrefixMatch),
`pk_engine_max_value_bytes` and `pk_engine_ram_budget_bytes` (the shim needs the
ceiling to reject an oversized stream before buffering), `pk_engine_strerror`,
`PK_ENGINE_NOMEM` (genuinely distinct from `PK_ENGINE_IO_ERROR`), and
diagnostic counters on `pk_engine_capacity_t` — spills, promotions,
spill_errors, evict_drops, and per-tier key counts. The first three capacity
fields still map field-for-field onto `CapacityResponse`.

### 3.6 Storage design decisions

**A spilled entry keeps its index entry in RAM.** Only the value moves to disk.
This makes a miss on a never-seen key cost zero filesystem work, keeps capacity
accounting exact without walking the spill tree, and lets a prefix scan see
spilled keys. Cost is one node plus the key bytes per spilled entry — noise
against megabyte-scale values.

**Eviction never spills the entry that was just written or promoted.** The
budget is divided across 256 shards, so a value larger than its shard's share
would otherwise be flushed to disk by the very operation that stored it, and
every large read would be a disk read. Asserted in
`test_engine_tiering.c::test_mru_protection`.

**The budget is per shard, not global** — `ram_budget_bytes / 256`. A global
byte counter would be one contended cache line touched by every write on every
core, which is the bottleneck the 256-way striping exists to avoid. Two
consequences, both asserted rather than left to be rediscovered:

- Occupancy is only as even as the hash. 24 MiB spread one-value-per-shard
  against an 8 MiB budget spills **nothing**
  (`test_engine_tiering.c::test_per_shard_budget`).
- Total resident bytes can exceed `ram_budget_bytes`, bounded by
  `budget + 256 × max_value_bytes`.

The control plane emits a startup warning when `ram_budget_bytes / 256 <
max_value_bytes`, which is the configuration where this bites.

**A failed spill drops the entry and counts it** in `spill_errors`. This is a
cache: a miss costs a recompute. A full or failing disk degrades the node into a
smaller cache, which is survivable; growing RAM past its budget is not.

**An unusable `--data-dir` is a hard startup failure.** Silently continuing as a
RAM-only cache would give a node a fraction of its configured capacity while
reporting itself healthy — the kind of quiet degradation that gets found in
production instead of at boot.

### 3.7 Wire limits

The unary/chunked line is 4 MiB (`UNARY_VALUE_LIMIT_BYTES`). gRPC's max message
size is set to **8 MiB** on both server and client — deliberately above the
line, so a slightly-oversized unary `Put` reaches the handler and gets a
specific "use `PutChunked`" reply instead of being killed by the transport with
a generic `RESOURCE_EXHAUSTED`. Far past 8 MiB the transport limit does apply,
which is the right answer anyway.

Keys are bounded at 64 KiB. They are identifiers, not payloads, and the engine's
key length is a `uint32` that must not be allowed to overflow.

`--max-value-bytes` above `UINT32_MAX` is refused at startup: the chunked path
buffers a value whole in Phase 1, so pretending to support more would be a lie.
Streaming straight into the engine is Phase 6.

### 3.8 Tooling

`deploy/test-engine.sh` and `deploy/bench-node.sh` are new, and `valgrind` was
added to `deploy/Dockerfile` — the same reason v1's image carries it. No
`Delete` RPC or engine delete was added; it is not in the frozen contract.

---

## 4. Benchmark

Two scenarios, one dedicated node, `deploy/bench-node.sh`. Every read verified
byte-for-byte against a value regenerated from the key index; the tool **fails
the run** rather than reporting throughput next to an unverified reply.

### Measurement environment — stated, because it bounds every number below

| | |
|---|---|
| Host | Apple Silicon macOS, Docker via colima |
| Container | `pulsekv-v2-dev`, linux/arm64, **2 vCPU** |
| Node | 1 process, 64 MiB RAM budget (**256 KiB per shard**), 64 MiB max value |
| Spill directory | `/tmp` **inside the container** — not the repo bind mount, which is virtiofs and would measure the host filesystem bridge instead of the tier |
| Client | 16 workers, one gRPC connection each, 16 KiB values, 80% reads |
| Ops | 40,000 measured, 4,000 warmup discarded |

**Two vCPUs is the dominant constraint.** The client, the node, and 16 worker
goroutines share them, so these are not throughput-ceiling numbers for the
engine — they are a controlled *comparison* between two tier configurations
measured under identical conditions, which is what Phase 1 needs.

### Scenario 1 — working set fits in RAM

2,048 keys × 16 KiB = 32 MiB (8 keys ≈ 128 KiB per shard, half the shard budget).

```
              count       min       p50       p99      p999       max      mean
read          31999   0.055ms   0.584ms   2.850ms   4.058ms   5.754ms   0.764ms
write          8001   0.071ms   0.538ms   2.690ms   4.216ms   5.000ms   0.704ms
overall       40000   0.055ms   0.575ms   2.817ms   4.149ms   5.754ms   0.752ms

populate      2048 keys in 134ms  (15,237 keys/s, 238.1 MiB/s)
throughput    20,665 ops/s over 1.936s  (322.9 MiB/s of value payload)
verification  31,999 reads, all verified byte-for-byte, 0 mismatches, 0 misses, 0 errors
capacity      keys=2048 ram=32.0 MiB nvme=0 B   — 0.0% on NVMe
```

### Scenario 2 — working set several times RAM

16,384 keys × 16 KiB = 256 MiB (64 keys ≈ 1 MiB per shard, **4× the shard
budget**). Runs against the same node, which still holds scenario 1's 2,048
keys, so total data is 288 MiB against a 64 MiB budget; scenario 1's keys are
cold and spill out immediately.

```
              count       min       p50       p99      p999       max      mean
read          31999   0.074ms   1.373ms   3.967ms   7.157ms  15.568ms   1.510ms
write          8001   0.102ms   1.222ms   3.680ms   7.592ms  14.413ms   1.325ms
overall       40000   0.074ms   1.346ms   3.930ms   7.169ms  15.568ms   1.473ms

populate      16384 keys in 1.875s  (8,737 keys/s, 136.5 MiB/s)
throughput    10,698 ops/s over 3.739s  (167.2 MiB/s of value payload)
verification  31,999 reads, all verified byte-for-byte, 0 mismatches, 0 misses, 0 errors
capacity      keys=18432 ram=64.0 MiB nvme=224.0 MiB   — 77.8% on NVMe
node counters 45,238 spills, 24,692 promotions, 0 spill errors
```

### What the NVMe tier costs

| | fits in RAM | 4× oversubscribed | ratio |
|---|---|---|---|
| throughput | 20,665 ops/s | 10,698 ops/s | **1.93× slower** |
| read p50 | 0.584 ms | 1.373 ms | 2.35× |
| read p99 | 2.850 ms | 3.967 ms | 1.39× |
| read max | 5.754 ms | 15.568 ms | 2.71× |
| value bytes on NVMe | 0.0% | 77.8% | — |

Read honestly: with **78% of value bytes on disk** the node still served every
one of 31,999 reads correctly, at roughly half the throughput and about 2.4×
the median latency. The tail is where the tier shows up most — max read latency
nearly triples, which is the spill-under-lock behaviour described below arriving
in the p999/max band rather than the median.

That the tail widens rather than the floor rising is the useful signal: the RAM
path is unaffected (min latency barely moves, 0.055 → 0.074 ms), and the cost is
concentrated in the operations that actually touch disk.

### Known cost, measured rather than hand-waved

Spill writes happen **with the shard lock held**, so one shard serializes behind
a disk write. That is visible in the p999/max column above. Dropping the lock
around the I/O means handling a concurrent overwrite or delete of the very node
being spilled; that complexity belongs with Phase 6's transport work, not here.

---

## 5. Exit criteria — verified

| # | Criterion | Evidence |
|---|---|---|
| 1 | `node/engine/` builds as its own CMake target; v1's suite unaffected | `cmake -S node/engine` builds standalone with no gRPC present. v1: `make` clean, `test_hashtable` / `test_wal` / `test_recovery` all **PASS**, `git status` on `src/ include/ tests/ Makefile Dockerfile README.md` empty |
| 2 | `pulsekv_engine.h` is the only engine header the shim sees | Enforced by CMake target scoping (PUBLIC `include/`, PRIVATE `src/`), not convention. No protobuf type in engine code; no hashtable/tiering internal in `main.cpp` |
| 3 | Chunked round-trips a multi-megabyte value; oversized unary fails fast | Smoke: 6.0 MiB over 6 chunks byte-for-byte on all 4 nodes; unary `Put` >4 MiB → `INVALID_ARGUMENT` **whose message names `PutChunked`** |
| 4 | Working set > RAM spills, serves correctly with promotion, survives cycles | `test_engine_tiering.c`: 4× oversubscribed working set fully correct; 10 full passes over 600 keys with zero wrong reads; spill files on disk counted independently and matched against `keys_in_nvme_tier` |
| 5 | `Get`/`Put`/`PutChunked`/`GetChunked`/`Capacity` real; `PrefixMatch` documented | All real; `PrefixMatch` scope in §3.4 |
| 6 | `deploy/smoke-test.sh` passes with real-behaviour assertions | **84/84** Go contract checks; no RPC returns `UNIMPLEMENTED` |
| 7 | Benchmark, correctness-verified and percentile-reported, both scenarios | §4 |
| 8 | This document | — |

### Test totals

| Suite | Result |
|---|---|
| `test_engine_basic` | 47 checks, 0 failed |
| `test_engine_chunked` | 20 checks, 0 failed |
| `test_engine_tiering` | 38 checks, 0 failed |
| `test_engine_stress` | 16 checks, 0 failed |
| `deploy/smoke-test.sh` Go leg | 84 checks, 0 failed |
| v1 regression | `make` + 3 standalone suites, all pass |

### ThreadSanitizer and Valgrind

v1 kept a parallel TSan build of its whole suite and ran its store tests under
Valgrind, because for a concurrent structure holding long-lived heap state "the
tests pass" is not evidence on its own. v2's engine has strictly more of both —
an LRU list and four counters per shard on top of the chains, plus values whose
ownership moves between RAM, disk, and the caller. `deploy/test-engine.sh --all`
runs all three modes:

| Mode | Result |
|---|---|
| Release | 4 suites, 121 checks, 0 failed |
| **ThreadSanitizer** (`-fsanitize=thread`) | 4 suites, 121 checks, 0 failed, **no races reported** |
| **Valgrind memcheck** (`--leak-check=full --errors-for-leak-kinds=definite --track-origins=yes --error-exitcode=42`) | 4 suites, 121 checks, 0 failed, **no leaks or invalid accesses** |

TSan earned its place immediately: its first run reported a genuine data race —
in the *test harness*, on a `volatile int` flag shared between the scanner
thread and main. `volatile` orders nothing and synchronizes nothing in C, and
the flag is now an `atomic_int`. No race was found in the engine itself, before
or after. Recorded here because a harness that races is a harness that will
have every future run start by triaging a race that was never in the engine.

**Running `--tsan` inside Docker requires `--security-opt seccomp=unconfined`.**
ThreadSanitizer disables ASLR via `personality(ADDR_NO_RANDOMIZE)`, which
Docker's default seccomp profile blocks; without the flag TSan aborts at startup
with a `tsan_platform_linux.cpp` CHECK failure. `deploy/test-engine.sh` detects
that specific failure and prints the required flag rather than reporting it as a
test failure.

---

## 6. Known limitations, carried forward deliberately

- **The NVMe tier is unbounded.** Nothing evicts *from* disk; it grows until the
  filesystem refuses, at which point spilling fails, the entry is dropped, and
  `spill_errors` counts it. A second-level eviction policy and disk-full fault
  injection are Phase 9 hardening — the prompt scoped thorough fault injection
  out of this phase.
- **Fixed bucket array**, inherited from v1. Well past `PK_TABLE_BUCKETS` live
  keys, chains lengthen and lookups drift toward O(n).
- **Spill I/O under the shard lock** — measured in §4.
- **`PrefixMatch` is O(total keys)** — §3.4.
- **Chunked values are buffered whole** before reaching the engine. True
  streaming into the engine is Phase 6, as the prompt directed.
- **Index memory is not counted against `ram_budget_bytes`.** The budget covers
  value bytes; keys and per-entry bookkeeping are not counted. Stated in the
  proto comments rather than silently approximated.
- **No `Delete`.** Not in the frozen contract.

---

## 7. Where Phase 2 starts

Phase 2 is the Go control plane: rendezvous hashing, the generic client SDK, and
the `ClusterMetadataService` skeleton becoming a real router. Nothing in
`node/` needs to change for it.

The seams that are already waiting:

- **`Config.ShardMap()` in `control/internal/config/config.go`** is still the
  Phase 0 round-robin placeholder, and says so in its own comment. Phase 2.1
  replaces it with rendezvous (highest-random-weight) hashing. Nothing depends
  on the current distribution; `pulsekv-smoke` asserts only the *shape* of the
  map (every shard owned, every owner known).
- **`control/cmd/pulsekv-node-bench`** already contains a working single-node
  client — unary and chunked, with verification. Phase 2.2's client SDK is that
  logic behind a router, and Phase 2.4's end-to-end routing test is this tool
  pointed at a cluster instead of a node.
- **`NodeInfo.alive`** is still a direct probe, not membership. Phase 3 replaces
  it with gossip.

Do not change `proto/node.proto`'s existing RPCs. Phase 2 needs no new ones.
