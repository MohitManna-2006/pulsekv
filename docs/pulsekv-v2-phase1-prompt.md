# PulseKV v2 — Phase 1 Implementation Prompt (for Claude Code)

**How to use this file:** paste everything below the line into Claude Code as the task prompt for
the Phase 1 session, run from inside the `pulsekv` repo root, on top of commit `73b6efa` ("V2
Phase0") or later.

---

You are implementing **Phase 1 only** of PulseKV v2. Before writing any code, read, in order:

1. `docs/pulsekv-v2-distributed-design.md` — what v2 is and why.
2. `docs/pulsekv-v2-implementation-plan.md`, Section 4 ("Phase 1 — data-plane storage node") —
   the original phase definition. This prompt expands it with the specifics Phase 0 settled.
3. `docs/pulsekv-v2-phase0-summary.md` — **as-built** reference for everything that already
   exists: exact file layout, the frozen proto contract, pinned toolchain, the commented CMake
   seam you're about to uncomment, and the deviations Phase 0 made from its own prompt. Treat this
   file as ground truth over the Phase 0 prompt where they'd ever disagree.
4. `proto/node.proto`, `node/grpc_shim/main.cpp`, `node/README.md`, `node/engine/README.md` — the
   actual current state of what you're extending.
5. `include/hashtable.h` and `src/hashtable.c` (v1) — what you're extracting from, read-only.

## Hard scope boundary

- **Do not modify** `src/`, `include/`, `tests/`, the root `Makefile`, the root `Dockerfile`, or
  `README.md`. You will **copy** from `src/hashtable.c` / `include/hashtable.h` into
  `node/engine/`, never move or edit the originals. v1 stays complete and independently buildable.
- **Do not** touch `control/internal/metadata/`, gossip, or Raft — those are Phases 2, 3, and 5.
- **Do not** implement replication, quorum writes, or anything durability-related beyond what's
  needed for a single node's own correctness. No WAL. Phase 1's storage is an explicitly
  loss-tolerant cache tier — that's the design doc's point, not an oversight.
- **Do not** implement the zero-copy / shared-memory transport optimization. Chunked streaming
  over plain gRPC is the correct scope for this phase; the zero-copy path is Phase 6, and doing it
  now would be optimizing before Phase 9's benchmark says it's needed.
- **Do not** touch `adapters/`. That's Phases 7 and 8.
- You may **extend** `proto/node.proto` (add messages/RPCs) per the frozen-contract rule already
  established in Phase 0: add, don't rename or remove. Regenerate stubs for all three languages
  the same way `deploy/gen-proto.sh` already does, and re-verify determinism the way Phase 0's
  summary documents (regenerate in a fresh container, diff against checked-in stubs).

## A scope correction to the plan document, settled before you start

`pulsekv-v2-implementation-plan.md` describes Phase 1.1 as extracting "v1's sharded hashtable
(`hashtable.c`) **and epoll core** (`main.c`'s worker model)." That instruction predates Phase 0's
decision to put gRPC C++ in front of the data plane. gRPC C++'s server already owns its own
networking and thread pool — there is no role left for v1's hand-rolled epoll loop here; it would
be dead code with no caller. **Only `hashtable.c`'s storage logic needs extracting.** Concurrent
RPC handler threads calling into the engine is exactly the access pattern v1's 256-shard mutex
striping was already built for, so this is a correction to what "extraction" means, not a
weakening of it. Note this in your Phase 1 summary the same way Phase 0 noted its deviations —
it's a real decision, not a shortcut.

## Target additions to the repository layout

```
pulsekv/
├── node/
│   ├── engine/                          # was an empty placeholder; now the real engine
│   │   ├── include/pulsekv_engine.h     # extern "C" API — see Step 1.1
│   │   ├── src/
│   │   │   ├── hashtable.c              # copied from v1, extended (see Step 1.1/1.3)
│   │   │   ├── hashtable.h
│   │   │   ├── tiering.c                # NEW — NVMe spill/promote/demote (Step 1.3)
│   │   │   └── engine.c                 # NEW — the extern "C" surface, wires the above together
│   │   ├── tests/
│   │   │   ├── test_engine_chunked.c    # Step 1.2 coverage
│   │   │   ├── test_engine_tiering.c    # Step 1.3 coverage
│   │   │   └── test_engine_stress.c     # concurrent Put/Get + eviction under load
│   │   ├── CMakeLists.txt               # NEW — builds libpulsekv_engine + the test binaries
│   │   └── README.md                    # already exists; update it, don't replace it blind
│   └── grpc_shim/
│       ├── main.cpp                     # UNIMPLEMENTED stubs become real calls (Step 1.4)
│       └── CMakeLists.txt               # uncomment the seam (Step 1.4)
├── proto/
│   └── node.proto                       # extended with chunked RPCs (Step 1.2)
├── control/
│   └── cmd/pulsekv-node-bench/main.go   # NEW — Step 1.5
└── docs/
    └── pulsekv-v2-phase1-summary.md     # NEW — final deliverable
```

## Step 1.1 — Extract the engine

Copy `include/hashtable.h` and `src/hashtable.c` into `node/engine/src/`. This is the RAM tier's
foundation — the 1,024-bucket / 256-shard design and FNV-1a routing carry over unchanged; nothing
about that design was wrong, it just needs a value-size ceiling raised (Step 1.2) and a tier below
it (Step 1.3).

Design and implement `node/engine/include/pulsekv_engine.h` as the **only** thing `grpc_shim`
includes from this directory — it must not see `hashtable.h`, tiering internals, or any other
implementation detail. Starting shape (adjust field names/error cases as you see fit, but keep the
boundary this narrow — protobuf types must never appear on this side of it):

```c
#ifdef __cplusplus
extern "C" {
#endif

typedef struct pk_engine pk_engine_t;

typedef struct {
  const char *data_dir;          /* NVMe spill directory for this node */
  uint64_t    ram_budget_bytes;  /* total RAM-tier budget across all shards */
  uint64_t    max_value_bytes;   /* hard cap per value, chunked or not */
} pk_engine_config_t;

pk_engine_t *pk_engine_create(const pk_engine_config_t *cfg);
void         pk_engine_destroy(pk_engine_t *e);

typedef enum {
  PK_ENGINE_OK,
  PK_ENGINE_NOT_FOUND,
  PK_ENGINE_TOO_LARGE,
  PK_ENGINE_INVALID,
  PK_ENGINE_IO_ERROR
} pk_engine_result_t;

pk_engine_result_t pk_engine_put(pk_engine_t *e,
                                  const uint8_t *key, uint32_t key_len,
                                  const uint8_t *val, uint64_t val_len);

/* On PK_ENGINE_OK, *out_val is heap-allocated and owned by the caller;
   free with pk_engine_free_value. */
pk_engine_result_t pk_engine_get(pk_engine_t *e,
                                  const uint8_t *key, uint32_t key_len,
                                  uint8_t **out_val, uint64_t *out_len);
void pk_engine_free_value(uint8_t *val);

typedef struct {
  uint64_t resident_keys;
  uint64_t bytes_in_ram_tier;
  uint64_t bytes_in_nvme_tier;
} pk_engine_capacity_t;

void pk_engine_capacity(const pk_engine_t *e, pk_engine_capacity_t *out);

#ifdef __cplusplus
}
#endif
```

Note this maps directly onto `CapacityResponse` in `proto/node.proto` — that's intentional;
`grpc_shim`'s `Capacity` handler becomes close to a direct field copy.

Write `node/engine/CMakeLists.txt` producing a static library target (`pulsekv_engine`) plus test
binaries. Uncomment the seam already sitting in `node/grpc_shim/CMakeLists.txt`:

```cmake
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../engine" engine)
target_link_libraries(pulsekv-node PRIVATE pulsekv_engine)
```

**Regression gate:** v1's own test suite (`make test_hashtable` etc. from the repo root, untouched
Makefile) must still build and pass exactly as before — you copied, you didn't move, and the copy
proves it by not affecting the original.

## Step 1.2 — Chunked framing for large values

v1's wire protocol bounds values at 64 KiB in one frame — correct for v1, wrong for KV-cache
blocks that run into the megabytes. Two things need to change, and they're different concerns:

**The hard ceiling.** `pk_engine_config_t.max_value_bytes` (Step 1.1) is the real, enforced cap —
default it to 64 MiB, configurable via a node CLI flag. A value length must be validated against
this cap *before* any allocation is made for it, the same bounds-before-trust discipline
`protocol.h` already uses in v1. `PK_ENGINE_TOO_LARGE` is the result when a caller (chunked or
not) exceeds it.

**The wire path.** gRPC's own default max message size (4 MiB) is a reasonable line between
"small enough for one unary message" and "needs streaming." Extend `proto/node.proto` with a
chunked path for values above that line — starting shape:

```proto
message PutChunk {
  bytes key = 1;              // present on the first chunk, empty thereafter is also fine —
                               // document whichever convention you pick
  uint32 chunk_index = 2;
  uint32 total_chunks = 3;
  uint64 total_length = 4;    // sent on the first chunk; lets the engine reject an oversized
                               // value before buffering a single byte of it
  bytes data = 5;
}

service NodeService {
  // ... existing RPCs unchanged ...
  rpc PutChunked(stream PutChunk) returns (PutResponse);
  rpc GetChunked(GetRequest) returns (stream GetChunk);
}

message GetChunk {
  uint32 chunk_index = 1;
  uint32 total_chunks = 2;
  uint64 total_length = 3;
  bytes data = 4;
}
```

Existing unary `Get`/`Put` remain the fast path for values ≤ 4 MiB. A unary `Put` whose value
exceeds that line should fail fast with `INVALID_ARGUMENT` and a message pointing at
`PutChunked`, rather than silently accepting an oversized unary payload — loud and specific beats
quietly working until it doesn't.

Phase 1's engine buffers a chunked value into one contiguous allocation before calling
`pk_engine_put` — validate `total_length` against `max_value_bytes` on receipt of the first chunk,
before allocating, so a lying or corrupt `total_length` can't force a bogus allocation. True
streaming-into-the-engine without a full in-memory buffer is Phase 6's job, once the transport
layer is being optimized for real; don't build it now.

**Tests** (`node/engine/tests/test_engine_chunked.c` plus `grpc_shim` integration coverage):
partial chunks arriving out of order (reject or reassemble — pick one, document it), a chunk count
that doesn't match `total_chunks`, a `total_length` that lies (too small, too large, zero),
oversized values via both unary and chunked paths, and a full round-trip of a real multi-megabyte
value through `PutChunked` → `GetChunked` with byte-for-byte verification.

## Step 1.3 — Tiered storage (NVMe spill)

New code — nothing in v1 does this. Each shard tracks its own resident byte count and an access
order (simplest correct approach: an intrusive doubly-linked list per shard, protected by the same
mutex the shard already uses for the hash table — don't add a second lock; that's how tiering bugs
and hash-table bugs turn into deadlocks together). When a shard's resident bytes exceed
`ram_budget_bytes / PK_TABLE_SHARDS`, evict the coldest entries in that shard to `data_dir` until
back under budget.

Spill file layout is your call, but it must satisfy: content-addressed by key hash (avoid
pathological single-directory fan-out — a two-level hex-prefix split, matching how v1 already
avoids long hash chains, is a reasonable default), and **written atomically** (write to a temp
path, `rename()` into place) so a crash mid-spill can't leave a half-written value that a later
read returns as valid. This is a cache, not a WAL — losing an in-flight spill on crash is fine;
returning corrupt bytes for a key that looks present is not.

A `Get` on a key not found in the RAM tier must check the NVMe tier before returning
`PK_ENGINE_NOT_FOUND`. A hit there **promotes** the value back into the RAM tier (subject to the
same budget — promoting one entry may need to evict another), and the spilled file is removed
once the promotion is durable in RAM. An NVMe read/write I/O error should degrade gracefully
(`PK_ENGINE_IO_ERROR`, logged) rather than crash the node — this is a cache miss path failing, not
a fatal condition. Full disk-full fault injection in the style of v1's `/dev/full` WAL test is
good but not required this phase; a basic "write fails cleanly" check is enough — save the
thorough fault-injection pass for Phase 9's hardening work.

**Tests** (`test_engine_tiering.c`): insert until the RAM budget is exceeded, verify the coldest
keys spilled and `pk_engine_capacity` reflects it correctly; `Get` on a spilled key returns the
correct value and promotes it; repeated promote/demote cycles preserve value integrity exactly
(no truncation, no corruption); a working set several times larger than the RAM budget is fully
correct end to end, just slower.

**Concurrency:** `test_engine_stress.c` — multiple threads doing concurrent Put/Get against a
budget small enough to force constant eviction, verified against expected final state, in the
spirit of v1's `test_thread_stress`.

## Step 1.4 — Wire the gRPC shim to the real engine

In `node/grpc_shim/main.cpp`, replace each `NotImplementedYet(...)` call with a real
implementation calling through `pulsekv_engine.h`:

- `Get` → `pk_engine_get`, mapping `PK_ENGINE_NOT_FOUND` to `found = false` (not a gRPC error —
  a miss is a normal, successful response), and any other non-OK result to an appropriate gRPC
  status.
- `Put` → `pk_engine_put`, with the ≤4 MiB fast-path/`PutChunked`-required split from Step 1.2.
- `PutChunked` / `GetChunked` → the new streaming handlers from Step 1.2.
- `PrefixMatch` → this needs a real decision: v1's hash table has no ordered iteration by prefix
  today. A reasonable Phase 1 scope is a full-shard-scan implementation (correct, not yet
  optimized — note the complexity honestly rather than silently) unless you find a cheaper
  approach; either way, document the choice.
- `Capacity` → `pk_engine_capacity`, mapped field-for-field onto `CapacityResponse`.

Add a `--data-dir` and `--ram-budget-bytes` (and any other engine config) CLI flag to
`node/grpc_shim/main.cpp`, threaded through to `pk_engine_config_t`, and update
`deploy/cluster.config.yaml` / `deploy/run-local-cluster.sh` so each node in the local dev cluster
gets its own `data_dir` under `deploy/run/` — nodes must not share a spill directory.

**`deploy/smoke-test.sh` must be updated** — this is expected, not incidental. The Phase 0 summary
says explicitly: *"`deploy/smoke-test.sh` will start failing the moment those RPCs stop returning
`UNIMPLEMENTED` — that is intended."* Update those assertions to check real behavior: a `Put`
followed by a `Get` returns the value; a `Get` for a never-written key returns `found = false`; a
large value round-trips correctly through `PutChunked`/`GetChunked`; `Capacity` reflects a
non-zero `resident_keys` after a `Put`.

## Step 1.5 — Node-level benchmark

Add `control/cmd/pulsekv-node-bench/main.go`, using the already-generated `control/gen/node/v1`
stubs, driving a single running node directly (no cluster routing yet — that's Phase 2). Same
evidence discipline v1's `benchmark.c` established: every response verified against expected
state (never count an unchecked reply as throughput), configurable concurrency and value size,
warmup excluded from measurement, and reported min/mean/p50/p99/p999/max latency plus throughput.

Run it twice and record both results in the summary doc: once with a working set that fits
entirely in the configured RAM budget, and once with a working set several times larger, to
produce the first real, measured before/after picture of what the NVMe tier costs — this is the
baseline Phase 9's cluster-wide benchmark will later compare distributed overhead against.

## Exit criteria — verify all of these before considering Phase 1 done

1. `node/engine/` builds as its own CMake target; v1's existing test suite is unaffected and
   still passes via the untouched root `Makefile`.
2. `pulsekv_engine.h` is the only header `grpc_shim` includes from `node/engine/` — no protobuf
   type appears in engine code, no hashtable/tiering internal appears in `main.cpp`.
3. Chunked Put/Get round-trips a multi-megabyte value correctly; a unary `Put` above the 4 MiB
   line fails fast and specifically rather than silently accepting an oversized payload.
4. A working set larger than the configured RAM budget spills to NVMe, is served correctly on
   read (with promotion back to RAM), and survives repeated promote/demote cycles with no
   corruption — verified by `test_engine_tiering.c` and the concurrent stress test.
5. `grpc_shim`'s `Get`/`Put`/`PutChunked`/`GetChunked`/`Capacity` are real; `PrefixMatch`'s scope
   and implementation choice are documented explicitly.
6. `deploy/smoke-test.sh` passes with the updated, real-behavior assertions — no RPC this phase
   was responsible for still returns `UNIMPLEMENTED`.
7. `pulsekv-node-bench` produces a correctness-verified, percentile-reported benchmark for both
   the fits-in-RAM and exceeds-RAM scenarios, results recorded in the summary doc.
8. `docs/pulsekv-v2-phase1-summary.md` is written, in the same evidence-first style as
   `pulsekv-v2-phase0-summary.md`: exact file layout, any deviations from this prompt with
   reasoning, exit-criteria evidence, and where Phase 2 should start.

Do not start any Phase 2 work — even the parts the implementation plan says can run in
parallel — until this phase's exit criteria are verified and the summary is written.
