# `node/engine/` — pure C storage engine

The RAM tier, the NVMe spill tier, and the eviction policy that moves values
between them. Populated in Phase 1 by extracting v1's sharded hash table and
building tiering underneath it.

```
node/engine/
├── include/pulsekv_engine.h   the whole public surface, extern "C"
├── src/
│   ├── engine.c               the public API; glue and result translation only
│   ├── hashtable.[ch]         RAM tier: v1's table + LRU + accounting + spill hook
│   └── tiering.[ch]           NVMe tier: paths, atomic writes, verified reads
├── tests/
│   ├── test_util.h            shared scaffolding, in v1's tests/ style
│   ├── test_engine_basic.c    round trips, ownership, collisions, scans
│   ├── test_engine_chunked.c  the value-size ceiling, multi-megabyte values
│   ├── test_engine_tiering.c  spill, promote, accounting, budget semantics
│   └── test_engine_stress.c   concurrency with eviction running underneath
└── CMakeLists.txt             libpulsekv_engine + the four test binaries
```

## Rules this directory is held to

1. **No gRPC, no C++, no protobuf.** Everything here compiles as C11 and is
   testable without a network stack. `node/grpc_shim/` is the only thing that
   knows gRPC exists, and it reaches in through `pulsekv_engine.h`. This is
   enforced by CMake, not by discipline: the `pulsekv_engine` target exports
   `include/` as PUBLIC and `src/` as PRIVATE, so `hashtable.h` and `tiering.h`
   are not on the shim's include path at all and a stray `#include` of one
   fails to compile.
2. **v1 was copied from, never moved.** `src/` and `include/` at the repo root
   are untouched and still build and pass their own suite. v1 is a finished
   project; v2 borrows components rather than migrating it.

## What came from v1, and what did not

| From v1 | Status |
|---|---|
| `src/hashtable.c` — 1,024 buckets over 256 mutex-striped shards, FNV-1a routing, copy-in/copy-out ownership | **Copied and extended.** The design needed no correcting; concurrent gRPC handler threads are exactly the access pattern the striping was built for. |
| `src/main.c`'s epoll worker model | **Deliberately not extracted.** gRPC C++ owns its own sockets and thread pool, so v1's event loop would be dead code with no caller. See the summary doc — this is a scope correction, not a shortcut. |
| `src/wal.c` | Not here. Phase 1's tier is explicitly loss-tolerant; the WAL gets repurposed narrowly for optional durable-mode replication in Phase 4. |

Extensions on top of the copy:

- `uint64` value lengths. v1 capped a value at 64 KiB in one frame; KV-cache
  blocks run to megabytes.
- A per-shard intrusive LRU list, under the shard mutex that already exists —
  **not** a second lock. A tiering lock and a table lock taken in two orders is
  how tiering bugs and hash-table bugs become deadlocks together.
- Per-shard, per-tier byte and key accounting, so eviction decisions and
  `CapacityResponse` read from the same numbers.
- Spill to and promotion from the NVMe tier.

## Design decisions worth knowing before changing anything

**A spilled entry keeps its index entry in RAM.** Only the value moves to disk.
That is what makes a miss on a never-seen key cost zero filesystem work, keeps
capacity accounting exact without walking the spill tree, and lets a prefix scan
see spilled keys. The cost is one node plus the key bytes per spilled entry,
which against megabyte-scale values is noise.

**The LRU list threads only resident entries.** A spilled entry has no RAM left
to reclaim, so leaving it in the list would make eviction walk past it forever.

**The budget is per shard, not global.** `ram_budget_bytes / 256` is what each
shard enforces. A global counter would be one contended cache line on every
write, which is the bottleneck the striping exists to avoid. Two consequences,
both asserted in `test_engine_tiering.c` rather than left to be rediscovered:

- A shard never evicts its only entry, so a value larger than its shard's share
  stays resident. Without that, every large write would be flushed to disk by
  the very operation that stored it.
- Total resident bytes can therefore exceed `ram_budget_bytes`, bounded by
  `budget + 256 × max_value_bytes` in the worst case.

**Spill files are self-describing and published by `rename()`.** Each file
carries a magic, the key, and both lengths; a read verifies all of them plus the
exact file size before returning a byte. Losing a spilled value on crash is
fine — this is a cache. Returning a truncated or mismatched one is not, and the
verification is what makes a 64-bit hash collision harmless instead of a silent
wrong answer.

**The tier is purged at startup and shutdown.** Spill files are unreachable once
the in-RAM index naming them is gone, so anything found in the directory is
garbage from a previous run. The engine creates and exclusively owns
`<data_dir>/spill/` and touches nothing outside it.

## Building and testing

Standalone — no gRPC toolchain needed:

```sh
cmake -S node/engine -B /tmp/eng -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/eng -j
/tmp/eng/test_engine_basic
```

Or the full suite, in the v2 dev image:

```sh
deploy/test-engine.sh            # release
deploy/test-engine.sh --all      # + ThreadSanitizer + Valgrind
```

The three modes exist for the same reason v1 kept a parallel TSan build and ran
its store tests under Valgrind: for a concurrent structure holding long-lived
heap state, "the tests pass" is not evidence on its own.

## Known limitations, carried forward deliberately

- **Fixed bucket array.** Inherited from v1. Well past `PK_TABLE_BUCKETS` live
  keys, chains lengthen and lookups drift toward O(n). Growth-and-rehash under
  the lock is still out of scope.
- **The NVMe tier is unbounded.** Nothing evicts *from* disk; the tier grows
  until the filesystem refuses, at which point spilling fails, the entry is
  dropped, and `spill_errors` counts it. A second-level eviction policy and
  disk-full fault injection are Phase 9 hardening.
- **Spill I/O happens under the shard lock.** One shard serializes behind a disk
  write. Measured in the Phase 1 summary rather than hand-waved. Dropping the
  lock around the I/O means handling a concurrent overwrite of the node being
  spilled; that belongs with Phase 6's transport work.
- **`PrefixMatch` is a full scan**, O(total keys), because the table has no
  ordered iteration.
