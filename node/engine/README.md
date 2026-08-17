# `node/engine/` — pure C storage engine

**Empty in Phase 0. This is a placeholder, not an oversight.**

Phase 1.1 populates this directory by extracting v1's proven components behind
a clean internal API:

| From v1 | Becomes | Phase |
|---|---|---|
| `src/hashtable.c`, `include/hashtable.h` | the RAM tier — 256-way sharded table, per-bucket mutexes | 1.1 |
| `src/main.c`'s epoll worker model | the node's event loop | 1.1 |
| `src/protocol.c`, `include/protocol.h` | extended with chunked/streaming framing for multi-MB values | 1.2 |
| `src/wal.c`, `include/wal.h` | repurposed narrowly for optional durable-mode replication, not the default cache path | 4.2 |

New in this directory, with no v1 ancestor:

- the NVMe spill tier, its eviction policy, and RAM promotion/demotion (Phase 1.3)
- the zero-copy bulk transport — `sendfile`/`splice`, shared memory for
  co-located adapters (Phase 6)

## Rules this directory is held to

1. **No gRPC, no C++.** Everything here compiles as C11 and is testable without
   a network stack. `node/grpc_shim/` is the only thing that knows gRPC exists,
   and it reaches in through `extern "C"` declarations. See `node/README.md` for
   why the boundary sits there.
2. **v1 is copied from, never moved.** `src/` and `include/` at the repo root
   stay intact and buildable. v1 is a finished project with its own test suite
   and docs; v2 borrows components from it rather than migrating it.
3. **v1's test suite is the regression gate for the extraction.** Phase 1.1 is a
   refactor with no behaviour change, and `tests/` proves that before any new
   capability lands on top.

## The seam that is already wired

`node/grpc_shim/CMakeLists.txt` carries a commented-out
`add_subdirectory(../engine)` and `target_link_libraries(... pulsekv_engine)`.
Phase 1.1 uncomments those, adds a `CMakeLists.txt` here defining the
`pulsekv_engine` target, and fills in `NodeServiceImpl`'s bodies. Nothing else
in the shim should need to change.
