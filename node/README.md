# `node/` — the PulseKV v2 data plane

One process per cluster node. It owns the actual storage: the in-memory shard
table, the NVMe spill tier, and (from Phase 6) the bulk blob transport. It is
the piece with direct lineage from v1.

```
node/
├── engine/      pure C. The storage engine. Empty in Phase 0.
└── grpc_shim/   thin C++. Turns NodeService RPCs into engine calls.
```

## Why there is C++ in an otherwise-C directory

This is the one design decision in `node/` that needs explaining, because it
looks like a contradiction with v1's from-scratch-C ethos.

gRPC has no supported C API for application use. The "C core" (`grpc/grpc.h`)
exists, but it is an implementation substrate for the language bindings — it is
explicitly not the surface applications are meant to code against, it has no
generated service stubs, and it changes without the compatibility guarantees the
C++ API carries. Writing a NodeService server directly against it would mean
hand-rolling completion-queue plumbing and message serialisation that
`protoc --grpc_out` generates for free everywhere else. That is effort spent
fighting a tool, and none of it is the interesting part of this project.

So the boundary sits here instead:

```
   gRPC (C++ API, generated stubs)         pure C, no gRPC anywhere
  ┌───────────────────────────┐         ┌──────────────────────────┐
  │  node/grpc_shim/main.cpp  │  ────►  │  node/engine/*.c         │
  │  NodeServiceImpl          │ extern  │  sharded table, epoll,   │
  │  ~200 lines, no logic     │   "C"   │  tiering, WAL, framing   │
  └───────────────────────────┘         └──────────────────────────┘
```

The rule the shim is held to: **it contains no storage logic.** It unpacks a
protobuf, calls one `extern "C"` function, packs the result, and returns a
status. Phase 0's `main.cpp` is ~280 lines and most of that is argument parsing
and shutdown handling; if it grows much past that, something has leaked in. Every decision worth making — hashing, locking, eviction, tier
placement, framing — lives in `engine/`, in C, testable without gRPC in the
picture at all. If the shim ever grows a branch that depends on what is *in*
the store, that branch belongs in `engine/`.

This split is the standard pattern for giving a C library a gRPC surface, and
it is the same shape the rest of this ecosystem uses — TiKV, Mooncake, and
SGLang all keep a native data plane behind a thin generated-RPC boundary.

### What it buys later

The bulk KV-cache data path is explicitly *not* going through gRPC (see
`docs/pulsekv-v2-distributed-design.md` §4.5). Multi-megabyte tensor blocks get
v2's own chunked/streaming transport in Phase 6, node-to-node, with the
shared-memory path for co-located adapters. gRPC carries control messages —
health, capacity, keys, iovec descriptors — and nothing large. Keeping the
engine free of gRPC is what makes that second, leaner data path possible without
a rewrite.

## `engine/` — empty on purpose in Phase 0

Phase 1.1 extracts v1's `src/hashtable.c` and the worker model from
`src/main.c` into here, behind a clean internal API, with v1's existing test
suite as the regression gate. Phase 0 deliberately does not touch it: the point
of Phase 0 is to freeze the contract so Phases 1 (this directory) and 2 (the Go
control plane) can proceed independently, and starting the extraction early
would defeat that.

v1's `src/` and `include/` stay exactly where they are. v1 remains a standalone,
complete, documented project; Phase 1 copies from it rather than moving it.

## `grpc_shim/` — Phase 0 behaviour

| RPC | Phase 0 | Arrives in |
|---|---|---|
| `HealthCheck` | real: `ok=true`, this node's ID, actual uptime | — |
| `Get` | `UNIMPLEMENTED` | Phase 1.4 |
| `Put` | `UNIMPLEMENTED` | Phase 1.4 |
| `PrefixMatch` | `UNIMPLEMENTED` | Phase 1.4 |
| `Capacity` | `UNIMPLEMENTED` | Phase 1.4 |

`UNIMPLEMENTED` rather than an empty success is load-bearing. A `Get` that
returned `found=false` would be indistinguishable from a working engine that
simply has nothing stored, and every layer built on top would silently treat the
skeleton as functional. `deploy/smoke-test.sh` asserts the status code on every
one of these RPCs for exactly that reason.

## Building and running by hand

```sh
cmake -S node/grpc_shim -B deploy/build/cmake -DCMAKE_BUILD_TYPE=Release
cmake --build deploy/build/cmake -j

deploy/build/cmake/pulsekv-node --node-id node-0 --port 7100
```

Requires the v2 dev image (`deploy/Dockerfile`) or a Linux host with
`libgrpc++-dev`, `protobuf-compiler-grpc`, `cmake`, and `pkg-config`. Normally
you would just run `deploy/run-local-cluster.sh`, which does this for every
node in `deploy/cluster.config.yaml`.

Server reflection is compiled in when `libgrpc++_reflection` is available, so a
running node can be poked at directly:

```sh
grpcurl -plaintext 127.0.0.1:7100 pulsekv.node.v1.NodeService/HealthCheck
```
