# `node/` — the PulseKV v2 data plane

One process per cluster node. It owns the actual storage: the in-memory shard
table, the NVMe spill tier, and (from Phase 6) the bulk blob transport. It is
the piece with direct lineage from v1.

```
node/
├── engine/      pure C. The storage engine. Empty in Phase 0.
└── grpc_shim/   thin C++. Turns NodeService RPCs into engine calls, and
                 (from Phase 4) forwards writes to this shard's replicas.
```

Phase 4 made the shim a gRPC **client** as well as a server: it polls
`ClusterMetadataService` for the shards it primaries, keeps cached
`NodeService` stubs to those shards' replica peers, and forwards writes to them.
That is a network-layer concern and it changed nothing below the line described
next — `pk_engine_put` cannot tell a client's write from a replicated copy, and
the engine has no concept of a primary, a replica, or a peer. Replication is
off entirely unless the node is started with `--metadata-addr`.

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
  │  + replication, no logic  │   "C"   │  tiering, WAL, framing   │
  └───────────────────────────┘         └──────────────────────────┘
```

The rule the shim is held to: **it contains no storage logic.** It unpacks a
protobuf, calls one `extern "C"` function, packs the result, and returns a
status. Every decision worth making — hashing, locking, eviction, tier
placement, framing — lives in `engine/`, in C, testable without gRPC in the
picture at all. If the shim ever grows a branch that depends on what is *in*
the store, that branch belongs in `engine/`.

Phase 4's replication code is the one thing here that is not a straight
unpack-call-pack, and it stays on the right side of that rule: it branches on
*where a key belongs* — which shard, which peers hold it — never on what is
stored under it. The engine is still the only thing that knows what is in the
store.

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

## `engine/` — the storage engine

Populated in Phase 1 by copying v1's `src/hashtable.c` and building a tiering
layer underneath it. v1's `src/` and `include/` stay exactly where they are —
v1 remains a standalone, complete, documented project, and its own test suite is
the regression gate proving the copy did not disturb it.

**v1's epoll worker model was deliberately not extracted.** The Phase 1 plan
called for it, but that instruction predates Phase 0's decision to put gRPC C++
in front of the data plane: gRPC's server already owns its sockets and its
thread pool, so v1's hand-rolled event loop would be dead code with no caller.
Only the storage logic needed extracting. See `node/engine/README.md` for the
full inventory of what came across and what did not.

## `grpc_shim/` — behaviour

Every RPC is real as of Phase 1; nothing returns `UNIMPLEMENTED`.

| RPC | Behaviour |
|---|---|
| `HealthCheck` | `ok=true`, this node's ID, actual uptime |
| `Get` | value for keys up to 4 MiB; a miss is `found=false` with status OK, never an error. A stored value above the unary limit returns `FAILED_PRECONDITION` naming `GetChunked`. |
| `Put` | writes values up to 4 MiB; above that, `INVALID_ARGUMENT` naming `PutChunked`. When this node primaries the key's shard it also replicates: in the background by default, or blocking for `require_replica_acks` replicas when asked. `from_replication` marks a forwarded copy, which is stored and never forwarded on. |
| `PutChunked` | client-streaming write for larger values. Chunks must arrive in order from index 0; `total_length` is validated against `--max-value-bytes` before a byte is buffered. Replicates in the background only — there is no chunked strong-ack mode. |
| `GetChunked` | server-streaming read, always valid. A miss is an empty stream. |
| `PrefixMatch` | full scan of all 256 shards, O(total keys). Values above the unary limit are flagged `value_omitted` rather than inlined. |
| `Capacity` | per-tier key and byte occupancy, straight from the engine |

Two behaviours worth knowing because they are easy to get subtly wrong, and are
asserted by `deploy/smoke-test.sh`:

- **A miss is a success.** Reporting it as a gRPC error would make every cache
  miss look like a failure in the caller's metrics.
- **A rejected write stores nothing.** The smoke test writes eight deliberately
  malformed requests per node and then checks that none of those keys exist.

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
