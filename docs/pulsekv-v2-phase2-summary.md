# PulseKV v2 — Phase 2 Summary

**Status: complete.** Read this first if you are picking up Phase 3.

Phase 2 turned the static Go control-plane skeleton into a real cluster router:
deterministic rendezvous-hash shard ownership, a public LLM-agnostic client SDK,
one shared unary/chunked wire implementation, and correctness-verifying tools
that prove physical placement rather than only successful round trips.

The node list is still static and read once from `deploy/cluster.config.yaml`.
There is no gossip, replication, Raft, or capacity-aware placement in this
phase. `node/`, `adapters/`, and `proto/` are untouched.

Companion docs: `pulsekv-v2-distributed-design.md` (what v2 is and why),
`pulsekv-v2-implementation-plan.md` (the phase order), and
`pulsekv-v2-phase1-summary.md` (the data-plane engine this phase routes to).

---

## 1. Exact implementation layout

```
control/
├── internal/
│   ├── router/
│   │   ├── router.go                 # key -> shard, shard -> HRW owner
│   │   └── router_test.go            # exact movement + distribution tests
│   ├── transport/
│   │   ├── transport.go              # the one unary/chunked wire implementation
│   │   └── transport_test.go         # framing and malformed-stream tests
│   ├── config/
│   │   ├── config.go                 # Config.ShardMap placeholder removed
│   │   └── config_test.go            # round-robin-only test removed
│   └── metadata/
│       ├── service.go                # GetShardMap calls router.AssignShards
│       └── service_test.go           # repeated-call determinism + HRW equality
├── pkg/client/
│   ├── client.go                     # public cluster SDK
│   └── client_test.go                # real-gRPC routing/refresh/chunk/Close tests
└── cmd/
    ├── pulsekv-example/main.go       # non-LLM note-store example
    ├── pulsekv-cluster-bench/main.go # routed correctness + latency benchmark
    ├── pulsekv-node-bench/main.go    # refactored onto internal/transport
    ├── pulsekv-smoke/main.go         # exact live map + physical placement checks
    ├── pulsekv-smoke/main_test.go    # four-node in-process routing proof
    └── controlplane/main.go          # Phase 2 scope comment corrected

deploy/
├── common.sh                         # paths for the new Phase 2 binaries
├── run-local-cluster.sh              # builds and advertises example + cluster bench
├── smoke-test.sh                     # Go contract + routing leg
├── cluster.config.yaml               # HRW/static-membership comments corrected
└── README.md                         # Phase 2 commands and guarantees

docs/pulsekv-v2-phase2-summary.md      # this file
```

No generated protobuf was changed, and `control/go.mod` / `go.sum` gained no
dependency. The router and transport are internal implementation details; the
only application-facing surface is `control/pkg/client`.

---

## 2. Router: the algorithm and the bug that mattered

There are deliberately two independent mappings:

1. **Key to logical shard:** stable FNV-1a-64 over the raw key, reduced modulo
   the configured shard count. Membership cannot change a key's shard.
2. **Logical shard to node:** rendezvous/HRW argmax over every configured node.
   Membership can move ownership while shard identity stays fixed.

`AssignShards` is pure: no config reads, RPCs, time, random seed, or previous
assignment. Ties use the lexicographically smaller node ID, so input slice
order cannot decide a collision.

### The first weight function was structurally correct and empirically bad

The original Phase 2 prompt proposed plain FNV-1a over
`"<shard>:<nodeID>"`. Its HRW movement invariant held, but its distribution did
not. Node IDs that differed only in trailing characters retained too much
shared high-bit structure, and weight comparison is dominated by those high
bits.

Measured before the correction, over 256 shards:

| Case | Plain FNV-1a result | Expected shape |
|---|---:|---:|
| 32 nodes | 12 nodes owned zero shards; busiest owned 39 | about 8 each |
| 16 nodes | min 3, max 39 | about 16 each |
| 4 -> 5 join | 127 shards moved | ideal about 51 |
| 8 -> 9 join | 131 shards moved | ideal about 28 |

The fix hashes each node ID once with FNV-1a (`nodeSeed`) and applies the
splitmix64 finalizer to `nodeSeed(nodeID) ^ mix64(shard)`. This supplies the
avalanche FNV-1a lacked for the patterned node IDs a real cluster uses. Hoisting
the node seed also reduces recomputation from one string hash per
`(shard,node)` pair to one string hash per node plus cheap integer mixing.

Measured after the correction:

- 32 nodes: min 2, max 12, and no empty owner.
- 4 -> 5 join: 68 of 256 shards moved.
- 8 -> 9 join: 38 of 256 shards moved.
- Every tested addition/removal case: **zero** shards moved between two nodes
  that existed both before and after the membership change.

The magnitude assertion in `TestAdditionOnlyTakesShards` remains essential.
The shape-only invariant passed even with the broken hash because an argmax
still only lets a joining node take shards; it says nothing about how many the
new node takes.

### Current router test evidence

The final verbose test run reported:

| Nodes | Min shards | Max shards | Expected average |
|---:|---:|---:|---:|
| 4 | 50 | 76 | 64 |
| 8 | 20 | 40 | 32 |
| 16 | 6 | 23 | 16 |
| 32 | 2 | 12 | 8 |

The 8 -> 9 test assigned the joining node 38 shards (ideal about 28) and moved
none between existing nodes. Eight general add/remove cases each reported zero
survivor-to-survivor movement.

---

## 3. Metadata wiring

`Config.ShardMap()` was removed completely, along with the test whose only
purpose was to assert its exact round-robin balance. No caller remains.

`metadata.Service.GetShardMap` now returns:

```go
router.AssignShards(s.cfg.NodeIDs(), s.cfg.ShardCount)
```

directly on each request. `TestGetShardMapIsDeterministicAndUsesHRW` calls the
service twice, compares both responses to an independent `AssignShards` call,
and compares them to each other.

`GetNodeList` and `aliveness()` were not changed. `alive` is still a bounded,
point-in-time `NodeService.HealthCheck`, has no effect on ownership, and is not
treated as membership. A node that fails that probe still owns its shards until
Phase 3 supplies a real membership view.

---

## 4. One wire implementation and one public SDK

### `control/internal/transport`

The unary/chunked logic was extracted from `pulsekv-node-bench`; the benchmark
and SDK now call the same code.

- `Put`: unary through the protobuf's 4 MiB limit; otherwise 1 MiB
  `PutChunked` frames.
- `Get`: tries unary and falls back to `GetChunked` only for
  `FAILED_PRECONDITION`, the server's explicit large-value signal.
- `GetWithMode`: lets the node benchmark select a direct path when it already
  knows the stored size.
- Chunked reads validate chunk indices, total count, repeated stream metadata,
  and exact total length.
- An empty stream is a miss. One empty chunk is a present zero-length value.

This fixes a subtle ambiguity in the old benchmark helper, which represented a
miss as `nil` bytes and therefore could not distinguish it from a legitimate
zero-length hit.

### `control/pkg/client`

The public API is deliberately generic:

```go
client.New(controlPlaneAddr, opts...)
Client.Get(ctx, key)
Client.Put(ctx, key, value)
Client.PrefixMatch(ctx, prefix)
Client.Close()
```

Behavior:

- `New` eagerly fetches both `GetNodeList` and `GetShardMap` and refuses an
  incomplete topology.
- The default five-second background poll installs a new topology only after
  full validation. A failed or malformed refresh retains the last complete
  map.
- Routing is `router.OwnerForKey` -> node ID -> advertised node address.
- Connections are lazy and reused once per unique node address, including
  concurrent first use.
- Duplicate node IDs, duplicate node addresses, missing shards, and unknown
  owners are rejected.
- `Close` is a synchronization barrier: concurrent callers wait for the same
  shutdown and receive the same result.
- Values above 4 MiB use the shared chunked transport transparently.

`PrefixMatch` fans the scan out to every physical node because a byte prefix
does not identify one logical shard. It de-duplicates returned keys and
re-fetches each through normal routing. The engine scan is not a snapshot, so
a key that disappears between listing and fetching is silently skipped; other
scan or fetch errors still fail the operation.

Focused real-gRPC tests cover exact owner routing, wrong-node absence,
cluster-wide prefix scans, the scan-to-fetch disappearance race, topology
refresh and last-good retention, a value 137 bytes above 4 MiB through both
streaming RPCs, 32-way concurrent first dialing, and concurrent `Close`.

---

## 5. Application and routing evidence

### Non-LLM example

`pulsekv-example` imports only the public SDK and implements a toy note store.
Against the four-node dev cluster it wrote, read, byte-verified, and prefix
listed three notes:

```
phase2-example-notes:groceries = oats, coffee, oranges
phase2-example-notes:project = ship the routing skeleton
phase2-example-notes:weekend = walk the High Line
verified 3 notes through the public PulseKV client
```

That is executable evidence for the design document's “any app can use this”
claim; no LLM type or adapter is involved.

### Cluster benchmark

`pulsekv-cluster-bench` first fetches metadata through its own connection and
cross-checks the live map against `router.AssignShards`. It then finds one key
owned by each shard owner and performs three independent checks per sample:

1. put through the public SDK;
2. direct `NodeService.Get` from the predicted owner must hit and match;
3. direct `NodeService.Get` from a different node must miss.

Only after that proof does it populate the working set, run discarded warmup,
and measure mixed traffic. Every read is regenerated and compared byte for
byte. Any mismatch, unexpected miss, incomplete operation count, or RPC error
makes the command exit non-zero.

### Smoke test

The Go leg used by `deploy/smoke-test.sh` now independently compares all 256
live shard entries to `router.AssignShards`, then writes six SDK-routed sample
keys chosen to cover all four owners. Each is read through the SDK, hit directly
on the predicted owner, and missed directly on a different node.

An in-process four-node gRPC test exercises the same complete path without
requiring the C++ cluster.

---

## 6. Verification evidence

### Static and race gates

All passed:

```sh
cd control
go test -count=1 ./...
go vet ./...
go test -race -count=1 \
  ./internal/router ./internal/metadata ./internal/transport \
  ./pkg/client ./cmd/pulsekv-smoke
go build ./cmd/...
```

The client was additionally run repeatedly under the race detector while its
refresh, connection cache, and close paths were being reviewed.

`git diff --check` is clean. These scope checks produce no output:

```sh
git diff --stat -- node
git diff --stat -- adapters
git diff --stat -- proto
```

### Four-node live smoke

The standard Linux dev image built the control plane, smoke tool, both
benchmarks, example, and C++ node; booted one control plane plus four nodes; and
reported all five healthy after one poll.

Results:

- Go smoke leg: **91/91** internal checks passed.
- Live map: all 256 entries exactly matched `router.AssignShards`.
- Routing samples: every one of four owners covered; predicted-node hit and
  different-node miss for every sample.
- Existing node contract coverage remained green, including a 6 MiB value over
  six chunks and all malformed-stream rejection checks on all four nodes.
- Python -> Go and Python -> C++ health checks passed.
- Reflection checks passed.
- Top-level `deploy/smoke-test.sh --no-install`: **6 checks, 0 failures**.
- Shutdown stopped all five processes and found no orphan.

### Correctness-verified unary cluster run

Configuration: 4 nodes, 256 shards, 8 workers, 16 KiB values, 256 keys,
400 discarded warmup operations, 4,000 measured operations, 80% reads, seed 42.

| Path | Count | Min | p50 | p99 | p999 | Max | Mean |
|---|---:|---:|---:|---:|---:|---:|---:|
| Read | 3,201 | 0.063 ms | 0.379 ms | 2.053 ms | 2.477 ms | 3.823 ms | 0.467 ms |
| Write | 799 | 0.059 ms | 0.348 ms | 1.780 ms | 2.461 ms | 2.461 ms | 0.451 ms |
| Overall | 4,000 | 0.059 ms | 0.374 ms | 2.015 ms | 2.477 ms | 3.823 ms | 0.464 ms |

- Throughput: **16,420 ops/s** over 244 ms, 256.6 MiB/s of value payload.
- Verification: **3,201/3,201 reads matched byte-for-byte**.
- Failures: 0 mismatches, 0 unexpected misses, 0 RPC errors.
- Routing proof: one direct owner-hit/value-match plus non-owner miss for each
  of the four owners.

These are local dev-cluster numbers, not Phase 9 performance targets.

### Live chunked-path sanity run

A separate small run used 5 MiB values, forcing SDK `PutChunked` and automatic
`GetChunked` fallback across the real C++ nodes:

- 8 keys / 40 MiB working set, 2 workers, 4 discarded warmup operations,
  24 measured operations.
- All four owners passed the independent physical-placement proof.
- **19/19 reads matched byte-for-byte**.
- 0 mismatches, 0 unexpected misses, 0 RPC errors.
- 71 ops/s and 357.0 MiB/s of value payload for this short run.

Twenty-four operations are enough to verify the path, not to claim stable tail
latency; the unary run above is the phase's benchmark result.

---

## 7. Deviations and decisions

### The prompt's plain-FNV HRW weight was replaced

This is the material deviation, and it was required by measurement. The exact
before/after evidence is in §2. The public behavior and movement invariant did
not change; the weight's avalanche quality did.

### Physical placement is checked with direct reads

The original prompt allowed observing a capacity counter. Direct predicted-node
hit/value-match plus different-node miss is stronger: it proves the exact key
and bytes are on one physical node and absent from another, without inferring
placement from an aggregate counter that other concurrent writes can change.

### PrefixMatch is a cluster fan-out

A prefix cannot be routed to one shard, so the SDK scans every node and then
routes each discovered key normally. This is sequential and O(total keys),
matching the engine's documented non-snapshot/full-scan semantics. Building a
secondary prefix index is outside Phase 2.

### Smoke routing lives in the Go smoke tool

`deploy/smoke-test.sh` invokes the expanded `pulsekv-smoke --mode=smoke` leg.
The exact HRW comparison stays in Go, where it imports the real router and
generated stubs, rather than duplicating either algorithm in shell.

### Deployment convenience was extended

`run-local-cluster.sh` now builds `pulsekv-example` and
`pulsekv-cluster-bench` beside the existing binaries and prints ready-to-run
commands. This is additive and does not change cluster startup behavior.

---

## 8. Exit criteria — verified

| # | Criterion | Evidence |
|---:|---|---|
| 1 | Round-robin config map gone; metadata serves deterministic HRW | `Config.ShardMap` and its exact-balance test removed; metadata repeated-call/HRW test passes; live 256-entry exact comparison passes |
| 2 | Public SDK exists; node benchmark shares wire logic | `control/pkg/client`; both SDK and node benchmark call `control/internal/transport`; real-gRPC unary/chunk tests pass |
| 3 | Non-LLM example runs | Three-note live output in §5 |
| 4 | Cluster benchmark verifies data, percentiles, and placement | 4,000-op result plus direct owner/non-owner proof in §6; separate live chunked run also passes |
| 5 | Extended smoke passes | 91/91 Go checks and 6/6 top-level checks, including exact map and six physical-placement samples |
| 6 | `node/` untouched | `git diff --stat -- node` is empty; `adapters/` and `proto/` are empty too |
| 7 | Phase summary written | This document, including the router bug/fix history and Phase 3 handoff |

---

## 9. Known limitations and where Phase 3 starts

- Membership is static. The client polls metadata, but metadata always derives
  the same node set loaded at control-plane startup.
- `NodeInfo.alive` is informational only. A failed node continues owning its
  shards; requests to those shards fail until Phase 3 changes membership.
- There is no replication, fallback owner, or repair. A wrong-node miss is the
  expected proof of single ownership in this phase.
- Prefix scans are sequential cluster fan-outs and are not snapshots.
- Background refresh deliberately keeps the last complete topology on control-
  plane failure. There is no topology version or push invalidation yet.
- Benchmark traffic is synthetic and local. Phase 9 owns sustained load,
  realistic hot-prefix distributions, observability, and tuning claims.

Phase 3 should begin at exactly two seams:

1. Replace `metadata.Service`'s static config node list and synchronous
   `aliveness()` probe with a maintained SWIM-style gossip membership snapshot.
2. Recompute the existing, unchanged `router.AssignShards` function whenever
   that snapshot changes and continue serving it through the same
   `ClusterMetadataService` RPCs.

The SDK's polling and atomic last-good topology replacement already consume
that contract. Phase 3 should reuse the smoke/benchmark physical-placement
proof while adding kill/restart chaos assertions: bounded movement, zero
survivor-to-survivor movement, no split-brain ownership, and explicitly bounded
unavailability for keys whose owner failed. Do not add replication or Raft to
that phase; those remain Phases 4 and 5.
