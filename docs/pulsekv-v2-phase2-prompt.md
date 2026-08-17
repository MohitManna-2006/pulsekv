# PulseKV v2 — Phase 2 Implementation Prompt (for Claude Code)

**How to use this file:** paste everything below the line into Claude Code as the task prompt for
the Phase 2 session, run from inside the `pulsekv` repo root, on top of commit `c7ef6d1` ("V2
phase 1") or later.

---

You are implementing **Phase 2 only** of PulseKV v2. Before writing any code, read, in order:

1. `docs/pulsekv-v2-distributed-design.md` — what v2 is and why. Section 4.1 specifically:
   rendezvous hashing was chosen over a hash ring or Jump Hash for minimal key movement under
   elastic membership.
2. `docs/pulsekv-v2-implementation-plan.md`, Section 5 ("Phase 2 — control-plane routing
   skeleton") — the original phase definition, expanded below with what Phases 0 and 1 actually
   built.
3. `docs/pulsekv-v2-phase1-summary.md`, especially **Section 7, "Where Phase 2 starts"** — it
   names the exact seams waiting for you: `Config.ShardMap()`'s round-robin placeholder,
   `pulsekv-node-bench`'s reusable client logic, and `NodeInfo.alive` staying a direct probe until
   Phase 3. Treat that section as your starting checklist.
4. `control/internal/config/config.go`, `control/internal/metadata/service.go`,
   `proto/metadata.proto`, `control/cmd/pulsekv-node-bench/main.go` — the actual current state of
   what you're extending.

## Hard scope boundary

- **Do not modify anything under `node/`.** This phase is pure control-plane (Go) work. The data
  plane doesn't change.
- **Do not** implement gossip membership. The node list stays exactly what it is —
  static, read once from `deploy/cluster.config.yaml` at control-plane startup. That's Phase 3.
- **Do not** implement Raft, replication, or anything durability-related.
- **Do not** touch `adapters/`. That's Phases 7 and 8.
- **Do not** rename or remove any proto RPC or message field. This phase most likely needs **no**
  proto changes at all — `NodeService` (data path) and `ClusterMetadataService` (shard map, node
  list) already carry everything routing needs. If you find yourself wanting a new RPC, stop and
  re-read the design doc before adding one; that would be a signal something's off, not a green
  light.
- **Do not** build capacity-aware placement. `NodeService.Capacity` exists and is real (Phase 1),
  but nothing in this phase should make routing decisions from it. Shard ownership is purely a
  function of the node list and the shard count — see the caution note in Step 2.1.

## Target additions to the repository layout

```
pulsekv/
├── control/
│   ├── internal/
│   │   ├── router/                       # NEW — Step 2.1
│   │   │   ├── router.go                 #   ShardForKey, AssignShards (rendezvous/HRW)
│   │   │   └── router_test.go            #   the movement-invariant tests
│   │   ├── config/config.go              # ShardMap() round-robin placeholder REMOVED
│   │   └── metadata/service.go           # GetShardMap now calls router.AssignShards
│   ├── pkg/
│   │   └── client/                       # NEW — Step 2.2, the public SDK
│   │       ├── client.go                 #   Client, Get/Put/PrefixMatch
│   │       └── client_test.go
│   ├── cmd/
│   │   ├── pulsekv-cluster-bench/main.go # NEW — Step 2.4
│   │   ├── pulsekv-example/main.go       # NEW — Step 2.2's reusability proof
│   │   └── pulsekv-node-bench/main.go    # unchanged behavior; may be refactored internally
│   │                                     # to share transport code with pkg/client (see 2.2)
│   └── cmd/pulsekv-smoke/main.go         # extended with routing checks (Step 2.4)
└── docs/
    └── pulsekv-v2-phase2-summary.md      # NEW — final deliverable
```

A note on the package layout: `control/internal/router` stays under `internal/`, which in Go means
it's importable by anything rooted at `pulsekv/control/` — including `control/pkg/client`, even
though that package is itself importable by other, external Go modules. That's the intended shape:
`pkg/client` is the public facade; `internal/router` is an implementation detail it's allowed to
depend on. Don't move `router` into `pkg/` to "simplify" this — the whole point of Go's `internal/`
convention is that a routing algorithm's exact shape isn't something external consumers should be
able to depend on directly.

## Step 2.1 — Rendezvous hashing router

There are two hashes here, not one, and they answer different questions. Keep them distinct:

**Key → shard.** A key is deterministically mapped to one of `cfg.ShardCount` (default 256,
distinct from the engine's own 256 lock shards — see `config.go`'s existing comment on this)
logical cluster shards. This mapping must **never** change for a fixed `ShardCount`, regardless of
cluster membership — it's what lets shard *ownership* move independently of shard *identity*.
Use `hash/fnv`'s 64-bit FNV-1a (same family v1 and the engine already use, for the same reason:
simple, deterministic, no new dependency) over the raw key bytes, reduced mod `ShardCount`.

```go
func ShardForKey(key []byte, shardCount uint32) uint32 {
    h := fnv.New64a()
    h.Write(key)
    return uint32(h.Sum64() % uint64(shardCount))
}
```

**Shard → node.** For each of the `ShardCount` shards, compute a weight against every currently
configured node and assign the shard to the highest-weight node — this is rendezvous/HRW hashing,
and it's what gives the property the design doc calls for: removing a node only moves *that node's*
shards, and adding a node only *takes* shards from existing owners, never reshuffles a shard
between two nodes that were both present before and after.

```go
func AssignShards(nodeIDs []string, shardCount uint32) map[uint32]string {
    // for each shard s in [0, shardCount):
    //   winner = argmax over nodeIDs of weight(s, nodeID)
    //   weight(s, nodeID) = FNV-1a-64("<s>:<nodeID>")
    //   tie-break: lexicographically smallest nodeID (won't matter in practice
    //   with a 64-bit hash, but pick a determinstic rule and test it)
}
```

Both functions are pure and side-effect-free — no gRPC, no config parsing, nothing but the
algorithm. That's what makes the tests below exact rather than approximate.

**Tests (`router_test.go`) — assert the actual invariant, not an approximation of it:**

- **No shard moves between two nodes that are both present before and after a membership change.**
  Compute `AssignShards` for node set A, then again for A with one node removed (or one added).
  For every shard whose owner is in *both* the before and after node sets, assert the owner is
  identical. This is the precise, provable rendezvous-hashing property — test it exactly, not with
  a "movement is roughly bounded" fuzzy assertion.
- Removing a node reassigns **only** the shards it owned; every shard it didn't own is unaffected.
- Adding a node only ever **takes** shards from existing owners; no shard moves between two
  pre-existing nodes as a side effect of the addition.
- Distribution sanity check over a representative shard count (e.g. 256 shards, 4–32 nodes): no
  node ends up with zero shards, and the max/min shard count per node stays within a reasonable
  bound of `ShardCount / len(nodeIDs)` — this is a sanity check on the hash quality, not a proof.
- `ShardForKey` is stable: the same key and `ShardCount` always produce the same shard, across
  repeated calls and across process restarts (no hidden randomness — this rules out
  `hash/maphash`, which is randomized per-process by default and would break this on its own even
  before considering multi-instance determinism, relevant once Phase 5 runs multiple control-plane
  replicas that all need to agree).

Once `router` is tested standalone, wire it in:

- Remove `Config.ShardMap()`'s round-robin body from `config.go` (or the whole method, your call —
  document which). `metadata.Service` should call `router.AssignShards(cfg.NodeIDs(),
  cfg.ShardCount)` directly.
- `GetShardMap` must remain **deterministic across repeated calls** with no membership change in
  between — assert this in a `metadata` package test, since it's what proves the computation is
  pure rather than accidentally order- or time-dependent.

**Caution, not a task:** `NodeService.Capacity` is real data now, but nothing in this phase should
read it for placement decisions. Shard ownership here is purely `(node list, shard count) →
map[shard]node`. The reason this matters: Phase 1's summary documents that a node's *actual*
resident bytes can exceed its configured RAM budget by up to `budget + 256 × max_value_bytes`,
because the budget is enforced per-engine-shard rather than globally. A future phase that starts
making placement or admission decisions from `Capacity` needs to treat it as a soft, self-reported
signal with that bound in mind — not build logic today that assumes it's exact. Nothing to
implement here, just don't paint yourself into a corner by quietly assuming otherwise.

## Step 2.2 — Generic client SDK

`control/pkg/client` is the "any app can use this" surface the design doc calls for — publicly
importable, LLM-agnostic, just `Get`/`Put`/`PrefixMatch` against the cluster.

```go
type Client struct { /* ... */ }

func New(controlPlaneAddr string, opts ...Option) (*Client, error)
func (c *Client) Get(ctx context.Context, key []byte) (value []byte, found bool, err error)
func (c *Client) Put(ctx context.Context, key, value []byte) error
func (c *Client) PrefixMatch(ctx context.Context, prefix []byte) (map[string][]byte, error)
func (c *Client) Close() error
```

Behavior:

- On construction (or lazily, on first use — your call), calls `ClusterMetadataService.GetNodeList`
  and `GetShardMap` against the given control-plane address to learn the cluster's current shape.
  Refresh on a simple interval (e.g. every few seconds) is sufficient for this phase — there's no
  membership dynamism to react to quickly yet (Phase 3), so don't build push invalidation now.
- Resolves every key via `router.ShardForKey` → shard → `shardMap[shard]` → node address, then
  dials (and reuses) a `NodeService` connection to that node.
- Handles the unary-vs-chunked value transport split transparently — **this logic already exists**
  in `pulsekv-node-bench`'s `put`/`get` helpers (the 4 MiB `UnaryLimit` check, the `PutChunked`
  streaming loop, the `GetChunked` reassembly). Extract it rather than re-deriving it; refactor
  `pulsekv-node-bench` to call the same extracted code so there's exactly one implementation of
  "how PulseKV sends a value over the wire," not two that can drift.
- `PrefixMatch` calls the target node's `NodeService.PrefixMatch` and fetches each matched key's
  value — note from `pulsekv_engine.h`/the Phase 1 summary that a prefix scan on the engine side
  is not a snapshot and a key can vanish between the scan and the fetch; the SDK should treat a
  subsequent miss on a listed key as normal, not an error, and say so in the doc comment.

**Prove the reusability claim, don't just assert it.** Add `control/cmd/pulsekv-example/main.go`:
a small, deliberately non-LLM program (e.g. a toy note/session store — anything unrelated to
KV-cache) that imports `pulsekv/control/pkg/client` and does a handful of `Put`/`Get` calls against
a running cluster. This is the concrete evidence for the design doc's claim that Layer 1 is usable
by "any app," the same way Phase 1 proved its claims with tests instead of comments.

## Step 2.3 — `ClusterMetadataService` becomes a real router

Mechanically small once 2.1 exists: `metadata.Service.GetShardMap` now returns
`router.AssignShards(...)` instead of the round-robin placeholder. `GetNodeList` and its liveness
probe (`aliveness()`) are unchanged — still a direct, bounded `HealthCheck` probe per node, still
explicitly not membership, exactly as `service.go`'s existing comment describes. Don't touch that
logic; Phase 3 replaces it.

## Step 2.4 — End-to-end routing test

Two things, both driving the SDK against the real 4-node local dev cluster:

**`control/cmd/pulsekv-cluster-bench/main.go`** — the cluster-routed sibling of
`pulsekv-node-bench`, built on `pkg/client` instead of dialing one node directly. Same evidence
discipline: every read verified byte-for-byte, warmup excluded, percentiles reported. Additionally
verify **routing correctness, not just data correctness**: for a sample of keys, independently
compute the expected owning node via `router.ShardForKey` + the shard map fetched from
`ClusterMetadataService`, then confirm (e.g. via that node's `Capacity.resident_keys` moving, or a
debug hook — pick a mechanism and document it) that the key's `Put` actually landed on the node the
router says it should have. A `Get` returning the right value from *some* node in the cluster is
not proof the routing logic is correct; it could be silently hitting the same node for everything.

**`deploy/smoke-test.sh`** gets a routing-shaped check: put a handful of keys chosen (or generated)
to land on different shards/nodes, get them back through the SDK, and assert the shard map's shape
is internally consistent — every shard owned, every owner a real configured node ID, matching the
existing Phase 0/1 "assert shape" pattern extended to also assert the HRW computation the smoke
tool can reproduce independently (it already links the same Go module, so it can call
`router.AssignShards` itself and compare against what `GetShardMap` returns over the wire — that's
a stronger check than eyeballing shape).

## Exit criteria — verify all of these before considering Phase 2 done

1. `router.AssignShards` and `router.ShardForKey` are pure, tested functions with the exact
   movement-invariant tests from Step 2.1 passing — not approximated, not skipped.
2. `Config.ShardMap()`'s round-robin body is gone; `ClusterMetadataService.GetShardMap` serves the
   real HRW computation, verified deterministic across repeated calls with no membership change.
3. `control/pkg/client` exists, is documented as the public SDK, and `pulsekv-node-bench` shares
   its wire-transport logic rather than duplicating it.
4. `pulsekv-example` demonstrates the SDK for a non-LLM purpose, actually running against the dev
   cluster.
5. `pulsekv-cluster-bench` produces a correctness-verified, percentile-reported benchmark that
   additionally verifies keys land on the node the router predicts, not just that values round-trip
   correctly.
6. `deploy/smoke-test.sh` passes with the new routing checks, including the independent
   `router.AssignShards` cross-check against the live `GetShardMap` response.
7. `node/` is untouched — `git diff --stat -- node` is empty.
8. `docs/pulsekv-v2-phase2-summary.md` is written in the same evidence-first style as the Phase 0
   and Phase 1 summaries: exact file layout, any deviations from this prompt with reasoning,
   exit-criteria evidence, and where Phase 3 (gossip membership) should start.

Do not start any Phase 3 work — gossip, dynamic membership, or anything that makes the node list
stop being "static, read once at startup" — until this phase's exit criteria are verified and the
summary is written.
