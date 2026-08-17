# PulseKV v2 — Phase 2 Continuation Prompt (for Codex)

**How to use this file:** paste everything below the line into Codex as the task prompt to
continue Phase 2, run from inside the `pulsekv` repo root, on top of commit `c7ef6d1` ("V2 phase
1"). Nothing from Phase 2 is committed yet — `control/internal/router/` is the only Phase 2 work
on disk so far.

---

You are continuing **Phase 2** of PulseKV v2, a distributed LLM KV-cache system, picked up
mid-phase from a previous session. Before writing any code, read, in order:

1. `docs/pulsekv-v2-distributed-design.md` — what v2 is and why.
2. `docs/pulsekv-v2-implementation-plan.md`, Section 5 — the original phase definition.
3. `docs/pulsekv-v2-phase1-summary.md`, Section 7 — the seams this phase builds on.
4. `docs/pulsekv-v2-phase2-prompt.md` — the full original Phase 2 prompt. **Step 2.1 (the router
   algorithm) is done and verified; this file exists as background for the design reasoning behind
   Steps 2.2–2.4, which are not.**
5. `control/internal/router/router.go` and `router_test.go` — read these in full. This is
   already-landed, already-verified work; understand it before touching anything downstream of it.
6. `control/internal/config/config.go`, `control/internal/metadata/service.go`,
   `control/cmd/pulsekv-node-bench/main.go` — what you're about to change.

## What's already done — do not re-derive or redesign this

`router.ShardForKey` and `router.AssignShards` are complete, correct, and tested. A previous
session's first attempt used a flawed weight function — plain FNV-1a over `"<shard>:<nodeID>"` —
which measurably broke rendezvous hashing's distribution (at 32 nodes, 12 owned zero shards; a
4→5 node join moved 127 of 256 shards against an ideal of ~51). The fix, now landed:

- `nodeSeed(nodeID)` hashes each node ID once (FNV-1a, same family as v1/the engine).
- `mix64` is a splitmix64 finalizer applied to `nodeSeed(id) ^ mix64(shard)` — this is what fixes
  the avalanche problem; FNV-1a's own final mixing step doesn't push a trailing-byte difference
  (e.g. `node-30` vs `node-31`) into the high bits that the weight comparison is dominated by.
- `AssignShards` hoists `nodeSeed` out of the shard loop, so a recomputation costs `O(nodes)`
  string hashes plus `O(shards × nodes)` cheap integer mixes, not `O(shards × nodes)` string
  hashes.
- Verified: at 256 shards, 32 nodes now range min 2 / max 12 shards with none empty; a 4→5 join
  moves 68 shards, an 8→9 join moves 38 (ideal ~28); every membership-change case shows exactly
  zero shards moving between two nodes present both before and after — the invariant this
  algorithm is supposed to guarantee, confirmed exactly, not statistically.
- `router_test.go` was itself corrected mid-session: `TestAdditionOnlyTakesShards` originally
  asserted *which* shards could move but not *how many*, so it passed cleanly against the broken
  hash — the movement invariant is structural to taking an argmax and holds regardless of hash
  quality. A magnitude assertion was added specifically because the shape-only test could not have
  caught the distribution bug. Do not remove or weaken that magnitude check.

`gofmt`, `go vet`, and `go test ./control/internal/router/...` are all clean. Treat this package as
correct and finished; the remaining work is wiring it in and building on top of it.

## Hard scope boundary

- **Do not modify anything under `node/`.** This phase is pure control-plane (Go) work.
- **Do not** implement gossip membership, Raft, or replication. The node list stays static, read
  once from `deploy/cluster.config.yaml`. That's Phases 3 and 5.
- **Do not** touch `adapters/`.
- **Do not** rename or remove any proto RPC or field. This phase needs no proto changes.
- **Do not** build capacity-aware placement from `NodeService.Capacity`. Shard ownership is purely
  `(node list, shard count) → map[shard]node`, nothing else.
- **Do not** re-touch `router.go`'s algorithm. If you find a real problem with it, stop and flag it
  explicitly rather than silently adjusting already-verified, already-measured code.

## Remaining work

### A. Wire the router in (finishes Steps 2.1 / 2.3)

- In `control/internal/config/config.go`: delete `Config.ShardMap()`'s round-robin body (the
  whole method, if nothing else calls it — check first). Delete whatever config test exercised the
  round-robin behavior specifically; it's testing removed code, not a regression to preserve.
- In `control/internal/metadata/service.go`: `GetShardMap` now calls
  `router.AssignShards(s.cfg.NodeIDs(), s.cfg.ShardCount)` directly. `GetNodeList` and its
  `aliveness()` probe are unchanged — still a direct `HealthCheck` probe, still explicitly not
  membership, per that file's existing comment. Don't touch that logic.
- Add a `metadata` package test asserting `GetShardMap` is **deterministic across repeated calls**
  with no membership change in between — this is what proves the computation is pure rather than
  accidentally order- or time-dependent, and it's cheap insurance for Phase 5, where multiple
  control-plane replicas will need to agree on this independently.

### B. Generic client SDK (Step 2.2)

Build `control/pkg/client`, the publicly importable "any app can use this" SDK:

```go
type Client struct { /* ... */ }

func New(controlPlaneAddr string, opts ...Option) (*Client, error)
func (c *Client) Get(ctx context.Context, key []byte) (value []byte, found bool, err error)
func (c *Client) Put(ctx context.Context, key, value []byte) error
func (c *Client) PrefixMatch(ctx context.Context, prefix []byte) (map[string][]byte, error)
func (c *Client) Close() error
```

- Learns cluster shape via `ClusterMetadataService.GetNodeList`/`GetShardMap` against the given
  control-plane address; a simple polling refresh interval is enough for this phase — no push
  invalidation, there's no membership dynamism to react to yet.
- Resolves every key via `router.ShardForKey` → shard → `shardMap[shard]` → node address (use
  `router.OwnerForKey` directly — it already exists for exactly this), then dials and reuses a
  `NodeService` connection to that node.
- **Extract, don't re-derive, the wire transport logic.** `pulsekv-node-bench`'s `put`/`get`
  helpers already implement the unary-vs-chunked split correctly (the 4 MiB `UnaryLimit` check,
  the `PutChunked` streaming loop, the `GetChunked` reassembly, the "empty stream means a miss"
  convention). Pull that into a shared package — `control/internal/transport` is a reasonable
  name — and have both `pkg/client` and a refactored `pulsekv-node-bench` call it, so there is
  exactly one implementation of "how PulseKV sends a value over the wire."
- `PrefixMatch`: a scan is not a snapshot (see `pulsekv_engine.h` and the Phase 1 summary) — a key
  listed by the scan can vanish before its value is fetched. Treat that as a normal, silently
  skipped case in the SDK, and say so in the doc comment.
- Add `control/cmd/pulsekv-example/main.go` — a small, deliberately non-LLM program (a toy
  note/session store, or similar) that imports `pulsekv/control/pkg/client` and runs real
  `Put`/`Get` calls against a live cluster. This is the concrete proof of the design doc's "any app
  can use this" claim — evidence, not a comment asserting it.

### C. Cluster-level benchmark and routing verification (Step 2.4)

- `control/cmd/pulsekv-cluster-bench/main.go` — the cluster-routed sibling of `pulsekv-node-bench`,
  built on `pkg/client`. Same evidence discipline as every prior benchmark in this project: every
  read verified byte-for-byte, warmup excluded from measurement, percentiles reported.
- **Prove routing correctness, not just data correctness.** For a sample of keys, independently
  compute the expected owning node via `router.OwnerForKey` plus the shard map fetched from
  `ClusterMetadataService`, then confirm the key actually landed there — e.g. call the *predicted*
  node's `NodeService.Get` directly and confirm a hit, and call a *different* node's `Get` directly
  and confirm a miss. A `Get` through the SDK returning the right value proves the data is
  correct somewhere in the cluster; it does not by itself prove the router sent it to the right
  place — direct per-node checks are what closes that gap.
- Extend `deploy/smoke-test.sh` with a routing check: put a handful of keys, confirm the shard map
  returned by `GetShardMap` matches an independently computed `router.AssignShards` call (the
  smoke tool already links this Go module, so this is a direct comparison, not an approximation),
  and confirm per-key placement the same way the cluster-bench tool does.
- Write `docs/pulsekv-v2-phase2-summary.md`, in the same evidence-first style as the Phase 0 and
  Phase 1 summaries: exact file layout, the router bug and fix (include the before/after numbers
  above — that's real evidence worth keeping on record, not just a commit message), any further
  deviations with reasoning, exit-criteria evidence, and where Phase 3 (gossip membership) should
  start.

## Exit criteria — verify all of these before considering Phase 2 done

1. `Config.ShardMap()`'s round-robin body is gone; `ClusterMetadataService.GetShardMap` serves
   `router.AssignShards` output; a determinism test passes.
2. `control/pkg/client` exists and is documented as the public SDK; `pulsekv-node-bench` shares its
   wire-transport code with it rather than duplicating it.
3. `pulsekv-example` runs against the dev cluster and demonstrates non-LLM use of the SDK.
4. `pulsekv-cluster-bench` produces a correctness-verified, percentile-reported benchmark that
   additionally proves per-key routing correctness via direct predicted-node vs. other-node checks.
5. `deploy/smoke-test.sh` passes with the new routing checks, including the independent
   `router.AssignShards` cross-check against the live `GetShardMap` response.
6. `node/` is untouched — `git diff --stat -- node` is empty.
7. `docs/pulsekv-v2-phase2-summary.md` is written, including the router bug/fix history from this
   session as part of the phase's record.

Do not start any Phase 3 work until these are verified and the summary is written.
