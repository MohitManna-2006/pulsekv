# PulseKV v2 — Phase 4 Summary

**Status: complete.** Read this first if you are picking up Phase 5.

Phase 4 gives every shard a primary plus a configurable number of replicas, and
makes the C++ data node forward its own writes to them. The default write path
is unchanged in shape and cost: the primary commits locally, answers the client,
and replicates afterwards. A caller who would rather pay replication latency
than lose a write opts in per call with `require_replica_acks`, and the primary
then blocks for exactly that many acks or fails loudly.

The Go control plane's entire role here is deciding placement. It never sits in
a write path. `node/grpc_shim` became a gRPC **client** of its peers as well as
a server: it polls `ClusterMetadataService` for the shards it primaries, keeps
cached `NodeService` stubs to those shards' replicas, and does the forwarding
itself. `node/engine/` is untouched and has no concept of a primary, a replica,
or a peer.

Phase 4 also closes the gap `pulsekv-v2-phase3-summary.md` §8 named: a node that
newly starts holding a shard — including a restarted node whose spill tier came
up empty — backfills it from a peer that still holds it.

What this phase deliberately does **not** do: reads stay primary-only, there is
no WAL or fsync on the cache write path, and replication factor is still
gossip-derived config rather than Raft-backed authority. Section 8 is explicit
about each.

Companion docs: `pulsekv-v2-distributed-design.md` §4.3 (why replication is off
the consensus path), `pulsekv-v2-implementation-plan.md` §7, and
`pulsekv-v2-phase3-summary.md` (the membership and coherence seams reused here).

---

## 1. Where the decision and the execution live

```text
        deploy/cluster*.yaml            replication_factor
                  │                      (or --replication-factor at boot)
                  ▼
        Go control plane
        one membership snapshot
                  │
                  ├──► router.AssignShards        ──► shard_to_node_id  (unchanged)
                  └──► router.AssignShardOwners   ──► shard_to_owners   (new)
                              │
                              │  asserted equal on the primary column
                              ▼
                   GetShardMap + topology fingerprint
                              │
             ┌────────────────┴─────────────────┐
             ▼                                  ▼
      Go client SDK                      C++ node poller
      routes to the PRIMARY              learns ITS replica peers
      (Get and Put unchanged)                   │
             │                                  │
             │  client write                    │  NodeService.Put
             ▼                                  ▼  from_replication = true
      ┌─────────────┐   local put   ┌──────────────────┐
      │  primary    │──────────────►│  replica, replica │
      └─────────────┘   then fan-out└──────────────────┘
```

Go decides placement; C executes replication. That split is what keeps
`node/engine/` untouched and the SDK's `Put`/`Get` shape unchanged.

---

## 2. Exact implementation layout

```text
proto/
├── metadata.proto        + ShardOwners, shard_to_owners, replication_factor
└── node.proto            + from_replication, require_replica_acks,
                            replicas_acked, PutChunk.from_replication

control/
├── internal/
│   ├── router/router.go        + AssignShardOwners, PrimaryMap, ShardOwners
│   │                             (AssignShards untouched)
│   ├── topology/topology.go    + Snapshot.Owners/.ReplicationFactor,
│   │                             FingerprintInput, owner-map validation
│   ├── metadata/service.go     + placement(): both views from one snapshot,
│   │                             asserted to agree before responding
│   ├── config/config.go        + ReplicationFactor (pointer-parsed so 0 is
│   │                             a setting, not an absence)
│   └── transport/transport.go  + PutWithAck, ErrChunkedAcksUnsupported
├── pkg/client/client.go        + Client.PutWithAck (the one SDK addition)
└── cmd/
    ├── controlplane/           + --replication-factor, --print-replication
    ├── pulsekv-chaos/          + strong-ack promotion set, promotion and
    │                             catch-up proofs, non-holder exclusion
    └── pulsekv-smoke/          + checkReplication leg, non-holder exclusion

node/grpc_shim/
├── main.cpp                    + ShardForKey (FNV-1a, matches Go),
│                                 TopologyView, PeerClients, AsyncQueue,
│                                 ReplicationManager (poller, forwarding,
│                                 catch-up); Put/PutChunked split
└── CMakeLists.txt              comment only — metadata stubs were already
                                compiled into pulsekv_proto since Phase 0

deploy/
├── cluster.config.yaml         + replication_factor: 1
├── cluster.chaos.config.yaml   + replication_factor: 1
├── run-local-cluster.sh        + --replication-factor override
├── common.sh                   + --metadata-addr per node, pk_replication_factor
├── chaos-test.sh               + --promotion-keys, promotion outcome report
└── smoke-test.sh               documents the new replication leg

docs/pulsekv-v2-phase4-summary.md   this file
```

`git diff --stat` for the phase: 36 files, +4,095 / −245. The C++ node accounts
for 1,131 of those lines.

---

## 3. The contract, and why it stayed compatible

### `shard_to_node_id` is still exactly the primary

`GetShardMapResponse` gained `shard_to_owners` and `replication_factor`, and
`shard_to_node_id` was left alone. That is what lets every Phase 2/3 consumer —
the SDK's routing, `pulsekv-smoke`, the chaos watcher, `pulsekv-cluster-bench` —
keep working with no change at all.

The seam is enforced in three independent places, because a silent divergence
would send those consumers to a node that does not hold the data:

- **`router_test.go`** asserts `AssignShardOwners(...)[s].Primary ==
  AssignShards(...)[s]` for every shard across 13 node counts × 4 replication
  factors.
- **`metadata.Service.placement()`** recomputes both from one membership
  snapshot and fails the RPC with `INTERNAL` if any shard disagrees.
- **`topology.validate()`** rejects a *received* snapshot whose two views
  disagree, so a bad publisher cannot be half-believed by a client.

### `require_replica_acks` and the error channel

`PutRequest` gained `from_replication` (set only by a forwarding primary) and
`require_replica_acks`. `PutResponse` gained `replicas_acked`. The pre-existing
unused `error` string stayed unused: a machine-readable count is what a caller
can act on, and gRPC discards the response message on a non-OK status anyway, so
the shortfall detail travels in the status message.

`PutChunk` gained only `from_replication`. There is deliberately no chunked ack
path — see §8.

### The fingerprint moved to v2

Replica placement and the replication factor are now part of what a topology
*is*, so `topology.Fingerprint` hashes them and its domain tag went from
`pulsekv-topology-v1` to `v2`.

This is a real compatibility break with a Phase 3 publisher, and it is the
correct one: a Phase 3 server and a Phase 4 client genuinely are not describing
the same thing, and `Fetch` refuses the pair rather than installing a
half-understood topology. Everything in this repo is built from one tree, so
nothing straddles the change. The generation-only fallback for pre-fingerprint
servers is untouched.

---

## 4. The write path

### Client write, `require_replica_acks = 0` (the default)

`pk_engine_put` locally, respond `OK` immediately, then hand the write to a
bounded background queue that forwards it to each replica with
`from_replication = true`. `replicas_acked` is `0` — nothing was waited for, so
nothing is claimed.

The queue is bounded in **tasks (1024) and bytes (64 MiB)**, and `Submit`
refuses rather than blocks. A burst that outruns the replica links is dropped and
counted, and the count is printed at shutdown. That is the design doc's trade
taken seriously: losing a replica copy costs a recompute; losing the node to an
unbounded queue costs everything on it.

### Client write, `require_replica_acks = N`

Validated **before** the local write, so a refused request stores nothing.
`INVALID_ARGUMENT`, naming the reason, when: the node has no `--metadata-addr`,
has not yet read a coherent topology, is not the primary for the key's shard, or
has fewer than N live replicas. Refusing beats hanging until the deadline and
then looking like a network problem.

Then: local put, fan out to every replica in parallel, and wait on a condition
variable until N acks arrive, every forward finishes, or the ack timeout
(default 2 s) expires. It returns as soon as N acks land rather than waiting for
the slowest replica.

On a shortfall the call returns `DEADLINE_EXCEEDED` — and the message says
plainly that **the local write already happened and is not rolled back**. A
client seeing this error has a durability gap, not a lost write. The write is
idempotent; retrying is safe.

### Replica write

`from_replication == true` → store locally, return `OK`, forward nothing. That
single branch is the whole of the fan-out loop prevention.

### `PutChunked`

Same split, keyed off chunk 0's `from_replication`, minus the strong-ack half.

---

## 5. Catch-up on newly-owned shards

When the poller sees this node holding a shard it did not hold before — as
primary *or* replica, including every shard it owns on a fresh start — it
backfills that shard from a peer that still holds it.

- Sources come from the same coherent snapshot: the shard's other current
  owners, highest-ranked first. No current holder means no backfill; the shard
  repopulates from future writes, exactly as it did in Phase 3.
- It calls the peer's `PrefixMatch` with an empty prefix, recomputes each key's
  shard with the same FNV-1a the router uses, and `pk_engine_put`s the matches.
  Values above the unary limit come back `value_omitted`, so those are refetched
  with `GetChunked` — without that, multi-megabyte values, which are the whole
  point of this cache, would silently never backfill.
- **Deviation, and it is a small one:** the prompt asked for one scan per
  newly-gained shard. Newly-gained shards are instead grouped by the peer that
  will serve them, so gaining 29 shards from one node is one scan rather than
  29. Same work, same result, same `O(total keys)` cost the engine's own
  `pk_engine_scan_prefix` already documents — just not repeated per shard.
- It runs on its own thread with a single-slot job queue holding the newest
  topology only, so a slow scan never delays a topology refresh and a backfill
  never runs against a two-generations-stale map.
- Failure is logged and dropped, never fatal to startup.

Measured in the eight-node chaos fixture: a restarted node refilled its
promotion keys in **108–431 ms** after rejoining.

---

## 6. Verification evidence

### Static, unit, and race

All from the final tree:

```sh
cd control && go build ./... && go vet ./... && gofmt -l .
go test ./... && go test -race -count=1 ./...
cd .. && bash -n deploy/*.sh && git diff --check
```

All clean. The C++ node builds with `-Wall -Wextra` and **zero warnings**.

New unit coverage: 9 router tests (primary agreement, ranking, promotion
successor, movement discipline under removal and addition, replica
distribution, edge cases), 5 topology tests (placement install, fingerprint
distinguishes replica placement and factor, five malformed-owner-map
rejections, pre-Phase-4 publisher), 4 metadata tests, 3 config tests, 3
transport tests, 2 client tests, 4 chaos tests, 1 smoke test.

### Protocol regeneration

`deploy/gen-proto.sh --all` in the pinned `pulsekv-v2-dev` image regenerated Go
and Python bindings and generated all three `.proto` files cleanly for C++.
A second regeneration produced a byte-identical tree — determinism re-verified
the same way Phase 0/1 documented.

### Live Docker proof

One container session, six legs, each booting a fixture and stopping cleanly:

| Leg | Fixture | Factor | Result |
|---|---|---:|---|
| 1 | four-node | 1 (config default) | smoke 95/95 |
| 2 | four-node | 2 (override) | smoke 95/95 |
| 3 | four-node | 0 (override) | smoke 93/93 |
| 4 | eight-node | 1 | pre-smoke 175/175, chaos 6/6, post-smoke 175/175 |
| 5 | eight-node | 2 | chaos 4/4, smoke 175/175 |
| 6 | eight-node | 0 | chaos 4/4 (promotion skipped), smoke 173/173 |

Every Phase 3 assertion stayed green throughout: exact live node set, coherent
content-verified topology, exact HRW ownership for all 256 shards, no
survivor-to-survivor movement, exact moved/stable counts, physical placement
proofs, and byte-correct survivor-stable reads under sustained load.

Chaos reports:

| | rf = 0 | rf = 1 | rf = 2 |
|---|---:|---:|---:|
| Transitions verified | 4 / 4 | 6 / 6 | 4 / 4 |
| Target-owned shards | 29 | 29 | 29 |
| Survivor-stable shards | 227 | 227 | 227 |
| Sustained operations | 87,791 | 296,210 | 219,598 |
| Byte-verified | 87,791 | 296,210 | 219,598 |
| Misses / mismatches / RPC errors | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 |
| Promotion proofs | skipped | 6 (48 keys) | 4 (32 keys) |

Per-transition promotion evidence at factor 1 (`node-3` is the target):

| # | Kind | Moved | Stable | Promotion proof |
|---:|---|---:|---:|---|
| 1 | removal | 29 | 227 | replica-promotion 8/8, shard 1: node-3 → node-2, 7 ms |
| 2 | rejoin | 29 | 227 | catch-up 8/8, shard 1: node-2 → node-3, 431 ms |
| 3 | removal | 29 | 227 | replica-promotion 8/8, shard 1: node-3 → node-2, 8 ms |
| 4 | rejoin | 29 | 227 | catch-up 8/8, shard 1: node-2 → node-3, 217 ms |
| 5 | removal | 29 | 227 | replica-promotion 8/8, shard 1: node-3 → node-2, 10 ms |
| 6 | rejoin | 29 | 227 | catch-up 8/8, shard 1: node-2 → node-3, 218 ms |

These are observed local-fixture values, not a latency SLA.

### What the promotion proof actually proves

Before each kill, the watcher writes one key per target-primaried shard with
`require_replica_acks` set to that shard's live replica count, and then reads
each key **directly from every replica's own address** to confirm the ack was
truthful. Only then does it publish the progress count that lets the shell kill
the node. The assertion is therefore deterministic rather than racing in-flight
async replication.

After the removal it asserts the shard went to the *top-ranked replica the
previous map named* — not merely to some node that answers — and that that node
returns the exact bytes, read directly rather than through the SDK.

After the rejoin it asserts the restarted target serves them. That node's engine
came up empty (the spill tier is purged at start, and there is no WAL), so the
only way it can answer is catch-up. Values are regenerated every cycle, so a
stale copy left somewhere cannot satisfy the next round.

At factor 0 the whole section is skipped, the report records **why** in
`promotion_skipped`, and `chaos-test.sh` prints it — a passing run that tested
nothing cannot be mistaken for a passing run that tested everything.

### Smoke replication leg

At factor ≥ 1: the published owner map must equal `router.AssignShardOwners`
exactly, a strong-ack `Put` must report at least the requested acks, the value
must be byte-identical on the primary **and every replica** read via direct
`NodeService.Get`, and asking for one more ack than exists must fail
`INVALID_ARGUMENT`. At factor 0 it instead asserts no shard has any replica, so
a cluster that was meant to replicate and silently is not cannot pass by
omission.

---

## 7. Three real bugs the live run found

The first full six-leg run failed four legs. All three causes were genuine.

1. **The smoke test compared against the config's replication factor, not the
   live one.** A cluster booted with `--replication-factor 2` was checked
   against the file's `1` and "failed". Fixed by treating the *live* factor as
   authoritative — the same lesson Phase 3 already learned about the config's
   node list — and noting the override in the pass message.

2. **A node's replica view lags ownership by up to one poll interval.** At boot,
   a strong-ack write arrived before the primary had polled, and was correctly
   refused with `INVALID_ARGUMENT`. Two fixes, because both were warranted:
   - the poller now uses a 200 ms interval instead of the configured 2 s
     whenever the view just changed *or* this node holds no shard at all —
     neither is a resting state, and both end in a change worth seeing;
   - the chaos harness and smoke test retry an `INVALID_ARGUMENT` strong-ack
     write for a bounded period. Only that code is retried;
     `DEADLINE_EXCEEDED` means the fan-out really fell short and is reported.

3. **"A non-owner must not have the key" stopped being true.** Phase 2's routing
   proof picked the first node that was not the primary and asserted a miss.
   With replication that node is sometimes a replica, which is *supposed* to
   have the key — so the assertion failed precisely when replication worked.
   Both the smoke test and the chaos watcher now exclude the whole owner set and
   probe a true non-holder, which makes the claim stronger: the key is on its
   holders and on nothing else. Note this was a latent flake in the chaos
   watcher too — it passed by luck on the earlier runs.

---

## 8. Deliberate limits and honest interpretation

- **Reads are primary-only.** The SDK routes `Get` to the primary and has no
  read fallback to a replica. A replica holds the data and will serve a direct
  `NodeService.Get` — that is what the smoke test exercises — but no
  client-visible read path uses it. This was an explicit out-of-scope
  simplification for Phase 4, not an oversight.
- **A strong-ack failure is a durability gap, not a lost write.** The primary
  commits locally before forwarding anything and never rolls that back.
  `DEADLINE_EXCEEDED` means "stored, less replicated than you asked for".
- **A crash between local commit and fan-out loses the in-flight copies.** There
  is no WAL, no fsync, and no durable record on the cache write path, by design
  (design doc §4.3). Closing this with durability machinery is not a Phase 4
  bug fix; it is a different system.
- **Chunked writes have no strong-ack mode.** `PutChunk` carries no
  `require_replica_acks`: a chunked write is a multi-megabyte value where
  blocking the client on fan-out is the wrong trade. `PutWithAck` refuses an
  oversized value with `ErrChunkedAcksUnsupported` rather than silently
  downgrading it to fire-and-forget.
- **`from_replication` is cooperation, not authorisation.** A client that sets
  it suppresses its own write's replication. Authenticated peer identity is
  production hardening, and belongs with the gossip encryption Phase 3 already
  named.
- **Replication has a convergence window.** For up to one node poll interval
  after ownership changes, a primary can be unaware it is the primary. Async
  writes in that window simply are not replicated; strong-ack writes are refused
  with `INVALID_ARGUMENT` and are safe to retry. Fast-poll mode narrows it to
  ~200 ms where it is most likely to be hit, but does not remove it.
- **A node removed from gossip while still running polls every 200 ms**
  indefinitely, because it holds no shard. That is ~5 requests/second per
  orphaned node against the control plane — deliberate, since such a node is
  waiting to rejoin, but worth knowing at scale.
- **Catch-up is correctness-first, not optimised.** A full `PrefixMatch` scan
  per source peer, `O(total keys on that peer)`, the precedent
  `pulsekv-v2-phase1-summary.md` already set for `PrefixMatch` itself. An
  indexed or shard-scoped scan is a later concern.
- **No cross-shard transaction semantics.** Each key replicates independently.
  There is no atomicity across two keys, and none is implied.
- **Replication factor is not yet authoritative.** It is derived from local
  config (or a boot flag) by each control plane, exactly like shard ownership.
  Two observers could publish different factors. Phase 5's Raft metadata plane
  is where this becomes a single consistent record.
- **The topology fingerprint is v2 and not backward compatible** with a Phase 3
  publisher. See §3.
- **Background replication drops under sustained overload.** Bounded queue, by
  design; the count is reported at shutdown but is not yet a metric.
- **Exit criterion 4 wording, honestly:** `git diff --stat -- node/engine` is
  empty. `git diff --stat -- adapters` is **not** — it shows five regenerated
  Python stub files under `adapters/pulsekv_adapters/gen/`. That is the
  mechanical output of `deploy/gen-proto.sh`, which Step 4.1 requires, and it is
  what Phase 3 did too. No hand-written adapter code was touched:
  `git diff --stat -- adapters ':!adapters/pulsekv_adapters/gen'` is empty.

---

## 9. Phase 5 handoff

Phase 5 replaces gossip-derived ownership with a Raft-backed metadata state
machine. The seams it should use:

1. **`metadata.Service.placement()` is the single place ownership is decided.**
   It takes one `membership.Snapshot` and returns both views plus the
   fingerprint input. Point it at a Raft-applied state instead of
   `membership.Source` and nothing downstream changes.
2. **`replication_factor` is already a published field**, so it can become a
   Raft log entry without another contract change. The design doc lists it
   alongside shard ownership as metadata-plane state.
3. **The topology fingerprint already covers replica placement and the factor**,
   so a client can detect a leader change that altered either.
4. **The C++ poller needs no change** to follow a Raft-backed publisher: it
   reads `GetNodeList` + `GetShardMap`, matches fingerprints, and trusts the
   content. Point `--metadata-addr` at the leader (or a follower with a
   documented staleness bound).
5. **The chaos harness's one-mutation/one-verified-epoch handshake extends
   directly** to leader kills. `verifyPromotion` already distinguishes "the node
   the map now names" from "the node that should have been promoted", which is
   the shape a fencing test needs.

The Phase 3 contracts to preserve are unchanged: content-verified topology
identity, authoritative empty-cluster state, exact placement proofs, and the
one-mutation/one-verified-epoch handshake. Phase 4 adds two more: the
`shard_to_node_id == shard_to_owners[s].primary` seam, and the rule that a
strong-ack response never claims more acks than it waited for.
