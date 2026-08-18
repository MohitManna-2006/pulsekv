# PulseKV v2 — Phase 5 Summary

**Status: complete.** Read this first if you are picking up Phase 6.

Phase 5 replaces "each control plane derives ownership from its own gossip view"
with "a Raft group agrees on the input, and every replica derives ownership from
that". `control_plane` in the dev config became a list; three replicas form a
metadata group; killing its leader re-elects in under a second while the data
plane keeps serving.

The single most important design decision is what is **not** in the Raft log.
The log carries exactly two things — the live data-node set and the replication
factor — and never the shard map. `router.AssignShards` and `AssignShardOwners`
are pure functions of that input, so once the group agrees on the input every
replica computes a byte-identical map locally with nothing further to
coordinate. `git diff --stat -- control/internal/router` is empty for this whole
phase, which is the point rather than a coincidence: the router package's own doc
comment has said since Phase 2 that this is what Phase 5 would be for.

Phase 4's replication write path is untouched. `from_replication`,
`require_replica_acks`, `ReplicationManager`, the async queue, and the catch-up
scan are byte-for-byte the same; the C++ node's only change is that
`--metadata-addr` now takes a list.

Companion docs: `pulsekv-v2-distributed-design.md` §4.3 (why consensus belongs
here and nowhere else), `pulsekv-v2-implementation-plan.md` §8, and
`pulsekv-v2-phase4-summary.md` §9, whose five named seams this phase is built on.

---

## 1. Where authority moved

```text
        deploy/cluster*.yaml          control_plane: [cp-0, cp-1, cp-2]
                  │                   replication_factor
                  ▼
      ┌───────────────────────────────────────────────┐
      │  every replica observes SWIM gossip           │   liveness detection
      └───────────────────────────────────────────────┘   (unchanged, Phase 3)
                  │
                  │  ONLY the Raft leader proposes what it sees
                  ▼
      ┌───────────────────────────────────────────────┐
      │  Raft log:  {live data nodes, replication     │   the agreed INPUT
      │              factor}                          │
      └───────────────────────────────────────────────┘
                  │
      ┌───────────┼───────────┐   every replica applies the same log
      ▼           ▼           ▼
    cp-0        cp-1        cp-2
      │           │           │   each runs the UNCHANGED pure router locally
      ▼           ▼           ▼
   identical shard + owner maps, no coordination for this step
      │           │           │
      └───────────┴───────────┘
                  │  GetNodeList / GetShardMap from ANY replica
                  ▼
        clients and data nodes, given the full replica list
```

Phase 3's gossip layer still does failure *detection* and is not gated on
leadership. Raft-backed metadata is now the authoritative record of what that
detection means. That is exactly the loop the implementation plan's step 5.3
described closing.

---

## 2. Exact implementation layout

```text
control/internal/metastore/          NEW — the Raft metadata plane
├── state.go       167  the replicated State (nodes + factor), commands, and the
│                       package comment explaining what is deliberately absent
├── fsm.go         140  raft.FSM: Apply / Snapshot / Restore
├── store.go       362  Raft lifecycle, membership.Source impl, Leader(), Propose
├── bridge.go      194  the leader-only gossip -> Raft proposer
└── *_test.go      935  live 3-replica groups over real TCP and on-disk stores

control/
├── internal/
│   ├── config/config.go        control_plane becomes a list; + Raft settings,
│   │                           ControlPlaneList (accepts the legacy mapping),
│   │                           indexed port defaults, even-group warning
│   ├── membership/view.go      + Snapshot.ReplicationFactor *int
│   ├── metadata/service.go     + WithLeaderInfo; placement() reads the agreed
│   │                           factor from the same coherent snapshot
│   └── topology/topology.go    + Snapshot.RaftLeaderID / RaftTerm (unhashed)
├── pkg/client/client.go        multi-endpoint metadata with sticky fallback
└── cmd/
    ├── controlplane/           + --node-id, Raft store, bridge, leader logging
    ├── pulsekv-smoke/          + --mode=leader, --mode=leader-wait,
    │                           quorum-aware --mode=wait, multi-endpoint
    ├── pulsekv-chaos/          + leader-failover scenario, replica-agreement
    │                           watcher, per-replica observation
    ├── pulsekv-cluster-bench/  + multi-endpoint metadata
    └── pulsekv-member/         + syncControlPlanes (see §7, bug 1)

node/grpc_shim/main.cpp         --metadata-addr takes a comma-separated list
                                and falls back across replicas. NOTHING ELSE.

proto/metadata.proto            + raft_leader_id, raft_term (additive,
                                diagnostic, deliberately not fingerprinted)

deploy/                         control_plane list + raft section in both
                                fixtures; three replicas booted and a leader
                                awaited before data nodes; chaos --scenario
```

`git diff --stat` for the phase: 32 files, +3,027 / −302, plus the new
`internal/metastore` package (1,798 lines including tests).

---

## 3. What is replicated, and why so little

`State` is two fields. That is the whole state machine.

Replicating the derived map instead would have meant putting 256 entries through
consensus to re-derive something already agreed, and — worse — introducing a
second way for two replicas to disagree: one where the input matched but the
derivation had drifted. Keeping the derivation local and pure means a
disagreement about ownership is impossible unless the replicas disagree about
membership, which Raft already prevents.

Three consequences worth naming:

- **The generation is now globally meaningful.** It is the Raft log index of the
  entry that last *changed* the state. Phase 3 had to document generation as
  "diagnostic, not a globally unique revision" because a restarted publisher
  could reuse a number for different content; a committed log index cannot. Two
  replicas reporting the same generation are reporting the same committed state,
  and the chaos harness now asserts exactly that.
- **The generation still only moves on content change.** `Apply` compares before
  assigning, so a re-proposed identical state does not look to clients like a
  membership change. The published Phase 3 contract is preserved.
- **The replication factor is finally agreed.** Phase 4's summary §8 listed
  "replication factor is not yet authoritative — two observers could publish
  different factors" as a limitation. The leader proposes it and every replica
  applies the same one, which closes that gap.

---

## 4. Reads on every replica

A follower answers `GetNodeList`/`GetShardMap` from its own applied log rather
than refusing or proxying. That is safe for a specific reason: Raft guarantees an
applied log is a *prefix* of the leader's committed log, so a follower's answer
can be older but never a contradictory reality.

**Observed staleness bound.** Committing a membership change and having every
replica serving it took under one bridge interval plus one heartbeat in the live
runs — the chaos watcher swept all three replicas 336–358 times per run and
never once found two of them claiming the same generation with different
content. In steady state a follower is at most one heartbeat (500 ms configured)
behind, and in practice the gap is invisible because the leader only proposes
when membership actually moves.

Consumers that need a *specific* generation (the smoke test's `topology-wait`,
the chaos watcher's transition detection) already poll until they see it, which
is unchanged from Phase 3 and works identically against a follower.

---

## 5. Multi-endpoint clients

Both the Go SDK and the C++ node take a comma-separated list and fall back
across it, preferring whichever replica last answered.

The important structural rule in both: **one topology fetch stays on one
replica.** Its two RPCs must observe the same publisher, and splitting them
across a leader and a slightly-behind follower would produce a fingerprint
mismatch that looks exactly like membership churn and burns the coherence retry
budget for nothing. The fingerprint algorithm itself is untouched and does not
know a Raft group exists.

`client.New("a:1,b:2,c:3")` — every existing single-address call site keeps
working unchanged, because a list of one is a list.

---

## 6. Verification evidence

### Static, unit, and race

```sh
cd control && go build ./... && go vet ./... && gofmt -l .
go test ./... && go test -race -count=1 ./...
cd .. && bash -n deploy/*.sh && git diff --check
```

All clean. The C++ node builds with `-Wall -Wextra` and zero warnings.

`internal/metastore`'s tests run **live three-replica Raft groups over real TCP
transports and on-disk BoltDB stores** — not mocks — including:

- election and convergence on one committed state, with every replica reporting
  the identical generation;
- only the leader may propose (every follower gets `ErrNotLeader`);
- leader kill → re-election at a higher term → the old leader restarted and
  **positively verified fenced**: asked to propose 20 times in a row and refused
  every time, then required to adopt state committed while it was away;
- a restarted follower recovering its committed state from disk;
- the bridge deferring an empty-membership proposal until its view settles, and
  committing it once the emptiness persists.

### Live Docker proof

One container session, four legs, each booting a fixture and stopping cleanly:

| Leg | Fixture | Result |
|---|---|---|
| 1 | 4 nodes, 3 replicas | smoke 95/95; all three replicas answer identically |
| 2 | 8 nodes, data-node chaos only | pre-smoke 175/175, chaos 6/6, post-smoke 175/175 |
| 3 | 8 nodes, **concurrent** leader + data-node kill | chaos 4/4, smoke 175/175 |
| 4 | 8 nodes, rf=2, concurrent kill | chaos 4/4, smoke 175/175 |

Every Phase 3 and Phase 4 assertion stayed green throughout: exact live node set,
coherent content-verified topology, exact HRW ownership for all 256 shards, no
survivor-to-survivor movement, replica-promotion and catch-up proofs, and
byte-correct reads under sustained load.

**Leader failover, measured:**

| | Leg 3 (rf=1) | Leg 4 (rf=2) |
|---|---|---|
| Failover | cp-1 (term 2) → cp-0 (term 3) | cp-2 (term 2) → cp-0 (term 3) |
| Time to survivor agreement | **980 ms** | **817 ms** |
| Measurement resolution | 32 sweeps | 26 sweeps |
| Old leader rejoined as follower in | 1,699 ms | 2,014 ms |
| Committed generation after rejoin | 9 | 9 |
| Data nodes after rejoin | 8 | 8 |
| Replica-agreement sweeps | 336 (925 reads) | 358 (990 reads) |
| **Disagreements** | **0** | **0** |
| Sustained operations / byte-verified | 105,139 / 105,139 | 105,139 / 105,139 |
| Misses / mismatches / RPC errors | 0 / 0 / 0 | 0 / 0 / 0 |

The failover clock starts at the first sweep that *observes* the disruption, not
when the harness published readiness — see §7, bug 3, for why that distinction
changed the reported number by 6×.

These are observed local-fixture values with a 500 ms election timeout, not a
production SLA.

### What the concurrency leg proves

Legs 3 and 4 kill the Raft leader and a data node **at the same time**, with
neither transition verified yet, and restart both at the same time. The data-node
promotion and catch-up proofs pass unchanged while an election is in flight, and
the election completes while shard ownership is moving. That is exit criterion 7:
control-plane leadership and data-node ownership are independent failure domains,
demonstrated rather than assumed.

### The fencing check, stated positively

Externally the harness proves fencing without needing a new RPC:

1. the old leader is killed;
2. **while it is down, committed membership changes** (the concurrent data-node
   kill) to a state it never applied — without this the check would be vacuous,
   since agreeing with a state you already had proves nothing;
3. it is restarted;
4. it reports the *new* leader, not itself, at a term ≥ the new one;
5. every replica, including it, converges on one committed generation **and one
   fingerprint** — identity compared by content, because two replicas at the same
   generation with different content is precisely the split brain being hunted.

The in-process half is stronger still: `TestLeaderFailoverFencesTheOldLeader`
asks the rejoined replica to commit a conflicting proposal 20 times and requires
every one to fail.

---

## 7. Three real bugs the live runs found

1. **Partial membership after boot, ~1 in 3 times.** The leader would publish
   three of four data nodes and stay there for 15 s. Cause: sidecars join the
   first control-plane seed that answers, and until Phase 5 there was only one —
   so it learned every node directly via push/pull. With three replicas only the
   seeded one learns directly; the others wait on gossip propagation, which is a
   best-effort UDP broadcast with a bounded retransmit count backstopped by the
   15 s push/pull interval. Whichever replica won the election might be one that
   had not heard. Fixed by having each sidecar push/pull with **every**
   control-plane replica, and each replica with the others — a handful of extra
   TCP syncs at startup that encode the actual requirement: any replica may
   become leader, and a leader proposes from its own view. 12/12 clean boots
   after the fix, from 8/12 before.

2. **`--mode=wait` demanded every control-plane replica.** A targeted data-node
   restart therefore failed during exactly the failover the chaos harness
   creates. Fixed with `--min-control-plane`: boot asks for all of them (a
   cluster silently coming up a replica short is worth catching), lifecycle
   operations ask for a quorum. A lifecycle check should not be stricter than
   the group it manages.

3. **Failover time was reported 6× too high.** The clock started when the
   harness published readiness, which folded in the shell's data-node kill
   before it even reached the leader. Fixed by anchoring the measurement to the
   first sweep that observes the disruption, and by giving leadership sweeps a
   tighter per-replica timeout than the data-plane one — a killed replica is
   exactly what those sweeps look at. 6,624 ms → 980 ms for the same event.

---

## 8. Deliberate limits and honest interpretation

- **Fixed group membership.** The Raft group is whatever `control_plane` lists at
  bootstrap. There is no `AddVoter`/`RemoveVoter` path, so growing or shrinking
  the group means stopping it and clearing `raft.data_dir`. Fine for a dev
  fixture; a real deployment needs online reconfiguration.
- **Bootstrap trusts the config.** Every replica bootstraps from the same file
  and hashicorp/raft refuses all but the first. Two replicas started with
  *different* peer lists would form two groups. The config is launch inventory,
  not an authenticated cluster identity — the same boundary Phase 3 named.
- **Reads are not linearizable.** A follower serves its applied state, which can
  lag. That is a documented staleness bound (§4), not a consistency guarantee. A
  caller needing the leader's latest committed state has no way to ask for it;
  there is no read-index or leader-forwarding path.
- **The leader's gossip view is still a single observer's view.** Raft makes the
  *decision* consistent; it does not make the *observation* correct. If the
  leader cannot see a data node that is genuinely alive, the group agrees on a
  membership that omits it. Bug 1 above was exactly this failure mode, and the
  fix reduced its likelihood rather than eliminating the class.
- **The empty-membership settle window is a heuristic.** A newly elected leader
  refuses to propose an empty node set for ~10 propose intervals. It prevents a
  real self-inflicted outage (a leader elected before its gossip view populates
  would commit "no live nodes" and every client would install it), but a cluster
  that genuinely empties during that window is published a little late.
- **Still no authentication or encryption**, for gossip or gRPC or the Raft
  transport. Phase 3 deferred it; Phase 5 adds a third unauthenticated channel.
  This must be closed before any multi-host deployment.
- **Reads remain primary-only in the data plane**, unchanged from Phase 4, and
  there are still no cross-shard transaction semantics.
- **`raft_leader_id`/`raft_term` are diagnostic and deliberately unfingerprinted.**
  Two replicas at the same committed state must produce the same fingerprint
  even when one has not yet noticed an election. Do not build routing on them.
- **Timings are a local-fixture profile.** 500 ms election timeout on loopback
  with three processes on one machine. Production needs environment-specific
  tuning and real observability.
- **`git diff --stat -- adapters` is not empty**, exactly as in Phases 3 and 4:
  two regenerated Python stub files under `adapters/pulsekv_adapters/gen/`, the
  mechanical output of the additive proto change. No hand-written adapter code
  was touched — `git diff --stat -- adapters ':!adapters/pulsekv_adapters/gen'`
  is empty.

---

## 9. Phase 6 handoff

Phase 6 is the large-blob transport optimisation: the zero-copy/shared-memory
path the design doc (§4.5) keeps off gRPC. Nothing in Phase 5 blocks it, and two
things help.

1. **The data path is untouched and clearly bounded.** `node/grpc_shim`'s only
   Phase 5 change is metadata dialing. The chunked `PutChunked`/`GetChunked`
   transport, `transport.ChunkSize`, and the 4 MiB unary limit are exactly where
   Phase 1 and Phase 4 left them, and `docs/pulsekv-v2-phase1-summary.md`
   documents the framing rules the new path must preserve.
2. **The control plane is now a stable, quorum-backed place to publish transport
   capability.** If a node needs to advertise "I support shared memory at this
   path" or a negotiated transport version, `NodeInfo` and the metadata group are
   the right home, and adding a field there is the same additive change the last
   three phases have made.

Start with `node/engine/`'s value-copy boundary: `pk_engine_put` copies the
caller's buffer in and `pk_engine_get` hands back heap-allocated bytes, so the
current chunked path buffers a whole multi-megabyte value twice. That is the
cost Phase 6 exists to remove, and it is the one place where the engine header's
contract will genuinely have to change — the first time since Phase 1.

The contracts to preserve are unchanged and now number five: content-verified
topology identity, authoritative empty-cluster state, exact placement proofs, the
one-mutation/one-verified-step chaos handshake, and Phase 4's
`shard_to_node_id == shard_to_owners[s].primary` seam. Phase 5 adds a sixth: two
replicas reporting the same committed generation must report the same
fingerprint.
