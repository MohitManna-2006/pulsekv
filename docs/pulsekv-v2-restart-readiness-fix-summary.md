# PulseKV v2 — Control-Plane Restart Readiness Fix

**Status: complete.** Not a numbered phase. A scoped control-plane bugfix run
before Phase 9, so Phase 9's soak and fault-injection work is not contaminated
by an already-known failure mode.

The bug, in one line: **a restarted control-plane replica answered
`GetNodeList`/`GetShardMap` with a `200 OK` empty topology — generation 0, zero
live data nodes, a valid fingerprint over an empty cluster — for up to a second
after its gRPC listener opened, before it had caught up with the metadata
group.** Measured on the dev fixture, it reproduced on **5 real-cluster restarts out of 5**.
After the fix: **0 out of 8** real-cluster restarts and **0 out of 30** in-process
ones.

This is the read-serving half of the rule `metastore.Bridge.proposeAllowed`
already enforces on the propose side: *"I have not looked yet" must never be
served as "there is nothing there."* Both mechanisms now exist; neither replaces
the other.

---

## 1. Root cause, measured rather than asserted

### 1.1 The startup order

`control/cmd/controlplane/main.go` does, in this order:

```
membership.New(...)     gossip observer
metastore.New(...)      raft.NewRaft returns
metadata.New(...)       over the store
net.Listen + Serve      <-- the gRPC listener opens HERE
```

`raft.NewRaft` returning does **not** mean the FSM holds anything. hashicorp/raft
restores a snapshot synchronously inside `NewRaft`, but it does not replay
committed log entries there: a follower applies its log only after a leader
tells it what the commit index is. So the listener opens over a zero-valued
`State{}` — and `State{}.Snapshot()` is byte-identical to a genuinely empty
cluster, which Phase 3 established as an authoritative state clients install and
act on.

### 1.2 In-process measurement, five restart shapes

A harness reproducing `main.go`'s exact startup order over real three-replica
Raft groups with on-disk BoltDB stores, dev-fixture timings (500 ms election
timeout), polling the FSM directly at 200 µs and the gRPC endpoint at 2 ms:

| Case | FSM at `metastore.New` return | Empty window observed |
|---|---|---|
| A: follower, intact `raft.db`, no snapshot | **generation 0, 0 nodes** | yes, sub-millisecond here |
| B: follower, snapshot on disk | generation 8, 9 nodes | **no** |
| C: follower, local state wiped | **generation 0, 0 nodes** | yes, 17 ms |
| D: **leader** killed and restarted | **generation 0, 0 nodes** | yes, 44 ms |
| E: leader restarted, membership changed while down | **generation 0, 0 nodes** | yes, 17 ms |

Two findings worth stating plainly because they contradict the obvious guess:

- **Snapshot restore is not the gap.** Case B is the only one where the FSM is
  already populated when the listener opens, because `raft.NewRaft` calls
  `fsm.Restore` synchronously. Ordinary log replay is the gap, not snapshot
  restore. The fix still covers B — a snapshot-restored state can be arbitrarily
  old, and until the replica has heard from the group it has verified nothing —
  but B was never the reproducer.
- **The window is not a fixed cost and nothing bounds it.** It lasts until the
  leader's replication goroutine reaches this replica. With no quorum it never
  ends, and the replica would have published an authoritative empty cluster
  indefinitely.

### 1.3 Real-cluster measurement, `deploy/cluster.chaos.config.yaml`

Eight data nodes, three control-plane replicas, one replica killed and restarted
per cycle, that replica polled **directly at 2 ms** from before its process
started — bypassing every multi-endpoint fallback. Listener-open is anchored on
an independent raw TCP connect, not on the prober's own gRPC channel, because
gRPC's default reconnect backoff otherwise measures itself rather than the
server.

**Before the fix — 5 cycles, 5 reproductions:**

| Cycle | Replica restarted | Listener open | Empty-answer window | Empty `200 OK` samples |
|---|---|---:|---:|---:|
| 1 | cp-1 (leader) | 341 ms | **501 ms** | 172 |
| 2 | cp-0 (follower) | 359 ms | **987 ms** | 354 |
| 3 | cp-2 (leader) | 349 ms | **482 ms** | 172 |
| 4 | cp-0 (leader) | 381 ms | **154 ms** | 47 |
| 5 | cp-1 (leader) | 340 ms | **299 ms** | 113 |

858 authoritative empty answers across five restarts. Phase 6 estimated this
window at ~1.5 s from logs; directly measured it is 154 ms – 987 ms on this
machine, and the tail is what matters — a client that refreshes inside it
installs an empty topology and starts failing with `ErrNoLiveNodes`.

**After the fix — 8 cycles:**

| Cycle | Replica restarted | Listener open | Refusal window | Empty `200 OK` samples |
|---|---|---:|---:|---:|
| 1 | cp-1 (leader) | 304 ms | 167 ms | **0** |
| 2 | cp-0 (follower) | 332 ms | 1.011 s | **0** |
| 3 | cp-2 (leader) | 341 ms | 1.233 s | **0** |
| 4 | cp-0 (leader) | 359 ms | 593 ms | **0** |
| 5 | cp-1 (leader) | 354 ms | 8 ms | **0** |
| 6 | cp-0 (follower) | 421 ms | 959 ms | **0** |
| 7 | cp-2 (leader) | 345 ms | 1.327 s | **0** |
| 8 | cp-0 (leader) | 342 ms | 435 ms | **0** |

2,036 refusals, zero empty answers. The gap is unchanged — it is a property of
Raft catch-up, not of this fix — but for its entire duration the replica now
says so instead of guessing.

---

## 2. The fix

### 2.1 An optional readiness capability on the membership source

`membership.Readiness` sits beside `membership.Source`:

```go
type Readiness interface {
	ServeReady() error
}
```

A source that does not implement it is treated as always ready, which is what
keeps a Phase 3/4 gossip-backed control plane behaving byte-identically. A
gossip view is never ambiguous about emptiness — it observes what it observes. A
Raft-backed view is, and only at startup.

`metadata.Service` discovers the capability by type assertion and checks it in
`snapshot()`, the single choke point both `GetNodeList` and `GetShardMap` pass
through, so the two RPCs can never disagree about whether this replica is
entitled to answer. The refusal is **`codes.Unavailable`** — the code whose
published meaning is "retry, possibly elsewhere" — carrying a message that names
the startup condition and the indices involved.

`HealthCheck` is deliberately **not** gated. It reports process liveness, and
`deploy/`'s `--mode=wait` waits on it to decide a replica started at all;
gating it would deadlock the boot this fix exists to protect.

### 2.2 What "caught up" means

`Store.ServeReady()` latches on four conditions, all real convergence signals
rather than timers — the settle window in `bridge.go` already established that a
fixed timer is the wrong tool here:

1. **A leader has been seen since this process started.** Without this the rest
   is vacuous: a fresh process has commit index 0 and applied index 0, which
   trivially satisfies "I have applied everything committed."
2. **What the group calls committed covers at least the log this replica already
   had on disk.** A replica must never answer from a state older than its own
   persisted log — case A exactly. The floor is capped at the current last index,
   so an uncommitted tail that a new leader truncates cannot strand the replica
   short of a bar it can never reach.
3. **Everything committed has been applied** (`raft.AppliedIndex >= CommitIndex`).
4. **The state machine has actually consumed it** — see §2.3.

It **latches**. Once caught up, a replica that later loses contact keeps serving.
Its state is then stale, which Phase 5 §4 documents as safe; reverting to
"not ready" on every partition would convert a documented staleness bound into
an outage and make this guard worse than the thing it replaces.

### 2.3 The fourth condition, which measurement forced

The first implementation used conditions 1–3 only. On the real fixture it still
leaked **one empty answer in 2 restarts out of 5**.

`raft.AppliedIndex` advances when a batch of entries is *queued on the FSM's
channel*, not when the FSM has run them — hashicorp/raft's own documentation
says exactly this. The gate opened one goroutine handoff before the state
machine held anything, and a 2 ms poll was fast enough to slip into it. This is
the same bug as the original, three orders of magnitude smaller.

The FSM's own mark cannot simply be compared against the commit index instead: a
Raft FSM never sees every entry. No-op entries (one per election) and
configuration entries are not dispatched to it at all, so its mark legitimately
trails the commit index forever, and a naive `fsmApplied >= commitIndex` would
refuse permanently after any election.

What the FSM must have consumed is every **command** entry. So `fsm` now records
the highest index it has actually consumed, updated inside `Apply` under the same
lock that guards the state — including for an entry whose content is unchanged,
because "I have consumed through here" and "the cluster changed here" are
different facts, and the generation deliberately tracks only the second.
`ServeReady` then asks the local log store whether any `LogCommand` exists in
`(fsmApplied, commitIndex]`. The scan walks back from the commit index and stops
at the FSM's mark, so its length is the number of trailing non-command entries —
normally zero, bounded by the elections since the last membership change, and
free once the replica is caught up.

`Restore` sets the mark too, from a new `applied_index` field on the snapshot
payload, falling back to the generation for a snapshot written before the field
existed. Without it a snapshot-recovered replica would report having consumed
nothing and go hunting for entries a compacted log can no longer produce.

**After adding it: 0 empty answers in 8 of 8 real-cluster restarts.**

---

## 3. Callers (step 3)

Both multi-endpoint callers already treated "this replica returned an error" the
same as "this replica is unreachable", so the new error path needed no behaviour
change — verified by test rather than by reading:

- **Go SDK** — `fetchTopology` falls back across the endpoint list on any error
  from `clustertopology.Fetch`, and `refreshLoop` deliberately retains the last
  complete topology when every endpoint fails. Two new tests pin this:
  a replica that *answers with* `Unavailable` is skipped in favour of a ready
  one, and when every replica refuses the client keeps routing on what it holds.
- **C++ topology poller** (`node/grpc_shim/main.cpp`) — `FetchViewFrom` returns
  `nullptr` on any non-OK status and `FetchView` advances to the next replica;
  `LogFailure` is already rate-limited per address, so a restart window cannot
  spam the node log. Unchanged, and `git diff --stat -- node` is empty.

Two harness call sites did need work, because they read one pinned replica:

- **`pulsekv-chaos`** read the data-node scenario's topology through
  `replicas[0]`. That pin is how the Phase 6 run caught this bug — but it also
  means the harness fails whenever the replica it pins is the one being
  restarted. It now reads through a `topologyReader` that falls back across the
  full list exactly as the SDK does, keeping one fetch on one replica so the two
  RPCs still observe the same publisher. The leader scenario's `observeReplicas`
  still reads each replica separately on purpose: that **is** its assertion, and
  it already treats an erroring replica as simply not participating in the sweep.
- **`pulsekv-smoke`**'s `dialAnyReplica` selected a replica on `HealthCheck`,
  which is ungated. It now selects on `GetNodeList` — the call the connection is
  about to be used for — so it cannot pick a replica that will then decline.

---

## 4. Regression tests (step 4)

Six new tests. The first four were each run three times against the pre-fix
behaviour — the service-level gate disabled, everything else identical — and
failed every time: **12 failures out of 12**. All six pass 5/5 with the fix,
including under `-race`.

| Test | What it pins | Pre-fix |
|---|---|---|
| `TestRestartedReplicaRefusesToPublishAnUncaughtUpTopology` | A replica restarted **without a quorum** — the window held open indefinitely, so the test has no timing assumption at all — refuses every direct read, then serves the real state once the quorum returns | FAIL 3/3 |
| `TestRestartedLeaderNeverPublishesAnEmptyTopologyWhileCatchingUp` | The Phase 6 shape: kill the leader, restart it, tight-poll only that replica from process start | FAIL 3/3 |
| `TestSDKKeepsRoutingWhenTheOnlyReachableReplicaIsCatchingUp` | Exit criterion 3: sustained SDK load across real data nodes while the only reachable replica is catching up — zero client-visible errors, zero `ErrNoLiveNodes` | FAIL 3/3 |
| `TestUncaughtUpSourceIsRefusedRatherThanPublishedEmpty` | Service level: both RPCs refuse with `Unavailable`, and the message identifies the condition | FAIL 3/3 |
| `TestServeReadyLatchesAndDoesNotReopenOnLostContact` | A caught-up replica that loses the group keeps serving its stale-but-consistent state | new API |
| `TestSnapshotRoundTripPreservesTheAppliedMark` | The applied mark survives snapshot/restore and tracks consumption, not the generation | new API |

Plus `TestSDKSeesNothingWhileAReplicaRestarts` (rolling restarts under live
load: ~9,000 operations, 0 failures) and `TestCaughtUpSourceStillPublishesAGenuinelyEmptyCluster` /
`TestSourceWithoutReadinessIsAlwaysServed` / `TestHealthCheckIsNotGatedOnReadiness`,
which pin the things that must **not** change.

**One honest note on test strength.** The rolling-restart SDK test passes against
the pre-fix code too, and that is worth reporting rather than hiding: the SDK
prefers whichever replica last answered, so after failing over away from a
replica it will not return to it until its new favourite fails. That stickiness
is why this bug survived Phase 5 and why it surfaced through the chaos watcher's
pinned replica rather than through the SDK. The deterministic test above forces
the case instead of hoping for it.

---

## 5. Existing suites (exit criterion 5)

| Leg | Runs | Result |
|---|---|---|
| 1: 4-node fixture, smoke + `verify-bulk-replication.sh` | 6 | **6/6 pass** (smoke 95/95; 6 MiB value replicated and re-served after the primary was destroyed) |
| 3: 8-node, concurrent leader + data-node chaos | 5 | **4/5 pass** (1 pre-existing flake, §5.1) |
| 4: 8-node rf=2, concurrent leader + data-node chaos | 5 | **5/5 pass** |
| 2: 8-node, data-node-only chaos + post-chaos smoke | 9 | 3/9 pass — **pre-existing, see §5.1** |

Legs 3 and 4 are the ones that exercise this fix: they kill and restart a Raft
leader under sustained load. Representative numbers, unchanged in character from
Phase 5 and 6:

```
leg 3   raft failover: cp-0(term 56) -> cp-2(term 57) in 1074 ms
        cp-0 rejoined under cp-2(term 57) in 1485 ms
        4 promotion proof(s) over 32 key(s) [catch-up-after-rejoin, replica-promotion]
        replica agreement: 317 sweep(s), 866 replica read(s), 0 disagreement(s)
        smoke 175/175

leg 4   raft failover: cp-0(term 58) -> cp-2(term 59) in 1116 ms
        cp-0 rejoined under cp-2(term 59) in 1566 ms
        4 promotion proof(s) over 32 key(s)
        replica agreement: 335 sweep(s), 923 replica read(s), 0 disagreement(s)
        smoke 175/175
```

Go: `go build ./...`, `go vet ./...`, `gofmt -l .`, `go test ./...` and
`go test -race -count=1 ./...` all clean. `git diff --check -- control` clean.
The C++ node builds with zero warnings in every leg above.

### 5.1 A pre-existing failure this fix does not cause and does not fix

`deploy/chaos-test.sh --scenario data-node` followed by a full smoke run fails
intermittently on one check:

```
FAIL  replication/direct replica reads    node-2 returned found=false
```

It is a **data-plane** check: a strong-ack `PutWithAck` reports every replica
stored the value, and a direct `NodeService.Get` against the replica's own
address then misses. The chaos run itself passes completely every time
(6 promotion proofs, 326 sweeps, 0 disagreements); only the post-chaos smoke
fails, always on `node-2`, the node the scenario crashed and rejoined.

Causality was established rather than argued, by running the identical leg six
times against a **pristine `git worktree` at HEAD** and six times with the fix,
concurrently on the same machine (separate container network namespaces):

| Tree | Matched 6-run comparison | All runs |
|---|---|---|
| Pristine HEAD (5a8e3a9) | **2 pass, 4 fail** | 4 pass, 5 fail of 9 |
| With this fix | **2 pass, 4 fail** | 3 pass, 6 fail of 9 |

Identical rates. The pristine tree also produced a second distinct failure in
one run (`routing/key[1] node-0 holds no copy of shard 219 but returned
found=true`) that the fixed tree did not. The mechanism agrees with the
measurement: `--scenario data-node` **restarts no control-plane replica at all**,
so `ServeReady` latches once at boot and is inert for the whole leg.

This belongs to whoever next works on data-plane replication after a chaos
target rejoins — plausibly Phase 9's soak work, which is exactly the kind of
intermittent failure it is meant to surface. It is recorded here so the next run
that hits it does not spend an afternoon suspecting this fix.

---

## 6. Scope

`git diff --stat -- src include tests node/engine adapters` shows exactly one
entry, `adapters/pulsekv_adapters/__init__.py`, which was already modified in
the working tree as uncommitted Phase 8 work before this session began.
Excluding it, the diffstat is empty, and `git diff --stat -- node` is empty
outright.

```
control/cmd/pulsekv-chaos/main.go    |  64 ++++++++++---
control/cmd/pulsekv-smoke/main.go    |   9 +-
control/internal/membership/view.go  |  24 ++++++
control/internal/metadata/service.go |  32 +++++++-
control/internal/metastore/fsm.go    |  59 ++++++++++++-
control/internal/metastore/store.go  | 155 +++++++++++++++++++++++++++++++++-
6 files changed, 323 insertions(+), 20 deletions(-)
```

Plus four new test files (`readiness_test.go` in `internal/metadata`,
`internal/metastore`, and `pkg/client`, and `readiness_sdk_test.go`).

Held to, as instructed: `bridge.go`'s `proposeAllowed` is **untouched** — it
correctly solves the propose side, and the settle window it implements is
complementary to this guard, not redundant with it. No Phase 6 engine-fd work
and no Phase 9 work was started.

---

## 7. Deliberate limits

- **The catch-up gap itself is unchanged.** This fix makes a replica honest
  about the gap; it does not shorten it. A restarted replica is unusable for
  158 ms – 1.3 s on the dev fixture, and a caller with one endpoint sees errors
  for that whole time. Shortening it means changing how quickly a rejoining
  replica is reached, which is Raft tuning, not a serving concern.
- **A replica with no quorum refuses forever, by design.** That is the correct
  answer — it genuinely does not know the committed state — but it does mean a
  single-replica-up control plane serves nothing rather than something stale.
  The SDK covers this by retaining its last complete topology; a fresh client
  starting in that window cannot come up at all.
- **One narrow residual remains, and it is not the one that was measured.** A
  replica whose local state was wiped entirely, catching up from a leader whose
  log exceeds one `MaxAppendEntries` batch, can have its commit index land on a
  prefix containing no command entry, opening the gate on an empty state. It
  requires the first 64 committed entries to contain no membership command —
  i.e. 60+ elections before the cluster ever agreed on a node set. Closing it
  needs the leader's commit index, which a follower cannot observe.
- **Readiness is not exposed on the wire.** There is no RPC to ask a replica
  whether it is caught up; a caller learns it by being refused. A
  `HealthCheck`-style readiness field would be a small additive proto change and
  is the natural home if Phase 9's observability work wants it.
- **The scan in `unappliedCommandIndex` reads the log store directly.** That is
  a layer the metadata plane otherwise does not touch. It is bounded and only
  runs before the latch closes, but it is a coupling worth knowing about.
- **All timings are a local-fixture profile** — 500 ms election timeout, three
  processes and eight nodes in one container on one machine. Production needs
  its own numbers.

---

## 8. Phase 9 can proceed

The failure mode Phase 6 reported as "pre-existing Phase 5 behaviour, fixing it
belongs to whoever next touches `control/`" is closed, with regression tests
that fail against the pre-fix behaviour and a direct-poll measurement showing
**0 empty answers across 8 real-cluster restart cycles** where the pre-fix code
produced 858 across 5, plus 30 in-process restarts with none.

Phase 9's soak and fault-injection runs will restart control-plane replicas
repeatedly. They will now see `Unavailable` with a message naming the startup
condition, and callers holding the full replica list will see nothing at all.
A `generation did not increase: N -> 0` in a Phase 9 run is no longer an
expected artefact to be explained away — it would be a new bug.

The one thing Phase 9 should carry forward is §5.1: the data-node-only chaos leg
is already flaky at HEAD, and that flake is independent of this work.
