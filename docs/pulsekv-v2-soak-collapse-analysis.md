# PulseKV v2 — The Soak Collapse: Root Cause, Fix, and Reconciliation

**Status:** resolved. This closes the gate in
`pulsekv-semantic-context-implementation-plan.md` §3 and risk-register row 16.

**Verdict in one line:** the collapse was a **defect in the test harness**
(`deploy/`), not in PulseKV's data plane, control plane, or SDK. Every v2
component behaved exactly as designed throughout — including at the moment of
collapse, which is precisely why it was so hard to see.

---

## 1. What the evidence actually is

The implementation plan §3 concluded that "no artifact matching that specific
incident's duration or failure signature exists in the current repository
snapshot." That was wrong, and worth correcting explicitly because it is what
kept the gate closed:

| Artifact | What it is |
|---|---|
| `deploy/run/soak-report.json` | A **180-second** run, 01:18:11–01:21:11 UTC on 2026-08-19. This is what the plan looked at |
| `deploy/run/logs/controlplane-cp-*.log` | The **same cluster's** logs, spanning **01:17:54 → 02:41:28 — 83 minutes** |
| `deploy/run/logs/soak-chaos.log` | 4,258 lines covering **every** soak run from 22:51 onward, appended across runs |
| `deploy/run/logs/data-node-*.log` | Per-node logs, also appended across runs |

The report is short because it belongs to one short run. The *cluster* it ran
against lived for 83 minutes, and the logs of that lifetime were on disk the
whole time.

The plan's specific error is worth naming precisely, because it is an easy one
to repeat. It did not ignore the chaos log — it cited it, and then described
both files together as "from a **180-second** run." Only the report was.
`soak-chaos.log` is 4,258 lines spanning nearly four hours and seven soak
invocations, and the three interleaved cycle counters that explain the whole
incident are visible in it on a first read. Inheriting the report's duration as
the log's duration is what made the incident look like it had left no trace.

A working copy of the pristine logs (taken before any work in this session
touched `deploy/run/`) was used for every claim below.

## 2. Timeline of the 2026-08-19 cluster

All times UTC, from `controlplane-cp-*.log` and `soak-chaos.log`.

| Time | Event |
|---|---|
| 01:17:54 | Cluster boots: 3 control-plane replicas, 4 data nodes |
| 01:17:57 | Membership committed: **generation 4, 4 data nodes** |
| 01:18:40 | A chaos injector at **cycle 152** crashes node-0 — this run is 46 seconds old |
| 01:18:50 | A chaos injector at **cycle 1** crashes node-0 |
| 01:18:57 | A chaos injector at **cycle 77** crashes node-0 |
| 01:18:57 | `error: no control-plane replica is running` — the first of **370** for this cluster (650 across the whole log, which spans seven soak invocations) |
| 01:19:00 | Membership committed: generation 9, 4 data nodes — *the control plane is demonstrably serving* |
| 01:19:26 → 01:21:11 | Generations 10–18 alternate 3↔4 nodes as three injectors crash and restart nodes on independent schedules |
| **01:21:12** | **Generation 19, 0 data nodes. The last membership ever committed.** |
| 01:32:59 | First of **13** failed cp-0 restarts: `bind: address already in use` on gossip port 7240 |
| 02:40:41 | Last failed cp-0 restart, same error |
| 02:41:28 | Last log line — **80 minutes 16 seconds after generation 19, with zero live data nodes and no membership change in between** |

The pristine logs are preserved at
`deploy/run/logs-incident-2026-08-19/` (that directory is gitignored along with
the rest of `deploy/run/`, so copy it somewhere durable before running `make
clean`).

Two facts from this table carry the whole analysis:

**Three chaos injectors were running at once.** Cycle counters 1–5, 77–88 and
152–165 interleave at overlapping timestamps against one cluster. A single
injector counts monotonically from 1, so three counters means three injectors —
survivors of three different soak invocations, all still crashing data nodes.

**The control plane was healthy at the exact moment the harness declared it
dead.** At 01:18:57 `local-node.sh` refused to act because "no control-plane
replica is running." Three seconds later, at 01:19:00, all three replicas
committed generation 9. The harness's liveness answer was not merely stale; it
was false.

## 3. Root cause: a lost update in the pid registry

`deploy/common.sh` keeps one file, `deploy/run/cluster.pids`, mapping a label
(`data:node-0`, `controlplane:cp-1`) to a pid. Every lifecycle decision the
harness makes — is this alive, what do I kill, may I start — was answered from
that file alone.

`pk_pid_set` updated it like this:

1. copy the file minus its own label into a temp file
2. append its own record
3. `mv` the temp file over the original

Step 3 is atomic. Steps 1–3 together are not. Two processes that read the same
starting state both write a file missing the other's record, and the later
`mv` wins. The file's own comment acknowledged the hazard and dismissed it:

> The deploy lifecycle is intentionally single-writer: chaos-test invokes
> local-node serially.

True of `chaos-test.sh`. Not true of `soak-test.sh`, whose background injector
is a second writer by construction — it cycles a data node and, every fourth
cycle, a control-plane replica, with nothing serialising the two. And not
remotely true with three injectors.

**Deterministic reproduction**, against the unmodified pre-fix code — 24
concurrent writers, distinct labels, no cluster required:

```
trial 1: 3/24 entries survived 24 concurrent pk_pid_set calls
trial 2: 2/24 entries survived 24 concurrent pk_pid_set calls
trial 3: 5/24 entries survived 24 concurrent pk_pid_set calls
```

Not a rare interleaving. Under contention the registry loses roughly 80–90% of
its contents, every time.

## 4. From a lost record to a permanent outage

The lost update is the root cause. It became an unrecoverable outage because
three separate pieces of the harness trusted the registry as the definition of
truth rather than as a cache of what it had started.

**4.1 — A live process reads as dead.** `pk_any_controlplane_alive` consulted
only the registry. With the control-plane records lost it answered "no", and
`local-node.sh` line 91 refuses outright on that answer:

```bash
pk_any_controlplane_alive || pk_die "no control-plane replica is running"
```

370 lifecycle operations were refused this way during this cluster's lifetime
(650 across the whole chaos log, which covers seven soak invocations). **Once a
data node was down, it could not be started again** — not because anything was
broken in the cluster, but because the script would not try.

**4.2 — Stopping reports success without stopping anything.**
`pk_signal_managed` returns "already gone" when the registry has no record, and
`pk_stop_managed` turned that into success. With a lost record that is a lie:
the process keeps running and keeps its ports.

**4.3 — Starting then launches a process that cannot work.** With the survivor
still holding gossip port 7240, each replacement cp-0 died at startup with
`bind: address already in use` — 13 times — and `pk_start_managed` had already
recorded the *doomed* process's pid, so the registry now pointed at a corpse.
Do that to all three replicas and the harness is certain the entire control
plane is dead while it serves normally.

**4.4 — Why the errors were instant, and permanent.** With no data node
startable, the cluster reached a genuinely empty membership. That state is
authoritative, not an error: `control/internal/topology/topology.go`'s
`validate` accepts an empty node set with an empty shard map as a valid
snapshot, so the SDK installs it. `client.clientForKey` then does:

```go
if len(c.topology.Nodes) == 0 || len(c.topology.ShardMap) == 0 {
    c.mu.RUnlock()
    return nil, ErrNoLiveNodes
}
```

No network call, no timeout — an immediate local error, on every operation.
`--continue-on-error=true` kept the load generator running through it, so the
benchmark process carried on producing intervals full of failures. That is
exactly the reported signature: *operations collapse, no recovery, benchmark
continues.*

Every layer here did the right thing. The cluster truthfully reported that it
had no data nodes; the SDK correctly refused to route to a cluster with none.
The lie was upstream, in a shell script's idea of which processes existed.

## 5. Why it looked like "53.5 minutes in"

Because the delay is a beat frequency, not a threshold.

Nothing accumulates and trips at a fixed time. Collapse requires two lifecycle
operations to overlap closely enough to lose a record that matters. With
injectors on independent ~30–60s schedules, that alignment happens when their
phases drift into each other — which can take a minute or an hour depending on
how the run started, how many injectors survived, and how long each node
restart happened to take. One run collapses at 53.5 minutes, another at 3.5.
The mechanism is identical; only the alignment differs.

This also explains why the incident resisted reproduction: rerunning the same
command with a clean process table does not reproduce it, because the essential
ingredient is a *second actor* that the command line does not mention.

## 6. Is this a v2 availability bug? No.

Stated plainly, because the gate turned on this question:

- `node/engine/`, `node/grpc_shim/`: never involved. Nodes were down because
  nothing restarted them.
- `control/`: correct throughout. The Raft group kept committing, the readiness
  gate behaved, and generation 19 with zero nodes was an accurate report of an
  empty cluster.
- `control/pkg/client`: correct. Returning `ErrNoLiveNodes` for a cluster with
  no live nodes is the documented, intended behaviour.
- `deploy/`: **defective.** Root cause and every amplifier live here.

Phase 9's distributed core is not implicated. The gate can close on the
evidence above rather than on a waiver.

## 7. Fixes

| # | Fix | File |
|---|---|---|
| 1 | Registry mutations serialise under a portable `mkdir` mutex, re-entrant, with a token-based stale-lock break that cannot destroy a newly acquired lock | `deploy/common.sh` |
| 2 | `pk_pids_for_label` / `pk_process_alive_for_label` answer liveness from the process table; `pk_any_controlplane_alive` falls back to them before reporting a replica dead | `deploy/common.sh` |
| 3 | `pk_stop_managed` sweeps for unrecorded processes matching the label instead of calling a missing record success | `deploy/common.sh` |
| 4 | `pk_start_managed` adopts a running-but-unrecorded process rather than launching a rival that must die on a port bind | `deploy/common.sh` |
| 5 | The injector is a named script that watches its parent and exits when the parent is gone; `pk_kill_tree` kills the whole tree on cleanup; a startup sweep clears strays | `deploy/soak-chaos-injector.sh`, `deploy/soak-test.sh`, `deploy/common.sh` |
| 6 | One soak per run directory, enforced (`pk_singleton_acquire`) | `deploy/soak-test.sh`, `deploy/common.sh` |
| 7 | The injector waits for the cluster to settle before cycling the Raft leader, so a single injector never has two lifecycle operations in flight | `deploy/soak-chaos-injector.sh` |
| 8 | A soak whose cluster served nothing now **fails**, and the report records how it was faulted | `deploy/soak-verdict.py`, `deploy/soak-test.sh` |

Fix 8 deserves its own note. The harness could not previously distinguish
"survived chaos" from "served nothing for an hour," because
`--continue-on-error=true` keeps the generator alive through either. Any
reporting interval that attempts operations and verifies none is now a
`degraded` verdict and a non-zero exit.

## 8. Verification

`deploy/test-lifecycle.sh` — eight checks, one per link in §3–§4, no cluster
required, about a second to run. Wired into `make test` and `make test-lifecycle`:

```
Against pre-fix common.sh:   6 of 8 checks FAIL
Against fixed common.sh:     8 of 8 checks pass
```

```
==> Lifecycle registry regression tests
    ok   concurrent pk_pid_set keeps every entry (24/24)
    ok   concurrent remove/set leaves exactly the intended entries
    ok   a recorded live process reads as alive
    ok   a live process is still found after its record is lost
    ok   pk_any_controlplane_alive survives a lost record
    ok   stop with a lost record actually stops the process
    ok   start adopts the running process instead of spawning a rival
    ok   start refuses to adopt a process running a different command

    ok   8 lifecycle check(s) passed
```

(The two that pass pre-fix are the two that cannot fail there: "a recorded live
process reads as alive" was never broken, and "refuses to adopt a different
command" passes trivially against code that never adopts anything.)

## 9. Reconciliation with the progress report

`pulsekv-v2-progress-report.md` §4.2's Phase 9.4 row narrated:

> 5,390 ops/s sustained · 13,809 errors survived · 182,312 reads verified ·
> crash/restarts every 15s

against `soak-report.json`, which holds 1,505.99 ops/s, 13,803 errors and
204,497 reads verified. The two do not match, and this investigation can now
say exactly why rather than only that:

1. **The report is overwritten by every run.** `soak-report.json` is a fixed
   path. At least seven soak runs executed between 22:51 and 02:41 on
   2026-08-18/19; each replaced the previous report. The run the progress
   report describes is not on disk, and cannot be — nothing preserved it.
2. **The narrated figures are not derivable from the surviving artifact.**
   5,390 ops/s exceeds *every* interval in it (the maximum is 2,435), so it is
   not a peak, a mean, or a windowed rate of this run. 182,312 is not the sum of
   any prefix of its intervals (they sum exactly to the reported 204,497).
   These are a different run's numbers.
3. **The near-match on errors is a coincidence, not a link.** 13,809 vs 13,803
   is 0.04% apart, which reads like the same run — but it cannot be, given
   point 2. Similar chaos exposure produces similar error counts.
4. **"Every 15s" was uncheckable.** The report recorded no fault-injection
   configuration at all, so no reader could have validated that claim against
   the file it cited. `soak-verdict.py` now writes a `fault_injection` block
   into every report, including how many injectors were observed.

**Resolution:** the Phase 9.4 row has been rewritten to cite the fresh run in
§10, whose artifact is preserved alongside it. The old figures are not
recoverable and are not retained as if they were.

## 10. The fresh long-duration soak

<!-- FRESH_SOAK_RESULTS -->

## 11. Recommendations not taken in this pass

- **`deploy/run/` is still one shared directory.** The singleton guard prevents
  two soaks; it does not prevent a soak and a chaos test. A per-run
  subdirectory would remove the shared state entirely, and is a bigger change
  than this fix warranted.
- **`pulsekv-cluster-bench`'s `proveRouting` pre-flight is racy.** It snapshots
  the topology, then writes through the SDK's live topology; if the two differ
  it fails the whole run with a routing-verification error. It reproduced once
  here, against a cluster still reconciling Raft state carried over from the
  incident, and cleared as soon as that state was wiped. It is a real defect in
  a pre-flight assertion, unrelated to the collapse, and is left tracked rather
  than folded into this fix — the same way Phase 9 handled the pre-existing
  `data-node` chaos flake.
- **`soak-report.json` still has a fixed default path.** The verdict block makes
  a degraded run obvious, but a run's report is still overwritten by the next
  one. Timestamped report names would have made this entire investigation a
  five-minute file listing.
