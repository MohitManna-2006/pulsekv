# PulseKV v2 — Phase 3 Summary

**Status: complete.** Read this first if you are picking up Phase 4.

Phase 3 replaces the static runtime node list with a live SWIM membership
view. Each C++ data node has a small Go membership sidecar; the Go control
plane observes the same ring, derives deterministic rendezvous-hash ownership
from its current live data set, and publishes coherent topology snapshots to
clients. The SDK installs those snapshots atomically and the new chaos harness
proves removal, recovery, bounded shard movement, and physical routing under
sustained load.

The C++ data plane is intentionally unchanged. This phase does not add
replication, key migration, persistent data, or Raft authority. Those
boundaries matter when interpreting the chaos result: survivor-owned keys stay
correct throughout churn, while data on a failed/restarted target is not
promised to survive until Phase 4.

Companion docs: `pulsekv-v2-distributed-design.md` (system intent),
`pulsekv-v2-implementation-plan.md` (phase order), and
`pulsekv-v2-phase2-summary.md` (the router and SDK seams reused here).

---

## 1. Runtime architecture and authority

```text
deploy/cluster*.yaml
  launch inventory + bootstrap addresses + fixed shard count
                  │
                  ├──────────────┐
                  ▼              ▼
       Go control-plane      Go sidecar per C++ node
       gossip observer       health/identity gate
                  │              │
                  └──── SWIM ring┘
                         │
                         ▼
              sorted live data-node view
                         │
                         ▼
             deterministic HRW shard map
                         │
                         ▼
       GetNodeList + GetShardMap + topology identity
                         │
                         ▼
              atomic SDK routing snapshot
```

The YAML node list is now **launch/bootstrap inventory**, not runtime
membership. It tells the local scripts which processes to create and gives
participants initial gossip addresses. Once running, only valid live
`role=data` members appear in `ClusterMetadataService`.

The control plane joins as an observer/bootstrap participant and does not
advertise a `NodeService`. Every data node joins as a stable member named
`data:<node_id>` and advertises its node ID plus service address in bounded,
versioned application metadata.

This sidecar design keeps `node/` unchanged and isolates membership policy in
Go, where Phase 5 can later replace gossip-derived ownership without changing
the C engine or the client-facing RPC names.

---

## 2. Exact implementation layout

```text
control/
├── internal/
│   ├── membership/
│   │   ├── metadata.go           versioned member metadata and validation
│   │   ├── view.go               sorted immutable live-data snapshots
│   │   ├── manager.go            memberlist lifecycle and delegates
│   │   └── *_test.go             convergence, labels, ambiguity, lifecycle
│   ├── topology/
│   │   ├── topology.go           coherent fetch, validation, fingerprint
│   │   └── topology_test.go
│   └── metadata/
│       ├── service.go            gossip snapshot -> HRW ownership
│       └── service_test.go
├── pkg/client/
│   ├── client.go                 atomic dynamic topology installation
│   └── client_test.go            churn, empty cluster, stale-connection tests
└── cmd/
    ├── controlplane/main.go      observer startup/join/shutdown lifecycle
    ├── pulsekv-member/           membership sidecar and C++ node supervisor
    ├── pulsekv-chaos/            sustained load and transition verifier
    ├── pulsekv-smoke/            topology-wait and coherence assertions
    └── pulsekv-cluster-bench/    shared coherent topology reader

proto/metadata.proto              generation, SHA-256 fingerprint, shard count

deploy/
├── cluster.config.yaml           four-node fast-loop fixture
├── cluster.chaos.config.yaml     eight-node Phase 3 fixture
├── run-local-cluster.sh          starts control + data/sidecar pairs
├── local-node.sh                 leave/crash/node-crash/start/restart/status
├── chaos-test.sh                 deterministic mutation/watcher handshake
├── stop-local-cluster.sh         ordered bounded cleanup
├── common.sh                     PID identity and process transaction helpers
└── README.md                     operator commands and guarantees
```

Generated Go and Python metadata bindings were regenerated from the protocol.
`control/go.mod` now directly depends on `github.com/hashicorp/memberlist`
v0.6.0.

---

## 3. Membership semantics

### Admission and published state

A sidecar fails closed. Before it opens a gossip participant, it calls the
configured `NodeService.HealthCheck` and verifies that the returned node ID is
the one it represents. It then attempts bounded joins through the control
plane and configured data peers, one seed at a time. This permits recovery
when the control-plane observer is unavailable but another data peer remains.

Member metadata is validated before admission. The published view separately
retains raw records and the last complete valid snapshot. If two members
ambiguously advertise the same node ID or service address, that candidate view
is not published; a later update or departure can resolve the ambiguity
without dropping the last good routing state.

Published nodes are sorted by node ID/address. A process-local generation
increments only when the effective valid data-node set changes, so harmless
memberlist callbacks do not create false routing epochs.

### Suspicion, failure, leave, and recovery

SWIM suspicion is deliberately not a terminal routing event. A member remains
published until memberlist reports a terminal leave/death, avoiding premature
ownership movement on a transient delayed probe.

There are three exercised removal paths:

- `leave`: the sidecar broadcasts a graceful leave before its data process is
  stopped.
- `crash`: both data process and sidecar receive `SIGKILL`; peers must detect
  the sidecar through SWIM probing and suspicion.
- `node-crash`: only the C++ process is killed. The still-running sidecar sees
  three consecutive bounded health failures, gracefully withdraws the data
  member, waits for service recovery, creates a fresh gossip participant, and
  rejoins automatically.

Signal handling is installed before startup work. Join and health attempts are
context-aware and bounded, and normal process shutdown broadcasts a graceful
leave before closing gossip listeners.

### Lifecycle engineering

Deployment PID records contain identity information, not only a numeric PID,
so a stale record or a label such as `node-1` cannot accidentally target
`node-10`. Pair start/restart is transactional: a failure to record or launch
the second process rolls back only the processes started by that invocation.
Ordered cluster shutdown removes the correctness watcher, sidecars, data
nodes, then the control observer, with bounded `SIGTERM` grace and `SIGKILL`
fallback.

---

## 4. Coherent metadata and SDK behavior

`GetNodeList` and `GetShardMap` remain separate RPCs, so a membership change
can occur between calls. A process-local generation counter alone cannot join
them safely: a restarted publisher can reuse the same number for different
content. Phase 3 therefore adds two compatible fields:

- `topology_fingerprint`: SHA-256 over the fixed shard count, sorted
  `(node_id, address)` pairs, and complete shard ownership map.
- `shard_count`: explicit cluster shape on `GetShardMapResponse`.

Consumers retry the two RPCs until both fingerprints match, then recompute the
fingerprint over the received content before installing it. The shared
`internal/topology` package centralizes this rule for the SDK, smoke test,
benchmark, and chaos watcher. Generation remains useful diagnostics, but it is
not treated as a globally unique revision. A generation-only compatibility
path remains for pre-Phase-3 metadata servers.

For every valid live set, the service recomputes the complete HRW shard map
using the Phase 2 pure router. That preserves the minimal-movement property:
on removal, only shards owned by the removed node may move; on rejoin, only
shards newly won by that node may move.

Zero live nodes is an authoritative topology, not a metadata error. It carries
the fixed shard count with empty node and owner maps. The SDK installs it,
retires stale connections, and makes `Get`/`Put` fail locally with
`client.ErrNoLiveNodes`. A transport or malformed-metadata failure still
retains the last complete good topology.

Topology installation and connection retirement are synchronized. New routes
become visible as one snapshot; connections to nodes that no longer own any
shard are removed and closed outside the client lock. `PrefixMatch` fans out
only to current shard owners, then re-fetches each result through normal
routing.

---

## 5. Chaos harness contract

`deploy/chaos-test.sh` owns process mutation. One `pulsekv-chaos` process owns
all correctness and topology assertions for the entire run. They coordinate
through an atomically replaced progress file:

1. the watcher seeds one deterministic key for every survivor-stable shard;
2. it publishes `0` only when sustained reads are active;
3. the shell performs exactly one removal or rejoin action;
4. the watcher validates the new coherent epoch and publishes the next count;
5. the shell does not mutate the cluster again until that count is visible.

For every transition, the watcher asserts:

- the exact expected live node set;
- a coherent, content-verified topology;
- exact HRW ownership for all 256 shards;
- no survivor-to-survivor shard movement;
- the exact number of moved and stable shards;
- one SDK-routed write is present on its predicted physical owner;
- that same key is absent from a live non-owner;
- all survivor-stable keys remain byte-correct under concurrent reads.

After valid CLI configuration, the report is written atomically in JSON on
both runtime success and runtime failure. CLI validation errors exit before a
report exists. The shell trap restores a partially removed target before
returning, and the normal cluster stop verifies no managed Phase 3 process is
left behind.

---

## 6. Verification evidence

### Static, unit, integration, and race checks

All of these passed from the final tree:

```sh
cd control
go test ./...
go vet ./...
go test -race -count=1 ./...

cd ..
bash -n deploy/*.sh
git diff --check
```

The member-sidecar suite includes real local memberlist participants and a
mutable fake `NodeService`. It proves bootstrap through a configured fallback
gossip address while the control gossip endpoint is unavailable, withdrawal
after a local service failure, and automatic rejoin after recovery. Membership
tests cover convergence, terminal removal, duplicate metadata ambiguity,
cluster-label isolation, shutdown, and bounded joins.

Protocol generation passed for every maintained consumer:

```sh
deploy/gen-proto.sh --all
```

That regenerated and checked Go and Python bindings and successfully generated
the C++ metadata stubs in a throwaway directory.

### Live Docker proof

The pinned `pulsekv-v2-dev` image was rebuilt from the final source. A complete
one-shot proof then performed:

1. four-node boot;
2. full smoke suite;
3. ordered clean stop;
4. eight-node chaos-fixture boot;
5. pre-chaos full smoke suite;
6. three alternating target cycles (`crash`, `node-crash`, `leave`);
7. post-chaos full smoke suite;
8. ordered clean stop and orphan check.

Both four-node and eight-node smoke runs passed every Go, Python, and available
reflection assertion. The eight-node chaos report recorded:

| Evidence | Result |
|---|---:|
| Topology transitions | 6 / 6 verified |
| Generation movement | 8 -> 14 |
| Target-owned shards | 20 |
| Survivor-stable shards | 236 |
| Sustained operations | 97,189 |
| Byte-verified operations | 97,189 |
| Misses | 0 |
| Mismatches | 0 |
| RPC errors | 0 |

Exact convergence observations for the six transitions were:

| Path | Epoch | Observed time | Moved | Stable |
|---|---:|---:|---:|---:|
| sidecar + node hard crash | removal 8 -> 9 | 4,845 ms | 20 | 236 |
| restart after hard crash | rejoin 9 -> 10 | 2,255 ms | 20 | 236 |
| data-only hard crash | removal 10 -> 11 | 1,064 ms | 20 | 236 |
| sidecar-supervised recovery | rejoin 11 -> 12 | 995 ms | 20 | 236 |
| graceful leave | removal 12 -> 13 | 593 ms | 20 | 236 |
| restart after leave | rejoin 13 -> 14 | 948 ms | 20 | 236 |

These are observed local-fixture values, not a production latency SLA. The
important exit-criterion evidence is that every transition completed within
the configured 15-second bound, moved exactly the permitted shards, preserved
all 236 survivor shards, and maintained correct physical routing.

---

## 7. Boundary review and fixes made before completion

The final review deliberately tested failure boundaries, not only the happy
path. It found and closed these issues before Phase 3 was declared complete:

- **Publisher restart collision:** local generation numbers could repeat.
  Fixed with the canonical content-derived topology fingerprint.
- **Total membership loss:** an empty view previously looked like a metadata
  failure and would have retained stale owners. Fixed with explicit
  `shard_count`, authoritative empty snapshots, and `ErrNoLiveNodes`.
- **Control-plane-only bootstrap:** a recovering sidecar could exit when the
  observer was unavailable. Fixed with data-peer seed fallback.
- **Transient data-node failure:** a sidecar could withdraw permanently.
  Fixed with an in-process health recovery supervisor and participant rebuild.
- **Unbounded startup/shutdown interaction:** sequential unreachable seeds
  could hold lifecycle work too long. Fixed with one-seed attempts, a one-second
  default TCP bound, context-aware retry loops, and early signal handling.
- **Partial process-pair failures:** launcher rollback could leave an
  untracked child or kill a pre-existing half-pair. Fixed and verified with a
  six-case mock matrix plus PID-record failure injection.

An independent deployment audit also exercised Linux `/proc` identity checks,
zero-padded CLI values, persistent-sidecar data-only failure paths, atomic PID
updates, and cleanup behavior. No blocker remained.

---

## 8. Deliberate limits and honest interpretation

- **No replication or migration:** HRW changes the owner map; it does not copy
  existing values. A failed node's target-owned keys can be unavailable, and
  a restarted C++ node starts with an empty spill tier. Phase 4 owns this gap.
- **No global metadata consensus:** separate gossip observers converge
  eventually and can temporarily publish different valid maps. Phase 3's
  “no split brain” proof means one owner per shard inside every coherent
  published/client-installed map, with direct physical-placement evidence. It
  does not claim instantaneous agreement between independent control planes.
  Phase 5 adds Raft authority and fencing.
- **Local security boundary:** the cluster label prevents accidental ring
  merging but is not authentication. The shipped configs bind service and
  gossip endpoints to loopback and log a warning for non-loopback use.
  Gossip encryption/authentication and secure gRPC credentials must be
  implemented and wired into runtime configuration before multi-host
  deployment.
- **No external sidecar supervisor:** `pulsekv-member` supervises withdrawal
  and rejoin when its C++ service fails, but the deployment scripts only
  background the sidecar itself. An independent sidecar crash is detected by
  SWIM and requires `local-node.sh start/restart` (or a real process manager)
  to restore the participant.
- **Local tuning:** the measured convergence times use memberlist's local
  profile plus Phase 3's bounded join/health settings. Production failure
  detection needs environment-specific tuning and observability.
- **Launch allow-list is not a trust boundary:** YAML determines locally
  managed processes and seeds; valid same-label gossip metadata is the live
  source. Authentication and Phase 5 admission policy must protect a real
  deployment.

---

## 9. Phase 4 handoff

Phase 4 can start without changing the membership or client coherence seams:

1. extend each topology entry from one owner to a primary plus configured
   replicas;
2. keep gossip as liveness detection while deriving primary/replica placement
   from one coherent live snapshot;
3. add asynchronous primary-to-replica writes and an optional stronger ack
   mode;
4. extend the chaos watcher so target-owned keys, not only survivor-stable
   keys, remain readable after primary loss and promotion;
5. distinguish a replica promotion from a new empty HRW owner and verify the
   promised staleness bound.

The critical Phase 3 contracts to preserve are the content-verified topology
identity, authoritative empty-cluster state, exact placement proofs, and the
one-mutation/one-verified-epoch chaos handshake.
