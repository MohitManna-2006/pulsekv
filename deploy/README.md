# `deploy/` — the v2 local dev cluster

The standard dev and test environment for every v2 phase from Phase 0 onward:
a control-plane process plus N data-plane nodes, all on one machine, booted
from one config file.

```
deploy/
├── Dockerfile              polyglot build image (C/C++/Go/Python + gRPC codegen)
├── cluster.config.yaml     four-node fast-loop launch/bootstrap inventory
├── cluster.chaos.config.yaml eight-node gossip/chaos/failover fixture
├── common.sh               shared paths and helpers, sourced by the scripts
├── gen-proto.sh            regenerate the Go and Python stubs
├── run-local-cluster.sh    build + boot + wait for health and membership
├── local-node.sh           leave, crash, start, and restart one node pair
├── chaos-test.sh           deterministic failure/rejoin test under sustained load
├── smoke-test.sh           assert the contract against the live cluster
├── stop-local-cluster.sh   terminate everything, sweep orphans
├── test-engine.sh          the C engine's suite: release / TSan / Valgrind
└── bench-node.sh           node benchmark, fits-in-RAM vs exceeds-RAM
```

`run-local-cluster.sh` also builds the Go membership sidecar and Phase 2/3
tools under `deploy/build/bin/`: `pulsekv-member`, `pulsekv-example`,
`pulsekv-cluster-bench`, `pulsekv-chaos`, and the single-node
`pulsekv-node-bench`.

Build output goes to `deploy/build/`, runtime state to `deploy/run/`. Both are
gitignored. They live under `deploy/` rather than the repo-root `build/`
because v1's `make clean` does `rm -rf build/`, and v2 artefacts disappearing
when someone cleans v1 would be a confusing afternoon.

## Quick start

Everything runs in the v2 dev image. v1 targets Linux and so does v2; the C++
gRPC shim in particular does not build on a macOS host without a fight.

```sh
# once, from the repo root
docker build -f deploy/Dockerfile -t pulsekv-v2-dev .

# interactive session
docker run --rm -it -v "$PWD:/src" -w /src pulsekv-v2-dev bash

# then, inside the container
deploy/run-local-cluster.sh
deploy/smoke-test.sh
deploy/build/bin/pulsekv-example
deploy/build/bin/pulsekv-cluster-bench --ops 10000 --warmup-ops 1000
deploy/chaos-test.sh --target node-2 --cycles 3 --seed 7
deploy/stop-local-cluster.sh
```

One-shot, CI shape:

```sh
docker run --rm -v "$PWD:/src" -w /src pulsekv-v2-dev bash -c '
  trap "deploy/stop-local-cluster.sh || true" EXIT
  deploy/run-local-cluster.sh
  deploy/smoke-test.sh --no-install
  deploy/chaos-test.sh --target node-2 --cycles 3 --seed 7'
```

`pulsekv-v2-versions` inside the image prints every resolved toolchain version.

## Process lifetime — the choice, and why

`run-local-cluster.sh` starts processes **in the background** and records their
PIDs in `deploy/run/cluster.pids`; per-process stdout/stderr goes to
`deploy/run/logs/<label>.log`. It starts one C++ data process and one Go gossip
sidecar per node. The script returns only after direct health checks pass and
the control plane publishes the exact configured live set with a coherent HRW
map. The cluster keeps running until `stop-local-cluster.sh`.

The alternative — holding everything in the foreground — would make it
impossible to run `smoke-test.sh` against the cluster from the same shell,
which is the normal workflow.

Consequences worth knowing:

- Inside `docker run --rm ... bash -c '...'`, the container dies when the
  command finishes and takes the cluster with it. Use the one-shot form above,
  or an interactive shell.
- `run-local-cluster.sh --restart` stops an existing cluster first.
- `stop-local-cluster.sh` gracefully removes sidecars before stopping data
  processes, then stops the control-plane observer. Its orphan sweep also
  covers the membership and chaos processes. A stop that leaves something
  holding a service or gossip port makes the next boot fail for reasons that
  look nothing like the actual cause.

## Resizing the cluster

Edit `nodes:` in `cluster.config.yaml`, assigning each node a unique service
port and gossip port. No script takes the node count as an argument or
hardcodes it — they all read the config through the control plane's own parser,
so the scripts and server cannot disagree about what the file says.

Default is 4 nodes (fast loop). `cluster.chaos.config.yaml` provides the
eight-node membership/replication fixture; the broader design target for gossip
and Phase 5 Raft chaos testing is 8–32 nodes on one machine.

## The control-plane group

`control_plane:` is a LIST, and that list is the Raft metadata group. The dev
configs ship three replicas -- the smallest group that survives losing one.
Every replica serves `ClusterMetadataService`; only the current Raft leader
turns what it sees in gossip into committed membership.

Reading from a follower is safe, not a compromise: a replica answers from its
own applied log, which is always a prefix of the leader's committed one, so it
can be a heartbeat behind but never describes a different cluster. Clients, data
nodes, and every tool are therefore given the whole list and fall back across
it, and no single replica is a single point of failure.

```sh
# who leads right now
deploy/build/bin/pulsekv-smoke --config deploy/cluster.config.yaml --mode=leader

# wait for the group to agree, optionally on someone new
deploy/build/bin/pulsekv-smoke --config deploy/cluster.config.yaml \
    --mode=leader-wait --min-replicas=3

# kill the leader and watch it fail over, under load
deploy/chaos-test.sh --config deploy/cluster.chaos.config.yaml \
    --scenario both --target node-3 --cycles 2
```

Raft log and snapshot state lives under `raft.data_dir` (`deploy/run/raft/<id>`)
and, unlike the engine's spill tier, is MEANT to survive a restart -- it is what
lets a returning replica catch up instead of starting over. `deploy/run/` is
gitignored; removing it resets the group, which is the right move when changing
the replica set in the config.

## Replication factor

`replication_factor` in the config sets how many replicas each shard gets beyond
its primary (design doc range: 0, 1, or 2; default 1). `0` is a legal, exercised
setting and is distinct from omitting the key.

`run-local-cluster.sh --replication-factor N` overrides it for one boot, which is
how the same fixture is run at 0, 1, and 2 without three config files. The
override goes to the control plane, which decides placement; data nodes are
handed `--metadata-addr` and read their replica peers from that decision. A node
started without `--metadata-addr` does not replicate at all.

## What each script guarantees

| Script | Guarantee |
|---|---|
| `run-local-cluster.sh` | Starts every control-plane replica and waits for the Raft group to elect a leader BEFORE booting data nodes -- until there is a leader nothing can commit membership, so a node that joined gossip would sit unpublished. Then every service answers `HealthCheck`, every sidecar is running, and metadata publishes the exact live node set and HRW map before the ready banner. On timeout it dumps logs, stops the partial cluster, and exits non-zero. |
| `local-node.sh` | Operates on one configured data/sidecar pair. Graceful leave is published before data shutdown; crash paths exercise either SWIM detection or the local-service watchdog; start waits for data health before advertising it. It requires only a QUORUM of control-plane replicas, not all of them: it legitimately runs while one is down, and demanding every replica would make the lifecycle scripts stricter than the group they manage. |
| `chaos-test.sh` | Runs one sustained correctness watcher while a target repeatedly leaves/fails and rejoins. It verifies every topology generation, exact minimal HRW movement, stable-key operations, and physical placement, then writes a JSON report. At replication factor >= 1 it also strong-ack seeds keys on shards the target primaries and proves the promoted replica — and, after the rejoin, the restarted target's freshly backfilled engine — serves them byte-for-byte. At factor 0 it records why that proof was skipped. With `--scenario both` it also kills the Raft leader at the same moment as the data node and restarts it at the same moment, then asserts the survivors converge on one new leader at a higher term, that no two replicas ever disagree about a committed state, and that the restarted former leader comes back fenced -- a follower that adopts the state committed while it was away rather than reasserting its own. |
| `smoke-test.sh` | Three legs — Go contract and routing assertions, the Python adapters client, and an optional grpcurl reflection check. The Go leg independently reproduces the HRW shard map and proves SDK writes hit the predicted owner and miss a node holding no copy of that shard. It also reproduces the primary+replica owner map, then proves a strong-ack write is byte-identical on every replica via direct `NodeService.Get`. Non-zero on any failure. |
| `stop-local-cluster.sh` | Stops watcher → sidecars → data nodes → control plane, with bounded SIGTERM grace, SIGKILL fallback, PID-identity guards, and an orphan sweep. |
| `test-engine.sh` | Builds and runs the pure-C engine suite. No cluster, no gRPC, no network. |
| `bench-node.sh` | Boots a dedicated node with a small RAM budget and benchmarks it twice — inside and well outside that budget. Fails the run on any unverified read. |

`smoke-test.sh`'s Go leg is the one that enforces the real behaviour: a Put
followed by a Get returns the value, a miss is `found=false` rather than an
error, a multi-megabyte value round-trips through `PutChunked`/`GetChunked`
byte-for-byte, and eight deliberately malformed chunked writes are each rejected
with a specific status **and leave no key behind**.

### Running `test-engine.sh --tsan` in Docker

ThreadSanitizer disables ASLR through `personality(ADDR_NO_RANDOMIZE)`, which
Docker's default seccomp profile blocks. Start the container with:

```sh
docker run --rm -it --security-opt seccomp=unconfined -v "$PWD:/src" -w /src pulsekv-v2-dev bash
```

Without it TSan aborts at startup; the script detects that specific failure and
tells you the flag rather than reporting it as a test failure.

### Where the benchmark's spill directory goes

`bench-node.sh` defaults its `--data-dir` to `/tmp` **inside the container**,
not `deploy/run/`. On the normal macOS + colima setup the repo is a virtiofs
bind mount, and putting the NVMe tier there measures the host-to-VM filesystem
bridge instead of the tier. The script warns if you point it back into the repo.

## Ports

| Process | Port |
|---|---|
| control-plane gRPC | 7000 |
| `node-N` gRPC | 7100 + N |
| control-plane gossip (TCP + UDP) | 7200 |
| `node-N` sidecar gossip (TCP + UDP) | 7201 + N |

All on `127.0.0.1`. The config loader rejects duplicate node IDs and any reused
service/gossip address, so a copy-paste slip fails at startup instead of
producing processes that quietly fight over one port.

## Watching membership change

With the cluster running, each command blocks until metadata has converged:

```sh
deploy/local-node.sh status node-2
deploy/local-node.sh leave node-2       # graceful gossip leave, then data stop
deploy/local-node.sh start node-2       # data health, sidecar join, full topology
deploy/local-node.sh crash node-2       # SIGKILL both; peers detect the failure
deploy/local-node.sh start node-2
deploy/local-node.sh node-crash node-2  # sidecar watchdog detects C++ failure
deploy/local-node.sh start node-2
```

On `node-crash`, the existing sidecar withdraws the unhealthy service after
three failed probes, stays alive as its supervisor, and rebuilds/rejoins its
gossip participant when the C++ service returns. `start` repairs the missing
data half of that pair; it does not layer a second sidecar over the first.

`GetNodeList` and `GetShardMap` carry a local `topology_generation` for
diagnostics and a SHA-256 `topology_fingerprint` over the complete topology.
Clients install a pair only when the fingerprints agree and match its content;
this remains safe even if a restarted control plane reuses a local generation
number. Gossip is eventually consistent; Phase 5 adds Raft authority across
multiple control-plane replicas.

## Poking at a running cluster

Both servers enable gRPC server reflection, and `grpcurl` is in the image:

```sh
grpcurl -plaintext 127.0.0.1:7000 list
grpcurl -plaintext 127.0.0.1:7000 pulsekv.metadata.v1.ClusterMetadataService/GetNodeList
grpcurl -plaintext 127.0.0.1:7100 pulsekv.node.v1.NodeService/HealthCheck
grpcurl -plaintext 127.0.0.1:7100 pulsekv.node.v1.NodeService/Capacity
```
