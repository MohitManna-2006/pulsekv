# `deploy/` — the v2 local dev cluster

The standard dev and test environment for every v2 phase from Phase 0 onward:
a control-plane process plus N data-plane nodes, all on one machine, booted
from one config file.

```
deploy/
├── Dockerfile              polyglot build image (C/C++/Go/Python + gRPC codegen)
├── cluster.config.yaml     the cluster's shape — the only file you edit to resize it
├── common.sh               shared paths and helpers, sourced by the scripts
├── gen-proto.sh            regenerate the Go and Python stubs
├── run-local-cluster.sh    build + boot + wait for health
├── smoke-test.sh           assert the Phase 0 contract against the live cluster
└── stop-local-cluster.sh   terminate everything, sweep orphans
```

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
deploy/stop-local-cluster.sh
```

One-shot, CI shape:

```sh
docker run --rm -v "$PWD:/src" -w /src pulsekv-v2-dev bash -c '
  deploy/run-local-cluster.sh && deploy/smoke-test.sh; rc=$?
  deploy/stop-local-cluster.sh; exit $rc'
```

`pulsekv-v2-versions` inside the image prints every resolved toolchain version.

## Process lifetime — the choice, and why

`run-local-cluster.sh` starts processes **in the background** and records their
PIDs in `deploy/run/cluster.pids`; per-process stdout/stderr goes to
`deploy/run/logs/<label>.log`. The script returns once every process passes its
health check, and the cluster keeps running until `stop-local-cluster.sh`.

The alternative — holding everything in the foreground — would make it
impossible to run `smoke-test.sh` against the cluster from the same shell,
which is the normal workflow.

Consequences worth knowing:

- Inside `docker run --rm ... bash -c '...'`, the container dies when the
  command finishes and takes the cluster with it. Use the one-shot form above,
  or an interactive shell.
- `run-local-cluster.sh --restart` stops an existing cluster first.
- `stop-local-cluster.sh` finishes with an orphan sweep: any `pulsekv-node` or
  `pulsekv-controlplane` still alive that is *not* in the pid file gets found,
  reported, and killed. A stop that leaves something holding port 7100 makes
  the next boot fail for reasons that look nothing like the actual cause.

## Resizing the cluster

Edit `nodes:` in `cluster.config.yaml`. Nothing else. No script takes the node
count as an argument or hardcodes it — they all read the config through
`controlplane --print-nodes`, which is the server's own parser, so the scripts
and the server cannot disagree about what the file says.

Default is 4 nodes (fast loop). The design target for Phase 3 gossip and Phase
5 Raft chaos testing is 8–32 nodes on one machine; `cluster.config.yaml`
documents the port layout for that.

## What each script guarantees

| Script | Guarantee |
|---|---|
| `run-local-cluster.sh` | Every process is listening and answering `HealthCheck` before the "cluster ready" banner prints. On timeout it dumps each process's log, says whether it exited or just never answered, stops the partial cluster, and exits non-zero. |
| `smoke-test.sh` | Three legs — Go contract assertions, the Python adapters client, and an optional grpcurl reflection check. Non-zero on any failure. |
| `stop-local-cluster.sh` | SIGTERM, up to 10s grace, then SIGKILL; plus the orphan sweep. Non-zero only if something could not be stopped. |

`smoke-test.sh`'s Go leg is the one that enforces Phase 0's real requirement:
every RPC other than `HealthCheck` must answer `UNIMPLEMENTED`, not a fake
success. A `Get` returning `found=false` would be indistinguishable from a
working engine that happens to be empty.

## Ports

| Process | Port |
|---|---|
| control plane | 7000 |
| `node-N` | 7100 + N |

All on `127.0.0.1`. 7100–7131 leaves room for the full 32-node test cluster
without touching the control plane's port. The config loader rejects duplicate
node IDs and duplicate `host:port` pairs outright, so a copy-paste slip fails at
startup instead of producing a cluster where two nodes quietly fight over one
port.

## Poking at a running cluster

Both servers enable gRPC server reflection, and `grpcurl` is in the image:

```sh
grpcurl -plaintext 127.0.0.1:7000 list
grpcurl -plaintext 127.0.0.1:7000 pulsekv.metadata.v1.ClusterMetadataService/GetNodeList
grpcurl -plaintext 127.0.0.1:7100 pulsekv.node.v1.NodeService/HealthCheck
grpcurl -plaintext 127.0.0.1:7100 pulsekv.node.v1.NodeService/Capacity   # UNIMPLEMENTED
```
