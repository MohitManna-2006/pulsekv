# PulseKV v2 — Phase 0 Summary

**Status: complete.** Read this first if you are picking up Phase 1.

Phase 0's job was to freeze the cross-language contract and stand up empty,
honest skeletons behind it, so Phase 1 (C data plane) and Phase 2 (Go control
plane) can proceed independently without diverging. There is no storage logic
anywhere in this phase, and that is the point.

Companion docs: `pulsekv-v2-distributed-design.md` (what v2 is and why),
`pulsekv-v2-implementation-plan.md` (the full phase-by-phase build order).

v1 is untouched. `src/`, `include/`, `tests/`, the root `Makefile`, the root
`Dockerfile`, `README.md`, and every pre-existing file under `docs/` are exactly
as they were.

---

## 1. Directory tree

```
pulsekv/
├── src/ include/ tests/ Makefile Dockerfile README.md   # v1 — untouched
│
├── proto/                          # the frozen contract
│   ├── node.proto                  #   NodeService
│   ├── metadata.proto              #   ClusterMetadataService
│   ├── adapter.proto               #   AdapterService
│   └── README.md                   #   codegen policy: what is checked in, and why
│
├── node/                           # C data plane
│   ├── engine/README.md            #   empty placeholder; Phase 1.1 fills it
│   ├── grpc_shim/
│   │   ├── main.cpp                #   NodeService impl (~280 lines, no storage logic)
│   │   └── CMakeLists.txt
│   ├── .gitignore
│   └── README.md                   #   why there is C++ in a C directory
│
├── control/                        # Go control plane
│   ├── go.mod / go.sum             #   module pulsekv/control
│   ├── cmd/controlplane/main.go    #   the server; also --print-nodes for the scripts
│   ├── cmd/pulsekv-smoke/main.go   #   contract verifier used by the deploy scripts
│   ├── internal/config/            #   cluster.config.yaml loader + validation (+ tests)
│   ├── internal/metadata/          #   ClusterMetadataService impl
│   └── gen/{node,metadata,adapter}/v1/   # generated Go stubs — CHECKED IN
│
├── adapters/                       # Python adapters
│   ├── pyproject.toml
│   ├── README.md
│   ├── .gitignore
│   └── pulsekv_adapters/
│       ├── __init__.py
│       ├── health_client.py        #   the only working code in Phase 0
│       └── gen/                    #   generated Python stubs — CHECKED IN
│
├── deploy/
│   ├── Dockerfile                  #   NEW polyglot build image (root Dockerfile untouched)
│   ├── cluster.config.yaml         #   the cluster's shape
│   ├── common.sh                   #   shared paths/helpers, sourced by the scripts
│   ├── gen-proto.sh                #   regenerate Go + Python stubs
│   ├── run-local-cluster.sh
│   ├── smoke-test.sh
│   ├── stop-local-cluster.sh
│   ├── .gitignore
│   └── README.md
│
└── docs/pulsekv-v2-phase0-summary.md   # this file
```

Gitignored, regenerated on demand: `node/grpc_shim/gen/`, `deploy/build/`,
`deploy/run/`.

---

## 2. Exact pinned versions

Everything is pinned in `deploy/Dockerfile`. Inside a running container,
`pulsekv-v2-versions` prints the resolved set. These are the values this phase
was built and verified against:

| Tool | Version | Pinned how |
|---|---|---|
| Base image | `debian:bookworm-slim` | image tag (same base as v1's Dockerfile) |
| gcc / g++ | 12.2.0 | Debian bookworm |
| cmake | 3.25.1 | Debian bookworm |
| **protoc** | **3.21.12** | Debian `protobuf-compiler` |
| **libprotobuf (C++)** | **3.21.12** | Debian `libprotobuf-dev` |
| **gRPC C++** | **1.51.1** | Debian `libgrpc++-dev` |
| **grpc_cpp_plugin** | **1.51.1-3+b1** | Debian `protobuf-compiler-grpc` |
| **Go** | **1.25.6** | `ARG GO_VERSION`, official tarball, `GOTOOLCHAIN=local` |
| **protoc-gen-go** | **v1.36.12** | `ARG`, `go install …@version` |
| **protoc-gen-go-grpc** | **v1.6.2** | `ARG`, `go install …@version` |
| grpcurl | v1.9.3 | `ARG`, `go install …@version` |
| **Python** | **3.11.2** | Debian bookworm, in a venv at `/opt/pulsekv-venv` |
| **grpcio** | **1.83.0** | `ARG`, pip `==` |
| **grpcio-tools** | **1.83.0** | `ARG`, pip `==` |
| **protobuf (Python runtime)** | **7.35.1** | `ARG`, pip `==`, mirrored in `pyproject.toml` |

Go module dependencies (`control/go.mod`): `google.golang.org/grpc v1.83.0`,
`google.golang.org/protobuf v1.36.12`, `gopkg.in/yaml.v3 v3.0.1`, plus four
indirect modules. No gossip or Raft libraries yet — those arrive in Phases 3
and 5.

**Three protoc generations, on purpose.** C++ uses Debian's protoc 3.21.12
because generated `.pb.cc` is ABI-coupled to the `libprotobuf` it links
against; mixing a newer standalone protoc with Debian's runtime is a link-time
or, worse, a run-time surprise. Go and Python have no such coupling and use
their own pinned generators (`protoc-gen-go`, and grpcio-tools' bundled
protoc). Each is internally consistent, which is the property that matters.

**Why not a newer gRPC C++.** Building gRPC from source would get 1.7x, at the
cost of a 20–45 minute image build and a from-source dependency tree to
maintain. Debian's 1.51.1 has every API this project uses — server builder,
generated service stubs, server reflection, channel arguments. Revisit if
Phase 6's zero-copy transport wants something newer; the shim is small enough
that moving it is not a project.

**Debian package versions are not `=`-pinned.** Debian's binNMU suffixes
(`1.51.1-3+b1`) differ per architecture, so a hard `apt-get install pkg=version`
pin makes the image un-buildable on the other arch. The base image tag is the
pin; the resolved versions are recorded into `/etc/pulsekv-v2-toolchain` at
build time so the image can always answer for itself.

**Architecture.** Built and verified on **linux/arm64** (Apple Silicon via
colima), including a full `--no-cache` build from scratch. The Dockerfile picks
the Go tarball from BuildKit's `TARGETARCH`, falling back to
`dpkg --print-architecture`; the verification runs used the legacy builder,
where `TARGETARCH` is unset, so the fallback is the path actually exercised.
linux/amd64 should build identically — **but that has not been run**, and
Debian's binNMU revisions may differ there.

---

## 3. The C++ shim — why C++ is in an otherwise-C directory

gRPC has no supported C API for application use; the "C core" is an
implementation substrate for the language bindings, with no generated service
stubs and none of the C++ API's compatibility guarantees. So the storage engine
stays pure C under `node/engine/`, and a thin C++ shim in `node/grpc_shim/`
implements `NodeService` against gRPC's first-class C++ API and reaches into
the engine through `extern "C"`.

The rule the shim is held to: **it contains no storage logic** — it unpacks a
protobuf, calls one C function, packs the result, returns a status. That keeps
every decision worth making (hashing, locking, eviction, tiering, framing) in C
and testable without a network stack, and it keeps the engine free of gRPC so
Phase 6's non-gRPC bulk transport can be added without a rewrite.

Full reasoning in `node/README.md`. `node/grpc_shim/CMakeLists.txt` already
carries the commented-out `add_subdirectory(../engine)` seam Phase 1.1
uncomments.

---

## 4. Cluster size

**Default: 4 nodes** plus 1 control plane — a fast local edit-run-check loop.

To change it, edit `nodes:` in `deploy/cluster.config.yaml`. **Nothing else.**
No script takes the node count as an argument or hardcodes it; they all read
the cluster's shape through `controlplane --print-nodes`, which is the server's
own parser, so the scripts and the server cannot disagree about what the file
says.

```yaml
control_plane:
  port: 7000
shard_count: 256
nodes:
  - {node_id: node-0, port: 7100}
  - {node_id: node-1, port: 7101}
  # ... up to node-31 on port 7131
```

The design target for Phase 3 (gossip convergence) and Phase 5 (Raft leader
election) chaos testing is **8–32 simulated nodes on one machine**. Ports
7100+N keep all 32 clear of the control plane. The config loader rejects
duplicate node IDs and duplicate `host:port` pairs outright, so a copy-paste
slip fails at startup rather than producing a cluster where two nodes quietly
fight over a port.

---

## 5. Commands

Everything runs in the v2 dev image. v2 targets Linux, and the C++ gRPC shim in
particular does not build on a macOS host — same posture as v1.

```sh
# Build the image once, from the repo root. Context is the repo root, not
# deploy/ — the image pre-warms the Go module cache from control/go.mod.
docker build -f deploy/Dockerfile -t pulsekv-v2-dev .

# Interactive session
docker run --rm -it -v "$PWD:/src" -w /src pulsekv-v2-dev bash

#   ... then, inside the container:
deploy/run-local-cluster.sh      # build + boot + wait for health
deploy/smoke-test.sh             # assert the Phase 0 contract
deploy/stop-local-cluster.sh     # terminate everything, sweep orphans
```

One-shot, CI shape:

```sh
docker run --rm -v "$PWD:/src" -w /src pulsekv-v2-dev bash -c '
  deploy/run-local-cluster.sh && deploy/smoke-test.sh; rc=$?
  deploy/stop-local-cluster.sh; exit $rc'
```

Other useful entry points:

```sh
deploy/gen-proto.sh --all                     # regenerate Go + Python; verify C++ generates
cd control && go test ./...                   # config loader unit tests
deploy/run-local-cluster.sh --restart         # replace a running cluster
deploy/run-local-cluster.sh --skip-build      # reboot from prebuilt binaries
pulsekv-v2-versions                           # resolved toolchain versions
grpcurl -plaintext 127.0.0.1:7000 list        # reflection is on for both servers
```

**Process lifetime:** `run-local-cluster.sh` starts processes in the background
and records PIDs in `deploy/run/cluster.pids`, with per-process logs in
`deploy/run/logs/`. It returns once every process is healthy; the cluster runs
until `stop-local-cluster.sh`. Foreground would have made it impossible to run
the smoke test from the same shell.

---

## 6. Exit criteria — verified

| # | Criterion | Evidence |
|---|---|---|
| 1 | All three `.proto` files generate cleanly for Go, Python, and C++ | `deploy/gen-proto.sh --all`: 6 Go files, 10 Python files, 12 C++ files; the Python leg imports the generated package to prove it loads. Regenerating in a fresh container reproduces the 16 checked-in files **byte for byte** (sha256-compared), so the committed stubs are exactly what the pinned toolchain emits |
| 2 | `control/` builds; `ClusterMetadataService` serves real `HealthCheck` and static `GetNodeList`/`GetShardMap` | `go vet` / `go build` / `go test` clean; smoke test asserts 4 nodes with config-matching addresses and 256 shards with known owners |
| 3 | `node/grpc_shim/` builds via CMake; real `HealthCheck`, `UNIMPLEMENTED` elsewhere | 16 of the 24 Go smoke checks are exactly this, across all 4 nodes |
| 4 | `adapters/` installs as a Python package; health client reaches a running service | `pip install ./adapters` runs in the smoke test; `pulsekv-health` then reaches the control plane **and** a C++ node |
| 5 | `run-local-cluster.sh` boots the control plane + 4 nodes on a clean machine using only `deploy/Dockerfile` | full flow verified in a fresh `--rm` container; `GOPROXY=off go build` confirms the image needs no network |
| 6 | `smoke-test.sh` passes end to end | 7 top-level checks green, the first of which is the Go leg's own 24 contract assertions |
| 7 | `stop-local-cluster.sh` stops everything with no orphans | "no orphaned pulsekv processes"; the sweep was also verified against a deliberately deleted pid file |

### Negative paths, also verified

Claiming a script "fails loudly" is worth nothing untested. Each of these was
run against the built cluster:

| Scenario | Result |
|---|---|
| A node's port already taken | Boot fails, names `node-1`, reports it `EXITED`, dumps its log, stops the partial cluster, exits 1 |
| Two nodes given the same port | Second refuses to start — `SO_REUSEPORT` is explicitly disabled (see §7) |
| Boot while a cluster is running | Refused, lists the running processes, suggests `--restart` |
| `--restart` | Stops the old cluster, boots a new one |
| PID file deleted, processes alive | Orphan sweep finds all 5 by command line, kills them, reports each |
| Smoke test with no cluster | Exits 1 with "no cluster is running" |
| Duplicate `node_id` and port in config | Rejected before anything starts, both problems reported at once |
| Stop with nothing running | Exits 0, "Nothing to stop" |

---

## 7. Deliberate deviations from the Phase 0 prompt

No RPC was renamed or removed. Everything below is additive or an
implementation choice the prompt left open.

**1. `AdapterService` health-check substitution — explicitly flagged.**
Phase 0's exit criteria ask for a Python client that calls
`AdapterService.HealthCheck`. **Nothing implements `AdapterService` in Phase
0** — it is the surface Phase 7's adapter will call *into*, and its server side
is Phase 7 work. So, as the prompt directs,
`adapters/pulsekv_adapters/health_client.py` proves the Python↔Go gRPC path by
calling **`ClusterMetadataService.HealthCheck`** instead. `check_adapter_service()`
also exists, uses the real generated `AdapterService` stubs, and against a Phase
0 cluster returns `ok=False` with `UNIMPLEMENTED` — which the smoke test asserts
rather than skips. `check_node()` was added too, because proving Python↔C++
costs three extra lines.

**2. `NodeInfo.alive` is a real probe, not a hardcoded `true`.** `GetNodeList`
health-checks each node in parallel with a 300 ms budget. This is not
membership — no failure detector, no suspicion state, no effect on the shard
map — and Phase 3's gossip replaces it wholesale. Returning `alive=true`
unconditionally would have been less code and a lie.

**3. Config gained `shard_count` and an optional per-node `host`.**
`GetShardMap` needs a shard count from somewhere, and `NodeInfo.address` is
`host:port`. Shards are assigned **round-robin**, which is deterministic and
obviously a placeholder — Phase 2.1 replaces it with rendezvous hashing.
Nothing should depend on the current distribution.

**4. Codegen policy differs per language** (documented in `proto/README.md`):
Go and Python stubs are **checked in** so `go build ./...` and
`pip install ./adapters` work with only that language's toolchain; C++ is
**generated by CMake at build time** because `.pb.cc` is ABI-coupled to the
local `libprotobuf`.

**5. The smoke client lives in the Go module** (`control/cmd/pulsekv-smoke`)
rather than being a throwaway script. It compiles against the same checked-in
stubs the control plane does, so it tests the contract instead of a second copy
of it. `grpcurl` is installed in the image and used for an optional reflection
leg, but never for the contract assertions.

**6. gRPC server reflection is enabled on both servers**, so a live cluster can
be inspected with `grpcurl` without being handed the `.proto` files. Optional in
CMake — a toolchain without `libgrpc++_reflection` still builds.

**7. `SO_REUSEPORT` is explicitly disabled on the node.** gRPC C++ enables it by
default, which on a single-machine dev cluster means two nodes handed the same
port would *both* bind successfully and the kernel would split connections
between them. That failure mode is extremely hard to read. Disabled, a port
collision is immediate and loud.

**8. Two extra files in `deploy/`:** `common.sh` (shared paths and helpers) and
`gen-proto.sh` (the codegen driver). Build output goes to `deploy/build/` and
runtime state to `deploy/run/`, rather than the repo-root `build/`, because
v1's `make clean` does `rm -rf build/`.

**9. Binaries are named `pulsekv-controlplane` and `pulsekv-node`.**

---

## 8. Where Phase 1 starts

`node/engine/` is empty and `node/grpc_shim/main.cpp`'s `Get`/`Put`/
`PrefixMatch`/`Capacity` return `UNIMPLEMENTED` from a single `NotImplementedYet()`
helper. Phase 1.1 extracts v1's `src/hashtable.c` and `src/main.c`'s worker
model into `node/engine/` behind an `extern "C"` API, with v1's existing test
suite as the regression gate — **copying from v1, never moving it.**

The CMake seam is already written and commented out:

```cmake
# add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../engine" engine)
# target_link_libraries(pulsekv-node PRIVATE pulsekv_engine)
```

Nothing else in the shim should need to change.

`deploy/smoke-test.sh` will start failing the moment those RPCs stop returning
`UNIMPLEMENTED` — that is intended. Update those assertions as each RPC lands;
the phase is not done while a stub still claims success it cannot deliver.

Phase 2 (Go control plane: rendezvous hashing, client SDK) can proceed in
parallel from here. Nothing after Phase 2 can.
