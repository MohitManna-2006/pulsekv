# PulseKV v2 — Phase 0 Implementation Prompt (for Claude Code)

**How to use this file:** paste everything below the line into Claude Code as the task prompt for
the first v2 session, run from inside the `pulsekv` repo root.

---

You are implementing **Phase 0 only** of PulseKV v2, a distributed LLM KV-cache system. Before
writing any code, read these two files in the repo for full context — they are authoritative:

- `docs/pulsekv-v2-distributed-design.md` — what v2 is and why.
- `docs/pulsekv-v2-implementation-plan.md` — the full phase-by-phase build order. Your scope
  today is **Section 3, Phase 0**, reproduced and expanded below with exact specifics.

## Hard scope boundary

- **Do not modify** anything under `src/`, `include/`, `tests/`, the root `Makefile`, the root
  `Dockerfile`, `README.md`, or any existing file under `docs/`. Those are PulseKV v1 — complete,
  documented, and untouched by this project.
- **Do not** implement real `Get`/`Put`/`PrefixMatch` logic in the C engine. That's Phase 1.
- **Do not** implement gossip membership or Raft. Those are Phases 3 and 5.
- **Do not** implement LLM adapter business logic. That's Phases 7 and 8. `adapters/` in this
  phase is a package skeleton plus a health-check-only gRPC client stub — nothing more.
- Your only goal: a frozen, code-generated protobuf/gRPC contract, and empty skeleton services on
  the Go and C sides that build and correctly answer a `HealthCheck` RPC, plus a script that boots
  a local multi-node dev cluster of those skeletons.

## Target repository layout

Create this structure (v1 directories are shown only for reference — do not touch them):

```
pulsekv/
├── src/ include/ tests/ Makefile Dockerfile README.md   # v1 — DO NOT TOUCH
├── docs/                                                 # existing docs — do not touch;
│                                                          # you may ADD new files here
├── proto/
│   ├── node.proto
│   ├── metadata.proto
│   └── adapter.proto
├── node/                            # C data-plane (Phase 0: skeleton + gRPC boundary only)
│   ├── engine/                      # empty placeholder; Phase 1 extracts v1's engine here
│   ├── grpc_shim/                   # thin C++ shim exposing NodeService over gRPC
│   │   ├── main.cpp
│   │   └── CMakeLists.txt
│   └── README.md                    # explain the shim's purpose (see "Technical decisions" below)
├── control/                         # Go control plane
│   ├── go.mod
│   ├── cmd/controlplane/main.go
│   ├── internal/metadata/           # ClusterMetadataService impl (Phase 0: static config only)
│   └── gen/                         # generated Go protobuf/grpc code (gitignored source, or committed — your call, document it)
├── adapters/                        # Python adapter package skeleton
│   ├── pyproject.toml
│   └── pulsekv_adapters/
│       ├── __init__.py
│       └── health_client.py         # calls AdapterService.HealthCheck only
└── deploy/
    ├── Dockerfile                   # NEW, v2-specific polyglot build image — do not edit root Dockerfile
    ├── cluster.config.yaml          # static node list: node_id, port, per node
    ├── run-local-cluster.sh
    ├── stop-local-cluster.sh
    └── smoke-test.sh
```

## Step 0.1 — Repository layout

Create the directories above. Each language-scoped directory should be independently buildable
(Go module in `control/`, C/CMake build in `node/`, Python package in `adapters/`).

## Step 0.2 — Define and generate the gRPC/protobuf contract

Create exactly these three `.proto` files under `proto/`. Use them as the starting contract — you
may add fields if you find a real gap, but do not remove or rename the RPCs below without a good
reason, since Phases 1–8 are written against this shape.

**`proto/node.proto`** — the interface a control-plane instance uses to talk to one data-plane
node:

```proto
syntax = "proto3";
package pulsekv.node.v1;
option go_package = "pulsekv/control/gen/node/v1;nodev1";

service NodeService {
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Put(PutRequest) returns (PutResponse);
  rpc PrefixMatch(PrefixMatchRequest) returns (stream PrefixMatchResponse);
  rpc Capacity(CapacityRequest) returns (CapacityResponse);
}

message HealthCheckRequest {}
message HealthCheckResponse {
  bool ok = 1;
  string node_id = 2;
  int64 uptime_seconds = 3;
}

message GetRequest { bytes key = 1; }
message GetResponse { bool found = 1; bytes value = 2; }

message PutRequest { bytes key = 1; bytes value = 2; }
message PutResponse { bool ok = 1; string error = 2; }

message PrefixMatchRequest { bytes prefix = 1; }
message PrefixMatchResponse { bytes key = 1; bytes value = 2; }

message CapacityRequest {}
message CapacityResponse {
  uint64 resident_keys = 1;
  uint64 bytes_in_ram_tier = 2;
  uint64 bytes_in_nvme_tier = 3;
}
```

**`proto/metadata.proto`** — the interface clients/adapters use to discover cluster shape:

```proto
syntax = "proto3";
package pulsekv.metadata.v1;
option go_package = "pulsekv/control/gen/metadata/v1;metadatav1";

service ClusterMetadataService {
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
  rpc GetNodeList(GetNodeListRequest) returns (GetNodeListResponse);
  rpc GetShardMap(GetShardMapRequest) returns (GetShardMapResponse);
}

message HealthCheckRequest {}
message HealthCheckResponse { bool ok = 1; int64 uptime_seconds = 2; }

message NodeInfo {
  string node_id = 1;
  string address = 2;   // host:port where this node's NodeService listens
  bool alive = 3;
}

message GetNodeListRequest {}
message GetNodeListResponse { repeated NodeInfo nodes = 1; }

message GetShardMapRequest {}
message GetShardMapResponse {
  // Phase 0: static, loaded from deploy/cluster.config.yaml.
  // Phase 3+ replaces the source of this data with gossip/Raft; the RPC shape does not change.
  map<uint32, string> shard_to_node_id = 1;
}
```

**`proto/adapter.proto`** — the narrow surface Python LLM adapters call, deliberately shaped like
SGLang HiCache's own `get`/`exist`/`set` backend interface so the Phase 7 adapter is a thin
pass-through:

```proto
syntax = "proto3";
package pulsekv.adapter.v1;
option go_package = "pulsekv/control/gen/adapter/v1;adapterv1";

service AdapterService {
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
  rpc Get(AdapterGetRequest) returns (AdapterGetResponse);
  rpc Exists(AdapterExistsRequest) returns (AdapterExistsResponse);
  rpc Set(AdapterSetRequest) returns (AdapterSetResponse);
}

message HealthCheckRequest {}
message HealthCheckResponse { bool ok = 1; }

message AdapterGetRequest { bytes key = 1; }
message AdapterGetResponse { bool found = 1; bytes value = 2; }

message AdapterExistsRequest { bytes key = 1; }
message AdapterExistsResponse { bool exists = 1; }

message AdapterSetRequest { bytes key = 1; bytes value = 2; }
message AdapterSetResponse { bool ok = 1; string error = 2; }
```

**Codegen:**

- Go: `protoc-gen-go` + `protoc-gen-go-grpc`, output into `control/gen/`.
- Python: `grpcio-tools` (`python -m grpc_tools.protoc`), output into
  `adapters/pulsekv_adapters/gen/`.
- C++ shim: standard `protoc` C++ plugin + gRPC C++ codegen, output into `node/grpc_shim/gen/`.

Document the exact `protoc`/plugin versions used in `deploy/Dockerfile` so the build is
reproducible — don't rely on whatever happens to be on the host.

**Phase 0 behavior requirement:** for every RPC above except `HealthCheck`, the skeleton
implementation must compile and return `grpc::StatusCode::UNIMPLEMENTED` (or the language
equivalent) rather than a fake success. `HealthCheck` must return real, correct data (`ok = true`,
actual `node_id`/`uptime_seconds`).

## Step 0.3 — C-side gRPC boundary: technical decision to implement

gRPC's native C support (the "C core") is low-level and not meant for direct application use —
don't fight it. The pragmatic, standard pattern (used broadly for giving C libraries a gRPC
surface) is:

- Keep the actual storage engine in pure C, under `node/engine/` (empty placeholder for now —
  Phase 1 extracts v1's `hashtable.c`/`main.c` logic here).
- Put a **thin C++ shim** in `node/grpc_shim/` that links against `node/engine/`'s public API via
  `extern "C"`, and implements `NodeService` using gRPC's first-class, well-documented C++ API.
  This shim is Phase 0's actual deliverable on the C side — for Phase 0 it doesn't call into a
  real engine yet (there isn't one), it just answers `HealthCheck` and returns `UNIMPLEMENTED` for
  everything else.
- Write a short `node/README.md` explaining this boundary so Phase 1 (and whoever picks it up)
  understands why there's C++ in an otherwise-C directory.

Build the shim with CMake (`node/grpc_shim/CMakeLists.txt`) rather than hand-written Makefiles —
gRPC C++'s official build guidance assumes CMake, and fighting that adds no value here.

## Step 0.4 — Local multi-process dev cluster

`deploy/cluster.config.yaml` — static config for Phase 0 (example shape, adjust as needed):

```yaml
control_plane:
  port: 7000
nodes:
  - node_id: node-0
    port: 7100
  - node_id: node-1
    port: 7101
  - node_id: node-2
    port: 7102
  - node_id: node-3
    port: 7103
```

Default to **4 nodes** for the fast local dev loop. Document in this same file (as a comment) that
the design target for later chaos/gossip/Raft testing (Phases 3 and 5) is 8–32 simulated nodes on
one machine, and that `cluster.config.yaml` should support scaling `nodes:` up to that range
without script changes.

`deploy/run-local-cluster.sh`:
- Builds the Go control-plane binary and the C++ `grpc_shim` binary.
- Starts the control-plane process on its configured port, loading `cluster.config.yaml` so
  `GetNodeList`/`GetShardMap` return real (static) data.
- Starts one `grpc_shim` node process per entry in `cluster.config.yaml`, each on its configured
  port.
- Polls every process's `HealthCheck` RPC until all report `ok = true` (or times out after a
  reasonable interval, e.g. 15s, and fails loudly with which process didn't come up).
- Prints a clear "cluster ready" banner listing every process, its PID, and its port.
- Leaves processes running in the foreground or writes PIDs to a file for `stop-local-cluster.sh`
  to clean up — your choice, document whichever you pick.

`deploy/stop-local-cluster.sh` — cleanly terminates everything `run-local-cluster.sh` started.

`deploy/smoke-test.sh` — after the cluster is up, calls `HealthCheck` against the control plane
and every node (via `grpcurl` if available, otherwise a small throwaway Go or Python client using
the generated stubs) and exits non-zero with a clear message if any check fails or any RPC other
than `HealthCheck` unexpectedly does *not* return `UNIMPLEMENTED`.

## Step 0.5 — Build environment

Create `deploy/Dockerfile` (new file, separate from the root v1 `Dockerfile`, which must not be
touched) containing everything needed to build and run all three languages reproducibly on a
machine that only has Docker (matching how v1 handles macOS development):

- A recent Debian or Ubuntu base
- `gcc`/`make` (for later phases' C work)
- `cmake` + `g++` + gRPC/protobuf C++ dev packages (for the `grpc_shim`)
- Go toolchain (a recent stable version)
- Python 3 + `pip`/`venv`
- `protoc` plus the Go, Python, and C++ gRPC codegen plugins, pinned to specific versions

## Exit criteria — verify all of these before considering Phase 0 done

1. `proto/node.proto`, `proto/metadata.proto`, `proto/adapter.proto` exist as specified (or with
   documented, deliberate deviations) and generate cleanly for Go, Python, and C++.
2. `control/` builds as a Go module and its `ClusterMetadataService` skeleton starts, serving real
   `HealthCheck` and static `GetNodeList`/`GetShardMap` data from `cluster.config.yaml`.
3. `node/grpc_shim/` builds via CMake and starts, serving real `HealthCheck` and
   `UNIMPLEMENTED` for `Get`/`Put`/`PrefixMatch`/`Capacity`.
4. `adapters/` installs as a Python package and its health-check client successfully calls a
   running `AdapterService`... — **note:** `AdapterService` itself isn't implemented by anything
   yet in Phase 0 (it's the surface Phase 7's adapter will call into the control plane through);
   for Phase 0, wire `adapters/pulsekv_adapters/health_client.py` to call
   `ClusterMetadataService.HealthCheck` instead, as the concrete proof the Python side can talk
   gRPC to the Go side at all. Note this substitution explicitly in your final summary.
5. `deploy/run-local-cluster.sh` boots the control plane plus 4 nodes (per the default config),
   all passing health checks, on a clean machine using only `deploy/Dockerfile`.
6. `deploy/smoke-test.sh` passes end to end.
7. Running `deploy/stop-local-cluster.sh` cleanly stops everything with no orphaned processes.

## Final deliverable

Write a short `docs/pulsekv-v2-phase0-summary.md` (new file — this doesn't touch any existing doc)
covering: the final directory tree, exact versions of every pinned tool (`protoc`, gRPC, Go,
Python), the C++ shim rationale in 2–3 sentences, the default cluster size and how to change it,
and the exact commands to boot the cluster and run the smoke test. This is the entry point the
Phase 1 session will read first.

Do not start any Phase 1 work, even if it looks trivial. Stop once the exit criteria above are all
verified and the summary doc is written.
