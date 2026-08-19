# PulseKV v3 / Phase 10 — Codebase Impact Map

**Status:** derived from direct inspection of the listed files (not from
docs alone) as of this investigation. Classification legend: **FROZEN** (no
phase of Phase 10 touches this), **READ-ONLY / INTEGRATION-ONLY** (Phase 10
code calls into it as a consumer, exactly as it exists today, no edits),
**EXTEND** (a real, scoped addition), **NEW COMPONENT**.

---

## `src/`, `include/`, `tests/` (v1)

**FROZEN.** No phase of this investigation or its implementation plan has
any reason to touch v1. Confirmed by inspection: v1 has no relationship to
v2's adapters, registry, or gateway concerns — it is a separate, complete,
standalone project per the project's own instructions.

## `node/engine/`

**FROZEN.** Verified against `node/engine/README.md`'s own stated rule ("No
gRPC, no C++, no protobuf... enforced by CMake, not by discipline") and
`pulsekv_engine.h`'s public surface — nothing in this header exposes, or
should expose, a canonicalization-aware concept. Design doc §9 rejected
placing any semantic logic here on both correctness-boundary and
CMake-enforcement grounds. No Phase 10 phase adds a dependency here.

## `node/grpc_shim/`

**FROZEN.** `node/README.md` states the shim's rule plainly: "it contains no
storage logic... unpacks a protobuf, calls one C function, packs the result,
returns a status." Canonicalization is neither storage logic nor a
protobuf-unpacking concern — it happens entirely upstream of any RPC this
shim serves. No Phase 10 phase touches `main.cpp`, `bulk.cc`, or `bulk.h`.

## `control/` (all of `internal/`, `pkg/client/`, `cmd/`)

**FROZEN.** Verified against `control/internal/metadata/service.go` (the
readiness-gated `snapshot()` choke point) and `control/internal/router/router.go`
(the pure `ShardForKey`/`AssignShards`/`AssignShardOwners` functions) — Phase
10's registry is explicitly kept out of this package (design doc §6 Finding
3, §9 option B) because Raft is reserved for low-volume cluster metadata, not
a per-request-read registry. `control/pkg/client/client.go` (the Go SDK) is
not called by anything in the gateway design — the gateway is a Python
process using `pulsekv_adapters.client.PulseKVClient`, the existing Python
SDK, not a new Go integration. No Phase 10 phase adds a dependency on any
`control/` package, and none of `control/cmd/pulsekv-{chaos,cluster-bench,
member,metrics,node-bench,smoke}` needs modification for anything in the
implementation plan through Phase 10.9.

## `proto/`

**READ-ONLY / INTEGRATION-ONLY**, with one specific, notable non-finding:
`proto/adapter.proto`'s `AdapterService` was frozen in Phase 0 as "the narrow
surface the Python LLM adapters call," but direct inspection of
`adapters/pulsekv_adapters/sglang.py` and `vllm.py` confirms neither one
constructs an `AdapterService` stub — both call `PulseKVClient` directly
against `ClusterMetadataService`/`NodeService`. `AdapterService` is
unimplemented and uncalled in the current codebase. Phase 10's gateway does
not implement or call `AdapterService` either — it uses the same
`PulseKVClient` pattern the real adapters already use (design doc §6 Finding
1). No `.proto` file needs a new field or RPC for anything in this
investigation's scope; if a future phase decides the gateway should be
reachable over gRPC by other services (not required by the current design),
that would be a genuinely new proto file under a `gateway.v1` package, not a
change to any of the three existing ones.

## `adapters/pulsekv_adapters/client.py`

**READ-ONLY / INTEGRATION-ONLY.** The gateway's Tier 0 registry-adjacent
lookups and its (design doc §8) illustrative diagram both treat this module
as a plain importable library — `PulseKVClient(control_plane_addresses=...)`
constructed and used exactly as `sglang.py`/`vllm.py` already do. No edit
anywhere in the implementation plan touches this file. Verified this class
has no LLM-specific coupling that would need generalizing further — it
already speaks in raw `bytes`/`str` keys and values, which is exactly what a
gateway needs.

## `adapters/pulsekv_adapters/{key.py, sglang.py, vllm.py, vllm_key.py}`

**FROZEN.** These are the exact modules the design doc's Finding 2 depends
on staying unmodified — SGLang's and vLLM's own tokenizers, operating on
gateway-produced canonical text, must produce the same block hashes they
would for any other input, through this completely unchanged code path. Any
edit here would undermine the specific claim (§6, Finding 2) that makes the
whole architecture "zero PulseKV-side changes." Phases 10.6/10.7's hard
scope boundaries state this explicitly as a rule, not just an expectation.

## `adapters/pulsekv_adapters/health_client.py`, `__init__.py`

**FROZEN.** No relationship to canonicalization; not read by any Phase 10
component.

## `adapters/tests/`

**FROZEN.** Existing adapter tests are the regression gate proving Phase 10
did not disturb Phase 7/8's behavior — Phase 10.6/10.7's exit criteria
include confirming these still pass, but no phase edits them.

## `deploy/`

**EXTEND.** Two concrete, scoped additions anticipated by the implementation
plan, both additive:

- `deploy/demo-semantic-sglang.sh` / `deploy/demo-semantic-vllm.sh` (Phase
  10.6/10.7) — new scripts, modeled on the existing
  `demo-cross-replica-{sglang,vllm}.sh` shape, not edits to those files.
- `Makefile` gains new targets (`make demo-semantic`, `make gateway-test` or
  similar) the same way Phase 8 added `demo-vllm` alongside the existing
  `demo-sglang` target without touching it. `deploy/cluster.config.yaml` is
  not expected to need changes — the gateway is a client of the existing
  cluster, not a new cluster member — but if Phase 10.6 finds the dev-cluster
  scripts need a minimal extension to also boot a gateway process, that is
  the one place a small, flagged extension to existing `deploy/` scripts
  (not proto, not core logic) would be acceptable, per the same precedent
  Phase 7's prompt set ("flag any such extension clearly").

`deploy/soak-test.sh` and `deploy/chaos-test.sh` are **READ-ONLY /
INTEGRATION-ONLY** for Phase 10.9, which reuses their existing harness shape
for the gateway's own soak run rather than rewriting either.

## `docs/`

**EXTEND.** This investigation's own five deliverables land here, plus one
progress-report and up to nine phase-summary docs as Phase 10 actually
executes (mirroring v2's `pulsekv-v2-phaseN-summary.md` pattern exactly). No
existing `docs/pulsekv-v2-*.md` file is edited by this investigation or its
plan — `docs/pulsekv-v2-semantic-canonicalization-report.md` is superseded
in spirit by `pulsekv-semantic-context-design.md` (which says so explicitly
in its own header) but is left in place as historical record, consistent
with how this project has handled every other prior-report/current-source
disagreement (precedence stated, old doc not deleted).

## `gateway/` (does not exist yet)

**NEW COMPONENT.** The entire deliverable of Phase 10.0 through 10.9, per
the implementation plan's directory layout (`pulsekv_gateway/{server,
decomposer,encoder,index,registry,models,normalizer,guardrail,assembler,
config,auditlog}.py`, `tests/`, `tests/corpus/`, `pyproject.toml`).

## `Dockerfile` (root, v1) and `deploy/Dockerfile` (v2)

**FROZEN** (root) / **EXTEND, deferred** (`deploy/Dockerfile`) — the v2 dev
image would eventually want the gateway's Python dependencies
(FastAPI/ASGI server, an embedding runtime) added for a fully-scripted
`make`-driven dev loop, but this is not required until Phase 10.5 actually
needs to run the gateway inside that image; earlier phases (10.0–10.4) can
develop and test the gateway's pure-Python logic without any container
change at all, since none of it depends on the C++/Go toolchain the current
image exists to provide.

---

## Summary table

| Directory | Classification |
|---|---|
| `src/`, `include/`, `tests/` (v1) | FROZEN |
| `node/engine/` | FROZEN |
| `node/grpc_shim/` | FROZEN |
| `control/` (all) | FROZEN |
| `proto/` | READ-ONLY / INTEGRATION-ONLY (no changes; `AdapterService` confirmed unused, not adopted) |
| `adapters/pulsekv_adapters/client.py` | READ-ONLY / INTEGRATION-ONLY |
| `adapters/pulsekv_adapters/{key,sglang,vllm,vllm_key}.py` | FROZEN |
| `adapters/pulsekv_adapters/{health_client,__init__}.py` | FROZEN |
| `adapters/tests/` | FROZEN (regression gate only) |
| `deploy/` | EXTEND (new demo scripts, new Makefile targets; existing scripts read-only/integration-only) |
| `docs/` | EXTEND (new docs only; existing docs untouched) |
| `gateway/` | NEW COMPONENT |
| `deploy/Dockerfile` | EXTEND, deferred to Phase 10.5 |
| root `Dockerfile` | FROZEN |
