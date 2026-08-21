# PulseKV v3 / Phase 10 — Codebase Impact Map

**Status:** reconciled through the merged Phase 10.6 implementation. Original
design-time classifications are retained where they explain intent; Phase
10.6 evidence supersedes the blanket adapter freeze. Classification legend:
**FROZEN** (semantic/core responsibility does not move here),
**READ-ONLY / INTEGRATION-ONLY** (consumed as-is, or mechanically maintained
only when a pinned real upstream API proves it necessary),
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

**Semantic responsibilities and key semantics: FROZEN. Engine-specific API
surface: INTEGRATION-ONLY.** The original map classified all four files as
source-frozen because Finding 2 expected canonical text to flow through the
existing adapters unchanged. Phase 10.6 disproved that stronger expectation:
real SGLang 0.5.15 required a mechanical compatibility repair in `sglang.py`
for its dynamic HiCache factory, batch/pool and tensor-transfer contracts.

That exception did not move matching, registry access or canonicalization
into the adapter, and `key.py` remained unchanged. `vllm.py` and
`vllm_key.py` have not been validated by Phase 10.7; remaining unchanged is
their desired initial compatibility hypothesis, not an unconditional fact.
Phase 10.7 must audit a pinned real vLLM API and name any gap before proposing
a compatibility change.

## `adapters/pulsekv_adapters/health_client.py`, `__init__.py`

**FROZEN.** No relationship to canonicalization; not read by any Phase 10
component.

## `adapters/tests/`

**EXTEND for compatibility evidence; existing behavior remains a regression
gate.** Phase 10.6 added `test_sglang_0515_contract.py` and adjusted the
existing SGLang adapter/integration tests. This is the correct home for
pinned upstream contract and fallback/trace behavior. Phase 10.7 may likewise
need test-only extensions after its read-only vLLM compatibility audit.

## `deploy/`

**EXTEND.** The original plan anticipated two additive demo scripts and
possible Make targets. As built through Phase 10.6:

- Phase 10.6 added `deploy/demo-semantic-sglang.sh`, modeled on the existing
  SGLang cross-replica proof but launching its own gateway/engine processes.
- `deploy/demo-semantic-vllm.sh` remains planned for Phase 10.7; it does not
  exist yet.
- No Phase 10.6 Makefile target or edit to an existing deploy script was
  merged. The demo calls the existing cluster lifecycle scripts as a client.

`deploy/soak-test.sh` and `deploy/chaos-test.sh` are **READ-ONLY /
INTEGRATION-ONLY** for Phase 10.9, which reuses their existing harness shape
for the gateway's own soak run rather than rewriting either.

## `docs/`

**EXTEND.** The design-time investigation's deliverables landed here, followed by
progress-report and up to nine phase-summary docs as Phase 10 actually
executes (mirroring v2's `pulsekv-v2-phaseN-summary.md` pattern exactly). No
existing `docs/pulsekv-v2-*.md` file is edited by this investigation or its
plan — `docs/pulsekv-v2-semantic-canonicalization-report.md` is superseded
in spirit by `pulsekv-semantic-context-design.md` (which says so explicitly
in its own header) but is left in place as historical record, consistent
with how this project has handled every other prior-report/current-source
disagreement (precedence stated, old doc not deleted).

## `gateway/`

**NEW COMPONENT, implemented through Phase 10.6.** The deliverable of Phase 10.0 through 10.9, per
the implementation plan's directory layout (`pulsekv_gateway/{server,
decomposer,encoder,index,registry,models,normalizer,guardrail,assembler,
config,auditlog}.py`, `tests/`, `tests/corpus/`, `pyproject.toml`).

## `Dockerfile` (root, v1) and `deploy/Dockerfile` (v2)

**FROZEN** (root) / **EXTEND if a future integrated image requires it**
(`deploy/Dockerfile`). Neither Phase 10.5 nor 10.6 changed these files. The
Phase 10.6 demo instead requires pre-existing, separately selected SGLang and
gateway virtual environments so its pinned engine dependency does not silently
become the general v2 build image's contract.

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
| `adapters/pulsekv_adapters/{key,sglang,vllm,vllm_key}.py` | Semantic responsibility/key semantics FROZEN; engine API compatibility INTEGRATION-ONLY (`sglang.py` repaired in 10.6; vLLM unverified for 10.7) |
| `adapters/pulsekv_adapters/{health_client,__init__}.py` | FROZEN |
| `adapters/tests/` | EXTEND for upstream compatibility evidence; existing tests remain regression gates |
| `deploy/` | EXTEND (`demo-semantic-sglang.sh` added in 10.6; vLLM demo remains planned; existing scripts integration-only) |
| `docs/` | EXTEND (phase summaries, status reconciliation and eventual progress report) |
| `gateway/` | NEW COMPONENT, implemented through Phase 10.6 |
| `deploy/Dockerfile` | Unchanged through 10.6; EXTEND only if a future integrated image requires it |
| root `Dockerfile` | FROZEN |
