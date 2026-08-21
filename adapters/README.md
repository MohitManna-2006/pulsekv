# `adapters/` — Python LLM serving integrations

Layer 2 of PulseKV v2 connects real inference-engine external-cache APIs to
the distributed PulseKV client. The adapters store and retrieve opaque KV
payloads; they do not decide semantic equivalence. Phase 10's canonicalization
happens in the separate gateway before the engine tokenizes a request.

## Current components

| Module | Current role | Evidence boundary |
|---|---|---|
| `pulsekv_adapters.client` | Generic topology-aware Python client for control-plane discovery and unary/chunked/bulk data-node operations | Shared by engine adapters; not LLM-semantic |
| `pulsekv_adapters.key` | SGLang token/block key helpers | Exact identity only |
| `pulsekv_adapters.sglang` | SGLang HiCache external-storage backend | Phase 7 integration, mechanically updated in Phase 10.6 for the real SGLang 0.5.15 contract |
| `pulsekv_adapters.vllm` | vLLM KVConnector v1 scheduler/worker integration | Implemented in Phase 8; no Phase 10.7 real-version proof yet |
| `pulsekv_adapters.vllm_key` | Model/layer-aware vLLM key derivation | Exact identity only; Phase 10.7 compatibility remains unverified |
| `pulsekv_adapters.health_client` | Minimal generated-client health wrapper | Compatibility utility |

Python is required here because SGLang and vLLM expose their external-cache
interfaces as Python contracts. PulseKV's C storage engine and Go control plane
remain independent of those engine-specific APIs.

## Generic PulseKV client

`PulseKVClient` discovers the shard map through one or more control-plane
addresses, routes a key to its primary/replicas, and selects unary, chunked or
bulk transport by payload size. Its public values are `str`/`bytes`; it has no
prompt, tokenizer, model or semantic-matching responsibility.

The checked-in protobuf stubs under `pulsekv_adapters/gen/` bind this client to
the existing control/data-plane RPC contracts. Do not hand-edit them.

## SGLang HiCache

`PulseKVHiCacheStorage` implements the external storage operations used by
SGLang HiCache. Phase 10.6 tested the real dynamic backend contract against
SGLang **0.5.15**, including:

- dynamic-factory construction and real base-class inheritance;
- v1 and v2 batch existence/get/set paths;
- pool transfers and hit policies;
- opaque-key and legacy method compatibility;
- CPU/CUDA target tensor copies with size validation;
- optional JSONL tracing that cannot break storage when its sink fails;
- fallback operation when SGLang is not installed.

This was a mechanical upstream-API compatibility repair. Semantic matching
still happens in `gateway/`; the adapter receives engine-generated keys and
opaque KV data.

The real Phase 10.6 proof is reconstructed in
[`docs/pulsekv-semantic-context-phase10.6-summary.md`](../docs/pulsekv-semantic-context-phase10.6-summary.md).

## vLLM KVConnector

`PulseKVKVConnector` carries the Phase 8 scheduler/worker split: scheduler-side
prefix matching, per-layer worker save/load, request lifecycle handling and
model/layer namespacing through `vllm_key.py`.

Those unit and integration surfaces are present, but **Phase 10.7 has not
started**. The repository does not yet contain a real, pinned current-vLLM
semantic gateway proof. Phase 10.7 must begin with a read-only compatibility
audit before treating `vllm.py`/`vllm_key.py` as compatible or proposing a
mechanical repair.

## Test surfaces

`adapters/tests/` currently covers:

- `test_client.py`: key-to-shard hashing, topology fingerprinting and optional
  live-cluster unary, miss, chunked-blob and prefix-match operations;
- `test_key_alignment.py`: token-byte encoding, reference and chained hashes,
  shared-prefix key equality and formatted SGLang cache keys;
- `test_sglang_adapter.py`: basic get/exist/set behavior, batch operations,
  longest-prefix existence and optional live storage operations;
- `test_sglang_integration.py`: backend registration, tensor page round trips
  and a simulated hierarchical radix-cache lifecycle;
- `test_sglang_0515_contract.py`: pinned SGLang 0.5.15 factory, v1/v2,
  pool-policy, tensor, generated-protobuf import, no-SGLang fallback and
  trace/trace-failure behavior;
- `test_vllm_adapter.py`: scheduler prefix matching, request lifecycle,
  worker layer save/load, model namespace isolation and optional live storage;
- `test_vllm_integration.py`: connector registration, multi-layer tensor
  round trips and simulated scheduler/worker coordination;
- `test_vllm_key.py`: token bytes, reference/chained block hashes, key
  formatting/parsing and per-layer key derivation.

Some tests are synthetic or use fakes and prove local contract behavior.
Tests gated on a live PulseKV cluster prove the RPC/storage path when that
cluster is supplied. The Phase 10.6 demo goes further for SGLang by launching
real serving processes and correlating cross-process write/read keys. No
equivalent Phase 10.7 vLLM result exists yet.

Run the adapter suite from the repository root with the existing Make target:

```bash
make test-adapter
```

Live-cluster tests use `PULSEKV_CONTROL_PLANE_ADDRESS` when available and skip
otherwise.

## Regenerating protobuf stubs

The generated Python protobuf code is checked in so the package imports from a
fresh clone. Regenerate it through the repository's existing deployment/codegen
workflow after an intentional `proto/` change; do not edit generated files by
hand.
