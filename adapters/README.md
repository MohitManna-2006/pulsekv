# `adapters/` — Python LLM serving adapters

Layer 2 of PulseKV v2: the thing that lets a real inference engine attach to
the cluster and get prefix-cache hits across replicas instead of recomputing
attention state per request.

Python is not a preference here — vLLM and SGLang expose their external-cache
interfaces as Python classes, so an adapter has to be one.

## Phase 0 status

A package skeleton and a health-check gRPC client. **No cache logic.** That is
the entire scope, on purpose: Phases 1–6 have to exist before an adapter has
anything to adapt to.

```
adapters/
├── pyproject.toml
└── pulsekv_adapters/
    ├── __init__.py
    ├── health_client.py      # the only working code in Phase 0
    └── gen/                  # generated stubs, checked in
```

## Install and run

```sh
pip install ./adapters
pulsekv-health --address 127.0.0.1:7000
```

or, without installing:

```sh
PYTHONPATH=adapters python -m pulsekv_adapters.health_client --address 127.0.0.1:7000
```

`deploy/smoke-test.sh` runs this against the live dev cluster as its Python leg.

## The `AdapterService` substitution

Phase 0's exit criteria ask for a client that calls
`AdapterService.HealthCheck`. Nothing implements `AdapterService` yet — it is
the surface Phase 7's adapter will call *into*, and its server side is Phase 7
work. So:

- `check_adapter_service()` exists, is correct, and uses the generated
  `AdapterService` stubs. Against a Phase 0 cluster it returns `ok=False` with
  `UNIMPLEMENTED`, which is the honest result.
- `check_cluster_metadata()` — calling `ClusterMetadataService.HealthCheck` on
  the Go control plane — is what actually proves Python↔Go gRPC works, and is
  what the smoke test asserts on.

`check_node()` is thrown in as well, because proving Python↔C++ costs three
extra lines and covers the other language boundary.

## What lands later

| Module | Interface | Phase |
|---|---|---|
| `pulsekv_adapters.sglang` | SGLang HiCache external backend: `get` / `exist` / `set` | 7 |
| `pulsekv_adapters.vllm` | vLLM KVConnector v1: `get_num_new_matched_tokens`, `save_kv_layer`, `request_finished` | 8 |

SGLang comes first because its interface is three methods; vLLM's splits across
scheduler-side and worker-side hooks invoked per transformer layer and needs the
Phase 6 bulk transport rather than the gRPC control path.

Both stay thin. `adapter.proto` was shaped to match HiCache's own backend
interface precisely so the Phase 7 adapter is a pass-through and not an
impedance-matching layer — routing belongs to the Go control plane and storage
belongs to the C data plane.

## Regenerating the stubs

`pulsekv_adapters/gen/` is checked in so `pip install ./adapters` works without
`protoc` on the machine. Regenerate inside the v2 dev image, never by hand:

```sh
docker run --rm -v "$PWD:/src" -w /src pulsekv-v2-dev deploy/gen-proto.sh
```

See `proto/README.md` for why the generated imports get rewritten afterwards.
