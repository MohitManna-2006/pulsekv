"""PulseKV v2 LLM serving adapters.

Phase 0 scope: this package is a skeleton plus one health-check gRPC client.
There is deliberately no cache logic here yet.

What lands later:

* Phase 7 — ``pulsekv_adapters.sglang``: an SGLang HiCache external storage
  backend implementing ``get`` / ``exist`` / ``set`` as a thin pass-through
  over :mod:`pulsekv_adapters.gen.adapter_pb2_grpc`.
* Phase 8 — ``pulsekv_adapters.vllm``: a vLLM KVConnector v1 implementation,
  scheduler-side and worker-side.

Both are thin by design. The routing lives in the Go control plane and the
storage lives in the C data plane; an adapter that starts making its own
decisions about either is a bug.
"""

from .health_client import (
    HealthCheckError,
    HealthResult,
    check_adapter_service,
    check_cluster_metadata,
    check_node,
)

__version__ = "0.0.1"

__all__ = [
    "HealthCheckError",
    "HealthResult",
    "check_adapter_service",
    "check_cluster_metadata",
    "check_node",
    "__version__",
]
