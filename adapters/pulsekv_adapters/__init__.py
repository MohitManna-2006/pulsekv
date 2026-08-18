"""PulseKV v2 LLM serving adapters.

Provides external storage backends for SGLang HiCache (Phase 7),
vLLM KVConnector v1 (Phase 8), and the generic PulseKV Client SDK.

Exported classes and utilities:
* :class:`PulseKVClient`: Generic cluster client for routing and transport.
* :class:`PulseKVHiCacheStorage`: SGLang HiCache L3 external storage backend.
* :class:`PulseKVKVConnector`: vLLM KVConnector v1 adapter.
* :func:`derive_block_hashes`, :func:`derive_prefix_keys`, :func:`get_hash_str`: SGLang key derivation.
* :func:`derive_vllm_block_hashes`, :func:`derive_vllm_layer_keys`, :func:`format_vllm_kv_key`: vLLM key derivation.
"""

from .client import (
    PulseKVClient,
    PulseKVClientError,
    TopologySnapshot,
    fnv1a_64,
    mix64,
    shard_for_key,
)
from .health_client import (
    HealthCheckError,
    HealthResult,
    check_adapter_service,
    check_cluster_metadata,
    check_node,
)
from .key import (
    derive_block_hashes,
    derive_prefix_keys,
    format_cache_key,
    get_hash_str,
    get_token_bytes,
)
from .sglang import (
    PulseKVHiCacheStorage,
    register_sglang_backend,
)
from .vllm import (
    PulseKVKVConnector,
    register_vllm_connector,
)
from .vllm_key import (
    derive_vllm_block_hashes,
    derive_vllm_layer_keys,
    format_vllm_kv_key,
    get_block_hash_str,
    parse_vllm_kv_key,
)

__version__ = "0.0.1"

__all__ = [
    "PulseKVClient",
    "PulseKVClientError",
    "TopologySnapshot",
    "PulseKVHiCacheStorage",
    "PulseKVKVConnector",
    "derive_block_hashes",
    "derive_prefix_keys",
    "format_cache_key",
    "get_hash_str",
    "get_token_bytes",
    "register_sglang_backend",
    "register_vllm_connector",
    "derive_vllm_block_hashes",
    "derive_vllm_layer_keys",
    "format_vllm_kv_key",
    "get_block_hash_str",
    "parse_vllm_kv_key",
    "HealthCheckError",
    "HealthResult",
    "check_adapter_service",
    "check_cluster_metadata",
    "check_node",
    "fnv1a_64",
    "mix64",
    "shard_for_key",
    "__version__",
]

