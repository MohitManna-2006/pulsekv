"""PulseKV v2 LLM serving adapters.

Phase 7 scope: SGLang HiCache external storage backend and generic PulseKV
Client SDK.

Exported classes and utilities:
* :class:`PulseKVClient`: Generic cluster client for routing and transport.
* :class:`PulseKVHiCacheStorage`: SGLang HiCache L3 external storage backend.
* :func:`derive_block_hashes`, :func:`derive_prefix_keys`, :func:`get_hash_str`: SGLang key derivation.
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

__version__ = "0.0.1"

__all__ = [
    "PulseKVClient",
    "PulseKVClientError",
    "TopologySnapshot",
    "PulseKVHiCacheStorage",
    "derive_block_hashes",
    "derive_prefix_keys",
    "format_cache_key",
    "get_hash_str",
    "get_token_bytes",
    "register_sglang_backend",
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
