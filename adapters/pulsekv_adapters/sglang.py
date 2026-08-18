"""SGLang HiCache external storage backend adapter for PulseKV.

Implements SGLang's external storage backend interface (get / exist / set / batch_*)
as a thin pass-through over the PulseKV generic Client SDK.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Union

from .client import PulseKVClient

logger = logging.getLogger(__name__)

# Try to import SGLang base classes and structures if sglang is available
try:
    from sglang.srt.mem_cache.hicache_storage import (
        HiCacheStorage,
        HiCacheStorageConfig,
        HiCacheStorageExtraInfo,
        PoolHitPolicy,
        PoolName,
        PoolTransfer,
        PoolTransferResult,
    )
    HAS_SGLANG = True
except ImportError:
    HAS_SGLANG = False

    class HiCacheStorage:  # type: ignore[no-redef]
        """Fallback base class when SGLang is not installed."""
        pass

    @dataclass
    class PoolTransferResult:  # type: ignore[no-redef]
        kv_hit_pages: int
        extra_pool_hit_pages: Dict[str, int]

        @classmethod
        def empty(cls) -> "PoolTransferResult":
            return cls(0, {})


class PulseKVHiCacheStorage(HiCacheStorage):
    """PulseKV L3 external storage backend for SGLang HiCache."""

    def __init__(
        self,
        config: Optional[Any] = None,
        control_plane_address: Optional[str] = None,
        client: Optional[PulseKVClient] = None,
        **kwargs: Any,
    ):
        super().__init__()
        self.config = config
        self._registered_pools: Dict[str, Any] = {}

        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            cp_addr = (
                control_plane_address
                or self._extract_cp_addr_from_config(config)
                or os.getenv("PULSEKV_CONTROL_PLANE_ADDRESS", "127.0.0.1:7000")
            )
            self._client = PulseKVClient(control_plane_addresses=cp_addr)
            self._owns_client = True

    @property
    def client(self) -> PulseKVClient:
        return self._client

    def _extract_cp_addr_from_config(self, config: Optional[Any]) -> Optional[str]:
        if config is None:
            return None
        if isinstance(config, dict):
            return config.get("control_plane_address") or config.get("control_plane")
        if hasattr(config, "extra_config") and isinstance(config.extra_config, dict):
            return config.extra_config.get("control_plane_address") or config.extra_config.get("control_plane")
        return None

    def register_mem_pool_host(self, mem_pool_host: Any) -> None:
        """Register the L2 Host KV Cache memory pool."""
        self.mem_pool_host = mem_pool_host

    def register_mem_host_pool_v2(self, host_pool: Any, host_pool_name: str) -> None:
        """Register a host memory pool by pool name (v2 interface)."""
        self._registered_pools[str(host_pool_name)] = host_pool

    # -------------------------------------------------------------------------
    # Core HiCache 3-Method Interface (get, exist/exists, set)
    # -------------------------------------------------------------------------

    def get(
        self,
        key: str,
        target_location: Optional[Any] = None,
        target_sizes: Optional[Any] = None,
    ) -> Optional[Union[bytes, Any]]:
        """Retrieve cached value for key.

        If target_location (e.g. torch.Tensor) is given, copies data into it.
        """
        val_bytes, found = self._client.get(key)
        if not found or val_bytes is None:
            return None

        if target_location is not None:
            try:
                import torch
                if isinstance(target_location, torch.Tensor):
                    # Copy raw bytes into target tensor memory
                    src_tensor = torch.frombuffer(val_bytes, dtype=torch.uint8)
                    target_location.view(-1).copy_(
                        src_tensor[: target_location.numel() * target_location.element_size()]
                    )
                    return target_location
            except Exception as e:
                logger.debug(f"Target tensor copy failed: {e}")

        return val_bytes

    def exist(self, key: str) -> bool:
        """Check if key exists in PulseKV cluster."""
        return self._client.exist(key)

    def exists(self, key: str) -> bool:
        """Alias for exist(key)."""
        return self.exist(key)

    def set(self, key: str, value: Union[bytes, bytearray, memoryview, Any]) -> bool:
        """Store key-value pair in PulseKV cluster."""
        val_bytes: bytes
        try:
            import torch
            if isinstance(value, torch.Tensor):
                val_bytes = value.detach().cpu().contiguous().numpy().tobytes()
            else:
                val_bytes = bytes(value)
        except Exception:
            val_bytes = bytes(value)

        return self._client.set(key, val_bytes)

    # -------------------------------------------------------------------------
    # Batch Operations
    # -------------------------------------------------------------------------

    def batch_exists(self, keys: Sequence[str]) -> List[bool]:
        """Check existence for a sequence of keys."""
        return [self.exist(k) for k in keys]

    def batch_exists_v2(
        self,
        keys: Sequence[str],
        pool_transfers: Optional[List[Any]] = None,
        extra_info: Optional[Any] = None,
    ) -> PoolTransferResult:
        """Check longest continuous prefix of existing keys for HiCache v2 scheduling."""
        kv_hit = 0
        for k in keys:
            if self.exist(k):
                kv_hit += 1
            else:
                break

        extra_hits: Dict[str, int] = {}
        if pool_transfers:
            for pt in pool_transfers:
                pname = str(getattr(pt, "name", "extra"))
                pkeys = getattr(pt, "keys", None)
                if pkeys:
                    p_hit = 0
                    for pk in pkeys:
                        if self.exist(pk):
                            p_hit += 1
                        else:
                            break
                    extra_hits[pname] = p_hit

        return PoolTransferResult(kv_hit_pages=kv_hit, extra_pool_hit_pages=extra_hits)

    def batch_get(self, keys: Sequence[str]) -> List[Optional[Union[bytes, Any]]]:
        """Retrieve values for multiple keys."""
        return [self.get(k) for k in keys]

    def batch_set(self, key_values: Dict[str, Any]) -> List[bool]:
        """Store multiple key-value pairs."""
        return [self.set(k, v) for k, v in key_values.items()]

    def close(self) -> None:
        """Close backend connections."""
        if self._owns_client and self._client:
            self._client.close()

    def __enter__(self) -> "PulseKVHiCacheStorage":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def register_sglang_backend() -> None:
    """Register PulseKV storage backend with SGLang's StorageBackendFactory if present."""
    try:
        from sglang.srt.mem_cache.storage.backend_factory import StorageBackendFactory
        StorageBackendFactory.register_backend(
            "pulsekv", "pulsekv_adapters.sglang", "PulseKVHiCacheStorage"
        )
        logger.info("Registered 'pulsekv' with SGLang StorageBackendFactory")
    except (ImportError, AttributeError):
        pass


# Automatically attempt registration on import
register_sglang_backend()
