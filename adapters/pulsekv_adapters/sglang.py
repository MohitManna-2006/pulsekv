"""SGLang 0.5.15 HiCache external-storage backend for PulseKV."""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence

from .client import PulseKVClient

logger = logging.getLogger(__name__)
_TRACE_LOCK = threading.Lock()
_TRACE_FAILED_PATHS: set[str] = set()


def _trace(
    operation: str,
    *,
    keys: Optional[Sequence[str]] = None,
    result: Optional[Any] = None,
    page_count: Optional[int] = None,
) -> None:
    """Append machine-readable integration evidence when explicitly enabled."""
    path = os.getenv("PULSEKV_SGLANG_TRACE_PATH")
    if not path or path in _TRACE_FAILED_PATHS:
        return
    record = {
        "timestamp": time.time(),
        "pid": os.getpid(),
        "replica": os.getenv("PULSEKV_SGLANG_REPLICA", ""),
        "operation": operation,
        "keys": list(keys or []),
        "batch_size": len(keys or []),
        "result": result,
    }
    if page_count is not None:
        record["page_count"] = page_count
    try:
        with _TRACE_LOCK, open(path, "a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
    except OSError as exc:
        with _TRACE_LOCK:
            first_failure = path not in _TRACE_FAILED_PATHS
            _TRACE_FAILED_PATHS.add(path)
        if first_failure:
            logger.error("Disabling PulseKV SGLang trace %s: %s", path, exc)

try:
    from sglang.srt.mem_cache.hicache_storage import (
        HiCacheStorage,
        HiCacheStorageConfig,
        HiCacheStorageExtraInfo,
    )

    HAS_SGLANG = True
except ImportError:  # pragma: no cover
    HAS_SGLANG = False

    class HiCacheStorage:  # type: ignore[no-redef]
        pass

    HiCacheStorageConfig = Any  # type: ignore[misc,assignment]
    HiCacheStorageExtraInfo = Any  # type: ignore[misc,assignment]

try:
    from sglang.srt.mem_cache.hicache_storage import (
        PoolHitPolicy,
        PoolName,
        PoolTransfer,
        PoolTransferResult,
    )
except ImportError:  # pragma: no cover; all are present in the target release
    class PoolName(str, Enum):  # type: ignore[no-redef]
        KV = "kv"
        MAMBA = "mamba"
        SWA = "swa"
        INDEXER = "indexer"
        DRAFT = "draft"

        def __str__(self) -> str:
            return self.value

    class PoolHitPolicy(str, Enum):  # type: ignore[no-redef]
        ALL_PAGES = "all_pages"
        TRAILING_PAGES = "trailing_pages"

    @dataclass
    class PoolTransfer:  # type: ignore[no-redef]
        name: PoolName
        host_indices: Optional[Any] = None
        device_indices: Optional[Any] = None
        keys: Optional[List[str]] = None
        hit_policy: PoolHitPolicy = PoolHitPolicy.ALL_PAGES
        nodes_to_load: Optional[List[Any]] = None
        indices_from_pool: Optional[PoolName] = None

    @dataclass
    class PoolTransferResult:  # type: ignore[no-redef]
        kv_hit_pages: int
        extra_pool_hit_pages: Dict[str, int]

        @classmethod
        def empty(cls) -> "PoolTransferResult":
            return cls(0, {})

        def update_kv_hit_pages(self, kv_hit_pages: int) -> None:
            self.kv_hit_pages = max(self.kv_hit_pages, kv_hit_pages)

        def update_extra_pool_hit_pages(
            self, results: Dict[str, List[bool]]
        ) -> None:
            self.extra_pool_hit_pages.update(
                {name: sum(entries) for name, entries in results.items()}
            )


class PulseKVHiCacheStorage(HiCacheStorage):
    """PulseKV L3 backend implementing SGLang 0.5.15's HiCache contract."""

    def __init__(self, storage_config: Optional[HiCacheStorageConfig] = None,
                 factory_kwargs: Optional[Dict[str, Any]] = None, *,
                 config: Optional[Any] = None,
                 control_plane_address: Optional[str] = None,
                 client: Optional[PulseKVClient] = None, **kwargs: Any) -> None:
        # The dynamic factory calls backend_class(storage_config, kwargs).
        if storage_config is None:
            storage_config = config
        if factory_kwargs is not None and not isinstance(factory_kwargs, dict):
            raise TypeError("factory_kwargs must be a mapping")
        self.config = storage_config
        self.registered_pools: Dict[Any, Any] = {}
        merged = dict(factory_kwargs or {})
        merged.update(kwargs)
        if client is None:
            client = merged.pop("client", None)
        cp_addr = (control_plane_address or merged.get("control_plane_address")
                   or merged.get("control_plane")
                   or self._extract_cp_addr_from_config(storage_config)
                   or os.getenv("PULSEKV_CONTROL_PLANE_ADDRESS", "127.0.0.1:7000"))
        if not isinstance(cp_addr, str) or not cp_addr.strip():
            raise ValueError("PulseKV control_plane_address must be a non-empty string")
        self.control_plane_address = cp_addr.strip()
        if client is not None:
            self._client, self._owns_client = client, False
        else:
            self._client = PulseKVClient(control_plane_addresses=self.control_plane_address)
            self._owns_client = True

    @staticmethod
    def _extract_cp_addr_from_config(config: Optional[Any]) -> Optional[str]:
        if config is None:
            return None
        source = config if isinstance(config, dict) else getattr(config, "extra_config", None)
        if isinstance(source, dict):
            return source.get("control_plane_address") or source.get("control_plane")
        return None

    @property
    def client(self) -> PulseKVClient:
        return self._client

    def register_mem_pool_host(self, mem_pool_host: Any) -> None:
        self.mem_pool_host = mem_pool_host

    def register_mem_host_pool_v2(self, host_pool: Any, host_pool_name: Any) -> None:
        self.registered_pools[host_pool_name] = host_pool

    @staticmethod
    def _tensor_bytes(value: Any) -> bytes:
        import torch
        return value.detach().contiguous().to("cpu").view(torch.uint8).numpy().tobytes()

    @staticmethod
    def _expected_size(target: Any, target_sizes: Optional[Any]) -> int:
        expected = target.numel() * target.element_size()
        if target_sizes is not None:
            if isinstance(target_sizes, Sequence) and not isinstance(target_sizes, (str, bytes, bytearray)):
                supplied = sum(int(size) for size in target_sizes)
            else:
                supplied = int(target_sizes)
            if supplied != expected:
                raise ValueError(f"target_sizes mismatch: target has {expected} bytes, got {supplied}")
        return expected

    @classmethod
    def _copy_bytes_to_tensor(cls, data: bytes, target: Any,
                              target_sizes: Optional[Any] = None) -> Any:
        import torch
        expected = cls._expected_size(target, target_sizes)
        if len(data) != expected:
            raise ValueError(f"stored value size mismatch: target needs {expected} bytes, got {len(data)}")
        # CPU staging plus tensor copy preserves dtype, shape, device, and strides.
        staging = torch.empty(target.shape, dtype=target.dtype, device="cpu")
        staging.view(torch.uint8).reshape(-1).copy_(torch.frombuffer(bytearray(data), dtype=torch.uint8))
        target.copy_(staging.to(target.device))
        return target

    def get(self, key: str, target_location: Optional[Any] = None,
            target_sizes: Optional[Any] = None) -> Optional[Any]:
        logger.debug("PulseKV HiCache get key=%s target=%s", key, target_location is not None)
        value, found = self._client.get(key)
        _trace("get", keys=[key], result="hit" if found and value is not None else "miss")
        if not found or value is None:
            return None
        return value if target_location is None else self._copy_bytes_to_tensor(value, target_location, target_sizes)

    def exist(self, key: str) -> bool:
        return self._client.exist(key)

    def exists(self, key: str) -> bool:
        logger.debug("PulseKV HiCache exists key=%s", key)
        result = self.exist(key)
        _trace("exists", keys=[key], result=result)
        return result

    def set(self, key: str, value: Optional[Any] = None,
            target_location: Optional[Any] = None,
            target_sizes: Optional[Any] = None) -> bool:
        logger.debug("PulseKV HiCache set key=%s", key)
        source = value if value is not None else target_location
        if source is None:
            return False
        try:
            import torch
            if isinstance(source, torch.Tensor):
                expected = self._expected_size(source, target_sizes)
                payload = self._tensor_bytes(source)
                if len(payload) != expected:
                    raise ValueError("serialized tensor size changed unexpectedly")
            else:
                payload = bytes(source)
                if target_sizes is not None and int(target_sizes) != len(payload):
                    raise ValueError(f"target_sizes mismatch: value has {len(payload)} bytes, got {target_sizes}")
        except ImportError:  # pragma: no cover
            payload = bytes(source)
        result = self._client.set(key, payload)
        _trace("set", keys=[key], result=result)
        return result

    def batch_exists(self, keys: List[str],
                     extra_info: Optional[HiCacheStorageExtraInfo] = None) -> int:
        logger.info("PulseKV HiCache batch_exists count=%d", len(keys))
        for index, key in enumerate(keys):
            if not self.exists(key):
                _trace("batch_exists", keys=keys, result=index, page_count=index)
                return index
        _trace("batch_exists", keys=keys, result=len(keys), page_count=len(keys))
        return len(keys)

    @staticmethod
    def _item_arg(values: Optional[Any], index: int) -> Optional[Any]:
        return None if values is None else values[index]

    def batch_get(self, keys: List[str], target_locations: Optional[Any] = None,
                  target_sizes: Optional[Any] = None) -> List[Optional[Any]]:
        logger.info("PulseKV HiCache batch_get count=%d", len(keys))
        results: List[Optional[Any]] = []
        for index, key in enumerate(keys):
            try:
                results.append(self.get(key, self._item_arg(target_locations, index),
                                        self._item_arg(target_sizes, index)))
            except (TypeError, ValueError, RuntimeError):
                logger.exception("PulseKV batch_get failed for key %s", key)
                results.append(None)
        _trace("batch_get", keys=keys,
               result=[item is not None for item in results],
               page_count=sum(item is not None for item in results))
        return results

    def batch_set(self, keys: List[str], values: Optional[Any] = None,
                  target_locations: Optional[Any] = None,
                  target_sizes: Optional[Any] = None) -> bool:
        logger.info("PulseKV HiCache batch_set count=%d", len(keys))
        for index, key in enumerate(keys):
            try:
                if not self.set(key, self._item_arg(values, index),
                                self._item_arg(target_locations, index),
                                self._item_arg(target_sizes, index)):
                    _trace("batch_set", keys=keys, result=False)
                    return False
            except (TypeError, ValueError, RuntimeError):
                logger.exception("PulseKV batch_set failed for key %s", key)
                _trace("batch_set", keys=keys, result=False)
                return False
        _trace("batch_set", keys=keys, result=True, page_count=len(keys))
        return True

    def _v1_pool(self) -> Any:
        if not hasattr(self, "mem_pool_host"):
            raise RuntimeError("register_mem_pool_host must be called before v1 I/O")
        return self.mem_pool_host

    @staticmethod
    def _validate_indices(keys: Sequence[str], host_indices: Any, pool: Any) -> int:
        page_size = getattr(pool, "page_size", 1) or 1
        expected = len(keys) * page_size
        if host_indices is None or host_indices.numel() != expected:
            actual = 0 if host_indices is None else host_indices.numel()
            raise ValueError(f"host_indices length mismatch: expected {expected}, got {actual}")
        return page_size

    def batch_get_v1(self, keys: List[str], host_indices: Any,
                     extra_info: Optional[HiCacheStorageExtraInfo] = None) -> List[bool]:
        logger.info("PulseKV HiCache batch_get_v1 count=%d", len(keys))
        pool = self._v1_pool()
        try:
            page_size = self._validate_indices(keys, host_indices, pool)
        except ValueError:
            logger.exception("PulseKV v1 get index validation failed")
            return [False] * len(keys)
        results = []
        for index, key in enumerate(keys):
            offset = host_indices[index * page_size].item()
            try:
                loaded = self.get(key, pool.get_dummy_flat_data_page())
                if loaded is not None:
                    pool.set_from_flat_data_page(offset, loaded)
                results.append(loaded is not None)
            except (TypeError, ValueError, RuntimeError):
                logger.exception("PulseKV v1 get failed for key %s", key)
                results.append(False)
        _trace("batch_get_v1", keys=keys, result=results,
               page_count=sum(results))
        return results

    def batch_set_v1(self, keys: List[str], host_indices: Any,
                     extra_info: Optional[HiCacheStorageExtraInfo] = None) -> List[bool]:
        logger.info("PulseKV HiCache batch_set_v1 count=%d", len(keys))
        pool = self._v1_pool()
        try:
            page_size = self._validate_indices(keys, host_indices, pool)
        except ValueError:
            logger.exception("PulseKV v1 set index validation failed")
            return [False] * len(keys)
        results = []
        for index, key in enumerate(keys):
            offset = host_indices[index * page_size].item()
            try:
                results.append(bool(self.set(key, pool.get_data_page(offset, flat=True))))
            except (TypeError, ValueError, RuntimeError):
                logger.exception("PulseKV v1 set failed for key %s", key)
                results.append(False)
        _trace("batch_set_v1", keys=keys, result=results,
               page_count=sum(results))
        return results

    @staticmethod
    def _component_key(pool_name: Any, key: str) -> str:
        return key if pool_name == PoolName.KV else f"{key}.{pool_name}"

    def batch_exists_v2(self, keys: List[str],
                        pool_transfers: Optional[List[PoolTransfer]] = None,
                        extra_info: Optional[HiCacheStorageExtraInfo] = None) -> PoolTransferResult:
        logger.info("PulseKV HiCache batch_exists_v2 count=%d", len(keys))
        kv_pages = self.batch_exists(keys, extra_info)
        hit_count: Dict[str, int] = {PoolName.KV: kv_pages} if kv_pages else {}
        final_pages = kv_pages
        for transfer in pool_transfers or []:
            if final_pages == 0:
                break
            name = transfer.name
            if transfer.hit_policy == PoolHitPolicy.ALL_PAGES:
                boundary = next((i for i in range(kv_pages)
                                 if not self.exists(self._component_key(name, keys[i]))), kv_pages)
            else:
                trailing = max(1, len(transfer.keys) if transfer.keys else 1)
                boundary = 0
                for prefix_len in range(kv_pages, 0, -1):
                    if all(self.exists(self._component_key(name, keys[i]))
                           for i in range(max(0, prefix_len - trailing), prefix_len)):
                        boundary = prefix_len
                        break
            if boundary:
                hit_count[name] = boundary
            final_pages = min(final_pages, boundary)
        _trace("batch_exists_v2", keys=keys, result=hit_count,
               page_count=final_pages)
        return PoolTransferResult(final_pages, hit_count)

    def _read_page(self, pool_name: Any, key: str, pool: Any, offset: int) -> bool:
        loaded = self.get(self._component_key(pool_name, key), pool.get_dummy_flat_data_page())
        if loaded is None:
            return False
        pool.set_from_flat_data_page(offset, loaded)
        return True

    def _write_page(self, pool_name: Any, key: str, pool: Any, offset: int) -> bool:
        return self.set(self._component_key(pool_name, key), pool.get_data_page(offset, flat=True))

    def _batch_io_v2(self, transfers: List[PoolTransfer], operation: Any) -> Dict[str, List[bool]]:
        results: Dict[str, List[bool]] = {}
        for transfer in transfers:
            keys = transfer.keys or []
            pool = self.registered_pools.get(transfer.name)
            if pool is None:
                results[transfer.name] = [False] * len(keys)
                continue
            try:
                page_size = self._validate_indices(keys, transfer.host_indices, pool)
            except ValueError:
                logger.exception("PulseKV v2 index validation failed for pool %s", transfer.name)
                results[transfer.name] = [False] * len(keys)
                continue
            entries = []
            for index, key in enumerate(keys):
                offset = transfer.host_indices[index * page_size].item()
                try:
                    entries.append(bool(operation(transfer.name, key, pool, offset)))
                except (TypeError, ValueError, RuntimeError):
                    logger.exception("PulseKV v2 I/O failed for key %s", key)
                    entries.append(False)
            results[transfer.name] = entries
        return results

    def batch_get_v2(self, transfers: List[PoolTransfer],
                     extra_info: Optional[HiCacheStorageExtraInfo] = None) -> Dict[str, List[bool]]:
        logger.info("PulseKV HiCache batch_get_v2 pools=%d", len(transfers))
        results = self._batch_io_v2(transfers, self._read_page)
        _trace("batch_get_v2", keys=[key for transfer in transfers for key in (transfer.keys or [])],
               result={str(name): values for name, values in results.items()},
               page_count=sum(sum(values) for values in results.values()))
        return results

    def batch_set_v2(self, transfers: List[PoolTransfer],
                     extra_info: Optional[HiCacheStorageExtraInfo] = None) -> Dict[str, List[bool]]:
        logger.info("PulseKV HiCache batch_set_v2 pools=%d", len(transfers))
        results = self._batch_io_v2(transfers, self._write_page)
        _trace("batch_set_v2", keys=[key for transfer in transfers for key in (transfer.keys or [])],
               result={str(name): values for name, values in results.items()},
               page_count=sum(sum(values) for values in results.values()))
        return results

    def close(self) -> None:
        if self._owns_client and self._client:
            self._client.close()

    def __enter__(self) -> "PulseKVHiCacheStorage":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def register_sglang_backend() -> bool:
    """Compatibility shim; 0.5.15 must use its ``dynamic`` backend path."""
    return HAS_SGLANG
