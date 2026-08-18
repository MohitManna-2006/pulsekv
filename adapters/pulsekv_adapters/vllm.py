"""vLLM KVConnector v1 external storage adapter for PulseKV.

Implements vLLM's KVConnectorBase_v1 interface (scheduler-side token matching,
worker-side per-layer KV save/load, and request lifecycle management) over the
PulseKV generic Client SDK and bulk transport layer.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from .client import PulseKVClient
from .vllm_key import (
    derive_vllm_block_hashes,
    derive_vllm_layer_keys,
    format_vllm_kv_key,
)

logger = logging.getLogger(__name__)

# Try to import vLLM base classes if vLLM is installed
try:
    from vllm.distributed.kv_transfer.kv_connector.v1 import (  # type: ignore[import-not-found]
        KVConnectorBase_v1,
    )
    HAS_VLLM = True
except ImportError:
    HAS_VLLM = False

    class KVConnectorBase_v1:  # type: ignore[no-redef]
        """Fallback base class when vLLM is not installed."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass


@dataclass
class ActiveRequestState:
    """State tracked for an in-flight inference request."""

    request_id: str
    prompt_token_ids: List[int]
    block_hashes: List[str]
    matched_blocks: int
    matched_tokens: int
    saved_layers: Dict[int, bool] = field(default_factory=dict)


class PulseKVKVConnector(KVConnectorBase_v1):
    """PulseKV KVConnector adapter for vLLM (KVConnectorBase_v1).

    Supports:
    * Scheduler-side prefix matching via ``get_num_new_matched_tokens``.
    * Worker-side layer KV caching via ``save_kv_layer`` and ``load_kv_layer``.
    * Zero-copy bulk transport / memfd fast paths via ``PulseKVClient``.
    * Request lifecycle coordination via ``request_finished``.
    """

    def __init__(
        self,
        config: Optional[Any] = None,
        control_plane_address: Optional[str] = None,
        client: Optional[PulseKVClient] = None,
        model_name: Optional[str] = None,
        block_size: int = 16,
        rank: int = 0,
        num_layers: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(config=config, **kwargs)
        self.config = config
        self._block_size = block_size
        self._model_name = model_name or self._extract_model_name(config)
        self._rank = rank
        self._num_layers = num_layers

        self._lock = threading.RLock()
        self._active_requests: Dict[str, ActiveRequestState] = {}

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
        """Return the underlying PulseKV client."""
        return self._client

    @property
    def block_size(self) -> int:
        """Token block size (e.g. 16 or 32 tokens)."""
        return self._block_size

    @property
    def model_name(self) -> Optional[str]:
        """Configured model namespace identifier."""
        return self._model_name

    @property
    def rank(self) -> int:
        """Worker rank."""
        return self._rank

    def _extract_cp_addr_from_config(self, config: Optional[Any]) -> Optional[str]:
        if config is None:
            return None
        if isinstance(config, dict):
            return config.get("control_plane_address") or config.get("control_plane")
        if hasattr(config, "extra_config") and isinstance(config.extra_config, dict):
            return config.extra_config.get("control_plane_address") or config.extra_config.get(
                "control_plane"
            )
        if hasattr(config, "kv_connector_config") and isinstance(
            config.kv_connector_config, dict
        ):
            return config.kv_connector_config.get("control_plane_address")
        return None

    def _extract_model_name(self, config: Optional[Any]) -> Optional[str]:
        if config is None:
            return None
        if isinstance(config, dict):
            return config.get("model_name") or config.get("model")
        if hasattr(config, "model_name"):
            return getattr(config, "model_name")
        if hasattr(config, "model"):
            return str(getattr(config, "model"))
        return None

    # -------------------------------------------------------------------------
    # Scheduler-Side Interface
    # -------------------------------------------------------------------------

    def get_num_new_matched_tokens(
        self,
        request_id: str,
        prompt_token_ids: Sequence[int],
        **kwargs: Any,
    ) -> int:
        """Query PulseKV cluster to determine how many prefix tokens are already cached.

        Computes chained block hashes for the prompt tokens, checks existence
        in the cluster starting from block 0, and returns ``matched_blocks * block_size``.
        """
        if not prompt_token_ids:
            return 0

        block_hashes = derive_vllm_block_hashes(
            prompt_token_ids, block_size=self._block_size
        )
        if not block_hashes:
            return 0

        matched_blocks = 0
        # Check consecutive prefix blocks
        for block_hash in block_hashes:
            # Probe layer 0 (or all layers if known)
            probe_key = format_vllm_kv_key(
                block_hash=block_hash,
                layer_id=0,
                model_name=self._model_name,
            )
            if not self._client.exist(probe_key):
                break
            matched_blocks += 1

        matched_tokens = matched_blocks * self._block_size

        with self._lock:
            self._active_requests[request_id] = ActiveRequestState(
                request_id=request_id,
                prompt_token_ids=list(prompt_token_ids),
                block_hashes=block_hashes,
                matched_blocks=matched_blocks,
                matched_tokens=matched_tokens,
            )

        return matched_tokens

    def get_matched_block_hashes(
        self,
        prompt_token_ids: Sequence[int],
    ) -> List[str]:
        """Return the list of matched block hashes for a given token sequence."""
        block_hashes = derive_vllm_block_hashes(
            prompt_token_ids, block_size=self._block_size
        )
        matched = []
        for bh in block_hashes:
            probe_key = format_vllm_kv_key(
                block_hash=bh,
                layer_id=0,
                model_name=self._model_name,
            )
            if not self._client.exist(probe_key):
                break
            matched.append(bh)
        return matched

    def request_finished(self, request_id: str, **kwargs: Any) -> None:
        """Handle request completion and clean up request tracking state."""
        with self._lock:
            self._active_requests.pop(request_id, None)

    # -------------------------------------------------------------------------
    # Worker-Side Interface (Per-Layer KV Transfer)
    # -------------------------------------------------------------------------

    def save_kv_layer(
        self,
        layer_id: int,
        block_hashes: Sequence[str],
        kv_data: Union[bytes, bytearray, memoryview, Any],
        **kwargs: Any,
    ) -> bool:
        """Save KV cache state for a single transformer layer.

        Args:
            layer_id: Integer index of the transformer layer.
            block_hashes: Sequence of block hash strings for this request/block range.
            kv_data: Serialized tensor bytes, or a PyTorch tensor containing KV data.
        """
        if not block_hashes:
            return True

        val_bytes = self._serialize_tensor_data(kv_data)

        if len(block_hashes) == 1:
            key = format_vllm_kv_key(
                block_hash=block_hashes[0],
                layer_id=layer_id,
                model_name=self._model_name,
            )
            return self._client.set(key, val_bytes)

        # Multi-block slice saving: if single byte buffer spans multiple blocks, chunk equally
        total_len = len(val_bytes)
        num_blocks = len(block_hashes)
        bytes_per_block = total_len // num_blocks

        all_ok = True
        for i, bh in enumerate(block_hashes):
            key = format_vllm_kv_key(
                block_hash=bh,
                layer_id=layer_id,
                model_name=self._model_name,
            )
            lo = i * bytes_per_block
            hi = lo + bytes_per_block if i < num_blocks - 1 else total_len
            block_bytes = val_bytes[lo:hi]
            if not self._client.set(key, block_bytes):
                all_ok = False

        return all_ok

    def load_kv_layer(
        self,
        layer_id: int,
        block_hashes: Sequence[str],
        target_location: Optional[Any] = None,
        **kwargs: Any,
    ) -> Optional[Union[bytes, Any]]:
        """Load KV cache state for a single transformer layer.

        Args:
            layer_id: Integer index of the transformer layer.
            block_hashes: Sequence of block hash strings to load.
            target_location: Optional pre-allocated PyTorch tensor or buffer to populate.

        Returns:
            Combined bytes or populated target tensor; None if any block is missing.
        """
        if not block_hashes:
            return b""

        loaded_chunks: List[bytes] = []
        for bh in block_hashes:
            key = format_vllm_kv_key(
                block_hash=bh,
                layer_id=layer_id,
                model_name=self._model_name,
            )
            val_bytes, found = self._client.get(key)
            if not found or val_bytes is None:
                return None
            loaded_chunks.append(val_bytes)

        combined_bytes = b"".join(loaded_chunks)

        if target_location is not None:
            self._copy_into_target(combined_bytes, target_location)
            return target_location

        return combined_bytes

    def save_kv_block(
        self,
        layer_id: int,
        block_hash: str,
        data: Union[bytes, bytearray, memoryview, Any],
    ) -> bool:
        """Store a single block for a given layer."""
        key = format_vllm_kv_key(
            block_hash=block_hash,
            layer_id=layer_id,
            model_name=self._model_name,
        )
        val_bytes = self._serialize_tensor_data(data)
        return self._client.set(key, val_bytes)

    def load_kv_block(
        self,
        layer_id: int,
        block_hash: str,
        target_location: Optional[Any] = None,
    ) -> Optional[Union[bytes, Any]]:
        """Load a single block for a given layer."""
        key = format_vllm_kv_key(
            block_hash=block_hash,
            layer_id=layer_id,
            model_name=self._model_name,
        )
        val_bytes, found = self._client.get(key)
        if not found or val_bytes is None:
            return None

        if target_location is not None:
            self._copy_into_target(val_bytes, target_location)
            return target_location

        return val_bytes

    # -------------------------------------------------------------------------
    # Helper Utilities (Tensor Serialization / Deserialization)
    # -------------------------------------------------------------------------

    def _serialize_tensor_data(self, data: Any) -> bytes:
        """Convert PyTorch tensor, numpy array, or raw buffer into bytes."""
        try:
            import torch
            if isinstance(data, torch.Tensor):
                return data.detach().cpu().contiguous().numpy().tobytes()
        except Exception:
            pass

        try:
            import numpy as np
            if isinstance(data, np.ndarray):
                return data.tobytes()
        except Exception:
            pass

        return bytes(data)

    def _copy_into_target(self, src_bytes: bytes, target_location: Any) -> None:
        """Copy raw bytes into a pre-allocated target PyTorch tensor or buffer."""
        try:
            import torch
            if isinstance(target_location, torch.Tensor):
                src_tensor = torch.frombuffer(src_bytes, dtype=torch.uint8)
                target_location.view(-1).copy_(
                    src_tensor[: target_location.numel() * target_location.element_size()]
                )
                return
        except Exception as e:
            logger.debug(f"Direct PyTorch tensor memory copy failed: {e}")

        try:
            if isinstance(target_location, (bytearray, memoryview)):
                n = min(len(target_location), len(src_bytes))
                target_location[:n] = src_bytes[:n]
        except Exception as e:
            logger.debug(f"Buffer memory copy failed: {e}")

    def close(self) -> None:
        """Close underlying client and clear request state."""
        with self._lock:
            self._active_requests.clear()
            if self._owns_client:
                self._client.close()

    def __enter__(self) -> "PulseKVKVConnector":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def register_vllm_connector() -> bool:
    """Register PulseKV with vLLM's KVConnector factory if available."""
    try:
        from vllm.distributed.kv_transfer.kv_connector.factory import (  # type: ignore[import-not-found]
            KVConnectorFactory,
        )
        # Register if factory registration API is exposed
        if hasattr(KVConnectorFactory, "register_connector"):
            KVConnectorFactory.register_connector("pulsekv", PulseKVKVConnector)
            logger.info("Successfully registered PulseKV with vLLM KVConnectorFactory")
            return True
    except Exception as e:
        logger.debug(f"vLLM KVConnectorFactory registration skipped or unsupported: {e}")
    return False
