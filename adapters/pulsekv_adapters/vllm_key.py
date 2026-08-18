"""vLLM KVConnector block and layer-aware key derivation scheme.

Implements token sequence block hashing and key formatting for vLLM KV transfer,
supporting multi-layer attention states and model-scoped key namespaces.
"""

from __future__ import annotations

import hashlib
import re
from typing import List, Optional, Sequence, Tuple, Union


def get_token_bytes(token: Union[int, tuple[int, ...]]) -> bytes:
    """Convert a token ID or tuple of token IDs to canonical 4-byte big-endian bytes."""
    if isinstance(token, int):
        return int(token).to_bytes(4, byteorder="big", signed=True)
    elif isinstance(token, (tuple, list)):
        return b"".join(int(t).to_bytes(4, byteorder="big", signed=True) for t in token)
    elif isinstance(token, bytes):
        return token
    else:
        raise TypeError(f"Unsupported token type: {type(token)}")


def get_block_hash_str(
    tokens: Sequence[Union[int, tuple[int, ...]]],
    prior_hash: Optional[str] = None,
) -> str:
    """Compute SHA-256 hash string for a vLLM token block."""
    hasher = hashlib.sha256()
    if prior_hash:
        hasher.update(prior_hash.encode("utf-8") if isinstance(prior_hash, str) else prior_hash)
    for token in tokens:
        hasher.update(get_token_bytes(token))
    return hasher.hexdigest()


def derive_vllm_block_hashes(
    token_ids: Sequence[int],
    block_size: int = 16,
    initial_prior_hash: Optional[str] = None,
) -> List[str]:
    """Derive chained SHA-256 block hashes for a sequence of token IDs.

    Args:
        token_ids: Sequence of token IDs in the prompt/request.
        block_size: Number of tokens per vLLM KV block (default: 16).
        initial_prior_hash: Optional prior hash for the first block.

    Returns:
        List of hexadecimal hash strings, one per complete block of `block_size` tokens.
    """
    num_blocks = len(token_ids) // block_size
    hashes: List[str] = []
    prior = initial_prior_hash

    for i in range(num_blocks):
        block_tokens = token_ids[i * block_size : (i + 1) * block_size]
        h = get_block_hash_str(block_tokens, prior_hash=prior)
        hashes.append(h)
        prior = h

    return hashes


def format_vllm_kv_key(
    block_hash: str,
    layer_id: int,
    model_name: Optional[str] = None,
    tag: Optional[str] = None,
) -> str:
    """Format a storage key for PulseKV given a block hash and layer index.

    Key structure:
    - Default: ``vllm:layer_{layer_id}:{block_hash}``
    - With model: ``vllm:{model_name}:layer_{layer_id}:{block_hash}``
    - With tag (e.g. k/v split): ``vllm:{model_name}:layer_{layer_id}:{block_hash}:{tag}``
    """
    parts = ["vllm"]
    if model_name:
        parts.append(str(model_name))
    parts.append(f"layer_{layer_id}")
    parts.append(str(block_hash))
    if tag:
        parts.append(str(tag))
    return ":".join(parts)


def parse_vllm_kv_key(key: str) -> Optional[dict]:
    """Parse a formatted vLLM KV key back into its component parts.

    Returns a dict with keys ``{"model_name", "layer_id", "block_hash", "tag"}``
    or None if the format does not match.
    """
    parts = key.split(":")
    if len(parts) < 3 or parts[0] != "vllm":
        return None

    # Determine if model_name is present
    if parts[1].startswith("layer_"):
        # No model_name: vllm:layer_X:hash[:tag]
        model_name = None
        layer_part = parts[1]
        block_hash = parts[2]
        tag = parts[3] if len(parts) > 3 else None
    else:
        # With model_name: vllm:model:layer_X:hash[:tag]
        model_name = parts[1]
        layer_part = parts[2] if len(parts) > 2 else ""
        block_hash = parts[3] if len(parts) > 3 else ""
        tag = parts[4] if len(parts) > 4 else None

    match = re.match(r"^layer_(\d+)$", layer_part)
    if not match:
        return None
    layer_id = int(match.group(1))

    return {
        "model_name": model_name,
        "layer_id": layer_id,
        "block_hash": block_hash,
        "tag": tag,
    }


def derive_vllm_layer_keys(
    token_ids_or_hashes: Union[Sequence[int], Sequence[str]],
    layer_id: int,
    block_size: int = 16,
    model_name: Optional[str] = None,
    tag: Optional[str] = None,
) -> List[str]:
    """Derive full PulseKV storage keys for a specific layer.

    Accepts either a sequence of raw token IDs (which are hashed first) or
    pre-computed block hash strings.
    """
    if not token_ids_or_hashes:
        return []

    if isinstance(token_ids_or_hashes[0], str):
        hashes = list(token_ids_or_hashes)  # type: ignore[arg-type]
    else:
        hashes = derive_vllm_block_hashes(
            token_ids_or_hashes, block_size=block_size  # type: ignore[arg-type]
        )

    return [
        format_vllm_kv_key(h, layer_id=layer_id, model_name=model_name, tag=tag)
        for h in hashes
    ]
