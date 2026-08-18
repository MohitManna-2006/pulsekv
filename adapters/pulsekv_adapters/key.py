"""SGLang HiCache block key derivation scheme.

Implements token sequence block hashing matching SGLang's RadixCache and
HiCache storage conventions (SHA-256 chained page hashing).
"""

from __future__ import annotations

import hashlib
from typing import Iterable, List, Optional, Sequence, Union


def get_token_bytes(token: Union[int, tuple[int, ...]]) -> bytes:
    """Convert a token ID or bigram tuple to canonical 4-byte big-endian bytes."""
    if isinstance(token, int):
        return int(token).to_bytes(4, byteorder="big", signed=True)
    elif isinstance(token, (tuple, list)):
        return b"".join(int(t).to_bytes(4, byteorder="big", signed=True) for t in token)
    elif isinstance(token, bytes):
        return token
    else:
        raise TypeError(f"Unsupported token type: {type(token)}")


def get_hash_str(
    tokens: Sequence[Union[int, tuple[int, ...]]],
    prior_hash: Optional[str] = None,
) -> str:
    """Compute SHA-256 hash string for a token page/block.

    This matches SGLang's ``sglang.srt.mem_cache.utils.get_hash_str`` /
    ``RadixKey.hash_page``.
    """
    hasher = hashlib.sha256()
    if prior_hash:
        hasher.update(prior_hash.encode("utf-8") if isinstance(prior_hash, str) else prior_hash)
    for token in tokens:
        hasher.update(get_token_bytes(token))
    return hasher.hexdigest()


def derive_block_hashes(
    token_ids: Sequence[int],
    page_size: int = 16,
    initial_prior_hash: Optional[str] = None,
) -> List[str]:
    """Derive chained SHA-256 block hashes for a full sequence of token IDs.

    Args:
        token_ids: Sequence of token IDs.
        page_size: Number of tokens per page/block (e.g. 16, 64, 128).
        initial_prior_hash: Optional prior hash for the first block.

    Returns:
        List of hexadecimal hash strings, one per complete block.
    """
    num_blocks = len(token_ids) // page_size
    hashes: List[str] = []
    prior = initial_prior_hash

    for i in range(num_blocks):
        block_tokens = token_ids[i * page_size : (i + 1) * page_size]
        h = get_hash_str(block_tokens, prior_hash=prior)
        hashes.append(h)
        prior = h

    return hashes


def format_cache_key(
    block_hash: str,
    pool_name: str = "kv",
    model_name: Optional[str] = None,
) -> str:
    """Format a storage key for PulseKV given a block hash and pool name.

    Default format follows SGLang HiCache storage key convention: ``{block_hash}.{pool_name}``
    or with model namespace ``sglang:{model_name}:{block_hash}.{pool_name}``.
    """
    base_key = f"{block_hash}.{pool_name}" if pool_name else block_hash
    if model_name:
        return f"sglang:{model_name}:{base_key}"
    return base_key


def derive_prefix_keys(
    token_ids: Sequence[int],
    page_size: int = 16,
    pool_name: str = "kv",
    model_name: Optional[str] = None,
    initial_prior_hash: Optional[str] = None,
) -> List[str]:
    """Derive full PulseKV storage keys for a sequence of token IDs."""
    hashes = derive_block_hashes(
        token_ids, page_size=page_size, initial_prior_hash=initial_prior_hash
    )
    return [format_cache_key(h, pool_name=pool_name, model_name=model_name) for h in hashes]
