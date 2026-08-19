"""Text normalization -- STUB. Implemented by Phase 10.2.

Design doc §11 Tier 0 ("normalize whitespace/casing deterministically before
hashing, never normalize meaning") and Tier 1 (structural re-serialization for
block types with real structure); plan §6.

The two functions here are different in kind, and the difference is the whole
correctness argument for Tier 1:

* ``normalize_for_hash`` removes incidental *rendering* differences before the
  exact hash is taken. It must never remove anything a reader could call
  meaning -- notably not punctuation or negation.
* ``normalize_structural`` parses and re-serializes a structured block (a tool
  schema's JSON) into a canonical key order and spacing. It changes zero
  semantic content, only serialization form, which is why design doc §11 rates
  it the strongest guarantee after exact match.

Neither is a canonicalization *decision*: both are deterministic, reversible-in-
principle text transforms feeding Tier 0's hash.
"""

from __future__ import annotations

from .models import BlockType


def normalize_for_hash(text: str) -> str:
    """Deterministic pre-hash normalization (Tier 0)."""
    raise NotImplementedError("Phase 10.2")


def supports_structural(block_type: BlockType) -> bool:
    """Whether Tier 1 has a canonical serialization for this block type."""
    raise NotImplementedError("Phase 10.2")


def normalize_structural(text: str, block_type: BlockType) -> str:
    """Canonical re-serialization for a structured block type (Tier 1).

    Raises rather than guessing when ``text`` does not parse as the structure
    its ``block_type`` claims: a block that fails to parse falls through to the
    ordinary path, it does not get a best-effort rewrite.
    """
    raise NotImplementedError("Phase 10.2")
