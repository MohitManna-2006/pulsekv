"""Embedding encoder -- STUB. Implemented by Phase 10.3.

Design doc §16 (embedding model identity), §11 Tier 2; plan §7.

The model is deliberately unchosen here. Plan §1 fixes only that it is CPU-only
(so the MVP needs no GPU dependency the rest of v2 does not have either) and
that the choice is made against real data in Phase 10.3, not assumed now.

Whatever is chosen, ``model_id`` and ``model_version`` must be reported
truthfully and stamped onto every record embedded with it: design doc §16 makes
a vector produced by model A an invalid comparison against model B's, and the
registry record carries both fields so that mismatch is detectable rather than
silent.
"""

from __future__ import annotations

from typing import Sequence

from .models import GatewayError


class EncoderError(GatewayError):
    """Base for encoder failures."""


class EncoderUnavailableError(EncoderError):
    """The encoder is unavailable or exceeded its latency budget.

    Design doc §17: tiers 2/3 are skipped for that request, anything already
    resolved by tiers 0/1 still applies, and everything else passes through
    unchanged.
    """


class Encoder:
    """Turns block text into a vector for Tier 2 retrieval."""

    @property
    def model_id(self) -> str:
        """Stamped onto every record this encoder embeds (design doc §16)."""
        raise NotImplementedError("Phase 10.3")

    @property
    def model_version(self) -> str:
        """Stamped alongside ``model_id``; the two are meaningless apart."""
        raise NotImplementedError("Phase 10.3")

    def encode(self, text: str) -> Sequence[float]:
        """Encode one block. Raises ``EncoderUnavailableError`` past budget."""
        raise NotImplementedError("Phase 10.3")
