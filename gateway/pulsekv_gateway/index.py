"""Vector candidate retrieval -- STUB. Implemented by Phase 10.3.

Design doc §11 Tier 2, §15 (namespace pre-filter); plan §7.

No index technology is chosen here, and design doc §10 argues one may not be
needed at MVP scale: a curated registry is estimated at low hundreds to low
thousands of entries, and brute-force cosine over a few thousand vectors is
sub-millisecond on any modern CPU with no new dependency. That estimate is
explicitly labelled an estimate; Phase 10.3 measures against Phase 10.1's
actual registry before justifying anything more complex.

The one non-negotiable property: ``namespace`` and ``block_type`` filter the
candidate set *before* similarity ranking. Design doc §15 rests its
"structurally impossible" cross-tenant claim on that ordering -- a
post-ranking filter would make cross-tenant safety a probability instead.
"""

from __future__ import annotations

from typing import Sequence, Tuple

from .models import BlockType, Candidate, CanonicalContextRecord


class VectorIndex:
    """Namespace-scoped nearest-neighbour retrieval over registered records."""

    def add(self, record: CanonicalContextRecord, embedding: Sequence[float]) -> None:
        """Index one published record's vector."""
        raise NotImplementedError("Phase 10.3")

    def remove(self, context_id: str, namespace: str, version: int) -> None:
        """Drop a deprecated version so it stops being retrievable."""
        raise NotImplementedError("Phase 10.3")

    def find_candidates(
        self,
        embedding: Sequence[float],
        *,
        namespace: str,
        block_type: BlockType,
        top_k: int,
    ) -> Tuple[Candidate, ...]:
        """Return the top-K candidates within the namespace and block type.

        Ordered by descending similarity. Returning an empty tuple is an
        ordinary outcome (``MatchOutcome.NO_CANDIDATE``), not an error.
        """
        raise NotImplementedError("Phase 10.3")
