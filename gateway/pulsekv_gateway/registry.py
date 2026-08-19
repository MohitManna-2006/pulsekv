"""Canonical Context Registry -- STUB. Implemented by Phase 10.1.

Design doc §10 (record shape, immutable versions, mutable current-version
pointer, why storage is an ordinary relational store and not PulseKV itself);
plan §5 (this phase's CRUD surface, invariants and exit criteria).

Storage technology is deliberately undecided here. Plan §5 recommends a real
SQL store from the start over SQLite-then-migrate; that choice, the migrations
directory, and the current-version pointer table are all Phase 10.1's, made
against Phase 10.1's own requirements rather than guessed now.

Two invariants Phase 10.1 must enforce at the storage layer, not merely inherit
from the frozen types:

* **Version immutability.** ``models.CanonicalContextRecord`` refuses in-process
  mutation, but an ``UPDATE`` of a published version's
  ``canonical_text``/``content_hash`` must be rejected by the database path too
  (plan §5). The type cannot enforce what SQL does behind its back.
* **Namespace isolation.** Two namespaces holding an identical
  ``content_hash`` must never see each other's rows. Plan §5 requires this
  proven here, at the storage layer -- design doc §15's claim that a
  cross-tenant match is "structurally impossible" rests on this being true
  below the matcher, not on the matcher passing namespace through correctly.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple

from .models import BlockType, Candidate, CanonicalContextRecord, GatewayError


class RegistryError(GatewayError):
    """Base for every registry failure.

    Plan §5: storage-backend problems surface as a typed, catchable exception
    rather than a bare driver/connection error, so Phase 10.5's fail-open
    wiring is one ``except`` clause (design doc §17).
    """


class RegistryUnavailableError(RegistryError):
    """The backing store could not be reached.

    Design doc §17: every block becomes a miss and the original text is
    forwarded unchanged. Logged, never silently swallowed.
    """


class RegistryVersionImmutableError(RegistryError):
    """An attempt to alter a published version's text, hash, or version."""


class RegistryNotFoundError(RegistryError):
    """A requested ``context_id``/``version`` does not exist."""


class Registry:
    """Durable, versioned, namespace-scoped storage for canonical contexts."""

    def register(self, record: CanonicalContextRecord) -> str:
        """Store a new record and return its ``context_id``."""
        raise NotImplementedError("Phase 10.1")

    def get(
        self, context_id: str, namespace: str, version: Optional[int] = None
    ) -> CanonicalContextRecord:
        """Fetch one version, or the current one when ``version`` is None.

        ``namespace`` is a parameter rather than something the caller filters
        afterwards, so no call site can accidentally read across tenants
        (design doc §15).
        """
        raise NotImplementedError("Phase 10.1")

    def resolve_alias(
        self, text: str, namespace: str
    ) -> Optional[CanonicalContextRecord]:
        """Tier 0's alias path: an exact registered alias string.

        Returns the record rather than plan §5's sketched ``Optional[context_id]``
        -- the caller (Tier 0, plan §6) immediately needs ``canonical_text`` and
        ``version`` to build a ``MatchResult`` and substitute, so returning the
        id alone would make every alias hit a mandatory second round trip.
        """
        raise NotImplementedError("Phase 10.1")

    def by_content_hash(
        self, content_hash: str, namespace: str
    ) -> Optional[CanonicalContextRecord]:
        """Tier 0's exact-hash path (plan §5: storage, not matching logic)."""
        raise NotImplementedError("Phase 10.1")

    def find_candidates(
        self,
        *,
        namespace: str,
        block_type: BlockType,
        top_k: int,
    ) -> Tuple[Candidate, ...]:
        """Tier 2 retrieval. Stubbed in 10.1, real vector search in 10.3.

        ``namespace`` and ``block_type`` are pre-filters on the candidate set,
        not checks applied to a winner afterwards (design doc §11, §15).
        """
        raise NotImplementedError("Phase 10.3")

    def publish_version(
        self, record: CanonicalContextRecord
    ) -> CanonicalContextRecord:
        """Publish a new version and move the current-version pointer.

        Design doc §10: existing decisions logged against an older version stay
        interpretable, so publishing never rewrites the older row.
        """
        raise NotImplementedError("Phase 10.1")

    def deprecate(
        self, context_id: str, namespace: str, version: int, at: datetime
    ) -> CanonicalContextRecord:
        """Stop serving one version as a match target (design doc §17)."""
        raise NotImplementedError("Phase 10.1")

    def close(self) -> None:
        """Release the backing store's resources."""
        raise NotImplementedError("Phase 10.1")
