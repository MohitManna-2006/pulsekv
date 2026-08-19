"""Decision log -- STUB. Implemented by Phase 10.2.

Design doc §21 (the record that answers "what did the gateway actually send to
the model for this request" without re-deriving it from scattered logs), §20
(privacy: hashes, not prompt text); plan §6.

Delivered in Phase 10.2 alongside the deterministic tiers, deliberately not
deferred to hardening: a decision that was never recorded cannot be audited
retroactively, and design doc §21 makes this a Phase 10.2 deliverable for that
reason.

``models.DecisionLogRecord`` has no field capable of holding prompt text, so
the privacy default is structural rather than a rule this module has to
remember. The correlation requirement is design doc §18's: ``request_id`` must
be enough to join these records against Phase 9's existing
``pulsekv_cache_hits_total``/``pulsekv_cache_misses_total`` series -- a join
across two existing observability surfaces, not a new metric PulseKV exposes.
"""

from __future__ import annotations

from typing import Iterable

from .models import DecisionLogRecord


class AuditLog:
    """Sink for per-block gateway decisions."""

    def record(self, decision: DecisionLogRecord) -> None:
        """Append one decision.

        Must not raise into the request path: a failing audit sink degrades
        observability, and design doc §17 has no failure mode in which the
        gateway's own bookkeeping is allowed to break the traffic it is
        observing.
        """
        raise NotImplementedError("Phase 10.2")

    def record_many(self, decisions: Iterable[DecisionLogRecord]) -> None:
        """Append every decision for one request."""
        raise NotImplementedError("Phase 10.2")

    def close(self) -> None:
        """Flush and release the sink."""
        raise NotImplementedError("Phase 10.2")
