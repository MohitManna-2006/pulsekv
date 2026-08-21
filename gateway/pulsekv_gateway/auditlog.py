"""Decision log (Phase 10.2).

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

Why nothing here raises
-----------------------
The stub this replaces stated the rule and this module keeps it: "a failing
audit sink degrades observability, and design doc §17 has no failure mode in
which the gateway's own bookkeeping is allowed to break the traffic it is
observing." So ``record`` swallows every sink failure -- but it *counts* them.
A silent swallow would trade one blind spot for another; ``dropped`` and
``last_error`` make a broken sink visible to the operator without making it
visible to the request. Phase 10.5 surfaces ``dropped`` in ``/healthz``; §18
has no Prometheus label for an audit-sink failure today, so the gateway does
not invent one.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Iterable, List, Optional, TextIO, Tuple

from .models import DecisionLogRecord

__all__ = ["AuditLog", "InMemoryAuditLog", "JsonlAuditLog"]


class AuditLog:
    """Sink for per-block gateway decisions.

    Base class carrying the never-raise guarantee. Subclasses implement
    ``_emit`` and may raise from it freely; this class is what turns a raised
    sink error into a counted drop.
    """

    def __init__(self) -> None:
        self._dropped = 0
        self._last_error: Optional[str] = None
        self._lock = threading.Lock()

    def record(self, decision: DecisionLogRecord) -> None:
        """Append one decision.

        Must not raise into the request path: a failing audit sink degrades
        observability, and design doc §17 has no failure mode in which the
        gateway's own bookkeeping is allowed to break the traffic it is
        observing.
        """
        try:
            self._emit(decision)
        except Exception as exc:  # noqa: BLE001 -- the whole point of this class
            with self._lock:
                self._dropped += 1
                self._last_error = f"{type(exc).__name__}: {exc}"

    def record_many(self, decisions: Iterable[DecisionLogRecord]) -> None:
        """Append every decision for one request."""
        for decision in decisions:
            self.record(decision)

    def close(self) -> None:
        """Flush and release the sink."""

    @property
    def dropped(self) -> int:
        """How many decisions this sink failed to write.

        Non-zero means the audit trail has holes, which design doc §21's
        "always answerable" property depends on not having. Traffic is
        unaffected by construction.
        """
        with self._lock:
            return self._dropped

    @property
    def last_error(self) -> Optional[str]:
        """The most recent sink failure, or None if there has been none."""
        with self._lock:
            return self._last_error

    def _emit(self, decision: DecisionLogRecord) -> None:
        raise NotImplementedError("use a concrete AuditLog subclass")

    def __enter__(self) -> "AuditLog":
        return self

    def __exit__(self, *_exc_info) -> None:
        self.close()


class InMemoryAuditLog(AuditLog):
    """Keeps decisions in a list. For tests and for a gateway with no sink yet.

    Not a durable audit trail and does not pretend to be one -- design doc §21
    wants a record that survives the request, which is ``JsonlAuditLog``'s job.
    """

    def __init__(self) -> None:
        super().__init__()
        self._records: List[DecisionLogRecord] = []

    def _emit(self, decision: DecisionLogRecord) -> None:
        with self._lock:
            self._records.append(decision)

    @property
    def records(self) -> Tuple[DecisionLogRecord, ...]:
        with self._lock:
            return tuple(self._records)

    def for_request(self, request_id: str) -> Tuple[DecisionLogRecord, ...]:
        """Every decision for one request, in the order they were recorded.

        This is design doc §21's actual question -- "what did the gateway do
        with this request" -- so it is a method rather than something each
        caller re-derives by filtering.
        """
        return tuple(r for r in self.records if r.request_id == request_id)


class JsonlAuditLog(AuditLog):
    """Appends one JSON object per line to a file.

    Newline-delimited JSON because the consumer design doc §18 names is a join
    against Phase 9's Prometheus series -- a line-oriented, append-only file is
    what every log shipper and every ``jq`` invocation already reads, and it
    needs no schema migration when a later phase adds a field.

    Each record is flushed as it is written. An audit trail that is still in a
    userspace buffer when the process dies has not recorded anything, and this
    file is small and written once per block, not per token.
    """

    def __init__(self, path: "str | Path") -> None:
        super().__init__()
        self._path = Path(path).expanduser()
        self._handle: Optional[TextIO] = None
        self._handle = self._open()

    def _open(self) -> TextIO:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        return self._path.open("a", encoding="utf-8")

    def _emit(self, decision: DecisionLogRecord) -> None:
        with self._lock:
            if self._handle is None:
                raise ValueError(f"{self._path}: audit log is closed")
            # model_dump_json rather than a hand-rolled dict: the frozen type
            # decides its own serialization, including the datetime format, so
            # a field added in a later phase appears here without this module
            # being edited.
            self._handle.write(decision.model_dump_json() + "\n")
            self._handle.flush()

    def close(self) -> None:
        with self._lock:
            handle, self._handle = self._handle, None
        if handle is not None:
            handle.close()

    @property
    def path(self) -> Path:
        return self._path

    def read_all(self) -> Tuple[DecisionLogRecord, ...]:
        """Parse the file back into records.

        Round-tripping through the frozen type on the way in *and* out is what
        makes the file an audit trail rather than a pile of text: a line that no
        longer validates fails here instead of being read as something it is
        not.
        """
        if not self._path.exists():
            return ()
        with self._path.open("r", encoding="utf-8") as handle:
            return tuple(
                DecisionLogRecord.model_validate(json.loads(line))
                for line in handle
                if line.strip()
            )
