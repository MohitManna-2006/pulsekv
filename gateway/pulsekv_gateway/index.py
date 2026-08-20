"""Vector candidate retrieval (Phase 10.3).

Design doc §11 Tier 2, §15 (namespace pre-filter); plan §7.

Index technology: **brute-force cosine over an in-memory matrix**, which is what
design doc §10 recommends and what this phase's measurements support -- see
``docs/pulsekv-semantic-context-phase10.3-summary.md`` §2 for the numbers. No
ANN library is added. At the scale §10 estimates (low hundreds to low
thousands of curated entries) the search is a rounding error next to the
encode that precedes it, and an approximate index would trade exactness for
time this phase has no evidence it needs.

**"Brute force" means one matrix-vector product, not a Python loop.** Measured
during this phase, a per-record Python loop over 384 floats costs ~80 ms at
5,000 records -- two orders of magnitude off design doc §10's "sub-millisecond
on any modern CPU", which is a claim about vectorized arithmetic. Each bucket
therefore keeps its vectors as one contiguous ``float32`` matrix and scores a
query with a single BLAS call. The pure-Python ``encoder.cosine_similarity``
remains the reference definition, and a test asserts the two agree.

The one non-negotiable property: ``namespace`` and ``block_type`` filter the
candidate set *before* similarity ranking. Design doc §15 rests its
"structurally impossible" cross-tenant claim on that ordering -- a
post-ranking filter would make cross-tenant safety a probability instead.

**How that ordering is implemented, and why it is not a filter at all.**
Vectors are held in separate buckets keyed by ``(namespace, block_type)``.
A query loads one bucket and never sees another, so there is no ranked list
containing another tenant's record to filter *out* -- the comparison is not
performed. This is stronger than filtering because a filter can be forgotten,
reordered, or short-circuited by a later edit; a bucket that was never loaded
cannot leak. ``TestNamespaceIsolation`` in the tests attempts the leak with
byte-identical text in two namespaces rather than merely exercising the filter.

Model identity is enforced here, in ``find_candidates``, because risk register
row 6 names this exact location: "enforce the version check in ``Index.top_k``,
not just document it". The as-built name for that method is
``VectorIndex.find_candidates`` (Phase 10.0 froze the stub under that name);
the requirement is the same and it is met at both ends -- ``add`` refuses a
mismatched record loudly, and ``find_candidates`` re-checks defensively, so an
encoder swapped underneath a warm index cannot serve stale vectors.
"""

from __future__ import annotations

import threading
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy

from .encoder import Encoder, EncoderError, cosine_similarity, vector_from_bytes
from .models import BlockType, Candidate, CanonicalContextRecord, GatewayError
from .registry import Registry, RegistryError

__all__ = ["IndexError_", "IndexModelMismatchError", "VectorIndex"]


class IndexError_(GatewayError):
    """Base for index failures. Trailing underscore avoids the builtin."""


class IndexModelMismatchError(IndexError_):
    """A record's embedding was produced by a different model than this index.

    Design doc §16: a vector from model A is not a valid comparison against
    model B's. Risk register row 6 rates the consequence as "degrades to false
    negatives (safe) if caught, but could contribute to false positives if a
    stale embedding is silently compared as if current" -- so it is caught.
    """


# A vector whose norm strays this far from 1.0 was not produced by an encoder
# that normalizes, and scoring it as a plain dot product would silently report
# something that is not a cosine.
_NORM_TOLERANCE = 1e-3


class _Bucket:
    """One ``(namespace, block_type)`` partition's vectors and records.

    ``matrix`` is the stacked form the query is actually scored against; it is
    dropped on every mutation and rebuilt on the next read. Rebuilding costs one
    copy per write burst rather than per query, and writes are rare (a registry
    is curated, and the index is built once at startup).
    """

    __slots__ = ("matrix", "records", "vectors")

    def __init__(self) -> None:
        self.records: List[CanonicalContextRecord] = []
        self.vectors: List[Tuple[float, ...]] = []
        self.matrix = None

    def invalidate(self) -> None:
        self.matrix = None

    def stacked(self):
        if self.matrix is None:
            self.matrix = numpy.array(self.vectors, dtype=numpy.float32)
        return self.matrix


class VectorIndex:
    """Namespace-scoped nearest-neighbour retrieval over registered records."""

    def __init__(self, encoder: Encoder) -> None:
        """Bind the index to the encoder whose vectors it may compare.

        Taking the encoder rather than a pair of strings is deliberate: the
        identity a candidate is checked against is then the identity of the
        thing that will actually embed the query, and the two cannot drift
        apart by someone updating one and not the other.
        """
        self._encoder = encoder
        self._model_id = encoder.model_id
        self._model_version = encoder.model_version
        self._buckets: Dict[Tuple[str, BlockType], _Bucket] = {}
        self._lock = threading.Lock()

    # -- population --------------------------------------------------------

    def add(self, record: CanonicalContextRecord, embedding: Sequence[float]) -> None:
        """Index one published record's vector.

        Refuses a record whose stamps do not match this index's encoder, and
        refuses a deprecated one: design doc §17 says a deprecated version
        "stops being served as a match target", and the cheapest way to
        guarantee that is for it never to enter the searchable set.
        """
        self._require_current_model(record)
        if record.is_deprecated:
            raise IndexError_(
                f"{record.namespace}/{record.context_id} v{record.version}: "
                f"deprecated versions are not match targets (design doc §17)"
            )
        vector = tuple(float(value) for value in embedding)
        if len(vector) != self._encoder.dimension:
            raise IndexError_(
                f"{record.context_id} v{record.version}: embedding has "
                f"{len(vector)} dimensions, encoder produces "
                f"{self._encoder.dimension}"
            )
        # Cosine is computed as a plain dot product, which is only a cosine for
        # unit vectors. Checked rather than assumed, so a corrupted or
        # hand-written blob reports itself instead of producing a similarity
        # that is quietly not one.
        norm = float(numpy.linalg.norm(numpy.array(vector, dtype=numpy.float64)))
        if abs(norm - 1.0) > _NORM_TOLERANCE:
            raise IndexError_(
                f"{record.context_id} v{record.version}: embedding norm is "
                f"{norm:.6f}, expected 1.0 -- cosine is scored as a dot product "
                f"and is only a cosine for normalized vectors"
            )
        key = (record.namespace, record.block_type)
        with self._lock:
            bucket = self._buckets.setdefault(key, _Bucket())
            bucket.invalidate()
            for position, existing in enumerate(bucket.records):
                if (existing.context_id, existing.version) == (
                    record.context_id,
                    record.version,
                ):
                    bucket.records[position] = record
                    bucket.vectors[position] = vector
                    return
            bucket.records.append(record)
            bucket.vectors.append(vector)

    def remove(self, context_id: str, namespace: str, version: int) -> None:
        """Drop a deprecated version so it stops being retrievable."""
        with self._lock:
            for (bucket_namespace, _), bucket in self._buckets.items():
                if bucket_namespace != namespace:
                    continue
                for position, record in enumerate(bucket.records):
                    if (record.context_id, record.version) == (context_id, version):
                        del bucket.records[position]
                        del bucket.vectors[position]
                        bucket.invalidate()
                        return

    def build_from_registry(
        self, registry: Registry, *, namespaces: Iterable[str]
    ) -> "BuildReport":
        """Load every current, live, correctly-stamped record's vector.

        ``Registry.list_records(namespace=..., current_only=True)`` is the scan
        Phase 10.2's handoff nominated. Namespaces are passed in rather than
        discovered: the registry deliberately has no "list every tenant" call,
        because design doc §15 makes a cross-tenant read something the storage
        API should not make expressible.

        Records without an embedding, or stamped with another model, are
        **skipped and counted**, not raised on. Risk register row 6's
        instruction is that such an entry be "treated as no embedding
        available"; a build that died on the first stale record would take the
        whole tier down for one bad row, which is the opposite of §17.
        """
        report = BuildReport()
        for namespace in namespaces:
            try:
                records = registry.list_records(
                    namespace=namespace, current_only=True
                )
            except RegistryError as exc:
                report.registry_errors += 1
                report.last_error = str(exc)
                continue
            for record in records:
                report.seen += 1
                if record.embedding is None:
                    report.without_embedding += 1
                    continue
                if (
                    record.embedding_model_id != self._model_id
                    or record.embedding_model_version != self._model_version
                ):
                    report.model_mismatched += 1
                    continue
                try:
                    vector = vector_from_bytes(
                        record.embedding, self._encoder.dimension
                    )
                except EncoderError:
                    report.malformed += 1
                    continue
                self.add(record, vector)
                report.indexed += 1
        return report

    # -- retrieval ---------------------------------------------------------

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

        Retrieval only. A ``Candidate`` is not a decision and this method has
        no threshold in it: design doc §11 is explicit that Tier 2 "produces
        candidates, never a decision", and τ is Phase 10.4's to earn against
        the adversarial corpus. ``top_k`` bounds how many candidates the guard
        will be offered; it does not decide any of them.
        """
        if top_k <= 0:
            return ()
        with self._lock:
            bucket = self._buckets.get((namespace, block_type))
            if bucket is None:
                return ()
            # Snapshot under the lock; score outside it. The comparison never
            # sees another bucket because no other bucket was read.
            records = list(bucket.records)
            matrix = bucket.stacked()

        # Risk register row 6's named enforcement point, applied before any
        # arithmetic. A record whose stamps drifted since it was added -- an
        # encoder swapped under a warm index -- is treated as having no
        # embedding at all: not compared as if current, and not included at a
        # lower confidence. Deprecated versions leave here too (design §17).
        live = [
            position
            for position, record in enumerate(records)
            if record.embedding_model_id == self._model_id
            and record.embedding_model_version == self._model_version
            and not record.is_deprecated
        ]
        if not live:
            return ()

        query = numpy.array(embedding, dtype=numpy.float32)
        # One matrix-vector product for the whole bucket. Both sides are unit
        # vectors (enforced in `add`), so the dot product *is* the cosine; the
        # clamp matches encoder.cosine_similarity's, which stays the reference
        # definition and is asserted to agree with this path.
        similarities = numpy.clip(matrix[live] @ query, 0.0, 1.0)
        scored: List[Tuple[float, CanonicalContextRecord]] = [
            (float(similarity), records[position])
            for similarity, position in zip(similarities, live)
        ]

        # Sorted by descending similarity, then by (context_id, version) so a
        # tie is resolved the same way on every run -- an unstable candidate
        # order would make Phase 10.4's top-1 guard non-reproducible.
        scored.sort(key=lambda item: (-item[0], item[1].context_id, item[1].version))
        return tuple(
            Candidate(record=record, similarity=similarity)
            for similarity, record in scored[:top_k]
        )

    # -- introspection -----------------------------------------------------

    def __len__(self) -> int:
        with self._lock:
            return sum(len(bucket.records) for bucket in self._buckets.values())

    @property
    def model_id(self) -> str:
        return self._model_id

    @property
    def model_version(self) -> str:
        return self._model_version

    def _require_current_model(self, record: CanonicalContextRecord) -> None:
        if (
            record.embedding_model_id != self._model_id
            or record.embedding_model_version != self._model_version
        ):
            raise IndexModelMismatchError(
                f"{record.namespace}/{record.context_id} v{record.version}: "
                f"embedded with {record.embedding_model_id}"
                f"/{record.embedding_model_version}, index runs "
                f"{self._model_id}/{self._model_version} (design doc §16)"
            )


class BuildReport:
    """What ``build_from_registry`` did, and what it declined to do.

    Counted rather than logged because these numbers are the operator's only
    warning that a model upgrade left the index empty: ``model_mismatched``
    equal to ``seen`` is exactly what a half-finished re-embedding looks like,
    and it would otherwise present as "Tier 2 quietly stopped matching".
    """

    __slots__ = (
        "indexed",
        "last_error",
        "malformed",
        "model_mismatched",
        "registry_errors",
        "seen",
        "without_embedding",
    )

    def __init__(self) -> None:
        self.seen = 0
        self.indexed = 0
        self.without_embedding = 0
        self.model_mismatched = 0
        self.malformed = 0
        self.registry_errors = 0
        self.last_error: Optional[str] = None

    def __repr__(self) -> str:
        return (
            f"BuildReport(seen={self.seen}, indexed={self.indexed}, "
            f"without_embedding={self.without_embedding}, "
            f"model_mismatched={self.model_mismatched}, "
            f"malformed={self.malformed}, registry_errors={self.registry_errors})"
        )
