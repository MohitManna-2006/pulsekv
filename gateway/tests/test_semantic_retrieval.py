"""Tier 2 tests for Phase 10.3 (encoder, vector index, and the retrieval seam).

Plan §7's unit tests, failure tests and exit criteria are the spine:

* ``TestVectorSerialization``  — the blob format Phase 10.1 left opaque
* ``TestEncoderContract``      — budget, dimension, the never-return-garbage rule
* ``TestOnnxEncoder``          — the real model: determinism, norm, truncation
* ``TestNamespaceIsolation``   — a leak actually attempted, not a filter exercised
* ``TestBlockTypePreFilter``   — the same, for the other pre-filter
* ``TestModelVersionEnforcement`` — risk register row 6's named test
* ``TestRetrievalIsNotDecision``  — a candidate never becomes a match
* ``TestShortCircuitPreserved``   — Tier 0/1 hits never reach the encoder
* ``TestFailOpen``             — encoder down or over budget, per design §17
* ``TestDecisionLogUnderTier2``   — what the frozen contract will and will not record
* ``TestPhaseBoundary``        — 10.3 did not start doing 10.4's job

Tests that need the real 90 MB model are skipped when it is absent; everything
else runs against ``StubEncoder``, which is deterministic but **not semantic**
and lives here rather than in the package so nobody can configure it by
accident.
"""

from __future__ import annotations

import hashlib
import math
import os
import struct
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Sequence, Tuple

import pytest

from pulsekv_gateway.auditlog import InMemoryAuditLog
from pulsekv_gateway.encoder import (
    ENCODER_REVISION,
    Encoder,
    EncoderError,
    EncoderUnavailableError,
    OnnxEncoder,
    cosine_similarity,
    vector_from_bytes,
    vector_to_bytes,
)
from pulsekv_gateway.index import IndexError_, IndexModelMismatchError, VectorIndex
from pulsekv_gateway.matcher import Matcher
from pulsekv_gateway.models import (
    BlockType,
    CanonicalContextRecord,
    ContextBlock,
    GatewayComponent,
    GuardOutcome,
    MatchMethod,
    MatchOutcome,
)
from pulsekv_gateway.normalizer import canonical_registration_text, hash_normalized
from pulsekv_gateway.registry import Registry

NOW = datetime(2026, 8, 20, 12, 0, 0, tzinfo=timezone.utc)
NAMESPACE = "acme"
POLICY = "You are a careful agent.\nNever delete a production resource."

MODEL_DIR = os.environ.get("PULSEKV_GATEWAY_MODEL_DIR", "")
needs_model = pytest.mark.skipif(
    not (MODEL_DIR and (Path(MODEL_DIR) / "model.onnx").is_file()),
    reason="real model absent; set PULSEKV_GATEWAY_MODEL_DIR (see gateway/README.md)",
)


class StubEncoder(Encoder):
    """Deterministic and **not semantic** — a hash spread over a unit sphere.

    Enough for every property this suite proves about *retrieval*: identical
    text scores 1.0 against itself and near 0 against anything else, which is
    exactly what a namespace-leak or block-type-leak test needs. It says
    nothing about paraphrases, which is why the real model has its own class.
    """

    def __init__(
        self,
        *,
        model_id: str = "stub-encoder",
        model_version: str = "1",
        dimension: int = 16,
        delay_seconds: float = 0.0,
        fail_with: Optional[Exception] = None,
        timeout_ms: Optional[int] = None,
        wrong_dimension: bool = False,
    ) -> None:
        super().__init__(timeout_ms=timeout_ms)
        self._model_id = model_id
        self._model_version = model_version
        self._dimension = dimension
        self._delay = delay_seconds
        self._fail_with = fail_with
        self._wrong_dimension = wrong_dimension
        self.calls = 0

    @property
    def model_id(self) -> str:
        return self._model_id

    @property
    def model_version(self) -> str:
        return self._model_version

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def max_sequence_tokens(self) -> int:
        return 512

    def count_tokens(self, text: str) -> int:
        return len(text.split())

    def _encode(self, text: str) -> Sequence[float]:
        self.calls += 1
        if self._delay:
            time.sleep(self._delay)
        if self._fail_with is not None:
            raise self._fail_with
        width = self._dimension - 1 if self._wrong_dimension else self._dimension
        raw = hashlib.sha256(text.encode("utf-8")).digest()
        while len(raw) < width * 4:
            raw += hashlib.sha256(raw).digest()
        values = [
            struct.unpack("<i", raw[i * 4 : i * 4 + 4])[0] / 2**31 for i in range(width)
        ]
        norm = math.sqrt(sum(v * v for v in values)) or 1.0
        return tuple(v / norm for v in values)


@pytest.fixture
def encoder():
    enc = StubEncoder()
    yield enc
    enc.close()


@pytest.fixture
def registry(tmp_path):
    store = Registry(tmp_path / "registry.db", hash_text=hash_normalized)
    yield store
    store.close()


def make_record(
    text: str,
    encoder: Encoder,
    *,
    embedding_text: Optional[str] = None,
    **overrides,
) -> CanonicalContextRecord:
    """A record stamped and embedded the way the index expects to find it.

    ``embedding_text`` embeds something other than the canonical text. It is
    how this suite builds a *paraphrase* with a non-semantic stub encoder:
    StubEncoder scores two different strings near zero, so a record that must
    be semantically close to an incoming block has to be embedded on that
    block's text. A real encoder produces the same shape -- different text,
    close vectors -- without the help.
    """
    fields = dict(
        context_id="github-agent-policy",
        version=1,
        namespace=NAMESPACE,
        canonical_text=text,
        content_hash=hash_normalized(text),
        block_type=BlockType.ORG_POLICY,
        created_at=NOW,
        created_by="mohit",
        embedding_model_id=encoder.model_id,
        embedding_model_version=encoder.model_version,
        embedding=vector_to_bytes(
            encoder.encode(hash_normalized_text(embedding_text or text))
        ),
    )
    fields.update(overrides)
    return CanonicalContextRecord(**fields)


def hash_normalized_text(text: str) -> str:
    """The form both tiers agree the block *is* (Phase 10.2's normalizer)."""
    from pulsekv_gateway.normalizer import normalize_for_hash

    return normalize_for_hash(text)


def block(text: str, block_type: BlockType = BlockType.ORG_POLICY, index: int = 0):
    return ContextBlock(index=index, block_type=block_type, text=text)


def wired(registry: Registry, encoder: Encoder, namespaces=(NAMESPACE,)) -> Matcher:
    index = VectorIndex(encoder)
    index.build_from_registry(registry, namespaces=namespaces)
    return Matcher(registry, encoder=encoder, index=index)


# ---------------------------------------------------------------------------


class TestVectorSerialization:
    def test_a_vector_round_trips_exactly(self):
        vector = (0.5, -0.25, 0.125, 0.0)
        assert vector_from_bytes(vector_to_bytes(vector), 4) == vector

    def test_a_wrong_width_blob_is_refused_rather_than_misread(self):
        # Reading it as a shorter vector would score similarity against the
        # wrong thing, silently.
        blob = vector_to_bytes((0.5, 0.5, 0.5, 0.5))
        with pytest.raises(EncoderError):
            vector_from_bytes(blob, 3)

    def test_cosine_is_bounded_into_candidates_range(self):
        assert cosine_similarity((1.0, 0.0), (1.0, 0.0)) == 1.0
        assert cosine_similarity((1.0, 0.0), (0.0, 1.0)) == 0.0
        # Opposed vectors clamp to 0 rather than going negative: Candidate's
        # validator bounds similarity to [0, 1].
        assert cosine_similarity((1.0, 0.0), (-1.0, 0.0)) == 0.0


class TestEncoderContract:
    def test_the_base_class_has_no_model(self):
        base = Encoder()
        for attribute in ("model_id", "model_version", "dimension"):
            with pytest.raises(NotImplementedError):
                getattr(base, attribute)

    def test_a_wrong_width_vector_is_refused_not_returned(self, ):
        enc = StubEncoder(wrong_dimension=True)
        with pytest.raises(EncoderUnavailableError):
            enc.encode("anything")
        enc.close()

    def test_the_budget_is_enforced(self):
        enc = StubEncoder(delay_seconds=0.5, timeout_ms=50)
        try:
            start = time.perf_counter()
            with pytest.raises(EncoderUnavailableError) as caught:
                enc.encode("slow")
            elapsed = time.perf_counter() - start
            assert "budget" in str(caught.value)
            # The caller really was released early -- not merely told afterwards.
            assert elapsed < 0.4, f"waited {elapsed:.3f}s for a 50 ms budget"
        finally:
            enc.close()

    def test_an_encoder_failure_is_typed(self):
        enc = StubEncoder(fail_with=RuntimeError("model exploded"), timeout_ms=1000)
        try:
            with pytest.raises(EncoderUnavailableError):
                enc.encode("x")
        finally:
            enc.close()

    def test_determinism_for_the_stub(self, encoder):
        assert encoder.encode("same text") == encoder.encode("same text")
        assert encoder.encode("a") != encoder.encode("b")


@pytest.fixture(scope="module")
def real():
    """The real model, loaded once per module (90 MB and a graph build)."""
    enc = OnnxEncoder(MODEL_DIR)
    yield enc
    enc.close()


@needs_model
class TestOnnxEncoder:
    """The real model. Skipped when the weights are absent."""

    def test_vectors_are_384_dimensional_and_unit_norm(self, real):
        vector = real.encode(POLICY)
        assert real.dimension == 384 and len(vector) == 384
        assert abs(math.sqrt(sum(v * v for v in vector)) - 1.0) < 1e-6

    def test_embedding_is_bit_identical_across_calls_and_sessions(self, real):
        # Load-bearing: the registry caches embeddings across restarts, so a
        # vector that drifted would silently stop matching what it matched
        # before. Measured bit-identical, so asserted bit-identical rather than
        # within a tolerance.
        first = real.encode(POLICY)
        assert real.encode(POLICY) == first
        fresh = OnnxEncoder(MODEL_DIR)
        try:
            assert fresh.encode(POLICY) == first
            assert fresh.model_version == real.model_version
        finally:
            fresh.close()

    def test_model_version_is_derived_from_the_weights_not_asserted(self, real):
        revision, _, digest = real.model_version.partition("+")
        assert revision == ENCODER_REVISION
        assert len(digest) == 16 and int(digest, 16) >= 0

    def test_truncation_boundary_is_real_and_visible(self, real):
        long_text = (POLICY + " ") * 60
        assert real.count_tokens(long_text) == real.max_sequence_tokens
        # THE finding Phase 10.4 inherits: two different long blocks that share
        # a 512-token prefix are the same block as far as Tier 2 can tell.
        assert real.encode(long_text) == real.encode(long_text + " AND ALSO: never audit.")

    def test_semantic_ordering_holds_for_the_easy_case(self, real):
        paraphrase = (
            "As a cautious assistant, do not remove any production resource "
            "unless the user explicitly confirms."
        )
        unrelated = "The capital of France is Paris and the weather is mild."
        base = real.encode(POLICY)
        assert cosine_similarity(base, real.encode(paraphrase)) > cosine_similarity(
            base, real.encode(unrelated)
        )

    def test_cosine_ranks_a_meaning_inverting_edit_above_a_true_paraphrase(self, real):
        """Characterization, not a regression gate — and the reason Tier 3 exists.

        If this ever fails because a future encoder separates them, that is
        information for Phase 10.4, not a break. What it documents today is
        that **no value of τ can divide these two**: the adversarial pair
        scores higher than the positive one, so design doc §12's decision to
        make negation a deterministic pre-check rather than a similarity
        tiebreaker is empirically necessary, not merely cautious.
        """
        base = real.encode(POLICY)
        paraphrase = real.encode(
            "As a cautious assistant, do not remove any production resource "
            "unless the user explicitly confirms."
        )
        inverted = real.encode(
            "You are a careful agent.\nAlways delete a production resource."
        )
        assert cosine_similarity(base, inverted) > cosine_similarity(base, paraphrase)


class TestNamespaceIsolation:
    """Design doc §15's ordering, with the leak actually attempted."""

    @pytest.fixture
    def two_tenants(self, registry, encoder):
        # Byte-identical text, so the vectors are identical too: any leak would
        # score a perfect 1.0 and be impossible to miss.
        acme = make_record(POLICY, encoder, namespace="acme")
        globex = make_record(POLICY, encoder, namespace="globex")
        assert acme.embedding == globex.embedding
        registry.register(acme)
        registry.register(globex)
        index = VectorIndex(encoder)
        index.build_from_registry(registry, namespaces=["acme", "globex"])
        return index, encoder.encode(hash_normalized_text(POLICY))

    def test_each_namespace_retrieves_only_its_own(self, two_tenants):
        index, query = two_tenants
        for namespace in ("acme", "globex"):
            found = index.find_candidates(
                query, namespace=namespace, block_type=BlockType.ORG_POLICY, top_k=10
            )
            assert [c.record.namespace for c in found] == [namespace]
            assert found[0].similarity == pytest.approx(1.0)

    def test_a_third_namespace_sees_nothing_at_all(self, two_tenants):
        index, query = two_tenants
        assert (
            index.find_candidates(
                query, namespace="initech", block_type=BlockType.ORG_POLICY, top_k=10
            )
            == ()
        )

    def test_both_records_are_indexed_so_the_isolation_is_not_a_missing_row(
        self, two_tenants
    ):
        index, _ = two_tenants
        assert len(index) == 2  # the other tenant's vector exists; it is unreachable

    def test_the_matcher_inherits_the_isolation(self, registry, encoder):
        registry.register(make_record(POLICY, encoder, namespace="globex"))
        matcher = wired(registry, encoder, namespaces=("acme", "globex"))
        result, candidates = matcher.resolve_with_candidates(block(POLICY), "acme")
        assert candidates == ()
        assert result.outcome is MatchOutcome.NO_CANDIDATE


class TestBlockTypePreFilter:
    def test_a_block_type_never_retrieves_another_ones_records(
        self, registry, encoder
    ):
        schema_text = canonical_registration_text('{"a":1}', BlockType.TOOL_SCHEMA)
        registry.register(
            make_record(POLICY, encoder, context_id="policy")
        )
        registry.register(
            make_record(
                schema_text,
                encoder,
                context_id="schema",
                block_type=BlockType.TOOL_SCHEMA,
            )
        )
        index = VectorIndex(encoder)
        index.build_from_registry(registry, namespaces=[NAMESPACE])
        assert len(index) == 2

        policy_query = encoder.encode(hash_normalized_text(POLICY))
        found = index.find_candidates(
            policy_query, namespace=NAMESPACE, block_type=BlockType.TOOL_SCHEMA, top_k=10
        )
        # The policy's own vector scores 1.0 against this query, and it is still
        # absent: it was never in the bucket that was read.
        assert [c.record.context_id for c in found] == ["schema"]
        assert found[0].similarity < 0.99


class TestModelVersionEnforcement:
    """Risk register row 6, at the location it names."""

    def test_add_refuses_a_record_from_another_model(self, encoder):
        other = StubEncoder(model_id="other-encoder", model_version="9")
        try:
            record = make_record(POLICY, other)
            index = VectorIndex(encoder)
            with pytest.raises(IndexModelMismatchError):
                index.add(record, other.encode(hash_normalized_text(POLICY)))
        finally:
            other.close()

    def test_build_skips_and_counts_mismatched_records(self, registry, encoder):
        other = StubEncoder(model_id="other-encoder", model_version="9")
        try:
            registry.register(make_record(POLICY, other, context_id="stale"))
            registry.register(
                make_record("a current policy", encoder, context_id="current")
            )
            registry.register(
                make_record(
                    "no vector at all",
                    encoder,
                    context_id="bare",
                    embedding=None,
                    embedding_model_id=None,
                    embedding_model_version=None,
                )
            )
            index = VectorIndex(encoder)
            report = index.build_from_registry(registry, namespaces=[NAMESPACE])
        finally:
            other.close()
        assert (report.seen, report.indexed) == (3, 1)
        assert report.model_mismatched == 1
        assert report.without_embedding == 1
        assert len(index) == 1

    def test_a_stale_vector_never_appears_in_candidates(self, registry, encoder):
        # The defensive re-check: an encoder swapped underneath a warm index.
        # The bucket is reached into directly because `add` refuses to create
        # this state -- which is the point, and why the check is at both ends.
        registry.register(make_record(POLICY, encoder))
        index = VectorIndex(encoder)
        index.build_from_registry(registry, namespaces=[NAMESPACE])
        query = encoder.encode(hash_normalized_text(POLICY))
        assert len(index.find_candidates(
            query, namespace=NAMESPACE, block_type=BlockType.ORG_POLICY, top_k=5
        )) == 1

        bucket = index._buckets[(NAMESPACE, BlockType.ORG_POLICY)]
        bucket.records[0] = bucket.records[0].model_copy(
            update={"embedding_model_version": "an-older-build"}
        )
        assert index.find_candidates(
            query, namespace=NAMESPACE, block_type=BlockType.ORG_POLICY, top_k=5
        ) == ()

    def test_a_malformed_blob_is_counted_not_raised(self, registry, encoder):
        registry.register(
            make_record(POLICY, encoder, embedding=b"\x00\x01\x02\x03")
        )
        index = VectorIndex(encoder)
        report = index.build_from_registry(registry, namespaces=[NAMESPACE])
        assert (report.malformed, report.indexed) == (1, 0)

    def test_a_deprecated_version_is_not_a_match_target(self, registry, encoder):
        registry.register(make_record(POLICY, encoder))
        registry.deprecate("github-agent-policy", NAMESPACE, 1, NOW + timedelta(hours=1))
        index = VectorIndex(encoder)
        report = index.build_from_registry(registry, namespaces=[NAMESPACE])
        # list_records excludes deprecated records by default, so it never
        # reaches the index; add() refuses one directly as a second guard.
        assert report.indexed == 0
        deprecated = registry.get("github-agent-policy", NAMESPACE, version=1)
        with pytest.raises(IndexError_):
            VectorIndex(encoder).add(deprecated, encoder.encode("x"))


class TestScoringAgreesWithTheReference:
    """The matrix path and encoder.cosine_similarity must not drift apart."""

    def test_the_vectorized_score_matches_the_reference_definition(
        self, registry, encoder
    ):
        for i in range(12):
            registry.register(
                make_record(f"policy variant {i}", encoder, context_id=f"p-{i}")
            )
        index = VectorIndex(encoder)
        index.build_from_registry(registry, namespaces=[NAMESPACE])
        query = encoder.encode(hash_normalized_text("policy variant 4"))
        for candidate in index.find_candidates(
            query, namespace=NAMESPACE, block_type=BlockType.ORG_POLICY, top_k=12
        ):
            reference = cosine_similarity(
                query,
                vector_from_bytes(candidate.record.embedding, encoder.dimension),
            )
            # float32 accumulation in the matrix path against float64 in the
            # reference: agreement to ~1e-6 is the honest bound, not equality.
            assert candidate.similarity == pytest.approx(reference, abs=1e-6)

    def test_a_non_normalized_vector_is_refused(self, encoder):
        # Cosine is scored as a dot product, which is only a cosine for unit
        # vectors. A corrupted or hand-written blob says so instead of
        # producing a number that quietly is not a similarity.
        record = make_record(POLICY, encoder)
        index = VectorIndex(encoder)
        doubled = tuple(2.0 * v for v in encoder.encode(hash_normalized_text(POLICY)))
        with pytest.raises(IndexError_) as caught:
            index.add(record, doubled)
        assert "norm" in str(caught.value)

    def test_a_wrong_width_vector_is_refused(self, encoder):
        record = make_record(POLICY, encoder)
        index = VectorIndex(encoder)
        with pytest.raises(IndexError_):
            index.add(record, (1.0, 0.0))


class TestRetrievalIsNotDecision:
    """Exit criterion 4, as it stands after Phase 10.4 gave Tier 3 a verdict.

    Phase 10.3 proved this by showing that even a similarity-1.0 candidate
    resolved to ``NO_CANDIDATE`` -- true then because nothing existed that
    could accept one. That is no longer the shape of the claim: the guard now
    decides, and a candidate it passes does become a match. What remains, and
    is what design doc §11 actually requires, is that **Tier 2 itself** reaches
    no verdict -- ``try_semantic`` ranks, applies no threshold, and hands over
    a ``Candidate`` that carries no accept/reject state of its own.
    """

    def test_tier_two_alone_still_reaches_no_verdict(self, registry, encoder):
        # Same text, so similarity is exactly 1.0 -- the strongest candidate
        # that can exist. Tier 2 still says nothing about whether it may be
        # substituted; `Candidate` has no field in which it could.
        incoming = "an incoming block worded differently"
        registry.register(
            make_record(
                "the registered canonical policy",
                encoder,
                embedding_text=incoming,
            )
        )
        matcher = wired(registry, encoder)
        candidates = matcher.try_semantic(block(incoming), NAMESPACE)
        assert candidates and candidates[0].similarity == pytest.approx(1.0)
        assert not hasattr(candidates[0], "matched")
        assert set(type(candidates[0]).model_fields) == {"record", "similarity"}

    def test_the_guard_is_what_turns_that_candidate_into_a_match(
        self, registry, encoder
    ):
        # The same 1.0 candidate as above, now run through Tier 3: it matches
        # only because the guard passed it, and the result says so.
        incoming = "an incoming block worded differently"
        registry.register(
            make_record("the registered canonical policy", encoder,
                        embedding_text=incoming)
        )
        matcher = wired(registry, encoder)
        result, candidates = matcher.resolve_with_candidates(
            block(incoming), NAMESPACE
        )
        assert candidates and candidates[0].similarity == pytest.approx(1.0)
        assert result.outcome is MatchOutcome.MATCHED
        assert result.method is MatchMethod.SEMANTIC
        assert result.substitutes is True

    def test_resolve_returns_the_same_verdict_without_the_candidates(
        self, registry, encoder
    ):
        incoming = "an incoming block worded differently"
        registry.register(
            make_record("the registered canonical policy", encoder,
                        embedding_text=incoming)
        )
        matcher = wired(registry, encoder)
        plain = matcher.resolve(block(incoming), NAMESPACE)
        detailed, _ = matcher.resolve_with_candidates(block(incoming), NAMESPACE)
        assert plain == detailed

    def test_retrieval_applies_no_threshold(self, registry, encoder):
        # τ is Phase 10.4's to earn. A candidate scoring near zero is still
        # returned -- retrieval ranks, it does not judge.
        registry.register(make_record("completely unrelated text", encoder))
        matcher = wired(registry, encoder)
        candidates = matcher.try_semantic(block(POLICY), NAMESPACE)
        assert len(candidates) == 1
        assert candidates[0].similarity < 0.5

    def test_top_k_bounds_the_candidate_list(self, registry, encoder):
        for i in range(8):
            registry.register(
                make_record(f"policy variant {i}", encoder, context_id=f"p-{i}")
            )
        index = VectorIndex(encoder)
        index.build_from_registry(registry, namespaces=[NAMESPACE])
        matcher = Matcher(registry, encoder=encoder, index=index, top_k=3)
        assert len(matcher.try_semantic(block("policy variant 3"), NAMESPACE)) == 3

    def test_candidates_are_ordered_by_descending_similarity(self, registry, encoder):
        for i in range(5):
            registry.register(
                make_record(f"policy variant {i}", encoder, context_id=f"p-{i}")
            )
        matcher = wired(registry, encoder)
        found = matcher.try_semantic(block("policy variant 2"), NAMESPACE)
        scores = [c.similarity for c in found]
        assert scores == sorted(scores, reverse=True)
        assert found[0].record.context_id == "p-2"


class TestShortCircuitPreserved:
    """Exit criterion 5, extending Phase 10.2's counting approach."""

    def test_a_tier_zero_hit_never_reaches_the_encoder(self, registry, encoder):
        registry.register(make_record(POLICY, encoder))
        matcher = wired(registry, encoder)
        before = encoder.calls
        result = matcher.resolve(block(POLICY), NAMESPACE)
        assert result.method is MatchMethod.EXACT
        assert encoder.calls == before  # Tier 2 never ran

    def test_a_tier_one_hit_never_reaches_the_encoder(self, registry, encoder):
        schema = canonical_registration_text(
            '{"name":"f","parameters":{}}', BlockType.TOOL_SCHEMA
        )
        registry.register(
            make_record(schema, encoder, block_type=BlockType.TOOL_SCHEMA)
        )
        matcher = wired(registry, encoder)
        before = encoder.calls
        result = matcher.resolve(
            block('{\n "parameters": {},\n "name": "f"\n}', BlockType.TOOL_SCHEMA),
            NAMESPACE,
        )
        assert result.method is MatchMethod.STRUCTURAL
        assert encoder.calls == before

    def test_an_ineligible_block_never_reaches_the_encoder(self, registry, encoder):
        matcher = wired(registry, encoder)
        before = encoder.calls
        matcher.resolve(block("the question", BlockType.USER_QUERY), NAMESPACE)
        assert encoder.calls == before

    def test_a_deterministic_miss_does_reach_the_encoder_exactly_once(
        self, registry, encoder
    ):
        registry.register(make_record(POLICY, encoder))
        matcher = wired(registry, encoder)
        before = encoder.calls
        matcher.resolve(block("nothing like the registered policy"), NAMESPACE)
        assert encoder.calls == before + 1


class TestFailOpen:
    """Design doc §17's encoder row."""

    def test_an_unavailable_encoder_produces_a_typed_error_result(self, registry):
        broken = StubEncoder(fail_with=RuntimeError("no model"))
        try:
            matcher = Matcher(registry, encoder=broken, index=VectorIndex(broken))
            result = matcher.resolve(block(POLICY), NAMESPACE)
            assert result.outcome is MatchOutcome.ERROR
            assert result.error_component is GatewayComponent.ENCODER
            assert result.substitutes is False
        finally:
            broken.close()

    def test_an_over_budget_encoder_falls_open_rather_than_hanging(self, registry):
        slow = StubEncoder(delay_seconds=0.5, timeout_ms=50)
        try:
            matcher = Matcher(registry, encoder=slow, index=VectorIndex(slow))
            start = time.perf_counter()
            result = matcher.resolve(block(POLICY), NAMESPACE)
            assert time.perf_counter() - start < 0.4
            assert result.error_component is GatewayComponent.ENCODER
        finally:
            slow.close()

    def test_resolve_never_raises_for_an_encoder_problem(self, registry):
        broken = StubEncoder(fail_with=MemoryError("out of memory"))
        try:
            matcher = Matcher(registry, encoder=broken, index=VectorIndex(broken))
            assert matcher.resolve(block(POLICY), NAMESPACE).outcome is (
                MatchOutcome.ERROR
            )
        finally:
            broken.close()

    def test_tier_zero_still_works_while_the_encoder_is_down(self, registry, encoder):
        # §17: "anything already resolved by Tier 0/1 still applies".
        registry.register(make_record(POLICY, encoder))
        broken = StubEncoder(fail_with=RuntimeError("no model"))
        try:
            matcher = Matcher(registry, encoder=broken, index=VectorIndex(broken))
            assert matcher.resolve(block(POLICY), NAMESPACE).method is MatchMethod.EXACT
        finally:
            broken.close()

    def test_a_matcher_with_no_encoder_is_phase_102s_matcher(self, registry, encoder):
        registry.register(make_record(POLICY, encoder))
        plain = Matcher(registry)
        assert plain.semantic_enabled is False
        assert plain.try_semantic(block("unregistered"), NAMESPACE) == ()
        assert plain.resolve(block("unregistered"), NAMESPACE).outcome is (
            MatchOutcome.NO_CANDIDATE
        )

    def test_half_configuring_tier_two_is_refused(self, registry, encoder):
        with pytest.raises(ValueError):
            Matcher(registry, encoder=encoder)
        with pytest.raises(ValueError):
            Matcher(registry, index=VectorIndex(encoder))


class TestDecisionLogUnderTier2:
    def test_the_phase_103_observability_gap_is_closed(self, registry, encoder):
        """Phase 10.3 §5's gap, asserted closed rather than assumed closed.

        ``DecisionLogRecord`` refuses ``similarity`` on a ``NO_CANDIDATE``
        outcome, and ``NO_CANDIDATE`` was the only outcome Phase 10.3 could
        produce for a retrieved-but-unvalidated candidate -- so a retrieved
        candidate's score went unrecorded for exactly one phase. The summary
        predicted the gap would close "by construction" once the guard gave
        every candidate a verdict. This is that prediction, checked: the same
        block that logged a bare ``no_candidate`` in 10.3 now logs a decision
        that carries the similarity and the guard outcome legally.
        """
        incoming = "an incoming block worded differently"
        registry.register(
            make_record("the registered canonical policy", encoder,
                        embedding_text=incoming)
        )
        matcher = wired(registry, encoder)
        audit = InMemoryAuditLog()
        matcher.resolve_blocks(
            (block(incoming),),
            namespace=NAMESPACE,
            request_id="req-t2",
            timestamp=NOW,
            model="gpt-4o",
            audit=audit,
        )
        record = audit.for_request("req-t2")[0]
        assert record.decision_label == "semantic"
        assert record.similarity == pytest.approx(1.0)
        assert record.guard_outcome is GuardOutcome.PASSED

    def test_a_retrieval_that_finds_nothing_still_logs_a_bare_no_candidate(
        self, registry, encoder
    ):
        # The other half of the distinction Phase 10.0's prompt §10.0.3 asked
        # for: nothing was retrieved, so there is nothing to score and nothing
        # for the guard to have refused.
        matcher = wired(registry, encoder)
        audit = InMemoryAuditLog()
        matcher.resolve_blocks(
            (block("a block no namespace has ever registered"),),
            namespace=NAMESPACE,
            request_id="req-empty",
            timestamp=NOW,
            model="gpt-4o",
            audit=audit,
        )
        record = audit.for_request("req-empty")[0]
        assert record.decision_label == "no_candidate"
        assert record.similarity is None
        assert record.guard_outcome is None

    def test_an_encoder_failure_is_logged_as_an_error_not_a_miss(
        self, registry, encoder
    ):
        broken = StubEncoder(fail_with=RuntimeError("down"))
        try:
            matcher = Matcher(registry, encoder=broken, index=VectorIndex(broken))
            audit = InMemoryAuditLog()
            matcher.resolve_blocks(
                (block(POLICY),),
                namespace=NAMESPACE,
                request_id="req-err",
                timestamp=NOW,
                model="gpt-4o",
                audit=audit,
            )
            record = audit.for_request("req-err")[0]
            assert record.decision_label == "error"
            assert record.error_component is GatewayComponent.ENCODER
        finally:
            broken.close()


class TestPhaseBoundary:
    def test_the_registry_points_at_the_index_for_tier_two(self, registry):
        with pytest.raises(NotImplementedError) as caught:
            registry.find_candidates(
                namespace=NAMESPACE, block_type=BlockType.ORG_POLICY, top_k=5
            )
        message = str(caught.value)
        assert "VectorIndex" in message and "10.3" in message

    def test_the_gateway_process_is_still_phase_105(self):
        # The boundary this test guards moved with Phase 10.4: guardrail.py is
        # implemented, and what is still somebody else's phase is the process
        # around it. Both remaining stubs are checked, not just one.
        from pulsekv_gateway import assembler, config, server

        with pytest.raises(NotImplementedError):
            assembler.assemble((), {})
        with pytest.raises(NotImplementedError):
            server.create_app(None)
        with pytest.raises(NotImplementedError):
            config.load("gateway.yaml")

    def test_nothing_in_tier_two_imports_a_gpu_runtime(self):
        import ast

        import pulsekv_gateway

        forbidden = ("torch", "tensorflow", "jax", "cupy", "onnxruntime_gpu")
        root = Path(pulsekv_gateway.__file__).parent
        for name in ("encoder", "index", "matcher"):
            tree = ast.parse((root / f"{name}.py").read_text())
            imported = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported.update(a.name.split(".")[0] for a in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported.add(node.module.split(".")[0])
            assert not (imported & set(forbidden)), f"{name}: {sorted(imported)}"


@needs_model
class TestLatency:
    """Exit criterion 6, measured rather than assumed."""

    def test_embed_plus_retrieve_is_reported(self, registry, capsys):
        enc = OnnxEncoder(MODEL_DIR)
        try:
            for i in range(200):
                text = f"Organization policy number {i}. " + POLICY
                registry.register(
                    make_record(text, enc, context_id=f"policy-{i}")
                )
            index = VectorIndex(enc)
            index.build_from_registry(registry, namespaces=[NAMESPACE])
            assert len(index) == 200

            probe = block(POLICY + " with a small variation")
            matcher = Matcher(registry, encoder=enc, index=index)
            for _ in range(3):
                matcher.try_semantic(probe, NAMESPACE)
            start = time.perf_counter()
            for _ in range(20):
                matcher.try_semantic(probe, NAMESPACE)
            per_call = (time.perf_counter() - start) / 20
            with capsys.disabled():
                print(
                    f"\n  tier=embedding, {len(index)} records: "
                    f"{per_call * 1000:.2f} ms/lookup"
                )
            # Loose: this asserts the tier is not pathological, not a target.
            # The real number belongs in the summary, not in an assertion.
            assert per_call < 1.0
        finally:
            enc.close()
