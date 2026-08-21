"""Tier 3 tests for Phase 10.4 (the equivalence guard and the corpus).

Plan §8's exit criteria are the spine:

* ``TestNegationCheck``       — design doc §12.1, a reject and its adjacent pass
* ``TestEntityCheck``         — design doc §12.2, the same shape
* ``TestTypeCheck``           — design doc §12.3, and whether it is redundant
* ``TestGuardIsRejectBiased`` — error and timeout are rejects, proven not asserted
* ``TestGuardReadsFullText``  — Phase 10.3's truncation finding, acted on
* ``TestCaseFolding``         — step 10.4.5, settled with corpus evidence
* ``TestTierThreeWiring``     — the first real MATCHED(semantic), and the fallback
* ``TestCorpusGuardChecks``   — the whole corpus, guard only, no model needed
* ``TestCorpusEndToEnd``      — the whole corpus through all four tiers
* ``TestCorpusCrossTenant`` / ``TestCorpusVersionUpdate`` — those two properties
* ``TestWholeCorpus``         — every example's records in ONE registry at once
* ``TestGuardLatency``        — §18's ``tier="guard"``, measured

The split down the middle is deliberate. The guard is deterministic string
comparison and needs no model, so **most of the corpus is asserted without one**
— every adversarial example that a deterministic check refuses is refused in
``TestCorpusGuardChecks``, which runs everywhere. Only the classes that are
genuinely about similarity (τ, retrieval, ranking) need the real encoder, and
those skip when it is absent, matching Phase 10.3's convention.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Optional

import pytest

import corpus_loader as cl
from pulsekv_gateway.auditlog import InMemoryAuditLog
from pulsekv_gateway.encoder import OnnxEncoder
from pulsekv_gateway.guardrail import (
    SIMILARITY_THRESHOLD,
    Guardrail,
    analyze,
    extract_entities,
    extract_polarity,
)
from pulsekv_gateway.matcher import DEFAULT_GUARD_TOP_N, Matcher
from pulsekv_gateway.models import (
    BlockType,
    Candidate,
    ContextBlock,
    GuardOutcome,
    MatchMethod,
    MatchOutcome,
    RejectionReason,
)
from pulsekv_gateway.normalizer import hash_normalized, normalize_for_hash
from pulsekv_gateway.registry import Registry

MODEL_DIR = os.environ.get("PULSEKV_GATEWAY_MODEL_DIR", "")
needs_model = pytest.mark.skipif(
    not (MODEL_DIR and (Path(MODEL_DIR) / "model.onnx").is_file()),
    reason="real model absent; set PULSEKV_GATEWAY_MODEL_DIR (see gateway/README.md)",
)

NAMESPACE = "acme"


# ---------------------------------------------------------------------------
# Unit-level fixtures: no registry, no encoder, no index. The guard compares
# two strings and a block type, so a candidate can be built by hand.
# ---------------------------------------------------------------------------


def block(text: str, block_type: BlockType = BlockType.ORG_POLICY) -> ContextBlock:
    return ContextBlock(index=0, block_type=block_type, text=text)


def candidate(
    text: str,
    block_type: BlockType = BlockType.ORG_POLICY,
    *,
    similarity: float = 1.0,
    namespace: str = NAMESPACE,
    context_id: str = "policy",
    version: int = 1,
) -> Candidate:
    return Candidate(
        record=cl.build_record(
            {
                "context_id": context_id,
                "version": version,
                "namespace": namespace,
                "block_type": block_type.value,
                "canonical_text": text,
            }
        ),
        similarity=similarity,
    )


@pytest.fixture
def guard():
    with Guardrail() as instance:
        yield instance


def verdict(guard: Guardrail, incoming: str, registered: str, **kw) -> GuardOutcome:
    block_type = kw.pop("block_type", BlockType.ORG_POLICY)
    query_type = kw.pop("query_type", block_type)
    return guard.check(
        block(incoming, query_type), candidate(registered, block_type, **kw)
    )


# ---------------------------------------------------------------------------


class TestNegationCheck:
    """Design doc §12.1. Each reject is paired with the adjacent case it must
    still pass, so a check that simply refuses everything cannot pass this."""

    def test_a_dropped_negation_is_refused(self, guard):
        result = verdict(
            guard, "The agent must modify IAM roles.",
            "The agent must not modify IAM roles.",
        )
        assert result.outcome is GuardOutcome.REJECTED
        assert result.rejection_reason is RejectionReason.NEGATION_MISMATCH

    def test_the_same_sentence_with_its_negation_intact_passes(self, guard):
        # Adjacent case: same shape, same entity, negation preserved through a
        # rewording that is not a negation change.
        assert verdict(
            guard, "IAM roles must not be modified by the agent.",
            "The agent must not modify IAM roles.",
        ).outcome is GuardOutcome.PASSED

    def test_a_contraction_counts_as_the_negation_it_is(self, guard):
        # Design doc §12 asks for "common contractions". Both directions: the
        # contraction must not be mistaken for an absent negation...
        assert verdict(
            guard, "You can deploy on Fridays.", "You can't deploy on Fridays."
        ).outcome is GuardOutcome.REJECTED
        # ...and must not be mistaken for a different one either.
        assert verdict(
            guard, "Don't push directly to main.", "Do not push directly to main."
        ).outcome is GuardOutcome.PASSED

    def test_an_added_exception_clause_is_refused(self, guard):
        assert verdict(
            guard, "Delete stale branches unless they are protected.",
            "Delete stale branches.",
        ).rejection_reason is RejectionReason.NEGATION_MISMATCH

    def test_a_negation_added_to_one_clause_of_two_is_refused(self, guard):
        """Both texts contain 'Never'; only the count differs.

        A presence-set comparison -- "is the marker in one and absent in the
        other" -- passes this pair. Comparing multisets is what refuses it, and
        this is the test that would fail if the guard were weakened to sets.
        """
        result = verdict(
            guard,
            "Never delete production data. Never delete staging data weekly.",
            "Never delete production data. Delete staging data weekly.",
        )
        assert result.outcome is GuardOutcome.REJECTED
        assert result.rejection_reason is RejectionReason.NEGATION_MISMATCH

    def test_synonyms_inside_one_polarity_family_pass(self, guard):
        # 'allowed' and 'permitted' are the same family, so a paraphrase that
        # swaps them costs nothing...
        assert verdict(
            guard, "Read-only queries are permitted.", "Read-only queries are allowed."
        ).outcome is GuardOutcome.PASSED

    def test_crossing_a_polarity_family_boundary_does_not(self, guard):
        # ...while 'allowed' against 'denied' crosses one, with no negation
        # marker anywhere for a marker-only check to find.
        assert verdict(
            guard, "Read-only queries are denied.", "Read-only queries are allowed."
        ).rejection_reason is RejectionReason.NEGATION_MISMATCH

    def test_a_reversed_order_relation_is_refused(self, guard):
        # Plan §8's named before/after failure mode.
        assert verdict(
            guard, "Run migrations after restarting the API servers.",
            "Run migrations before restarting the API servers.",
        ).rejection_reason is RejectionReason.NEGATION_MISMATCH

    def test_a_reversed_threshold_is_refused(self, guard):
        assert verdict(
            guard, "Alert when latency is below 250 ms.",
            "Alert when latency is above 250 ms.",
        ).rejection_reason is RejectionReason.NEGATION_MISMATCH

    def test_extraction_is_case_insensitive_for_polarity_terms(self):
        # Polarity terms are function words; their case is rendering. Entities
        # are the opposite -- see TestCaseFolding.
        assert extract_polarity("Never delete.") == extract_polarity("never delete.")


class TestEntityCheck:
    """Design doc §12.2."""

    def test_an_environment_swap_is_refused(self, guard):
        result = verdict(
            guard, "Run migrations against the production database first.",
            "Run migrations against the staging database first.",
        )
        assert result.outcome is GuardOutcome.REJECTED
        assert result.rejection_reason is RejectionReason.ENTITY_MISMATCH

    def test_the_same_environment_survives_a_rewording(self, guard):
        assert verdict(
            guard, "Against the staging database, run migrations first.",
            "Run migrations against the staging database first.",
        ).outcome is GuardOutcome.PASSED

    def test_a_command_flag_swap_is_refused(self, guard):
        assert verdict(
            guard, "Always run the sync tool with --force.",
            "Always run the sync tool with --dry-run.",
        ).rejection_reason is RejectionReason.ENTITY_MISMATCH

    def test_a_number_swap_is_refused(self, guard):
        assert verdict(
            guard, "Retain audit logs for 30 days.", "Retain audit logs for 90 days."
        ).rejection_reason is RejectionReason.ENTITY_MISMATCH

    def test_the_check_compares_for_equality_not_superset(self, guard):
        """Design doc §12 states the rule as superset-or-equal; this is stricter.

        §12: the candidate's entities "must be a superset-or-equal set of what's
        semantically load-bearing in the incoming block". Read literally, a
        registered text mentioning MORE environments than the block satisfies
        it -- and substituting that text extends a deletion instruction from
        staging to production, which is the exact failure §12 exists to
        prevent. The guard compares for equality, which is strictly more
        reject-biased and is what §12's own failure-bias paragraph authorises.
        """
        incoming = "Delete unused resources in staging."
        registered = "Delete unused resources in staging and production."
        assert set(extract_entities(incoming)) < set(extract_entities(registered))
        assert verdict(
            guard, incoming, registered
        ).rejection_reason is RejectionReason.ENTITY_MISMATCH

    def test_repeating_an_entity_is_not_a_difference(self, guard):
        # Entities are compared as sets, not multisets: saying 'production'
        # twice is emphasis, not a second production. (Polarity is the
        # opposite -- see the added-clause test above.)
        assert verdict(
            guard,
            "Never touch production. Production is off limits to agents.",
            "Agents must never touch production.",
        ).outcome is GuardOutcome.PASSED

    def test_a_cadence_is_treated_as_a_value(self, guard):
        # Measured at 0.8911, above the lowest-scoring genuine paraphrase in the
        # corpus -- so τ cannot be raised to cover this without refusing real
        # matches, and the value lexicon has to carry it.
        assert verdict(
            guard, "Rotate service credentials monthly.",
            "Rotate service credentials quarterly.",
        ).rejection_reason is RejectionReason.ENTITY_MISMATCH

    def test_sentence_case_is_not_mistaken_for_a_proper_noun(self):
        # If it were, two paraphrases starting with different words would never
        # agree on an entity set, and the check would refuse everything.
        assert extract_entities("Never delete a resource.") == frozenset()
        # An uppercase letter sentence case cannot explain is a name wherever
        # it appears.
        assert "name:GitHub" in extract_entities("Use GitHub for this.")
        assert "name:IAM" in extract_entities("IAM roles are managed centrally.")

    def test_a_capitalized_word_mid_sentence_is_a_name(self):
        assert "name:Fridays" in extract_entities("Do not deploy on Fridays.")


class TestTypeCheck:
    """Design doc §12.3, and the question of whether it is already redundant."""

    def test_a_cross_type_candidate_is_refused(self, guard):
        result = guard.check(
            block("Deleting a resource requires approval.", BlockType.TOOL_SCHEMA),
            candidate("Deleting a resource requires approval.", BlockType.ORG_POLICY),
        )
        assert result.outcome is GuardOutcome.REJECTED
        assert result.rejection_reason is RejectionReason.TYPE_MISMATCH

    def test_the_same_pair_at_the_same_type_passes(self, guard):
        assert guard.check(
            block("Deleting a resource requires approval.", BlockType.ORG_POLICY),
            candidate("Deleting a resource requires approval.", BlockType.ORG_POLICY),
        ).outcome is GuardOutcome.PASSED

    def test_type_is_checked_before_anything_else(self, guard):
        # A pair that would also fail the negation check reports the type, not
        # the negation: the cheapest and most decisive check short-circuits.
        assert guard.check(
            block("Never delete.", BlockType.TOOL_SCHEMA),
            candidate("Always delete.", BlockType.ORG_POLICY),
        ).rejection_reason is RejectionReason.TYPE_MISMATCH

    @needs_model
    def test_tier_two_really_does_make_this_redundant(self, real_encoder, tmp_path):
        """Step 10.4.3 asks for confirmation, not an assumption.

        Two near-identical records under two block types, and a query whose
        vector is far closer to the wrong-typed one than any threshold would
        separate. If Tier 2's partition were a post-hoc filter that someone
        reordered away, the ORG_POLICY record would be a live candidate for an
        AGENT_INSTRUCTION block and only §12.3 would stop it. This test proves
        the partition holds *and* names what would catch the failure if it did
        not -- which is the whole argument for keeping a redundant check.

        The two texts are near-identical rather than identical for a reason
        worth recording: the registry's live-content-hash uniqueness is scoped
        to the namespace and does NOT include block type, because Tier 0's hash
        lookup does not either. So one namespace cannot hold the same text
        under two types at all, and the type check is one layer further from
        reachable than it looks.
        """
        policy = "Deleting a resource requires approval."
        instruction = "Deleting a resource requires an approval."
        registry = Registry(tmp_path / "types.db", hash_text=hash_normalized)
        try:
            stored = cl.populate(
                registry,
                [
                    {"context_id": "as-policy", "version": 1, "namespace": NAMESPACE,
                     "block_type": "org_policy", "canonical_text": policy},
                    {"context_id": "as-instruction", "version": 1,
                     "namespace": NAMESPACE, "block_type": "agent_instruction",
                     "canonical_text": instruction},
                ],
                real_encoder,
            )
            index = cl.build_index(registry, real_encoder, [NAMESPACE])
            assert len(index) == 2

            query = real_encoder.encode(normalize_for_hash(policy))
            found = index.find_candidates(
                query, namespace=NAMESPACE, block_type=BlockType.AGENT_INSTRUCTION,
                top_k=10,
            )
            # Retrieval already refuses it: the org_policy record is in the
            # index and scores 1.0 against this query, and is still not here.
            assert [c.record.context_id for c in found] == ["as-instruction"]

            # And if it ever were, the guard refuses it -- checked by handing
            # over the candidate retrieval declined to produce.
            leaked = Candidate(
                record=stored[(NAMESPACE, "as-policy", 1)], similarity=1.0
            )
            with Guardrail() as second:
                refused = second.check(
                    block(policy, BlockType.AGENT_INSTRUCTION), leaked
                )
            assert refused.rejection_reason is RejectionReason.TYPE_MISMATCH
        finally:
            registry.close()


class TestGuardIsRejectBiased:
    """Design doc §12's failure bias and §17's guard row, proven not asserted."""

    class Exploding(Guardrail):
        def _check(self, *_args):
            raise RuntimeError("guard blew up")

    class Slow(Guardrail):
        def _check(self, *args):
            time.sleep(0.5)
            return super()._check(*args)

    def test_an_exception_inside_the_guard_becomes_a_reject(self):
        with self.Exploding() as broken:
            result = broken.check(block("anything"), candidate("anything"))
        assert result.outcome is GuardOutcome.ERROR
        assert result.rejection_reason is RejectionReason.GUARD_ERROR

    def test_check_never_raises(self):
        # It cannot: a caller that had to catch would need its own reject path,
        # and §12 says the reject path is this module's guarantee.
        with self.Exploding() as broken:
            assert broken.check(None, None).outcome is GuardOutcome.ERROR

    def test_a_guard_past_its_budget_becomes_a_timeout_reject(self):
        with self.Slow(timeout_ms=50) as slow:
            start = time.perf_counter()
            result = slow.check(block("anything"), candidate("anything"))
            elapsed = time.perf_counter() - start
        assert result.outcome is GuardOutcome.TIMEOUT
        assert result.rejection_reason is RejectionReason.GUARD_TIMEOUT
        # The caller really was released early, not merely told afterwards.
        assert elapsed < 0.4, f"waited {elapsed:.3f}s for a 50 ms budget"

    def test_there_is_no_reduced_confidence_accept(self):
        # Every GuardOutcome that is not PASSED must carry a reason; the
        # contract has no half-accept and this asserts the enum has not grown
        # one. Design doc §12: "There is no 'reduced confidence' substitution
        # mode."
        assert {o for o in GuardOutcome if o is not GuardOutcome.PASSED} == {
            GuardOutcome.REJECTED, GuardOutcome.ERROR, GuardOutcome.TIMEOUT
        }

    def test_a_guard_that_raises_still_rejects_at_the_matcher(self, tmp_path):
        """The reject bias must not depend on the guard being this one.

        ``Guardrail.check`` converts its own failures. A *substituted* guard --
        a subclass, a double, a Phase 10.5 wrapper -- has no such guarantee, so
        the matcher wraps it too. Proven with a guard that raises out of
        ``check`` itself rather than out of ``_check``.
        """

        class RaisesFromCheck(Guardrail):
            def check(self, *_args, **_kwargs):
                raise RuntimeError("a guard that does not honour the contract")

        registry = Registry(tmp_path / "r.db", hash_text=hash_normalized)
        encoder = _ConstantEncoder()
        try:
            record = cl.build_record(
                {"context_id": "p", "version": 1, "namespace": NAMESPACE,
                 "block_type": "org_policy", "canonical_text": "registered text"},
                encoder,
            )
            registry.register(record)
            index = cl.build_index(registry, encoder, [NAMESPACE])
            matcher = Matcher(
                registry, encoder=encoder, index=index, guardrail=RaisesFromCheck()
            )
            result = matcher.resolve(block("an incoming block"), NAMESPACE)
            assert result.outcome is MatchOutcome.REJECTED
            assert result.rejection_reason is RejectionReason.GUARD_ERROR
            assert result.substitutes is False
        finally:
            registry.close()
            encoder.close()

    def test_a_cross_namespace_candidate_is_an_error_not_a_quiet_pass(self, guard):
        # Reaching the guard with another tenant's record means retrieval is
        # broken (design doc §15 partitions it away), so it is reported as a
        # defect -- and a defect is still a reject.
        result = guard.check(
            block("policy text"),
            candidate("policy text", namespace="globex"),
            namespace="acme",
        )
        assert result.outcome is GuardOutcome.ERROR
        assert result.rejection_reason is RejectionReason.GUARD_ERROR
        assert "globex" in result.detail

    def test_the_namespace_check_is_opt_in_and_silent_otherwise(self, guard):
        # Phase 10.0 froze `check(block, candidate)`; the namespace argument is
        # additive, and a caller that does not pass it gets Phase 10.0's
        # behavior rather than a surprise error.
        assert guard.check(
            block("policy text"), candidate("policy text", namespace="globex")
        ).outcome is GuardOutcome.PASSED


class TestGuardReadsFullText:
    """Phase 10.3's second finding, acted on rather than noted."""

    def test_a_difference_past_the_truncation_boundary_is_still_seen(self, guard):
        example = _corpus("adversarial_negative", "truncation-boundary-negation")
        registered = cl.text_of(example.records[0]["canonical_text"])
        incoming = cl.text_of(example.query["text"])
        # The two texts are identical for their first 512 tokens; only the last
        # paragraph differs. The encoder cannot see the difference at all --
        # TestCorpusEndToEnd asserts the vectors are byte-identical -- and the
        # guard, reading the whole string, refuses it.
        assert registered[:2000] == incoming[:2000]
        assert guard.check(
            block(incoming, BlockType.RAG_DOCUMENT),
            candidate(registered, BlockType.RAG_DOCUMENT),
        ).rejection_reason is RejectionReason.NEGATION_MISMATCH

    def test_the_guard_reads_canonical_text_not_a_normalized_reduction(self, guard):
        # normalize_for_hash collapses blank-line runs and trailing whitespace.
        # Those are rendering, so the guard's verdict must not change with them
        # -- but the guard must not be *routed through* the hash normalizer
        # either, or a reader would reasonably infer it sees a reduced form.
        plain = "Never delete production."
        padded = "Never delete production.   \n\n\n"
        assert analyze(plain).polarity == analyze(padded).polarity
        assert analyze(plain).entities == analyze(padded).entities


class TestCaseFolding:
    """Step 10.4.5, settled with corpus evidence rather than re-argued.

    Phase 10.2 declined to case-fold in ``normalize_for_hash`` and named the
    reason: a Tier 0/1 hit has no guard behind it, so a normalization that can
    collapse two distinct entities is more dangerous there than at Tier 2. It
    also named where the question should be settled -- "Phase 10.4's
    adversarial-negative corpus ... a case-only entity swap is precisely the
    shape that set will contain". It does, and it settles it the same way, for
    a reason Phase 10.2 could not have known.
    """

    def test_entities_are_compared_case_sensitively(self, guard):
        result = verdict(
            guard, "Set AWS_PROFILE=production before running the deploy script.",
            "Set AWS_PROFILE=Production before running the deploy script.",
        )
        assert result.outcome is GuardOutcome.REJECTED
        assert result.rejection_reason is RejectionReason.ENTITY_MISMATCH

    def test_an_environment_word_keeps_its_case_through_extraction(self):
        # The lexicon is consulted case-insensitively, so `Production` is
        # recognised as an environment at all -- but the entity it produces
        # keeps the case it was written in, so the two do not compare equal.
        assert extract_entities("Deploy to Production now.") == {"val:Production"}
        assert extract_entities("Deploy to production now.") == {"val:production"}

    @needs_model
    def test_the_encoder_cannot_see_case_at_all(self, real_encoder):
        """The measurement that decides it: the tokenizer is uncased.

        A case-only swap does not merely score high -- it produces
        **byte-identical vectors**, similarity exactly 1.0000, tied with a
        genuine paraphrase in the corpus at the same number. So τ cannot refuse
        it at any value without refusing that paraphrase too, and Tier 2 cannot
        flag it either. The entity check is the only layer that sees it.
        """
        upper = "Set AWS_PROFILE=Production before running the deploy script."
        lower = "Set AWS_PROFILE=production before running the deploy script."
        assert real_encoder.encode(normalize_for_hash(upper)) == real_encoder.encode(
            normalize_for_hash(lower)
        )

    def test_folding_in_the_hash_normalizer_would_collide_at_tier_zero(self):
        """Why the answer is still 'no' even though Tier 3 now catches it.

        The guard is a backstop for Tier 2/3, and design doc §11 is explicit
        that it "never runs on Tier 0/1 hits". If ``normalize_for_hash`` folded
        case, these two blocks would hash identically, Tier 0 would substitute
        one for the other, and the check that catches the pair at Tier 3 would
        never be consulted. The guard's existence therefore does not weaken
        Phase 10.2's argument -- it is evidence for it, because it shows the
        pair is dangerous enough to need catching.
        """
        upper = "Set AWS_PROFILE=Production before running the deploy script."
        lower = "Set AWS_PROFILE=production before running the deploy script."
        assert hash_normalized(upper) != hash_normalized(lower)
        assert hash_normalized(upper.casefold()) == hash_normalized(lower.casefold())


class _ConstantEncoder:
    """A two-vector encoder: every text embeds to one of two unit vectors.

    Deliberately degenerate. Similarity is not the variable a Tier 3 test wants
    to vary, and a stub that scored plausibly would quietly become the thing
    being measured -- so this one scores exactly 1.0 by default, and exactly
    0.6 for a text carrying the marker ``faraway``, which is how a below-τ
    candidate is constructed without pretending to be semantic. It lives here
    rather than in the package for the same reason Phase 10.3's StubEncoder
    does: a non-semantic encoder must not be configurable into a deployment by
    accident.
    """

    model_id = "constant-encoder"
    model_version = "2"
    dimension = 4

    def encode(self, text):
        if "faraway" in text:
            return (0.6, 0.8, 0.0, 0.0)
        return (1.0, 0.0, 0.0, 0.0)

    def count_tokens(self, text):
        return len(text.split())

    max_sequence_tokens = 512

    def close(self):
        pass


def _corpus(category: str, example_id: str) -> cl.Example:
    for example in cl.load(category):
        if example.id == example_id:
            return example
    raise AssertionError(f"{category}/{example_id}: not in the corpus")


class TestTierThreeWiring:
    """The matcher end of it: the first real MATCHED(semantic) this project has
    produced, and the fallback that every rejection lands in."""

    @pytest.fixture
    def wired(self, tmp_path):
        registry = Registry(tmp_path / "t3.db", hash_text=hash_normalized)
        encoder = _ConstantEncoder()
        cl.populate(
            registry,
            [{"context_id": "prod-deletion", "version": 3, "namespace": NAMESPACE,
              "block_type": "org_policy",
              "canonical_text": "Never delete a production resource."}],
            encoder,
        )
        index = cl.build_index(registry, encoder, [NAMESPACE])
        yield Matcher(registry, encoder=encoder, index=index)
        registry.close()

    def test_a_paraphrase_resolves_to_a_semantic_match(self, wired):
        result = wired.resolve(block("A production resource is never deleted."), NAMESPACE)
        assert result.outcome is MatchOutcome.MATCHED
        assert result.method is MatchMethod.SEMANTIC
        assert (result.context_id, result.version) == ("prod-deletion", 3)
        assert result.confidence == pytest.approx(1.0)
        assert result.substitutes is True

    def test_an_adversarial_block_falls_through_to_its_original_text(self, wired):
        """Reject -> the original block is what goes forward, byte for byte.

        The assembler is Phase 10.5's, so the substitution step is written out
        here rather than imported: its documented contract is that "a block
        with no entry is emitted byte-identical to its input". This is that
        contract applied to a real Tier 3 rejection.
        """
        incoming = "A production resource is always deleted."
        result = wired.resolve(block(incoming), NAMESPACE)
        assert result.outcome is MatchOutcome.REJECTED
        assert result.rejection_reason is RejectionReason.NEGATION_MISMATCH
        assert (result.context_id, result.version) == ("prod-deletion", 3)
        assert result.confidence == pytest.approx(1.0)

        substitutions = {0: "REPLACED"} if result.substitutes else {}
        forwarded = substitutions.get(0, incoming)
        assert forwarded == incoming
        assert forwarded is incoming  # not a copy, not a re-render

    def test_the_decision_log_records_a_semantic_match_completely(self, wired):
        audit = InMemoryAuditLog()
        wired.resolve_blocks(
            (block("A production resource is never deleted."),),
            namespace=NAMESPACE, request_id="req-m", timestamp=cl.CORPUS_NOW,
            model="gpt-4o", audit=audit,
        )
        record = audit.for_request("req-m")[0]
        assert record.decision_label == "semantic"
        assert record.guard_outcome is GuardOutcome.PASSED
        assert record.similarity == pytest.approx(1.0)
        assert (record.context_id, record.version) == ("prod-deletion", 3)

    def test_the_decision_log_records_a_rejection_with_its_reason(self, wired):
        audit = InMemoryAuditLog()
        wired.resolve_blocks(
            (block("A production resource is always deleted."),),
            namespace=NAMESPACE, request_id="req-r", timestamp=cl.CORPUS_NOW,
            model="gpt-4o", audit=audit,
        )
        record = audit.for_request("req-r")[0]
        assert record.decision_label == "rejected"
        assert record.rejection_reason is RejectionReason.NEGATION_MISMATCH
        assert record.guard_outcome is GuardOutcome.REJECTED
        assert record.similarity == pytest.approx(1.0)

    def test_below_tau_is_a_rejection_at_the_gate_not_a_guard_verdict(self, tmp_path):
        # models.py requires guard_outcome to be unset for low_similarity: the
        # candidate was refused *by the τ gate*, and that stays true even
        # though the guard ran first -- its opinion changed nothing.
        registry = Registry(tmp_path / "tau.db", hash_text=hash_normalized)
        encoder = _ConstantEncoder()
        try:
            cl.populate(
                registry,
                [{"context_id": "p", "version": 1, "namespace": NAMESPACE,
                  "block_type": "org_policy", "canonical_text": "registered text"}],
                encoder,
            )
            index = cl.build_index(registry, encoder, [NAMESPACE])
            matcher = Matcher(registry, encoder=encoder, index=index)
            audit = InMemoryAuditLog()
            # The marker puts the query on the encoder's other vector: one
            # candidate, retrieved, scored 0.6, below τ.
            results = matcher.resolve_blocks(
                (block("an incoming block faraway"),),
                namespace=NAMESPACE, request_id="req-t", timestamp=cl.CORPUS_NOW,
                model="gpt-4o", audit=audit,
            )
            assert results[0].rejection_reason is RejectionReason.LOW_SIMILARITY
            assert results[0].confidence == pytest.approx(0.6)
            assert audit.for_request("req-t")[0].guard_outcome is None
        finally:
            registry.close()
            encoder.close()

    def test_a_matcher_without_tier_two_never_reaches_tier_three(self, tmp_path):
        registry = Registry(tmp_path / "t0.db", hash_text=hash_normalized)
        try:
            plain = Matcher(registry)
            assert plain.semantic_enabled is False
            assert plain.resolve(block("unregistered"), NAMESPACE).outcome is (
                MatchOutcome.NO_CANDIDATE
            )
        finally:
            registry.close()

    def test_a_nonsense_guard_depth_is_refused_rather_than_ignored(self, tmp_path):
        registry = Registry(tmp_path / "t0.db", hash_text=hash_normalized)
        try:
            with pytest.raises(ValueError):
                Matcher(registry, guard_top_n=0)
            with pytest.raises(ValueError):
                Matcher(registry, similarity_threshold=1.5)
        finally:
            registry.close()


# ---------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------


def _guard_expectation(example: cl.Example) -> Optional[RejectionReason]:
    """What the guard alone should say about an example, or None for "passes".

    An adversarial example whose expected reason is ``low_similarity`` must
    PASS the guard -- that is the point of it: it belongs to the class the
    deterministic checks cannot see, and if a check started refusing it, τ
    would no longer be earning anything. Positives are handled separately,
    against their file's own ``guard_verdict`` (exactly one of them records a
    refusal, and explains it).
    """
    if example.guard_direct and example.guard_direct.get("expect_rejection_reason"):
        # A failure retrieval partitions away entirely: the pipeline outcome is
        # a miss, and the guard is the only place the reason can be asserted.
        return RejectionReason(example.guard_direct["expect_rejection_reason"])
    reason = example.expect.get("rejection_reason")
    if reason in (None, "low_similarity"):
        return None
    return RejectionReason(reason)


class TestCorpusGuardChecks:
    """Every example the deterministic checks can decide, decided -- no model.

    This is the half of plan §8's "zero false positives, confirmed by test"
    that does not depend on an encoder being installed, and it is most of it:
    19 of the 25 adversarial examples are refused by a check that never looks
    at a vector.
    """

    @pytest.mark.parametrize(
        "example", cl.load("adversarial_negative"), ids=lambda e: e.id
    )
    def test_adversarial_examples_are_refused_for_the_stated_reason(
        self, guard, example
    ):
        expected = _guard_expectation(example)
        record = cl.build_record(example.records[0])
        result = guard.check(
            example.block, Candidate(record=record, similarity=1.0)
        )
        assert result.outcome is not GuardOutcome.ERROR, result.detail
        if expected is None:
            # A tau-* example: the guard must find nothing, or the example is
            # silently testing the guard instead of the threshold.
            assert result.outcome is GuardOutcome.PASSED, (
                f"{example.id}: refused as {result.rejection_reason} -- this "
                f"example exists to prove τ does something, and a guard check "
                f"that refuses it means τ is no longer what refuses it"
            )
            assert example.expect["rejection_reason"] == "low_similarity"
        else:
            assert result.outcome is GuardOutcome.REJECTED, (
                f"{example.id}: {cl.text_of(example.query['text'])[:60]!r} was "
                f"not refused"
            )
            assert result.rejection_reason is expected, (
                f"{example.id}: refused for {result.rejection_reason.value}, "
                f"corpus says {expected.value} -- a negative that passes for "
                f"the wrong reason is a test that is not testing anything"
            )

    @pytest.mark.parametrize(
        "example", cl.load("positive_paraphrase"), ids=lambda e: e.id
    )
    def test_positive_examples_get_the_verdict_their_file_records(
        self, guard, example
    ):
        payload = _payload(example)
        expected = payload.get("guard_verdict", "passed")
        record = cl.build_record(example.records[0])
        result = guard.check(
            example.block, Candidate(record=record, similarity=1.0)
        )
        assert result.outcome.value == expected, (
            f"{example.id}: guard said {result.outcome.value} "
            f"({result.detail}), corpus records {expected}"
        )
        if expected != "passed":
            assert result.rejection_reason.value == payload["guard_rejection_reason"]

    def test_the_guard_only_pass_covers_most_of_the_adversarial_suite(self):
        # Stated as a number so it cannot quietly drift to "none of it".
        adversarial = cl.load("adversarial_negative")
        deterministic = [e for e in adversarial if _guard_expectation(e) is not None]
        assert len(deterministic) >= 18
        assert len(deterministic) / len(adversarial) > 0.7


def _payload(example: cl.Example) -> dict:
    """The raw file, for the few fields the ``Example`` view does not carry."""
    return json.loads(example.path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# The corpus, through all four tiers. Needs the real encoder.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def real_encoder():
    """The real model, loaded once per module (90 MB and a graph build)."""
    encoder = OnnxEncoder(MODEL_DIR)
    yield encoder
    encoder.close()


class _Env:
    """One isolated registry+index per example, plus one holding all of them.

    Built once per module. Every example gets its own store because an
    example's stated property is about its own pair -- and then
    ``TestWholeCorpus`` puts every record in one registry, which is where the
    interesting failures live (a query whose nearest neighbour turns out to
    belong to a different example).
    """

    def __init__(self, encoder, tmp_root: Path) -> None:
        self.encoder = encoder
        self.isolated = {}
        for example in cl.load():
            registry = Registry(
                tmp_root / f"{example.category}-{example.id}.db",
                hash_text=hash_normalized,
            )
            stored = cl.populate(registry, example.records, encoder)
            index = cl.build_index(
                registry, encoder, cl.namespaces_of([example])
            )
            self.isolated[example.id] = (registry, index, stored)

        every = cl.load()
        self.whole_registry = Registry(
            tmp_root / "whole-corpus.db", hash_text=hash_normalized
        )
        self.whole_stored = cl.populate(
            self.whole_registry,
            [spec for example in every for spec in example.records],
            encoder,
        )
        self.whole_index = cl.build_index(
            self.whole_registry, encoder, cl.namespaces_of(every)
        )

    def matcher(self, example_id: str, **kw) -> Matcher:
        registry, index, _ = self.isolated[example_id]
        return Matcher(registry, encoder=self.encoder, index=index, **kw)

    def resolve(self, example: cl.Example, **kw):
        return self.matcher(example.id, **kw).resolve_with_candidates(
            example.block, example.namespace
        )

    def close(self) -> None:
        for registry, _, _ in self.isolated.values():
            registry.close()
        self.whole_registry.close()


@pytest.fixture(scope="module")
def env(real_encoder, tmp_path_factory):
    built = _Env(real_encoder, tmp_path_factory.mktemp("corpus"))
    yield built
    built.close()


@needs_model
class TestCorpusEndToEnd:
    """Plan §8's exit criteria 1-3, against the real encoder."""

    @pytest.mark.parametrize(
        "example", cl.load("adversarial_negative"), ids=lambda e: e.id
    )
    def test_no_adversarial_example_is_ever_matched(self, env, example):
        """**The** exit criterion: zero false positives, confirmed by test.

        Stated separately from the reason check below because they are two
        different claims. This one is the safety property and would still have
        to hold if every rejection reason in the corpus were wrong.
        """
        result, candidates = env.resolve(example)
        assert result.outcome is not MatchOutcome.MATCHED, (
            f"{example.id}: FALSE POSITIVE -- matched {result.context_id} "
            f"v{result.version} at {result.confidence:.4f}. {example.why}"
        )
        assert result.substitutes is False

    @pytest.mark.parametrize(
        "example", cl.load("adversarial_negative"), ids=lambda e: e.id
    )
    def test_adversarial_examples_are_refused_for_the_stated_reason(self, env, example):
        # The corpus README: a negative must name "the specific RejectionReason
        # expected, so a test that passes for the wrong reason fails".
        result, candidates = env.resolve(example)
        assert result.outcome is cl.expected_outcome(example.expect), (
            f"{example.id}: {cl.describe(result, candidates)}"
        )
        expected = cl.expected_reason(example.expect)
        if expected is not None:
            assert result.rejection_reason is expected, (
                f"{example.id}: refused for "
                f"{result.rejection_reason and result.rejection_reason.value}, "
                f"corpus says {expected.value}"
            )
        for forbidden in example.expect.get("never_retrieves", ()):
            assert not any(
                (c.record.namespace, c.record.context_id, c.record.version)
                == (forbidden["namespace"], forbidden["context_id"],
                    forbidden["version"])
                for c in candidates
            ), f"{example.id}: {forbidden} was retrieved"

    def test_positive_paraphrase_match_rate_is_measured_and_reported(
        self, env, capsys
    ):
        """Plan §8: reported honestly, not gated at a number.

        Two things *are* asserted, because neither is about the rate. First,
        that a positive which matches matched the record its file names -- a
        match to some other record is a different event and must not be counted
        as a success. Second, that the rate is not zero, which would mean the
        guard refuses everything and the suite had stopped measuring anything.
        """
        rows, matched = [], 0
        for example in cl.load("positive_paraphrase"):
            result, candidates = env.resolve(example)
            top = candidates[0].similarity if candidates else float("nan")
            if result.outcome is MatchOutcome.MATCHED:
                assert (result.context_id, result.version) == (
                    example.expect["context_id"], example.expect["version"]
                ), f"{example.id}: matched the wrong record"
                matched += 1
            rows.append((example.id, top, cl.describe(result, candidates)))

        with capsys.disabled():
            print(f"\n  positive_paraphrase, tau={SIMILARITY_THRESHOLD}:")
            for name, top, outcome in rows:
                print(f"    {top:7.4f}  {outcome:40s} {name}")
            print(f"    match rate: {matched}/{len(rows)} = "
                  f"{100 * matched / len(rows):.0f}%")
        assert matched > 0

    def test_tau_is_earned_by_the_adversarial_suite_not_chosen_before_it(self, env):
        """τ's derivation, as an assertion rather than a claim in a document.

        The adversarial suite contains a class the deterministic checks cannot
        refuse -- lowercase job titles, team names, verbs. Those pairs are what
        τ is for, and this asserts the number is above them: dropping τ to just
        under that class's ceiling produces a real false positive, and the
        shipped τ produces none. A threshold picked from folklore would fail
        the first half of this test, not the second.
        """
        adversarial = cl.load("adversarial_negative")

        def false_positives(tau: float) -> list:
            return [
                e.id for e in adversarial
                if env.matcher(e.id, similarity_threshold=tau)
                .resolve(e.block, e.namespace).outcome is MatchOutcome.MATCHED
            ]

        assert false_positives(SIMILARITY_THRESHOLD) == []
        # 0.80 is below the guard-blind class's ceiling. Something gets through.
        assert false_positives(0.80), (
            "no τ value in this corpus produces a false positive, which would "
            "mean the deterministic checks refuse everything and τ is not "
            "earning anything -- see the tau-* examples"
        )

    def test_the_truncation_pair_really_does_embed_identically(
        self, env, real_encoder
    ):
        """Phase 10.3's finding 2, restated over corpus text.

        Not "scores high": byte-identical vectors. Whatever the encoder is
        doing past token 512, it is doing none of it, and the guard's
        full-text reading is the only reason this pair is refused.
        """
        example = _corpus("adversarial_negative", "truncation-boundary-negation")
        registered = cl.text_of(example.records[0]["canonical_text"])
        incoming = cl.text_of(example.query["text"])
        assert registered != incoming
        assert real_encoder.count_tokens(registered) == (
            real_encoder.max_sequence_tokens
        )
        assert real_encoder.encode(normalize_for_hash(registered)) == (
            real_encoder.encode(normalize_for_hash(incoming))
        )
        result, candidates = env.resolve(example)
        assert candidates[0].similarity == pytest.approx(1.0)
        assert result.rejection_reason is RejectionReason.NEGATION_MISMATCH

    def test_the_corpus_covers_every_rejection_reason_the_guard_can_produce(self):
        # A corpus that never exercises a reason is a corpus that cannot catch
        # that reason regressing.
        reasons = set()
        for example in cl.load("adversarial_negative"):
            for source in (example.expect, example.guard_direct or {}):
                value = source.get("rejection_reason") or source.get(
                    "expect_rejection_reason"
                )
                if value:
                    reasons.add(RejectionReason(value))
        assert reasons == {
            RejectionReason.NEGATION_MISMATCH,
            RejectionReason.ENTITY_MISMATCH,
            RejectionReason.TYPE_MISMATCH,
            RejectionReason.LOW_SIMILARITY,
        }
        # guard_error/guard_timeout are the two the corpus cannot carry -- they
        # are failures of the guard process, not properties of a text pair.
        # TestGuardIsRejectBiased covers both.


@needs_model
class TestCorpusCrossTenant:
    """The corpus README's property: no candidate from another namespace is
    ever returned, retrieved, or scored (design doc §15, risk register row 3)."""

    @pytest.mark.parametrize("example", cl.load("cross_tenant"), ids=lambda e: e.id)
    def test_the_declared_outcome_holds(self, env, example):
        result, candidates = env.resolve(example)
        assert result.outcome is cl.expected_outcome(example.expect), (
            f"{example.id}: {cl.describe(result, candidates)}"
        )
        if result.outcome is MatchOutcome.MATCHED:
            assert (result.context_id, result.version) == (
                example.expect["context_id"], example.expect["version"]
            )
            _, _, stored = env.isolated[example.id]
            key = (example.namespace, result.context_id, result.version)
            assert stored[key].namespace == example.namespace

    @pytest.mark.parametrize("example", cl.load("cross_tenant"), ids=lambda e: e.id)
    def test_no_other_tenants_record_is_even_scored(self, env, example):
        _, candidates = env.resolve(example)
        for forbidden in example.expect.get("never_retrieves", ()):
            assert not any(
                (c.record.namespace, c.record.context_id, c.record.version)
                == (forbidden["namespace"], forbidden["context_id"],
                    forbidden["version"])
                for c in candidates
            ), f"{example.id}: {forbidden['namespace']} leaked into candidates"
        # The other tenant's record demonstrably exists -- the isolation is not
        # a missing row (the same shape Phase 10.3's TestNamespaceIsolation used).
        _, index, stored = env.isolated[example.id]
        assert len(stored) >= 2 or example.id == "third-tenant-sees-nothing"
        assert all(c.record.namespace == example.namespace for c in candidates)

    def test_the_guard_refuses_a_cross_namespace_candidate_it_cannot_receive(
        self, env
    ):
        # Defense in depth for the one failure whose blast radius is another
        # tenant's text: retrieval partitions it away, and if that partition
        # were ever undone, the guard still refuses. Asserted by handing over
        # the candidate retrieval declined to produce.
        example = _corpus("cross_tenant", "tenant-scoped-resource-names")
        _, _, stored = env.isolated[example.id]
        leaked = cl.candidate_for(example, stored, example.guard_direct)
        assert leaked.record.namespace == "globex"
        with Guardrail() as guard:
            result = guard.check(
                example.block, leaked, namespace=example.namespace
            )
        assert result.rejection_reason is RejectionReason(
            example.guard_direct["expect_rejection_reason"]
        )
        assert result.outcome is GuardOutcome.ERROR


@needs_model
class TestCorpusVersionUpdate:
    """The corpus README's property: an old version's already-logged decisions
    stay interpretable, and publishing never reaches backwards (§10, §17)."""

    @pytest.mark.parametrize("example", cl.load("version_update"), ids=lambda e: e.id)
    def test_the_decision_before_the_bump_is_the_declared_one(self, env, example):
        result, candidates = env.resolve(example)
        assert result.outcome is cl.expected_outcome(example.expect), (
            f"{example.id}: {cl.describe(result, candidates)}"
        )
        assert result.method is cl.expected_method(example.expect)
        assert (result.context_id, result.version) == (
            example.expect["context_id"], example.expect["version"]
        )
        for forbidden in example.expect.get("never_retrieves", ()):
            assert not any(
                (c.record.context_id, c.record.version)
                == (forbidden["context_id"], forbidden["version"])
                for c in candidates
            ), f"{example.id}: deprecated {forbidden} is still a match target"

    @pytest.mark.parametrize(
        "example",
        [e for e in cl.load("version_update") if e.then],
        ids=lambda e: e.id,
    )
    def test_publishing_a_new_version_does_not_reach_backwards(
        self, env, real_encoder, example
    ):
        registry, _, _ = env.isolated[example.id]
        matcher = env.matcher(example.id)
        audit = InMemoryAuditLog()
        matcher.resolve_blocks(
            (example.block,), namespace=example.namespace, request_id="before",
            timestamp=cl.CORPUS_NOW, model="gpt-4o", audit=audit,
        )
        before = audit.for_request("before")[0]
        old_version = before.version
        old_record = registry.get(
            before.context_id, example.namespace, version=old_version
        )

        cl.populate(registry, example.then["publish"], real_encoder)
        # A real gateway rebuilds the index after a publish; so does this.
        index = cl.build_index(registry, real_encoder, [example.namespace])
        after_matcher = Matcher(
            registry, encoder=real_encoder, index=index
        )

        if example.then.get("expect_previous_decision_unchanged"):
            # The record object in the log is frozen; what could change is the
            # registry underneath it. Both are checked.
            assert audit.for_request("before")[0] == before
            still = registry.get(
                before.context_id, example.namespace, version=old_version
            )
            assert still.canonical_text == old_record.canonical_text
            assert still.content_hash == old_record.content_hash
            assert still.version == old_version

        result = after_matcher.resolve(example.block, example.namespace)
        expected = example.then["expect"]
        assert result.outcome is cl.expected_outcome(expected), (
            f"{example.id} after publish: {cl.describe(result)}"
        )
        if expected.get("version") is not None:
            assert (result.context_id, result.version) == (
                expected["context_id"], expected["version"]
            )
        if expected.get("rejection_reason"):
            assert result.rejection_reason is cl.expected_reason(expected)
        assert before.version == old_version  # said twice on purpose


@needs_model
class TestWholeCorpus:
    """Every example's records in ONE registry, which is the realistic shape.

    The per-example tests give each pair its own store, so the top candidate is
    always the record the example is about. Here it need not be: 43 records
    across three namespaces, and a query's nearest neighbour can belong to a
    different example entirely. This is the run that would surface a false
    positive caused by the *registry*, which no isolated test can.
    """

    def test_no_adversarial_query_matches_anything_at_all(self, env, capsys):
        matcher = Matcher(
            env.whole_registry, encoder=env.encoder, index=env.whole_index
        )
        false_positives = []
        for example in cl.load("adversarial_negative"):
            result = matcher.resolve(example.block, example.namespace)
            if result.outcome is MatchOutcome.MATCHED:
                false_positives.append(
                    (example.id, result.context_id, result.version,
                     round(result.confidence, 4))
                )
        with capsys.disabled():
            print(f"\n  whole-corpus: {len(env.whole_stored)} records, "
                  f"{len(env.whole_index)} indexed vectors")
        assert false_positives == [], f"FALSE POSITIVES: {false_positives}"

    def test_a_deprecated_version_is_registered_but_not_indexed(self, env):
        # The one-record gap between the registry and the index, named rather
        # than left as an off-by-one someone has to rediscover.
        deprecated = [
            key for key, record in env.whole_stored.items() if record.is_deprecated
        ]
        assert deprecated, "the corpus no longer exercises deprecation"
        assert len(env.whole_index) == len(env.whole_stored) - len(deprecated)

    def test_going_deeper_than_the_top_candidate_changes_nothing_here(
        self, env, capsys
    ):
        """Design doc §11's open question, answered with the corpus it named.

        §11 sets the MVP at "just the top-1, escalate only if Phase 10.4's
        evaluation corpus shows a real need for more". It does not: at
        guard_top_n 1, 3 and 5 the positive match rate is identical and the
        adversarial false-positive count stays zero. So the MVP setting ships
        unchanged -- and this test is what would notice if a future corpus
        disagreed.
        """
        rates = {}
        for depth in (1, 3, 5):
            matcher = Matcher(
                env.whole_registry, encoder=env.encoder, index=env.whole_index,
                guard_top_n=depth,
            )
            matched = sum(
                1 for e in cl.load("positive_paraphrase")
                if matcher.resolve(e.block, e.namespace).outcome
                is MatchOutcome.MATCHED
            )
            adversarial = sum(
                1 for e in cl.load("adversarial_negative")
                if matcher.resolve(e.block, e.namespace).outcome
                is MatchOutcome.MATCHED
            )
            rates[depth] = (matched, adversarial)
        with capsys.disabled():
            print(f"  guard_top_n -> (positives matched, adversarial matched): "
                  f"{rates}")
        assert len({value for value in rates.values()}) == 1
        assert all(adversarial == 0 for _, adversarial in rates.values())
        assert DEFAULT_GUARD_TOP_N == 1


@needs_model
class TestGuardLatency:
    """Design doc §18's ``pulsekv_semantic_lookup_latency_seconds{tier="guard"}``,
    measured rather than assumed -- the same treatment Phase 10.3 gave Tier 2."""

    def test_the_guard_costs_are_reported(self, capsys):
        short = _corpus("adversarial_negative", "negation-never-vs-always")
        long = _corpus("adversarial_negative", "truncation-boundary-negation")
        with Guardrail() as guard:
            rows = []
            for label, example in (("short", short), ("512-token", long)):
                record = cl.build_record(example.records[0])
                pair = Candidate(record=record, similarity=1.0)
                for _ in range(5):
                    guard.check(example.block, pair)
                start = time.perf_counter()
                for _ in range(200):
                    guard.check(example.block, pair)
                rows.append((label, (time.perf_counter() - start) / 200 * 1000))
        with capsys.disabled():
            print("\n  tier=guard:")
            for label, milliseconds in rows:
                print(f"    {label:10s} {milliseconds:7.3f} ms/check")
        # Not a performance gate -- a bound loose enough that only a change of
        # algorithm trips it, so the number in the summary stays honest.
        assert all(milliseconds < 25.0 for _, milliseconds in rows)
