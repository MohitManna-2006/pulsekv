"""Contract tests for the Phase 10.0 frozen types.

Phase 10.0's exit criteria call for schema round-trips and a proof that version
immutability is enforced at the type level. This suite covers those, plus the
cross-field rules that make the design doc's pipeline invariants
unrepresentable to violate — because a validator nobody tested is a comment
with extra steps.

Organised by what is being proven, not by which class is under test:

* ``TestRoundTrip``          — every frozen type survives JSON exactly
* ``TestImmutability``       — a published version cannot be edited
* ``TestNamespaceIsMandatory`` — no record exists outside a namespace
* ``TestBlockTaxonomy``      — design doc §13's table is complete and honest
* ``TestDecisionStates``     — illegal match/rejection/bypass states raise
* ``TestTierProvenance``     — a Tier 0/1 hit cannot carry Tier 2/3 evidence
* ``TestProblemsReportedTogether`` — the control/internal/config posture
* ``TestPrivacyByShape``     — the audit record cannot hold prompt text
* ``TestStubsAreStubs``      — Phase 10.0 shipped no behavior by accident
"""

from __future__ import annotations

import importlib
import inspect
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from pulsekv_gateway.models import (
    BLOCK_ELIGIBILITY,
    DETERMINISTIC_METHODS,
    BlockEligibility,
    BlockType,
    BypassReason,
    Candidate,
    CanonicalContextRecord,
    ContextBlock,
    DecisionLogRecord,
    GatewayComponent,
    GuardOutcome,
    GuardResult,
    MatchMethod,
    MatchOutcome,
    MatchResult,
    RejectionReason,
    is_mvp_eligible,
)

HASH_A = "a" * 64
HASH_B = "b" * 64
NOW = datetime(2026, 8, 18, 12, 0, 0, tzinfo=timezone.utc)


def make_record(**overrides) -> CanonicalContextRecord:
    fields = dict(
        context_id="github-agent-policy",
        version=1,
        namespace="acme",
        canonical_text="Use the GitHub tool only for repositories the user owns.",
        content_hash=HASH_A,
        block_type=BlockType.ORG_POLICY,
        created_at=NOW,
        created_by="platform-team",
    )
    fields.update(overrides)
    return CanonicalContextRecord(**fields)


def make_decision(**overrides) -> DecisionLogRecord:
    fields = dict(
        request_id="req-0001",
        timestamp=NOW,
        namespace="acme",
        model="meta-llama/Llama-3-8B-Instruct",
        block_index=0,
        block_type=BlockType.ORG_POLICY,
        block_content_hash=HASH_B,
        outcome=MatchOutcome.NO_CANDIDATE,
    )
    fields.update(overrides)
    return DecisionLogRecord(**fields)


# ---------------------------------------------------------------------------


class TestRoundTrip:
    """Every frozen type survives JSON unchanged.

    Exact equality, not field-spotting: a round trip that loses an enum's
    identity or a datetime's offset would still "look right" under a looser
    assertion.
    """

    def test_registry_record(self):
        record = make_record(
            aliases=("legacy-github-policy", "gh-policy-v1"),
            deprecated_at=NOW + timedelta(days=30),
        )
        assert CanonicalContextRecord.model_validate_json(record.model_dump_json()) == record

    def test_registry_record_with_binary_embedding(self):
        # Non-UTF-8 bytes: the reason the contract configures base64 for JSON
        # rather than relying on pydantic's utf-8 default, which cannot encode
        # an arbitrary vector blob.
        blob = bytes([0xFF, 0x00, 0xFE, 0x80])
        record = make_record(
            embedding=blob, embedding_model_id="bge-small", embedding_model_version="1.5"
        )
        restored = CanonicalContextRecord.model_validate_json(record.model_dump_json())
        assert restored == record
        assert restored.embedding == blob

    @pytest.mark.parametrize(
        "result",
        [
            MatchResult.match(method=MatchMethod.EXACT, context_id="p", version=1),
            MatchResult.match(method=MatchMethod.ALIAS, context_id="p", version=2),
            MatchResult.match(method=MatchMethod.STRUCTURAL, context_id="p", version=3),
            MatchResult.match(
                method=MatchMethod.SEMANTIC, context_id="p", version=4, confidence=0.97
            ),
            MatchResult.no_candidate(),
            MatchResult.rejected(
                reason=RejectionReason.NEGATION_MISMATCH,
                context_id="p",
                version=1,
                confidence=0.98,
            ),
            MatchResult.bypassed(BypassReason.INELIGIBLE_BLOCK_TYPE),
            MatchResult.errored(GatewayComponent.REGISTRY),
        ],
        ids=lambda r: r.outcome.value + (f"-{r.method.value}" if r.method else ""),
    )
    def test_match_result(self, result):
        assert MatchResult.model_validate_json(result.model_dump_json()) == result

    def test_decision_log_record(self):
        decision = make_decision(
            outcome=MatchOutcome.MATCHED,
            method=MatchMethod.SEMANTIC,
            context_id="github-agent-policy",
            version=4,
            similarity=0.981,
            guard_outcome=GuardOutcome.PASSED,
        )
        assert DecisionLogRecord.model_validate_json(decision.model_dump_json()) == decision

    def test_candidate_and_guard_result(self):
        candidate = Candidate(record=make_record(), similarity=0.9)
        assert Candidate.model_validate_json(candidate.model_dump_json()) == candidate

        guard = GuardResult(
            outcome=GuardOutcome.REJECTED,
            rejection_reason=RejectionReason.ENTITY_MISMATCH,
            detail="environment differs: staging vs production",
        )
        assert GuardResult.model_validate_json(guard.model_dump_json()) == guard

    def test_context_block(self):
        block = ContextBlock(
            index=2,
            block_type=BlockType.TOOL_SCHEMA,
            text='{"name": "search"}',
            token_estimate=7,
        )
        assert ContextBlock.model_validate_json(block.model_dump_json()) == block

    def test_unknown_field_is_refused(self):
        # Same posture as dec.KnownFields(true) in control/internal/config:
        # a typo'd key is an error, not a silently ignored one. This is also
        # what makes the round trips above meaningful — a dropped field would
        # have to fail on the way back in.
        payload = make_record().model_dump()
        payload["canonical_txt"] = "typo"
        with pytest.raises(ValidationError) as excinfo:
            CanonicalContextRecord.model_validate(payload)
        assert excinfo.value.errors()[0]["type"] == "extra_forbidden"


class TestImmutability:
    """Design doc §10: a published version's text and hash never change."""

    @pytest.mark.parametrize(
        "field,value",
        [
            ("canonical_text", "a different policy entirely"),
            ("content_hash", HASH_B),
            ("version", 2),
            ("context_id", "someone-elses-policy"),
            ("namespace", "other-tenant"),
        ],
    )
    def test_published_fields_cannot_be_reassigned(self, field, value):
        record = make_record()
        with pytest.raises(ValidationError) as excinfo:
            setattr(record, field, value)
        assert excinfo.value.errors()[0]["type"] == "frozen_instance"
        # And the object really is unchanged, not merely noisy about it.
        assert getattr(record, field) != value

    def test_aliases_are_a_tuple_not_a_list(self):
        # A frozen model with a mutable list field is only frozen at the top
        # level: record.aliases.append(...) would otherwise edit a published
        # version's alias set in place.
        record = make_record(aliases=("gh-policy-v1",))
        assert isinstance(record.aliases, tuple)
        with pytest.raises(AttributeError):
            record.aliases.append("sneaky-alias")  # type: ignore[attr-defined]

    def test_every_contract_type_is_frozen(self):
        for model in (
            CanonicalContextRecord,
            MatchResult,
            DecisionLogRecord,
            Candidate,
            GuardResult,
            ContextBlock,
        ):
            assert model.model_config.get("frozen") is True, model.__name__

    def test_deprecation_produces_a_new_record(self):
        record = make_record()
        deprecated = record.deprecate(NOW + timedelta(days=1))

        assert deprecated is not record
        assert record.deprecated_at is None and not record.is_deprecated
        assert deprecated.is_deprecated
        # Deprecation is the one legal state change, and it touches none of the
        # fields that give a logged decision its meaning.
        assert deprecated.canonical_text == record.canonical_text
        assert deprecated.content_hash == record.content_hash
        assert deprecated.version == record.version

    def test_deprecating_twice_is_refused(self):
        deprecated = make_record().deprecate(NOW)
        with pytest.raises(ValueError, match="already deprecated"):
            deprecated.deprecate(NOW + timedelta(days=1))

    def test_deprecation_cannot_predate_creation(self):
        with pytest.raises(ValidationError, match="must not precede created_at"):
            make_record(deprecated_at=NOW - timedelta(seconds=1))


class TestNamespaceIsMandatory:
    """Design doc §15 / plan §5: there is no default or global namespace."""

    def test_record_requires_namespace(self):
        fields = make_record().model_dump()
        del fields["namespace"]
        with pytest.raises(ValidationError) as excinfo:
            CanonicalContextRecord.model_validate(fields)
        assert excinfo.value.errors()[0]["type"] == "missing"

    def test_decision_requires_namespace(self):
        fields = make_decision().model_dump()
        del fields["namespace"]
        with pytest.raises(ValidationError) as excinfo:
            DecisionLogRecord.model_validate(fields)
        assert excinfo.value.errors()[0]["type"] == "missing"

    @pytest.mark.parametrize("bad", ["", " acme", "acme corp", "-acme", "acme/team"])
    def test_namespace_shape_is_constrained(self, bad):
        # Two namespaces differing only by whitespace or punctuation would be a
        # tenant-isolation hazard, so the shape is checked at the contract edge
        # rather than trusted from the deployment layer that supplies it.
        with pytest.raises(ValidationError):
            make_record(namespace=bad)


class TestBlockTaxonomy:
    """Design doc §13's eligibility table, transcribed and locked."""

    def test_every_block_type_is_classified(self):
        # An unclassified type would otherwise reach the matcher with no
        # eligibility answer at all.
        assert set(BLOCK_ELIGIBILITY) == set(BlockType)

    @pytest.mark.parametrize(
        "block_type",
        [
            BlockType.SYSTEM_PROMPT,
            BlockType.TOOL_SCHEMA,
            BlockType.TOOL_POLICY,
            BlockType.ORG_POLICY,
            BlockType.AGENT_INSTRUCTION,
            BlockType.RAG_DOCUMENT,
        ],
    )
    def test_eligible_types(self, block_type):
        assert is_mvp_eligible(block_type)

    @pytest.mark.parametrize(
        "block_type,eligibility",
        [
            (BlockType.REPOSITORY_CONTEXT, BlockEligibility.DEFERRED),
            (BlockType.FEW_SHOT_EXAMPLES, BlockEligibility.DEFERRED),
            (BlockType.CONVERSATION_HISTORY, BlockEligibility.INELIGIBLE),
            (BlockType.USER_QUERY, BlockEligibility.INELIGIBLE),
        ],
    )
    def test_non_eligible_types(self, block_type, eligibility):
        assert BLOCK_ELIGIBILITY[block_type] is eligibility
        assert not is_mvp_eligible(block_type)

    def test_user_query_is_never_eligible(self):
        # The master prompt's core constraint, and the one row of §13's table
        # that no later phase is expected to revisit.
        assert BLOCK_ELIGIBILITY[BlockType.USER_QUERY] is BlockEligibility.INELIGIBLE
        assert not ContextBlock(
            index=0, block_type=BlockType.USER_QUERY, text="how do I reset my password?"
        ).is_mvp_eligible


class TestDecisionStates:
    """The five outcomes are distinguishable, and their illegal mixtures raise."""

    def test_match_carries_method_id_and_confidence(self):
        result = MatchResult.match(method=MatchMethod.EXACT, context_id="p", version=1)
        assert result.matched and result.substitutes
        assert result.confidence == 1.0  # Tier 0/1 are exact, not scored
        assert result.rejection_reason is None

    def test_semantic_match_requires_an_explicit_confidence(self):
        with pytest.raises(ValueError, match="required for method=semantic"):
            MatchResult.match(method=MatchMethod.SEMANTIC, context_id="p", version=1)

    def test_no_candidate_is_distinct_from_a_rejection(self):
        # The distinction Phase 10.0's prompt §10.0.3 requires: nothing was
        # found, versus something was found and refused.
        miss = MatchResult.no_candidate()
        refusal = MatchResult.rejected(
            reason=RejectionReason.ENTITY_MISMATCH,
            context_id="p",
            version=1,
            confidence=0.96,
        )
        assert miss.outcome is MatchOutcome.NO_CANDIDATE
        assert miss.rejection_reason is None and miss.context_id is None
        assert refusal.outcome is MatchOutcome.REJECTED
        assert refusal.rejection_reason is RejectionReason.ENTITY_MISMATCH
        assert not miss.matched and not refusal.matched
        assert miss != refusal

    def test_fail_open_error_is_distinct_from_a_miss(self):
        # Risk register row 5's detection signature depends on this: an error
        # spike with no drop in request success is what "fail-open worked"
        # looks like, and that is unprovable if an error logs as a plain miss.
        error = MatchResult.errored(GatewayComponent.REGISTRY)
        assert error.outcome is MatchOutcome.ERROR
        assert error.error_component is GatewayComponent.REGISTRY
        assert not error.substitutes

    @pytest.mark.parametrize(
        "kwargs,expected",
        [
            (
                dict(outcome=MatchOutcome.MATCHED, matched=False, method=MatchMethod.EXACT,
                     context_id="p", version=1, confidence=1.0),
                "matched: must be True",
            ),
            (
                dict(outcome=MatchOutcome.NO_CANDIDATE, matched=True),
                "matched: must be False",
            ),
            (
                dict(outcome=MatchOutcome.MATCHED, matched=True, context_id="p",
                     version=1, confidence=1.0),
                "method: required",
            ),
            (
                dict(outcome=MatchOutcome.MATCHED, matched=True, method=MatchMethod.EXACT,
                     confidence=1.0),
                "context_id and version: both required",
            ),
            (
                dict(outcome=MatchOutcome.MATCHED, matched=True, method=MatchMethod.EXACT,
                     context_id="p", version=1, confidence=0.8),
                "confidence: must be 1.0 for method=exact",
            ),
            (
                dict(outcome=MatchOutcome.REJECTED, matched=False, context_id="p",
                     version=1, confidence=0.9),
                "rejection_reason: required",
            ),
            (
                dict(outcome=MatchOutcome.REJECTED, matched=False,
                     rejection_reason=RejectionReason.LOW_SIMILARITY, confidence=0.4),
                "context_id and version: both required",
            ),
            (
                dict(outcome=MatchOutcome.NO_CANDIDATE, matched=False,
                     rejection_reason=RejectionReason.GUARD_ERROR),
                "rejection_reason: must be unset",
            ),
            (
                dict(outcome=MatchOutcome.BYPASSED, matched=False),
                "bypass_reason: required",
            ),
            (
                dict(outcome=MatchOutcome.BYPASSED, matched=False,
                     bypass_reason=BypassReason.DISABLED, context_id="p", version=1),
                "context_id: must be unset",
            ),
            (
                dict(outcome=MatchOutcome.ERROR, matched=False),
                "error_component: required",
            ),
        ],
    )
    def test_illegal_combinations_raise(self, kwargs, expected):
        with pytest.raises(ValidationError, match=expected):
            MatchResult(**kwargs)

    def test_guard_result_pairs_outcome_with_reason(self):
        assert GuardResult(outcome=GuardOutcome.PASSED).rejection_reason is None
        with pytest.raises(ValidationError, match="must be unset when outcome=passed"):
            GuardResult(
                outcome=GuardOutcome.PASSED,
                rejection_reason=RejectionReason.NEGATION_MISMATCH,
            )
        with pytest.raises(ValidationError, match="rejection_reason: required"):
            GuardResult(outcome=GuardOutcome.TIMEOUT)
        with pytest.raises(ValidationError, match="requires guard_timeout"):
            GuardResult(
                outcome=GuardOutcome.TIMEOUT,
                rejection_reason=RejectionReason.ENTITY_MISMATCH,
            )


class TestTierProvenance:
    """Design doc §11/§12: the guard never runs on Tier 0/1, and a semantic
    match is only reachable through a passing guard."""

    @pytest.mark.parametrize("method", sorted(DETERMINISTIC_METHODS, key=lambda m: m.value))
    def test_deterministic_hits_carry_no_tier_2_or_3_evidence(self, method):
        decision = make_decision(
            outcome=MatchOutcome.MATCHED, method=method, context_id="p", version=1
        )
        assert decision.similarity is None and decision.guard_outcome is None
        assert decision.decision_label == method.value

        with pytest.raises(ValidationError, match="no embedding is computed"):
            make_decision(
                outcome=MatchOutcome.MATCHED, method=method, context_id="p",
                version=1, similarity=0.99,
            )
        with pytest.raises(ValidationError, match="the guard never runs"):
            make_decision(
                outcome=MatchOutcome.MATCHED, method=method, context_id="p",
                version=1, guard_outcome=GuardOutcome.PASSED,
            )

    def test_semantic_match_requires_a_passing_guard(self):
        with pytest.raises(ValidationError, match="only reachable through a passing"):
            make_decision(
                outcome=MatchOutcome.MATCHED, method=MatchMethod.SEMANTIC,
                context_id="p", version=1, similarity=0.99,
                guard_outcome=GuardOutcome.REJECTED,
            )

    def test_low_similarity_is_refused_before_the_guard_runs(self):
        # Design doc §12 runs the guard only on a candidate that already
        # cleared τ, so a below-τ top candidate is a rejection with no guard
        # outcome — the one RejectionReason that is not a guard verdict.
        decision = make_decision(
            outcome=MatchOutcome.REJECTED,
            rejection_reason=RejectionReason.LOW_SIMILARITY,
            context_id="p",
            version=1,
            similarity=0.41,
        )
        assert decision.guard_outcome is None
        with pytest.raises(ValidationError, match="refused at the τ gate"):
            make_decision(
                outcome=MatchOutcome.REJECTED,
                rejection_reason=RejectionReason.LOW_SIMILARITY,
                context_id="p", version=1, similarity=0.41,
                guard_outcome=GuardOutcome.REJECTED,
            )

    @pytest.mark.parametrize(
        "reason,expected_guard",
        [
            (RejectionReason.NEGATION_MISMATCH, GuardOutcome.REJECTED),
            (RejectionReason.ENTITY_MISMATCH, GuardOutcome.REJECTED),
            (RejectionReason.TYPE_MISMATCH, GuardOutcome.REJECTED),
            (RejectionReason.GUARD_ERROR, GuardOutcome.ERROR),
            (RejectionReason.GUARD_TIMEOUT, GuardOutcome.TIMEOUT),
            (RejectionReason.LOW_SIMILARITY, None),
        ],
    )
    def test_projection_from_match_result(self, reason, expected_guard):
        # One mapping, defined once, so Phase 10.2's writer and any later one
        # cannot each invent their own.
        result = MatchResult.rejected(
            reason=reason, context_id="p", version=3, confidence=0.93
        )
        block = ContextBlock(index=1, block_type=BlockType.TOOL_POLICY, text="policy")
        decision = DecisionLogRecord.from_match_result(
            result,
            request_id="req-7",
            timestamp=NOW,
            namespace="acme",
            model="meta-llama/Llama-3-8B-Instruct",
            block=block,
            block_content_hash=HASH_B,
        )
        assert decision.guard_outcome is expected_guard
        assert decision.similarity == 0.93
        assert decision.block_index == 1
        assert decision.block_type is BlockType.TOOL_POLICY
        assert decision.decision_label == "rejected"

    def test_projection_of_a_deterministic_hit_drops_confidence(self):
        # MatchResult.confidence is 1.0 on a Tier 0/1 hit; the log's
        # `similarity` means "Tier 2 ran", so it must stay empty here.
        result = MatchResult.match(method=MatchMethod.ALIAS, context_id="p", version=2)
        decision = DecisionLogRecord.from_match_result(
            result,
            request_id="req-8",
            timestamp=NOW,
            namespace="acme",
            model="m",
            block=ContextBlock(index=0, block_type=BlockType.ORG_POLICY, text="t"),
            block_content_hash=HASH_B,
        )
        assert decision.similarity is None
        assert decision.decision_label == "alias"

    @pytest.mark.parametrize(
        "result,label",
        [
            (MatchResult.no_candidate(), "no_candidate"),
            (MatchResult.bypassed(BypassReason.BELOW_MIN_TOKENS), "bypassed"),
            (MatchResult.errored(GatewayComponent.ENCODER), "error"),
            (
                MatchResult.match(
                    method=MatchMethod.SEMANTIC, context_id="p", version=1, confidence=0.99
                ),
                "semantic",
            ),
        ],
        ids=["no_candidate", "bypassed", "error", "semantic"],
    )
    def test_every_outcome_projects_and_round_trips(self, result, label):
        decision = DecisionLogRecord.from_match_result(
            result,
            request_id="req-9",
            timestamp=NOW,
            namespace="acme",
            model="m",
            block=ContextBlock(index=0, block_type=BlockType.ORG_POLICY, text="t"),
            block_content_hash=HASH_B,
        )
        assert decision.decision_label == label
        assert DecisionLogRecord.model_validate_json(decision.model_dump_json()) == decision


class TestProblemsReportedTogether:
    """control/internal/config's posture: one round trip to fix, not five."""

    def test_field_problems_are_collected(self):
        with pytest.raises(ValidationError) as excinfo:
            CanonicalContextRecord(
                context_id="ok",
                version=0,               # must be >= 1
                canonical_text="",       # must be non-empty
                content_hash="not-hex",  # must be sha256 hex
                block_type=BlockType.ORG_POLICY,
                created_at=NOW,
                created_by="",           # must be non-empty
            )                            # and namespace is missing entirely
        reported = {error["loc"][0] for error in excinfo.value.errors()}
        assert reported == {
            "version",
            "namespace",
            "canonical_text",
            "content_hash",
            "created_by",
        }

    def test_cross_field_problems_are_collected(self):
        with pytest.raises(ValidationError) as excinfo:
            MatchResult(outcome=MatchOutcome.MATCHED, matched=False)
        message = excinfo.value.errors()[0]["msg"]
        assert "matched: must be True" in message
        assert "method: required" in message
        assert "context_id and version: both required" in message
        assert "confidence: required" in message

    def test_embedding_identity_is_all_or_nothing(self):
        # Half an embedding identity is worse than none: design doc §16's whole
        # point is that a vector is only comparable when both halves match.
        with pytest.raises(ValidationError, match="set both or neither"):
            make_record(embedding_model_id="bge-small")
        with pytest.raises(ValidationError, match="requires embedding_model_id"):
            make_record(embedding=b"\x00\x01")

    def test_duplicate_aliases_are_refused_not_deduplicated(self):
        with pytest.raises(ValidationError, match="duplicate entries"):
            make_record(aliases=("gh-policy", "gh-policy"))

    @pytest.mark.parametrize(
        "field,value",
        [("created_at", datetime(2026, 8, 18, 12, 0)), ("deprecated_at", datetime(2027, 1, 1))],
    )
    def test_naive_timestamps_are_refused(self, field, value):
        # An audit timestamp with no offset cannot be ordered against another
        # host's records.
        with pytest.raises(ValidationError, match="must be timezone-aware"):
            make_record(**{field: value})

    def test_decision_timestamp_must_be_aware(self):
        with pytest.raises(ValidationError, match="must be timezone-aware"):
            make_decision(timestamp=datetime(2026, 8, 18, 12, 0))


class TestPrivacyByShape:
    """Design doc §20: the audit trail stores hashes, never prompt text."""

    EXPECTED_FIELDS = {
        "request_id",
        "timestamp",
        "namespace",
        "model",
        "block_index",
        "block_type",
        "block_content_hash",
        "outcome",
        "method",
        "context_id",
        "version",
        "similarity",
        "guard_outcome",
        "rejection_reason",
        "bypass_reason",
        "error_component",
    }

    def test_decision_log_field_set_is_locked(self):
        # Locked deliberately: a later phase adding a raw-text field to the
        # audit record has to change this test, which makes the privacy
        # decision visible in review instead of accidental.
        assert set(DecisionLogRecord.model_fields) == self.EXPECTED_FIELDS

    def test_decision_log_cannot_be_given_block_text(self):
        with pytest.raises(ValidationError) as excinfo:
            make_decision(text="the raw prompt block")
        assert excinfo.value.errors()[0]["type"] == "extra_forbidden"

    def test_block_text_lives_only_on_the_in_memory_type(self):
        # ContextBlock is the only contract type that holds prompt text, and it
        # never reaches an audit sink.
        assert "text" in ContextBlock.model_fields
        assert "text" not in DecisionLogRecord.model_fields
        assert "text" not in MatchResult.model_fields


class TestStubsAreStubs:
    """Phase 10.0 produces no runtime behavior — asserted, not assumed."""

    # Modules leave this list as their phase implements them: `registry` in
    # Phase 10.1, `normalizer`/`decomposer`/`matcher`/`auditlog` in Phase 10.2,
    # `encoder`/`index` in Phase 10.3, and `guardrail` in Phase 10.4. The
    # discipline moves with them rather than lapsing -- each phase's test file
    # carries a TestPhaseBoundary asserting that what still belongs to a later
    # phase keeps raising NotImplementedError, and that no later phase's
    # dependency crept in early.
    STUB_MODULES = [
        "assembler",
        "server",
    ]

    @pytest.mark.parametrize("name", STUB_MODULES + ["config"])
    def test_module_imports_without_side_effects(self, name):
        importlib.import_module(f"pulsekv_gateway.{name}")

    @staticmethod
    def _call_with_placeholders(func) -> None:
        """Invoke ``func`` with a placeholder per parameter, however declared.

        Keyword-only parameters (``registry.find_candidates``,
        ``index.find_candidates``) cannot be filled positionally, so each
        parameter is bound by its own kind rather than by count. Callers pass
        *bound* methods, whose signatures already exclude ``self``.
        """
        parameters = list(inspect.signature(func).parameters.values())
        args = [
            None
            for parameter in parameters
            if parameter.kind
            in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
        ]
        kwargs = {
            parameter.name: None
            for parameter in parameters
            if parameter.kind is parameter.KEYWORD_ONLY
        }
        func(*args, **kwargs)

    @pytest.mark.parametrize("name", STUB_MODULES)
    def test_every_callable_raises_not_implemented(self, name):
        module = importlib.import_module(f"pulsekv_gateway.{name}")
        checked = 0
        for obj in vars(module).values():
            if inspect.isfunction(obj) and obj.__module__ == module.__name__:
                checked += 1
                with pytest.raises(NotImplementedError):
                    self._call_with_placeholders(obj)
            elif inspect.isclass(obj) and obj.__module__ == module.__name__:
                if issubclass(obj, Exception):
                    continue
                instance = obj()
                for attr, member in vars(obj).items():
                    if attr.startswith("_"):
                        continue
                    checked += 1
                    with pytest.raises(NotImplementedError):
                        if isinstance(member, property):
                            getattr(instance, attr)
                        else:
                            self._call_with_placeholders(getattr(instance, attr))
        assert checked, f"{name}: no callables were checked"

    def test_config_declares_a_shape_but_implements_no_validation(self):
        # config.py is the one non-models module with real field declarations
        # (Phase 10.0's prompt asks for the shape), so it is checked separately
        # from the pure stubs: the shape exists, the behavior does not.
        from pulsekv_gateway.config import GatewayConfig, NamespaceSource, load

        config = GatewayConfig(
            namespace_source=NamespaceSource.HEADER,
            namespace_header="x-pulsekv-namespace",
            registry_dsn="postgresql://localhost/pulsekv_registry",
            upstream_url="http://127.0.0.1:30000",
        )
        assert config.enabled is True
        assert config.bypass_min_eligible_tokens > 0  # placeholder, see config.py
        with pytest.raises(NotImplementedError):
            config.validate_config()
        with pytest.raises(NotImplementedError):
            config.warnings()
        with pytest.raises(NotImplementedError):
            load("gateway.yaml")

    def test_nothing_imports_the_pulsekv_client_or_grpc(self):
        # Design doc §8 has the gateway importing PulseKVClient as an ordinary
        # library eventually, but Phase 10.0 calls nothing: an import here
        # would make the contract package depend on a running cluster's stubs.
        # Parsed rather than grepped — every module mentions pulsekv_adapters
        # in prose, and prose is exactly what this must not trip on.
        import ast
        import pathlib

        import pulsekv_gateway

        forbidden = ("pulsekv_adapters", "grpc")
        root = pathlib.Path(pulsekv_gateway.__file__).parent
        for path in sorted(root.glob("*.py")):
            tree = ast.parse(path.read_text(), filename=str(path))
            imported: set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported.update(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported.add(node.module)
            offenders = {
                name
                for name in imported
                for prefix in forbidden
                if name == prefix or name.startswith(prefix + ".")
            }
            assert not offenders, f"{path.name} imports {sorted(offenders)}"
