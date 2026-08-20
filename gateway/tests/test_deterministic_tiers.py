"""Deterministic-tier tests for Phase 10.2 (Tiers 0 and 1, and the decision log).

Plan §6's unit, integration and exit criteria are the spine of this suite:

* ``TestDecomposition``      — §13's eligibility table, proven not assumed
* ``TestNormalizationRules`` — each rule with an adjacent case it must *not* absorb
* ``TestTierZeroExactMatch`` — the hash path
* ``TestTierZeroAliases``    — the alias path, and which wins
* ``TestTierOneStructural``  — canonical re-serialization, and what it refuses
* ``TestTierOrdering``       — cheapest first, hard short-circuit, method recorded
* ``TestBypassAndFailOpen``  — ineligible blocks and a dead registry
* ``TestDecisionLog``        — every block recorded, no prompt text, never raises
* ``TestEndToEnd``           — decompose → normalize → Tier 0 → audit log
* ``TestIndexUsage``         — the lookup is indexed, shown by query plan and by timing
* ``TestPhaseBoundary``      — 10.2 did not start doing 10.3's job
"""

from __future__ import annotations

import json
import sqlite3
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pulsekv_gateway.auditlog import AuditLog, InMemoryAuditLog, JsonlAuditLog
from pulsekv_gateway.decomposer import (
    BLOCK_TYPE_ANNOTATION,
    DecompositionError,
    decompose,
)
from pulsekv_gateway.matcher import Matcher
from pulsekv_gateway.models import (
    BlockType,
    BypassReason,
    CanonicalContextRecord,
    ContextBlock,
    GatewayComponent,
    MatchMethod,
    MatchOutcome,
    is_mvp_eligible,
)
from pulsekv_gateway.normalizer import (
    StructuralNormalizationError,
    canonical_registration_text,
    hash_normalized,
    normalize_for_hash,
    normalize_structural,
    supports_structural,
)
from pulsekv_gateway.registry import Registry

NOW = datetime(2026, 8, 19, 12, 0, 0, tzinfo=timezone.utc)
POLICY = "You are a careful agent.\nNever delete a production resource."
NAMESPACE = "acme"


def make_record(text: str, **overrides) -> CanonicalContextRecord:
    """A record whose hash is computed the way Tier 0 will look it up."""
    fields = dict(
        context_id="github-agent-policy",
        version=1,
        namespace=NAMESPACE,
        canonical_text=text,
        content_hash=hash_normalized(text),
        block_type=BlockType.ORG_POLICY,
        created_at=NOW,
        created_by="mohit",
    )
    fields.update(overrides)
    return CanonicalContextRecord(**fields)


def block(text: str, block_type: BlockType = BlockType.ORG_POLICY, index: int = 0):
    return ContextBlock(index=index, block_type=block_type, text=text)


@pytest.fixture
def registry(tmp_path):
    # hash_text=hash_normalized is not optional: it is the Phase 10.1 seam that
    # makes the hash written on registration equal the hash Tier 0 computes.
    store = Registry(tmp_path / "registry.db", hash_text=hash_normalized)
    yield store
    store.close()


class CountingRegistry:
    """Wraps a registry and records every lookup the matcher makes.

    Duck-typed rather than a subclass: the matcher calls exactly two read
    methods, and counting them is how the short-circuit is *proven* rather than
    inferred from the returned method label.
    """

    def __init__(self, inner: Registry) -> None:
        self.inner = inner
        self.calls: list[str] = []

    def by_content_hash(self, content_hash, namespace):
        self.calls.append("by_content_hash")
        return self.inner.by_content_hash(content_hash, namespace)

    def resolve_alias(self, text, namespace):
        self.calls.append("resolve_alias")
        return self.inner.resolve_alias(text, namespace)

    def count(self, name: str) -> int:
        return self.calls.count(name)


# ---------------------------------------------------------------------------


class TestDecomposition:
    def test_roles_map_to_design_doc_13s_taxonomy(self):
        blocks = decompose(
            {
                "model": "m",
                "messages": [
                    {"role": "system", "content": "sys"},
                    {"role": "user", "content": "first question"},
                    {"role": "assistant", "content": "answer"},
                    {"role": "user", "content": "the actual question"},
                ],
                "tools": [{"type": "function", "function": {"name": "f"}}],
            }
        )
        assert [b.block_type for b in blocks] == [
            BlockType.SYSTEM_PROMPT,
            BlockType.CONVERSATION_HISTORY,  # an earlier user turn is history
            BlockType.CONVERSATION_HISTORY,
            BlockType.USER_QUERY,  # only the last user message
            BlockType.TOOL_SCHEMA,
        ]
        assert [b.index for b in blocks] == [0, 1, 2, 3, 4]

    def test_user_query_and_history_are_never_eligible(self):
        # Plan §6 asks for this proven by test rather than by convention.
        blocks = decompose(
            {
                "messages": [
                    {"role": "user", "content": "q"},
                    {"role": "assistant", "content": "a"},
                ]
            }
        )
        assert all(not b.is_mvp_eligible for b in blocks)
        assert not is_mvp_eligible(BlockType.USER_QUERY)
        assert not is_mvp_eligible(BlockType.CONVERSATION_HISTORY)

    def test_developer_role_is_a_system_prompt(self):
        blocks = decompose({"messages": [{"role": "developer", "content": "sys"}]})
        assert blocks[0].block_type is BlockType.SYSTEM_PROMPT

    def test_an_application_may_annotate_a_block_type(self):
        blocks = decompose(
            {
                "messages": [
                    {
                        "role": "system",
                        "content": "org policy text",
                        BLOCK_TYPE_ANNOTATION: "org_policy",
                    }
                ]
            }
        )
        assert blocks[0].block_type is BlockType.ORG_POLICY

    def test_an_unknown_annotation_raises_rather_than_being_ignored(self):
        with pytest.raises(DecompositionError) as caught:
            decompose(
                {
                    "messages": [
                        {
                            "role": "system",
                            "content": "x",
                            BLOCK_TYPE_ANNOTATION: "orgpolicy",
                        }
                    ]
                }
            )
        assert "orgpolicy" in str(caught.value)

    def test_empty_and_multimodal_content_produce_no_block(self):
        blocks = decompose(
            {
                "messages": [
                    {"role": "system", "content": ""},
                    {"role": "user", "content": [{"type": "text", "text": "hi"}]},
                    {"role": "system", "content": "real"},
                ]
            }
        )
        # A block that is not emitted is a block that is forwarded untouched.
        assert [b.text for b in blocks] == ["real"]
        assert blocks[0].index == 0

    def test_a_malformed_request_raises_a_typed_error(self):
        for bad in ({"messages": "not a list"}, {"tools": 5}, {"messages": [7]}):
            with pytest.raises(DecompositionError):
                decompose(bad)

    def test_token_estimate_is_left_for_phase_105(self):
        blocks = decompose({"messages": [{"role": "system", "content": "x"}]})
        assert blocks[0].token_estimate is None


class TestNormalizationRules:
    """Each rule with a positive case and an adjacent case it must not absorb."""

    def test_unicode_nfc_composes_but_does_not_change_letters(self):
        composed = "café policy"          # é as one code point
        decomposed = "café policy"        # e + combining acute
        assert composed != decomposed
        assert normalize_for_hash(composed) == normalize_for_hash(decomposed)
        # Adjacent negative: a different letter is a different block.
        assert normalize_for_hash("cafe policy") != normalize_for_hash(composed)

    def test_nfkc_is_not_used(self):
        # NFKC would rewrite the ligature to "fi" and ½ to "1⁄2"; those are
        # compatibility mappings, not canonical ones, and they change meaning.
        assert normalize_for_hash("ﬁle") != normalize_for_hash("file")
        assert normalize_for_hash("½") != normalize_for_hash("1/2")
        assert normalize_for_hash("ﬁle") == unicodedata.normalize("NFC", "ﬁle")

    def test_line_endings_are_equivalent(self):
        assert normalize_for_hash("a\r\nb") == normalize_for_hash("a\nb")
        assert normalize_for_hash("a\rb") == normalize_for_hash("a\nb")
        # Adjacent negative: an actual extra line is not a line ending.
        assert normalize_for_hash("a\n\nb") != normalize_for_hash("a\nb")

    def test_trailing_whitespace_goes_but_indentation_stays(self):
        assert normalize_for_hash("a   \nb\t\n") == normalize_for_hash("a\nb")
        # Adjacent negative — the prompt's own example: indentation is syntax.
        assert normalize_for_hash("    indented") != normalize_for_hash("indented")

    def test_blank_line_runs_collapse_but_a_blank_line_is_not_removed(self):
        assert normalize_for_hash("a\n\n\n\nb") == normalize_for_hash("a\n\nb")
        assert normalize_for_hash("a\n\nb") != normalize_for_hash("a\nb")

    def test_internal_whitespace_runs_are_not_collapsed(self):
        # The one whitespace operation that can change meaning: column
        # alignment and code indentation both live inside a line.
        assert normalize_for_hash("a  b") != normalize_for_hash("a b")

    def test_case_is_not_normalized(self):
        # Deliberate: design doc §12's entity check never runs on a Tier 0 hit,
        # so a case-only collapse here would have no guard behind it.
        assert normalize_for_hash("deploy to PROD") != normalize_for_hash("deploy to prod")

    def test_punctuation_and_negation_survive(self):
        assert normalize_for_hash("Do not delete.") != normalize_for_hash("Do not delete")
        assert normalize_for_hash("Do not delete") != normalize_for_hash("Do delete")

    def test_a_non_breaking_space_is_not_ordinary_whitespace(self):
        assert normalize_for_hash("a ") != normalize_for_hash("a")

    def test_normalization_is_idempotent(self):
        # The property the two sides of Tier 0 depend on: the registry hashes a
        # canonical text on write, the matcher hashes a block on read.
        for text in ("a\r\n\n\n  b  ", POLICY, "  \n\n ", "café"):
            once = normalize_for_hash(text)
            assert normalize_for_hash(once) == once


class TestTierZeroExactMatch:
    def test_a_byte_identical_block_hits(self, registry):
        registry.register(make_record(POLICY))
        result = Matcher(registry).resolve(block(POLICY), NAMESPACE)
        assert result.outcome is MatchOutcome.MATCHED
        assert result.method is MatchMethod.EXACT
        assert (result.context_id, result.version) == ("github-agent-policy", 1)
        assert result.confidence == 1.0  # exact by construction, never scored

    def test_an_incidentally_different_block_hits(self, registry):
        registry.register(make_record(POLICY))
        noisy = POLICY.replace("\n", "   \r\n") + "\n\n\n"
        assert noisy != POLICY
        result = Matcher(registry).resolve(block(noisy), NAMESPACE)
        assert result.method is MatchMethod.EXACT

    def test_a_different_block_misses_with_no_candidate(self, registry):
        registry.register(make_record(POLICY))
        result = Matcher(registry).resolve(block("something else entirely"), NAMESPACE)
        assert result.outcome is MatchOutcome.NO_CANDIDATE
        assert not result.matched
        assert result.rejection_reason is None  # a miss is not a rejection

    def test_an_empty_registry_misses(self, registry):
        result = Matcher(registry).resolve(block(POLICY), NAMESPACE)
        assert result.outcome is MatchOutcome.NO_CANDIDATE

    def test_another_namespace_does_not_hit(self, registry):
        registry.register(make_record(POLICY))
        result = Matcher(registry).resolve(block(POLICY), "globex")
        assert result.outcome is MatchOutcome.NO_CANDIDATE

    def test_a_deprecated_version_is_no_longer_a_match_target(self, registry):
        registry.register(make_record(POLICY))
        matcher = Matcher(registry)
        assert matcher.resolve(block(POLICY), NAMESPACE).matched
        registry.deprecate("github-agent-policy", NAMESPACE, 1, NOW + timedelta(hours=1))
        assert not matcher.resolve(block(POLICY), NAMESPACE).matched


class TestTierZeroAliases:
    def test_a_registered_alias_resolves(self, registry):
        registry.register(make_record(POLICY, aliases=("gh-policy",)))
        result = Matcher(registry).resolve(block("gh-policy"), NAMESPACE)
        assert result.method is MatchMethod.ALIAS
        assert result.context_id == "github-agent-policy"

    def test_an_alias_matches_through_incidental_whitespace(self, registry):
        registry.register(make_record(POLICY, aliases=("gh-policy",)))
        result = Matcher(registry).resolve(block("gh-policy \r\n"), NAMESPACE)
        assert result.method is MatchMethod.ALIAS

    def test_but_leading_indentation_is_not_incidental_for_an_alias_either(
        self, registry
    ):
        # Aliases go through the same normalizer as every other block and get
        # no special case: leading whitespace on a line with content is
        # preserved (see normalizer's rule 5), so it is preserved here too.
        # Pinned rather than left accidental -- if a later phase decides an
        # alias should tolerate indentation, that is a deliberate change.
        registry.register(make_record(POLICY, aliases=("gh-policy",)))
        result = Matcher(registry).resolve(block("  gh-policy"), NAMESPACE)
        assert result.outcome is MatchOutcome.NO_CANDIDATE

    def test_the_content_hash_wins_when_both_could_hit(self, registry):
        # The block text is one context's canonical text *and* another's alias.
        # A hash hit says "this block is that version's text", which is the
        # more specific statement.
        registry.register(make_record("shared string", context_id="by-hash"))
        registry.register(
            make_record(
                "different text", context_id="by-alias", aliases=("shared string",)
            )
        )
        result = Matcher(registry).resolve(block("shared string"), NAMESPACE)
        assert result.method is MatchMethod.EXACT
        assert result.context_id == "by-hash"

    def test_an_unregistered_alias_misses(self, registry):
        registry.register(make_record(POLICY, aliases=("gh-policy",)))
        result = Matcher(registry).resolve(block("gh-policy-v2"), NAMESPACE)
        assert result.outcome is MatchOutcome.NO_CANDIDATE


class TestTierOneStructural:
    # Canonical form: keys sorted at every level, no insignificant whitespace.
    SCHEMA = '{"name":"delete_file","parameters":{"force":{"type":"boolean"},"path":{"type":"string"}}}'

    def test_the_constant_is_actually_canonical(self):
        # Guards the rest of this class: a SCHEMA that was not already
        # canonical would make every test below pass or fail for the wrong
        # reason.
        assert normalize_structural(self.SCHEMA, BlockType.TOOL_SCHEMA) == self.SCHEMA

    def test_key_order_and_whitespace_converge(self, registry):
        registry.register(
            make_record(self.SCHEMA, block_type=BlockType.TOOL_SCHEMA)
        )
        reordered = (
            '{\n  "parameters": {\n    "force": {"type": "boolean"},\n'
            '    "path":  {"type": "string"}\n  },\n  "name": "delete_file"\n}'
        )
        result = Matcher(registry).resolve(
            block(reordered, BlockType.TOOL_SCHEMA), NAMESPACE
        )
        assert result.method is MatchMethod.STRUCTURAL
        assert result.confidence == 1.0

    def test_a_semantically_different_schema_does_not_match(self, registry):
        registry.register(
            make_record(self.SCHEMA, block_type=BlockType.TOOL_SCHEMA)
        )
        changed = self.SCHEMA.replace('"boolean"', '"string"')
        result = Matcher(registry).resolve(
            block(changed, BlockType.TOOL_SCHEMA), NAMESPACE
        )
        assert result.outcome is MatchOutcome.NO_CANDIDATE

    def test_a_block_that_is_not_json_falls_through_without_raising(self, registry):
        registry.register(
            make_record(self.SCHEMA, block_type=BlockType.TOOL_SCHEMA)
        )
        result = Matcher(registry).resolve(
            block("not json at all {", BlockType.TOOL_SCHEMA), NAMESPACE
        )
        assert result.outcome is MatchOutcome.NO_CANDIDATE

    def test_only_tool_schema_has_a_canonical_serialization(self):
        assert supports_structural(BlockType.TOOL_SCHEMA)
        for other in BlockType:
            if other is not BlockType.TOOL_SCHEMA:
                assert not supports_structural(other)
                with pytest.raises(StructuralNormalizationError):
                    normalize_structural("{}", other)

    def test_duplicate_keys_are_refused(self):
        # Python keeps the last, so {"a":1,"a":2} and {"a":2} would canonicalize
        # identically while being two different documents.
        with pytest.raises(StructuralNormalizationError):
            normalize_structural('{"a":1,"a":2}', BlockType.TOOL_SCHEMA)

    def test_non_finite_numbers_are_refused(self):
        for literal in ("NaN", "Infinity", "-Infinity"):
            with pytest.raises(StructuralNormalizationError):
                normalize_structural(f'{{"x":{literal}}}', BlockType.TOOL_SCHEMA)

    def test_canonical_registration_text_is_the_form_to_register(self, registry):
        # The trap this helper exists to close: register a pretty-printed
        # schema and Tier 1 can never reach it, because the registry stores one
        # hash per record and Tier 1 looks up the canonical form's hash.
        pretty = '{\n  "b": 1,\n  "a": 2\n}'
        canonical = canonical_registration_text(pretty, BlockType.TOOL_SCHEMA)
        assert canonical == '{"a":2,"b":1}'
        registry.register(
            make_record(canonical, block_type=BlockType.TOOL_SCHEMA)
        )
        result = Matcher(registry).resolve(
            block(pretty, BlockType.TOOL_SCHEMA), NAMESPACE
        )
        assert result.method is MatchMethod.STRUCTURAL

    def test_a_schema_registered_in_a_non_canonical_form_is_unreachable(self, registry):
        # The reason canonical_registration_text exists. The registry stores
        # one hash per record, and Tier 1 looks up the hash of the *canonical*
        # form, so a record registered in any other serialization can never be
        # reached by Tier 1 -- only byte-identically, through Tier 0.
        unsorted = '{"b":1,"a":2}'
        assert normalize_structural(unsorted, BlockType.TOOL_SCHEMA) != unsorted
        registry.register(make_record(unsorted, block_type=BlockType.TOOL_SCHEMA))
        matcher = Matcher(registry)
        assert matcher.resolve(
            block('{"a":2,"b":1}', BlockType.TOOL_SCHEMA), NAMESPACE
        ).outcome is MatchOutcome.NO_CANDIDATE
        # Byte-identical still works, via Tier 0.
        assert matcher.resolve(
            block(unsorted, BlockType.TOOL_SCHEMA), NAMESPACE
        ).method is MatchMethod.EXACT

    def test_canonical_registration_text_passes_prose_through(self):
        assert canonical_registration_text(POLICY, BlockType.ORG_POLICY) == POLICY


class TestTierOrdering:
    """Cheapest tier first, hard short-circuit, and the winner is recorded."""

    CANONICAL = '{"a":2,"b":1}'

    def test_a_block_hitting_both_tiers_resolves_via_tier_zero(self, registry):
        registry.register(
            make_record(self.CANONICAL, block_type=BlockType.TOOL_SCHEMA)
        )
        counting = CountingRegistry(registry)
        result = Matcher(counting).resolve(
            block(self.CANONICAL, BlockType.TOOL_SCHEMA), NAMESPACE
        )
        # models.MatchMethod is the field that carries which tier resolved it.
        assert result.method is MatchMethod.EXACT
        # The proof that Tier 1 never ran: exactly one hash lookup happened.
        assert counting.count("by_content_hash") == 1
        assert counting.count("resolve_alias") == 0

    def test_tier_one_runs_only_after_tier_zero_misses(self, registry):
        registry.register(
            make_record(self.CANONICAL, block_type=BlockType.TOOL_SCHEMA)
        )
        counting = CountingRegistry(registry)
        result = Matcher(counting).resolve(
            block('{\n "b":1,\n "a":2\n}', BlockType.TOOL_SCHEMA), NAMESPACE
        )
        assert result.method is MatchMethod.STRUCTURAL
        # Tier 0's hash, then Tier 1's hash.
        assert counting.count("by_content_hash") == 2

    def test_tier_one_never_runs_for_an_unstructured_type(self, registry):
        registry.register(make_record(POLICY))
        counting = CountingRegistry(registry)
        Matcher(counting).resolve(block("nothing registered"), NAMESPACE)
        assert counting.count("by_content_hash") == 1

    def test_an_alias_hit_short_circuits_before_tier_one(self, registry):
        registry.register(
            make_record(
                self.CANONICAL,
                block_type=BlockType.TOOL_SCHEMA,
                aliases=("the-schema",),
            )
        )
        counting = CountingRegistry(registry)
        result = Matcher(counting).resolve(
            block("the-schema", BlockType.TOOL_SCHEMA), NAMESPACE
        )
        assert result.method is MatchMethod.ALIAS
        assert counting.count("by_content_hash") == 1  # Tier 0's only, not Tier 1's


class TestBypassAndFailOpen:
    def test_an_ineligible_block_is_bypassed_without_a_lookup(self, registry):
        counting = CountingRegistry(registry)
        matcher = Matcher(counting)
        for block_type in (BlockType.USER_QUERY, BlockType.CONVERSATION_HISTORY):
            result = matcher.resolve(block("anything", block_type), NAMESPACE)
            assert result.outcome is MatchOutcome.BYPASSED
            assert result.bypass_reason is BypassReason.INELIGIBLE_BLOCK_TYPE
        assert counting.calls == []

    def test_a_deferred_block_type_is_also_bypassed(self, registry):
        result = Matcher(registry).resolve(
            block("code", BlockType.REPOSITORY_CONTEXT), NAMESPACE
        )
        assert result.bypass_reason is BypassReason.INELIGIBLE_BLOCK_TYPE

    def test_a_dead_registry_fails_open_rather_than_raising(self, tmp_path):
        store = Registry(tmp_path / "registry.db", hash_text=hash_normalized)
        store.register(make_record(POLICY))
        matcher = Matcher(store)
        store.close()
        result = matcher.resolve(block(POLICY), NAMESPACE)
        # Distinct from a miss on purpose: risk register row 5's detection
        # signature is an error spike with no drop in request success.
        assert result.outcome is MatchOutcome.ERROR
        assert result.error_component is GatewayComponent.REGISTRY
        assert not result.substitutes

    def test_nothing_that_is_not_a_match_ever_substitutes(self, registry):
        matcher = Matcher(registry)
        for candidate in (
            matcher.resolve(block("unregistered"), NAMESPACE),
            matcher.resolve(block("q", BlockType.USER_QUERY), NAMESPACE),
        ):
            assert not candidate.substitutes


class TestDecisionLog:
    def test_every_block_is_recorded_including_bypassed_ones(self, registry):
        registry.register(make_record(POLICY))
        audit = InMemoryAuditLog()
        blocks = (
            block(POLICY, BlockType.ORG_POLICY, 0),
            block("unregistered", BlockType.ORG_POLICY, 1),
            block("the question", BlockType.USER_QUERY, 2),
        )
        Matcher(registry).resolve_blocks(
            blocks,
            namespace=NAMESPACE,
            request_id="req-1",
            timestamp=NOW,
            model="gpt-4o",
            audit=audit,
        )
        assert [r.decision_label for r in audit.for_request("req-1")] == [
            "exact",
            "no_candidate",
            "bypassed",
        ]
        assert [r.block_index for r in audit.records] == [0, 1, 2]

    def test_the_written_record_holds_no_prompt_text(self, tmp_path, registry):
        secret = "PATIENT NAME: Jane Doe. Never disclose."
        audit = JsonlAuditLog(tmp_path / "decisions.jsonl")
        Matcher(registry).resolve_blocks(
            (block(secret),),
            namespace=NAMESPACE,
            request_id="req-2",
            timestamp=NOW,
            model="gpt-4o",
            audit=audit,
        )
        audit.close()
        written = (tmp_path / "decisions.jsonl").read_text()
        assert "Jane Doe" not in written
        assert hash_normalized(secret) in written

    def test_the_log_round_trips_through_the_frozen_type(self, tmp_path, registry):
        registry.register(make_record(POLICY))
        audit = JsonlAuditLog(tmp_path / "decisions.jsonl")
        Matcher(registry).resolve_blocks(
            (block(POLICY), block("q", BlockType.USER_QUERY, 1)),
            namespace=NAMESPACE,
            request_id="req-3",
            timestamp=NOW,
            model="gpt-4o",
            audit=audit,
        )
        audit.close()
        records = JsonlAuditLog(tmp_path / "decisions.jsonl").read_all()
        assert len(records) == 2
        assert records[0].method is MatchMethod.EXACT
        assert records[0].similarity is None  # no embedding is computed at Tier 0/1
        assert records[0].guard_outcome is None  # the guard never runs on a Tier 0 hit
        assert records[1].bypass_reason is BypassReason.INELIGIBLE_BLOCK_TYPE

    def test_a_broken_sink_never_raises_into_the_request_path(self, tmp_path, registry):
        audit = JsonlAuditLog(tmp_path / "decisions.jsonl")
        audit.close()  # the sink is now dead under the matcher's feet
        results = Matcher(registry).resolve_blocks(
            (block(POLICY),),
            namespace=NAMESPACE,
            request_id="req-4",
            timestamp=NOW,
            model="gpt-4o",
            audit=audit,
        )
        assert len(results) == 1  # traffic was unaffected
        assert audit.dropped == 1  # but the hole in the trail is counted
        assert "closed" in (audit.last_error or "")

    def test_the_base_class_counts_rather_than_raising(self):
        sink = AuditLog()
        record = _sample_record()
        sink.record(record)
        assert sink.dropped == 1
        assert sink.last_error is not None

    def test_record_many_counts_each_drop(self):
        sink = AuditLog()
        sink.record_many([_sample_record(), _sample_record()])
        assert sink.dropped == 2


class TestEndToEnd:
    """Plan §6's integration test: decompose → normalize → Tier 0 → audit log."""

    def test_a_whole_request_through_the_pipeline(self, registry, tmp_path):
        system_text = "You are a careful agent.\nNever delete a production resource."
        schema = canonical_registration_text(
            '{"name":"delete_file","parameters":{"path":{"type":"string"}}}',
            BlockType.TOOL_SCHEMA,
        )
        registry.register(make_record(system_text, context_id="org-policy"))
        registry.publish_version(
            make_record(
                schema,
                context_id="delete-file-schema",
                block_type=BlockType.TOOL_SCHEMA,
            )
        )

        request = {
            "model": "gpt-4o",
            "messages": [
                # Same meaning as registered, different rendering.
                {"role": "system", "content": system_text.replace("\n", "  \r\n")},
                {"role": "user", "content": "delete /tmp/x"},
            ],
            "tools": [
                {"parameters": {"path": {"type": "string"}}, "name": "delete_file"}
            ],
        }
        audit = JsonlAuditLog(tmp_path / "decisions.jsonl")
        blocks = decompose(request)
        results = Matcher(registry).resolve_blocks(
            blocks,
            namespace=NAMESPACE,
            request_id="req-e2e",
            timestamp=NOW,
            model=request["model"],
            audit=audit,
        )
        audit.close()

        assert [r.decision_label for r in JsonlAuditLog(tmp_path / "decisions.jsonl").read_all()] == [
            "exact",       # system prompt, incidental whitespace absorbed
            "bypassed",    # user query, never eligible
            "structural",  # tool schema, key order re-canonicalized
        ]
        assert [r.substitutes for r in results] == [True, False, True]
        # And the substitutions point at the right records.
        assert results[0].context_id == "org-policy"
        assert results[2].context_id == "delete-file-schema"

    def test_a_request_with_nothing_registered_changes_nothing(self, registry):
        request = {
            "model": "m",
            "messages": [
                {"role": "system", "content": "unregistered system prompt"},
                {"role": "user", "content": "q"},
            ],
        }
        blocks = decompose(request)
        results = Matcher(registry).resolve_blocks(
            blocks,
            namespace=NAMESPACE,
            request_id="req-none",
            timestamp=NOW,
            model="m",
        )
        assert not any(r.substitutes for r in results)


class TestIndexUsage:
    """Deterministic tiers exist to be cheap; that is shown, not assumed.

    Both assertions matter and neither replaces the other. The query plan says
    *which* index is used, which is what catches a planner regression; the
    timing says the cost does not track registry size, which is the property
    that actually matters and which the plan alone can hide. Phase 10.2 found
    exactly that gap: ``resolve_alias``'s plan said SEARCH while its cost grew
    8.4x for 21x the rows, because the planner was seeking on ``namespace``
    alone and filtering ``alias`` per row (see migration 002).
    """

    # For each lookup: the index it must use, and the constraint its plan must
    # show beyond `namespace`. Named rather than matched loosely, so a plan
    # that degrades to a different index -- or to seeking on `namespace` alone,
    # which is exactly the defect migration 002 fixed -- fails here instead of
    # passing on the word "SEARCH".
    EXPECTED_PLAN = {
        "by_content_hash": ("canonical_context_live_content_hash", "content_hash=?"),
        "resolve_alias": ("alias_binding_lookup", "alias=?"),
    }

    @staticmethod
    def _bulk_load(path: Path, count: int, start: int, *, aliases: bool) -> None:
        # Written with raw SQL in one transaction on purpose: this fixture
        # exercises the *read* path, and going through register() would mean
        # `count` fsyncs (Phase 10.1 runs synchronous=FULL) for a test that
        # measures lookups.
        contexts, owners, bindings, pointers = [], [], [], []
        for index in range(start, start + count):
            text = f"filler policy number {index}"
            contexts.append(
                (
                    NAMESPACE, f"filler-{index}", 1, text, hash_normalized(text),
                    BlockType.ORG_POLICY.value, NOW.isoformat(), "bulk",
                )
            )
            if aliases:
                owners.append((NAMESPACE, f"filler-alias-{index}", f"filler-{index}"))
                bindings.append(
                    (NAMESPACE, f"filler-{index}", 1, f"filler-alias-{index}", 0)
                )
                pointers.append((NAMESPACE, f"filler-{index}", 1, NOW.isoformat()))
        connection = sqlite3.connect(str(path))
        try:
            connection.executemany(
                "INSERT INTO canonical_context (namespace, context_id, version,"
                " canonical_text, content_hash, block_type, created_at, created_by)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                contexts,
            )
            if aliases:
                connection.executemany(
                    "INSERT INTO alias_owner (namespace, alias, context_id)"
                    " VALUES (?, ?, ?)",
                    owners,
                )
                connection.executemany(
                    "INSERT INTO alias_binding (namespace, context_id, version,"
                    " alias, ordinal) VALUES (?, ?, ?, ?, ?)",
                    bindings,
                )
                connection.executemany(
                    "INSERT INTO current_version (namespace, context_id, version,"
                    " updated_at) VALUES (?, ?, ?, ?)",
                    pointers,
                )
            connection.commit()
        finally:
            connection.close()

    def test_the_covering_index_migration_is_applied(self, registry):
        assert registry.applied_migrations() == (
            "001_initial",
            "002_alias_lookup_covering_index",
        )

    def test_each_lookup_seeks_its_own_index_on_the_full_key(self, tmp_path):
        path = tmp_path / "registry.db"
        store = Registry(path, hash_text=hash_normalized)
        store.register(make_record(POLICY, aliases=("gh-policy",)))

        # Capture the SQL the registry actually issues, so this test cannot
        # drift from the implementation by asserting against a copy of it.
        connection = store._connection()  # deliberate: proving the real query
        seen: list[str] = []
        connection.set_trace_callback(seen.append)
        store.by_content_hash(hash_normalized(POLICY), NAMESPACE)
        store.resolve_alias("gh-policy", NAMESPACE)
        connection.set_trace_callback(None)

        checked = set()
        for sql in seen:
            if not sql.lstrip().upper().startswith("SELECT"):
                continue
            # Matched on table and column names: set_trace_callback hands back
            # the SQL with parameters already substituted, so "= ?" is gone.
            # Alias first -- its query also touches canonical_context.
            if "alias_binding" in sql and "current_version" in sql:
                lookup = "resolve_alias"
            elif "canonical_context" in sql and "content_hash" in sql:
                lookup = "by_content_hash"
            else:
                continue  # the per-record alias hydration, seeking the PK
            plan = " ".join(
                str(row[3]) for row in connection.execute("EXPLAIN QUERY PLAN " + sql)
            )
            index, constraint = self.EXPECTED_PLAN[lookup]
            assert "SCAN" not in plan, f"{lookup} scans: {plan}"
            assert index in plan, f"{lookup} did not use {index}: {plan}"
            assert "namespace=?" in plan, f"{lookup} is not namespace-scoped: {plan}"
            assert constraint in plan, (
                f"{lookup} seeks on namespace alone and filters the rest per "
                f"row -- the migration-002 defect: {plan}"
            )
            checked.add(lookup)
        assert checked == {"by_content_hash", "resolve_alias"}, checked
        store.close()

    def test_the_alias_plan_seeks_on_alias_and_not_only_namespace(self, tmp_path):
        # The precise regression migration 002 fixed.
        path = tmp_path / "registry.db"
        store = Registry(path, hash_text=hash_normalized)
        store.register(make_record(POLICY, aliases=("gh-policy",)))
        connection = store._connection()
        seen: list[str] = []
        connection.set_trace_callback(seen.append)
        store.resolve_alias("gh-policy", NAMESPACE)
        connection.set_trace_callback(None)
        sql = next(
            s for s in seen if "alias_binding" in s and "current_version" in s
        )
        plan = " ".join(
            str(row[3]) for row in connection.execute("EXPLAIN QUERY PLAN " + sql)
        )
        assert "alias=?" in plan, f"alias is filtered per row, not sought: {plan}"
        store.close()

    @pytest.mark.parametrize("lookup", ["by_content_hash", "resolve_alias"])
    def test_lookup_cost_does_not_track_registry_size(self, tmp_path, lookup):
        path = tmp_path / "registry.db"
        store = Registry(path, hash_text=hash_normalized)
        store.register(make_record(POLICY, aliases=("gh-policy",)))
        target = hash_normalized(POLICY)

        def once() -> None:
            if lookup == "by_content_hash":
                store.by_content_hash(target, NAMESPACE)
            else:
                store.resolve_alias("gh-policy", NAMESPACE)

        def timed(iterations: int = 300) -> float:
            start = time.perf_counter()
            for _ in range(iterations):
                once()
            return time.perf_counter() - start

        self._bulk_load(path, 200, start=0, aliases=True)
        timed(50)  # warm the page cache and the statement cache
        small = min(timed() for _ in range(3))

        self._bulk_load(path, 4000, start=1000, aliases=True)
        large = min(timed() for _ in range(3))
        store.close()

        # A cost that tracked the 21x row increase would show up here. The
        # bound is deliberately loose -- this is a shape check, not a
        # benchmark, and Phase 10.8 owns real numbers.
        assert large < small * 5, (
            f"{lookup}: {small:.4f}s -> {large:.4f}s for 21x the rows"
        )


class TestPhaseBoundary:
    def test_candidate_retrieval_is_still_phase_103(self, registry):
        with pytest.raises(NotImplementedError) as caught:
            registry.find_candidates(
                namespace=NAMESPACE, block_type=BlockType.ORG_POLICY, top_k=5
            )
        assert "10.3" in str(caught.value)

    def test_a_tier_zero_or_one_miss_reports_no_candidate_not_an_error(self, registry):
        # The seam Phase 10.3 fills: today nothing was retrieved to consider.
        result = Matcher(registry).resolve(block("unregistered"), NAMESPACE)
        assert result.outcome is MatchOutcome.NO_CANDIDATE
        assert result.error_component is None

    def test_no_phase_102_module_imports_a_model_or_vector_library(self):
        import ast

        import pulsekv_gateway

        forbidden = (
            "numpy", "scipy", "torch", "onnxruntime", "sentence_transformers",
            "transformers", "sklearn", "faiss", "pulsekv_adapters", "grpc",
        )
        root = Path(pulsekv_gateway.__file__).parent
        for name in ("normalizer", "decomposer", "matcher", "auditlog"):
            tree = ast.parse((root / f"{name}.py").read_text())
            imported = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported.update(a.name.split(".")[0] for a in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported.add(node.module.split(".")[0])
            assert not (imported & set(forbidden)), f"{name}: {sorted(imported)}"


def _sample_record():
    from pulsekv_gateway.models import DecisionLogRecord, MatchResult

    return DecisionLogRecord.from_match_result(
        MatchResult.no_candidate(),
        request_id="req-x",
        timestamp=NOW,
        namespace=NAMESPACE,
        model="m",
        block=block("text"),
        block_content_hash=hash_normalized("text"),
    )
