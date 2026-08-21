"""Tier orchestration -- all four tiers (Phases 10.2, 10.3, 10.4).

Design doc §11 (four tiers, cheapest and most deterministic first, each a
strict filter before the next is attempted -- not parallel races); plan §6, §7,
§8.

The tier order is a correctness property, not an optimization:

  0. exact hash (and registered aliases)
  1. structural normalization, then tier 0's hash, for structured types only
  2. embedding + candidate retrieval, namespace-pre-filtered -- produces
     candidates, never a decision
  3. equivalence guard against the top candidate -- reject-biased

Every failure, timeout, miss and rejection produces a ``MatchResult`` that does
not substitute, and the caller forwards the original block unchanged. Design
doc §7.3 admits no partial-credit mode: there is no "lower-confidence canonical
guess" state to fall back to.

Reading §11 and §13 together on Tier 1's position
-------------------------------------------------
The two sections describe the same mechanism from different ends, and they look
like they disagree. §11 calls Tier 1 "a pre-processing step *before* Tier 0 for
structured types" -- structural normalization happens before a hash is
computed. §13's table says ``TOOL_SCHEMA`` is matched by "Tier 1 (structural)
then Tier 0". Both are describing one lookup mechanism with two front-ends:

    text  --[normalize_for_hash]------------------> hash -> registry   (Tier 0)
    text  --[normalize_structural]--[normalize_for_hash]-> hash -> registry (Tier 1)

So "Tier 1 then Tier 0" is about the *hash pipeline* (structural normalization
feeds Tier 0's hash), not about which is attempted first. What is attempted
first is Tier 0, because it is strictly cheaper -- a normalize and a hash,
against a parse, a re-serialize, a normalize and a hash -- and because on a
block that hits both, both resolve to the same record: a schema already in its
canonical form hashes to that record either way. Phase 10.0's ``matcher.py``
stub states the resulting order outright ("0. exact hash ... 1. structural
normalization, then tier 0's hash") and this module implements that.

Short-circuit is a hard rule: the first tier to produce a match returns, and no
later tier runs. A deterministic hit is confidence 1.0 by construction (§11 --
Tier 0/1 are exact, not scored), which ``MatchResult``'s own validators
already enforce.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional, Sequence, Tuple

from .auditlog import AuditLog
from .encoder import Encoder, EncoderError
from .guardrail import SIMILARITY_THRESHOLD, Guardrail
from .index import VectorIndex
from .models import (
    BypassReason,
    Candidate,
    ContextBlock,
    DecisionLogRecord,
    GatewayComponent,
    GuardOutcome,
    GuardResult,
    MatchMethod,
    MatchResult,
    RejectionReason,
)
from .normalizer import (
    StructuralNormalizationError,
    hash_normalized,
    normalize_for_hash,
    normalize_structural,
    supports_structural,
)
from .registry import Registry, RegistryError, content_hash_for

__all__ = ["DEFAULT_GUARD_TOP_N", "DEFAULT_TOP_K", "Matcher"]

# How many candidates Tier 2 offers the guard. Design doc §11 settles the MVP at
# "just the top-1, escalate only if Phase 10.4's evaluation corpus shows a real
# need for more" -- so retrieval returns a few and Phase 10.4 decides how deep
# to look. This is a retrieval width, never a threshold.
DEFAULT_TOP_K = 5

# How many of them the guard actually adjudicates. Design doc §11 sets the MVP
# at "just the top-1, escalate only if Phase 10.4's evaluation corpus shows a
# real need for more", and 10.4's corpus is what settled it: see the phase
# summary's escalation section for the measurement. Raising this cannot lower
# the bar -- every extra candidate must clear the same three checks and the
# same τ, and it is ranked below the one before it -- but it does mean a
# rejected top-1 no longer ends the block's chances.
DEFAULT_GUARD_TOP_N = 1


class Matcher:
    """Resolves one block to a ``MatchResult`` through tiers 0-3.

    Complete as of Phase 10.4: Tier 0/1 in 10.2 (``try_exact``,
    ``try_structural``), Tier 2 in 10.3 (``try_semantic``), Tier 3 in 10.4
    (``try_guard``). Every tier is separately callable so a test can prove one
    without standing up the others, and ``resolve`` is the only thing that
    orders them.

    The registry handed in **must** have been constructed with
    ``hash_text=normalizer.hash_normalized`` (Phase 10.1 left that parameter
    for this handoff). A registry built with the default plain-SHA-256 hasher
    stores hashes of un-normalized text and Tier 0 will miss every record whose
    canonical text has so much as a trailing newline.
    """

    def __init__(
        self,
        registry: Registry,
        *,
        encoder: Optional[Encoder] = None,
        index: Optional[VectorIndex] = None,
        top_k: int = DEFAULT_TOP_K,
        guardrail: Optional[Guardrail] = None,
        similarity_threshold: float = SIMILARITY_THRESHOLD,
        guard_top_n: int = DEFAULT_GUARD_TOP_N,
    ) -> None:
        """``encoder`` and ``index`` are optional and additive to Phase 10.2's
        signature: ``Matcher(registry)`` still builds a working deterministic
        matcher, and Tier 2 simply does not run. Supplying one without the
        other is refused rather than half-enabling the tier.

        ``guardrail``, ``similarity_threshold`` and ``guard_top_n`` are Phase
        10.4's additions and all three default to something usable, so no
        existing call site changes. The guard is *not* optional in the way the
        encoder is: a matcher with Tier 2 configured and no guard would be a
        matcher that substitutes on similarity alone, which design doc §11
        forbids outright -- so a default ``Guardrail()`` is constructed rather
        than leaving the attribute None.

        τ's earned default lives in ``guardrail.SIMILARITY_THRESHOLD`` next to
        the checks whose coverage determines it. Phase 10.5 surfaces it as a
        config field that reads this value as its default.
        """
        if (encoder is None) != (index is None):
            raise ValueError(
                "encoder and index: supply both or neither -- an index cannot "
                "rank without the encoder that embeds the query, and an encoder "
                "has nothing to rank against"
            )
        if not 0.0 <= similarity_threshold <= 1.0:
            raise ValueError(
                f"similarity_threshold: {similarity_threshold} is outside [0, 1]; "
                f"Candidate.similarity is a clamped cosine"
            )
        if guard_top_n < 1:
            raise ValueError(
                f"guard_top_n: {guard_top_n} would run Tier 2 and then consult "
                f"none of it"
            )
        self._registry = registry
        self._encoder = encoder
        self._index = index
        self._top_k = top_k
        self._guardrail = guardrail if guardrail is not None else Guardrail()
        self._similarity_threshold = similarity_threshold
        self._guard_top_n = guard_top_n

    @property
    def semantic_enabled(self) -> bool:
        """Whether Tier 2 is configured. False makes this Phase 10.2's matcher."""
        return self._encoder is not None and self._index is not None

    # -- the pipeline ------------------------------------------------------

    def resolve(self, block: ContextBlock, namespace: str) -> MatchResult:
        """Run the tiers in order and return the first accepted match.

        Never raises for an expected failure: a registry outage, an encoder
        timeout or a guard error each become a non-substituting ``MatchResult``
        (``MatchOutcome.ERROR`` with the failing component) so the caller's
        fail-open path is the same path as an ordinary miss (design doc §17).
        """
        return self.resolve_with_candidates(block, namespace)[0]

    def resolve_with_candidates(
        self, block: ContextBlock, namespace: str
    ) -> Tuple[MatchResult, Tuple[Candidate, ...]]:
        """``resolve``, plus whatever Tier 2 retrieved on the way.

        Added in Phase 10.3 and additive to Phase 10.2's surface. It existed
        because ``MatchResult`` had no state for "a candidate was found but
        nothing has validated it yet" -- which stopped being a state in Phase
        10.4: the guard now gives every retrieved candidate a verdict, so the
        ``MatchResult`` this returns already accounts for them.

        It stays because the candidates themselves are still worth seeing from
        the outside -- what the guard refused, and at what similarity, is how
        the corpus harness and any later diagnostic tell a rejection apart from
        a retrieval that found nothing (design doc §18's
        ``pulsekv_semantic_candidates_total``).
        """
        if not block.is_mvp_eligible:
            # Design doc §13's taxonomy, checked before any work is done. A
            # USER_QUERY or CONVERSATION_HISTORY block never reaches a lookup.
            return MatchResult.bypassed(BypassReason.INELIGIBLE_BLOCK_TYPE), ()

        try:
            hit = self.try_exact(block, namespace)
            if hit is not None:
                return hit, ()

            if supports_structural(block.block_type):
                hit = self.try_structural(block, namespace)
                if hit is not None:
                    return hit, ()
        except RegistryError:
            # Design doc §17: the registry is a required dependency that fails
            # *open* for the caller. Distinct from a miss on purpose -- risk
            # register row 5's detection signature is an error spike with no
            # drop in request success, which is unprovable if this is logged as
            # an ordinary miss.
            return MatchResult.errored(GatewayComponent.REGISTRY), ()

        # ---- Tier 2: candidates, never a decision (design doc §11). ----
        try:
            candidates = self.try_semantic(block, namespace)
        except EncoderError:
            # Design doc §17's encoder row: Tier 2/3 are skipped, anything
            # already resolved by Tier 0/1 still applies, and this block passes
            # through unchanged. ERROR rather than a miss for the same reason
            # the registry branch above is -- models.py's own MatchOutcome
            # docstring names the encoder here.
            return MatchResult.errored(GatewayComponent.ENCODER), ()

        # ---- Tier 3: the equivalence guard (design doc §12). ----
        if not candidates:
            return MatchResult.no_candidate(), ()
        return self.try_guard(block, namespace, candidates), candidates

    def resolve_blocks(
        self,
        blocks: Sequence[ContextBlock],
        *,
        namespace: str,
        request_id: str,
        timestamp: datetime,
        model: str,
        audit: Optional[AuditLog] = None,
    ) -> Tuple[MatchResult, ...]:
        """Resolve every block of one request, recording each decision.

        Deliberately takes already-decomposed blocks rather than a request:
        Phase 10.5 owns the request surface, and this keeps the matcher from
        acquiring an opinion about wire format. The wiring 10.5 performs is
        ``decompose(request)`` -> this -> the assembler.

        Plan §6's invariant is that the decision log records **every** block's
        outcome, "including bypassed/ineligible ones (so a later query 'what
        did the gateway do with this request' is always answerable, not just
        for matches)". That is why the audit record is written here, for every
        block, rather than at the match sites.
        """
        results: list[MatchResult] = []
        records: list[DecisionLogRecord] = []
        for block in blocks:
            result = self.resolve(block, namespace)
            results.append(result)
            if audit is not None:
                records.append(
                    DecisionLogRecord.from_match_result(
                        result,
                        request_id=request_id,
                        timestamp=timestamp,
                        namespace=namespace,
                        model=model,
                        block=block,
                        # The Tier 0 hash of the original block, whichever tier
                        # resolved it. An audit trail wants one stable identity
                        # per incoming block: logging Tier 1's structural hash
                        # for schemas would mean the same block hashed
                        # differently depending on which tier happened to win.
                        block_content_hash=self.block_hash(block),
                    )
                )
        if audit is not None:
            audit.record_many(records)
        return tuple(results)

    # -- tiers -------------------------------------------------------------

    def try_exact(
        self, block: ContextBlock, namespace: str
    ) -> Optional[MatchResult]:
        """Tier 0: the normalized block's hash, then its registered aliases.

        Returns None on a miss so the caller can fall through; raises only
        ``RegistryError``, which ``resolve`` turns into a fail-open result.

        Order within the tier is hash first, then aliases. A content-hash hit
        means the block *is* some version's canonical text, which is the most
        specific statement available; an alias is a registered pointer to a
        context. If both could hit, the more specific one wins.

        Aliases are tried against the raw text and then the normalized text.
        Design doc §10 calls them "deterministic exact-match strings", and both
        forms are exactly that -- two indexed equality lookups, no scoring --
        while covering an alias registered with or without the incidental
        whitespace Tier 0's hash already absorbs.
        """
        normalized = normalize_for_hash(block.text)

        record = self._registry.by_content_hash(
            content_hash_for(normalized), namespace
        )
        if record is not None:
            return MatchResult.match(
                method=MatchMethod.EXACT,
                context_id=record.context_id,
                version=record.version,
            )

        for candidate in _distinct(block.text, normalized):
            record = self._registry.resolve_alias(candidate, namespace)
            if record is not None:
                return MatchResult.match(
                    method=MatchMethod.ALIAS,
                    context_id=record.context_id,
                    version=record.version,
                )
        return None

    def try_structural(
        self, block: ContextBlock, namespace: str
    ) -> Optional[MatchResult]:
        """Tier 1: canonical re-serialization, then Tier 0's hash.

        Only reached for a block type with a canonical serialization, and only
        after Tier 0 missed. A block that does not parse as the structure its
        type claims is a miss, not an error: design doc §11's guarantee for
        this tier holds only when the parse succeeded, so a failed parse falls
        through to the ordinary path rather than getting a best-effort rewrite.
        """
        try:
            canonical = normalize_structural(block.text, block.block_type)
        except StructuralNormalizationError:
            return None

        record = self._registry.by_content_hash(
            hash_normalized(canonical), namespace
        )
        if record is None:
            return None
        return MatchResult.match(
            method=MatchMethod.STRUCTURAL,
            context_id=record.context_id,
            version=record.version,
        )

    def try_semantic(
        self, block: ContextBlock, namespace: str
    ) -> Tuple[Candidate, ...]:
        """Tier 2: embed the block and retrieve the top-K nearest registered
        canonical texts *within* the request's namespace and block type.

        Returns candidates, never a decision (design doc §11). An empty tuple
        is an ordinary outcome; ``EncoderError`` is raised for the caller to
        turn into design doc §17's fail-open result.

        The text embedded is ``normalize_for_hash``'s output -- the same form
        Tier 0 hashed. Embedding the raw block instead would give one block two
        identities, one per tier, and the similarity Tier 3 reasons about would
        not be the similarity of the thing Tier 0 looked up.
        """
        if self._encoder is None or self._index is None:
            return ()
        vector = self._encoder.encode(normalize_for_hash(block.text))
        return self._index.find_candidates(
            vector,
            namespace=namespace,
            block_type=block.block_type,
            top_k=self._top_k,
        )

    def try_guard(
        self,
        block: ContextBlock,
        namespace: str,
        candidates: Sequence[Candidate],
    ) -> MatchResult:
        """Tier 3: turn Tier 2's candidates into a decision, reject-biased.

        This is the only place in the pipeline that can produce
        ``MatchMethod.SEMANTIC``. Everything else it can produce refuses, and
        every refusal leads to the same place as a miss: the caller forwards
        the block's original text unchanged (design doc §7.3, §12).

        **The guard runs before τ, not after it.** Design doc §12 describes τ
        as a gate the candidate clears *before* the guard sees it, and this
        implementation deliberately inverts that order for reject-biased
        reasons that only became visible with a real encoder:

        * Phase 10.3 measured a meaning-inverting edit at 0.9933 against a
          genuine paraphrase at 0.7989, and 10.4's corpus reproduced the
          inversion at scale: genuine paraphrases span 0.8462-1.0000,
          adversarial pairs 0.1333-1.0000, and 17 of the 24 scored adversarial
          pairs sit at or above the *lowest* genuine paraphrase. A τ gate in
          front of the guard would therefore *not* filter adversarial pairs
          out; it would only decide which of them the guard never gets to
          name.
        * The verdict is identical either way. Below-τ is a reject and a guard
          mismatch is a reject, and neither order can produce an accept the
          other refuses -- an accept still requires both.
        * What differs is the *reason recorded*. Running the guard first means
          a negation pair is logged as ``negation_mismatch`` whatever it
          scored, instead of a ``low_similarity`` that hides the reason the
          candidate was actually dangerous. Risk register row 1's whole
          detection story is the reject metric's reason label, so this is the
          difference between an audit trail that can be read and one that
          cannot.

        The contract stays intact: a ``LOW_SIMILARITY`` rejection is still
        recorded with ``guard_outcome`` unset, which ``models.py`` requires and
        which is still true in substance -- the candidate was refused *by the τ
        gate*, and the guard's opinion of it changed nothing.

        Candidates are ranked by descending similarity, so the first one to
        clear both the guard and τ wins and the rest are never examined.
        """
        first_refusal: Optional[MatchResult] = None

        for candidate in candidates[: self._guard_top_n]:
            verdict = self._guard(block, candidate, namespace)

            if verdict.outcome is not GuardOutcome.PASSED:
                refusal = MatchResult.rejected(
                    reason=verdict.rejection_reason,
                    context_id=candidate.record.context_id,
                    version=candidate.record.version,
                    confidence=candidate.similarity,
                )
                first_refusal = first_refusal or refusal
                continue

            if candidate.similarity < self._similarity_threshold:
                # Ranked descending, so nothing below this can clear τ either.
                # Recorded against this candidate because it is the one that
                # was considered -- the strongest thing the namespace had.
                return first_refusal or MatchResult.rejected(
                    reason=RejectionReason.LOW_SIMILARITY,
                    context_id=candidate.record.context_id,
                    version=candidate.record.version,
                    confidence=candidate.similarity,
                )

            return MatchResult.match(
                method=MatchMethod.SEMANTIC,
                context_id=candidate.record.context_id,
                version=candidate.record.version,
                confidence=candidate.similarity,
            )

        return first_refusal or MatchResult.no_candidate()

    def _guard(
        self, block: ContextBlock, candidate: Candidate, namespace: str
    ) -> GuardResult:
        """``Guardrail.check``, with the reject bias enforced from the outside.

        ``Guardrail.check`` already converts its own failures into an ERROR
        verdict. This wrapper exists because a *substituted* guard -- a
        subclass, a test double, a Phase 10.5 wrapper -- has no such guarantee,
        and design doc §17's "guard errors or times out -> treated as reject"
        must not depend on the implementation being the one in this repository.
        """
        try:
            return self._guardrail.check(block, candidate, namespace=namespace)
        except BaseException as exc:  # noqa: BLE001 -- §12: any doubt is a reject
            return GuardResult(
                outcome=GuardOutcome.ERROR,
                rejection_reason=RejectionReason.GUARD_ERROR,
                detail=f"{type(exc).__name__}: {exc}",
            )

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def block_hash(block: ContextBlock) -> str:
        """The block's stable identity in the decision log (design doc §21).

        Always the Tier 0 normalized hash, independent of which tier resolved
        the block -- see ``resolve_blocks`` for why.
        """
        return hash_normalized(block.text)


def _distinct(*values: str) -> Iterable[str]:
    """The values in order, without repeats -- dict preserves insertion order."""
    return dict.fromkeys(values)
