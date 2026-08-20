"""Tier orchestration -- Tiers 0/1 in Phase 10.2, 2/3 in Phase 10.3/10.4.

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
from .index import VectorIndex
from .models import (
    BypassReason,
    Candidate,
    ContextBlock,
    DecisionLogRecord,
    GatewayComponent,
    MatchMethod,
    MatchResult,
)
from .normalizer import (
    StructuralNormalizationError,
    hash_normalized,
    normalize_for_hash,
    normalize_structural,
    supports_structural,
)
from .registry import Registry, RegistryError, content_hash_for

__all__ = ["DEFAULT_TOP_K", "Matcher"]

# How many candidates Tier 2 offers the guard. Design doc §11 settles the MVP at
# "just the top-1, escalate only if Phase 10.4's evaluation corpus shows a real
# need for more" -- so retrieval returns a few and Phase 10.4 decides how deep
# to look. This is a retrieval width, never a threshold.
DEFAULT_TOP_K = 5


class Matcher:
    """Resolves one block to a ``MatchResult`` through tiers 0-3.

    Phase 10.2 implements tiers 0 and 1. Tier 2 is where Phase 10.3 inserts
    ``try_semantic``; see ``resolve`` for the exact seam and what a miss means
    until then.

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
    ) -> None:
        """``encoder`` and ``index`` are optional and additive to Phase 10.2's
        signature: ``Matcher(registry)`` still builds a working deterministic
        matcher, and Tier 2 simply does not run. Supplying one without the
        other is refused rather than half-enabling the tier.
        """
        if (encoder is None) != (index is None):
            raise ValueError(
                "encoder and index: supply both or neither -- an index cannot "
                "rank without the encoder that embeds the query, and an encoder "
                "has nothing to rank against"
            )
        self._registry = registry
        self._encoder = encoder
        self._index = index
        self._top_k = top_k

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

        Added in Phase 10.3 and additive to Phase 10.2's surface. It exists
        because ``MatchResult`` is frozen and has no state for "a candidate was
        found but nothing has validated it yet" -- see the Tier 2 comment in
        the body. **This is Phase 10.4's input seam:** the guard consumes the
        returned candidates and turns the top one into a real ``MatchResult``.
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

        # ---- Tier 2. Phase 10.4 inserts the guard immediately below. ----
        try:
            candidates = self.try_semantic(block, namespace)
        except EncoderError:
            # Design doc §17's encoder row: Tier 2/3 are skipped, anything
            # already resolved by Tier 0/1 still applies, and this block passes
            # through unchanged. ERROR rather than a miss for the same reason
            # the registry branch above is -- models.py's own MatchOutcome
            # docstring names the encoder here.
            return MatchResult.errored(GatewayComponent.ENCODER), ()

        # A candidate is not a match, and Phase 10.3 has nothing that could
        # make it one: design doc §11 says Tier 2 "produces candidates, never a
        # decision", and the guard that earns an accept is Phase 10.4's.
        #
        # The candidates are returned rather than logged because the frozen
        # contract has no state for them yet. DecisionLogRecord's validator
        # refuses `similarity` on a NO_CANDIDATE outcome -- verified, not
        # assumed: it raises "similarity: must be unset when
        # outcome=no_candidate". That gap is real and closes by construction in
        # 10.4, when a retrieved candidate becomes MATCHED or REJECTED, both of
        # which carry similarity legally. See the phase summary §5.
        return MatchResult.no_candidate(), candidates

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
