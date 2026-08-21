"""Tier 3 equivalence guard (Phase 10.4).

Design doc §12 -- "the hardest correctness problem in this design"; plan §8.

Three deterministic checks, all reject-biased, none of them a model:

1. **Negation/polarity.** Token-level negation and exception markers present in
   one text and absent in the other. A mismatch is an automatic reject
   independent of similarity, and design doc §12 is explicit that this runs
   *before* similarity is consulted for the accept/reject decision -- not as a
   tiebreaker.
2. **Entity/value preservation.** Numbers, identifiers, resource and
   environment names, command flags. A ``staging``/``production`` or
   ``--force``/``--dry-run`` difference is a set-difference on extracted
   literals, not a judgement call, and needs no neural check.
3. **Structural-type consistency.** A ``TOOL_SCHEMA`` never matches a candidate
   registered as ``ORG_POLICY``, independent of similarity.

Any error or timeout inside this module is a reject, never a pass.

Why this module reads the *whole* text, and never the embedded form
-------------------------------------------------------------------
Phase 10.3 measured the encoder's truncation boundary and asserted it by test:
the model sees the first 512 tokens of a block and **nothing else**, so two
long blocks sharing a 512-token prefix produce byte-identical vectors no matter
how they differ afterwards. Design doc §19 puts the bypass threshold at 512
tokens, which means the blocks this feature exists for are long enough to land
on that boundary by construction -- it is the common case, not an edge case.

Every check below therefore runs over the full ``block.text`` and the full
``candidate.record.canonical_text``. Nothing here consults the vector, the
similarity, or ``Encoder``. A negation that appears in token 900 is invisible
to Tier 2 and fully visible here, which is the entire reason Tier 3 is a
separate tier rather than a threshold on Tier 2's score.

The text is NFC-normalized before tokenizing, and nothing else is done to it.
``normalizer.normalize_for_hash`` is deliberately *not* used: its rules exist
to make a hash well-defined (blank-line runs, trailing whitespace), none of
which changes a token, and routing the guard through the hash normalizer would
invite the belief that the guard sees the same reduced form the hash does.

Why the checks compare *families*, not surface forms
----------------------------------------------------
``allowed``/``permitted`` mean the same thing and ``allowed``/``denied`` do
not. A raw token comparison rejects the first pair (a paraphrase, and a false
negative) while catching the second. Every polarity term below is therefore
mapped to a canonical family key -- ``PERMIT``, ``FORBID``, ``NEG``, ... -- and
the multiset of family keys is compared. Synonyms and inflections inside one
family are free; crossing a family boundary is a reject.

Entities get the opposite treatment: they are compared **case-sensitively and
as written**, because the whole point of the check is that ``staging`` and
``production``, or ``Production`` and ``production``, are different values even
when the encoder cannot tell (measured: cosine 1.0000 for a case-only swap --
the model's tokenizer is uncased). See the Phase 10.4 summary's case-folding
section.

On τ
----
τ is not what separates a paraphrase from its negation, and this module does
not pretend otherwise. Phase 10.3 measured a negation pair at 0.9933 against a
genuine paraphrase at 0.7989, and Phase 10.4's corpus reproduced that inversion
at scale: 17 of its 24 scored adversarial pairs outrank the lowest-scoring
genuine paraphrase. τ's actual job is narrower and real: refusing a candidate
that is merely the nearest thing in a sparse namespace. Its value and the
methodology that produced it are in ``SIMILARITY_THRESHOLD`` below.
"""

from __future__ import annotations

import re
import threading
import unicodedata
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import FrozenSet, List, Mapping, Optional, Sequence

from .models import (
    Candidate,
    ContextBlock,
    GatewayError,
    GuardOutcome,
    GuardResult,
    RejectionReason,
)

__all__ = [
    "SIMILARITY_THRESHOLD",
    "Guardrail",
    "GuardrailError",
    "TextFacts",
    "analyze",
    "extract_entities",
    "extract_polarity",
]


# --------------------------------------------------------------------------
# τ
# --------------------------------------------------------------------------

# Design doc §12 refused to assert a number before an evaluation corpus
# existed. The corpus exists (`gateway/tests/corpus/`), and this is the number
# it produced.
#
# **Methodology.** τ was tuned against the adversarial-negative suite
# specifically, as design doc §12 requires -- not against the positive suite,
# and not from general embedding folklore:
#
#   1. Every adversarial pair was scored with the real encoder.
#   2. Each was classified by whether one of the three deterministic checks
#      above refuses it *without* any similarity signal.
#   3. τ has to exceed the highest-scoring pair the checks do **not** refuse --
#      those are the only ones for which the threshold is load-bearing at all.
#      19 of the 25 adversarial examples are refused without any similarity
#      signal; the remaining six are the corpus's `tau-*` examples, and that
#      class tops out at 0.8187 (`tau-role-swap-approver`: the approver in a
#      policy changed from one lowercase job title to another, which no
#      deterministic extractor here recognises as an entity).
#   4. Rounded up to 0.90 rather than sitting on 0.8187 + ε. 0.8187 is a floor
#      on where an unmeasured example of that class lands, not a ceiling, and a
#      threshold fitted 0.011 above the highest observed member of a
#      six-example class is fitted to that example rather than to the class.
#      The full sweep, including what a lower τ would buy, is in the phase
#      summary §4.
#
# **What τ is not.** It is not what separates a paraphrase from its negation.
# Measured over the corpus: positive pairs span 0.8462-1.0000, adversarial
# pairs 0.1333-1.0000, and 17 of the 24 scored adversarial pairs sit at or
# above the LOWEST positive one. Three pairs score exactly 1.0000 with
# byte-identical vectors -- two adversarial (a case-only entity swap, a
# negation past the truncation boundary) and one genuine paraphrase. At the top
# of the scale the corpus holds meanings and their opposites at the same score.
# No value of τ divides them. The deterministic checks do that work; τ only
# refuses candidates that are the nearest neighbour of nothing in particular.
#
# **What it costs.** Positive pairs below τ are reported as misses in the
# summary rather than bought back by lowering it -- plan §8 states outright
# that a low match rate with zero false positives is the preferred outcome.
SIMILARITY_THRESHOLD = 0.90


class GuardrailError(GatewayError):
    """A guard failure. Callers treat it as a reject (design doc §12, §17)."""


# --------------------------------------------------------------------------
# Lexicons
# --------------------------------------------------------------------------

# Design doc §12's own list is `not`, `never`, `without`, `except`,
# `excluding`, `unless` "and their common contractions". Everything in _NEG and
# _EXCEPT below is that list plus the quantifier negators that say the same
# thing with a different part of speech (`no permission` is `not permitted`).
#
# `not`, `never` and the whole no-family share ONE family key. Splitting them
# would reject "Do not delete production" against "Never delete production",
# which is among the most common paraphrases in policy text, and no adversarial
# pair in the corpus is admitted by merging them -- the *count* is what catches
# a negation added to one clause of many, and the count survives the merge.
_NEG = "not no never none nothing neither nor nobody nowhere cannot".split()

_EXCEPT = (
    "except excepting exception exceptions exclude excludes excluding excluded "
    "unless without omit omits omitting omitted"
).split()

# The counterpart to `never`. `Never delete X` vs `Always delete X` is already
# caught by the NEG count; ALWAYS additionally catches `Always run with
# --dry-run` against a bare `Run with --dry-run`.
_ALWAYS = "always ever".split()

# Plan §8 names "before/after" as a required adversarial failure mode, and the
# corpus measures that inversion at 0.9861 -- higher than nine of the thirteen
# genuine paraphrases in it. It is a polarity flip in exactly design doc §12's
# sense (the relation between two clauses is reversed), and it is invisible to
# cosine.
#
# Only the forms that *relate two clauses* are listed. The corresponding
# adverbs -- `afterwards`, `beforehand`, `previously`, `earlier`, `later`,
# `subsequently` -- were tried and removed on corpus evidence: they are
# discourse markers that a paraphrase adds and drops freely without reversing
# anything, and `positive_paraphrase/retention-90` ("then archive" against "and
# afterwards archived") was refused for carrying one. Dropping them costs no
# adversarial catch: an inversion written with an adverb still moves the
# preposition's count, because the clause it used to bind is gone.
_BEFORE = "before prior".split()
_AFTER = "after".split()

# Threshold inversion, the corpus's other §12 failure mode (measured 0.9972).
# `most`, `least`, `minimum` and `maximum` are deliberately absent: their
# direction depends on the idiom around them ("at most" is an upper bound,
# "the most" is not), and a direction-ambiguous term in a direction-sensitive
# family would make the check's verdict depend on phrasing rather than meaning.
_ABOVE = "above over exceed exceeds exceeded exceeding greater higher more larger".split()
_BELOW = "below under less fewer lower smaller beneath".split()

# Permission polarity. "Requests are allowed" vs "Requests are denied" carries
# no negation marker and no entity difference, so without this family the only
# thing standing between it and a substitution is τ.
_PERMIT = (
    "allow allows allowed allowing permit permits permitted permitting "
    "authorize authorizes authorized grant grants granted enable enables "
    "enabled enabling"
).split()
_FORBID = (
    "deny denies denied denying forbid forbids forbidden prohibit prohibits "
    "prohibited disallow disallows disallowed refuse refuses refused reject "
    "rejects rejected disable disables disabled prevent prevents prevented "
    "avoid avoids avoided refrain refrains revoke revokes revoked"
).split()

# Obligation polarity. `must`, `should`, `may` and `shall` are deliberately NOT
# here: they differ in *strength*, not direction, they are the single most
# common word class in policy prose, and design doc §12 does not name them.
# Including them measurably costs positive matches for a failure mode nothing
# in the plan's list asks for. Recorded as a known limitation in the summary:
# "must not" against "may not" passes this check.
_REQUIRE = (
    "require requires required requiring requirement requirements mandatory "
    "need needs needed necessary"
).split()
_OPTIONAL = "optional optionally".split()

_POLARITY_FAMILIES: Mapping[str, str] = {
    **{word: "NEG" for word in _NEG},
    **{word: "EXCEPT" for word in _EXCEPT},
    **{word: "ALWAYS" for word in _ALWAYS},
    **{word: "BEFORE" for word in _BEFORE},
    **{word: "AFTER" for word in _AFTER},
    **{word: "ABOVE" for word in _ABOVE},
    **{word: "BELOW" for word in _BELOW},
    **{word: "PERMIT" for word in _PERMIT},
    **{word: "FORBID" for word in _FORBID},
    **{word: "REQUIRE" for word in _REQUIRE},
    **{word: "OPTIONAL" for word in _OPTIONAL},
}

# Design doc §12 names "environment names" as an entity class in so many words,
# and a deployment tier written in lowercase prose is not a proper noun, has no
# digit and is not an identifier -- so nothing else here would extract it.
# Cadences are in the same position: a rotation period is a *value*, and the
# corpus measures `quarterly` against `monthly` at 0.8911, above the lowest
# positive pair. Booleans and nulls are literal values in structured content.
_VALUE_WORDS: FrozenSet[str] = frozenset(
    "production prod staging stage preprod pre-production development dev "
    "sandbox canary qa test testing local live "
    "hourly daily nightly weekly biweekly fortnightly monthly quarterly "
    "annually yearly "
    "true false null nil".split()
)

_TOKEN_SPLIT = re.compile(r"\s+")
_SENTENCE_ENDS = (".", "!", "?", ":", ";")
_STRIP_LEADING = "\"'`([{<«“‘"
_STRIP_TRAILING = "\"'`)]}>»”’.,;:!?"
_FLAG = re.compile(r"^--?[A-Za-z][A-Za-z0-9._-]*$")
_DIGIT = re.compile(r"[0-9]")
_IDENTIFIERISH = re.compile(r"[A-Za-z0-9][_/\\@=][A-Za-z0-9]|[A-Za-z0-9]\.[A-Za-z0-9]|::")
_APOSTROPHE_NT = ("n't", "n’t")


class TextFacts:
    """Everything the guard extracted from one text, computed in one pass.

    Held as a small object rather than a tuple so a caller reading
    ``facts.polarity`` cannot silently swap it with ``facts.entities``.
    """

    __slots__ = ("polarity", "entities")

    def __init__(self, polarity: "Counter[str]", entities: FrozenSet[str]) -> None:
        self.polarity = polarity
        self.entities = entities


def analyze(text: str) -> TextFacts:
    """Extract polarity families and entities from one full text.

    One tokenization pass feeds both checks, so their order in ``check`` is
    about which rejection reason gets recorded, not about cost.
    """
    polarity: "Counter[str]" = Counter()
    entities: List[str] = []

    for line in unicodedata.normalize("NFC", text).split("\n"):
        sentence_start = True
        for raw in _TOKEN_SPLIT.split(line):
            if not raw:
                continue
            token = _strip_edges(raw)
            if not token:
                # Bare punctuation (a `-` bullet, an `=` rule) neither starts a
                # sentence nor ends one; it is not a token in any sense the
                # checks care about.
                continue

            family = _polarity_family(token)
            if family is not None:
                polarity[family] += 1

            entity = _entity(token, sentence_start=sentence_start)
            if entity is not None:
                entities.append(entity)

            sentence_start = raw.endswith(_SENTENCE_ENDS)

    return TextFacts(polarity, frozenset(entities))


def extract_polarity(text: str) -> "Counter[str]":
    """The multiset of polarity families in ``text`` (design doc §12.1)."""
    return analyze(text).polarity


def extract_entities(text: str) -> FrozenSet[str]:
    """The set of entities/values in ``text`` (design doc §12.2)."""
    return analyze(text).entities


def _strip_edges(token: str) -> str:
    """Drop surrounding quotes and sentence punctuation, keep the token.

    A leading ``-`` survives on purpose: ``--dry-run`` is the entity, and a
    stripped ``dry-run`` would compare equal to prose about a dry run.
    """
    return token.lstrip(_STRIP_LEADING).rstrip(_STRIP_TRAILING)


def _polarity_family(token: str) -> Optional[str]:
    folded = token.casefold()
    if folded.endswith(_APOSTROPHE_NT):
        # Every ``n't`` contraction is the NEG family: design doc §12 asks for
        # "their common contractions", and enumerating them would miss the
        # next one someone writes.
        return "NEG"
    return _POLARITY_FAMILIES.get(folded)


def _entity(token: str, *, sentence_start: bool) -> Optional[str]:
    """Classify one token, or None if it carries no extractable value.

    Order matters: a flag is checked before a number so ``--retry-3`` is a
    flag, and a value word before a proper noun so ``Production`` and
    ``production`` land in the same class and differ only by case.
    """
    if _FLAG.search(token):
        return f"flag:{token}"
    if _DIGIT.search(token):
        return f"num:{token}"
    if _IDENTIFIERISH.search(token):
        return f"id:{token}"
    if token.casefold() in _VALUE_WORDS:
        # Case is preserved -- `Production` and `production` are different
        # values and the encoder cannot tell them apart -- EXCEPT at a sentence
        # boundary, where orthography forces a capital and the case therefore
        # carries no information. Refusing to guess there is the same rule
        # `_is_proper` applies for the same reason, and without it "Production
        # is off limits" and "production is off limits" would be different
        # values, which would refuse ordinary paraphrases for a difference no
        # writer chose. The classes above never reach this branch: a digit, a
        # leading dash or an embedded `=`/`/`/`_` is not something sentence
        # case can explain, so `AWS_PROFILE=Production` keeps its capital
        # wherever it sits.
        return f"val:{token.casefold() if sentence_start else token}"
    if _is_proper(token, sentence_start=sentence_start):
        return f"name:{token}"
    return None


def _is_proper(token: str, *, sentence_start: bool) -> bool:
    """Proper-noun-like, without mistaking sentence case for a name.

    ``IAM``, ``GitHub`` and ``PulseKV`` carry an uppercase letter that sentence
    case cannot explain, so they are names wherever they appear. A token that
    is merely capitalized is a name only away from a sentence boundary --
    otherwise every ``Never`` starting a policy line would be an entity, and
    two paraphrases that begin differently would never agree on anything.
    """
    if not any(character.isupper() for character in token):
        return False
    if any(character.isupper() for character in token[1:]):
        return True
    return not sentence_start


# --------------------------------------------------------------------------
# The guard
# --------------------------------------------------------------------------


class Guardrail:
    """Decides whether a Tier 2 candidate may actually be substituted."""

    def __init__(self, *, timeout_ms: Optional[int] = None) -> None:
        """``timeout_ms`` bounds one ``check``; None runs it inline.

        The checks are linear scans over two strings with no nested
        quantifiers, so the budget is a backstop against a pathological input
        rather than the normal control flow -- which is why the default is to
        spawn no thread at all. Its shape deliberately mirrors
        ``Encoder.encode``'s: risk register row 14 asks for real timeouts, and
        two components enforcing budgets two different ways is how one of them
        ends up aspirational.
        """
        self._timeout_ms = timeout_ms
        self._pool: Optional[ThreadPoolExecutor] = None
        self._pool_lock = threading.Lock()

    def check(
        self,
        block: ContextBlock,
        candidate: Candidate,
        *,
        namespace: Optional[str] = None,
    ) -> GuardResult:
        """Run the three checks against one candidate.

        Returns a ``GuardResult``; a caller that receives anything other than
        ``GuardOutcome.PASSED`` forwards the block's original text unchanged.

        **Never raises.** Design doc §12's failure bias is that a guard error
        or timeout is a reject, so every escape route from this method is a
        ``GuardResult`` -- an exception would leave the decision to whatever
        the caller happened to write around it.

        ``namespace`` is optional and additive: when the caller passes the
        namespace it actually queried, a candidate from any other namespace is
        refused here as well as being unreachable at retrieval (design doc §15
        makes that structural in ``VectorIndex``). Defense in depth for the one
        failure whose blast radius is a different tenant's text.
        """
        try:
            if self._timeout_ms is None:
                return self._check(block, candidate, namespace)
            future = self._executor().submit(self._check, block, candidate, namespace)
            try:
                return future.result(timeout=self._timeout_ms / 1000.0)
            except FutureTimeoutError:
                future.cancel()
                return GuardResult(
                    outcome=GuardOutcome.TIMEOUT,
                    rejection_reason=RejectionReason.GUARD_TIMEOUT,
                    detail=f"guard exceeded its {self._timeout_ms} ms budget",
                )
        except BaseException as exc:  # noqa: BLE001 -- §12: any doubt is a reject
            # Same normalization ``Encoder.encode`` performs, for the same
            # reason: a failure that escaped as an exception would reach a
            # caller that has no reject path of its own to fall into.
            return GuardResult(
                outcome=GuardOutcome.ERROR,
                rejection_reason=RejectionReason.GUARD_ERROR,
                detail=f"{type(exc).__name__}: {exc}",
            )

    def close(self) -> None:
        """Release the budget worker pool, if one was started."""
        with self._pool_lock:
            pool, self._pool = self._pool, None
        if pool is not None:
            pool.shutdown(wait=False)

    def __enter__(self) -> "Guardrail":
        return self

    def __exit__(self, *_exc_info) -> None:
        self.close()

    # -- the checks --------------------------------------------------------

    def _check(
        self,
        block: ContextBlock,
        candidate: Candidate,
        namespace: Optional[str],
    ) -> GuardResult:
        record = candidate.record

        # 1. Type, first: one enum comparison, and the most decisive thing that
        #    can be known about a candidate. Design doc §12.3.
        if block.block_type is not record.block_type:
            return _reject(
                RejectionReason.TYPE_MISMATCH,
                f"block is {block.block_type.value}, candidate "
                f"{record.context_id} v{record.version} is {record.block_type.value}",
            )

        # 2. Namespace, if the caller supplied one. Reaching here means the
        #    retrieval layer handed over another tenant's record, which is a
        #    defect rather than an ordinary rejection -- so it is reported as a
        #    guard *error*, which §12 also treats as a reject.
        if namespace is not None and record.namespace != namespace:
            raise GuardrailError(
                f"candidate {record.context_id} v{record.version} belongs to "
                f"namespace {record.namespace!r}, query was {namespace!r} "
                f"(design doc §15: retrieval is partitioned, this is unreachable)"
            )

        # One pass over each full text; see the module docstring on why it is
        # the full text and not the embedded prefix.
        incoming = analyze(block.text)
        registered = analyze(record.canonical_text)

        # 3. Polarity, before similarity is consulted anywhere (§12.1). Phase
        #    10.3 measured a meaning-inverting edit at 0.9933 against a genuine
        #    paraphrase at 0.7989: a threshold cannot separate these at any
        #    value, so this check cannot be a tiebreaker under one.
        if incoming.polarity != registered.polarity:
            return _reject(
                RejectionReason.NEGATION_MISMATCH,
                "polarity differs: " + _describe_counter_diff(
                    incoming.polarity, registered.polarity
                ),
            )

        # 4. Entities/values (§12.2).
        if incoming.entities != registered.entities:
            return _reject(
                RejectionReason.ENTITY_MISMATCH,
                "entities differ: " + _describe_set_diff(
                    incoming.entities, registered.entities
                ),
            )

        return GuardResult(outcome=GuardOutcome.PASSED)

    def _executor(self) -> ThreadPoolExecutor:
        with self._pool_lock:
            if self._pool is None:
                self._pool = ThreadPoolExecutor(
                    max_workers=4, thread_name_prefix="pulsekv-guard"
                )
            return self._pool


def _reject(reason: RejectionReason, detail: str) -> GuardResult:
    return GuardResult(
        outcome=GuardOutcome.REJECTED, rejection_reason=reason, detail=detail
    )


def _describe_counter_diff(
    incoming: "Counter[str]", registered: "Counter[str]", limit: int = 4
) -> str:
    """Which families differ, and by how much. Families, never text.

    ``GuardResult.detail`` is read by an operator and never reaches the
    decision log -- ``DecisionLogRecord`` has no field that could hold it, by
    design doc §20's construction -- but this stays a summary of *counts* by
    family rather than a quotation of the block regardless.
    """
    families = sorted(set(incoming) | set(registered))
    parts = [
        f"{family} {incoming[family]}->{registered[family]}"
        for family in families
        if incoming[family] != registered[family]
    ]
    return _join(parts, limit)


def _describe_set_diff(
    incoming: FrozenSet[str], registered: FrozenSet[str], limit: int = 4
) -> str:
    """Which extracted literals differ, in which direction."""
    parts = [f"-{item}" for item in sorted(incoming - registered)]
    parts += [f"+{item}" for item in sorted(registered - incoming)]
    return _join(parts, limit)


def _join(parts: Sequence[str], limit: int) -> str:
    if not parts:
        return "(none)"
    shown = ", ".join(parts[:limit])
    remainder = len(parts) - limit
    return shown if remainder <= 0 else f"{shown} (+{remainder} more)"
