"""Frozen contract types for the PulseKV Context Gateway (Phase 10.0).

This module is the whole of Phase 10.0's runtime output. Every other module in
``pulsekv_gateway`` is a signature-only stub whose bodies raise
``NotImplementedError``; the types here are what those stubs are typed against,
and what Phases 10.1-10.9 build to. It plays the role v2's Phase 0 played for
the gRPC contract: freeze the shape first so later phases do not diverge.

Design references, cited per type below:

* ``docs/pulsekv-semantic-context-design.md`` §10 (registry record), §11
  (matching pipeline tiers), §12 (equivalence guard), §13 (block taxonomy and
  MVP eligibility), §15 (tenant isolation), §16 (embedding model identity),
  §17 (failure model), §18 (metric label vocabularies), §20 (privacy), §21
  (decision log).
* ``docs/pulsekv-semantic-context-implementation-plan.md`` §4 (this phase),
  §5-§9 (the phases that consume these types).

Three properties are enforced by the types themselves rather than by
convention, because all three are invariants the design doc states as
correctness requirements rather than preferences:

1. **Immutability.** Every model here is ``frozen=True``. A published
   ``CanonicalContextRecord``'s ``canonical_text``/``content_hash``/``version``
   cannot be reassigned in process; attempting it raises. Design doc §10 makes
   this the basis of both the exact-hash tier and the audit trail's meaning.
2. **Namespace is mandatory.** ``namespace`` has no default anywhere it
   appears. Design doc §15 makes namespace a retrieval *pre-filter*, and plan
   §5 states there is no default/global namespace; a defaulted field would
   make a cross-tenant record constructible by omission.
3. **Illegal decision states are unrepresentable.** A match with no method, a
   rejection with no reason, a deterministic (Tier 0/1) hit carrying a
   similarity score, or a semantic match that never passed the guard all raise
   at construction. Design doc §11 says the guard never runs on Tier 0/1 hits
   and §12 says a semantic accept only happens through the guard; those are
   encoded as validators, not comments.

**Validation posture.** Cross-field checks collect every problem and raise them
together, mirroring ``control/internal/config``'s ``Validate()``
(``errors.Join`` over a ``problems`` slice) and the "rejected before anything
starts, both problems reported at once" negative-path row in
``docs/pulsekv-v2-phase0-summary.md`` §6. Pydantic already reports all
*field-level* errors of a construction at once; the ``_problems`` helpers below
extend that to the cross-field rules.

**What this module deliberately does not do:** compute a hash, encode an
embedding, talk to a registry, or import anything from ``pulsekv_adapters``.
Phase 10.0 produces no runtime behavior.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Mapping, Optional, Tuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from pydantic import ValidationError as PydanticValidationError

__all__ = [
    "BLOCK_ELIGIBILITY",
    "CONTENT_HASH_ALGORITHM",
    "CONTENT_HASH_PATTERN",
    "DETERMINISTIC_METHODS",
    "IDENTIFIER_PATTERN",
    "MAX_CONTEXT_ID_CHARS",
    "MAX_NAMESPACE_CHARS",
    "BlockEligibility",
    "BlockType",
    "BypassReason",
    "Candidate",
    "CanonicalContextRecord",
    "ContentHash",
    "ContextBlock",
    "ContextId",
    "DecisionLogRecord",
    "GatewayComponent",
    "GatewayError",
    "GuardOutcome",
    "GuardResult",
    "MatchMethod",
    "MatchOutcome",
    "MatchResult",
    "Namespace",
    "RejectionReason",
    "is_mvp_eligible",
]


# --------------------------------------------------------------------------
# Scalar constraints
# --------------------------------------------------------------------------

# Borrowed verbatim from control/internal/config/config.go's nodeIDPattern
# (`^[A-Za-z0-9][A-Za-z0-9._-]*$`). Reused rather than reinvented because a
# context_id and a namespace are the same kind of thing that file's node_id is:
# an operator-assigned identifier that appears in logs, metric labels
# (design doc §18's `pulsekv_canonical_context_hits_total{context_id,version}`)
# and, for namespace, a tenant-isolation boundary. Two namespaces that differ
# only by surrounding whitespace or case-of-punctuation would be an isolation
# hazard, so the shape is constrained at the contract edge rather than trusted
# from the deployment layer that supplies it (design doc §15).
IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]*$"

# Limits are in characters, not bytes (config.go counts bytes; Python strings
# make characters the natural unit). Both are far above any realistic
# identifier and exist to refuse a pathological value at the edge.
MAX_CONTEXT_ID_CHARS = 128
MAX_NAMESPACE_CHARS = 128

# Design doc §10: `content_hash: SHA-256 of canonical_text, used for the
# exact-hash tier`. Phase 10.0 fixes the *algorithm and rendering* so every
# later phase agrees on them, and deliberately computes nothing: the hash
# input's normalization (design doc §11 Tier 0, "normalize whitespace/casing
# deterministically before hashing") is a Phase 10.2 deliverable in
# normalizer.py, and the computation itself is Phase 10.1's storage-layer job.
CONTENT_HASH_ALGORITHM = "sha256"
CONTENT_HASH_PATTERN = r"^[0-9a-f]{64}$"

ContextId = Annotated[
    str,
    StringConstraints(pattern=IDENTIFIER_PATTERN, max_length=MAX_CONTEXT_ID_CHARS),
]

Namespace = Annotated[
    str,
    StringConstraints(pattern=IDENTIFIER_PATTERN, max_length=MAX_NAMESPACE_CHARS),
]

ContentHash = Annotated[str, StringConstraints(pattern=CONTENT_HASH_PATTERN)]


# --------------------------------------------------------------------------
# Enums
# --------------------------------------------------------------------------


class BlockType(str, Enum):
    """Every block class in design doc §13's taxonomy, eligible or not.

    The ineligible members (``CONVERSATION_HISTORY``, ``USER_QUERY``) and the
    deferred ones (``REPOSITORY_CONTEXT``, ``FEW_SHOT_EXAMPLES``) are present
    on purpose: a decomposer must be able to *name* what it saw, and later
    phases check eligibility by looking a member up in ``BLOCK_ELIGIBILITY``
    rather than by string comparison against a list that can drift. Block type
    is also part of the Tier 2 retrieval key and the Tier 3 type-consistency
    check (design doc §11, §12.3), so it is a closed set by construction.
    """

    SYSTEM_PROMPT = "system_prompt"
    TOOL_SCHEMA = "tool_schema"
    TOOL_POLICY = "tool_policy"
    ORG_POLICY = "org_policy"
    AGENT_INSTRUCTION = "agent_instruction"
    RAG_DOCUMENT = "rag_document"
    REPOSITORY_CONTEXT = "repository_context"
    FEW_SHOT_EXAMPLES = "few_shot_examples"
    CONVERSATION_HISTORY = "conversation_history"
    USER_QUERY = "user_query"


class BlockEligibility(str, Enum):
    """The three verdicts in design doc §13's "MVP eligible?" column.

    ``DEFERRED`` and ``INELIGIBLE`` are distinct because they are different
    decisions with different futures: a deferred type is waiting on an
    eligibility study (§13's own words for ``REPOSITORY_CONTEXT`` and
    ``FEW_SHOT_EXAMPLES``), while an ineligible one is excluded on principle
    and is not expected to be revisited -- ``USER_QUERY`` is the master
    prompt's core constraint and ``CONVERSATION_HISTORY`` carries a leakage
    risk (§15). Collapsing them into one "not now" value would lose that.
    """

    ELIGIBLE = "eligible"
    DEFERRED = "deferred"
    INELIGIBLE = "ineligible"


# Design doc §13's table, transcribed. Exhaustiveness over BlockType is
# asserted by tests/test_models.py, so adding a BlockType member without
# classifying it fails the suite rather than defaulting to something.
#
# RAG_DOCUMENT is ELIGIBLE with a condition §13 states in prose: only a
# *registered* document (one with its own context_id) is a legal match target.
# That condition needs no separate enum value because it is already structural
# -- an unregistered document has no registry record, so it can never be
# retrieved as a candidate. Phase 10.4's corpus should nonetheless treat
# RAG_DOCUMENT as its highest-risk eligible type, since two genuinely
# different documents from one corpus are exactly the high-similarity /
# different-meaning shape §12's guard exists to refuse.
BLOCK_ELIGIBILITY: Mapping[BlockType, BlockEligibility] = {
    BlockType.SYSTEM_PROMPT: BlockEligibility.ELIGIBLE,
    BlockType.TOOL_SCHEMA: BlockEligibility.ELIGIBLE,
    BlockType.TOOL_POLICY: BlockEligibility.ELIGIBLE,
    BlockType.ORG_POLICY: BlockEligibility.ELIGIBLE,
    BlockType.AGENT_INSTRUCTION: BlockEligibility.ELIGIBLE,
    BlockType.RAG_DOCUMENT: BlockEligibility.ELIGIBLE,
    BlockType.REPOSITORY_CONTEXT: BlockEligibility.DEFERRED,
    BlockType.FEW_SHOT_EXAMPLES: BlockEligibility.DEFERRED,
    BlockType.CONVERSATION_HISTORY: BlockEligibility.INELIGIBLE,
    BlockType.USER_QUERY: BlockEligibility.INELIGIBLE,
}


def is_mvp_eligible(block_type: BlockType) -> bool:
    """True only for design doc §13's "Yes" rows.

    Deferred types answer False: Phase 10 does not canonicalize them, and the
    difference between "deferred" and "ineligible" matters to a human reading
    the taxonomy, not to the matcher deciding whether to touch a block.
    """
    return BLOCK_ELIGIBILITY[block_type] is BlockEligibility.ELIGIBLE


class MatchMethod(str, Enum):
    """How a match was reached. Label set matches design doc §18's
    ``pulsekv_semantic_match_total{method=...}`` exactly."""

    EXACT = "exact"
    ALIAS = "alias"
    STRUCTURAL = "structural"
    SEMANTIC = "semantic"


# Tier 0/1 methods. Design doc §11: the guard "never runs on Tier 0/1 hits,
# which need no guard by construction", and no embedding is computed for them
# -- so a result carrying one of these methods must not carry a similarity
# score or a guard outcome. Named here because three separate validators below
# depend on the same distinction.
DETERMINISTIC_METHODS = frozenset(
    {MatchMethod.EXACT, MatchMethod.ALIAS, MatchMethod.STRUCTURAL}
)


class MatchOutcome(str, Enum):
    """What happened to one block, at a coarser grain than ``MatchMethod``.

    Outcome and method are deliberately separate axes. Design doc §21 lists a
    single flat decision vocabulary (``bypassed|exact|alias|structural|
    semantic|rejected``) which conflates the two; that flat form is a
    *projection* of this pair and is produced by
    ``DecisionLogRecord.decision_label``. Keeping them apart here is what makes
    the illegal combinations checkable.

    ``NO_CANDIDATE`` and ``ERROR`` are the two states §21's flat list does not
    name, and both are load-bearing:

    * ``NO_CANDIDATE`` is "retrieval returned nothing to consider" -- the
      Phase 10.0 prompt (§10.0.3) requires this be distinguishable from a
      rejection, because a rejection means a guard refused something and a
      miss means there was nothing to refuse.
    * ``ERROR`` is design doc §17's fail-open fallback: the registry was
      unreachable, or the encoder was unavailable. It is not a miss, and the
      difference is exactly the detection signature the risk register (row 5)
      relies on -- an error spike with no drop in request success is what
      "fail-open worked" looks like, and that is unprovable if an error is
      logged as an ordinary miss.
    """

    MATCHED = "matched"
    NO_CANDIDATE = "no_candidate"
    REJECTED = "rejected"
    BYPASSED = "bypassed"
    ERROR = "error"


class RejectionReason(str, Enum):
    """Why a considered candidate was refused.

    Label set matches design doc §18's
    ``pulsekv_semantic_reject_total{reason=...}`` exactly.

    ``LOW_SIMILARITY`` is the one member that is not a guard verdict: design
    doc §12 runs the guard only against a candidate that "already cleared a
    similarity threshold (τ)", so a below-τ top candidate is refused at the
    threshold gate before Tier 3 runs. It is still a rejection rather than a
    miss -- a candidate existed and was considered -- and §18 counts it under
    the reject metric, so it lives here, with ``guard_outcome`` left None to
    record that the guard did not run. Validators below enforce that pairing.
    """

    LOW_SIMILARITY = "low_similarity"
    NEGATION_MISMATCH = "negation_mismatch"
    ENTITY_MISMATCH = "entity_mismatch"
    TYPE_MISMATCH = "type_mismatch"
    GUARD_ERROR = "guard_error"
    GUARD_TIMEOUT = "guard_timeout"


class BypassReason(str, Enum):
    """Why a block was never put through the pipeline.

    Label set matches design doc §18's
    ``pulsekv_semantic_bypass_total{reason=...}`` exactly.

    ``DISABLED`` is a request-level condition: a gateway with the feature off
    examines no blocks, so it normally emits the metric without emitting a
    per-block ``DecisionLogRecord`` (a record requires a
    ``block_content_hash``, and a disabled gateway hashes nothing). The member
    lives here so the vocabulary is one set rather than two.
    """

    BELOW_MIN_TOKENS = "below_min_tokens"
    INELIGIBLE_BLOCK_TYPE = "ineligible_block_type"
    DISABLED = "disabled"


class GuardOutcome(str, Enum):
    """Tier 3's verdict, present only when Tier 3 actually ran.

    Design doc §12's failure bias is that an error or a timeout is a reject,
    never a pass -- so ``ERROR`` and ``TIMEOUT`` are distinct outcomes for
    observability, but all three non-``PASSED`` values lead to the same
    fallback: forward the original block unchanged.
    """

    PASSED = "passed"
    REJECTED = "rejected"
    ERROR = "error"
    TIMEOUT = "timeout"


class GatewayComponent(str, Enum):
    """Which component failed, for design doc §17's fail-open bookkeeping.

    Label set matches ``pulsekv_semantic_error_total{component=...}`` in §18.
    """

    REGISTRY = "registry"
    ENCODER = "encoder"
    GUARD = "guard"


class GatewayError(Exception):
    """Base class for every typed gateway failure.

    Plan §5 requires the registry to raise "a typed, catchable exception (not a
    bare connection error) that Phase 10.5's gateway wiring can catch cleanly
    for its fail-open path". One base class here means that wiring is a single
    ``except GatewayError`` rather than a growing tuple, and each component
    subclasses it in its own module (``registry.RegistryError``,
    ``encoder.EncoderError``, ``guardrail.GuardrailError``).
    """


# --------------------------------------------------------------------------
# Shared model configuration
# --------------------------------------------------------------------------

# frozen: nothing in this contract is mutable in place (see module docstring).
# extra="forbid": the same posture as `dec.KnownFields(true)` in
#   control/internal/config/config.go -- a typo'd field is an error, not a
#   silently ignored key.
# validate_default: defaults are validated too, so a default can never be a
#   value the field's own constraints would reject.
# *_json_bytes="base64": the embedding blob is opaque binary; pydantic's
#   default utf-8 JSON encoding for bytes would fail on it. base64 both ways
#   makes the round trip exact and is asserted in the tests.
_CONTRACT_CONFIG = ConfigDict(
    frozen=True,
    extra="forbid",
    validate_default=True,
    ser_json_bytes="base64",
    val_json_bytes="base64",
)


def _raise_problems(problems: list[str]) -> None:
    """Raise every cross-field problem at once, or return quietly.

    Mirrors ``config.Validate()``'s ``errors.Join(problems...)`` in
    ``control/internal/config``: one round trip to fix a bad value, not five.
    Pydantic reports field-level errors together already; this covers the
    rules that span fields.
    """
    if problems:
        # dict.fromkeys de-duplicates while preserving order: a rule reached
        # through two paths is one problem, not two.
        raise ValueError("; ".join(dict.fromkeys(problems)))


def _require_aware(value: Optional[datetime], field: str) -> Optional[datetime]:
    """Reject naive datetimes.

    An audit record (design doc §21) whose timestamp has no offset cannot be
    ordered against another host's records, and the registry's
    ``created_at``/``deprecated_at`` are part of that same trail.
    """
    if value is not None and value.tzinfo is None:
        raise ValueError(f"{field}: must be timezone-aware (UTC recommended)")
    return value


# --------------------------------------------------------------------------
# Registry record -- design doc §10
# --------------------------------------------------------------------------


class CanonicalContextRecord(BaseModel):
    """One published version of one canonical context.

    Design doc §10's record shape, with the fields that are load-bearing for
    §7's invariants kept and the speculative ones left out (see the Phase 10.0
    summary for the field-by-field accounting, including why ``safety_class``
    is absent and why ``embedding`` is present as an opaque blob).

    **Immutability.** A ``(context_id, version)`` pair names a record whose
    ``canonical_text`` and ``content_hash`` never change -- that is what makes
    a Tier 0 hit and an audit entry mean the same thing forever (§10).
    Assignment raises here; Phase 10.1 must enforce the same rule at the
    storage layer, where an ``UPDATE`` of those columns on a published row is
    rejected rather than merely discouraged (plan §5).

    **What is *not* on this record:** which version is current. §10's "mutable
    pointer" is mutable registry state and would contradict this record's
    immutability if stored here -- publishing version 5 would have to rewrite
    version 4's row. Phase 10.1 owns that pointer separately.

    **Hash consistency is not checked here.** ``content_hash`` is declared as
    lowercase hex SHA-256 (``CONTENT_HASH_ALGORITHM``) and its *shape* is
    validated, but Phase 10.0 computes no hashes: the normalization that feeds
    the hash is Phase 10.2's (normalizer.py) and the computation is Phase
    10.1's. A record whose hash does not match its text is constructible here
    and must be refused by the storage layer.
    """

    model_config = _CONTRACT_CONFIG

    context_id: ContextId = Field(
        description="Operator-assigned stable name, e.g. 'github-agent-policy'."
    )
    version: int = Field(
        ge=1,
        description=(
            "Monotonically increasing, immutable once published. Version 1 is "
            "the first publication; 0 is not a version."
        ),
    )
    namespace: Namespace = Field(
        description=(
            "Tenant/org scope. Required, no default -- design doc §15 makes "
            "this a retrieval pre-filter and plan §5 states no global "
            "namespace exists."
        )
    )
    canonical_text: str = Field(
        min_length=1,
        description="The exact string substituted into the prompt on a match.",
    )
    content_hash: ContentHash = Field(
        description=(
            "SHA-256 of the normalized canonical_text, lowercase hex. Computed "
            "by Phase 10.1/10.2, never by this module."
        )
    )
    block_type: BlockType = Field(
        description=(
            "Part of the Tier 2 retrieval key and the Tier 3 type-consistency "
            "check, not a descriptive tag (design doc §11, §12.3)."
        )
    )
    embedding_model_id: Optional[str] = Field(
        default=None,
        min_length=1,
        description=(
            "Design doc §16: an entry embedded with model A is not a valid "
            "candidate while the gateway runs model B."
        ),
    )
    embedding_model_version: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Set together with embedding_model_id or not at all.",
    )
    embedding: Optional[bytes] = Field(
        default=None,
        description=(
            "Opaque precomputed vector. Nothing in Phase 10.0-10.2 interprets "
            "these bytes; plan §5 stores them as a blob if provided and Phase "
            "10.3 chooses the encoder and the layout. Base64 in JSON."
        ),
    )
    aliases: Tuple[str, ...] = Field(
        default=(),
        description=(
            "Deterministic exact-match strings resolving to this context_id "
            "with method=alias, no embedding involved (design doc §10). A "
            "tuple rather than a list so the frozen record is frozen all the "
            "way down."
        ),
    )
    created_at: datetime
    created_by: str = Field(min_length=1)
    deprecated_at: Optional[datetime] = Field(
        default=None,
        description=(
            "Set when this version stops being served as a match target. "
            "Design doc §17: already-issued decisions are not retroactively "
            "invalidated -- there is nothing to invalidate, the text was "
            "already sent."
        ),
    )

    @field_validator("created_at", "deprecated_at")
    @classmethod
    def _timestamps_are_aware(cls, value, info):
        return _require_aware(value, info.field_name)

    @field_validator("aliases")
    @classmethod
    def _aliases_are_clean(cls, value: Tuple[str, ...]) -> Tuple[str, ...]:
        problems: list[str] = []
        if any(not alias for alias in value):
            problems.append("aliases: must not contain an empty string")
        duplicates = sorted({a for a in value if value.count(a) > 1})
        if duplicates:
            # Refused rather than silently de-duplicated: a duplicate alias is
            # a sign the caller built the list wrong, and quietly repairing it
            # would hide that.
            problems.append(f"aliases: duplicate entries {duplicates}")
        _raise_problems(problems)
        return value

    @model_validator(mode="after")
    def _check_record(self) -> "CanonicalContextRecord":
        problems: list[str] = []

        has_id = self.embedding_model_id is not None
        has_version = self.embedding_model_version is not None
        if has_id != has_version:
            # Half an embedding identity is worse than none: §16's whole point
            # is that a candidate is only comparable when both halves match.
            problems.append(
                "embedding_model_id and embedding_model_version: set both or neither"
            )
        if self.embedding is not None and not (has_id and has_version):
            problems.append(
                "embedding: requires embedding_model_id and embedding_model_version "
                "(an unattributed vector cannot be compared safely -- design doc §16)"
            )
        if self.deprecated_at is not None and self.deprecated_at < self.created_at:
            problems.append("deprecated_at: must not precede created_at")

        _raise_problems(problems)
        return self

    @property
    def is_deprecated(self) -> bool:
        """Whether this version has stopped being a legal match target."""
        return self.deprecated_at is not None

    def deprecate(self, at: datetime) -> "CanonicalContextRecord":
        """Return a new record marked deprecated at ``at``.

        Deprecation is the one legal state change to a published version
        (design doc §17), and it does not contradict immutability because it
        touches neither ``canonical_text``, ``content_hash``, nor ``version``.
        Returning a new instance rather than mutating keeps that visible: the
        caller's original object is unchanged, and Phase 10.1's storage layer
        is what actually persists the transition.
        """
        if self.deprecated_at is not None:
            raise ValueError(
                f"{self.context_id} v{self.version}: already deprecated at "
                f"{self.deprecated_at.isoformat()}"
            )
        # Re-validated through the full model rather than model_copy(), so the
        # deprecated_at >= created_at rule cannot be bypassed by this path.
        return type(self).model_validate({**self.model_dump(), "deprecated_at": at})


class Candidate(BaseModel):
    """A Tier 2 retrieval result: one registered record and its similarity.

    The hand-off type between ``index.VectorIndex`` (Phase 10.3) and
    ``guardrail.Guardrail`` (Phase 10.4). A candidate is explicitly *not* a
    decision -- design doc §11 is direct about this: Tier 2 "produces
    candidates, never a decision", and embedding similarity is not equivalence.
    """

    model_config = _CONTRACT_CONFIG

    record: CanonicalContextRecord
    similarity: float = Field(
        ge=0.0,
        le=1.0,
        description=(
            "Cosine similarity against the incoming block, in [0, 1]. The "
            "threshold τ this is compared against is a Phase 10.4 deliverable "
            "tuned on the adversarial-negative corpus, not a constant this "
            "phase may assert (design doc §12)."
        ),
    )


class GuardResult(BaseModel):
    """Tier 3's verdict on one candidate (plan §8's ``Guardrail.check``).

    Reject-biased by construction: any outcome other than ``PASSED`` carries a
    reason and leads to the original block being forwarded unchanged. There is
    no reduced-confidence substitution mode (design doc §12).
    """

    model_config = _CONTRACT_CONFIG

    outcome: GuardOutcome
    rejection_reason: Optional[RejectionReason] = None
    detail: Optional[str] = Field(
        default=None,
        description=(
            "Optional human-readable specifics for an operator, e.g. which "
            "entity differed. Never carries raw block text into the decision "
            "log -- design doc §20 stores hashes, not prompts."
        ),
    )

    @model_validator(mode="after")
    def _check_guard_result(self) -> "GuardResult":
        problems: list[str] = []
        if self.outcome is GuardOutcome.PASSED:
            if self.rejection_reason is not None:
                problems.append("rejection_reason: must be unset when outcome=passed")
        elif self.rejection_reason is None:
            problems.append(f"rejection_reason: required when outcome={self.outcome.value}")
        else:
            expected = {
                GuardOutcome.ERROR: RejectionReason.GUARD_ERROR,
                GuardOutcome.TIMEOUT: RejectionReason.GUARD_TIMEOUT,
            }.get(self.outcome)
            if expected is not None and self.rejection_reason is not expected:
                problems.append(
                    f"rejection_reason: outcome={self.outcome.value} requires "
                    f"{expected.value}, got {self.rejection_reason.value}"
                )
            if self.outcome is GuardOutcome.REJECTED and self.rejection_reason in (
                RejectionReason.GUARD_ERROR,
                RejectionReason.GUARD_TIMEOUT,
            ):
                problems.append(
                    "rejection_reason: guard_error/guard_timeout describe outcome="
                    "error/timeout, not a substantive rejection"
                )
        _raise_problems(problems)
        return self


# --------------------------------------------------------------------------
# Request-side types
# --------------------------------------------------------------------------


class ContextBlock(BaseModel):
    """One decomposed block of an incoming request (plan §6's decomposer).

    Holds raw text and therefore never leaves the process: design doc §20
    stores ``content_hash`` and ``context_id``/``version`` in the audit trail,
    not prompts. ``DecisionLogRecord`` deliberately has no text field, so this
    type and that one cannot be confused.

    ``index`` is the block's position in the original request. Design doc §14
    forbids reordering -- the assembler substitutes in place -- so the index is
    both the assembler's identity for a block and what makes two blocks of the
    same type distinguishable in the decision log.
    """

    model_config = _CONTRACT_CONFIG

    index: int = Field(ge=0)
    block_type: BlockType
    text: str = Field(min_length=1)
    token_estimate: Optional[int] = Field(
        default=None,
        ge=0,
        description=(
            "Approximate token count, used only for the bypass heuristic in "
            "design doc §19 (skip short blocks). Deliberately an estimate: the "
            "gateway does not tokenize (§22 rejects gateway-side tokenization "
            "because a gateway tokenizer that disagreed with the engine's "
            "would be a drift risk). Nothing that affects cache identity may "
            "read this field."
        ),
    )

    @property
    def is_mvp_eligible(self) -> bool:
        """Whether design doc §13 lets this block be canonicalized at all."""
        return is_mvp_eligible(self.block_type)


# --------------------------------------------------------------------------
# Decision types -- master prompt §30 sketch, design doc §21
# --------------------------------------------------------------------------


class MatchResult(BaseModel):
    """What the matcher decided about one block (plan §8's
    ``Matcher.resolve``).

    Fields follow the master prompt's §30 sketch (``matched``, ``context_id``,
    ``version``, ``confidence``, ``method``, ``rejection_reason``) plus three
    additions the design doc's own vocabulary requires: ``outcome`` (so a miss,
    a rejection, a bypass and a fail-open error are distinguishable rather than
    all being ``matched=False``), ``bypass_reason`` and ``error_component``
    (§18's separate metric label sets, which do not belong in
    ``rejection_reason``).

    Construct through the classmethods below rather than by hand: they are the
    only combinations the validators allow, and they make the intended state
    explicit at the call site.
    """

    model_config = _CONTRACT_CONFIG

    outcome: MatchOutcome
    matched: bool = Field(
        description=(
            "True exactly when outcome is MATCHED. Kept as a real field "
            "because the master prompt's contract sketch names it and callers "
            "read it; a validator makes the two unable to disagree."
        )
    )
    context_id: Optional[ContextId] = None
    version: Optional[int] = Field(default=None, ge=1)
    confidence: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description=(
            "1.0 for a Tier 0/1 hit, which is exact by construction; the "
            "candidate's cosine similarity for Tier 2/3. On a rejection this "
            "is the refused candidate's similarity."
        ),
    )
    method: Optional[MatchMethod] = None
    rejection_reason: Optional[RejectionReason] = Field(
        default=None,
        description=(
            "Set only when a candidate was considered and refused -- by the "
            "Tier 3 guard, or by the τ threshold gate for LOW_SIMILARITY. A "
            "miss with nothing to consider is outcome=NO_CANDIDATE with no "
            "reason (Phase 10.0 prompt §10.0.3)."
        ),
    )
    bypass_reason: Optional[BypassReason] = None
    error_component: Optional[GatewayComponent] = None

    @model_validator(mode="after")
    def _check_result(self) -> "MatchResult":
        problems: list[str] = []

        if self.matched != (self.outcome is MatchOutcome.MATCHED):
            problems.append(
                f"matched: must be {self.outcome is MatchOutcome.MATCHED} when "
                f"outcome={self.outcome.value}"
            )

        def forbid(**fields) -> None:
            for name, value in fields.items():
                if value is not None:
                    problems.append(
                        f"{name}: must be unset when outcome={self.outcome.value}"
                    )

        if self.outcome is MatchOutcome.MATCHED:
            if self.method is None:
                problems.append("method: required when outcome=matched")
            if self.context_id is None or self.version is None:
                problems.append(
                    "context_id and version: both required when outcome=matched"
                )
            if self.confidence is None:
                problems.append("confidence: required when outcome=matched")
            elif (
                self.method in DETERMINISTIC_METHODS
                and self.confidence != 1.0
            ):
                # Tier 0/1 are exact, not scored (design doc §11).
                problems.append(
                    f"confidence: must be 1.0 for method={self.method.value}"
                )
            forbid(
                rejection_reason=self.rejection_reason,
                bypass_reason=self.bypass_reason,
                error_component=self.error_component,
            )

        elif self.outcome is MatchOutcome.REJECTED:
            if self.rejection_reason is None:
                problems.append("rejection_reason: required when outcome=rejected")
            if self.context_id is None or self.version is None:
                # A rejection always refused a specific candidate; recording
                # which one is what makes design doc §21's audit trail usable.
                problems.append(
                    "context_id and version: both required when outcome=rejected "
                    "(they identify the refused candidate)"
                )
            if self.confidence is None:
                problems.append(
                    "confidence: required when outcome=rejected (the refused "
                    "candidate's similarity)"
                )
            forbid(
                method=self.method,
                bypass_reason=self.bypass_reason,
                error_component=self.error_component,
            )

        elif self.outcome is MatchOutcome.BYPASSED:
            if self.bypass_reason is None:
                problems.append("bypass_reason: required when outcome=bypassed")
            forbid(
                context_id=self.context_id,
                version=self.version,
                confidence=self.confidence,
                method=self.method,
                rejection_reason=self.rejection_reason,
                error_component=self.error_component,
            )

        elif self.outcome is MatchOutcome.ERROR:
            if self.error_component is None:
                problems.append("error_component: required when outcome=error")
            forbid(
                context_id=self.context_id,
                version=self.version,
                confidence=self.confidence,
                method=self.method,
                rejection_reason=self.rejection_reason,
                bypass_reason=self.bypass_reason,
            )

        else:  # MatchOutcome.NO_CANDIDATE
            forbid(
                context_id=self.context_id,
                version=self.version,
                confidence=self.confidence,
                method=self.method,
                rejection_reason=self.rejection_reason,
                bypass_reason=self.bypass_reason,
                error_component=self.error_component,
            )

        _raise_problems(problems)
        return self

    @property
    def substitutes(self) -> bool:
        """Whether the assembler may replace the block's text.

        The single question design doc §7.3 turns on: everything that is not an
        accepted match forwards the original block unchanged, with no
        partial-credit mode in between.
        """
        return self.outcome is MatchOutcome.MATCHED

    # -- constructors -------------------------------------------------------

    @classmethod
    def match(
        cls,
        *,
        method: MatchMethod,
        context_id: str,
        version: int,
        confidence: Optional[float] = None,
    ) -> "MatchResult":
        """An accepted match. Tier 0/1 default to confidence 1.0; Tier 2/3
        must pass the candidate's similarity explicitly."""
        if confidence is None:
            if method is MatchMethod.SEMANTIC:
                raise ValueError("confidence: required for method=semantic")
            confidence = 1.0
        return cls(
            outcome=MatchOutcome.MATCHED,
            matched=True,
            method=method,
            context_id=context_id,
            version=version,
            confidence=confidence,
        )

    @classmethod
    def no_candidate(cls) -> "MatchResult":
        """Nothing was retrieved to consider. Not an error, not a rejection."""
        return cls(outcome=MatchOutcome.NO_CANDIDATE, matched=False)

    @classmethod
    def rejected(
        cls,
        *,
        reason: RejectionReason,
        context_id: str,
        version: int,
        confidence: float,
    ) -> "MatchResult":
        """A candidate was considered and refused."""
        return cls(
            outcome=MatchOutcome.REJECTED,
            matched=False,
            rejection_reason=reason,
            context_id=context_id,
            version=version,
            confidence=confidence,
        )

    @classmethod
    def bypassed(cls, reason: BypassReason) -> "MatchResult":
        """The block never entered the pipeline."""
        return cls(
            outcome=MatchOutcome.BYPASSED, matched=False, bypass_reason=reason
        )

    @classmethod
    def errored(cls, component: GatewayComponent) -> "MatchResult":
        """A component failed and the gateway fell open (design doc §17)."""
        return cls(
            outcome=MatchOutcome.ERROR, matched=False, error_component=component
        )


class DecisionLogRecord(BaseModel):
    """One audit record: what the gateway decided about one block.

    Design doc §21's field list, with §20's privacy default enforced
    structurally: **this type has no field that can hold prompt text.** The
    block is identified by ``block_content_hash`` only. A future phase that
    wants raw text in the audit trail has to add a field here, which is a
    visible contract change rather than an accident -- tests/test_models.py
    locks the field set for exactly that reason.

    A record exists for every block the gateway *examined*. A request the
    gateway never processed at all (feature disabled) is counted by design doc
    §18's request-level metrics instead; there is no block hash to log.
    """

    model_config = _CONTRACT_CONFIG

    request_id: str = Field(
        min_length=1,
        description=(
            "Correlation key. Design doc §18's closing requirement is joining "
            "these records against Phase 9's existing "
            "pulsekv_cache_hits_total/misses series; this field is what makes "
            "that join possible."
        ),
    )
    timestamp: datetime
    namespace: Namespace
    model: str = Field(
        min_length=1,
        description=(
            "The inference model the request targets. Required: §21 lists it "
            "unconditionally, and an OpenAI-compatible request always carries "
            "one."
        ),
    )
    block_index: int = Field(ge=0)
    block_type: BlockType
    block_content_hash: ContentHash = Field(
        description="Hash of the original (normalized) block -- never its text."
    )
    outcome: MatchOutcome
    method: Optional[MatchMethod] = None
    context_id: Optional[ContextId] = None
    version: Optional[int] = Field(default=None, ge=1)
    similarity: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description=(
            "Present exactly when Tier 2 ran. Distinct from MatchResult."
            "confidence, which is 1.0 on a Tier 0/1 hit where no similarity "
            "was ever computed."
        ),
    )
    guard_outcome: Optional[GuardOutcome] = Field(
        default=None,
        description="Present exactly when Tier 3 ran (design doc §11).",
    )
    rejection_reason: Optional[RejectionReason] = None
    bypass_reason: Optional[BypassReason] = None
    error_component: Optional[GatewayComponent] = None

    @field_validator("timestamp")
    @classmethod
    def _timestamp_is_aware(cls, value, info):
        return _require_aware(value, info.field_name)

    @model_validator(mode="after")
    def _check_record(self) -> "DecisionLogRecord":
        problems: list[str] = []

        # Reuse MatchResult's rules for the fields the two share, so the two
        # types can never drift into disagreeing about what a decision is.
        # A deterministic hit's confidence is 1.0 by construction rather than a
        # similarity (design doc §11); passing similarity through for those
        # would re-report the tier-provenance problem checked just below, in
        # MatchResult's words instead of this type's.
        if self.outcome is MatchOutcome.MATCHED and self.method in DETERMINISTIC_METHODS:
            proxy_confidence: Optional[float] = 1.0
        else:
            proxy_confidence = self.similarity
        try:
            MatchResult(
                outcome=self.outcome,
                matched=self.outcome is MatchOutcome.MATCHED,
                context_id=self.context_id,
                version=self.version,
                confidence=proxy_confidence,
                method=self.method,
                rejection_reason=self.rejection_reason,
                bypass_reason=self.bypass_reason,
                error_component=self.error_component,
            )
        except PydanticValidationError as exc:
            # Unwrap to the messages themselves, in this type's vocabulary: a
            # caller should read its own field names, not "1 validation error
            # for MatchResult", and this type calls MatchResult.confidence
            # `similarity` (see that field's docstring for why they differ).
            for err in exc.errors():
                problems.append(
                    str(err["msg"])
                    .removeprefix("Value error, ")
                    .replace("confidence:", "similarity:")
                )

        # Tier-provenance rules that only the log carries (design doc §11):
        # the guard never runs on Tier 0/1, and no embedding is computed there.
        if self.method is not None and self.method in DETERMINISTIC_METHODS:
            if self.similarity is not None:
                problems.append(
                    f"similarity: no embedding is computed for method="
                    f"{self.method.value} (Tier 0/1)"
                )
            if self.guard_outcome is not None:
                problems.append(
                    f"guard_outcome: the guard never runs on method="
                    f"{self.method.value} hits (design doc §11)"
                )
        elif self.method is MatchMethod.SEMANTIC:
            if self.guard_outcome is not GuardOutcome.PASSED:
                problems.append(
                    "guard_outcome: a semantic match is only reachable through "
                    "a passing Tier 3 guard (design doc §12)"
                )

        if self.outcome is MatchOutcome.REJECTED:
            if self.rejection_reason is RejectionReason.LOW_SIMILARITY:
                if self.guard_outcome is not None:
                    problems.append(
                        "guard_outcome: must be unset for low_similarity -- the "
                        "candidate was refused at the τ gate before Tier 3 ran"
                    )
            elif self.rejection_reason is not None:
                expected = {
                    RejectionReason.GUARD_ERROR: GuardOutcome.ERROR,
                    RejectionReason.GUARD_TIMEOUT: GuardOutcome.TIMEOUT,
                }.get(self.rejection_reason, GuardOutcome.REJECTED)
                if self.guard_outcome is not expected:
                    problems.append(
                        f"guard_outcome: rejection_reason="
                        f"{self.rejection_reason.value} requires "
                        f"{expected.value}, got "
                        f"{self.guard_outcome.value if self.guard_outcome else None}"
                    )
        elif self.outcome is not MatchOutcome.MATCHED and self.guard_outcome is not None:
            problems.append(
                f"guard_outcome: must be unset when outcome={self.outcome.value}"
            )

        _raise_problems(problems)
        return self

    @property
    def decision_label(self) -> str:
        """Design doc §21's flat decision vocabulary, rendered from this pair.

        §21 names ``bypassed|exact|alias|structural|semantic|rejected``; a
        match renders as its method and everything else renders as its outcome,
        which additionally yields ``no_candidate`` and ``error`` for the two
        states §21's list does not name (see ``MatchOutcome``).
        """
        if self.outcome is MatchOutcome.MATCHED and self.method is not None:
            return self.method.value
        return self.outcome.value

    @classmethod
    def from_match_result(
        cls,
        result: MatchResult,
        *,
        request_id: str,
        timestamp: datetime,
        namespace: str,
        model: str,
        block: ContextBlock,
        block_content_hash: str,
    ) -> "DecisionLogRecord":
        """Project a matcher decision into an audit record.

        Pure field mapping -- no I/O, no hashing (the caller supplies the hash
        it already computed at Tier 0). It exists so Phase 10.2's auditlog.py
        and any later writer cannot each invent their own mapping of
        ``confidence`` to ``similarity`` or of a rejection reason to a guard
        outcome.
        """
        similarity = (
            result.confidence
            if result.method is None or result.method is MatchMethod.SEMANTIC
            else None
        )
        return cls(
            request_id=request_id,
            timestamp=timestamp,
            namespace=namespace,
            model=model,
            block_index=block.index,
            block_type=block.block_type,
            block_content_hash=block_content_hash,
            outcome=result.outcome,
            method=result.method,
            context_id=result.context_id,
            version=result.version,
            similarity=similarity,
            guard_outcome=_guard_outcome_for(result),
            rejection_reason=result.rejection_reason,
            bypass_reason=result.bypass_reason,
            error_component=result.error_component,
        )


def _guard_outcome_for(result: MatchResult) -> Optional[GuardOutcome]:
    """Which Tier 3 outcome a MatchResult implies, or None if it never ran.

    Total over every state ``MatchResult`` allows: Tier 0/1 hits and misses
    never reach the guard, a semantic match only exists because the guard
    passed, and a rejection maps to the guard verdict its reason describes --
    except LOW_SIMILARITY, which is refused at the τ gate before Tier 3.
    """
    if result.outcome is MatchOutcome.MATCHED:
        return GuardOutcome.PASSED if result.method is MatchMethod.SEMANTIC else None
    if result.outcome is not MatchOutcome.REJECTED:
        return None
    return {
        RejectionReason.LOW_SIMILARITY: None,
        RejectionReason.GUARD_ERROR: GuardOutcome.ERROR,
        RejectionReason.GUARD_TIMEOUT: GuardOutcome.TIMEOUT,
    }.get(result.rejection_reason, GuardOutcome.REJECTED)
