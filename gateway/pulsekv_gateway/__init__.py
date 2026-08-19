"""PulseKV Context Gateway.

Phase 10.0 froze this package's contract types; everything else here is a
signature-only stub. See ``models`` for the contract and
``docs/pulsekv-semantic-context-phase10.0-summary.md`` for what was frozen and
why.

Only ``models`` is re-exported. Importing the contract deliberately does not
pull in the stub modules, so a later phase adding a real dependency to (say)
``encoder`` cannot make the contract itself unimportable.
"""

from __future__ import annotations

from .models import (
    BLOCK_ELIGIBILITY,
    CONTENT_HASH_ALGORITHM,
    CONTENT_HASH_PATTERN,
    DETERMINISTIC_METHODS,
    IDENTIFIER_PATTERN,
    MAX_CONTEXT_ID_CHARS,
    MAX_NAMESPACE_CHARS,
    BlockEligibility,
    BlockType,
    BypassReason,
    Candidate,
    CanonicalContextRecord,
    ContentHash,
    ContextBlock,
    ContextId,
    DecisionLogRecord,
    GatewayComponent,
    GatewayError,
    GuardOutcome,
    GuardResult,
    MatchMethod,
    MatchOutcome,
    MatchResult,
    Namespace,
    RejectionReason,
    is_mvp_eligible,
)

__version__ = "0.0.1"

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
    "__version__",
    "is_mvp_eligible",
]
