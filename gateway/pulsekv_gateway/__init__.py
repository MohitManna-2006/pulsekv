"""PulseKV Context Gateway.

Phase 10.5 completes the standalone OpenAI-compatible reverse proxy around the
contract, registry, deterministic tiers, semantic retrieval, and guardrail.
See ``models`` for the frozen contract and the phase summaries in ``docs/``
for the evidence behind each implemented layer.

Only ``models`` is re-exported. Importing the contract deliberately does not
start the proxy or open its registry.
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
