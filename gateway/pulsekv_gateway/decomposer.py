"""Request decomposition (Phase 10.2).

Design doc §13 (per-block granularity, and why neither whole-prompt nor
sub-block is the MVP unit); plan §6.

Decomposition is per-block, in the request's own order. A block this module
cannot classify is not a candidate for anything: it is emitted with the
narrowest type that fits and, if that type is ineligible under
``models.BLOCK_ELIGIBILITY``, it is forwarded untouched. Design doc §5's
non-goals make ``USER_QUERY`` never eligible, so a misclassification that
*widens* eligibility is the failure mode to design against here.

Classification, and its one honest limitation
---------------------------------------------
An OpenAI-compatible request carries three things this module can classify
without guessing:

* a ``system`` message -> ``SYSTEM_PROMPT``
* an entry in ``tools`` -> ``TOOL_SCHEMA``
* the final ``user`` message -> ``USER_QUERY`` (never eligible, §5)

Everything else -- assistant turns, tool results, and any ``user`` message that
is not the last one -- is ``CONVERSATION_HISTORY``, which §13 marks ineligible
for both reuse and leakage reasons. Both fallbacks are ineligible, so the
distinction between them changes no behavior; it exists so the decision log
says what the gateway actually saw.

**The limitation:** four of §13's six eligible types -- ``ORG_POLICY``,
``TOOL_POLICY``, ``AGENT_INSTRUCTION`` and ``RAG_DOCUMENT`` -- are not derivable
from an unannotated OpenAI request. They all arrive inside a system message and
nothing in the wire format distinguishes an org policy from any other system
text. Guessing between them by inspecting content would be exactly the
eligibility-widening failure this module is designed against.

So they arrive only when the application says so, via the optional
``x_pulsekv_block_type`` key on a message object. That key is a minimal
extension point, not a finished interface: **Phase 10.5 owns the request
surface** and may formalize, rename or replace it. It is here because without
it this phase could only ever produce two of six eligible types, and plan §6's
integration test would exercise a quarter of the taxonomy. An unrecognized
value raises rather than being ignored, matching this codebase's
``dec.KnownFields(true)`` posture -- the caller decides whether to fail open,
which is design doc §17's split, not this module's.

Multimodal content (a ``content`` that is a list of parts rather than a string)
is not decomposed: nothing in §13's taxonomy names it, and a block that is not
emitted is a block that is forwarded untouched, which is the safe direction.
"""

from __future__ import annotations

import json
from typing import Any, List, Mapping, Optional, Sequence, Tuple

from .models import BlockType, ContextBlock, GatewayError

__all__ = [
    "BLOCK_TYPE_ANNOTATION",
    "DecompositionError",
    "decompose",
]

# See the module docstring: an extension point Phase 10.5 may replace, not a
# frozen part of the wire format.
BLOCK_TYPE_ANNOTATION = "x_pulsekv_block_type"

_ROLE_DEFAULTS = {
    "system": BlockType.SYSTEM_PROMPT,
    "developer": BlockType.SYSTEM_PROMPT,  # OpenAI's newer name for the same role
}


class DecompositionError(GatewayError):
    """A request could not be decomposed as an OpenAI-compatible request.

    A ``GatewayError`` subclass so Phase 10.5's fail-open wiring stays one
    ``except`` clause (design doc §17). Raised rather than silently degraded,
    because every case that reaches it is a malformed request or a typo'd
    annotation -- both of which are worth an operator seeing.
    """


def decompose(request: Mapping[str, Any]) -> Tuple[ContextBlock, ...]:
    """Split an OpenAI-compatible request into ordered blocks.

    Returned in request order with ``ContextBlock.index`` matching position;
    design doc §14 forbids reordering, so the index is the assembler's identity
    for a block on the way back out.

    ``index`` is the position in *this function's* deterministic ordering --
    every message in request order, then every tool in request order. Tools are
    a separate top-level array in the wire format rather than interleaved with
    messages, so there is no single original sequence to index into; the
    assembler recovers a block's location by running the same decomposition,
    which is deterministic for a given request.

    ``token_estimate`` is left unset. Phase 10.0's summary (§5.2) records that
    design doc §19 states the bypass threshold in tokens while §22 rejects
    gateway-side tokenization, and assigns the resolution of that tension to
    Phase 10.5. This phase does not pre-empt it with a guess.
    """
    if not isinstance(request, Mapping):
        raise DecompositionError(
            f"request must be a mapping, got {type(request).__name__}"
        )

    messages = request.get("messages", ())
    if not isinstance(messages, Sequence) or isinstance(messages, (str, bytes)):
        raise DecompositionError("messages must be a list of message objects")

    tools = request.get("tools", ())
    if not isinstance(tools, Sequence) or isinstance(tools, (str, bytes)):
        raise DecompositionError("tools must be a list of tool objects")

    last_user = _last_user_index(messages)
    blocks: List[ContextBlock] = []

    for position, message in enumerate(messages):
        if not isinstance(message, Mapping):
            raise DecompositionError(
                f"messages[{position}] must be a message object, got "
                f"{type(message).__name__}"
            )
        content = message.get("content")
        if not isinstance(content, str) or not content:
            # Empty, absent, or multimodal content: no block, so the message is
            # forwarded untouched. ContextBlock.text is min_length=1 anyway.
            continue
        blocks.append(
            ContextBlock(
                index=len(blocks),
                block_type=_classify(message, position, last_user),
                text=content,
            )
        )

    for position, tool in enumerate(tools):
        if not isinstance(tool, Mapping):
            raise DecompositionError(
                f"tools[{position}] must be a tool object, got {type(tool).__name__}"
            )
        blocks.append(
            ContextBlock(
                index=len(blocks),
                block_type=BlockType.TOOL_SCHEMA,
                # Tools arrive already parsed, so no original byte sequence
                # survives to preserve. This re-serialization keeps the
                # request's own key order, which is what leaves Tier 1's
                # canonical re-ordering something real to do.
                text=json.dumps(tool, ensure_ascii=False, sort_keys=False),
            )
        )

    return tuple(blocks)


def _classify(
    message: Mapping[str, Any], position: int, last_user: Optional[int]
) -> BlockType:
    annotated = message.get(BLOCK_TYPE_ANNOTATION)
    if annotated is not None:
        try:
            return BlockType(annotated)
        except ValueError as exc:
            raise DecompositionError(
                f"messages[{position}].{BLOCK_TYPE_ANNOTATION}: {annotated!r} is not "
                f"a known block type (see models.BlockType)"
            ) from exc

    role = message.get("role")
    if role in _ROLE_DEFAULTS:
        return _ROLE_DEFAULTS[role]
    if role == "user" and position == last_user:
        return BlockType.USER_QUERY
    # Assistant turns, tool results, and every user message that is not the
    # last one. Ineligible either way; named honestly for the decision log.
    return BlockType.CONVERSATION_HISTORY


def _last_user_index(messages: Sequence[Any]) -> Optional[int]:
    last: Optional[int] = None
    for position, message in enumerate(messages):
        if isinstance(message, Mapping) and message.get("role") == "user":
            last = position
    return last
