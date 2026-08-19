"""Request decomposition -- STUB. Implemented by Phase 10.2.

Design doc §13 (per-block granularity, and why neither whole-prompt nor
sub-block is the MVP unit); plan §6.

Decomposition is per-block, in the request's own order. A block this module
cannot classify is not a candidate for anything: it is emitted with the
narrowest type that fits and, if that type is ineligible under
``models.BLOCK_ELIGIBILITY``, it is forwarded untouched. Design doc §5's
non-goals make ``USER_QUERY`` never eligible, so a misclassification that
*widens* eligibility is the failure mode to design against here.
"""

from __future__ import annotations

from typing import Any, Mapping, Tuple

from .models import ContextBlock


def decompose(request: Mapping[str, Any]) -> Tuple[ContextBlock, ...]:
    """Split an OpenAI-compatible request into ordered blocks.

    Returned in request order with ``ContextBlock.index`` matching position;
    design doc §14 forbids reordering, so the index is the assembler's identity
    for a block on the way back out.
    """
    raise NotImplementedError("Phase 10.2")
