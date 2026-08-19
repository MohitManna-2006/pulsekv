"""Prompt assembly -- STUB. Implemented by Phase 10.5.

Design doc §14 (substitute in place, preserve the application's block order
exactly, never reorder to "maximize cache reuse"); plan §9.

Reordering is out of scope for the same reason unproven rewording is: it is a
second semantics-changing operation, and this design makes safety claims about
exactly one. An application whose prompt puts dynamic content before static
content gets no prefix-caching benefit -- from this gateway or from PulseKV's
exact matching with or without it -- and that is a property of the
application's own prompt construction, correctly out of scope to silently
"fix".

Risk register row 11 is this module's failure mode: an assembler that corrupts
a block it was supposed to leave alone. Phase 10.5's test for it is a
byte-for-byte identity assertion -- a request with no accepted matches must
produce output byte-identical to its input.
"""

from __future__ import annotations

from typing import Mapping, Sequence, Tuple

from .models import ContextBlock


def assemble(
    blocks: Sequence[ContextBlock], substitutions: Mapping[int, str]
) -> Tuple[str, ...]:
    """Render blocks in their original order, substituting where accepted.

    ``substitutions`` maps ``ContextBlock.index`` to the canonical text an
    accepted ``MatchResult`` resolved to. A block with no entry is emitted
    byte-identical to its input -- that is the whole of design doc §7.3's
    fallback, and it covers misses, rejections, bypasses and component errors
    alike, since none of them produce a substitution.

    The OpenAI-compatible request envelope this feeds back into is Phase 10.5's
    (``server``); this function deals only in block text.
    """
    raise NotImplementedError("Phase 10.5")
