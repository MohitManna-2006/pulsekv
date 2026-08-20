"""Text normalization for the deterministic tiers (Phase 10.2).

Design doc §11 Tier 0 ("normalize whitespace/casing deterministically before
hashing, never normalize meaning") and Tier 1 (structural re-serialization for
block types with real structure); plan §6.

The two functions here are different in kind, and the difference is the whole
correctness argument for Tier 1:

* ``normalize_for_hash`` removes incidental *rendering* differences before the
  exact hash is taken. It must never remove anything a reader could call
  meaning -- notably not punctuation or negation.
* ``normalize_structural`` parses and re-serializes a structured block (a tool
  schema's JSON) into a canonical key order and spacing. It changes zero
  semantic content, only serialization form, which is why design doc §11 rates
  it the strongest guarantee after exact match.

Neither is a canonicalization *decision*: both are deterministic, reversible-in-
principle text transforms feeding Tier 0's hash.

What ``normalize_for_hash`` does, and why each rule is safe
-----------------------------------------------------------
Design doc §11 sanctions "whitespace/casing"; the binding constraint is the
clause that follows it, "never normalize meaning". Every rule below is
insignificant in every text format design doc §13's eligible block types can
carry, and each is listed with what makes it so:

1. **Unicode NFC.** Canonical composition. Not in §11's list, added because it
   is what makes byte-level hashing well-defined at all: ``é`` written as U+00E9
   and as U+0065 U+0301 are *the same character* by Unicode's own definition of
   canonical equivalence, and hashing them differently would be the incidental
   variation Tier 0 exists to absorb. **NFKC is deliberately not used** --
   compatibility decomposition is not meaning-preserving (it rewrites ``ﬁ`` to
   ``fi``, ``½`` to ``1⁄2``, and superscripts to their base digits), which is
   exactly the class of change §11 forbids.
2. **Line endings normalized to LF.** CRLF, CR and LF are the same line break;
   which one appears is a property of the editor and OS that produced the text.
3. **Trailing whitespace stripped per line.** Invisible in every format.
4. **Runs of blank lines collapsed to one.** Insignificant in prose, Markdown
   (two or more blank lines are one paragraph break), JSON, YAML and code.
5. **Leading and trailing blank lines removed.** Note what this is *not*:
   leading whitespace on a line that has content is never touched, on any line
   including the first. Stripping the block as a single string would have
   re-indented the first line relative to the rest, which is a structural
   change wearing a rendering change's clothes.

Only ASCII inline whitespace (space, tab, vertical tab, form feed) is stripped,
never Python's default ``str.strip()`` set: that set includes U+00A0 NO-BREAK
SPACE, which is typographically *not* an ordinary space and whose removal a
reader could reasonably call a change in meaning.

What it deliberately does **not** do
------------------------------------
* **Case folding**, though §11 names casing as normalizable. §11's sentence
  ends "never normalize meaning", and case is meaning-bearing in exactly the
  content this MVP targets: JSON keys in a ``TOOL_SCHEMA`` are case-sensitive,
  and environment names, resource names and command flags are the entity class
  design doc §12's guard exists to protect. That guard **never runs on a Tier
  0/1 hit** (§11), so Tier 0 has no safety net behind it and a normalization
  that can collapse two distinct entities is strictly more dangerous here than
  at Tier 2. Design doc §4's "bias hard toward zero false positives" is the
  tiebreaker. Nothing is lost against the motivating workload: §3 names
  whitespace, ordering and wording drift as the variation to absorb, not case.
  Phase 10.4's adversarial-negative corpus is where this should be settled with
  data -- a case-only entity swap is precisely the shape that set will contain.
* **Collapsing whitespace runs inside a line.** This is the one whitespace
  operation that can change meaning: indentation is syntax in code and in YAML,
  and column alignment is structure in a table. ``RAG_DOCUMENT`` is an eligible
  type that can carry either.
* **Punctuation stripping** and **stop-word removal**, neither of which §11
  mentions and both of which delete negation and scope markers outright.

Idempotence is a property, not an accident: ``normalize_for_hash`` applied
twice equals applying it once, which is what lets the registry hash a canonical
text on write and the matcher hash an incoming block on read and get the same
answer. It is asserted by test.
"""

from __future__ import annotations

import json
import unicodedata
from typing import Any, List, Tuple

from .models import BlockType, GatewayError
from .registry import content_hash_for

__all__ = [
    "BLOCK_TYPES_WITH_STRUCTURE",
    "StructuralNormalizationError",
    "canonical_registration_text",
    "hash_normalized",
    "normalize_for_hash",
    "normalize_structural",
    "supports_structural",
]

# Space, tab, vertical tab, form feed. Deliberately not Python's default strip
# set -- see the module docstring on U+00A0.
_INLINE_WHITESPACE = " \t\v\f"

# Design doc §13's table gives exactly one row the "Tier 1 (structural) then
# Tier 0" treatment. A type is listed here only when a *parser* can prove the
# re-serialization preserves meaning; prose has no such parser, which is why
# every other eligible type goes through Tier 0 alone.
BLOCK_TYPES_WITH_STRUCTURE = frozenset({BlockType.TOOL_SCHEMA})


class StructuralNormalizationError(GatewayError):
    """A block did not parse as the structure its ``block_type`` claims.

    Raised rather than returning a best-effort rewrite: design doc §11 rates
    Tier 1's guarantee as "changes zero semantic content, only serialization
    form", and that claim only holds when the parse actually succeeded. A block
    that fails here falls through to the ordinary path unchanged.

    A ``GatewayError`` subclass so Phase 10.5's fail-open wiring stays one
    ``except`` clause (design doc §17).
    """


def normalize_for_hash(text: str) -> str:
    """Deterministic pre-hash normalization (Tier 0).

    See the module docstring for the rule list and the justification for each,
    including the two rules §11 permits that are deliberately not implemented.
    """
    text = unicodedata.normalize("NFC", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # split("\n") rather than splitlines(): splitlines also breaks on \x0b,
    # \x0c, \x1c-\x1e, \x85, U+2028 and U+2029, which would silently turn a
    # form feed inside a line into a line break -- a structural change, not a
    # rendering one.
    collapsed: List[str] = []
    for line in text.split("\n"):
        line = line.rstrip(_INLINE_WHITESPACE)
        if not line and collapsed and not collapsed[-1]:
            continue
        collapsed.append(line)

    # Leading/trailing *blank lines* go; leading whitespace on a line that has
    # content stays. Stripping the block as one string would have removed the
    # first line's indentation while every other line kept its own -- which is
    # not a rendering change, it is an inconsistent re-indent.
    while collapsed and not collapsed[0]:
        collapsed.pop(0)
    while collapsed and not collapsed[-1]:
        collapsed.pop()
    return "\n".join(collapsed)


def hash_normalized(text: str) -> str:
    """The content hash both sides of Tier 0 must agree on.

    **A registry that serves Tier 0 must be constructed with this function**::

        Registry(path, hash_text=hash_normalized)

    Phase 10.1 left ``hash_text`` as a constructor parameter for exactly this
    handoff (Phase 10.1 summary §4): 10.1 hashes the text it is given, 10.2
    decides what is given. A registry built with the default plain-SHA-256
    hasher stores hashes of un-normalized text, and Tier 0 would then miss
    every record that has so much as a trailing space -- so this is a
    deployment-time requirement, not a preference.
    """
    return content_hash_for(normalize_for_hash(text))


def supports_structural(block_type: BlockType) -> bool:
    """Whether Tier 1 has a canonical serialization for this block type."""
    return block_type in BLOCK_TYPES_WITH_STRUCTURE


def normalize_structural(text: str, block_type: BlockType) -> str:
    """Canonical re-serialization for a structured block type (Tier 1).

    Raises rather than guessing when ``text`` does not parse as the structure
    its ``block_type`` claims: a block that fails to parse falls through to the
    ordinary path, it does not get a best-effort rewrite.

    The canonical form is JSON with keys sorted, no insignificant whitespace,
    and non-ASCII left as itself (the NFC pass in ``normalize_for_hash``
    already settled its encoding). Two refusals are worth naming, because both
    are places a naive re-serializer would silently invent equivalence:

    * **Duplicate keys.** RFC 8259 leaves the meaning of a repeated key
      undefined and Python's parser keeps the last one, so ``{"a":1,"a":2}``
      and ``{"a":2}`` would canonicalize identically while being two different
      documents to some other reader. Refused.
    * **``NaN``/``Infinity``.** Accepted by Python's parser as an extension,
      not valid JSON, and not representable on the way back out. Refused.
    """
    if not supports_structural(block_type):
        raise StructuralNormalizationError(
            f"{block_type.value}: no canonical serialization is defined for this "
            f"block type (design doc §13 lists only tool_schema)"
        )
    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_constant,
        )
        return json.dumps(
            parsed,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except RecursionError as exc:
        raise StructuralNormalizationError(
            f"{block_type.value}: JSON nested too deeply to canonicalize"
        ) from exc
    except (ValueError, TypeError) as exc:
        raise StructuralNormalizationError(
            f"{block_type.value}: not canonicalizable as JSON: {exc}"
        ) from exc


def canonical_registration_text(text: str, block_type: BlockType) -> str:
    """The form a canonical context must be *registered* in to be reachable.

    Tier 1 looks a block up by the hash of its canonical serialization, and the
    registry stores one hash per record, so a ``TOOL_SCHEMA`` registered in a
    pretty-printed form is unreachable from a compact one and vice versa. This
    function is the single answer to "what text do I register", and the
    registration path should route through it rather than re-deriving the rule.

    Falls back to the text unchanged for types with no structure, so a caller
    can apply it uniformly without branching on block type.
    """
    if supports_structural(block_type):
        return normalize_structural(text, block_type)
    return text


def _reject_duplicate_keys(pairs: List[Tuple[str, Any]]) -> dict:
    seen = set()
    for key, _value in pairs:
        if key in seen:
            raise ValueError(f"duplicate object key {key!r}")
        seen.add(key)
    return dict(pairs)


def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name} is not valid JSON")
