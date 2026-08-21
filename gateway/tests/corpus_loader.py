"""Loading and running ``gateway/tests/corpus/`` (Phase 10.4).

The corpus is data, not code: every example is a JSON file stating what is
registered, what arrives, and what must happen. This module is the only thing
that knows how to turn one of those files into a live registry, index and
matcher -- so ``test_guardrail.py`` reads as assertions about outcomes rather
than as fixture plumbing, and so a later phase can re-run the same corpus
against a changed guard without reimplementing the harness (the corpus README
requires exactly that: a regression gate on every later change to
``guardrail.py``, ``encoder.py``, or the registry's embedding model version).

The file format, which Phase 10.0 deliberately left to this phase
--------------------------------------------------------------

::

    {
      "id":       "negation-never-vs-always",
      "category": "adversarial_negative",
      "why":      "what this example proves, and why cosine cannot see it",
      "records":  [ {context_id, version, namespace, block_type,
                     canonical_text, aliases?, deprecated?} ],
      "query":    {namespace, block_type, text},
      "expect":   {outcome, method?, rejection_reason?, context_id?, version?,
                   never_retrieves?},
      "guard_direct": {...},   # optional -- see below
      "then":     {publish?, expect?, expect_previous_decision_unchanged?}
    }

``canonical_text`` and ``text`` are a string, or a list of strings joined with
newlines so a multi-line block stays readable in the file rather than becoming
one long escaped line.

The README's requirement that a negative example name "the specific
``RejectionReason`` expected, so a test that passes for the wrong reason fails"
is ``expect.rejection_reason``, and it is compared, never merely printed.

**No example carries a similarity number.** Similarity is a property of the
encoder, not of the pair: pinning one in the corpus would turn a model upgrade
(design doc §16 already treats that as invalidating every stored vector) into a
corpus-wide diff of numbers nobody could review. The harness measures and
reports them instead, and the only threshold anywhere is τ.

``guard_direct``
    Two failure modes are, by construction, unreachable through retrieval: a
    block type Tier 2 partitions away, and a namespace Tier 2 partitions away.
    An example that names one of them still has to prove the guard would refuse
    it, so ``guard_direct`` tells the harness to hand ``Guardrail.check`` a
    candidate it could never have retrieved. Both halves are asserted -- that
    retrieval does not surface it, and that the guard refuses it -- because
    those are two different claims and only the second one survives someone
    deleting the partition.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pulsekv_gateway.encoder import Encoder, vector_to_bytes
from pulsekv_gateway.index import VectorIndex
from pulsekv_gateway.models import (
    BlockType,
    CanonicalContextRecord,
    Candidate,
    ContextBlock,
    MatchMethod,
    MatchOutcome,
    RejectionReason,
)
from pulsekv_gateway.normalizer import (
    canonical_registration_text,
    hash_normalized,
    normalize_for_hash,
)
from pulsekv_gateway.registry import Registry, RegistryNotFoundError

CORPUS_ROOT = Path(__file__).parent / "corpus"
CATEGORIES = (
    "positive_paraphrase",
    "adversarial_negative",
    "cross_tenant",
    "version_update",
)

# Every corpus record is created at one instant so a test never depends on wall
# clock ordering. Deprecation happens a second later, which is all the registry
# needs to accept it (``deprecated_at`` must not precede ``created_at``).
CORPUS_NOW = datetime(2026, 8, 20, 12, 0, 0, tzinfo=timezone.utc)


def text_of(value) -> str:
    """A corpus text field: a string, or lines to join."""
    return "\n".join(value) if isinstance(value, list) else value


class Example:
    """One corpus file, parsed. Attribute access, so a typo is an error."""

    __slots__ = (
        "id", "category", "why", "records", "query", "expect",
        "guard_direct", "then", "path",
    )

    def __init__(self, payload: Mapping, path: Path) -> None:
        self.path = path
        self.id = payload["id"]
        self.category = payload["category"]
        self.why = payload["why"]
        self.records = tuple(payload["records"])
        self.query = payload["query"]
        self.expect = payload["expect"]
        self.guard_direct = payload.get("guard_direct")
        self.then = payload.get("then")
        if self.category != path.parent.name:
            raise ValueError(
                f"{path}: category {self.category!r} does not match its directory"
            )

    def __repr__(self) -> str:  # pragma: no cover -- pytest ids use `id`
        return f"<Example {self.category}/{self.id}>"

    @property
    def block(self) -> ContextBlock:
        return ContextBlock(
            index=0,
            block_type=BlockType(self.query["block_type"]),
            text=text_of(self.query["text"]),
        )

    @property
    def namespace(self) -> str:
        return self.query["namespace"]


def load(category: Optional[str] = None) -> Tuple[Example, ...]:
    """Every example, or every example in one category, ordered by id."""
    categories = (category,) if category else CATEGORIES
    found: List[Example] = []
    for name in categories:
        directory = CORPUS_ROOT / name
        for path in sorted(directory.glob("*.json")):
            found.append(Example(json.loads(path.read_text(encoding="utf-8")), path))
    return tuple(found)


def build_record(
    spec: Mapping, encoder: Optional[Encoder] = None
) -> CanonicalContextRecord:
    """One corpus record spec as a registry record.

    Registered in the form that is actually reachable:
    ``canonical_registration_text`` for a block type with a canonical
    serialization, and ``hash_normalized`` for the content hash -- the same
    two functions Tier 0/1 look a block up with. Getting either wrong would
    make a corpus example unmatchable for a reason that has nothing to do with
    the guard.

    Without an ``encoder`` the record carries no embedding, which is exactly
    what the guard-only half of the suite needs: those tests call
    ``Guardrail.check`` directly and never retrieve anything.
    """
    block_type = BlockType(spec["block_type"])
    text = canonical_registration_text(text_of(spec["canonical_text"]), block_type)
    fields = dict(
        context_id=spec["context_id"],
        version=spec["version"],
        namespace=spec["namespace"],
        canonical_text=text,
        content_hash=hash_normalized(text),
        block_type=block_type,
        aliases=tuple(spec.get("aliases", ())),
        created_at=CORPUS_NOW,
        created_by="phase-10.4-corpus",
    )
    if encoder is not None:
        fields.update(
            embedding_model_id=encoder.model_id,
            embedding_model_version=encoder.model_version,
            embedding=vector_to_bytes(encoder.encode(normalize_for_hash(text))),
        )
    return CanonicalContextRecord(**fields)


def populate(
    registry: Registry, specs: Iterable[Mapping], encoder: Optional[Encoder] = None
) -> Dict[Tuple[str, str, int], CanonicalContextRecord]:
    """Register every spec through the API an operator would use.

    Versions of one context go in ascending order -- ``register`` for the
    first, ``publish_version`` for the rest -- and a version flagged
    ``deprecated`` is deprecated as soon as it is inserted rather than at the
    end. That ordering is load-bearing, not tidiness: the registry refuses two
    *live* records with the same content hash in one namespace (Tier 0 must
    resolve to one record), so a v2 that restates v1 verbatim is only
    insertable once v1 has stopped being live.

    Whether a context is new is asked of the **registry**, not remembered
    across this call. A version_update example publishes its later versions in
    a second ``populate`` -- after a decision has been logged against the
    first -- and call-local memory would make that second call take the
    ``register`` path, which only moves the current-version pointer for a
    context it is creating. The pointer would silently stay on v1, the index
    (built from current versions) would never see v2, and the example would
    quietly test nothing.
    """
    stored: Dict[Tuple[str, str, int], CanonicalContextRecord] = {}
    ordered = sorted(
        specs, key=lambda s: (s["namespace"], s["context_id"], s["version"])
    )
    for spec in ordered:
        record = build_record(spec, encoder)
        key = (record.namespace, record.context_id, record.version)
        if key in stored:
            continue
        if _context_exists(registry, record.namespace, record.context_id):
            registry.publish_version(record)
        else:
            registry.register(record)
        if spec.get("deprecated"):
            record = registry.deprecate(
                record.context_id,
                record.namespace,
                record.version,
                CORPUS_NOW.replace(second=1),
            )
        stored[key] = record
    return stored


def _context_exists(registry: Registry, namespace: str, context_id: str) -> bool:
    try:
        registry.get(context_id, namespace)
    except RegistryNotFoundError:
        return False
    return True


def build_index(
    registry: Registry, encoder: Encoder, namespaces: Sequence[str]
) -> VectorIndex:
    index = VectorIndex(encoder)
    index.build_from_registry(registry, namespaces=list(dict.fromkeys(namespaces)))
    return index


def namespaces_of(examples: Iterable[Example]) -> Tuple[str, ...]:
    seen: List[str] = []
    for example in examples:
        for spec in example.records:
            if spec["namespace"] not in seen:
                seen.append(spec["namespace"])
        if example.namespace not in seen:
            seen.append(example.namespace)
    return tuple(seen)


def candidate_for(
    example: Example,
    stored: Mapping[Tuple[str, str, int], CanonicalContextRecord],
    spec: Mapping,
) -> Candidate:
    """A ``Candidate`` the index could not have produced (see ``guard_direct``)."""
    key = (
        spec.get("namespace", example.records[0]["namespace"]),
        spec.get("context_id", spec.get("candidate_context_id")),
        spec.get("version", 1),
    )
    return Candidate(record=stored[key], similarity=spec.get("similarity", 1.0))


def expected_outcome(expect: Mapping) -> MatchOutcome:
    return MatchOutcome(expect["outcome"])


def expected_method(expect: Mapping) -> Optional[MatchMethod]:
    value = expect.get("method")
    return MatchMethod(value) if value else None


def expected_reason(expect: Mapping) -> Optional[RejectionReason]:
    value = expect.get("rejection_reason")
    return RejectionReason(value) if value else None


def describe(result, candidates: Sequence[Candidate] = ()) -> str:
    """A one-line rendering for an assertion message or a report row."""
    top = f"{candidates[0].similarity:.4f}" if candidates else "-"
    detail = result.method.value if result.method else (
        result.rejection_reason.value if result.rejection_reason else ""
    )
    return f"{result.outcome.value}({detail}) top={top}"
