"""Tier 3 equivalence guard -- STUB. Implemented by Phase 10.4.

Design doc §12 (the hardest correctness problem in the design); plan §8.

Three checks, all deterministic, all reject-biased:

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

**τ is not defined here.** Design doc §12 refuses to assert a threshold before
an evaluation corpus exists, because the adversarial-negative corpus is built
from high-similarity/opposite-meaning pairs -- exactly what a cutoff picked
from general folklore separates worst. τ is a Phase 10.4 deliverable, earned
against ``tests/corpus/adversarial_negative/`` and reported with the corpus
size and methodology that produced it.

Any error or timeout inside this module is a reject, never a pass.
"""

from __future__ import annotations

from .models import Candidate, ContextBlock, GatewayError, GuardResult


class GuardrailError(GatewayError):
    """A guard failure. Callers treat it as a reject (design doc §12, §17)."""


class Guardrail:
    """Decides whether a Tier 2 candidate may actually be substituted."""

    def check(self, block: ContextBlock, candidate: Candidate) -> GuardResult:
        """Run the three checks against one candidate.

        Returns a ``GuardResult``; a caller that receives anything other than
        ``GuardOutcome.PASSED`` forwards the block's original text unchanged.
        """
        raise NotImplementedError("Phase 10.4")
