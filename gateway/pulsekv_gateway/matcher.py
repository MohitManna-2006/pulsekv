"""Tier orchestration -- STUB. Tiers 0/1 in Phase 10.2, 2/3 in Phase 10.3/10.4.

Design doc §11 (four tiers, cheapest and most deterministic first, each a
strict filter before the next is attempted -- not parallel races); plan §6, §7,
§8.

The tier order is a correctness property, not an optimization:

  0. exact hash (and registered aliases)
  1. structural normalization, then tier 0's hash, for structured types only
  2. embedding + candidate retrieval, namespace-pre-filtered -- produces
     candidates, never a decision
  3. equivalence guard against the top candidate -- reject-biased

Every failure, timeout, miss and rejection produces a ``MatchResult`` that does
not substitute, and the caller forwards the original block unchanged. Design
doc §7.3 admits no partial-credit mode: there is no "lower-confidence canonical
guess" state to fall back to.
"""

from __future__ import annotations

from .models import ContextBlock, MatchResult


class Matcher:
    """Resolves one block to a ``MatchResult`` through tiers 0-3."""

    def resolve(self, block: ContextBlock, namespace: str) -> MatchResult:
        """Run the tiers in order and return the first accepted match.

        Never raises for an expected failure: a registry outage, an encoder
        timeout or a guard error each become a non-substituting ``MatchResult``
        (``MatchOutcome.ERROR`` with the failing component) so the caller's
        fail-open path is the same path as an ordinary miss (design doc §17).
        """
        raise NotImplementedError("Phase 10.2 (tiers 0/1), 10.3-10.4 (tiers 2/3)")
