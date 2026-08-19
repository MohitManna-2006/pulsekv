# Evaluation corpus

**Status: skeleton only.** Phase 10.0 creates the four category directories and
this description of what belongs in each. **No examples are written yet** —
that is Phase 10.4's work, and deliberately so: an adversarial example is only
meaningful once there is a real Tier 3 guard to run it against, and examples
written before the guard exists tend to describe the guard the author imagined
rather than the failures the guard actually has.

The four categories mirror the test classes in
`docs/pulsekv-semantic-context-design.md` §12 and
`docs/pulsekv-semantic-context-implementation-plan.md` §8. Each directory below
states the property Phase 10.4 asserts against it — a property, not a vibe,
because "hard examples" is not something a test suite can fail.

The corpus is checked in and re-run as a regression gate on every later change
to `guardrail.py`, `encoder.py`, or the registry's embedding model version
(risk register row 1), not just once in Phase 10.4.

---

## `positive_paraphrase/`

Pairs where a registered canonical context and an incoming block genuinely mean
the same thing but are worded differently: whitespace and ordering churn, a
maintainer's wording tweak between deploys, two teams paraphrasing one internal
policy document. These are the cases the whole feature exists to capture
(design doc §3).

**Property Phase 10.4 asserts:** the match rate is *measured and reported
honestly*, not gated at a number. Plan §8 is explicit that a low match rate
with zero false positives is a legitimate, ship-able outcome, and is preferred
over lowering τ to force the rate up. A regression in this rate is a signal to
investigate, not a build failure.

## `adversarial_negative/`

High-similarity, opposite-meaning pairs — the specific failure modes design doc
§12 names: negation and exception markers (`not`, `never`, `without`,
`except`, `unless`), entity and value swaps (`staging` vs `production`,
`--force` vs `--dry-run`), before/after and threshold inversions. These are
built to be *hard for cosine similarity specifically*: a pair that a naive
embedding cutoff rates near-identical while a careful reader calls them
opposites.

**Property Phase 10.4 asserts: zero false positives on this corpus, confirmed
by test, at whatever τ the phase lands on.** Not "low", not a probabilistic
claim — zero, or the τ is wrong. This suite is what earns the τ threshold in
the first place; a threshold picked from general embedding folklore and then
checked here would have the reasoning backwards (design doc §12).

## `cross_tenant/`

Two namespaces that register near-identical (or identical) canonical texts, and
requests in one namespace that would match the other's entry if namespace were
ever treated as a post-hoc filter instead of a retrieval pre-filter.

**Property Phase 10.4 asserts:** no candidate from another namespace is ever
returned, retrieved, or scored — the isolation holds structurally, at the
retrieval layer, rather than being caught by a check on the winner (design doc
§15). Phase 10.1 proves the same property one layer lower, at storage; this
suite proves the matcher above it did not undo that.

## `version_update/`

A registered context that gets a new published version: decisions logged
against version 4 while version 5 exists, a deprecated version that must stop
being a match target, and an alias that survives (or does not survive) a
version bump.

**Property Phase 10.4 asserts:** an older version's already-logged decisions
remain interpretable after a new version is published, and a new version never
retroactively changes what an old decision meant (design doc §10, §17). This is
the audit trail's whole basis — a hit against version 4 has to mean the same
substituted text forever.

---

## What does not belong here

- **Real customer or production prompt text.** Design doc §20 keeps raw prompt
  text out of the audit trail by default; a corpus checked into the repository
  is a weaker place for it, not a stronger one. Examples should be synthetic or
  drawn from public/internal-generic material.
- **`USER_QUERY` or `CONVERSATION_HISTORY` pairs.** Those types are ineligible
  by design (§13), not merely unhandled; a corpus entry for them would be
  testing something the gateway must never do.
- **Examples that only exercise Tier 0.** A byte-identical pair proves the hash
  works, which unit tests already cover. This corpus is for the cases where
  judgement is involved.

## File format

Deliberately not fixed by Phase 10.0. Phase 10.4 chooses it against its own
harness. Whatever it chooses must carry, per example: the namespace, the block
type, the registered canonical text (or a reference to a fixture record), the
incoming block text, and the expected outcome — including, for the negative
suites, the specific `RejectionReason` expected, so a test that passes for the
wrong reason fails.
