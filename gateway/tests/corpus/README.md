# Evaluation corpus

**Status: populated (Phase 10.4).** 44 examples across the four categories, run
as a regression gate by `gateway/tests/test_guardrail.py`. Phase 10.0 created
the directories and the properties below and deliberately wrote no examples,
on the grounds that an adversarial example is only meaningful once there is a
real Tier 3 guard to run it against. That turned out to be right in a way it
could not have predicted: three of these examples exist *because* the guard
existed to be measured, and the corpus changed the guard rather than the other
way round —

| Example | What it changed |
|---|---|
| `positive_paraphrase/retention-90` | Refused for carrying the adverb `afterwards`. The guard's order family was narrowed to the prepositional forms (`before`/`prior`/`after`), which relate two clauses, and the adverbs that a paraphrase adds and drops freely were removed |
| `adversarial_negative/entity-added-production` | Satisfies design doc §12's superset-or-equal entity rule *and* is a false positive. The entity check compares for equality instead |
| `version_update/old-decision-stays-interpretable` | Registered a real limitation rather than a pass: a new version that adds a clause carrying no polarity term and no extractable value is accepted |

Read the `why` field of any example for what it proves and, where the measured
outcome is not the obvious one, what it measured.

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

Two things *are* asserted, because neither is about the rate: a positive that
matches must have matched the record its own file names (a match to something
else is a different event, not a success), and the rate must not be zero, which
would mean the guard refuses everything and the suite had stopped measuring.

**Measured, 13 examples, τ = 0.90: 9 matched (69%).** Of the four misses, three
score 0.8462-0.8874 and are refused by τ; one (`deletion-approval`) is refused
by the guard, and its file says exactly which family refused it and what
keeping that family buys.

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

**Measured, 25 examples, τ = 0.90: zero, in two independent runs** — one
registry per example, and one registry holding every record in the corpus at
once. Similarities range 0.1333 to 1.0000, and 17 of the 24 scored pairs sit at
or above the *lowest-scoring genuine paraphrase* in `positive_paraphrase/`.
Two of them reach 1.0000, tied with a real paraphrase at the same score.

The `tau-*` examples are a distinct class and are named so they stay one: they
are the pairs every deterministic check *passes*, so τ is the only thing
refusing them. `test_adversarial_examples_are_refused_for_the_stated_reason`
asserts the guard finds nothing in them — if a check ever started catching one,
τ would silently stop being load-bearing, and the number would stop being
earned.

## `cross_tenant/`

Two namespaces that register near-identical (or identical) canonical texts, and
requests in one namespace that would match the other's entry if namespace were
ever treated as a post-hoc filter instead of a retrieval pre-filter.

**Property Phase 10.4 asserts:** no candidate from another namespace is ever
returned, retrieved, or scored — the isolation holds structurally, at the
retrieval layer, rather than being caught by a check on the winner (design doc
§15). Phase 10.1 proves the same property one layer lower, at storage; this
suite proves the matcher above it did not undo that.

**Measured, 3 examples: holds.** Each query is a paraphrase rather than a copy,
so it genuinely reaches Tier 2/3 — a byte-identical query would prove only that
the hash lookup is namespace-scoped, which Phase 10.1 already proved.

`tenant-scoped-resource-names` additionally asserts the *second* layer, through
`guard_direct`: handed the other tenant's record directly, the guard refuses it
as a `guard_error`, because a cross-namespace candidate reaching Tier 3 means
retrieval is broken and a defect is still a reject. Risk register row 3 says a
future edit moving the filter is the way this breaks, and one layer cannot
prove the other.

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

**Measured, 3 examples: holds.** The harness resolves against v1, keeps the
decision record, publishes v2, and then re-reads *both* the logged record and
the registry row behind it. A deprecated version stops being a match target
while still being readable, and an alias survives a version bump by moving with
the current-version pointer, which is the answer to the question this section
asked.

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

One JSON file per example, chosen in Phase 10.4 against its own harness. The
loader is `gateway/tests/corpus_loader.py`, whose docstring is the normative
description; the shape is:

```json
{
  "id":       "negation-never-vs-always",
  "category": "adversarial_negative",
  "why":      "what this example proves, and why cosine cannot see it",
  "records":  [ {"context_id": "...", "version": 1, "namespace": "acme",
                 "block_type": "org_policy", "canonical_text": ["line", "line"],
                 "aliases": [], "deprecated": false} ],
  "query":    {"namespace": "acme", "block_type": "org_policy", "text": "..."},
  "expect":   {"outcome": "rejected", "rejection_reason": "negation_mismatch",
               "context_id": "...", "version": 1, "never_retrieves": []},
  "guard_direct": {},
  "then":     {}
}
```

Every field Phase 10.0 asked for is here, `RejectionReason` included and
compared rather than printed. `canonical_text` and `text` may be a list of
lines, joined with newlines, so a multi-line block stays readable instead of
becoming one escaped line.

Three things the format deliberately does **not** carry:

- **No similarity numbers.** Similarity is a property of the encoder, not of a
  text pair. Pinning one here would turn a model upgrade — which design doc §16
  already treats as invalidating every stored vector — into a corpus-wide diff
  of numbers nobody could review. The harness measures and prints them; the
  only threshold anywhere is τ, and it lives in `guardrail.py`.
- **No fixture references.** Each example states its own records in full. A
  shared fixture would make one example's edit silently change another's
  meaning, which is the failure mode a regression corpus exists to avoid.
- **No expected candidate ordering.** `never_retrieves` states what must *not*
  appear; what does appear, and in what order, is Tier 2's business.

Two optional blocks handle the cases the ordinary shape cannot express:

- **`guard_direct`** — a failure Tier 2 partitions away entirely (a wrong block
  type, another tenant's namespace) can never be produced by retrieval. The
  example still has to prove the guard would refuse it, so this block tells the
  harness to hand `Guardrail.check` a candidate it could not have retrieved.
  Both halves are asserted: that retrieval does not surface it, *and* that the
  guard refuses it. Only the second survives someone deleting the partition.
- **`then`** — a second act, for `version_update/`: publish these records, then
  re-resolve and expect this. `expect_previous_decision_unchanged` asserts the
  decision already logged, and the registry row behind it, are untouched.
