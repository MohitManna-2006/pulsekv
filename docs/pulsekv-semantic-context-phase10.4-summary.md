# PulseKV v3 / Phase 10.4 — Equivalence validation, guardrails, evaluation corpus

**Status:** complete. `Matcher.resolve(block, namespace) -> MatchResult` is now
complete across all four tiers, and this project has produced its first real
`MATCHED(method=semantic)` outcomes. There is still no gateway process around
it — that is Phase 10.5.

**Scope actually touched:** `gateway/` and `docs/` only. `git diff --stat --
src include tests node control proto adapters` is **empty**, and so is
`git status --porcelain` over the same paths.

**Headline numbers, all measured:**

| | |
|---|---|
| τ | **0.90**, derived in §4 from the adversarial suite, not from the positive one |
| Adversarial-negative false positives | **0 of 25**, in two independent runs (§5) |
| Positive-paraphrase match rate | **9 of 13 (69%)**, reported per-example in §6 |
| Corpus | **44 examples**, 38 of them text pairs, checked in |
| Similarity spans | positives **0.8462–1.0000**, adversarials **0.1333–1.0000** — 17 of 24 adversarial pairs outrank the lowest positive (§4) |
| Guard latency | **0.090 ms** short, **2.066 ms** on a 512-token block (§11) |
| Tests | **415 passing** with the model (336 passing / 79 skipped without it) |

---

## 1. The §3 gate — closed, and still closed

Phase 10.3 was the first phase of Phase 10 to run with the soak gate genuinely
closed rather than waived. Re-checked at the start of this phase rather than
carried over: `deploy/run/soak-report.json` is unchanged on disk (written
2026-08-19 11:34) and still reads

```json
"verdict": { "result": "healthy", "problems": [], "error_rate": 0.0307,
             "dead_intervals": 0, "longest_dead_interval_run": 0,
             "intervals_evaluated": 90 }
```

5400.05 s, 17,691,636 operations, 13,062,930 verified, 74 crash cycles all
recovered, **0 value mismatches**, **0 dead intervals of 90**. Nothing has
regressed and no commit since has touched `deploy/`. **Phase 10.4 needed no
waiver and did not request one.**

The gap Phase 10.3 recorded is still open and still not this phase's:
`docs/pulsekv-v2-soak-collapse-analysis.md` §10 remains the unfilled
`<!-- FRESH_SOAK_RESULTS -->` placeholder, and `pulsekv-v2-progress-report.md`
§4.2 still carries the old Phase 9.4 figures. Writing into a v2 document is
Phase 9.x work; repeated here so it stays visible.

---

## 2. The three checks, as built

`Guardrail.check(block, candidate, *, namespace=None) -> GuardResult`. The
`namespace` keyword is additive to the signature Phase 10.0 froze; a caller
that omits it gets exactly Phase 10.0's contract.

### Order, and why type goes first

```
block_type equality      one enum comparison        -> TYPE_MISMATCH
namespace  equality      one string comparison      -> GUARD_ERROR   (opt-in)
    --- one tokenization pass over each full text ---
polarity   multiset      family counts              -> NEGATION_MISMATCH
entities   set           case-sensitive literals    -> ENTITY_MISMATCH
                                                    -> PASSED
```

The prompt asks for the cheapest and most decisive check first. Type is both:
one enum comparison, and the most decisive thing knowable about a candidate.
The two text checks share **one** tokenization pass, so their order relative to
each other is not a cost decision at all — it decides which reason gets
recorded, and polarity goes first because design doc §12 ranks it first and
because it is the failure the embedding is most blind to.

### 2.1 Negation/polarity — §12's check, plus a documented extension

Design doc §12 names `not`, `never`, `without`, `except`, `excluding`,
`unless` "and their common contractions". Implemented, with four decisions
worth stating:

**Contractions are handled by rule, not by list.** Any token ending in `n't`
(or `n’t`) is the negation family, so `can't`, `mustn't` and the next one
someone writes are all covered. `adversarial_negative/negation-contraction-cant-vs-can`
is the pair that an implementation enumerating six literal tokens would pass.

**Terms are compared as families, not surface forms.** `allowed` and
`permitted` are one family; `allowed` and `denied` are two. Without this,
`positive_paraphrase/permit-synonym` — a real paraphrase measuring 0.9960 —
would be refused for a synonym, while `adversarial_negative/polarity-allow-vs-deny`
would still need catching. Inflections and synonyms inside a family are free;
crossing a boundary is a reject.

**Counts are compared, not presence.** §12 phrases the rule as a marker
"present in one text and absent in the other". `adversarial_negative/negation-scope-added-clause`
is the pair that phrasing admits: both texts contain `Never`, and only the
count differs, because a second prohibition was added to one clause of two. A
presence-set comparison passes it. Multiset comparison is what refuses it. The
cost is a paraphrase that merges two prohibitions into one sentence, which is a
missed match — design doc §4's tiebreaker settles which of those to prefer.

**The vocabulary beyond §12's six, each entry tied to a named failure mode.**
Plan §8's adversarial list names failure modes §12's six words do not cover, so
the term set is grouped into families and extended to reach them:

| Family | Reaches | Corpus evidence |
|---|---|---|
| `NEG` | negation, incl. the no-family quantifiers | `negation-*`, 0.9055–0.9885 |
| `EXCEPT` | §12's exception markers | `exception-unless-added` 0.9210, `exception-without-removed` 0.9713 |
| `ALWAYS` | the counterpart to `never` | `negation-never-vs-always` 0.9885 |
| `BEFORE` / `AFTER` | plan §8's "before/after" | `order-before-vs-after` 0.9861 |
| `ABOVE` / `BELOW` | plan §8's threshold inversion | `threshold-above-vs-below` 0.9972 |
| `PERMIT` / `FORBID` | antonym swap with no negation marker | `polarity-allow-vs-deny` 0.7942 |
| `REQUIRE` / `OPTIONAL` | an obligation that stops being mandatory | `polarity-required-vs-optional` 0.9574 |

`not` and `never` share one family. Splitting them would refuse "Do not delete
production" against "Never delete production", among the commonest paraphrases
in policy text, and no adversarial pair in the corpus is admitted by merging
them — the *count* is what catches a negation added to one clause, and the
count survives the merge.

**Two exclusions, both deliberate.** `must`, `should`, `may` and `shall` are
not polarity terms: they differ in strength rather than direction, they are the
single most common word class in policy prose, and §12 does not name them. So
"must not" against "may not" passes this check — a named limitation (§8), not
an oversight. `most`, `least`, `minimum` and `maximum` are absent from the
comparison families because their direction depends on the idiom around them
("at most" is an upper bound, "the most" is not), and a direction-ambiguous
term inside a direction-sensitive family makes the verdict depend on phrasing.

**One family was narrowed by the corpus, after it was written.** `BEFORE` and
`AFTER` originally included the adverbs `afterwards`, `beforehand`,
`previously`, `earlier`, `subsequently`. `positive_paraphrase/retention-90`
("then archive" against "and afterwards archived") was refused for carrying
one. The families now hold only the forms that *relate two clauses*, and the
catch is unaffected: an inversion written with an adverb still moves the
preposition's count, because the clause it used to bind is gone.

### 2.2 Entity/value preservation — and one deliberate departure from §12

Five classes, checked in order so a token lands in exactly one:

| Class | Rule | Catches |
|---|---|---|
| `flag:` | `--?[A-Za-z][A-Za-z0-9._-]*` | `--dry-run` vs `--force` |
| `num:` | contains an ASCII digit | `90` vs `30`, `250 ms`, `us-east-1`, `Sev-1` |
| `id:` | internal `_ / \ @ = ::` or a dotted alphanumeric | `AWS_PROFILE=Production`, `prod.internal` |
| `val:` | in a closed lexicon of environments, cadences, booleans/nulls | `staging` vs `production`, `quarterly` vs `monthly` |
| `name:` | an uppercase letter sentence case cannot explain | `GitHub`, `IAM`, `Production` mid-sentence |

**Equality, not superset-or-equal.** §12 states the rule as "the candidate's
extracted set must be a superset-or-equal of what's semantically load-bearing
in the incoming block". Read literally that admits a false positive, and the
corpus contains it: `adversarial_negative/entity-added-production` registers
"Delete unused resources in staging **and production**" against an incoming
"Delete unused resources in staging". The block's entities are a strict subset
of the candidate's, so superset-or-equal is satisfied — and substituting would
extend a deletion instruction to production. Equality refuses it. Equality is
strictly more reject-biased than the rule it replaces, which §12's own
failure-bias paragraph authorises ("any signal of doubt ... routes to use the
original block"). `TestEntityCheck.test_the_check_compares_for_equality_not_superset`
asserts the subset relation *and* the rejection, so the departure cannot be
undone by accident.

**A set, not a multiset** — the opposite of the polarity rule, and for a reason
that does not transfer: saying `production` twice is emphasis, not a second
production, whereas a second `never` is a second prohibition.

**Why environments and cadences need a lexicon at all.** A deployment tier
written in lowercase prose is not a proper noun, has no digit and is not an
identifier, so nothing else would extract it — and §12 names environment names
in so many words. Cadences are in the same position and had to be measured to
justify: `value-cadence-quarterly-vs-monthly` scores **0.8911**, *above* the
lowest-scoring genuine paraphrase in the corpus. τ cannot be raised far enough
to refuse it without refusing real matches, so the lexicon has to carry it.

### 2.3 Structural-type consistency — confirmed redundant, kept anyway

Step 10.4.3 asks for confirmation rather than assumption.
`TestTypeCheck.test_tier_two_really_does_make_this_redundant` builds two
near-identical records under two block types, shows the wrong-typed one is in
the index and scores 1.0 against the query, and shows retrieval returns only
the right-typed one — then hands the guard the candidate retrieval declined to
produce and shows it refused. Both halves, because they are two claims and only
the second survives someone deleting the partition.

The test also turned up a second layer nobody had written down: the registry's
live-content-hash uniqueness is scoped to `(namespace, content_hash)` and does
**not** include block type, because Tier 0's hash lookup does not either. One
namespace cannot hold the same text under two types at all. The check is one
layer further from reachable than it looked, and it stays: risk register row 3
says a future edit is how these properties break, and a check that costs one
enum comparison is not where to economise.

---

## 3. Where the guard sits in the pipeline, and why τ comes last

`Matcher.try_guard` is the new tier. Design doc §12 describes τ as a gate the
candidate clears *before* the guard sees it. **This implementation inverts that
order**, deliberately:

- Phase 10.3 measured a meaning-inverting edit at 0.9933 against a genuine
  paraphrase at 0.7989. This phase reproduced the inversion at corpus scale:
  genuine paraphrases span **0.8462–1.0000**, adversarial pairs span
  **0.1333–1.0000**, and **17 of the 24 scored adversarial pairs sit at or
  above the lowest-scoring genuine paraphrase**. A τ gate in front of the guard
  would therefore not filter adversarial pairs out. It would only decide which
  of them the guard never gets to name.
- The verdict is identical either way. Below-τ is a reject and a guard mismatch
  is a reject; an accept still requires both. Neither order can produce an
  accept the other refuses.
- What differs is the reason recorded. Running the guard first means a negation
  pair is logged `negation_mismatch` whatever it scored, instead of a
  `low_similarity` that hides why the candidate was dangerous. Risk register
  row 1's entire detection story is the reject metric's reason label
  (`pulsekv_semantic_reject_total{reason=...}`, design doc §18), so this is the
  difference between an audit trail that can be read and one that cannot.

The frozen contract is intact. `models.py` requires a `LOW_SIMILARITY`
rejection to carry no `guard_outcome`, and that stays true in substance: the
candidate was refused *by the τ gate*, and the guard's opinion of it changed
nothing. Asserted by
`TestTierThreeWiring.test_below_tau_is_a_rejection_at_the_gate_not_a_guard_verdict`.

**The Phase 10.3 §5 observability gap closed by construction, as predicted.**
A retrieved candidate now becomes `MATCHED` or `REJECTED`, both of which carry
`similarity` legally.
`TestDecisionLogUnderTier2.test_the_phase_103_observability_gap_is_closed` is
the same block that logged a bare `no_candidate` in 10.3, now logging
`semantic` with its similarity and `guard_outcome=PASSED`.

**Top-1 only, and the corpus is why.** Design doc §11 sets the MVP at "just the
top-1, escalate only if Phase 10.4's evaluation corpus shows a real need for
more". It does not: at `guard_top_n` 1, 3 and 5 the positive match rate is
identical (10 of 13 on the whole-corpus run) and adversarial false positives
stay at zero. `DEFAULT_GUARD_TOP_N = 1` ships unchanged, the parameter exists
so a future corpus can move it, and
`TestWholeCorpus.test_going_deeper_than_the_top_candidate_changes_nothing_here`
is what would notice.

---

## 4. τ = 0.90, and how it was derived

### The method

τ was tuned against the adversarial-negative suite specifically, per design doc
§12 and plan §8:

1. Score every adversarial pair with the real encoder.
2. Classify each by whether a deterministic check refuses it **without any
   similarity signal**. 19 of 25 are refused that way. The remaining six are
   the `tau-*` examples: lowercase job titles, team names, verbs, degrees of
   detail — differences no extractor in this guard recognises.
3. τ must exceed that second class's ceiling, because those are the only pairs
   for which the threshold is load-bearing at all. Measured ceiling:
   **0.8187** (`tau-role-swap-approver`, an approver changing from "release
   manager" to "on-call engineer").
4. Round up rather than sit on 0.8187 + ε.

### The sweep, reported in full

| τ | positives matched | adversarial false positives |
|---|---|---|
| 0.80 | 12/13 | **1** |
| 0.82 | 12/13 | 0 |
| 0.83 | 12/13 | 0 |
| 0.85 | 11/13 | 0 |
| 0.88 | 11/13 | 0 |
| **0.90** | **9/13** | **0** |
| 0.92 | 8/13 | 0 |
| 0.95 | 7/13 | 0 |
| 0.99 | 3/13 | 0 |

**The counterfactual, stated rather than omitted:** τ = 0.83 also produces zero
false positives on this corpus and would raise the positive rate to 12 of 13.
It was not chosen. The guard-blind class has six members and 0.8187 is a floor
on where an unmeasured real-world example lands, not a ceiling — a threshold
fitted 0.011 above the highest observed member of a six-example class is fitted
to that example, not to the class. Plan §8 is explicit that a low match rate
with zero false positives "is explicitly preferred over lowering τ to force a
higher match rate", and design doc §4's tiebreaker points the same way. The
table is here so a reader with different evidence can disagree with the
specific number without having to re-derive the method.

`TestCorpusEndToEnd.test_tau_is_earned_by_the_adversarial_suite_not_chosen_before_it`
asserts both halves: zero false positives at the shipped τ, and *at least one*
at 0.80. The second assertion is the one that matters — a τ with no false
positive anywhere below it would mean the deterministic checks refuse
everything and τ is not earning anything.

### What τ is not

**It is not what separates a paraphrase from its negation, and cannot be.** The
ranges overlap almost entirely — 17 of the 24 scored adversarial pairs outrank
the lowest genuine paraphrase — and at the very top they coincide exactly.
Sorted, from the corpus:

```
1.0000  adversarial  entity-case-only-swap           byte-identical vectors
1.0000  adversarial  truncation-boundary-negation    byte-identical vectors
1.0000  POSITIVE     long-runbook-paraphrase...      byte-identical vectors
0.9972  adversarial  threshold-above-vs-below
0.9960  POSITIVE     permit-synonym
0.9957  POSITIVE     contraction-dont
0.9885  adversarial  negation-never-vs-always
...
0.9119  POSITIVE     careful-agent
0.8905  adversarial  entity-staging-vs-production
0.8653  POSITIVE     deletion-approval
0.8462  POSITIVE     citation-discipline             <- lowest positive
0.8187  adversarial  tau-role-swap-approver          <- highest guard-blind; τ starts here
0.7942  adversarial  polarity-allow-vs-deny
```

**Three pairs score exactly 1.0000 with byte-identical vectors, and one of them
is a genuine paraphrase.** At the top of the similarity scale this corpus holds
a meaning and its opposite at the same number, for the same reason — the
encoder read neither difference, one because its tokenizer is uncased and two
because they fall past token 512. Nothing a threshold can do reaches them.

τ's actual job is narrower and real: refusing a candidate that is merely the
nearest thing in a sparse namespace. The three deterministic checks do
everything else. Where this summary or `guardrail.py` names τ, it names it as
that and not as a safety property.

### Where τ lives

`guardrail.SIMILARITY_THRESHOLD`, with its derivation in the comment above it;
`Matcher(..., similarity_threshold=...)` for a caller that wants another. It is
**not** a `GatewayConfig` field: `config.py` is Phase 10.5's, and the number
belongs next to the checks whose coverage determines it. Phase 10.5 should
surface it as a config field defaulting to this constant.

---

## 5. Zero false positives — proven twice, two different ways

Exit criterion 2 is asserted in two independent runs, because they can fail
differently.

**Per-example, isolated.** Each example gets its own registry and index, so the
top candidate is always the record the example is about, and the specific
`RejectionReason` in its file is compared rather than printed — the corpus
README's requirement that "a test that passes for the wrong reason fails".
Result: **0 of 25 matched**, every rejection for the stated reason.

**Whole-corpus.** All 43 distinct records from all 44 examples in **one**
registry across three namespaces, 42 of them indexed (the 43rd is a deprecated
version, correctly excluded). A query's nearest neighbour can now belong to a
different example entirely. Result: **0 of 25 matched**. This is the run that
would surface a false positive caused by registry *density*, which no isolated
test can, and it found two real defects while being built (§7).

**And most of it needs no model at all.** The guard is deterministic string
comparison, so `TestCorpusGuardChecks` runs every adversarial example through
`Guardrail.check` directly with no encoder: 19 of 25 are refused there, for the
right reason, on any machine. Only the classes genuinely about similarity skip
when the weights are absent.

**Full adversarial results**, τ = 0.90, similarity of the top candidate:

```
1.0000  entity_mismatch     entity-case-only-swap          1.0000  entity_mismatch    truncation-boundary-entity
1.0000  negation_mismatch   truncation-boundary-negation   0.9972  negation_mismatch  threshold-above-vs-below
0.9885  negation_mismatch   negation-never-vs-always       0.9861  negation_mismatch  order-before-vs-after
0.9810  negation_mismatch   negation-scope-added-clause    0.9713  negation_mismatch  exception-without-removed
0.9699  entity_mismatch     entity-number-90-vs-30         0.9630  entity_mismatch    entity-added-production
0.9574  negation_mismatch   polarity-required-vs-optional  0.9463  entity_mismatch    entity-flag-dryrun-vs-force
0.9212  negation_mismatch   negation-must-not-dropped      0.9210  negation_mismatch  exception-unless-added
0.9055  negation_mismatch   negation-contraction-cant-can  0.8911  entity_mismatch    value-cadence-quarterly-monthly
0.8905  entity_mismatch     entity-staging-vs-production   0.7942  negation_mismatch  polarity-allow-vs-deny
0.8187  low_similarity      tau-role-swap-approver         0.7858  low_similarity     tau-detail-swap
0.7423  low_similarity      tau-service-name-swap          0.7350  low_similarity     tau-team-swap
0.6879  low_similarity      tau-verb-swap                  0.1333  low_similarity     tau-unrelated-same-type
   n/a  type_mismatch       type-mismatch-schema-vs-policy (guard_direct; unreachable by retrieval)
```

**The two adversarial cases at 1.0000 are the point of the whole tier.** Both
are pairs the encoder cannot distinguish *at all* — not "scores them close",
but produces **byte-identical vectors**, and a genuine paraphrase
(`positive_paraphrase/long-runbook-paraphrase-past-truncation`) sits at the
same 1.0000 for the same reason:

- `entity-case-only-swap` — `AWS_PROFILE=Production` against
  `AWS_PROFILE=production`. The model's tokenizer is uncased.
- `truncation-boundary-negation` / `-entity` — two 512-token runbooks whose
  final paragraph inverts an escalation rule or doubles a threshold. The model
  stopped reading before it.

---

## 6. Positive-paraphrase match rate: 9 of 13 (69%), per example

```
0.9960  matched     permit-synonym                          synonyms inside one polarity family
0.9957  matched     contraction-dont                        "Do not" / "Don't"
0.9892  matched     github-force-push                       clause order flipped
0.9804  matched     pagerduty-escalation                    time-box moved to the front
0.9647  matched     sentence-reorder                        two sentences swapped
0.9589  matched     two-teams-one-policy                    two teams, one internal rule
0.9218  matched     runbook-restart-procedure               RAG_DOCUMENT restated
0.9119  matched     careful-agent                           "careful" / "cautious"
1.0000  matched     long-runbook-paraphrase-past-truncation paraphrase past the 512-token boundary
0.8874  MISSED (τ)  bullet-marker-churn                     "-" bullets vs "*" bullets
0.8847  MISSED (τ)  retention-90                            passive voice
0.8653  MISSED (g)  deletion-approval                       "require" (REQUIRE family) vs "obtain"
0.8462  MISSED (τ)  citation-discipline                     clauses swapped, noun phrase reordered
```

Reported, not tuned. Three of the four misses are τ's doing and sit in
0.8462–0.8874, immediately below the threshold — §4's table is what they would
cost to buy back.

**The fourth miss is the guard's, and it is the most instructive line here.**
`deletion-approval` pairs "All deletions **require** written approval" with
"**obtain** written approval". `require` is in the `REQUIRE` polarity family
and `obtain` is in none, so the multisets differ. The *same family* is what
refuses `polarity-required-vs-optional` at 0.9574 — a pair the entity check
cannot see, since both sides say "production". Dropping `REQUIRE` buys this
match and costs that refusal. Design doc §4's tiebreaker settles which way that
trade goes, and the corpus file records the trade so the next person can
re-examine it with new evidence rather than rediscover it.

**One number that is higher than it looks.** On the whole-corpus run,
`retention-90` *does* match — not its own record, but `audit-retention-policy`,
a different registered context stating the same retention rule in wording
closer to the incoming block. Semantically that is a correct substitution and
the guard passed it on its merits; against the corpus's declared expectation it
is a match to the wrong record, so it is **not** counted in the 69%. It is
worth naming because it says something real about how this feature behaves in
production: a denser registry raises the match rate, and a block will resolve
to whichever equivalent canonical context is nearest, not to the one an author
had in mind. The registry already refuses two byte-identical live texts in one
namespace; it cannot refuse two that merely mean the same thing.

---

## 7. Two defects the corpus found, both real

**1. The corpus harness never moved the current-version pointer.**
`corpus_loader.populate` decided `register` vs `publish_version` from
call-local memory of which contexts it had seen. A `version_update` example
publishes its later versions in a *second* `populate` — after a decision has
been logged against the first — so that call took the `register` path, which
only moves the pointer for a context it is creating. The pointer stayed on v1,
the index (built from current versions) never saw v2, and both
`version_update` examples were quietly asserting a state that had not changed.
Fixed by asking the registry whether the context exists.
`alias-follows-the-current-version` is the example that exposed it: the alias
kept resolving to v1 after the bump.

**2. Two corpus contexts held byte-identical text in one namespace.** The
whole-corpus run refused to build:

```
acme/audit-retention-policy v1: content_hash 6bf0139... is already live on
audit-log-retention v1; Tier 0's exact-hash lookup must resolve to one record
```

The registry is right and the corpus was wrong. No isolated test could have
found it — each example registers only its own records — which is the argument
for the whole-corpus run existing at all.

---

## 8. Known limitations, named rather than left to be discovered

1. **Modality is not polarity.** `must not` against `may not` passes the
   polarity check: both carry one `NEG` and neither `must` nor `may` is a
   family member. Excluding them is deliberate (§2.1) and it leaves a real gap.
2. **A candidate that adds a clause carrying no polarity term and no
   extractable value is accepted.** `version_update/old-decision-stays-interpretable`
   records the instance: v2 adds "Archived logs are held for a further two
   years", and neither text check has anything to compare, because `two` is
   spelled out and `years` is not a value word. Design doc §12's own
   superset-or-equal rule permits this explicitly and the stricter equality
   rule does not close it either — there is no entity on either side of the
   difference. Spelling numerals into the value lexicon would catch this
   instance and was rejected: it would refuse "90 days" against "ninety days",
   one value written two ways. **A content-volume check is the concrete
   follow-up**, and the audit trail keeps the case traceable meanwhile (§21).
3. **Lowercase hyphenated resource names are not entities.**
   `payments-api` against `ledger-api` is not extracted, because adding `-` to
   the identifier signal would make every hyphenated English word an entity and
   refuse `read-only` against `read only`. Measured at 0.7423, so τ refuses it
   — recorded as `tau-service-name-swap` so the trade is a measured one rather
   than an unnoticed hole.
4. **The guard-blind class is exactly six examples.** Everything §4 concludes
   about τ rests on them. That is a small number and the honest reason the
   margin is 0.08 rather than 0.01.
5. **`guard_error` and `guard_timeout` cannot be corpus examples.** They are
   failures of the guard process, not properties of a text pair.
   `TestGuardIsRejectBiased` covers both;
   `test_the_corpus_covers_every_rejection_reason_the_guard_can_produce`
   asserts the corpus covers the other four so the split stays deliberate.

---

## 9. The case-folding decision — settled, with the evidence

Phase 10.2 declined to case-fold in `normalize_for_hash` and named where the
question should be settled: "Phase 10.4's adversarial-negative corpus is where
this should be settled with data — a case-only entity swap is precisely the
shape that set will contain." It does, and the answer is **unchanged: no case
folding** — for a reason Phase 10.2 could not have known.

**The measurement.** `adversarial_negative/entity-case-only-swap`:

```
Set AWS_PROFILE=Production before running the deploy script.
Set AWS_PROFILE=production before running the deploy script.

cosine = 1.0000     encode(a) == encode(b)  ->  True   (byte-identical vectors)
```

The model's tokenizer is uncased. This is not a high score; it is the *absence*
of a signal. It is the highest similarity anywhere in the corpus, positive
examples included, and **no value of τ can refuse it.**

**Why the guard's existence does not reverse Phase 10.2's decision.** The
argument for folding was that the guard now runs on anything reaching Tier 2/3
and would catch a case-only collapse. The entity check does catch it — the
`val:`/`id:` classes preserve case, so `Production` and `production` are
different entities and the pair is refused. But design doc §11 is explicit that
the guard **never runs on a Tier 0/1 hit**. If `normalize_for_hash` folded
case, these two blocks would hash *identically*, Tier 0 would substitute one
for the other, and the check that catches the pair at Tier 3 would never be
consulted. So the guard's existence is evidence *for* Phase 10.2's decision,
not against it: it demonstrates the pair is dangerous enough to need catching,
in a tier that has no guard behind it.
`TestCaseFolding.test_folding_in_the_hash_normalizer_would_collide_at_tier_zero`
asserts exactly that — the two texts hash differently today and identically
under a folding normalizer.

**Consequence: `normalizer.normalize_for_hash` is unchanged, and so is Tier 0's
match set.** Nothing in this phase moves what Tier 0 matches. Phase 10.3's
observation — that Tier 2 inherits the same normalized text, so the decision
now affects what is *embedded* as well as what is hashed — is answered the same
way, and by the measurement above it makes no difference to Tier 2 either: the
encoder is uncased whatever the normalizer does.

**One refinement the corpus did force, inside the guard.** Case is preserved on
entities *except* at a sentence boundary, where orthography forces a capital
and the case therefore carries no information. Without this, "Production is off
limits" and "production is off limits" would be different values, and ordinary
paraphrases would be refused for a difference no writer chose. The classes
above `val:` never reach the rule: a digit, a leading dash or an embedded
`=`/`/`/`_` is not something sentence case can explain, so
`AWS_PROFILE=Production` keeps its capital wherever it sits — which is why the
adversarial example above is still refused. It is the same rule `_is_proper`
already applied to proper nouns, for the same reason.

---

## 10. Corpus composition

44 examples, checked in under `gateway/tests/corpus/`, one JSON file each. The
format is documented in that directory's README and in `corpus_loader.py`; it
carries everything Phase 10.0 asked for and deliberately carries **no
similarity numbers**, because similarity is a property of the encoder and
pinning one would turn a model upgrade into a corpus-wide diff of numbers
nobody could review.

| Category | Count | What it holds |
|---|---|---|
| `positive_paraphrase/` | 13 | Real registered-context paraphrases across five block types, including one 512-token pair whose difference is entirely past the truncation boundary |
| `adversarial_negative/` | 25 | 10 polarity, 6 entity/value, 2 truncation-boundary, 1 structural type, 6 `tau-*` |
| `cross_tenant/` | 3 | Identical text in two tenants; tenant-scoped resource names; a third tenant that registered nothing |
| `version_update/` | 3 | A decision surviving a publish; a deprecated version; an alias across a bump |

Every query is a paraphrase, never a copy: the corpus README excludes examples
that only exercise Tier 0, and a byte-identical query would prove only that the
hash lookup is namespace-scoped — which Phase 10.1 already proved one layer
lower.

---

## 11. Latency

Design doc §18's `pulsekv_semantic_lookup_latency_seconds{tier="guard"}`,
measured the way Phase 10.3 measured Tier 2. Apple Silicon, CPU, 200
iterations after 5 warm-ups:

| Block | Guard | For scale: Tier 2 on the same block (Phase 10.3) |
|---|---|---|
| Short (a two-line policy) | **0.090 ms** | 5.5 ms |
| 512-token runbook | **2.066 ms** | ~88 ms |

**Tier 3 costs 1.6% of Tier 2 on a short block and 2.3% on a long one.** The
expensive part of semantic matching remains the encode, by roughly two orders
of magnitude, and adding the guard does not change Phase 10.3's conclusion that
Tier 0/1's short-circuit is worth more than it appeared. `PLACEHOLDER_GUARD_TIMEOUT_MS`
in `config.py` is 50 ms, which these numbers say is loose rather than tight —
Phase 10.5 owns that constant and now has a measurement to set it from.

The guard's budget is enforced the way `Encoder.encode`'s is (a worker thread
and a bounded wait) and defaults to **off**, because the checks are linear
scans with no nested quantifiers and the budget is a backstop rather than the
normal control flow. `TestGuardIsRejectBiased.test_a_guard_past_its_budget_becomes_a_timeout_reject`
proves the caller is released early rather than merely told afterwards, the
same assertion Phase 10.3 makes about the encoder.

---

## 12. Tests

**415 passing** with the model; **336 passing, 79 skipped** without it.

```
test_models.py                92   (was 94; the two guardrail stub cases retired)
test_registry.py              58   unchanged
test_deterministic_tiers.py   63   unchanged
test_semantic_retrieval.py    50   (was 48; see below)
test_guardrail.py            152   new
```

New suite by what it proves:

```
TestCorpusEndToEnd 54 · TestCorpusGuardChecks 39 · TestNegationCheck 10
TestEntityCheck 9 · TestGuardIsRejectBiased 7 · TestCorpusCrossTenant 7
TestTierThreeWiring 7 · TestCorpusVersionUpdate 5 · TestCaseFolding 4
TestTypeCheck 4 · TestWholeCorpus 3 · TestGuardReadsFullText 2
TestGuardLatency 1
```

**Four Phase 10.3 tests changed, because they asserted Phase 10.4 had not
happened** — the same boundary-moving each phase has done:

| Test | Then | Now |
|---|---|---|
| `TestStubsAreStubs` | `guardrail` in `STUB_MODULES` | removed, as `encoder`/`index` were in 10.3 |
| `TestPhaseBoundary.test_the_guard_is_still_phase_104` | guard raises `NotImplementedError` | replaced by `test_the_gateway_process_is_still_phase_105`, which checks all three remaining stubs rather than one |
| `TestRetrievalIsNotDecision` | a 1.0 candidate resolves to `NO_CANDIDATE` | split: Tier 2 alone still reaches no verdict (`Candidate` has no field for one), and the guard is what turns it into a match |
| `TestDecisionLogUnderTier2` | a retrieved candidate logs a bare `no_candidate` | asserts 10.3 §5's gap closed, plus a new test that a retrieval finding *nothing* still logs the bare form |

The two-vector `_ConstantEncoder` used by the Tier 3 wiring tests lives in the
test file, not the package, for the same reason Phase 10.3's `StubEncoder`
does: a non-semantic encoder must not be configurable into a deployment by
accident.

**Reproducing:**

```bash
python3 -m venv /tmp/gwvenv
/tmp/gwvenv/bin/pip install pydantic==2.13.4 numpy==2.5.2 onnxruntime==1.29.0 \
    tokenizers==0.23.1 pytest==9.1.1
# weights: see gateway/README.md
PULSEKV_GATEWAY_MODEL_DIR=... PYTHONPATH=gateway \
    /tmp/gwvenv/bin/python -m pytest gateway/tests -q      # 415 passed
```

---

## 13. Exit criteria — verified

| # | Criterion | Evidence |
|---|---|---|
| 1 | Three guard checks, tested, deterministic, reject-biased | §2. `TestNegationCheck`, `TestEntityCheck`, `TestTypeCheck` each pair a correct reject with the adjacent case that must still pass, so a check that refuses everything fails. `TestGuardIsRejectBiased`: error, timeout, and a non-conforming substituted guard all reject; `check` never raises |
| 2 | Zero false positives on the adversarial corpus at the chosen τ | **0 of 25**, proven in two independent runs (§5) — per-example isolated, and every record in one registry. 19 of the 25 are proven without an encoder at all |
| 3 | Positive match rate reported honestly | **9/13 = 69%**, per example, with each miss attributed to τ or to the guard (§6) |
| 4 | Cross-tenant and version-update suites pass | §10. Cross-tenant additionally proves the second layer via `guard_direct`; version-update re-reads the logged decision *and* the registry row behind it after a publish |
| 5 | Case folding explicitly resolved with corpus evidence | §9. Unchanged: no folding. Evidence is a measured cosine of exactly 1.0000 with byte-identical vectors, plus a test showing a folding normalizer collides the pair at Tier 0, where no guard runs |
| 6 | First real `MATCHED(semantic)`, and rejects fall through unchanged | `TestTierThreeWiring`: a paraphrase resolves to `MATCHED(method=semantic)` with the candidate's similarity as confidence; an adversarial block resolves to `REJECTED(negation_mismatch)` and the forwarded text is the original object, not a copy. Both decision-log projections asserted |
| 7 | `git diff --stat -- src include tests node control proto adapters` empty | **Empty**, and `git status --porcelain` over the same paths too |
| 8 | `gateway/tests/corpus/` populated across all four categories, checked in | 44 files, §10. The `.gitkeep` placeholders are gone |
| 9 | This summary, with cited results and τ's derivation | This document; §4 for τ |

---

## 14. What Phase 10.5 can now assume

**`Matcher.resolve(block, namespace) -> MatchResult` is complete across all
four tiers.** It never raises for an expected failure — a registry outage, an
encoder timeout, a guard error and a guard timeout each become a
non-substituting `MatchResult` — and `result.substitutes` is the single
question the assembler has to ask. There is no partial-credit state to handle.

What 10.5 builds around it:

- **`Assembler.assemble(blocks, substitutions)`.** Its contract is already
  written in the stub and already relied on by this phase's end-to-end test: a
  block with no entry is emitted byte-identical to its input. That covers
  misses, rejections, bypasses and component errors alike, because none of them
  substitute.
- **Config.** Two numbers this phase produced want config fields:
  `guardrail.SIMILARITY_THRESHOLD` (0.90) and a guard timeout — for which
  `PLACEHOLDER_GUARD_TIMEOUT_MS = 50` is now a measured-loose bound rather than
  a round number (§11). `Matcher` takes both as constructor arguments already.
- **Index freshness.** `VectorIndex.build_from_registry` reads current
  versions, so a publish requires a rebuild for the new version to become a
  match target. The version-update tests rebuild explicitly, and 10.5 owns
  deciding when a live process does.
- **Metrics.** Every label in design doc §18's
  `pulsekv_semantic_reject_total{reason=...}` is now producible, and each has a
  corpus example except `guard_error`/`guard_timeout`, which have unit tests
  (§8, limitation 5). `pulsekv_semantic_lookup_latency_seconds{tier="guard"}`
  has its first real numbers.

**And one thing 10.5 should not assume:** that a high similarity means
anything on its own. The single strongest signal this phase produced is that
three corpus pairs score exactly 1.0000 with byte-identical vectors, and two of
them are adversarial while the third is a real paraphrase — the same number,
opposite meanings, no way to tell from the score. Any future code that reads
`MatchResult.confidence` and treats it as a safety property is reading it
wrong; it is a retrieval score attached to a decision the guard already made.

**Deliverable:** `docs/pulsekv-semantic-context-phase10.5-summary.md`.
