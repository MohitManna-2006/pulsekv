# PulseKV v3 / Phase 10.2 — Deterministic tiers and the decision log

**Status:** complete. Tier 0 (exact hash and registered aliases) and Tier 1
(structural re-serialization) resolve blocks against Phase 10.1's registry with
no embedding, no model and no new dependency; every decision the gateway makes
about every block is recorded in an audit trail that structurally cannot hold
prompt text.

**Scope actually touched:** `gateway/` only. `git diff --stat -- src include
tests node control proto adapters` is **empty**, and so is `git status
--porcelain -- gateway/tests/corpus`.

---

## 0. Two path discrepancies in the phase prompt

Recorded rather than silently resolved, per this project's convention.

| The prompt says | What exists |
|---|---|
| `gateway/registry_store.py` | `gateway/pulsekv_gateway/registry.py`. There is no `registry_store.py`; Phase 10.0 created `registry.py` and Phase 10.1 implemented it under that name |
| `gateway/models.py` | `gateway/pulsekv_gateway/models.py` |

The prompt's own instruction — treat the Phase 10.1 summary as ground truth
where they disagree — resolves both. Method names (`register`,
`publish_version`, `list_records`, `from_dsn`, `content_hash_for`,
`find_candidates`) were quoted accurately; only the paths differ.

---

## 1. The §3 gate — still the same waiver

The 90-minute confirmation soak relaunched at 03:14:49 UTC is **still running**,
so **Phase 10.2 runs under the same waiver Phase 10.1 ran under**, granted by
the repository owner (Mohit Manna) for the same reason: this phase produces
pure-Python matching logic with no dependency on cluster runtime behavior.

State at 15:19 UTC, checked live rather than carried over:

| | |
|---|---|
| Load generator | `4500s` of a declared `sustained 1h30m0s` — **83%**, up from 3180s (59%) when Phase 10.1 checked |
| Pace | Real-time again. Phase 10.1 recorded an 11h20m/1800s divergence and read it as host suspension; the run has advanced 1320s in the 22 minutes since, which is consistent with that reading |
| Chaos | 62 crash cycles, last log line `[2026-08-19T15:19:07Z]` |
| Report / verdict | **Still none.** The only `soak-report.json` on disk remains the preserved pre-fix incident artifact |
| `pulsekv-v2-soak-collapse-analysis.md` §10 | Still the unfilled `<!-- FRESH_SOAK_RESULTS -->` placeholder |

In-flight signals, still not a verdict:

- **0 dead windows across 75 reporting intervals.** `soak-verdict.py`'s
  `dead_windows()` — operations attempted, zero verified — has not appeared.
- **Exactly one injector**, against three in the incident run.
- **3.13% error rate** (451,631 errors over ~14.5M attempted operations)
  against the script's 50% ceiling.

At the current pace the remaining 900s finishes within the hour, at which point
`soak-verdict.py` produces the first real verdict and the waiver can be
superseded by a result rather than by another waiver.

---

## 2. Step 10.2.1 — what the design doc actually specifies

The prompt's sketch and the design doc describe different shapes, and the
difference is load-bearing enough to state before anything else.

**The prompt's sketch:** Tier 0 is "byte-for-byte identical input"; Tier 1 is
"normalized match" applying whitespace/case/punctuation/unicode normalization
*after* Tier 0 fails.

**What design doc §11 says:** Tier 0 is *already* the normalized-text hash —
"SHA-256(normalized_block_text) against `content_hash` in the registry (and
against registered aliases) ... normalize whitespace/casing deterministically
before hashing, never normalize meaning." Tier 1 is something else entirely:
*structural* normalization, "only for block types with real structure ... a
tool-schema JSON block is parsed and re-serialized in a canonical key
order/whitespace before Tier 0's hash is computed."

So there is no "byte-for-byte" tier and no separate "normalized" tier. There is
one lookup mechanism with two front-ends:

```
block text ──[normalize_for_hash]──────────────────────────► hash ─► registry   Tier 0
           └─[normalize_structural]──[normalize_for_hash]──► hash ─► registry   Tier 1
```

**The ordering question, resolved.** §11 calls Tier 1 "a pre-processing step
*before* Tier 0", while §13's table says `TOOL_SCHEMA` is matched by "Tier 1
(structural) then Tier 0" — both of which sound like Tier 1 runs first, against
the prompt's "tier 0 before tier 1". They reconcile: both sentences describe the
*hash pipeline* (structural normalization feeds Tier 0's hash), not the order of
attempts. What is attempted first is Tier 0, on three grounds:

1. Phase 10.0's `matcher.py` stub — written when the contract was frozen —
   states the order outright: "0. exact hash (and registered aliases) 1.
   structural normalization, then tier 0's hash, for structured types only".
2. §11's own framing is "cheapest and most deterministic first", and Tier 0 is
   a normalize plus a hash against Tier 1's parse, re-serialize, normalize and
   hash.
3. On a block that would hit both, they resolve to the *same record* — a schema
   already in canonical form hashes identically down either path — so running
   the cheaper one first costs nothing in match rate.

This is the reading implemented, and the prompt's ordering requirement is
satisfied by it.

### The normalization rules, and the two the doc permits that were not implemented

`normalize_for_hash` implements five rules. §11 sanctions "whitespace/casing";
the binding constraint is the clause that follows, "never normalize meaning".

| Rule | Why it is rendering, not meaning |
|---|---|
| **Unicode NFC** | Not in §11's list. Added because it is what makes byte-hashing well-defined: `é` as U+00E9 and as U+0065 U+0301 are *the same character* by Unicode's own definition of canonical equivalence. **NFKC is explicitly not used** — compatibility decomposition rewrites `ﬁ`→`fi`, `½`→`1⁄2` and superscripts to base digits, which is exactly the class of change §11 forbids. Tested both ways |
| **Line endings → LF** | CRLF, CR and LF are one line break; which appears is a property of the editor and OS |
| **Trailing whitespace stripped per line** | Invisible in every format |
| **Runs of blank lines collapsed to one** | Insignificant in prose, Markdown (2+ blank lines are one paragraph break), JSON, YAML and code |
| **Leading and trailing blank *lines* removed** | Note what this is not — see the bug in §6 below |

Only ASCII inline whitespace (space, tab, vertical tab, form feed) is stripped,
never Python's default `str.strip()` set, which includes U+00A0 NO-BREAK SPACE —
typographically not an ordinary space, and whose removal a reader could
reasonably call a change in meaning. Tested.

**Case folding is deliberately not implemented**, though §11 names casing.
Reasoning, because this is the one place this phase declines something the
design doc permits:

- Case is meaning-bearing in exactly the content this MVP targets. JSON keys in
  a `TOOL_SCHEMA` are case-sensitive; environment names, resource names and
  command flags are the entity class design doc §12's guard exists to protect.
- **That guard never runs on a Tier 0/1 hit** (§11 is explicit). So Tier 0 has
  no safety net behind it, and a normalization that can collapse two distinct
  entities is strictly more dangerous here than at Tier 2, where the guard
  does run.
- §4's "bias hard toward zero false positives over high match rate" is the
  tiebreaker whenever a normalization *could* collapse two meanings.
- Nothing is lost against the motivating workload: §3 names whitespace,
  ordering and wording drift as the variation to absorb — not case.

Phase 10.4 is where this should be settled with data rather than argument: a
case-only entity swap is precisely the shape its adversarial-negative corpus
will contain. Until then the conservative direction costs a miss, which is
today's status quo.

**Collapsing whitespace runs inside a line is also not implemented** — the one
whitespace operation that can change meaning, since indentation is syntax in
code and YAML and column alignment is structure in a table, and `RAG_DOCUMENT`
is an eligible type that can carry either. The phase prompt raises this exact
hazard; the implementation agrees with it. **Punctuation stripping** is neither
mentioned by §11 nor safe (it deletes negation and scope markers) and is not
implemented.

Every rule has a positive case and an adjacent negative case in
`TestNormalizationRules` — `a  b` ≠ `a b`, `deploy to PROD` ≠ `deploy to prod`,
`Do not delete.` ≠ `Do not delete`, `    indented` ≠ `indented`, `ﬁle` ≠ `file`.

---

## 3. Step 10.2.2 — implementation

Four stub modules became real. `models.py` and `registry.py` were not touched.

| Module | What it is now |
|---|---|
| `normalizer.py` | `normalize_for_hash`, `normalize_structural`, `supports_structural`, plus two functions the seams need: `hash_normalized` and `canonical_registration_text` |
| `decomposer.py` | `decompose(request) -> Tuple[ContextBlock, ...]`, classifying against §13's taxonomy |
| `matcher.py` | `Matcher.try_exact`, `.try_structural`, `.resolve`, `.resolve_blocks` |
| `auditlog.py` | `AuditLog` (the never-raise base), `InMemoryAuditLog`, `JsonlAuditLog` |

**Every deterministic hit is confidence 1.0, and cannot be otherwise.**
`MatchResult.match()` defaults it for non-semantic methods and the frozen type's
own validator rejects any other value for a deterministic method — so the "never
partially fill or guess a confidence" requirement is enforced by the contract,
not by this phase remembering to.

**The registry seam Phase 10.1 left is now used.** `hash_normalized` is the
composition Phase 10.1's `hash_text` parameter was built for (10.1 summary §4:
"10.1 hashes the text it is given, 10.2 decides what is given"). A registry
serving Tier 0 **must** be constructed as `Registry(path,
hash_text=hash_normalized)`; built with the default plain-SHA-256 hasher it
stores hashes of un-normalized text and Tier 0 misses every record with so much
as a trailing newline. Stated in three docstrings because it is a deployment
requirement, not a preference.

**`canonical_registration_text` closes a trap that is otherwise easy to hit.**
The registry stores one hash per record, and Tier 1 looks up the hash of the
*canonical* serialization — so a `TOOL_SCHEMA` registered pretty-printed is
unreachable from a compact one and vice versa. This function is the single
answer to "what text do I register", and a test asserts both halves: routed
through it, a pretty-printed block hits via Tier 1; registered in a
non-canonical form, the same context is reachable only byte-identically through
Tier 0.

**Tier 1 refuses two things a naive re-serializer would silently accept:**

- **Duplicate object keys.** RFC 8259 leaves a repeated key undefined and
  Python keeps the last, so `{"a":1,"a":2}` and `{"a":2}` would canonicalize
  identically while being two different documents to some other reader.
- **`NaN`/`Infinity`.** Accepted by Python's parser as an extension, not valid
  JSON, not representable on the way out.

A block that does not parse is a **miss, not an error**: §11's guarantee for
this tier ("changes zero semantic content, only serialization form") holds only
when the parse succeeded, so a failed parse falls through unchanged rather than
receiving a best-effort rewrite.

### Decomposition, and its one honest limitation

`decompose` classifies what an OpenAI-compatible request actually carries: a
`system`/`developer` message is `SYSTEM_PROMPT`, a `tools` entry is
`TOOL_SCHEMA`, the final `user` message is `USER_QUERY`, and everything else —
assistant turns, tool results, and any earlier `user` message — is
`CONVERSATION_HISTORY`. Both fallbacks are ineligible, so the distinction
changes no behavior; it exists so the decision log says what the gateway saw.

**Four of §13's six eligible types are not derivable from an unannotated
request.** `ORG_POLICY`, `TOOL_POLICY`, `AGENT_INSTRUCTION` and `RAG_DOCUMENT`
all arrive inside a system message and nothing in the wire format distinguishes
them. Guessing between them by inspecting content would be exactly the
eligibility-*widening* failure the stub's own docstring says to design against.
They therefore arrive only when the application says so, through an optional
`x_pulsekv_block_type` key on a message object — **a minimal extension point,
not a finished interface: Phase 10.5 owns the request surface** and may
formalize, rename or replace it. It is here because without it this phase could
produce two of six eligible types and plan §6's integration test would exercise
a quarter of the taxonomy. An unrecognized value raises `DecompositionError`
rather than being ignored, matching this codebase's `dec.KnownFields(true)`
posture; the *caller* decides whether to fail open, which is §17's split.

`token_estimate` is left unset. Phase 10.0's summary §5.2 records the tension
between §19 (threshold in tokens) and §22 (no gateway-side tokenization) and
assigns it to Phase 10.5; this phase does not pre-empt it with a guess, which is
also why `BypassReason.BELOW_MIN_TOKENS` is not emitted here.

### The decision log

Delivered per plan §6 and design §21, which calls it "a Phase 10.2 deliverable
... not deferred to Phase 10.9". Written by `Matcher.resolve_blocks` for **every
block including bypassed ones**, which is plan §6's stated invariant — a later
"what did the gateway do with this request" must be answerable for the whole
request, not only its matches.

`block_content_hash` is always the **Tier 0** normalized hash of the original
block, whichever tier resolved it. An audit trail wants one stable identity per
incoming block; logging Tier 1's structural hash for schemas would mean the same
block hashed differently depending on which tier happened to win.

Nothing in `auditlog.py` raises into the request path — the stub's rule, kept.
But a silent swallow would trade one blind spot for another, so failures are
**counted**: `dropped` and `last_error` make a broken sink visible to the
operator without making it visible to the request. Tested by closing the sink
under a running matcher: traffic completes, `dropped == 1`. §18 has no metric
label for an audit-sink failure today; that is noted here rather than invented.

---

## 4. Step 10.2.3 — ordering, short-circuit, and where this plugs in

`Matcher.resolve` is the pipeline:

```
ineligible block type ─────────────────────────► BYPASSED (no lookup at all)
Tier 0  hash → alias(raw) → alias(normalized) ─► MATCHED (exact | alias)
Tier 1  structured types only, on a Tier 0 miss ► MATCHED (structural)
        ── Phase 10.3 inserts try_semantic here, 10.4 the guard behind it ──
otherwise ─────────────────────────────────────► NO_CANDIDATE
RegistryError anywhere above ──────────────────► ERROR(component=registry)
```

**The short-circuit is proven by counting lookups, not by reading the returned
label.** `TestTierOrdering` wraps the registry in a counter: a block that would
hit both tiers records `method=exact` **and** exactly one `by_content_hash`
call, which is only possible if Tier 1 never ran. A Tier-1-only hit records two.
An ineligible block records zero calls of any kind.

**Order within Tier 0 is hash, then aliases.** A content-hash hit means the
block *is* some version's canonical text, which is the most specific statement
available; an alias is a registered pointer to a context. A test registers one
string as one context's canonical text and another's alias and asserts the hash
wins. Aliases are tried against the raw text and then the normalized text — both
are the "deterministic exact-match strings" §10 describes, two indexed equality
lookups, no scoring.

**Where the pipeline plugs in: not `find_candidates`.** The prompt asks this
explicitly. `Registry.find_candidates` **still raises
`NotImplementedError("Phase 10.3")`, unchanged**, because the design doc places
it in Tier 2: its docstring says "Tier 2 retrieval. Stubbed in 10.1, real vector
search in 10.3", and §11 makes Tier 2 candidate *retrieval*, a different thing
from the deterministic tiers. Deterministic matching is not a stage inside it.
The entry point is `Matcher.resolve` (one block) and `Matcher.resolve_blocks`
(one request's blocks, writing the audit trail), and Phase 10.5 wires
`decompose(request) → resolve_blocks(...) → assembler`. `resolve_blocks`
deliberately takes already-decomposed blocks rather than a request, so the
matcher never acquires an opinion about wire format.

---

## 5. Step 10.2.4 — tests

**219 passing**, up from Phase 10.1's 164:

```
test_models.py               98   (was 106; eight parametrized stub cases retired
                                   with the four modules this phase implemented)
test_registry.py             58   unchanged
test_deterministic_tiers.py  63   new
```

New suite by what it proves:

```
TestNormalizationRules 10 · TestTierOneStructural 10 · TestDecomposition  8
TestTierZeroExactMatch  6 · TestDecisionLog        6 · TestTierZeroAliases 5
TestIndexUsage          5 · TestTierOrdering       4 · TestBypassAndFailOpen 4
TestPhaseBoundary       3 · TestEndToEnd           2
```

Mapping to the prompt's required cases:

| Required | Where |
|---|---|
| Tier 0 hit and miss | `TestTierZeroExactMatch` — byte-identical, incidentally-different, different, empty registry, wrong namespace, deprecated version |
| Each Tier 1 normalization rule, positive + adjacent negative | `TestNormalizationRules` (10 tests, every rule paired with a case it must not absorb) and `TestTierOneStructural` (key order and whitespace converge; a changed type does not) |
| Ordering/short-circuit, recording which tier resolved it | `TestTierOrdering` — `MatchMethod` is the field that carries it; lookup counting is the proof |
| Empty registry and no-match-at-any-tier | `TestTierZeroExactMatch`, `TestEndToEnd.test_a_request_with_nothing_registered_changes_nothing` |
| Index usage | `TestIndexUsage`, below |
| Integration: decompose → normalize → Tier 0 → audit (plan §6) | `TestEndToEnd` — a real request producing `exact`, `bypassed`, `structural` in one audit file |

### Step 10.2.4's index check found a real defect

The requirement is that a deterministic tier's lookup does not scale with
registry size. Query plan plus timing, and the timing is what caught it:

```
by_content_hash    201 records ->  0.0186 ms      SEARCH canonical_context USING INDEX
                  4201 records ->  0.0191 ms      canonical_context_live_content_hash
                                   (21x rows, 1.03x time)   (namespace=? AND content_hash=?)

resolve_alias      201 aliases ->  0.0295 ms      SEARCH binding USING COVERING INDEX
                  4201 aliases ->  0.2486 ms      sqlite_autoindex_alias_binding_1 (namespace=?)
                                   (21x rows, 8.43x time)
```

`by_content_hash` was fine. **`resolve_alias` was not.** The planner drove from
`alias_binding`'s primary key constrained on `namespace` alone and filtered
`alias` row by row, declining migration 001's `alias_binding_lookup (namespace,
alias)` because that index does not carry `context_id`/`version` and using it
would have cost a table lookup per row to satisfy the join.

Note that the plan *said* `SEARCH`, not `SCAN`. A test asserting only "SEARCH
and not SCAN" would have passed while alias resolution grew with tenant size —
which is why both checks are kept and why the plan assertion now names the index
**and** the constraint beyond `namespace`.

**Fixed additively** by `migrations/002_alias_lookup_covering_index.sql`, which
widens the index to `(namespace, alias, context_id, version)` so the planner can
seek the full key and still answer the join from the index alone:

```
resolve_alias      201 aliases ->  0.0187 ms      SEARCH binding USING COVERING INDEX
                  4201 aliases ->  0.0191 ms      alias_binding_lookup (namespace=? AND alias=?)
                                   (21x rows, 1.02x time)
```

Indexes are derived data, so replacing one rewrites no history: no immutability
rule in 001 is relaxed and no row is touched.

---

## 6. Two bugs found by the tests, both real

Recorded because "the tests passed first time" is usually a statement about the
tests.

**1. Block-level strip re-indented the first line.** `normalize_for_hash`
originally stripped leading and trailing whitespace from the block as one
string. That removes the *first* line's indentation while every other line keeps
its own — not a rendering change but an inconsistent re-indent, and it made
`    indented` and `indented` hash identically, contradicting this phase's own
stated rule that indentation is syntax. Fixed: leading and trailing **blank
lines** are removed; leading whitespace on any line that has content, including
the first, is never touched.

**2. A test registered a schema that was not canonical.** `TestTierOneStructural`
originally registered `{"name":...,"parameters":{"path":...,"force":...}}`, whose
inner keys are not sorted, then expected Tier 1 to match a reordered variant.
The implementation correctly missed. The test was wrong, and the failure is the
`canonical_registration_text` trap arriving in practice — so the fixed suite now
asserts both directions, plus a guard test that the constant really is canonical
so the rest of that class cannot pass for the wrong reason.

A third, smaller consequence surfaced from fix 1: an alias no longer resolves
through *leading* indentation (`  gh-policy` misses), because aliases go through
the same normalizer as every block and get no special case. Pinned by a test
rather than left accidental, so a later phase changing it does so deliberately.

---

## 7. Exit criteria — verified

| # | Criterion | Evidence |
|---|---|---|
| 1 | Tiers 0 and 1 implemented, tested, conforming to `MatchResult` | 63 tests. Conformance is enforced by the frozen type: `MatchResult`'s validator rejects a deterministic method with any confidence but 1.0, and `DecisionLogRecord` rejects a similarity or guard outcome on a Tier 0/1 hit. Asserted in `TestDecisionLog.test_the_log_round_trips_through_the_frozen_type` |
| 2 | Ordering/short-circuit proven, not just each tier | `TestTierOrdering` counts registry lookups: both-tier block → `method=exact` and exactly one `by_content_hash`; Tier-1-only → two; ineligible → zero |
| 3 | The index is demonstrably used | `EXPLAIN QUERY PLAN` on the SQL captured from the live connection (via `set_trace_callback`, so the test cannot drift from a copy of the query) names the index and the constraint; plus timing at 201 and 4201 records. §5 records what this caught |
| 4 | `registry.py`'s Phase 10.1 interface unchanged | The file is byte-identical to what Phase 10.1 shipped (mtime 10:52, before this session's first edit at 11:09). The only registry-side change is a new migration file. One Phase 10.1 *test* was updated — it hardcoded `("001_initial",)`; its subject is "no migration is applied twice", now expressed as a comparison so a later phase need not edit it again |
| 5 | `find_candidates`'s 10.3 boundary correctly reflected | **Still fully `NotImplementedError("Phase 10.3")`.** §4 above gives the design-doc reading: it is Tier 2 retrieval, and the deterministic tiers are not a stage inside it. Asserted by `TestPhaseBoundary` |
| 6 | `git diff --stat -- src include tests node control proto adapters` empty | **Empty.** `git status --porcelain` over the same paths and over `gateway/tests/corpus` is also empty |
| 7 | This summary, with cited test results | This document |
| — | No new dependency, especially no ML library | `pyproject.toml` unchanged; still `pydantic==2.13.4` and `pytest==9.1.1` (dev). `TestPhaseBoundary` AST-checks all four modules against `numpy`, `scipy`, `torch`, `onnxruntime`, `sentence_transformers`, `transformers`, `sklearn`, `faiss`, `pulsekv_adapters` and `grpc` |

**Reproducing:**

```bash
python3 -m venv /tmp/gwvenv && /tmp/gwvenv/bin/pip install pydantic==2.13.4 pytest==9.1.1
PYTHONPATH=gateway /tmp/gwvenv/bin/python -m pytest gateway/tests -q     # 219 passed
```

---

## 8. What Phase 10.3 can now assume exists

1. **A short-circuiting Tier 0/1 already sitting in front of the seam.** The
   insertion point is one marked comment in `Matcher.resolve`, between Tier 1
   and the `no_candidate()` return. Everything reaching it has already missed
   both deterministic tiers, so `try_semantic` never has to re-check them.
2. **A miss reports `NO_CANDIDATE`, not an error**, and that stays true — 10.3
   changes what produces it, not what it means. `ERROR` remains reserved for
   fail-open (risk register row 5's detection signature depends on the two not
   being conflated).
3. **`normalize_for_hash` is the block's canonical text form**, and it is
   idempotent (asserted). Whatever 10.3 embeds should be the same normalized
   text Tier 0 hashed, or the two tiers will disagree about what the block is.
4. **The decision log already carries `similarity` and `guard_outcome` fields**
   that Tier 0/1 must leave unset and Tier 2/3 must fill — enforced by
   `DecisionLogRecord`'s validators, so 10.3 cannot forget to populate them.
5. **`Registry.list_records(namespace=..., current_only=True)`** is the scan to
   build an index from, namespace-scoped by construction.
6. **`find_candidates` is untouched and still raising**, with `namespace` and
   `block_type` already in its signature as pre-filters (§11, §15).

Two things 10.3 should read before starting:

- **The embedding-model version check is not implemented anywhere yet.** Risk
  register row 6 requires it enforced "in `Index.top_k`, not just documented".
  Phase 10.1 stores `embedding_model_id`/`_version` and 10.2 reads neither.
- **The case-folding decision in §2 above is deferred to 10.4, not settled.** If
  10.3's corpus work produces evidence either way, that belongs in 10.4's τ
  tuning rather than being changed quietly in the normalizer — the two tiers
  share `normalize_for_hash`, so a change there moves Tier 0's match set too.

**Deliverable:** `docs/pulsekv-semantic-context-phase10.3-summary.md`.
