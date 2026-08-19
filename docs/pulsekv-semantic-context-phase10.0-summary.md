# PulseKV v3 / Phase 10.0 — Contracts, Architecture Freeze, Corpus Skeleton

**Status:** complete. **Produces no runtime behavior**, by design — this phase
freezes the internal contract Phases 10.1–10.9 build against, the same role v2's
Phase 0 played for the gRPC contract.

**Scope actually touched:** one new top-level directory, `gateway/`. Nothing
under `src/`, `include/`, `tests/`, `node/`, `control/`, `proto/`, or
`adapters/` was read-modify-written; verified in §6 below, not asserted.

---

## 1. The §3 gate — waived, for Phase 10.0 only

> **Update, later the same day: the gate is now RESOLVED, and this waiver is
> superseded.** The collapse was root-caused to a test-harness defect in
> `deploy/` — not a v2 availability bug — then fixed, regression-tested, and
> re-verified with a fresh long-duration soak. See
> `docs/pulsekv-v2-soak-collapse-analysis.md` and the rewritten
> implementation-plan §3. Phase 10.1 is no longer blocked. The waiver record
> below is kept as-is, because it is what Phase 10.0 actually ran under.

Implementation plan §3 makes the unresolved soak-collapse question a hard gate.
Checked against the repository before any file was created:

| What the plan says to look for | What is on disk (Aug 18, 2026) |
|---|---|
| The actual 4-hour run's logs | Not present. `deploy/run/soak-report.json` reports `duration_seconds = 180.02`; `find . -name "*soak*"` returns only `deploy/soak-test.sh`, that report, `soak-metrics.prom`, and `logs/soak-chaos.log` — no artifact of any longer run exists |
| Reconciliation of the throughput/volume discrepancy | Still unreconciled. On disk: `ops_per_second = 1505.99`, `operations = 271111`, `verified = 204497`, `rpc_errors = 13803`. `pulsekv-v2-progress-report.md` §4.2 narrates 5,390 ops/s and 182,312 reads verified for what it calls the same Phase 9.4 evidence |
| A characterization of the collapse, if it reproduces | None exists. Risk register row 16 still lists it as an open, unverified question |
| Any commit resolving it | None. `git log` since the docs were written shows no gate work |

**Resolution: explicitly waived for Phase 10.0 only, by the repository owner
(Mohit Manna), in the implementation session of Aug 18, 2026**, on the grounds
plan §3 itself states — "Phase 10.0 can start in parallel with this
investigation since it produces no code that depends on cluster runtime
behavior." The waiver was requested and granted before any file was created,
not assumed.

**The gate remains closed for Phase 10.1 onward.** Nothing in this phase makes
the underlying question less relevant: every risk in the register still assumes
a stable v2 cluster beneath the gateway. Plan §3's three steps — locate or
reproduce the 4-hour run, reconcile the progress report's figures against the
artifact that produced them, characterize the collapse if it reproduces — are
unchanged and remain Phase 9.x work, not Phase 10 scope. This phase did not
attempt them.

---

## 2. Exact layout produced

```
gateway/
├── .gitignore                   # __pycache__, *.pyc, .venv, *.egg-info  [addition, §5.1]
├── README.md                    # component overview                     [addition, §5.1]
├── pyproject.toml               # pydantic==2.13.4; pytest==9.1.1 (dev)
├── pulsekv_gateway/
│   ├── __init__.py       (74)   # re-exports models only
│   ├── models.py       (1155)   # REAL — the frozen contract
│   ├── config.py        (196)   # shape + validation posture; behavior stubbed
│   ├── registry.py      (127)   # stub — Phase 10.1
│   ├── decomposer.py     (28)   # stub — Phase 10.2
│   ├── normalizer.py     (44)   # stub — Phase 10.2
│   ├── matcher.py        (37)   # stub — Phase 10.2 / 10.3 / 10.4
│   ├── auditlog.py       (46)   # stub — Phase 10.2
│   ├── encoder.py        (51)   # stub — Phase 10.3
│   ├── index.py          (49)   # stub — Phase 10.3
│   ├── guardrail.py      (47)   # stub — Phase 10.4
│   ├── assembler.py      (41)   # stub — Phase 10.5
│   └── server.py         (43)   # stub — Phase 10.5
└── tests/
    ├── test_models.py   (777)   # 108 tests, all passing
    └── corpus/
        ├── README.md            # the four categories and their asserted properties
        ├── positive_paraphrase/.gitkeep
        ├── adversarial_negative/.gitkeep
        ├── cross_tenant/.gitkeep
        └── version_update/.gitkeep
```

26 functions across the 10 pure-stub modules; every one is a docstring plus a
single `raise NotImplementedError(...)`, checked by AST rather than by eye
(§6, criterion 2).

---

## 3. What was frozen, and where each decision came from

### 3.1 Six models, nine enums

| Type | Source | Role |
|---|---|---|
| `CanonicalContextRecord` | design §10 | One published version of one canonical context |
| `MatchResult` | master prompt §30, design §21 | What the matcher decided about one block |
| `DecisionLogRecord` | design §21, §20 | The audit record |
| `ContextBlock` | plan §6, §9 | One decomposed block (the only type holding raw text) |
| `Candidate` | design §11 Tier 2 | The Tier 2 → Tier 3 hand-off |
| `GuardResult` | plan §8 | Tier 3's verdict |
| `BlockType`, `BlockEligibility` | design §13 | The taxonomy table, ineligible rows included |
| `MatchMethod`, `MatchOutcome` | design §18, §21 | How, and what happened |
| `RejectionReason`, `BypassReason`, `GuardOutcome`, `GatewayComponent` | design §18 | Metric label vocabularies, transcribed exactly |

The four label-set enums use design doc §18's metric label strings verbatim, so
a metric emitted in Phase 10.2+ and a decision logged in Phase 10.2+ cannot
disagree about what to call the same thing.

### 3.2 Three invariants moved from prose into the type system

1. **Immutability.** Every contract model is `frozen=True`. Assigning
   `canonical_text`, `content_hash`, `version`, `context_id` or `namespace` on a
   published record raises `ValidationError(frozen_instance)`. `aliases` is a
   `tuple`, not a `list`, because a frozen model with a mutable list field is
   only frozen at the top level. Deprecation — the one legal state change
   (design §17) — is `record.deprecate(at)`, which returns a **new** re-validated
   record and leaves the original untouched.

2. **Namespace is mandatory.** No default anywhere it appears, and its shape is
   constrained by the same regex `control/internal/config/config.go` applies to
   `node_id` (`^[A-Za-z0-9][A-Za-z0-9._-]*$`). Two namespaces differing only by
   whitespace or punctuation would be a tenant-isolation hazard, so the shape is
   refused at the contract edge rather than trusted from the deployment layer
   that supplies it (design §15).

3. **Illegal decision states do not construct.** Encoded as validators, with
   the design doc section each one comes from cited in the error message:
   - a match with no method, no `context_id`/`version`, or no confidence
   - a Tier 0/1 hit carrying a similarity score or a guard outcome — design §11
     says the guard "never runs on Tier 0/1 hits" and no embedding is computed
     for them
   - a semantic match whose guard did not pass — design §12 admits no other
     route to a substitution
   - a rejection with no reason, or with no identified candidate
   - `matched` disagreeing with `outcome`

### 3.3 Four non-match states, not one

The Phase 10.0 prompt (§10.0.3) requires "no candidate found at all" to be
distinguishable from a guard rejection. Implemented as `MatchOutcome`, with two
members design §21's flat decision list does not name:

- `NO_CANDIDATE` — retrieval returned nothing to consider.
- `REJECTED` — a specific candidate was considered and refused; carries the
  candidate's `context_id`/`version`/similarity so the audit trail can say which.
- `BYPASSED` — the block never entered the pipeline (§18's bypass reasons).
- `ERROR` — a component failed and the gateway fell open (design §17). Kept
  distinct from a miss deliberately: **risk register row 5's detection signature
  is "an error spike with no corresponding drop in request success"**, which is
  unprovable if a fail-open error is logged as an ordinary miss.

§21's flat vocabulary is preserved as a projection: `DecisionLogRecord.decision_label`
renders `bypassed|exact|alias|structural|semantic|rejected`, plus `no_candidate`
and `error`.

### 3.4 Privacy enforced by shape, not by rule

`DecisionLogRecord` has **no field capable of holding prompt text** — the block
is identified by `block_content_hash` alone (design §20). `extra="forbid"` means
passing `text=` to it raises rather than being ignored, and `TestPrivacyByShape`
locks the exact 16-field set, so a later phase adding a raw-text field has to
change a test — making the privacy decision visible in review instead of
accidental. `ContextBlock` is the only contract type that holds text, and it is
in-memory only.

### 3.5 Validation posture inherited, not invented

Cross-field checks accumulate into a `problems` list and raise together —
directly mirroring `control/internal/config/config.go`'s `Validate()`
(`errors.Join(problems...)`) and the as-built evidence in
`pulsekv-v2-phase0-summary.md` §6: *"Duplicate `node_id` and port in config →
Rejected before anything starts, both problems reported at once."* Pydantic
already reports field-level errors together; the accumulator extends that to the
rules that span fields. `extra="forbid"` is the contract's equivalent of that
file's `dec.KnownFields(true)`.

`config.py` commits Phase 10.5 to the same posture in prose **and** ships the
five-item checklist `validate_config()` must implement, each item traced to a
document, so 10.5 is not re-deciding questions already answered.

---

## 4. Deliberate deviations from the Phase 10.0 prompt

Every one is an addition or a refinement; none removes anything the prompt asked
for.

| # | Deviation | Reasoning |
|---|---|---|
| 1 | **A summary doc exists at all.** Plan §4 says Phase 10.0 "does not produce its own summary doc; its output *is* the frozen contract" | The Phase 10.0 prompt's closing instruction explicitly requires one. Prompt wins; the conflict is recorded here rather than silently resolved |
| 2 | **`gateway/README.md` and `gateway/.gitignore` added** beyond the prescribed tree | Every other top-level component here has a README (`adapters/`, `node/`, `node/engine/`, `proto/`); `pyproject.toml`'s `readme = "README.md"` would also fail to build without one. `.gitignore` keeps `__pycache__` out of a package whose test suite imports it |
| 3 | **`embedding` field kept** on the registry record, though the prompt's §10.0.2 field list names only `embedding_model_id`/`_version` | Design §10's record shape lists it and plan §5 says 10.1 "stores embeddings as an opaque blob field if provided". Typed `Optional[bytes]`, never interpreted, base64 in JSON. Including it now avoids the schema change at 10.1 that plan §4 says it does not expect. It picks no model and no index |
| 4 | **`safety_class` omitted**, though design §10's sketch lists it | §10 marks it "reserved, unused in the MVP — not fabricated content", and the prompt's own field list omits it. An always-`None` field with no defined values would freeze a shape nobody has designed |
| 5 | **`content_hash` shape is validated** (64 lowercase hex) though the prompt says "just the field" | A format constraint is not a hash computation — nothing here hashes anything. §10 fixes the algorithm as SHA-256, so the shape is design-determined. Consistency between hash and text is explicitly *not* checked and is called out in the docstring as Phase 10.1's storage-layer duty |
| 6 | **Three types added beyond the three named** (`ContextBlock`, `Candidate`, `GuardResult`) | Each is a cross-phase seam the prompt's own stub list requires typing: `decompose → matcher → assembler` (plan §6/§9), `index → guardrail` (plan §7/§8), and `Guardrail.check → GuardResult` is named verbatim in plan §8. Without them every stub signature would degrade to `str` and lose the block-type binding §12.3's check depends on |
| 7 | **`MatchOutcome`, `bypass_reason`, `error_component` added** to the master prompt's §30 `MatchResult` sketch | §30's sketch has one bool for four different outcomes. §18's metrics already separate bypass reasons from rejection reasons, and the prompt itself demands "no candidate found" be distinguishable. `matched: bool` is kept as a real field, with a validator making it unable to disagree with `outcome` |
| 8 | **`DecisionLogRecord.from_match_result` included** — arguably behavior in a contracts phase | Pure field projection: no I/O, no hashing (the caller passes the hash it already has). It exists so 10.2's writer and any later one cannot each invent their own mapping of `confidence`→`similarity` or rejection reason→guard outcome. Fully tested across all six reasons and all five outcomes |
| 9 | **`block_index` added** to §21's field list | §21 logs "per request, per eligible block"; without an index two blocks of the same type in one request are indistinguishable, and design §14's order-preservation has no identity to preserve |
| 10 | **`Registry.resolve_alias` returns the record**, where plan §5 sketches `-> Optional[context_id]` | Tier 0's caller (plan §6) immediately needs `canonical_text` and `version` to build a `MatchResult` and substitute. Returning the id alone makes every alias hit a mandatory second round trip. Flagged in the method's docstring |
| 11 | **`LOW_SIMILARITY` treated as a rejection, not a miss** | Design §12 runs the guard only on candidates that already cleared τ, so a below-τ top candidate is refused *before* Tier 3 — yet §18 counts it under `pulsekv_semantic_reject_total`. Modelled as `REJECTED` with `guard_outcome = None`, which is the only reading consistent with both sections. The pairing is enforced by a validator |
| 12 | **`PLACEHOLDER_BYPASS_MIN_ELIGIBLE_TOKENS = 512`** | Design §19 refuses 512 as a *design conclusion* while requiring the threshold to "ship as a configuration default pending Phase 10.8 data". The field needs a value; the constant's name and comment make it impossible to read as evidence. Three per-tier timeout placeholders were added on the same basis (design §17's budgets, risk register row 14's "real timeouts, not aspirational") |
| 13 | **`.gitkeep` in each corpus directory** | Git does not track empty directories; without them criterion 4's structure would not survive a clone. Each says what it is and that 10.4 replaces it. No examples were written |
| 14 | **A stub-discipline test lives in `test_models.py`** rather than its own file | The prescribed layout has exactly one test file. `TestStubsAreStubs` asserts criterion 2 mechanically — every stub callable raises `NotImplementedError`, and no module imports `pulsekv_adapters` or `grpc` (checked by AST, since every module mentions `pulsekv_adapters` in prose) |

---

## 5. Notes and questions surfaced for later phases

Recorded because they were found while freezing the contract, not resolved here.

1. **`AdapterService` is uncalled, but not unreferenced.** The prompt asked to
   confirm this for `sglang.py`/`vllm.py` — confirmed, neither mentions it. But
   `adapters/pulsekv_adapters/health_client.py` *does* construct an
   `AdapterServiceStub` (it is the `pulsekv-health` entry point, used by
   `deploy/smoke-test.sh`), calling a service nothing serves. Design §6's
   Finding 1 and the codebase-impact map both say "unimplemented and uncalled",
   which is true of the *server* side and of the two real adapters, but a reader
   grepping the tree will find that one client-side reference. Nothing in Phase
   10 depends on it either way; noted so the discrepancy is not rediscovered as a
   surprise.

2. **The bypass threshold measures something the gateway refuses to compute.**
   Design §19's bypass policy is stated in *tokens*, while §22 rejects
   gateway-side tokenization outright (a gateway tokenizer that disagreed with
   the engine's would be a drift risk). `ContextBlock.token_estimate` is
   therefore documented as an estimate that nothing affecting cache identity may
   read. **Phase 10.5 must decide how the estimate is produced** — this is a real
   tension between two design sections, not an oversight in either.

3. **`RAG_DOCUMENT` is the highest-risk eligible type.** §13 admits it only if
   pre-registered, which is structural (an unregistered document has no record to
   retrieve). But two genuinely different documents from one corpus are exactly
   the high-similarity/different-meaning shape §12's guard exists to refuse.
   Flagged in `BLOCK_ELIGIBILITY`'s comment as an attention point for 10.4's
   corpus.

4. **Plan §4 forward-references a "§5.2"** for the `gateway/` layout, which does
   not exist in that document — the layout only exists in this phase's prompt.
   The layout implemented is the prompt's, plus the two additions in §4 above.

5. **`PulseKVClient` cannot enforce registry semantics, and is not asked to.**
   Read for this phase: its public surface is `get`/`set`/`exist`/`exists`/`put`/
   `prefix_match`/`close`, over `str|bytes` keys and `bytes` values, raising
   `PulseKVClientError`. There is no namespace concept, no delete, no conditional
   or compare-and-swap write, and `exist()` is a full `get()` rather than a cheap
   probe. The contract frozen here assumes none of those — consistent with design
   §10's ruling that the registry lives in an ordinary relational store, not in
   PulseKV.

---

## 6. Exit criteria — verified

| # | Criterion | Evidence |
|---|---|---|
| 1 | `git diff --stat -- src include tests node control proto adapters` is empty | Empty. `git status --porcelain` over the same paths is also empty — no untracked files were added there either. Whole-repo status shows exactly two untracked entries: the pre-existing v3 docs and the new `gateway/` |
| 2 | `models.py` has real, tested types; every other `.py` has only signatures with `NotImplementedError` bodies | AST check over the 10 stub modules: **26 functions, every one a docstring plus a single `raise NotImplementedError(...)`, zero non-conforming**. `TestStubsAreStubs` invokes all 26 at runtime and asserts each raises; a separate test does the same for `config.py`'s three callables. No module imports `pulsekv_adapters` or `grpc` (AST-verified). `__init__.py` contains only re-exports and defines no callables |
| 3 | Tests round-trip each frozen type and prove version immutability is enforced at the type level | **108 tests, all passing** (`pytest tests -q` → `108 passed in 0.24s`). `TestRoundTrip` round-trips all six models through JSON with exact equality, including a non-UTF-8 embedding blob and tz-aware datetimes. `TestImmutability` asserts `ValidationError(frozen_instance)` on reassignment of `canonical_text`, `content_hash`, `version`, `context_id` and `namespace`, that the object is genuinely unchanged afterward, that `aliases` is an immutable tuple, and that all six models declare `frozen=True` |
| 4 | `tests/corpus/` exists with four category directories and a README; no adversarial examples | Four directories, each with a `.gitkeep` explaining itself; `README.md` gives each category 2–4 sentences and the property 10.4 asserts (notably: adversarial_negative's is "zero false positives, confirmed by test, at whatever τ the phase lands on"), plus what does *not* belong there and why the file format is 10.4's decision. Zero example files |
| 5 | `pyproject.toml` declares no more than a schema/validation library and a test framework | Built a wheel and read its metadata: `Requires-Dist: pydantic==2.13.4` and `Requires-Dist: pytest==9.1.1; extra == "dev"`. Nothing else. No embedding runtime, no vector index, no web framework, no database driver. `Requires-Python: >=3.10`, verified honest — no 3.11+ construct appears anywhere (`StrEnum`, `datetime.UTC`, `except*`, `tomllib`, `typing.Self` all absent; every module carries `from __future__ import annotations`) |
| 6 | Plan §3's gate confirmed resolved or explicitly waived, stating which and by whom | **Explicitly waived, for Phase 10.0 only, by the repository owner (Mohit Manna), in the Aug 18, 2026 implementation session**, before any file was created. Evidence that the gate is genuinely still open — not quietly assumed closed — is tabulated in §1. It remains closed for 10.1 onward |
| 7 | No `docs/pulsekv-v2-*.md` file is modified | `git status --porcelain -- 'docs/pulsekv-v2-*.md'` reports only `?? docs/pulsekv-v2-semantic-canonicalization-report.md`, which was already untracked at session start (it appears in the opening `git status` snapshot) and whose mtime is unchanged at Aug 18 21:47. This phase read v2 docs and wrote none | **(Still true of Phase 10.0. The separate Phase 9.x gate work that followed does edit `pulsekv-v2-progress-report.md` and adds `pulsekv-v2-soak-collapse-analysis.md`; that is not this phase's change.)**

**Reproducing the test run** (the environment used was a throwaway venv outside
the repository, so nothing was installed into the user's Python):

```bash
python3 -m venv /tmp/gwvenv && /tmp/gwvenv/bin/pip install pydantic==2.13.4 pytest==9.1.1
cd gateway && PYTHONPATH=. /tmp/gwvenv/bin/python -m pytest tests -q
```

---

## 7. Where Phase 10.1 starts

**Precondition, unchanged:** plan §3's gate is waived only for 10.0. Resolve it —
or take a second, separate, conscious waiver — before beginning 10.1.

The contract is the input. `gateway/pulsekv_gateway/registry.py` already carries
the method set, the typed exception hierarchy (`RegistryError` →
`RegistryUnavailableError` / `RegistryVersionImmutableError` /
`RegistryNotFoundError`, all under `models.GatewayError` so 10.5's fail-open
wiring is one `except`), and the two invariants restated at the top of that file.
Phase 10.1's job is to make them true below the type system:

1. **Choose the store and write the schema.** Plan §5 recommends a real SQL store
   from the start over SQLite-then-migrate. `CanonicalContextRecord`'s fields map
   to columns directly; `embedding` is an opaque blob. Design §10 rules out
   exactly one option — PulseKV itself, whose NVMe tier is loss-tolerant by design
   and therefore wrong for records that must not come back different.

2. **Add the current-version pointer, which is deliberately not on the record.**
   §10's "immutable versions, mutable pointer" cannot be a record field without
   contradicting immutability — publishing v5 would have to rewrite v4's row.
   It is separate mutable state 10.1 owns.

3. **Enforce version immutability in SQL, not just in Python.** The type refuses
   in-process mutation; an `UPDATE` of a published version's
   `canonical_text`/`content_hash` must be rejected by the storage path too.

4. **Prove namespace isolation at the storage layer.** Two namespaces holding an
   identical `content_hash` must never see each other's rows. Plan §5 wants this
   proven here, not inferred from the matcher passing namespace through correctly
   later — design §15's "structurally impossible" claim rests on it.

5. **Prove durability across an actual process restart**, per plan §5's exit
   criteria — restart in the test, not trust in the storage engine's guarantees.

6. **Compute `content_hash` for the first time.** `CONTENT_HASH_ALGORITHM` and
   `CONTENT_HASH_PATTERN` fix the algorithm and rendering; the normalization that
   feeds it is Phase 10.2's `normalizer.normalize_for_hash`, so 10.1 should hash
   the text it is given and let 10.2 decide what is given.

`Registry.find_candidates` is stubbed with the right signature but belongs to
Phase 10.3 — its `NotImplementedError` says so. Nothing in 10.1 should compute or
search an embedding.

**Deliverable:** `docs/pulsekv-semantic-context-phase10.1-summary.md`.
