# PulseKV v3 — Phase 10.0 Implementation Prompt (for Claude Code)

**How to use this file:** paste everything below the line into Claude Code,
run from inside the `pulsekv` repo root, on top of a clean `git status`. Do
**not** run this until the gate in
`pulsekv-semantic-context-implementation-plan.md` §3 has been resolved or
explicitly, consciously waived — that gate exists because this investigation
could not confirm or rule out a possible v2 availability bug (the master
prompt's §25 soak-collapse question) with the evidence currently in the
repository, and Phase 10 assumes a stable v2 core underneath it.

---

You are implementing **Phase 10.0 only** of PulseKV v3: contracts,
architecture freeze, and the evaluation-corpus skeleton for the Semantic
Context Canonicalization feature. This phase produces **no runtime
behavior** — no gateway process, no registry storage, no embedding, no
matching logic. Its entire job is freezing the internal contract later
phases build against, the same role v2's Phase 0 played for the gRPC
contract, so Phase 10.1 onward doesn't diverge. Before writing anything,
read, in order:

1. `docs/pulsekv-semantic-context-design.md` — the full architecture this
   phase's contract must match, especially §6 (three findings from reading
   the actual current source, not just prior docs), §7 (correctness
   invariants), §10 (registry design), §11 (matching pipeline), §12
   (equivalence validation), §21 (auditability).
2. `docs/pulsekv-semantic-context-implementation-plan.md` §3 through §5 —
   the pre-Phase-10 gate, and Phase 10.0's exact scope and exit criteria.
3. `docs/pulsekv-semantic-context-codebase-impact.md` — confirms which
   directories this phase must not touch.
4. `adapters/pulsekv_adapters/client.py` — read this closely. Phase 10.1
   onward will use `PulseKVClient` as an ordinary imported library; this
   phase does not call it, but the contract you freeze should not assume
   anything about PulseKV that this file's actual public API (`get`, `set`,
   `exist`, `put`, `prefix_match`, `close`) doesn't already provide.
5. `proto/adapter.proto` and `proto/README.md` — confirm for yourself that
   `AdapterService` is unimplemented and uncalled (grep
   `adapters/pulsekv_adapters/` for any reference to it — there should be
   none in `sglang.py`/`vllm.py`). This phase's contract does not extend or
   implement `AdapterService`; it defines a new, internal-to-`gateway/`
   Python contract instead.
6. `control/internal/config/config.go` and its test file — read this for the
   "reject before anything starts, report every problem at once" config
   validation pattern already established in this codebase
   (`pulsekv-v2-phase0-summary.md` §6's negative-path table is the
   as-built evidence this pattern exists and is tested). Phase 10.0's own
   config/contract validation should follow the same posture, not invent a
   new one.

## Hard scope boundary

- **Do not create the `gateway/pulsekv_gateway/{server,registry,encoder,
  index,guardrail,assembler}.py` implementation files with real logic in
  them.** This phase defines types and interfaces (`models.py`, and stub
  files with `NotImplementedError` bodies where the design doc's directory
  layout calls for a module that later phases fill in), not working code.
- **Do not modify** `src/`, `include/`, `tests/`, `node/`, `control/`,
  `proto/`, or any file under `adapters/pulsekv_adapters/`. Every phase in
  the implementation plan repeats this boundary; Phase 10.0 is not an
  exception just because it's early.
- **Do not pick an embedding model, vector index library, or relational
  database.** Those are Phase 10.3 and Phase 10.1 decisions respectively,
  made against real data this phase does not yet have. If you find yourself
  adding a dependency to `pyproject.toml` beyond a schema/validation library
  (e.g. `pydantic`) and a testing framework, stop and reconsider.
- **Do not populate the evaluation corpus with real adversarial examples
  yet.** That is Phase 10.4's job, once a real guard exists to test against.
  This phase creates the corpus's directory structure and a README
  describing what belongs in each category, per the four classes in the
  design doc §12 / implementation plan §8 (positive paraphrase, adversarial
  negative, cross-tenant, version-update).
- **Do not resolve implementation plan §3's soak-collapse gate as part of
  this session.** That is separate work (Phase 9.x stabilization scope, not
  Phase 10 scope) — confirm it has already been resolved or explicitly
  waived before starting; do not attempt to resolve it yourself as a
  substitute for that check.

## Step 10.0.1 — repository layout

Create the `gateway/` directory per the implementation plan's layout:

```
gateway/
├── pyproject.toml
├── pulsekv_gateway/
│   ├── __init__.py
│   ├── models.py          # real: the frozen contract types (this step's main output)
│   ├── registry.py        # stub: class shape only, NotImplementedError bodies
│   ├── decomposer.py       # stub
│   ├── normalizer.py       # stub
│   ├── matcher.py          # stub
│   ├── encoder.py          # stub
│   ├── index.py            # stub
│   ├── guardrail.py        # stub
│   ├── assembler.py        # stub
│   ├── auditlog.py         # stub
│   ├── server.py           # stub
│   └── config.py           # stub
└── tests/
    ├── test_models.py       # real: schema round-trip tests
    └── corpus/
        ├── README.md         # real: describes the four categories, empty otherwise
        ├── positive_paraphrase/
        ├── adversarial_negative/
        ├── cross_tenant/
        └── version_update/
```

## Step 10.0.2 — freeze the registry record contract

In `models.py`, define the canonical-context record exactly per design doc
§10: `context_id`, `version` (immutable once published — encode this as a
frozen/immutable dataclass or a Pydantic model with `frozen=True`, not just
a comment saying so), `namespace` (required, no default), `canonical_text`,
`content_hash` (document that it's computed, don't compute it yet — no
hashing logic in this phase, just the field), `block_type` (an enum matching
the design doc §13 eligibility table — include the ineligible types too,
e.g. `USER_QUERY`, `CONVERSATION_HISTORY`, so `Matcher` code in later phases
can exhaustively check eligibility against one enum rather than a
string), `embedding_model_id`/`embedding_model_version` (optional, per
design doc §16), `aliases`, `created_at`/`created_by`/`deprecated_at`.

## Step 10.0.3 — freeze the `MatchResult` and decision-log contracts

Per master prompt §30's sketch and design doc §21: `MatchResult` with
`matched: bool`, `context_id`, `version`, `confidence`, `method` (enum:
`exact | alias | structural | semantic`), `rejection_reason` (populated only
when `matched=False` and a guard actually ran and rejected something —
distinguish this from "no candidate found at all," which is a different,
also-representable state). Decision-log record per design doc §21's field
list, with `content_hash` stored, not raw text, matching the privacy default
in §20.

## Step 10.0.4 — config validation posture

`config.py`'s stub should at minimum define the config shape (namespace
resolution source, registry connection target, bypass-threshold default —
value TBD by Phase 10.8, but the field and a documented placeholder default
belong here) and a docstring committing later phases to the
reject-everything-at-once validation pattern from `control/internal/config`,
so Phase 10.5 doesn't have to make that design decision itself later.

## Step 10.0.5 — evaluation corpus skeleton

`tests/corpus/README.md` documents each of the four categories with 2-3
sentences on what belongs there and what property Phase 10.4 will assert
against it (e.g., adversarial_negative's property is "zero false positives
when run through the Tier 3 guard," not just "hard examples"). No actual
example files yet.

## Exit criteria — verify all of these before considering Phase 10.0 done

1. `git diff --stat -- src include tests node control proto adapters` is
   empty.
2. `gateway/pulsekv_gateway/models.py` contains real, tested types for the
   registry record, `MatchResult`, and the decision-log record; every other
   `.py` file under `pulsekv_gateway/` contains only class/function
   signatures with `NotImplementedError` bodies — no matching logic, no
   registry storage, no HTTP server, no embedding calls anywhere.
3. `tests/test_models.py` round-trips each frozen type and asserts version
   immutability is enforced at the type level (attempting to mutate a
   published version's `canonical_text` raises).
4. `tests/corpus/` exists with the four category directories and a README
   describing each; no adversarial examples populated yet.
5. `gateway/pyproject.toml` declares no more than a schema/validation
   library and a test framework as dependencies — no embedding, vector
   index, or web framework dependency yet.
6. `docs/pulsekv-semantic-context-implementation-plan.md` §3's gate is
   confirmed resolved or explicitly waived (state which, and by whom/how,
   in this phase's summary — do not silently assume it).
7. No `docs/pulsekv-v2-*.md` file is modified.

Write `docs/pulsekv-semantic-context-phase10.0-summary.md` in the same
evidence-first style as v2's phase summaries: exact layout produced,
deviations from this prompt with reasoning, exit-criteria evidence, and
where Phase 10.1 (the registry's real storage implementation) should start.

Do not start any Phase 10.1 work — including real registry storage,
real hashing, or real CRUD logic — until this phase's exit criteria are
verified and the summary is written.
