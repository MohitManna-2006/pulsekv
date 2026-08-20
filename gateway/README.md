# PulseKV Context Gateway

A new, standalone component: the ingress point where a request's reusable
context blocks are replaced with agreed canonical text **before** the text
reaches an inference engine's tokenizer, so two differently-worded blocks that
mean the same thing produce byte-identical tokens and hit PulseKV's existing
exact-match cache.

Nothing in `node/`, `control/`, `proto/`, or `adapters/` changes for this to
work. That is the design's central claim, and it rests on one verified fact:
SGLang's and vLLM's own tokenizers already derive block hashes from the tokens
they produce, so identical text in means identical cache keys out, through
completely unmodified adapter code
(`docs/pulsekv-semantic-context-design.md` §6, Finding 2).

## Status: Phase 10.3 — deterministic matching, plus semantic retrieval

Blocks that are byte-identical after incidental-rendering cleanup, or that are
the same tool schema serialized differently, resolve to a registered canonical
context with no embedding involved. On a miss, Tier 2 now embeds the block and
retrieves the nearest registered contexts within its namespace — as
**candidates only**. Nothing turns a candidate into a substitution yet; that is
Phase 10.4's guard. `guardrail.py`, `assembler.py` and `server.py` are still
signature-only stubs whose bodies raise `NotImplementedError`.

| Module | Phase | What it becomes |
|---|---|---|
| `models.py` | **10.0 (done)** | Registry record, `MatchResult`, decision-log record, block taxonomy |
| `registry.py` | **10.1 (done)** | Durable, versioned, namespace-scoped storage (SQLite, WAL) |
| `normalizer.py`, `decomposer.py`, `matcher.py`, `auditlog.py` | **10.2 (done)** | Tier 0/1 and the decision log |
| `encoder.py`, `index.py` | **10.3 (done)** | Tier 2 candidate retrieval (MiniLM-L6-v2, ONNX CPU) |
| `guardrail.py` | 10.4 | Tier 3 equivalence guard, and the τ threshold it earns |
| `server.py`, `assembler.py`, `config.py` | 10.5 | The actual proxy process |

### The two deterministic tiers

```
block text ──[normalize_for_hash]──────────────────────► hash ─► registry   Tier 0
           └─[normalize_structural]─[normalize_for_hash]─► hash ─► registry   Tier 1
```

One lookup mechanism, two front-ends. Tier 0 runs first because it is cheaper;
Tier 1 runs only for a block type with a canonical serialization (`TOOL_SCHEMA`)
and only after Tier 0 misses. The first hit wins and no later tier runs. A hit
is confidence 1.0 by construction — Tier 0/1 are exact, never scored.

Normalization removes rendering, never meaning: Unicode NFC, line endings,
trailing whitespace per line, blank-line runs. It deliberately does **not** fold
case or collapse whitespace inside a line — see `normalizer.py`'s docstring for
why each of those would have no guard behind it.

Tier 2 embeds that same normalized text and ranks registered contexts by cosine
similarity, partitioned by `(namespace, block_type)` so a cross-tenant
comparison is never performed rather than filtered away. It applies no
threshold and reaches no verdict.

### Running Tier 2

The 90 MB weights are not vendored. Fetch them once:

```bash
DIR=~/.cache/pulsekv-gateway/models/all-MiniLM-L6-v2
mkdir -p "$DIR"
BASE=https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main
for f in onnx/model.onnx tokenizer.json tokenizer_config.json config.json; do
  curl -sSL -o "$DIR/$(basename $f)" "$BASE/$f"
done
export PULSEKV_GATEWAY_MODEL_DIR="$DIR"
```

Without them, `OnnxEncoder` raises a typed error naming this command, Tier 2
does not run, and Tiers 0/1 keep working — which is design doc §17's fail-open
behavior, not a degraded mode. The Tier 2 tests skip themselves when the
weights are absent.

## The invariant everything else serves

A missed match costs a prefill recompute — the system's normal, correct
behavior today. A false match silently changes what the model was asked. Every
tier, guard, timeout and error path in this package is biased toward the first
(`docs/pulsekv-semantic-context-design.md` §4, §7, §12).

## Layout

```
gateway/
├── pyproject.toml          # pydantic + pytest, nothing else yet
├── pulsekv_gateway/        # the package
│   └── migrations/         # the registry's schema, applied on open
└── tests/
    ├── test_models.py               # contract tests
    ├── test_registry.py             # storage tests
    ├── test_deterministic_tiers.py  # tier 0/1 and the decision log
    ├── test_semantic_retrieval.py   # tier 2: encoder, index, retrieval seam
    └── corpus/             # evaluation corpus, populated in Phase 10.4
```

The registry keeps its records in an ordinary SQLite file — no service to run
beside the gateway, and no new dependency, since `sqlite3` is in the standard
library. Version immutability and namespace isolation are enforced by triggers
and compound keys, so they hold even for a caller holding a raw connection.
Design doc §10 rules out exactly one store: PulseKV itself, whose NVMe tier is
loss-tolerant by design and therefore wrong for records that must not come back
different.

## Running the tests

```bash
python -m venv .venv && .venv/bin/pip install -e './gateway[dev]'
.venv/bin/python -m pytest gateway/tests -q
```

## Documents

- `docs/pulsekv-semantic-context-design.md` — architecture, invariants, tiers
- `docs/pulsekv-semantic-context-implementation-plan.md` — phase-by-phase plan
- `docs/pulsekv-semantic-context-codebase-impact.md` — what stays frozen
- `docs/pulsekv-semantic-context-risk-register.md` — risks and their detection
- `docs/pulsekv-semantic-context-phase10.0-summary.md` — what the contract froze
- `docs/pulsekv-semantic-context-phase10.1-summary.md` — the registry, as built
- `docs/pulsekv-semantic-context-phase10.2-summary.md` — the deterministic tiers
- `docs/pulsekv-semantic-context-phase10.3-summary.md` — Tier 2, and its real latency
