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

## Status: Phase 10.5 — working fail-open ingress gateway

The package is now an OpenAI-compatible reverse proxy. It accepts
`POST /v1/chat/completions`, decomposes eligible blocks, runs the complete
four-tier matcher, substitutes accepted canonical text in the original
position, and streams the downstream response. A miss, rejection, timeout,
unsafe assembly, or registry/encoder/guard failure forwards the original
request body byte-for-byte.

| Module | Phase | What it becomes |
|---|---|---|
| `models.py` | **10.0 (done)** | Registry record, `MatchResult`, decision-log record, block taxonomy |
| `registry.py` | **10.1 (done)** | Durable, versioned, namespace-scoped storage (SQLite, WAL) |
| `normalizer.py`, `decomposer.py`, `matcher.py`, `auditlog.py` | **10.2 (done)** | Tier 0/1 and the decision log |
| `encoder.py`, `index.py` | **10.3 (done)** | Tier 2 candidate retrieval (MiniLM-L6-v2, ONNX CPU) |
| `guardrail.py` | **10.4 (done)** | Tier 3 equivalence guard, τ = 0.90, and the corpus that earned it |
| `server.py`, `assembler.py`, `config.py` | **10.5 (done)** | Strict config, order-preserving assembly, and the proxy process |

### Running the gateway

Install it and copy the documented configuration:

```bash
python -m venv .venv
.venv/bin/pip install -e './gateway[dev]'
cp gateway/gateway.example.yaml /tmp/pulsekv-gateway.yaml
.venv/bin/pulsekv-gateway --config /tmp/pulsekv-gateway.yaml
```

The example uses a trusted `x-pulsekv-namespace` header. Static,
API-key-to-namespace, and exact-route-to-namespace modes are also validated
config shapes. Namespace input comes only from deployment/authentication
state, never from prompt text. The trusted namespace header is removed before
forwarding; inference authorization and unrelated headers are retained.

```bash
curl http://127.0.0.1:8088/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'x-pulsekv-namespace: acme' \
  -d '{"model":"local-model","messages":[{"role":"system","content":"registered alias or paraphrase"},{"role":"user","content":"hello"}]}'
```

`GET /healthz` reports process and semantic-tier state without configuration
secrets. `GET /readyz` verifies that the registry remains readable. The
gateway chooses design option E (inline reverse proxy); applications or the
load balancer must retain `upstream_url` as a direct fallback route. That
route is intentionally independent of this process, because no in-process
fail-open handler can help when the process itself is unreachable.

The default eligible-token bypass uses a coarse character-count estimate only
to avoid work; it never participates in identity or acceptance. The 512-token
default and timeout budgets remain explicitly provisional until Phase 10.8.

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

### Tier 3, and what similarity is actually for

The guard runs three deterministic checks over the **full text** of the block
and of the candidate — never the embedded prefix, because the encoder stops
reading at 512 tokens and the blocks this feature targets are longer than that
by construction. Block type first, then polarity (negation, exception, order,
comparison, permission and obligation terms, compared as a multiset of
families), then entities (numbers, flags, identifiers, environment names,
proper nouns, compared as a case-sensitive set). Any mismatch, any error, any
timeout is a reject; there is no reduced-confidence accept.

Only a candidate the guard has passed is compared against τ = 0.90. That order
is deliberate. Measured across the 38 pairs in `tests/corpus/`, genuine
paraphrases span 0.8462–1.0000 and adversarial pairs 0.1333–1.0000 — and 17 of
the 24 scored adversarial pairs sit at or above the *lowest* genuine
paraphrase. Three pairs score exactly 1.0000 with byte-identical vectors: two
adversarial and one a real paraphrase. **No value of τ separates meaning from
its opposite.** The deterministic checks do that; τ only refuses a candidate
that is the nearest neighbour of nothing in particular.

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

Set the fetched directory as `model_dir` in the gateway YAML. Without it,
Tier 2 does not run and Tiers 0/1 keep working. If a configured encoder cannot
load or times out, the same fail-open behavior applies. The Tier 2 tests use
`PULSEKV_GATEWAY_MODEL_DIR` and skip themselves when the weights are absent.

## The invariant everything else serves

A missed match costs a prefill recompute — the system's normal, correct
behavior today. A false match silently changes what the model was asked. Every
tier, guard, timeout and error path in this package is biased toward the first
(`docs/pulsekv-semantic-context-design.md` §4, §7, §12).

## Layout

```
gateway/
├── pyproject.toml          # pinned runtime and test dependencies
├── gateway.example.yaml    # complete operator-facing example
├── pulsekv_gateway/        # the package
│   └── migrations/         # the registry's schema, applied on open
└── tests/
    ├── test_models.py               # contract tests
    ├── test_registry.py             # storage tests
    ├── test_deterministic_tiers.py  # tier 0/1 and the decision log
    ├── test_semantic_retrieval.py   # tier 2: encoder, index, retrieval seam
    ├── test_guardrail.py            # tier 3: the guard, and the corpus run
    ├── test_gateway.py              # config, assembly, proxy, failure paths
    ├── corpus_loader.py             # turns a corpus file into a live matcher
    └── corpus/             # 44 evaluation examples, four categories
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
- `docs/pulsekv-semantic-context-phase10.4-summary.md` — Tier 3, τ, and the corpus
- `docs/pulsekv-semantic-context-phase10.5-summary.md` — proxy, failure evidence, and `T_gateway`
