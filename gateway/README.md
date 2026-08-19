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

## Status: Phase 10.0 — contract only

This package currently contains **no runtime behavior**. `models.py` is the
frozen contract; every other module is a signature-only stub whose bodies raise
`NotImplementedError` with the phase that fills them in.

| Module | Phase | What it becomes |
|---|---|---|
| `models.py` | **10.0 (done)** | Registry record, `MatchResult`, decision-log record, block taxonomy |
| `registry.py` | 10.1 | Durable, versioned, namespace-scoped storage |
| `normalizer.py`, `decomposer.py`, `matcher.py`, `auditlog.py` | 10.2 | Tier 0/1 and the decision log |
| `encoder.py`, `index.py` | 10.3 | Tier 2 candidate retrieval |
| `guardrail.py` | 10.4 | Tier 3 equivalence guard, and the τ threshold it earns |
| `server.py`, `assembler.py`, `config.py` | 10.5 | The actual proxy process |

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
└── tests/
    ├── test_models.py      # contract tests
    └── corpus/             # evaluation corpus, populated in Phase 10.4
```

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
- `docs/pulsekv-semantic-context-phase10.0-summary.md` — what this phase froze
