# PulseKV semantic context — current progress

**Status snapshot:** Phase 10.6 complete; Phase 10.7 next and not started.

**Authority:** current merged implementation/tests outrank this index; each
summary is the phase-specific evidence record. The Phase 10.6 summary is a
post-hoc reconstruction and says so explicitly.

| Phase | Status | Main deliverable | Evidence | Next dependency |
|---|---|---|---|---|
| 10.0 | COMPLETE | Contracts and corpus skeleton | `pulsekv-semantic-context-phase10.0-summary.md` | Registry |
| 10.1 | COMPLETE | Durable namespace/version registry | `pulsekv-semantic-context-phase10.1-summary.md` | Deterministic tiers |
| 10.2 | COMPLETE | Exact/alias/structural tiers and audit log | `pulsekv-semantic-context-phase10.2-summary.md` | Semantic retrieval |
| 10.3 | COMPLETE | Embedding candidate retrieval | `pulsekv-semantic-context-phase10.3-summary.md` | Guardrails/corpus |
| 10.4 | COMPLETE | Equivalence guard and measured corpus | `pulsekv-semantic-context-phase10.4-summary.md` | Gateway |
| 10.5 | COMPLETE | Fail-open OpenAI-compatible gateway | `pulsekv-semantic-context-phase10.5-summary.md` | Real-engine integration |
| 10.6 | COMPLETE | Real SGLang 0.5.15 compatibility and cross-replica proof | `f8da035`, PR #3; post-hoc `pulsekv-semantic-context-phase10.6-summary.md` | vLLM compatibility audit |
| 10.7 | NEXT / NOT STARTED | Pinned real-vLLM semantic proof | None yet | Select/audit a real vLLM version before changes |
| 10.8 | PLANNED | Three-way performance/economics benchmark | None yet | Real SGLang and vLLM paths |
| 10.9 | PLANNED | Hardening, tenant isolation and soak | None yet | Benchmark and both engines |

Phase status distinguishes implementation from later validation: Phase 10.6
proves one pinned real SGLang path, not all engine versions or net production
performance. Phase 10.8 remains the authority for the economic question.
