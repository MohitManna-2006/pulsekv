# PulseKV v2 — Implementation Plan

**Status:** implementation plan, not yet started. Companion to
`docs/pulsekv-v2-distributed-design.md`, which explains *what* v2 is and *why*. This document
defines *how* it gets built — phase by phase, step by step, in the same spirit as v1's build
order, but at the granularity needed to actually execute it.

Does not modify v1. v1's source, tests, and docs remain untouched and complete on their own.

---

## 1. Component and language map

v2 is deliberately polyglot — each piece uses whatever is standard/optimal for that role in real
distributed systems, not one language end to end. This mirrors how systems in this space are
actually built (Consul/Nomad: Go control plane; TiKV: Rust/C++ data plane, Go placement driver;
Mooncake/LMCache/vLLM/SGLang: Python orchestration over native transfer engines).

| Component | Language | Why | Reused from v1? |
|---|---|---|---|
| **Data-plane storage node** — per-node RAM/NVMe tiered store, epoll networking, blob transfer | **C** | Direct continuity with v1's proven sharded hashtable and epoll core; this is where hand-rolled low-level performance work is the actual differentiator | Yes — extended, not rewritten |
| **Control plane** — gossip membership, Raft metadata, cluster routing table service | **Go** | Mature, battle-tested libraries for the genuinely-solved-problems part of this build (`hashicorp/memberlist` for SWIM gossip, `hashicorp/raft` or `etcd/raft` for consensus); this is the standard industry choice for this exact role | No — new |
| **LLM adapters** — SGLang HiCache backend, vLLM KVConnector | **Python** | Not a choice — vLLM and SGLang's plugin interfaces are Python classes; adapters are thin shims that call into the Go control plane and C data plane over RPC/shared memory | No — new |
| **Cross-component contract** — control-plane RPC, adapter-to-cluster calls | **gRPC / Protobuf** | Language-neutral, used to bind Go ↔ Python ↔ C boundaries cleanly | No — new (v1's binary protocol stays internal to the C data plane) |
| **Bulk KV blob transfer** | **C, custom binary framing** (chunked/streaming, zero-copy where possible) | gRPC is fine for control messages; multi-megabyte tensor blobs need a leaner, streaming, potentially shared-memory path — same reasoning SGLang's own 2026 HiCache RFC uses | Extends v1's framing discipline |

**The build-vs-borrow rule for this project:** hand-build the pieces that are the actual novel
contribution (tiered storage engine, sharding strategy tuned for KV-cache access patterns, blob
transport, the LLM adapters themselves). Use vetted libraries for pieces that are a solved,
commodity problem (gossip protocol implementation, Raft algorithm implementation). This is the
same judgment call real infra teams make, and it's a stronger engineering story than either
"hand-rolled everything, including reinventing Raft" or "glued together existing tools with no
original engine."

---

## 2. Phase dependency graph

```
Phase 0  Foundations & contracts
   |
   ├──────────────┬──────────────┐
   v              v              (parallel once contract is fixed)
Phase 1        Phase 2
Data-plane     Control-plane
node engine    routing skeleton
(C)            (Go)
   |              |
   └──────┬───────┘
          v
     Phase 3  Membership & elasticity (Go)
          v
     Phase 4  Replication (Go + C)
          v
     Phase 5  Raft metadata plane (Go)
          v
     Phase 6  Large-blob transport (C)
          v
     Phase 7  SGLang HiCache adapter (Python)  ──> first real end-to-end demo
          v
     Phase 8  vLLM KVConnector adapter (Python)
          v
     Phase 9  Distributed benchmark, tuning, hardening
```

Phases 1 and 2 are the only ones safe to parallelize, and only after Phase 0's RPC contract is
frozen — everything from Phase 3 onward depends on both the data-plane node and the control-plane
skeleton existing and speaking the same contract.

---

## 3. Phase 0 — Foundations and contracts

**Goal:** make every cross-language decision explicit *before* any component-specific code is
written, so Phases 1 and 2 don't diverge.

**Steps**

- **0.1 — Repository layout.** Decide module boundaries: e.g. `node/` (C data-plane engine),
  `control/` (Go control plane), `adapters/` (Python), `proto/` (shared gRPC/protobuf
  definitions), `deploy/` (local multi-process dev cluster scripts). A monorepo with clear
  language-scoped directories is the pragmatic default unless there's a reason to split repos.
- **0.2 — Define the gRPC/protobuf contract.** At minimum: `NodeService` (get/put/prefix-match
  against a single data-plane node), `ClusterMetadataService` (shard map lookup, membership
  snapshot), and `AdapterService` (the narrow surface the Python adapters call). Freezing this
  early is what lets Phases 1 and 2 proceed independently.
- **0.3 — Local multi-process dev cluster.** A script/Makefile target that boots N data-plane
  nodes + a control-plane instance on one machine (different ports), used as the standard dev/test
  environment for every phase from here on — the v2 equivalent of v1's Docker dev image.
- **0.4 — Decide cluster size target for design purposes.** Recommend: design and test against
  8–32 simulated nodes locally; this is enough to exercise rendezvous hashing, gossip convergence,
  and Raft leader election meaningfully without needing real multi-host infrastructure yet.

**Exit criteria:** protobuf contract merged; empty skeleton services on both Go and C sides build
and respond to a health check; local dev cluster script boots N nodes.

---

## 4. Phase 1 — Data-plane storage node (C)

**Goal:** turn v1's single-node engine into a *node* that a cluster can address — same
correctness guarantees, new capabilities (chunked values, tiered storage), speaking the frozen
gRPC contract for control messages.

**Steps**

- **1.1 — Extract the reusable node engine.** Pull v1's sharded hashtable (`hashtable.c`) and
  epoll core (`main.c`'s worker model) behind a clean internal API boundary so it can be linked
  into a "cluster node" binary distinct from the standalone v1 server. No behavior change yet —
  this is a refactor with v1's existing test suite as the regression gate.
- **1.2 — Extend framing for large values.** v1's protocol assumes values ≤ 64 KiB in one frame.
  Add chunked/streaming request and response framing so multi-megabyte KV-cache blocks can be
  written and read without violating the existing bounds-checking discipline.
- **1.3 — Tiered storage.** Add an NVMe spill tier below the in-memory shard: an eviction policy
  (start with size/age-based, leave cost-aware eviction as a documented future refinement),
  promotion back to RAM on access, and demotion under memory pressure. This is new code, not in
  v1.
- **1.4 — gRPC control surface.** Implement `NodeService` (from Phase 0.2) so the Go control
  plane can query node health, capacity, and shard assignment without going through the C data
  path.
- **1.5 — Node-level benchmark.** Extend v1's `benchmark.c` methodology (correctness-verifying,
  percentile-reporting) to a single node under the new chunked-value and tiering paths before any
  cluster-level component exists — establishes a clean baseline to compare distributed overhead
  against later.

**Tests:** unit tests for chunked framing (partial chunks, oversized/undersized, interleaved
requests — same discipline as v1's `test_multi_client`); tiering tests (promotion/demotion
correctness, eviction under memory pressure, data integrity across tier moves); a node-level
benchmark run with correctness verification, matching v1's evidence standard.

**Exit criteria:** a single data-plane node passes all new tests, degrades gracefully with a
working set larger than RAM (spills to NVMe without corruption), and responds correctly to
`NodeService` health/capacity queries.

---

## 5. Phase 2 — Control-plane routing skeleton (Go)

**Goal:** a minimal, *static* cluster router — no membership changes yet, just correct rendezvous
hashing and request routing across a fixed node list, to prove the client-to-node path end to end.

**Steps**

- **2.1 — Rendezvous hashing router.** Implement HRW hashing over a static node list; unit test
  for uniform distribution and minimal-movement-on-node-removal properties.
- **2.2 — Generic client SDK.** A small Go client (get/put/prefix-match) that resolves a key to a
  node via the router and calls that node's `NodeService` over gRPC. This is the SDK referenced
  in the design doc as the reusable "any app can use this" surface.
- **2.3 — `ClusterMetadataService` skeleton.** Serves the (currently static) node list and shard
  map; this is the seam Phase 5's Raft-backed version will later replace without changing the
  client-facing contract.
- **2.4 — End-to-end routing test.** Boot N data-plane nodes (Phase 1) + one control-plane
  instance (this phase) via the Phase 0.3 dev cluster script; run get/put through the client SDK;
  verify correct routing and correct values.

**Exit criteria:** a multi-node, statically-configured cluster correctly routes and serves
get/put/prefix-match through the client SDK, with no membership dynamism yet — this is the proof
that the cross-language contract and routing logic are both correct before adding churn.

---

## 6. Phase 3 — Membership and elasticity (Go)

**Goal:** replace the static node list with a live, self-healing membership view — nodes can join,
leave, or fail, and the shard map updates accordingly without a central coordinator or client-
visible downtime beyond the affected keys.

**Steps**

- **3.1 — Integrate gossip membership** (`hashicorp/memberlist` or equivalent SWIM
  implementation). Each control-plane/node pairing joins the gossip ring; failure detection via
  periodic probing and suspicion states.
- **3.2 — Dynamic shard map recomputation.** On membership change, recompute rendezvous-hash
  assignments and propagate the updated shard map through `ClusterMetadataService`.
- **3.3 — Chaos test harness.** Script that kills/restarts nodes under sustained client load
  (reusing the Phase 2.4 test client) and asserts: bounded key movement per membership change, no
  split-brain routing (two nodes never believe they both own the same shard), and no client-visible
  correctness failures — only expected, bounded unavailability for keys on the affected node.

**Exit criteria:** cluster survives repeated random node kill/restart cycles under load with
measured, bounded impact; chaos test suite passes reproducibly.

---

## 7. Phase 4 — Replication (Go control + C data plane)

**Goal:** tolerate node loss without losing cached data beyond acceptable staleness, using the
tunable model from the design doc — no consensus on the write path.

**Steps**

- **4.1 — Async primary + replica writes.** Control plane assigns each shard a primary and a
  configurable number of replicas (0–2 default); primary writes propagate asynchronously.
  Implemented as an extension to the `NodeService` write path (primary forwards to replicas after
  local durability, not before responding to the client — cache writes shouldn't pay replication
  latency by default).
- **4.2 — Optional stronger mode.** A per-namespace flag for quorum/chain replication for callers
  who want an ack only after replication (the "actual apps beyond LLM caching" durability path
  called out in the design doc).
- **4.3 — Fault injection tests.** Kill a replica mid-write, kill a primary and verify promotion
  of a replica, verify a client reading through the router observes bounded staleness, never
  corrupted data.

**Exit criteria:** replication factor is honored and observable; a killed primary's shard is
served correctly (possibly stale, never corrupt) by a promoted replica; quorum mode demonstrably
waits for the configured ack count.

---

## 8. Phase 5 — Raft-backed metadata plane (Go)

**Goal:** move shard ownership and cluster configuration off ad hoc gossip-derived state and onto
a small, consistent, consensus-backed source of truth — the one place in this system where real
consensus is the correct tool.

**Steps**

- **5.1 — Integrate a Raft library** (`hashicorp/raft` or `etcd/raft`) for a small metadata group
  (distinct from the data-plane nodes — this group can be 3–5 dedicated control-plane replicas).
- **5.2 — Metadata state machine.** Shard-to-node ownership, replication factor config, and
  cluster membership snapshot become Raft log entries applied to a state machine; reads served
  from the current leader (or followers with a documented staleness bound).
- **5.3 — Wire gossip and replication decisions through Raft-backed config.** Phase 3's gossip
  layer still does failure *detection*; Raft-backed metadata becomes the authoritative record of
  *shard ownership* derived from that detection, closing the loop cleanly.
- **5.4 — Chaos test: leader failover.** Kill the Raft leader under sustained membership churn;
  verify a new leader is elected, metadata remains consistent, and the data plane continues
  serving correctly throughout.

**Exit criteria:** metadata plane survives leader failure with no metadata inconsistency; shard
ownership is never ambiguous or double-assigned, even under simultaneous membership churn and
leader election.

---

## 9. Phase 6 — Large-blob transport optimization (C)

**Goal:** move multi-megabyte KV-cache blocks between nodes efficiently — this is the phase that
actually matters for LLM-serving latency, since Phase 1's chunked framing proves correctness but
not performance.

**Steps**

- **6.1 — Chunked/streaming transfer protocol** between data-plane nodes directly (not through the
  Go control plane, which stays out of the bulk-data path entirely).
- **6.2 — Zero-copy path.** `sendfile`/`splice` for the NVMe-tier-to-network case; a shared-memory
  path for co-located node/adapter processes on the same host, following the pattern from SGLang's
  2026 HiCache RFC (shared memfd region for tensor payload, control messages carry only
  keys/iovec descriptors).
- **6.3 — Benchmark.** Multi-megabyte block transfer latency and throughput, node-to-node and
  node-to-adapter, measured before and after 6.2's zero-copy path — same "measure, change,
  remeasure" discipline v1 used in its Step 8.

**Exit criteria:** measured, documented improvement from the zero-copy path over the naive chunked
path; no correctness regression versus Phase 1's chunked-transfer tests.

---

## 10. Phase 7 — SGLang HiCache adapter (Python)

**Goal:** the first real, live integration — this is the milestone that proves the whole system
against an actual inference engine, not a synthetic client.

**Steps**

- **7.1 — Thin backend class.** Implement SGLang's external storage backend interface
  (`get`, `exist`, `set`) as a Python class that calls the Go client SDK (via a Python gRPC
  client) for control/routing and the C transport (Phase 6) for bulk data.
- **7.2 — Key scheme alignment.** Match SGLang's own block-hash key derivation (content hash of
  token sequence, chained to the previous block) so identical prefixes from different requests —
  and different SGLang replicas — resolve to the same cluster key.
- **7.3 — Integration test against a real local SGLang server**, using the Phase 0.3 dev cluster
  as the backend.
- **7.4 — Demo.** Two or more SGLang replicas sharing the cluster; a prefix computed by one
  replica produces a verified cache hit when served by a different replica.

**Exit criteria:** the Phase 7.4 demo reproduces reliably — this is the design doc's primary
success criterion, and the first point at which the project is a working answer to the stated
problem rather than infrastructure-in-waiting.

---

## 11. Phase 8 — vLLM KVConnector adapter (Python)

**Goal:** the harder, lower-level integration — proves the system works against the interface with
tighter coupling to the inference engine's own scheduling and memory management.

**Steps**

- **8.1 — Scheduler-side connector.** Implement `get_num_new_matched_tokens` and related
  scheduler-side hooks to advertise which prefix blocks are available in the cluster, returning a
  bitmask vLLM can use for admission decisions.
- **8.2 — Worker-side connector.** Implement `save_kv_layer` and the corresponding load hooks,
  invoked per transformer layer during the forward pass — this is the tightest-latency part of the
  integration and needs the Phase 6 transport path, not the control-plane gRPC path, for actual KV
  tensor movement.
- **8.3 — `request_finished` handling.** Correctly implement the handoff of block-freeing
  responsibility so the cluster and vLLM's own memory manager never disagree about block
  ownership.
- **8.4 — Integration test against a real vLLM deployment**, same cross-replica cache-hit demo as
  Phase 7.4.

**Exit criteria:** equivalent demo to Phase 7.4, inside vLLM; no block-ownership races between
vLLM's scheduler and the cluster under concurrent requests.

---

## 12. Phase 9 — Distributed benchmark, tuning, and operational hardening

**Goal:** apply v1's Step-8 discipline at cluster scale — measure honestly, optimize only what the
data supports, report scorecards rather than hiding misses.

**Steps**

- **9.1 — Multi-node, correctness-verifying load generator.** The distributed equivalent of v1's
  500-client epoll benchmark: drives realistic LLM-serving-shaped traffic (skewed hot prefixes,
  large value sizes, mixed read/write) across multiple simulated inference replicas.
- **9.2 — Measured optimization loop.** Same rule as v1: only keep a change the benchmark actually
  supports.
- **9.3 — Observability.** Metrics (Prometheus-style) for cache hit rate, replication lag, gossip
  convergence time, Raft leader stability, tier promotion/demotion rates, and per-phase latency
  breakdown (control-plane routing vs. data transfer).
- **9.4 — Long-duration soak and fault testing.** Multi-hour mixed workload; repeated random node
  kills; disk-full and NVMe-tier-failure injection; verify no slow leak in correctness or
  performance over time.
- **9.5 — Final scorecard report.** A `docs/pulsekv-v2-progress-report.md`, written in the same
  evidence-first style as v1's progress report — claims backed by specific test/benchmark results,
  gaps stated honestly rather than implied away.

**Exit criteria:** the design doc's three success criteria (cross-replica cache hit demo, bounded-
impact node-failure survival, honest benchmark scorecard) are all met with recorded evidence.

---

## 13. What "done" looks like

v2 is complete when:

1. A live SGLang (Phase 7) and a live vLLM (Phase 8) deployment can both attach to the same
   cluster and demonstrably share cached prefixes across replicas.
2. The cluster survives node loss, replica loss, and metadata-leader loss under load with bounded,
   measured impact and no correctness violations (chaos suites from Phases 3, 4, and 5 all green).
3. A multi-node benchmark (Phase 9) reports honest throughput/latency numbers against explicit
   targets, the same way v1 did — met targets called out as met, missed targets called out as
   missed.
4. The generic client SDK (Phase 2.2) works as a standalone cache/store for a non-LLM toy
   application, proving Layer 1's reusability claim isn't just asserted in the design doc.

---

## 14. Suggested sequencing note for a solo build

Given this is most realistically built incrementally by one person (with implementation handed to
Claude Code phase by phase): treat each phase above as its own session/branch with its own test
gate before moving on, exactly like v1's commit history (one milestone commit per step). Phases 1
and 2 are the only safe parallelization point; everything else is a true dependency chain. Phase 7
(SGLang) is the single highest-leverage milestone to reach first among Phases 7–8, since it has the
simplest external interface — treat reaching a working Phase 7.4 demo as the "MVP complete" marker
for the whole project, with Phase 8 (vLLM) and Phase 9 (hardening) as the extension that turns an
MVP into the flagship version.
