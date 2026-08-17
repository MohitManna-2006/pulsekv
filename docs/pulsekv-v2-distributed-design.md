# PulseKV v2 — Distributed LLM KV-Cache Layer

**Status:** design proposal, not yet implemented. Does not modify or replace the v1 single-node
store documented in `docs/system-design.md`, `docs/architecture-guide.md`, and
`docs/project-progress-report.md`. This document defines what v2 is, why it is a distinct
architecture rather than an incremental patch, and what order to build it in.

---

## 1. Why v2 exists

v1 proved out a correct, fast, single-node epoll/WAL key-value engine. It does not exercise
distributed systems (no replication, no consensus, no partitioning) and it targets a well-known
problem category (a smaller single-node Redis). v2 keeps the parts of v1 worth keeping — the
sharded-table concurrency model, the epoll networking core, the binary framing discipline — and
retargets the project at two things at once:

1. **A real distributed systems build**: consensus for metadata, gossip membership, replication,
   consistent hashing, dynamic rebalancing — the systems that were missing from v1.
2. **A real, currently-unsolved-at-scale infrastructure problem**: LLM inference KV-cache
   management. Recomputing attention KV state per request is expensive; keeping it resident only
   in GPU HBM does not scale across a fleet; sharing cached prompt prefixes across replicas is
   where inference cost actually goes down. This is an active, live problem — not a hypothetical
   one.

### Confirming this is a real, current problem (not a stale assumption)

As of 2026, multiple production and research systems exist specifically to solve this:

- **Mooncake** (Best Paper, FAST 2025): a distributed KV-cache system built around a "Transfer
  Engine," now integrated into vLLM v1 and TensorRT-LLM for disaggregated prefill/decode.
  [Mooncake](https://kvcache-ai.github.io/Mooncake/)
- **LMCache**: a KV-cache engine that plugs into vLLM, SGLang, and NVIDIA Dynamo, with pluggable
  storage backends spanning CPU RAM, local NVMe, Redis/Valkey, Mooncake, S3-compatible object
  storage, and GPU-direct storage. [LMCache](https://github.com/lmcache/lmcache) ·
  [tech report](https://lmcache.ai/tech_report.pdf)
- **NVIDIA Dynamo / NIXL**: Dynamo 1.0 uses NIXL as its KV transport layer between disaggregated
  prefill and decode workers; LMCache and Dynamo share the same NIXL primitives.
- **SGLang HiCache**: a hierarchical KV-cache system with a genuinely simple external backend
  interface (`get`, `exist`, `set`), supporting Mooncake, 3FS, GDS, and S3-compatible backends,
  with a 2026 RFC proposing a vendor-neutral RPC connector (standalone storage server process,
  zero-copy shared-memory data plane, gRPC-over-Unix-socket control plane).
  [HiCache blog](https://www.lmsys.org/blog/2025-09-10-sglang-hicache/) ·
  [HiCache RFC](https://github.com/sgl-project/sglang/issues/24542)
- **vLLM KVConnector v1**: a scheduler-side/worker-side split interface — scheduler-side methods
  report how many tokens' worth of KV are available externally
  (`get_num_new_matched_tokens`), worker-side methods load/save KV per layer
  (`save_kv_layer`), and `request_finished` hands off (or retains) responsibility for freeing
  blocks. [vLLM KVConnector v1 docs](https://docs.vllm.ai/en/stable/api/vllm/distributed/kv_transfer/kv_connector/v1/)

The conclusion: this is not a niche or invented problem. It is the subject of active RFCs,
dedicated open-source projects, and hardware-vendor investment (NVIDIA's ICMSP architecture,
announced CES 2026, targets the same three-tier GPU-HBM/CPU-DRAM/NVMe hierarchy this design uses).
Building a credible version of this system is a legitimate, timely engineering statement.

---

## 2. Goals and non-goals

**Goals**

- A distributed, horizontally scalable KV-cache cluster that any application can use as a
  general-purpose fast cache/store (not LLM-specific at the core).
- A concrete, working adapter that plugs into a real inference engine (SGLang first, vLLM second)
  using their actual external-cache interfaces, so it can be demoed end-to-end against a live
  model server.
- Real distributed-systems mechanics: gossip-based membership, consistent-hash sharding,
  replication with tunable durability, and a small consensus-backed metadata plane — not just a
  bigger single-node store.
- Reuse what v1 already proved: sharded in-memory table design, epoll event loop discipline,
  binary framing rigor, disciplined testing (unit, concurrency, fault-injection, benchmark).

**Non-goals (explicitly out of scope for v2)**

- General-purpose OLTP/ACID database semantics. This is a cache/state layer, not a system of
  record.
- Full Raft-replicated durability for every cache write. Cache data is allowed to be lossy —
  worst case is a recompute, not data loss of record.
- Rewriting v1. v1 remains a standalone, complete, documented project. v2 is a new system that
  borrows components, not a migration.

---

## 3. Two-layer architecture

```
                 ┌─────────────────────────────────────────────┐
                 │            Layer 2: LLM adapters             │
                 │  SGLang HiCache backend  |  vLLM KVConnector  │
                 │  (block-hash-keyed get/exist/set/save/load)   │
                 └───────────────────┬───────────────────────────┘
                                      │ generic client SDK (get/put/prefix-match)
                 ┌───────────────────▼───────────────────────────┐
                 │         Layer 1: distributed KV engine          │
                 │  rendezvous-hash sharding · gossip membership   │
                 │  chain/async replication · tiered node storage  │
                 │  Raft-backed metadata plane (shard ownership,    │
                 │  cluster config) — NOT on the data write path   │
                 └───────────────────────────────────────────────┘
```

Layer 1 is reusable on its own — any application gets a horizontally scalable, replicated,
tiered KV cache from it. Layer 2 is what makes the project point at the 2026 problem directly:
it's the thing that lets an actual vLLM or SGLang deployment attach to the cluster and get cache
hits across replicas instead of recomputing attention state per request.

---

## 4. Layer 1 — distributed KV engine

### 4.1 Sharding: rendezvous hashing (HRW)

Chosen over a hash ring or Jump Hash because inference fleets scale elastically (nodes join and
leave under autoscaling, not just append/remove-from-the-end), and rendezvous hashing handles
arbitrary node removal/addition with minimal key movement, no virtual-node bookkeeping, and a
simple, stateless "highest random weight wins" placement rule. Cost is O(n) hash evaluations per
lookup against the current node set, which is acceptable at cluster sizes in the tens-to-low
hundreds of nodes; Maglev-style precomputed lookup tables are a documented future optimization if
lookup cost becomes measurable at larger scale.

### 4.2 Membership: gossip (SWIM-style)

Nodes exchange periodic heartbeats and suspicion state peer-to-peer rather than through a central
registry. New nodes are discovered and failed nodes are evicted from the shard map without a
single point of failure or a control-plane round trip on the request path. This is the piece v1
had zero of, and it's what makes the cluster elastic rather than statically configured.

### 4.3 Replication: tunable, off the Raft path

Cache writes are high-volume and loss-tolerant, so full consensus per write would be the wrong
trade-off — it's the mistake this design deliberately avoids. Instead:

- Default: primary write + async replication to a configurable number of replicas (0, 1, or 2).
  A replica lag or a lost replica degrades hit rate, not correctness — a miss just means
  recompute.
- Optional stronger mode per key-namespace: quorum/chain replication for callers that want an ack
  only after replication, for use cases beyond pure inference caching (e.g. session state) where
  losing a write matters more.
- **Raft is reserved for the metadata plane only**: shard ownership, cluster membership snapshot,
  replication-factor config. This is a low-volume, latency-insensitive write path — exactly where
  consensus belongs, and exactly how systems like etcd, TiKV, and CockroachDB separate their
  metadata and data planes in practice.

### 4.4 Storage tiers per node

```
node-local RAM (hottest)  →  node-local NVMe (warm)  →  fetch from peer node (cold)  →  miss
```

This is the same three-tier shape used by Mooncake, LMCache, and NVIDIA's ICMSP hardware
architecture — GPU HBM sits above this system, inside the inference engine itself; PulseKV v2
owns everything below it.

### 4.5 Transport

v1's framing assumed small values (max 64 KiB) and a single-shot buffer copy per request. KV-cache
blocks are large (megabytes per prefix chunk), so v2's wire path needs chunked/streaming transfer
and should avoid full userspace copies where possible (`sendfile`/`splice` first; `io_uring` or
RDMA-class transport as a later optimization once the simpler path is measured). The 2026 SGLang
HiCache RFC's approach — a standalone storage-server process, KV tensor payload delivered via a
zero-copy shared-memory region, and only keys/iovec descriptors going over a control-plane RPC —
is a strong reference pattern worth following directly.

### 4.6 Generic client SDK

A minimal `get(key) / put(key, value) / prefix_match(prefix)` API, independent of any LLM
semantics. This is what lets Layer 1 be reused for things that have nothing to do with inference —
session storage, a feature-store cache, generic app-level caching — which is the "easily
replicate and use for actual apps" requirement.

---

## 5. Layer 2 — LLM serving adapters

### 5.1 SGLang HiCache backend (build first)

SGLang's external storage backend interface is deliberately small: `get(key)`, `exist(key)`,
`set(key, value)`. This is the fastest path to a real, working integration — implement those
three methods against the Layer 1 client SDK, register as a HiCache storage backend, and prefix
cache hits/misses can be demonstrated against an actual running SGLang server. Recommended first
target for the "real integration" milestone.

### 5.2 vLLM KVConnector v1 (build second)

Materially more involved: it requires a scheduler-side component (`get_num_new_matched_tokens` to
advertise how much of a request's prefix is already cached, returning a bitmask of available
blocks) and a worker-side component (`save_kv_layer` / load hooks invoked per transformer layer
during the forward pass), plus correct handling of `request_finished` block-freeing semantics.
This is a heavier, lower-level integration than HiCache's three-method interface and should follow
once the SGLang path is proven, reusing the same Layer 1 client underneath.

### 5.3 Key scheme

Cache keys must be block-hash-addressable in a way that lines up with how vLLM/SGLang themselves
hash prefix blocks (content hash of token sequence, chained with the previous block's hash) so
that identical prompt prefixes from different requests — and different replicas — resolve to the
same Layer-1 key regardless of which node originally computed them.

---

## 6. What's reused from v1 vs. net-new

| Component | v1 | v2 |
|---|---|---|
| Sharded in-memory table | Single-node, 256 shards | Reused per-node as the RAM tier |
| epoll event loop / thread-per-core | Reused directly | Reused directly for cluster-internal RPC |
| Binary framing discipline | Small fixed values | Extended for chunked/streaming large blobs |
| WAL codec/durability | Reused conceptually | Repurposed narrowly, only for optional durable-mode replication, not the default cache path |
| Consistent hashing | Absent | New |
| Gossip membership | Absent | New |
| Replication (chain/quorum) | Absent | New |
| Raft metadata plane | Absent | New |
| Storage tiering (NVMe/remote) | Absent | New |
| LLM adapter (HiCache/KVConnector) | Absent | New |

---

## 7. Build order

Mirrors v1's philosophy — each phase removes one concrete limitation from the last, always
correctness-first, always benchmark-verified before calling a phase done.

1. **Cluster skeleton**: static node list, rendezvous-hash routing, generic get/put client SDK
   talking to per-node in-memory stores over the existing epoll/binary-framing core (no
   replication, no membership changes yet — proves routing correctness).
2. **Gossip membership**: dynamic join/leave/failure detection; shard map recomputed on
   membership change; verify via a chaos test (kill/restart nodes under load, confirm bounded key
   movement and no split-brain routing).
3. **Replication**: async primary+replica writes with configurable factor; verify data survives a
   single-node kill without client-visible data loss beyond expected staleness.
4. **Raft metadata plane**: move shard ownership/config out of static gossip-derived state into a
   small Raft-backed group; verify metadata consistency under concurrent membership churn.
5. **Storage tiering**: add NVMe spill for cold entries per node; verify tier promotion/demotion
   under memory pressure with a working set larger than RAM.
6. **Large-blob transport**: replace small-value framing assumptions with chunked/streaming
   transfer; benchmark multi-megabyte block transfer latency node-to-node.
7. **SGLang HiCache adapter**: implement `get/exist/set` against the client SDK; demonstrate real
   prefix cache hits across two or more SGLang replicas sharing the cluster.
8. **vLLM KVConnector adapter**: scheduler-side + worker-side integration; demonstrate the same
   cross-replica cache-hit behavior inside a real vLLM deployment.
9. **Benchmark and tune**: reproduce v1's discipline — a load-generator-driven benchmark
   (multi-node this time), correctness verification on every response, measured hot-path changes
   only, honest scorecards for any target not fully met.

---

## 8. Open questions to resolve before implementation starts

- **Cluster size target for design/testing** — tens of nodes vs. hundreds changes whether plain
  rendezvous hashing is sufficient or a Maglev-style lookup table is needed sooner.
- **Where does the Raft implementation come from** — hand-rolled (more "aura," more time, more
  risk of subtle bugs) vs. a vetted library (faster to a correct metadata plane, less of the from-
  scratch story). Given v1's from-scratch ethos, hand-rolling is the more consistent choice, but
  it's a real multi-week sub-project on its own and should be scoped explicitly.
- **Language/runtime for the cluster-internal RPC layer** — staying in C keeps continuity with v1
  but means hand-building gossip, chunked transport, and a Raft implementation with zero
  dependencies; a companion Rust or Go component (common in this ecosystem — e.g. distributed
  systems primitives) is worth an explicit discussion rather than a default assumption.
- **Target durability tier for the "actual apps" use case** — if this cluster is also meant to
  back real application state (not just LLM cache), the async-replication default needs a
  documented, tested path to the stronger quorum mode, not just a mention.

---

## 9. Success criteria

- A live SGLang server, backed by this cluster, demonstrably reuses cached prefixes across two or
  more replicas — a prefix computed by one instance produces a cache hit when a different instance
  serves a request sharing that prefix.
- The cluster survives a node kill under load with bounded, measurable impact (no split-brain, no
  unbounded key movement, replication factor honored).
- Benchmark results follow v1's standard: every response verified for correctness, throughput and
  latency reported honestly against stated targets, no target hidden behind a passing test.
