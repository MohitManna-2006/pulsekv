# PulseKV v3 / Phase 10.1 — Canonical Context Registry

**Status:** complete. The first phase of Phase 10 that produces runtime
behavior: durable, versioned, namespace-scoped storage for canonical contexts,
built to Phase 10.0's frozen types without changing them.

**Scope actually touched:** `gateway/` only. `src/`, `include/`, `tests/`,
`node/`, `control/`, `proto/` and `adapters/` are untouched — verified in §8,
not asserted. `gateway/tests/corpus/` is untouched: that corpus is Phase 10.4's
to curate.

---

## 1. The §3 gate — a second explicit waiver, for Phase 10.1

Implementation plan §3 declares the soak-collapse gate **RESOLVED**, and the
substance of that resolution is on disk and checkable: the root cause (an
unlocked read-modify-write on `deploy/`'s pid registry), eight fixes, and
`deploy/test-lifecycle.sh`, whose 8 checks cover each link in the chain. What is
*not* yet on disk is the re-verification that plan §3 and
`pulsekv-v2-soak-collapse-analysis.md` §10 both describe as done.

Checked against the repository before any file was created (2026-08-19, 14:34 UTC):

| What the documents say | What is on disk |
|---|---|
| Plan §3: "A fresh long-duration soak is recorded in `pulsekv-v2-progress-report.md` §4.2 with its artifact preserved" | §4.2 still holds only the Phase 9.4 figures (5,390 ops/s, 182,312 verified) — the same numbers the analysis doc §9 flags as unreconciled. No fresh soak is recorded there |
| Analysis doc §10, "The fresh long-duration soak" | An unfilled placeholder: the heading, then `<!-- FRESH_SOAK_RESULTS -->`, then nothing |
| The 90-minute confirmation soak, relaunched 03:14:49 UTC on the fixed `deploy/` code | **Still running.** `deploy/run/logs/soak-chaos.log` line 1 is `[2026-08-19T03:14:50Z] [injector] started`. At 14:34 UTC its load generator's last reporting row was `1800s` against a declared `sustained 1h30m0s` (30 of 90 minutes); by 14:58 UTC it was `3180s` (53 of 90) |
| A verdict from `deploy/soak-verdict.py` | None exists. The only `soak-report.json` on disk is `deploy/run/logs-incident-2026-08-19/soak-report.json`, the preserved *pre-fix* artifact (180.02s, `verdict: null`, predating the verdict script) |

**Resolution: explicitly waived for Phase 10.1, by the repository owner (Mohit
Manna), in the implementation session of Aug 19, 2026**, on the same grounds
Phase 10.0's waiver rested on and which apply unchanged here: this phase
produces a pure-Python storage module with no dependency on cluster runtime
behavior. Phase 10.0's summary §7 anticipated exactly this decision — "Resolve
it — or take a second, separate, conscious waiver — before beginning 10.1." The
waiver was requested and granted before any file was created.

**In-flight signals from the running soak**, recorded because they are evidence
and burying them would repeat the failure that made the gate necessary. These
are observations, not a verdict — the run has not produced one:

- **0 dead windows** across 53 reporting intervals. Every interval served
  verified reads. `soak-verdict.py`'s `dead_windows()` — operations attempted,
  zero verified — is the exact signature that ran for 75 minutes in the
  incident, and it has not appeared.
- **Exactly one injector.** `grep '[injector] started'` returns one line. The
  incident run had three interleaving on one cluster; the soak-singleton guard
  and the self-terminating named injector are holding.
- **~3.27% error rate** (316,732 errors over ~9.7M attempted operations)
  against `soak-verdict.py`'s 50% ceiling, clustered around crash/restart
  cycles as chaos would predict.
- **Nodes come back.** 24+ crash/restart cycles, with `cluster.pids` showing
  every replica and node live. "Once a node was down it could not be brought
  back" — link 3 of the original chain — is not recurring.

**One fact the next session needs: this run's wall clock and its own clock
disagree, and by a lot.** At 14:34 UTC, 11h20m had elapsed since launch but the
load generator reported only 1800s of its own measured time — roughly 22x
slower than real time. Over the following 23 minutes it advanced 1380s, i.e.
back to real-time pace. The cluster runs inside a Linux VM (the pids in
`deploy/run/cluster.pids` are VM pids, invisible to the host's `ps`), so the
most likely reading is that the VM was suspended for long stretches and has
since resumed, freezing both the benchmark's monotonic clock and the injector's
sleep pacing while wall clock ran on. **That is a reading, not a diagnosis** —
it was not investigated, and it is Phase 9.x work rather than Phase 10 scope.

Two consequences worth carrying forward. First, "how long until the soak
finishes" cannot be answered from wall clock: at real-time pace the remaining
~37 minutes finishes soon, but another suspension makes that arbitrarily long,
so the gate should not be assumed to close on its own. Second, a 90-minute soak
that takes 11+ hours of wall clock has been fault-injected across a *far* longer
real interval than its parameters describe — which strengthens rather than
weakens what it has shown so far, but means its final report's duration figure
will not describe how long the cluster was actually under chaos.

---

## 2. Step 10.1.1 — the storage backend, and why

**Chosen: SQLite**, one file, `sqlite3` from the standard library.

Two reasons, stated rather than defaulted to:

1. **The invariants this phase must prove are constraints, and a constraint
   engine is what enforces constraints.** Version immutability and namespace
   isolation are the two things plan §5 requires proven *below* the type
   system. In SQL they are three triggers and a compound primary key —
   enforced even against a caller holding a raw connection that never imports
   `pulsekv_gateway`. In a JSON/NDJSON append store they would be Python code
   a future writer can forget, which is precisely the backdoor this phase's
   scope boundary forbids. An embedded KV library has the same gap and adds a
   dependency to close it.
2. **Zero new infrastructure and zero new dependencies.** There is no service
   to run beside the gateway, and because `sqlite3` ships with Python,
   `pyproject.toml`'s deliberate "no database driver" property (Phase 10.0's
   exit criterion 5) survives this phase literally intact — the wheel built in
   §8 still declares `pydantic==2.13.4` and nothing else. Design doc §10
   recommends "Postgres, or SQLite for the MVP's realistic scale" against an
   estimated low-hundreds-to-low-thousands of curated entries; plan §1 records
   the intended shape as "SQLite MVP, Postgres-ready".

Design doc §10 rules out exactly one candidate, and this phase respects it:
**not PulseKV itself.** `node/engine/README.md` states its NVMe tier is
loss-tolerant by design; a registry entry is not, because losing one and having
it come back *different* would break the "version 4 always means this text"
claim that both the exact tier and the audit trail rest on.

**On plan §5's parenthetical** — "a real SQL store from the start ... over
SQLite-then-migrate, to avoid a schema-migration exercise mid-project". SQLite
*is* a real SQL store; what that sentence guards against is an ad-hoc store with
no schema to carry forward. `migrations/` is a real, ordered, ledgered
directory and `001_initial.sql` is ordinary portable DDL, so the Postgres path
is a second migration dialect and a sibling class — not a rewrite of how records
are shaped. The only constructs that would need translating are the trigger
bodies; the partial UNIQUE index Postgres spells identically.

---

## 3. Exact layout produced

```
gateway/
├── pyproject.toml                      # +8 lines: package-data for the schema
├── README.md                           # status table, layout, one paragraph on the store
├── pulsekv_gateway/
│   ├── registry.py           (935)     # REAL — was a 127-line stub
│   └── migrations/
│       └── 001_initial.sql   (181)     # the schema, and where the invariants live
└── tests/
    ├── test_models.py                  # -1 entry in STUB_MODULES, +5 lines of comment
    └── test_registry.py      (782)     # 58 tests
```

Everything else in `pulsekv_gateway/` is unchanged, including `models.py` — the
frozen contract needed no amendment to build storage around it, which is the
outcome plan §4's handoff predicted.

---

## 4. Step 10.1.2 — the interface

Every method the stub declared kept its name and signature. Two were added
(`list_records`, `from_dsn`), one module function was added
(`content_hash_for`), and one method deliberately still raises.

| Method | Behavior |
|---|---|
| `register(record) -> str` | Stores a version. Points the context at it **only if the context is new** |
| `publish_version(record) -> Record` | Stores a version **and** moves the current-version pointer. Returns the record read back from the store, not the one passed in |
| `get(context_id, namespace, version=None) -> Record` | One version, or the current one. Returns deprecated records — this is the audit read |
| `by_content_hash(hash, namespace) -> Optional[Record]` | Tier 0's exact-hash path. Live records only |
| `resolve_alias(text, namespace) -> Optional[Record]` | Tier 0's alias path. Resolves through the current-version pointer, live only |
| `list_records(*, namespace, block_type=None, include_deprecated=False, current_only=False, limit=None, offset=0)` | The scan Phase 10.2/10.3 enter through. `namespace` is keyword-only with no default |
| `deprecate(context_id, namespace, version, at) -> Record` | The one legal mutation, and one-way |
| `find_candidates(...)` | Still `NotImplementedError("Phase 10.3")` |
| `close()` | Closes every connection on every thread. Idempotent |

**Why `register` and `publish_version` differ in exactly one thing.** Design
doc §10's "immutable versions, mutable pointer" cannot be a field on the record
(Phase 10.0 summary §7.2 says so: publishing v5 would have to rewrite v4's row).
It is the `current_version` table, and it is the only mutable state this phase
owns. `register` sets it only for a context's first version, because otherwise
the context would have no current version and `get(context_id, namespace)` could
not answer at all. Registering a later version without publishing it is
therefore a **staged version**: durable, addressable by explicit version number,
and not yet what the context resolves to.

**"Update" is additive-only, as the frozen type already models it.** There is no
update method. A new version is a new row; the pointer moves. Confirmed against
`models.py` before implementing, per the prompt's instruction not to assume a
shape the contract does not have.

**Delete/deactivate: soft only, because that is what the type has.**
`CanonicalContextRecord` carries `deprecated_at`, `is_deprecated` and a
`deprecate(at)` method that returns a *new* record; there is no active flag and
no hard-delete concept, so none was added. `deprecate()` builds the new record
by calling the contract's own `deprecate()`, so the rule that `deprecated_at`
must not precede `created_at` is applied by the contract rather than
re-implemented — and a violation surfaces as `RegistryConflictError`, not as a
`ValueError` leaking out of the storage layer.

**The current-version pointer is left alone when the current version is
deprecated.** Moving it back to an older version would be the gateway deciding
on an operator's behalf which retired text becomes current again. Instead the
context resolves to a deprecated version until someone publishes a replacement,
and every lookup that feeds matching already refuses to return it. This is
recorded as a deliberate choice, not an oversight.

### Content hashing, and the seam left for Phase 10.2

`content_hash_for(text)` is the first hash computed anywhere in this project —
Phase 10.0 fixed `CONTENT_HASH_ALGORITHM` and `CONTENT_HASH_PATTERN` and
deliberately computed nothing. It hashes exactly the text it is handed.

`models.CanonicalContextRecord`'s own docstring assigns the verification here by
name: *"A record whose hash does not match its text is constructible here and
must be refused by the storage layer."* It is refused —
`RegistryContentHashMismatchError`.

That check has to survive Phase 10.2 adding normalization *before* hashing
(design doc §11 Tier 0), which would otherwise make every record fail
verification. `Registry(hash_text=...)` is the seam: it defaults to plain
SHA-256 because 10.1 has no normalizer, and 10.2 passes
`lambda t: content_hash_for(normalize_for_hash(t))` with no schema change and no
API change. Phase 10.0 summary §7.6 states the split this preserves — 10.1
hashes the text it is given, 10.2 decides what is given — and a test exercises
the hook with a real whitespace/case normalizer.

---

## 5. Step 10.1.3 — concurrency and durability

Phase 10.5 runs the gateway as multiple worker processes, so the store is built
for concurrent readers and writers now, before a process exists to exercise it.

**WAL mode is on** — `PRAGMA journal_mode=WAL`, set once and persistent in the
file, verified as `wal` by a test reading it back through a raw connection.
Readers do not block the writer and the writer does not block readers. The open
path *checks* SQLite's answer rather than assuming it: on a filesystem that
cannot support WAL the mode silently stays `delete`, and a registry quietly
running without WAL is a concurrency story that is not true.

**`PRAGMA synchronous=FULL`** — every commit is fsynced before it is
acknowledged. This is the same append-then-fsync discipline v1's own WAL uses,
and it is the deliberate choice over the faster `NORMAL`: `NORMAL` survives a
process crash but can lose the tail of the log to a power cut or kernel panic,
and design doc §10's entire argument for keeping the registry out of PulseKV is
that a registry entry is *not* loss-tolerant the way a cache entry is. Registry
writes are rare and curated; the fsync is on no hot path.

**`BEGIN IMMEDIATE` for every write.** Each write does check-then-insert (does
this version exist? is this hash already live? does another context own this
alias?), which is only correct if no other process can interleave between the
check and the insert. IMMEDIATE takes the write lock at statement one. A
deferred transaction would take it at the first *write* and could deadlock two
upgrading writers into `SQLITE_BUSY` — the classic SQLite failure that
`busy_timeout` cannot resolve, because neither side can proceed.

**One connection per thread**, since `sqlite3.Connection` is not safe to share
concurrently; `close()` still closes all of them. `busy_timeout` defaults to 5s.

**Migrations need no lock.** Each file is self-contained, wrapped in its own
`BEGIN IMMEDIATE`, built entirely from `IF NOT EXISTS`/`OR IGNORE`, and records
itself in `schema_migrations` as its last act — so two workers racing to migrate
on startup both succeed and neither corrupts the other.

**Minimum SQLite 3.24.0**, checked at open and refused loudly rather than
discovered as a syntax error mid-write. The pointer table's upsert
(`ON CONFLICT ... DO UPDATE`) is what sets the floor; the partial UNIQUE index
needs only 3.8.0. Same "reject before anything starts" posture
`control/internal/config` already applies to a bad cluster config (risk register
row 13).

### The schema, and which rule each part carries

| Object | Carries |
|---|---|
| `canonical_context`, PK `(namespace, context_id, version)` | Namespace leads every key, so a query that forgets it cannot reach another tenant's row through a covering index |
| Partial unique index on `(namespace, content_hash) WHERE deprecated_at IS NULL` | Tier 0's lookup resolves to at most one record — unambiguous by construction, not by picking a winner. Deprecated rows keep their hash (the audit trail needs them) but stop competing for it, so retired text may be published again |
| `current_version` | Design doc §10's mutable pointer, kept off the record so publishing v5 never rewrites v4 |
| `alias_owner` / `alias_binding` | An alias string names exactly one context within a namespace (or Tier 0 would be non-deterministic), while many versions may declare it. `ordinal` preserves the frozen record's tuple order, without which a record read back would not equal the one written |
| `canonical_context_version_is_immutable` | Aborts any UPDATE naming a frozen column. `BEFORE UPDATE OF` fires on the SET list, not on a value comparison, so `SET canonical_text = canonical_text` aborts too |
| `canonical_context_deprecation_is_one_way` | A retired version does not come back |
| `canonical_context_is_append_only` | A deleted version would make every decision logged against it uninterpretable |
| `alias_owner_is_stable` | Re-pointing an alias would silently change what an alias hit substitutes |

The SQL constraints are the floor, not the mechanism: each write checks its
invariants in Python first so the caller gets a typed error naming what it
collided with, and the constraint behind it holds even if a future code path
forgets the check.

---

## 6. Deliberate deviations and additions

Every one is an addition or a refinement; none removes anything asked for.

| # | Deviation | Reasoning |
|---|---|---|
| 1 | **Two exception subclasses added** — `RegistryConflictError`, `RegistryContentHashMismatchError` | Both under the existing `RegistryError`, so Phase 10.5's fail-open wiring is still one `except GatewayError`. The four stub classes had no shape for "this write would make a lookup ambiguous" or "this record lied about its hash", and collapsing them into `RegistryVersionImmutableError` would make that error mean three unrelated things |
| 2 | **`list_records` added**, not in plan §5's method list | The Phase 10.1 prompt's §10.1.2 requires it explicitly: "Phase 10.2/10.3's matching logic will need to scan the registry; this is their entry point." `namespace` is keyword-only with no default, so an all-tenant enumeration is not writable by accident |
| 3 | **`Registry.from_dsn` added** | `config.GatewayConfig.registry_dsn` is documented as opaque *because the store was this phase's choice*; this is where the choice is decoded. A non-`sqlite` scheme is refused rather than guessed at, so a config pointing at Postgres fails at startup instead of silently opening an empty SQLite file beside it |
| 4 | **`Registry` is the SQLite implementation, not an ABC with one subclass** | An abstract base with a single implementation is speculative generality. Postgres-readiness is a property of the SQL and the migrations directory, not of a Python indirection nobody has a second implementation for yet |
| 5 | **An in-memory database is refused** | `:memory:` is private to one connection, so every thread and worker process would silently get its own empty registry and nothing would survive a restart — the exact opposite of this phase's objective. Refusing it costs three lines |
| 6 | **Re-registering a byte-identical record is refused, not idempotent** | Silent idempotence hides a caller that believes it is publishing something new. `models.py` sets the precedent by refusing duplicate aliases rather than de-duplicating them |
| 7 | **Versions must strictly advance** | Design doc §10 says "monotonically increasing"; enforcing `new > max` is the direct reading. Contiguity is *not* required, because no document asks for it |
| 8 | **Records are re-validated through the frozen type on read** | Constructed through the model rather than `model_construct`, so a hand-edited or older-schema row fails loudly instead of flowing onward as a record nothing else would have accepted. Proven by a test that edits a row with raw SQL |
| 9 | **`gateway/tests/test_models.py` edited** — `registry` removed from `STUB_MODULES` | Necessary: registry is no longer a stub. This is the new component's own test file, not the frozen top-level `tests/`. The discipline moves rather than lapsing — `TestPhaseBoundary` asserts `find_candidates` still raises `NotImplementedError("Phase 10.3")` |
| 10 | **`pyproject.toml` gained `[tool.setuptools.package-data]`** | The schema is data, not code, so setuptools would have left it out of the wheel and an installed gateway would have had nothing to migrate from. Verified by building the wheel and reading its contents (§8). No dependency was added |
| 11 | **`gateway/README.md` status table updated** | It said "Phase 10.0 — contract only" and "This package currently contains no runtime behavior", both of which stopped being true |

---

## 7. Notes and questions surfaced for later phases

Recorded because they were found while building, not resolved here.

1. **`pulsekv_canonical_context_hits_total{context_id,version}` has no namespace
   label.** Design doc §18 fixes that label set, but `context_id` is
   namespace-scoped in this schema — two tenants may each hold a
   `github-agent-policy`, and their hit counts would merge into one series.
   Storage is right (the stub's `get(context_id, namespace, ...)` signature
   already treats `context_id` as namespace-scoped, and design §15 makes
   namespace the boundary); the *metric* is what needs a third label or an
   explicit decision. **Phase 10.2 owns this**, as the first phase to emit it.

2. **An alias declared only by an older version stops resolving.** An alias
   names a context (design doc §10) and resolves through the current-version
   pointer, so publishing a new version that drops an alias retires it. That is
   the safe reading — the alternative resolves to text the current version no
   longer endorses — but it means "publish a new version" silently changes which
   aliases work. Phase 10.4's version-update corpus is where that behavior
   should be exercised against a real scenario.

3. **`by_content_hash` returns the version whose hash matched, not the current
   one.** This is the only sound answer (the incoming block *is* that version's
   text), but it means a Tier 0 hit can substitute a non-current version's text.
   Phase 10.2 should decide whether that is reported as `method=exact` against
   that version, which is what the decision log's shape already suggests.

4. **Deprecating the current version leaves a context resolving to a deprecated
   record.** `get()` returns it (with `is_deprecated` true); every matching
   lookup refuses it. Phase 10.5's operator surface may want to warn on this
   state — it is legal, honest, and probably not what an operator meant.

5. **No `Registry` method enumerates namespaces**, deliberately: a cross-tenant
   read is exactly what design §15 exists to make unwritable. If an admin
   surface needs one in 10.5, it should be a separate, explicitly-named call, not
   a default on the tenant-scoped API.

---

## 8. Exit criteria — verified

Plan §5's exit criteria, each with the evidence rather than the claim.

| # | Criterion | Evidence |
|---|---|---|
| 1 | CRUD is durable across a **real process restart** | `TestDurabilityAcrossRestart` runs a child `subprocess` that registers a record and exits; the parent reopens the file and reads it. A second test is stronger: the child commits and then `SIGKILL`s itself, so it never runs a finally block, flush, or close — parent asserts `returncode == -9` and the row is still there. A third asserts reopening applies no migration twice |
| 2 | Namespace isolation proven **at the storage layer** | `TestNamespaceIsolation`: two namespaces hold records with a byte-identical `content_hash` (asserted equal in the fixture, not assumed) and an identical alias string. `by_content_hash`, `resolve_alias`, `get` and `list_records` each return only the caller's namespace; a third namespace sees nothing. `list_records()` with no namespace raises `TypeError` |
| 3 | Version immutability proven | `TestVersionImmutability`: republishing a version is refused (with different content *and* with identical content); versions must advance. Then the real proof — a **raw `sqlite3` connection that never imports `pulsekv_gateway`** gets `IntegrityError` on `UPDATE` of `canonical_text`, `content_hash`, `version`, `namespace`, `created_by`, on the no-op `SET canonical_text = canonical_text`, on `DELETE`, and on un-deprecating |
| 4 | Storage unavailable → a typed, catchable exception | `TestFailureModes`: an unopenable path, a file that is not a database, a closed registry, an in-memory database and a non-`sqlite` DSN each raise `RegistryUnavailableError`; all six error classes are asserted to subclass `GatewayError`, so 10.5's fail-open path is one `except` |
| 5 | A failed write leaves nothing behind | `TestTransactions` breaks the step *after* a successful row insert (`publish_version` inserts, then moves the pointer) and asserts the inserted row is gone, the pointer is unmoved, and the connection is not left holding the write lock — a leaked transaction would deadlock every later writer against `busy_timeout` |
| 6 | Nothing in this phase computes or searches an embedding | `TestPhaseBoundary`: `find_candidates` raises `NotImplementedError` mentioning 10.3; a 256-byte binary blob round-trips byte-exactly; an AST check asserts `registry.py` imports nothing outside the standard library plus `.models` |
| 7 | The hard scope boundary held | `git status --porcelain -- src include tests node control proto adapters` is **empty**. `git status --porcelain -- gateway/tests/corpus` is **empty**. Whole-repo status shows only `gateway/` paths |
| 8 | The frozen contract is unchanged | `models.py` does not appear in `git status`. Storage was built around the types as frozen; no field was added, widened or relaxed |
| 9 | No new dependency | The built wheel declares `Requires-Dist: pydantic==2.13.4` and `pytest==9.1.1; extra == "dev"` — identical to Phase 10.0 — and contains `pulsekv_gateway/migrations/001_initial.sql` |

**Test run:** `164 passed in 1.23s` — Phase 10.0's 108 contract tests still
green (minus the two parametrized cases that applied to `registry` *as a stub*),
plus 58 new storage tests:

```
TestCrudRoundTrip 9 · TestVersionImmutability 6 · TestDeprecation 7
TestNamespaceIsolation 5 · TestFailureModes 8 · TestCurrentVersionPointer 4
TestAliases 4 · TestConcurrency 4 · TestContentHashIntegrity 3
TestDurabilityAcrossRestart 3 · TestPhaseBoundary 3 · TestTransactions 2
```

**Reproducing** (a throwaway venv outside the repository, so nothing is
installed into the user's Python):

```bash
python3 -m venv /tmp/gwvenv && /tmp/gwvenv/bin/pip install pydantic==2.13.4 pytest==9.1.1
PYTHONPATH=gateway /tmp/gwvenv/bin/python -m pytest gateway/tests -q
```

**The invariants, demonstrated rather than asserted** (real output, run against
a scratch database):

```
1. PRAGMAs actually in force
   journal_mode  = wal
   triggers      = ['alias_owner_is_stable', 'canonical_context_deprecation_is_one_way',
                    'canonical_context_is_append_only', 'canonical_context_version_is_immutable']

2. SQL refuses history rewrites, from a connection that never imports us
   UPDATE canonical_context SET canonical_text = 'rewritten'  -> IntegrityError: a published
       version is immutable (design doc §10) -- publish a new version instead
   UPDATE canonical_context SET canonical_text = canonical_text -> IntegrityError: (same)
   DELETE FROM canonical_context  -> IntegrityError: published versions are never deleted
       (design doc §10, §17) -- deprecate instead
   UPDATE alias_owner SET context_id = 'stolen' -> IntegrityError: an alias string names one
       context_id within a namespace (design doc §10)

3. Namespace isolation with a byte-identical content_hash
   shared hash   = ec7087a5947fb437...
   acme -> acme | globex -> globex | initech -> None
   alias 'gh-policy' in acme -> github-agent-policy | in globex -> None
```

---

## 9. Where Phase 10.2 starts

Plan §5's handoff is satisfied: **`Registry.by_content_hash` and
`Registry.resolve_alias` are ready for Tier 0 to call**, both namespace-scoped,
both refusing deprecated versions, both returning the full record so a
`MatchResult` can be built without a second round trip.

Three things 10.2 inherits rather than decides:

1. **The normalization seam.** `Registry(hash_text=...)` is where
   `normalizer.normalize_for_hash` plugs in. Write the normalizer, pass it, and
   the stored hash and the lookup hash stay consistent with no schema change.
   Note the consequence: a registry populated with un-normalized hashes and one
   populated with normalized hashes are different databases, so this should be
   settled before anything real is registered.
2. **`list_records` is the scan.** Tier 1's structural work and 10.3's index
   build both start there; `current_only=True` gives one record per context.
3. **The metric-label question in §7.1** — `pulsekv_canonical_context_hits_total`
   has no namespace label, and 10.2 is the first phase to emit it.

Nothing in 10.2 should need to modify `registry.py`'s schema. If it does, that
is a signal worth stopping on: plan §2 warns that a schema change after 10.2 is
written means rework, which is why this phase was sequenced first.

**Deliverable:** `docs/pulsekv-semantic-context-phase10.2-summary.md`.
