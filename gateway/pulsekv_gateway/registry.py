"""Canonical Context Registry -- durable, versioned, namespace-scoped storage.

Design doc §10 (record shape, immutable versions with a mutable current-version
pointer, why the store is an ordinary relational database and explicitly *not*
PulseKV itself); §15 (namespace as a structural boundary); §17 (deprecation,
fail-open); plan §5 (this phase's CRUD surface, invariants and exit criteria).

**This module stores records. It does not match anything.** Tier 0's exact-hash
and alias lookups live here because they are storage reads, not matching logic
(plan §5 says so explicitly); the decision of what to do with a hit is Phase
10.2's. ``find_candidates`` keeps its signature and its ``NotImplementedError``
-- embeddings are stored as an opaque blob and nothing here computes, compares
or searches one. That is Phase 10.3.

Storage backend: SQLite
-----------------------
Chosen over a JSON/NDJSON append store and an embedded KV library for two
reasons:

1. **The invariants are constraints, and a constraint engine enforces them.**
   Version immutability and namespace isolation are the two things plan §5
   requires proven *below* the type system. In SQL they are a trigger and a
   compound primary key -- enforced even for a caller holding a raw connection
   that never imports this package. In a file store they would be Python code
   that a future writer can forget, which is precisely the "backdoor" this
   phase is told not to create.
2. **Zero new infrastructure, and zero new dependencies.** ``sqlite3`` is in
   the standard library, so ``pyproject.toml``'s deliberate "no database
   driver" property (Phase 10.0 summary, exit criterion 5) survives this phase
   untouched, and there is no separate service to run beside the gateway.
   Design doc §10 recommends "Postgres, or SQLite for the MVP's realistic
   scale" against an estimated low-hundreds-to-low-thousands of curated
   entries; plan §1 records the intended shape as "SQLite MVP, Postgres-ready".

Plan §5's parenthetical prefers "a real SQL store from the start ... over
SQLite-then-migrate, to avoid a schema-migration exercise mid-project". SQLite
*is* a real SQL store; what that sentence is guarding against is an ad-hoc
store with no schema to carry forward. ``migrations/001_initial.sql`` is
ordinary portable DDL and ``migrations/`` is a real, ordered, ledgered
directory, so a Postgres port is a second migration dialect and a sibling
class -- not a rewrite of how records are shaped.

Concurrency and durability
--------------------------
Gateway deployments may run multiple process instances, so this module is
built for concurrent readers and writers even though the Phase 10.5 console
entry point deliberately starts one Uvicorn worker per process:

* **WAL mode** (``PRAGMA journal_mode=WAL``, set once and persistent in the
  file). Readers do not block the writer and the writer does not block
  readers; without it every reader would serialize behind each write. The
  open path *verifies* SQLite returned ``wal`` rather than assuming it -- on a
  filesystem that cannot support it the mode silently stays ``delete``, and a
  registry quietly running without WAL is a concurrency story that is not
  true.
* **``PRAGMA synchronous=FULL``** -- each commit is fsynced before it is
  acknowledged. This is the same append-then-fsync discipline v1's own WAL
  uses, and it is chosen deliberately over the faster ``NORMAL``: ``NORMAL``
  survives a process crash but can lose the tail of the log to a power cut or
  kernel panic, and design doc §10's whole argument for keeping the registry
  out of PulseKV is that a registry entry is *not* loss-tolerant the way a
  cache entry is. Registry writes are rare and curated; the fsync is not on
  any hot path.
* **``BEGIN IMMEDIATE`` for every write.** The check-then-insert sequences
  below (does this version exist? is this hash already live? does another
  context own this alias?) are only correct if no other process can interleave
  between the check and the insert. IMMEDIATE takes the write lock at
  statement one, so they cannot. A deferred transaction would take it only at
  the first write and could deadlock two upgrading writers into
  ``SQLITE_BUSY`` -- the classic SQLite failure that ``busy_timeout`` cannot
  resolve, because neither side can proceed.
* **One connection per thread**, since ``sqlite3.Connection`` is not safe to
  share concurrently. ``close()`` still closes all of them.

The SQL constraints are the floor, not the mechanism: each write checks its
invariants in Python first so the caller gets a typed error naming what it
collided with, and the trigger/index behind it guarantees the invariant even
if a future code path forgets the check.
"""

from __future__ import annotations

import hashlib
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Tuple

from .models import (
    CONTENT_HASH_ALGORITHM,
    BlockType,
    Candidate,
    CanonicalContextRecord,
    GatewayError,
)

__all__ = [
    "DEFAULT_BUSY_TIMEOUT_MS",
    "MINIMUM_SQLITE_VERSION",
    "Registry",
    "RegistryConflictError",
    "RegistryContentHashMismatchError",
    "RegistryError",
    "RegistryNotFoundError",
    "RegistryUnavailableError",
    "RegistryVersionImmutableError",
    "content_hash_for",
]

# How long a writer waits for another process's write lock before giving up.
# Registry writes are short (one small transaction), so a wait this long means
# something is genuinely wrong rather than merely busy.
DEFAULT_BUSY_TIMEOUT_MS = 5_000

# The oldest SQLite that can run this schema. `ON CONFLICT ... DO UPDATE`
# (the current-version pointer's upsert) arrived in 3.24.0; the partial UNIQUE
# index that keeps Tier 0's lookup unambiguous needs only 3.8.0. Checked at
# open rather than discovered as a syntax error mid-write, following the same
# "reject before anything starts" posture control/internal/config already
# applies to a bad cluster config (risk register row 13).
MINIMUM_SQLITE_VERSION = (3, 24, 0)

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def content_hash_for(text: str) -> str:
    """Hash ``text`` the way ``models.ContentHash`` is specified to be hashed.

    Phase 10.0 fixed the algorithm (``CONTENT_HASH_ALGORITHM``) and the
    rendering (``CONTENT_HASH_PATTERN``: 64 lowercase hex) and deliberately
    computed nothing; this is the first place in the project that computes one.

    It hashes exactly the text it is handed. Design doc §11's Tier 0 normalizes
    whitespace and casing *before* hashing, but that normalization is Phase
    10.2's ``normalizer.normalize_for_hash`` -- so this function stays the raw
    primitive and ``Registry(hash_text=...)`` is where 10.2 composes the two.
    """
    return hashlib.new(CONTENT_HASH_ALGORITHM, text.encode("utf-8")).hexdigest()


class RegistryError(GatewayError):
    """Base for every registry failure.

    Plan §5: storage-backend problems surface as a typed, catchable exception
    rather than a bare driver/connection error, so Phase 10.5's fail-open
    wiring is one ``except`` clause (design doc §17).
    """


class RegistryUnavailableError(RegistryError):
    """The backing store could not be reached.

    Design doc §17: every block becomes a miss and the original text is
    forwarded unchanged. Logged, never silently swallowed.
    """


class RegistryVersionImmutableError(RegistryError):
    """An attempt to alter a published version's text, hash, or version."""


class RegistryNotFoundError(RegistryError):
    """A requested ``context_id``/``version`` does not exist."""


class RegistryConflictError(RegistryError):
    """A write collided with an invariant that keeps lookups deterministic.

    Added by Phase 10.1, alongside ``RegistryContentHashMismatchError``, under
    the existing ``RegistryError`` base so Phase 10.5's fail-open wiring is
    unchanged. Raised for: a version that does not advance the context's
    highest version; a second *live* record claiming a namespace's
    ``content_hash`` (which would make Tier 0's lookup ambiguous); an alias
    string already naming a different context in that namespace; and
    re-deprecating an already-deprecated version.
    """


class RegistryContentHashMismatchError(RegistryError):
    """``content_hash`` does not hash ``canonical_text``.

    ``models.CanonicalContextRecord``'s own docstring assigns this check here:
    "A record whose hash does not match its text is constructible here and must
    be refused by the storage layer." A record that lied about its hash would
    be invisible to the exact tier that looks it up by that hash, and would
    make the audit trail's "version 4 always means this text" claim false.
    """


class Registry:
    """Durable, versioned, namespace-scoped storage for canonical contexts.

    Backed by SQLite; see the module docstring for why, and for the WAL /
    ``synchronous=FULL`` / ``BEGIN IMMEDIATE`` posture this class holds.

    ``register`` and ``publish_version`` differ in exactly one thing: the
    current-version pointer (design doc §10's "immutable versions, mutable
    pointer"). ``register`` stores a version and points at it only if the
    context is new -- otherwise the context would have no current version and
    ``get(context_id, namespace)`` could not answer. ``publish_version`` stores
    it *and* moves the pointer. Registering a later version without publishing
    it is therefore a staged version: durable, addressable by explicit version
    number, and not yet what the context resolves to.
    """

    def __init__(
        self,
        database: "str | Path",
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
        hash_text: Callable[[str], str] = content_hash_for,
    ) -> None:
        """Open (creating if needed) the registry at ``database``.

        ``hash_text`` is how a record's ``content_hash`` is checked against its
        ``canonical_text`` on write. It defaults to a plain SHA-256 of the text
        because Phase 10.1 has no normalizer; Phase 10.2 passes
        ``lambda t: content_hash_for(normalize_for_hash(t))`` and neither the
        schema nor this API changes. Phase 10.0's summary (§7.6) states the
        split: 10.1 hashes the text it is given, 10.2 decides what is given.
        """
        self._database = self._resolve_database(database)
        self._busy_timeout_ms = int(busy_timeout_ms)
        self._hash_text = hash_text
        self._local = threading.local()
        self._lock = threading.Lock()
        self._connections: List[sqlite3.Connection] = []
        self._closed = False
        self._migrate()

    # -- construction ------------------------------------------------------

    @classmethod
    def from_dsn(
        cls,
        dsn: str,
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
        hash_text: Callable[[str], str] = content_hash_for,
    ) -> "Registry":
        """Open a registry from ``config.GatewayConfig.registry_dsn``.

        That field is documented as an opaque string precisely because the
        store was Phase 10.1's choice to make; this is where the choice is
        decoded. ``sqlite:///var/lib/pulsekv/registry.db`` and a bare
        filesystem path are both accepted. Any other scheme is refused rather
        than guessed at -- Phase 10.1 ships one backend, and a config pointing
        at a Postgres URL should fail at startup (risk register row 13's
        "fail loud at startup"), not silently open an empty SQLite file beside
        it.
        """
        target = dsn
        if "://" in dsn:
            scheme, _, remainder = dsn.partition("://")
            if scheme != "sqlite":
                raise RegistryUnavailableError(
                    f"registry_dsn scheme {scheme!r} is not supported: Phase 10.1 "
                    f"ships the SQLite backend only (design doc §10)"
                )
            target = remainder
        return cls(target, busy_timeout_ms=busy_timeout_ms, hash_text=hash_text)

    @staticmethod
    def _resolve_database(database: "str | Path") -> Path:
        text = str(database)
        # An in-memory database is private to one connection, so every thread
        # and every worker process would silently get its own empty registry
        # and nothing would survive a restart -- the exact opposite of this
        # phase's objective. Refused rather than quietly accepted.
        if text == ":memory:" or "mode=memory" in text:
            raise RegistryUnavailableError(
                "an in-memory database is not a registry: it is private to one "
                "connection and does not survive a restart (plan §5 requires "
                "durability proven across one)"
            )
        return Path(text).expanduser()

    # -- connection management ---------------------------------------------

    def _open(self) -> sqlite3.Connection:
        try:
            connection = sqlite3.connect(
                str(self._database),
                timeout=self._busy_timeout_ms / 1000.0,
                isolation_level=None,  # explicit BEGIN IMMEDIATE, no implicit ones
                check_same_thread=False,  # each connection still serves one thread
            )
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                connection.close()
                raise RegistryUnavailableError(
                    f"{self._database}: could not enable WAL (journal_mode is "
                    f"{mode!r}); concurrent readers and writers are not safe here"
                )
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA foreign_keys = ON")
            version = tuple(
                int(part) for part in sqlite3.sqlite_version.split(".")[:3]
            )
            if version < MINIMUM_SQLITE_VERSION:
                connection.close()
                raise RegistryUnavailableError(
                    f"SQLite {sqlite3.sqlite_version} is older than the "
                    f"{'.'.join(str(part) for part in MINIMUM_SQLITE_VERSION)} this "
                    f"schema requires"
                )
        except sqlite3.Error as exc:
            raise RegistryUnavailableError(f"{self._database}: {exc}") from exc
        return connection

    def _connection(self) -> sqlite3.Connection:
        if self._closed:
            raise RegistryUnavailableError(f"{self._database}: registry is closed")
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = self._open()
            self._local.connection = connection
            with self._lock:
                self._connections.append(connection)
        return connection

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        """Run a write inside one ``BEGIN IMMEDIATE`` transaction.

        IMMEDIATE rather than DEFERRED because every writer here reads before
        it writes; see the module docstring.
        """
        connection = self._connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as exc:
            raise RegistryUnavailableError(f"{self._database}: {exc}") from exc
        try:
            yield connection
        except BaseException:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass  # the original failure is the one worth reporting
            raise
        try:
            connection.execute("COMMIT")
        except sqlite3.Error as exc:
            raise RegistryUnavailableError(f"{self._database}: {exc}") from exc

    def close(self) -> None:
        """Release every connection this registry opened, on any thread.

        Idempotent. Calls after this raise ``RegistryUnavailableError`` rather
        than a bare ``ProgrammingError`` from the driver, so a caller's
        fail-open path treats a closed registry the same as an unreachable one.
        Like any resource close, it must not race with an in-flight call.
        """
        with self._lock:
            self._closed = True
            connections, self._connections = self._connections, []
        for connection in connections:
            try:
                connection.close()
            except sqlite3.Error:
                pass
        self._local = threading.local()

    def __enter__(self) -> "Registry":
        return self

    def __exit__(self, *_exc_info) -> None:
        self.close()

    # -- schema ------------------------------------------------------------

    def _migrate(self) -> None:
        """Apply every unapplied migration, in filename order.

        Each file is self-contained, wrapped in its own ``BEGIN IMMEDIATE``,
        built entirely from ``IF NOT EXISTS``/``OR IGNORE`` statements, and
        records itself in ``schema_migrations`` as its last act. Two worker
        processes racing to migrate on startup therefore both succeed, which is
        why no migration lock is needed.
        """
        connection = self._connection()
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "    version    TEXT NOT NULL PRIMARY KEY,"
                "    applied_at TEXT NOT NULL"
                ")"
            )
            applied = {
                row[0]
                for row in connection.execute("SELECT version FROM schema_migrations")
            }
            for path in sorted(_MIGRATIONS_DIR.glob("*.sql")):
                if path.stem in applied:
                    continue
                connection.executescript(path.read_text(encoding="utf-8"))
        except sqlite3.Error as exc:
            raise RegistryUnavailableError(
                f"{self._database}: applying schema failed: {exc}"
            ) from exc
        except OSError as exc:
            raise RegistryUnavailableError(
                f"{_MIGRATIONS_DIR}: cannot read migrations: {exc}"
            ) from exc

    def applied_migrations(self) -> Tuple[str, ...]:
        """Which migrations this database has, oldest first."""
        rows = self._query(
            "SELECT version FROM schema_migrations ORDER BY version", ()
        )
        return tuple(row["version"] for row in rows)

    # -- writes ------------------------------------------------------------

    def register(self, record: CanonicalContextRecord) -> str:
        """Store ``record`` and return its ``context_id``.

        Points the context at this version only when the context is new; see
        the class docstring for the split with ``publish_version``.
        """
        with self._write() as connection:
            first_version = self._highest_version(
                connection, record.namespace, record.context_id
            ) is None
            self._insert(connection, record)
            if first_version:
                self._point_at(connection, record)
        return record.context_id

    def publish_version(
        self, record: CanonicalContextRecord
    ) -> CanonicalContextRecord:
        """Publish a new version and move the current-version pointer.

        Design doc §10: existing decisions logged against an older version stay
        interpretable, so publishing never rewrites the older row -- it inserts
        a new one and re-points. Returns the record as the store now holds it,
        read back rather than echoed, so the return value is evidence the write
        landed.
        """
        with self._write() as connection:
            self._insert(connection, record)
            self._point_at(connection, record)
            stored = self._read_one(
                connection, record.namespace, record.context_id, record.version
            )
        if stored is None:  # pragma: no cover -- would mean the commit lied
            raise RegistryUnavailableError(
                f"{record.context_id} v{record.version}: written but not readable"
            )
        return stored

    def deprecate(
        self, context_id: str, namespace: str, version: int, at: datetime
    ) -> CanonicalContextRecord:
        """Stop serving one version as a match target (design doc §17).

        The only legal mutation of a published row, and one-way. It is *not* a
        delete: the row stays, so a decision logged against this version stays
        interpretable, and already-issued decisions are not retroactively
        invalidated -- there is nothing to invalidate, the text was already
        sent.

        The current-version pointer is left where it is. Moving it silently
        would be the gateway deciding on an operator's behalf which older
        version becomes current again; instead the context resolves to a
        deprecated version until someone publishes a replacement, and every
        lookup that feeds matching (``by_content_hash``, ``resolve_alias``)
        already refuses to return it.
        """
        with self._write() as connection:
            existing = self._read_one(connection, namespace, context_id, version)
            if existing is None:
                raise RegistryNotFoundError(
                    f"{namespace}/{context_id} v{version}: not found"
                )
            if existing.is_deprecated:
                raise RegistryConflictError(
                    f"{namespace}/{context_id} v{version}: already deprecated at "
                    f"{existing.deprecated_at.isoformat()}"
                )
            # Built through the frozen type so its own rule (deprecated_at must
            # not precede created_at) is applied by the contract rather than
            # re-implemented here.
            try:
                updated = existing.deprecate(at)
            except (ValueError, TypeError) as exc:
                raise RegistryConflictError(
                    f"{namespace}/{context_id} v{version}: {exc}"
                ) from exc
            self._execute(
                connection,
                "UPDATE canonical_context SET deprecated_at = ? "
                "WHERE namespace = ? AND context_id = ? AND version = ?",
                (
                    updated.deprecated_at.isoformat(),
                    namespace,
                    context_id,
                    version,
                ),
            )
        return updated

    # -- reads -------------------------------------------------------------

    def get(
        self, context_id: str, namespace: str, version: Optional[int] = None
    ) -> CanonicalContextRecord:
        """Fetch one version, or the current one when ``version`` is None.

        ``namespace`` is a parameter rather than something the caller filters
        afterwards, so no call site can accidentally read across tenants
        (design doc §15).

        Deprecated versions are returned here. This is the administrative and
        audit read -- "what did version 4 actually say" must stay answerable
        after version 4 is retired. The lookups that feed matching filter them
        out instead.
        """
        connection = self._connection()
        if version is None:
            row = self._query_one(
                "SELECT version FROM current_version "
                "WHERE namespace = ? AND context_id = ?",
                (namespace, context_id),
                connection=connection,
            )
            if row is None:
                raise RegistryNotFoundError(
                    f"{namespace}/{context_id}: no current version"
                )
            version = int(row["version"])
        record = self._read_one(connection, namespace, context_id, version)
        if record is None:
            raise RegistryNotFoundError(
                f"{namespace}/{context_id} v{version}: not found"
            )
        return record

    def by_content_hash(
        self, content_hash: str, namespace: str
    ) -> Optional[CanonicalContextRecord]:
        """Tier 0's exact-hash path (plan §5: storage, not matching logic).

        Live records only -- a deprecated version has stopped being a match
        target (design doc §17). At most one live record per (namespace,
        content_hash) exists, guaranteed by a partial unique index, so this
        answer is unambiguous by construction rather than by picking a winner.
        """
        connection = self._connection()
        row = self._query_one(
            "SELECT * FROM canonical_context "
            "WHERE namespace = ? AND content_hash = ? AND deprecated_at IS NULL",
            (namespace, content_hash),
            connection=connection,
        )
        if row is None:
            return None
        return self._hydrate(connection, row)

    def resolve_alias(
        self, text: str, namespace: str
    ) -> Optional[CanonicalContextRecord]:
        """Tier 0's alias path: an exact registered alias string.

        Returns the record rather than plan §5's sketched ``Optional[context_id]``
        -- the caller (Tier 0, plan §6) immediately needs ``canonical_text`` and
        ``version`` to build a ``MatchResult`` and substitute, so returning the
        id alone would make every alias hit a mandatory second round trip.

        An alias names a *context*, not a text (design doc §10), so it resolves
        through the current-version pointer: the alias must be declared by the
        version that is current, and that version must not be deprecated.
        Either miss returns None, which Tier 0 treats as any other miss.
        """
        connection = self._connection()
        row = self._query_one(
            "SELECT record.* FROM alias_binding AS binding "
            "JOIN current_version AS pointer "
            "  ON pointer.namespace = binding.namespace "
            " AND pointer.context_id = binding.context_id "
            " AND pointer.version = binding.version "
            "JOIN canonical_context AS record "
            "  ON record.namespace = binding.namespace "
            " AND record.context_id = binding.context_id "
            " AND record.version = binding.version "
            "WHERE binding.namespace = ? AND binding.alias = ? "
            "  AND record.deprecated_at IS NULL",
            (namespace, text),
            connection=connection,
        )
        if row is None:
            return None
        return self._hydrate(connection, row)

    def list_records(
        self,
        *,
        namespace: str,
        block_type: Optional[BlockType] = None,
        include_deprecated: bool = False,
        current_only: bool = False,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Tuple[CanonicalContextRecord, ...]:
        """Enumerate one namespace's records, oldest context and version first.

        Phase 10.2/10.3's entry point for scanning the registry: 10.3 builds
        its vector index from ``current_only=True`` over a namespace, and 10.2
        uses the same call to reason about what is registered at all.

        ``namespace`` is keyword-only and has no default, so an enumeration
        across every tenant is not something a caller can write by accident --
        design doc §15's isolation claim is about what the storage API makes
        expressible, not only about what today's callers happen to pass.
        """
        clauses = ["record.namespace = ?"]
        parameters: List[object] = [namespace]
        joins = ""
        if current_only:
            joins = (
                " JOIN current_version AS pointer "
                "   ON pointer.namespace = record.namespace "
                "  AND pointer.context_id = record.context_id "
                "  AND pointer.version = record.version"
            )
        if block_type is not None:
            clauses.append("record.block_type = ?")
            parameters.append(block_type.value)
        if not include_deprecated:
            clauses.append("record.deprecated_at IS NULL")
        sql = (
            "SELECT record.* FROM canonical_context AS record"
            + joins
            + " WHERE "
            + " AND ".join(clauses)
            + " ORDER BY record.context_id, record.version"
        )
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            parameters.extend([int(limit), int(offset)])
        elif offset:
            sql += " LIMIT -1 OFFSET ?"
            parameters.append(int(offset))

        connection = self._connection()
        rows = self._query(sql, tuple(parameters), connection=connection)
        if not rows:
            return ()
        aliases = self._aliases_in(connection, namespace)
        return tuple(
            self._to_record(row, aliases.get(self._key(row), ()))
            for row in rows
        )

    def find_candidates(
        self,
        *,
        namespace: str,
        block_type: BlockType,
        top_k: int,
    ) -> Tuple[Candidate, ...]:
        """Not implemented here. Tier 2 retrieval lives in ``index.VectorIndex``.

        Phase 10.3 built Tier 2 and deliberately did **not** fill this in. The
        reasons, recorded so a later reader does not mistake it for unfinished
        work:

        * Plan §7 places the interface on the index, not the registry:
          ``Index.top_k(vector, namespace, block_type, k) -> List[Candidate]``.
          The as-built name is ``VectorIndex.find_candidates``.
        * This signature has no query vector, and ``Candidate`` requires a
          similarity. Ranking without one is not possible, and adding the
          parameter would change a Phase 10.1 signature.
        * ``registry.py`` is pinned by its own test to standard-library imports
          only, so it cannot host vector math without either breaking that pin
          or growing a second copy of the model-identity enforcement that risk
          register row 6 requires to live in exactly one place.
        * Phase 10.2's handoff already assigns this module the supporting role:
          ``list_records(namespace=..., current_only=True)`` is the scan
          ``VectorIndex.build_from_registry`` reads.

        ``namespace`` and ``block_type`` remain pre-filters wherever retrieval
        happens (design doc §11, §15) -- ``VectorIndex`` partitions on exactly
        this pair.
        """
        raise NotImplementedError(
            "Tier 2 retrieval is index.VectorIndex.find_candidates (Phase 10.3); "
            "this module supplies its records through list_records"
        )

    # -- internals ---------------------------------------------------------

    def _insert(
        self, connection: sqlite3.Connection, record: CanonicalContextRecord
    ) -> None:
        """Insert one version, refusing every write that would break a lookup.

        Checked in Python for the error message, guaranteed in SQL for the
        invariant. Every check runs inside the caller's ``BEGIN IMMEDIATE``, so
        no other process can slip between a check and the insert.
        """
        expected = self._hash_text(record.canonical_text)
        if expected != record.content_hash:
            raise RegistryContentHashMismatchError(
                f"{record.namespace}/{record.context_id} v{record.version}: "
                f"content_hash {record.content_hash} does not hash canonical_text "
                f"(expected {expected})"
            )

        existing = self._read_one(
            connection, record.namespace, record.context_id, record.version
        )
        if existing is not None:
            raise RegistryVersionImmutableError(
                f"{record.namespace}/{record.context_id} v{record.version}: already "
                f"published"
                + ("" if existing == record else " with different content")
                + " -- a published version is immutable (design doc §10); publish a "
                "new version instead"
            )

        highest = self._highest_version(
            connection, record.namespace, record.context_id
        )
        if highest is not None and record.version <= highest:
            raise RegistryConflictError(
                f"{record.namespace}/{record.context_id} v{record.version}: versions "
                f"are monotonically increasing (design doc §10) and v{highest} "
                f"already exists"
            )

        if record.deprecated_at is None:
            clash = self._query_one(
                "SELECT context_id, version FROM canonical_context "
                "WHERE namespace = ? AND content_hash = ? AND deprecated_at IS NULL",
                (record.namespace, record.content_hash),
                connection=connection,
            )
            if clash is not None:
                raise RegistryConflictError(
                    f"{record.namespace}/{record.context_id} v{record.version}: "
                    f"content_hash {record.content_hash} is already live on "
                    f"{clash['context_id']} v{clash['version']}; Tier 0's exact-hash "
                    f"lookup must resolve to one record"
                )

        for alias in record.aliases:
            owner = self._query_one(
                "SELECT context_id FROM alias_owner WHERE namespace = ? AND alias = ?",
                (record.namespace, alias),
                connection=connection,
            )
            if owner is not None and owner["context_id"] != record.context_id:
                raise RegistryConflictError(
                    f"{record.namespace}: alias {alias!r} already names "
                    f"{owner['context_id']}, not {record.context_id} -- an alias "
                    f"string names one context within a namespace (design doc §10)"
                )

        self._execute(
            connection,
            "INSERT INTO canonical_context ("
            "    namespace, context_id, version, canonical_text, content_hash,"
            "    block_type, embedding_model_id, embedding_model_version, embedding,"
            "    created_at, created_by, deprecated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                record.namespace,
                record.context_id,
                record.version,
                record.canonical_text,
                record.content_hash,
                record.block_type.value,
                record.embedding_model_id,
                record.embedding_model_version,
                record.embedding,
                record.created_at.isoformat(),
                record.created_by,
                None if record.deprecated_at is None else record.deprecated_at.isoformat(),
            ),
        )
        for ordinal, alias in enumerate(record.aliases):
            self._execute(
                connection,
                "INSERT OR IGNORE INTO alias_owner (namespace, alias, context_id) "
                "VALUES (?, ?, ?)",
                (record.namespace, alias, record.context_id),
            )
            self._execute(
                connection,
                "INSERT INTO alias_binding "
                "(namespace, context_id, version, alias, ordinal) "
                "VALUES (?, ?, ?, ?, ?)",
                (record.namespace, record.context_id, record.version, alias, ordinal),
            )

    def _point_at(
        self, connection: sqlite3.Connection, record: CanonicalContextRecord
    ) -> None:
        self._execute(
            connection,
            "INSERT INTO current_version (namespace, context_id, version, updated_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (namespace, context_id) DO UPDATE SET "
            "    version = excluded.version, updated_at = excluded.updated_at",
            (
                record.namespace,
                record.context_id,
                record.version,
                record.created_at.isoformat(),
            ),
        )

    def _highest_version(
        self, connection: sqlite3.Connection, namespace: str, context_id: str
    ) -> Optional[int]:
        row = self._query_one(
            "SELECT MAX(version) AS highest FROM canonical_context "
            "WHERE namespace = ? AND context_id = ?",
            (namespace, context_id),
            connection=connection,
        )
        if row is None or row["highest"] is None:
            return None
        return int(row["highest"])

    def _read_one(
        self,
        connection: sqlite3.Connection,
        namespace: str,
        context_id: str,
        version: int,
    ) -> Optional[CanonicalContextRecord]:
        row = self._query_one(
            "SELECT * FROM canonical_context "
            "WHERE namespace = ? AND context_id = ? AND version = ?",
            (namespace, context_id, version),
            connection=connection,
        )
        if row is None:
            return None
        return self._hydrate(connection, row)

    def _hydrate(
        self, connection: sqlite3.Connection, row: sqlite3.Row
    ) -> CanonicalContextRecord:
        aliases = tuple(
            entry["alias"]
            for entry in self._query(
                "SELECT alias FROM alias_binding "
                "WHERE namespace = ? AND context_id = ? AND version = ? "
                "ORDER BY ordinal",
                (row["namespace"], row["context_id"], row["version"]),
                connection=connection,
            )
        )
        return self._to_record(row, aliases)

    def _aliases_in(
        self, connection: sqlite3.Connection, namespace: str
    ) -> Dict[Tuple[str, int], Tuple[str, ...]]:
        """Every alias in one namespace, grouped by the version declaring it.

        One query instead of one per record: a namespace holds low thousands of
        entries at the scale design doc §10 estimates, and an N+1 read pattern
        on a registry scan is exactly the thing Phase 10.3 would inherit.
        """
        grouped: Dict[Tuple[str, int], List[str]] = {}
        for row in self._query(
            "SELECT context_id, version, alias FROM alias_binding "
            "WHERE namespace = ? ORDER BY context_id, version, ordinal",
            (namespace,),
            connection=connection,
        ):
            grouped.setdefault(
                (row["context_id"], int(row["version"])), []
            ).append(row["alias"])
        return {key: tuple(value) for key, value in grouped.items()}

    @staticmethod
    def _key(row: sqlite3.Row) -> Tuple[str, int]:
        return (row["context_id"], int(row["version"]))

    @staticmethod
    def _to_record(
        row: sqlite3.Row, aliases: Tuple[str, ...]
    ) -> CanonicalContextRecord:
        """Rebuild the frozen type from a row, re-validating on the way in.

        Constructed through the model rather than ``model_construct`` so a row
        that somehow violates the contract -- hand-edited, or written by an
        older schema -- fails loudly here instead of flowing on as a record
        nothing else would have accepted.
        """
        deprecated_at = row["deprecated_at"]
        return CanonicalContextRecord(
            namespace=row["namespace"],
            context_id=row["context_id"],
            version=int(row["version"]),
            canonical_text=row["canonical_text"],
            content_hash=row["content_hash"],
            block_type=BlockType(row["block_type"]),
            embedding_model_id=row["embedding_model_id"],
            embedding_model_version=row["embedding_model_version"],
            embedding=row["embedding"],
            aliases=aliases,
            created_at=datetime.fromisoformat(row["created_at"]),
            created_by=row["created_by"],
            deprecated_at=(
                None if deprecated_at is None else datetime.fromisoformat(deprecated_at)
            ),
        )

    # -- SQL plumbing ------------------------------------------------------
    #
    # Every driver exception becomes a typed RegistryError here, so no caller
    # ever sees a bare sqlite3 error (plan §5's failure-test requirement).
    # sqlite3.IntegrityError means a constraint or trigger refused the write --
    # the invariant held below Python, which is what the SQL floor is for -- so
    # it maps to a conflict, not to "unavailable".

    def _execute(
        self, connection: sqlite3.Connection, sql: str, parameters: Tuple
    ) -> sqlite3.Cursor:
        try:
            return connection.execute(sql, parameters)
        except sqlite3.IntegrityError as exc:
            raise RegistryConflictError(f"{self._database}: {exc}") from exc
        except sqlite3.Error as exc:
            raise RegistryUnavailableError(f"{self._database}: {exc}") from exc

    def _query(
        self,
        sql: str,
        parameters: Tuple,
        connection: Optional[sqlite3.Connection] = None,
    ) -> List[sqlite3.Row]:
        connection = connection or self._connection()
        return self._execute(connection, sql, parameters).fetchall()

    def _query_one(
        self,
        sql: str,
        parameters: Tuple,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Optional[sqlite3.Row]:
        connection = connection or self._connection()
        return self._execute(connection, sql, parameters).fetchone()
