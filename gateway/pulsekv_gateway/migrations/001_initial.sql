-- Canonical Context Registry -- initial schema (Phase 10.1).
--
-- Design doc §10 (record shape, immutable versions + mutable current-version
-- pointer), §15 (namespace as a structural boundary, not a post-hoc filter),
-- §17 (deprecation stops a version being a match target, retroactively
-- invalidates nothing); plan §5 (this phase's invariants and exit criteria).
--
-- Two rules live here rather than only in Python, because plan §5 requires the
-- storage layer to reject what the frozen type refuses in process:
--
--   1. A published version is immutable. The triggers below abort any UPDATE
--      that names a frozen column and any DELETE at all, so a caller holding a
--      raw connection -- one that never imports pulsekv_gateway -- still cannot
--      rewrite history. models.CanonicalContextRecord cannot enforce what SQL
--      does behind its back.
--   2. A namespace is a hard boundary. Every key here leads with `namespace`,
--      so a query that forgets it cannot accidentally read another tenant's
--      row via a covering index.
--
-- Portability: this is deliberately ordinary SQL. The only SQLite-specific
-- constructs are the trigger bodies (a Postgres port becomes a BEFORE UPDATE
-- rule or a plpgsql trigger) and the partial UNIQUE index, which Postgres
-- spells identically. Plan §1 calls the store "SQLite MVP, Postgres-ready" and
-- this file is where that claim is either honoured or lost.
--
-- Idempotent by construction: every statement is IF NOT EXISTS / OR IGNORE, so
-- two gateway worker processes racing to migrate on startup (design doc §8's
-- multi-process deployment, plan §9) both succeed and neither corrupts the
-- other. That is why the runner needs no migration lock of its own.

BEGIN IMMEDIATE;

-- ---------------------------------------------------------------------------
-- One published version of one canonical context. Design doc §10.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS canonical_context (
    namespace               TEXT    NOT NULL,
    context_id              TEXT    NOT NULL,
    version                 INTEGER NOT NULL,
    canonical_text          TEXT    NOT NULL,
    content_hash            TEXT    NOT NULL,
    block_type              TEXT    NOT NULL,
    embedding_model_id      TEXT,
    embedding_model_version TEXT,
    embedding               BLOB,
    created_at              TEXT    NOT NULL,
    created_by              TEXT    NOT NULL,
    deprecated_at           TEXT,

    PRIMARY KEY (namespace, context_id, version),

    -- Mirrors of the frozen type's own field constraints. Duplicated on
    -- purpose: the type guards the process, these guard the file.
    CHECK (version >= 1),
    CHECK (length(canonical_text) > 0),
    CHECK (length(content_hash) = 64),
    CHECK (length(created_by) > 0),
    -- Design doc §16: half an embedding identity is worse than none.
    CHECK ((embedding_model_id IS NULL) = (embedding_model_version IS NULL)),
    CHECK (embedding IS NULL OR embedding_model_id IS NOT NULL)
);

-- Tier 0's lookup (plan §5's `by_content_hash`) must be unambiguous: at most
-- one *live* record per (namespace, content_hash). Deprecated rows keep their
-- hash -- the audit trail needs them (design doc §10) -- but stop competing
-- for it, so the same text may be re-published after its version is retired.
CREATE UNIQUE INDEX IF NOT EXISTS canonical_context_live_content_hash
    ON canonical_context (namespace, content_hash)
    WHERE deprecated_at IS NULL;

-- Phase 10.2/10.3 scan the registry by namespace and block type; design doc
-- §11 makes both retrieval *pre-filters*, so they lead the index.
CREATE INDEX IF NOT EXISTS canonical_context_scan
    ON canonical_context (namespace, block_type, deprecated_at);

-- ---------------------------------------------------------------------------
-- The mutable current-version pointer. Design doc §10's "immutable versions,
-- mutable pointer"; kept out of the record itself because publishing v5 would
-- otherwise have to rewrite v4's row and contradict immutability (Phase 10.0
-- summary §7.2 assigns this table to Phase 10.1).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS current_version (
    namespace  TEXT    NOT NULL,
    context_id TEXT    NOT NULL,
    version    INTEGER NOT NULL,
    updated_at TEXT    NOT NULL,

    PRIMARY KEY (namespace, context_id),
    FOREIGN KEY (namespace, context_id, version)
        REFERENCES canonical_context (namespace, context_id, version)
);

-- ---------------------------------------------------------------------------
-- Aliases. Design doc §10: "deterministic exact-match strings that resolve to
-- this context_id with method=alias, no embedding involved".
--
-- Split in two because they answer two different questions:
--   alias_owner   -- which context does this alias string name? Exactly one,
--                    forever, within a namespace. If one string could name two
--                    contexts, Tier 0 would be non-deterministic, which is the
--                    one thing an exact tier may never be.
--   alias_binding -- which versions declare it? Many, since a new version
--                    normally carries its predecessor's aliases forward.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alias_owner (
    namespace  TEXT NOT NULL,
    alias      TEXT NOT NULL,
    context_id TEXT NOT NULL,

    PRIMARY KEY (namespace, alias),
    CHECK (length(alias) > 0)
);

CREATE TABLE IF NOT EXISTS alias_binding (
    namespace  TEXT    NOT NULL,
    context_id TEXT    NOT NULL,
    version    INTEGER NOT NULL,
    alias      TEXT    NOT NULL,
    -- The frozen record's `aliases` is an ordered tuple; without this the
    -- record read back would not equal the record written.
    ordinal    INTEGER NOT NULL,

    PRIMARY KEY (namespace, context_id, version, alias),
    FOREIGN KEY (namespace, context_id, version)
        REFERENCES canonical_context (namespace, context_id, version),
    FOREIGN KEY (namespace, alias)
        REFERENCES alias_owner (namespace, alias)
);

CREATE INDEX IF NOT EXISTS alias_binding_lookup
    ON alias_binding (namespace, alias);

-- ---------------------------------------------------------------------------
-- Immutability, enforced below Python.
-- ---------------------------------------------------------------------------

-- BEFORE UPDATE OF fires when a column appears in the SET list at all, whether
-- or not its value would change -- so `SET canonical_text = canonical_text`
-- aborts too. deprecated_at is deliberately absent from this list: design doc
-- §17 makes deprecation the one legal transition on a published version.
CREATE TRIGGER IF NOT EXISTS canonical_context_version_is_immutable
BEFORE UPDATE OF
    namespace, context_id, version, canonical_text, content_hash, block_type,
    embedding, embedding_model_id, embedding_model_version, created_at, created_by
ON canonical_context
BEGIN
    SELECT RAISE(ABORT,
        'canonical_context: a published version is immutable (design doc §10) -- publish a new version instead');
END;

CREATE TRIGGER IF NOT EXISTS canonical_context_deprecation_is_one_way
BEFORE UPDATE OF deprecated_at ON canonical_context
WHEN OLD.deprecated_at IS NOT NULL
BEGIN
    SELECT RAISE(ABORT,
        'canonical_context: deprecation is one-way (design doc §17) -- a retired version does not come back');
END;

-- A deleted version would make every decision logged against it
-- uninterpretable, which is the property design doc §10 says immutability
-- exists to provide. Deprecation is the supported way to stop serving one.
CREATE TRIGGER IF NOT EXISTS canonical_context_is_append_only
BEFORE DELETE ON canonical_context
BEGIN
    SELECT RAISE(ABORT,
        'canonical_context: published versions are never deleted (design doc §10, §17) -- deprecate instead');
END;

-- Re-pointing an alias at a different context would silently change what a
-- Tier 0 alias hit substitutes.
CREATE TRIGGER IF NOT EXISTS alias_owner_is_stable
BEFORE UPDATE OF context_id ON alias_owner
BEGIN
    SELECT RAISE(ABORT,
        'alias_owner: an alias string names one context_id within a namespace (design doc §10)');
END;

INSERT OR IGNORE INTO schema_migrations (version, applied_at)
VALUES ('001_initial', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));

COMMIT;
