-- Make Tier 0's alias lookup independent of how many aliases a namespace holds.
--
-- Found in Phase 10.2 by the index-usage check that phase's own exit criteria
-- require. Measured before this migration, with one namespace:
--
--     resolve_alias:  201 aliases -> 0.0295 ms
--                    4201 aliases -> 0.2486 ms   (8.4x for 21x the rows)
--
-- and the query plan showed why:
--
--     SEARCH binding USING COVERING INDEX sqlite_autoindex_alias_binding_1 (namespace=?)
--
-- The planner drove from alias_binding's primary key -- (namespace,
-- context_id, version, alias) -- constrained on `namespace` alone, then
-- filtered `alias` row by row. It declined migration 001's
-- alias_binding_lookup (namespace, alias) because that index does not carry
-- context_id/version, so using it would have cost a table lookup per row to
-- satisfy the join; a covering scan of one namespace looked cheaper.
--
-- Widening the index to carry the two join columns removes that trade: the
-- planner can seek straight to (namespace, alias) and still answer the join
-- from the index alone. `ordinal` is deliberately not included -- this query
-- never reads it, and the record's alias *order* is reconstructed by a
-- different query that already seeks on the primary key.
--
-- Indexes are derived data, so replacing one is not a history rewrite: none of
-- the immutability rules in 001 are relaxed, and no row is touched.

BEGIN IMMEDIATE;

DROP INDEX IF EXISTS alias_binding_lookup;

CREATE INDEX IF NOT EXISTS alias_binding_lookup
    ON alias_binding (namespace, alias, context_id, version);

INSERT OR IGNORE INTO schema_migrations (version, applied_at)
VALUES ('002_alias_lookup_covering_index', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));

COMMIT;
