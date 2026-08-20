"""Storage tests for the Canonical Context Registry (Phase 10.1).

Plan §5's exit criteria are the spine of this suite, and each is proven rather
than inferred from the storage engine's own guarantees:

* ``TestCrudRoundTrip``        — a record survives storage byte-exactly
* ``TestCurrentVersionPointer``— design doc §10's "immutable versions, mutable pointer"
* ``TestVersionImmutability``  — refused in Python *and* below it, in SQL
* ``TestNamespaceIsolation``   — two tenants with an identical hash never meet
* ``TestDeprecation``          — a retired version leaves the match path, not the record
* ``TestContentHashIntegrity`` — the check models.py delegates to the storage layer
* ``TestAliases``              — an alias string names one context, deterministically
* ``TestDurabilityAcrossRestart`` — a real process restart, and a real SIGKILL
* ``TestConcurrency``          — WAL is on, and concurrent writers do not corrupt
* ``TestTransactions``         — a failed write leaves nothing behind
* ``TestFailureModes``         — every failure is a typed, catchable GatewayError
* ``TestPhaseBoundary``        — 10.1 did not quietly start doing 10.3's job
"""

from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import pulsekv_gateway
from pulsekv_gateway.models import (
    BlockType,
    CanonicalContextRecord,
    GatewayError,
)
from pulsekv_gateway.registry import (
    Registry,
    RegistryConflictError,
    RegistryContentHashMismatchError,
    RegistryError,
    RegistryNotFoundError,
    RegistryUnavailableError,
    RegistryVersionImmutableError,
    content_hash_for,
)

NOW = datetime(2026, 8, 19, 12, 0, 0, tzinfo=timezone.utc)
POLICY_TEXT = "Never delete a production resource without an explicit confirmation."
GATEWAY_ROOT = str(Path(pulsekv_gateway.__file__).parent.parent)


def make_record(**overrides) -> CanonicalContextRecord:
    """A valid record whose hash actually hashes its text."""
    text = overrides.pop("canonical_text", POLICY_TEXT)
    fields = dict(
        context_id="github-agent-policy",
        version=1,
        namespace="acme",
        canonical_text=text,
        content_hash=content_hash_for(text),
        block_type=BlockType.ORG_POLICY,
        created_at=NOW,
        created_by="mohit",
    )
    fields.update(overrides)
    return CanonicalContextRecord(**fields)


@pytest.fixture
def registry(tmp_path):
    store = Registry(tmp_path / "registry.db")
    yield store
    store.close()


# ---------------------------------------------------------------------------


class TestCrudRoundTrip:
    def test_register_and_get_returns_an_identical_record(self, registry):
        record = make_record()
        assert registry.register(record) == "github-agent-policy"
        assert registry.get("github-agent-policy", "acme") == record

    def test_every_field_survives_including_the_opaque_embedding_blob(self, registry):
        # Design doc §16: the embedding is stored, never interpreted. A blob
        # that is not valid UTF-8 is the honest test of "opaque".
        record = make_record(
            embedding=b"\x00\xff\xfe binary \x80 vector",
            embedding_model_id="bge-small-en",
            embedding_model_version="1.5",
            aliases=("gh-policy", "github-policy-v1"),
            deprecated_at=NOW + timedelta(days=1),
        )
        registry.register(record)
        assert registry.get("github-agent-policy", "acme", version=1) == record

    def test_alias_order_is_preserved(self, registry):
        # The frozen record's `aliases` is an ordered tuple; a set-shaped store
        # would round-trip a record that no longer equals the one written.
        aliases = ("zeta", "alpha", "mid")
        registry.register(make_record(aliases=aliases))
        assert registry.get("github-agent-policy", "acme").aliases == aliases

    def test_non_utc_offsets_and_microseconds_survive(self, registry):
        stamp = datetime(2026, 8, 19, 4, 5, 6, 789012, tzinfo=timezone(timedelta(hours=-5)))
        registry.register(make_record(created_at=stamp))
        assert registry.get("github-agent-policy", "acme").created_at == stamp

    def test_list_records_enumerates_one_namespace_in_a_stable_order(self, registry):
        registry.register(make_record(context_id="b-policy", canonical_text="b"))
        registry.register(make_record(context_id="a-policy", canonical_text="a"))
        registry.publish_version(
            make_record(context_id="a-policy", version=2, canonical_text="a2")
        )
        listed = registry.list_records(namespace="acme")
        assert [(r.context_id, r.version) for r in listed] == [
            ("a-policy", 1),
            ("a-policy", 2),
            ("b-policy", 1),
        ]

    def test_list_records_filters_by_block_type_and_current_only(self, registry):
        registry.register(make_record(context_id="a-policy", canonical_text="a"))
        registry.publish_version(
            make_record(context_id="a-policy", version=2, canonical_text="a2")
        )
        registry.register(
            make_record(
                context_id="a-schema",
                canonical_text="{}",
                block_type=BlockType.TOOL_SCHEMA,
            )
        )
        current = registry.list_records(namespace="acme", current_only=True)
        assert [(r.context_id, r.version) for r in current] == [
            ("a-policy", 2),
            ("a-schema", 1),
        ]
        schemas = registry.list_records(
            namespace="acme", block_type=BlockType.TOOL_SCHEMA
        )
        assert [r.context_id for r in schemas] == ["a-schema"]

    def test_list_records_paginates(self, registry):
        for index in range(5):
            registry.register(
                make_record(context_id=f"policy-{index}", canonical_text=f"text {index}")
            )
        page = registry.list_records(namespace="acme", limit=2, offset=2)
        assert [r.context_id for r in page] == ["policy-2", "policy-3"]

    def test_by_content_hash_is_tier_zeros_lookup(self, registry):
        record = make_record()
        registry.register(record)
        assert registry.by_content_hash(record.content_hash, "acme") == record
        assert registry.by_content_hash("f" * 64, "acme") is None

    def test_get_raises_typed_not_found(self, registry):
        with pytest.raises(RegistryNotFoundError):
            registry.get("absent", "acme")
        registry.register(make_record())
        with pytest.raises(RegistryNotFoundError):
            registry.get("github-agent-policy", "acme", version=7)


class TestCurrentVersionPointer:
    """Design doc §10: immutable versions, mutable pointer."""

    def test_first_register_points_the_context_at_it(self, registry):
        registry.register(make_record())
        assert registry.get("github-agent-policy", "acme").version == 1

    def test_register_of_a_later_version_stages_it_without_repointing(self, registry):
        registry.register(make_record())
        registry.register(make_record(version=2, canonical_text="v2 text"))
        assert registry.get("github-agent-policy", "acme").version == 1
        assert registry.get("github-agent-policy", "acme", version=2).version == 2

    def test_publish_version_moves_the_pointer_and_leaves_history(self, registry):
        first = make_record()
        registry.register(first)
        second = registry.publish_version(make_record(version=2, canonical_text="v2 text"))
        assert registry.get("github-agent-policy", "acme") == second
        # The whole point of immutability: v1 is still exactly what it was, so
        # a decision logged against it stays interpretable (design doc §10).
        assert registry.get("github-agent-policy", "acme", version=1) == first

    def test_publish_version_returns_what_the_store_holds(self, registry):
        record = make_record(aliases=("gh-policy",))
        assert registry.publish_version(record) == record


class TestVersionImmutability:
    """Plan §5: rejected at the storage layer, not merely discouraged."""

    def test_republishing_a_version_is_refused(self, registry):
        registry.register(make_record())
        with pytest.raises(RegistryVersionImmutableError):
            registry.register(make_record(canonical_text="different text"))

    def test_republishing_an_identical_version_is_also_refused(self, registry):
        # Silent idempotence would hide a caller that thinks it is publishing
        # something new, and this codebase refuses rather than quietly repairs
        # (models.py does the same with duplicate aliases).
        registry.register(make_record())
        with pytest.raises(RegistryVersionImmutableError):
            registry.register(make_record())

    def test_versions_must_advance(self, registry):
        registry.publish_version(make_record(version=3))
        with pytest.raises(RegistryConflictError):
            registry.register(make_record(version=2, canonical_text="earlier"))

    def test_sql_refuses_an_update_from_a_connection_that_never_imports_us(
        self, tmp_path
    ):
        # The claim under test is that the invariant lives below Python. This
        # test therefore does not use Registry at all for the mutation: it is
        # the raw driver, exactly as a curious operator with sqlite3 would be.
        path = tmp_path / "registry.db"
        with Registry(path) as store:
            store.register(make_record())
        raw = sqlite3.connect(str(path))
        try:
            for column, value in (
                ("canonical_text", "rewritten"),
                ("content_hash", "a" * 64),
                ("version", 9),
                ("namespace", "evil"),
                ("created_by", "someone-else"),
            ):
                with pytest.raises(sqlite3.IntegrityError):
                    raw.execute(
                        f"UPDATE canonical_context SET {column} = ?", (value,)
                    )
            # Even a no-op assignment aborts: BEFORE UPDATE OF fires on the SET
            # list, not on a value comparison.
            with pytest.raises(sqlite3.IntegrityError):
                raw.execute(
                    "UPDATE canonical_context SET canonical_text = canonical_text"
                )
        finally:
            raw.close()

    def test_sql_refuses_a_delete(self, tmp_path):
        path = tmp_path / "registry.db"
        with Registry(path) as store:
            store.register(make_record())
        raw = sqlite3.connect(str(path))
        try:
            with pytest.raises(sqlite3.IntegrityError):
                raw.execute("DELETE FROM canonical_context")
        finally:
            raw.close()

    def test_deprecation_is_the_one_permitted_update(self, tmp_path):
        path = tmp_path / "registry.db"
        with Registry(path) as store:
            store.register(make_record())
            store.deprecate("github-agent-policy", "acme", 1, NOW + timedelta(hours=1))
        raw = sqlite3.connect(str(path))
        try:
            # Already deprecated, so the one-way trigger refuses even this.
            with pytest.raises(sqlite3.IntegrityError):
                raw.execute("UPDATE canonical_context SET deprecated_at = NULL")
        finally:
            raw.close()


class TestNamespaceIsolation:
    """Design doc §15's "structurally impossible", proven at the storage layer.

    Plan §5 asks for exactly this and asks for it *here* — not inferred later
    from the matcher passing namespace through correctly.
    """

    @pytest.fixture
    def two_tenants(self, registry):
        shared = make_record(namespace="acme", aliases=("shared-alias",))
        other = make_record(namespace="globex", aliases=("shared-alias",))
        # Same text, therefore the same content_hash, in two namespaces.
        assert shared.content_hash == other.content_hash
        registry.register(shared)
        registry.register(other)
        return registry, shared, other

    def test_identical_hashes_coexist_and_never_cross(self, two_tenants):
        registry, shared, other = two_tenants
        assert registry.by_content_hash(shared.content_hash, "acme") == shared
        assert registry.by_content_hash(shared.content_hash, "globex") == other

    def test_get_does_not_reach_across_namespaces(self, two_tenants):
        registry, _, _ = two_tenants
        assert registry.get("github-agent-policy", "acme").namespace == "acme"
        with pytest.raises(RegistryNotFoundError):
            registry.get("github-agent-policy", "initech")

    def test_alias_resolution_does_not_reach_across_namespaces(self, two_tenants):
        registry, shared, other = two_tenants
        assert registry.resolve_alias("shared-alias", "acme") == shared
        assert registry.resolve_alias("shared-alias", "globex") == other
        assert registry.resolve_alias("shared-alias", "initech") is None

    def test_listing_is_scoped_and_cannot_be_written_unscoped(self, two_tenants):
        registry, _, _ = two_tenants
        assert {r.namespace for r in registry.list_records(namespace="acme")} == {"acme"}
        assert registry.list_records(namespace="initech") == ()
        # namespace is keyword-only with no default: an all-tenant enumeration
        # is not something a caller can write by accident.
        with pytest.raises(TypeError):
            registry.list_records()  # type: ignore[call-arg]

    def test_unknown_namespace_simply_has_nothing(self, two_tenants):
        registry, shared, _ = two_tenants
        assert registry.by_content_hash(shared.content_hash, "initech") is None


class TestDeprecation:
    def test_a_deprecated_version_leaves_the_match_path(self, registry):
        record = make_record(aliases=("gh-policy",))
        registry.register(record)
        registry.deprecate("github-agent-policy", "acme", 1, NOW + timedelta(hours=1))
        assert registry.by_content_hash(record.content_hash, "acme") is None
        assert registry.resolve_alias("gh-policy", "acme") is None

    def test_but_the_record_itself_remains_readable(self, registry):
        record = make_record()
        registry.register(record)
        returned = registry.deprecate(
            "github-agent-policy", "acme", 1, NOW + timedelta(hours=1)
        )
        stored = registry.get("github-agent-policy", "acme", version=1)
        assert stored == returned
        assert stored.is_deprecated
        assert stored.canonical_text == record.canonical_text

    def test_deprecating_twice_is_refused(self, registry):
        registry.register(make_record())
        registry.deprecate("github-agent-policy", "acme", 1, NOW + timedelta(hours=1))
        with pytest.raises(RegistryConflictError):
            registry.deprecate(
                "github-agent-policy", "acme", 1, NOW + timedelta(hours=2)
            )

    def test_deprecating_before_creation_is_refused_by_the_contracts_own_rule(
        self, registry
    ):
        registry.register(make_record())
        with pytest.raises(RegistryConflictError):
            registry.deprecate(
                "github-agent-policy", "acme", 1, NOW - timedelta(hours=1)
            )

    def test_deprecating_something_absent_is_not_found(self, registry):
        with pytest.raises(RegistryNotFoundError):
            registry.deprecate("absent", "acme", 1, NOW)

    def test_a_retired_hash_can_be_published_again(self, registry):
        # The live-only uniqueness index is what makes this legal: history
        # keeps its hash, but stops competing for Tier 0's lookup.
        record = make_record()
        registry.register(record)
        registry.deprecate("github-agent-policy", "acme", 1, NOW + timedelta(hours=1))
        revived = registry.publish_version(make_record(version=2))
        assert registry.by_content_hash(record.content_hash, "acme") == revived

    def test_two_live_records_cannot_share_a_hash(self, registry):
        registry.register(make_record(context_id="first"))
        with pytest.raises(RegistryConflictError):
            registry.register(make_record(context_id="second"))


class TestContentHashIntegrity:
    """models.py delegates this check here, by name."""

    def test_a_record_that_lies_about_its_hash_is_refused(self, registry):
        with pytest.raises(RegistryContentHashMismatchError):
            registry.register(make_record(content_hash="0" * 64))

    def test_content_hash_for_matches_the_frozen_rendering(self):
        import re

        from pulsekv_gateway.models import CONTENT_HASH_PATTERN

        assert re.match(CONTENT_HASH_PATTERN, content_hash_for("anything"))

    def test_phase_102_can_supply_its_normalizer_without_a_schema_change(self, tmp_path):
        # Phase 10.2 owns the normalization that feeds the hash; the registry
        # takes it as a parameter rather than assuming today's identity.
        def normalized(text: str) -> str:
            return content_hash_for(" ".join(text.split()).casefold())

        store = Registry(tmp_path / "registry.db", hash_text=normalized)
        try:
            spaced = "  ORG   Policy \n Text  "
            record = make_record(
                canonical_text=spaced, content_hash=normalized(spaced)
            )
            store.register(record)
            assert store.by_content_hash(record.content_hash, "acme") == record
            with pytest.raises(RegistryContentHashMismatchError):
                store.register(
                    make_record(
                        context_id="other",
                        canonical_text=spaced,
                        content_hash=content_hash_for(spaced),
                    )
                )
        finally:
            store.close()


class TestAliases:
    def test_an_alias_names_one_context_within_a_namespace(self, registry):
        registry.register(make_record(context_id="first", aliases=("shared",)))
        with pytest.raises(RegistryConflictError):
            registry.register(
                make_record(
                    context_id="second",
                    canonical_text="another text",
                    aliases=("shared",),
                )
            )

    def test_a_new_version_may_carry_its_predecessors_aliases_forward(self, registry):
        registry.register(make_record(aliases=("gh-policy",)))
        published = registry.publish_version(
            make_record(version=2, canonical_text="v2 text", aliases=("gh-policy",))
        )
        assert registry.resolve_alias("gh-policy", "acme") == published

    def test_an_alias_declared_only_by_an_older_version_stops_resolving(self, registry):
        registry.register(make_record(aliases=("retired-alias",)))
        registry.publish_version(make_record(version=2, canonical_text="v2 text"))
        # The alias names a context; the context now resolves to v2, which does
        # not declare it. Missing rather than silently resolving to v1's text.
        assert registry.resolve_alias("retired-alias", "acme") is None

    def test_an_unregistered_alias_is_a_miss(self, registry):
        registry.register(make_record())
        assert registry.resolve_alias("never-registered", "acme") is None


class TestDurabilityAcrossRestart:
    """Plan §5: proven with an actual restart, not assumed from the engine."""

    @staticmethod
    def _run_child(script: str, path: Path) -> subprocess.CompletedProcess:
        env = dict(os.environ, PYTHONPATH=GATEWAY_ROOT)
        return subprocess.run(
            [sys.executable, "-c", textwrap.dedent(script), str(path)],
            capture_output=True,
            text=True,
            env=env,
        )

    def test_a_record_written_by_another_process_is_readable_here(self, tmp_path):
        path = tmp_path / "registry.db"
        result = self._run_child(
            """
            import sys
            from datetime import datetime, timezone
            from pulsekv_gateway.models import BlockType, CanonicalContextRecord
            from pulsekv_gateway.registry import Registry, content_hash_for

            text = "written by a process that has since exited"
            store = Registry(sys.argv[1])
            store.register(CanonicalContextRecord(
                context_id="survivor", version=1, namespace="acme",
                canonical_text=text, content_hash=content_hash_for(text),
                block_type=BlockType.ORG_POLICY,
                created_at=datetime(2026, 8, 19, 12, tzinfo=timezone.utc),
                created_by="child",
            ))
            store.close()
            """,
            path,
        )
        assert result.returncode == 0, result.stderr
        with Registry(path) as store:
            assert store.get("survivor", "acme").created_by == "child"

    def test_a_commit_survives_a_sigkill_with_no_clean_shutdown(self, tmp_path):
        # The stronger reading of "durable": the writing process never gets to
        # run a finally block, flush, or close a connection. WAL plus
        # synchronous=FULL is what makes the committed row still be there.
        path = tmp_path / "registry.db"
        result = self._run_child(
            """
            import os, signal, sys
            from datetime import datetime, timezone
            from pulsekv_gateway.models import BlockType, CanonicalContextRecord
            from pulsekv_gateway.registry import Registry, content_hash_for

            text = "committed, then the process was killed"
            store = Registry(sys.argv[1])
            store.register(CanonicalContextRecord(
                context_id="killed-mid-flight", version=1, namespace="acme",
                canonical_text=text, content_hash=content_hash_for(text),
                block_type=BlockType.ORG_POLICY,
                created_at=datetime(2026, 8, 19, 12, tzinfo=timezone.utc),
                created_by="doomed",
            ))
            os.kill(os.getpid(), signal.SIGKILL)
            """,
            path,
        )
        assert result.returncode == -signal.SIGKILL
        with Registry(path) as store:
            assert store.get("killed-mid-flight", "acme").created_by == "doomed"

    def test_reopening_applies_no_migration_twice(self, tmp_path):
        path = tmp_path / "registry.db"
        with Registry(path) as first:
            first.register(make_record())
            applied = first.applied_migrations()
        assert applied  # at least one migration ran on a fresh file
        with Registry(path) as second:
            # Compared rather than hardcoded: the subject of this test is that
            # reopening applies nothing again, and a later phase adding a
            # migration should not have to edit it.
            assert second.applied_migrations() == applied
            assert second.get("github-agent-policy", "acme").version == 1


class TestConcurrency:
    def test_wal_is_actually_on(self, tmp_path):
        path = tmp_path / "registry.db"
        with Registry(path):
            pass
        raw = sqlite3.connect(str(path))
        try:
            assert raw.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        finally:
            raw.close()

    def test_concurrent_processes_all_land(self, tmp_path):
        # Four writers, one file, no coordination beyond SQLite's own locking.
        # Every record must be present and readable afterwards.
        path = tmp_path / "registry.db"
        with Registry(path):
            pass  # create the schema once, then let the children race
        script = textwrap.dedent(
            """
            import sys
            from datetime import datetime, timezone
            from pulsekv_gateway.models import BlockType, CanonicalContextRecord
            from pulsekv_gateway.registry import Registry, content_hash_for

            index = sys.argv[2]
            text = "policy text number " + index
            store = Registry(sys.argv[1])
            store.register(CanonicalContextRecord(
                context_id="policy-" + index, version=1, namespace="acme",
                canonical_text=text, content_hash=content_hash_for(text),
                block_type=BlockType.ORG_POLICY,
                created_at=datetime(2026, 8, 19, 12, tzinfo=timezone.utc),
                created_by="worker-" + index,
            ))
            store.close()
            """
        )
        env = dict(os.environ, PYTHONPATH=GATEWAY_ROOT)
        children = [
            subprocess.Popen(
                [sys.executable, "-c", script, str(path), str(index)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )
            for index in range(4)
        ]
        for child in children:
            _, stderr = child.communicate(timeout=60)
            assert child.returncode == 0, stderr
        with Registry(path) as store:
            assert [r.context_id for r in store.list_records(namespace="acme")] == [
                "policy-0",
                "policy-1",
                "policy-2",
                "policy-3",
            ]

    def test_concurrent_threads_all_land(self, registry):
        errors = []

        def write(index: int) -> None:
            try:
                registry.register(
                    make_record(
                        context_id=f"threaded-{index}",
                        canonical_text=f"thread text {index}",
                    )
                )
            except GatewayError as exc:  # pragma: no cover - a failure is the finding
                errors.append(exc)

        threads = [threading.Thread(target=write, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        assert not errors
        assert len(registry.list_records(namespace="acme")) == 8

    def test_one_writer_does_not_block_a_readers_view_of_committed_data(self, registry):
        # WAL's actual promise, exercised: a reader on a second connection sees
        # a committed row while the writer's connection stays open.
        registry.register(make_record())
        seen = []

        def read() -> None:
            seen.append(registry.get("github-agent-policy", "acme"))

        thread = threading.Thread(target=read)
        thread.start()
        thread.join(timeout=30)
        assert seen and seen[0].context_id == "github-agent-policy"


class TestTransactions:
    def test_a_failed_write_leaves_nothing_behind(self, registry, monkeypatch):
        # The checks in _insert all run before any row is written, so the only
        # way to observe a rollback of a *completed* insert is to break the
        # step after it. publish_version inserts the record and moves the
        # pointer in one transaction; if the second half fails, the first half
        # must not survive.
        registry.register(make_record())

        def explode(*_args, **_kwargs):
            raise RuntimeError("simulated failure after the row was inserted")

        monkeypatch.setattr(Registry, "_point_at", explode)
        with pytest.raises(RuntimeError):
            registry.publish_version(make_record(version=2, canonical_text="v2 text"))
        monkeypatch.undo()

        with pytest.raises(RegistryNotFoundError):
            registry.get("github-agent-policy", "acme", version=2)
        assert registry.get("github-agent-policy", "acme").version == 1
        assert len(registry.list_records(namespace="acme")) == 1

    def test_a_rolled_back_write_does_not_leave_the_connection_in_a_transaction(
        self, registry, monkeypatch
    ):
        # A leaked transaction would hold the write lock and deadlock every
        # later writer against busy_timeout.
        def explode(*_args, **_kwargs):
            raise RuntimeError("simulated failure")

        monkeypatch.setattr(Registry, "_point_at", explode)
        with pytest.raises(RuntimeError):
            registry.register(make_record())
        monkeypatch.undo()
        registry.register(make_record(context_id="after-rollback"))
        assert registry.get("after-rollback", "acme").version == 1


class TestFailureModes:
    """Plan §5: a typed, catchable exception, never a bare driver error."""

    def test_every_registry_error_is_a_gateway_error(self):
        # Phase 10.5's fail-open wiring is one `except GatewayError`.
        for error in (
            RegistryError,
            RegistryUnavailableError,
            RegistryVersionImmutableError,
            RegistryNotFoundError,
            RegistryConflictError,
            RegistryContentHashMismatchError,
        ):
            assert issubclass(error, GatewayError)

    def test_an_unopenable_path_is_unavailable_not_a_driver_error(self, tmp_path):
        with pytest.raises(RegistryUnavailableError):
            Registry(tmp_path / "no" / "such" / "directory" / "registry.db")

    def test_a_file_that_is_not_a_database_is_unavailable(self, tmp_path):
        path = tmp_path / "registry.db"
        path.write_bytes(b"this is not a SQLite database, it is 40 bytes of noise.")
        with pytest.raises(RegistryUnavailableError):
            Registry(path)

    def test_a_closed_registry_refuses_further_calls(self, tmp_path):
        store = Registry(tmp_path / "registry.db")
        store.register(make_record())
        store.close()
        with pytest.raises(RegistryUnavailableError):
            store.get("github-agent-policy", "acme")
        with pytest.raises(RegistryUnavailableError):
            store.register(make_record(context_id="after-close"))

    def test_close_is_idempotent(self, tmp_path):
        store = Registry(tmp_path / "registry.db")
        store.close()
        store.close()

    def test_an_in_memory_database_is_refused(self):
        # It would be private per connection and vanish on restart: the exact
        # opposite of what this phase is for.
        with pytest.raises(RegistryUnavailableError):
            Registry(":memory:")
        with pytest.raises(RegistryUnavailableError):
            Registry("file:reg?mode=memory&cache=shared")

    def test_from_dsn_accepts_sqlite_and_refuses_anything_else(self, tmp_path):
        path = tmp_path / "registry.db"
        with Registry.from_dsn(f"sqlite://{path}") as store:
            store.register(make_record())
        with Registry(path) as store:
            assert store.get("github-agent-policy", "acme").version == 1
        with pytest.raises(RegistryUnavailableError):
            Registry.from_dsn("postgresql://localhost/pulsekv_registry")

    def test_a_hand_edited_row_fails_loudly_on_read(self, tmp_path):
        # Records are re-validated through the frozen type on the way out, so a
        # row that no longer satisfies the contract does not flow onward as if
        # it did.
        path = tmp_path / "registry.db"
        with Registry(path) as store:
            store.register(make_record())
        raw = sqlite3.connect(str(path))
        try:
            # Not a column the immutability trigger guards, so this write lands.
            raw.execute("UPDATE canonical_context SET deprecated_at = '1999-01-01T00:00:00+00:00'")
            raw.commit()
        finally:
            raw.close()
        with Registry(path) as store:
            with pytest.raises(Exception) as caught:
                store.get("github-agent-policy", "acme")
            assert "deprecated_at" in str(caught.value)


class TestPhaseBoundary:
    """Phase 10.1 stores; it does not match, embed, or search."""

    def test_find_candidates_is_still_phase_103(self, registry):
        with pytest.raises(NotImplementedError) as caught:
            registry.find_candidates(
                namespace="acme", block_type=BlockType.ORG_POLICY, top_k=5
            )
        assert "10.3" in str(caught.value)

    def test_the_embedding_is_stored_verbatim_and_never_interpreted(self, registry):
        blob = bytes(range(256))
        registry.register(
            make_record(
                embedding=blob,
                embedding_model_id="bge-small-en",
                embedding_model_version="1.5",
            )
        )
        assert registry.get("github-agent-policy", "acme").embedding == blob

    def test_the_registry_module_imports_nothing_beyond_the_standard_library(self):
        import ast

        from pulsekv_gateway import registry as module

        tree = ast.parse(Path(module.__file__).read_text(), filename=module.__file__)
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        # `.models` is a relative import (node.module is "models" with level 1);
        # everything else must be stdlib, keeping pyproject.toml's "no database
        # driver" property true through this phase.
        assert imported <= {
            "__future__",
            "contextlib",
            "datetime",
            "hashlib",
            "models",
            "pathlib",
            "sqlite3",
            "threading",
            "typing",
        }, sorted(imported)
