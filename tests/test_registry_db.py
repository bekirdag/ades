import json
import sqlite3
from pathlib import Path

from ades.packs.installer import PackInstaller
from ades.packs.registry import PackRegistry
from ades.storage.backend import MetadataBackend
from ades.storage.registry_db import (
    SQLITE_BUSY_TIMEOUT_MS,
    PackMetadataStore,
)
from ades.pipeline.tagger import tag_text
from ades.storage.paths import build_storage_layout, ensure_storage_layout
from tests.pack_generation_helpers import (
    create_bundle_backed_general_pack_source,
    create_bundle_backed_general_pack_source_with_alias_collision,
)
from tests.pack_registry_helpers import create_pack_source


def test_pack_metadata_is_synced_to_sqlite(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    layout = build_storage_layout(tmp_path)

    assert layout.registry_db.exists()

    connection = sqlite3.connect(layout.registry_db)
    try:
        packs = {
            row[0]: row[1]
            for row in connection.execute(
                "SELECT pack_id, active FROM installed_packs ORDER BY pack_id ASC"
            ).fetchall()
        }
        dependencies = connection.execute(
            """
            SELECT dependency_pack_id
            FROM pack_dependencies
            WHERE pack_id = 'finance-en'
            """
        ).fetchall()
        aliases = connection.execute(
            """
            SELECT alias_text, label
            FROM pack_aliases
            WHERE pack_id = 'finance-en'
            ORDER BY position ASC
            """
        ).fetchall()
        rules = connection.execute(
            """
            SELECT rule_name, label
            FROM pack_rules
            WHERE pack_id = 'finance-en'
            ORDER BY position ASC
            """
        ).fetchall()
    finally:
        connection.close()

    assert packs == {"finance-en": 1, "general-en": 1}
    assert dependencies == [("general-en",)]
    assert ("TICKA", "ticker") in aliases
    assert ("currency_amount", "currency_amount") in rules


def test_inactive_pack_is_not_used_by_runtime(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    registry = PackRegistry(tmp_path)

    finance_pack = registry.get_pack("finance-en")
    assert finance_pack is not None
    assert finance_pack.active is True

    assert registry.set_pack_active("finance-en", False) is True

    inactive_pack = registry.get_pack("finance-en")
    assert inactive_pack is not None
    assert inactive_pack.active is False
    assert registry.get_pack("finance-en", active_only=True) is None

    response = tag_text(
        text="TICKA rallied on EXCHX after USD 12.5 guidance.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )
    assert "pack_not_installed:finance-en" in response.warnings


def test_registry_lookup_candidates_respects_active_state(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    registry = PackRegistry(tmp_path)

    exact_alias = registry.lookup_candidates("TICKA", exact_alias=True)
    assert exact_alias
    assert exact_alias[0]["kind"] == "alias"
    assert exact_alias[0]["value"] == "TICKA"

    rules = registry.lookup_candidates("currency_amount")
    assert any(candidate["kind"] == "rule" for candidate in rules)

    registry.set_pack_active("finance-en", False)
    hidden = registry.lookup_candidates("TICKA", exact_alias=True)
    assert hidden == []
    visible = registry.lookup_candidates("TICKA", exact_alias=True, active_only=False)
    assert visible


def test_registry_sqlite_uses_wal_and_exposes_alias_metadata(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    layout = build_storage_layout(tmp_path)

    connection = sqlite3.connect(layout.registry_db)
    try:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
    finally:
        connection.close()

    assert str(journal_mode).lower() == "wal"

    registry = PackRegistry(tmp_path)
    candidates = registry.lookup_candidates("TICKA", exact_alias=True)

    assert candidates
    alias_candidate = candidates[0]
    assert alias_candidate["canonical_text"] == "TICKA"
    assert alias_candidate["generated"] is False
    assert alias_candidate["entity_prior"] == 1.0
    assert alias_candidate["source_priority"] == 0.6
    assert alias_candidate["popularity_weight"] == 0.5


def test_pack_metadata_store_initialization_does_not_sync_global_search_index(
    tmp_path: Path,
    monkeypatch,
) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))

    def fail_sync(self: PackMetadataStore, connection: sqlite3.Connection) -> None:
        raise AssertionError("constructor must not rebuild the global search index")

    monkeypatch.setattr(PackMetadataStore, "_sync_search_index", fail_sync)

    store = PackMetadataStore(layout)

    assert store.count_installed_packs() == 0


def test_pack_metadata_store_connections_use_busy_timeout(tmp_path: Path) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    store = PackMetadataStore(layout)

    with store._connect() as connection:
        busy_timeout = connection.execute("PRAGMA busy_timeout").fetchone()[0]
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]

    assert int(busy_timeout) == SQLITE_BUSY_TIMEOUT_MS
    assert str(journal_mode).lower() == "wal"


def test_pack_registry_ignores_hidden_backup_pack_directories(tmp_path: Path) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    create_pack_source(layout.packs_dir, pack_id="general-en", domain="general")
    hidden_backup_dir = layout.packs_dir / ".general-en.backup"
    create_pack_source(hidden_backup_dir.parent, pack_id=hidden_backup_dir.name, domain="general")

    synced: list[str] = []

    class _Store:
        def sync_pack_from_dir(self, pack_dir: Path):
            synced.append(pack_dir.name)
            return None

    registry = object.__new__(PackRegistry)
    registry.layout = layout
    registry.store = _Store()

    repaired = PackRegistry._sync_missing_filesystem_packs(registry, [])

    assert repaired is True
    assert synced == ["general-en"]


def test_pack_registry_get_pack_falls_back_to_visible_manifest_without_full_sync(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    create_pack_source(layout.packs_dir, pack_id="general-en", domain="general")

    class _Store:
        def get_pack(self, pack_id: str, *, active_only: bool = False):
            return None

        def sync_pack_from_dir(self, pack_dir: Path):
            raise AssertionError("get_pack fallback must not trigger full pack sync")

    registry = object.__new__(PackRegistry)
    registry.layout = layout
    registry.store = _Store()

    manifest = PackRegistry.get_pack(registry, "general-en")

    assert manifest is not None
    assert manifest.pack_id == "general-en"


def test_pack_registry_list_installed_packs_uses_manifest_fallback_without_sync_in_postgresql_mode(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    create_pack_source(layout.packs_dir, pack_id="general-en", domain="general")

    class _Store:
        def list_installed_packs(self, *, active_only: bool = False):
            return []

        def count_installed_packs(self) -> int:
            raise AssertionError("postgresql status fallback must not count-sync metadata")

        def sync_from_filesystem(self, packs_dir: Path | None = None) -> None:
            raise AssertionError("postgresql status fallback must not sync packs from disk")

        def sync_pack_from_dir(self, pack_dir: Path):
            raise AssertionError("postgresql status fallback must not repair metadata")

    registry = object.__new__(PackRegistry)
    registry.layout = layout
    registry.store = _Store()
    registry.metadata_backend = MetadataBackend.POSTGRESQL

    packs = PackRegistry.list_installed_packs(registry)

    assert [pack.pack_id for pack in packs] == ["general-en"]


def test_pack_registry_lookup_skips_implicit_sync_in_postgresql_mode(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    create_pack_source(layout.packs_dir, pack_id="finance-en", domain="finance")

    class _Store:
        def lookup_candidates(
            self,
            query: str,
            *,
            pack_id: str | None = None,
            exact_alias: bool = False,
            fuzzy: bool = False,
            active_only: bool = True,
            limit: int = 20,
        ):
            return []

        def get_pack(self, pack_id: str, *, active_only: bool = False):
            return None

        def sync_pack_from_dir(self, pack_dir: Path):
            raise AssertionError("postgresql lookup must not trigger implicit pack sync")

    registry = object.__new__(PackRegistry)
    registry.layout = layout
    registry.store = _Store()
    registry.metadata_backend = MetadataBackend.POSTGRESQL

    candidates = PackRegistry.lookup_candidates(
        registry,
        "AAPL",
        pack_id="finance-en",
        exact_alias=True,
    )

    assert candidates == []


def test_pack_alias_column_migration_tolerates_duplicate_column_race() -> None:
    class _Connection:
        def __init__(self) -> None:
            self.alter_attempts: list[str] = []

        def execute(self, statement: str):
            if statement == "PRAGMA table_info(pack_aliases)":
                class _Rows:
                    @staticmethod
                    def fetchall():
                        return [
                            (0, "pack_id", "TEXT", 1, None, 0),
                            (1, "alias_text", "TEXT", 1, None, 0),
                            (2, "label", "TEXT", 1, None, 0),
                        ]

                return _Rows()
            if statement.startswith("ALTER TABLE pack_aliases ADD COLUMN normalized_alias_text"):
                self.alter_attempts.append(statement)
                raise sqlite3.OperationalError("duplicate column name: normalized_alias_text")
            self.alter_attempts.append(statement)
            return None

    connection = _Connection()

    PackMetadataStore._ensure_pack_alias_columns(connection)  # type: ignore[arg-type]

    assert connection.alter_attempts


def test_sync_pack_streams_aliases_json_without_eager_json_loads(
    tmp_path: Path, monkeypatch
) -> None:
    pack_dir = create_pack_source(
        tmp_path,
        pack_id="stream-pack",
        domain="general",
        aliases=(("ALPHA", "organization"), ("BETA", "person")),
    )
    layout = ensure_storage_layout(build_storage_layout(tmp_path / "install"))
    store = PackMetadataStore(layout)

    original_loads = json.loads

    def guarded_loads(payload: str, *args: object, **kwargs: object) -> object:
        if '"aliases"' in payload:
            raise AssertionError("aliases.json should not be eagerly decoded")
        return original_loads(payload, *args, **kwargs)

    monkeypatch.setattr("ades.storage.registry_db.json.loads", guarded_loads)

    store.sync_pack_from_dir(pack_dir)

    assert any(
        candidate["value"] == "ALPHA" and candidate["label"] == "organization"
        for candidate in store.lookup_candidates("ALPHA", exact_alias=True, active_only=False)
    )
    assert any(
        candidate["value"] == "BETA" and candidate["label"] == "person"
        for candidate in store.lookup_candidates("BETA", exact_alias=True, active_only=False)
    )


def test_sync_pack_accepts_aliases_jsonl(tmp_path: Path) -> None:
    pack_dir = create_pack_source(
        tmp_path,
        pack_id="jsonl-pack",
        domain="general",
    )
    aliases_path = pack_dir / "aliases.json"
    aliases_path.unlink()
    (pack_dir / "aliases.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"text": "GAMMA", "label": "organization"}),
                json.dumps({"text": "DELTA", "label": "person"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    layout = ensure_storage_layout(build_storage_layout(tmp_path / "install"))
    store = PackMetadataStore(layout)
    store.sync_pack_from_dir(pack_dir)

    assert any(
        candidate["value"] == "GAMMA" and candidate["label"] == "organization"
        for candidate in store.lookup_candidates("GAMMA", exact_alias=True, active_only=False)
    )
    assert any(
        candidate["value"] == "DELTA" and candidate["label"] == "person"
        for candidate in store.lookup_candidates("DELTA", exact_alias=True, active_only=False)
    )


def test_sync_pack_accepts_bundle_entities_jsonl(tmp_path: Path) -> None:
    pack_dir = create_bundle_backed_general_pack_source(tmp_path)

    layout = ensure_storage_layout(build_storage_layout(tmp_path / "install"))
    store = PackMetadataStore(layout)
    store.sync_pack_from_dir(pack_dir)

    assert any(
        candidate["value"] == "Jordan Vale" and candidate["label"] == "person"
        for candidate in store.lookup_candidates(
            "Jordan Vale",
            exact_alias=True,
            active_only=False,
        )
    )
    assert any(
        candidate["value"] == "Beacon" and candidate["label"] == "organization"
        for candidate in store.lookup_candidates(
            "Beacon",
            exact_alias=True,
            active_only=False,
        )
    )
    assert any(
        candidate["value"] == "North Harbor" and candidate["label"] == "location"
        for candidate in store.lookup_candidates(
            "North Harbor",
            exact_alias=True,
            active_only=False,
        )
    )


def test_sync_pack_bundle_entities_jsonl_resolves_alias_collisions(tmp_path: Path) -> None:
    pack_dir = create_bundle_backed_general_pack_source_with_alias_collision(tmp_path)

    layout = ensure_storage_layout(build_storage_layout(tmp_path / "install"))
    store = PackMetadataStore(layout)
    store.sync_pack_from_dir(pack_dir)

    candidates = store.lookup_candidates(
        "Jordan Vale",
        exact_alias=True,
        active_only=False,
    )

    person_candidates = [
        candidate for candidate in candidates if candidate["label"] == "person"
    ]
    assert len(person_candidates) == 1
    assert person_candidates[0]["entity_id"] == "person:jordan-elliott-vale-primary"
    assert person_candidates[0]["canonical_text"] == "Jordan Elliott Vale Prime"


def test_sync_pack_can_skip_search_index_for_large_bundle_alias_sets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_dir = create_bundle_backed_general_pack_source(tmp_path)

    monkeypatch.setattr("ades.storage.registry_db.MAX_ALIAS_SEARCH_INDEX_ROWS", 1)

    layout = ensure_storage_layout(build_storage_layout(tmp_path / "install"))
    store = PackMetadataStore(layout)
    store.sync_pack_from_dir(pack_dir)

    assert store._search_index_supported is False
    assert any(
        candidate["value"] == "Jordan Vale" and candidate["label"] == "person"
        for candidate in store.lookup_candidates(
            "Jordan Vale",
            exact_alias=True,
            active_only=False,
        )
    )


def test_sync_pack_flushes_alias_batches_incrementally(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_dir = create_pack_source(
        tmp_path,
        pack_id="batch-pack",
        domain="general",
        aliases=(
            ("ALPHA", "organization"),
            ("BETA", "person"),
            ("GAMMA", "location"),
        ),
    )
    layout = ensure_storage_layout(build_storage_layout(tmp_path / "install"))
    store = PackMetadataStore(layout)

    flush_sizes: list[int] = []
    original_flush = PackMetadataStore._flush_alias_batch

    def tracking_flush(
        cls,
        connection: sqlite3.Connection,
        alias_rows: list[tuple[object, ...]],
    ) -> None:
        flush_sizes.append(len(alias_rows))
        original_flush(connection=connection, alias_rows=alias_rows)  # type: ignore[misc]

    monkeypatch.setattr("ades.storage.registry_db.ALIAS_SYNC_BATCH_SIZE", 1)
    monkeypatch.setattr(
        PackMetadataStore,
        "_flush_alias_batch",
        classmethod(tracking_flush),
    )

    store.sync_pack_from_dir(pack_dir)

    assert flush_sizes == [1, 1, 1]
