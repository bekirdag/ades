from pathlib import Path

from tests.pack_registry_helpers import create_pack_source

from ades.storage.paths import build_storage_layout, ensure_storage_layout
from ades.storage.postgresql import PostgreSQLMetadataStore


class _RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def __enter__(self) -> "_RecordingConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, statement: str, params: tuple[object, ...] | list[object] = ()):
        self.statements.append(statement)
        return self


class _RecordingCopy:
    def __init__(self, sql: str, sink: list[tuple[str, object]]) -> None:
        self.sql = sql
        self.sink = sink

    def __enter__(self) -> "_RecordingCopy":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def write_row(self, row: tuple[object, ...]) -> None:
        self.sink.append(("row", row))


class _RecordingCursor:
    def __init__(self, *, supports_copy: bool) -> None:
        self.supports_copy = supports_copy
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.copy_calls: list[str] = []
        self.copy_rows: list[tuple[str, object]] = []
        if supports_copy:
            self.copy = self._copy  # type: ignore[assignment]

    def __enter__(self) -> "_RecordingCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def executemany(
        self,
        statement: str,
        rows: list[tuple[object, ...]],
    ) -> None:
        self.executemany_calls.append((statement, list(rows)))

    def _copy(self, statement: str) -> _RecordingCopy:
        self.copy_calls.append(statement)
        return _RecordingCopy(statement, self.copy_rows)


class _AliasReplaceConnection:
    def __init__(self, *, supports_copy: bool) -> None:
        self.executed: list[tuple[str, tuple[object, ...] | list[object]]] = []
        self.cursor_impl = _RecordingCursor(supports_copy=supports_copy)

    def execute(self, statement: str, params: tuple[object, ...] | list[object] = ()):
        self.executed.append((statement, params))
        return self

    def cursor(self) -> _RecordingCursor:
        return self.cursor_impl


class _LookupConnection:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, list[object]]] = []

    def __enter__(self) -> "_LookupConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, statement: str, params: tuple[object, ...] | list[object] = ()):
        self.calls.append((statement, list(params)))
        return self

    def fetchall(self) -> list[dict[str, object]]:
        return list(self.rows)


def test_postgresql_ensure_schema_includes_alias_column_migrations(tmp_path: Path) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    recorder = _RecordingConnection()
    store = object.__new__(PostgreSQLMetadataStore)
    store.layout = layout
    store.database_url = "postgresql://example.invalid/ades"
    store._connect = lambda: recorder  # type: ignore[method-assign]
    store._schema_state = lambda connection: (False, False)  # type: ignore[method-assign]

    store.ensure_schema()

    assert any(
        "ADD COLUMN IF NOT EXISTS normalized_alias_text" in statement
        for statement in recorder.statements
    )
    assert any(
        "ADD COLUMN IF NOT EXISTS entity_id" in statement
        for statement in recorder.statements
    )
    assert any(
        "CREATE EXTENSION IF NOT EXISTS pg_trgm" in statement
        for statement in recorder.statements
    )
    assert any(
        "SELECT pg_advisory_lock" in statement
        for statement in recorder.statements
    )
    assert any(
        "SELECT pg_advisory_unlock" in statement
        for statement in recorder.statements
    )
    assert any(
        "idx_pack_aliases_alias_text_trgm" in statement
        for statement in recorder.statements
    )
    assert any(
        "idx_pack_rules_rule_name_trgm" in statement
        for statement in recorder.statements
    )


def test_postgresql_ensure_schema_skips_runtime_ddl_when_schema_current(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    recorder = _RecordingConnection()
    store = object.__new__(PostgreSQLMetadataStore)
    store.layout = layout
    store.database_url = "postgresql://example.invalid/ades"
    store._connect = lambda: recorder  # type: ignore[method-assign]
    store._schema_state = lambda connection: (True, True)  # type: ignore[method-assign]

    store.ensure_schema()

    assert recorder.statements == []
    assert store._pg_trgm_enabled is True


def test_postgresql_fuzzy_alias_lookup_uses_pg_trgm_when_enabled() -> None:
    rows = [
        {
            "pack_id": "finance-en",
            "alias_text": "Org Beta",
            "label": "organization",
            "canonical_text": "Org Beta Holdings",
            "alias_score": 0.92,
            "generated": False,
            "source_name": "fixture",
            "entity_id": "fixture:1",
            "source_priority": 0.8,
            "popularity_weight": 0.7,
            "source_domain": "finance",
            "active": True,
            "match_score": 0.63,
        }
    ]
    connection = _LookupConnection(rows)
    store = object.__new__(PostgreSQLMetadataStore)
    store._pg_trgm_enabled = True
    store._connect = lambda: connection  # type: ignore[method-assign]

    candidates = store._lookup_alias_candidates(
        "org bta",
        pack_id=None,
        exact_alias=False,
        fuzzy=True,
        active_only=True,
        limit=10,
    )

    assert candidates
    statement, params = connection.calls[0]
    assert "similarity(a.alias_text" in statement
    assert "match_score" in statement
    assert "LIKE" not in statement
    assert params[-2] == 0.2
    assert candidates[0]["value"] == "Org Beta"


def test_postgresql_fuzzy_alias_lookup_falls_back_without_pg_trgm() -> None:
    rows = [
        {
            "pack_id": "finance-en",
            "alias_text": "Org Beta",
            "label": "organization",
            "canonical_text": "Org Beta Holdings",
            "alias_score": 0.92,
            "generated": False,
            "source_name": "fixture",
            "entity_id": "fixture:1",
            "source_priority": 0.8,
            "popularity_weight": 0.7,
            "source_domain": "finance",
            "active": True,
        }
    ]
    connection = _LookupConnection(rows)
    store = object.__new__(PostgreSQLMetadataStore)
    store._pg_trgm_enabled = False
    store._connect = lambda: connection  # type: ignore[method-assign]

    candidates = store._lookup_alias_candidates(
        "org beta",
        pack_id=None,
        exact_alias=False,
        fuzzy=True,
        active_only=True,
        limit=10,
    )

    assert candidates
    statement, _params = connection.calls[0]
    assert "LIKE" in statement
    assert "similarity(" not in statement


def test_postgresql_sync_from_filesystem_ignores_hidden_backup_directories(
    tmp_path: Path,
) -> None:
    visible_pack = create_pack_source(tmp_path, pack_id="general-en", domain="general")
    hidden_pack = create_pack_source(tmp_path, pack_id=".general-en.backup", domain="general")
    captured: list[str] = []

    store = object.__new__(PostgreSQLMetadataStore)
    store.layout = ensure_storage_layout(build_storage_layout(tmp_path))
    store.sync_pack_from_dir = lambda pack_dir: captured.append(pack_dir.name)  # type: ignore[method-assign]

    PostgreSQLMetadataStore.sync_from_filesystem(store, tmp_path)

    assert visible_pack.name in captured
    assert hidden_pack.name not in captured


def test_postgresql_manifest_rows_ignore_hidden_backup_paths(tmp_path: Path) -> None:
    hidden_pack = create_pack_source(tmp_path, pack_id=".general-en.backup", domain="general")
    store = object.__new__(PostgreSQLMetadataStore)

    manifest = PostgreSQLMetadataStore._manifest_from_row(
        store,
        None,
        {
            "manifest_path": str(hidden_pack / "manifest.json"),
            "sha256": None,
            "active": True,
            "install_path": str(hidden_pack),
            "installed_at": "2026-04-13T00:00:00+00:00",
        },
    )

    assert manifest is None


def test_postgresql_replace_aliases_uses_copy_when_available() -> None:
    connection = _AliasReplaceConnection(supports_copy=True)
    store = object.__new__(PostgreSQLMetadataStore)

    PostgreSQLMetadataStore._replace_aliases(
        store,
        connection,
        "general-en",
        [
            {
                "text": "Daniel Loeb",
                "label": "person",
                "normalized_text": "daniel loeb",
                "canonical_text": "Daniel S. Loeb",
                "alias_score": 0.91,
                "generated": False,
                "source_name": "wikidata-general-entities",
                "entity_id": "wikidata:Q5218659",
                "source_priority": 0.8,
                "popularity_weight": 0.7,
                "source_domain": "general",
            }
        ],
    )

    assert any(
        "DELETE FROM pack_aliases" in statement for statement, _params in connection.executed
    )
    assert connection.cursor_impl.copy_calls
    assert "COPY pack_aliases" in connection.cursor_impl.copy_calls[0]
    assert len(connection.cursor_impl.copy_rows) == 1
    copied_row = connection.cursor_impl.copy_rows[0][1]
    assert copied_row[0] == "general-en"
    assert copied_row[1] == "Daniel Loeb"
    assert copied_row[3] == "daniel loeb"
    assert copied_row[-1] == 0
    assert connection.cursor_impl.executemany_calls == []


def test_postgresql_replace_aliases_falls_back_to_executemany_without_copy() -> None:
    connection = _AliasReplaceConnection(supports_copy=False)
    store = object.__new__(PostgreSQLMetadataStore)

    PostgreSQLMetadataStore._replace_aliases(
        store,
        connection,
        "general-en",
        [
            {
                "text": "Third Point",
                "label": "organization",
                "normalized_text": "third point",
                "canonical_text": "Third Point Management",
                "alias_score": 0.88,
                "generated": False,
                "source_name": "wikidata-general-entities",
                "entity_id": "wikidata:Q17164492",
                "source_priority": 0.75,
                "popularity_weight": 0.6,
                "source_domain": "general",
            }
        ],
    )

    assert connection.cursor_impl.copy_calls == []
    assert len(connection.cursor_impl.executemany_calls) == 1
    statement, rows = connection.cursor_impl.executemany_calls[0]
    assert "INSERT INTO pack_aliases" in statement
    assert len(rows) == 1
    assert rows[0][0] == "general-en"
    assert rows[0][1] == "Third Point"
