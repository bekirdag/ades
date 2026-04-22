from pathlib import Path

import ades.storage.postgresql as postgresql_module
from tests.pack_registry_helpers import create_pack_source

from ades.service.models import VectorIndexBuildResponse
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


class _ParameterizedRecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | list[object]]] = []

    def __enter__(self) -> "_ParameterizedRecordingConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, statement: str, params: tuple[object, ...] | list[object] = ()):
        self.calls.append((statement, params))
        return self


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
        "CREATE TABLE IF NOT EXISTS vector_build_jobs" in statement
        for statement in recorder.statements
    )
    assert any(
        "CREATE TABLE IF NOT EXISTS vector_collection_releases" in statement
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
    normalized_alias_column_index = next(
        index
        for index, statement in enumerate(recorder.statements)
        if "ADD COLUMN IF NOT EXISTS normalized_alias_text" in statement
    )
    alias_lookup_index = next(
        index
        for index, statement in enumerate(recorder.statements)
        if "CREATE INDEX IF NOT EXISTS idx_pack_aliases_lookup" in statement
    )
    assert normalized_alias_column_index < alias_lookup_index


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


def test_postgresql_record_vector_build_persists_release_state() -> None:
    recorder = _ParameterizedRecordingConnection()
    store = object.__new__(PostgreSQLMetadataStore)
    store._connect = lambda: recorder  # type: ignore[method-assign]

    result = VectorIndexBuildResponse(
        output_dir="/mnt/githubActions/ades_big_data/vector_indexes/general-en",
        manifest_path="/mnt/githubActions/ades_big_data/vector_indexes/general-en/qid_graph_index_manifest.json",
        artifact_path="/mnt/githubActions/ades_big_data/vector_indexes/general-en/qid_graph_points.jsonl.gz",
        collection_name="ades-qids-20260419",
        alias_name="ades-qids-current",
        qdrant_url="http://qdrant.local:6333",
        published=True,
        dimensions=384,
        point_count=125,
        target_entity_count=125,
        bundle_count=1,
        bundle_dirs=["/mnt/githubActions/ades_big_data/pack_sources/bundles/general-en"],
        pack_ids=["general-en"],
        truthy_path="/mnt/githubActions/ades_big_data/pack_sources/raw/wikidata_truthy.nt.gz",
        processed_line_count=1000,
        matched_statement_count=250,
        allowed_predicates=["P31", "P463"],
        warnings=["skipped_non_wikidata:general-en:4"],
    )

    build_id = store.record_vector_build(result, build_id="vector-build:test")

    assert build_id == "vector-build:test"
    assert len(recorder.calls) == 2
    insert_statement, insert_params = recorder.calls[0]
    release_statement, release_params = recorder.calls[1]
    assert "INSERT INTO vector_build_jobs" in insert_statement
    assert insert_params[0] == "vector-build:test"
    assert insert_params[1] == "ades-qids-20260419"
    assert insert_params[2] == "ades-qids-current"
    assert insert_params[10] == "[\"/mnt/githubActions/ades_big_data/pack_sources/bundles/general-en\"]"
    assert insert_params[11] == "[\"general-en\"]"
    assert insert_params[15] == "[\"P31\", \"P463\"]"
    assert insert_params[16] == "[\"skipped_non_wikidata:general-en:4\"]"
    assert "INSERT INTO vector_collection_releases" in release_statement
    assert release_params[0] == "ades-qids-current"
    assert release_params[1] == "ades-qids-20260419"
    assert release_params[2] == "vector-build:test"


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


def test_postgresql_load_aliases_streams_pretty_printed_pack_fixture(
    tmp_path: Path,
) -> None:
    pack_dir = create_pack_source(
        tmp_path,
        pack_id="general-en",
        domain="general",
        aliases=(
            ("Daniel Loeb", "person"),
            ("Third Point", "organization"),
        ),
    )

    aliases = PostgreSQLMetadataStore._load_aliases(pack_dir, "general")

    assert aliases == [
        {
            "text": "Daniel Loeb",
            "label": "person",
            "normalized_text": "daniel loeb",
            "canonical_text": "Daniel Loeb",
            "alias_score": 1.0,
            "generated": False,
            "source_name": "",
            "entity_id": "",
            "source_priority": 0.6,
            "popularity_weight": 0.5,
            "source_domain": "general",
        },
        {
            "text": "Third Point",
            "label": "organization",
            "normalized_text": "third point",
            "canonical_text": "Third Point",
            "alias_score": 1.0,
            "generated": False,
            "source_name": "",
            "entity_id": "",
            "source_priority": 0.6,
            "popularity_weight": 0.5,
            "source_domain": "general",
        },
    ]


def test_postgresql_iter_alias_payload_items_supports_line_oriented_json(
    tmp_path: Path,
) -> None:
    path = tmp_path / "aliases.json"
    path.write_text(
        "{\n"
        '  "aliases": [\n'
        '    {"text":"Daniel Loeb","label":"person","canonical_text":"Daniel S. Loeb"},\n'
        '    {"text":"Third Point","label":"organization","score":0.88}\n'
        "  ]\n"
        "}\n",
        encoding="utf-8",
    )

    aliases = [
        PostgreSQLMetadataStore._coerce_alias_record(item, domain="general")
        for item in PostgreSQLMetadataStore._iter_alias_payload_items(path)
    ]

    assert aliases[0]["canonical_text"] == "Daniel S. Loeb"
    assert aliases[0]["normalized_text"] == "daniel loeb"
    assert aliases[1]["alias_score"] == 0.88
    assert aliases[1]["normalized_text"] == "third point"


def test_postgresql_replace_aliases_batches_generator_input_without_copy(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        postgresql_module,
        "_ALIAS_EXECUTEMANY_BATCH_SIZE",
        2,
    )
    connection = _AliasReplaceConnection(supports_copy=False)
    store = object.__new__(PostgreSQLMetadataStore)

    def alias_rows():
        for index in range(5):
            yield {
                "text": f"Alias {index}",
                "label": "organization",
                "normalized_text": f"alias {index}",
                "canonical_text": f"Alias Canonical {index}",
                "alias_score": 0.8,
                "generated": False,
                "source_name": "fixture",
                "entity_id": f"fixture:{index}",
                "source_priority": 0.7,
                "popularity_weight": 0.6,
                "source_domain": "general",
            }

    PostgreSQLMetadataStore._replace_aliases(
        store,
        connection,
        "general-en",
        alias_rows(),
    )

    batch_sizes = [
        len(rows)
        for _statement, rows in connection.cursor_impl.executemany_calls
    ]
    assert batch_sizes == [2, 2, 1]
    flattened_rows = [
        row
        for _statement, rows in connection.cursor_impl.executemany_calls
        for row in rows
    ]
    assert [row[1] for row in flattened_rows] == [
        "Alias 0",
        "Alias 1",
        "Alias 2",
        "Alias 3",
        "Alias 4",
    ]
