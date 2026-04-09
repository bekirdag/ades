import sqlite3
from pathlib import Path

import pytest

from ades.storage.paths import build_storage_layout, ensure_storage_layout
from ades.storage.postgresql import PostgreSQLMetadataStore
from ades.storage.registry_db import PackMetadataStore
from tests.pack_registry_helpers import create_finance_registry_sources


class _SQLitePsycopgConnectionAdapter:
    def __init__(self, db_path: Path) -> None:
        self._connection = sqlite3.connect(db_path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")

    def __enter__(self) -> "_SQLitePsycopgConnectionAdapter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            self._connection.commit()
        else:
            self._connection.rollback()
        self._connection.close()
        return False

    def execute(
        self,
        statement: str,
        params: tuple[object, ...] | list[object] = (),
    ) -> sqlite3.Cursor:
        translated = statement.replace("%s", "?")
        return self._connection.execute(translated, tuple(params))


def _seed_store(tmp_path: Path) -> tuple[PackMetadataStore, Path]:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    create_finance_registry_sources(layout.packs_dir)
    store = PackMetadataStore(layout)
    store.sync_from_filesystem(layout.packs_dir)
    return store, layout.registry_db


def _build_sqlite_store(tmp_path: Path) -> tuple[object, Path]:
    return _seed_store(tmp_path)


def _build_postgresql_store(tmp_path: Path) -> tuple[object, Path]:
    sqlite_store, db_path = _seed_store(tmp_path)
    store = object.__new__(PostgreSQLMetadataStore)
    store.layout = sqlite_store.layout
    store.database_url = "postgresql://example.invalid/ades"
    store._connect = lambda: _SQLitePsycopgConnectionAdapter(db_path)  # type: ignore[method-assign]
    return store, db_path


def _pack_metadata_counts(db_path: Path, pack_id: str) -> dict[str, int]:
    with sqlite3.connect(db_path) as connection:
        return {
            "installed_packs": int(
                connection.execute(
                    "SELECT COUNT(*) FROM installed_packs WHERE pack_id = ?",
                    (pack_id,),
                ).fetchone()[0]
            ),
            "pack_dependencies": int(
                connection.execute(
                    "SELECT COUNT(*) FROM pack_dependencies WHERE pack_id = ?",
                    (pack_id,),
                ).fetchone()[0]
            ),
            "pack_labels": int(
                connection.execute(
                    "SELECT COUNT(*) FROM pack_labels WHERE pack_id = ?",
                    (pack_id,),
                ).fetchone()[0]
            ),
            "pack_rules": int(
                connection.execute(
                    "SELECT COUNT(*) FROM pack_rules WHERE pack_id = ?",
                    (pack_id,),
                ).fetchone()[0]
            ),
            "pack_aliases": int(
                connection.execute(
                    "SELECT COUNT(*) FROM pack_aliases WHERE pack_id = ?",
                    (pack_id,),
                ).fetchone()[0]
            ),
        }


def _installed_pack_ids(db_path: Path) -> list[str]:
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            "SELECT pack_id FROM installed_packs ORDER BY pack_id ASC"
        ).fetchall()
    return [str(row[0]) for row in rows]


@pytest.mark.parametrize(
    "store_builder",
    [_build_sqlite_store, _build_postgresql_store],
    ids=["sqlite", "postgresql"],
)
def test_delete_pack_contract_removes_pack_and_cascades_metadata(
    tmp_path: Path,
    store_builder,
) -> None:
    store, db_path = store_builder(tmp_path)

    assert _pack_metadata_counts(db_path, "finance-en") == {
        "installed_packs": 1,
        "pack_dependencies": 1,
        "pack_labels": 3,
        "pack_rules": 1,
        "pack_aliases": 2,
    }

    assert store.delete_pack("finance-en") is True
    assert store.delete_pack("finance-en") is False

    assert _installed_pack_ids(db_path) == ["general-en"]
    assert _pack_metadata_counts(db_path, "finance-en") == {
        "installed_packs": 0,
        "pack_dependencies": 0,
        "pack_labels": 0,
        "pack_rules": 0,
        "pack_aliases": 0,
    }
