from pathlib import Path

import ades.storage.postgresql as postgresql_module
from ades.storage.paths import build_storage_layout, ensure_storage_layout
from ades.storage.postgresql import PostgreSQLMetadataStore


class _FakeConnectionPool:
    created: list["_FakeConnectionPool"] = []

    def __init__(
        self,
        *,
        conninfo: str,
        kwargs: dict[str, object],
        min_size: int,
        max_size: int,
        open: bool,
        timeout: float,
        name: str,
    ) -> None:
        self.conninfo = conninfo
        self.kwargs = kwargs
        self.min_size = min_size
        self.max_size = max_size
        self.open = open
        self.timeout = timeout
        self.name = name
        self.wait_calls: list[float] = []
        self.close_calls = 0
        self.connection_marker = object()
        _FakeConnectionPool.created.append(self)

    def wait(self, *, timeout: float) -> None:
        self.wait_calls.append(timeout)

    def connection(self) -> object:
        return self.connection_marker

    def close(self) -> None:
        self.close_calls += 1


def test_shared_connection_pool_is_cached_by_database_url(monkeypatch) -> None:
    postgresql_module._close_shared_pools()
    _FakeConnectionPool.created.clear()
    monkeypatch.setattr(postgresql_module, "ConnectionPool", _FakeConnectionPool)

    pool_a = postgresql_module._get_shared_connection_pool("postgresql://db.example/ades")
    pool_b = postgresql_module._get_shared_connection_pool("postgresql://db.example/ades")

    assert pool_a is pool_b
    assert len(_FakeConnectionPool.created) == 1
    created_pool = _FakeConnectionPool.created[0]
    assert created_pool.kwargs == {"row_factory": postgresql_module.dict_row}
    assert created_pool.min_size == 2
    assert created_pool.max_size == 10
    assert created_pool.open is True
    assert created_pool.timeout == 30.0
    assert created_pool.name == "ades-metadata-store"
    assert created_pool.wait_calls == [30.0]

    postgresql_module._close_shared_pools()
    assert created_pool.close_calls == 1


def test_postgresql_store_constructor_uses_shared_pool(monkeypatch, tmp_path: Path) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    pool_marker = object()
    monkeypatch.setattr(postgresql_module, "psycopg", object())
    monkeypatch.setattr(
        postgresql_module,
        "_get_shared_connection_pool",
        lambda database_url: pool_marker,
    )
    ensure_schema_calls: list[str] = []
    monkeypatch.setattr(
        PostgreSQLMetadataStore,
        "ensure_schema",
        lambda self: ensure_schema_calls.append("called"),
    )

    store = PostgreSQLMetadataStore(layout, database_url="postgresql://db.example/ades")

    assert store._pool is pool_marker
    assert ensure_schema_calls == ["called"]


def test_postgresql_store_connect_prefers_pool_connection() -> None:
    store = object.__new__(PostgreSQLMetadataStore)
    store.database_url = "postgresql://db.example/ades"

    class _Pool:
        def __init__(self) -> None:
            self.marker = object()

        def connection(self) -> object:
            return self.marker

    store._pool = _Pool()

    assert store._connect() is store._pool.marker


def test_postgresql_store_connect_falls_back_to_direct_connection(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    class _FakePsycopg:
        @staticmethod
        def connect(database_url: str, *, row_factory: object) -> object:
            calls.append((database_url, row_factory))
            return {"database_url": database_url, "row_factory": row_factory}

    monkeypatch.setattr(postgresql_module, "psycopg", _FakePsycopg)
    store = object.__new__(PostgreSQLMetadataStore)
    store.database_url = "postgresql://db.example/ades"
    store._pool = None

    connection = store._connect()

    assert connection == {
        "database_url": "postgresql://db.example/ades",
        "row_factory": postgresql_module.dict_row,
    }
    assert calls == [("postgresql://db.example/ades", postgresql_module.dict_row)]
