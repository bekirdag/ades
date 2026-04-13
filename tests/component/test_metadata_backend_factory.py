from pathlib import Path

import pytest

from ades.storage import (
    ExactExtractionBackend,
    MetadataBackend,
    OperatorLookupBackend,
    RuntimeTarget,
    UnsupportedRuntimeConfigurationError,
    build_metadata_store,
    resolve_backend_plan,
)
from ades.storage.paths import build_storage_layout, ensure_storage_layout
from tests.pack_registry_helpers import create_finance_registry_sources


def test_backend_factory_returns_sqlite_store_for_local_runtime(tmp_path: Path) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))

    store = build_metadata_store(
        layout,
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
    )

    assert store.__class__.__name__ == "PackMetadataStore"


def test_backend_factory_rejects_invalid_runtime_backend_pair(tmp_path: Path) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))

    with pytest.raises(UnsupportedRuntimeConfigurationError):
        build_metadata_store(
            layout,
            runtime_target=RuntimeTarget.LOCAL,
            metadata_backend=MetadataBackend.POSTGRESQL,
        )


def test_backend_factory_requires_postgresql_configuration(tmp_path: Path) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))

    with pytest.raises(RuntimeError):
        build_metadata_store(
            layout,
            runtime_target=RuntimeTarget.PRODUCTION_SERVER,
            metadata_backend=MetadataBackend.POSTGRESQL,
        )


def test_backend_factory_sqlite_store_supports_delete_pack_contract(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    create_finance_registry_sources(layout.packs_dir)
    store = build_metadata_store(
        layout,
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
    )

    store.sync_from_filesystem(layout.packs_dir)

    assert [pack.pack_id for pack in store.list_installed_packs()] == [
        "finance-en",
        "general-en",
    ]
    assert store.delete_pack("finance-en") is True
    assert [pack.pack_id for pack in store.list_installed_packs()] == ["general-en"]
    assert store.delete_pack("finance-en") is False


def test_backend_plan_is_explicit_for_local_sqlite_runtime() -> None:
    plan = resolve_backend_plan(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
    )

    assert plan.exact_extraction_backend is ExactExtractionBackend.COMPILED_MATCHER
    assert plan.operator_lookup_backend is OperatorLookupBackend.SQLITE_SEARCH_INDEX


def test_backend_factory_returns_postgresql_store_for_production_runtime(
    tmp_path: Path,
    monkeypatch,
) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    captured: dict[str, object] = {}

    class _FakePostgreSQLMetadataStore:
        def __init__(self, resolved_layout, *, database_url: str | None) -> None:
            captured["layout"] = resolved_layout
            captured["database_url"] = database_url

    monkeypatch.setattr(
        "ades.storage.postgresql.PostgreSQLMetadataStore",
        _FakePostgreSQLMetadataStore,
    )

    store = build_metadata_store(
        layout,
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://db.example/ades",
    )

    assert store.__class__.__name__ == "_FakePostgreSQLMetadataStore"
    assert captured == {
        "layout": layout,
        "database_url": "postgresql://db.example/ades",
    }
