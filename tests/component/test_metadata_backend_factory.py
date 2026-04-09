from pathlib import Path

import pytest

from ades.storage import (
    MetadataBackend,
    RuntimeTarget,
    UnsupportedRuntimeConfigurationError,
    build_metadata_store,
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
