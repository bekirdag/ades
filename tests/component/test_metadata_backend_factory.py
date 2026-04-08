from pathlib import Path

import pytest

from ades.storage import (
    MetadataBackend,
    RuntimeTarget,
    UnsupportedRuntimeConfigurationError,
    build_metadata_store,
)
from ades.storage.paths import build_storage_layout, ensure_storage_layout


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


def test_backend_factory_exposes_postgresql_placeholder(tmp_path: Path) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))

    with pytest.raises(NotImplementedError):
        build_metadata_store(
            layout,
            runtime_target=RuntimeTarget.PRODUCTION_SERVER,
            metadata_backend=MetadataBackend.POSTGRESQL,
        )
