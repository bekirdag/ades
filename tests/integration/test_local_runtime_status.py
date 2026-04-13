from pathlib import Path

import pytest

from ades import pull_pack, status
from ades.storage.registry_db import PackMetadataStore
from ades.storage import UnsupportedRuntimeConfigurationError


def test_local_runtime_status_reports_local_sqlite_mode(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    runtime_status = status(storage_root=tmp_path)

    assert runtime_status.runtime_target == "local"
    assert runtime_status.metadata_backend == "sqlite"
    assert runtime_status.metadata_persistence_backend == "sqlite"
    assert runtime_status.exact_extraction_backend == "compiled_matcher"
    assert runtime_status.operator_lookup_backend == "sqlite_search_index"
    assert "finance-en" in runtime_status.installed_packs


def test_local_runtime_status_rejects_missing_explicit_config(
    tmp_path: Path, monkeypatch
) -> None:
    missing_config = tmp_path / "missing.toml"
    monkeypatch.setenv("ADES_CONFIG_FILE", str(missing_config))

    with pytest.raises(
        FileNotFoundError,
        match=f"ades config file not found: {missing_config}",
    ):
        status(storage_root=tmp_path)


def test_local_runtime_status_rejects_invalid_runtime_target(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "ades.toml"
    config_path.write_text("", encoding="utf-8")
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("ADES_RUNTIME_TARGET", "broken-runtime")

    with pytest.raises(
        UnsupportedRuntimeConfigurationError,
        match="Unsupported ades runtime target",
    ):
        status(storage_root=tmp_path)


def test_local_runtime_status_does_not_sync_global_search_index(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fail_sync(self: PackMetadataStore, connection: object) -> None:
        raise AssertionError("status() must not rebuild the global search index")

    monkeypatch.setattr(PackMetadataStore, "_sync_search_index", fail_sync)

    runtime_status = status(storage_root=tmp_path)

    assert runtime_status.runtime_target == "local"
    assert runtime_status.exact_extraction_backend == "compiled_matcher"


def test_status_reports_production_postgresql_mode_with_database_url(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class _FakePack:
        pack_id = "general-en"

    class _FakePlan:
        metadata_persistence_backend = type("_Value", (), {"value": "postgresql"})()
        exact_extraction_backend = type("_Value", (), {"value": "compiled_matcher"})()
        operator_lookup_backend = type("_Value", (), {"value": "postgresql_search"})()

    class _FakeRegistry:
        def __init__(self, *args, **kwargs) -> None:
            self.backend_plan = _FakePlan()

        def list_installed_packs(self, *, active_only: bool = False):
            return [_FakePack()]

    monkeypatch.setenv("ADES_RUNTIME_TARGET", "production_server")
    monkeypatch.setenv("ADES_METADATA_BACKEND", "postgresql")
    monkeypatch.setenv("ADES_DATABASE_URL", "postgresql://db.example/ades")
    monkeypatch.setattr("ades.api.PackRegistry", _FakeRegistry)

    runtime_status = status(storage_root=tmp_path)

    assert runtime_status.runtime_target == "production_server"
    assert runtime_status.metadata_backend == "postgresql"
    assert runtime_status.metadata_persistence_backend == "postgresql"
    assert runtime_status.exact_extraction_backend == "compiled_matcher"
    assert runtime_status.operator_lookup_backend == "postgresql_search"
    assert runtime_status.installed_packs == ["general-en"]
