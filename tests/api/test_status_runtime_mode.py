from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from ades.storage.registry_db import PackMetadataStore


def test_status_endpoint_reports_local_sqlite_mode(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    with TestClient(create_app(storage_root=tmp_path)) as client:
        response = client.get("/v0/status")

        assert response.status_code == 200
        payload = response.json()
        assert payload["runtime_target"] == "local"
        assert payload["metadata_backend"] == "sqlite"
        assert payload["metadata_persistence_backend"] == "sqlite"
        assert payload["exact_extraction_backend"] == "compiled_matcher"
        assert payload["operator_lookup_backend"] == "sqlite_search_index"
        assert "finance-en" in payload["installed_packs"]
        assert "finance-en" in payload["prewarmed_packs"]


def test_status_endpoint_returns_404_for_missing_explicit_config(
    tmp_path: Path, monkeypatch
) -> None:
    missing_config = tmp_path / "missing.toml"
    client = TestClient(create_app(storage_root=tmp_path))
    monkeypatch.setenv("ADES_CONFIG_FILE", str(missing_config))

    response = client.get("/v0/status")

    assert response.status_code == 404
    assert response.json() == {
        "detail": f"ades config file not found: {missing_config}"
    }


def test_status_endpoint_returns_400_for_invalid_runtime_target(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "ades.toml"
    config_path.write_text("", encoding="utf-8")
    client = TestClient(create_app(storage_root=tmp_path))
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("ADES_RUNTIME_TARGET", "broken-runtime")

    response = client.get("/v0/status")

    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported ades runtime target: 'broken-runtime'"


def test_status_endpoint_does_not_sync_global_search_index(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fail_sync(self: PackMetadataStore, connection: object) -> None:
        raise AssertionError("/v0/status must not rebuild the global search index")

    monkeypatch.setattr(PackMetadataStore, "_sync_search_index", fail_sync)
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.get("/v0/status")

    assert response.status_code == 200
    assert response.json()["exact_extraction_backend"] == "compiled_matcher"


def test_status_endpoint_reports_production_postgresql_mode(
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
    monkeypatch.setattr("ades.service.app.PackRegistry", _FakeRegistry)
    monkeypatch.setattr("ades.service.app.load_pack_runtime", lambda *args, **kwargs: None)
    with TestClient(create_app(storage_root=tmp_path)) as client:
        response = client.get("/v0/status")

        assert response.status_code == 200
        payload = response.json()
        assert payload["runtime_target"] == "production_server"
        assert payload["metadata_backend"] == "postgresql"
        assert payload["metadata_persistence_backend"] == "postgresql"
        assert payload["exact_extraction_backend"] == "compiled_matcher"
        assert payload["operator_lookup_backend"] == "postgresql_search"
        assert payload["installed_packs"] == ["general-en"]
        assert payload["prewarmed_packs"] == []
