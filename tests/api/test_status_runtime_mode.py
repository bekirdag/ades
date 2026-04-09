from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_status_endpoint_reports_local_sqlite_mode(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.get("/v0/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["runtime_target"] == "local"
    assert payload["metadata_backend"] == "sqlite"
    assert "finance-en" in payload["installed_packs"]


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
