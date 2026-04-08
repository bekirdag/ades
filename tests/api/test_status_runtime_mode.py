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
