from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app


def test_available_packs_endpoint_lists_registry_metadata(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.get("/v0/packs/available")

    assert response.status_code == 200
    finance_pack = next(pack for pack in response.json() if pack["pack_id"] == "finance-en")
    assert finance_pack["description"]
    assert "finance" in finance_pack["tags"]
