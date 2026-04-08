from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_lookup_endpoint_supports_multi_term_cross_field_search(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.get(
        "/v0/lookup",
        params={"q": "apple organization", "limit": 10},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidates"]
    assert payload["candidates"][0]["kind"] == "alias"
    assert payload["candidates"][0]["value"] == "Apple"
    assert payload["candidates"][0]["label"] == "organization"
