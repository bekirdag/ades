from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from ades.storage.registry_db import PackMetadataStore


def test_lookup_endpoint_supports_multi_term_cross_field_search(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.get(
        "/v0/lookup",
        params={"q": "org beta organization", "limit": 10},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidates"]
    assert payload["candidates"][0]["kind"] == "alias"
    assert payload["candidates"][0]["value"] == "Org Beta"
    assert payload["candidates"][0]["label"] == "organization"


def test_lookup_endpoint_exact_lookup_does_not_sync_global_search_index(
    tmp_path: Path,
    monkeypatch,
) -> None:
    PackInstaller(tmp_path).install("finance-en")

    def fail_sync(self: PackMetadataStore, connection: object) -> None:
        raise AssertionError("/v0/lookup exact lookup must not rebuild the global search index")

    monkeypatch.setattr(PackMetadataStore, "_sync_search_index", fail_sync)
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.get(
        "/v0/lookup",
        params={"q": "TICKA", "exact_alias": True, "limit": 10},
    )

    assert response.status_code == 200


def test_lookup_endpoint_supports_explicit_fuzzy_mode(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.get(
        "/v0/lookup",
        params={"q": "org beta", "fuzzy": True, "limit": 10},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["fuzzy"] is True
    assert payload["candidates"]
