from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_registry_helpers import delete_installed_pack_metadata


def test_tag_endpoint_uses_lookup_for_dependency_aliases(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "Apple said AAPL traded on NASDAQ.",
            "pack": "finance-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("Apple", "organization") in pairs
    assert ("AAPL", "ticker") in pairs
    assert ("NASDAQ", "exchange") in pairs


def test_tag_endpoint_recovers_after_metadata_row_deletion(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    delete_installed_pack_metadata(tmp_path, "finance-en")
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "AAPL traded on NASDAQ.",
            "pack": "finance-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("AAPL", "ticker") in pairs
    assert ("NASDAQ", "exchange") in pairs
