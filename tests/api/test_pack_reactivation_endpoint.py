from pathlib import Path

from fastapi.testclient import TestClient

from ades import build_registry
from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_registry_helpers import create_finance_registry_sources


def test_service_pack_endpoints_reflect_reactivated_dependency_after_repull(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    installer = PackInstaller(install_root, registry_url=registry.index_url)
    installer.install("finance-en")

    client = TestClient(create_app(storage_root=install_root))

    deactivate_response = client.post("/v0/packs/general-en/deactivate")
    assert deactivate_response.status_code == 200
    assert deactivate_response.json()["active"] is False

    repull_result = installer.install("finance-en")
    assert repull_result.installed == ["general-en"]
    assert repull_result.skipped == ["finance-en"]

    active_packs = client.get("/v0/packs", params={"active_only": True})
    assert active_packs.status_code == 200
    assert {pack["pack_id"] for pack in active_packs.json()} == {"finance-en", "general-en"}

    tag_response = client.post(
        "/v0/tag",
        json={
            "text": "Contact us at team@example.com and watch AAPL on NASDAQ.",
            "pack": "finance-en",
            "content_type": "text/plain",
        },
    )
    assert tag_response.status_code == 200
    entities = {
        (entity["text"], entity["label"], entity["provenance"]["source_pack"])
        for entity in tag_response.json()["entities"]
    }
    assert ("team@example.com", "email_address", "general-en") in entities
