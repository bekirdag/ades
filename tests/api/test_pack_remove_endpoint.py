from pathlib import Path

from fastapi.testclient import TestClient

from ades import build_registry
from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_registry_helpers import create_finance_registry_sources


def test_service_remove_endpoint_deletes_requested_pack(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    PackInstaller(install_root, registry_url=registry.index_url).install("finance-en")

    client = TestClient(create_app(storage_root=install_root))

    response = client.delete("/v0/packs/finance-en")

    assert response.status_code == 200
    assert response.json()["pack_id"] == "finance-en"

    packs_response = client.get("/v0/packs")
    assert packs_response.status_code == 200
    assert {pack["pack_id"] for pack in packs_response.json()} == {"general-en"}

    missing_response = client.get("/v0/packs/finance-en")
    assert missing_response.status_code == 404


def test_service_blocks_dependency_removal_with_installed_dependents(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    PackInstaller(install_root, registry_url=registry.index_url).install("finance-en")

    client = TestClient(create_app(storage_root=install_root))

    response = client.delete("/v0/packs/general-en")

    assert response.status_code == 400
    assert (
        response.json()["detail"]
        == "Cannot remove pack general-en while installed dependent packs exist: finance-en"
    )


def test_service_remove_missing_pack_returns_404(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.delete("/v0/packs/finance-en")

    assert response.status_code == 404
    assert response.json()["detail"] == "Pack not found: finance-en"
