from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ades import build_registry
from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_registry_helpers import create_finance_registry_sources


def test_service_pack_endpoints_reflect_preserved_state_after_failed_update(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources-initial")
    initial_registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry-initial")

    install_root = tmp_path / "install"
    PackInstaller(install_root, registry_url=initial_registry.index_url).install("finance-en")

    updated_general_dir, updated_finance_dir = create_finance_registry_sources(
        tmp_path / "sources-update",
        general_version="0.1.0",
        finance_version="0.2.0",
    )
    updated_registry = build_registry(
        [updated_general_dir, updated_finance_dir],
        output_dir=tmp_path / "registry-update",
    )
    (tmp_path / "registry-update" / "artifacts" / "finance-en-0.2.0.tar.zst").write_bytes(
        b"corrupted-artifact"
    )

    with pytest.raises(ValueError, match="checksum mismatch"):
        PackInstaller(install_root, registry_url=updated_registry.index_url).install("finance-en")

    client = TestClient(create_app(storage_root=install_root))

    pack_response = client.get("/v0/packs/finance-en")
    assert pack_response.status_code == 200
    assert pack_response.json()["version"] == "0.1.0"

    packs_response = client.get("/v0/packs")
    assert packs_response.status_code == 200
    assert [pack["pack_id"] for pack in packs_response.json()] == ["finance-en", "general-en"]

    status_response = client.get("/v0/status")
    assert status_response.status_code == 200
    assert status_response.json()["installed_packs"] == ["finance-en", "general-en"]
