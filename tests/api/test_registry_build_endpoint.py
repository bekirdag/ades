from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_registry_helpers import create_finance_registry_sources


def test_registry_build_endpoint_creates_consumable_static_registry(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    output_dir = tmp_path / "registry"

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/build",
        json={
            "pack_dirs": [str(general_dir), str(finance_dir)],
            "output_dir": str(output_dir),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_count"] == 2
    assert payload["index_path"] == str((output_dir / "index.json").resolve())
    assert len(payload["packs"]) == 2

    installer = PackInstaller(tmp_path / "install", registry_url=payload["index_url"])
    result = installer.install("finance-en")
    assert result.installed == ["general-en", "finance-en"]
