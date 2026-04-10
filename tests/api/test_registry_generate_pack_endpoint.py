from pathlib import Path

from fastapi.testclient import TestClient

from ades import build_registry
from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_generation_helpers import create_finance_generation_bundle


def test_registry_generate_pack_endpoint_creates_pack_directory(tmp_path: Path) -> None:
    bundle_dir = create_finance_generation_bundle(tmp_path / "bundle")
    output_dir = tmp_path / "generated-packs"

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/generate-pack",
        json={
            "bundle_dir": str(bundle_dir),
            "output_dir": str(output_dir),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "finance-en"
    assert payload["alias_count"] >= 3
    assert payload["rule_count"] == 1

    registry = build_registry([payload["pack_dir"]], output_dir=tmp_path / "registry")
    installer = PackInstaller(tmp_path / "install", registry_url=registry.index_url)
    result = installer.install("finance-en")
    assert result.installed == ["finance-en"]
