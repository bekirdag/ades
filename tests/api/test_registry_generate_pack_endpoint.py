import json
from pathlib import Path

from fastapi.testclient import TestClient

from ades import build_registry
from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_generation_helpers import (
    create_finance_generation_bundle,
    create_pre_resolved_general_generation_bundle,
)


def test_registry_generate_pack_endpoint_creates_pack_directory(tmp_path: Path) -> None:
    bundle_dir = create_finance_generation_bundle(tmp_path / "bundle")
    output_dir = tmp_path / "generated-packs"
    install_root = tmp_path / "service-storage"

    client = TestClient(create_app(storage_root=install_root))
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
    assert payload["matcher_algorithm"] == "token_trie_v1"
    assert Path(payload["matcher_artifact_path"]).exists()
    assert Path(payload["matcher_entries_path"]).exists()
    assert payload["publishable_sources_only"] is False
    assert payload["source_license_classes"] == {"build-only": 1, "ship-now": 1}

    registry = build_registry([payload["pack_dir"]], output_dir=tmp_path / "registry")
    installer = PackInstaller(install_root, registry_url=registry.index_url)
    result = installer.install("finance-en")
    assert result.installed == ["finance-en"]
    pack_response = client.get("/v0/packs/finance-en")
    assert pack_response.status_code == 200
    assert pack_response.json()["matcher_ready"] is True
    assert pack_response.json()["matcher_algorithm"] == "token_trie_v1"


def test_registry_generate_pack_endpoint_supports_pre_resolved_general_bundle(
    tmp_path: Path,
) -> None:
    bundle_dir = create_pre_resolved_general_generation_bundle(tmp_path / "bundle")
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
    assert payload["pack_id"] == "general-en"
    assert payload["matcher_algorithm"] == "token_trie_v1"
    build_metadata = json.loads(
        (Path(payload["pack_dir"]) / "build.json").read_text(encoding="utf-8")
    )
    assert build_metadata["analysis_backend"] == "pre_resolved_bundle"
