from pathlib import Path

from fastapi.testclient import TestClient

from tests.pack_registry_helpers import create_pack_source
from ades.service.app import create_app


def test_registry_build_endpoint_returns_404_for_missing_pack_directory(
    tmp_path: Path,
) -> None:
    missing_pack_dir = tmp_path / "missing-pack"
    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))

    response = client.post(
        "/v0/registry/build",
        json={
            "pack_dirs": [str(missing_pack_dir)],
            "output_dir": str(tmp_path / "registry"),
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"] == f"Pack directory not found: {missing_pack_dir.resolve()}"


def test_registry_build_endpoint_returns_400_for_duplicate_pack_ids(tmp_path: Path) -> None:
    pack_dir = create_pack_source(tmp_path / "sources", pack_id="general-en", domain="general")
    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))

    response = client.post(
        "/v0/registry/build",
        json={
            "pack_dirs": [str(pack_dir), str(pack_dir)],
            "output_dir": str(tmp_path / "registry"),
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Duplicate pack id in registry build: general-en"
