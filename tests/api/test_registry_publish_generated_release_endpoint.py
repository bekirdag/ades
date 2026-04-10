from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app
from tests.object_storage_publish_helpers import (
    create_reviewed_registry_dir,
    install_object_storage_publish_stub,
)


def test_registry_publish_generated_release_endpoint(
    tmp_path: Path,
    monkeypatch,
) -> None:
    registry_dir = create_reviewed_registry_dir(tmp_path)
    install_object_storage_publish_stub(
        monkeypatch,
        registry_dir=registry_dir,
        prefix="generated-pack-releases/finance-general-medical-2026-04-10",
    )

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/publish-generated-release",
        json={
            "registry_dir": str(registry_dir),
            "prefix": "generated-pack-releases/finance-general-medical-2026-04-10",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["bucket"] == "ades-test"
    assert payload["object_count"] == 3
    assert payload["registry_url_candidates"][1].startswith(
        "https://fsn1.your-objectstorage.com/ades-test/"
    )
    assert payload["objects"][1]["key"].endswith("packs/general-en/manifest.json")
