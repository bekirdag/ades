from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app
from tests.published_registry_smoke_helpers import (
    create_working_published_registry_dir,
    serve_published_registry_dir,
)


def test_registry_smoke_published_release_endpoint(tmp_path: Path) -> None:
    registry_dir = create_working_published_registry_dir(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))

    with serve_published_registry_dir(registry_dir) as registry_url:
        response = client.post(
            "/v0/registry/smoke-published-release",
            json={
                "registry_url": registry_url,
                "pack_ids": ["finance-en"],
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["passed"] is True
    assert payload["pack_count"] == 1
    assert payload["cases"][0]["pack_id"] == "finance-en"
    assert payload["cases"][0]["installed_pack_ids"] == ["finance-en"]
    assert "exchange" in payload["cases"][0]["matched_labels"]
