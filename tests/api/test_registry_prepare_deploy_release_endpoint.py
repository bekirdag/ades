from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app
from tests.published_registry_smoke_helpers import (
    create_working_published_registry_dir,
    serve_published_registry_dir,
    write_registry_promotion_spec,
)


def test_registry_prepare_deploy_release_endpoint(
    tmp_path: Path,
) -> None:
    registry_dir = create_working_published_registry_dir(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))

    with serve_published_registry_dir(registry_dir) as registry_url:
        promotion_spec_path = write_registry_promotion_spec(
            tmp_path,
            registry_url=registry_url,
        )
        response = client.post(
            "/v0/registry/prepare-deploy-release",
            json={
                "output_dir": str(tmp_path / "deploy-registry"),
                "promotion_spec_path": str(promotion_spec_path),
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "promoted"
    assert payload["promoted_registry_url"] == registry_url
    assert payload["consumer_smoke"]["passed"] is True
    assert payload["pack_count"] == 3
