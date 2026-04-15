from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_file_endpoint_returns_404_for_missing_source_path(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    missing_path = tmp_path / "missing-report.html"
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag/file",
        json={"path": str(missing_path), "pack": "finance-en"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == f"File not found: {missing_path.resolve()}"
