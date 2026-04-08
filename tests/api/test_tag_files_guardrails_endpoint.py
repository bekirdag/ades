from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_files_endpoint_applies_max_files_guardrail(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    first = tmp_path / "alpha.html"
    second = tmp_path / "beta.html"
    first.write_text("<p>Apple said AAPL rallied.</p>", encoding="utf-8")
    second.write_text("<p>NASDAQ guidance moved.</p>", encoding="utf-8")

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/files",
        json={
            "paths": [str(first), str(second)],
            "pack": "finance-en",
            "max_files": 1,
        },
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload["item_count"] == 1
    assert payload["summary"]["max_files"] == 1
    assert payload["summary"]["processed_count"] == 1
    assert payload["summary"]["limit_skipped_count"] == 1
    assert payload["skipped"][0]["reason"] == "max_files_limit"
