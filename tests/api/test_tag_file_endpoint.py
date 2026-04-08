from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_file_endpoint_tags_local_documents(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    input_path = tmp_path / "report.html"
    input_path.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/file",
        json={
            "path": str(input_path),
            "pack": "finance-en",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    pairs = {(entity["text"], entity["label"]) for entity in payload["entities"]}

    assert payload["source_path"] == str(input_path.resolve())
    assert payload["content_type"] == "text/html"
    assert ("Apple", "organization") in pairs
    assert ("AAPL", "ticker") in pairs
    assert ("NASDAQ", "exchange") in pairs
