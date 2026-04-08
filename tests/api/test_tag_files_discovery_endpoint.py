from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_files_endpoint_supports_directory_and_glob_discovery(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    nested_dir = corpus_dir / "nested"
    glob_dir = tmp_path / "globbed"
    nested_dir.mkdir(parents=True)
    glob_dir.mkdir()

    nested_input = nested_dir / "report.html"
    globbed_input = glob_dir / "bulletin.html"
    nested_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")
    globbed_input.write_text("<p>NASDAQ said Apple moved AAPL guidance.</p>", encoding="utf-8")

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/files",
        json={
            "directories": [str(corpus_dir)],
            "glob_patterns": [str(glob_dir / "*.html")],
            "pack": "finance-en",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    source_paths = {item["source_path"] for item in payload["items"]}

    assert payload["pack"] == "finance-en"
    assert payload["item_count"] == 2
    assert source_paths == {
        str(nested_input.resolve()),
        str(globbed_input.resolve()),
    }
