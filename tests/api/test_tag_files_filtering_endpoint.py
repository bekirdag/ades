from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_files_endpoint_supports_filters_and_returns_summary(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    keep_dir = corpus_dir / "keep"
    skip_dir = corpus_dir / "skip"
    keep_dir.mkdir(parents=True)
    skip_dir.mkdir()

    keep_input = keep_dir / "keep-report.html"
    skip_input = skip_dir / "skip-report.html"
    keep_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")
    skip_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/files",
        json={
            "directories": [str(corpus_dir)],
            "pack": "finance-en",
            "include_patterns": ["*report.html"],
            "exclude_patterns": ["skip*"],
        },
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload["pack"] == "finance-en"
    assert payload["item_count"] == 1
    assert payload["summary"]["discovered_count"] == 2
    assert payload["summary"]["included_count"] == 1
    assert payload["summary"]["excluded_count"] == 1
    assert payload["summary"]["include_patterns"] == ["*report.html"]
    assert payload["summary"]["exclude_patterns"] == ["skip*"]
    assert payload["items"][0]["source_path"] == str(keep_input.resolve())
