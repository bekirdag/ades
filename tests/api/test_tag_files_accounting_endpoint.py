from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_files_endpoint_reports_processed_sizes_and_skipped_rejected_inputs(
    tmp_path: Path,
) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    keep_input = corpus_dir / "keep-report.html"
    generated_output = corpus_dir / "keep-report.finance-en.ades.json"
    keep_input.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")
    generated_output.write_text("{}", encoding="utf-8")
    missing_input = tmp_path / "missing.html"

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/files",
        json={
            "paths": [str(missing_input)],
            "directories": [str(corpus_dir)],
            "glob_patterns": [str(tmp_path / "*.txt")],
            "pack": "finance-en",
        },
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload["item_count"] == 1
    assert payload["summary"]["processed_count"] == 1
    assert payload["summary"]["processed_input_bytes"] == len(
        "<p>Org Beta said TICKA traded on EXCHX.</p>".encode("utf-8")
    )
    assert payload["summary"]["skipped_count"] == 2
    assert payload["summary"]["rejected_count"] == 1
    assert any(item["reason"] == "generated_output" for item in payload["skipped"])
    assert any(item["reason"] == "glob_no_matches" for item in payload["skipped"])
    assert payload["rejected"][0]["reason"] == "file_not_found"
    assert payload["items"][0]["input_size_bytes"] == len(
        "<p>Org Beta said TICKA traded on EXCHX.</p>".encode("utf-8")
    )
