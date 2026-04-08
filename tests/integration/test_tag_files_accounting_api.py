from pathlib import Path

from ades import pull_pack, tag_files


def test_public_api_reports_processed_sizes_and_skipped_rejected_inputs(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    keep_input = corpus_dir / "keep-report.html"
    generated_output = corpus_dir / "keep-report.finance-en.ades.json"
    keep_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")
    generated_output.write_text("{}", encoding="utf-8")
    missing_input = tmp_path / "missing.html"

    response = tag_files(
        [missing_input],
        pack="finance-en",
        storage_root=tmp_path,
        directories=[corpus_dir],
        glob_patterns=[str(tmp_path / "*.txt")],
    )

    assert response.item_count == 1
    assert response.summary.processed_count == 1
    assert response.summary.processed_input_bytes == len(
        "<p>Apple said AAPL traded on NASDAQ.</p>".encode("utf-8")
    )
    assert response.summary.skipped_count == 2
    assert response.summary.rejected_count == 1
    assert any(item.reason == "generated_output" for item in response.skipped)
    assert any(item.reason == "glob_no_matches" for item in response.skipped)
    assert response.rejected[0].reason == "file_not_found"
    assert response.items[0].input_size_bytes == len(
        "<p>Apple said AAPL traded on NASDAQ.</p>".encode("utf-8")
    )
