from pathlib import Path

from ades import pull_pack, tag_files


def test_public_api_can_filter_discovered_batch_sources_and_report_summary(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    corpus_dir = tmp_path / "corpus"
    keep_dir = corpus_dir / "keep"
    skip_dir = corpus_dir / "skip"
    keep_dir.mkdir(parents=True)
    skip_dir.mkdir()

    keep_input = keep_dir / "keep-report.html"
    skip_input = skip_dir / "skip-report.html"
    keep_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")
    skip_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")

    response = tag_files(
        pack="finance-en",
        storage_root=tmp_path,
        directories=[corpus_dir],
        include_patterns=["*report.html"],
        exclude_patterns=["skip*"],
    )

    assert response.pack == "finance-en"
    assert response.item_count == 1
    assert response.summary.discovered_count == 2
    assert response.summary.included_count == 1
    assert response.summary.excluded_count == 1
    assert response.summary.include_patterns == ["*report.html"]
    assert response.summary.exclude_patterns == ["skip*"]
    assert response.items[0].source_path == str(keep_input.resolve())
