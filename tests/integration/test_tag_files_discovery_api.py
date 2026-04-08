from pathlib import Path

from ades import pull_pack, tag_files


def test_public_api_can_discover_directories_and_globs_for_batch_tagging(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    corpus_dir = tmp_path / "corpus"
    nested_dir = corpus_dir / "nested"
    glob_dir = tmp_path / "globbed"
    nested_dir.mkdir(parents=True)
    glob_dir.mkdir()

    nested_input = nested_dir / "report.html"
    globbed_input = glob_dir / "bulletin.html"
    nested_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")
    globbed_input.write_text("<p>NASDAQ said Apple moved AAPL guidance.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    response = tag_files(
        pack="finance-en",
        storage_root=tmp_path,
        output_dir=output_dir,
        directories=[corpus_dir],
        glob_patterns=[str(glob_dir / "*.html")],
    )

    assert response.pack == "finance-en"
    assert response.item_count == 2
    source_paths = {item.source_path for item in response.items}
    assert source_paths == {
        str(nested_input.resolve()),
        str(globbed_input.resolve()),
    }
