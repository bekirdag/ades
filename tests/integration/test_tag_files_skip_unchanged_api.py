from pathlib import Path

from ades import pull_pack, tag_files


def test_public_api_can_skip_unchanged_sources_from_manifest(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    stable_input = corpus_dir / "stable.html"
    changed_input = corpus_dir / "changed.html"
    stable_input.write_text("<p>Apple said AAPL rallied.</p>", encoding="utf-8")
    changed_input.write_text("<p>NASDAQ guidance moved.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    initial = tag_files(
        directories=[corpus_dir],
        pack="finance-en",
        storage_root=tmp_path,
        output_dir=output_dir,
        write_manifest=True,
    )
    manifest_path = Path(initial.saved_manifest_path)

    changed_input.write_text("<p>NASDAQ guidance moved again.</p>", encoding="utf-8")
    replay = tag_files(
        directories=[corpus_dir],
        pack="finance-en",
        storage_root=tmp_path,
        manifest_input_path=manifest_path,
        skip_unchanged=True,
    )

    assert replay.item_count == 1
    assert replay.summary.unchanged_skipped_count == 1
    assert any(item.reason == "unchanged_since_manifest" for item in replay.skipped)
    assert replay.items[0].source_path == str(changed_input.resolve())
