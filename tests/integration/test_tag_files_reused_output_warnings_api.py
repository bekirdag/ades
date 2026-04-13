from pathlib import Path

from ades import pull_pack, tag_files


def test_public_api_warns_when_reused_output_file_is_missing(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    stable_input = corpus_dir / "stable.html"
    changed_input = corpus_dir / "changed.html"
    stable_input.write_text("<p>Org Beta said TICKA rallied.</p>", encoding="utf-8")
    changed_input.write_text("<p>EXCHX guidance moved.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    initial = tag_files(
        directories=[corpus_dir],
        pack="finance-en",
        storage_root=tmp_path,
        output_dir=output_dir,
        write_manifest=True,
    )
    stable_output = output_dir / "stable.finance-en.ades.json"
    stable_output.unlink()
    manifest_path = Path(initial.saved_manifest_path)

    changed_input.write_text("<p>EXCHX guidance moved again.</p>", encoding="utf-8")
    replay = tag_files(
        directories=[corpus_dir],
        pack="finance-en",
        storage_root=tmp_path,
        manifest_input_path=manifest_path,
        skip_unchanged=True,
        reuse_unchanged_outputs=True,
    )

    warning = f"reused_output_missing_file:{stable_output.resolve()}"
    assert replay.summary.reused_output_missing_count == 1
    assert warning in replay.warnings
    assert replay.reused_items[0].saved_output_exists is False
    assert warning in replay.reused_items[0].warnings
