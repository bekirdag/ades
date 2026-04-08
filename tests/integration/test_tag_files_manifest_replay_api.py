from pathlib import Path

from ades import pull_pack, tag_files


def test_public_api_can_replay_processed_items_from_saved_manifest(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    first_input = tmp_path / "alpha.html"
    second_input = tmp_path / "beta.html"
    first_input.write_text("<p>Apple said AAPL rallied.</p>", encoding="utf-8")
    second_input.write_text("<p>NASDAQ guidance moved.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    initial = tag_files(
        [first_input, second_input],
        pack="finance-en",
        storage_root=tmp_path,
        output_dir=output_dir,
        write_manifest=True,
        max_files=1,
    )
    manifest_path = Path(initial.saved_manifest_path)

    replay = tag_files(
        pack=None,
        storage_root=tmp_path,
        manifest_input_path=manifest_path,
        manifest_replay_mode="processed",
    )

    assert replay.pack == "finance-en"
    assert replay.item_count == 1
    assert replay.summary.manifest_input_path == str(manifest_path.resolve())
    assert replay.summary.manifest_replay_mode == "processed"
    assert replay.summary.manifest_candidate_count == 1
    assert replay.summary.manifest_selected_count == 1
    assert replay.items[0].source_path == str(first_input.resolve())
