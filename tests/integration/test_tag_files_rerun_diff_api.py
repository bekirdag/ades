from pathlib import Path

from ades import pull_pack, tag_files


def test_public_api_reports_rerun_diff(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    stable_input = corpus_dir / "01-stable.html"
    changed_input = corpus_dir / "02-changed.html"
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
    stable_output = output_dir / "01-stable.finance-en.ades.json"
    stable_output.unlink()
    changed_input.write_text("<p>EXCHX guidance moved again.</p>", encoding="utf-8")
    new_input = corpus_dir / "03-new.html"
    skipped_input = corpus_dir / "04-skipped.html"
    new_input.write_text("<p>Org Beta traded on EXCHX.</p>", encoding="utf-8")
    skipped_input.write_text("<p>Late file.</p>", encoding="utf-8")

    replay = tag_files(
        directories=[corpus_dir],
        pack="finance-en",
        storage_root=tmp_path,
        manifest_input_path=Path(initial.saved_manifest_path),
        skip_unchanged=True,
        reuse_unchanged_outputs=True,
        repair_missing_reused_outputs=True,
        max_files=3,
    )

    assert replay.summary.limit_skipped_count == 1
    assert replay.rerun_diff is not None
    assert replay.rerun_diff.changed == [str(changed_input.resolve())]
    assert replay.rerun_diff.newly_processed == [str(new_input.resolve())]
    assert replay.rerun_diff.reused == [str(stable_input.resolve())]
    assert replay.rerun_diff.repaired == [str(stable_input.resolve())]
    assert [item.model_dump(mode="python") for item in replay.rerun_diff.skipped] == [
        {
            "reference": str(skipped_input.resolve()),
            "reason": "max_files_limit",
            "source_kind": "limit",
            "size_bytes": skipped_input.stat().st_size,
        }
    ]

