import json
from pathlib import Path

from ades import pull_pack, tag_files


def test_public_api_reports_manifest_lineage(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    source_path = corpus_dir / "report.html"
    source_path.write_text("<p>Apple said AAPL rallied.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    initial = tag_files(
        directories=[corpus_dir],
        pack="finance-en",
        storage_root=tmp_path,
        output_dir=output_dir,
        write_manifest=True,
    )

    manifest_path = output_dir.resolve() / "batch.finance-en.ades-manifest.json"
    assert initial.saved_manifest_path == str(manifest_path)
    assert initial.lineage is not None
    assert initial.lineage.run_id.startswith("ades-run-")
    assert initial.lineage.root_run_id == initial.lineage.run_id
    assert initial.lineage.parent_run_id is None
    assert initial.lineage.source_manifest_path is None

    child_manifest_path = output_dir.resolve() / "child.finance-en.ades-manifest.json"
    replay = tag_files(
        pack="finance-en",
        storage_root=tmp_path,
        manifest_input_path=manifest_path,
        manifest_replay_mode="processed",
        output_dir=output_dir,
        write_manifest=True,
        manifest_output_path=child_manifest_path,
    )

    assert replay.saved_manifest_path == str(child_manifest_path)
    assert replay.lineage is not None
    assert replay.lineage.parent_run_id == initial.lineage.run_id
    assert replay.lineage.root_run_id == initial.lineage.root_run_id
    assert replay.lineage.source_manifest_path == str(manifest_path)

    child_manifest_payload = json.loads(child_manifest_path.read_text(encoding="utf-8"))
    assert child_manifest_payload["lineage"]["run_id"] == replay.lineage.run_id
    assert child_manifest_payload["lineage"]["parent_run_id"] == initial.lineage.run_id
    assert child_manifest_payload["lineage"]["root_run_id"] == initial.lineage.root_run_id
    assert child_manifest_payload["lineage"]["source_manifest_path"] == str(
        manifest_path
    )

