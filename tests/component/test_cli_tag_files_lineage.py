import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_files_reports_manifest_lineage(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    source_path = corpus_dir / "report.html"
    source_path.write_text("<p>Apple said AAPL rallied.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    runner = CliRunner()
    initial = runner.invoke(
        app,
        [
            "tag-files",
            "--directory",
            str(corpus_dir),
            "--pack",
            "finance-en",
            "--output-dir",
            str(output_dir),
            "--write-manifest",
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert initial.exit_code == 0
    initial_payload = json.loads(initial.stdout)
    manifest_path = output_dir.resolve() / "batch.finance-en.ades-manifest.json"
    assert initial_payload["saved_manifest_path"] == str(manifest_path)
    assert initial_payload["lineage"]["run_id"].startswith("ades-run-")
    assert initial_payload["lineage"]["root_run_id"] == initial_payload["lineage"]["run_id"]
    assert initial_payload["lineage"]["parent_run_id"] is None
    assert initial_payload["lineage"]["source_manifest_path"] is None
    assert initial_payload["lineage"]["created_at"]

    child_manifest_path = output_dir.resolve() / "child.finance-en.ades-manifest.json"
    replay = runner.invoke(
        app,
        [
            "tag-files",
            "--manifest-input",
            str(manifest_path),
            "--manifest-mode",
            "processed",
            "--pack",
            "finance-en",
            "--output-dir",
            str(output_dir),
            "--write-manifest",
            "--manifest-output",
            str(child_manifest_path),
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert replay.exit_code == 0
    replay_payload = json.loads(replay.stdout)
    assert replay_payload["saved_manifest_path"] == str(child_manifest_path)
    assert replay_payload["lineage"]["run_id"].startswith("ades-run-")
    assert replay_payload["lineage"]["run_id"] != initial_payload["lineage"]["run_id"]
    assert replay_payload["lineage"]["parent_run_id"] == initial_payload["lineage"]["run_id"]
    assert replay_payload["lineage"]["root_run_id"] == initial_payload["lineage"]["root_run_id"]
    assert replay_payload["lineage"]["source_manifest_path"] == str(manifest_path)

    child_manifest_payload = json.loads(child_manifest_path.read_text(encoding="utf-8"))
    assert child_manifest_payload["lineage"] == replay_payload["lineage"]

