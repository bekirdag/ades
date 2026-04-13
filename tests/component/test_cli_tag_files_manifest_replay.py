import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_files_can_resume_from_saved_manifest(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    first_input = tmp_path / "alpha.html"
    second_input = tmp_path / "beta.html"
    first_input.write_text("<p>Org Beta said TICKA rallied.</p>", encoding="utf-8")
    second_input.write_text("<p>EXCHX guidance moved.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    runner = CliRunner()
    initial = runner.invoke(
        app,
        [
            "tag-files",
            str(first_input),
            str(second_input),
            "--pack",
            "finance-en",
            "--output-dir",
            str(output_dir),
            "--write-manifest",
            "--max-files",
            "1",
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )
    assert initial.exit_code == 0

    manifest_path = output_dir / "batch.finance-en.ades-manifest.json"
    replay = runner.invoke(
        app,
        [
            "tag-files",
            "--manifest-input",
            str(manifest_path),
            "--manifest-mode",
            "resume",
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert replay.exit_code == 0
    payload = json.loads(replay.stdout)
    assert payload["pack"] == "finance-en"
    assert payload["item_count"] == 1
    assert payload["summary"]["manifest_input_path"] == str(manifest_path.resolve())
    assert payload["summary"]["manifest_replay_mode"] == "resume"
    assert payload["summary"]["manifest_candidate_count"] == 1
    assert payload["summary"]["manifest_selected_count"] == 1
    assert payload["items"][0]["source_path"] == str(second_input.resolve())
