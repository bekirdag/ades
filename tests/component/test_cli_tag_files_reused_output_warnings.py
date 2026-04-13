import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_files_warns_when_reused_output_file_is_missing(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    stable_input = corpus_dir / "stable.html"
    changed_input = corpus_dir / "changed.html"
    stable_input.write_text("<p>Org Beta said TICKA rallied.</p>", encoding="utf-8")
    changed_input.write_text("<p>EXCHX guidance moved.</p>", encoding="utf-8")
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

    (output_dir / "stable.finance-en.ades.json").unlink()
    changed_input.write_text("<p>EXCHX guidance moved again.</p>", encoding="utf-8")
    manifest_path = output_dir / "batch.finance-en.ades-manifest.json"
    replay = runner.invoke(
        app,
        [
            "tag-files",
            "--directory",
            str(corpus_dir),
            "--manifest-input",
            str(manifest_path),
            "--skip-unchanged",
            "--reuse-unchanged-outputs",
            "--pack",
            "finance-en",
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert replay.exit_code == 0
    payload = json.loads(replay.stdout)
    warning = f"reused_output_missing_file:{(output_dir / 'stable.finance-en.ades.json').resolve()}"
    assert payload["summary"]["reused_output_missing_count"] == 1
    assert warning in payload["warnings"]
    assert payload["reused_items"][0]["saved_output_exists"] is False
    assert warning in payload["reused_items"][0]["warnings"]
