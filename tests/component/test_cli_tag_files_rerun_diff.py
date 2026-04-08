import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_files_reports_rerun_diff(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    stable_input = corpus_dir / "01-stable.html"
    changed_input = corpus_dir / "02-changed.html"
    stable_input.write_text("<p>Apple said AAPL rallied.</p>", encoding="utf-8")
    changed_input.write_text("<p>NASDAQ guidance moved.</p>", encoding="utf-8")
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

    stable_output = output_dir / "01-stable.finance-en.ades.json"
    stable_output.unlink()
    changed_input.write_text("<p>NASDAQ guidance moved again.</p>", encoding="utf-8")
    new_input = corpus_dir / "03-new.html"
    skipped_input = corpus_dir / "04-skipped.html"
    new_input.write_text("<p>Apple traded on NASDAQ.</p>", encoding="utf-8")
    skipped_input.write_text("<p>Late file.</p>", encoding="utf-8")
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
            "--repair-missing-reused-outputs",
            "--max-files",
            "3",
            "--pack",
            "finance-en",
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert replay.exit_code == 0
    payload = json.loads(replay.stdout)
    assert payload["summary"]["limit_skipped_count"] == 1
    assert payload["rerun_diff"]["changed"] == [str(changed_input.resolve())]
    assert payload["rerun_diff"]["newly_processed"] == [str(new_input.resolve())]
    assert payload["rerun_diff"]["reused"] == [str(stable_input.resolve())]
    assert payload["rerun_diff"]["repaired"] == [str(stable_input.resolve())]
    assert payload["rerun_diff"]["skipped"] == [
        {
            "reference": str(skipped_input.resolve()),
            "reason": "max_files_limit",
            "source_kind": "limit",
            "size_bytes": skipped_input.stat().st_size,
        }
    ]

