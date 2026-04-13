import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_files_reports_sizes_and_skipped_rejected_inputs(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    keep_input = corpus_dir / "keep-report.html"
    generated_output = corpus_dir / "keep-report.finance-en.ades.json"
    keep_input.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")
    generated_output.write_text("{}", encoding="utf-8")
    missing_input = tmp_path / "missing.html"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "tag-files",
            str(missing_input),
            "--directory",
            str(corpus_dir),
            "--glob",
            str(tmp_path / "*.txt"),
            "--pack",
            "finance-en",
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)

    assert payload["item_count"] == 1
    assert payload["summary"]["processed_count"] == 1
    assert payload["summary"]["processed_input_bytes"] == len(
        "<p>Org Beta said TICKA traded on EXCHX.</p>".encode("utf-8")
    )
    assert payload["summary"]["skipped_count"] == 2
    assert payload["summary"]["rejected_count"] == 1
    assert payload["skipped"][0]["reason"] == "generated_output"
    assert any(item["reason"] == "glob_no_matches" for item in payload["skipped"])
    assert payload["rejected"][0]["reason"] == "file_not_found"
