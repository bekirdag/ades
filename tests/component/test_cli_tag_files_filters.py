import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_files_supports_include_exclude_filters_and_summary(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    keep_dir = corpus_dir / "keep"
    skip_dir = corpus_dir / "skip"
    keep_dir.mkdir(parents=True)
    skip_dir.mkdir()

    keep_input = keep_dir / "keep-report.html"
    skip_input = skip_dir / "skip-report.html"
    keep_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")
    skip_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "tag-files",
            "--directory",
            str(corpus_dir),
            "--pack",
            "finance-en",
            "--include",
            "*report.html",
            "--exclude",
            "skip*",
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)

    assert payload["item_count"] == 1
    assert payload["summary"]["discovered_count"] == 2
    assert payload["summary"]["included_count"] == 1
    assert payload["summary"]["excluded_count"] == 1
    assert payload["summary"]["include_patterns"] == ["*report.html"]
    assert payload["summary"]["exclude_patterns"] == ["skip*"]
    assert payload["items"][0]["source_path"] == str(keep_input.resolve())
