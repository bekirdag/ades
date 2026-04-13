import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_files_applies_max_files_guardrail(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    first = tmp_path / "alpha.html"
    second = tmp_path / "beta.html"
    first.write_text("<p>Org Beta said TICKA rallied.</p>", encoding="utf-8")
    second.write_text("<p>EXCHX guidance moved.</p>", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "tag-files",
            str(first),
            str(second),
            "--pack",
            "finance-en",
            "--max-files",
            "1",
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)

    assert payload["item_count"] == 1
    assert payload["summary"]["max_files"] == 1
    assert payload["summary"]["processed_count"] == 1
    assert payload["summary"]["limit_skipped_count"] == 1
    assert payload["skipped"][0]["reason"] == "max_files_limit"
