import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_supports_local_files(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    input_path = tmp_path / "report.html"
    input_path.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "tag",
            "--file",
            str(input_path),
            "--pack",
            "finance-en",
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    pairs = {(entity["text"], entity["label"]) for entity in payload["entities"]}

    assert payload["source_path"] == str(input_path.resolve())
    assert payload["content_type"] == "text/html"
    assert ("Apple", "organization") in pairs
    assert ("AAPL", "ticker") in pairs
    assert ("NASDAQ", "exchange") in pairs
