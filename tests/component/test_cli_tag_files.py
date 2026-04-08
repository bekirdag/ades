import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_files_supports_multiple_local_documents(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    alpha_dir = tmp_path / "alpha"
    beta_dir = tmp_path / "beta"
    alpha_dir.mkdir()
    beta_dir.mkdir()
    first_input = alpha_dir / "report.html"
    second_input = beta_dir / "report.html"
    first_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")
    second_input.write_text("<p>NASDAQ said Apple moved AAPL guidance.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "tag-files",
            str(first_input),
            str(second_input),
            "--pack",
            "finance-en",
            "--output-dir",
            str(output_dir),
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    saved_paths = {item["saved_output_path"] for item in payload["items"]}

    assert payload["pack"] == "finance-en"
    assert payload["item_count"] == 2
    assert saved_paths == {
        str(output_dir.resolve() / "report.finance-en.ades.json"),
        str(output_dir.resolve() / "report.finance-en.ades-2.json"),
    }
