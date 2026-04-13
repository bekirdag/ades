import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_can_persist_pack_aware_json_output(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    input_path = tmp_path / "report.html"
    input_path.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "tag",
            "--file",
            str(input_path),
            "--pack",
            "finance-en",
            "--output-dir",
            str(output_dir),
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    saved_path = output_dir.resolve() / "report.finance-en.ades.json"

    assert payload["saved_output_path"] == str(saved_path)
    persisted = json.loads(saved_path.read_text(encoding="utf-8"))
    assert persisted["saved_output_path"] == str(saved_path)
    assert persisted["pack"] == "finance-en"
