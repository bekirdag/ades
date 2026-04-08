import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_lookup_supports_multi_term_cross_field_search(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["packs", "lookup", "apple organization"],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["candidates"]
    assert payload["candidates"][0]["kind"] == "alias"
    assert payload["candidates"][0]["value"] == "Apple"
    assert payload["candidates"][0]["label"] == "organization"
