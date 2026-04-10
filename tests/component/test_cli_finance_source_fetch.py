import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.finance_bundle_helpers import create_finance_remote_sources


def test_cli_can_fetch_finance_source_snapshot(tmp_path: Path) -> None:
    remote_sources = create_finance_remote_sources(tmp_path / "remote")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "registry",
            "fetch-finance-sources",
            "--output-dir",
            str(tmp_path / "raw" / "finance-en"),
            "--snapshot",
            "2026-04-10",
            "--sec-url",
            remote_sources["sec_companies_url"],
            "--symbol-directory-url",
            remote_sources["symbol_directory_url"],
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["pack_id"] == "finance-en"
    assert payload["snapshot"] == "2026-04-10"
    assert Path(payload["source_manifest_path"]).exists()
    assert Path(payload["sec_companies_path"]).exists()
