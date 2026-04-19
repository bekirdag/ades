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
            "--sec-submissions-url",
            remote_sources["sec_submissions_url"],
            "--sec-companyfacts-url",
            remote_sources["sec_companyfacts_url"],
            "--symbol-directory-url",
            remote_sources["symbol_directory_url"],
            "--other-listed-url",
            remote_sources["other_listed_url"],
            "--finance-people-url",
            remote_sources["finance_people_url"],
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["pack_id"] == "finance-en"
    assert payload["snapshot"] == "2026-04-10"
    assert Path(payload["source_manifest_path"]).exists()
    assert Path(payload["sec_companies_path"]).exists()
    assert Path(payload["sec_submissions_path"]).exists()
    assert Path(payload["sec_companyfacts_path"]).exists()
    assert Path(payload["other_listed_path"]).exists()
    assert Path(payload["finance_people_path"]).exists()


def test_cli_can_derive_finance_people_from_sec_proxy_filings(tmp_path: Path) -> None:
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
            "2026-04-11",
            "--sec-url",
            remote_sources["sec_companies_url"],
            "--sec-submissions-url",
            remote_sources["sec_submissions_url"],
            "--sec-companyfacts-url",
            remote_sources["sec_companyfacts_url"],
            "--symbol-directory-url",
            remote_sources["symbol_directory_url"],
            "--other-listed-url",
            remote_sources["other_listed_url"],
            "--derive-finance-people-from-sec",
            "--finance-people-archive-base-url",
            remote_sources["finance_people_archive_base_url"],
            "--finance-people-max-companies",
            "2",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["finance_people_url"].startswith("operator://ades/sec-proxy-people-entities")
    assert Path(payload["finance_people_path"]).exists()
