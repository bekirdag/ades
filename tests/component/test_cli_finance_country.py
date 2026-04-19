import json
from pathlib import Path

import ades.packs.finance_country as finance_country_module
from typer.testing import CliRunner

from ades.cli import app
from tests.finance_bundle_helpers import create_finance_country_profiles


def test_cli_can_fetch_and_build_finance_country_bundles(tmp_path, monkeypatch) -> None:
    profiles = create_finance_country_profiles(tmp_path / "remote-country-sources")
    monkeypatch.setattr(finance_country_module, "FINANCE_COUNTRY_PROFILES", profiles)
    runner = CliRunner()

    fetch_result = runner.invoke(
        app,
        [
            "registry",
            "fetch-finance-country-sources",
            "--output-dir",
            str(tmp_path / "raw" / "finance-country-en"),
            "--snapshot",
            "2026-04-19",
            "--country-code",
            "us",
            "--country-code",
            "uk",
        ],
    )

    assert fetch_result.exit_code == 0
    fetch_payload = json.loads(fetch_result.stdout)
    assert fetch_payload["country_count"] == 2

    build_result = runner.invoke(
        app,
        [
            "registry",
            "build-finance-country-bundles",
            "--snapshot-dir",
            fetch_payload["snapshot_dir"],
            "--output-dir",
            str(tmp_path / "bundles" / "finance-country-en"),
            "--country-code",
            "uk",
            "--version",
            "0.2.0",
        ],
    )

    assert build_result.exit_code == 0
    build_payload = json.loads(build_result.stdout)
    assert build_payload["country_count"] == 1
    assert build_payload["bundles"][0]["pack_id"] == "finance-uk-en"
    assert Path(build_payload["bundles"][0]["bundle_manifest_path"]).exists()
