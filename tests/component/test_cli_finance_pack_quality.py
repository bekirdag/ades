import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_cli_can_validate_generated_finance_pack_quality(tmp_path: Path) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    bundles_root = tmp_path / "bundles"
    quality_root = tmp_path / "quality-output"
    runner = CliRunner()

    bundle_result = runner.invoke(
        app,
        [
            "registry",
            "build-finance-bundle",
            "--sec-company-tickers",
            str(snapshots["sec_companies"]),
            "--symbol-directory",
            str(snapshots["symbol_directory"]),
            "--curated-entities",
            str(snapshots["curated_entities"]),
            "--output-dir",
            str(bundles_root),
        ],
    )

    assert bundle_result.exit_code == 0
    bundle_payload = json.loads(bundle_result.stdout)
    validate_result = runner.invoke(
        app,
        [
            "registry",
            "validate-finance-quality",
            bundle_payload["bundle_dir"],
            "--output-dir",
            str(quality_root),
        ],
    )

    assert validate_result.exit_code == 0
    validate_payload = json.loads(validate_result.stdout)
    assert validate_payload["passed"] is True
    assert validate_payload["expected_recall"] == 1.0
    assert Path(validate_payload["generated_pack_dir"]).exists()
