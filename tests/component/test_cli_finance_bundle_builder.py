import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_cli_can_build_finance_bundle_then_generate_pack(tmp_path: Path) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    bundles_root = tmp_path / "bundles"
    generated_root = tmp_path / "generated-packs"
    runner = CliRunner()

    bundle_result = runner.invoke(
        app,
        [
            "registry",
            "build-finance-bundle",
            "--sec-company-tickers",
            str(snapshots["sec_companies"]),
            "--sec-submissions",
            str(snapshots["sec_submissions"]),
            "--sec-companyfacts",
            str(snapshots["sec_companyfacts"]),
            "--symbol-directory",
            str(snapshots["symbol_directory"]),
            "--other-listed",
            str(snapshots["other_listed"]),
            "--curated-entities",
            str(snapshots["curated_entities"]),
            "--output-dir",
            str(bundles_root),
        ],
    )

    assert bundle_result.exit_code == 0
    bundle_payload = json.loads(bundle_result.stdout)
    assert bundle_payload["pack_id"] == "finance-en"
    assert bundle_payload["symbol_count"] == 3
    assert Path(bundle_payload["bundle_dir"]).exists()
    assert Path(bundle_payload["sources_lock_path"]).exists()

    generate_result = runner.invoke(
        app,
        [
            "registry",
            "generate-pack",
            bundle_payload["bundle_dir"],
            "--output-dir",
            str(generated_root),
        ],
    )

    assert generate_result.exit_code == 0
    generate_payload = json.loads(generate_result.stdout)
    assert generate_payload["pack_id"] == "finance-en"
    assert Path(generate_payload["pack_dir"]).exists()
