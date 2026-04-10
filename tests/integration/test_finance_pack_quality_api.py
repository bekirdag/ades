from pathlib import Path

from ades import build_finance_source_bundle, validate_finance_pack_quality
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_public_api_can_validate_generated_finance_pack_quality(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")

    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        symbol_directory_path=snapshots["symbol_directory"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )
    report = validate_finance_pack_quality(
        bundle.bundle_dir,
        output_dir=tmp_path / "quality-output",
    )

    assert report.passed is True
    assert report.registry_index_url.startswith("file://")
    assert report.passed_case_count == report.fixture_count
