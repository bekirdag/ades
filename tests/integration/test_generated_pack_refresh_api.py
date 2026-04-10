from pathlib import Path

from ades import build_finance_source_bundle, refresh_generated_packs
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_public_api_can_refresh_generated_finance_pack_release(tmp_path: Path) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        symbol_directory_path=snapshots["symbol_directory"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    response = refresh_generated_packs(
        [bundle.bundle_dir],
        output_dir=tmp_path / "refresh-output",
    )

    assert response.passed is True
    assert response.registry is not None
    assert response.registry.pack_count == 1
    assert response.packs[0].pack_id == "finance-en"
    assert response.packs[0].quality.passed is True
