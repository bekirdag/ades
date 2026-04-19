import ades.packs.finance_country as finance_country_module

from ades import (
    build_finance_country_source_bundles,
    fetch_finance_country_source_snapshots,
)
from tests.finance_bundle_helpers import create_finance_country_profiles


def test_public_api_can_fetch_and_build_finance_country_bundles(
    tmp_path,
    monkeypatch,
) -> None:
    profiles = create_finance_country_profiles(tmp_path / "remote-country-sources")
    monkeypatch.setattr(finance_country_module, "FINANCE_COUNTRY_PROFILES", profiles)

    fetch_result = fetch_finance_country_source_snapshots(
        output_dir=tmp_path / "raw" / "finance-country-en",
        snapshot="2026-04-19",
        country_codes=["us"],
    )
    assert fetch_result.country_count == 1
    assert fetch_result.countries[0].pack_id == "finance-us-en"

    build_result = build_finance_country_source_bundles(
        snapshot_dir=fetch_result.snapshot_dir,
        output_dir=tmp_path / "bundles" / "finance-country-en",
        country_codes=["us"],
        version="0.2.0",
    )
    assert build_result.country_count == 1
    assert build_result.bundles[0].pack_id == "finance-us-en"
    assert build_result.bundles[0].exchange_count == 1
