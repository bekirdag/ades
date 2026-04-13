from pathlib import Path

from ades import fetch_finance_source_snapshot
from tests.finance_bundle_helpers import create_finance_remote_sources


def test_public_api_can_fetch_finance_source_snapshot(tmp_path: Path) -> None:
    remote_sources = create_finance_remote_sources(tmp_path / "remote")

    result = fetch_finance_source_snapshot(
        output_dir=tmp_path / "raw" / "finance-en",
        snapshot="2026-04-10",
        sec_companies_url=remote_sources["sec_companies_url"],
        sec_submissions_url=remote_sources["sec_submissions_url"],
        sec_companyfacts_url=remote_sources["sec_companyfacts_url"],
        symbol_directory_url=remote_sources["symbol_directory_url"],
        other_listed_url=remote_sources["other_listed_url"],
    )

    assert result.pack_id == "finance-en"
    assert result.snapshot == "2026-04-10"
    assert Path(result.source_manifest_path).exists()
    assert Path(result.sec_submissions_path).exists()
