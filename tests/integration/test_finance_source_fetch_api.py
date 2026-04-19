from pathlib import Path
import json

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
        finance_people_url=remote_sources["finance_people_url"],
    )

    assert result.pack_id == "finance-en"
    assert result.snapshot == "2026-04-10"
    assert Path(result.source_manifest_path).exists()
    assert Path(result.finance_people_path).exists()
    assert Path(result.sec_submissions_path).exists()


def test_public_api_can_derive_finance_people_from_sec_proxy_filings(
    tmp_path: Path,
) -> None:
    remote_sources = create_finance_remote_sources(tmp_path / "remote")

    result = fetch_finance_source_snapshot(
        output_dir=tmp_path / "raw" / "finance-en",
        snapshot="2026-04-11",
        sec_companies_url=remote_sources["sec_companies_url"],
        sec_submissions_url=remote_sources["sec_submissions_url"],
        sec_companyfacts_url=remote_sources["sec_companyfacts_url"],
        symbol_directory_url=remote_sources["symbol_directory_url"],
        other_listed_url=remote_sources["other_listed_url"],
        derive_finance_people_from_sec=True,
        finance_people_archive_base_url=remote_sources[
            "finance_people_archive_base_url"
        ],
        finance_people_max_companies=2,
    )

    people_payload = json.loads(Path(result.finance_people_path).read_text(encoding="utf-8"))
    extracted_names = {
        item["canonical_text"] for item in people_payload.get("entities", [])
    }
    assert "Jane Doe" in extracted_names
    assert "Mary Major" in extracted_names
