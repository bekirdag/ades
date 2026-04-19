import json
from pathlib import Path

from ades.packs.finance_bundle import build_finance_source_bundle
from ades.packs.finance_sources import (
    DEFAULT_CURATED_FINANCE_ENTITIES,
    _extract_people_from_sec_proxy_document,
    fetch_finance_source_snapshot,
)
from tests.finance_bundle_helpers import create_finance_remote_sources


def test_fetch_finance_source_snapshot_writes_immutable_snapshot_dir(tmp_path: Path) -> None:
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
    assert result.source_count == 7
    assert result.curated_entity_count == len(DEFAULT_CURATED_FINANCE_ENTITIES)
    assert Path(result.snapshot_dir).exists()
    assert Path(result.sec_companies_path).exists()
    assert Path(result.sec_submissions_path).exists()
    assert Path(result.sec_companyfacts_path).exists()
    assert Path(result.symbol_directory_path).exists()
    assert Path(result.other_listed_path).exists()
    assert Path(result.finance_people_path).exists()
    assert Path(result.curated_entities_path).exists()
    assert Path(result.source_manifest_path).exists()
    assert result.warnings == []

    manifest = json.loads(Path(result.source_manifest_path).read_text(encoding="utf-8"))
    assert manifest["pack_id"] == "finance-en"
    assert manifest["snapshot"] == "2026-04-10"
    sources = {item["name"]: item for item in manifest["sources"]}
    assert sources["sec-company-tickers"]["source_url"] == remote_sources["sec_companies_url"]
    assert sources["sec-submissions"]["source_url"] == remote_sources["sec_submissions_url"]
    assert (
        sources["sec-companyfacts"]["source_url"]
        == remote_sources["sec_companyfacts_url"]
    )
    assert (
        sources["symbol-directory"]["source_url"]
        == remote_sources["symbol_directory_url"]
    )
    assert sources["other-listed"]["source_url"] == remote_sources["other_listed_url"]
    assert sources["finance-people"]["source_url"] == remote_sources["finance_people_url"]
    assert sources["curated-finance-entities"]["source_url"].startswith("operator://")
    curated_entities = json.loads(Path(result.curated_entities_path).read_text(encoding="utf-8"))
    assert any(
        item["canonical_text"] == "S&P 500"
        for item in curated_entities.get("entities", [])
    )
    assert any(
        item["canonical_text"] == "Brent crude"
        for item in curated_entities.get("entities", [])
    )

    bundle = build_finance_source_bundle(
        sec_companies_path=result.sec_companies_path,
        sec_submissions_path=result.sec_submissions_path,
        sec_companyfacts_path=result.sec_companyfacts_path,
        symbol_directory_path=result.symbol_directory_path,
        other_listed_path=result.other_listed_path,
        finance_people_path=result.finance_people_path,
        curated_entities_path=result.curated_entities_path,
        output_dir=tmp_path / "bundles",
    )
    assert bundle.sec_issuer_count == 2
    assert bundle.symbol_count == 3
    assert bundle.person_count == 2
    assert bundle.company_equity_ticker_count == 3
    assert bundle.company_name_enriched_issuer_count == 2
    assert bundle.curated_entity_count == len(DEFAULT_CURATED_FINANCE_ENTITIES)


def test_fetch_finance_source_snapshot_rejects_existing_snapshot_dir(
    tmp_path: Path,
) -> None:
    remote_sources = create_finance_remote_sources(tmp_path / "remote")
    output_root = tmp_path / "raw" / "finance-en"

    fetch_finance_source_snapshot(
        output_dir=output_root,
        snapshot="2026-04-10",
        sec_companies_url=remote_sources["sec_companies_url"],
        sec_submissions_url=remote_sources["sec_submissions_url"],
        sec_companyfacts_url=remote_sources["sec_companyfacts_url"],
        symbol_directory_url=remote_sources["symbol_directory_url"],
        other_listed_url=remote_sources["other_listed_url"],
        finance_people_url=remote_sources["finance_people_url"],
    )

    try:
        fetch_finance_source_snapshot(
            output_dir=output_root,
            snapshot="2026-04-10",
            sec_companies_url=remote_sources["sec_companies_url"],
            sec_submissions_url=remote_sources["sec_submissions_url"],
            sec_companyfacts_url=remote_sources["sec_companyfacts_url"],
            symbol_directory_url=remote_sources["symbol_directory_url"],
            other_listed_url=remote_sources["other_listed_url"],
            finance_people_url=remote_sources["finance_people_url"],
        )
    except FileExistsError as exc:
        assert "already exists" in str(exc)
    else:
        raise AssertionError("Expected FileExistsError for an immutable snapshot dir.")


def test_fetch_finance_source_snapshot_can_derive_people_from_sec_proxy_filings(
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

    assert result.finance_people_url.startswith("operator://ades/sec-proxy-people-entities")
    assert any("bounded to 2 companies" in warning for warning in result.warnings)
    people_payload = json.loads(Path(result.finance_people_path).read_text(encoding="utf-8"))
    extracted_names = {
        item["canonical_text"] for item in people_payload.get("entities", [])
    }
    assert extracted_names == {
        "Jane Doe",
        "John Roe",
        "Lucy Stone",
        "Mark Smith",
        "Mary Major",
    }


def test_sec_proxy_people_extractor_ignores_sentence_level_title_fragments() -> None:
    text = """
    <html>
      <body>
        <p>Jane Doe | Chief Executive Officer</p>
        <p>Mr. Gregoire served as Executive Director at Electronic Data Systems (EDS) and has been chair of multiple committees.</p>
        <p>Our Corporate Secretary at Advanced Micro Devices, Inc. will respond to stockholder communications.</p>
        <p>Director Skills Matrix</p>
        <p>Director | Fees earned or</p>
        <p>CHIEF EXECUTIVE OFFICER PAY RATIO</p>
        <p>Chief Financial Officer: John Roe</p>
        <p>Outside directors receive cash fees as compensation for his or her service as a director.</p>
      </body>
    </html>
    """

    entities = _extract_people_from_sec_proxy_document(
        text=text,
        employer_cik="0000000001",
        employer_name="Example Issuer",
        employer_ticker="EXMPL",
        filing_date="2026-04-19",
        filing_url="https://example.test/proxy.htm",
        source_form="DEF 14A",
    )

    assert [item["canonical_text"] for item in entities] == ["Jane Doe"]
