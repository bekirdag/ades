import json
from pathlib import Path

from ades.packs.finance_bundle import build_finance_source_bundle
from ades.packs.finance_sources import fetch_finance_source_snapshot
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
    )

    assert result.pack_id == "finance-en"
    assert result.snapshot == "2026-04-10"
    assert result.source_count == 6
    assert result.curated_entity_count == 0
    assert Path(result.snapshot_dir).exists()
    assert Path(result.sec_companies_path).exists()
    assert Path(result.sec_submissions_path).exists()
    assert Path(result.sec_companyfacts_path).exists()
    assert Path(result.symbol_directory_path).exists()
    assert Path(result.other_listed_path).exists()
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
    assert sources["curated-finance-entities"]["source_url"].startswith("operator://")

    bundle = build_finance_source_bundle(
        sec_companies_path=result.sec_companies_path,
        sec_submissions_path=result.sec_submissions_path,
        sec_companyfacts_path=result.sec_companyfacts_path,
        symbol_directory_path=result.symbol_directory_path,
        other_listed_path=result.other_listed_path,
        curated_entities_path=result.curated_entities_path,
        output_dir=tmp_path / "bundles",
    )
    assert bundle.sec_issuer_count == 2
    assert bundle.symbol_count == 3
    assert bundle.curated_entity_count == 0


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
        )
    except FileExistsError as exc:
        assert "already exists" in str(exc)
    else:
        raise AssertionError("Expected FileExistsError for an immutable snapshot dir.")
