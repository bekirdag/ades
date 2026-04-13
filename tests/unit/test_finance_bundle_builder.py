import json
from hashlib import sha256
from pathlib import Path
import zipfile

from ades.packs.finance_bundle import build_finance_source_bundle
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_build_finance_source_bundle_writes_normalized_bundle(tmp_path: Path) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path)

    result = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        sec_submissions_path=snapshots["sec_submissions"],
        sec_companyfacts_path=snapshots["sec_companyfacts"],
        symbol_directory_path=snapshots["symbol_directory"],
        other_listed_path=snapshots["other_listed"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    assert result.pack_id == "finance-en"
    assert result.version == "0.2.0"
    assert Path(result.sources_lock_path).exists()
    assert result.source_count == 6
    assert result.entity_record_count == 10
    assert result.rule_record_count == 2
    assert result.sec_issuer_count == 2
    assert result.symbol_count == 3
    assert result.curated_entity_count == 5
    assert result.warnings == []

    bundle_manifest = json.loads(Path(result.bundle_manifest_path).read_text(encoding="utf-8"))
    assert bundle_manifest["pack_id"] == "finance-en"
    assert bundle_manifest["entities_path"] == "normalized/entities.jsonl"
    assert bundle_manifest["stoplisted_aliases"] == ["N/A", "ON", "USD"]
    assert len(bundle_manifest["sources"]) == 6
    sources_lock = json.loads(Path(result.sources_lock_path).read_text(encoding="utf-8"))
    assert sources_lock["pack_id"] == "finance-en"
    assert sources_lock["version"] == "0.2.0"
    assert sources_lock["bundle_manifest_path"] == "bundle.json"
    assert sources_lock["entities_path"] == "normalized/entities.jsonl"
    assert sources_lock["rules_path"] == "normalized/rules.jsonl"
    assert sources_lock["source_count"] == 6
    lock_sources = {item["name"]: item for item in sources_lock["sources"]}
    assert lock_sources["sec-company-tickers"]["snapshot_sha256"] == _sha256(
        snapshots["sec_companies"]
    )
    assert lock_sources["sec-submissions"]["snapshot_sha256"] == _sha256(
        snapshots["sec_submissions"]
    )
    assert lock_sources["sec-companyfacts"]["snapshot_sha256"] == _sha256(
        snapshots["sec_companyfacts"]
    )
    assert lock_sources["symbol-directory"]["snapshot_sha256"] == _sha256(
        snapshots["symbol_directory"]
    )
    assert lock_sources["other-listed"]["snapshot_sha256"] == _sha256(
        snapshots["other_listed"]
    )
    assert lock_sources["curated-finance-entities"]["snapshot_sha256"] == _sha256(
        snapshots["curated_entities"]
    )
    assert lock_sources["sec-company-tickers"]["adapter"] == "sec_company_tickers_json"
    assert lock_sources["sec-submissions"]["adapter"] == "sec_submissions_zip"
    assert lock_sources["sec-companyfacts"]["adapter"] == "sec_companyfacts_zip"
    assert lock_sources["symbol-directory"]["adapter"] == "delimited_symbol_directory"
    assert lock_sources["other-listed"]["adapter"] == "delimited_symbol_directory"
    assert lock_sources["curated-finance-entities"]["adapter"] == "curated_finance_entities"
    assert all(item["adapter_version"] == "1" for item in lock_sources.values())

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(
        item["entity_type"] == "issuer" and item["canonical_text"] == "Issuer Alpha Holdings"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "ticker" and item["canonical_text"] == "TICKA"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "ticker" and item["canonical_text"] == "TICKC"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "exchange" and item["canonical_text"] == "EXCHX"
        for item in entity_records
    )
    issuer_alpha_record = next(
        item
        for item in entity_records
        if item["entity_type"] == "issuer" and item["canonical_text"] == "Issuer Alpha Holdings"
    )
    assert "Issuer Alpha Legacy" in issuer_alpha_record["aliases"]

    rule_records = [
        json.loads(line)
        for line in Path(result.rules_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {
        "name": "currency_amount",
        "label": "currency_amount",
        "kind": "regex",
        "pattern": r"(USD|EUR|GBP|TRY)\s?[0-9]+(?:\.[0-9]+)?",
    } in rule_records
    assert {
        "name": "ticker_symbol",
        "label": "ticker",
        "kind": "regex",
        "pattern": r"\$[A-Z]{1,5}",
    } in rule_records


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def test_build_finance_source_bundle_ignores_unrelated_sec_zip_members(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path)

    with zipfile.ZipFile(snapshots["sec_submissions"], "a") as archive:
        archive.writestr("CIK9999999999.json", "{not-valid-json")
        archive.writestr("CIK0000789019-submissions-001.json", "{not-valid-json")
    with zipfile.ZipFile(snapshots["sec_companyfacts"], "a") as archive:
        archive.writestr("CIK9999999999.json", "{not-valid-json")

    result = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        sec_submissions_path=snapshots["sec_submissions"],
        sec_companyfacts_path=snapshots["sec_companyfacts"],
        symbol_directory_path=snapshots["symbol_directory"],
        other_listed_path=snapshots["other_listed"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    assert result.sec_issuer_count == 2
    assert result.entity_record_count == 10
