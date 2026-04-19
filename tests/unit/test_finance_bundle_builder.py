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
        finance_people_path=snapshots["finance_people"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    assert result.pack_id == "finance-en"
    assert result.version == "0.2.0"
    assert Path(result.sources_lock_path).exists()
    assert result.source_count == 7
    assert result.entity_record_count == 17
    assert result.rule_record_count == 5
    assert result.sec_issuer_count == 2
    assert result.symbol_count == 3
    assert result.person_count == 2
    assert result.company_equity_ticker_count == 3
    assert result.company_name_enriched_issuer_count == 2
    assert result.curated_entity_count == 10
    assert result.warnings == []

    bundle_manifest = json.loads(Path(result.bundle_manifest_path).read_text(encoding="utf-8"))
    assert bundle_manifest["pack_id"] == "finance-en"
    assert bundle_manifest["entities_path"] == "normalized/entities.jsonl"
    assert bundle_manifest["stoplisted_aliases"] == ["N/A", "ON", "USD"]
    assert bundle_manifest["coverage"] == {
        "sec_issuer_count": 2,
        "person_count": 2,
        "company_equity_ticker_count": 3,
        "company_name_enriched_issuer_count": 2,
    }
    assert len(bundle_manifest["sources"]) == 7
    sources_lock = json.loads(Path(result.sources_lock_path).read_text(encoding="utf-8"))
    assert sources_lock["pack_id"] == "finance-en"
    assert sources_lock["version"] == "0.2.0"
    assert sources_lock["bundle_manifest_path"] == "bundle.json"
    assert sources_lock["entities_path"] == "normalized/entities.jsonl"
    assert sources_lock["rules_path"] == "normalized/rules.jsonl"
    assert sources_lock["source_count"] == 7
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
    assert lock_sources["finance-people"]["snapshot_sha256"] == _sha256(
        snapshots["finance_people"]
    )
    assert lock_sources["curated-finance-entities"]["snapshot_sha256"] == _sha256(
        snapshots["curated_entities"]
    )
    assert lock_sources["sec-company-tickers"]["adapter"] == "sec_company_tickers_json"
    assert lock_sources["sec-submissions"]["adapter"] == "sec_submissions_zip"
    assert lock_sources["sec-companyfacts"]["adapter"] == "sec_companyfacts_zip"
    assert lock_sources["symbol-directory"]["adapter"] == "delimited_symbol_directory"
    assert lock_sources["other-listed"]["adapter"] == "delimited_symbol_directory"
    assert lock_sources["finance-people"]["adapter"] == "finance_people_entities"
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
    assert any(
        item["entity_type"] == "market_index" and item["canonical_text"] == "S&P 500"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "commodity" and item["canonical_text"] == "Brent crude"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Jane Doe"
        for item in entity_records
    )
    issuer_alpha_record = next(
        item
        for item in entity_records
        if item["entity_type"] == "issuer" and item["canonical_text"] == "Issuer Alpha Holdings"
    )
    assert "Issuer Alpha Legacy" in issuer_alpha_record["aliases"]
    assert issuer_alpha_record["metadata"]["company_names"] == ["Issuer Alpha Holdings"]
    jane_doe_record = next(
        item
        for item in entity_records
        if item["entity_type"] == "person" and item["canonical_text"] == "Jane Doe"
    )
    assert jane_doe_record["metadata"]["employer_ticker"] == "TICKA"
    assert "Jane A. Doe" in jane_doe_record["aliases"]

    rule_records = [
        json.loads(line)
        for line in Path(result.rules_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {
        "name": "currency_amount_code",
        "label": "currency_amount",
        "kind": "regex",
        "pattern": r"\b(?:USD|EUR|GBP|TRY|JPY|CNY|CNH|CHF|CAD|AUD|NZD|HKD|SGD|SEK|NOK|DKK|INR|KRW|TWD|MXN|BRL|ZAR|RUB)\b\s?[0-9]+(?:\.[0-9]+)?(?:\s?(?:million|billion|trillion|mn|bn|tn|m|b|t))?",
    } in rule_records
    assert {
        "name": "currency_amount_symbol",
        "label": "currency_amount",
        "kind": "regex",
        "pattern": r"(?:US|C|A|HK|NZ|S)?[$€£¥₹]\s?[0-9]+(?:\.[0-9]+)?(?:\s?(?:million|billion|trillion|mn|bn|tn|m|b|t))?",
    } in rule_records
    assert {
        "name": "ticker_symbol",
        "label": "ticker",
        "kind": "regex",
        "pattern": r"\$[A-Z]{1,5}",
    } in rule_records
    assert {
        "name": "percentage",
        "label": "percentage",
        "kind": "regex",
        "pattern": r"[-+]?[0-9]+(?:\.[0-9]+)?%",
    } in rule_records
    assert {
        "name": "basis_points",
        "label": "basis_points",
        "kind": "regex",
        "pattern": r"[-+]?[0-9]+(?:\.[0-9]+)?\s?(?:bp|bps|basis points?)",
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
        finance_people_path=snapshots["finance_people"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    assert result.sec_issuer_count == 2
    assert result.entity_record_count == 17


def test_build_finance_source_bundle_preserves_optional_curated_fields(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path)
    curated_payload = json.loads(snapshots["curated_entities"].read_text(encoding="utf-8"))
    curated_payload["entities"].append(
        {
            "entity_type": "market_index",
            "canonical_text": "Custom Volatility Index",
            "aliases": ["CVI"],
            "blocked_aliases": ["custom volatility"],
            "build_only": True,
            "metadata": {"region": "test"},
        }
    )
    snapshots["curated_entities"].write_text(
        json.dumps(curated_payload, indent=2) + "\n",
        encoding="utf-8",
    )

    result = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        sec_submissions_path=snapshots["sec_submissions"],
        sec_companyfacts_path=snapshots["sec_companyfacts"],
        symbol_directory_path=snapshots["symbol_directory"],
        other_listed_path=snapshots["other_listed"],
        finance_people_path=snapshots["finance_people"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    custom_record = next(
        item
        for item in entity_records
        if item["canonical_text"] == "Custom Volatility Index"
    )
    assert custom_record["blocked_aliases"] == ["custom volatility"]
    assert custom_record["build_only"] is True
    assert custom_record["metadata"] == {"region": "test"}


def test_build_finance_source_bundle_drops_surname_only_people_aliases(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path)
    people_payload = json.loads(snapshots["finance_people"].read_text(encoding="utf-8"))
    people_payload["entities"][0]["aliases"].append("Doe")
    snapshots["finance_people"].write_text(
        json.dumps(people_payload, indent=2) + "\n",
        encoding="utf-8",
    )

    result = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        sec_submissions_path=snapshots["sec_submissions"],
        sec_companyfacts_path=snapshots["sec_companyfacts"],
        symbol_directory_path=snapshots["symbol_directory"],
        other_listed_path=snapshots["other_listed"],
        finance_people_path=snapshots["finance_people"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    jane_doe_record = next(
        item
        for item in entity_records
        if item["entity_type"] == "person" and item["canonical_text"] == "Jane Doe"
    )
    assert "Jane A. Doe" in jane_doe_record["aliases"]
    assert "Doe" not in jane_doe_record["aliases"]


def test_build_finance_source_bundle_blocks_high_risk_exact_ticker_aliases(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path)
    snapshots["symbol_directory"].write_text(
        "\n".join(
            [
                "Symbol|Security Name|Test Issue",
                "BY|Byline Bancorp, Inc. Common Stock|N",
                "BETA|Beta Technologies, Inc. Class A Common Stock|N",
                "AAPL|Apple Inc. Common Stock|N",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    snapshots["other_listed"].write_text(
        "\n".join(
            [
                "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|EXCHX Symbol",
                "TICKC|Issuer Gamma Group Common Units|N|TICKC|N|100|N|N",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        sec_submissions_path=snapshots["sec_submissions"],
        sec_companyfacts_path=snapshots["sec_companyfacts"],
        symbol_directory_path=snapshots["symbol_directory"],
        other_listed_path=snapshots["other_listed"],
        finance_people_path=snapshots["finance_people"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    by_record = next(
        item for item in entity_records if item["entity_type"] == "ticker" and item["canonical_text"] == "BY"
    )
    beta_record = next(
        item for item in entity_records if item["entity_type"] == "ticker" and item["canonical_text"] == "BETA"
    )
    aapl_record = next(
        item for item in entity_records if item["entity_type"] == "ticker" and item["canonical_text"] == "AAPL"
    )
    assert by_record["blocked_aliases"] == ["BY"]
    assert beta_record["blocked_aliases"] == ["BETA"]
    assert "blocked_aliases" not in aapl_record


def test_build_finance_source_bundle_merges_formal_company_names_from_equity_listings(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path)
    sec_payload = json.loads(snapshots["sec_companies"].read_text(encoding="utf-8"))
    sec_payload["0"]["title"] = "ISSUER ALPHA CORP"
    snapshots["sec_companies"].write_text(
        json.dumps(sec_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    with zipfile.ZipFile(
        snapshots["sec_submissions"],
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        archive.writestr(
            "CIK0000320193.json",
            json.dumps(
                {
                    "cik": "0000320193",
                    "name": "ISSUER ALPHA CORP",
                    "tickers": ["TICKA"],
                    "exchanges": ["Exchange Alpha"],
                    "formerNames": [],
                },
                indent=2,
            )
            + "\n",
        )
    with zipfile.ZipFile(
        snapshots["sec_companyfacts"],
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        archive.writestr(
            "CIK0000320193.json",
            json.dumps(
                {
                    "cik": 320193,
                    "entityName": "ISSUER ALPHA CORP",
                },
                indent=2,
            )
            + "\n",
        )
    snapshots["symbol_directory"].write_text(
        "\n".join(
            [
                "Symbol|Security Name|Test Issue",
                "TICKA|Issuer Alpha Corporation Common Stock|N",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    snapshots["other_listed"].write_text(
        "\n".join(
            [
                "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|EXCHX Symbol",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        sec_submissions_path=snapshots["sec_submissions"],
        sec_companyfacts_path=snapshots["sec_companyfacts"],
        symbol_directory_path=snapshots["symbol_directory"],
        other_listed_path=snapshots["other_listed"],
        finance_people_path=snapshots["finance_people"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    assert result.company_equity_ticker_count == 1
    assert result.company_name_enriched_issuer_count == 1

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    issuer_alpha_record = next(
        item
        for item in entity_records
        if item["entity_type"] == "issuer" and item["canonical_text"] == "ISSUER ALPHA CORP"
    )
    assert "Issuer Alpha Corporation" in issuer_alpha_record["aliases"]
    assert issuer_alpha_record["metadata"]["company_names"] == ["Issuer Alpha Corporation"]


def test_build_finance_source_bundle_prefers_title_case_company_name_over_sec_all_caps(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path)
    sec_payload = json.loads(snapshots["sec_companies"].read_text(encoding="utf-8"))
    sec_payload["0"]["title"] = "ISSUER ALPHA CORP"
    snapshots["sec_companies"].write_text(
        json.dumps(sec_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    with zipfile.ZipFile(
        snapshots["sec_submissions"],
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        archive.writestr(
            "CIK0000320193.json",
            json.dumps(
                {
                    "cik": "0000320193",
                    "name": "ISSUER ALPHA CORP",
                    "tickers": ["TICKA"],
                    "exchanges": ["Exchange Alpha"],
                    "formerNames": [],
                },
                indent=2,
            )
            + "\n",
        )
    with zipfile.ZipFile(
        snapshots["sec_companyfacts"],
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        archive.writestr(
            "CIK0000320193.json",
            json.dumps(
                {
                    "cik": 320193,
                    "entityName": "ISSUER ALPHA CORPORATION",
                },
                indent=2,
            )
            + "\n",
        )
    snapshots["symbol_directory"].write_text(
        "\n".join(
            [
                "Symbol|Security Name|Test Issue",
                "TICKA|Issuer Alpha Corporation Common Stock|N",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    snapshots["other_listed"].write_text(
        "\n".join(
            [
                "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|EXCHX Symbol",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        sec_submissions_path=snapshots["sec_submissions"],
        sec_companyfacts_path=snapshots["sec_companyfacts"],
        symbol_directory_path=snapshots["symbol_directory"],
        other_listed_path=snapshots["other_listed"],
        finance_people_path=snapshots["finance_people"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    issuer_alpha_record = next(
        item
        for item in entity_records
        if item["entity_type"] == "issuer" and item["canonical_text"] == "ISSUER ALPHA CORP"
    )

    assert "Issuer Alpha Corporation" in issuer_alpha_record["aliases"]
    assert "ISSUER ALPHA CORPORATION" not in issuer_alpha_record["aliases"]
