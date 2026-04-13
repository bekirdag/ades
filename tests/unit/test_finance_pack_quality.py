import json
from pathlib import Path

from ades.packs.finance_bundle import build_finance_source_bundle
from ades.packs.finance_quality import validate_finance_pack_quality
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_validate_finance_pack_quality_reports_passing_fixture_metrics(tmp_path: Path) -> None:
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
    assert report.fixture_profile == "benchmark"
    assert report.fixture_count == 5
    assert report.passed_case_count == 5
    assert report.failed_case_count == 0
    assert report.expected_entity_count == 16
    assert report.actual_entity_count == 16
    assert report.matched_expected_entity_count == 16
    assert report.missing_expected_entity_count == 0
    assert report.unexpected_entity_count == 0
    assert report.expected_recall == 1.0
    assert report.precision == 1.0
    assert report.alias_count == 18
    assert report.unique_canonical_count == 9
    assert report.rule_count == 2
    assert Path(report.generated_pack_dir).exists()
    assert Path(report.registry_index_path).exists()
    assert Path(report.storage_root).exists()


def test_validate_finance_pack_quality_flags_unexpected_alias_hits(tmp_path: Path) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        symbol_directory_path=snapshots["symbol_directory"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )
    entities_path = Path(bundle.entities_path)
    with entities_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "entity_id": "exchange:market",
                    "entity_type": "exchange",
                    "canonical_text": "Market",
                    "aliases": [],
                    "source_name": "curated-finance-entities",
                    "source_id": "market",
                }
            )
            + "\n"
        )

    report = validate_finance_pack_quality(
        bundle.bundle_dir,
        output_dir=tmp_path / "quality-output",
        max_unexpected_hits=0,
    )

    assert report.passed is False
    assert report.unexpected_entity_count >= 1
    assert any("Unexpected entity count" in item for item in report.failures)
    produce_case = next(case for case in report.cases if case.name == "produce-market-non-hit")
    assert any(entity.text == "market" and entity.label == "exchange" for entity in produce_case.unexpected_entities)
