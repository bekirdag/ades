from pathlib import Path

from ades.packs.general_bundle import build_general_source_bundle
from ades.packs.general_quality import validate_general_pack_quality
from tests.general_bundle_helpers import create_general_raw_snapshots


def test_validate_general_pack_quality_reports_passing_fixture_metrics(tmp_path: Path) -> None:
    snapshots = create_general_raw_snapshots(tmp_path / "snapshots")
    bundle = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=snapshots["geonames_places"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    report = validate_general_pack_quality(
        bundle.bundle_dir,
        output_dir=tmp_path / "quality-output",
    )

    assert report.pack_id == "general-en"
    assert report.fixture_profile == "default"
    assert report.fixture_count == 10
    assert report.passed_case_count == 10
    assert report.failed_case_count == 0
    assert report.expected_entity_count == 22
    assert report.actual_entity_count == 22
    assert report.matched_expected_entity_count == 22
    assert report.missing_expected_entity_count == 0
    assert report.unexpected_entity_count == 0
    assert report.expected_recall == 1.0
    assert report.precision == 1.0
    assert report.alias_count == 29
    assert report.unique_canonical_count == 16
    assert report.rule_count == 2
    assert report.dropped_alias_count == 0
    assert report.ambiguous_alias_count == 0
    assert report.dropped_alias_ratio == 0.0
    assert report.passed is True
    assert report.failures == []
    assert report.warnings == []
