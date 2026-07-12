from pathlib import Path

from ades.packs.bdya_phase6 import build_bdya_domain_source_bundles
from ades.packs.politics_quality import validate_politics_pack_quality


def _build_politics_bundle(tmp_path: Path) -> str:
    bundles = build_bdya_domain_source_bundles(output_dir=tmp_path / "bundles")
    return next(
        bundle.bundle_dir
        for bundle in bundles
        if bundle.pack_id == "politics-vector-en"
    )


def test_validate_politics_pack_quality_reports_passing_fixture_metrics(
    tmp_path: Path,
) -> None:
    bundle_dir = _build_politics_bundle(tmp_path)

    report = validate_politics_pack_quality(
        bundle_dir,
        output_dir=tmp_path / "quality-output",
    )

    assert report.pack_id == "politics-vector-en"
    assert report.fixture_profile == "benchmark"
    assert report.fixture_count == 5
    assert report.passed_case_count == 5
    assert report.failed_case_count == 0
    assert report.expected_entity_count == 17
    assert report.actual_entity_count == 17
    assert report.matched_expected_entity_count == 17
    assert report.missing_expected_entity_count == 0
    assert report.unexpected_entity_count == 0
    assert report.expected_recall == 1.0
    assert report.precision == 1.0
    assert report.alias_count == 101
    assert report.unique_canonical_count == 24
    assert report.rule_count == 12
    assert report.dropped_alias_count == 0
    assert report.ambiguous_alias_count == 0
    assert report.passed is True
    assert report.failures == []

    negative_case = next(
        case for case in report.cases if case.name == "blocked-noisy-politics-language"
    )
    assert negative_case.actual_entities == []


def test_validate_politics_pack_quality_supports_smoke_profile(
    tmp_path: Path,
) -> None:
    bundle_dir = _build_politics_bundle(tmp_path)

    report = validate_politics_pack_quality(
        bundle_dir,
        output_dir=tmp_path / "quality-output",
        fixture_profile="smoke",
    )

    assert report.fixture_profile == "smoke"
    assert report.fixture_count == 2
    assert report.expected_entity_count == 4
    assert report.passed is True
