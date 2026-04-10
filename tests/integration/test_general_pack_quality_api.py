from pathlib import Path

from ades import build_general_source_bundle, validate_general_pack_quality
from tests.general_bundle_helpers import create_general_raw_snapshots


def test_public_api_can_validate_generated_general_pack_quality(
    tmp_path: Path,
) -> None:
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

    assert report.passed is True
    assert report.pack_id == "general-en"
    assert report.expected_recall == 1.0
