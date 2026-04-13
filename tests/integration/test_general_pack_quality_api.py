import json
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
    assert report.fixture_profile == "benchmark"
    assert report.expected_recall == 1.0
    assert report.expected_entity_count == 22


def test_public_api_uses_general_pack_default_ambiguity_budget(
    tmp_path: Path,
) -> None:
    snapshots = create_general_raw_snapshots(tmp_path / "snapshots")
    bundle = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=snapshots["geonames_places"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )
    entities_path = Path(bundle.entities_path)
    entity_rows = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    entity_rows.append(
        {
            "entity_id": "curated:test-byzantium-labs",
            "entity_type": "organization",
            "canonical_text": "Byzantium Labs",
            "aliases": ["Byzantium"],
            "source_name": "curated-general",
        }
    )
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in entity_rows),
        encoding="utf-8",
    )

    report = validate_general_pack_quality(
        bundle.bundle_dir,
        output_dir=tmp_path / "quality-output",
    )

    assert report.passed is True
    assert report.ambiguous_alias_count > 0
