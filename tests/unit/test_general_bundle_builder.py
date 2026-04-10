import json
from pathlib import Path

from ades.packs.general_bundle import build_general_source_bundle
from tests.general_bundle_helpers import create_general_raw_snapshots


def test_build_general_source_bundle_writes_normalized_bundle(tmp_path: Path) -> None:
    snapshots = create_general_raw_snapshots(tmp_path)

    result = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=snapshots["geonames_places"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    assert result.pack_id == "general-en"
    assert result.version == "0.2.0"
    assert result.source_count == 3
    assert result.entity_record_count == 4
    assert result.rule_record_count == 2
    assert result.wikidata_entity_count == 2
    assert result.geonames_location_count == 1
    assert result.curated_entity_count == 1
    assert result.warnings == []

    bundle_manifest = json.loads(Path(result.bundle_manifest_path).read_text(encoding="utf-8"))
    assert bundle_manifest["pack_id"] == "general-en"
    assert bundle_manifest["entities_path"] == "normalized/entities.jsonl"
    assert len(bundle_manifest["sources"]) == 3

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Tim Cook"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "organization" and item["canonical_text"] == "OpenAI"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "location" and item["canonical_text"] == "Istanbul"
        for item in entity_records
    )

    rule_records = [
        json.loads(line)
        for line in Path(result.rules_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {
        "name": "email_address",
        "label": "email_address",
        "kind": "regex",
        "pattern": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    } in rule_records
    assert {
        "name": "url",
        "label": "url",
        "kind": "regex",
        "pattern": r"https?://[^\s]+",
    } in rule_records
