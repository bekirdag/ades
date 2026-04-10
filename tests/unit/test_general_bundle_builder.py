import json
from hashlib import sha256
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
    assert Path(result.sources_lock_path).exists()
    assert result.source_count == 3
    assert result.entity_record_count == 13
    assert result.rule_record_count == 2
    assert result.wikidata_entity_count == 10
    assert result.geonames_location_count == 2
    assert result.curated_entity_count == 1
    assert result.warnings == []

    bundle_manifest = json.loads(Path(result.bundle_manifest_path).read_text(encoding="utf-8"))
    assert bundle_manifest["pack_id"] == "general-en"
    assert bundle_manifest["entities_path"] == "normalized/entities.jsonl"
    assert len(bundle_manifest["sources"]) == 3
    sources_lock = json.loads(Path(result.sources_lock_path).read_text(encoding="utf-8"))
    assert sources_lock["pack_id"] == "general-en"
    assert sources_lock["version"] == "0.2.0"
    assert sources_lock["bundle_manifest_path"] == "bundle.json"
    assert sources_lock["entities_path"] == "normalized/entities.jsonl"
    assert sources_lock["rules_path"] == "normalized/rules.jsonl"
    assert sources_lock["source_count"] == 3
    lock_sources = {item["name"]: item for item in sources_lock["sources"]}
    assert lock_sources["wikidata-general-entities"]["snapshot_sha256"] == _sha256(
        snapshots["wikidata_entities"]
    )
    assert lock_sources["geonames-places"]["snapshot_sha256"] == _sha256(
        snapshots["geonames_places"]
    )
    assert lock_sources["curated-general-entities"]["snapshot_sha256"] == _sha256(
        snapshots["curated_entities"]
    )
    assert lock_sources["wikidata-general-entities"]["adapter"] == "wikidata_general_entities"
    assert lock_sources["geonames-places"]["adapter"] == "geonames_places_delimited"
    assert lock_sources["curated-general-entities"]["adapter"] == "curated_general_entities"
    assert all(item["adapter_version"] == "1" for item in lock_sources.values())

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
    assert any(
        item["entity_type"] == "location" and item["canonical_text"] == "London"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Satya Nadella"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Jensen Huang"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "organization" and item["canonical_text"] == "Nvidia"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "organization" and item["canonical_text"] == "Anthropic"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Sam Altman"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "organization"
        and item["canonical_text"] == "Google DeepMind"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Sundar Pichai"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Demis Hassabis"
        for item in entity_records
    )
    sundar_record = next(
        item for item in entity_records if item["canonical_text"] == "Sundar Pichai"
    )
    assert "Pichai" not in sundar_record["aliases"]
    assert "Sundara Pichai" in sundar_record["aliases"]
    demis_record = next(
        item for item in entity_records if item["canonical_text"] == "Demis Hassabis"
    )
    assert "Hassabis" not in demis_record["aliases"]
    assert "Sir Demis Hassabis" in demis_record["aliases"]

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


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
