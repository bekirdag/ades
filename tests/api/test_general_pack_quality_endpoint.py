import json
from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.general_bundle import build_general_source_bundle
from ades.service.app import create_app
from tests.general_bundle_helpers import create_general_raw_snapshots


def test_general_quality_endpoint_validates_generated_pack(tmp_path) -> None:
    snapshots = create_general_raw_snapshots(tmp_path / "snapshots")
    bundle = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=snapshots["geonames_places"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/validate-general-quality",
        json={
            "bundle_dir": bundle.bundle_dir,
            "output_dir": str(tmp_path / "quality-output"),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "general-en"
    assert payload["fixture_profile"] == "benchmark"
    assert payload["passed"] is True
    assert payload["expected_entity_count"] == 22
    assert payload["alias_count"] == 34


def test_general_quality_endpoint_uses_general_pack_default_ambiguity_budget(
    tmp_path,
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

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/validate-general-quality",
        json={
            "bundle_dir": bundle.bundle_dir,
            "output_dir": str(tmp_path / "quality-output"),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["passed"] is True
    assert payload["ambiguous_alias_count"] > 0
