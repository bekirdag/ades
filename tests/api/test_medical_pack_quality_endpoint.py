from fastapi.testclient import TestClient

from ades.packs.general_bundle import build_general_source_bundle
from ades.packs.medical_bundle import build_medical_source_bundle
from ades.service.app import create_app
from tests.general_bundle_helpers import create_general_raw_snapshots
from tests.medical_bundle_helpers import create_medical_raw_snapshots


def test_medical_quality_endpoint_validates_generated_pack(tmp_path) -> None:
    general_snapshots = create_general_raw_snapshots(tmp_path / "general-snapshots")
    medical_snapshots = create_medical_raw_snapshots(tmp_path / "medical-snapshots")
    general_bundle = build_general_source_bundle(
        wikidata_entities_path=general_snapshots["wikidata_entities"],
        geonames_places_path=general_snapshots["geonames_places"],
        curated_entities_path=general_snapshots["curated_entities"],
        output_dir=tmp_path / "general-bundles",
    )
    medical_bundle = build_medical_source_bundle(
        disease_ontology_path=medical_snapshots["disease_ontology"],
        hgnc_genes_path=medical_snapshots["hgnc_genes"],
        uniprot_proteins_path=medical_snapshots["uniprot_proteins"],
        clinical_trials_path=medical_snapshots["clinical_trials"],
        curated_entities_path=medical_snapshots["curated_entities"],
        output_dir=tmp_path / "medical-bundles",
    )

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/validate-medical-quality",
        json={
            "bundle_dir": medical_bundle.bundle_dir,
            "general_bundle_dir": general_bundle.bundle_dir,
            "output_dir": str(tmp_path / "quality-output"),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "medical-en"
    assert payload["passed"] is True
    assert payload["fixture_profile"] == "benchmark"
    assert payload["expected_entity_count"] == 21
