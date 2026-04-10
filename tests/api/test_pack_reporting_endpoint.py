from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.medical_bundle import build_medical_source_bundle
from ades.service.app import create_app
from tests.medical_bundle_helpers import create_medical_raw_snapshots


def test_registry_report_pack_endpoint_returns_generated_pack_statistics(tmp_path: Path) -> None:
    snapshots = create_medical_raw_snapshots(tmp_path / "snapshots")
    bundle = build_medical_source_bundle(
        disease_ontology_path=snapshots["disease_ontology"],
        hgnc_genes_path=snapshots["hgnc_genes"],
        uniprot_proteins_path=snapshots["uniprot_proteins"],
        clinical_trials_path=snapshots["clinical_trials"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/report-pack",
        json={
            "bundle_dir": bundle.bundle_dir,
            "output_dir": str(tmp_path / "generated-packs"),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "medical-en"
    assert payload["alias_count"] == 12
    assert payload["unique_canonical_count"] == 6
    assert payload["label_distribution"]["gene"] == 2
