from pathlib import Path

from fastapi.testclient import TestClient

from ades import generate_pack_source
from ades.service.app import create_app
from tests.medical_bundle_helpers import create_medical_raw_snapshots


def test_medical_bundle_builder_endpoint_creates_bundle_directory(tmp_path: Path) -> None:
    snapshots = create_medical_raw_snapshots(tmp_path / "snapshots")
    output_dir = tmp_path / "bundles"

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/build-medical-bundle",
        json={
            "disease_ontology_path": str(snapshots["disease_ontology"]),
            "hgnc_genes_path": str(snapshots["hgnc_genes"]),
            "uniprot_proteins_path": str(snapshots["uniprot_proteins"]),
            "clinical_trials_path": str(snapshots["clinical_trials"]),
            "orange_book_products_path": str(snapshots["orange_book_products"]),
            "curated_entities_path": str(snapshots["curated_entities"]),
            "output_dir": str(output_dir),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "medical-en"
    assert payload["entity_record_count"] == 7
    assert payload["drug_count"] == 1
    assert Path(payload["sources_lock_path"]).exists()

    generated = generate_pack_source(
        payload["bundle_dir"],
        output_dir=tmp_path / "generated-packs",
    )
    assert Path(generated.pack_dir).exists()
