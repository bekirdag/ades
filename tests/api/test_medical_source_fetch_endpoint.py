from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app
from tests.medical_bundle_helpers import create_medical_remote_sources


def test_medical_source_fetch_endpoint_downloads_snapshot(tmp_path: Path) -> None:
    remote_sources = create_medical_remote_sources(tmp_path / "remote")

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/fetch-medical-sources",
        json={
            "output_dir": str(tmp_path / "raw" / "medical-en"),
            "snapshot": "2026-04-10",
            "disease_ontology_url": remote_sources["disease_ontology_url"],
            "hgnc_genes_url": remote_sources["hgnc_genes_url"],
            "uniprot_proteins_url": remote_sources["uniprot_proteins_url"],
            "clinical_trials_url": remote_sources["clinical_trials_url"],
            "orange_book_url": remote_sources["orange_book_url"],
            "uniprot_max_records": 10,
            "clinical_trials_max_records": 10,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "medical-en"
    assert payload["snapshot"] == "2026-04-10"
    assert Path(payload["curated_entities_path"]).exists()
    assert Path(payload["orange_book_path"]).exists()
