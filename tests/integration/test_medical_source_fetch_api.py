from pathlib import Path

from ades import fetch_medical_source_snapshot
from tests.medical_bundle_helpers import create_medical_remote_sources


def test_public_api_can_fetch_medical_source_snapshot(tmp_path: Path) -> None:
    remote_sources = create_medical_remote_sources(tmp_path / "remote")

    result = fetch_medical_source_snapshot(
        output_dir=tmp_path / "raw" / "medical-en",
        snapshot="2026-04-10",
        disease_ontology_url=remote_sources["disease_ontology_url"],
        hgnc_genes_url=remote_sources["hgnc_genes_url"],
        uniprot_proteins_url=remote_sources["uniprot_proteins_url"],
        clinical_trials_url=remote_sources["clinical_trials_url"],
        orange_book_url=remote_sources["orange_book_url"],
        uniprot_max_records=10,
        clinical_trials_max_records=10,
    )

    assert result.pack_id == "medical-en"
    assert result.snapshot == "2026-04-10"
    assert Path(result.source_manifest_path).exists()
    assert Path(result.orange_book_path).exists()
