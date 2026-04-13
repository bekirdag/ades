from pathlib import Path

from ades.packs.medical_bundle import build_medical_source_bundle
from ades.packs.medical_sources import (
    DEFAULT_CLINICAL_TRIALS_FIELDS,
    _ensure_clinical_trials_fields,
    fetch_medical_source_snapshot,
)
from tests.medical_bundle_helpers import create_medical_remote_sources


def test_fetch_medical_source_snapshot_writes_immutable_snapshot_dir(tmp_path: Path) -> None:
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
    assert result.source_count == 6
    assert result.disease_count == 2
    assert result.gene_count == 1
    assert result.protein_count == 1
    assert result.clinical_trial_count == 1
    assert result.orange_book_product_count == 1
    assert result.curated_entity_count == 0
    assert Path(result.snapshot_dir).exists()
    assert Path(result.disease_ontology_path).exists()
    assert Path(result.hgnc_genes_path).exists()
    assert Path(result.uniprot_proteins_path).exists()
    assert Path(result.clinical_trials_path).exists()
    assert Path(result.orange_book_path).exists()
    assert Path(result.curated_entities_path).exists()
    assert Path(result.source_manifest_path).exists()
    assert result.warnings == []

    bundle = build_medical_source_bundle(
        disease_ontology_path=result.disease_ontology_path,
        hgnc_genes_path=result.hgnc_genes_path,
        uniprot_proteins_path=result.uniprot_proteins_path,
        clinical_trials_path=result.clinical_trials_path,
        orange_book_products_path=result.orange_book_path,
        curated_entities_path=result.curated_entities_path,
        output_dir=tmp_path / "bundles",
    )
    assert bundle.disease_count == 2
    assert bundle.gene_count == 1
    assert bundle.protein_count == 1
    assert bundle.clinical_trial_count == 1
    assert bundle.drug_count == 1


def test_fetch_medical_source_snapshot_rejects_existing_snapshot_dir(
    tmp_path: Path,
) -> None:
    remote_sources = create_medical_remote_sources(tmp_path / "remote")
    output_root = tmp_path / "raw" / "medical-en"

    fetch_medical_source_snapshot(
        output_dir=output_root,
        snapshot="2026-04-10",
        disease_ontology_url=remote_sources["disease_ontology_url"],
        hgnc_genes_url=remote_sources["hgnc_genes_url"],
        uniprot_proteins_url=remote_sources["uniprot_proteins_url"],
        clinical_trials_url=remote_sources["clinical_trials_url"],
        orange_book_url=remote_sources["orange_book_url"],
        uniprot_max_records=10,
        clinical_trials_max_records=10,
    )

    try:
        fetch_medical_source_snapshot(
            output_dir=output_root,
            snapshot="2026-04-10",
            disease_ontology_url=remote_sources["disease_ontology_url"],
            hgnc_genes_url=remote_sources["hgnc_genes_url"],
            uniprot_proteins_url=remote_sources["uniprot_proteins_url"],
            clinical_trials_url=remote_sources["clinical_trials_url"],
            orange_book_url=remote_sources["orange_book_url"],
            uniprot_max_records=10,
            clinical_trials_max_records=10,
        )
    except FileExistsError as exc:
        assert "already exists" in str(exc)
    else:
        raise AssertionError("Expected FileExistsError for an immutable snapshot dir.")


def test_ensure_clinical_trials_fields_adds_required_projection_to_http_urls() -> None:
    projected = _ensure_clinical_trials_fields(
        "https://clinicaltrials.gov/api/v2/studies?format=json"
    )

    assert projected.startswith("https://clinicaltrials.gov/api/v2/studies?")
    assert "fields=" in projected
    for field_name in DEFAULT_CLINICAL_TRIALS_FIELDS:
        assert field_name in projected


def test_ensure_clinical_trials_fields_leaves_non_http_urls_unchanged() -> None:
    file_url = "file:///tmp/clinical_trials.source.json"
    assert _ensure_clinical_trials_fields(file_url) == file_url
