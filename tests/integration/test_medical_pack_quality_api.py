from pathlib import Path

from ades import (
    build_general_source_bundle,
    build_medical_source_bundle,
    validate_medical_pack_quality,
)
from tests.general_bundle_helpers import create_general_raw_snapshots
from tests.medical_bundle_helpers import create_medical_raw_snapshots


def test_public_api_can_validate_generated_medical_pack_quality(
    tmp_path: Path,
) -> None:
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

    report = validate_medical_pack_quality(
        medical_bundle.bundle_dir,
        general_bundle_dir=general_bundle.bundle_dir,
        output_dir=tmp_path / "quality-output",
    )

    assert report.passed is True
    assert report.pack_id == "medical-en"
    assert report.expected_recall == 1.0
