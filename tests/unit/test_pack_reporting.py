from pathlib import Path

from ades.packs.medical_bundle import build_medical_source_bundle
from ades.packs.reporting import report_generated_pack
from tests.medical_bundle_helpers import create_medical_raw_snapshots


def test_report_generated_pack_returns_stable_medical_bundle_statistics(tmp_path: Path) -> None:
    snapshots = create_medical_raw_snapshots(tmp_path / "snapshots")
    bundle = build_medical_source_bundle(
        disease_ontology_path=snapshots["disease_ontology"],
        hgnc_genes_path=snapshots["hgnc_genes"],
        uniprot_proteins_path=snapshots["uniprot_proteins"],
        clinical_trials_path=snapshots["clinical_trials"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    report = report_generated_pack(
        bundle.bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    assert report.pack_id == "medical-en"
    assert report.version == "0.2.0"
    assert report.source_count == 5
    assert report.publishable_source_count == 5
    assert report.restricted_source_count == 0
    assert report.publishable_sources_only is True
    assert report.source_license_classes == {"ship-now": 5}
    assert report.label_count == 6
    assert report.alias_count == 12
    assert report.unique_canonical_count == 6
    assert report.rule_count == 2
    assert report.included_entity_count == 6
    assert report.included_rule_count == 2
    assert report.dropped_record_count == 0
    assert report.dropped_alias_count == 0
    assert report.ambiguous_alias_count == 0
    assert report.dropped_alias_ratio == 0.0
    assert report.label_distribution == {
        "clinical_trial": 3,
        "disease": 4,
        "dosage": 1,
        "drug": 2,
        "gene": 2,
        "protein": 2,
    }
    assert report.warnings == []
    assert Path(report.pack_dir).exists()
    assert Path(report.manifest_path).exists()
