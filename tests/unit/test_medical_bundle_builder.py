import json
from hashlib import sha256
from pathlib import Path

from ades.packs.medical_bundle import build_medical_source_bundle
from tests.medical_bundle_helpers import create_medical_raw_snapshots


def test_build_medical_source_bundle_writes_normalized_bundle(tmp_path: Path) -> None:
    snapshots = create_medical_raw_snapshots(tmp_path)

    result = build_medical_source_bundle(
        disease_ontology_path=snapshots["disease_ontology"],
        hgnc_genes_path=snapshots["hgnc_genes"],
        uniprot_proteins_path=snapshots["uniprot_proteins"],
        clinical_trials_path=snapshots["clinical_trials"],
        orange_book_products_path=snapshots["orange_book_products"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    assert result.pack_id == "medical-en"
    assert result.version == "0.2.0"
    assert Path(result.sources_lock_path).exists()
    assert result.source_count == 6
    assert result.entity_record_count == 7
    assert result.rule_record_count == 2
    assert result.disease_count == 2
    assert result.gene_count == 1
    assert result.protein_count == 1
    assert result.clinical_trial_count == 1
    assert result.drug_count == 1
    assert result.curated_entity_count == 1
    assert result.warnings == []

    bundle_manifest = json.loads(Path(result.bundle_manifest_path).read_text(encoding="utf-8"))
    assert bundle_manifest["pack_id"] == "medical-en"
    assert bundle_manifest["dependencies"] == ["general-en"]
    assert bundle_manifest["entities_path"] == "normalized/entities.jsonl"
    assert len(bundle_manifest["sources"]) == 6
    sources_lock = json.loads(Path(result.sources_lock_path).read_text(encoding="utf-8"))
    assert sources_lock["pack_id"] == "medical-en"
    assert sources_lock["version"] == "0.2.0"
    assert sources_lock["bundle_manifest_path"] == "bundle.json"
    assert sources_lock["entities_path"] == "normalized/entities.jsonl"
    assert sources_lock["rules_path"] == "normalized/rules.jsonl"
    assert sources_lock["source_count"] == 6
    lock_sources = {item["name"]: item for item in sources_lock["sources"]}
    assert lock_sources["disease-ontology"]["snapshot_sha256"] == _sha256(
        snapshots["disease_ontology"]
    )
    assert lock_sources["hgnc-genes"]["snapshot_sha256"] == _sha256(
        snapshots["hgnc_genes"]
    )
    assert lock_sources["uniprot-proteins"]["snapshot_sha256"] == _sha256(
        snapshots["uniprot_proteins"]
    )
    assert lock_sources["clinical-trials"]["snapshot_sha256"] == _sha256(
        snapshots["clinical_trials"]
    )
    assert lock_sources["orange-book"]["snapshot_sha256"] == _sha256(
        snapshots["orange_book_products"]
    )
    assert lock_sources["curated-medical-entities"]["snapshot_sha256"] == _sha256(
        snapshots["curated_entities"]
    )
    assert lock_sources["disease-ontology"]["adapter"] == "disease_ontology_terms"
    assert lock_sources["hgnc-genes"]["adapter"] == "hgnc_complete_set"
    assert lock_sources["uniprot-proteins"]["adapter"] == "uniprot_reviewed_proteins"
    assert lock_sources["clinical-trials"]["adapter"] == "clinical_trials_json"
    assert lock_sources["orange-book"]["adapter"] == "orange_book_products"
    assert lock_sources["curated-medical-entities"]["adapter"] == "curated_medical_entities"
    assert all(item["adapter_version"] == "1" for item in lock_sources.values())

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(
        item["entity_type"] == "drug" and item["canonical_text"] == "Compound Alpha"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "disease" and item["canonical_text"] == "disease alpha"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "gene" and item["canonical_text"] == "GENEA1"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "protein" and item["canonical_text"] == "Protein Alpha"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "clinical_trial"
        and item["canonical_text"] == "Study Alpha Compound Trial"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "drug" and item["canonical_text"] == "Brand Alpha"
        for item in entity_records
    )

    rule_records = [
        json.loads(line)
        for line in Path(result.rules_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {
        "name": "clinical_trial_id",
        "label": "clinical_trial",
        "kind": "regex",
        "pattern": r"NCT[0-9]{8}",
    } in rule_records
    assert {
        "name": "dosage",
        "label": "dosage",
        "kind": "regex",
        "pattern": r"[0-9]+\s?(mg|ml|mcg)",
    } in rule_records


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def test_build_medical_source_bundle_drops_unsafe_lowercase_gene_aliases(
    tmp_path: Path,
) -> None:
    snapshots = create_medical_raw_snapshots(tmp_path / "snapshots")
    snapshots["hgnc_genes"].write_text(
        json.dumps(
            {
                "genes": [
                    {
                        "hgnc_id": "HGNC:1100",
                        "symbol": "GENEA1",
                        "aliases": ["GENEA1ALT", "for", "in", "p53"],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_medical_source_bundle(
        disease_ontology_path=snapshots["disease_ontology"],
        hgnc_genes_path=snapshots["hgnc_genes"],
        uniprot_proteins_path=snapshots["uniprot_proteins"],
        clinical_trials_path=snapshots["clinical_trials"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    gene_alpha_record = next(
        item for item in entity_records if item["entity_type"] == "gene" and item["canonical_text"] == "GENEA1"
    )
    assert gene_alpha_record["aliases"] == ["GENEA1ALT"]


def test_build_medical_source_bundle_drops_symbolic_disease_aliases(
    tmp_path: Path,
) -> None:
    snapshots = create_medical_raw_snapshots(tmp_path / "snapshots")
    snapshots["disease_ontology"].write_text(
        json.dumps(
            {
                "terms": [
                    {
                        "id": "DOID:8469",
                        "name": "disease beta",
                        "synonyms": ["disease beta alias", "ALS1"],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    snapshots["hgnc_genes"].write_text(
        json.dumps(
            {
                "genes": [
                    {
                        "hgnc_id": "HGNC:1100",
                        "symbol": "ALS1",
                        "aliases": [],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_medical_source_bundle(
        disease_ontology_path=snapshots["disease_ontology"],
        hgnc_genes_path=snapshots["hgnc_genes"],
        uniprot_proteins_path=snapshots["uniprot_proteins"],
        clinical_trials_path=snapshots["clinical_trials"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    disease_beta_record = next(
        item
        for item in entity_records
        if item["entity_type"] == "disease" and item["canonical_text"] == "disease beta"
    )
    assert disease_beta_record["aliases"] == ["disease beta alias"]


def test_build_medical_source_bundle_drops_symbolic_protein_aliases(
    tmp_path: Path,
) -> None:
    snapshots = create_medical_raw_snapshots(tmp_path / "snapshots")
    snapshots["uniprot_proteins"].write_text(
        json.dumps(
            {
                "proteins": [
                    {
                        "accession": "P04637",
                        "recommended_name": "Protein Alpha",
                        "aliases": ["Protein Alpha Long Form", "GENEA1", "P150"],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_medical_source_bundle(
        disease_ontology_path=snapshots["disease_ontology"],
        hgnc_genes_path=snapshots["hgnc_genes"],
        uniprot_proteins_path=snapshots["uniprot_proteins"],
        clinical_trials_path=snapshots["clinical_trials"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    p53_protein = next(
        item
        for item in entity_records
        if item["entity_type"] == "protein" and item["canonical_text"] == "Protein Alpha"
    )
    assert p53_protein["aliases"] == ["Protein Alpha Long Form"]
