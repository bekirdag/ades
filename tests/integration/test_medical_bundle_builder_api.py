from pathlib import Path

from ades import (
    build_general_source_bundle,
    build_medical_source_bundle,
    build_registry,
    generate_pack_source,
    pull_pack,
    tag,
)
from tests.general_bundle_helpers import create_general_raw_snapshots
from tests.medical_bundle_helpers import create_medical_raw_snapshots


def test_public_api_can_build_medical_bundle_then_generate_install_and_tag(
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
    assert Path(general_bundle.sources_lock_path).exists()
    assert Path(medical_bundle.sources_lock_path).exists()
    general_generated = generate_pack_source(
        general_bundle.bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    medical_generated = generate_pack_source(
        medical_bundle.bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    registry = build_registry(
        [general_generated.pack_dir, medical_generated.pack_dir],
        output_dir=tmp_path / "registry",
    )

    install_root = tmp_path / "install"
    pull_pack(
        "medical-en",
        storage_root=install_root,
        registry_url=registry.index_url,
    )
    response = tag(
        "BRCA1 and p53 protein were studied in NCT04280705 after Aspirin 10 mg dosing for diabetes.",
        pack="medical-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert ("BRCA1", "gene") in entities
    assert ("p53 protein", "protein") in entities
    assert ("NCT04280705", "clinical_trial") in entities
    assert ("Aspirin", "drug") in entities
    assert ("10 mg", "dosage") in entities
    assert ("diabetes", "disease") in entities
