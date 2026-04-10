import json
from pathlib import Path

from ades.packs.general_bundle import build_general_source_bundle
from ades.packs.medical_bundle import build_medical_source_bundle
from ades.packs.refresh import refresh_generated_pack_registry
from tests.general_bundle_helpers import create_general_raw_snapshots
from tests.medical_bundle_helpers import create_medical_raw_snapshots


def test_refresh_generated_pack_registry_builds_multi_pack_release(
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

    response = refresh_generated_pack_registry(
        [general_bundle.bundle_dir, medical_bundle.bundle_dir],
        output_dir=tmp_path / "refresh-output",
    )

    assert response.passed is True
    assert response.pack_count == 2
    assert response.registry is not None
    assert response.registry.packs[0].pack_id == "general-en"
    assert response.registry.packs[1].pack_id == "medical-en"
    assert response.packs[0].pack_id == "general-en"
    assert response.packs[1].pack_id == "medical-en"
    assert response.packs[1].quality.passed is True
    index_payload = json.loads(Path(response.registry.index_path).read_text(encoding="utf-8"))
    assert sorted(index_payload["packs"]) == ["general-en", "medical-en"]


def test_refresh_generated_pack_registry_skips_registry_on_quality_failure(
    tmp_path: Path,
) -> None:
    general_snapshots = create_general_raw_snapshots(tmp_path / "general-snapshots")
    general_bundle = build_general_source_bundle(
        wikidata_entities_path=general_snapshots["wikidata_entities"],
        geonames_places_path=general_snapshots["geonames_places"],
        curated_entities_path=general_snapshots["curated_entities"],
        output_dir=tmp_path / "general-bundles",
    )

    entities_path = Path(general_bundle.entities_path)
    records = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    filtered_records = [
        record for record in records if record.get("canonical_text") != "OpenAI"
    ]
    entities_path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in filtered_records),
        encoding="utf-8",
    )

    response = refresh_generated_pack_registry(
        [general_bundle.bundle_dir],
        output_dir=tmp_path / "refresh-output",
    )

    assert response.passed is False
    assert response.registry is None
    assert "registry_build_skipped:quality_failed" in response.warnings
    assert response.packs[0].pack_id == "general-en"
    assert response.packs[0].quality.passed is False
    assert response.packs[0].quality.failures
