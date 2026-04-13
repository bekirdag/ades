import json

from typer.testing import CliRunner

from ades.cli import app
from tests.general_bundle_helpers import create_general_raw_snapshots
from tests.medical_bundle_helpers import create_medical_raw_snapshots


def test_cli_can_validate_generated_medical_pack_quality(tmp_path) -> None:
    general_snapshots = create_general_raw_snapshots(tmp_path / "general-snapshots")
    medical_snapshots = create_medical_raw_snapshots(tmp_path / "medical-snapshots")
    runner = CliRunner()

    general_bundle_result = runner.invoke(
        app,
        [
            "registry",
            "build-general-bundle",
            "--wikidata-entities",
            str(general_snapshots["wikidata_entities"]),
            "--geonames-places",
            str(general_snapshots["geonames_places"]),
            "--curated-entities",
            str(general_snapshots["curated_entities"]),
            "--output-dir",
            str(tmp_path / "general-bundles"),
        ],
    )
    medical_bundle_result = runner.invoke(
        app,
        [
            "registry",
            "build-medical-bundle",
            "--disease-ontology",
            str(medical_snapshots["disease_ontology"]),
            "--hgnc-genes",
            str(medical_snapshots["hgnc_genes"]),
            "--uniprot-proteins",
            str(medical_snapshots["uniprot_proteins"]),
            "--clinical-trials",
            str(medical_snapshots["clinical_trials"]),
            "--curated-entities",
            str(medical_snapshots["curated_entities"]),
            "--output-dir",
            str(tmp_path / "medical-bundles"),
        ],
    )

    assert general_bundle_result.exit_code == 0
    assert medical_bundle_result.exit_code == 0
    general_bundle_payload = json.loads(general_bundle_result.stdout)
    medical_bundle_payload = json.loads(medical_bundle_result.stdout)

    quality_result = runner.invoke(
        app,
        [
            "registry",
            "validate-medical-quality",
            medical_bundle_payload["bundle_dir"],
            "--general-bundle-dir",
            general_bundle_payload["bundle_dir"],
            "--output-dir",
            str(tmp_path / "quality-output"),
        ],
    )

    assert quality_result.exit_code == 0
    quality_payload = json.loads(quality_result.stdout)
    assert quality_payload["pack_id"] == "medical-en"
    assert quality_payload["fixture_profile"] == "benchmark"
    assert quality_payload["passed"] is True
    assert quality_payload["expected_recall"] == 1.0
    assert quality_payload["expected_entity_count"] == 21
