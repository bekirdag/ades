import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.general_bundle_helpers import create_general_raw_snapshots
from tests.medical_bundle_helpers import create_medical_raw_snapshots


def test_cli_can_build_medical_bundle_then_generate_pack(tmp_path: Path) -> None:
    general_snapshots = create_general_raw_snapshots(tmp_path / "general-snapshots")
    medical_snapshots = create_medical_raw_snapshots(tmp_path / "medical-snapshots")
    bundles_root = tmp_path / "bundles"
    generate_root = tmp_path / "generated-packs"
    registry_root = tmp_path / "registry"

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
            str(bundles_root / "general"),
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
            str(bundles_root / "medical"),
        ],
    )

    assert general_bundle_result.exit_code == 0
    assert medical_bundle_result.exit_code == 0
    general_bundle_payload = json.loads(general_bundle_result.stdout)
    medical_bundle_payload = json.loads(medical_bundle_result.stdout)
    assert general_bundle_payload["pack_id"] == "general-en"
    assert medical_bundle_payload["pack_id"] == "medical-en"
    assert Path(general_bundle_payload["sources_lock_path"]).exists()
    assert Path(medical_bundle_payload["sources_lock_path"]).exists()
    assert Path(medical_bundle_payload["bundle_dir"]).exists()

    general_generate_result = runner.invoke(
        app,
        [
            "registry",
            "generate-pack",
            general_bundle_payload["bundle_dir"],
            "--output-dir",
            str(generate_root),
        ],
    )
    medical_generate_result = runner.invoke(
        app,
        [
            "registry",
            "generate-pack",
            medical_bundle_payload["bundle_dir"],
            "--output-dir",
            str(generate_root),
        ],
    )
    assert general_generate_result.exit_code == 0
    assert medical_generate_result.exit_code == 0
    general_generate_payload = json.loads(general_generate_result.stdout)
    medical_generate_payload = json.loads(medical_generate_result.stdout)
    assert general_generate_payload["pack_id"] == "general-en"
    assert medical_generate_payload["pack_id"] == "medical-en"

    registry_result = runner.invoke(
        app,
        [
            "registry",
            "build",
            general_generate_payload["pack_dir"],
            medical_generate_payload["pack_dir"],
            "--output-dir",
            str(registry_root),
        ],
    )
    assert registry_result.exit_code == 0
