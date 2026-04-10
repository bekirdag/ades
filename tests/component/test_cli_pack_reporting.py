import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.medical_bundle_helpers import create_medical_raw_snapshots


def test_cli_can_report_generated_medical_pack_statistics(tmp_path: Path) -> None:
    snapshots = create_medical_raw_snapshots(tmp_path / "snapshots")
    bundles_root = tmp_path / "bundles"
    generated_root = tmp_path / "generated-packs"
    runner = CliRunner()

    bundle_result = runner.invoke(
        app,
        [
            "registry",
            "build-medical-bundle",
            "--disease-ontology",
            str(snapshots["disease_ontology"]),
            "--hgnc-genes",
            str(snapshots["hgnc_genes"]),
            "--uniprot-proteins",
            str(snapshots["uniprot_proteins"]),
            "--clinical-trials",
            str(snapshots["clinical_trials"]),
            "--curated-entities",
            str(snapshots["curated_entities"]),
            "--output-dir",
            str(bundles_root),
        ],
    )

    assert bundle_result.exit_code == 0
    bundle_payload = json.loads(bundle_result.stdout)

    report_result = runner.invoke(
        app,
        [
            "registry",
            "report-pack",
            bundle_payload["bundle_dir"],
            "--output-dir",
            str(generated_root),
        ],
    )

    assert report_result.exit_code == 0
    report_payload = json.loads(report_result.stdout)
    assert report_payload["pack_id"] == "medical-en"
    assert report_payload["alias_count"] == 12
    assert report_payload["unique_canonical_count"] == 6
    assert report_payload["label_distribution"]["dosage"] == 1
    assert Path(report_payload["pack_dir"]).exists()
