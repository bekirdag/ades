import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.medical_bundle_helpers import create_medical_remote_sources


def test_cli_can_fetch_medical_source_snapshot(tmp_path: Path) -> None:
    remote_sources = create_medical_remote_sources(tmp_path / "remote")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "registry",
            "fetch-medical-sources",
            "--output-dir",
            str(tmp_path / "raw" / "medical-en"),
            "--snapshot",
            "2026-04-10",
            "--disease-ontology-url",
            remote_sources["disease_ontology_url"],
            "--hgnc-genes-url",
            remote_sources["hgnc_genes_url"],
            "--uniprot-proteins-url",
            remote_sources["uniprot_proteins_url"],
            "--clinical-trials-url",
            remote_sources["clinical_trials_url"],
            "--uniprot-max-records",
            "10",
            "--clinical-trials-max-records",
            "10",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["pack_id"] == "medical-en"
    assert payload["snapshot"] == "2026-04-10"
    assert Path(payload["source_manifest_path"]).exists()
    assert Path(payload["disease_ontology_path"]).exists()

