import json
from pathlib import Path

from typer.testing import CliRunner

from ades import build_general_source_bundle, build_medical_source_bundle
from ades.cli import app
from tests.general_bundle_helpers import create_general_raw_snapshots
from tests.medical_bundle_helpers import create_medical_raw_snapshots


runner = CliRunner()


def test_cli_can_refresh_generated_pack_registry_release(tmp_path: Path) -> None:
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

    result = runner.invoke(
        app,
        [
            "registry",
            "refresh-generated-packs",
            general_bundle.bundle_dir,
            medical_bundle.bundle_dir,
            "--output-dir",
            str(tmp_path / "refresh-output"),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["passed"] is True
    assert payload["pack_count"] == 2
    assert payload["registry"]["pack_count"] == 2
    assert [item["pack_id"] for item in payload["packs"]] == ["general-en", "medical-en"]
    assert payload["packs"][0]["report"]["publishable_sources_only"] is True


def test_cli_reports_source_governance_failure_during_generated_pack_refresh(
    tmp_path: Path,
) -> None:
    general_snapshots = create_general_raw_snapshots(tmp_path / "general-snapshots")
    general_bundle = build_general_source_bundle(
        wikidata_entities_path=general_snapshots["wikidata_entities"],
        geonames_places_path=general_snapshots["geonames_places"],
        curated_entities_path=general_snapshots["curated_entities"],
        output_dir=tmp_path / "general-bundles",
    )

    bundle_manifest_path = Path(general_bundle.bundle_dir) / "bundle.json"
    bundle_manifest = json.loads(bundle_manifest_path.read_text(encoding="utf-8"))
    bundle_manifest["sources"][0]["license_class"] = "build-only"
    bundle_manifest_path.write_text(
        json.dumps(bundle_manifest, indent=2) + "\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "refresh-generated-packs",
            general_bundle.bundle_dir,
            "--output-dir",
            str(tmp_path / "refresh-output"),
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["passed"] is False
    assert payload["registry"] is None
    assert "registry_build_skipped:source_governance_failed" in payload["warnings"]
    assert payload["packs"][0]["report"]["publishable_sources_only"] is False
