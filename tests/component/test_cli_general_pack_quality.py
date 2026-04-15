import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.general_bundle_helpers import create_general_raw_snapshots


def test_cli_can_validate_generated_general_pack_quality(tmp_path) -> None:
    snapshots = create_general_raw_snapshots(tmp_path / "snapshots")
    bundles_root = tmp_path / "bundles"
    quality_root = tmp_path / "quality-output"
    runner = CliRunner()

    bundle_result = runner.invoke(
        app,
        [
            "registry",
            "build-general-bundle",
            "--wikidata-entities",
            str(snapshots["wikidata_entities"]),
            "--geonames-places",
            str(snapshots["geonames_places"]),
            "--curated-entities",
            str(snapshots["curated_entities"]),
            "--output-dir",
            str(bundles_root),
        ],
    )

    assert bundle_result.exit_code == 0
    bundle_payload = json.loads(bundle_result.stdout)

    quality_result = runner.invoke(
        app,
        [
            "registry",
            "validate-general-quality",
            bundle_payload["bundle_dir"],
            "--output-dir",
            str(quality_root),
        ],
    )

    assert quality_result.exit_code == 0
    quality_payload = json.loads(quality_result.stdout)
    assert quality_payload["pack_id"] == "general-en"
    assert quality_payload["fixture_profile"] == "benchmark"
    assert quality_payload["passed"] is True
    assert quality_payload["expected_recall"] == 1.0
    assert quality_payload["expected_entity_count"] == 34
    assert quality_payload["alias_count"] == 47


def test_cli_uses_general_pack_default_ambiguity_budget(tmp_path) -> None:
    snapshots = create_general_raw_snapshots(tmp_path / "snapshots")
    bundles_root = tmp_path / "bundles"
    quality_root = tmp_path / "quality-output"
    runner = CliRunner()

    bundle_result = runner.invoke(
        app,
        [
            "registry",
            "build-general-bundle",
            "--wikidata-entities",
            str(snapshots["wikidata_entities"]),
            "--geonames-places",
            str(snapshots["geonames_places"]),
            "--curated-entities",
            str(snapshots["curated_entities"]),
            "--output-dir",
            str(bundles_root),
        ],
    )

    assert bundle_result.exit_code == 0
    bundle_payload = json.loads(bundle_result.stdout)

    entities_path = Path(bundle_payload["entities_path"])
    entity_rows = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    entity_rows.append(
        {
            "entity_id": "curated:test-byzantium-labs",
            "entity_type": "organization",
            "canonical_text": "Byzantium Labs",
            "aliases": ["Byzantium"],
            "source_name": "curated-general",
        }
    )
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in entity_rows),
        encoding="utf-8",
    )

    quality_result = runner.invoke(
        app,
        [
            "registry",
            "validate-general-quality",
            bundle_payload["bundle_dir"],
            "--output-dir",
            str(quality_root),
        ],
    )

    assert quality_result.exit_code == 0
    quality_payload = json.loads(quality_result.stdout)
    assert quality_payload["passed"] is True
    assert quality_payload["ambiguous_alias_count"] > 0
