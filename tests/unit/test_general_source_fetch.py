import json
from pathlib import Path

from ades.packs.general_bundle import build_general_source_bundle
from ades.packs.general_sources import fetch_general_source_snapshot
from tests.general_bundle_helpers import create_general_remote_sources


def test_fetch_general_source_snapshot_writes_immutable_snapshot_dir(tmp_path: Path) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")

    result = fetch_general_source_snapshot(
        output_dir=tmp_path / "raw" / "general-en",
        snapshot="2026-04-10",
        wikidata_url=remote_sources["wikidata_url"],
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    assert result.pack_id == "general-en"
    assert result.snapshot == "2026-04-10"
    assert result.source_count == 3
    assert result.wikidata_entity_count == 6
    assert result.geonames_location_count == 2
    assert result.curated_entity_count >= 2
    assert Path(result.snapshot_dir).exists()
    assert Path(result.wikidata_entities_path).exists()
    assert Path(result.geonames_places_path).exists()
    assert Path(result.curated_entities_path).exists()
    assert Path(result.source_manifest_path).exists()
    assert result.warnings == []

    manifest = json.loads(Path(result.source_manifest_path).read_text(encoding="utf-8"))
    assert manifest["pack_id"] == "general-en"
    assert manifest["snapshot"] == "2026-04-10"
    sources = {item["name"]: item for item in manifest["sources"]}
    assert sources["wikidata-general-entities"]["source_url"] == remote_sources["wikidata_url"]
    assert sources["geonames-places"]["source_url"] == remote_sources["geonames_places_url"]
    assert sources["curated-general-entities"]["source_url"].startswith("operator://")

    bundle = build_general_source_bundle(
        wikidata_entities_path=result.wikidata_entities_path,
        geonames_places_path=result.geonames_places_path,
        curated_entities_path=result.curated_entities_path,
        output_dir=tmp_path / "bundles",
    )
    assert bundle.wikidata_entity_count == 6
    assert bundle.geonames_location_count == 2
    assert bundle.curated_entity_count >= 2


def test_fetch_general_source_snapshot_rejects_existing_snapshot_dir(
    tmp_path: Path,
) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")
    output_root = tmp_path / "raw" / "general-en"

    fetch_general_source_snapshot(
        output_dir=output_root,
        snapshot="2026-04-10",
        wikidata_url=remote_sources["wikidata_url"],
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    try:
        fetch_general_source_snapshot(
            output_dir=output_root,
            snapshot="2026-04-10",
            wikidata_url=remote_sources["wikidata_url"],
            geonames_places_url=remote_sources["geonames_places_url"],
        )
    except FileExistsError as exc:
        assert "already exists" in str(exc)
    else:
        raise AssertionError("Expected FileExistsError for an immutable snapshot dir.")
