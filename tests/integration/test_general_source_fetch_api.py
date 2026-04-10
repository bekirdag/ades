from pathlib import Path

from ades import fetch_general_source_snapshot
from tests.general_bundle_helpers import create_general_remote_sources


def test_public_api_can_fetch_general_source_snapshot(tmp_path: Path) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")

    result = fetch_general_source_snapshot(
        output_dir=tmp_path / "raw" / "general-en",
        snapshot="2026-04-10",
        wikidata_url=remote_sources["wikidata_url"],
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    assert result.pack_id == "general-en"
    assert result.snapshot == "2026-04-10"
    assert result.wikidata_entity_count == 6
    assert result.geonames_location_count == 2
    assert Path(result.source_manifest_path).exists()
