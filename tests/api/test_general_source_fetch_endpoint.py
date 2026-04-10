from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app
from tests.general_bundle_helpers import create_general_remote_sources


def test_general_source_fetch_endpoint_downloads_snapshot(tmp_path: Path) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/fetch-general-sources",
        json={
            "output_dir": str(tmp_path / "raw" / "general-en"),
            "snapshot": "2026-04-10",
            "wikidata_url": remote_sources["wikidata_url"],
            "geonames_places_url": remote_sources["geonames_places_url"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "general-en"
    assert payload["snapshot"] == "2026-04-10"
    assert payload["wikidata_entity_count"] == 10
    assert payload["geonames_location_count"] == 2
    assert Path(payload["curated_entities_path"]).exists()
