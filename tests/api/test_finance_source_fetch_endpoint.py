from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app
from tests.finance_bundle_helpers import create_finance_remote_sources


def test_finance_source_fetch_endpoint_downloads_snapshot(tmp_path: Path) -> None:
    remote_sources = create_finance_remote_sources(tmp_path / "remote")

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/fetch-finance-sources",
        json={
            "output_dir": str(tmp_path / "raw" / "finance-en"),
            "snapshot": "2026-04-10",
            "sec_companies_url": remote_sources["sec_companies_url"],
            "symbol_directory_url": remote_sources["symbol_directory_url"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "finance-en"
    assert payload["snapshot"] == "2026-04-10"
    assert Path(payload["curated_entities_path"]).exists()
