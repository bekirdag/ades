from pathlib import Path
import json

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
            "sec_submissions_url": remote_sources["sec_submissions_url"],
            "sec_companyfacts_url": remote_sources["sec_companyfacts_url"],
            "symbol_directory_url": remote_sources["symbol_directory_url"],
            "other_listed_url": remote_sources["other_listed_url"],
            "finance_people_url": remote_sources["finance_people_url"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "finance-en"
    assert payload["snapshot"] == "2026-04-10"
    assert Path(payload["curated_entities_path"]).exists()
    assert Path(payload["finance_people_path"]).exists()
    assert Path(payload["sec_submissions_path"]).exists()


def test_finance_source_fetch_endpoint_can_derive_sec_proxy_people(
    tmp_path: Path,
) -> None:
    remote_sources = create_finance_remote_sources(tmp_path / "remote")

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/fetch-finance-sources",
        json={
            "output_dir": str(tmp_path / "raw" / "finance-en"),
            "snapshot": "2026-04-11",
            "sec_companies_url": remote_sources["sec_companies_url"],
            "sec_submissions_url": remote_sources["sec_submissions_url"],
            "sec_companyfacts_url": remote_sources["sec_companyfacts_url"],
            "symbol_directory_url": remote_sources["symbol_directory_url"],
            "other_listed_url": remote_sources["other_listed_url"],
            "derive_finance_people_from_sec": True,
            "finance_people_archive_base_url": remote_sources[
                "finance_people_archive_base_url"
            ],
            "finance_people_max_companies": 2,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    people_payload = json.loads(
        Path(payload["finance_people_path"]).read_text(encoding="utf-8")
    )
    extracted_names = {
        item["canonical_text"] for item in people_payload.get("entities", [])
    }
    assert "Jane Doe" in extracted_names
    assert "John Roe" in extracted_names
