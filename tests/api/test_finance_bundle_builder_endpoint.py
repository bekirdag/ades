from pathlib import Path

from fastapi.testclient import TestClient

from ades import generate_pack_source
from ades.service.app import create_app
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_finance_bundle_builder_endpoint_creates_bundle_directory(tmp_path: Path) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    output_dir = tmp_path / "bundles"

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/build-finance-bundle",
        json={
            "sec_companies_path": str(snapshots["sec_companies"]),
            "sec_submissions_path": str(snapshots["sec_submissions"]),
            "sec_companyfacts_path": str(snapshots["sec_companyfacts"]),
            "symbol_directory_path": str(snapshots["symbol_directory"]),
            "other_listed_path": str(snapshots["other_listed"]),
            "finance_people_path": str(snapshots["finance_people"]),
            "curated_entities_path": str(snapshots["curated_entities"]),
            "output_dir": str(output_dir),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "finance-en"
    assert payload["entity_record_count"] == 17
    assert payload["symbol_count"] == 3
    assert payload["person_count"] == 2
    assert payload["company_equity_ticker_count"] == 3
    assert payload["company_name_enriched_issuer_count"] == 2
    assert Path(payload["sources_lock_path"]).exists()

    generated = generate_pack_source(
        payload["bundle_dir"],
        output_dir=tmp_path / "generated-packs",
    )
    assert Path(generated.pack_dir).exists()
