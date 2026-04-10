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
            "symbol_directory_path": str(snapshots["symbol_directory"]),
            "curated_entities_path": str(snapshots["curated_entities"]),
            "output_dir": str(output_dir),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "finance-en"
    assert payload["entity_record_count"] == 6
    assert Path(payload["sources_lock_path"]).exists()

    generated = generate_pack_source(
        payload["bundle_dir"],
        output_dir=tmp_path / "generated-packs",
    )
    assert Path(generated.pack_dir).exists()
