from pathlib import Path

from fastapi.testclient import TestClient

from ades import build_finance_source_bundle
from ades.service.app import create_app
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_refresh_generated_packs_endpoint_builds_registry_release(tmp_path: Path) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        symbol_directory_path=snapshots["symbol_directory"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )
    client = TestClient(create_app())

    response = client.post(
        "/v0/registry/refresh-generated-packs",
        json={
            "bundle_dirs": [bundle.bundle_dir],
            "output_dir": str(tmp_path / "refresh-output"),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["passed"] is True
    assert payload["pack_count"] == 1
    assert payload["registry"]["pack_count"] == 1
    assert payload["packs"][0]["pack_id"] == "finance-en"
