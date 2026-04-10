from pathlib import Path

from fastapi.testclient import TestClient

from ades import build_finance_source_bundle
from ades.service.app import create_app
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_finance_pack_quality_endpoint_validates_generated_bundle(tmp_path: Path) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        symbol_directory_path=snapshots["symbol_directory"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/validate-finance-quality",
        json={
            "bundle_dir": bundle.bundle_dir,
            "output_dir": str(tmp_path / "quality-output"),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["passed"] is True
    assert payload["fixture_count"] == 3
    assert payload["passed_case_count"] == 3
