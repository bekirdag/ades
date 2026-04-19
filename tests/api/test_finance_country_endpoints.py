import ades.packs.finance_country as finance_country_module
from fastapi.testclient import TestClient

from ades.service.app import create_app
from tests.finance_bundle_helpers import create_finance_country_profiles


def test_finance_country_endpoints_fetch_and_build(tmp_path, monkeypatch) -> None:
    profiles = create_finance_country_profiles(tmp_path / "remote-country-sources")
    monkeypatch.setattr(finance_country_module, "FINANCE_COUNTRY_PROFILES", profiles)

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    fetch_response = client.post(
        "/v0/registry/fetch-finance-country-sources",
        json={
            "output_dir": str(tmp_path / "raw" / "finance-country-en"),
            "snapshot": "2026-04-19",
            "country_codes": ["us", "uk"],
        },
    )

    assert fetch_response.status_code == 200
    fetch_payload = fetch_response.json()
    assert fetch_payload["country_count"] == 2
    assert {item["pack_id"] for item in fetch_payload["countries"]} == {
        "finance-us-en",
        "finance-uk-en",
    }

    build_response = client.post(
        "/v0/registry/build-finance-country-bundles",
        json={
            "snapshot_dir": fetch_payload["snapshot_dir"],
            "output_dir": str(tmp_path / "bundles" / "finance-country-en"),
            "country_codes": ["us"],
            "version": "0.2.0",
        },
    )

    assert build_response.status_code == 200
    build_payload = build_response.json()
    assert build_payload["country_count"] == 1
    assert build_payload["bundles"][0]["pack_id"] == "finance-us-en"
    assert build_payload["bundles"][0]["organization_count"] >= 2
