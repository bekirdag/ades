from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_service_endpoints_reflect_installed_packs(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    with TestClient(create_app(storage_root=tmp_path)) as client:
        healthz = client.get("/healthz")
        assert healthz.status_code == 200
        assert healthz.json()["status"] == "ok"

        status_response = client.get("/v0/status")
        assert status_response.status_code == 200
        assert status_response.json()["storage_root"] == str(tmp_path)
        assert "finance-en" in status_response.json()["installed_packs"]
        assert set(status_response.json()["prewarmed_packs"]) >= {"finance-en", "general-en"}

        packs_response = client.get("/v0/packs")
        assert packs_response.status_code == 200
        pack_ids = {pack["pack_id"] for pack in packs_response.json()}
        assert "finance-en" in pack_ids
        assert "general-en" in pack_ids

        available_response = client.get("/v0/packs/available")
        assert available_response.status_code == 200
        available_pack_ids = {pack["pack_id"] for pack in available_response.json()}
        assert "finance-en" in available_pack_ids
        finance_available = next(
            pack for pack in available_response.json() if pack["pack_id"] == "finance-en"
        )
        assert "finance" in finance_available["tags"]

        pack_response = client.get("/v0/packs/finance-en")
        assert pack_response.status_code == 200
        assert pack_response.json()["pack_id"] == "finance-en"
        assert pack_response.json()["active"] is True


def test_service_tag_endpoint_uses_requested_pack(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "TICKA rallied on EXCHX after USD 12.5 guidance from Org Beta.",
            "pack": "finance-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    labels = {entity["label"] for entity in payload["entities"]}

    assert "ticker" in labels
    assert "exchange" in labels
    assert "currency_amount" in labels


def test_service_returns_404_for_missing_pack(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.get("/v0/packs/missing-pack")
    assert response.status_code == 404


def test_service_activation_and_lookup_endpoints(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    with TestClient(create_app(storage_root=tmp_path)) as client:
        deactivate_response = client.post("/v0/packs/finance-en/deactivate")
        assert deactivate_response.status_code == 200
        assert deactivate_response.json()["active"] is False
        deactivated_status = client.get("/v0/status")
        assert "finance-en" not in deactivated_status.json()["prewarmed_packs"]

        active_packs = client.get("/v0/packs", params={"active_only": True})
        assert active_packs.status_code == 200
        assert {pack["pack_id"] for pack in active_packs.json()} == {"general-en"}

        inactive_lookup = client.get("/v0/lookup", params={"q": "TICKA", "exact_alias": True})
        assert inactive_lookup.status_code == 200
        assert inactive_lookup.json()["candidates"] == []

        activate_response = client.post("/v0/packs/finance-en/activate")
        assert activate_response.status_code == 200
        assert activate_response.json()["active"] is True
        activated_status = client.get("/v0/status")
        assert "finance-en" in activated_status.json()["prewarmed_packs"]

        lookup = client.get("/v0/lookup", params={"q": "TICKA", "exact_alias": True})
        assert lookup.status_code == 200
        payload = lookup.json()
        assert payload["candidates"]
        assert payload["candidates"][0]["kind"] == "alias"
        assert payload["candidates"][0]["value"] == "TICKA"
