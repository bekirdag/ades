from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app


def test_pack_list_endpoint_returns_404_for_missing_explicit_config(
    tmp_path: Path, monkeypatch
) -> None:
    missing_config = tmp_path / "missing.toml"
    client = TestClient(create_app(storage_root=tmp_path))
    monkeypatch.setenv("ADES_CONFIG_FILE", str(missing_config))

    response = client.get("/v0/packs")

    assert response.status_code == 404
    assert response.json() == {
        "detail": f"ades config file not found: {missing_config}"
    }


def test_available_pack_list_endpoint_returns_400_for_invalid_runtime_target(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "ades.toml"
    config_path.write_text("", encoding="utf-8")
    client = TestClient(create_app(storage_root=tmp_path))
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("ADES_RUNTIME_TARGET", "broken-runtime")

    response = client.get(
        "/v0/packs/available",
        params={"registry_url": "https://example.invalid/index.json"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported ades runtime target: 'broken-runtime'"
