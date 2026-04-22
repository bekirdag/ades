from fastapi.testclient import TestClient

from ades.service.app import create_app


def test_service_startup_skips_prewarm_when_disabled(monkeypatch, tmp_path) -> None:
    calls = {"count": 0}

    def _fail_prewarm(self) -> None:
        calls["count"] += 1
        raise AssertionError("prewarm should be skipped")

    monkeypatch.setenv("ADES_SERVICE_PREWARM_ENABLED", "false")
    monkeypatch.setattr(
        "ades.service.app._ServiceRuntimeState.prewarm_active_packs",
        _fail_prewarm,
    )

    with TestClient(create_app(storage_root=tmp_path / "service-storage")) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert calls["count"] == 0
