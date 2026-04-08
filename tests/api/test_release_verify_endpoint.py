from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app
from tests.release_helpers import build_fake_release_runner


def test_release_verify_endpoint_reports_verified_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("ades.release._run_command", build_fake_release_runner())
    client = TestClient(create_app())

    response = client.post(
        "/v0/release/verify",
        json={"output_dir": str(tmp_path / "dist"), "clean": True},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["overall_success"] is True
    assert payload["versions_match"] is True
    assert payload["wheel"]["file_name"].endswith(".whl")
    assert payload["sdist"]["file_name"].endswith(".tar.gz")
    assert payload["npm_tarball"]["file_name"].endswith(".tgz")
