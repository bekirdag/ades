from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app
from tests.release_helpers import (
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def test_release_verify_endpoint_reports_smoke_install_results(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    client = TestClient(create_app())

    verify_response = client.post(
        "/v0/release/verify",
        json={"output_dir": str(tmp_path / "dist"), "clean": True},
    )

    assert verify_response.status_code == 200
    verify_payload = verify_response.json()
    assert verify_payload["overall_success"] is True
    assert verify_payload["smoke_install"] is True
    assert verify_payload["python_install_smoke"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["passed"] is True


def test_release_endpoints_report_versions_sync_and_manifest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(
        tmp_path / "repo",
        version_file_version="0.1.0",
        pyproject_version="0.0.9",
        npm_version="0.0.8",
    )
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(
        monkeypatch,
        build_fake_release_runner(python_version="0.4.0", npm_version="0.4.0"),
    )
    client = TestClient(create_app())

    versions_response = client.get("/v0/release/versions")
    assert versions_response.status_code == 200
    assert versions_response.json()["synchronized"] is False

    sync_response = client.post("/v0/release/sync-version", json={"version": "0.4.0"})
    assert sync_response.status_code == 200
    assert sync_response.json()["version_state"]["synchronized"] is True

    manifest_response = client.post(
        "/v0/release/manifest",
        json={
            "output_dir": str(tmp_path / "dist"),
            "clean": True,
            "smoke_install": False,
        },
    )
    assert manifest_response.status_code == 200
    payload = manifest_response.json()
    assert payload["release_version"] == "0.4.0"
    assert payload["verification"]["overall_success"] is True
    assert payload["verification"]["smoke_install"] is False
    assert payload["verification"]["python_install_smoke"] is None
    assert payload["verification"]["npm_install_smoke"] is None
    assert payload["verification"]["wheel"]["file_name"].endswith(".whl")
    assert Path(payload["manifest_path"]).exists()
