from pathlib import Path

from fastapi.testclient import TestClient

from ades.release import write_release_manifest
from ades.service.app import create_app
from tests.release_helpers import (
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def test_release_publish_endpoint_runs_coordinated_publish(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    client = TestClient(create_app())

    validate_response = client.post(
        "/v0/release/validate",
        json={
            "output_dir": str(tmp_path / "dist"),
            "clean": True,
            "smoke_install": False,
        },
    )
    assert validate_response.status_code == 200
    manifest_path = validate_response.json()["manifest_path"]

    publish_response = client.post(
        "/v0/release/publish",
        json={"manifest_path": manifest_path, "dry_run": False},
    )

    assert publish_response.status_code == 200
    payload = publish_response.json()
    assert payload["overall_success"] is True
    assert payload["python_publish"]["executed"] is True
    assert payload["npm_publish"]["executed"] is True


def test_release_publish_endpoint_rejects_unvalidated_manifest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    manifest = write_release_manifest(
        output_dir=tmp_path / "dist",
        smoke_install=False,
    )
    client = TestClient(create_app())

    publish_response = client.post(
        "/v0/release/publish",
        json={"manifest_path": manifest.manifest_path, "dry_run": True},
    )

    assert publish_response.status_code == 400
    assert "ades release validate" in publish_response.json()["detail"]


def test_release_publish_endpoint_reports_missing_manifest_path(tmp_path: Path) -> None:
    missing_manifest = tmp_path / "missing-release-manifest.json"
    client = TestClient(create_app())

    publish_response = client.post(
        "/v0/release/publish",
        json={"manifest_path": str(missing_manifest), "dry_run": True},
    )

    assert publish_response.status_code == 404
    assert publish_response.json() == {
        "detail": f"Release manifest not found: {missing_manifest.resolve()}"
    }
