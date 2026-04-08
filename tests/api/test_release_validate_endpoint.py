from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app
from tests.release_helpers import (
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def test_release_validate_endpoint_runs_tests_and_persists_manifest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    client = TestClient(create_app())

    response = client.post(
        "/v0/release/validate",
        json={
            "output_dir": str(tmp_path / "dist"),
            "clean": True,
            "smoke_install": False,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["overall_success"] is True
    assert payload["tests"]["passed"] is True
    assert payload["manifest"]["verification"]["overall_success"] is True
    assert payload["manifest"]["verification"]["smoke_install"] is False
    assert payload["manifest"]["verification"]["python_install_smoke"] is None
    assert payload["manifest"]["verification"]["npm_install_smoke"] is None
    assert Path(payload["manifest_path"]).exists()
