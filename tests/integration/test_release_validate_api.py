from pathlib import Path

from ades import validate_release
from tests.release_helpers import (
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def test_public_release_validate_api_runs_tests_and_persists_manifest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    response = validate_release(
        output_dir=tmp_path / "dist",
        smoke_install=False,
    )

    assert response.tests.passed is True
    assert response.overall_success is True
    assert response.manifest is not None
    assert response.manifest.verification.smoke_install is False
    assert response.manifest.verification.python_install_smoke is None
    assert response.manifest.verification.npm_install_smoke is None
    assert Path(response.manifest_path).exists()
