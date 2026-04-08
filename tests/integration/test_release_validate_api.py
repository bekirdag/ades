from pathlib import Path

from ades import validate_release
from tests.release_helpers import build_fake_release_runner, create_release_project


def test_public_release_validate_api_runs_tests_and_persists_manifest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    monkeypatch.setattr("ades.release._run_command", build_fake_release_runner())

    response = validate_release(output_dir=tmp_path / "dist")

    assert response.tests.passed is True
    assert response.overall_success is True
    assert response.manifest is not None
    assert Path(response.manifest_path).exists()
