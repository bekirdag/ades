from pathlib import Path

import pytest

from ades.release import (
    release_versions,
    sync_release_version,
    verify_release_artifacts,
    write_release_manifest,
)
from ades.version import __version__
from tests.release_helpers import (
    build_failed_release_runner,
    build_fake_release_runner,
    create_release_project,
)


def test_release_versions_reports_synchronized_state(monkeypatch, tmp_path: Path) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)

    response = release_versions()

    assert response.version_file_version == __version__
    assert response.pyproject_version == __version__
    assert response.npm_version == __version__
    assert response.synchronized is True


def test_sync_release_version_updates_all_release_files(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo", pyproject_version="0.0.9")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)

    response = sync_release_version("0.2.0")

    assert response.target_version == "0.2.0"
    assert response.version_state.synchronized is True
    assert response.version_state.version_file_version == "0.2.0"
    assert response.version_state.pyproject_version == "0.2.0"
    assert response.version_state.npm_version == "0.2.0"
    assert len(response.updated_files) == 3


def test_verify_release_artifacts_builds_and_hashes_expected_outputs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    monkeypatch.setattr("ades.release._run_command", build_fake_release_runner())

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.python_version == __version__
    assert response.npm_version == __version__
    assert response.version_state.synchronized is True
    assert response.versions_match is True
    assert response.overall_success is True
    assert response.wheel.file_name.endswith(".whl")
    assert response.sdist.file_name.endswith(".tar.gz")
    assert response.npm_tarball.file_name.endswith(".tgz")
    assert len(response.wheel.sha256) == 64
    assert len(response.sdist.sha256) == 64
    assert len(response.npm_tarball.sha256) == 64


def test_verify_release_artifacts_reports_version_mismatch_warning(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(
        tmp_path / "repo",
        pyproject_version="0.1.1",
        npm_version="0.1.2",
    )
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    monkeypatch.setattr(
        "ades.release._run_command",
        build_fake_release_runner(python_version="0.1.1", npm_version="0.1.2"),
    )

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.versions_match is False
    assert response.overall_success is False
    assert response.version_state.synchronized is False
    assert response.warnings == [
        f"version_mismatch:version_file={__version__},pyproject=0.1.1,npm=0.1.2"
    ]


def test_verify_release_artifacts_raises_on_python_build_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    monkeypatch.setattr(
        "ades.release._run_command",
        build_failed_release_runner(stderr="python build failed"),
    )

    with pytest.raises(ValueError, match="python build failed"):
        verify_release_artifacts(output_dir=tmp_path / "dist")


def test_write_release_manifest_can_sync_then_persist_manifest(
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
    monkeypatch.setattr(
        "ades.release._run_command",
        build_fake_release_runner(python_version="0.2.0", npm_version="0.2.0"),
    )

    response = write_release_manifest(
        output_dir=tmp_path / "dist",
        version="0.2.0",
    )

    assert response.release_version == "0.2.0"
    assert response.version_sync is not None
    assert response.version_sync.target_version == "0.2.0"
    assert response.version_state.synchronized is True
    assert Path(response.manifest_path).exists()
