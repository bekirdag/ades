from pathlib import Path

from ades.release import verify_release_artifacts
from ades.version import __version__
import pytest

from tests.release_helpers import build_failed_release_runner, build_fake_release_runner


def test_verify_release_artifacts_builds_and_hashes_expected_outputs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("ades.release._run_command", build_fake_release_runner())

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.python_version == __version__
    assert response.npm_version == __version__
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
    monkeypatch.setattr("ades.release._run_command", build_fake_release_runner())
    monkeypatch.setattr(
        "ades.release.load_npm_package_json",
        lambda *, package_dir=None: {"name": "ades-cli", "version": "0.1.1"},
    )

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.versions_match is False
    assert response.overall_success is False
    assert response.warnings == [f"version_mismatch:python={__version__},npm=0.1.1"]


def test_verify_release_artifacts_raises_on_python_build_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "ades.release._run_command",
        build_failed_release_runner(stderr="python build failed"),
    )

    with pytest.raises(ValueError, match="python build failed"):
        verify_release_artifacts(output_dir=tmp_path / "dist")
