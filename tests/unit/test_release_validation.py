from pathlib import Path

from ades.release import validate_release_workflow
from tests.release_helpers import build_fake_release_runner, create_release_project


def test_validate_release_workflow_runs_tests_then_writes_manifest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    monkeypatch.setattr("ades.release._run_command", build_fake_release_runner())

    response = validate_release_workflow(output_dir=tmp_path / "dist")

    assert response.tests.passed is True
    assert response.tests.command[1:3] == ["-m", "pytest"]
    assert response.overall_success is True
    assert response.manifest is not None
    assert response.manifest_path == response.manifest.manifest_path
    assert Path(response.manifest_path).exists()
    assert response.warnings == []


def test_validate_release_workflow_stops_before_packaging_when_tests_fail(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    monkeypatch.setattr(
        "ades.release._run_command",
        build_fake_release_runner(
            test_returncode=1,
            test_stdout="",
            test_stderr="1 failed",
        ),
    )

    response = validate_release_workflow(output_dir=tmp_path / "dist")

    assert response.tests.passed is False
    assert response.tests.exit_code == 1
    assert response.tests.stderr == "1 failed"
    assert response.overall_success is False
    assert response.manifest is None
    assert response.manifest_path is None
    assert response.warnings == ["tests_failed:1"]
