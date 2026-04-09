from pathlib import Path

import pytest

from ades.release import (
    _parse_status_version,
    release_versions,
    sync_release_version,
    verify_release_artifacts,
    write_release_manifest,
)
from ades.service.models import ReleaseCommandResult
from ades.version import __version__
from tests.release_helpers import (
    build_failed_release_runner,
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
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
    patch_release_runner(monkeypatch, build_fake_release_runner())

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.python_version == __version__
    assert response.npm_version == __version__
    assert response.version_state.synchronized is True
    assert response.versions_match is True
    assert response.smoke_install is True
    assert response.overall_success is True
    assert response.wheel.file_name.endswith(".whl")
    assert response.sdist.file_name.endswith(".tar.gz")
    assert response.npm_tarball.file_name.endswith(".tgz")
    assert response.python_install_smoke is not None
    assert response.python_install_smoke.passed is True
    assert response.python_install_smoke.reported_version == __version__
    assert response.python_install_smoke.pull is not None
    assert response.python_install_smoke.pull.passed is True
    assert response.python_install_smoke.tag is not None
    assert response.python_install_smoke.tag.passed is True
    assert response.python_install_smoke.recovery_status is not None
    assert response.python_install_smoke.recovery_status.passed is True
    assert response.python_install_smoke.serve is not None
    assert response.python_install_smoke.serve.passed is True
    assert response.python_install_smoke.serve_healthz is not None
    assert response.python_install_smoke.serve_healthz.passed is True
    assert response.python_install_smoke.serve_status is not None
    assert response.python_install_smoke.serve_status.passed is True
    assert "general-en" in response.python_install_smoke.pulled_pack_ids
    assert {"organization", "email_address"} <= set(response.python_install_smoke.tagged_labels)
    assert "general-en" in response.python_install_smoke.recovered_pack_ids
    assert "general-en" in response.python_install_smoke.served_pack_ids
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.passed is True
    assert response.npm_install_smoke.reported_version == __version__
    assert response.npm_install_smoke.pull is not None
    assert response.npm_install_smoke.pull.passed is True
    assert response.npm_install_smoke.tag is not None
    assert response.npm_install_smoke.tag.passed is True
    assert response.npm_install_smoke.recovery_status is not None
    assert response.npm_install_smoke.recovery_status.passed is True
    assert response.npm_install_smoke.serve is not None
    assert response.npm_install_smoke.serve.passed is True
    assert response.npm_install_smoke.serve_healthz is not None
    assert response.npm_install_smoke.serve_healthz.passed is True
    assert response.npm_install_smoke.serve_status is not None
    assert response.npm_install_smoke.serve_status.passed is True
    assert "general-en" in response.npm_install_smoke.pulled_pack_ids
    assert {"organization", "email_address"} <= set(response.npm_install_smoke.tagged_labels)
    assert "general-en" in response.npm_install_smoke.recovered_pack_ids
    assert "general-en" in response.npm_install_smoke.served_pack_ids
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
    patch_release_runner(
        monkeypatch,
        build_fake_release_runner(python_version="0.1.1", npm_version="0.1.2"),
    )

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.versions_match is False
    assert response.overall_success is False
    assert response.version_state.synchronized is False
    assert response.warnings == [
        f"version_mismatch:version_file={__version__},pyproject=0.1.1,npm=0.1.2",
        "python_wheel_version_mismatch:0.1.1",
        "npm_tarball_version_mismatch:0.1.1",
    ]


def test_verify_release_artifacts_raises_on_python_build_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(
        monkeypatch,
        build_failed_release_runner(stderr="python build failed"),
    )

    with pytest.raises(ValueError, match="python build failed"):
        verify_release_artifacts(output_dir=tmp_path / "dist")


def test_verify_release_artifacts_can_skip_smoke_install(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    response = verify_release_artifacts(
        output_dir=tmp_path / "dist",
        smoke_install=False,
    )

    assert response.smoke_install is False
    assert response.overall_success is True
    assert response.python_install_smoke is None
    assert response.npm_install_smoke is None
    assert response.warnings == []


def test_verify_release_artifacts_reports_smoke_install_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(
        monkeypatch,
        build_fake_release_runner(
            npm_install_returncode=4,
            npm_install_stderr="npm install failed",
        ),
    )

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.python_install_smoke is not None
    assert response.python_install_smoke.passed is True
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.passed is False
    assert response.npm_install_smoke.install.exit_code == 4
    assert response.npm_install_smoke.install.stderr == "npm install failed"
    assert (
        response.npm_install_smoke.invoke.stderr
        == "Skipped because npm tarball installation failed."
    )
    assert response.warnings == ["npm_tarball_install_failed:4"]


def test_verify_release_artifacts_reports_tag_smoke_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(
        monkeypatch,
        build_fake_release_runner(
            npm_tag_returncode=7,
            npm_tag_stderr="tag failed",
        ),
    )

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.pull is not None
    assert response.npm_install_smoke.pull.passed is True
    assert response.npm_install_smoke.tag is not None
    assert response.npm_install_smoke.tag.passed is False
    assert response.npm_install_smoke.tag.exit_code == 7
    assert response.npm_install_smoke.tag.stderr == "tag failed"
    assert response.warnings == ["npm_tarball_tag_failed:7"]


def test_verify_release_artifacts_reports_serve_smoke_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        if "node_modules/.bin" not in str(executable):
            return (
                ReleaseCommandResult(
                    command=[str(executable), "serve"],
                    exit_code=0,
                    passed=True,
                    stdout="Using storage root\n",
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/healthz"],
                    exit_code=0,
                    passed=True,
                    stdout='{"status":"ok","version":"0.1.0"}',
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/v0/status"],
                    exit_code=0,
                    passed=True,
                    stdout='{"service":"ades","version":"0.1.0","installed_packs":["general-en"]}',
                    stderr="",
                ),
                ["general-en"],
            )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve"],
                exit_code=9,
                passed=False,
                stdout="",
                stderr="serve failed",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=1,
                passed=False,
                stdout="",
                stderr="connection refused",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=-1,
                passed=False,
                stdout="",
                stderr="Skipped because service startup did not pass the health probe.",
            ),
            [],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.serve is not None
    assert response.npm_install_smoke.serve.passed is False
    assert response.npm_install_smoke.serve.exit_code == 9
    assert response.npm_install_smoke.serve.stderr == "serve failed"
    assert response.warnings == ["npm_tarball_serve_failed:9"]


def test_verify_release_artifacts_reports_recovery_status_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_recovery_status(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        if "node_modules/.bin" not in str(executable):
            return (
                ReleaseCommandResult(
                    command=[str(executable), "status"],
                    exit_code=0,
                    passed=True,
                    stdout=(
                        '{"service":"ades","version":"'
                        + expected_version
                        + '","installed_packs":["general-en"]}'
                    ),
                    stderr="",
                ),
                ["general-en"],
            )
        return (
            ReleaseCommandResult(
                command=[str(executable), "status"],
                exit_code=11,
                passed=False,
                stdout="",
                stderr="status failed after metadata reset",
            ),
            [],
        )

    monkeypatch.setattr("ades.release._run_cli_recovery_status_smoke", fake_recovery_status)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.recovery_status is not None
    assert response.npm_install_smoke.recovery_status.passed is False
    assert response.npm_install_smoke.recovery_status.exit_code == 11
    assert response.npm_install_smoke.recovery_status.stderr == "status failed after metadata reset"
    assert response.warnings == ["npm_tarball_recovery_status_failed:11"]


def test_parse_status_version_accepts_wrapper_logs_before_json() -> None:
    result = ReleaseCommandResult(
        command=["ades", "status"],
        exit_code=0,
        passed=True,
        stdout=(
            "Collecting ades==0.1.0\n"
            "Successfully installed ades-0.1.0\n"
            "{\n"
            '  "service": "ades",\n'
            '  "version": "0.1.0"\n'
            "}\n"
        ),
        stderr="",
    )

    assert _parse_status_version(result) == "0.1.0"


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
    patch_release_runner(
        monkeypatch,
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
    assert response.verification.smoke_install is True
    assert response.verification.python_install_smoke is not None
    assert response.verification.python_install_smoke.passed is True
    assert Path(response.manifest_path).exists()
