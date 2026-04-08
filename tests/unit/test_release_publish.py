from pathlib import Path

import pytest

from ades.release import publish_release_manifest, validate_release_workflow, write_release_manifest
from tests.release_helpers import (
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def test_publish_release_manifest_runs_python_then_npm_publish(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    validation = validate_release_workflow(
        output_dir=tmp_path / "dist",
        smoke_install=False,
    )
    response = publish_release_manifest(manifest_path=validation.manifest_path)

    assert response.validated_manifest is True
    assert response.overall_success is True
    assert response.python_publish.executed is True
    assert response.python_publish.command.command[1:3] == ["-m", "twine"]
    assert response.python_publish.command.passed is True
    assert response.npm_publish.executed is True
    assert response.npm_publish.command.command[:2] == ["npm", "publish"]
    assert response.npm_publish.command.passed is True
    assert response.warnings == []


def test_publish_release_manifest_requires_validated_manifest(
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

    with pytest.raises(
        ValueError,
        match="ades release validate",
    ):
        publish_release_manifest(manifest_path=manifest.manifest_path)


def test_publish_release_manifest_can_dry_run(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    validation = validate_release_workflow(
        output_dir=tmp_path / "dist",
        smoke_install=False,
    )
    response = publish_release_manifest(
        manifest_path=validation.manifest_path,
        dry_run=True,
    )

    assert response.overall_success is True
    assert response.python_publish.executed is False
    assert response.python_publish.command.stdout == "Dry run: skipped Python publish."
    assert response.npm_publish.executed is False
    assert response.npm_publish.command.stdout == "Dry run: skipped npm publish."
    assert response.warnings == []


def test_publish_release_manifest_skips_npm_when_python_publish_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    validation = validate_release_workflow(
        output_dir=tmp_path / "dist",
        smoke_install=False,
    )
    patch_release_runner(
        monkeypatch,
        build_fake_release_runner(
            twine_upload_returncode=1,
            twine_upload_stderr="upload failed",
        ),
    )

    response = publish_release_manifest(manifest_path=validation.manifest_path)

    assert response.overall_success is False
    assert response.python_publish.executed is True
    assert response.python_publish.passed is False
    assert response.python_publish.command.stderr == "upload failed"
    assert response.npm_publish.executed is False
    assert response.npm_publish.command.stderr == "Skipped because Python publish failed."
    assert response.warnings == [
        "python_publish_failed:1",
        "npm_publish_skipped:python_publish_failed",
    ]
