import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.release import validate_release_workflow
from tests.release_helpers import (
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def test_cli_release_publish_runs_coordinated_publish(
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
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "release",
            "publish",
            "--manifest-path",
            validation.manifest_path,
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["overall_success"] is True
    assert payload["python_publish"]["executed"] is True
    assert payload["npm_publish"]["executed"] is True


def test_cli_release_publish_exits_nonzero_when_publish_fails(
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
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "release",
            "publish",
            "--manifest-path",
            validation.manifest_path,
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["overall_success"] is False
    assert payload["warnings"] == [
        "python_publish_failed:1",
        "npm_publish_skipped:python_publish_failed",
    ]
