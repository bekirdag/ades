import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.release_helpers import (
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def test_cli_release_validate_runs_full_release_flow(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["release", "validate", "--output-dir", str(tmp_path / "dist")],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["overall_success"] is True
    assert payload["tests"]["passed"] is True
    assert payload["manifest"]["verification"]["overall_success"] is True
    assert payload["manifest"]["verification"]["python_install_smoke"]["passed"] is True
    assert Path(payload["manifest_path"]).exists()


def test_cli_release_validate_can_disable_smoke_install(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "release",
            "validate",
            "--output-dir",
            str(tmp_path / "dist"),
            "--no-smoke-install",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["overall_success"] is True
    assert payload["manifest"]["verification"]["smoke_install"] is False
    assert payload["manifest"]["verification"]["python_install_smoke"] is None
    assert payload["manifest"]["verification"]["npm_install_smoke"] is None


def test_cli_release_validate_exits_nonzero_when_tests_fail(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(
        monkeypatch,
        build_fake_release_runner(test_returncode=1, test_stderr="1 failed"),
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["release", "validate", "--output-dir", str(tmp_path / "dist")],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["overall_success"] is False
    assert payload["tests"]["passed"] is False
    assert payload["manifest"] is None
    assert payload["warnings"] == ["tests_failed:1"]
