import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.release_helpers import build_fake_release_runner, create_release_project


def test_cli_release_commands_sync_versions_and_write_manifest(
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
    runner = CliRunner()

    sync_result = runner.invoke(app, ["release", "sync-version", "0.2.0"])
    assert sync_result.exit_code == 0
    sync_payload = json.loads(sync_result.stdout)
    assert sync_payload["target_version"] == "0.2.0"
    assert sync_payload["version_state"]["synchronized"] is True

    manifest_result = runner.invoke(
        app,
        ["release", "manifest", "--output-dir", str(tmp_path / "dist")],
    )
    assert manifest_result.exit_code == 0
    manifest_payload = json.loads(manifest_result.stdout)
    assert manifest_payload["release_version"] == "0.2.0"
    assert manifest_payload["verification"]["overall_success"] is True
    assert manifest_payload["verification"]["wheel"]["file_name"].endswith(".whl")
    assert Path(manifest_payload["manifest_path"]).exists()
