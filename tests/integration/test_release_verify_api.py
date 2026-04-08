from pathlib import Path

from ades import release_versions, sync_release_version, verify_release, write_release_manifest
from tests.release_helpers import build_fake_release_runner, create_release_project


def test_public_release_api_can_sync_versions_and_persist_manifest(
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
        build_fake_release_runner(python_version="0.3.0", npm_version="0.3.0"),
    )

    before = release_versions()
    sync = sync_release_version("0.3.0")
    verification = verify_release(output_dir=tmp_path / "dist")
    manifest = write_release_manifest(output_dir=tmp_path / "dist")

    assert before.synchronized is False
    assert sync.version_state.version_file_version == "0.3.0"
    assert verification.version_state.synchronized is True
    assert verification.output_dir == str((tmp_path / "dist").resolve())
    assert manifest.release_version == "0.3.0"
    assert Path(manifest.manifest_path).exists()
