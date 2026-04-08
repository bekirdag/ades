from pathlib import Path

from ades import publish_release, validate_release
from tests.release_helpers import (
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def test_public_release_publish_api_uses_validated_manifest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    validation = validate_release(
        output_dir=tmp_path / "dist",
        smoke_install=False,
    )
    response = publish_release(manifest_path=validation.manifest_path)

    assert response.validated_manifest is True
    assert response.overall_success is True
    assert response.python_publish.command.command[1:3] == ["-m", "twine"]
    assert response.npm_publish.command.command[:2] == ["npm", "publish"]
