from pathlib import Path

from ades import verify_release
from tests.release_helpers import build_fake_release_runner


def test_public_api_can_build_and_verify_release_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("ades.release._run_command", build_fake_release_runner())

    response = verify_release(output_dir=tmp_path / "dist")

    assert response.overall_success is True
    assert response.warnings == []
    assert response.output_dir == str((tmp_path / "dist").resolve())
    assert response.wheel.path.endswith(".whl")
    assert response.sdist.path.endswith(".tar.gz")
    assert response.npm_tarball.path.endswith(".tgz")
