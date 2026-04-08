import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.release_helpers import build_fake_release_runner


def test_cli_release_verify_outputs_verified_artifact_metadata(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("ades.release._run_command", build_fake_release_runner())
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["release", "verify", "--output-dir", str(tmp_path / "dist")],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["overall_success"] is True
    assert payload["versions_match"] is True
    assert payload["wheel"]["file_name"].endswith(".whl")
    assert payload["sdist"]["file_name"].endswith(".tar.gz")
    assert payload["npm_tarball"]["file_name"].endswith(".tgz")
