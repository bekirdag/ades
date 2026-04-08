import json
import os
from pathlib import Path
import subprocess

from tests.npm_wrapper_helpers import NPM_WRAPPER_BIN


def test_npm_wrapper_reports_bootstrap_info_with_overrides(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    env = os.environ.copy()
    env["ADES_NPM_RUNTIME_DIR"] = str(runtime_dir)
    env["ADES_PYTHON_PACKAGE_SPEC"] = str(tmp_path / "local-package")

    result = subprocess.run(
        ["node", str(NPM_WRAPPER_BIN), "--npm-bootstrap-info"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["npmPackage"] == "ades-cli"
    assert payload["command"] == "ades"
    assert payload["runtimeDir"] == str(runtime_dir.resolve())
    assert payload["pythonPackageSpec"] == str((tmp_path / "local-package").resolve())
