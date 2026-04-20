import json
import os
from pathlib import Path
import subprocess

from tests.npm_wrapper_helpers import (
    NPM_WRAPPER_BIN,
    create_fake_python_environment,
    read_json_lines,
)


def test_npm_wrapper_bootstraps_runtime_and_reuses_it_on_second_run(tmp_path: Path) -> None:
    fake_python, log_path = create_fake_python_environment(tmp_path)
    runtime_dir = tmp_path / "runtime"
    env = os.environ.copy()
    env["ADES_PYTHON_BIN"] = str(fake_python)
    env["ADES_NPM_RUNTIME_DIR"] = str(runtime_dir)
    env["ADES_PYTHON_PACKAGE_SPEC"] = "ades-tool==0.1.0"
    env["ADES_NPM_TEST_LOG"] = str(log_path)

    first = subprocess.run(
        ["node", str(NPM_WRAPPER_BIN), "status", "--json"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert first.returncode == 0
    assert json.loads(first.stdout) == {"argv": ["status", "--json"], "bootstrap": "ok"}
    first_log = read_json_lines(log_path)
    assert [entry["argv"] for entry in first_log] == [
        ["-m", "venv", str(runtime_dir.resolve())],
        ["-m", "pip", "install", "--upgrade", "ades-tool==0.1.0"],
    ]

    second = subprocess.run(
        ["node", str(NPM_WRAPPER_BIN), "packs", "list"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert second.returncode == 0
    assert json.loads(second.stdout) == {"argv": ["packs", "list"], "bootstrap": "ok"}
    second_log = read_json_lines(log_path)
    assert [entry["argv"] for entry in second_log] == [
        ["-m", "venv", str(runtime_dir.resolve())],
        ["-m", "pip", "install", "--upgrade", "ades-tool==0.1.0"],
    ]
