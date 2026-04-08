import json
from pathlib import Path
import subprocess
from typing import Callable

from ades.version import __version__


def _write_artifact(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def build_fake_release_runner(
    *,
    python_version: str = __version__,
    npm_version: str = __version__,
):
    """Return a fake subprocess runner for release artifact verification tests."""

    def runner(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
        if len(command) >= 3 and command[1:3] == ["-m", "build"]:
            outdir = Path(command[command.index("--outdir") + 1])
            wheel_path = outdir / f"ades-{python_version}-py3-none-any.whl"
            sdist_path = outdir / f"ades-{python_version}.tar.gz"
            _write_artifact(wheel_path, b"fake wheel artifact")
            _write_artifact(sdist_path, b"fake sdist artifact")
            return subprocess.CompletedProcess(command, 0, stdout="built", stderr="")
        if len(command) >= 2 and command[:2] == ["npm", "pack"]:
            outdir = Path(command[command.index("--pack-destination") + 1])
            tarball_name = f"ades-cli-{npm_version}.tgz"
            tarball_path = outdir / tarball_name
            _write_artifact(tarball_path, b"fake npm tarball")
            payload = json.dumps([{"filename": tarball_name}])
            return subprocess.CompletedProcess(command, 0, stdout=payload, stderr="")
        raise AssertionError(f"Unexpected command: {command!r} (cwd={cwd})")

    return runner


def build_failed_release_runner(*, stderr: str) -> Callable[..., subprocess.CompletedProcess[str]]:
    """Return a fake subprocess runner that always fails the Python build step."""

    def runner(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, stdout="", stderr=stderr)

    return runner
