import json
from pathlib import Path
import subprocess
from typing import Callable

from ades.version import __version__


def _write_artifact(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def create_release_project(
    root: Path,
    *,
    version_file_version: str = __version__,
    pyproject_version: str = __version__,
    npm_version: str = __version__,
) -> tuple[Path, Path]:
    """Create a minimal coordinated release checkout for sync tests."""

    project_root = root.resolve()
    version_path = project_root / "src" / "ades" / "version.py"
    pyproject_path = project_root / "pyproject.toml"
    npm_package_json_path = project_root / "npm" / "ades-cli" / "package.json"
    version_path.parent.mkdir(parents=True, exist_ok=True)
    npm_package_json_path.parent.mkdir(parents=True, exist_ok=True)

    version_path.write_text(
        f'"""Package version."""\n\n__version__ = "{version_file_version}"\n',
        encoding="utf-8",
    )
    pyproject_path.write_text(
        "\n".join(
            [
                "[project]",
                'name = "ades"',
                f'version = "{pyproject_version}"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    npm_package_json_path.write_text(
        json.dumps(
            {
                "name": "ades-cli",
                "version": npm_version,
                "bin": {"ades": "bin/ades.cjs"},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return project_root, npm_package_json_path.parent


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
