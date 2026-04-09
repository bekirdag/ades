import json
from pathlib import Path
import subprocess
from typing import Callable
from typing import Any

from ades.service.models import ReleaseCommandResult
from ades.version import __version__


def _write_artifact(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


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
    test_returncode: int = 0,
    test_stdout: str = "111 passed",
    test_stderr: str = "",
    wheel_install_returncode: int = 0,
    wheel_install_stderr: str = "",
    wheel_invoke_returncode: int = 0,
    wheel_invoke_stderr: str = "",
    wheel_pull_returncode: int = 0,
    wheel_pull_stderr: str = "",
    wheel_tag_returncode: int = 0,
    wheel_tag_stderr: str = "",
    npm_install_returncode: int = 0,
    npm_install_stderr: str = "",
    npm_invoke_returncode: int = 0,
    npm_invoke_stderr: str = "",
    npm_pull_returncode: int = 0,
    npm_pull_stderr: str = "",
    npm_tag_returncode: int = 0,
    npm_tag_stderr: str = "",
    twine_upload_returncode: int = 0,
    twine_upload_stdout: str = "uploaded",
    twine_upload_stderr: str = "",
    npm_publish_returncode: int = 0,
    npm_publish_stdout: str = "published",
    npm_publish_stderr: str = "",
):
    """Return a fake subprocess runner for release artifact verification tests."""

    def runner(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
        if len(command) >= 3 and command[1:3] == ["-m", "pytest"]:
            return subprocess.CompletedProcess(
                command,
                test_returncode,
                stdout=test_stdout,
                stderr=test_stderr,
            )
        if len(command) >= 3 and command[1:3] == ["-m", "venv"]:
            venv_dir = Path(command[-1])
            if command[0].endswith("python.exe"):
                _write_text(venv_dir / "Scripts" / "python.exe", "")
            else:
                _write_text(venv_dir / "bin" / "python", "#!/usr/bin/env python\n")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if len(command) >= 3 and command[1:3] == ["-m", "pip"]:
            python_bin = Path(command[0])
            bin_dir = python_bin.parent
            if python_bin.name == "python.exe":
                _write_text(bin_dir / "ades.exe", "")
            else:
                _write_text(bin_dir / "ades", "#!/usr/bin/env python\n")
            return subprocess.CompletedProcess(
                command,
                wheel_install_returncode,
                stdout="installed" if wheel_install_returncode == 0 else "",
                stderr=wheel_install_stderr,
            )
        if len(command) >= 3 and command[1:3] == ["-m", "build"]:
            outdir = Path(command[command.index("--outdir") + 1])
            wheel_path = outdir / f"ades-{python_version}-py3-none-any.whl"
            sdist_path = outdir / f"ades-{python_version}.tar.gz"
            _write_artifact(wheel_path, b"fake wheel artifact")
            _write_artifact(sdist_path, b"fake sdist artifact")
            return subprocess.CompletedProcess(command, 0, stdout="built", stderr="")
        if len(command) >= 3 and command[1:3] == ["-m", "twine"]:
            return subprocess.CompletedProcess(
                command,
                twine_upload_returncode,
                stdout=twine_upload_stdout if twine_upload_returncode == 0 else "",
                stderr=twine_upload_stderr,
            )
        if len(command) >= 2 and command[:2] == ["npm", "pack"]:
            outdir = Path(command[command.index("--pack-destination") + 1])
            tarball_name = f"ades-cli-{npm_version}.tgz"
            tarball_path = outdir / tarball_name
            _write_artifact(tarball_path, b"fake npm tarball")
            payload = json.dumps([{"filename": tarball_name}])
            return subprocess.CompletedProcess(command, 0, stdout=payload, stderr="")
        if len(command) >= 2 and command[:2] == ["npm", "publish"]:
            return subprocess.CompletedProcess(
                command,
                npm_publish_returncode,
                stdout=npm_publish_stdout if npm_publish_returncode == 0 else "",
                stderr=npm_publish_stderr,
            )
        if len(command) >= 2 and command[:2] == ["npm", "install"]:
            prefix_dir = Path(command[command.index("--prefix") + 1])
            _write_text(
                prefix_dir / "node_modules" / ".bin" / "ades",
                "#!/usr/bin/env node\n",
            )
            return subprocess.CompletedProcess(
                command,
                npm_install_returncode,
                stdout="added 1 package" if npm_install_returncode == 0 else "",
                stderr=npm_install_stderr,
            )
        if command and Path(command[0]).name.startswith("ades"):
            is_npm_wrapper = "node_modules/.bin" in command[0]
            subcommand = command[1] if len(command) > 1 else "status"

            if subcommand == "status":
                payload = json.dumps(
                    {
                        "service": "ades",
                        "version": python_version,
                        "runtime_target": "local",
                        "metadata_backend": "sqlite",
                        "storage_root": str(Path(cwd) / "storage"),
                        "host": "127.0.0.1",
                        "port": 8734,
                        "installed_packs": [],
                    }
                )
                return subprocess.CompletedProcess(
                    command,
                    npm_invoke_returncode if is_npm_wrapper else wheel_invoke_returncode,
                    stdout=payload
                    if (npm_invoke_returncode if is_npm_wrapper else wheel_invoke_returncode) == 0
                    else "",
                    stderr=npm_invoke_stderr if is_npm_wrapper else wheel_invoke_stderr,
                )

            if subcommand == "pull":
                requested_pack = command[2]
                installed = (
                    ["general-en", "finance-en"]
                    if requested_pack == "finance-en"
                    else [requested_pack]
                )
                payload = json.dumps(
                    {
                        "requested_pack": requested_pack,
                        "registry_url": "file:///fake-registry/index.json",
                        "installed": installed,
                        "skipped": [],
                    }
                )
                return subprocess.CompletedProcess(
                    command,
                    npm_pull_returncode if is_npm_wrapper else wheel_pull_returncode,
                    stdout=payload
                    if (npm_pull_returncode if is_npm_wrapper else wheel_pull_returncode) == 0
                    else "",
                    stderr=npm_pull_stderr if is_npm_wrapper else wheel_pull_stderr,
                )

            if subcommand == "tag":
                payload = json.dumps(
                    {
                        "version": python_version,
                        "pack": "finance-en",
                        "language": "en",
                        "content_type": "text/plain",
                        "entities": [
                            {
                                "text": "Apple",
                                "label": "organization",
                                "start": 0,
                                "end": 5,
                                "confidence": 1.0,
                            },
                            {
                                "text": "AAPL",
                                "label": "ticker",
                                "start": 11,
                                "end": 15,
                                "confidence": 1.0,
                            },
                            {
                                "text": "NASDAQ",
                                "label": "exchange",
                                "start": 26,
                                "end": 32,
                                "confidence": 1.0,
                            },
                            {
                                "text": "USD 12.5",
                                "label": "currency_amount",
                                "start": 39,
                                "end": 47,
                                "confidence": 0.9,
                            },
                        ],
                        "topics": [],
                        "warnings": [],
                        "timing_ms": 1,
                    }
                )
                return subprocess.CompletedProcess(
                    command,
                    npm_tag_returncode if is_npm_wrapper else wheel_tag_returncode,
                    stdout=payload
                    if (npm_tag_returncode if is_npm_wrapper else wheel_tag_returncode) == 0
                    else "",
                    stderr=npm_tag_stderr if is_npm_wrapper else wheel_tag_stderr,
                )

            raise AssertionError(f"Unexpected ades subcommand: {subcommand!r}")
        raise AssertionError(f"Unexpected command: {command!r} (cwd={cwd})")

    return runner


def build_failed_release_runner(*, stderr: str) -> Callable[..., subprocess.CompletedProcess[str]]:
    """Return a fake subprocess runner that always fails the Python build step."""

    def runner(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, stdout="", stderr=stderr)

    return runner


def build_fake_service_smoke(
    *,
    version: str,
    storage_root: Path,
) -> tuple[ReleaseCommandResult, ReleaseCommandResult, ReleaseCommandResult, list[str]]:
    """Return a deterministic fake `ades serve` smoke result."""

    serve = ReleaseCommandResult(
        command=["ades", "serve", "--host", "127.0.0.1", "--port", "8734"],
        exit_code=0,
        passed=True,
        stdout=f"Using storage root: {storage_root}\n",
        stderr="",
    )
    healthz = ReleaseCommandResult(
        command=["GET", "http://127.0.0.1:8734/healthz"],
        exit_code=0,
        passed=True,
        stdout=json.dumps({"status": "ok", "version": version}),
        stderr="",
    )
    status = ReleaseCommandResult(
        command=["GET", "http://127.0.0.1:8734/v0/status"],
        exit_code=0,
        passed=True,
        stdout=json.dumps(
            {
                "service": "ades",
                "version": version,
                "runtime_target": "local",
                "metadata_backend": "sqlite",
                "storage_root": str(storage_root),
                "host": "127.0.0.1",
                "port": 8734,
                "installed_packs": ["general-en", "finance-en"],
            }
        ),
        stderr="",
    )
    return serve, healthz, status, ["general-en", "finance-en"]


def build_fake_recovery_status(
    *,
    version: str,
    storage_root: Path,
) -> tuple[ReleaseCommandResult, list[str]]:
    """Return a deterministic fake recovery `ades status` result."""

    status = ReleaseCommandResult(
        command=["ades", "status"],
        exit_code=0,
        passed=True,
        stdout=json.dumps(
            {
                "service": "ades",
                "version": version,
                "runtime_target": "local",
                "metadata_backend": "sqlite",
                "storage_root": str(storage_root),
                "host": "127.0.0.1",
                "port": 8734,
                "installed_packs": ["general-en", "finance-en"],
            }
        ),
        stderr="",
    )
    return status, ["general-en", "finance-en"]


def patch_release_runner(monkeypatch: Any, runner: Callable[..., subprocess.CompletedProcess[str]]) -> None:
    """Patch both release command helpers to the same fake runner."""

    monkeypatch.setattr("ades.release._run_command", runner)
    monkeypatch.setattr(
        "ades.release._run_command_with_env",
        lambda command, *, cwd, env: runner(command, cwd=cwd),
    )
    monkeypatch.setattr(
        "ades.release._run_cli_recovery_status_smoke",
        lambda *, executable, working_dir, storage_root, expected_version, extra_env=None: build_fake_recovery_status(
            version=expected_version,
            storage_root=storage_root,
        ),
    )
    monkeypatch.setattr(
        "ades.release._run_cli_service_smoke",
        lambda *, executable, working_dir, storage_root, expected_version, extra_env=None: build_fake_service_smoke(
            version=expected_version,
            storage_root=storage_root,
        ),
    )
