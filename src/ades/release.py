"""Release version coordination, artifact verification, and manifest helpers."""

from __future__ import annotations

from datetime import datetime, UTC
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Sequence

from .distribution import (
    NPM_COMMAND_NAME,
    NPM_PYTHON_BIN_ENV,
    NPM_PYTHON_PACKAGE_SPEC_ENV,
    NPM_RUNTIME_DIR_ENV,
    load_npm_package_json,
    resolve_npm_package_dir,
    resolve_project_root,
)
from .service.models import (
    ReleaseArtifactSummary,
    ReleaseCommandResult,
    ReleaseInstallSmokeResult,
    ReleaseManifestResponse,
    ReleasePublishResponse,
    ReleasePublishTargetResult,
    ReleaseValidationResponse,
    ReleaseValidationSummary,
    ReleaseVerificationResponse,
    ReleaseVersionState,
    ReleaseVersionSyncResponse,
)


PYTHON_PACKAGE_NAME = "ades"
PYTHON_BUILD_COMMAND = [sys.executable, "-m", "build", "--wheel", "--sdist"]
NPM_PACK_COMMAND = ["npm", "pack", "--json"]
PYTHON_PUBLISH_COMMAND = [sys.executable, "-m", "twine", "upload", "--non-interactive"]
NPM_PUBLISH_COMMAND = ["npm", "publish"]
DEFAULT_RELEASE_TEST_COMMAND = [sys.executable, "-m", "pytest", "-q"]
VERSION_FILE_RELATIVE_PATH = Path("src/ades/version.py")
PYPROJECT_RELATIVE_PATH = Path("pyproject.toml")
NPM_PACKAGE_JSON_RELATIVE_PATH = Path("npm/ades-cli/package.json")
VERSION_ASSIGNMENT_PATTERN = re.compile(r'(__version__\s*=\s*")([^"]+)(")')
PYPROJECT_VERSION_PATTERN = re.compile(r'(?m)^(version\s*=\s*")([^"]+)(")$')
TWINE_USERNAME_ALIAS_ENV = "ADES_RELEASE_TWINE_USERNAME"
TWINE_PASSWORD_ALIAS_ENV = "ADES_RELEASE_TWINE_PASSWORD"
TWINE_TOKEN_ALIAS_ENV = "ADES_RELEASE_TWINE_TOKEN"
TWINE_REPOSITORY_URL_ALIAS_ENV = "ADES_RELEASE_TWINE_REPOSITORY_URL"
NPM_TOKEN_ALIAS_ENV = "ADES_RELEASE_NPM_TOKEN"
NPM_REGISTRY_ALIAS_ENV = "ADES_RELEASE_NPM_REGISTRY"
NPM_ACCESS_ALIAS_ENV = "ADES_RELEASE_NPM_ACCESS"


def _run_command(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run one packaging command and capture structured text output."""

    return subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )


def _run_command_with_env(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    """Run one command with an explicit environment map."""

    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def _normalize_release_test_command(
    command: Sequence[str] | None = None,
) -> list[str]:
    """Return one validated release-validation test command."""

    if command is None:
        return list(DEFAULT_RELEASE_TEST_COMMAND)
    normalized = [part for part in command if str(part)]
    if not normalized:
        raise ValueError("Release validation tests_command cannot be empty.")
    return [str(part) for part in normalized]


def _build_command_result(
    command: Sequence[str],
    result: subprocess.CompletedProcess[str],
) -> ReleaseCommandResult:
    """Convert one subprocess result into the shared command-result model."""

    return ReleaseCommandResult(
        command=[str(part) for part in command],
        exit_code=result.returncode,
        passed=result.returncode == 0,
        stdout=result.stdout or "",
        stderr=result.stderr or "",
    )


def _skipped_command_result(
    command: Sequence[str],
    *,
    reason: str,
) -> ReleaseCommandResult:
    """Return a skipped command result used when a prior step already failed."""

    return ReleaseCommandResult(
        command=[str(part) for part in command],
        exit_code=-1,
        passed=False,
        stdout="",
        stderr=reason,
    )


def _dry_run_command_result(
    command: Sequence[str],
    *,
    detail: str,
) -> ReleaseCommandResult:
    """Return a successful command result used for dry-run publish steps."""

    return ReleaseCommandResult(
        command=[str(part) for part in command],
        exit_code=0,
        passed=True,
        stdout=detail,
        stderr="",
    )


def _build_clean_runtime_env(*, overrides: dict[str, str] | None = None) -> dict[str, str]:
    """Build a clean child-process environment without source-checkout Python paths."""

    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.pop("VIRTUAL_ENV", None)
    if overrides:
        env.update({key: str(value) for key, value in overrides.items()})
    return env


def _resolve_project_root_path(project_root: str | Path | None = None) -> Path:
    """Resolve the release source checkout root."""

    return (Path(project_root).expanduser() if project_root is not None else resolve_project_root()).resolve()


def _resolve_npm_package_dir_path(
    *,
    project_root: Path,
    npm_package_dir: str | Path | None = None,
) -> Path:
    """Resolve the npm package directory for this release checkout."""

    if npm_package_dir is not None:
        return Path(npm_package_dir).expanduser().resolve()
    return (project_root / NPM_PACKAGE_JSON_RELATIVE_PATH.parent).resolve()


def _version_file_path(project_root: Path) -> Path:
    """Return the source version file path."""

    return (project_root / VERSION_FILE_RELATIVE_PATH).resolve()


def _pyproject_path(project_root: Path) -> Path:
    """Return the pyproject.toml path."""

    return (project_root / PYPROJECT_RELATIVE_PATH).resolve()


def _npm_package_json_path(project_root: Path) -> Path:
    """Return the npm package.json path."""

    return (project_root / NPM_PACKAGE_JSON_RELATIVE_PATH).resolve()


def _validate_version(value: str) -> str:
    """Validate and normalize a coordinated release version string."""

    normalized = value.strip()
    if not normalized:
        raise ValueError("Release version cannot be empty.")
    if normalized != value:
        raise ValueError("Release version cannot have leading or trailing whitespace.")
    if any(char.isspace() for char in normalized):
        raise ValueError("Release version cannot contain whitespace.")
    return normalized


def _parse_version_file_version(text: str) -> str:
    """Extract the Python package version from src/ades/version.py."""

    match = VERSION_ASSIGNMENT_PATTERN.search(text)
    if match is None:
        raise ValueError("Could not parse __version__ from src/ades/version.py.")
    return match.group(2)


def _replace_version_file_version(text: str, version: str) -> str:
    """Replace the Python package version in src/ades/version.py."""

    updated, count = VERSION_ASSIGNMENT_PATTERN.subn(rf'\g<1>{version}\g<3>', text, count=1)
    if count != 1:
        raise ValueError("Could not update __version__ in src/ades/version.py.")
    return updated


def _parse_pyproject_version(text: str) -> str:
    """Extract the project version from pyproject.toml."""

    match = PYPROJECT_VERSION_PATTERN.search(text)
    if match is None:
        raise ValueError("Could not parse project.version from pyproject.toml.")
    return match.group(2)


def _replace_pyproject_version(text: str, version: str) -> str:
    """Replace the project version in pyproject.toml."""

    updated, count = PYPROJECT_VERSION_PATTERN.subn(rf'\g<1>{version}\g<3>', text, count=1)
    if count != 1:
        raise ValueError("Could not update project.version in pyproject.toml.")
    return updated


def _ensure_project_layout(project_root: Path, npm_package_dir: Path) -> None:
    """Validate that the expected source checkout layout is present."""

    version_path = _version_file_path(project_root)
    pyproject_path = _pyproject_path(project_root)
    package_json_path = npm_package_dir / "package.json"
    if not version_path.exists():
        raise FileNotFoundError(f"Release coordination requires {version_path}.")
    if not pyproject_path.exists():
        raise FileNotFoundError(f"Release coordination requires {pyproject_path}.")
    if not package_json_path.exists():
        raise FileNotFoundError(
            f"Release coordination requires package.json under {npm_package_dir}."
        )


def _prepare_output_dir(output_dir: Path, *, clean: bool) -> tuple[Path, Path]:
    """Create clean subdirectories for Python and npm artifacts."""

    python_dir = output_dir / "python"
    npm_dir = output_dir / "npm"
    if clean:
        shutil.rmtree(python_dir, ignore_errors=True)
        shutil.rmtree(npm_dir, ignore_errors=True)
    python_dir.mkdir(parents=True, exist_ok=True)
    npm_dir.mkdir(parents=True, exist_ok=True)
    return python_dir, npm_dir


def _resolve_venv_python(venv_dir: Path) -> Path:
    """Return the Python executable inside one virtual environment."""

    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _resolve_venv_command(venv_dir: Path, command_name: str) -> Path:
    """Return one installed console script path inside a virtual environment."""

    if os.name == "nt":
        for suffix in (".exe", ".cmd", ""):
            candidate = venv_dir / "Scripts" / f"{command_name}{suffix}"
            if suffix == "" or candidate.exists():
                return candidate
    return venv_dir / "bin" / command_name


def _resolve_npm_installed_command(prefix_dir: Path, command_name: str) -> Path:
    """Return one installed npm bin command under a local prefix."""

    bin_dir = prefix_dir / "node_modules" / ".bin"
    if os.name == "nt":
        for suffix in (".cmd", ".exe", ""):
            candidate = bin_dir / f"{command_name}{suffix}"
            if suffix == "" or candidate.exists():
                return candidate
    return bin_dir / command_name


def _parse_status_payload(output: str) -> dict[str, object] | None:
    """Return the last JSON object found in one command stdout stream."""

    text = output.strip()
    if not text:
        return None
    decoder = json.JSONDecoder()
    payload: dict[str, object] | None = None
    for index, character in enumerate(text):
        if character != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            payload = candidate
    return payload


def _parse_status_version(result: ReleaseCommandResult) -> str | None:
    """Extract the reported ades version from a JSON `ades status` payload."""

    if not result.passed:
        return None
    payload = _parse_status_payload(result.stdout or "")
    if payload is None:
        return None
    version = payload.get("version")
    return version if isinstance(version, str) and version else None


def _sha256(path: Path) -> str:
    """Return the SHA-256 digest for one file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_artifact_summary(path: Path) -> ReleaseArtifactSummary:
    """Return stable metadata for one built artifact."""

    resolved_path = path.expanduser().resolve()
    return ReleaseArtifactSummary(
        path=str(resolved_path),
        file_name=resolved_path.name,
        size_bytes=resolved_path.stat().st_size,
        sha256=_sha256(resolved_path),
    )


def _resolve_single_artifact(
    *,
    directory: Path,
    prefix: str,
    suffixes: tuple[str, ...],
) -> Path:
    """Resolve exactly one matching artifact from a build output directory."""

    matches = sorted(
        path
        for path in directory.iterdir()
        if path.is_file()
        and path.name.startswith(prefix)
        and any(path.name.endswith(suffix) for suffix in suffixes)
    )
    if not matches:
        raise FileNotFoundError(f"No artifact matched {prefix} in {directory}.")
    if len(matches) > 1:
        joined = ", ".join(path.name for path in matches)
        raise ValueError(f"Expected one artifact for {prefix} in {directory}, found: {joined}")
    return matches[0]


def _raise_on_failure(
    result: subprocess.CompletedProcess[str],
    *,
    command: list[str],
    cwd: Path,
) -> None:
    """Raise a concise error when a packaging command fails."""

    if result.returncode == 0:
        return
    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    detail = stderr or stdout or f"exit code {result.returncode}"
    raise ValueError(f"Packaging command failed in {cwd}: {' '.join(command)}: {detail}")


def release_versions(*, project_root: str | Path | None = None) -> ReleaseVersionState:
    """Return the current coordinated release version state."""

    resolved_project_root = _resolve_project_root_path(project_root)
    resolved_npm_package_dir = _resolve_npm_package_dir_path(project_root=resolved_project_root)
    _ensure_project_layout(resolved_project_root, resolved_npm_package_dir)

    version_path = _version_file_path(resolved_project_root)
    pyproject_path = _pyproject_path(resolved_project_root)
    npm_package_json_path = _npm_package_json_path(resolved_project_root)

    version_file_version = _parse_version_file_version(
        version_path.read_text(encoding="utf-8")
    )
    pyproject_version = _parse_pyproject_version(
        pyproject_path.read_text(encoding="utf-8")
    )
    npm_package = load_npm_package_json(package_dir=resolved_npm_package_dir)
    npm_version = str(npm_package.get("version", ""))
    synchronized = (
        version_file_version == pyproject_version == npm_version
        and version_file_version != ""
    )

    return ReleaseVersionState(
        project_root=str(resolved_project_root),
        version_file_path=str(version_path),
        pyproject_path=str(pyproject_path),
        npm_package_json_path=str(npm_package_json_path),
        version_file_version=version_file_version,
        pyproject_version=pyproject_version,
        npm_version=npm_version,
        synchronized=synchronized,
    )


def sync_release_version(
    version: str,
    *,
    project_root: str | Path | None = None,
) -> ReleaseVersionSyncResponse:
    """Synchronize the coordinated release version files to one version."""

    target_version = _validate_version(version)
    resolved_project_root = _resolve_project_root_path(project_root)
    resolved_npm_package_dir = _resolve_npm_package_dir_path(project_root=resolved_project_root)
    _ensure_project_layout(resolved_project_root, resolved_npm_package_dir)

    version_path = _version_file_path(resolved_project_root)
    pyproject_path = _pyproject_path(resolved_project_root)
    npm_package_json_path = _npm_package_json_path(resolved_project_root)

    version_text = version_path.read_text(encoding="utf-8")
    version_path.write_text(
        _replace_version_file_version(version_text, target_version),
        encoding="utf-8",
    )

    pyproject_text = pyproject_path.read_text(encoding="utf-8")
    pyproject_path.write_text(
        _replace_pyproject_version(pyproject_text, target_version),
        encoding="utf-8",
    )

    npm_package = load_npm_package_json(package_dir=resolved_npm_package_dir)
    npm_package["version"] = target_version
    npm_package_json_path.write_text(
        json.dumps(npm_package, indent=2) + "\n",
        encoding="utf-8",
    )

    version_state = release_versions(project_root=resolved_project_root)
    return ReleaseVersionSyncResponse(
        target_version=target_version,
        updated_files=[
            str(version_path),
            str(pyproject_path),
            str(npm_package_json_path),
        ],
        version_state=version_state,
    )


def _build_python_artifacts(
    *,
    project_root: Path,
    output_dir: Path,
    version_state: ReleaseVersionState,
) -> tuple[Path, Path]:
    """Build the Python wheel and sdist artifacts."""

    command = [*PYTHON_BUILD_COMMAND, "--outdir", str(output_dir)]
    result = _run_command(command, cwd=project_root)
    _raise_on_failure(result, command=command, cwd=project_root)
    wheel = _resolve_single_artifact(
        directory=output_dir,
        prefix=f"{PYTHON_PACKAGE_NAME}-{version_state.pyproject_version}",
        suffixes=(".whl",),
    )
    sdist = _resolve_single_artifact(
        directory=output_dir,
        prefix=f"{PYTHON_PACKAGE_NAME}-{version_state.pyproject_version}",
        suffixes=(".tar.gz", ".zip"),
    )
    return wheel, sdist


def _build_npm_artifact(*, npm_package_dir: Path, output_dir: Path) -> Path:
    """Build the npm tarball and return its resolved artifact path."""

    command = [*NPM_PACK_COMMAND, "--pack-destination", str(output_dir)]
    result = _run_command(command, cwd=npm_package_dir)
    _raise_on_failure(result, command=command, cwd=npm_package_dir)
    payload = json.loads(result.stdout or "[]")
    if not payload:
        raise ValueError("npm pack did not return a tarball payload.")
    filename = payload[0].get("filename")
    if not isinstance(filename, str) or not filename:
        raise ValueError("npm pack did not return a valid tarball filename.")
    artifact_path = output_dir / filename
    if not artifact_path.exists():
        raise FileNotFoundError(f"npm pack did not create {artifact_path}.")
    return artifact_path


def _run_python_install_smoke(
    *,
    wheel_path: Path,
    expected_version: str,
) -> ReleaseInstallSmokeResult:
    """Install the built wheel into a clean venv and invoke the console script."""

    with tempfile.TemporaryDirectory(prefix="ades-wheel-smoke-") as temp_dir:
        working_dir = Path(temp_dir).resolve()
        environment_dir = working_dir / "venv"
        storage_root = working_dir / "storage"

        create_env_command = [sys.executable, "-m", "venv", str(environment_dir)]
        create_env_result = _run_command(create_env_command, cwd=working_dir)
        if create_env_result.returncode != 0:
            install = _build_command_result(create_env_command, create_env_result)
            invoke_command = [str(_resolve_venv_command(environment_dir, "ades")), "status"]
            invoke = _skipped_command_result(
                invoke_command,
                reason="Skipped because virtual environment creation failed.",
            )
            return ReleaseInstallSmokeResult(
                artifact_kind="python_wheel",
                working_dir=str(working_dir),
                environment_dir=str(environment_dir),
                storage_root=str(storage_root),
                install=install,
                invoke=invoke,
                passed=False,
                reported_version=None,
            )

        runtime_python = _resolve_venv_python(environment_dir)
        install_command = [str(runtime_python), "-m", "pip", "install", str(wheel_path)]
        install_result = _run_command_with_env(
            install_command,
            cwd=working_dir,
            env=_build_clean_runtime_env(),
        )
        install = _build_command_result(install_command, install_result)

        invoke_command = [str(_resolve_venv_command(environment_dir, "ades")), "status"]
        if install.passed:
            invoke_result = _run_command_with_env(
                invoke_command,
                cwd=working_dir,
                env=_build_clean_runtime_env(
                    overrides={"ADES_STORAGE_ROOT": str(storage_root)},
                ),
            )
            invoke = _build_command_result(invoke_command, invoke_result)
        else:
            invoke = _skipped_command_result(
                invoke_command,
                reason="Skipped because wheel installation failed.",
            )

        reported_version = _parse_status_version(invoke)
        passed = install.passed and invoke.passed and reported_version == expected_version
        return ReleaseInstallSmokeResult(
            artifact_kind="python_wheel",
            working_dir=str(working_dir),
            environment_dir=str(environment_dir),
            storage_root=str(storage_root),
            install=install,
            invoke=invoke,
            passed=passed,
            reported_version=reported_version,
        )


def _run_npm_install_smoke(
    *,
    wheel_path: Path,
    npm_tarball_path: Path,
    expected_version: str,
) -> ReleaseInstallSmokeResult:
    """Install the npm tarball under a clean prefix and invoke the wrapper."""

    with tempfile.TemporaryDirectory(prefix="ades-npm-smoke-") as temp_dir:
        working_dir = Path(temp_dir).resolve()
        install_prefix = working_dir / "npm-prefix"
        environment_dir = working_dir / "npm-runtime"
        storage_root = working_dir / "storage"

        install_command = [
            "npm",
            "install",
            "--prefix",
            str(install_prefix),
            str(npm_tarball_path),
        ]
        install_result = _run_command_with_env(
            install_command,
            cwd=working_dir,
            env=_build_clean_runtime_env(),
        )
        install = _build_command_result(install_command, install_result)

        invoke_command = [str(_resolve_npm_installed_command(install_prefix, NPM_COMMAND_NAME)), "status"]
        if install.passed:
            invoke_result = _run_command_with_env(
                invoke_command,
                cwd=working_dir,
                env=_build_clean_runtime_env(
                    overrides={
                        "ADES_STORAGE_ROOT": str(storage_root),
                        NPM_PYTHON_BIN_ENV: sys.executable,
                        NPM_PYTHON_PACKAGE_SPEC_ENV: str(wheel_path),
                        NPM_RUNTIME_DIR_ENV: str(environment_dir),
                    },
                ),
            )
            invoke = _build_command_result(invoke_command, invoke_result)
        else:
            invoke = _skipped_command_result(
                invoke_command,
                reason="Skipped because npm tarball installation failed.",
            )

        reported_version = _parse_status_version(invoke)
        passed = install.passed and invoke.passed and reported_version == expected_version
        return ReleaseInstallSmokeResult(
            artifact_kind="npm_tarball",
            working_dir=str(working_dir),
            environment_dir=str(environment_dir),
            storage_root=str(storage_root),
            install=install,
            invoke=invoke,
            passed=passed,
            reported_version=reported_version,
        )


def _smoke_install_warnings(
    smoke_result: ReleaseInstallSmokeResult,
    *,
    expected_version: str,
) -> list[str]:
    """Return deterministic warnings for one smoke-install result."""

    prefix = smoke_result.artifact_kind
    if not smoke_result.install.passed:
        return [f"{prefix}_install_failed:{smoke_result.install.exit_code}"]
    if not smoke_result.invoke.passed:
        return [f"{prefix}_invoke_failed:{smoke_result.invoke.exit_code}"]
    if smoke_result.reported_version is None:
        return [f"{prefix}_missing_reported_version"]
    if smoke_result.reported_version != expected_version:
        return [f"{prefix}_version_mismatch:{smoke_result.reported_version}"]
    return []


def verify_release_artifacts(
    *,
    output_dir: str | Path,
    clean: bool = True,
    smoke_install: bool = True,
    project_root: str | Path | None = None,
) -> ReleaseVerificationResponse:
    """Build and verify Python and npm release artifacts for this checkout."""

    version_state = release_versions(project_root=project_root)
    resolved_project_root = Path(version_state.project_root)
    resolved_npm_package_dir = _resolve_npm_package_dir_path(project_root=resolved_project_root)
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    python_output_dir, npm_output_dir = _prepare_output_dir(resolved_output_dir, clean=clean)

    wheel_path, sdist_path = _build_python_artifacts(
        project_root=resolved_project_root,
        output_dir=python_output_dir,
        version_state=version_state,
    )
    npm_tarball_path = _build_npm_artifact(
        npm_package_dir=resolved_npm_package_dir,
        output_dir=npm_output_dir,
    )

    warnings: list[str] = []
    if not version_state.synchronized:
        warnings.append(
            "version_mismatch:"
            f"version_file={version_state.version_file_version},"
            f"pyproject={version_state.pyproject_version},"
            f"npm={version_state.npm_version}"
        )

    python_install_smoke = (
        _run_python_install_smoke(
            wheel_path=wheel_path,
            expected_version=version_state.version_file_version,
        )
        if smoke_install
        else None
    )
    npm_install_smoke = (
        _run_npm_install_smoke(
            wheel_path=wheel_path,
            npm_tarball_path=npm_tarball_path,
            expected_version=version_state.version_file_version,
        )
        if smoke_install
        else None
    )
    if python_install_smoke is not None:
        warnings.extend(
            _smoke_install_warnings(
                python_install_smoke,
                expected_version=version_state.version_file_version,
            )
        )
    if npm_install_smoke is not None:
        warnings.extend(
            _smoke_install_warnings(
                npm_install_smoke,
                expected_version=version_state.version_file_version,
            )
        )
    smoke_success = (
        True
        if not smoke_install
        else bool(
            python_install_smoke
            and python_install_smoke.passed
            and npm_install_smoke
            and npm_install_smoke.passed
        )
    )

    return ReleaseVerificationResponse(
        project_root=str(resolved_project_root),
        output_dir=str(resolved_output_dir),
        python_package=PYTHON_PACKAGE_NAME,
        python_version=version_state.version_file_version,
        npm_package="ades-cli",
        npm_version=version_state.npm_version,
        version_state=version_state,
        versions_match=version_state.synchronized,
        smoke_install=smoke_install,
        overall_success=version_state.synchronized and smoke_success,
        warnings=warnings,
        wheel=_build_artifact_summary(wheel_path),
        sdist=_build_artifact_summary(sdist_path),
        npm_tarball=_build_artifact_summary(npm_tarball_path),
        python_install_smoke=python_install_smoke,
        npm_install_smoke=npm_install_smoke,
    )


def _default_release_manifest_path(
    *,
    output_dir: Path,
    version: str,
) -> Path:
    """Return the default release-manifest path for one output directory."""

    return (output_dir / f"release-manifest.{version}.json").resolve()


def _build_validation_summary(tests: ReleaseCommandResult) -> ReleaseValidationSummary:
    """Return the persisted validation metadata for one release manifest."""

    return ReleaseValidationSummary(
        validated_at=datetime.now(UTC),
        tests_command=list(tests.command),
        tests_exit_code=tests.exit_code,
        tests_passed=tests.passed,
    )


def _read_release_manifest(manifest_path: str | Path) -> tuple[Path, ReleaseManifestResponse]:
    """Load and validate one persisted release manifest."""

    resolved_manifest_path = Path(manifest_path).expanduser().resolve()
    if not resolved_manifest_path.exists():
        raise FileNotFoundError(f"Release manifest not found: {resolved_manifest_path}")
    try:
        payload = json.loads(resolved_manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Release manifest is not valid JSON: {resolved_manifest_path}") from exc
    return resolved_manifest_path, ReleaseManifestResponse.model_validate(payload)


def _resolve_manifest_artifact(path: str) -> Path:
    """Resolve one manifest-recorded artifact path and ensure it still exists."""

    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Release artifact not found: {resolved_path}")
    return resolved_path


def _build_python_publish_env() -> tuple[dict[str, str], str | None]:
    """Return the child environment and target repository for Python publication."""

    env = _build_clean_runtime_env()
    repository_url = (
        os.environ.get(TWINE_REPOSITORY_URL_ALIAS_ENV)
        or env.get("TWINE_REPOSITORY_URL")
    )
    token = os.environ.get(TWINE_TOKEN_ALIAS_ENV)
    username = os.environ.get(TWINE_USERNAME_ALIAS_ENV)
    password = os.environ.get(TWINE_PASSWORD_ALIAS_ENV)
    if token:
        env["TWINE_USERNAME"] = "__token__"
        env["TWINE_PASSWORD"] = token
    else:
        if username is not None:
            env["TWINE_USERNAME"] = username
        if password is not None:
            env["TWINE_PASSWORD"] = password
    if repository_url:
        env["TWINE_REPOSITORY_URL"] = repository_url
    return env, repository_url


def _build_npm_publish_env() -> tuple[dict[str, str], str | None, str | None]:
    """Return the child environment, registry, and access level for npm publication."""

    env = _build_clean_runtime_env()
    registry = os.environ.get(NPM_REGISTRY_ALIAS_ENV) or env.get("NPM_CONFIG_REGISTRY")
    token = os.environ.get(NPM_TOKEN_ALIAS_ENV) or env.get("NODE_AUTH_TOKEN")
    access = os.environ.get(NPM_ACCESS_ALIAS_ENV)
    if registry:
        env["NPM_CONFIG_REGISTRY"] = registry
    if token:
        env["NODE_AUTH_TOKEN"] = token
    return env, registry, access


def write_release_manifest(
    *,
    output_dir: str | Path,
    manifest_path: str | Path | None = None,
    version: str | None = None,
    clean: bool = True,
    smoke_install: bool = True,
    validation: ReleaseValidationSummary | None = None,
    project_root: str | Path | None = None,
) -> ReleaseManifestResponse:
    """Synchronize versions if requested, verify artifacts, and persist a release manifest."""

    version_sync = (
        sync_release_version(version, project_root=project_root)
        if version is not None
        else None
    )
    verification = verify_release_artifacts(
        output_dir=output_dir,
        clean=clean,
        smoke_install=smoke_install,
        project_root=project_root,
    )
    release_version = verification.version_state.version_file_version
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_manifest_path = (
        Path(manifest_path).expanduser().resolve()
        if manifest_path is not None
        else _default_release_manifest_path(output_dir=resolved_output_dir, version=release_version)
    )
    resolved_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    response = ReleaseManifestResponse(
        manifest_path=str(resolved_manifest_path),
        generated_at=datetime.now(UTC),
        release_version=release_version,
        version_state=verification.version_state,
        version_sync=version_sync,
        validation=validation,
        verification=verification,
    )
    resolved_manifest_path.write_text(
        json.dumps(response.model_dump(mode="json"), indent=2) + "\n",
        encoding="utf-8",
    )
    return response


def validate_release_workflow(
    *,
    output_dir: str | Path,
    manifest_path: str | Path | None = None,
    version: str | None = None,
    clean: bool = True,
    smoke_install: bool = True,
    tests_command: Sequence[str] | None = None,
    project_root: str | Path | None = None,
) -> ReleaseValidationResponse:
    """Run the local test suite, then build and persist the release manifest."""

    resolved_project_root = _resolve_project_root_path(project_root)
    normalized_tests_command = _normalize_release_test_command(tests_command)
    tests_result = _run_command(normalized_tests_command, cwd=resolved_project_root)
    tests = ReleaseCommandResult(
        command=normalized_tests_command,
        exit_code=tests_result.returncode,
        passed=tests_result.returncode == 0,
        stdout=tests_result.stdout or "",
        stderr=tests_result.stderr or "",
    )
    warnings: list[str] = []
    manifest: ReleaseManifestResponse | None = None
    manifest_output_path: str | None = None
    if tests.passed:
        manifest = write_release_manifest(
            output_dir=output_dir,
            manifest_path=manifest_path,
            version=version,
            clean=clean,
            smoke_install=smoke_install,
            validation=_build_validation_summary(tests),
            project_root=resolved_project_root,
        )
        manifest_output_path = manifest.manifest_path
        warnings.extend(manifest.verification.warnings)
    else:
        warnings.append(f"tests_failed:{tests.exit_code}")
    overall_success = tests.passed and manifest is not None and manifest.verification.overall_success
    return ReleaseValidationResponse(
        project_root=str(resolved_project_root),
        output_dir=str(Path(output_dir).expanduser().resolve()),
        tests=tests,
        manifest=manifest,
        manifest_path=manifest_output_path,
        overall_success=overall_success,
        warnings=warnings,
    )


def publish_release_manifest(
    *,
    manifest_path: str | Path,
    dry_run: bool = False,
) -> ReleasePublishResponse:
    """Publish one validated release manifest to Python and npm registries."""

    resolved_manifest_path, manifest = _read_release_manifest(manifest_path)
    if manifest.validation is None or not manifest.validation.tests_passed:
        raise ValueError(
            "Release publish requires a manifest created by `ades release validate`."
        )
    if not manifest.verification.overall_success:
        raise ValueError(
            "Release publish requires a manifest with successful release verification."
        )

    project_root = Path(manifest.verification.project_root).expanduser().resolve()
    wheel_path = _resolve_manifest_artifact(manifest.verification.wheel.path)
    sdist_path = _resolve_manifest_artifact(manifest.verification.sdist.path)
    npm_tarball_path = _resolve_manifest_artifact(manifest.verification.npm_tarball.path)

    python_env, python_registry = _build_python_publish_env()
    python_command = [*PYTHON_PUBLISH_COMMAND]
    if python_registry:
        python_command.extend(["--repository-url", python_registry])
    python_command.extend([str(wheel_path), str(sdist_path)])
    if dry_run:
        python_publish_command = _dry_run_command_result(
            python_command,
            detail="Dry run: skipped Python publish.",
        )
        python_executed = False
    else:
        python_result = _run_command_with_env(
            python_command,
            cwd=project_root,
            env=python_env,
        )
        python_publish_command = _build_command_result(python_command, python_result)
        python_executed = True
    python_publish = ReleasePublishTargetResult(
        artifact_paths=[str(wheel_path), str(sdist_path)],
        registry=python_registry,
        command=python_publish_command,
        executed=python_executed,
        passed=python_publish_command.passed,
    )

    npm_env, npm_registry, npm_access = _build_npm_publish_env()
    npm_command = [*NPM_PUBLISH_COMMAND, str(npm_tarball_path)]
    if npm_registry:
        npm_command.extend(["--registry", npm_registry])
    if npm_access:
        npm_command.extend(["--access", npm_access])
    if dry_run:
        npm_publish_command = _dry_run_command_result(
            npm_command,
            detail="Dry run: skipped npm publish.",
        )
        npm_executed = False
    elif not python_publish.passed:
        npm_publish_command = _skipped_command_result(
            npm_command,
            reason="Skipped because Python publish failed.",
        )
        npm_executed = False
    else:
        npm_result = _run_command_with_env(
            npm_command,
            cwd=project_root,
            env=npm_env,
        )
        npm_publish_command = _build_command_result(npm_command, npm_result)
        npm_executed = True
    npm_publish = ReleasePublishTargetResult(
        artifact_paths=[str(npm_tarball_path)],
        registry=npm_registry,
        command=npm_publish_command,
        executed=npm_executed,
        passed=npm_publish_command.passed,
    )

    warnings: list[str] = []
    if not python_publish.passed:
        warnings.append(f"python_publish_failed:{python_publish.command.exit_code}")
    if not dry_run:
        if python_publish.passed and not npm_publish.passed:
            warnings.append(f"npm_publish_failed:{npm_publish.command.exit_code}")
        elif not python_publish.passed:
            warnings.append("npm_publish_skipped:python_publish_failed")

    return ReleasePublishResponse(
        manifest_path=str(resolved_manifest_path),
        release_version=manifest.release_version,
        dry_run=dry_run,
        validated_manifest=True,
        python_publish=python_publish,
        npm_publish=npm_publish,
        overall_success=python_publish.passed and npm_publish.passed,
        warnings=warnings,
    )
