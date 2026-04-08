"""Release version coordination, artifact verification, and manifest helpers."""

from __future__ import annotations

from datetime import datetime, UTC
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

from .distribution import load_npm_package_json, resolve_npm_package_dir, resolve_project_root
from .service.models import (
    ReleaseArtifactSummary,
    ReleaseManifestResponse,
    ReleaseVerificationResponse,
    ReleaseVersionState,
    ReleaseVersionSyncResponse,
)


PYTHON_PACKAGE_NAME = "ades"
PYTHON_BUILD_COMMAND = [sys.executable, "-m", "build", "--wheel", "--sdist"]
NPM_PACK_COMMAND = ["npm", "pack", "--json"]
VERSION_FILE_RELATIVE_PATH = Path("src/ades/version.py")
PYPROJECT_RELATIVE_PATH = Path("pyproject.toml")
NPM_PACKAGE_JSON_RELATIVE_PATH = Path("npm/ades-cli/package.json")
VERSION_ASSIGNMENT_PATTERN = re.compile(r'(__version__\s*=\s*")([^"]+)(")')
PYPROJECT_VERSION_PATTERN = re.compile(r'(?m)^(version\s*=\s*")([^"]+)(")$')


def _run_command(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run one packaging command and capture structured text output."""

    return subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )


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


def verify_release_artifacts(
    *,
    output_dir: str | Path,
    clean: bool = True,
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

    return ReleaseVerificationResponse(
        project_root=str(resolved_project_root),
        output_dir=str(resolved_output_dir),
        python_package=PYTHON_PACKAGE_NAME,
        python_version=version_state.version_file_version,
        npm_package="ades-cli",
        npm_version=version_state.npm_version,
        version_state=version_state,
        versions_match=version_state.synchronized,
        overall_success=version_state.synchronized,
        warnings=warnings,
        wheel=_build_artifact_summary(wheel_path),
        sdist=_build_artifact_summary(sdist_path),
        npm_tarball=_build_artifact_summary(npm_tarball_path),
    )


def _default_release_manifest_path(
    *,
    output_dir: Path,
    version: str,
) -> Path:
    """Return the default release-manifest path for one output directory."""

    return (output_dir / f"release-manifest.{version}.json").resolve()


def write_release_manifest(
    *,
    output_dir: str | Path,
    manifest_path: str | Path | None = None,
    version: str | None = None,
    clean: bool = True,
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
        verification=verification,
    )
    resolved_manifest_path.write_text(
        json.dumps(response.model_dump(mode="json"), indent=2) + "\n",
        encoding="utf-8",
    )
    return response
