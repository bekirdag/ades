"""Release artifact build and verification helpers."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

from .distribution import load_npm_package_json, resolve_npm_package_dir, resolve_project_root
from .service.models import ReleaseArtifactSummary, ReleaseVerificationResponse
from .version import __version__


PYTHON_PACKAGE_NAME = "ades"
PYTHON_BUILD_COMMAND = [sys.executable, "-m", "build", "--wheel", "--sdist"]
NPM_PACK_COMMAND = ["npm", "pack", "--json"]


def _run_command(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run one packaging command and capture structured text output."""

    return subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )


def _ensure_project_layout(project_root: Path, npm_package_dir: Path) -> None:
    """Validate that the expected source checkout layout is present."""

    pyproject_path = project_root / "pyproject.toml"
    if not pyproject_path.exists():
        raise FileNotFoundError(
            f"Release verification requires pyproject.toml under {project_root}."
        )
    package_json_path = npm_package_dir / "package.json"
    if not package_json_path.exists():
        raise FileNotFoundError(
            f"Release verification requires package.json under {npm_package_dir}."
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
    raise ValueError(
        f"Packaging command failed in {cwd}: {' '.join(command)}: {detail}"
    )


def _build_python_artifacts(*, project_root: Path, output_dir: Path) -> tuple[Path, Path]:
    """Build the Python wheel and sdist artifacts."""

    command = [*PYTHON_BUILD_COMMAND, "--outdir", str(output_dir)]
    result = _run_command(command, cwd=project_root)
    _raise_on_failure(result, command=command, cwd=project_root)
    wheel = _resolve_single_artifact(
        directory=output_dir,
        prefix=f"{PYTHON_PACKAGE_NAME}-{__version__}",
        suffixes=(".whl",),
    )
    sdist = _resolve_single_artifact(
        directory=output_dir,
        prefix=f"{PYTHON_PACKAGE_NAME}-{__version__}",
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
) -> ReleaseVerificationResponse:
    """Build and verify Python and npm release artifacts for this checkout."""

    project_root = resolve_project_root().resolve()
    npm_package_dir = resolve_npm_package_dir().resolve()
    _ensure_project_layout(project_root, npm_package_dir)
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    python_output_dir, npm_output_dir = _prepare_output_dir(resolved_output_dir, clean=clean)

    npm_package = load_npm_package_json(package_dir=npm_package_dir)
    npm_package_name = str(npm_package.get("name", ""))
    npm_version = str(npm_package.get("version", ""))

    wheel_path, sdist_path = _build_python_artifacts(
        project_root=project_root,
        output_dir=python_output_dir,
    )
    npm_tarball_path = _build_npm_artifact(
        npm_package_dir=npm_package_dir,
        output_dir=npm_output_dir,
    )

    versions_match = npm_version == __version__
    warnings: list[str] = []
    if not versions_match:
        warnings.append(f"version_mismatch:python={__version__},npm={npm_version}")

    return ReleaseVerificationResponse(
        project_root=str(project_root),
        output_dir=str(resolved_output_dir),
        python_package=PYTHON_PACKAGE_NAME,
        python_version=__version__,
        npm_package=npm_package_name,
        npm_version=npm_version,
        versions_match=versions_match,
        overall_success=versions_match,
        warnings=warnings,
        wheel=_build_artifact_summary(wheel_path),
        sdist=_build_artifact_summary(sdist_path),
        npm_tarball=_build_artifact_summary(npm_tarball_path),
    )
