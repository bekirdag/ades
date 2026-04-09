"""Release version coordination, artifact verification, and manifest helpers."""

from __future__ import annotations

from datetime import datetime, UTC
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from typing import Sequence
import urllib.error
import urllib.request

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
SMOKE_DEPENDENCY_PACK_ID = "general-en"
SMOKE_PULL_PACK_ID = "finance-en"
SMOKE_EXPECTED_PACK_IDS = ("general-en", "finance-en")
SMOKE_REMOVE_REMAINING_PACK_IDS = (SMOKE_DEPENDENCY_PACK_ID,)
SMOKE_REMOVE_GUARDRAIL_MESSAGE = (
    f"Cannot remove pack {SMOKE_DEPENDENCY_PACK_ID} while installed dependent packs exist: "
    f"{SMOKE_PULL_PACK_ID}"
)
SMOKE_TAG_TEXT = "Apple said AAPL traded on NASDAQ after USD 12.5 guidance."
SMOKE_TAG_FILE_NAME = "serve-smoke-input.html"
SMOKE_TAG_FILE_CONTENT = "<p>Apple said AAPL traded on NASDAQ after USD 12.5 guidance.</p>\n"
SMOKE_TAG_BATCH_FILES = (
    ("serve-smoke-batch-alpha.html", "<p>Apple said AAPL rallied.</p>\n"),
    ("serve-smoke-batch-beta.html", "<p>NASDAQ closed near USD 12.5.</p>\n"),
)
SMOKE_TAG_BATCH_OUTPUT_DIR_NAME = "serve-smoke-batch-outputs"
SMOKE_TAG_BATCH_MANIFEST_FILE_NAME = (
    f"serve-smoke-batch-manifest.{SMOKE_PULL_PACK_ID}.ades-manifest.json"
)
SMOKE_TAG_BATCH_REPLAY_MANIFEST_FILE_NAME = (
    f"serve-smoke-batch-replay.{SMOKE_PULL_PACK_ID}.ades-manifest.json"
)
SMOKE_TAG_BATCH_REPLAY_MODE = "processed"
SMOKE_TAG_REQUIRED_LABELS = ("organization", "ticker", "exchange", "currency_amount")
SMOKE_SERVE_HOST = "127.0.0.1"
SMOKE_SERVE_STARTUP_TIMEOUT_SECONDS = 20.0
SMOKE_SERVE_POLL_INTERVAL_SECONDS = 0.2


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


def _parse_last_json_object(output: str) -> dict[str, object] | None:
    """Return the last JSON object found in one command stdout stream."""

    text = output.strip()
    if not text:
        return None
    decoder = json.JSONDecoder()
    payload: dict[str, object] | None = None
    payload_end = -1
    for index, character in enumerate(text):
        if character != "{":
            continue
        try:
            candidate, consumed = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        end_index = index + consumed
        if isinstance(candidate, dict) and end_index >= payload_end:
            payload = candidate
            payload_end = end_index
    return payload


def _parse_command_payload(result: ReleaseCommandResult) -> dict[str, object] | None:
    """Return the JSON payload emitted by one successful command when present."""

    if not result.passed:
        return None
    return _parse_last_json_object(result.stdout or "")


def _parse_status_version(result: ReleaseCommandResult) -> str | None:
    """Extract the reported ades version from a JSON `ades status` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return None
    version = payload.get("version")
    return version if isinstance(version, str) and version else None


def _parse_pull_pack_ids(result: ReleaseCommandResult) -> list[str]:
    """Extract pack ids reported by one JSON `ades pull` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return []
    pack_ids: list[str] = []
    for field_name in ("installed", "skipped"):
        values = payload.get(field_name)
        if not isinstance(values, list):
            continue
        for value in values:
            if isinstance(value, str) and value:
                pack_ids.append(value)
    return list(dict.fromkeys(pack_ids))


def _parse_tag_labels(result: ReleaseCommandResult) -> list[str]:
    """Extract entity labels reported by one JSON `ades tag` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return []
    entities = payload.get("entities")
    if not isinstance(entities, list):
        return []
    labels: list[str] = []
    for item in entities:
        if not isinstance(item, dict):
            continue
        label = item.get("label")
        if isinstance(label, str) and label:
            labels.append(label)
    return list(dict.fromkeys(labels))


def _parse_batch_tag_labels(result: ReleaseCommandResult) -> list[str]:
    """Extract entity labels reported by one JSON `ades tag-files` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return []
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    labels: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        entities = item.get("entities")
        if not isinstance(entities, list):
            continue
        for entity in entities:
            if not isinstance(entity, dict):
                continue
            label = entity.get("label")
            if isinstance(label, str) and label:
                labels.append(label)
    return list(dict.fromkeys(labels))


def _parse_batch_saved_manifest_path(result: ReleaseCommandResult) -> str | None:
    """Extract the saved manifest path reported by one JSON `ades tag-files` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return None
    saved_manifest_path = payload.get("saved_manifest_path")
    return (
        saved_manifest_path
        if isinstance(saved_manifest_path, str) and saved_manifest_path
        else None
    )


def _parse_batch_saved_output_paths(result: ReleaseCommandResult) -> list[str]:
    """Extract saved output paths reported by one JSON `ades tag-files` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return []
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    output_paths: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        saved_output_path = item.get("saved_output_path")
        if isinstance(saved_output_path, str) and saved_output_path:
            output_paths.append(saved_output_path)
    return output_paths


def _parse_batch_pack_id(result: ReleaseCommandResult) -> str | None:
    """Extract the top-level pack id reported by one JSON `ades tag-files` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return None
    pack = payload.get("pack")
    return pack if isinstance(pack, str) and pack else None


def _parse_batch_item_count(result: ReleaseCommandResult) -> int | None:
    """Extract the top-level item count reported by one JSON `ades tag-files` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return None
    item_count = payload.get("item_count")
    return item_count if isinstance(item_count, int) and not isinstance(item_count, bool) else None


def _parse_batch_summary_value(
    result: ReleaseCommandResult,
    field_name: str,
) -> str | None:
    """Extract one string summary value from a JSON `ades tag-files` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return None
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return None
    value = summary.get(field_name)
    return value if isinstance(value, str) and value else None


def _parse_batch_summary_int_value(
    result: ReleaseCommandResult,
    field_name: str,
) -> int | None:
    """Extract one integer summary value from a JSON `ades tag-files` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return None
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return None
    value = summary.get(field_name)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _parse_batch_manifest_input_path(result: ReleaseCommandResult) -> str | None:
    """Extract the manifest input path reported by one JSON `ades tag-files` payload."""

    return _parse_batch_summary_value(result, "manifest_input_path")


def _parse_batch_manifest_replay_mode(result: ReleaseCommandResult) -> str | None:
    """Extract the manifest replay mode reported by one JSON `ades tag-files` payload."""

    return _parse_batch_summary_value(result, "manifest_replay_mode")


def _parse_batch_manifest_candidate_count(result: ReleaseCommandResult) -> int | None:
    """Extract the manifest candidate count reported by one JSON `ades tag-files` payload."""

    return _parse_batch_summary_int_value(result, "manifest_candidate_count")


def _parse_batch_manifest_selected_count(result: ReleaseCommandResult) -> int | None:
    """Extract the manifest selected count reported by one JSON `ades tag-files` payload."""

    return _parse_batch_summary_int_value(result, "manifest_selected_count")


def _parse_batch_rerun_diff(result: ReleaseCommandResult) -> dict[str, object] | None:
    """Extract the rerun-diff payload reported by one JSON `ades tag-files` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return None
    rerun_diff = payload.get("rerun_diff")
    return rerun_diff if isinstance(rerun_diff, dict) else None


def _parse_batch_rerun_diff_manifest_input_path(
    result: ReleaseCommandResult,
) -> str | None:
    """Extract the rerun-diff manifest input path from one JSON `ades tag-files` payload."""

    rerun_diff = _parse_batch_rerun_diff(result)
    if rerun_diff is None:
        return None
    manifest_input_path = rerun_diff.get("manifest_input_path")
    return (
        manifest_input_path
        if isinstance(manifest_input_path, str) and manifest_input_path
        else None
    )


def _parse_batch_rerun_diff_list(
    result: ReleaseCommandResult,
    field_name: str,
) -> list[object] | None:
    """Extract one rerun-diff list field from a JSON `ades tag-files` payload."""

    rerun_diff = _parse_batch_rerun_diff(result)
    if rerun_diff is None:
        return None
    values = rerun_diff.get(field_name)
    return values if isinstance(values, list) else None


def _parse_batch_rerun_diff_path_list(
    result: ReleaseCommandResult,
    field_name: str,
) -> list[str] | None:
    """Extract one rerun-diff path list from a JSON `ades tag-files` payload."""

    values = _parse_batch_rerun_diff_list(result, field_name)
    if values is None:
        return None
    paths: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value:
            return None
        paths.append(value)
    return paths


def _parse_batch_lineage_value(
    result: ReleaseCommandResult,
    field_name: str,
) -> str | None:
    """Extract one string lineage value from a JSON `ades tag-files` payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return None
    lineage = payload.get("lineage")
    if not isinstance(lineage, dict):
        return None
    value = lineage.get(field_name)
    return value if isinstance(value, str) and value else None


def _parse_batch_lineage_source_manifest_path(result: ReleaseCommandResult) -> str | None:
    """Extract the lineage source manifest path from a JSON `ades tag-files` payload."""

    return _parse_batch_lineage_value(result, "source_manifest_path")


def _parse_batch_lineage_run_id(result: ReleaseCommandResult) -> str | None:
    """Extract the lineage run id from a JSON `ades tag-files` payload."""

    return _parse_batch_lineage_value(result, "run_id")


def _parse_batch_lineage_root_run_id(result: ReleaseCommandResult) -> str | None:
    """Extract the lineage root run id from a JSON `ades tag-files` payload."""

    return _parse_batch_lineage_value(result, "root_run_id")


def _parse_batch_lineage_parent_run_id(result: ReleaseCommandResult) -> str | None:
    """Extract the lineage parent run id from a JSON `ades tag-files` payload."""

    return _parse_batch_lineage_value(result, "parent_run_id")


def _parse_iso8601_datetime(value: str) -> datetime | None:
    """Parse one timezone-aware ISO 8601 datetime string."""

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _parse_batch_lineage_created_at(result: ReleaseCommandResult) -> datetime | None:
    """Extract the lineage creation timestamp from a JSON `ades tag-files` payload."""

    value = _parse_batch_lineage_value(result, "created_at")
    if value is None:
        return None
    return _parse_iso8601_datetime(value)


def _batch_lineage_field_is_unset(
    result: ReleaseCommandResult,
    field_name: str,
) -> bool:
    """Return whether one lineage field is absent or explicitly null."""

    payload = _parse_command_payload(result)
    if payload is None:
        return False
    lineage = payload.get("lineage")
    if not isinstance(lineage, dict):
        return False
    return field_name not in lineage or lineage.get(field_name) is None


def _parse_status_installed_pack_ids(result: ReleaseCommandResult) -> list[str]:
    """Extract installed pack ids reported by one JSON status payload."""

    payload = _parse_command_payload(result)
    if payload is None:
        return []
    installed_packs = payload.get("installed_packs")
    if not isinstance(installed_packs, list):
        return []
    return [str(value) for value in installed_packs if isinstance(value, str) and value]


def _healthz_matches_expected(
    result: ReleaseCommandResult,
    *,
    expected_version: str,
) -> bool:
    """Return whether one healthz response reported the expected service state."""

    payload = _parse_command_payload(result)
    if payload is None:
        return False
    return payload.get("status") == "ok" and payload.get("version") == expected_version


def _reserve_loopback_port(host: str) -> int:
    """Return one currently-free loopback TCP port."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind((host, 0))
        return int(handle.getsockname()[1])


def _probe_http_endpoint(url: str) -> ReleaseCommandResult:
    """Fetch one localhost endpoint and capture the response as a command result."""

    command = ["GET", url]
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=2.0) as response:
            body = response.read().decode("utf-8", errors="replace")
            return ReleaseCommandResult(
                command=command,
                exit_code=0 if 200 <= response.status < 300 else int(response.status),
                passed=200 <= response.status < 300,
                stdout=body,
                stderr="",
            )
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return ReleaseCommandResult(
            command=command,
            exit_code=int(exc.code),
            passed=False,
            stdout=body,
            stderr=str(exc),
        )
    except Exception as exc:
        return ReleaseCommandResult(
            command=command,
            exit_code=1,
            passed=False,
            stdout="",
            stderr=str(exc),
        )


def _post_http_json_endpoint(url: str, payload: object) -> ReleaseCommandResult:
    """POST one JSON payload to a localhost endpoint and capture the response."""

    command = ["POST", url]
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=2.0) as response:
            body = response.read().decode("utf-8", errors="replace")
            return ReleaseCommandResult(
                command=command,
                exit_code=0 if 200 <= response.status < 300 else int(response.status),
                passed=200 <= response.status < 300,
                stdout=body,
                stderr="",
            )
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return ReleaseCommandResult(
            command=command,
            exit_code=int(exc.code),
            passed=False,
            stdout=body,
            stderr=str(exc),
        )
    except Exception as exc:
        return ReleaseCommandResult(
            command=command,
            exit_code=1,
            passed=False,
            stdout="",
            stderr=str(exc),
        )


def _clear_registry_metadata_files(storage_root: Path) -> None:
    """Remove local SQLite registry files so recovery can bootstrap from pack manifests."""

    registry_dir = storage_root / "registry"
    for file_name in ("ades.db", "ades.db-shm", "ades.db-wal"):
        metadata_path = registry_dir / file_name
        if metadata_path.exists():
            metadata_path.unlink()


def _missing_smoke_pack_ids(pack_ids: Sequence[str]) -> list[str]:
    """Return the required smoke pack IDs missing from one installed-pack list."""

    available = set(pack_ids)
    return [pack_id for pack_id in SMOKE_EXPECTED_PACK_IDS if pack_id not in available]


def _format_smoke_pack_ids(pack_ids: Sequence[str]) -> str:
    """Return one stable warning fragment for a pack-id collection."""

    return ",".join(pack_ids) if pack_ids else "none"


def _write_smoke_input_file(working_dir: Path) -> Path:
    """Persist one deterministic local file used by live service smoke requests."""

    input_path = working_dir / SMOKE_TAG_FILE_NAME
    input_path.write_text(SMOKE_TAG_FILE_CONTENT, encoding="utf-8")
    return input_path.resolve()


def _write_batch_smoke_input_files(working_dir: Path) -> list[Path]:
    """Persist deterministic local files used by the live batch smoke request."""

    paths: list[Path] = []
    for file_name, content in SMOKE_TAG_BATCH_FILES:
        input_path = working_dir / file_name
        input_path.write_text(content, encoding="utf-8")
        paths.append(input_path.resolve())
    return paths


def _smoke_batch_output_dir(working_dir: Path) -> Path:
    return (working_dir / SMOKE_TAG_BATCH_OUTPUT_DIR_NAME).resolve()


def _expected_smoke_batch_manifest_path(working_dir: Path, *, replay: bool = False) -> str:
    file_name = (
        SMOKE_TAG_BATCH_REPLAY_MANIFEST_FILE_NAME
        if replay
        else SMOKE_TAG_BATCH_MANIFEST_FILE_NAME
    )
    return str((_smoke_batch_output_dir(working_dir) / file_name).resolve())


def _expected_smoke_batch_output_paths(working_dir: Path) -> list[str]:
    output_dir = _smoke_batch_output_dir(working_dir)
    return [
        str((output_dir / f"{Path(file_name).stem}.{SMOKE_PULL_PACK_ID}.ades.json").resolve())
        for file_name, _ in SMOKE_TAG_BATCH_FILES
    ]


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


def _run_cli_runtime_smoke(
    *,
    executable: Path,
    working_dir: Path,
    storage_root: Path,
    extra_env: dict[str, str] | None = None,
) -> tuple[
    ReleaseCommandResult,
    ReleaseCommandResult,
    ReleaseCommandResult,
    str | None,
    list[str],
    list[str],
]:
    """Run clean-environment status, pull, and tag checks for one installed CLI."""

    env_overrides = {"ADES_STORAGE_ROOT": str(storage_root)}
    if extra_env:
        env_overrides.update(extra_env)
    smoke_env = _build_clean_runtime_env(overrides=env_overrides)

    status_command = [str(executable), "status"]
    status_result = _run_command_with_env(status_command, cwd=working_dir, env=smoke_env)
    status = _build_command_result(status_command, status_result)
    reported_version = _parse_status_version(status)

    pull_command = [str(executable), "pull", SMOKE_PULL_PACK_ID]
    if status.passed:
        pull_result = _run_command_with_env(pull_command, cwd=working_dir, env=smoke_env)
        pull = _build_command_result(pull_command, pull_result)
    else:
        pull = _skipped_command_result(
            pull_command,
            reason="Skipped because status invocation failed.",
        )

    tag_command = [str(executable), "tag", SMOKE_TAG_TEXT, "--pack", SMOKE_PULL_PACK_ID]
    if pull.passed:
        tag_result = _run_command_with_env(tag_command, cwd=working_dir, env=smoke_env)
        tag = _build_command_result(tag_command, tag_result)
    else:
        tag = _skipped_command_result(
            tag_command,
            reason="Skipped because pack pull failed.",
        )

    return (
        status,
        pull,
        tag,
        reported_version,
        _parse_pull_pack_ids(pull),
        _parse_tag_labels(tag),
    )


def _run_cli_recovery_status_smoke(
    *,
    executable: Path,
    working_dir: Path,
    storage_root: Path,
    expected_version: str,
    extra_env: dict[str, str] | None = None,
) -> tuple[ReleaseCommandResult, list[str]]:
    """Delete local metadata and confirm `ades status` bootstraps from on-disk packs."""

    _clear_registry_metadata_files(storage_root)
    env_overrides = {"ADES_STORAGE_ROOT": str(storage_root)}
    if extra_env:
        env_overrides.update(extra_env)
    smoke_env = _build_clean_runtime_env(overrides=env_overrides)
    status_command = [str(executable), "status"]
    status_result = _run_command_with_env(status_command, cwd=working_dir, env=smoke_env)
    status = _build_command_result(status_command, status_result)
    return status, _parse_status_installed_pack_ids(status)


def _run_cli_remove_smoke(
    *,
    executable: Path,
    working_dir: Path,
    storage_root: Path,
    expected_version: str,
    extra_env: dict[str, str] | None = None,
) -> tuple[
    ReleaseCommandResult,
    ReleaseCommandResult,
    ReleaseCommandResult,
    list[str],
]:
    """Prove dependency-removal guardrails and final remaining-pack state."""

    env_overrides = {"ADES_STORAGE_ROOT": str(storage_root)}
    if extra_env:
        env_overrides.update(extra_env)
    smoke_env = _build_clean_runtime_env(overrides=env_overrides)
    remove_guardrail_command = [
        str(executable),
        "packs",
        "remove",
        SMOKE_DEPENDENCY_PACK_ID,
    ]
    remove_guardrail_result = _run_command_with_env(
        remove_guardrail_command,
        cwd=working_dir,
        env=smoke_env,
    )
    remove_guardrail = _build_command_result(
        remove_guardrail_command,
        remove_guardrail_result,
    )
    remove_command = [str(executable), "packs", "remove", SMOKE_PULL_PACK_ID]
    remove_status_command = [str(executable), "status"]
    if (
        remove_guardrail.passed
        or SMOKE_REMOVE_GUARDRAIL_MESSAGE not in remove_guardrail.stderr
    ):
        reason = "Skipped because dependency-removal guardrail did not fail as expected."
        return (
            remove_guardrail,
            _skipped_command_result(remove_command, reason=reason),
            _skipped_command_result(remove_status_command, reason=reason),
            [],
        )
    remove_result = _run_command_with_env(remove_command, cwd=working_dir, env=smoke_env)
    remove = _build_command_result(remove_command, remove_result)
    if not remove.passed:
        return (
            remove_guardrail,
            remove,
            _skipped_command_result(
                remove_status_command,
                reason="Skipped because pack removal failed.",
            ),
            [],
        )
    remove_status_result = _run_command_with_env(
        remove_status_command,
        cwd=working_dir,
        env=smoke_env,
    )
    remove_status = _build_command_result(remove_status_command, remove_status_result)
    return (
        remove_guardrail,
        remove,
        remove_status,
        _parse_status_installed_pack_ids(remove_status),
    )


def _run_cli_service_smoke(
    *,
    executable: Path,
    working_dir: Path,
    storage_root: Path,
    expected_version: str,
    extra_env: dict[str, str] | None = None,
) -> tuple[
    ReleaseCommandResult,
    ReleaseCommandResult,
    ReleaseCommandResult,
    ReleaseCommandResult,
    ReleaseCommandResult,
    ReleaseCommandResult,
    ReleaseCommandResult,
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
]:
    """Run one clean-environment `ades serve` smoke check and probe its endpoints."""

    env_overrides = {"ADES_STORAGE_ROOT": str(storage_root)}
    if extra_env:
        env_overrides.update(extra_env)
    smoke_env = _build_clean_runtime_env(overrides=env_overrides)

    port = _reserve_loopback_port(SMOKE_SERVE_HOST)
    serve_command = [
        str(executable),
        "serve",
        "--host",
        SMOKE_SERVE_HOST,
        "--port",
        str(port),
    ] 
    healthz_url = f"http://{SMOKE_SERVE_HOST}:{port}/healthz"
    status_url = f"http://{SMOKE_SERVE_HOST}:{port}/v0/status"
    tag_url = f"http://{SMOKE_SERVE_HOST}:{port}/v0/tag"
    tag_file_url = f"http://{SMOKE_SERVE_HOST}:{port}/v0/tag/file"
    tag_files_url = f"http://{SMOKE_SERVE_HOST}:{port}/v0/tag/files"
    smoke_input_path = _write_smoke_input_file(working_dir)
    smoke_batch_input_paths = _write_batch_smoke_input_files(working_dir)
    smoke_batch_output_dir = _smoke_batch_output_dir(working_dir)
    smoke_batch_manifest_path = Path(
        _expected_smoke_batch_manifest_path(working_dir)
    ).resolve()
    smoke_batch_replay_manifest_path = Path(
        _expected_smoke_batch_manifest_path(working_dir, replay=True)
    ).resolve()
    process = subprocess.Popen(
        serve_command,
        cwd=working_dir,
        env=smoke_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    serve_healthz = ReleaseCommandResult(
        command=["GET", healthz_url],
        exit_code=1,
        passed=False,
        stdout="",
        stderr="Service did not become ready before the health probe completed.",
    )
    serve_status = _skipped_command_result(
        ["GET", status_url],
        reason="Skipped because service startup did not pass the health probe.",
    )
    serve_tag = _skipped_command_result(
        ["POST", tag_url],
        reason="Skipped because service startup did not pass the health probe.",
    )
    serve_tag_file = _skipped_command_result(
        ["POST", tag_file_url],
        reason="Skipped because service startup did not pass the health probe.",
    )
    serve_tag_files = _skipped_command_result(
        ["POST", tag_files_url],
        reason="Skipped because service startup did not pass the health probe.",
    )
    serve_tag_files_replay = _skipped_command_result(
        ["POST", tag_files_url],
        reason="Skipped because service startup did not pass the health probe.",
    )
    started = False
    serve_stdout = ""
    serve_stderr = ""
    serve_tagged_labels: list[str] = []
    serve_tag_file_labels: list[str] = []
    serve_tag_files_labels: list[str] = []
    serve_tag_files_replay_labels: list[str] = []
    try:
        deadline = time.monotonic() + SMOKE_SERVE_STARTUP_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            serve_healthz = _probe_http_endpoint(healthz_url)
            if serve_healthz.passed:
                started = True
                break
            if process.poll() is not None:
                break
            time.sleep(SMOKE_SERVE_POLL_INTERVAL_SECONDS)
        if started:
            serve_status = _probe_http_endpoint(status_url)
            if serve_status.passed:
                serve_tag = _post_http_json_endpoint(
                    tag_url,
                    {
                        "text": SMOKE_TAG_TEXT,
                        "pack": SMOKE_PULL_PACK_ID,
                        "content_type": "text/plain",
                    },
                )
                serve_tagged_labels = _parse_tag_labels(serve_tag)
                serve_tag_file = _post_http_json_endpoint(
                    tag_file_url,
                    {
                        "path": str(smoke_input_path),
                        "pack": SMOKE_PULL_PACK_ID,
                    },
                )
                serve_tag_file_labels = _parse_tag_labels(serve_tag_file)
                serve_tag_files = _post_http_json_endpoint(
                    tag_files_url,
                    {
                        "paths": [str(path) for path in smoke_batch_input_paths],
                        "pack": SMOKE_PULL_PACK_ID,
                        "output": {
                            "directory": str(smoke_batch_output_dir),
                            "write_manifest": True,
                            "manifest_path": str(smoke_batch_manifest_path),
                        },
                    },
                )
                serve_tag_files_labels = _parse_batch_tag_labels(serve_tag_files)
                if serve_tag_files.passed:
                    initial_manifest_path = _parse_batch_saved_manifest_path(serve_tag_files)
                    if initial_manifest_path is not None:
                        serve_tag_files_replay = _post_http_json_endpoint(
                            tag_files_url,
                            {
                                "manifest_input_path": initial_manifest_path,
                                "manifest_replay_mode": SMOKE_TAG_BATCH_REPLAY_MODE,
                                "pack": SMOKE_PULL_PACK_ID,
                                "output": {
                                    "directory": str(smoke_batch_output_dir),
                                    "write_manifest": True,
                                    "manifest_path": str(smoke_batch_replay_manifest_path),
                                },
                            },
                        )
                        serve_tag_files_replay_labels = _parse_batch_tag_labels(
                            serve_tag_files_replay
                        )
                    else:
                        serve_tag_files_replay = _skipped_command_result(
                            ["POST", tag_files_url],
                            reason="Skipped because live batch tagging did not write a manifest.",
                        )
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10.0)
        try:
            serve_stdout, serve_stderr = process.communicate(timeout=1.0)
        except subprocess.TimeoutExpired:
            serve_stdout = ""
            serve_stderr = ""
    serve = ReleaseCommandResult(
        command=serve_command,
        exit_code=0 if started else (process.returncode if process.returncode is not None else 1),
        passed=started,
        stdout=serve_stdout,
        stderr=serve_stderr,
    ) 
    served_pack_ids = _parse_status_installed_pack_ids(serve_status)
    return (
        serve,
        serve_healthz,
        serve_status,
        serve_tag,
        serve_tag_file,
        serve_tag_files,
        serve_tag_files_replay,
        served_pack_ids,
        serve_tagged_labels,
        serve_tag_file_labels,
        serve_tag_files_labels,
        serve_tag_files_replay_labels,
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
            executable = _resolve_venv_command(environment_dir, "ades")
            invoke_command = [str(executable), "status"]
            invoke = _skipped_command_result(
                invoke_command,
                reason="Skipped because virtual environment creation failed.",
            )
            pull_command = [str(executable), "pull", SMOKE_PULL_PACK_ID]
            pull = _skipped_command_result(
                pull_command,
                reason="Skipped because virtual environment creation failed.",
            )
            tag_command = [
                str(executable),
                "tag",
                SMOKE_TAG_TEXT,
                "--pack",
                SMOKE_PULL_PACK_ID,
            ]
            tag = _skipped_command_result(
                tag_command,
                reason="Skipped because virtual environment creation failed.",
            )
            recovery_status = _skipped_command_result(
                invoke_command,
                reason="Skipped because virtual environment creation failed.",
            )
            serve_command = [str(executable), "serve", "--host", SMOKE_SERVE_HOST, "--port", "0"]
            serve = _skipped_command_result(
                serve_command,
                reason="Skipped because virtual environment creation failed.",
            )
            serve_healthz = _skipped_command_result(
                ["GET", f"http://{SMOKE_SERVE_HOST}:0/healthz"],
                reason="Skipped because virtual environment creation failed.",
            )
            serve_status = _skipped_command_result(
                ["GET", f"http://{SMOKE_SERVE_HOST}:0/v0/status"],
                reason="Skipped because virtual environment creation failed.",
            )
            return ReleaseInstallSmokeResult(
                artifact_kind="python_wheel",
                working_dir=str(working_dir),
                environment_dir=str(environment_dir),
                storage_root=str(storage_root),
                install=install,
                invoke=invoke,
                pull=pull,
                tag=tag,
                recovery_status=recovery_status,
                serve=serve,
                serve_healthz=serve_healthz,
                serve_status=serve_status,
                serve_tag=_skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag"],
                    reason="Skipped because virtual environment creation failed.",
                ),
                serve_tag_file=_skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/file"],
                    reason="Skipped because virtual environment creation failed.",
                ),
                serve_tag_files=_skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/files"],
                    reason="Skipped because virtual environment creation failed.",
                ),
                serve_tag_files_replay=_skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/files"],
                    reason="Skipped because virtual environment creation failed.",
                ),
                remove_guardrail=_skipped_command_result(
                    [str(executable), "packs", "remove", SMOKE_DEPENDENCY_PACK_ID],
                    reason="Skipped because virtual environment creation failed.",
                ),
                remove=_skipped_command_result(
                    [str(executable), "packs", "remove", SMOKE_PULL_PACK_ID],
                    reason="Skipped because virtual environment creation failed.",
                ),
                remove_status=_skipped_command_result(
                    [str(executable), "status"],
                    reason="Skipped because virtual environment creation failed.",
                ),
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

        executable = _resolve_venv_command(environment_dir, "ades")
        if install.passed:
            (
                invoke,
                pull,
                tag,
                reported_version,
                pulled_pack_ids,
                tagged_labels,
            ) = _run_cli_runtime_smoke(
                executable=executable,
                working_dir=working_dir,
                storage_root=storage_root,
            )
            if tag.passed:
                recovery_status, recovered_pack_ids = _run_cli_recovery_status_smoke(
                    executable=executable,
                    working_dir=working_dir,
                    storage_root=storage_root,
                    expected_version=expected_version,
                )
                _clear_registry_metadata_files(storage_root)
                (
                    serve,
                    serve_healthz,
                    serve_status,
                    serve_tag,
                    serve_tag_file,
                    serve_tag_files,
                    serve_tag_files_replay,
                    served_pack_ids,
                    serve_tagged_labels,
                    serve_tag_file_labels,
                    serve_tag_files_labels,
                    serve_tag_files_replay_labels,
                ) = _run_cli_service_smoke(
                    executable=executable,
                    working_dir=working_dir,
                    storage_root=storage_root,
                    expected_version=expected_version,
                )
                (
                    remove_guardrail,
                    remove,
                    remove_status,
                    remaining_pack_ids,
                ) = _run_cli_remove_smoke(
                    executable=executable,
                    working_dir=working_dir,
                    storage_root=storage_root,
                    expected_version=expected_version,
                )
            else:
                recovery_status = _skipped_command_result(
                    [str(executable), "status"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_command = [
                    str(executable),
                    "serve",
                    "--host",
                    SMOKE_SERVE_HOST,
                    "--port",
                    "0",
                ]
                serve = _skipped_command_result(
                    serve_command,
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_healthz = _skipped_command_result(
                    ["GET", f"http://{SMOKE_SERVE_HOST}:0/healthz"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_status = _skipped_command_result(
                    ["GET", f"http://{SMOKE_SERVE_HOST}:0/v0/status"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_tag = _skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_tag_file = _skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/file"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_tag_files = _skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/files"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_tag_files_replay = _skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/files"],
                    reason="Skipped because post-pull tagging failed.",
                )
                recovered_pack_ids = []
                served_pack_ids = []
                serve_tagged_labels = []
                serve_tag_file_labels = []
                serve_tag_files_labels = []
                serve_tag_files_replay_labels = []
                remove_guardrail = _skipped_command_result(
                    [str(executable), "packs", "remove", SMOKE_DEPENDENCY_PACK_ID],
                    reason="Skipped because post-pull tagging failed.",
                )
                remove = _skipped_command_result(
                    [str(executable), "packs", "remove", SMOKE_PULL_PACK_ID],
                    reason="Skipped because post-pull tagging failed.",
                )
                remove_status = _skipped_command_result(
                    [str(executable), "status"],
                    reason="Skipped because post-pull tagging failed.",
                )
                remaining_pack_ids = []
        else:
            invoke_command = [str(executable), "status"]
            invoke = _skipped_command_result(
                invoke_command,
                reason="Skipped because wheel installation failed.",
            )
            pull_command = [str(executable), "pull", SMOKE_PULL_PACK_ID]
            pull = _skipped_command_result(
                pull_command,
                reason="Skipped because wheel installation failed.",
            )
            tag_command = [
                str(executable),
                "tag",
                SMOKE_TAG_TEXT,
                "--pack",
                SMOKE_PULL_PACK_ID,
            ]
            tag = _skipped_command_result(
                tag_command,
                reason="Skipped because wheel installation failed.",
            )
            recovery_status = _skipped_command_result(
                invoke_command,
                reason="Skipped because wheel installation failed.",
            )
            serve_command = [str(executable), "serve", "--host", SMOKE_SERVE_HOST, "--port", "0"]
            serve = _skipped_command_result(
                serve_command,
                reason="Skipped because wheel installation failed.",
            )
            serve_healthz = _skipped_command_result(
                ["GET", f"http://{SMOKE_SERVE_HOST}:0/healthz"],
                reason="Skipped because wheel installation failed.",
            )
            serve_status = _skipped_command_result(
                ["GET", f"http://{SMOKE_SERVE_HOST}:0/v0/status"],
                reason="Skipped because wheel installation failed.",
            )
            serve_tag = _skipped_command_result(
                ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag"],
                reason="Skipped because wheel installation failed.",
            )
            serve_tag_file = _skipped_command_result(
                ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/file"],
                reason="Skipped because wheel installation failed.",
            )
            serve_tag_files = _skipped_command_result(
                ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/files"],
                reason="Skipped because wheel installation failed.",
            )
            serve_tag_files_replay = _skipped_command_result(
                ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/files"],
                reason="Skipped because wheel installation failed.",
            )
            reported_version = None
            pulled_pack_ids = []
            tagged_labels = []
            recovered_pack_ids = []
            served_pack_ids = []
            serve_tagged_labels = []
            serve_tag_file_labels = []
            serve_tag_files_labels = []
            serve_tag_files_replay_labels = []
            remove_guardrail = _skipped_command_result(
                [str(executable), "packs", "remove", SMOKE_DEPENDENCY_PACK_ID],
                reason="Skipped because wheel installation failed.",
            )
            remove = _skipped_command_result(
                [str(executable), "packs", "remove", SMOKE_PULL_PACK_ID],
                reason="Skipped because wheel installation failed.",
            )
            remove_status = _skipped_command_result(
                [str(executable), "status"],
                reason="Skipped because wheel installation failed.",
            )
            remaining_pack_ids = []

        required_labels = set(SMOKE_TAG_REQUIRED_LABELS)
        missing_pulled_pack_ids = _missing_smoke_pack_ids(pulled_pack_ids)
        missing_recovered_pack_ids = _missing_smoke_pack_ids(recovered_pack_ids)
        missing_served_pack_ids = _missing_smoke_pack_ids(served_pack_ids)
        missing_serve_tag_labels = sorted(required_labels - set(serve_tagged_labels))
        missing_serve_tag_file_labels = sorted(required_labels - set(serve_tag_file_labels))
        missing_serve_tag_files_labels = sorted(required_labels - set(serve_tag_files_labels))
        missing_serve_tag_files_replay_labels = sorted(
            required_labels - set(serve_tag_files_replay_labels)
        )
        serve_tag_files_pack_id = _parse_batch_pack_id(serve_tag_files)
        serve_tag_files_item_count = _parse_batch_item_count(serve_tag_files)
        serve_tag_files_manifest_path = _parse_batch_saved_manifest_path(serve_tag_files)
        serve_tag_files_output_paths = _parse_batch_saved_output_paths(serve_tag_files)
        serve_tag_files_lineage_run_id = _parse_batch_lineage_run_id(serve_tag_files)
        serve_tag_files_lineage_root_run_id = _parse_batch_lineage_root_run_id(
            serve_tag_files
        )
        serve_tag_files_lineage_created_at = _parse_batch_lineage_created_at(
            serve_tag_files
        )
        has_expected_serve_tag_files_lineage_parent_run_id = _batch_lineage_field_is_unset(
            serve_tag_files,
            "parent_run_id",
        )
        has_expected_serve_tag_files_lineage_source_manifest_path = (
            _batch_lineage_field_is_unset(serve_tag_files, "source_manifest_path")
        )
        serve_tag_files_replay_manifest_path = _parse_batch_saved_manifest_path(
            serve_tag_files_replay
        )
        serve_tag_files_replay_output_paths = _parse_batch_saved_output_paths(
            serve_tag_files_replay
        )
        serve_tag_files_replay_pack_id = _parse_batch_pack_id(serve_tag_files_replay)
        serve_tag_files_replay_item_count = _parse_batch_item_count(serve_tag_files_replay)
        serve_tag_files_replay_manifest_input_path = _parse_batch_manifest_input_path(
            serve_tag_files_replay
        )
        serve_tag_files_replay_mode = _parse_batch_manifest_replay_mode(
            serve_tag_files_replay
        )
        serve_tag_files_replay_manifest_candidate_count = (
            _parse_batch_manifest_candidate_count(serve_tag_files_replay)
        )
        serve_tag_files_replay_manifest_selected_count = (
            _parse_batch_manifest_selected_count(serve_tag_files_replay)
        )
        serve_tag_files_replay_source_manifest_path = (
            _parse_batch_lineage_source_manifest_path(serve_tag_files_replay)
        )
        serve_tag_files_replay_run_id = _parse_batch_lineage_run_id(
            serve_tag_files_replay
        )
        serve_tag_files_replay_root_run_id = _parse_batch_lineage_root_run_id(
            serve_tag_files_replay
        )
        serve_tag_files_replay_parent_run_id = _parse_batch_lineage_parent_run_id(
            serve_tag_files_replay
        )
        serve_tag_files_replay_created_at = _parse_batch_lineage_created_at(
            serve_tag_files_replay
        )
        serve_tag_files_replay_rerun_diff_manifest_input_path = (
            _parse_batch_rerun_diff_manifest_input_path(serve_tag_files_replay)
        )
        serve_tag_files_replay_rerun_diff_changed_paths = _parse_batch_rerun_diff_path_list(
            serve_tag_files_replay,
            "changed",
        )
        serve_tag_files_replay_rerun_diff_newly_processed = _parse_batch_rerun_diff_path_list(
            serve_tag_files_replay,
            "newly_processed",
        )
        serve_tag_files_replay_rerun_diff_reused = _parse_batch_rerun_diff_path_list(
            serve_tag_files_replay,
            "reused",
        )
        serve_tag_files_replay_rerun_diff_repaired = _parse_batch_rerun_diff_path_list(
            serve_tag_files_replay,
            "repaired",
        )
        serve_tag_files_replay_rerun_diff_skipped = _parse_batch_rerun_diff_list(
            serve_tag_files_replay,
            "skipped",
        )
        expected_serve_tag_files_replay_rerun_diff_changed_paths = sorted(
            str((working_dir / file_name).resolve())
            for file_name, _ in SMOKE_TAG_BATCH_FILES
        )
        expected_serve_tag_files_manifest_path = _expected_smoke_batch_manifest_path(
            working_dir
        )
        expected_serve_tag_files_output_paths = _expected_smoke_batch_output_paths(
            working_dir
        )
        expected_serve_tag_files_replay_manifest_path = (
            _expected_smoke_batch_manifest_path(working_dir, replay=True)
        )
        has_expected_serve_tag_files_pack_id = (
            serve_tag_files_pack_id == SMOKE_PULL_PACK_ID
        )
        has_expected_serve_tag_files_item_count = (
            serve_tag_files_item_count == len(SMOKE_TAG_BATCH_FILES)
        )
        has_expected_serve_tag_files_manifest_path = (
            serve_tag_files_manifest_path == expected_serve_tag_files_manifest_path
        )
        has_expected_serve_tag_files_output_paths = (
            serve_tag_files_output_paths == expected_serve_tag_files_output_paths
        )
        has_expected_serve_tag_files_replay_manifest_path = (
            serve_tag_files_replay_manifest_path
            == expected_serve_tag_files_replay_manifest_path
        )
        has_expected_serve_tag_files_replay_output_paths = (
            serve_tag_files_replay_output_paths == expected_serve_tag_files_output_paths
        )
        has_expected_serve_tag_files_replay_pack_id = (
            serve_tag_files_replay_pack_id == SMOKE_PULL_PACK_ID
        )
        has_expected_serve_tag_files_replay_item_count = (
            serve_tag_files_replay_item_count == len(SMOKE_TAG_BATCH_FILES)
        )
        has_expected_serve_tag_files_replay_manifest_input_path = (
            serve_tag_files_manifest_path is not None
            and serve_tag_files_replay_manifest_input_path == serve_tag_files_manifest_path
        )
        has_expected_serve_tag_files_replay_mode = (
            serve_tag_files_replay_mode == SMOKE_TAG_BATCH_REPLAY_MODE
        )
        has_expected_serve_tag_files_replay_manifest_candidate_count = (
            serve_tag_files_replay_manifest_candidate_count == len(SMOKE_TAG_BATCH_FILES)
        )
        has_expected_serve_tag_files_replay_manifest_selected_count = (
            serve_tag_files_replay_manifest_selected_count == len(SMOKE_TAG_BATCH_FILES)
        )
        has_expected_serve_tag_files_replay_source_manifest_path = (
            serve_tag_files_manifest_path is not None
            and serve_tag_files_replay_source_manifest_path == serve_tag_files_manifest_path
        )
        has_expected_serve_tag_files_lineage_run_id = (
            serve_tag_files_lineage_run_id is not None
        )
        has_expected_serve_tag_files_replay_run_id = (
            serve_tag_files_lineage_run_id is not None
            and serve_tag_files_replay_run_id is not None
            and serve_tag_files_replay_run_id != serve_tag_files_lineage_run_id
        )
        has_expected_serve_tag_files_lineage_root_run_id = (
            serve_tag_files_lineage_root_run_id == serve_tag_files_lineage_run_id
        )
        has_expected_serve_tag_files_lineage_created_at = (
            serve_tag_files_lineage_created_at is not None
        )
        has_expected_serve_tag_files_replay_root_run_id = (
            serve_tag_files_lineage_root_run_id is not None
            and serve_tag_files_replay_root_run_id == serve_tag_files_lineage_root_run_id
        )
        has_expected_serve_tag_files_replay_parent_run_id = (
            serve_tag_files_lineage_run_id is not None
            and serve_tag_files_replay_parent_run_id == serve_tag_files_lineage_run_id
        )
        has_expected_serve_tag_files_replay_created_at = (
            serve_tag_files_replay_created_at is not None
        )
        has_expected_serve_tag_files_replay_created_at_order = (
            serve_tag_files_lineage_created_at is not None
            and serve_tag_files_replay_created_at is not None
            and serve_tag_files_replay_created_at > serve_tag_files_lineage_created_at
        )
        has_expected_serve_tag_files_replay_rerun_diff = (
            _parse_batch_rerun_diff(serve_tag_files_replay) is not None
        )
        has_expected_serve_tag_files_replay_rerun_diff_manifest_input_path = (
            serve_tag_files_manifest_path is not None
            and serve_tag_files_replay_rerun_diff_manifest_input_path
            == serve_tag_files_manifest_path
        )
        has_expected_serve_tag_files_replay_rerun_diff_changed_paths = (
            serve_tag_files_replay_rerun_diff_changed_paths is not None
            and sorted(serve_tag_files_replay_rerun_diff_changed_paths)
            == expected_serve_tag_files_replay_rerun_diff_changed_paths
        )
        has_expected_serve_tag_files_replay_rerun_diff_newly_processed = (
            serve_tag_files_replay_rerun_diff_newly_processed == []
        )
        has_expected_serve_tag_files_replay_rerun_diff_reused = (
            serve_tag_files_replay_rerun_diff_reused == []
        )
        has_expected_serve_tag_files_replay_rerun_diff_repaired = (
            serve_tag_files_replay_rerun_diff_repaired == []
        )
        has_expected_serve_tag_files_replay_rerun_diff_skipped = (
            serve_tag_files_replay_rerun_diff_skipped == []
        )
        has_expected_remove_guardrail = (
            not remove_guardrail.passed
            and SMOKE_REMOVE_GUARDRAIL_MESSAGE in remove_guardrail.stderr
        )
        remove_status_version = _parse_status_version(remove_status)
        has_expected_remove_status = remove_status_version == expected_version
        has_expected_remaining_pack_ids = (
            remaining_pack_ids == list(SMOKE_REMOVE_REMAINING_PACK_IDS)
        )
        passed = (
            install.passed
            and invoke.passed
            and reported_version == expected_version
            and pull.passed
            and not missing_pulled_pack_ids
            and tag.passed
            and required_labels.issubset(set(tagged_labels))
            and recovery_status.passed
            and _parse_status_version(recovery_status) == expected_version
            and not missing_recovered_pack_ids
            and serve.passed
            and serve_healthz.passed
            and _healthz_matches_expected(serve_healthz, expected_version=expected_version)
            and serve_status.passed
            and _parse_status_version(serve_status) == expected_version
            and not missing_served_pack_ids
            and serve_tag.passed
            and not missing_serve_tag_labels
            and serve_tag_file.passed
            and not missing_serve_tag_file_labels
            and serve_tag_files.passed
            and not missing_serve_tag_files_labels
            and has_expected_serve_tag_files_pack_id
            and has_expected_serve_tag_files_item_count
            and has_expected_serve_tag_files_manifest_path
            and has_expected_serve_tag_files_output_paths
            and has_expected_serve_tag_files_lineage_run_id
            and has_expected_serve_tag_files_lineage_root_run_id
            and has_expected_serve_tag_files_lineage_created_at
            and has_expected_serve_tag_files_lineage_parent_run_id
            and has_expected_serve_tag_files_lineage_source_manifest_path
            and serve_tag_files_replay.passed
            and not missing_serve_tag_files_replay_labels
            and has_expected_serve_tag_files_replay_pack_id
            and has_expected_serve_tag_files_replay_item_count
            and has_expected_serve_tag_files_replay_manifest_input_path
            and has_expected_serve_tag_files_replay_mode
            and has_expected_serve_tag_files_replay_manifest_candidate_count
            and has_expected_serve_tag_files_replay_manifest_selected_count
            and has_expected_serve_tag_files_replay_source_manifest_path
            and has_expected_serve_tag_files_replay_run_id
            and has_expected_serve_tag_files_replay_root_run_id
            and has_expected_serve_tag_files_replay_parent_run_id
            and has_expected_serve_tag_files_replay_created_at
            and has_expected_serve_tag_files_replay_created_at_order
            and has_expected_serve_tag_files_replay_manifest_path
            and has_expected_serve_tag_files_replay_output_paths
            and has_expected_serve_tag_files_replay_rerun_diff
            and has_expected_serve_tag_files_replay_rerun_diff_manifest_input_path
            and has_expected_serve_tag_files_replay_rerun_diff_changed_paths
            and has_expected_serve_tag_files_replay_rerun_diff_newly_processed
            and has_expected_serve_tag_files_replay_rerun_diff_reused
            and has_expected_serve_tag_files_replay_rerun_diff_repaired
            and has_expected_serve_tag_files_replay_rerun_diff_skipped
            and has_expected_remove_guardrail
            and remove.passed
            and remove_status.passed
            and has_expected_remove_status
            and has_expected_remaining_pack_ids
        )
        return ReleaseInstallSmokeResult(
            artifact_kind="python_wheel",
            working_dir=str(working_dir),
            environment_dir=str(environment_dir),
            storage_root=str(storage_root),
            install=install,
            invoke=invoke,
            pull=pull,
            tag=tag,
            recovery_status=recovery_status,
            serve=serve,
            serve_healthz=serve_healthz,
            serve_status=serve_status,
            serve_tag=serve_tag,
            serve_tag_file=serve_tag_file,
            serve_tag_files=serve_tag_files,
            serve_tag_files_replay=serve_tag_files_replay,
            remove_guardrail=remove_guardrail,
            remove=remove,
            remove_status=remove_status,
            passed=passed,
            reported_version=reported_version,
            pulled_pack_ids=pulled_pack_ids,
            tagged_labels=tagged_labels,
            recovered_pack_ids=recovered_pack_ids,
            served_pack_ids=served_pack_ids,
            remaining_pack_ids=remaining_pack_ids,
            serve_tagged_labels=serve_tagged_labels,
            serve_tag_file_labels=serve_tag_file_labels,
            serve_tag_files_labels=serve_tag_files_labels,
            serve_tag_files_replay_labels=serve_tag_files_replay_labels,
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

        executable = _resolve_npm_installed_command(install_prefix, NPM_COMMAND_NAME)
        if install.passed:
            (
                invoke,
                pull,
                tag,
                reported_version,
                pulled_pack_ids,
                tagged_labels,
            ) = _run_cli_runtime_smoke(
                executable=executable,
                working_dir=working_dir,
                storage_root=storage_root,
                extra_env={
                    NPM_PYTHON_BIN_ENV: sys.executable,
                    NPM_PYTHON_PACKAGE_SPEC_ENV: str(wheel_path),
                    NPM_RUNTIME_DIR_ENV: str(environment_dir),
                },
            )
            if tag.passed:
                recovery_status, recovered_pack_ids = _run_cli_recovery_status_smoke(
                    executable=executable,
                    working_dir=working_dir,
                    storage_root=storage_root,
                    expected_version=expected_version,
                    extra_env={
                        NPM_PYTHON_BIN_ENV: sys.executable,
                        NPM_PYTHON_PACKAGE_SPEC_ENV: str(wheel_path),
                        NPM_RUNTIME_DIR_ENV: str(environment_dir),
                    },
                )
                _clear_registry_metadata_files(storage_root)
                (
                    serve,
                    serve_healthz,
                    serve_status,
                    serve_tag,
                    serve_tag_file,
                    serve_tag_files,
                    serve_tag_files_replay,
                    served_pack_ids,
                    serve_tagged_labels,
                    serve_tag_file_labels,
                    serve_tag_files_labels,
                    serve_tag_files_replay_labels,
                ) = _run_cli_service_smoke(
                    executable=executable,
                    working_dir=working_dir,
                    storage_root=storage_root,
                    expected_version=expected_version,
                    extra_env={
                        NPM_PYTHON_BIN_ENV: sys.executable,
                        NPM_PYTHON_PACKAGE_SPEC_ENV: str(wheel_path),
                        NPM_RUNTIME_DIR_ENV: str(environment_dir),
                    },
                )
                (
                    remove_guardrail,
                    remove,
                    remove_status,
                    remaining_pack_ids,
                ) = _run_cli_remove_smoke(
                    executable=executable,
                    working_dir=working_dir,
                    storage_root=storage_root,
                    expected_version=expected_version,
                    extra_env={
                        NPM_PYTHON_BIN_ENV: sys.executable,
                        NPM_PYTHON_PACKAGE_SPEC_ENV: str(wheel_path),
                        NPM_RUNTIME_DIR_ENV: str(environment_dir),
                    },
                )
            else:
                recovery_status = _skipped_command_result(
                    [str(executable), "status"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_command = [
                    str(executable),
                    "serve",
                    "--host",
                    SMOKE_SERVE_HOST,
                    "--port",
                    "0",
                ]
                serve = _skipped_command_result(
                    serve_command,
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_healthz = _skipped_command_result(
                    ["GET", f"http://{SMOKE_SERVE_HOST}:0/healthz"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_status = _skipped_command_result(
                    ["GET", f"http://{SMOKE_SERVE_HOST}:0/v0/status"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_tag = _skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_tag_file = _skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/file"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_tag_files = _skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/files"],
                    reason="Skipped because post-pull tagging failed.",
                )
                serve_tag_files_replay = _skipped_command_result(
                    ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/files"],
                    reason="Skipped because post-pull tagging failed.",
                )
                recovered_pack_ids = []
                served_pack_ids = []
                serve_tagged_labels = []
                serve_tag_file_labels = []
                serve_tag_files_labels = []
                serve_tag_files_replay_labels = []
                remove_guardrail = _skipped_command_result(
                    [str(executable), "packs", "remove", SMOKE_DEPENDENCY_PACK_ID],
                    reason="Skipped because post-pull tagging failed.",
                )
                remove = _skipped_command_result(
                    [str(executable), "packs", "remove", SMOKE_PULL_PACK_ID],
                    reason="Skipped because post-pull tagging failed.",
                )
                remove_status = _skipped_command_result(
                    [str(executable), "status"],
                    reason="Skipped because post-pull tagging failed.",
                )
                remaining_pack_ids = []
        else:
            invoke_command = [str(executable), "status"]
            invoke = _skipped_command_result(
                invoke_command,
                reason="Skipped because npm tarball installation failed.",
            )
            pull_command = [str(executable), "pull", SMOKE_PULL_PACK_ID]
            pull = _skipped_command_result(
                pull_command,
                reason="Skipped because npm tarball installation failed.",
            )
            tag_command = [
                str(executable),
                "tag",
                SMOKE_TAG_TEXT,
                "--pack",
                SMOKE_PULL_PACK_ID,
            ]
            tag = _skipped_command_result(
                tag_command,
                reason="Skipped because npm tarball installation failed.",
            )
            recovery_status = _skipped_command_result(
                invoke_command,
                reason="Skipped because npm tarball installation failed.",
            )
            serve_command = [str(executable), "serve", "--host", SMOKE_SERVE_HOST, "--port", "0"]
            serve = _skipped_command_result(
                serve_command,
                reason="Skipped because npm tarball installation failed.",
            )
            serve_healthz = _skipped_command_result(
                ["GET", f"http://{SMOKE_SERVE_HOST}:0/healthz"],
                reason="Skipped because npm tarball installation failed.",
            )
            serve_status = _skipped_command_result(
                ["GET", f"http://{SMOKE_SERVE_HOST}:0/v0/status"],
                reason="Skipped because npm tarball installation failed.",
            )
            serve_tag = _skipped_command_result(
                ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag"],
                reason="Skipped because npm tarball installation failed.",
            )
            serve_tag_file = _skipped_command_result(
                ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/file"],
                reason="Skipped because npm tarball installation failed.",
            )
            serve_tag_files = _skipped_command_result(
                ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/files"],
                reason="Skipped because npm tarball installation failed.",
            )
            serve_tag_files_replay = _skipped_command_result(
                ["POST", f"http://{SMOKE_SERVE_HOST}:0/v0/tag/files"],
                reason="Skipped because npm tarball installation failed.",
            )
            reported_version = None
            pulled_pack_ids = []
            tagged_labels = []
            recovered_pack_ids = []
            served_pack_ids = []
            serve_tagged_labels = []
            serve_tag_file_labels = []
            serve_tag_files_labels = []
            serve_tag_files_replay_labels = []
            remove_guardrail = _skipped_command_result(
                [str(executable), "packs", "remove", SMOKE_DEPENDENCY_PACK_ID],
                reason="Skipped because npm tarball installation failed.",
            )
            remove = _skipped_command_result(
                [str(executable), "packs", "remove", SMOKE_PULL_PACK_ID],
                reason="Skipped because npm tarball installation failed.",
            )
            remove_status = _skipped_command_result(
                [str(executable), "status"],
                reason="Skipped because npm tarball installation failed.",
            )
            remaining_pack_ids = []

        required_labels = set(SMOKE_TAG_REQUIRED_LABELS)
        missing_pulled_pack_ids = _missing_smoke_pack_ids(pulled_pack_ids)
        missing_recovered_pack_ids = _missing_smoke_pack_ids(recovered_pack_ids)
        missing_served_pack_ids = _missing_smoke_pack_ids(served_pack_ids)
        missing_serve_tag_labels = sorted(required_labels - set(serve_tagged_labels))
        missing_serve_tag_file_labels = sorted(required_labels - set(serve_tag_file_labels))
        missing_serve_tag_files_labels = sorted(required_labels - set(serve_tag_files_labels))
        missing_serve_tag_files_replay_labels = sorted(
            required_labels - set(serve_tag_files_replay_labels)
        )
        serve_tag_files_pack_id = _parse_batch_pack_id(serve_tag_files)
        serve_tag_files_item_count = _parse_batch_item_count(serve_tag_files)
        serve_tag_files_manifest_path = _parse_batch_saved_manifest_path(serve_tag_files)
        serve_tag_files_output_paths = _parse_batch_saved_output_paths(serve_tag_files)
        serve_tag_files_lineage_run_id = _parse_batch_lineage_run_id(serve_tag_files)
        serve_tag_files_lineage_root_run_id = _parse_batch_lineage_root_run_id(
            serve_tag_files
        )
        serve_tag_files_lineage_created_at = _parse_batch_lineage_created_at(
            serve_tag_files
        )
        has_expected_serve_tag_files_lineage_parent_run_id = _batch_lineage_field_is_unset(
            serve_tag_files,
            "parent_run_id",
        )
        has_expected_serve_tag_files_lineage_source_manifest_path = (
            _batch_lineage_field_is_unset(serve_tag_files, "source_manifest_path")
        )
        serve_tag_files_replay_manifest_path = _parse_batch_saved_manifest_path(
            serve_tag_files_replay
        )
        serve_tag_files_replay_output_paths = _parse_batch_saved_output_paths(
            serve_tag_files_replay
        )
        serve_tag_files_replay_pack_id = _parse_batch_pack_id(serve_tag_files_replay)
        serve_tag_files_replay_item_count = _parse_batch_item_count(serve_tag_files_replay)
        serve_tag_files_replay_manifest_input_path = _parse_batch_manifest_input_path(
            serve_tag_files_replay
        )
        serve_tag_files_replay_mode = _parse_batch_manifest_replay_mode(
            serve_tag_files_replay
        )
        serve_tag_files_replay_manifest_candidate_count = (
            _parse_batch_manifest_candidate_count(serve_tag_files_replay)
        )
        serve_tag_files_replay_manifest_selected_count = (
            _parse_batch_manifest_selected_count(serve_tag_files_replay)
        )
        serve_tag_files_replay_source_manifest_path = (
            _parse_batch_lineage_source_manifest_path(serve_tag_files_replay)
        )
        serve_tag_files_replay_run_id = _parse_batch_lineage_run_id(
            serve_tag_files_replay
        )
        serve_tag_files_replay_root_run_id = _parse_batch_lineage_root_run_id(
            serve_tag_files_replay
        )
        serve_tag_files_replay_parent_run_id = _parse_batch_lineage_parent_run_id(
            serve_tag_files_replay
        )
        serve_tag_files_replay_created_at = _parse_batch_lineage_created_at(
            serve_tag_files_replay
        )
        serve_tag_files_replay_rerun_diff_manifest_input_path = (
            _parse_batch_rerun_diff_manifest_input_path(serve_tag_files_replay)
        )
        serve_tag_files_replay_rerun_diff_changed_paths = _parse_batch_rerun_diff_path_list(
            serve_tag_files_replay,
            "changed",
        )
        serve_tag_files_replay_rerun_diff_newly_processed = _parse_batch_rerun_diff_path_list(
            serve_tag_files_replay,
            "newly_processed",
        )
        serve_tag_files_replay_rerun_diff_reused = _parse_batch_rerun_diff_path_list(
            serve_tag_files_replay,
            "reused",
        )
        serve_tag_files_replay_rerun_diff_repaired = _parse_batch_rerun_diff_path_list(
            serve_tag_files_replay,
            "repaired",
        )
        serve_tag_files_replay_rerun_diff_skipped = _parse_batch_rerun_diff_list(
            serve_tag_files_replay,
            "skipped",
        )
        expected_serve_tag_files_replay_rerun_diff_changed_paths = sorted(
            str((working_dir / file_name).resolve())
            for file_name, _ in SMOKE_TAG_BATCH_FILES
        )
        expected_serve_tag_files_manifest_path = _expected_smoke_batch_manifest_path(
            working_dir
        )
        expected_serve_tag_files_output_paths = _expected_smoke_batch_output_paths(
            working_dir
        )
        expected_serve_tag_files_replay_manifest_path = (
            _expected_smoke_batch_manifest_path(working_dir, replay=True)
        )
        has_expected_serve_tag_files_pack_id = (
            serve_tag_files_pack_id == SMOKE_PULL_PACK_ID
        )
        has_expected_serve_tag_files_item_count = (
            serve_tag_files_item_count == len(SMOKE_TAG_BATCH_FILES)
        )
        has_expected_serve_tag_files_manifest_path = (
            serve_tag_files_manifest_path == expected_serve_tag_files_manifest_path
        )
        has_expected_serve_tag_files_output_paths = (
            serve_tag_files_output_paths == expected_serve_tag_files_output_paths
        )
        has_expected_serve_tag_files_replay_manifest_path = (
            serve_tag_files_replay_manifest_path
            == expected_serve_tag_files_replay_manifest_path
        )
        has_expected_serve_tag_files_replay_output_paths = (
            serve_tag_files_replay_output_paths == expected_serve_tag_files_output_paths
        )
        has_expected_serve_tag_files_replay_pack_id = (
            serve_tag_files_replay_pack_id == SMOKE_PULL_PACK_ID
        )
        has_expected_serve_tag_files_replay_item_count = (
            serve_tag_files_replay_item_count == len(SMOKE_TAG_BATCH_FILES)
        )
        has_expected_serve_tag_files_replay_manifest_input_path = (
            serve_tag_files_manifest_path is not None
            and serve_tag_files_replay_manifest_input_path == serve_tag_files_manifest_path
        )
        has_expected_serve_tag_files_replay_mode = (
            serve_tag_files_replay_mode == SMOKE_TAG_BATCH_REPLAY_MODE
        )
        has_expected_serve_tag_files_replay_manifest_candidate_count = (
            serve_tag_files_replay_manifest_candidate_count == len(SMOKE_TAG_BATCH_FILES)
        )
        has_expected_serve_tag_files_replay_manifest_selected_count = (
            serve_tag_files_replay_manifest_selected_count == len(SMOKE_TAG_BATCH_FILES)
        )
        has_expected_serve_tag_files_replay_source_manifest_path = (
            serve_tag_files_manifest_path is not None
            and serve_tag_files_replay_source_manifest_path == serve_tag_files_manifest_path
        )
        has_expected_serve_tag_files_lineage_run_id = (
            serve_tag_files_lineage_run_id is not None
        )
        has_expected_serve_tag_files_replay_run_id = (
            serve_tag_files_lineage_run_id is not None
            and serve_tag_files_replay_run_id is not None
            and serve_tag_files_replay_run_id != serve_tag_files_lineage_run_id
        )
        has_expected_serve_tag_files_lineage_root_run_id = (
            serve_tag_files_lineage_root_run_id == serve_tag_files_lineage_run_id
        )
        has_expected_serve_tag_files_lineage_created_at = (
            serve_tag_files_lineage_created_at is not None
        )
        has_expected_serve_tag_files_replay_root_run_id = (
            serve_tag_files_lineage_root_run_id is not None
            and serve_tag_files_replay_root_run_id == serve_tag_files_lineage_root_run_id
        )
        has_expected_serve_tag_files_replay_parent_run_id = (
            serve_tag_files_lineage_run_id is not None
            and serve_tag_files_replay_parent_run_id == serve_tag_files_lineage_run_id
        )
        has_expected_serve_tag_files_replay_created_at = (
            serve_tag_files_replay_created_at is not None
        )
        has_expected_serve_tag_files_replay_created_at_order = (
            serve_tag_files_lineage_created_at is not None
            and serve_tag_files_replay_created_at is not None
            and serve_tag_files_replay_created_at > serve_tag_files_lineage_created_at
        )
        has_expected_serve_tag_files_replay_rerun_diff = (
            _parse_batch_rerun_diff(serve_tag_files_replay) is not None
        )
        has_expected_serve_tag_files_replay_rerun_diff_manifest_input_path = (
            serve_tag_files_manifest_path is not None
            and serve_tag_files_replay_rerun_diff_manifest_input_path
            == serve_tag_files_manifest_path
        )
        has_expected_serve_tag_files_replay_rerun_diff_changed_paths = (
            serve_tag_files_replay_rerun_diff_changed_paths is not None
            and sorted(serve_tag_files_replay_rerun_diff_changed_paths)
            == expected_serve_tag_files_replay_rerun_diff_changed_paths
        )
        has_expected_serve_tag_files_replay_rerun_diff_newly_processed = (
            serve_tag_files_replay_rerun_diff_newly_processed == []
        )
        has_expected_serve_tag_files_replay_rerun_diff_reused = (
            serve_tag_files_replay_rerun_diff_reused == []
        )
        has_expected_serve_tag_files_replay_rerun_diff_repaired = (
            serve_tag_files_replay_rerun_diff_repaired == []
        )
        has_expected_serve_tag_files_replay_rerun_diff_skipped = (
            serve_tag_files_replay_rerun_diff_skipped == []
        )
        has_expected_remove_guardrail = (
            not remove_guardrail.passed
            and SMOKE_REMOVE_GUARDRAIL_MESSAGE in remove_guardrail.stderr
        )
        remove_status_version = _parse_status_version(remove_status)
        has_expected_remove_status = remove_status_version == expected_version
        has_expected_remaining_pack_ids = (
            remaining_pack_ids == list(SMOKE_REMOVE_REMAINING_PACK_IDS)
        )
        passed = (
            install.passed
            and invoke.passed
            and reported_version == expected_version
            and pull.passed
            and not missing_pulled_pack_ids
            and tag.passed
            and required_labels.issubset(set(tagged_labels))
            and recovery_status.passed
            and _parse_status_version(recovery_status) == expected_version
            and not missing_recovered_pack_ids
            and serve.passed
            and serve_healthz.passed
            and _healthz_matches_expected(serve_healthz, expected_version=expected_version)
            and serve_status.passed
            and _parse_status_version(serve_status) == expected_version
            and not missing_served_pack_ids
            and serve_tag.passed
            and not missing_serve_tag_labels
            and serve_tag_file.passed
            and not missing_serve_tag_file_labels
            and serve_tag_files.passed
            and not missing_serve_tag_files_labels
            and has_expected_serve_tag_files_pack_id
            and has_expected_serve_tag_files_item_count
            and has_expected_serve_tag_files_manifest_path
            and has_expected_serve_tag_files_output_paths
            and has_expected_serve_tag_files_lineage_run_id
            and has_expected_serve_tag_files_lineage_root_run_id
            and has_expected_serve_tag_files_lineage_created_at
            and has_expected_serve_tag_files_lineage_parent_run_id
            and has_expected_serve_tag_files_lineage_source_manifest_path
            and serve_tag_files_replay.passed
            and not missing_serve_tag_files_replay_labels
            and has_expected_serve_tag_files_replay_pack_id
            and has_expected_serve_tag_files_replay_item_count
            and has_expected_serve_tag_files_replay_manifest_input_path
            and has_expected_serve_tag_files_replay_mode
            and has_expected_serve_tag_files_replay_manifest_candidate_count
            and has_expected_serve_tag_files_replay_manifest_selected_count
            and has_expected_serve_tag_files_replay_source_manifest_path
            and has_expected_serve_tag_files_replay_run_id
            and has_expected_serve_tag_files_replay_root_run_id
            and has_expected_serve_tag_files_replay_parent_run_id
            and has_expected_serve_tag_files_replay_created_at
            and has_expected_serve_tag_files_replay_created_at_order
            and has_expected_serve_tag_files_replay_manifest_path
            and has_expected_serve_tag_files_replay_output_paths
            and has_expected_serve_tag_files_replay_rerun_diff
            and has_expected_serve_tag_files_replay_rerun_diff_manifest_input_path
            and has_expected_serve_tag_files_replay_rerun_diff_changed_paths
            and has_expected_serve_tag_files_replay_rerun_diff_newly_processed
            and has_expected_serve_tag_files_replay_rerun_diff_reused
            and has_expected_serve_tag_files_replay_rerun_diff_repaired
            and has_expected_serve_tag_files_replay_rerun_diff_skipped
            and has_expected_remove_guardrail
            and remove.passed
            and remove_status.passed
            and has_expected_remove_status
            and has_expected_remaining_pack_ids
        )
        return ReleaseInstallSmokeResult(
            artifact_kind="npm_tarball",
            working_dir=str(working_dir),
            environment_dir=str(environment_dir),
            storage_root=str(storage_root),
            install=install,
            invoke=invoke,
            pull=pull,
            tag=tag,
            recovery_status=recovery_status,
            serve=serve,
            serve_healthz=serve_healthz,
            serve_status=serve_status,
            serve_tag=serve_tag,
            serve_tag_file=serve_tag_file,
            serve_tag_files=serve_tag_files,
            serve_tag_files_replay=serve_tag_files_replay,
            remove_guardrail=remove_guardrail,
            remove=remove,
            remove_status=remove_status,
            passed=passed,
            reported_version=reported_version,
            pulled_pack_ids=pulled_pack_ids,
            tagged_labels=tagged_labels,
            recovered_pack_ids=recovered_pack_ids,
            served_pack_ids=served_pack_ids,
            remaining_pack_ids=remaining_pack_ids,
            serve_tagged_labels=serve_tagged_labels,
            serve_tag_file_labels=serve_tag_file_labels,
            serve_tag_files_labels=serve_tag_files_labels,
            serve_tag_files_replay_labels=serve_tag_files_replay_labels,
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
    if smoke_result.pull is None or not smoke_result.pull.passed:
        exit_code = None if smoke_result.pull is None else smoke_result.pull.exit_code
        return [f"{prefix}_pull_failed:{exit_code}"]
    missing_pulled_pack_ids = _missing_smoke_pack_ids(smoke_result.pulled_pack_ids)
    if missing_pulled_pack_ids:
        return [f"{prefix}_pull_missing_pack:{','.join(missing_pulled_pack_ids)}"]
    if smoke_result.tag is None or not smoke_result.tag.passed:
        exit_code = None if smoke_result.tag is None else smoke_result.tag.exit_code
        return [f"{prefix}_tag_failed:{exit_code}"]
    missing_labels = sorted(set(SMOKE_TAG_REQUIRED_LABELS) - set(smoke_result.tagged_labels))
    if missing_labels:
        return [f"{prefix}_tag_missing_labels:{','.join(missing_labels)}"]
    if smoke_result.recovery_status is None or not smoke_result.recovery_status.passed:
        exit_code = (
            None if smoke_result.recovery_status is None else smoke_result.recovery_status.exit_code
        )
        return [f"{prefix}_recovery_status_failed:{exit_code}"]
    recovered_version = _parse_status_version(smoke_result.recovery_status)
    if recovered_version is None:
        return [f"{prefix}_recovery_status_missing_reported_version"]
    if recovered_version != expected_version:
        return [f"{prefix}_recovery_status_version_mismatch:{recovered_version}"]
    missing_recovered_pack_ids = _missing_smoke_pack_ids(smoke_result.recovered_pack_ids)
    if missing_recovered_pack_ids:
        return [
            f"{prefix}_recovery_status_missing_pack:{','.join(missing_recovered_pack_ids)}"
        ]
    if smoke_result.serve is None or not smoke_result.serve.passed:
        exit_code = None if smoke_result.serve is None else smoke_result.serve.exit_code
        return [f"{prefix}_serve_failed:{exit_code}"]
    if smoke_result.serve_healthz is None or not smoke_result.serve_healthz.passed:
        exit_code = None if smoke_result.serve_healthz is None else smoke_result.serve_healthz.exit_code
        return [f"{prefix}_serve_healthz_failed:{exit_code}"]
    if not _healthz_matches_expected(smoke_result.serve_healthz, expected_version=expected_version):
        return [f"{prefix}_serve_healthz_invalid"]
    if smoke_result.serve_status is None or not smoke_result.serve_status.passed:
        exit_code = None if smoke_result.serve_status is None else smoke_result.serve_status.exit_code
        return [f"{prefix}_serve_status_failed:{exit_code}"]
    served_version = _parse_status_version(smoke_result.serve_status)
    if served_version is None:
        return [f"{prefix}_serve_status_missing_reported_version"]
    if served_version != expected_version:
        return [f"{prefix}_serve_status_version_mismatch:{served_version}"]
    missing_served_pack_ids = _missing_smoke_pack_ids(smoke_result.served_pack_ids)
    if missing_served_pack_ids:
        return [f"{prefix}_serve_status_missing_pack:{','.join(missing_served_pack_ids)}"]
    if smoke_result.serve_tag is None or not smoke_result.serve_tag.passed:
        exit_code = None if smoke_result.serve_tag is None else smoke_result.serve_tag.exit_code
        return [f"{prefix}_serve_tag_failed:{exit_code}"]
    missing_serve_tag_labels = sorted(
        set(SMOKE_TAG_REQUIRED_LABELS) - set(smoke_result.serve_tagged_labels)
    )
    if missing_serve_tag_labels:
        return [f"{prefix}_serve_tag_missing_labels:{','.join(missing_serve_tag_labels)}"]
    if smoke_result.serve_tag_file is None or not smoke_result.serve_tag_file.passed:
        exit_code = (
            None if smoke_result.serve_tag_file is None else smoke_result.serve_tag_file.exit_code
        )
        return [f"{prefix}_serve_tag_file_failed:{exit_code}"]
    missing_serve_tag_file_labels = sorted(
        set(SMOKE_TAG_REQUIRED_LABELS) - set(smoke_result.serve_tag_file_labels)
    )
    if missing_serve_tag_file_labels:
        return [
            f"{prefix}_serve_tag_file_missing_labels:{','.join(missing_serve_tag_file_labels)}"
        ]
    if smoke_result.serve_tag_files is None or not smoke_result.serve_tag_files.passed:
        exit_code = (
            None if smoke_result.serve_tag_files is None else smoke_result.serve_tag_files.exit_code
        )
        return [f"{prefix}_serve_tag_files_failed:{exit_code}"]
    missing_serve_tag_files_labels = sorted(
        set(SMOKE_TAG_REQUIRED_LABELS) - set(smoke_result.serve_tag_files_labels)
    )
    if missing_serve_tag_files_labels:
        return [
            f"{prefix}_serve_tag_files_missing_labels:{','.join(missing_serve_tag_files_labels)}"
        ]
    serve_tag_files_pack_id = _parse_batch_pack_id(smoke_result.serve_tag_files)
    if serve_tag_files_pack_id is None:
        return [f"{prefix}_serve_tag_files_missing_pack"]
    if serve_tag_files_pack_id != SMOKE_PULL_PACK_ID:
        return [f"{prefix}_serve_tag_files_invalid_pack:{serve_tag_files_pack_id}"]
    serve_tag_files_item_count = _parse_batch_item_count(smoke_result.serve_tag_files)
    if serve_tag_files_item_count is None:
        return [f"{prefix}_serve_tag_files_missing_item_count"]
    if serve_tag_files_item_count != len(SMOKE_TAG_BATCH_FILES):
        return [
            f"{prefix}_serve_tag_files_invalid_item_count:{serve_tag_files_item_count}"
        ]
    serve_tag_files_manifest_path = _parse_batch_saved_manifest_path(smoke_result.serve_tag_files)
    if serve_tag_files_manifest_path is None:
        return [f"{prefix}_serve_tag_files_missing_manifest_path"]
    expected_serve_tag_files_manifest_path = _expected_smoke_batch_manifest_path(
        Path(smoke_result.working_dir)
    )
    if serve_tag_files_manifest_path != expected_serve_tag_files_manifest_path:
        return [f"{prefix}_serve_tag_files_invalid_manifest_path"]
    serve_tag_files_output_paths = _parse_batch_saved_output_paths(smoke_result.serve_tag_files)
    expected_serve_tag_files_output_paths = _expected_smoke_batch_output_paths(
        Path(smoke_result.working_dir)
    )
    if serve_tag_files_output_paths != expected_serve_tag_files_output_paths:
        return [f"{prefix}_serve_tag_files_invalid_output_paths"]
    if smoke_result.serve_tag_files_replay is None or not smoke_result.serve_tag_files_replay.passed:
        exit_code = (
            None
            if smoke_result.serve_tag_files_replay is None
            else smoke_result.serve_tag_files_replay.exit_code
        )
        return [f"{prefix}_serve_tag_files_replay_failed:{exit_code}"]
    missing_serve_tag_files_replay_labels = sorted(
        set(SMOKE_TAG_REQUIRED_LABELS) - set(smoke_result.serve_tag_files_replay_labels)
    )
    if missing_serve_tag_files_replay_labels:
        return [
            f"{prefix}_serve_tag_files_replay_missing_labels:"
            f"{','.join(missing_serve_tag_files_replay_labels)}"
        ]
    serve_tag_files_replay_pack_id = _parse_batch_pack_id(smoke_result.serve_tag_files_replay)
    if serve_tag_files_replay_pack_id is None:
        return [f"{prefix}_serve_tag_files_replay_missing_pack"]
    if serve_tag_files_replay_pack_id != SMOKE_PULL_PACK_ID:
        return [f"{prefix}_serve_tag_files_replay_invalid_pack:{serve_tag_files_replay_pack_id}"]
    serve_tag_files_replay_item_count = _parse_batch_item_count(smoke_result.serve_tag_files_replay)
    if serve_tag_files_replay_item_count is None:
        return [f"{prefix}_serve_tag_files_replay_missing_item_count"]
    if serve_tag_files_replay_item_count != len(SMOKE_TAG_BATCH_FILES):
        return [
            f"{prefix}_serve_tag_files_replay_invalid_item_count:"
            f"{serve_tag_files_replay_item_count}"
        ]
    serve_tag_files_replay_rerun_diff = _parse_batch_rerun_diff(
        smoke_result.serve_tag_files_replay
    )
    if serve_tag_files_replay_rerun_diff is None:
        return [f"{prefix}_serve_tag_files_replay_missing_rerun_diff"]
    serve_tag_files_replay_rerun_diff_manifest_input_path = (
        serve_tag_files_replay_rerun_diff.get("manifest_input_path")
    )
    if serve_tag_files_replay_rerun_diff_manifest_input_path is None:
        return [f"{prefix}_serve_tag_files_replay_missing_rerun_diff_manifest_input_path"]
    if (
        not isinstance(serve_tag_files_replay_rerun_diff_manifest_input_path, str)
        or not serve_tag_files_replay_rerun_diff_manifest_input_path
        or serve_tag_files_replay_rerun_diff_manifest_input_path
        != serve_tag_files_manifest_path
    ):
        return [f"{prefix}_serve_tag_files_replay_invalid_rerun_diff_manifest_input_path"]
    serve_tag_files_replay_rerun_diff_changed = serve_tag_files_replay_rerun_diff.get(
        "changed"
    )
    if serve_tag_files_replay_rerun_diff_changed is None:
        return [f"{prefix}_serve_tag_files_replay_missing_rerun_diff_changed"]
    expected_serve_tag_files_replay_rerun_diff_changed_paths = sorted(
        str((Path(smoke_result.working_dir) / file_name).resolve())
        for file_name, _ in SMOKE_TAG_BATCH_FILES
    )
    if (
        not isinstance(serve_tag_files_replay_rerun_diff_changed, list)
        or any(
            not isinstance(value, str) or not value
            for value in serve_tag_files_replay_rerun_diff_changed
        )
        or sorted(serve_tag_files_replay_rerun_diff_changed)
        != expected_serve_tag_files_replay_rerun_diff_changed_paths
    ):
        return [f"{prefix}_serve_tag_files_replay_invalid_rerun_diff_changed_paths"]
    serve_tag_files_replay_rerun_diff_newly_processed = serve_tag_files_replay_rerun_diff.get(
        "newly_processed"
    )
    if serve_tag_files_replay_rerun_diff_newly_processed is None:
        return [f"{prefix}_serve_tag_files_replay_missing_rerun_diff_newly_processed"]
    if (
        not isinstance(serve_tag_files_replay_rerun_diff_newly_processed, list)
        or any(
            not isinstance(value, str) or not value
            for value in serve_tag_files_replay_rerun_diff_newly_processed
        )
        or serve_tag_files_replay_rerun_diff_newly_processed != []
    ):
        return [f"{prefix}_serve_tag_files_replay_invalid_rerun_diff_newly_processed"]
    serve_tag_files_replay_rerun_diff_reused = serve_tag_files_replay_rerun_diff.get(
        "reused"
    )
    if serve_tag_files_replay_rerun_diff_reused is None:
        return [f"{prefix}_serve_tag_files_replay_missing_rerun_diff_reused"]
    if (
        not isinstance(serve_tag_files_replay_rerun_diff_reused, list)
        or any(
            not isinstance(value, str) or not value
            for value in serve_tag_files_replay_rerun_diff_reused
        )
        or serve_tag_files_replay_rerun_diff_reused != []
    ):
        return [f"{prefix}_serve_tag_files_replay_invalid_rerun_diff_reused"]
    serve_tag_files_replay_rerun_diff_repaired = serve_tag_files_replay_rerun_diff.get(
        "repaired"
    )
    if serve_tag_files_replay_rerun_diff_repaired is None:
        return [f"{prefix}_serve_tag_files_replay_missing_rerun_diff_repaired"]
    if (
        not isinstance(serve_tag_files_replay_rerun_diff_repaired, list)
        or any(
            not isinstance(value, str) or not value
            for value in serve_tag_files_replay_rerun_diff_repaired
        )
        or serve_tag_files_replay_rerun_diff_repaired != []
    ):
        return [f"{prefix}_serve_tag_files_replay_invalid_rerun_diff_repaired"]
    serve_tag_files_replay_rerun_diff_skipped = serve_tag_files_replay_rerun_diff.get(
        "skipped"
    )
    if serve_tag_files_replay_rerun_diff_skipped is None:
        return [f"{prefix}_serve_tag_files_replay_missing_rerun_diff_skipped"]
    if (
        not isinstance(serve_tag_files_replay_rerun_diff_skipped, list)
        or serve_tag_files_replay_rerun_diff_skipped != []
    ):
        return [f"{prefix}_serve_tag_files_replay_invalid_rerun_diff_skipped"]
    serve_tag_files_replay_manifest_input_path = _parse_batch_manifest_input_path(
        smoke_result.serve_tag_files_replay
    )
    if serve_tag_files_replay_manifest_input_path is None:
        return [f"{prefix}_serve_tag_files_replay_missing_manifest_input_path"]
    if serve_tag_files_replay_manifest_input_path != serve_tag_files_manifest_path:
        return [f"{prefix}_serve_tag_files_replay_invalid_manifest_input_path"]
    serve_tag_files_replay_mode = _parse_batch_manifest_replay_mode(
        smoke_result.serve_tag_files_replay
    )
    if serve_tag_files_replay_mode is None:
        return [f"{prefix}_serve_tag_files_replay_missing_manifest_replay_mode"]
    if serve_tag_files_replay_mode != SMOKE_TAG_BATCH_REPLAY_MODE:
        return [f"{prefix}_serve_tag_files_replay_invalid_manifest_replay_mode"]
    serve_tag_files_replay_manifest_candidate_count = _parse_batch_manifest_candidate_count(
        smoke_result.serve_tag_files_replay
    )
    if serve_tag_files_replay_manifest_candidate_count is None:
        return [f"{prefix}_serve_tag_files_replay_missing_manifest_candidate_count"]
    if serve_tag_files_replay_manifest_candidate_count != len(SMOKE_TAG_BATCH_FILES):
        return [
            f"{prefix}_serve_tag_files_replay_invalid_manifest_candidate_count:"
            f"{serve_tag_files_replay_manifest_candidate_count}"
        ]
    serve_tag_files_replay_manifest_selected_count = _parse_batch_manifest_selected_count(
        smoke_result.serve_tag_files_replay
    )
    if serve_tag_files_replay_manifest_selected_count is None:
        return [f"{prefix}_serve_tag_files_replay_missing_manifest_selected_count"]
    if serve_tag_files_replay_manifest_selected_count != len(SMOKE_TAG_BATCH_FILES):
        return [
            f"{prefix}_serve_tag_files_replay_invalid_manifest_selected_count:"
            f"{serve_tag_files_replay_manifest_selected_count}"
        ]
    serve_tag_files_replay_source_manifest_path = _parse_batch_lineage_source_manifest_path(
        smoke_result.serve_tag_files_replay
    )
    if serve_tag_files_replay_source_manifest_path is None:
        return [f"{prefix}_serve_tag_files_replay_missing_lineage_source_manifest_path"]
    if serve_tag_files_replay_source_manifest_path != serve_tag_files_manifest_path:
        return [f"{prefix}_serve_tag_files_replay_invalid_lineage_source_manifest_path"]
    serve_tag_files_lineage_run_id = _parse_batch_lineage_run_id(smoke_result.serve_tag_files)
    if serve_tag_files_lineage_run_id is None:
        return [f"{prefix}_serve_tag_files_missing_lineage_run_id"]
    serve_tag_files_replay_run_id = _parse_batch_lineage_run_id(
        smoke_result.serve_tag_files_replay
    )
    if serve_tag_files_replay_run_id is None:
        return [f"{prefix}_serve_tag_files_replay_missing_lineage_run_id"]
    if serve_tag_files_replay_run_id == serve_tag_files_lineage_run_id:
        return [f"{prefix}_serve_tag_files_replay_invalid_lineage_run_id"]
    serve_tag_files_lineage_root_run_id = _parse_batch_lineage_root_run_id(
        smoke_result.serve_tag_files
    )
    if serve_tag_files_lineage_root_run_id is None:
        return [f"{prefix}_serve_tag_files_missing_lineage_root_run_id"]
    if serve_tag_files_lineage_root_run_id != serve_tag_files_lineage_run_id:
        return [f"{prefix}_serve_tag_files_invalid_lineage_root_run_id"]
    if not _batch_lineage_field_is_unset(smoke_result.serve_tag_files, "parent_run_id"):
        return [f"{prefix}_serve_tag_files_invalid_lineage_parent_run_id"]
    if not _batch_lineage_field_is_unset(
        smoke_result.serve_tag_files,
        "source_manifest_path",
    ):
        return [f"{prefix}_serve_tag_files_invalid_lineage_source_manifest_path"]
    serve_tag_files_lineage_created_at_value = _parse_batch_lineage_value(
        smoke_result.serve_tag_files,
        "created_at",
    )
    if serve_tag_files_lineage_created_at_value is None:
        return [f"{prefix}_serve_tag_files_missing_lineage_created_at"]
    serve_tag_files_lineage_created_at = _parse_iso8601_datetime(
        serve_tag_files_lineage_created_at_value
    )
    if serve_tag_files_lineage_created_at is None:
        return [f"{prefix}_serve_tag_files_invalid_lineage_created_at"]
    serve_tag_files_replay_root_run_id = _parse_batch_lineage_root_run_id(
        smoke_result.serve_tag_files_replay
    )
    if serve_tag_files_replay_root_run_id is None:
        return [f"{prefix}_serve_tag_files_replay_missing_lineage_root_run_id"]
    if serve_tag_files_replay_root_run_id != serve_tag_files_lineage_root_run_id:
        return [f"{prefix}_serve_tag_files_replay_invalid_lineage_root_run_id"]
    serve_tag_files_replay_parent_run_id = _parse_batch_lineage_parent_run_id(
        smoke_result.serve_tag_files_replay
    )
    if serve_tag_files_replay_parent_run_id is None:
        return [f"{prefix}_serve_tag_files_replay_missing_lineage_parent_run_id"]
    if serve_tag_files_replay_parent_run_id != serve_tag_files_lineage_run_id:
        return [f"{prefix}_serve_tag_files_replay_invalid_lineage_parent_run_id"]
    serve_tag_files_replay_created_at_value = _parse_batch_lineage_value(
        smoke_result.serve_tag_files_replay,
        "created_at",
    )
    if serve_tag_files_replay_created_at_value is None:
        return [f"{prefix}_serve_tag_files_replay_missing_lineage_created_at"]
    serve_tag_files_replay_created_at = _parse_iso8601_datetime(
        serve_tag_files_replay_created_at_value
    )
    if serve_tag_files_replay_created_at is None:
        return [f"{prefix}_serve_tag_files_replay_invalid_lineage_created_at"]
    if serve_tag_files_replay_created_at <= serve_tag_files_lineage_created_at:
        return [f"{prefix}_serve_tag_files_replay_invalid_lineage_created_at_order"]
    serve_tag_files_replay_manifest_path = _parse_batch_saved_manifest_path(
        smoke_result.serve_tag_files_replay
    )
    if serve_tag_files_replay_manifest_path is None:
        return [f"{prefix}_serve_tag_files_replay_missing_manifest_path"]
    expected_serve_tag_files_replay_manifest_path = _expected_smoke_batch_manifest_path(
        Path(smoke_result.working_dir),
        replay=True,
    )
    if serve_tag_files_replay_manifest_path != expected_serve_tag_files_replay_manifest_path:
        return [f"{prefix}_serve_tag_files_replay_invalid_manifest_path"]
    serve_tag_files_replay_output_paths = _parse_batch_saved_output_paths(
        smoke_result.serve_tag_files_replay
    )
    if serve_tag_files_replay_output_paths != expected_serve_tag_files_output_paths:
        return [f"{prefix}_serve_tag_files_replay_invalid_output_paths"]
    if smoke_result.remove_guardrail is None:
        return [f"{prefix}_remove_guardrail_missing"]
    if smoke_result.remove_guardrail.passed:
        return [f"{prefix}_remove_guardrail_unexpected_success"]
    if SMOKE_REMOVE_GUARDRAIL_MESSAGE not in smoke_result.remove_guardrail.stderr:
        return [f"{prefix}_remove_guardrail_missing_dependency_message"]
    if smoke_result.remove is None or not smoke_result.remove.passed:
        exit_code = None if smoke_result.remove is None else smoke_result.remove.exit_code
        return [f"{prefix}_remove_failed:{exit_code}"]
    if smoke_result.remove_status is None or not smoke_result.remove_status.passed:
        exit_code = (
            None if smoke_result.remove_status is None else smoke_result.remove_status.exit_code
        )
        return [f"{prefix}_remove_status_failed:{exit_code}"]
    remove_status_version = _parse_status_version(smoke_result.remove_status)
    if remove_status_version is None:
        return [f"{prefix}_remove_status_missing_reported_version"]
    if remove_status_version != expected_version:
        return [f"{prefix}_remove_status_version_mismatch:{remove_status_version}"]
    if smoke_result.remaining_pack_ids != list(SMOKE_REMOVE_REMAINING_PACK_IDS):
        return [
            f"{prefix}_remove_status_invalid_remaining_packs:"
            f"{_format_smoke_pack_ids(smoke_result.remaining_pack_ids)}"
        ]
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
