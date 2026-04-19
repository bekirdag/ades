"""Local HTTP client helpers for daemon-first tagging."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any
import urllib.error
import urllib.request

from ..config import DEFAULT_STORAGE_ROOT, Settings, get_settings
from .models import BatchTagResponse, TagResponse

SERVICE_MODE_ENV = "ADES_TAG_SERVICE_MODE"
SERVICE_AUTOSTART_ENV = "ADES_TAG_AUTOSTART_SERVICE"
_SERVICE_MODE_AUTO = "auto"
_SERVICE_MODE_IN_PROCESS = "in_process"
_SERVICE_MODE_SERVICE = "service"
_REQUEST_TIMEOUT_SECONDS = 3600.0
_STARTUP_TIMEOUT_SECONDS = 8.0


class LocalServiceUnavailableError(RuntimeError):
    """Raised when the local HTTP service cannot be reached."""


class LocalServiceRequestError(RuntimeError):
    """Raised when the local HTTP service rejects one tag request."""

    def __init__(self, detail: str, *, status_code: int | None = None) -> None:
        super().__init__(detail)
        self.status_code = status_code


def _normalize_service_mode(value: str | None) -> str:
    raw = str(value or _SERVICE_MODE_AUTO).strip().lower()
    if raw in {_SERVICE_MODE_AUTO, _SERVICE_MODE_IN_PROCESS, _SERVICE_MODE_SERVICE}:
        return raw
    raise ValueError(
        f"Unsupported {SERVICE_MODE_ENV} value: {value!r}. Use auto, service, or in_process."
    )


def _parse_bool_env(value: str | None, *, default: bool) -> bool:
    if value is None:
        return default
    raw = value.strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"Unsupported {SERVICE_AUTOSTART_ENV} value: {value!r}. Use true/false."
    )


def _client_host(host: str) -> str:
    raw = host.strip()
    if raw in {"", "0.0.0.0", "::", "[::]"}:
        return "127.0.0.1"
    return raw


def service_base_url(settings: Settings | None = None) -> str:
    """Return the localhost base URL for the configured service."""

    resolved = settings or get_settings()
    return f"http://{_client_host(resolved.host)}:{resolved.port}"


def should_use_local_service(settings: Settings | None = None) -> bool:
    """Return whether CLI tagging should prefer the warm local service."""

    resolved = settings or get_settings()
    mode = _normalize_service_mode(os.environ.get(SERVICE_MODE_ENV))
    if mode == _SERVICE_MODE_SERVICE:
        return True
    if mode == _SERVICE_MODE_IN_PROCESS:
        return False
    if resolved.storage_root.expanduser().resolve() != DEFAULT_STORAGE_ROOT.expanduser().resolve():
        return False
    return True


def probe_local_service(
    settings: Settings | None = None,
    *,
    timeout_seconds: float = 0.5,
) -> bool:
    """Return whether the configured local service responds on /healthz."""

    url = f"{service_base_url(settings)}/healthz"
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            if not 200 <= response.status < 300:
                return False
            body = response.read().decode("utf-8", errors="replace")
    except Exception:
        return False
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return False
    return isinstance(payload, dict) and payload.get("status") == "ok"


def _spawn_local_service(settings: Settings) -> subprocess.Popen[bytes]:
    """Start one detached local service process in the current Python environment."""

    command = [
        sys.executable,
        "-m",
        "ades",
        "serve",
        "--host",
        settings.host,
        "--port",
        str(settings.port),
    ]
    popen_kwargs: dict[str, Any] = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "close_fds": True,
        "env": dict(os.environ),
    }
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        if creationflags:
            popen_kwargs["creationflags"] = creationflags
    else:
        popen_kwargs["start_new_session"] = True
    return subprocess.Popen(command, **popen_kwargs)


def ensure_local_service(
    settings: Settings | None = None,
    *,
    startup_timeout_seconds: float = _STARTUP_TIMEOUT_SECONDS,
) -> None:
    """Ensure the configured local service is responding, auto-starting when allowed."""

    resolved = settings or get_settings()
    if probe_local_service(resolved):
        return
    if not _parse_bool_env(os.environ.get(SERVICE_AUTOSTART_ENV), default=True):
        raise LocalServiceUnavailableError(
            f"ades local service is unavailable at {service_base_url(resolved)}."
        )

    process = _spawn_local_service(resolved)
    deadline = time.monotonic() + startup_timeout_seconds
    while time.monotonic() < deadline:
        if probe_local_service(resolved):
            return
        if process.poll() is not None:
            break
        time.sleep(0.25)
    if probe_local_service(resolved):
        return
    raise LocalServiceUnavailableError(
        f"ades local service did not become ready at {service_base_url(resolved)}."
    )


def _output_payload(
    *,
    path: str | Path | None = None,
    directory: str | Path | None = None,
    manifest_path: str | Path | None = None,
    write_manifest: bool = False,
    pretty: bool = True,
) -> dict[str, Any] | None:
    if (
        path is None
        and directory is None
        and manifest_path is None
        and not write_manifest
        and pretty
    ):
        return None
    payload: dict[str, Any] = {"pretty": pretty, "write_manifest": write_manifest}
    if path is not None:
        payload["path"] = str(Path(path).expanduser())
    if directory is not None:
        payload["directory"] = str(Path(directory).expanduser())
    if manifest_path is not None:
        payload["manifest_path"] = str(Path(manifest_path).expanduser())
    return payload


def _request_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout_seconds: float = _REQUEST_TIMEOUT_SECONDS,
) -> Any:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8") if payload is not None else None,
        headers={"Content-Type": "application/json"} if payload is not None else {},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = _extract_error_detail(exc.read().decode("utf-8", errors="replace"), fallback=str(exc))
        if exc.code == 404:
            raise FileNotFoundError(detail) from exc
        if exc.code in {400, 422}:
            raise ValueError(detail) from exc
        raise LocalServiceRequestError(detail, status_code=int(exc.code)) from exc
    except Exception as exc:
        raise LocalServiceUnavailableError(str(exc)) from exc
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise LocalServiceRequestError(
            f"ades local service returned invalid JSON from {url}."
        ) from exc


def _extract_error_detail(body: str, *, fallback: str) -> str:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return body.strip() or fallback
    if isinstance(payload, dict):
        detail = payload.get("detail")
        if isinstance(detail, str) and detail.strip():
            return detail.strip()
        if detail is not None:
            return json.dumps(detail, ensure_ascii=True)
    return body.strip() or fallback


def _options_payload(
    *,
    debug: bool = False,
    hybrid: bool | None = None,
    include_related_entities: bool = False,
    include_graph_support: bool = False,
    refine_links: bool = False,
    refinement_depth: str = "light",
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if debug:
        payload["debug"] = True
    if hybrid is not None:
        payload["hybrid"] = hybrid
    if include_related_entities:
        payload["include_related_entities"] = True
    if include_graph_support:
        payload["include_graph_support"] = True
    if refine_links:
        payload["refine_links"] = True
    if refinement_depth != "light":
        payload["refinement_depth"] = refinement_depth
    return payload


def tag_via_local_service(
    text: str,
    *,
    pack: str,
    content_type: str = "text/plain",
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    pretty_output: bool = True,
    settings: Settings | None = None,
    debug: bool = False,
    hybrid: bool | None = None,
    include_related_entities: bool = False,
    include_graph_support: bool = False,
    refine_links: bool = False,
    refinement_depth: str = "light",
) -> TagResponse:
    """Tag inline text through the warm local HTTP service."""

    resolved = settings or get_settings()
    ensure_local_service(resolved)
    payload: dict[str, Any] = {
        "text": text,
        "pack": pack,
        "content_type": content_type,
        "options": _options_payload(
            debug=debug,
            hybrid=hybrid,
            include_related_entities=include_related_entities,
            include_graph_support=include_graph_support,
            refine_links=refine_links,
            refinement_depth=refinement_depth,
        ),
    }
    output = _output_payload(path=output_path, directory=output_dir, pretty=pretty_output)
    if output is not None:
        payload["output"] = output
    response = _request_json("POST", f"{service_base_url(resolved)}/v0/tag", payload=payload)
    return TagResponse.model_validate(response)


def tag_file_via_local_service(
    path: str | Path,
    *,
    pack: str,
    content_type: str | None = None,
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    pretty_output: bool = True,
    settings: Settings | None = None,
    debug: bool = False,
    hybrid: bool | None = None,
    include_related_entities: bool = False,
    include_graph_support: bool = False,
    refine_links: bool = False,
    refinement_depth: str = "light",
) -> TagResponse:
    """Tag one local file through the warm local HTTP service."""

    resolved = settings or get_settings()
    ensure_local_service(resolved)
    payload: dict[str, Any] = {
        "path": str(Path(path).expanduser()),
        "pack": pack,
        "options": _options_payload(
            debug=debug,
            hybrid=hybrid,
            include_related_entities=include_related_entities,
            include_graph_support=include_graph_support,
            refine_links=refine_links,
            refinement_depth=refinement_depth,
        ),
    }
    if content_type is not None:
        payload["content_type"] = content_type
    output = _output_payload(path=output_path, directory=output_dir, pretty=pretty_output)
    if output is not None:
        payload["output"] = output
    response = _request_json("POST", f"{service_base_url(resolved)}/v0/tag/file", payload=payload)
    return TagResponse.model_validate(response)


def tag_files_via_local_service(
    paths: list[str | Path],
    *,
    pack: str | None = None,
    content_type: str | None = None,
    output_dir: str | Path | None = None,
    pretty_output: bool = True,
    settings: Settings | None = None,
    directories: list[str | Path] | None = None,
    glob_patterns: list[str] | None = None,
    manifest_input_path: str | Path | None = None,
    manifest_replay_mode: str = "resume",
    skip_unchanged: bool = False,
    reuse_unchanged_outputs: bool = False,
    repair_missing_reused_outputs: bool = False,
    recursive: bool = True,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    max_files: int | None = None,
    max_input_bytes: int | None = None,
    write_manifest: bool = False,
    manifest_output_path: str | Path | None = None,
    debug: bool = False,
    hybrid: bool | None = None,
    include_related_entities: bool = False,
    include_graph_support: bool = False,
    refine_links: bool = False,
    refinement_depth: str = "light",
) -> BatchTagResponse:
    """Tag multiple local files through the warm local HTTP service."""

    resolved = settings or get_settings()
    ensure_local_service(resolved)
    payload: dict[str, Any] = {
        "paths": [str(Path(path).expanduser()) for path in paths],
        "directories": [str(Path(path).expanduser()) for path in (directories or [])],
        "glob_patterns": list(glob_patterns or []),
        "manifest_replay_mode": manifest_replay_mode,
        "skip_unchanged": skip_unchanged,
        "reuse_unchanged_outputs": reuse_unchanged_outputs,
        "repair_missing_reused_outputs": repair_missing_reused_outputs,
        "recursive": recursive,
        "include_patterns": list(include_patterns or []),
        "exclude_patterns": list(exclude_patterns or []),
        "options": _options_payload(
            debug=debug,
            hybrid=hybrid,
            include_related_entities=include_related_entities,
            include_graph_support=include_graph_support,
            refine_links=refine_links,
            refinement_depth=refinement_depth,
        ),
    }
    if pack is not None:
        payload["pack"] = pack
    if content_type is not None:
        payload["content_type"] = content_type
    if manifest_input_path is not None:
        payload["manifest_input_path"] = str(Path(manifest_input_path).expanduser())
    if max_files is not None:
        payload["max_files"] = max_files
    if max_input_bytes is not None:
        payload["max_input_bytes"] = max_input_bytes
    output = _output_payload(
        directory=output_dir,
        manifest_path=manifest_output_path,
        write_manifest=write_manifest,
        pretty=pretty_output,
    )
    if output is not None:
        payload["output"] = output
    response = _request_json(
        "POST",
        f"{service_base_url(resolved)}/v0/tag/files",
        payload=payload,
    )
    return BatchTagResponse.model_validate(response)
