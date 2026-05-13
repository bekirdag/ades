"""Shared release manifest and gate helpers for semantic artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Any, Iterable


@dataclass(frozen=True)
class ReleaseGateResult:
    """One release-gate command execution result."""

    command: str
    exit_code: int | None
    ok: bool
    stdout_tail: str = ""
    stderr_tail: str = ""


@dataclass(frozen=True)
class ArtifactDescriptor:
    """One artifact that must be covered by a semantic release manifest."""

    kind: str
    name: str
    path: str | Path
    required: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


def utc_timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def run_release_gate_commands(
    commands: Iterable[str],
    *,
    working_dir: str | Path | None = None,
) -> tuple[bool, list[ReleaseGateResult]]:
    """Run shell release gates in order and stop at the first failure."""

    cwd = (
        str(Path(working_dir).expanduser().resolve())
        if working_dir is not None
        else None
    )
    results: list[ReleaseGateResult] = []
    passed = True
    for raw_command in commands:
        command = raw_command.strip()
        if not command:
            continue
        result = subprocess.run(  # noqa: S602
            command,
            cwd=cwd,
            shell=True,
            check=False,
            text=True,
            capture_output=True,
        )
        gate_result = ReleaseGateResult(
            command=command,
            exit_code=result.returncode,
            ok=result.returncode == 0,
            stdout_tail=result.stdout.strip()[-4000:],
            stderr_tail=result.stderr.strip()[-4000:],
        )
        results.append(gate_result)
        if not gate_result.ok:
            passed = False
            break
    return passed, results


def append_release_gate_warnings(
    gate_results: Iterable[ReleaseGateResult],
    *,
    warnings: list[str],
) -> None:
    """Append compact, existing-style release-gate warnings."""

    for result in gate_results:
        if result.ok:
            warnings.append(f"release_gate_passed:{result.command}")
            continue
        warnings.append(
            f"release_gate_failed:{result.command}:exit_code={result.exit_code}"
        )
        if result.stdout_tail:
            warnings.append(f"release_gate_stdout:{result.command}:{result.stdout_tail[-1000:]}")
        if result.stderr_tail:
            warnings.append(f"release_gate_stderr:{result.command}:{result.stderr_tail[-1000:]}")


def release_gate_results_payload(
    gate_results: Iterable[ReleaseGateResult],
) -> list[dict[str, Any]]:
    return [
        {
            "command": result.command,
            "exit_code": result.exit_code,
            "ok": result.ok,
            "stdout_tail": result.stdout_tail,
            "stderr_tail": result.stderr_tail,
        }
        for result in gate_results
    ]


def artifact_descriptor_from_mapping(value: dict[str, Any]) -> ArtifactDescriptor:
    kind = str(value.get("kind") or "").strip()
    path = value.get("path")
    if not kind:
        raise ValueError("Artifact descriptor is missing kind.")
    if not path:
        raise ValueError(f"Artifact descriptor {kind!r} is missing path.")
    name = str(value.get("name") or kind).strip() or kind
    required = value.get("required", True)
    metadata = value.get("metadata")
    return ArtifactDescriptor(
        kind=kind,
        name=name,
        path=str(path),
        required=bool(required),
        metadata=metadata if isinstance(metadata, dict) else {},
    )


def parse_artifact_descriptor(value: str) -> ArtifactDescriptor:
    """Parse kind:path or kind:name:path command-line descriptors."""

    parts = [part.strip() for part in value.split(":", 2)]
    if len(parts) == 2:
        kind, path = parts
        name = kind
    elif len(parts) == 3:
        kind, name, path = parts
    else:
        raise ValueError(
            "Artifact descriptor must be kind:path or kind:name:path."
        )
    if not kind or not path:
        raise ValueError(
            "Artifact descriptor must include non-empty kind and path."
        )
    return ArtifactDescriptor(kind=kind, name=name or kind, path=path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_directory(path: Path) -> tuple[str, int, int]:
    digest = hashlib.sha256()
    file_count = 0
    size_bytes = 0
    for child in sorted(item for item in path.rglob("*") if item.is_file()):
        rel_path = child.relative_to(path).as_posix()
        child_size = child.stat().st_size
        digest.update(rel_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(child_size).encode("ascii"))
        digest.update(b"\0")
        digest.update(_sha256_file(child).encode("ascii"))
        digest.update(b"\n")
        file_count += 1
        size_bytes += child_size
    return digest.hexdigest(), file_count, size_bytes


def inspect_artifact(descriptor: ArtifactDescriptor) -> dict[str, Any]:
    path = Path(descriptor.path).expanduser().resolve()
    payload: dict[str, Any] = {
        "kind": descriptor.kind,
        "name": descriptor.name,
        "path": str(path),
        "required": descriptor.required,
        "exists": path.exists(),
        "metadata": descriptor.metadata,
    }
    if not path.exists():
        return payload
    if path.is_file():
        payload.update(
            {
                "artifact_type": "file",
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        )
        if path.suffix.casefold() == ".json":
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    payload["json_keys"] = sorted(data.keys())[:64]
                    if "schema_version" in data:
                        payload["schema_version"] = data["schema_version"]
                    if "generated_at" in data:
                        payload["artifact_generated_at"] = data["generated_at"]
                    if "built_at" in data:
                        payload["artifact_built_at"] = data["built_at"]
            except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                payload["json_readable"] = False
        return payload
    if path.is_dir():
        sha256, file_count, size_bytes = _hash_directory(path)
        payload.update(
            {
                "artifact_type": "directory",
                "file_count": file_count,
                "size_bytes": size_bytes,
                "sha256": sha256,
            }
        )
        return payload
    payload["artifact_type"] = "other"
    return payload


def load_artifact_descriptors_from_spec(path: str | Path) -> tuple[list[ArtifactDescriptor], list[str]]:
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Artifact spec must be one JSON object.")
    raw_artifacts = payload.get("artifacts")
    if not isinstance(raw_artifacts, list):
        raise ValueError("Artifact spec must include an artifacts list.")
    artifacts = [
        artifact_descriptor_from_mapping(item)
        for item in raw_artifacts
        if isinstance(item, dict)
    ]
    rollback_instructions = payload.get("rollback_instructions")
    if not isinstance(rollback_instructions, list):
        return artifacts, []
    return artifacts, [str(item) for item in rollback_instructions]


def write_artifact_release_manifest(
    artifacts: Iterable[ArtifactDescriptor],
    *,
    output_path: str | Path,
    release_gate_commands: Iterable[str] = (),
    release_gate_working_dir: str | Path | None = None,
    require_kinds: Iterable[str] = (),
    rollback_instructions: Iterable[str] = (),
) -> dict[str, Any]:
    """Write one release manifest covering packs, graph, vector, and news artifacts."""

    resolved_artifacts = list(artifacts)
    artifact_payloads = [inspect_artifact(descriptor) for descriptor in resolved_artifacts]
    required_kinds = sorted({kind.strip() for kind in require_kinds if kind.strip()})
    present_kinds = {
        str(item.get("kind"))
        for item in artifact_payloads
        if item.get("exists") is True
    }
    missing_required_kinds = [
        kind for kind in required_kinds if kind not in present_kinds
    ]
    missing_required_artifacts = [
        item["kind"]
        for item in artifact_payloads
        if item.get("required") is True and item.get("exists") is not True
    ]
    gate_commands = [command.strip() for command in release_gate_commands if command.strip()]
    release_gate_passed, gate_results = run_release_gate_commands(
        gate_commands,
        working_dir=release_gate_working_dir,
    )
    passed = (
        not missing_required_kinds
        and not missing_required_artifacts
        and release_gate_passed
    )
    manifest = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "passed": passed,
        "artifacts": artifact_payloads,
        "required_kinds": required_kinds,
        "missing_required_kinds": missing_required_kinds,
        "missing_required_artifacts": missing_required_artifacts,
        "release_gate_commands": gate_commands,
        "release_gate_working_dir": (
            str(Path(release_gate_working_dir).expanduser().resolve())
            if release_gate_working_dir is not None
            else None
        ),
        "release_gate_passed": release_gate_passed,
        "release_gate_results": release_gate_results_payload(gate_results),
        "rollback_instructions": list(rollback_instructions),
    }
    resolved_output_path = Path(output_path).expanduser().resolve()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_output_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest
