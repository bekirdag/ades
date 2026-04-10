"""Deterministic source-lock helpers for normalized bundle builders."""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse


def build_bundle_source_entry(
    *,
    name: str,
    snapshot_path: str | Path,
    record_count: int,
    adapter: str,
    license_class: str = "ship-now",
    license_name: str = "operator-supplied",
    adapter_version: str = "1",
    version: str | None = None,
    extra_notes: str | None = None,
) -> dict[str, Any]:
    """Build one normalized bundle source entry with stable adapter metadata."""

    resolved_path = Path(snapshot_path).expanduser().resolve()
    notes = [f"adapter={adapter}", f"adapter_version={adapter_version}"]
    if extra_notes:
        notes.append(extra_notes)
    payload: dict[str, Any] = {
        "name": name,
        "snapshot_uri": resolved_path.as_uri(),
        "license_class": license_class,
        "license": license_name,
        "retrieved_at": _path_timestamp(resolved_path),
        "record_count": record_count,
        "notes": ";".join(notes),
    }
    if version is not None:
        payload["version"] = version
    return payload


def write_sources_lock(
    *,
    bundle_dir: str | Path,
    pack_id: str,
    version: str,
    entities_path: str,
    rules_path: str,
    sources: list[dict[str, Any]],
) -> Path:
    """Write deterministic `sources.lock.json` metadata for a bundle build."""

    resolved_bundle_dir = Path(bundle_dir).expanduser().resolve()
    lock_path = resolved_bundle_dir / "sources.lock.json"
    payload = {
        "schema_version": 1,
        "pack_id": pack_id,
        "version": version,
        "bundle_manifest_path": "bundle.json",
        "entities_path": entities_path,
        "rules_path": rules_path,
        "source_count": len(sources),
        "sources": [_build_lock_source_entry(item) for item in sources],
    }
    lock_path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    return lock_path


def _build_lock_source_entry(source: dict[str, Any]) -> dict[str, Any]:
    snapshot_uri = str(source["snapshot_uri"])
    snapshot_path = _path_from_snapshot_uri(snapshot_uri)
    note_metadata = _parse_source_notes(
        str(source["notes"]) if source.get("notes") is not None else None
    )
    payload: dict[str, Any] = {
        "name": str(source["name"]),
        "snapshot_uri": snapshot_uri,
        "snapshot_sha256": _sha256_file(snapshot_path),
        "snapshot_size_bytes": snapshot_path.stat().st_size,
        "license_class": str(source["license_class"]),
        "license": str(source["license"]) if source.get("license") is not None else None,
        "retrieved_at": (
            str(source["retrieved_at"]) if source.get("retrieved_at") is not None else None
        ),
        "record_count": (
            int(source["record_count"]) if source.get("record_count") is not None else None
        ),
        "adapter": note_metadata.get("adapter"),
        "adapter_version": note_metadata.get("adapter_version", "1"),
        "notes": str(source["notes"]) if source.get("notes") is not None else None,
    }
    if source.get("version") is not None:
        payload["version"] = str(source["version"])
    return {key: value for key, value in payload.items() if value is not None}


def _parse_source_notes(notes: str | None) -> dict[str, str]:
    if not notes:
        return {}
    metadata: dict[str, str] = {}
    for part in notes.replace(",", ";").split(";"):
        fragment = part.strip()
        if not fragment or "=" not in fragment:
            continue
        key, value = fragment.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and value:
            metadata[key] = value
    return metadata


def _path_from_snapshot_uri(snapshot_uri: str) -> Path:
    parsed = urlparse(snapshot_uri)
    if parsed.scheme not in ("", "file"):
        raise ValueError(
            f"Unsupported snapshot_uri scheme for sources.lock.json: {snapshot_uri}"
        )
    if parsed.scheme == "":
        return Path(snapshot_uri).expanduser().resolve()
    path_text = unquote(parsed.path)
    if parsed.netloc:
        path_text = f"//{parsed.netloc}{path_text}"
    return Path(path_text).resolve()


def _path_timestamp(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat().replace(
        "+00:00", "Z"
    )


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()
