"""Stable local JSON persistence for tag responses."""

from __future__ import annotations

import json
from pathlib import Path
import re

from ..service.models import TagResponse

FILENAME_PART_RE = re.compile(r"[^a-z0-9-]+")


def _slug_filename_part(value: str) -> str:
    normalized = FILENAME_PART_RE.sub("-", value.casefold()).strip("-")
    return normalized or "ades"


def build_tag_output_path(
    *,
    pack_id: str,
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    source_path: str | Path | None = None,
) -> Path:
    """Resolve the final JSON output path for a tag response."""

    if output_path is not None and output_dir is not None:
        raise ValueError("Only one of output_path or output_dir may be set.")
    if output_path is None and output_dir is None:
        raise ValueError("An output path or directory is required.")

    if output_path is not None:
        resolved_path = Path(output_path).expanduser().resolve()
        if resolved_path.exists() and resolved_path.is_dir():
            raise IsADirectoryError(f"Output path is a directory: {resolved_path}")
        return resolved_path

    resolved_dir = Path(output_dir).expanduser().resolve()
    if resolved_dir.exists() and not resolved_dir.is_dir():
        raise NotADirectoryError(f"Output directory is not a directory: {resolved_dir}")
    source_stem = Path(source_path).stem if source_path is not None else "ades"
    filename = (
        f"{_slug_filename_part(source_stem)}.{_slug_filename_part(pack_id)}.ades.json"
    )
    return resolved_dir / filename


def persist_tag_response_json(
    response: TagResponse,
    *,
    pack_id: str,
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    pretty: bool = True,
) -> TagResponse:
    """Persist a tag response to a stable JSON file and return the updated response."""

    destination = build_tag_output_path(
        pack_id=pack_id,
        output_path=output_path,
        output_dir=output_dir,
        source_path=response.source_path,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)

    updated_response = response.model_copy(update={"saved_output_path": str(destination)})
    payload = json.dumps(
        updated_response.model_dump(mode="json"),
        indent=2 if pretty else None,
        ensure_ascii=False,
        sort_keys=True,
    )
    if pretty:
        payload += "\n"
    destination.write_text(payload, encoding="utf-8")
    return updated_response


def _dedupe_output_path(destination: Path, *, used_paths: set[Path]) -> Path:
    candidate = destination
    suffix = 2
    while candidate in used_paths:
        candidate = destination.with_name(f"{destination.stem}-{suffix}{destination.suffix}")
        suffix += 1
    used_paths.add(candidate)
    return candidate


def persist_tag_responses_json(
    responses: list[TagResponse],
    *,
    pack_id: str,
    output_dir: str | Path,
    pretty: bool = True,
) -> list[TagResponse]:
    """Persist multiple tag responses to unique stable JSON files."""

    used_paths: set[Path] = set()
    persisted: list[TagResponse] = []
    for response in responses:
        destination = build_tag_output_path(
            pack_id=pack_id,
            output_dir=output_dir,
            source_path=response.source_path,
        )
        unique_destination = _dedupe_output_path(destination, used_paths=used_paths)
        persisted.append(
            persist_tag_response_json(
                response,
                pack_id=pack_id,
                output_path=unique_destination,
                pretty=pretty,
            )
        )
    return persisted
