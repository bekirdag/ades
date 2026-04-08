"""Stable local JSON persistence for tag responses."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from uuid import uuid4

from ..service.models import (
    BatchManifest,
    BatchManifestItem,
    BatchRunLineage,
    BatchTagResponse,
    TagResponse,
)
from ..version import __version__

FILENAME_PART_RE = re.compile(r"[^a-z0-9-]+")
REPLAYABLE_MANIFEST_SKIP_REASONS = frozenset({"max_files_limit", "max_input_bytes_limit"})
MANIFEST_REPLAY_MODES = frozenset({"resume", "processed", "all"})


@dataclass(slots=True)
class BatchManifestReplayPlan:
    """Replay plan derived from a saved batch manifest artifact."""

    manifest: BatchManifest
    paths: list[Path]
    content_type_overrides: dict[Path, str]
    item_index: dict[Path, BatchManifestItem]
    candidate_count: int
    selected_count: int


def new_batch_run_id() -> str:
    """Return a stable unique identifier for one batch run."""

    return f"ades-run-{uuid4().hex}"


def build_batch_run_lineage(
    *,
    parent_manifest: BatchManifest | None = None,
    source_manifest_path: str | Path | None = None,
) -> BatchRunLineage:
    """Build batch lineage metadata for a root run or rerun."""

    run_id = new_batch_run_id()
    parent_lineage = parent_manifest.lineage if parent_manifest is not None else None
    root_run_id = parent_lineage.root_run_id if parent_lineage is not None else run_id
    parent_run_id = parent_lineage.run_id if parent_lineage is not None else None
    resolved_source_manifest_path = (
        str(Path(source_manifest_path).expanduser().resolve())
        if source_manifest_path is not None
        else None
    )
    return BatchRunLineage(
        run_id=run_id,
        root_run_id=root_run_id,
        parent_run_id=parent_run_id,
        source_manifest_path=resolved_source_manifest_path,
        created_at=datetime.now(timezone.utc),
    )


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


def build_batch_manifest_output_path(
    *,
    pack_id: str,
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> Path:
    """Resolve the final JSON output path for a batch manifest artifact."""

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
    filename = f"batch.{_slug_filename_part(pack_id)}.ades-manifest.json"
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


def _iter_manifest_items(manifest: BatchManifest) -> list[BatchManifestItem]:
    return [*manifest.items, *manifest.reused_items]


def _resolve_saved_output_path_status(saved_output_path: str | None) -> tuple[str | None, bool | None]:
    if saved_output_path is None:
        return None, None
    resolved_path = Path(saved_output_path).expanduser().resolve()
    return str(resolved_path), resolved_path.exists()


def verify_reused_output_items(
    items: list[BatchManifestItem],
) -> tuple[list[BatchManifestItem], int]:
    """Verify reused output references and attach deterministic warning state."""

    verified_items: list[BatchManifestItem] = []
    missing_count = 0
    for item in items:
        resolved_output_path, saved_output_exists = _resolve_saved_output_path_status(
            item.saved_output_path
        )
        warnings = set(item.warnings)
        if resolved_output_path is None:
            warnings.add(
                f"reused_output_missing_saved_output_path:{item.source_path or 'unknown'}"
            )
            missing_count += 1
        elif not saved_output_exists:
            warnings.add(f"reused_output_missing_file:{resolved_output_path}")
            missing_count += 1
        sorted_warnings = sorted(warnings)
        verified_items.append(
            item.model_copy(
                update={
                    "saved_output_path": resolved_output_path,
                    "saved_output_exists": saved_output_exists,
                    "warning_count": len(sorted_warnings),
                    "warnings": sorted_warnings,
                }
            )
        )
    return verified_items, missing_count


def build_batch_manifest_item_from_tag_response(
    response: TagResponse,
    *,
    extra_warnings: list[str] | None = None,
) -> BatchManifestItem:
    """Build one compact manifest item from a full tag response."""

    warnings = sorted({*response.warnings, *(extra_warnings or [])})
    saved_output_path, saved_output_exists = _resolve_saved_output_path_status(
        response.saved_output_path
    )
    return BatchManifestItem(
        source_path=response.source_path,
        saved_output_path=saved_output_path,
        saved_output_exists=saved_output_exists,
        content_type=response.content_type,
        input_size_bytes=response.input_size_bytes,
        source_fingerprint=response.source_fingerprint,
        warning_count=len(warnings),
        warnings=warnings,
        entity_count=len(response.entities),
        topic_count=len(response.topics),
        timing_ms=response.timing_ms,
    )


def aggregate_batch_warnings(
    responses: list[TagResponse],
    *,
    reused_items: list[BatchManifestItem] | None = None,
) -> list[str]:
    """Collect unique warnings from a batch of per-item tag responses."""

    warnings = {warning for response in responses for warning in response.warnings}
    if reused_items is not None:
        warnings.update(
            warning
            for item in reused_items
            for warning in item.warnings
        )
    return sorted(warnings)


def load_batch_manifest(path: str | Path) -> BatchManifest:
    """Load and validate a saved batch manifest artifact from disk."""

    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Batch manifest not found: {resolved_path}")
    if not resolved_path.is_file():
        raise IsADirectoryError(f"Batch manifest path is not a file: {resolved_path}")

    try:
        payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid batch manifest JSON: {resolved_path}") from exc

    try:
        return BatchManifest.model_validate(payload)
    except Exception as exc:
        raise ValueError(f"Invalid batch manifest payload: {resolved_path}") from exc


def build_batch_manifest_replay_plan(
    path: str | Path,
    *,
    mode: str = "resume",
) -> BatchManifestReplayPlan:
    """Build a deterministic replay plan from a saved batch manifest artifact."""

    if mode not in MANIFEST_REPLAY_MODES:
        raise ValueError(
            f"Invalid manifest replay mode: {mode}. Expected one of: all, processed, resume."
        )

    manifest = load_batch_manifest(path)
    paths: list[Path] = []
    content_type_overrides: dict[Path, str] = {}
    item_index: dict[Path, BatchManifestItem] = {}
    seen: set[Path] = set()
    candidate_count = 0

    for item in _iter_manifest_items(manifest):
        if item.source_path:
            item_index[Path(item.source_path).expanduser().resolve()] = item

    def add_candidate(source_path: str, *, content_type: str | None = None) -> None:
        nonlocal candidate_count
        candidate_count += 1
        resolved_path = Path(source_path).expanduser().resolve()
        if resolved_path in seen:
            return
        seen.add(resolved_path)
        paths.append(resolved_path)
        if content_type is not None:
            content_type_overrides[resolved_path] = content_type

    if mode in {"processed", "all"}:
        for item in _iter_manifest_items(manifest):
            if item.source_path:
                add_candidate(item.source_path, content_type=item.content_type)

    if mode in {"resume", "all"}:
        for skipped in manifest.skipped:
            if skipped.reason in REPLAYABLE_MANIFEST_SKIP_REASONS:
                add_candidate(skipped.reference)

    return BatchManifestReplayPlan(
        manifest=manifest,
        paths=paths,
        content_type_overrides=content_type_overrides,
        item_index=item_index,
        candidate_count=candidate_count,
        selected_count=len(paths),
    )


def build_batch_manifest(response: BatchTagResponse) -> BatchManifest:
    """Build a stable manifest payload from a batch tag response."""

    verified_reused_items, reused_output_missing_count = verify_reused_output_items(
        response.reused_items
    )
    summary = response.summary.model_copy(
        update={"reused_output_missing_count": reused_output_missing_count}
    )
    warnings = sorted(
        {
            *response.warnings,
            *aggregate_batch_warnings(response.items, reused_items=verified_reused_items),
        }
    )
    return BatchManifest(
        version=response.items[0].version if response.items else __version__,
        pack=response.pack,
        item_count=response.item_count,
        summary=summary,
        warnings=warnings,
        lineage=response.lineage,
        rerun_diff=response.rerun_diff,
        skipped=response.skipped,
        rejected=response.rejected,
        reused_items=verified_reused_items,
        items=[
            build_batch_manifest_item_from_tag_response(item)
            for item in response.items
        ],
    )


def persist_batch_tag_manifest_json(
    response: BatchTagResponse,
    *,
    pack_id: str,
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    pretty: bool = True,
) -> BatchTagResponse:
    """Persist a stable batch manifest JSON file and return the updated response."""

    destination = build_batch_manifest_output_path(
        pack_id=pack_id,
        output_path=output_path,
        output_dir=output_dir,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)

    updated_response = response.model_copy(update={"saved_manifest_path": str(destination)})
    manifest = build_batch_manifest(updated_response)
    payload = json.dumps(
        manifest.model_dump(mode="json"),
        indent=2 if pretty else None,
        ensure_ascii=False,
        sort_keys=True,
    )
    if pretty:
        payload += "\n"
    destination.write_text(payload, encoding="utf-8")
    return updated_response


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
