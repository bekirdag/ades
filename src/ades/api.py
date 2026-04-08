"""Public library helpers for ades v0.1.0."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from .config import Settings, get_settings
from .packs.installer import InstallResult, PackInstaller
from .packs.registry import PackRegistry
from .pipeline.files import TagFileSkippedEntry, discover_tag_file_sources, load_tag_file
from .pipeline.tagger import tag_text
from .service.models import (
    BatchSourceSummary,
    BatchTagResponse,
    LookupCandidate,
    LookupResponse,
    PackSummary,
    SourceFingerprint,
    StatusResponse,
    TagResponse,
)
from .storage.paths import build_storage_layout, ensure_storage_layout
from .storage.results import (
    aggregate_batch_warnings,
    build_batch_manifest_replay_plan,
    persist_batch_tag_manifest_json,
    persist_tag_response_json,
    persist_tag_responses_json,
)


def _resolve_settings(
    *,
    storage_root: str | Path | None = None,
    host: str | None = None,
    port: int | None = None,
    default_pack: str | None = None,
    registry_url: str | None = None,
) -> Settings:
    base = get_settings()
    return Settings(
        host=host or base.host,
        port=port if port is not None else base.port,
        storage_root=Path(storage_root).expanduser() if storage_root is not None else base.storage_root,
        default_pack=default_pack or base.default_pack,
        registry_url=registry_url if registry_url is not None else base.registry_url,
        runtime_target=base.runtime_target,
        metadata_backend=base.metadata_backend,
    )


def status(
    *,
    storage_root: str | Path | None = None,
    host: str | None = None,
    port: int | None = None,
) -> StatusResponse:
    """Return the current local ades runtime status."""

    settings = _resolve_settings(storage_root=storage_root, host=host, port=port)
    layout = ensure_storage_layout(build_storage_layout(settings.storage_root))
    registry = PackRegistry(
        layout.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
    )
    return StatusResponse(
        service="ades",
        version=settings_version(),
        runtime_target=settings.runtime_target.value,
        metadata_backend=settings.metadata_backend.value,
        storage_root=str(layout.storage_root),
        host=settings.host,
        port=settings.port,
        installed_packs=[pack.pack_id for pack in registry.list_installed_packs()],
    )


def list_packs(
    *,
    storage_root: str | Path | None = None,
    active_only: bool = False,
) -> list[PackSummary]:
    """List installed packs for a storage root."""

    settings = _resolve_settings(storage_root=storage_root)
    registry = PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
    )
    return [
        PackSummary(**pack.to_summary())
        for pack in registry.list_installed_packs(active_only=active_only)
    ]


def get_pack(
    pack_id: str,
    *,
    storage_root: str | Path | None = None,
    active_only: bool = False,
) -> PackSummary | None:
    """Return a single installed pack summary."""

    settings = _resolve_settings(storage_root=storage_root)
    registry = PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
    )
    manifest = registry.get_pack(pack_id, active_only=active_only)
    if manifest is None:
        return None
    return PackSummary(**manifest.to_summary())


def pull_pack(
    pack_id: str,
    *,
    storage_root: str | Path | None = None,
    registry_url: str | None = None,
) -> InstallResult:
    """Install a pack and its dependencies."""

    settings = _resolve_settings(storage_root=storage_root, registry_url=registry_url)
    installer = PackInstaller(
        settings.storage_root,
        registry_url=settings.registry_url,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
    )
    return installer.install(pack_id)


def tag(
    text: str,
    *,
    pack: str | None = None,
    content_type: str = "text/plain",
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    pretty_output: bool = True,
    storage_root: str | Path | None = None,
) -> TagResponse:
    """Run in-process tagging through the installed local runtime."""

    settings = _resolve_settings(storage_root=storage_root)
    resolved_pack = pack or settings.default_pack
    response = tag_text(
        text=text,
        pack=resolved_pack,
        content_type=content_type,
        storage_root=settings.storage_root,
    )
    if output_path is not None or output_dir is not None:
        return persist_tag_response_json(
            response,
            pack_id=resolved_pack,
            output_path=output_path,
            output_dir=output_dir,
            pretty=pretty_output,
        )
    return response


def tag_file(
    path: str | Path,
    *,
    pack: str | None = None,
    content_type: str | None = None,
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    pretty_output: bool = True,
    storage_root: str | Path | None = None,
) -> TagResponse:
    """Run in-process tagging for a local file path."""

    settings = _resolve_settings(storage_root=storage_root)
    resolved_pack = pack or settings.default_pack
    resolved_path, text, resolved_content_type, input_size_bytes, source_fingerprint = load_tag_file(
        path,
        content_type=content_type,
    )
    response = tag_text(
        text=text,
        pack=resolved_pack,
        content_type=resolved_content_type,
        storage_root=settings.storage_root,
    )
    response = response.model_copy(
        update={
            "content_type": resolved_content_type,
            "source_path": str(resolved_path),
            "input_size_bytes": input_size_bytes,
            "source_fingerprint": SourceFingerprint(**source_fingerprint.to_dict()),
        }
    )
    if output_path is not None or output_dir is not None:
        return persist_tag_response_json(
            response,
            pack_id=resolved_pack,
            output_path=output_path,
            output_dir=output_dir,
            pretty=pretty_output,
        )
    return response


def tag_files(
    paths: Iterable[str | Path] = (),
    *,
    pack: str | None = None,
    content_type: str | None = None,
    output_dir: str | Path | None = None,
    pretty_output: bool = True,
    storage_root: str | Path | None = None,
    directories: Iterable[str | Path] = (),
    glob_patterns: Iterable[str] = (),
    manifest_input_path: str | Path | None = None,
    manifest_replay_mode: str = "resume",
    skip_unchanged: bool = False,
    recursive: bool = True,
    include_patterns: Iterable[str] = (),
    exclude_patterns: Iterable[str] = (),
    max_files: int | None = None,
    max_input_bytes: int | None = None,
    write_manifest: bool = False,
    manifest_output_path: str | Path | None = None,
) -> BatchTagResponse:
    """Run in-process tagging for multiple local file paths."""

    settings = _resolve_settings(storage_root=storage_root)
    explicit_paths = [Path(path) for path in paths]
    resolved_directories = [Path(path) for path in directories]
    resolved_glob_patterns = list(glob_patterns)
    has_discovery_sources = bool(explicit_paths or resolved_directories or resolved_glob_patterns)
    replay_plan = (
        build_batch_manifest_replay_plan(
            manifest_input_path,
            mode=manifest_replay_mode,
        )
        if manifest_input_path is not None
        else None
    )
    if skip_unchanged and replay_plan is None:
        raise ValueError("skip_unchanged requires manifest_input_path.")
    resolved_pack = pack or (replay_plan.manifest.pack if replay_plan is not None else None) or settings.default_pack
    if (write_manifest or manifest_output_path is not None) and output_dir is None:
        raise ValueError("Batch manifest export requires output_dir.")
    discovery = discover_tag_file_sources(
        paths=[
            *explicit_paths,
            *(replay_plan.paths if replay_plan is not None and not has_discovery_sources else []),
        ],
        directories=resolved_directories,
        glob_patterns=resolved_glob_patterns,
        recursive=recursive,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
        max_files=max_files,
        max_input_bytes=max_input_bytes,
    )
    items: list[TagResponse] = []
    unchanged_skipped_count = 0
    for path in discovery.paths:
        manifest_content_type = (
            replay_plan.content_type_overrides.get(path)
            if replay_plan is not None
            else None
        )
        resolved_path, text, resolved_content_type, input_size_bytes, source_fingerprint = load_tag_file(
            path,
            content_type=content_type or manifest_content_type,
        )
        baseline_item = replay_plan.item_index.get(resolved_path) if replay_plan is not None else None
        if (
            skip_unchanged
            and baseline_item is not None
            and baseline_item.source_fingerprint is not None
            and baseline_item.source_fingerprint.model_dump(mode="python") == source_fingerprint.to_dict()
        ):
            unchanged_skipped_count += 1
            discovery.skipped.append(
                TagFileSkippedEntry(
                    reference=str(resolved_path),
                    reason="unchanged_since_manifest",
                    source_kind="fingerprint",
                    size_bytes=input_size_bytes,
                )
            )
            continue
        response = tag_text(
            text=text,
            pack=resolved_pack,
            content_type=resolved_content_type,
            storage_root=settings.storage_root,
        )
        items.append(
            response.model_copy(
                update={
                    "content_type": resolved_content_type,
                    "source_path": str(resolved_path),
                    "input_size_bytes": input_size_bytes,
                    "source_fingerprint": SourceFingerprint(**source_fingerprint.to_dict()),
                }
            )
        )
    if output_dir is not None:
        items = persist_tag_responses_json(
            items,
            pack_id=resolved_pack,
            output_dir=output_dir,
            pretty=pretty_output,
        )
    summary_payload = discovery.summary.to_dict()
    summary_payload["processed_count"] = len(items)
    summary_payload["processed_input_bytes"] = sum(item.input_size_bytes or 0 for item in items)
    summary_payload["skipped_count"] = len(discovery.skipped)
    summary_payload["unchanged_skipped_count"] = unchanged_skipped_count
    summary_payload["manifest_input_path"] = (
        str(Path(manifest_input_path).expanduser().resolve())
        if manifest_input_path is not None
        else None
    )
    summary_payload["manifest_replay_mode"] = manifest_replay_mode if replay_plan is not None else None
    summary_payload["manifest_candidate_count"] = (
        replay_plan.candidate_count
        if replay_plan is not None and not has_discovery_sources
        else 0
    )
    summary_payload["manifest_selected_count"] = (
        replay_plan.selected_count
        if replay_plan is not None and not has_discovery_sources
        else 0
    )
    summary = BatchSourceSummary(**summary_payload)
    warnings = aggregate_batch_warnings(items)
    if replay_plan is not None and not has_discovery_sources and replay_plan.selected_count == 0:
        warnings = sorted({*warnings, "manifest_replay_empty"})
    if replay_plan is not None and pack is not None and pack != replay_plan.manifest.pack:
        warnings = sorted({*warnings, f"manifest_pack_override:{replay_plan.manifest.pack}->{pack}"})
    if not items:
        warnings = sorted({*warnings, "no_files_processed"})
    response = BatchTagResponse(
        pack=resolved_pack,
        item_count=len(items),
        summary=summary,
        warnings=warnings,
        skipped=[entry.to_dict() for entry in discovery.skipped],
        rejected=[entry.to_dict() for entry in discovery.rejected],
        items=items,
    )
    if write_manifest or manifest_output_path is not None:
        return persist_batch_tag_manifest_json(
            response,
            pack_id=resolved_pack,
            output_dir=output_dir,
            output_path=manifest_output_path,
            pretty=pretty_output,
        )
    return response


def set_pack_active(
    pack_id: str,
    active: bool,
    *,
    storage_root: str | Path | None = None,
) -> PackSummary | None:
    """Set the active state for an installed pack and return its summary."""

    settings = _resolve_settings(storage_root=storage_root)
    registry = PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
    )
    if registry.get_pack(pack_id) is None:
        return None
    registry.set_pack_active(pack_id, active)
    manifest = registry.get_pack(pack_id)
    if manifest is None:
        return None
    return PackSummary(**manifest.to_summary())


def activate_pack(pack_id: str, *, storage_root: str | Path | None = None) -> PackSummary | None:
    """Activate an installed pack."""

    return set_pack_active(pack_id, True, storage_root=storage_root)


def deactivate_pack(pack_id: str, *, storage_root: str | Path | None = None) -> PackSummary | None:
    """Deactivate an installed pack."""

    return set_pack_active(pack_id, False, storage_root=storage_root)


def lookup_candidates(
    query: str,
    *,
    storage_root: str | Path | None = None,
    pack_id: str | None = None,
    exact_alias: bool = False,
    active_only: bool = True,
    limit: int = 20,
) -> LookupResponse:
    """Search deterministic metadata candidates from the configured local store."""

    settings = _resolve_settings(storage_root=storage_root)
    registry = PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
    )
    bounded_limit = max(1, min(limit, 100))
    candidates = [
        LookupCandidate(**item)
        for item in registry.lookup_candidates(
            query,
            pack_id=pack_id,
            exact_alias=exact_alias,
            active_only=active_only,
            limit=bounded_limit,
        )
    ]
    return LookupResponse(
        query=query,
        pack_id=pack_id,
        exact_alias=exact_alias,
        active_only=active_only,
        candidates=candidates,
    )


def create_service_app(*, storage_root: str | Path | None = None):
    """Create the FastAPI service app for direct embedding."""

    from .service.app import create_app

    return create_app(storage_root=storage_root)


def settings_version() -> str:
    """Return the current package version without widening public imports."""

    from .version import __version__

    return __version__
