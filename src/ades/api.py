"""Public library helpers for ades v0.1.0."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from .config import Settings, get_settings
from .packs.installer import InstallResult, PackInstaller
from .packs.registry import PackRegistry
from .pipeline.files import discover_tag_file_sources, load_tag_file, load_tag_files
from .pipeline.tagger import tag_text
from .service.models import (
    BatchSourceSummary,
    BatchTagResponse,
    LookupCandidate,
    LookupResponse,
    PackSummary,
    StatusResponse,
    TagResponse,
)
from .storage.paths import build_storage_layout, ensure_storage_layout
from .storage.results import persist_tag_response_json, persist_tag_responses_json


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
    resolved_path, text, resolved_content_type = load_tag_file(path, content_type=content_type)
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
    recursive: bool = True,
    include_patterns: Iterable[str] = (),
    exclude_patterns: Iterable[str] = (),
) -> BatchTagResponse:
    """Run in-process tagging for multiple local file paths."""

    settings = _resolve_settings(storage_root=storage_root)
    resolved_pack = pack or settings.default_pack
    discovery = discover_tag_file_sources(
        paths=paths,
        directories=directories,
        glob_patterns=glob_patterns,
        recursive=recursive,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
    )
    loaded_files = load_tag_files(discovery.paths, content_type=content_type)
    items: list[TagResponse] = []
    for resolved_path, text, resolved_content_type in loaded_files:
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
    return BatchTagResponse(
        pack=resolved_pack,
        item_count=len(items),
        summary=BatchSourceSummary(**discovery.summary.to_dict()),
        items=items,
    )


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
