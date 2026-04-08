"""Production FastAPI application for the PostgreSQL-backed server tool."""

from __future__ import annotations

from pathlib import Path

from ..config import get_settings
from ..storage import MetadataBackend, RuntimeTarget
from .app import create_app


def create_production_app(
    *,
    storage_root: str | Path | None = None,
    database_url: str | None = None,
):
    """Create the production-server app after validating the runtime configuration."""

    settings = get_settings()
    resolved_storage_root = (
        Path(storage_root).expanduser() if storage_root is not None else settings.storage_root
    )
    resolved_database_url = database_url if database_url is not None else settings.database_url

    if settings.runtime_target is not RuntimeTarget.PRODUCTION_SERVER:
        raise RuntimeError(
            "Production app requested without ADES_RUNTIME_TARGET=production_server."
        )
    if settings.metadata_backend is not MetadataBackend.POSTGRESQL:
        raise RuntimeError(
            "Production app requested without ADES_METADATA_BACKEND=postgresql."
        )
    if not resolved_database_url:
        raise RuntimeError(
            "Production app requested without ADES_DATABASE_URL or config.database_url."
        )

    return create_app(storage_root=resolved_storage_root)
