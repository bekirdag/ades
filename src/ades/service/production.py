"""Placeholder service entrypoint for the future production server tool."""

from __future__ import annotations

from pathlib import Path


def create_production_app(*, storage_root: str | Path | None = None):
    """Reserved entrypoint for the future PostgreSQL-backed production server."""

    raise NotImplementedError(
        "The PostgreSQL-backed production server is planned for a later phase. "
        "Use the local SQLite tool in v0.1.0."
    )
