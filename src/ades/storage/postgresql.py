"""Placeholder metadata backend for the future production server tool."""

from __future__ import annotations

from .paths import StorageLayout


class PostgreSQLMetadataStore:
    """Reserved placeholder for the future PostgreSQL-backed metadata store."""

    def __init__(self, layout: StorageLayout) -> None:
        self.layout = layout
        raise NotImplementedError(
            "The PostgreSQL metadata store is reserved for the future "
            "production_server tool and is not implemented in v0.1.0."
        )
