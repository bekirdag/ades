"""Runtime target and metadata backend selection for ades."""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from ..packs.manifest import PackManifest
    from .paths import StorageLayout


class RuntimeTarget(str, Enum):
    """Supported high-level runtime targets."""

    LOCAL = "local"
    PRODUCTION_SERVER = "production_server"


class MetadataBackend(str, Enum):
    """Supported metadata backend families."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


class UnsupportedRuntimeConfigurationError(RuntimeError):
    """Raised when the requested runtime/backend combination is invalid."""


class MetadataStore(Protocol):
    """Protocol implemented by pack metadata stores."""

    def count_installed_packs(self) -> int: ...

    def sync_from_filesystem(self, packs_dir: object | None = None) -> None: ...

    def sync_pack_from_dir(
        self,
        pack_dir: object,
        *,
        active: bool | None = None,
    ) -> "PackManifest | None": ...

    def list_installed_packs(self, *, active_only: bool = False) -> list["PackManifest"]: ...

    def get_pack(self, pack_id: str, *, active_only: bool = False) -> "PackManifest | None": ...

    def set_pack_active(self, pack_id: str, active: bool) -> bool: ...

    def list_pack_labels(self, pack_id: str) -> list[str]: ...

    def list_pack_rules(self, pack_id: str) -> list[dict[str, str]]: ...

    def list_pack_aliases(self, pack_id: str) -> list[dict[str, str]]: ...

    def lookup_candidates(
        self,
        query: str,
        *,
        pack_id: str | None = None,
        exact_alias: bool = False,
        active_only: bool = True,
        limit: int = 20,
    ) -> list[dict[str, str | float | bool | None]]: ...


def normalize_runtime_target(value: RuntimeTarget | str | None) -> RuntimeTarget:
    """Coerce a runtime target string into the canonical enum."""

    if isinstance(value, RuntimeTarget):
        return value
    raw = str(value or RuntimeTarget.LOCAL.value).strip().lower()
    try:
        return RuntimeTarget(raw)
    except ValueError as exc:
        raise UnsupportedRuntimeConfigurationError(
            f"Unsupported ades runtime target: {value!r}"
        ) from exc


def normalize_metadata_backend(value: MetadataBackend | str | None) -> MetadataBackend:
    """Coerce a metadata backend string into the canonical enum."""

    if isinstance(value, MetadataBackend):
        return value
    raw = str(value or MetadataBackend.SQLITE.value).strip().lower()
    try:
        return MetadataBackend(raw)
    except ValueError as exc:
        raise UnsupportedRuntimeConfigurationError(
            f"Unsupported ades metadata backend: {value!r}"
        ) from exc


def build_metadata_store(
    layout: "StorageLayout",
    *,
    runtime_target: RuntimeTarget | str | None = None,
    metadata_backend: MetadataBackend | str | None = None,
    database_url: str | None = None,
) -> MetadataStore:
    """Create the store for the requested runtime target and backend."""

    resolved_target = normalize_runtime_target(runtime_target)
    resolved_backend = normalize_metadata_backend(metadata_backend)

    if resolved_target is RuntimeTarget.LOCAL and resolved_backend is MetadataBackend.SQLITE:
        from .registry_db import PackMetadataStore

        return PackMetadataStore(layout)

    if (
        resolved_target is RuntimeTarget.PRODUCTION_SERVER
        and resolved_backend is MetadataBackend.POSTGRESQL
    ):
        from .postgresql import PostgreSQLMetadataStore

        return PostgreSQLMetadataStore(layout, database_url=database_url)

    raise UnsupportedRuntimeConfigurationError(
        "Unsupported ades runtime/backend combination: "
        f"{resolved_target.value}+{resolved_backend.value}"
    )
