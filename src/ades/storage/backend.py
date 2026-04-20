"""Runtime target and metadata backend selection for ades."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Iterator, Protocol

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


class ExactExtractionBackend(str, Enum):
    """Supported exact-extraction backend lanes."""

    METADATA_STORE_EXACT_ALIAS = "metadata_store_exact_alias"
    COMPILED_MATCHER = "compiled_matcher"


class OperatorLookupBackend(str, Enum):
    """Supported operator-lookup backend lanes."""

    SQLITE_SEARCH_INDEX = "sqlite_search_index"
    POSTGRESQL_SEARCH = "postgresql_search"


@dataclass(frozen=True)
class BackendPlan:
    """Explicit backend ownership split for one runtime configuration."""

    runtime_target: RuntimeTarget
    metadata_persistence_backend: MetadataBackend
    exact_extraction_backend: ExactExtractionBackend
    operator_lookup_backend: OperatorLookupBackend


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

    def repair_pack_installation_paths(
        self,
        pack_id: str,
        *,
        install_path: str,
        manifest_path: str,
        active: bool | None = None,
    ) -> bool: ...

    def delete_pack(self, pack_id: str) -> bool: ...

    def list_pack_labels(self, pack_id: str) -> list[str]: ...

    def list_pack_rules(self, pack_id: str) -> list[dict[str, str]]: ...

    def list_pack_aliases(self, pack_id: str) -> list[dict[str, str]]: ...

    def count_pack_aliases(self, pack_id: str) -> int: ...

    def iter_pack_aliases(
        self,
        pack_id: str,
    ) -> Iterator[dict[str, str | float | bool]]: ...

    def lookup_candidates(
        self,
        query: str,
        *,
        pack_id: str | None = None,
        exact_alias: bool = False,
        fuzzy: bool = False,
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

    plan = resolve_backend_plan(
        runtime_target=runtime_target,
        metadata_backend=metadata_backend,
    )

    if (
        plan.runtime_target is RuntimeTarget.LOCAL
        and plan.metadata_persistence_backend is MetadataBackend.SQLITE
    ):
        from .registry_db import PackMetadataStore

        return PackMetadataStore(layout)

    if (
        plan.runtime_target is RuntimeTarget.PRODUCTION_SERVER
        and plan.metadata_persistence_backend is MetadataBackend.POSTGRESQL
    ):
        from .postgresql import PostgreSQLMetadataStore

        return PostgreSQLMetadataStore(layout, database_url=database_url)

    raise UnsupportedRuntimeConfigurationError(
        "Unsupported ades runtime/backend combination: "
        f"{plan.runtime_target.value}+{plan.metadata_persistence_backend.value}"
    )


def resolve_backend_plan(
    *,
    runtime_target: RuntimeTarget | str | None = None,
    metadata_backend: MetadataBackend | str | None = None,
) -> BackendPlan:
    """Resolve the current ownership split between runtime and metadata backends."""

    resolved_target = normalize_runtime_target(runtime_target)
    resolved_backend = normalize_metadata_backend(metadata_backend)

    if resolved_target is RuntimeTarget.LOCAL and resolved_backend is MetadataBackend.SQLITE:
        return BackendPlan(
            runtime_target=resolved_target,
            metadata_persistence_backend=resolved_backend,
            exact_extraction_backend=ExactExtractionBackend.COMPILED_MATCHER,
            operator_lookup_backend=OperatorLookupBackend.SQLITE_SEARCH_INDEX,
        )

    if (
        resolved_target is RuntimeTarget.PRODUCTION_SERVER
        and resolved_backend is MetadataBackend.POSTGRESQL
    ):
        return BackendPlan(
            runtime_target=resolved_target,
            metadata_persistence_backend=resolved_backend,
            exact_extraction_backend=ExactExtractionBackend.COMPILED_MATCHER,
            operator_lookup_backend=OperatorLookupBackend.POSTGRESQL_SEARCH,
        )

    raise UnsupportedRuntimeConfigurationError(
        "Unsupported ades runtime/backend combination: "
        f"{resolved_target.value}+{resolved_backend.value}"
    )
