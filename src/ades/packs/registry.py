"""Installed pack discovery."""

from __future__ import annotations

import json
from pathlib import Path

from .fetch import normalize_source_url, read_text
from ..storage.backend import (
    MetadataBackend,
    RuntimeTarget,
    build_metadata_store,
    normalize_metadata_backend,
    normalize_runtime_target,
)
from ..storage.paths import build_storage_layout, ensure_storage_layout
from .manifest import PackManifest, RegistryIndex


class PackRegistry:
    """Read-only view of locally installed pack metadata."""

    def __init__(
        self,
        storage_root: Path,
        *,
        runtime_target: RuntimeTarget | str = RuntimeTarget.LOCAL,
        metadata_backend: MetadataBackend | str = MetadataBackend.SQLITE,
        database_url: str | None = None,
    ) -> None:
        self.layout = ensure_storage_layout(build_storage_layout(storage_root))
        self.runtime_target = normalize_runtime_target(runtime_target)
        self.metadata_backend = normalize_metadata_backend(metadata_backend)
        self.store = build_metadata_store(
            self.layout,
            runtime_target=self.runtime_target,
            metadata_backend=self.metadata_backend,
            database_url=database_url,
        )
        self._bootstrap_if_needed()

    def list_installed_packs(self, *, active_only: bool = False) -> list[PackManifest]:
        """Return all installed packs that have a manifest."""

        packs = self.store.list_installed_packs(active_only=active_only)
        known_packs = packs if not active_only else self.store.list_installed_packs(active_only=False)
        if self._sync_missing_filesystem_packs(known_packs):
            return self.store.list_installed_packs(active_only=active_only)
        if packs or self.store.count_installed_packs() > 0:
            return packs
        self.store.sync_from_filesystem(self.layout.packs_dir)
        return self.store.list_installed_packs(active_only=active_only)

    def get_pack(self, pack_id: str, *, active_only: bool = False) -> PackManifest | None:
        """Return a single pack if it is installed."""

        pack = self.store.get_pack(pack_id, active_only=active_only)
        if pack is not None:
            return pack
        if active_only and self.store.get_pack(pack_id, active_only=False) is not None:
            return None
        pack_dir = self.layout.packs_dir / pack_id
        if not (pack_dir / "manifest.json").exists():
            return None
        self.store.sync_pack_from_dir(pack_dir)
        return self.store.get_pack(pack_id, active_only=active_only)

    def pack_exists(self, pack_id: str, *, active_only: bool = False) -> bool:
        """Check whether a pack manifest is present."""

        return self.get_pack(pack_id, active_only=active_only) is not None

    def sync_pack_from_disk(self, pack_id: str, *, active: bool | None = None) -> bool:
        """Refresh one installed pack from its on-disk metadata when present."""

        pack_dir = self.layout.packs_dir / pack_id
        return self.store.sync_pack_from_dir(pack_dir, active=active) is not None

    def set_pack_active(self, pack_id: str, active: bool) -> bool:
        """Mark a pack active or inactive in local metadata."""

        return self.store.set_pack_active(pack_id, active)

    def list_pack_labels(self, pack_id: str) -> list[str]:
        """Return indexed label entries for a pack."""

        return self.store.list_pack_labels(pack_id)

    def list_pack_rules(self, pack_id: str) -> list[dict[str, str]]:
        """Return indexed deterministic rules for a pack."""

        return self.store.list_pack_rules(pack_id)

    def list_pack_aliases(self, pack_id: str) -> list[dict[str, str]]:
        """Return indexed aliases for a pack."""

        return self.store.list_pack_aliases(pack_id)

    def lookup_candidates(
        self,
        query: str,
        *,
        pack_id: str | None = None,
        exact_alias: bool = False,
        active_only: bool = True,
        limit: int = 20,
    ) -> list[dict[str, str | float | bool | None]]:
        """Return deterministic metadata candidates from the configured store."""

        candidates = self.store.lookup_candidates(
            query,
            pack_id=pack_id,
            exact_alias=exact_alias,
            active_only=active_only,
            limit=limit,
        )
        if candidates:
            return candidates
        if pack_id is not None:
            if self.get_pack(pack_id, active_only=active_only) is None:
                return []
        else:
            known_packs = self.store.list_installed_packs(active_only=False)
            if not self._sync_missing_filesystem_packs(known_packs):
                return []
        return self.store.lookup_candidates(
            query,
            pack_id=pack_id,
            exact_alias=exact_alias,
            active_only=active_only,
            limit=limit,
        )

    def _bootstrap_if_needed(self) -> None:
        if self.store.count_installed_packs() == 0:
            self.store.sync_from_filesystem(self.layout.packs_dir)

    def _sync_missing_filesystem_packs(self, packs: list[PackManifest]) -> bool:
        if not self.layout.packs_dir.exists():
            return False
        known_ids = {pack.pack_id for pack in packs}
        repaired = False
        for manifest_path in sorted(self.layout.packs_dir.glob("*/manifest.json")):
            pack_dir = manifest_path.parent
            if pack_dir.name in known_ids:
                continue
            self.store.sync_pack_from_dir(pack_dir)
            repaired = True
        return repaired


def default_registry_url() -> str:
    """Return the bundled development registry URL."""

    index_path = (
        Path(__file__).resolve().parent.parent / "resources" / "registry" / "index.json"
    )
    return index_path.resolve().as_uri()


def load_registry_index(source: str | Path | None = None) -> RegistryIndex:
    """Load the static registry index from a URL or path."""

    source_url = normalize_source_url(source or default_registry_url())
    return RegistryIndex.from_dict(
        json.loads(read_text(source_url)),
        base_url=source_url,
    )
