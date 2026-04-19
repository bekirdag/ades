"""Installed pack discovery."""

from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import shutil

from .fetch import normalize_source_url, read_text
from ..storage.backend import (
    MetadataBackend,
    RuntimeTarget,
    build_metadata_store,
    normalize_metadata_backend,
    normalize_runtime_target,
    resolve_backend_plan,
)
from ..storage.paths import build_storage_layout, ensure_storage_layout, iter_pack_manifest_paths
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
        self.backend_plan = resolve_backend_plan(
            runtime_target=self.runtime_target,
            metadata_backend=self.metadata_backend,
        )
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
        if not self._allows_implicit_metadata_repairs():
            return self._merge_visible_filesystem_packs(packs, active_only=active_only)
        known_packs = packs if not active_only else self.store.list_installed_packs(active_only=False)
        if self._sync_missing_filesystem_packs(known_packs):
            return self.store.list_installed_packs(active_only=active_only)
        repaired_packs = self._repair_noncanonical_pack_metadata_batch(
            packs,
            active_only=active_only,
        )
        if repaired_packs is not None:
            return repaired_packs
        if packs or self.store.count_installed_packs() > 0:
            return packs
        self.store.sync_from_filesystem(self.layout.packs_dir)
        return self.store.list_installed_packs(active_only=active_only)

    def get_pack(self, pack_id: str, *, active_only: bool = False) -> PackManifest | None:
        """Return a single pack if it is installed."""

        pack = self.store.get_pack(pack_id, active_only=active_only)
        if pack is not None:
            if self._allows_implicit_metadata_repairs():
                repaired_pack, repaired = self._repair_noncanonical_pack_metadata(pack)
                if repaired:
                    refreshed = self.store.get_pack(pack_id, active_only=active_only)
                    if refreshed is not None:
                        return refreshed
                return repaired_pack
            return pack
        if active_only and self.store.get_pack(pack_id, active_only=False) is not None:
            return None
        pack_dir = self.layout.packs_dir / pack_id
        if not (pack_dir / "manifest.json").exists():
            return None
        manifest = PackManifest.load(pack_dir / "manifest.json")
        if active_only and not manifest.active:
            return None
        return manifest

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

    def remove_pack(self, pack_id: str) -> bool:
        """Remove one installed pack from metadata and disk."""

        pack_dir = self.layout.packs_dir / pack_id
        removed_from_store = self.store.delete_pack(pack_id)
        removed_from_disk = False
        if pack_dir.exists():
            shutil.rmtree(pack_dir)
            removed_from_disk = True
        return removed_from_store or removed_from_disk

    def list_pack_labels(self, pack_id: str) -> list[str]:
        """Return indexed label entries for a pack."""

        return self.store.list_pack_labels(pack_id)

    def list_pack_rules(self, pack_id: str) -> list[dict[str, str]]:
        """Return indexed deterministic rules for a pack."""

        return self.store.list_pack_rules(pack_id)

    def list_pack_aliases(self, pack_id: str) -> list[dict[str, str | float | bool]]:
        """Return indexed aliases for a pack."""

        return self.store.list_pack_aliases(pack_id)

    def lookup_candidates(
        self,
        query: str,
        *,
        pack_id: str | None = None,
        exact_alias: bool = False,
        fuzzy: bool = False,
        active_only: bool = True,
        limit: int = 20,
    ) -> list[dict[str, str | float | bool | None]]:
        """Return deterministic metadata candidates from the configured store."""

        candidates = self.store.lookup_candidates(
            query,
            pack_id=pack_id,
            exact_alias=exact_alias,
            fuzzy=fuzzy,
            active_only=active_only,
            limit=limit,
        )
        if candidates:
            return candidates
        if not self._allows_implicit_metadata_repairs():
            if pack_id is not None and self.get_pack(pack_id, active_only=active_only) is None:
                return []
            return []
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
            fuzzy=fuzzy,
            active_only=active_only,
            limit=limit,
        )

    def _bootstrap_if_needed(self) -> None:
        if not self._allows_implicit_metadata_repairs():
            return
        if self.store.count_installed_packs() == 0:
            self.store.sync_from_filesystem(self.layout.packs_dir)

    def _allows_implicit_metadata_repairs(self) -> bool:
        return self.metadata_backend == MetadataBackend.SQLITE

    def _merge_visible_filesystem_packs(
        self,
        packs: list[PackManifest],
        *,
        active_only: bool,
    ) -> list[PackManifest]:
        manifests_by_id = {pack.pack_id: pack for pack in packs}
        if self.layout.packs_dir.exists():
            for manifest_path in iter_pack_manifest_paths(self.layout.packs_dir):
                manifest = PackManifest.load(manifest_path)
                if active_only and not manifest.active:
                    continue
                manifests_by_id.setdefault(manifest.pack_id, manifest)
        return sorted(manifests_by_id.values(), key=lambda pack: pack.pack_id)

    def _sync_missing_filesystem_packs(self, packs: list[PackManifest]) -> bool:
        if not self.layout.packs_dir.exists():
            return False
        known_ids = {pack.pack_id for pack in packs}
        repaired = False
        for manifest_path in iter_pack_manifest_paths(self.layout.packs_dir):
            pack_dir = manifest_path.parent
            if pack_dir.name in known_ids:
                continue
            self.store.sync_pack_from_dir(pack_dir)
            repaired = True
        return repaired

    def _repair_noncanonical_pack_metadata_batch(
        self,
        packs: list[PackManifest],
        *,
        active_only: bool,
    ) -> list[PackManifest] | None:
        repaired_any = False
        for pack in packs:
            _, repaired = self._repair_noncanonical_pack_metadata(pack)
            repaired_any = repaired_any or repaired
        if not repaired_any:
            return None
        return self.store.list_installed_packs(active_only=active_only)

    def _repair_noncanonical_pack_metadata(
        self,
        pack: PackManifest,
    ) -> tuple[PackManifest, bool]:
        canonical_pack_dir = self.layout.packs_dir / pack.pack_id
        canonical_manifest_path = canonical_pack_dir / "manifest.json"
        if not canonical_manifest_path.exists():
            return pack, False
        if self._paths_match(pack.install_path, canonical_pack_dir) and self._paths_match(
            pack.manifest_path,
            canonical_manifest_path,
        ):
            return pack, False
        canonical_manifest = PackManifest.load(canonical_manifest_path)
        if canonical_manifest.pack_id != pack.pack_id:
            return pack, False
        if self._can_repair_pack_paths_without_full_sync(pack, canonical_manifest):
            repair_paths = getattr(self.store, "repair_pack_installation_paths", None)
            if callable(repair_paths) and repair_paths(
                pack.pack_id,
                install_path=str(canonical_pack_dir),
                manifest_path=str(canonical_manifest_path),
                active=pack.active,
            ):
                return (
                    replace(
                        pack,
                        install_path=str(canonical_pack_dir),
                        manifest_path=str(canonical_manifest_path),
                    ),
                    True,
                )
        refreshed = self.store.sync_pack_from_dir(canonical_pack_dir, active=pack.active)
        return refreshed or pack, True

    @staticmethod
    def _can_repair_pack_paths_without_full_sync(
        stored_pack: PackManifest,
        canonical_manifest: PackManifest,
    ) -> bool:
        return (
            stored_pack.pack_id == canonical_manifest.pack_id
            and stored_pack.schema_version == canonical_manifest.schema_version
            and stored_pack.version == canonical_manifest.version
            and stored_pack.language == canonical_manifest.language
            and stored_pack.domain == canonical_manifest.domain
            and stored_pack.tier == canonical_manifest.tier
            and stored_pack.min_ades_version == canonical_manifest.min_ades_version
            and (stored_pack.sha256 or None) == (canonical_manifest.sha256 or None)
        )

    @staticmethod
    def _paths_match(stored_path: str | Path | None, expected_path: Path) -> bool:
        if stored_path is None:
            return False
        try:
            return Path(stored_path).resolve(strict=False) == expected_path.resolve(strict=False)
        except OSError:
            return Path(stored_path) == expected_path


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
