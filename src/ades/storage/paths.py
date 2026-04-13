"""Filesystem layout helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass(frozen=True)
class StorageLayout:
    """Canonical storage layout under the large-data root."""

    storage_root: Path
    packs_dir: Path
    models_dir: Path
    cache_dir: Path
    tmp_dir: Path
    logs_dir: Path
    registry_db: Path


def build_storage_layout(storage_root: Path) -> StorageLayout:
    """Create an in-memory layout object."""

    root = storage_root.expanduser()
    return StorageLayout(
        storage_root=root,
        packs_dir=root / "packs",
        models_dir=root / "models",
        cache_dir=root / "cache",
        tmp_dir=root / "tmp",
        logs_dir=root / "logs",
        registry_db=root / "registry" / "ades.db",
    )


def ensure_storage_layout(layout: StorageLayout) -> StorageLayout:
    """Create the required top-level directories if they do not exist."""

    for path in (
        layout.storage_root,
        layout.packs_dir,
        layout.models_dir,
        layout.cache_dir,
        layout.tmp_dir,
        layout.logs_dir,
        layout.registry_db.parent,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return layout


def iter_pack_manifest_paths(packs_dir: Path) -> Iterator[Path]:
    """Yield installable pack manifests while skipping hidden backup/work dirs."""

    if not packs_dir.exists():
        return
    for manifest_path in sorted(packs_dir.glob("*/manifest.json")):
        pack_dir = manifest_path.parent
        if pack_dir.name.startswith("."):
            continue
        yield manifest_path
