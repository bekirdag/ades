"""Repair missing matcher artifacts for installed packs."""

from __future__ import annotations

from dataclasses import replace
from itertools import chain
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

from ..runtime_matcher import (
    build_matcher_artifact_from_alias_payloads,
    build_matcher_artifact_from_aliases_json,
)
from .manifest import PackManifest, PackMatcher

if TYPE_CHECKING:
    from ..storage.backend import MetadataStore


def pack_manifest_has_usable_matcher(
    pack_dir: Path,
    manifest: PackManifest,
    *,
    metadata_store: MetadataStore | None = None,
) -> bool:
    """Return whether the manifest references matcher files that exist on disk."""

    if manifest.matcher is None:
        return False
    artifact_path = pack_dir / manifest.matcher.artifact_path
    entries_path = pack_dir / manifest.matcher.entries_path
    if not artifact_path.exists() or not entries_path.exists():
        return False
    if metadata_store is None:
        return True
    alias_count = metadata_store.count_pack_aliases(manifest.pack_id)
    if alias_count <= 0:
        return True
    return manifest.matcher.entry_count == alias_count


def ensure_pack_matcher_artifacts(
    pack_dir: Path,
    manifest: PackManifest,
    *,
    metadata_store: MetadataStore | None = None,
) -> tuple[PackManifest, bool]:
    """Rebuild matcher artifacts from the metadata store or aliases.json when missing/stale."""

    resolved_pack_dir = pack_dir.expanduser().resolve()
    if pack_manifest_has_usable_matcher(
        resolved_pack_dir,
        manifest,
        metadata_store=metadata_store,
    ):
        return manifest, False

    matcher_output_dir = resolved_pack_dir / "matcher"
    matcher_result = _build_matcher_artifacts(
        resolved_pack_dir,
        manifest,
        matcher_output_dir=matcher_output_dir,
        metadata_store=metadata_store,
    )
    if matcher_result is None:
        return manifest, False
    repaired_manifest = replace(
        manifest,
        matcher=PackMatcher(
            schema_version=1,
            algorithm=matcher_result.algorithm,
            normalization=matcher_result.normalization,
            artifact_path=str(Path(matcher_result.artifact_path).relative_to(resolved_pack_dir)),
            entries_path=str(Path(matcher_result.entries_path).relative_to(resolved_pack_dir)),
            entry_count=matcher_result.entry_count,
            state_count=matcher_result.state_count,
            max_alias_length=matcher_result.max_alias_length,
            artifact_sha256=matcher_result.artifact_sha256,
            entries_sha256=matcher_result.entries_sha256,
        ),
    )
    _write_manifest_atomically(resolved_pack_dir / "manifest.json", repaired_manifest)
    return repaired_manifest, True


def _build_matcher_artifacts(
    pack_dir: Path,
    manifest: PackManifest,
    *,
    matcher_output_dir: Path,
    metadata_store: MetadataStore | None,
):
    if metadata_store is not None:
        alias_count = metadata_store.count_pack_aliases(manifest.pack_id)
        if alias_count > 0:
            alias_iter = iter(metadata_store.iter_pack_aliases(manifest.pack_id))
            first_alias = next(alias_iter, None)
            if first_alias is not None:
                return build_matcher_artifact_from_alias_payloads(
                    chain((first_alias,), alias_iter),
                    output_dir=matcher_output_dir,
                )

    aliases_path = pack_dir / "aliases.json"
    if not aliases_path.exists():
        return None
    return build_matcher_artifact_from_aliases_json(
        aliases_path,
        output_dir=matcher_output_dir,
    )


def _write_manifest_atomically(manifest_path: Path, manifest: PackManifest) -> None:
    payload = json.dumps(manifest.to_dict(), indent=2) + "\n"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=manifest_path.parent,
        prefix=f".{manifest_path.stem}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(payload)
        temp_path = Path(handle.name)
    temp_path.replace(manifest_path)
