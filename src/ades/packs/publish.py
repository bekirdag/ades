"""Static registry build helpers for pack publication."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import shutil
import tarfile
import tempfile
from pathlib import Path

import zstandard

from .manifest import PackArtifact, PackManifest, RegistryIndex, RegistryPack


IGNORED_PACK_FILE_NAMES = {
    ".DS_Store",
}
IGNORED_PACK_SUFFIXES = {
    ".pyc",
    ".tar.zst",
}
IGNORED_PACK_PATTERNS = (
    "__pycache__",
    "*.ades.json",
    "*.ades-manifest.json",
)


@dataclass(frozen=True)
class RegistryBuildPackResult:
    """Published output for one pack inside a static registry."""

    pack_id: str
    version: str
    source_path: str
    manifest_path: str
    artifact_path: str
    artifact_sha256: str
    artifact_size_bytes: int


@dataclass(frozen=True)
class RegistryBuildResult:
    """Summary of a static registry build."""

    output_dir: str
    index_path: str
    index_url: str
    generated_at: str
    packs: list[RegistryBuildPackResult]


def build_static_registry(
    pack_dirs: list[str | Path],
    *,
    output_dir: str | Path,
) -> RegistryBuildResult:
    """Build a static file-based registry from local pack directories."""

    resolved_pack_dirs = [Path(pack_dir).expanduser().resolve() for pack_dir in pack_dirs]
    if not resolved_pack_dirs:
        raise ValueError("At least one pack directory is required.")

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    packs_output_dir = resolved_output_dir / "packs"
    artifacts_output_dir = resolved_output_dir / "artifacts"
    packs_output_dir.mkdir(parents=True, exist_ok=True)
    artifacts_output_dir.mkdir(parents=True, exist_ok=True)

    generated_at = _utc_timestamp()
    built_packs: list[RegistryBuildPackResult] = []
    registry_entries: dict[str, RegistryPack] = {}
    seen_pack_ids: set[str] = set()

    for pack_dir in resolved_pack_dirs:
        if not pack_dir.exists():
            raise FileNotFoundError(f"Pack directory not found: {pack_dir}")
        if not pack_dir.is_dir():
            raise NotADirectoryError(f"Pack directory is not a directory: {pack_dir}")

        source_manifest_path = pack_dir / "manifest.json"
        if not source_manifest_path.exists():
            raise FileNotFoundError(f"Pack manifest not found: {source_manifest_path}")

        source_manifest = PackManifest.load(source_manifest_path)
        if source_manifest.pack_id in seen_pack_ids:
            raise ValueError(f"Duplicate pack id in registry build: {source_manifest.pack_id}")
        seen_pack_ids.add(source_manifest.pack_id)

        _validate_manifest_sources(pack_dir, source_manifest)

        artifact_filename = f"{source_manifest.pack_id}-{source_manifest.version}.tar.zst"
        artifact_relative_url = f"../../artifacts/{artifact_filename}"
        packaging_manifest = _build_published_manifest(
            source_manifest,
            artifact_relative_url=artifact_relative_url,
            artifact_sha256=None,
        )
        artifact_path = artifacts_output_dir / artifact_filename
        _write_pack_artifact(pack_dir, packaging_manifest, artifact_path)
        artifact_sha256 = _sha256_file(artifact_path)
        published_manifest = _build_published_manifest(
            source_manifest,
            artifact_relative_url=artifact_relative_url,
            artifact_sha256=artifact_sha256,
        )

        published_pack_dir = packs_output_dir / source_manifest.pack_id
        if published_pack_dir.exists():
            shutil.rmtree(published_pack_dir)
        shutil.copytree(
            pack_dir,
            published_pack_dir,
            ignore=shutil.ignore_patterns(*IGNORED_PACK_PATTERNS, "*.tar.zst", "__pycache__", "*.pyc"),
        )
        (published_pack_dir / "manifest.json").write_text(
            json.dumps(published_manifest.to_dict(), indent=2) + "\n",
            encoding="utf-8",
        )

        registry_entries[source_manifest.pack_id] = RegistryPack(
            pack_id=source_manifest.pack_id,
            version=source_manifest.version,
            manifest_url=f"packs/{source_manifest.pack_id}/manifest.json",
        )
        built_packs.append(
            RegistryBuildPackResult(
                pack_id=source_manifest.pack_id,
                version=source_manifest.version,
                source_path=str(pack_dir),
                manifest_path=str((published_pack_dir / "manifest.json").resolve()),
                artifact_path=str(artifact_path.resolve()),
                artifact_sha256=artifact_sha256,
                artifact_size_bytes=artifact_path.stat().st_size,
            )
        )

    index = RegistryIndex(
        schema_version=1,
        generated_at=generated_at,
        packs=registry_entries,
    )
    index_path = resolved_output_dir / "index.json"
    index_path.write_text(json.dumps(index.to_dict(), indent=2) + "\n", encoding="utf-8")
    return RegistryBuildResult(
        output_dir=str(resolved_output_dir),
        index_path=str(index_path.resolve()),
        index_url=index_path.resolve().as_uri(),
        generated_at=generated_at,
        packs=sorted(built_packs, key=lambda item: item.pack_id),
    )


def _build_published_manifest(
    source_manifest: PackManifest,
    *,
    artifact_relative_url: str,
    artifact_sha256: str | None,
) -> PackManifest:
    return PackManifest(
        schema_version=source_manifest.schema_version,
        pack_id=source_manifest.pack_id,
        version=source_manifest.version,
        language=source_manifest.language,
        domain=source_manifest.domain,
        tier=source_manifest.tier,
        dependencies=list(source_manifest.dependencies),
        artifacts=[
            PackArtifact(
                name="pack",
                url=artifact_relative_url,
                sha256=artifact_sha256,
            )
        ],
        models=list(source_manifest.models),
        rules=list(source_manifest.rules),
        labels=list(source_manifest.labels),
        min_ades_version=source_manifest.min_ades_version,
    )


def _validate_manifest_sources(pack_dir: Path, manifest: PackManifest) -> None:
    for relative_path in [*manifest.rules, *manifest.labels, *manifest.models]:
        resolved = pack_dir / relative_path
        if not resolved.exists():
            raise FileNotFoundError(
                f"Pack source file listed in manifest is missing: {resolved}"
            )


def _write_pack_artifact(source_pack_dir: Path, manifest: PackManifest, artifact_path: Path) -> None:
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ades-pack-build-") as temp_dir:
        staging_dir = Path(temp_dir) / source_pack_dir.name
        shutil.copytree(
            source_pack_dir,
            staging_dir,
            ignore=shutil.ignore_patterns(*IGNORED_PACK_PATTERNS, "*.tar.zst", "__pycache__", "*.pyc"),
        )
        (staging_dir / "manifest.json").write_text(
            json.dumps(manifest.to_dict(), indent=2) + "\n",
            encoding="utf-8",
        )
        _pack_directory_to_tar_zst(staging_dir, artifact_path)


def _pack_directory_to_tar_zst(source_dir: Path, artifact_path: Path) -> None:
    with artifact_path.open("wb") as raw_handle:
        compressor = zstandard.ZstdCompressor(level=10)
        with compressor.stream_writer(raw_handle) as compressed_handle:
            with tarfile.open(fileobj=compressed_handle, mode="w|") as archive:
                for entry in _iter_pack_entries(source_dir):
                    arcname = entry.relative_to(source_dir).as_posix()
                    tar_info = archive.gettarinfo(str(entry), arcname=arcname)
                    tar_info.uid = 0
                    tar_info.gid = 0
                    tar_info.uname = ""
                    tar_info.gname = ""
                    tar_info.mtime = 0
                    if tar_info.isfile():
                        with entry.open("rb") as file_handle:
                            archive.addfile(tar_info, fileobj=file_handle)
                    else:
                        archive.addfile(tar_info)


def _iter_pack_entries(source_dir: Path) -> list[Path]:
    entries = [path for path in source_dir.rglob("*") if _should_include_in_pack(path)]
    return sorted(entries, key=lambda item: item.relative_to(source_dir).as_posix())


def _should_include_in_pack(path: Path) -> bool:
    if path.name in IGNORED_PACK_FILE_NAMES:
        return False
    if any(part == "__pycache__" for part in path.parts):
        return False
    if any(path.match(pattern) for pattern in IGNORED_PACK_PATTERNS):
        return False
    if path.suffix in IGNORED_PACK_SUFFIXES:
        return False
    return True


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            hasher.update(chunk)
    return hasher.hexdigest()


def _utc_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
