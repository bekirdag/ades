"""Static registry build helpers for pack publication."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
import shutil
import subprocess
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


@dataclass(frozen=True)
class ObjectStoragePublishedObject:
    """One published object discovered under an object-storage prefix."""

    key: str
    size_bytes: int


@dataclass(frozen=True)
class ObjectStoragePublishResult:
    """Summary of one registry publication to S3-compatible object storage."""

    registry_dir: str
    bucket: str
    endpoint: str
    endpoint_url: str
    prefix: str
    storage_uri: str
    index_storage_uri: str
    published_at: str
    delete: bool
    object_count: int
    objects: list[ObjectStoragePublishedObject]


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
            language=source_manifest.language,
            domain=source_manifest.domain,
            tier=source_manifest.tier,
            description=source_manifest.description,
            tags=list(source_manifest.tags),
            dependencies=list(source_manifest.dependencies),
            min_ades_version=source_manifest.min_ades_version,
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


def publish_registry_to_object_storage(
    registry_dir: str | Path,
    *,
    prefix: str,
    bucket: str | None = None,
    endpoint: str | None = None,
    region: str = "us-east-1",
    delete: bool = True,
) -> ObjectStoragePublishResult:
    """Publish one reviewed static registry directory to S3-compatible storage."""

    resolved_registry_dir = Path(registry_dir).expanduser().resolve()
    if not resolved_registry_dir.exists():
        raise FileNotFoundError(f"Registry directory not found: {resolved_registry_dir}")
    if not resolved_registry_dir.is_dir():
        raise NotADirectoryError(
            f"Registry directory is not a directory: {resolved_registry_dir}"
        )
    index_path = resolved_registry_dir / "index.json"
    if not index_path.exists():
        raise FileNotFoundError(f"Registry index not found: {index_path}")

    resolved_bucket = bucket or os.environ.get("ADES_PACK_OBJECT_STORAGE_BUCKET")
    if not resolved_bucket:
        raise ValueError(
            "Object storage bucket is required. Set ADES_PACK_OBJECT_STORAGE_BUCKET or pass bucket."
        )
    resolved_endpoint = endpoint or os.environ.get("ADES_PACK_OBJECT_STORAGE_ENDPOINT")
    if not resolved_endpoint:
        raise ValueError(
            "Object storage endpoint is required. Set ADES_PACK_OBJECT_STORAGE_ENDPOINT or pass endpoint."
        )

    normalized_prefix = prefix.strip("/")
    if not normalized_prefix:
        raise ValueError("Object storage prefix is required.")

    resolved_endpoint_url = (
        resolved_endpoint
        if resolved_endpoint.startswith(("http://", "https://"))
        else f"https://{resolved_endpoint}"
    )
    storage_uri = f"s3://{resolved_bucket}/{normalized_prefix}/"
    command_env = _build_object_storage_env(region=region)
    sync_command = [
        "aws",
        "s3",
        "sync",
        str(resolved_registry_dir),
        storage_uri,
        "--endpoint-url",
        resolved_endpoint_url,
        "--only-show-errors",
    ]
    if delete:
        sync_command.append("--delete")
    _run_object_storage_command(sync_command, env=command_env)

    list_command = [
        "aws",
        "s3",
        "ls",
        storage_uri,
        "--recursive",
        "--endpoint-url",
        resolved_endpoint_url,
    ]
    listed = _run_object_storage_command(list_command, env=command_env)
    objects = _parse_listed_objects(listed.stdout)
    return ObjectStoragePublishResult(
        registry_dir=str(resolved_registry_dir),
        bucket=resolved_bucket,
        endpoint=resolved_endpoint,
        endpoint_url=resolved_endpoint_url,
        prefix=normalized_prefix,
        storage_uri=storage_uri,
        index_storage_uri=f"{storage_uri}index.json",
        published_at=_utc_timestamp(),
        delete=delete,
        object_count=len(objects),
        objects=objects,
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
        description=source_manifest.description,
        tags=list(source_manifest.tags),
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


def _build_object_storage_env(*, region: str) -> dict[str, str]:
    env = os.environ.copy()
    access_key = env.get("AWS_ACCESS_KEY_ID") or env.get(
        "ADES_PACK_OBJECT_STORAGE_ACCESS_KEY_ID"
    )
    secret_key = env.get("AWS_SECRET_ACCESS_KEY") or env.get(
        "ADES_PACK_OBJECT_STORAGE_SECRET_ACCESS_KEY"
    )
    if not access_key or not secret_key:
        raise ValueError(
            "Object storage credentials are required. Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
            "or ADES_PACK_OBJECT_STORAGE_ACCESS_KEY_ID/ADES_PACK_OBJECT_STORAGE_SECRET_ACCESS_KEY."
        )
    env["AWS_ACCESS_KEY_ID"] = access_key
    env["AWS_SECRET_ACCESS_KEY"] = secret_key
    env.setdefault("AWS_DEFAULT_REGION", region)
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    return env


def _run_object_storage_command(
    command: list[str],
    *,
    env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
    except FileNotFoundError as exc:
        raise FileNotFoundError("AWS CLI not found on PATH.") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        if not detail:
            detail = f"Object storage command failed: {' '.join(command)}"
        raise ValueError(detail) from exc


def _parse_listed_objects(stdout: str) -> list[ObjectStoragePublishedObject]:
    objects: list[ObjectStoragePublishedObject] = []
    for line in stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        parts = stripped.split(maxsplit=3)
        if len(parts) != 4:
            continue
        size_text = parts[2]
        key = parts[3]
        objects.append(
            ObjectStoragePublishedObject(
                key=key,
                size_bytes=int(size_text),
            )
        )
    return objects
