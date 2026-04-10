"""Static registry build helpers for pack publication."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import zstandard

from .fetch import normalize_source_url
from .installer import PackInstaller
from .manifest import PackArtifact, PackManifest, RegistryIndex, RegistryPack
from .registry import PackRegistry, load_registry_index
from ..pipeline.tagger import tag_text


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
    registry_url_candidates: list[str]
    published_at: str
    delete: bool
    object_count: int
    objects: list[ObjectStoragePublishedObject]


@dataclass(frozen=True)
class PublishedRegistrySmokeCaseResult:
    """Outcome for one clean-consumer smoke case against a published registry."""

    pack_id: str
    text: str
    expected_installed_pack_ids: list[str] = field(default_factory=list)
    pulled_pack_ids: list[str] = field(default_factory=list)
    installed_pack_ids: list[str] = field(default_factory=list)
    missing_installed_pack_ids: list[str] = field(default_factory=list)
    expected_entity_texts: list[str] = field(default_factory=list)
    matched_entity_texts: list[str] = field(default_factory=list)
    missing_entity_texts: list[str] = field(default_factory=list)
    tagged_entity_texts: list[str] = field(default_factory=list)
    expected_labels: list[str] = field(default_factory=list)
    matched_labels: list[str] = field(default_factory=list)
    missing_labels: list[str] = field(default_factory=list)
    tagged_labels: list[str] = field(default_factory=list)
    passed: bool = True
    failure: str | None = None


@dataclass(frozen=True)
class PublishedRegistrySmokeResult:
    """Aggregate clean-consumer smoke result for one published registry URL."""

    registry_url: str
    checked_at: str
    fixture_profile: str
    pack_count: int
    passed_case_count: int
    failed_case_count: int
    passed: bool
    failures: list[str] = field(default_factory=list)
    cases: list[PublishedRegistrySmokeCaseResult] = field(default_factory=list)


@dataclass(frozen=True)
class _PublishedRegistrySmokeSpec:
    """Deterministic smoke fixture for one published pack contract."""

    pack_id: str
    text: str
    expected_installed_pack_ids: tuple[str, ...]
    expected_entity_texts: tuple[str, ...]
    expected_labels: tuple[str, ...]


_PUBLISHED_REGISTRY_SMOKE_FIXTURE_PROFILE = "published_registry_consumer"
_PUBLISHED_REGISTRY_SMOKE_SPECS: dict[str, _PublishedRegistrySmokeSpec] = {
    "finance-en": _PublishedRegistrySmokeSpec(
        pack_id="finance-en",
        text="Apple said AAPL traded on NASDAQ after USD 12.5 guidance.",
        expected_installed_pack_ids=("finance-en",),
        expected_entity_texts=("Apple", "AAPL", "NASDAQ", "USD 12.5"),
        expected_labels=("organization", "ticker", "exchange", "currency_amount"),
    ),
    "medical-en": _PublishedRegistrySmokeSpec(
        pack_id="medical-en",
        text=(
            "BRCA1 and p53 protein were studied in NCT04280705 after "
            "Aspirin 10 mg dosing for diabetes."
        ),
        expected_installed_pack_ids=("general-en", "medical-en"),
        expected_entity_texts=(
            "BRCA1",
            "p53 protein",
            "NCT04280705",
            "Aspirin",
            "10 mg",
            "diabetes",
        ),
        expected_labels=("gene", "protein", "clinical_trial", "drug", "dosage", "disease"),
    ),
}


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
    registry_url_candidates = _build_registry_url_candidates(
        bucket=resolved_bucket,
        endpoint_url=resolved_endpoint_url,
        prefix=normalized_prefix,
    )
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
        registry_url_candidates=registry_url_candidates,
        published_at=_utc_timestamp(),
        delete=delete,
        object_count=len(objects),
        objects=objects,
    )


def smoke_test_published_registry(
    registry_url: str | Path,
    *,
    pack_ids: list[str] | tuple[str, ...] | None = None,
) -> PublishedRegistrySmokeResult:
    """Validate clean pull/install/tag behavior from a published registry URL."""

    normalized_registry_url = normalize_source_url(registry_url)
    index = _load_published_registry_index(normalized_registry_url)
    requested_pack_ids = _resolve_smoke_pack_ids(index=index, pack_ids=pack_ids)
    cases = [
        _run_published_registry_smoke_case(
            registry_url=normalized_registry_url,
            spec=_PUBLISHED_REGISTRY_SMOKE_SPECS[pack_id],
        )
        for pack_id in requested_pack_ids
    ]
    failures = [case.failure for case in cases if case.failure]
    passed_case_count = sum(1 for case in cases if case.passed)
    return PublishedRegistrySmokeResult(
        registry_url=normalized_registry_url,
        checked_at=_utc_timestamp(),
        fixture_profile=_PUBLISHED_REGISTRY_SMOKE_FIXTURE_PROFILE,
        pack_count=len(cases),
        passed_case_count=passed_case_count,
        failed_case_count=len(cases) - passed_case_count,
        passed=not failures and passed_case_count == len(cases),
        failures=failures,
        cases=cases,
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


def _load_published_registry_index(registry_url: str) -> RegistryIndex:
    """Load one published registry index with deterministic operator-facing errors."""

    try:
        return load_registry_index(registry_url)
    except FileNotFoundError:
        raise
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            raise FileNotFoundError(f"Published registry index not found: {registry_url}") from exc
        raise ValueError(f"Unable to load published registry index: {registry_url}") from exc
    except httpx.HTTPError as exc:
        raise ValueError(f"Unable to load published registry index: {registry_url}") from exc


def _resolve_smoke_pack_ids(
    *,
    index: RegistryIndex,
    pack_ids: list[str] | tuple[str, ...] | None,
) -> list[str]:
    """Choose the published smoke cases to evaluate for one registry."""

    if pack_ids:
        requested_pack_ids = list(dict.fromkeys(pack_ids))
        unsupported = [
            pack_id
            for pack_id in requested_pack_ids
            if pack_id not in _PUBLISHED_REGISTRY_SMOKE_SPECS
        ]
        if unsupported:
            raise ValueError(
                "Unsupported published-registry smoke pack(s): "
                + ", ".join(sorted(unsupported))
            )
        missing = [pack_id for pack_id in requested_pack_ids if pack_id not in index.packs]
        if missing:
            raise ValueError(
                "Requested smoke pack(s) not present in published registry: "
                + ", ".join(sorted(missing))
            )
        return requested_pack_ids

    requested_pack_ids = [
        pack_id for pack_id in _PUBLISHED_REGISTRY_SMOKE_SPECS if pack_id in index.packs
    ]
    if not requested_pack_ids:
        raise ValueError(
            "Published registry does not contain any supported smoke packs. "
            f"Supported smoke packs: {', '.join(sorted(_PUBLISHED_REGISTRY_SMOKE_SPECS))}"
        )
    return requested_pack_ids


def _run_published_registry_smoke_case(
    *,
    registry_url: str,
    spec: _PublishedRegistrySmokeSpec,
) -> PublishedRegistrySmokeCaseResult:
    """Run one deterministic clean-consumer smoke case."""

    pulled_pack_ids: list[str] = []
    installed_pack_ids: list[str] = []
    tagged_entity_texts: list[str] = []
    tagged_labels: list[str] = []
    matched_entity_texts: list[str] = []
    missing_entity_texts = list(spec.expected_entity_texts)
    matched_labels: list[str] = []
    missing_labels = list(spec.expected_labels)
    missing_installed_pack_ids = list(spec.expected_installed_pack_ids)
    failure: str | None = None

    try:
        with tempfile.TemporaryDirectory(
            prefix=f"ades-published-registry-smoke-{spec.pack_id}-"
        ) as storage_root_str:
            storage_root = Path(storage_root_str)
            installer = PackInstaller(storage_root, registry_url=registry_url)
            install_result = installer.install(spec.pack_id)
            pulled_pack_ids = list(install_result.installed)
            registry = PackRegistry(storage_root)
            installed_pack_ids = sorted(
                pack.pack_id for pack in registry.list_installed_packs()
            )
            tag_response = tag_text(
                text=spec.text,
                pack=spec.pack_id,
                content_type="text/plain",
                storage_root=storage_root,
            )
            tagged_entity_texts = sorted({entity.text for entity in tag_response.entities})
            tagged_labels = sorted({entity.label for entity in tag_response.entities})
            matched_entity_texts = [
                value for value in spec.expected_entity_texts if value in tagged_entity_texts
            ]
            missing_entity_texts = [
                value for value in spec.expected_entity_texts if value not in tagged_entity_texts
            ]
            matched_labels = [
                value for value in spec.expected_labels if value in tagged_labels
            ]
            missing_labels = [
                value for value in spec.expected_labels if value not in tagged_labels
            ]
            missing_installed_pack_ids = [
                value
                for value in spec.expected_installed_pack_ids
                if value not in installed_pack_ids
            ]
            failure = _summarize_smoke_case_failure(
                spec=spec,
                missing_installed_pack_ids=missing_installed_pack_ids,
                missing_entity_texts=missing_entity_texts,
                missing_labels=missing_labels,
            )
    except FileNotFoundError as exc:
        failure = str(exc)
    except httpx.HTTPError as exc:
        failure = f"Unable to consume published registry for {spec.pack_id}: {exc}"
    except ValueError as exc:
        failure = str(exc)

    return PublishedRegistrySmokeCaseResult(
        pack_id=spec.pack_id,
        text=spec.text,
        expected_installed_pack_ids=list(spec.expected_installed_pack_ids),
        pulled_pack_ids=pulled_pack_ids,
        installed_pack_ids=installed_pack_ids,
        missing_installed_pack_ids=missing_installed_pack_ids,
        expected_entity_texts=list(spec.expected_entity_texts),
        matched_entity_texts=matched_entity_texts,
        missing_entity_texts=missing_entity_texts,
        tagged_entity_texts=tagged_entity_texts,
        expected_labels=list(spec.expected_labels),
        matched_labels=matched_labels,
        missing_labels=missing_labels,
        tagged_labels=tagged_labels,
        passed=failure is None,
        failure=failure,
    )


def _summarize_smoke_case_failure(
    *,
    spec: _PublishedRegistrySmokeSpec,
    missing_installed_pack_ids: list[str],
    missing_entity_texts: list[str],
    missing_labels: list[str],
) -> str | None:
    """Return a stable failure summary for one smoke case."""

    parts: list[str] = []
    if missing_installed_pack_ids:
        parts.append("missing installed packs: " + ", ".join(missing_installed_pack_ids))
    if missing_entity_texts:
        parts.append("missing entities: " + ", ".join(missing_entity_texts))
    if missing_labels:
        parts.append("missing labels: " + ", ".join(missing_labels))
    if not parts:
        return None
    return f"{spec.pack_id} smoke mismatch: {'; '.join(parts)}"


def _build_registry_url_candidates(
    *,
    bucket: str,
    endpoint_url: str,
    prefix: str,
) -> list[str]:
    parsed = urlsplit(endpoint_url)
    scheme = parsed.scheme or "https"
    endpoint_host = parsed.netloc or parsed.path
    endpoint_path = parsed.path.rstrip("/")
    candidates: list[str] = []
    if endpoint_host:
        if endpoint_path:
            candidates.append(
                f"{scheme}://{bucket}.{endpoint_host}{endpoint_path}/{prefix}/index.json"
            )
        else:
            candidates.append(f"{scheme}://{bucket}.{endpoint_host}/{prefix}/index.json")
    path_style = f"{endpoint_url.rstrip('/')}/{bucket}/{prefix}/index.json"
    if path_style not in candidates:
        candidates.append(path_style)
    return candidates
