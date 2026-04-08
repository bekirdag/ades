"""Pack installation flow."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import io
import json
import shutil
import tarfile
from pathlib import Path

from .fetch import normalize_source_url, read_bytes, read_text
from .manifest import PackManifest, RegistryIndex
from .registry import PackRegistry, default_registry_url
from ..storage.backend import MetadataBackend, RuntimeTarget


@dataclass
class InstallResult:
    """Summary of a pack installation run."""

    requested_pack: str
    registry_url: str
    installed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


class PackInstaller:
    """Dependency-aware installer for registry-backed packs."""

    def __init__(
        self,
        storage_root: Path,
        registry_url: str | None = None,
        *,
        runtime_target: RuntimeTarget | str = RuntimeTarget.LOCAL,
        metadata_backend: MetadataBackend | str = MetadataBackend.SQLITE,
    ) -> None:
        self.registry = PackRegistry(
            storage_root,
            runtime_target=runtime_target,
            metadata_backend=metadata_backend,
        )
        self.registry_url = normalize_source_url(registry_url or default_registry_url())
        self._index: RegistryIndex | None = None

    def install(self, pack_id: str) -> InstallResult:
        """Install a pack and its dependencies into local storage."""

        result = InstallResult(requested_pack=pack_id, registry_url=self.registry_url)
        self._install_recursive(pack_id, result=result, visiting=set())
        return result

    def available_packs(self) -> list[str]:
        """List packs in the configured registry."""

        return sorted(self._load_index().packs)

    def _install_recursive(
        self,
        pack_id: str,
        *,
        result: InstallResult,
        visiting: set[str],
    ) -> None:
        if pack_id in visiting:
            raise ValueError(f"Cyclic pack dependency detected at {pack_id}")

        entry = self._load_index().get(pack_id)
        if entry is None:
            raise ValueError(f"Pack not found in registry: {pack_id}")

        installed = self.registry.get_pack(pack_id)
        if installed is not None and installed.version == entry.version:
            if pack_id not in result.skipped:
                result.skipped.append(pack_id)
            return

        visiting.add(pack_id)
        manifest = self._load_manifest(entry.manifest_url)
        for dependency in manifest.dependencies:
            self._install_recursive(dependency, result=result, visiting=visiting)

        self._install_manifest(manifest)
        visiting.remove(pack_id)
        if pack_id not in result.installed:
            result.installed.append(pack_id)

    def _load_index(self) -> RegistryIndex:
        if self._index is None:
            data = json.loads(read_text(self.registry_url))
            self._index = RegistryIndex.from_dict(data, base_url=self.registry_url)
        return self._index

    def _load_manifest(self, manifest_url: str) -> PackManifest:
        data = json.loads(read_text(manifest_url))
        return PackManifest.from_dict(data, base_url=manifest_url)

    def _install_manifest(self, manifest: PackManifest) -> None:
        artifact = self._choose_artifact(manifest)
        payload = read_bytes(artifact.url)
        self._verify_sha256(payload, artifact.sha256, artifact.url)

        destination = self.registry.layout.packs_dir / manifest.pack_id
        if destination.exists():
            shutil.rmtree(destination)
        destination.mkdir(parents=True, exist_ok=True)
        self._extract_tar_zst(payload, destination)
        self.registry.store.sync_pack_from_dir(destination)

        installed_manifest = self.registry.get_pack(manifest.pack_id)
        if installed_manifest is None:
            raise ValueError(f"Installed pack manifest missing after extraction: {manifest.pack_id}")

    @staticmethod
    def _choose_artifact(manifest: PackManifest):
        if not manifest.artifacts:
            raise ValueError(f"Pack has no installable artifacts: {manifest.pack_id}")
        return manifest.artifacts[0]

    @staticmethod
    def _verify_sha256(payload: bytes, expected: str | None, source: str) -> None:
        if not expected:
            return
        actual = hashlib.sha256(payload).hexdigest()
        if actual != expected:
            raise ValueError(
                f"Artifact checksum mismatch for {source}: expected {expected}, got {actual}"
            )

    @staticmethod
    def _extract_tar_zst(payload: bytes, destination: Path) -> None:
        import zstandard

        decompressor = zstandard.ZstdDecompressor()
        tar_bytes = decompressor.decompress(payload)
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as archive:
            archive.extractall(destination)
