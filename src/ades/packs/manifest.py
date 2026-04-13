"""Pack manifest models."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any
from urllib.parse import urljoin


@dataclass(frozen=True)
class PackArtifact:
    """Artifact metadata for a pack."""

    name: str
    url: str
    sha256: str | None = None

    def to_dict(self) -> dict[str, str]:
        """Serialize one artifact entry."""

        payload = {
            "name": self.name,
            "url": self.url,
        }
        if self.sha256 is not None:
            payload["sha256"] = self.sha256
        return payload


@dataclass(frozen=True)
class PackMatcher:
    """Matcher artifact metadata for a pack."""

    schema_version: int
    algorithm: str
    normalization: str
    artifact_path: str
    entries_path: str
    entry_count: int
    state_count: int
    max_alias_length: int
    artifact_sha256: str | None = None
    entries_sha256: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize matcher metadata."""

        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "algorithm": self.algorithm,
            "normalization": self.normalization,
            "artifact_path": self.artifact_path,
            "entries_path": self.entries_path,
            "entry_count": self.entry_count,
            "state_count": self.state_count,
            "max_alias_length": self.max_alias_length,
        }
        if self.artifact_sha256 is not None:
            payload["artifact_sha256"] = self.artifact_sha256
        if self.entries_sha256 is not None:
            payload["entries_sha256"] = self.entries_sha256
        return payload


@dataclass(frozen=True)
class PackManifest:
    """Machine-readable metadata for a single pack."""

    schema_version: int
    pack_id: str
    version: str
    language: str
    domain: str
    tier: str
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    artifacts: list[PackArtifact] = field(default_factory=list)
    models: list[str] = field(default_factory=list)
    rules: list[str] = field(default_factory=list)
    labels: list[str] = field(default_factory=list)
    matcher: PackMatcher | None = None
    min_ades_version: str = "0.1.0"
    min_entities_per_100_tokens_warning: float | None = None
    sha256: str | None = None
    active: bool = True
    install_path: str | None = None
    manifest_path: str | None = None
    installed_at: str | None = None

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        base_url: str | None = None,
    ) -> "PackManifest":
        """Build a manifest from JSON-like data."""

        artifacts = [
            PackArtifact(
                name=str(item["name"]),
                url=urljoin(base_url, str(item["url"])) if base_url else str(item["url"]),
                sha256=item.get("sha256"),
            )
            for item in data.get("artifacts", [])
        ]
        matcher_data = data.get("matcher")
        matcher = None
        if matcher_data is not None:
            matcher = PackMatcher(
                schema_version=int(matcher_data.get("schema_version", 1)),
                algorithm=str(matcher_data["algorithm"]),
                normalization=str(matcher_data["normalization"]),
                artifact_path=str(matcher_data["artifact_path"]),
                entries_path=str(matcher_data["entries_path"]),
                entry_count=int(matcher_data["entry_count"]),
                state_count=int(matcher_data["state_count"]),
                max_alias_length=int(matcher_data["max_alias_length"]),
                artifact_sha256=(
                    str(matcher_data["artifact_sha256"])
                    if matcher_data.get("artifact_sha256") is not None
                    else None
                ),
                entries_sha256=(
                    str(matcher_data["entries_sha256"])
                    if matcher_data.get("entries_sha256") is not None
                    else None
                ),
            )
        return cls(
            schema_version=int(data.get("schema_version", 1)),
            pack_id=str(data["pack_id"]),
            version=str(data["version"]),
            language=str(data["language"]),
            domain=str(data["domain"]),
            tier=str(data["tier"]),
            description=str(data["description"]) if data.get("description") is not None else None,
            tags=list(data.get("tags", [])),
            dependencies=list(data.get("dependencies", [])),
            artifacts=artifacts,
            models=list(data.get("models", [])),
            rules=list(data.get("rules", [])),
            labels=list(data.get("labels", [])),
            matcher=matcher,
            min_ades_version=str(data.get("min_ades_version", "0.1.0")),
            min_entities_per_100_tokens_warning=(
                float(data["min_entities_per_100_tokens_warning"])
                if data.get("min_entities_per_100_tokens_warning") is not None
                else None
            ),
            sha256=data.get("sha256"),
            active=bool(data.get("active", True)),
            install_path=data.get("install_path"),
            manifest_path=data.get("manifest_path"),
            installed_at=data.get("installed_at"),
        )

    @classmethod
    def load(cls, path: Path) -> "PackManifest":
        """Load a manifest from disk."""

        return cls.from_dict(json.loads(path.read_text()), base_url=path.resolve().as_uri())

    def to_summary(self) -> dict[str, str | bool]:
        """Return the service summary shape."""

        return {
            "pack_id": self.pack_id,
            "version": self.version,
            "language": self.language,
            "domain": self.domain,
            "tier": self.tier,
            "description": self.description,
            "tags": list(self.tags),
            "active": self.active,
            "matcher_ready": self.matcher is not None,
            "matcher_algorithm": self.matcher.algorithm if self.matcher is not None else None,
            "matcher_entry_count": self.matcher.entry_count if self.matcher is not None else None,
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialize the manifest into stable JSON-compatible data."""

        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "pack_id": self.pack_id,
            "version": self.version,
            "language": self.language,
            "domain": self.domain,
            "tier": self.tier,
            "tags": list(self.tags),
            "dependencies": list(self.dependencies),
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "models": list(self.models),
            "rules": list(self.rules),
            "labels": list(self.labels),
            "min_ades_version": self.min_ades_version,
        }
        if self.matcher is not None:
            payload["matcher"] = self.matcher.to_dict()
        if self.min_entities_per_100_tokens_warning is not None:
            payload["min_entities_per_100_tokens_warning"] = (
                self.min_entities_per_100_tokens_warning
            )
        if self.description is not None:
            payload["description"] = self.description
        if self.sha256 is not None:
            payload["sha256"] = self.sha256
        if not self.active:
            payload["active"] = self.active
        if self.install_path is not None:
            payload["install_path"] = self.install_path
        if self.manifest_path is not None:
            payload["manifest_path"] = self.manifest_path
        if self.installed_at is not None:
            payload["installed_at"] = self.installed_at
        return payload


@dataclass(frozen=True)
class RegistryPack:
    """Registry index entry for a pack."""

    pack_id: str
    version: str
    manifest_url: str
    language: str | None = None
    domain: str | None = None
    tier: str | None = None
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    min_ades_version: str = "0.1.0"

    def to_summary(self) -> dict[str, Any]:
        """Return a user-facing summary for one available registry pack."""

        return {
            "pack_id": self.pack_id,
            "version": self.version,
            "language": self.language,
            "domain": self.domain,
            "tier": self.tier,
            "description": self.description,
            "tags": list(self.tags),
            "dependencies": list(self.dependencies),
            "min_ades_version": self.min_ades_version,
            "manifest_url": self.manifest_url,
        }


@dataclass(frozen=True)
class RegistryIndex:
    """Static registry index."""

    schema_version: int
    generated_at: str
    packs: dict[str, RegistryPack]

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        base_url: str | None = None,
    ) -> "RegistryIndex":
        packs = {
            pack_id: RegistryPack(
                pack_id=pack_id,
                version=str(item["version"]),
                manifest_url=(
                    urljoin(base_url, str(item["manifest_url"]))
                    if base_url
                    else str(item["manifest_url"])
                ),
                language=str(item["language"]) if item.get("language") is not None else None,
                domain=str(item["domain"]) if item.get("domain") is not None else None,
                tier=str(item["tier"]) if item.get("tier") is not None else None,
                description=(
                    str(item["description"])
                    if item.get("description") is not None
                    else None
                ),
                tags=list(item.get("tags", [])),
                dependencies=list(item.get("dependencies", [])),
                min_ades_version=str(item.get("min_ades_version", "0.1.0")),
            )
            for pack_id, item in data.get("packs", {}).items()
        }
        return cls(
            schema_version=int(data.get("schema_version", 1)),
            generated_at=str(data.get("generated_at", "")),
            packs=packs,
        )

    def get(self, pack_id: str) -> RegistryPack | None:
        """Return a single pack entry."""

        return self.packs.get(pack_id)

    def list_packs(self) -> list[RegistryPack]:
        """Return all registry pack entries in stable order."""

        return [self.packs[pack_id] for pack_id in sorted(self.packs)]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the registry index into stable JSON-compatible data."""

        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at,
            "packs": {
                pack_id: {
                    "version": pack.version,
                    "manifest_url": pack.manifest_url,
                    "language": pack.language,
                    "domain": pack.domain,
                    "tier": pack.tier,
                    "description": pack.description,
                    "tags": list(pack.tags),
                    "dependencies": list(pack.dependencies),
                    "min_ades_version": pack.min_ades_version,
                }
                for pack_id, pack in sorted(self.packs.items())
            },
        }
