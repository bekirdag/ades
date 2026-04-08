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


@dataclass(frozen=True)
class PackManifest:
    """Machine-readable metadata for a single pack."""

    schema_version: int
    pack_id: str
    version: str
    language: str
    domain: str
    tier: str
    dependencies: list[str] = field(default_factory=list)
    artifacts: list[PackArtifact] = field(default_factory=list)
    models: list[str] = field(default_factory=list)
    rules: list[str] = field(default_factory=list)
    labels: list[str] = field(default_factory=list)
    min_ades_version: str = "0.1.0"
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
        return cls(
            schema_version=int(data.get("schema_version", 1)),
            pack_id=str(data["pack_id"]),
            version=str(data["version"]),
            language=str(data["language"]),
            domain=str(data["domain"]),
            tier=str(data["tier"]),
            dependencies=list(data.get("dependencies", [])),
            artifacts=artifacts,
            models=list(data.get("models", [])),
            rules=list(data.get("rules", [])),
            labels=list(data.get("labels", [])),
            min_ades_version=str(data.get("min_ades_version", "0.1.0")),
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
            "active": self.active,
        }


@dataclass(frozen=True)
class RegistryPack:
    """Registry index entry for a pack."""

    pack_id: str
    version: str
    manifest_url: str


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
