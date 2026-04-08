"""Runtime configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from .storage.backend import (
    MetadataBackend,
    RuntimeTarget,
    normalize_metadata_backend,
    normalize_runtime_target,
)

DEFAULT_STORAGE_ROOT = Path("/mnt/githubActions/ades_big_data")
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8734
DEFAULT_PACK = "general-en"


@dataclass(frozen=True)
class Settings:
    """Resolved runtime settings for the local service and CLI."""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    storage_root: Path = DEFAULT_STORAGE_ROOT
    default_pack: str = DEFAULT_PACK
    registry_url: str | None = None
    runtime_target: RuntimeTarget = RuntimeTarget.LOCAL
    metadata_backend: MetadataBackend = MetadataBackend.SQLITE

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            host=os.getenv("ADES_HOST", DEFAULT_HOST),
            port=int(os.getenv("ADES_PORT", str(DEFAULT_PORT))),
            storage_root=Path(
                os.getenv("ADES_STORAGE_ROOT", str(DEFAULT_STORAGE_ROOT))
            ).expanduser(),
            default_pack=os.getenv("ADES_DEFAULT_PACK", DEFAULT_PACK),
            registry_url=os.getenv("ADES_REGISTRY_URL"),
            runtime_target=normalize_runtime_target(os.getenv("ADES_RUNTIME_TARGET")),
            metadata_backend=normalize_metadata_backend(os.getenv("ADES_METADATA_BACKEND")),
        )


def get_settings() -> Settings:
    """Resolve settings from the current environment."""

    return Settings.from_env()
