"""Runtime configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import tomllib
from typing import Any

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
CONFIG_FILE_ENV = "ADES_CONFIG_FILE"
DATABASE_URL_ENV = "ADES_DATABASE_URL"
DEFAULT_CONFIG_PATHS = (
    Path("/etc/ades/config.toml"),
    Path.home() / ".config" / "ades" / "config.toml",
)


class InvalidConfigurationError(ValueError):
    """Raised when one configuration value cannot be parsed safely."""


def _resolve_config_path(env_map: dict[str, str]) -> Path | None:
    explicit = env_map.get(CONFIG_FILE_ENV)
    if explicit:
        path = Path(explicit).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"ades config file not found: {path}")
        return path

    for candidate in DEFAULT_CONFIG_PATHS:
        if candidate.exists():
            return candidate
    return None


def _load_config_values(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise InvalidConfigurationError(
            f"Invalid ades config file: {path}: {exc}"
        ) from exc
    section = payload.get("ades")
    if isinstance(section, dict):
        return dict(section)
    if isinstance(payload, dict):
        return dict(payload)
    return {}


def _value_from_env_or_config(
    env_map: dict[str, str],
    config_values: dict[str, Any],
    *,
    env_name: str,
    config_name: str,
) -> Any:
    if env_name in env_map:
        return env_map[env_name]
    return config_values.get(config_name)


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
    database_url: str | None = None
    config_path: Path | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        env_map = dict(os.environ)
        config_path = _resolve_config_path(env_map)
        config_values = _load_config_values(config_path)

        host = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_HOST",
            config_name="host",
        )
        port = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_PORT",
            config_name="port",
        )
        storage_root = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_STORAGE_ROOT",
            config_name="storage_root",
        )
        default_pack = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_DEFAULT_PACK",
            config_name="default_pack",
        )
        registry_url = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_REGISTRY_URL",
            config_name="registry_url",
        )
        runtime_target = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_RUNTIME_TARGET",
            config_name="runtime_target",
        )
        metadata_backend = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_METADATA_BACKEND",
            config_name="metadata_backend",
        )
        database_url = _value_from_env_or_config(
            env_map,
            config_values,
            env_name=DATABASE_URL_ENV,
            config_name="database_url",
        )

        raw_port = port if port is not None else DEFAULT_PORT
        try:
            resolved_port = int(raw_port)
        except ValueError as exc:
            raise InvalidConfigurationError(
                f"Invalid ades port: {raw_port!r}"
            ) from exc

        return cls(
            host=str(host or DEFAULT_HOST),
            port=resolved_port,
            storage_root=Path(
                str(storage_root or DEFAULT_STORAGE_ROOT)
            ).expanduser(),
            default_pack=str(default_pack or DEFAULT_PACK),
            registry_url=str(registry_url) if registry_url else None,
            runtime_target=normalize_runtime_target(runtime_target),
            metadata_backend=normalize_metadata_backend(metadata_backend),
            database_url=str(database_url) if database_url else None,
            config_path=config_path,
        )


def get_settings() -> Settings:
    """Resolve settings from the current environment."""

    return Settings.from_env()
