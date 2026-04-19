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
DEFAULT_VECTOR_COLLECTION_ALIAS = "ades-qids-current"
DEFAULT_VECTOR_RELATED_LIMIT = 5
CONFIG_FILE_ENV = "ADES_CONFIG_FILE"
DATABASE_URL_ENV = "ADES_DATABASE_URL"
DEFAULT_CONFIG_PATHS = (
    Path("/etc/ades/config.toml"),
    Path.home() / ".config" / "ades" / "config.toml",
)


class InvalidConfigurationError(ValueError):
    """Raised when one configuration value cannot be parsed safely."""


def _coerce_bool(value: Any, *, name: str) -> bool:
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise InvalidConfigurationError(f"Invalid {name}: {value!r}")


def _coerce_int(value: Any, *, name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidConfigurationError(f"Invalid {name}: {value!r}") from exc


def _coerce_float(value: Any, *, name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise InvalidConfigurationError(f"Invalid {name}: {value!r}") from exc


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
    vector_search_enabled: bool = False
    vector_search_url: str | None = None
    vector_search_api_key: str | None = None
    vector_search_collection_alias: str = DEFAULT_VECTOR_COLLECTION_ALIAS
    vector_search_related_limit: int = DEFAULT_VECTOR_RELATED_LIMIT
    vector_search_score_threshold: float | None = None
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
        vector_search_enabled = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_VECTOR_SEARCH_ENABLED",
            config_name="vector_search_enabled",
        )
        vector_search_url = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_VECTOR_SEARCH_URL",
            config_name="vector_search_url",
        )
        vector_search_api_key = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_VECTOR_SEARCH_API_KEY",
            config_name="vector_search_api_key",
        )
        vector_search_collection_alias = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_VECTOR_SEARCH_COLLECTION_ALIAS",
            config_name="vector_search_collection_alias",
        )
        vector_search_related_limit = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_VECTOR_SEARCH_RELATED_LIMIT",
            config_name="vector_search_related_limit",
        )
        vector_search_score_threshold = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_VECTOR_SEARCH_SCORE_THRESHOLD",
            config_name="vector_search_score_threshold",
        )

        raw_port = port if port is not None else DEFAULT_PORT
        resolved_port = _coerce_int(raw_port, name="ades port")
        resolved_vector_search_enabled = (
            _coerce_bool(vector_search_enabled, name="ades vector search enabled")
            if vector_search_enabled is not None
            else False
        )
        resolved_vector_related_limit = (
            _coerce_int(
                vector_search_related_limit,
                name="ades vector search related limit",
            )
            if vector_search_related_limit is not None
            else DEFAULT_VECTOR_RELATED_LIMIT
        )
        if resolved_vector_related_limit <= 0:
            raise InvalidConfigurationError(
                f"Invalid ades vector search related limit: {resolved_vector_related_limit!r}"
            )
        resolved_vector_score_threshold = (
            _coerce_float(
                vector_search_score_threshold,
                name="ades vector search score threshold",
            )
            if vector_search_score_threshold is not None
            else None
        )

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
            vector_search_enabled=resolved_vector_search_enabled,
            vector_search_url=str(vector_search_url) if vector_search_url else None,
            vector_search_api_key=(
                str(vector_search_api_key) if vector_search_api_key else None
            ),
            vector_search_collection_alias=str(
                vector_search_collection_alias or DEFAULT_VECTOR_COLLECTION_ALIAS
            ),
            vector_search_related_limit=resolved_vector_related_limit,
            vector_search_score_threshold=resolved_vector_score_threshold,
            config_path=config_path,
        )


def get_settings() -> Settings:
    """Resolve settings from the current environment."""

    return Settings.from_env()
