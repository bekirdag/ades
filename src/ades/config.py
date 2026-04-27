"""Runtime configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
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
DEFAULT_VECTOR_SEARCH_DOMAIN_PACK_ROUTES = {
    "business": "business-vector-en",
    "economics": "economics-vector-en",
    "finance": "finance-en",
    "politics": "politics-vector-en",
}
DEFAULT_VECTOR_SEARCH_PACK_COLLECTION_ALIASES = {
    "business-vector-en": "ades-qids-business-current",
    "economics-vector-en": "ades-qids-economics-current",
    "finance-en": "ades-qids-finance-current",
    "politics-vector-en": "ades-qids-politics-current",
}
DEFAULT_VECTOR_SEARCH_COUNTRY_ALIASES = {
    "ar": "ar",
    "argentina": "ar",
    "au": "au",
    "australia": "au",
    "br": "br",
    "brazil": "br",
    "ca": "ca",
    "canada": "ca",
    "cn": "cn",
    "china": "cn",
    "prc": "cn",
    "de": "de",
    "germany": "de",
    "fr": "fr",
    "france": "fr",
    "gb": "uk",
    "great-britain": "uk",
    "britain": "uk",
    "uk": "uk",
    "united-kingdom": "uk",
    "in": "in",
    "india": "in",
    "id": "id",
    "indonesia": "id",
    "it": "it",
    "italy": "it",
    "jp": "jp",
    "japan": "jp",
    "kr": "kr",
    "korea": "kr",
    "south-korea": "kr",
    "republic-of-korea": "kr",
    "mx": "mx",
    "mexico": "mx",
    "ru": "ru",
    "russia": "ru",
    "sa": "sa",
    "saudi-arabia": "sa",
    "tr": "tr",
    "turkey": "tr",
    "turkiye": "tr",
    "us": "us",
    "usa": "us",
    "united-states": "us",
    "za": "za",
    "south-africa": "za",
}
DEFAULT_GRAPH_CONTEXT_RELATED_LIMIT = 5
DEFAULT_GRAPH_CONTEXT_SEED_NEIGHBOR_LIMIT = 24
DEFAULT_GRAPH_CONTEXT_CANDIDATE_LIMIT = 128
DEFAULT_GRAPH_CONTEXT_MIN_SUPPORTING_SEEDS = 2
DEFAULT_GRAPH_CONTEXT_VECTOR_PROPOSAL_LIMIT = 8
DEFAULT_NEWS_CONTEXT_MIN_SUPPORTING_SEEDS = 2
DEFAULT_NEWS_CONTEXT_MIN_PAIR_COUNT = 1
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


def _coerce_string_mapping(
    value: Any,
    *,
    name: str,
    normalize_keys: bool = True,
) -> dict[str, str]:
    parsed = value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise InvalidConfigurationError(f"Invalid {name}: {value!r}") from exc
    if not isinstance(parsed, dict):
        raise InvalidConfigurationError(f"Invalid {name}: {value!r}")
    result: dict[str, str] = {}
    for raw_key, raw_value in parsed.items():
        key = str(raw_key).strip()
        mapped_value = str(raw_value).strip()
        if not key or not mapped_value:
            raise InvalidConfigurationError(f"Invalid {name}: {value!r}")
        if normalize_keys:
            key = key.casefold()
        result[key] = mapped_value
    return result


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
    vector_search_domain_pack_routes: dict[str, str] = field(
        default_factory=lambda: dict(DEFAULT_VECTOR_SEARCH_DOMAIN_PACK_ROUTES)
    )
    vector_search_pack_collection_aliases: dict[str, str] = field(
        default_factory=lambda: dict(DEFAULT_VECTOR_SEARCH_PACK_COLLECTION_ALIASES)
    )
    vector_search_country_aliases: dict[str, str] = field(
        default_factory=lambda: dict(DEFAULT_VECTOR_SEARCH_COUNTRY_ALIASES)
    )
    graph_context_enabled: bool = False
    graph_context_artifact_path: Path | None = None
    graph_context_max_depth: int = 2
    graph_context_related_limit: int = DEFAULT_GRAPH_CONTEXT_RELATED_LIMIT
    graph_context_seed_neighbor_limit: int = DEFAULT_GRAPH_CONTEXT_SEED_NEIGHBOR_LIMIT
    graph_context_candidate_limit: int = DEFAULT_GRAPH_CONTEXT_CANDIDATE_LIMIT
    graph_context_min_supporting_seeds: int = DEFAULT_GRAPH_CONTEXT_MIN_SUPPORTING_SEEDS
    graph_context_vector_proposals_enabled: bool = False
    graph_context_vector_proposal_limit: int = (
        DEFAULT_GRAPH_CONTEXT_VECTOR_PROPOSAL_LIMIT
    )
    graph_context_genericity_penalty_enabled: bool = True
    news_context_artifact_path: Path | None = None
    news_context_min_supporting_seeds: int = DEFAULT_NEWS_CONTEXT_MIN_SUPPORTING_SEEDS
    news_context_min_pair_count: int = DEFAULT_NEWS_CONTEXT_MIN_PAIR_COUNT
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
        vector_search_domain_pack_routes = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_VECTOR_SEARCH_DOMAIN_PACK_ROUTES",
            config_name="vector_search_domain_pack_routes",
        )
        vector_search_pack_collection_aliases = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_VECTOR_SEARCH_PACK_COLLECTION_ALIASES",
            config_name="vector_search_pack_collection_aliases",
        )
        vector_search_country_aliases = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_VECTOR_SEARCH_COUNTRY_ALIASES",
            config_name="vector_search_country_aliases",
        )
        graph_context_enabled = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_GRAPH_CONTEXT_ENABLED",
            config_name="graph_context_enabled",
        )
        graph_context_artifact_path = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_GRAPH_CONTEXT_ARTIFACT_PATH",
            config_name="graph_context_artifact_path",
        )
        graph_context_max_depth = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_GRAPH_CONTEXT_MAX_DEPTH",
            config_name="graph_context_max_depth",
        )
        graph_context_related_limit = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_GRAPH_CONTEXT_RELATED_LIMIT",
            config_name="graph_context_related_limit",
        )
        graph_context_seed_neighbor_limit = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_GRAPH_CONTEXT_SEED_NEIGHBOR_LIMIT",
            config_name="graph_context_seed_neighbor_limit",
        )
        graph_context_candidate_limit = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_GRAPH_CONTEXT_CANDIDATE_LIMIT",
            config_name="graph_context_candidate_limit",
        )
        graph_context_min_supporting_seeds = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_GRAPH_CONTEXT_MIN_SUPPORTING_SEEDS",
            config_name="graph_context_min_supporting_seeds",
        )
        graph_context_vector_proposals_enabled = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_GRAPH_CONTEXT_VECTOR_PROPOSALS_ENABLED",
            config_name="graph_context_vector_proposals_enabled",
        )
        graph_context_vector_proposal_limit = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_GRAPH_CONTEXT_VECTOR_PROPOSAL_LIMIT",
            config_name="graph_context_vector_proposal_limit",
        )
        graph_context_genericity_penalty_enabled = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_GRAPH_CONTEXT_GENERICITY_PENALTY_ENABLED",
            config_name="graph_context_genericity_penalty_enabled",
        )
        news_context_artifact_path = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_NEWS_CONTEXT_ARTIFACT_PATH",
            config_name="news_context_artifact_path",
        )
        news_context_min_supporting_seeds = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_NEWS_CONTEXT_MIN_SUPPORTING_SEEDS",
            config_name="news_context_min_supporting_seeds",
        )
        news_context_min_pair_count = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_NEWS_CONTEXT_MIN_PAIR_COUNT",
            config_name="news_context_min_pair_count",
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
        resolved_vector_search_domain_pack_routes = (
            _coerce_string_mapping(
                vector_search_domain_pack_routes,
                name="ades vector search domain pack routes",
            )
            if vector_search_domain_pack_routes is not None
            else dict(DEFAULT_VECTOR_SEARCH_DOMAIN_PACK_ROUTES)
        )
        resolved_vector_search_pack_collection_aliases = (
            _coerce_string_mapping(
                vector_search_pack_collection_aliases,
                name="ades vector search pack collection aliases",
            )
            if vector_search_pack_collection_aliases is not None
            else dict(DEFAULT_VECTOR_SEARCH_PACK_COLLECTION_ALIASES)
        )
        resolved_vector_search_country_aliases = (
            _coerce_string_mapping(
                vector_search_country_aliases,
                name="ades vector search country aliases",
            )
            if vector_search_country_aliases is not None
            else dict(DEFAULT_VECTOR_SEARCH_COUNTRY_ALIASES)
        )
        resolved_graph_context_enabled = (
            _coerce_bool(graph_context_enabled, name="ades graph context enabled")
            if graph_context_enabled is not None
            else False
        )
        resolved_graph_context_max_depth = (
            _coerce_int(graph_context_max_depth, name="ades graph context max depth")
            if graph_context_max_depth is not None
            else 2
        )
        resolved_graph_context_related_limit = (
            _coerce_int(
                graph_context_related_limit,
                name="ades graph context related limit",
            )
            if graph_context_related_limit is not None
            else DEFAULT_GRAPH_CONTEXT_RELATED_LIMIT
        )
        resolved_graph_context_seed_neighbor_limit = (
            _coerce_int(
                graph_context_seed_neighbor_limit,
                name="ades graph context seed neighbor limit",
            )
            if graph_context_seed_neighbor_limit is not None
            else DEFAULT_GRAPH_CONTEXT_SEED_NEIGHBOR_LIMIT
        )
        resolved_graph_context_candidate_limit = (
            _coerce_int(
                graph_context_candidate_limit,
                name="ades graph context candidate limit",
            )
            if graph_context_candidate_limit is not None
            else DEFAULT_GRAPH_CONTEXT_CANDIDATE_LIMIT
        )
        resolved_graph_context_min_supporting_seeds = (
            _coerce_int(
                graph_context_min_supporting_seeds,
                name="ades graph context min supporting seeds",
            )
            if graph_context_min_supporting_seeds is not None
            else DEFAULT_GRAPH_CONTEXT_MIN_SUPPORTING_SEEDS
        )
        resolved_graph_context_vector_proposals_enabled = (
            _coerce_bool(
                graph_context_vector_proposals_enabled,
                name="ades graph context vector proposals enabled",
            )
            if graph_context_vector_proposals_enabled is not None
            else False
        )
        resolved_graph_context_vector_proposal_limit = (
            _coerce_int(
                graph_context_vector_proposal_limit,
                name="ades graph context vector proposal limit",
            )
            if graph_context_vector_proposal_limit is not None
            else DEFAULT_GRAPH_CONTEXT_VECTOR_PROPOSAL_LIMIT
        )
        resolved_graph_context_genericity_penalty_enabled = (
            _coerce_bool(
                graph_context_genericity_penalty_enabled,
                name="ades graph context genericity penalty enabled",
            )
            if graph_context_genericity_penalty_enabled is not None
            else True
        )
        resolved_news_context_min_supporting_seeds = (
            _coerce_int(
                news_context_min_supporting_seeds,
                name="ades news context min supporting seeds",
            )
            if news_context_min_supporting_seeds is not None
            else DEFAULT_NEWS_CONTEXT_MIN_SUPPORTING_SEEDS
        )
        resolved_news_context_min_pair_count = (
            _coerce_int(
                news_context_min_pair_count,
                name="ades news context min pair count",
            )
            if news_context_min_pair_count is not None
            else DEFAULT_NEWS_CONTEXT_MIN_PAIR_COUNT
        )
        if resolved_graph_context_max_depth < 1:
            raise InvalidConfigurationError(
                f"Invalid ades graph context max depth: {resolved_graph_context_max_depth!r}"
            )
        if resolved_graph_context_related_limit <= 0:
            raise InvalidConfigurationError(
                "Invalid ades graph context related limit: "
                f"{resolved_graph_context_related_limit!r}"
            )
        if resolved_graph_context_seed_neighbor_limit <= 0:
            raise InvalidConfigurationError(
                "Invalid ades graph context seed neighbor limit: "
                f"{resolved_graph_context_seed_neighbor_limit!r}"
            )
        if resolved_graph_context_candidate_limit <= 0:
            raise InvalidConfigurationError(
                "Invalid ades graph context candidate limit: "
                f"{resolved_graph_context_candidate_limit!r}"
            )
        if resolved_graph_context_min_supporting_seeds <= 0:
            raise InvalidConfigurationError(
                "Invalid ades graph context min supporting seeds: "
                f"{resolved_graph_context_min_supporting_seeds!r}"
            )
        if resolved_graph_context_vector_proposal_limit <= 0:
            raise InvalidConfigurationError(
                "Invalid ades graph context vector proposal limit: "
                f"{resolved_graph_context_vector_proposal_limit!r}"
            )
        if resolved_news_context_min_supporting_seeds <= 0:
            raise InvalidConfigurationError(
                "Invalid ades news context min supporting seeds: "
                f"{resolved_news_context_min_supporting_seeds!r}"
            )
        if resolved_news_context_min_pair_count <= 0:
            raise InvalidConfigurationError(
                "Invalid ades news context min pair count: "
                f"{resolved_news_context_min_pair_count!r}"
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
            vector_search_domain_pack_routes=resolved_vector_search_domain_pack_routes,
            vector_search_pack_collection_aliases=(
                resolved_vector_search_pack_collection_aliases
            ),
            vector_search_country_aliases=resolved_vector_search_country_aliases,
            graph_context_enabled=resolved_graph_context_enabled,
            graph_context_artifact_path=(
                Path(str(graph_context_artifact_path)).expanduser()
                if graph_context_artifact_path
                else None
            ),
            graph_context_max_depth=resolved_graph_context_max_depth,
            graph_context_related_limit=resolved_graph_context_related_limit,
            graph_context_seed_neighbor_limit=resolved_graph_context_seed_neighbor_limit,
            graph_context_candidate_limit=resolved_graph_context_candidate_limit,
            graph_context_min_supporting_seeds=resolved_graph_context_min_supporting_seeds,
            graph_context_vector_proposals_enabled=(
                resolved_graph_context_vector_proposals_enabled
            ),
            graph_context_vector_proposal_limit=(
                resolved_graph_context_vector_proposal_limit
            ),
            graph_context_genericity_penalty_enabled=(
                resolved_graph_context_genericity_penalty_enabled
            ),
            news_context_artifact_path=(
                Path(str(news_context_artifact_path)).expanduser()
                if news_context_artifact_path
                else None
            ),
            news_context_min_supporting_seeds=(
                resolved_news_context_min_supporting_seeds
            ),
            news_context_min_pair_count=resolved_news_context_min_pair_count,
            config_path=config_path,
        )


def get_settings() -> Settings:
    """Resolve settings from the current environment."""

    return Settings.from_env()
