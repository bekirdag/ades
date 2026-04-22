"""Runtime configuration."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
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
DEFAULT_FINANCE_VECTOR_COLLECTION_ALIAS = "ades-qids-finance-current"
DEFAULT_BUSINESS_VECTOR_COLLECTION_ALIAS = "ades-qids-business-current"
DEFAULT_ECONOMICS_VECTOR_COLLECTION_ALIAS = "ades-qids-economics-current"
DEFAULT_POLITICS_VECTOR_COLLECTION_ALIAS = "ades-qids-politics-current"
DEFAULT_FINANCE_POLITICS_VECTOR_COLLECTION_ALIAS = "ades-qids-finance-politics-current"
DEFAULT_VECTOR_RELATED_LIMIT = 5
DEFAULT_GRAPH_CONTEXT_RELATED_LIMIT = 5
DEFAULT_GRAPH_CONTEXT_SEED_NEIGHBOR_LIMIT = 24
DEFAULT_GRAPH_CONTEXT_CANDIDATE_LIMIT = 128
DEFAULT_GRAPH_CONTEXT_MIN_SUPPORTING_SEEDS = 2
DEFAULT_GRAPH_CONTEXT_VECTOR_PROPOSAL_LIMIT = 8
DEFAULT_GENERAL_PROFILE_PACK_IDS = ("general-en",)
DEFAULT_POLITICS_PROFILE_PACK_IDS = ("politics-vector-en",)
DEFAULT_POLITICS_PROFILE_DEFAULT_PACK = "politics-vector-en"
DEFAULT_BUSINESS_PROFILE_PACK_IDS = ("business-vector-en",)
DEFAULT_BUSINESS_PROFILE_DEFAULT_PACK = "general-en"
DEFAULT_ECONOMICS_PROFILE_PACK_IDS = ("economics-vector-en",)
DEFAULT_ECONOMICS_PROFILE_DEFAULT_PACK = "general-en"
DEFAULT_FINANCE_PROFILE_PACK_IDS = (
    "general-en",
    "finance-en",
    "finance-ar-en",
    "finance-us-en",
    "finance-uk-en",
    "finance-de-en",
    "finance-fr-en",
    "finance-jp-en",
)
CONFIG_FILE_ENV = "ADES_CONFIG_FILE"
DATABASE_URL_ENV = "ADES_DATABASE_URL"
DEFAULT_CONFIG_PATHS = (
    Path("/etc/ades/config.toml"),
    Path.home() / ".config" / "ades" / "config.toml",
)


class InvalidConfigurationError(ValueError):
    """Raised when one configuration value cannot be parsed safely."""


@dataclass(frozen=True)
class RetrievalProfile:
    """One whitelisted retrieval profile for domain-specific routing."""

    name: str
    domain_hints: tuple[str, ...] = ()
    pack_ids: tuple[str, ...] = ()
    default_pack: str | None = None
    vector_search_enabled: bool | None = None
    vector_search_collection_alias: str | None = None
    vector_search_related_limit: int | None = None
    vector_search_score_threshold: float | None = None
    graph_context_enabled: bool | None = None
    graph_context_artifact_path: Path | None = None
    graph_context_vector_proposals_enabled: bool | None = None
    graph_context_vector_proposal_limit: int | None = None


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


def _normalize_profile_key(value: str) -> str:
    return value.strip().casefold().replace("-", "_")


def _coerce_optional_str(value: Any, *, name: str) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized


def _coerce_str_tuple(value: Any, *, name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        normalized = value.strip()
        return (normalized,) if normalized else ()
    if not isinstance(value, (list, tuple, set)):
        raise InvalidConfigurationError(f"Invalid {name}: {value!r}")
    values: list[str] = []
    seen: set[str] = set()
    for item in value:
        normalized = str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)
    return tuple(values)


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


def _default_retrieval_profiles(
    *,
    default_pack: str,
    graph_context_artifact_path: Path | None,
    vector_search_collection_alias: str,
) -> dict[str, RetrievalProfile]:
    finance_pack_ids = DEFAULT_FINANCE_PROFILE_PACK_IDS
    politics_pack_ids = DEFAULT_POLITICS_PROFILE_PACK_IDS
    finance_politics_pack_ids = (*finance_pack_ids, "politics-vector-en")
    business_pack_ids = DEFAULT_BUSINESS_PROFILE_PACK_IDS
    economics_pack_ids = DEFAULT_ECONOMICS_PROFILE_PACK_IDS
    return {
        "general": RetrievalProfile(
            name="general",
            domain_hints=("general", "default", "auto"),
            pack_ids=DEFAULT_GENERAL_PROFILE_PACK_IDS,
            default_pack=default_pack,
            vector_search_collection_alias=vector_search_collection_alias,
            graph_context_artifact_path=graph_context_artifact_path,
        ),
        "politics": RetrievalProfile(
            name="politics",
            domain_hints=("politics", "political", "policy", "government"),
            pack_ids=politics_pack_ids,
            default_pack=DEFAULT_POLITICS_PROFILE_DEFAULT_PACK,
            vector_search_collection_alias=DEFAULT_POLITICS_VECTOR_COLLECTION_ALIAS,
            graph_context_artifact_path=graph_context_artifact_path,
        ),
        "finance": RetrievalProfile(
            name="finance",
            domain_hints=("finance",),
            pack_ids=finance_pack_ids,
            default_pack="general-en",
            vector_search_collection_alias=DEFAULT_FINANCE_VECTOR_COLLECTION_ALIAS,
            graph_context_artifact_path=graph_context_artifact_path,
        ),
        "business": RetrievalProfile(
            name="business",
            domain_hints=("business",),
            pack_ids=business_pack_ids,
            default_pack=DEFAULT_BUSINESS_PROFILE_DEFAULT_PACK,
            vector_search_collection_alias=DEFAULT_BUSINESS_VECTOR_COLLECTION_ALIAS,
            graph_context_artifact_path=graph_context_artifact_path,
        ),
        "economics": RetrievalProfile(
            name="economics",
            domain_hints=("economics", "economic", "macro"),
            pack_ids=economics_pack_ids,
            default_pack=DEFAULT_ECONOMICS_PROFILE_DEFAULT_PACK,
            vector_search_collection_alias=DEFAULT_ECONOMICS_VECTOR_COLLECTION_ALIAS,
            graph_context_artifact_path=graph_context_artifact_path,
        ),
        "finance_politics": RetrievalProfile(
            name="finance_politics",
            domain_hints=("finance_politics", "finance-politics"),
            pack_ids=finance_politics_pack_ids,
            default_pack="general-en",
            vector_search_collection_alias=DEFAULT_FINANCE_POLITICS_VECTOR_COLLECTION_ALIAS,
            graph_context_artifact_path=graph_context_artifact_path,
        ),
    }


def _profile_from_config_entry(
    name: str,
    payload: Any,
    *,
    default_profile: RetrievalProfile | None = None,
) -> RetrievalProfile:
    if not isinstance(payload, dict):
        raise InvalidConfigurationError(
            f"Invalid retrieval_profiles.{name}: expected table/object."
        )
    raw_vector_search_enabled = payload.get("vector_search_enabled")
    raw_graph_context_enabled = payload.get("graph_context_enabled")
    raw_vector_proposals_enabled = payload.get(
        "graph_context_vector_proposals_enabled"
    )
    raw_graph_context_artifact_path = payload.get("graph_context_artifact_path")
    raw_vector_search_related_limit = payload.get("vector_search_related_limit")
    raw_vector_search_score_threshold = payload.get("vector_search_score_threshold")
    raw_vector_proposal_limit = payload.get("graph_context_vector_proposal_limit")
    return RetrievalProfile(
        name=name,
        domain_hints=(
            _coerce_str_tuple(
                payload.get("domain_hints"),
                name=f"retrieval_profiles.{name}.domain_hints",
            )
            or (default_profile.domain_hints if default_profile is not None else ())
        ),
        pack_ids=(
            _coerce_str_tuple(
                payload.get("pack_ids"),
                name=f"retrieval_profiles.{name}.pack_ids",
            )
            or (default_profile.pack_ids if default_profile is not None else ())
        ),
        default_pack=(
            _coerce_optional_str(
                payload.get("default_pack"),
                name=f"retrieval_profiles.{name}.default_pack",
            )
            or (default_profile.default_pack if default_profile is not None else None)
        ),
        vector_search_enabled=(
            _coerce_bool(
                raw_vector_search_enabled,
                name=f"retrieval_profiles.{name}.vector_search_enabled",
            )
            if raw_vector_search_enabled is not None
            else (default_profile.vector_search_enabled if default_profile else None)
        ),
        vector_search_collection_alias=(
            _coerce_optional_str(
                payload.get("vector_search_collection_alias")
                or payload.get("collection_alias"),
                name=f"retrieval_profiles.{name}.vector_search_collection_alias",
            )
            or (
                default_profile.vector_search_collection_alias
                if default_profile is not None
                else None
            )
        ),
        vector_search_related_limit=(
            _coerce_int(
                raw_vector_search_related_limit,
                name=f"retrieval_profiles.{name}.vector_search_related_limit",
            )
            if raw_vector_search_related_limit is not None
            else (default_profile.vector_search_related_limit if default_profile else None)
        ),
        vector_search_score_threshold=(
            _coerce_float(
                raw_vector_search_score_threshold,
                name=f"retrieval_profiles.{name}.vector_search_score_threshold",
            )
            if raw_vector_search_score_threshold is not None
            else (default_profile.vector_search_score_threshold if default_profile else None)
        ),
        graph_context_enabled=(
            _coerce_bool(
                raw_graph_context_enabled,
                name=f"retrieval_profiles.{name}.graph_context_enabled",
            )
            if raw_graph_context_enabled is not None
            else (default_profile.graph_context_enabled if default_profile else None)
        ),
        graph_context_artifact_path=(
            Path(str(raw_graph_context_artifact_path)).expanduser()
            if raw_graph_context_artifact_path
            else (
                default_profile.graph_context_artifact_path
                if default_profile is not None
                else None
            )
        ),
        graph_context_vector_proposals_enabled=(
            _coerce_bool(
                raw_vector_proposals_enabled,
                name=(
                    "retrieval_profiles."
                    f"{name}.graph_context_vector_proposals_enabled"
                ),
            )
            if raw_vector_proposals_enabled is not None
            else (
                default_profile.graph_context_vector_proposals_enabled
                if default_profile is not None
                else None
            )
        ),
        graph_context_vector_proposal_limit=(
            _coerce_int(
                raw_vector_proposal_limit,
                name=(
                    "retrieval_profiles."
                    f"{name}.graph_context_vector_proposal_limit"
                ),
            )
            if raw_vector_proposal_limit is not None
            else (
                default_profile.graph_context_vector_proposal_limit
                if default_profile is not None
                else None
            )
        ),
    )


def _resolve_retrieval_profiles(
    raw_profiles: Any,
    *,
    default_pack: str,
    graph_context_artifact_path: Path | None,
    vector_search_collection_alias: str,
) -> dict[str, RetrievalProfile]:
    profiles = _default_retrieval_profiles(
        default_pack=default_pack,
        graph_context_artifact_path=graph_context_artifact_path,
        vector_search_collection_alias=vector_search_collection_alias,
    )
    if raw_profiles is None:
        return profiles
    if not isinstance(raw_profiles, dict):
        raise InvalidConfigurationError("Invalid retrieval_profiles: expected table/object.")
    for raw_name, payload in raw_profiles.items():
        normalized_name = _normalize_profile_key(str(raw_name))
        profiles[normalized_name] = _profile_from_config_entry(
            normalized_name,
            payload,
            default_profile=profiles.get(normalized_name),
        )
    return profiles


def _infer_profile_name_from_pack(pack: str | None) -> str | None:
    normalized_pack = _normalize_profile_key(pack or "")
    if not normalized_pack:
        return None
    if normalized_pack.startswith("business"):
        return "business"
    if normalized_pack.startswith("economics"):
        return "economics"
    if normalized_pack.startswith("politics"):
        return "politics"
    if normalized_pack.startswith("finance"):
        return "finance"
    if normalized_pack.startswith("general"):
        return "general"
    return None


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
    retrieval_profile_name: str | None = None
    retrieval_profile_pack_ids: tuple[str, ...] = ()
    retrieval_profiles: dict[str, RetrievalProfile] = field(default_factory=dict)
    config_path: Path | None = None

    def resolve_retrieval_profile(
        self,
        *,
        retrieval_profile: str | None = None,
        domain_hint: str | None = None,
        pack: str | None = None,
    ) -> RetrievalProfile | None:
        if not self.retrieval_profiles:
            return None
        if retrieval_profile:
            normalized_name = _normalize_profile_key(retrieval_profile)
            profile = self.retrieval_profiles.get(normalized_name)
            if profile is None:
                raise InvalidConfigurationError(
                    f"Unknown retrieval profile: {retrieval_profile}"
                )
            return profile
        if domain_hint:
            normalized_hint = _normalize_profile_key(domain_hint)
            direct_profile = self.retrieval_profiles.get(normalized_hint)
            if direct_profile is not None:
                return direct_profile
            for profile in self.retrieval_profiles.values():
                if normalized_hint in {
                    _normalize_profile_key(item) for item in profile.domain_hints
                }:
                    return profile
            raise InvalidConfigurationError(f"Unknown domain hint: {domain_hint}")
        current_profile_name = (
            _normalize_profile_key(self.retrieval_profile_name)
            if self.retrieval_profile_name
            else None
        )
        if current_profile_name and current_profile_name in self.retrieval_profiles:
            return self.retrieval_profiles[current_profile_name]
        inferred_profile_name = _infer_profile_name_from_pack(pack)
        if inferred_profile_name and inferred_profile_name in self.retrieval_profiles:
            return self.retrieval_profiles[inferred_profile_name]
        return self.retrieval_profiles.get("general")

    def apply_retrieval_profile(
        self,
        *,
        retrieval_profile: str | None = None,
        domain_hint: str | None = None,
        pack: str | None = None,
    ) -> "Settings":
        profile = self.resolve_retrieval_profile(
            retrieval_profile=retrieval_profile,
            domain_hint=domain_hint,
            pack=pack,
        )
        if profile is None:
            return self
        return replace(
            self,
            default_pack=profile.default_pack or self.default_pack,
            vector_search_enabled=(
                self.vector_search_enabled
                if profile.vector_search_enabled is None
                else profile.vector_search_enabled
            ),
            vector_search_collection_alias=(
                profile.vector_search_collection_alias
                or self.vector_search_collection_alias
            ),
            vector_search_related_limit=(
                self.vector_search_related_limit
                if profile.vector_search_related_limit is None
                else profile.vector_search_related_limit
            ),
            vector_search_score_threshold=(
                self.vector_search_score_threshold
                if profile.vector_search_score_threshold is None
                else profile.vector_search_score_threshold
            ),
            graph_context_enabled=(
                self.graph_context_enabled
                if profile.graph_context_enabled is None
                else profile.graph_context_enabled
            ),
            graph_context_artifact_path=(
                profile.graph_context_artifact_path or self.graph_context_artifact_path
            ),
            graph_context_vector_proposals_enabled=(
                self.graph_context_vector_proposals_enabled
                if profile.graph_context_vector_proposals_enabled is None
                else profile.graph_context_vector_proposals_enabled
            ),
            graph_context_vector_proposal_limit=(
                self.graph_context_vector_proposal_limit
                if profile.graph_context_vector_proposal_limit is None
                else profile.graph_context_vector_proposal_limit
            ),
            retrieval_profile_name=profile.name,
            retrieval_profile_pack_ids=profile.pack_ids,
        )

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
        retrieval_profile = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_RETRIEVAL_PROFILE",
            config_name="retrieval_profile",
        )
        graph_context_genericity_penalty_enabled = _value_from_env_or_config(
            env_map,
            config_values,
            env_name="ADES_GRAPH_CONTEXT_GENERICITY_PENALTY_ENABLED",
            config_name="graph_context_genericity_penalty_enabled",
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
        resolved_graph_context_artifact_path = (
            Path(str(graph_context_artifact_path)).expanduser()
            if graph_context_artifact_path
            else None
        )
        resolved_retrieval_profiles = _resolve_retrieval_profiles(
            config_values.get("retrieval_profiles"),
            default_pack=str(default_pack or DEFAULT_PACK),
            graph_context_artifact_path=resolved_graph_context_artifact_path,
            vector_search_collection_alias=str(
                vector_search_collection_alias or DEFAULT_VECTOR_COLLECTION_ALIAS
            ),
        )

        settings = cls(
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
            graph_context_enabled=resolved_graph_context_enabled,
            graph_context_artifact_path=resolved_graph_context_artifact_path,
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
            retrieval_profile_name=_coerce_optional_str(
                retrieval_profile,
                name="ades retrieval profile",
            ),
            retrieval_profiles=resolved_retrieval_profiles,
            config_path=config_path,
        )
        if settings.retrieval_profile_name:
            return settings.apply_retrieval_profile(
                retrieval_profile=settings.retrieval_profile_name,
                pack=settings.default_pack,
            )
        return settings


def get_settings() -> Settings:
    """Resolve settings from the current environment."""

    return Settings.from_env()
