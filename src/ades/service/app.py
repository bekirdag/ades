"""Local FastAPI application for ades."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
import hashlib
import os
from pathlib import Path
import re
import time
from typing import Literal

from fastapi import FastAPI, HTTPException, Query

from ..api import (
    _resolve_settings,
    activate_pack,
    benchmark_runtime,
    benchmark_matcher_backends,
    build_finance_source_bundle,
    build_finance_country_source_bundles,
    fetch_finance_source_snapshot,
    fetch_finance_country_source_snapshots,
    build_general_source_bundle,
    fetch_general_source_snapshot,
    build_medical_source_bundle,
    fetch_medical_source_snapshot,
    build_market_graph_store,
    build_qid_graph_store,
    build_registry,
    compare_extraction_quality_reports,
    deactivate_pack,
    diff_pack_versions,
    evaluate_extraction_quality,
    evaluate_extraction_release_thresholds,
    evaluate_vector_quality,
    evaluate_vector_release_thresholds_from_report,
    expand_impact_paths,
    generate_pack_source,
    get_pack,
    get_pack_health,
    list_available_packs,
    list_packs,
    lookup_candidates,
    npm_installer_info,
    prepare_registry_deploy_release,
    publish_generated_registry_release,
    publish_release,
    report_generated_pack,
    release_versions,
    refresh_generated_packs,
    remove_pack,
    smoke_test_published_generated_registry,
    status,
    sync_release_version,
    tag,
    tag_file,
    tag_files,
    validate_general_pack_quality,
    validate_finance_pack_quality,
    validate_medical_pack_quality,
    validate_release,
    verify_release,
    write_release_manifest,
)
from ..config import Settings
from ..packs.registry import PackRegistry
from ..packs.runtime import clear_pack_runtime_cache, load_pack_runtime
from ..news_events import (
    extract_news_event_signals,
    gate_terminal_candidates_by_event_signals,
)
from ..impact.graph_store import MarketGraphStore
from ..impact.source_catalog import classify_source_tier
from ..runtime_matcher import clear_runtime_matcher_cache, load_runtime_matcher
from ..storage import UnsupportedRuntimeConfigurationError
from ..version import __version__
from .models import (
    AvailablePackSummary,
    BatchFileTagRequest,
    BatchTagResponse,
    EntityMatch,
    FileTagRequest,
    ImpactCandidate,
    ImpactSourceEntity,
    ImpactExpansionRequest,
    ImpactExpansionResult,
    ImpactRelationshipPath,
    LookupResponse,
    MarketGraphStoreBuildRequest,
    MarketGraphStoreBuildResponse,
    NpmInstallerInfo,
    PackSummary,
    PackHealthResponse,
    QidGraphStoreBuildRequest,
    QidGraphStoreBuildResponse,
    ReleaseManifestRequest,
    ReleaseManifestResponse,
    ReleasePublishRequest,
    ReleasePublishResponse,
    ReleaseValidationRequest,
    ReleaseValidationResponse,
    ReleaseVersionState,
    ReleaseVersionSyncRequest,
    ReleaseVersionSyncResponse,
    ReleaseVerificationRequest,
    ReleaseVerificationResponse,
    RegistryBuildFinanceBundleRequest,
    RegistryBuildFinanceBundleResponse,
    RegistryBuildFinanceCountryBundlesRequest,
    RegistryBuildFinanceCountryBundlesResponse,
    RegistryFetchFinanceSourcesRequest,
    RegistryFetchFinanceSourcesResponse,
    RegistryFetchFinanceCountrySourcesRequest,
    RegistryFetchFinanceCountrySourcesResponse,
    RegistryFetchGeneralSourcesRequest,
    RegistryFetchGeneralSourcesResponse,
    RegistryFetchMedicalSourcesRequest,
    RegistryFetchMedicalSourcesResponse,
    RegistryBuildGeneralBundleRequest,
    RegistryBuildGeneralBundleResponse,
    RegistryBuildMedicalBundleRequest,
    RegistryBuildMedicalBundleResponse,
    RegistryBuildRequest,
    RegistryBuildResponse,
    RegistryBenchmarkRuntimeRequest,
    RegistryBenchmarkRuntimeResponse,
    RegistryBenchmarkMatcherBackendsRequest,
    RegistryBenchmarkMatcherBackendsResponse,
    RegistryCompareExtractionQualityRequest,
    RegistryCompareExtractionQualityResponse,
    RegistryDiffPackRequest,
    RegistryEvaluateExtractionQualityRequest,
    RegistryEvaluateExtractionQualityResponse,
    RegistryEvaluateReleaseThresholdsRequest,
    RegistryEvaluateReleaseThresholdsResponse,
    RegistryEvaluateVectorQualityRequest,
    RegistryEvaluateVectorQualityResponse,
    RegistryEvaluateVectorReleaseThresholdsRequest,
    RegistryEvaluateVectorReleaseThresholdsResponse,
    RegistryPackQualityResponse,
    RegistryPackDiffResponse,
    RegistryPublishGeneratedReleaseRequest,
    RegistryPublishGeneratedReleaseResponse,
    RegistryPrepareDeployReleaseRequest,
    RegistryPrepareDeployReleaseResponse,
    RegistrySmokePublishedReleaseRequest,
    RegistrySmokePublishedReleaseResponse,
    RegistryRefreshGeneratedPacksRequest,
    RegistryRefreshGeneratedPacksResponse,
    RegistryValidateFinanceQualityRequest,
    RegistryValidateFinanceQualityResponse,
    RegistryValidateGeneralQualityRequest,
    RegistryValidateMedicalQualityRequest,
    RegistryGeneratePackRequest,
    RegistryGeneratePackResponse,
    RegistryReportPackRequest,
    RegistryReportPackResponse,
    NewsAnalyzeArtifactMetadata,
    NewsAnalyzeArtifactVersions,
    NewsAnalyzeCandidatePath,
    NewsAnalyzeCountryScope,
    NewsAnalyzeDebug,
    NewsAnalyzeDebugPath,
    NewsAnalyzeDebugRejectedCandidate,
    NewsAnalyzeDebugSourceEvidence,
    NewsAnalyzeDebugUnresolvedEntity,
    NewsAnalyzeDiagnostic,
    NewsAnalyzePackDecision,
    NewsAnalyzePassiveEntity,
    NewsAnalyzeRawEntity,
    NewsAnalyzeRejectedCandidate,
    NewsAnalyzeRelationshipProposal,
    NewsAnalyzeRequest,
    NewsAnalyzeResponse,
    NewsAnalyzeSourceLaneCoverage,
    NewsAnalyzeTopicScope,
    NewsAnalyzeUnresolvedEntity,
    StatusResponse,
    TagRequest,
    TagResponse,
)
from ..packs.bdya_phase6 import BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES
from ..packs.g20_country_aliases import DEFAULT_CURATED_G20_COUNTRY_ENTITIES


def _bool_option(options: dict[str, object] | None, key: str) -> bool | None:
    if not options or key not in options:
        return None
    value = options[key]
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    raise ValueError(f"Invalid boolean option for {key}.")


def _refinement_depth_option(options: dict[str, object] | None) -> str:
    if not options or "refinement_depth" not in options:
        return "light"
    value = options["refinement_depth"]
    normalized = str(value).strip().casefold()
    if normalized in {"light", "deep"}:
        return normalized
    raise ValueError("Invalid refinement_depth option.")


def _string_option(options: dict[str, object] | None, key: str) -> str | None:
    if not options or key not in options:
        return None
    value = options[key]
    if not isinstance(value, str):
        raise ValueError(f"Invalid string option for {key}.")
    normalized = value.strip()
    return normalized or None


def _int_option(options: dict[str, object] | None, key: str) -> int | None:
    if not options or key not in options:
        return None
    value = options[key]
    if isinstance(value, bool):
        raise ValueError(f"Invalid integer option for {key}.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer option for {key}.") from exc
    return parsed


def _list_string_option(options: dict[str, object] | None, key: str) -> list[str] | None:
    if not options or key not in options:
        return None
    value = options[key]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        result = [str(item).strip() for item in value if str(item).strip()]
        return result
    raise ValueError(f"Invalid list option for {key}.")


def _request_string_option(
    request_value: object | None,
    options: dict[str, object] | None,
    key: str,
) -> str | None:
    if request_value is not None:
        value = request_value
    elif options and key in options:
        value = options[key]
    else:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Invalid string option for {key}.")
    normalized = value.strip()
    return normalized or None


def _load_iso_country_names() -> dict[str, str]:
    names: dict[str, str] = {}
    for path in (Path("/usr/share/zoneinfo/iso3166.tab"),):
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line or line.startswith("#") or "\t" not in line:
                continue
            code, name = line.split("\t", 1)
            normalized_code = code.strip().casefold()
            normalized_name = name.strip()
            if re.fullmatch(r"[a-z]{2}", normalized_code) and normalized_name:
                names[normalized_code] = normalized_name
    return names


_CURATED_COUNTRY_BY_CODE = {
    str(entity["entity_id"]).split(":", 1)[1].casefold(): str(entity["canonical_text"])
    for entity in DEFAULT_CURATED_G20_COUNTRY_ENTITIES
    if isinstance(entity.get("entity_id"), str) and str(entity["entity_id"]).startswith("country:")
}
_COUNTRY_BY_CODE = {**_load_iso_country_names(), **_CURATED_COUNTRY_BY_CODE}
_NEWS_ANALYZE_BASE_PACKS = (
    "general-en",
    "business-vector-en",
    "economics-vector-en",
    "politics-vector-en",
    "finance-en",
)
_ENV_TRUE_VALUES = {"1", "true", "yes", "y", "on"}
_NEWS_ANALYZE_COUNTRY_FINANCE_PACK_BY_CODE = {
    "ar": "finance-ar-en",
    "au": "finance-au-en",
    "br": "finance-br-en",
    "ca": "finance-ca-en",
    "cn": "finance-cn-en",
    "de": "finance-de-en",
    "fr": "finance-fr-en",
    "id": "finance-id-en",
    "in": "finance-in-en",
    "it": "finance-it-en",
    "jp": "finance-jp-en",
    "kr": "finance-kr-en",
    "mx": "finance-mx-en",
    "ru": "finance-ru-en",
    "sa": "finance-sa-en",
    "tr": "finance-tr-en",
    "uk": "finance-uk-en",
    "us": "finance-us-en",
    "za": "finance-za-en",
}
_NO_TERMINAL_REASON_ORDER = (
    "no_entity_match",
    "no_path",
    "no_terminal_security",
    "event_incompatible",
    "low_tier",
    "stale",
    "conflicted",
    "macro_gated",
    "missing_country_pack",
    "missing_source_lane",
    "stale_artifact",
)
_NO_TERMINAL_REASON_MESSAGES = {
    "no_entity_match": "No production entity match was available for terminal expansion.",
    "no_path": "Recognized entities did not have a complete source-backed terminal path.",
    "no_terminal_security": "Source entities were recognized, but no terminal security or tradable asset was produced.",
    "event_incompatible": "Terminal candidates were rejected by event compatibility gates.",
    "low_tier": "Only low-tier or proposal-only evidence was available.",
    "stale": "A stale source or relationship warning affected terminal candidate generation.",
    "conflicted": "A conflict warning affected terminal candidate generation.",
    "macro_gated": "Currency, rates, or broad macro candidates were blocked by macro/FX/rates gating.",
    "missing_country_pack": "A needed country finance pack was not installed or available.",
    "missing_source_lane": "A needed source lane or impact expansion path was unavailable.",
    "stale_artifact": "A stale artifact warning affected terminal candidate generation.",
}
_NEWS_ANALYZE_DEBUG_PATH_LIMIT = 32
_NEWS_ANALYZE_DEBUG_REJECTED_LIMIT = 32
_NEWS_ANALYZE_DEBUG_UNRESOLVED_LIMIT = 32
_NEWS_ANALYZE_DEBUG_EVIDENCE_LIMIT = 24
_NEWS_ANALYZE_DEBUG_EVIDENCE_CHAR_LIMIT = 240
_NEWS_ANALYZE_DEBUG_ENTITY_REF_LIMIT = 64
_COUNTRY_ALIAS_TO_CODE: dict[str, str] = {}
for _code, _name in _COUNTRY_BY_CODE.items():
    _normalized_name = str(_name).strip()
    if not _normalized_name:
        continue
    _COUNTRY_ALIAS_TO_CODE[_normalized_name.casefold()] = _code
    if "," in _normalized_name:
        _short_name = _normalized_name.split(",", 1)[0].strip()
        if _short_name:
            _COUNTRY_ALIAS_TO_CODE[_short_name.casefold()] = _code
for _country in DEFAULT_CURATED_G20_COUNTRY_ENTITIES:
    entity_id = str(_country.get("entity_id", ""))
    if not entity_id.startswith("country:"):
        continue
    code = entity_id.split(":", 1)[1].casefold()
    _COUNTRY_ALIAS_TO_CODE[str(_country.get("canonical_text", "")).casefold()] = code
    for _alias in _country.get("aliases", []):
        if isinstance(_alias, str):
            _COUNTRY_ALIAS_TO_CODE[_alias.casefold()] = code

_HIDDEN_PASSIVE_ARTIFACT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "url_or_link",
        re.compile(
            r"(?:https?://|www\.|[A-Za-z0-9.-]+\.(?:com|org|net|int|gov|edu|io|co|uk|de|fr|tr|cn|jp|br|ca|au)(?:/|\b))",
            re.IGNORECASE,
        ),
    ),
    ("email", re.compile(r"^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$", re.IGNORECASE)),
    ("phone_number", re.compile(r"^\+?\d[\d\s().-]{6,}$")),
    (
        "percentage",
        re.compile(
            r"^[+-]?\d+(?:[.,]\d+)?\s*(?:%|percent|percentage points?|basis points?|bps)$",
            re.IGNORECASE,
        ),
    ),
    (
        "currency_amount",
        re.compile(
            r"^(?:[$\u20ac\u00a3\u00a5\u20ba\u20b9\u20a9]|(?:USD|EUR|GBP|JPY|TRY|CNY|CAD|AUD|BRL|INR|KRW|ZAR|SAR|MXN|RUB)\b)\s*[+-]?\d+(?:[.,]\d+)*(?:\s*(?:bn|billion|m|million|k))?$",
            re.IGNORECASE,
        ),
    ),
    (
        "raw_ticker_identifier",
        re.compile(
            r"\bfinance\s+[a-z]{2}\s+ticker(?:\s+[a-z0-9._:-]+)?\b",
            re.IGNORECASE,
        ),
    ),
    ("html_fragment", re.compile(r"<[^>]+>")),
)
_PASSIVE_ARTIFACT_LABELS = {
    "basis_point",
    "basis_points",
    "currency_amount",
    "date",
    "email",
    "html",
    "link",
    "money",
    "numeric",
    "number",
    "percentage",
    "phone",
    "price",
    "rate",
    "url",
    "website",
}
_CURRENCY_AMOUNT_LABELS = {"currency_amount", "money", "price"}
_PERCENTAGE_LABELS = {"percentage", "basis_point", "basis_points", "rate"}
_PASSIVE_POLICY_BODY_PATTERN = re.compile(
    r"\b(?:central bank|federal reserve|fed|ecb|treasury|ministry|minister|"
    r"parliament|commission|regulator|authority|cabinet|congress|senate|"
    r"government|court|sec|fda|ofac)\b",
    re.IGNORECASE,
)
_SOURCE_OUTLET_PATTERN = re.compile(
    r"\b(?:reuters|associated press|ap news|bloomberg|france 24|cnn|bbc|"
    r"cnbc|marketwatch|wall street journal|financial times)\b",
    re.IGNORECASE,
)
_PASSIVE_COLLISION_SURFACE_REASONS = {
    "jmm": ("weak_alias", "ambiguous_acronym"),
    "mbg": ("weak_alias", "ambiguous_acronym"),
    "oas": ("weak_alias", "ambiguous_acronym"),
    "onu": ("weak_alias", "ambiguous_acronym"),
    "six": ("weak_alias", "ambiguous_acronym"),
    "lee": ("weak_alias", "homograph_alias"),
}
_PASSIVE_GENERIC_FINANCE_ALIAS_TERMS = {
    "a",
    "an",
    "bank",
    "banks",
    "business",
    "company",
    "corp",
    "corporation",
    "finance",
    "financial",
    "financials",
    "firm",
    "group",
    "holding",
    "holdings",
    "inc",
    "industries",
    "industry",
    "institution",
    "institutions",
    "lender",
    "lenders",
    "limited",
    "llc",
    "ltd",
    "market",
    "markets",
    "plc",
    "sector",
    "services",
    "stock",
    "stocks",
    "the",
}
_DIRECT_TERMINAL_METADATA_CONTEXT_PATTERN = re.compile(
    r"(?:^|[\s(/-])(?:ap|associated press|reuters|afp|epa|shutterstock|getty images)"
    r"\s+photo(?:\b|/)|\b(?:file\s+)?(?:photo|image|picture|caption)"
    r"\s*(?:by|/|:|credit|courtesy|provided by)?\b|"
    r"\b(?:credit|courtesy|copyright|provided by|via)\s*(?:[:/-]|\b)",
    re.IGNORECASE,
)
_DIRECT_MARKET_TERMINAL_ENTITY_TYPES = {"commodity", "crypto"}
_DIRECT_MARKET_TERMINAL_SIGNAL_FAMILIES = {
    "agriculture",
    "commodity",
    "crypto",
    "energy",
    "safe_haven",
}
_INTERNAL_NEWS_METADATA_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^\s*primary\s+signal\s*:\s*.*$", re.IGNORECASE | re.MULTILINE),
    re.compile(
        r"^\s*market\s+exposure\s+is\s+concentrated\s+around\b.*$",
        re.IGNORECASE | re.MULTILINE,
    ),
)
_NON_MARKET_SIGNAL_TERMS = {
    "barcelona",
    "celebrity",
    "entertainment",
    "fifa",
    "football",
    "gossip",
    "league",
    "soccer",
    "sports",
    "sports rights",
    "tabloid",
}
_MARKET_CONTEXT_TERMS = {
    "acquisition",
    "bid",
    "bond",
    "buyout",
    "company",
    "earnings",
    "equity",
    "exchange",
    "investor",
    "issuer",
    "market",
    "merger",
    "regulator",
    "revenue",
    "share",
    "stock",
    "takeover",
    "ticker",
}
_CORPORATE_HQ_COUNTRY_CUES: tuple[tuple[str, re.Pattern[str], str], ...] = (
    ("uk", re.compile(r"\b(?:bp|british\s+petroleum)\b", re.IGNORECASE), "BP"),
    ("de", re.compile(r"\bbayer\b", re.IGNORECASE), "Bayer"),
)
_CRYPTO_TOPIC_PATTERN = re.compile(
    r"\b(?:bitcoin|btc|ether|ethereum|crypto(?:currency|currencies)?|digital\s+assets?)\b",
    re.IGNORECASE,
)
_LEGAL_TOPIC_PATTERN = re.compile(
    r"\b(?:court|lawsuit|legal|penalt(?:y|ies)|fine|fined|settlement|regulator|regulatory|enforcement|probe)\b",
    re.IGNORECASE,
)
_MA_TOPIC_PATTERN = re.compile(
    r"\b(?:m&a|merger|takeover|acquisition|buyout|bid|offer|stake|shares?)\b",
    re.IGNORECASE,
)
_TECH_TOPIC_PATTERN = re.compile(
    r"\b(?:ai|artificial\s+intelligence|cloud|microsoft|semiconductor|chipmaker|software|technology|tech)\b",
    re.IGNORECASE,
)
_NON_PHYSICAL_ASSET_PATTERN = re.compile(
    r"\b(?:bitcoin|crypto(?:currency|currencies)?|digital\s+assets?|software|ai|artificial\s+intelligence)\b",
    re.IGNORECASE,
)
_AIRLINE_NEGATIVE_COST_PATTERN = re.compile(
    r"\b(?:oil|crude|brent|wti|fuel|jet\s+fuel)\b.{0,120}\b(?:rise|rises|rose|rising|jump|jumps|jumped|surge|surges|surged|hike|hikes|hiked|higher|costlier|spike|spikes|spiked)\b"
    r"|\b(?:rise|rises|rose|rising|jump|jumps|jumped|surge|surges|surged|hike|hikes|hiked|higher|costlier|spike|spikes|spiked)\b.{0,120}\b(?:oil|crude|brent|wti|fuel|jet\s+fuel)\b",
    re.IGNORECASE | re.DOTALL,
)
_SEMICONDUCTOR_NEGATIVE_WARNING_PATTERN = re.compile(
    r"\b(?:semiconductor|semiconductors|chipmaker|chipmakers|chips?|morgan\s+stanley)\b.{0,140}\b(?:warn|warns|warned|warning|downgrade|downgrades|downgraded|cut|cuts|cutting|lower|lowers|lowered|weak|weaker|negative)\b"
    r"|\b(?:warn|warns|warned|warning|downgrade|downgrades|downgraded|cut|cuts|cutting|lower|lowers|lowered|weak|weaker|negative)\b.{0,140}\b(?:semiconductor|semiconductors|chipmaker|chipmakers|chips?|morgan\s+stanley)\b",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class _DirectMarketTerminalRecord:
    entity_ref: str
    name: str
    entity_type: str
    category: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class _MarketSectorSeedSpec:
    search_terms: tuple[str, ...]
    aliases: tuple[str, ...]


_MARKET_SECTOR_SEED_SIGNAL_TYPES = {
    "sector_policy_change",
    "regulatory_enforcement",
    "tariff",
    "export_control",
    "sanctions",
    "supply_disruption",
    "production_decrease",
    "production_increase",
}
_MARKET_SECTOR_SEED_FAMILIES = {"equity", "ticker", "sector", "equity_index"}
_MARKET_SECTOR_SEED_ENTITY_TYPES = ("sector", "organization")
_MARKET_SECTOR_SEED_LIMIT_PER_TERM = 8
_MARKET_SECTOR_SEED_MAX_REFS = 12
_MARKET_SECTOR_SEED_SPECS: tuple[_MarketSectorSeedSpec, ...] = (
    _MarketSectorSeedSpec(
        search_terms=("mining", "minerals", "metals"),
        aliases=(
            "mining",
            "miner",
            "miners",
            "mining company",
            "mining companies",
            "listed miners",
            "mining sector",
            "mining industry",
            "metal miners",
            "metals mining",
        ),
    ),
    _MarketSectorSeedSpec(
        search_terms=("bank", "banking", "lender"),
        aliases=("bank", "banks", "banking", "lender", "lenders"),
    ),
    _MarketSectorSeedSpec(
        search_terms=("oil", "gas", "energy"),
        aliases=(
            "oil",
            "gas",
            "oil and gas",
            "energy company",
            "energy companies",
            "energy sector",
            "power utilities",
        ),
    ),
    _MarketSectorSeedSpec(
        search_terms=("airline", "aviation", "airways"),
        aliases=("airline", "airlines", "aviation", "airways", "carrier", "carriers"),
    ),
    _MarketSectorSeedSpec(
        search_terms=("shipping", "freight", "logistics"),
        aliases=("shipping", "freight", "logistics", "ports", "port operators"),
    ),
    _MarketSectorSeedSpec(
        search_terms=("semiconductor", "chip"),
        aliases=("semiconductor", "semiconductors", "chipmaker", "chipmakers", "chips"),
    ),
    _MarketSectorSeedSpec(
        search_terms=("pharma", "pharmaceutical", "healthcare"),
        aliases=("pharma", "pharmaceutical", "drugmaker", "drugmakers", "healthcare"),
    ),
    _MarketSectorSeedSpec(
        search_terms=("telecom", "telecommunications"),
        aliases=("telecom", "telecoms", "telecommunications"),
    ),
    _MarketSectorSeedSpec(
        search_terms=("agriculture", "food"),
        aliases=("agriculture", "farm", "farms", "food producer", "food producers"),
    ),
)


def _dedupe_string_values(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = value.strip() if isinstance(value, str) else ""
        key = normalized.casefold()
        if not normalized or key in seen:
            continue
        seen.add(key)
        result.append(normalized)
    return result


def _candidate_artifact_ref(artifact_versions: NewsAnalyzeArtifactVersions) -> str | None:
    return (
        artifact_versions.impact_artifact_hash
        or artifact_versions.impact_artifact_version
        or artifact_versions.impact_graph_version
    )


def _candidate_path_confidence(candidate: ImpactCandidate) -> float | None:
    values = [candidate.confidence]
    for relationship_path in candidate.relationship_paths:
        values.extend(edge.confidence for edge in relationship_path.edges)
    return min(values) if values else None


def _relationship_path_confidence(
    candidate: ImpactCandidate,
    relationship_path: ImpactRelationshipPath,
) -> float | None:
    values = [candidate.confidence]
    values.extend(edge.confidence for edge in relationship_path.edges)
    return min(values) if values else None


def _weakest_edge(relationship_path: ImpactRelationshipPath) -> object | None:
    if not relationship_path.edges:
        return None
    return min(
        relationship_path.edges,
        key=lambda edge: (
            edge.confidence,
            edge.source_ref.casefold(),
            edge.relation.casefold(),
            edge.target_ref.casefold(),
        ),
    )


def _edge_ref(edge: object | None) -> str | None:
    if edge is None:
        return None
    source_ref = getattr(edge, "source_ref", "")
    relation = getattr(edge, "relation", "")
    target_ref = getattr(edge, "target_ref", "")
    if not (source_ref and relation and target_ref):
        return None
    return f"{source_ref}->{relation}->{target_ref}"


def _edge_source_tier(edge: object) -> str:
    source_tier = str(getattr(edge, "source_tier", "") or "").strip()
    if source_tier:
        return source_tier
    return classify_source_tier(
        str(getattr(edge, "source_name", "") or ""),
        str(getattr(edge, "source_url", "") or ""),
    )


def _edge_effective_from(edge: object) -> str | None:
    effective_from = getattr(edge, "effective_from", None)
    if effective_from:
        return str(effective_from)
    source_year = getattr(edge, "source_year", None)
    if isinstance(source_year, int):
        return f"{source_year:04d}-01-01"
    return None


def _edge_effective_to(edge: object) -> str | None:
    effective_to = getattr(edge, "effective_to", None)
    return str(effective_to) if effective_to else None


def _path_source_tiers(relationship_path: ImpactRelationshipPath) -> list[str]:
    return _dedupe_string_values([_edge_source_tier(edge) for edge in relationship_path.edges])


def _path_effective_from(relationship_path: ImpactRelationshipPath) -> str | None:
    values = _dedupe_string_values([_edge_effective_from(edge) for edge in relationship_path.edges])
    return max(values) if values else None


def _path_effective_to(relationship_path: ImpactRelationshipPath) -> str | None:
    values = _dedupe_string_values([_edge_effective_to(edge) for edge in relationship_path.edges])
    return min(values) if values else None


def _terminal_identity_parts(
    terminal_ref: str,
) -> tuple[str | None, str | None, str | None, dict[str, str]]:
    raw_ref = terminal_ref.strip()
    security_ids: dict[str, str] = {}
    if raw_ref:
        security_ids["ades_ref"] = raw_ref

    finance_ticker_match = re.match(
        r"^finance-([a-z]{2})-ticker:(.+)$",
        raw_ref,
        flags=re.IGNORECASE,
    )
    if finance_ticker_match:
        jurisdiction = finance_ticker_match.group(1).casefold()
        ticker_parts = [part for part in finance_ticker_match.group(2).split(":") if part]
        if not ticker_parts:
            return jurisdiction, None, None, security_ids
        exchange = ticker_parts[0] if len(ticker_parts) > 1 else None
        ticker = ":".join(ticker_parts[1:]) if len(ticker_parts) > 1 else ticker_parts[0]
        security_ids["ticker"] = ticker
        if exchange:
            security_ids["exchange_ticker"] = f"{exchange}:{ticker}"
        return jurisdiction, exchange, ticker, security_ids

    finance_colon_ref_match = re.match(
        r"^finance-([a-z]{2}):(issuer|security|equity):(.+)$",
        raw_ref,
        flags=re.IGNORECASE,
    )
    if finance_colon_ref_match:
        jurisdiction = finance_colon_ref_match.group(1).casefold()
        ref_type = finance_colon_ref_match.group(2).casefold()
        local_id = finance_colon_ref_match.group(3).strip()
        if ref_type == "equity" and local_id:
            security_ids["ticker"] = local_id
            return jurisdiction, None, local_id, security_ids
        if ref_type == "security" and local_id:
            security_ids["local_security_id"] = local_id
        return jurisdiction, None, None, security_ids

    finance_ref_match = re.match(
        r"^finance-([a-z]{2})-(?:issuer|security|equity):(.+)$",
        raw_ref,
        flags=re.IGNORECASE,
    )
    if finance_ref_match:
        return finance_ref_match.group(1).casefold(), None, None, security_ids

    ades_security_match = re.match(
        r"^ades:security:([a-z]{2}):([^:]+):(.+)$",
        raw_ref,
        flags=re.IGNORECASE,
    )
    if ades_security_match:
        jurisdiction = ades_security_match.group(1).casefold()
        exchange = ades_security_match.group(2)
        security_ids["local_security_id"] = ades_security_match.group(3)
        return jurisdiction, exchange, None, security_ids

    ades_security_local_match = re.match(
        r"^ades:security:(.+)$",
        raw_ref,
        flags=re.IGNORECASE,
    )
    if ades_security_local_match:
        security_ids["local_security_id"] = ades_security_local_match.group(1)
        return None, None, None, security_ids

    prefixed_id_match = re.match(
        r"^(sec-cik|isin|openfigi|figi|lei):(.+)$",
        raw_ref,
        flags=re.IGNORECASE,
    )
    if prefixed_id_match:
        raw_key = prefixed_id_match.group(1).casefold()
        key = "cik" if raw_key == "sec-cik" else raw_key.replace("-", "_")
        security_ids[key] = prefixed_id_match.group(2)

    return None, None, None, security_ids


def _candidate_key(candidate: ImpactCandidate) -> str:
    return candidate.entity_ref.strip().casefold()


def _build_artifact_metadata(
    artifact_versions: NewsAnalyzeArtifactVersions,
) -> NewsAnalyzeArtifactMetadata:
    source_lane_versions = dict(artifact_versions.packs)
    if artifact_versions.impact_graph_version is not None:
        source_lane_versions["impact_graph"] = artifact_versions.impact_graph_version
    if artifact_versions.impact_artifact_version is not None:
        source_lane_versions["impact_artifact"] = artifact_versions.impact_artifact_version

    artifact_id = (
        artifact_versions.impact_artifact_hash
        or artifact_versions.impact_artifact_version
        or artifact_versions.impact_graph_version
        or artifact_versions.ades_version
    )
    return NewsAnalyzeArtifactMetadata(
        artifact_id=artifact_id,
        artifact_version=artifact_versions.impact_artifact_version,
        artifact_hash=artifact_versions.impact_artifact_hash,
        graph_version=artifact_versions.impact_graph_version,
        ades_version=artifact_versions.ades_version,
        source_lane_versions=source_lane_versions,
    )


def _build_candidate_paths(
    *,
    terminal_candidates: list[ImpactCandidate],
    artifact_ref: str | None,
) -> list[NewsAnalyzeCandidatePath]:
    candidate_paths: list[NewsAnalyzeCandidatePath] = []
    for candidate in terminal_candidates:
        relationship_paths = candidate.relationship_paths or [
            ImpactRelationshipPath(path_depth=0, edges=[])
        ]
        for relationship_path in relationship_paths:
            weakest_edge = _weakest_edge(relationship_path)
            jurisdiction, exchange, ticker, security_ids = _terminal_identity_parts(
                candidate.entity_ref
            )
            candidate_paths.append(
                NewsAnalyzeCandidatePath(
                    terminal_ref=candidate.entity_ref,
                    terminal_type=candidate.entity_type,
                    terminal_name=candidate.name,
                    jurisdiction=jurisdiction,
                    exchange=exchange,
                    ticker=ticker,
                    security_ids=security_ids,
                    source_entity_refs=candidate.source_entity_refs,
                    event_compatibility=candidate.compatible_event_types,
                    path_confidence=_relationship_path_confidence(candidate, relationship_path),
                    weakest_edge=weakest_edge,
                    weakest_edge_ref=_edge_ref(weakest_edge),
                    weakest_edge_confidence=(
                        getattr(weakest_edge, "confidence", None)
                        if weakest_edge is not None
                        else None
                    ),
                    source_tiers=_path_source_tiers(relationship_path),
                    effective_from=_path_effective_from(relationship_path),
                    effective_to=_path_effective_to(relationship_path),
                    relationship_path=relationship_path,
                    artifact_ref=artifact_ref,
                )
            )
    return candidate_paths


def _rejected_candidate(
    candidate: ImpactCandidate,
    *,
    reason_code: str,
    reason: str,
    artifact_ref: str | None,
) -> NewsAnalyzeRejectedCandidate:
    return NewsAnalyzeRejectedCandidate(
        terminal_ref=candidate.entity_ref,
        terminal_type=candidate.entity_type,
        terminal_name=candidate.name,
        reason_code=reason_code,
        reason=reason,
        source_entity_refs=candidate.source_entity_refs,
        event_compatibility=candidate.compatible_event_types,
        path_confidence=_candidate_path_confidence(candidate),
        relationship_paths=candidate.relationship_paths,
        artifact_ref=artifact_ref,
    )


def _build_rejected_candidates(
    *,
    pre_gate_candidates: list[ImpactCandidate],
    gated_candidates: list[ImpactCandidate],
    returned_candidates: list[ImpactCandidate],
    artifact_ref: str | None,
) -> list[NewsAnalyzeRejectedCandidate]:
    gated_refs = {_candidate_key(candidate) for candidate in gated_candidates}
    returned_refs = {_candidate_key(candidate) for candidate in returned_candidates}
    rejected: list[NewsAnalyzeRejectedCandidate] = []
    seen: set[tuple[str, str]] = set()

    for candidate in pre_gate_candidates:
        key = _candidate_key(candidate)
        if key and key not in gated_refs:
            marker = (key, "event_incompatible")
            if marker not in seen:
                seen.add(marker)
                rejected.append(
                    _rejected_candidate(
                        candidate,
                        reason_code="event_incompatible",
                        reason="Candidate did not satisfy ADES event-signal compatibility gates.",
                        artifact_ref=artifact_ref,
                    )
                )

    for candidate in gated_candidates:
        key = _candidate_key(candidate)
        if key and key not in returned_refs:
            marker = (key, "candidate_limit")
            if marker not in seen:
                seen.add(marker)
                rejected.append(
                    _rejected_candidate(
                        candidate,
                        reason_code="candidate_limit",
                        reason="Candidate was valid but omitted by the response candidate limit.",
                        artifact_ref=artifact_ref,
                    )
                )

    return rejected


def _build_no_terminal_reasons(
    *,
    terminal_candidates: list[ImpactCandidate],
    production_entities: list[EntityMatch],
    entity_refs: list[str],
    impact_paths: ImpactExpansionResult | None,
    pre_gate_candidates: list[ImpactCandidate],
    unresolved_entities: list[NewsAnalyzeUnresolvedEntity],
    rejected_candidates: list[NewsAnalyzeRejectedCandidate],
    pack_decisions: list[NewsAnalyzePackDecision],
    warnings: list[str],
) -> list[str]:
    if terminal_candidates:
        return []

    warning_text = " ".join(warnings).casefold()
    rejected_text = " ".join(
        [candidate.reason_code for candidate in rejected_candidates]
        + [candidate.reason for candidate in rejected_candidates]
    ).casefold()
    unresolved_reason_text = " ".join(
        entity.missing_reason for entity in unresolved_entities
    ).casefold()
    reasons: set[str] = set()

    if not production_entities and not entity_refs and impact_paths is None:
        reasons.add("no_entity_match")
    if unresolved_entities:
        reasons.add("no_path")
    if impact_paths is not None and impact_paths.source_entities and not pre_gate_candidates:
        reasons.add("no_terminal_security")
    if any(candidate.reason_code == "event_incompatible" for candidate in rejected_candidates):
        reasons.add("event_incompatible")
    if "event_signal_gate_filtered:missing_macro_fx_rates_event_signal" in warning_text:
        reasons.add("macro_gated")
    if "low_trust" in warning_text or "proposal_only" in warning_text:
        reasons.add("low_tier")
    if "conflict" in warning_text or "conflict" in rejected_text:
        reasons.add("conflicted")
    if "stale_artifact" in warning_text or "stale artifact" in warning_text:
        reasons.add("stale_artifact")
    if (
        "stale" in warning_text or "stale" in rejected_text or "stale" in unresolved_reason_text
    ) and "stale_artifact" not in reasons:
        reasons.add("stale")
    if any(decision.country_code and not decision.selected for decision in pack_decisions):
        reasons.add("missing_country_pack")
    if (entity_refs and impact_paths is None) or any(
        token in warning_text
        for token in (
            "source_lane",
            "pack_failed",
            "pack_not_found",
            "impact_expand_failed",
        )
    ):
        reasons.add("missing_source_lane")

    if not reasons:
        reasons.add("no_path" if production_entities or entity_refs else "no_entity_match")

    return [reason for reason in _NO_TERMINAL_REASON_ORDER if reason in reasons]


def _known_unresolved_entity_ref(entity: NewsAnalyzeUnresolvedEntity) -> str | None:
    entity_ref = entity.entity_ref.strip()
    if not entity_ref or entity_ref.startswith("ades:news:unresolved:"):
        return None
    return entity_ref


def _unresolved_missing_detail(
    missing_reason: str,
) -> tuple[str | None, str | None]:
    if missing_reason == "impact_expansion_unavailable":
        return "impact_expansion", "impact_graph"
    if missing_reason == "country_scope_without_terminal_candidate":
        return "country_to_terminal_candidate", "terminal_candidate"
    if missing_reason == "passive_relationship_without_terminal_candidate":
        return "source_backed_terminal_bridge", "terminal_candidate"
    if missing_reason == "no_source_backed_market_relationship":
        return "source_backed_market_relationship", "market_relationship"
    if missing_reason == "no_market_event_signal":
        return "market_event_signal", "event_signal"
    return missing_reason, None


def _unresolved_source_lane_suggestion(entity: NewsAnalyzeUnresolvedEntity) -> str:
    country_code = (entity.country_scope or "").strip().casefold()
    if (
        entity.missing_reason
        in {
            "country_scope_without_terminal_candidate",
            "no_source_backed_market_relationship",
            "passive_relationship_without_terminal_candidate",
        }
        and country_code
    ):
        return f"finance-{country_code}-en"
    if entity.missing_reason == "impact_expansion_unavailable":
        return "impact_graph"
    if entity.missing_reason == "no_market_event_signal":
        return "news_event_signal_extraction"
    entity_type = (entity.entity_type or "").strip().casefold()
    if entity_type in {"country", "sector", "industry"} and country_code:
        return f"finance-{country_code}-en"
    if entity_type in {"organization", "company", "product", "brand", "program"}:
        return "issuer_relationship_source_lane"
    return "source_backed_market_relationship_lane"


def _unresolved_review_priority(entity: NewsAnalyzeUnresolvedEntity) -> str:
    if entity.candidate_proposals > 0 or entity.missing_reason in {
        "country_scope_without_terminal_candidate",
        "no_source_backed_market_relationship",
        "passive_relationship_without_terminal_candidate",
    }:
        return "high"
    if entity.event_types or entity.mention_count > 1:
        return "medium"
    return "low"


def _unresolved_replay_key(entity: NewsAnalyzeUnresolvedEntity) -> str:
    normalized_ref = _known_unresolved_entity_ref(entity) or entity.entity_ref
    key_parts = [
        normalized_ref.casefold(),
        entity.missing_reason,
        (entity.country_scope or "").casefold(),
        ",".join(entity.event_types),
    ]
    digest = hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()[:16]
    return f"unresolved-entity:{digest}"


def _unresolved_nearest_known_node(entity: NewsAnalyzeUnresolvedEntity) -> str | None:
    normalized_ref = _known_unresolved_entity_ref(entity)
    if normalized_ref is not None:
        return normalized_ref
    if entity.country_scope:
        return f"country:{entity.country_scope}"
    if entity.topic_scope:
        return f"topic:{entity.topic_scope}"
    return None


def _build_diagnostics(
    *,
    warnings: list[str],
    quality_flags: list[str],
    unresolved_entities: list[NewsAnalyzeUnresolvedEntity],
    rejected_candidates: list[NewsAnalyzeRejectedCandidate],
    no_terminal_reasons: list[str],
) -> list[NewsAnalyzeDiagnostic]:
    diagnostics: list[NewsAnalyzeDiagnostic] = []
    for flag in quality_flags:
        diagnostics.append(
            NewsAnalyzeDiagnostic(
                code=flag,
                severity="warning" if flag.startswith("NO_") else "info",
                message=flag.lower(),
            )
        )
    for warning in warnings:
        diagnostics.append(
            NewsAnalyzeDiagnostic(
                code=warning.split(":", 1)[0],
                severity="warning",
                message=warning,
            )
        )
    for entity in unresolved_entities:
        missing_relation, missing_node = _unresolved_missing_detail(entity.missing_reason)
        normalized_ref = _known_unresolved_entity_ref(entity)
        diagnostics.append(
            NewsAnalyzeDiagnostic(
                code=entity.missing_reason,
                severity="info",
                message=entity.missing_reason,
                entity_ref=entity.entity_ref,
                event_type=entity.event_types[0] if entity.event_types else None,
                entity_text=entity.name,
                normalized_ref=normalized_ref,
                entity_type=entity.entity_type,
                jurisdiction=entity.country_scope,
                missing_relation=missing_relation,
                missing_node=missing_node,
                nearest_known_node=_unresolved_nearest_known_node(entity),
                source_lane_suggestion=_unresolved_source_lane_suggestion(entity),
                review_priority=_unresolved_review_priority(entity),
                replay_key=_unresolved_replay_key(entity),
            )
        )
    for candidate in rejected_candidates:
        diagnostics.append(
            NewsAnalyzeDiagnostic(
                code=candidate.reason_code,
                severity="info",
                message=candidate.reason,
                terminal_ref=candidate.terminal_ref,
                event_type=(
                    candidate.event_compatibility[0] if candidate.event_compatibility else None
                ),
            )
        )
    for reason in no_terminal_reasons:
        diagnostics.append(
            NewsAnalyzeDiagnostic(
                code=f"no_terminal:{reason}",
                severity="warning",
                message=_NO_TERMINAL_REASON_MESSAGES.get(reason, reason),
            )
        )
    return diagnostics


def _debug_snippet(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(str(value).split())
    if not normalized:
        return None
    if len(normalized) <= _NEWS_ANALYZE_DEBUG_EVIDENCE_CHAR_LIMIT:
        return normalized
    return normalized[: _NEWS_ANALYZE_DEBUG_EVIDENCE_CHAR_LIMIT - 3].rstrip() + "..."


def _append_debug_evidence(
    evidence: list[NewsAnalyzeDebugSourceEvidence],
    seen: set[tuple[str, str, str]],
    *,
    evidence_type: Literal["entity_mention", "event_signal", "event_sentence", "path_source"],
    ref: str,
    text: str | None,
    source_name: str | None = None,
    source_url: str | None = None,
    source_snapshot: str | None = None,
    start: int | None = None,
    end: int | None = None,
) -> None:
    if len(evidence) >= _NEWS_ANALYZE_DEBUG_EVIDENCE_LIMIT:
        return
    snippet = _debug_snippet(text)
    if snippet is None:
        return
    key = (evidence_type, ref, snippet)
    if key in seen:
        return
    seen.add(key)
    evidence.append(
        NewsAnalyzeDebugSourceEvidence(
            evidence_type=evidence_type,
            ref=ref,
            text=snippet,
            source_name=source_name,
            source_url=source_url,
            source_snapshot=source_snapshot,
            start=start,
            end=end,
        )
    )


def _debug_unresolved_entity(
    entity: NewsAnalyzeUnresolvedEntity,
    diagnostic_by_entity_reason: dict[tuple[str | None, str], NewsAnalyzeDiagnostic],
) -> NewsAnalyzeDebugUnresolvedEntity:
    diagnostic = diagnostic_by_entity_reason.get((entity.entity_ref, entity.missing_reason))
    return NewsAnalyzeDebugUnresolvedEntity(
        entity_ref=entity.entity_ref,
        name=entity.name,
        entity_type=entity.entity_type,
        missing_reason=entity.missing_reason,
        country_scope=entity.country_scope,
        topic_scope=entity.topic_scope,
        event_types=entity.event_types,
        review_priority=diagnostic.review_priority if diagnostic else None,
        replay_key=diagnostic.replay_key if diagnostic else None,
        source_lane_suggestion=(diagnostic.source_lane_suggestion if diagnostic else None),
    )


def _build_news_analyze_debug(
    *,
    entity_refs: list[str],
    graph_enabled_packs: list[str],
    candidate_paths: list[NewsAnalyzeCandidatePath],
    rejected_candidates: list[NewsAnalyzeRejectedCandidate],
    unresolved_entities: list[NewsAnalyzeUnresolvedEntity],
    diagnostics: list[NewsAnalyzeDiagnostic],
    passive_entities: list[NewsAnalyzePassiveEntity],
    event_signals: list[object],
    warnings: list[str],
) -> NewsAnalyzeDebug:
    diagnostic_by_entity_reason = {
        (diagnostic.entity_ref, diagnostic.code): diagnostic
        for diagnostic in diagnostics
        if diagnostic.entity_ref
    }
    debug_paths: list[NewsAnalyzeDebugPath] = []
    evidence: list[NewsAnalyzeDebugSourceEvidence] = []
    seen_evidence: set[tuple[str, str, str]] = set()

    for candidate_path in candidate_paths[:_NEWS_ANALYZE_DEBUG_PATH_LIMIT]:
        edges = candidate_path.relationship_path.edges
        source_names = _dedupe_string_values(
            [edge.source_name for edge in edges if edge.source_name]
        )
        source_urls = _dedupe_string_values([edge.source_url for edge in edges if edge.source_url])
        source_snapshots = _dedupe_string_values(
            [edge.source_snapshot for edge in edges if edge.source_snapshot]
        )
        debug_paths.append(
            NewsAnalyzeDebugPath(
                terminal_ref=candidate_path.terminal_ref,
                terminal_name=candidate_path.terminal_name,
                terminal_type=candidate_path.terminal_type,
                source_entity_refs=candidate_path.source_entity_refs,
                relationship_depth=candidate_path.relationship_path.path_depth,
                edge_refs=_dedupe_string_values([_edge_ref(edge) for edge in edges]),
                source_tiers=candidate_path.source_tiers,
                source_names=source_names,
                source_urls=source_urls,
                source_snapshots=source_snapshots,
                artifact_ref=candidate_path.artifact_ref,
            )
        )
        for edge in edges:
            _append_debug_evidence(
                evidence,
                seen_evidence,
                evidence_type="path_source",
                ref=_edge_ref(edge) or candidate_path.terminal_ref,
                text=(f"{edge.relation} via {edge.source_name}; snapshot={edge.source_snapshot}"),
                source_name=edge.source_name,
                source_url=edge.source_url,
                source_snapshot=edge.source_snapshot,
            )

    for signal in event_signals:
        event_type = str(getattr(signal, "event_type", "") or "event_signal")
        _append_debug_evidence(
            evidence,
            seen_evidence,
            evidence_type="event_signal",
            ref=event_type,
            text=getattr(signal, "evidence_text", None),
            start=getattr(signal, "evidence_start", None),
            end=getattr(signal, "evidence_end", None),
        )
        _append_debug_evidence(
            evidence,
            seen_evidence,
            evidence_type="event_sentence",
            ref=event_type,
            text=getattr(signal, "source_sentence", None),
        )

    for entity in passive_entities:
        if len(evidence) >= _NEWS_ANALYZE_DEBUG_EVIDENCE_LIMIT:
            break
        _append_debug_evidence(
            evidence,
            seen_evidence,
            evidence_type="entity_mention",
            ref=entity.entity_ref,
            text=entity.evidence_text,
        )

    return NewsAnalyzeDebug(
        limits={
            "paths": _NEWS_ANALYZE_DEBUG_PATH_LIMIT,
            "rejected_candidates": _NEWS_ANALYZE_DEBUG_REJECTED_LIMIT,
            "unresolved_entities": _NEWS_ANALYZE_DEBUG_UNRESOLVED_LIMIT,
            "source_evidence": _NEWS_ANALYZE_DEBUG_EVIDENCE_LIMIT,
            "source_evidence_chars": _NEWS_ANALYZE_DEBUG_EVIDENCE_CHAR_LIMIT,
        },
        entity_refs=_dedupe_string_values(entity_refs)[:_NEWS_ANALYZE_DEBUG_ENTITY_REF_LIMIT],
        graph_enabled_packs=_dedupe_string_values(graph_enabled_packs),
        candidate_path_count=len(candidate_paths),
        rejected_candidate_count=len(rejected_candidates),
        unresolved_entity_count=len(unresolved_entities),
        paths=debug_paths,
        rejected_candidates=[
            NewsAnalyzeDebugRejectedCandidate(
                terminal_ref=candidate.terminal_ref,
                terminal_name=candidate.terminal_name,
                terminal_type=candidate.terminal_type,
                reason_code=candidate.reason_code,
                reason=candidate.reason,
                source_entity_refs=candidate.source_entity_refs,
                path_confidence=candidate.path_confidence,
                artifact_ref=candidate.artifact_ref,
            )
            for candidate in rejected_candidates[:_NEWS_ANALYZE_DEBUG_REJECTED_LIMIT]
        ],
        unresolved_entities=[
            _debug_unresolved_entity(entity, diagnostic_by_entity_reason)
            for entity in unresolved_entities[:_NEWS_ANALYZE_DEBUG_UNRESOLVED_LIMIT]
        ],
        source_evidence=evidence,
        diagnostic_codes=_dedupe_string_values([diagnostic.code for diagnostic in diagnostics]),
        warnings=warnings,
    )


def _build_source_lane_coverage(
    *,
    tag_responses: list[TagResponse],
    pack_decisions: list[NewsAnalyzePackDecision],
    artifact_versions: NewsAnalyzeArtifactVersions,
    source_entities: list[ImpactSourceEntity],
    terminal_candidates: list[ImpactCandidate],
    unresolved_entities: list[NewsAnalyzeUnresolvedEntity],
    rejected_candidates: list[NewsAnalyzeRejectedCandidate],
    warnings: list[str],
    impact_paths: ImpactExpansionResult | None,
) -> list[NewsAnalyzeSourceLaneCoverage]:
    source_lane_order = _dedupe_string_values(
        [
            *[decision.pack_id for decision in pack_decisions],
            *[response.pack for response in tag_responses],
            *list(artifact_versions.packs.keys()),
        ]
    )
    source_entities_by_ref = {
        source_entity.entity_ref.casefold(): source_entity for source_entity in source_entities
    }
    source_lane_coverage: list[NewsAnalyzeSourceLaneCoverage] = []
    used_packs = {response.pack for response in tag_responses if response.pack}

    for lane in source_lane_order:
        lane_key = lane.casefold()
        lane_source_refs = {
            source_entity.entity_ref.casefold()
            for source_entity in source_entities
            if (source_entity.library_id or "").casefold() == lane_key
        }
        terminal_count = 0
        rejected_count = 0
        for candidate in terminal_candidates:
            candidate_lanes = {
                (source_entities_by_ref.get(ref.casefold()).library_id or "").casefold()
                for ref in candidate.source_entity_refs
                if source_entities_by_ref.get(ref.casefold()) is not None
            }
            if lane_key in candidate_lanes:
                terminal_count += 1
        for candidate in rejected_candidates:
            candidate_lanes = {
                (source_entities_by_ref.get(ref.casefold()).library_id or "").casefold()
                for ref in candidate.source_entity_refs
                if source_entities_by_ref.get(ref.casefold()) is not None
            }
            if lane_key in candidate_lanes:
                rejected_count += 1
        source_lane_coverage.append(
            NewsAnalyzeSourceLaneCoverage(
                lane=lane,
                version=artifact_versions.packs.get(lane),
                used=lane in used_packs or bool(lane_source_refs),
                source_entity_count=len(lane_source_refs),
                terminal_candidate_count=terminal_count,
                unresolved_entity_count=0,
                rejected_candidate_count=rejected_count,
                warning_count=sum(1 for warning in warnings if lane_key in warning.casefold()),
            )
        )

    if impact_paths is not None or artifact_versions.impact_artifact_hash:
        source_lane_coverage.append(
            NewsAnalyzeSourceLaneCoverage(
                lane="impact_graph",
                version=artifact_versions.impact_graph_version,
                used=impact_paths is not None,
                source_entity_count=len(source_entities),
                terminal_candidate_count=len(terminal_candidates),
                unresolved_entity_count=len(unresolved_entities),
                rejected_candidate_count=len(rejected_candidates),
                warning_count=len(impact_paths.warnings) if impact_paths is not None else 0,
                artifact_hash=artifact_versions.impact_artifact_hash,
            )
        )
    return source_lane_coverage


def _normalize_market_asset_phrase(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold()


def _phrase_matches_text(phrase: str, text: str) -> bool:
    normalized = str(phrase or "").strip()
    if len(normalized) < 3:
        return False
    return (
        re.search(
            rf"(?<![A-Za-z0-9]){re.escape(normalized)}(?![A-Za-z0-9])",
            text,
            re.IGNORECASE,
        )
        is not None
    )


def _direct_market_terminal_records() -> tuple[_DirectMarketTerminalRecord, ...]:
    records: list[_DirectMarketTerminalRecord] = []
    for record in BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES:
        entity_type = str(record.get("entity_type") or "").strip().casefold()
        if entity_type not in _DIRECT_MARKET_TERMINAL_ENTITY_TYPES:
            continue
        metadata = dict(record.get("metadata") or {})
        category = str(metadata.get("category") or "").strip().casefold()
        entity_ref = str(record.get("entity_id") or "").strip()
        name = str(record.get("canonical_text") or "").strip()
        if not entity_ref or not name:
            continue
        aliases = _dedupe_string_values(
            [
                name,
                *[str(alias) for alias in record.get("aliases", []) if str(alias).strip()],
            ]
        )
        records.append(
            _DirectMarketTerminalRecord(
                entity_ref=entity_ref,
                name=name,
                entity_type=entity_type,
                category=category,
                aliases=tuple(aliases),
            )
        )
    return tuple(records)


def _has_direct_market_terminal_signal(event_signals: list[object]) -> bool:
    for signal in event_signals:
        families = {
            str(family).strip().casefold()
            for family in getattr(signal, "compatible_asset_families", []) or []
            if str(family).strip()
        }
        if families & _DIRECT_MARKET_TERMINAL_SIGNAL_FAMILIES:
            return True
    return False


def _raw_entity_terminal_seed_ref(
    raw_entity: NewsAnalyzeRawEntity,
    *,
    records: tuple[_DirectMarketTerminalRecord, ...],
) -> str | None:
    raw_ref = (raw_entity.entity_ref or raw_entity.entity_id or "").strip()
    if raw_ref:
        for record in records:
            if raw_ref.casefold() == record.entity_ref.casefold():
                return record.entity_ref

    labels = {
        str(value or "").strip().casefold()
        for value in (raw_entity.entity_type, raw_entity.label, raw_entity.category)
        if str(value or "").strip()
    }
    raw_phrase = _normalize_market_asset_phrase(
        raw_entity.canonical_text or raw_entity.name or raw_entity.text
    )
    if not raw_phrase:
        return None
    for record in records:
        accepted_labels = {record.entity_type}
        if record.category:
            accepted_labels.add(record.category)
        if record.entity_type == "commodity":
            accepted_labels.add("commodity")
        if not (labels & accepted_labels):
            continue
        if any(raw_phrase == _normalize_market_asset_phrase(alias) for alias in record.aliases):
            return record.entity_ref
    return None


def _direct_market_terminal_seed_refs(
    *,
    text: str,
    request: NewsAnalyzeRequest,
    event_signals: list[object],
) -> list[str]:
    if not _has_direct_market_terminal_signal(event_signals):
        return []
    records = _direct_market_terminal_records()
    seed_refs: list[str] = []
    for raw_entity in [*request.raw_entities, *request.categorized_entities]:
        seed_ref = _raw_entity_terminal_seed_ref(raw_entity, records=records)
        if seed_ref:
            seed_refs.append(seed_ref)

    for record in records:
        if any(_phrase_matches_text(alias, text) for alias in record.aliases):
            seed_refs.append(record.entity_ref)
    return _dedupe_string_values(seed_refs)


def _has_market_sector_seed_signal(event_signals: list[object]) -> bool:
    for signal in event_signals:
        event_type = str(getattr(signal, "event_type", "") or "").strip().casefold()
        families = {
            str(family).strip().casefold()
            for family in getattr(signal, "compatible_asset_families", []) or []
            if str(family).strip()
        }
        if event_type in _MARKET_SECTOR_SEED_SIGNAL_TYPES:
            return True
        if families & _MARKET_SECTOR_SEED_FAMILIES:
            return True
    return False


def _market_sector_seed_terms_from_text(
    *,
    text: str,
    event_signals: list[object],
) -> list[str]:
    if not _has_market_sector_seed_signal(event_signals):
        return []
    seed_terms: list[str] = []
    for spec in _MARKET_SECTOR_SEED_SPECS:
        if any(_phrase_matches_text(alias, text) for alias in spec.aliases):
            seed_terms.extend(spec.search_terms)
    return _dedupe_string_values(seed_terms)


def _finance_graph_country_code(country_code: str | None) -> str | None:
    normalized = (country_code or "").strip().casefold()
    if not normalized:
        return None
    if normalized == "gb":
        return "uk"
    return normalized


def _news_graph_enabled_packs(
    *,
    tag_responses: list[TagResponse],
    pack_decisions: list[NewsAnalyzePackDecision],
) -> list[str]:
    graph_packs: list[str | None] = [response.pack for response in tag_responses]
    for decision in pack_decisions:
        if decision.selected:
            graph_packs.append(decision.pack_id)
    return _dedupe_string_values(graph_packs)


def _market_sector_seed_refs_from_graph(
    *,
    text: str,
    event_signals: list[object],
    country_codes: list[str],
    graph_enabled_packs: list[str],
    settings: Settings,
    warnings: list[str],
) -> list[str]:
    terms = _market_sector_seed_terms_from_text(text=text, event_signals=event_signals)
    artifact_path = settings.impact_expansion_artifact_path
    if not terms or not settings.impact_expansion_enabled or artifact_path is None:
        return []
    graph_country_codes = _dedupe_string_values(
        [_finance_graph_country_code(code) for code in country_codes]
    )
    try:
        with MarketGraphStore(artifact_path) as store:
            nodes_by_term = store.search_seed_nodes(
                terms,
                country_codes=graph_country_codes,
                pack_ids=graph_enabled_packs,
                entity_types=_MARKET_SECTOR_SEED_ENTITY_TYPES,
                limit_per_term=_MARKET_SECTOR_SEED_LIMIT_PER_TERM,
            )
    except (FileNotFoundError, OSError, ValueError):
        warnings.append("ADES_NEWS_ANALYZE_SECTOR_SEED_SEARCH_FAILED")
        return []

    seed_refs: list[str] = []
    for term in terms:
        for node in nodes_by_term.get(term.casefold(), ()):
            seed_refs.append(node.entity_ref)
            if len(seed_refs) >= _MARKET_SECTOR_SEED_MAX_REFS:
                return _dedupe_string_values(seed_refs)
    return _dedupe_string_values(seed_refs)


def _score_entity(entity: EntityMatch) -> float | None:
    scores = [
        value for value in (entity.relevance, entity.confidence) if isinstance(value, (float, int))
    ]
    return float(max(scores)) if scores else None


def _entity_ref(entity: EntityMatch) -> str:
    linked_ref = entity.link.entity_id.strip() if entity.link else ""
    if linked_ref:
        return linked_ref
    digest = hashlib.sha1(f"{entity.label}:{entity.text}".encode("utf-8")).hexdigest()
    return f"ades:unlinked:{digest[:16]}"


def _entity_name(entity: EntityMatch) -> str:
    if entity.link and entity.link.canonical_text.strip():
        return entity.link.canonical_text.strip()
    return entity.text.strip()


_DIRECT_TERMINAL_REF_PREFIXES = (
    "ades:security:",
    "ades:impact:commodity:",
    "ades:impact:crypto:",
    "ades:impact:currency:",
    "ades:impact:currency-pair:",
    "ades:impact:equity-index:",
    "ades:impact:forex:",
    "ades:impact:fx:",
    "ades:impact:index:",
    "ades:impact:rates:",
    "ades:impact:bond:",
)
_DIRECT_TERMINAL_TYPE_TOKENS = {
    "bond",
    "commodity",
    "crypto",
    "currency",
    "currency_pair",
    "equity",
    "equity_index",
    "etf",
    "forex",
    "forex_pair",
    "fx",
    "fx_pair",
    "index",
    "market_index",
    "rate",
    "rates",
    "security",
    "stock",
    "ticker",
}
_LOW_TRUST_ENTITY_PROVENANCE_TOKENS = {
    "llm",
    "search",
    "proposal",
    "news",
}
_LOW_TRUST_MATCH_SOURCE_TOKENS = {
    "llm",
    "search",
    "proposal",
}
_LOW_TRUST_MODEL_PROVENANCE_TOKENS = {
    "anthropic",
    "claude",
    "gemini",
    "gpt",
    "llama",
    "llm",
    "mistral",
    "openai",
    "qwen",
}


def _is_direct_terminal_ref(entity_ref: str) -> bool:
    normalized_ref = entity_ref.strip().casefold()
    if not normalized_ref:
        return False
    if normalized_ref.startswith("finance-") and (
        "-ticker:" in normalized_ref
        or "-security:" in normalized_ref
        or ":ticker:" in normalized_ref
        or ":equity:" in normalized_ref
        or ":security:" in normalized_ref
        or ":index:" in normalized_ref
        or ":equity-index:" in normalized_ref
        or ":market-index:" in normalized_ref
    ):
        return True
    return any(normalized_ref.startswith(prefix) for prefix in _DIRECT_TERMINAL_REF_PREFIXES)


def _provenance_value_tokens(value: str | None) -> set[str]:
    return {token for token in re.split(r"[^a-z0-9]+", (value or "").casefold()) if token}


def _is_low_trust_entity_claim(entity: EntityMatch) -> bool:
    provenance = entity.provenance
    if provenance is None:
        return False
    if provenance.match_kind == "proposal":
        return True
    model_tokens = _provenance_value_tokens(provenance.model_name) | _provenance_value_tokens(
        provenance.model_version
    )
    if model_tokens & _LOW_TRUST_MODEL_PROVENANCE_TOKENS:
        return True
    if _provenance_value_tokens(provenance.match_source) & _LOW_TRUST_MATCH_SOURCE_TOKENS:
        return True
    values = (
        provenance.match_path,
        provenance.lane or "",
        entity.link.provider if entity.link else "",
    )
    return any(
        _provenance_value_tokens(value) & _LOW_TRUST_ENTITY_PROVENANCE_TOKENS for value in values
    )


def _direct_terminal_entity_type(entity_ref: str, entity_type: str | None) -> str | None:
    normalized_ref = entity_ref.strip().casefold()
    normalized_type = (entity_type or "").strip().casefold()
    if "-ticker:" in normalized_ref or ":ticker:" in normalized_ref:
        return "ticker"
    if ":equity:" in normalized_ref or normalized_type in {"equity", "stock", "ticker"}:
        return normalized_type or "equity"
    if normalized_ref.startswith("ades:security:") or ":security:" in normalized_ref:
        return "security"
    if normalized_ref.startswith("ades:impact:commodity:"):
        return "commodity"
    if normalized_ref.startswith("ades:impact:crypto:"):
        return "crypto"
    if normalized_ref.startswith(
        ("ades:impact:currency-pair:", "ades:impact:forex:", "ades:impact:fx:")
    ):
        return "currency_pair"
    if normalized_ref.startswith("ades:impact:currency:"):
        return "currency"
    if (
        ":index:" in normalized_ref
        or ":equity-index:" in normalized_ref
        or ":market-index:" in normalized_ref
        or normalized_ref.startswith(("ades:impact:equity-index:", "ades:impact:index:"))
        or normalized_type in {"equity_index", "index", "market_index"}
    ):
        return normalized_type or "market_index"
    if normalized_ref.startswith("ades:impact:rates:"):
        return "rates"
    if normalized_ref.startswith("ades:impact:bond:"):
        return "bond"
    return entity_type


def _direct_terminal_identity_terms(entity_ref: str) -> list[str]:
    _jurisdiction, _exchange, ticker, security_ids = _terminal_identity_parts(entity_ref)
    terms = [ticker]
    terms.extend(security_ids.values())
    if ":" in entity_ref:
        terms.append(entity_ref.rsplit(":", 1)[1])
    return _dedupe_string_values(terms)


def _direct_terminal_entity_is_story_anchored(entity: EntityMatch, text: str) -> bool:
    entity_ref = _entity_ref(entity)
    if not _is_direct_terminal_ref(entity_ref):
        return True
    if not text.strip():
        return False
    terms = [
        *_direct_terminal_identity_terms(entity_ref),
        _entity_name(entity),
        entity.text,
        *entity.aliases,
    ]
    for term in _dedupe_string_values(terms):
        normalized = re.sub(r"[^A-Za-z0-9]+", "", term)
        if len(normalized) < 2:
            continue
        if _entity_phrase_occurs_in_story_context(text, term):
            return True
    return False


def _production_entity_ref_for_news_seed(
    entity: EntityMatch,
    *,
    text: str,
    warnings: list[str],
) -> str | None:
    entity_ref = entity.link.entity_id.strip() if entity.link else ""
    if not entity_ref:
        return None
    if not _is_direct_terminal_ref(entity_ref):
        return entity_ref
    if _direct_terminal_entity_is_story_anchored(entity, text):
        return entity_ref
    warnings.append(
        f"ADES_NEWS_ANALYZE_DROPPED_UNANCHORED_DIRECT_TERMINAL:{entity_ref[:120]}"
    )
    return None


def _is_direct_terminal_source_entity(source_entity: ImpactSourceEntity) -> bool:
    entity_ref = source_entity.entity_ref.strip()
    normalized_ref = entity_ref.casefold()
    normalized_type = (source_entity.entity_type or "").strip().casefold()
    if _is_direct_terminal_ref(normalized_ref):
        return True
    return bool(
        source_entity.is_tradable
        and (
            normalized_type in _DIRECT_TERMINAL_TYPE_TOKENS
            or any(token in normalized_ref for token in (":ticker:", "-ticker:", ":equity:"))
        )
    )


def _direct_terminal_refs_from_source_entity(
    source_entity: ImpactSourceEntity,
) -> list[str]:
    refs: list[str] = []
    entity_ref = source_entity.entity_ref.strip()
    if entity_ref and _is_direct_terminal_source_entity(source_entity):
        refs.append(entity_ref)
    for same_as_ref in source_entity.same_as_refs:
        same_as = same_as_ref.strip()
        if same_as and _is_direct_terminal_ref(same_as):
            refs.append(same_as)
    return _dedupe_string_values(refs)


_DIRECT_TERMINAL_ANCHOR_SUFFIX_PATTERN = re.compile(
    r"\b(?:"
    r"inc|incorporated|corp|corporation|company|co|ltd|limited|llc|plc|"
    r"holdings?|group|sa|s\.a\.|ag|nv|n\.v\.|spa|s\.p\.a\."
    r")\.?$",
    flags=re.IGNORECASE,
)


def _direct_terminal_anchor_name_variants(value: str) -> list[str]:
    normalized = re.sub(r"\s+", " ", value).strip()
    if not normalized:
        return []
    variants = [normalized]
    stripped = normalized
    while True:
        updated = _DIRECT_TERMINAL_ANCHOR_SUFFIX_PATTERN.sub("", stripped).strip(" ,.-")
        updated = re.sub(r"\s+", " ", updated).strip()
        if not updated or updated.casefold() == stripped.casefold():
            break
        variants.append(updated)
        stripped = updated
    return _dedupe_string_values(variants)


def _direct_terminal_source_entity_is_story_anchored(
    source_entity: ImpactSourceEntity,
    terminal_ref: str,
    text: str,
) -> bool:
    if not text.strip():
        return False
    terms: list[str] = [*_direct_terminal_identity_terms(terminal_ref)]
    terms.extend(_direct_terminal_anchor_name_variants(source_entity.name))
    entity_ref = source_entity.entity_ref.strip()
    if ":" in entity_ref:
        terms.append(entity_ref.rsplit(":", 1)[1])
    for same_as_ref in source_entity.same_as_refs:
        same_as = same_as_ref.strip()
        if same_as.casefold() == terminal_ref.strip().casefold():
            terms.extend(_direct_terminal_identity_terms(same_as))
    for term in _dedupe_string_values(terms):
        normalized = re.sub(r"[^A-Za-z0-9]+", "", term)
        if len(normalized) < 2:
            continue
        if _entity_phrase_occurs_in_story_context(text, term):
            return True
    return False


def _source_entity_ref_index(
    source_entities: list[ImpactSourceEntity],
) -> dict[str, ImpactSourceEntity]:
    by_ref: dict[str, ImpactSourceEntity] = {}
    for source_entity in source_entities:
        refs = [source_entity.entity_ref, *source_entity.same_as_refs]
        for raw_ref in refs:
            ref = raw_ref.strip()
            if ref:
                by_ref.setdefault(ref.casefold(), source_entity)
    return by_ref


def _source_entity_same_as_terminal_refs(
    source_entity: ImpactSourceEntity,
) -> set[str]:
    return {
        same_as_ref.strip().casefold()
        for same_as_ref in source_entity.same_as_refs
        if same_as_ref.strip() and _is_direct_terminal_ref(same_as_ref.strip())
    }


def _candidate_direct_terminal_source_entity(
    candidate: ImpactCandidate,
    source_entities_by_ref: dict[str, ImpactSourceEntity],
) -> ImpactSourceEntity | None:
    terminal_ref = candidate.entity_ref.strip()
    if not terminal_ref or not _is_direct_terminal_ref(terminal_ref):
        return None
    related_refs = [*candidate.source_entity_refs]
    for relationship_path in candidate.relationship_paths:
        for edge in relationship_path.edges:
            related_refs.extend([edge.source_ref, edge.target_ref])
    related_refs.append(terminal_ref)
    terminal_key = terminal_ref.casefold()
    for raw_ref in related_refs:
        ref = str(raw_ref).strip()
        if not ref:
            continue
        source_entity = source_entities_by_ref.get(ref.casefold())
        if source_entity is None:
            continue
        if terminal_key in _source_entity_same_as_terminal_refs(source_entity):
            return source_entity
    return None


def _append_unanchored_direct_terminal_warning(
    warnings: list[str] | None,
    terminal_ref: str,
) -> None:
    if warnings is not None:
        warnings.append(
            f"ADES_NEWS_ANALYZE_DROPPED_UNANCHORED_DIRECT_TERMINAL:{terminal_ref[:120]}"
        )


def _filter_unanchored_direct_terminal_candidates(
    candidates: list[ImpactCandidate],
    source_entities: list[ImpactSourceEntity],
    *,
    text: str,
    warnings: list[str],
) -> list[ImpactCandidate]:
    source_entities_by_ref = _source_entity_ref_index(source_entities)
    filtered: list[ImpactCandidate] = []
    for candidate in candidates:
        source_entity = _candidate_direct_terminal_source_entity(
            candidate,
            source_entities_by_ref,
        )
        if source_entity is None or _direct_terminal_source_entity_is_story_anchored(
            source_entity,
            candidate.entity_ref,
            text,
        ):
            filtered.append(candidate)
            continue
        _append_unanchored_direct_terminal_warning(warnings, candidate.entity_ref)
    return filtered


def _direct_terminal_candidates_from_source_entities(
    source_entities: list[ImpactSourceEntity],
    *,
    text: str,
    warnings: list[str] | None = None,
) -> list[ImpactCandidate]:
    candidates: list[ImpactCandidate] = []
    seen_refs: set[str] = set()
    for source_entity in source_entities:
        entity_ref = source_entity.entity_ref.strip()
        for terminal_ref in _direct_terminal_refs_from_source_entity(source_entity):
            if terminal_ref in seen_refs:
                continue
            is_same_as_shortcut = (
                terminal_ref.casefold()
                in _source_entity_same_as_terminal_refs(source_entity)
            )
            if is_same_as_shortcut and not _direct_terminal_source_entity_is_story_anchored(
                source_entity,
                terminal_ref,
                text,
            ):
                _append_unanchored_direct_terminal_warning(warnings, terminal_ref)
                continue
            seen_refs.add(terminal_ref)
            source_refs = [ref for ref in (entity_ref, terminal_ref) if ref]
            candidates.append(
                ImpactCandidate(
                    entity_ref=terminal_ref,
                    name=source_entity.name.strip() or terminal_ref,
                    entity_type=_direct_terminal_entity_type(
                        terminal_ref,
                        source_entity.entity_type,
                    ),
                    is_tradable=True,
                    evidence_level="direct",
                    confidence=0.92 if source_entity.is_graph_seed else 0.86,
                    source_entity_refs=_dedupe_string_values(source_refs),
                    relationship_paths=[],
                    compatible_event_types=[],
                )
            )
    return candidates


def _merge_direct_terminal_candidates(
    direct_candidates: list[ImpactCandidate],
    expanded_candidates: list[ImpactCandidate],
) -> list[ImpactCandidate]:
    merged: list[ImpactCandidate] = []
    by_ref: dict[str, ImpactCandidate] = {}
    for candidate in [*direct_candidates, *expanded_candidates]:
        ref = candidate.entity_ref.strip()
        if not ref:
            continue
        existing = by_ref.get(ref)
        if existing is None:
            by_ref[ref] = candidate
            merged.append(candidate)
            continue
        relationship_paths = [
            *existing.relationship_paths,
            *[
                path
                for path in candidate.relationship_paths
                if path not in existing.relationship_paths
            ],
        ]
        source_entity_refs = _dedupe_string_values(
            [*existing.source_entity_refs, *candidate.source_entity_refs]
        )
        compatible_event_types = _dedupe_string_values(
            [*existing.compatible_event_types, *candidate.compatible_event_types]
        )
        preferred_name = existing.name
        if candidate.relationship_paths and not existing.relationship_paths:
            preferred_name = candidate.name
        merged_candidate = existing.model_copy(
            update={
                "name": preferred_name,
                "evidence_level": "direct"
                if "direct" in {existing.evidence_level, candidate.evidence_level}
                else existing.evidence_level,
                "confidence": max(existing.confidence, candidate.confidence),
                "source_entity_refs": source_entity_refs,
                "relationship_paths": relationship_paths,
                "compatible_event_types": compatible_event_types,
            }
        )
        by_ref[ref] = merged_candidate
        for index, item in enumerate(merged):
            if item.entity_ref == ref:
                merged[index] = merged_candidate
                break
    return merged


def _entity_merge_key(entity: EntityMatch) -> str:
    if entity.link and entity.link.entity_id.strip():
        entity_ref = entity.link.entity_id.strip().casefold()
        if entity_ref.startswith("ades:heuristic_structural_"):
            return f"heuristic:{entity.label.casefold()}:{_entity_name(entity).casefold()}"
        return f"ref:{entity_ref}"
    return f"{entity.label.casefold()}:{_entity_name(entity).casefold()}"


def _merge_entity_matches(entities: list[EntityMatch]) -> list[EntityMatch]:
    by_key: dict[str, EntityMatch] = {}
    for entity in entities:
        key = _entity_merge_key(entity)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = entity
            continue
        representative = (
            entity if (_score_entity(entity) or 0) > (_score_entity(existing) or 0) else existing
        )
        by_key[key] = representative.model_copy(
            update={
                "aliases": _dedupe_string_values(
                    [existing.text, entity.text, *existing.aliases, *entity.aliases]
                ),
                "mention_count": max(1, existing.mention_count) + max(1, entity.mention_count),
                "confidence": max(
                    [
                        value
                        for value in (existing.confidence, entity.confidence)
                        if value is not None
                    ],
                    default=None,
                ),
                "relevance": max(
                    [
                        value
                        for value in (existing.relevance, entity.relevance)
                        if value is not None
                    ],
                    default=None,
                ),
            }
        )
    return sorted(
        by_key.values(),
        key=lambda item: (-(_score_entity(item) or 0.0), _entity_name(item).casefold()),
    )


def _normalize_news_packs(request: NewsAnalyzeRequest, settings: Settings) -> list[str]:
    requested = request.packs or [settings.default_pack]
    return _dedupe_string_values(requested)[: request.options.max_packs]


def _news_analyze_hybrid_option(request: NewsAnalyzeRequest) -> bool | None:
    if request.options.hybrid is not None:
        return request.options.hybrid
    raw_value = os.getenv("ADES_NEWS_ANALYZE_HYBRID_ENABLED")
    if raw_value is None or not raw_value.strip():
        return None
    return raw_value.strip().lower() in _ENV_TRUE_VALUES


def _news_analyze_hybrid_pack_limit() -> int | None:
    raw_value = os.getenv("ADES_NEWS_ANALYZE_HYBRID_PACK_LIMIT")
    if raw_value is None or not raw_value.strip():
        return None
    try:
        return max(0, int(raw_value))
    except ValueError:
        return None


def _pack_is_available(registry: PackRegistry, pack_id: str) -> bool:
    try:
        return registry.pack_exists(pack_id, active_only=True)
    except ValueError:
        return False


def _add_pack_decision(
    *,
    decisions: list[NewsAnalyzePackDecision],
    packs: list[str],
    registry: PackRegistry,
    pack_id: str,
    reason: str,
    country_code: str | None = None,
    detail: str | None = None,
) -> None:
    if any(decision.pack_id == pack_id for decision in decisions):
        return
    selected = _pack_is_available(registry, pack_id)
    decisions.append(
        NewsAnalyzePackDecision(
            pack_id=pack_id,
            selected=selected,
            reason=reason,
            country_code=country_code,
            detail=detail,
        )
    )
    if selected and pack_id not in packs:
        packs.append(pack_id)


def _build_initial_news_pack_plan(
    *,
    request: NewsAnalyzeRequest,
    settings: Settings,
    registry: PackRegistry,
) -> tuple[list[str], list[NewsAnalyzePackDecision]]:
    packs: list[str] = []
    decisions: list[NewsAnalyzePackDecision] = []
    requested_packs = _normalize_news_packs(request, settings)
    if request.options.auto_pack_planning:
        for pack_id in _NEWS_ANALYZE_BASE_PACKS:
            _add_pack_decision(
                decisions=decisions,
                packs=packs,
                registry=registry,
                pack_id=pack_id,
                reason="base_required",
            )
        for pack_id in requested_packs:
            _add_pack_decision(
                decisions=decisions,
                packs=packs,
                registry=registry,
                pack_id=pack_id,
                reason="request_pack",
            )
    else:
        for pack_id in requested_packs:
            _add_pack_decision(
                decisions=decisions,
                packs=packs,
                registry=registry,
                pack_id=pack_id,
                reason="request_pack",
            )
    return packs[: request.options.max_packs], decisions


def _country_alias_pattern(alias: str) -> re.Pattern[str]:
    return re.compile(
        rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])",
        re.IGNORECASE,
    )


def _country_codes_from_text(text: str) -> list[str]:
    codes: list[str] = []
    for alias, code in sorted(_COUNTRY_ALIAS_TO_CODE.items(), key=lambda item: -len(item[0])):
        if len(alias) < 2:
            continue
        if _country_alias_pattern(alias).search(text):
            codes.append(code)
    return _dedupe_string_values(codes)


def _country_codes_from_entities(entities: list[EntityMatch]) -> list[str]:
    codes: list[str] = []
    for entity in entities:
        code = _entity_ref_country_code(entity.link.entity_id if entity.link else None)
        code = code or _country_code_from_text(_entity_name(entity))
        if code:
            codes.append(code)
    return _dedupe_string_values(codes)


def _country_code_candidates_for_pack_plan(
    *,
    request: NewsAnalyzeRequest,
    text: str,
    entities: list[EntityMatch],
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []

    def _append(value: str | None, reason: str) -> None:
        code = _country_code_from_text(value)
        if code and not any(existing == code for existing, _ in candidates):
            candidates.append((code, reason))

    _append(request.country_hint, "country_hint")
    _append(request.hints.country if request.hints else None, "country_hint")
    _append(request.source.source_country if request.source else None, "source_country")
    for code in _country_codes_from_text(text):
        if not any(existing == code for existing, _ in candidates):
            candidates.append((code, "text_country_mention"))
    for code in _country_codes_from_entities(entities):
        if not any(existing == code for existing, _ in candidates):
            candidates.append((code, "entity_country_match"))
    return candidates


def _add_country_finance_pack_plan(
    *,
    decisions: list[NewsAnalyzePackDecision],
    packs: list[str],
    registry: PackRegistry,
    request: NewsAnalyzeRequest,
    text: str,
    entities: list[EntityMatch],
) -> list[str]:
    selected: list[str] = []
    if not request.options.auto_pack_planning or request.options.max_country_finance_packs <= 0:
        return selected
    for country_code, reason in _country_code_candidates_for_pack_plan(
        request=request,
        text=text,
        entities=entities,
    ):
        pack_id = _NEWS_ANALYZE_COUNTRY_FINANCE_PACK_BY_CODE.get(country_code)
        if not pack_id:
            continue
        before = set(packs)
        _add_pack_decision(
            decisions=decisions,
            packs=packs,
            registry=registry,
            pack_id=pack_id,
            reason=reason,
            country_code=country_code,
        )
        if pack_id in packs and pack_id not in before:
            selected.append(pack_id)
        if len(selected) >= request.options.max_country_finance_packs:
            break
        if len(packs) >= request.options.max_packs:
            break
    return selected


def _news_analysis_text(request: NewsAnalyzeRequest) -> str:
    return "\n\n".join(
        part.strip()
        for part in (request.title, request.description, request.text)
        if part and part.strip()
    )


def _clean_news_analysis_text(text: str) -> str:
    cleaned = text
    for pattern in _INTERNAL_NEWS_METADATA_PATTERNS:
        cleaned = pattern.sub("", cleaned)
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    return "\n".join(lines).strip()


def _story_quality_warnings(raw_text: str, analysis_text: str) -> list[str]:
    warnings: list[str] = []
    if any(pattern.search(raw_text) for pattern in _INTERNAL_NEWS_METADATA_PATTERNS):
        warnings.append("ADES_NEWS_ANALYZE_STRIPPED_INTERNAL_METADATA")
    factual_text = analysis_text.casefold()
    if "cantarell" in factual_text and "petrobras" in factual_text and "pemex" not in factual_text:
        warnings.append("ADES_NEWS_ANALYZE_FACTUAL_CONSISTENCY:CANTARELL_OPERATOR_PEMEX")
    return warnings


def _request_domain_terms(request: NewsAnalyzeRequest) -> list[str]:
    terms: list[str] = []
    for value in (request.domain_hint,):
        if value:
            terms.append(str(value))
    if request.hints and request.hints.topics:
        terms.extend(str(topic) for topic in request.hints.topics if str(topic).strip())
    for raw_entity in [*request.raw_entities, *request.categorized_entities]:
        for field_name in (
            "text",
            "name",
            "canonical_text",
            "label",
            "entity_type",
            "category",
            "matched_signal",
        ):
            value = getattr(raw_entity, field_name, None)
            if value and str(value).strip():
                terms.append(str(value))
    return terms


def _has_market_context(text: str) -> bool:
    folded = text.casefold()
    return any(re.search(rf"\b{re.escape(term)}\b", folded) for term in _MARKET_CONTEXT_TERMS)


def _has_non_market_signal(text: str) -> str | None:
    if re.search(
        r"\bmatched_signal\s*[:=]\s*(?:sports?|entertainment|celebrity|tabloid|match)\b",
        text,
        re.IGNORECASE,
    ):
        return "matched_signal_sports"
    folded = text.casefold()
    for term in sorted(_NON_MARKET_SIGNAL_TERMS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(term)}\b", folded):
            return f"non_market_{term.replace(' ', '_')}"
    return None


def _non_market_news_rejection_reasons(
    *,
    request: NewsAnalyzeRequest,
    text: str,
    event_signals: list[object],
) -> list[str]:
    domain_text = " ".join(_request_domain_terms(request))
    combined_text = "\n".join(part for part in (domain_text, text) if part.strip())
    reason = _has_non_market_signal(combined_text)
    if not reason:
        return []
    if reason == "matched_signal_sports":
        return [reason]
    if event_signals and _has_market_context(text):
        return []
    return [reason]


def _news_country_hint(request: NewsAnalyzeRequest) -> tuple[str | None, str | None]:
    if request.country_hint and request.country_hint.strip():
        return request.country_hint, "country_hint"
    if request.hints and request.hints.country and request.hints.country.strip():
        return request.hints.country, "country_hint"
    if request.source and request.source.source_country and request.source.source_country.strip():
        return request.source.source_country, "source_hint"
    return None, None


def _entity_ref_country_code(entity_ref: str | None) -> str | None:
    if not entity_ref:
        return None
    normalized = entity_ref.strip().casefold()
    if normalized.startswith("country:"):
        code = normalized.split(":", 1)[1]
        return code if code in _COUNTRY_BY_CODE or re.fullmatch(r"[a-z]{2}", code) else None
    return None


def _country_code_from_text(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().casefold().replace("_", "-")
    if normalized.startswith("country:"):
        normalized = normalized.split(":", 1)[1]
    if normalized in _COUNTRY_BY_CODE:
        return normalized
    if re.fullmatch(r"[a-z]{2}", normalized):
        return normalized
    return _COUNTRY_ALIAS_TO_CODE.get(normalized)


def _country_name_from_impact_sources(
    country_code: str,
    impact_source_entities: list[ImpactSourceEntity] | None,
) -> str | None:
    target_ref = f"country:{country_code.casefold()}"
    for source_entity in impact_source_entities or []:
        refs = [source_entity.entity_ref, *source_entity.same_as_refs]
        if not any(ref.casefold() == target_ref for ref in refs):
            continue
        name = source_entity.name.strip()
        if not name or name.casefold() == target_ref or re.fullmatch(r"[A-Za-z]{2}", name):
            continue
        if ":" in name:
            continue
        return name
    return None


def _country_name_from_impact_graph(
    country_code: str,
    settings: Settings | None,
) -> str | None:
    artifact_path = settings.impact_expansion_artifact_path if settings else None
    if artifact_path is None:
        return None
    try:
        with MarketGraphStore(artifact_path) as store:
            node = store.node(f"country:{country_code.casefold()}")
    except (FileNotFoundError, OSError, ValueError):
        return None
    if node is None:
        return None
    name = node.canonical_name.strip()
    if not name or re.fullmatch(r"[A-Za-z]{2}", name) or ":" in name:
        return None
    return name


def _build_country_scope(
    *,
    country_hint: str | None,
    hint_source: str | None = None,
    entities: list[EntityMatch],
    impact_source_entities: list[ImpactSourceEntity] | None = None,
    settings: Settings | None = None,
    text: str | None = None,
) -> NewsAnalyzeCountryScope | None:
    def _scope_for_code(
        code: str,
        source: str,
        preferred_name: str | None = None,
    ) -> NewsAnalyzeCountryScope:
        name = preferred_name or _COUNTRY_BY_CODE.get(code)
        for entity in entities:
            entity_name = _entity_name(entity)
            entity_code = _entity_ref_country_code(entity.link.entity_id if entity.link else None)
            entity_code = entity_code or _country_code_from_text(entity_name)
            if entity_code == code:
                name = entity_name
                break
        name = name or _country_name_from_impact_sources(
            code,
            impact_source_entities,
        )
        name = name or _country_name_from_impact_graph(code, settings)
        return NewsAnalyzeCountryScope(
            country_code=code,
            entity_ref=f"country:{code}",
            name=name or code.upper(),
            source=source,
        )

    hinted_code = _country_code_from_text(country_hint)
    if hinted_code and hint_source != "source_hint":
        return _scope_for_code(
            hinted_code,
            hint_source if hint_source == "country_hint" else "country_hint",
        )
    for entity in entities:
        code = _entity_ref_country_code(entity.link.entity_id if entity.link else None)
        code = code or _country_code_from_text(_entity_name(entity))
        if code:
            return _scope_for_code(code, "entity_match", preferred_name=_entity_name(entity))
    if text:
        text_country_codes = _country_codes_from_text(text)
        if text_country_codes:
            return _scope_for_code(text_country_codes[0], "text_country_mention")
    if hinted_code:
        return _scope_for_code(
            hinted_code,
            hint_source if hint_source == "source_hint" else "country_hint",
        )
    return None


def _additional_country_scopes_from_text(
    *,
    text: str | None,
    primary_scope: NewsAnalyzeCountryScope | None,
) -> list[NewsAnalyzeCountryScope]:
    if not text:
        return []
    seen = {primary_scope.country_code.casefold()} if primary_scope else set()
    scopes: list[NewsAnalyzeCountryScope] = []

    def _append(code: str, source: Literal["entity_match", "text_country_mention"]) -> None:
        normalized_code = code.casefold()
        if not normalized_code or normalized_code in seen:
            return
        seen.add(normalized_code)
        scopes.append(
            NewsAnalyzeCountryScope(
                country_code=normalized_code,
                entity_ref=f"country:{normalized_code}",
                name=_COUNTRY_BY_CODE.get(normalized_code, normalized_code.upper()),
                source=source,
            )
        )

    for code in _country_codes_from_text(text):
        _append(code, "text_country_mention")
    for code, pattern, _actor in _CORPORATE_HQ_COUNTRY_CUES:
        if pattern.search(text):
            _append(code, "entity_match")
    return scopes


def _text_contains_entity_phrase(text: str | None, name: str) -> bool:
    if not text or not name.strip():
        return False
    pattern = re.compile(
        rf"(?<![A-Za-z0-9]){re.escape(name.strip())}(?![A-Za-z0-9])",
        re.IGNORECASE,
    )
    return bool(pattern.search(text))


def _entity_phrase_occurs_in_story_context(text: str | None, name: str) -> bool:
    if not text or not name.strip():
        return False
    pattern = re.compile(
        rf"(?<![A-Za-z0-9]){re.escape(name.strip())}(?![A-Za-z0-9])",
        re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        line_start = text.rfind("\n", 0, match.start()) + 1
        line_end = text.find("\n", match.end())
        if line_end == -1:
            line_end = len(text)
        sentence_start = max(
            line_start,
            text.rfind(".", 0, match.start()) + 1,
            text.rfind("!", 0, match.start()) + 1,
            text.rfind("?", 0, match.start()) + 1,
        )
        next_boundaries = [
            pos for pos in (
                text.find(".", match.end()),
                text.find("!", match.end()),
                text.find("?", match.end()),
            )
            if pos != -1
        ]
        sentence_end = min([line_end, *next_boundaries]) if next_boundaries else line_end
        context = text[sentence_start:sentence_end].strip()
        if not context:
            continue
        if _DIRECT_TERMINAL_METADATA_CONTEXT_PATTERN.search(context):
            continue
        return True
    return False


def _source_outlet_is_story_subject(*, request: NewsAnalyzeRequest, name: str) -> bool:
    subject_text = "\n".join(
        part.strip() for part in (request.title, request.description) if part and part.strip()
    )
    return _text_contains_entity_phrase(subject_text, name)


def _hidden_artifact_reasons(
    value: str,
    *,
    entity: EntityMatch | None = None,
    source_outlet_artifact: bool = False,
) -> list[str]:
    normalized = value.strip()
    if not normalized:
        return ["empty_text"]
    reasons: list[str] = []
    label = entity.label.strip().casefold() if entity else ""
    if normalized.casefold() in {
        "none",
        "null",
        "nil",
        "n/a",
        "na",
        "not applicable",
        "unknown",
    }:
        reasons.append("none_placeholder")
    for reason, pattern in _HIDDEN_PASSIVE_ARTIFACT_PATTERNS:
        if pattern.search(normalized):
            reasons.append(reason)
    if label in _PASSIVE_ARTIFACT_LABELS:
        if label in _CURRENCY_AMOUNT_LABELS:
            reasons.append("currency_amount")
        elif label in _PERCENTAGE_LABELS:
            reasons.append("percentage")
        else:
            reasons.append(label)
    if re.fullmatch(r"[+-]?\d+(?:[.,]\d+)*", normalized):
        reasons.append("numeric_fragment")
    if re.fullmatch(
        r"[+-]?\d+(?:[.,]\d+)?\s*(?:seconds?|minutes?|hours?|days?|weeks?|months?|years?)",
        normalized,
        flags=re.IGNORECASE,
    ):
        reasons.append("duration_or_time_window")
    if re.search(
        r"(?:^|\b)(?:ap|associated press|reuters|getty images|afp|epa|shutterstock)"
        r"\s+photo(?:\b|/)|\bphoto\s*(?:by|/|:|credit)\b",
        normalized,
        flags=re.IGNORECASE,
    ):
        reasons.append("photo_credit")
    if normalized.casefold() in {"read more", "click here", "subscribe", "advertisement"}:
        reasons.append("boilerplate")
    if source_outlet_artifact:
        reasons.append("source_outlet_artifact")
    return _dedupe_string_values(reasons)


def _passive_role(entity: EntityMatch, name: str) -> str:
    label = entity.label.strip().casefold()
    entity_type = label
    if entity.link and entity.link.entity_id.startswith("country:"):
        return "country_scope"
    if _country_code_from_text(name):
        return "country_scope"
    if _SOURCE_OUTLET_PATTERN.search(name):
        return "source_outlet"
    if _PASSIVE_POLICY_BODY_PATTERN.search(name):
        return "policy_body"
    if entity_type in {"person", "people"}:
        return "person"
    if entity_type in {"location", "place", "city", "region", "country"}:
        return "location"
    if entity_type in {"organization", "company", "issuer", "government"}:
        score = _score_entity(entity)
        return "actor" if score is not None and score >= 0.75 else "organization"
    return "context_only"


def _passive_quality(
    entity: EntityMatch,
    hidden_reasons: list[str],
    weak_alias_reasons: list[str] | None = None,
) -> str:
    if hidden_reasons:
        return "hidden_artifact"
    if weak_alias_reasons:
        return "weak"
    provenance = entity.provenance
    alias_quality = (
        provenance.alias_quality.strip().casefold()
        if provenance and provenance.alias_quality
        else ""
    )
    if provenance and (
        provenance.weak_alias
        or alias_quality in {"weak", "low_quality", "suspect", "search_only", "hidden"}
    ):
        return "weak"
    score = _score_entity(entity)
    if score is None:
        return "medium"
    if score >= 0.85:
        return "strong"
    if score >= 0.55:
        return "medium"
    return "weak"


def _passive_weak_alias_reasons(entity: EntityMatch) -> list[str]:
    provenance = entity.provenance
    if provenance is None:
        return []
    alias_quality = provenance.alias_quality.strip().casefold() if provenance.alias_quality else ""
    reasons: list[str] = []
    if provenance.weak_alias:
        reasons.append("weak_alias")
    if alias_quality in {"weak", "low_quality", "suspect", "search_only", "hidden"}:
        reasons.append(f"{alias_quality}_alias")
    return reasons


def _passive_contextual_alias_reasons(
    entity: EntityMatch,
    request: NewsAnalyzeRequest,
    canonical_name: str,
) -> list[str]:
    surface = entity.text.strip()
    normalized_surface = re.sub(r"[^A-Za-z0-9]+", "", surface).casefold()
    reasons = _PASSIVE_COLLISION_SURFACE_REASONS.get(normalized_surface)
    if not reasons:
        return []
    if canonical_name and canonical_name.casefold() != surface.casefold():
        if _text_contains_entity_phrase(_news_analysis_text(request), canonical_name):
            return []
    return list(reasons)


def _passive_finance_alias_reasons(
    entity: EntityMatch,
    request: NewsAnalyzeRequest,
    canonical_name: str,
) -> list[str]:
    entity_ref = _entity_ref(entity).casefold()
    if not entity_ref.startswith("finance-") or _is_direct_terminal_ref(entity_ref):
        return []
    surface = entity.text.strip()
    if not surface or not canonical_name or surface.casefold() == canonical_name.casefold():
        return []
    if _text_contains_entity_phrase(_news_analysis_text(request), canonical_name):
        return []
    tokens = re.findall(r"[A-Za-z0-9]+", surface.casefold())
    if not tokens:
        return []
    generic_tokens = [
        token for token in tokens if token in _PASSIVE_GENERIC_FINANCE_ALIAS_TERMS
    ]
    if generic_tokens and (len(tokens) <= 3 or len(generic_tokens) == len(tokens)):
        return ["weak_alias", "unanchored_finance_alias"]
    return []


def _passive_display_eligible(quality: str, weak_alias_reasons: list[str]) -> bool:
    if quality == "hidden_artifact":
        return False
    if quality != "weak":
        return True
    return not weak_alias_reasons


def _is_tradable_entity(entity: EntityMatch, terminal_refs: set[str]) -> bool:
    if _is_low_trust_entity_claim(entity):
        return False
    entity_ref = _entity_ref(entity).casefold()
    if entity_ref in terminal_refs:
        return True
    label = entity.label.strip().casefold()
    if label in {"asset", "ticker", "security", "commodity", "currency", "index"}:
        return True
    if entity_ref.startswith("finance-") and any(
        token in entity_ref for token in ("-ticker:", ":ticker:", "currency", "commodity", "index")
    ):
        return True
    return False


def _build_passive_entities(
    *,
    entities: list[EntityMatch],
    terminal_candidates: list[object],
    country_scope: NewsAnalyzeCountryScope | None,
    country_scopes: list[NewsAnalyzeCountryScope] | None = None,
    max_entities: int,
    request: NewsAnalyzeRequest,
) -> list[NewsAnalyzePassiveEntity]:
    terminal_refs = {
        getattr(candidate, "entity_ref", "").casefold()
        for candidate in terminal_candidates
        if getattr(candidate, "entity_ref", "")
    }
    passive_entities: list[NewsAnalyzePassiveEntity] = []
    for entity in entities:
        if _is_tradable_entity(entity, terminal_refs):
            continue
        name = _entity_name(entity)
        entity_ref = _entity_ref(entity)
        role = _passive_role(entity, name)
        hidden_reasons = _hidden_artifact_reasons(
            name,
            entity=entity,
            source_outlet_artifact=(
                role == "source_outlet"
                and not _source_outlet_is_story_subject(request=request, name=name)
            ),
        )
        weak_alias_reasons = _dedupe_string_values(
            [
                *_passive_weak_alias_reasons(entity),
                *_passive_contextual_alias_reasons(entity, request, name),
                *_passive_finance_alias_reasons(entity, request, name),
            ]
        )
        quality = _passive_quality(entity, hidden_reasons, weak_alias_reasons)
        reasons = _dedupe_string_values(
            [
                *hidden_reasons,
                *weak_alias_reasons,
                *(entity.provenance.quality_reasons if entity.provenance else []),
            ]
        )
        passive_entities.append(
            NewsAnalyzePassiveEntity(
                entity_ref=entity_ref,
                name=name,
                entity_type=entity.label,
                label=entity.label,
                role=role,  # type: ignore[arg-type]
                quality=quality,  # type: ignore[arg-type]
                display_eligible=_passive_display_eligible(quality, weak_alias_reasons),
                source_pack=entity.provenance.source_pack if entity.provenance else None,
                source_domain=entity.provenance.source_domain if entity.provenance else None,
                aliases=_dedupe_string_values(entity.aliases)[:12],
                mention_count=entity.mention_count,
                confidence=entity.confidence,
                relevance=entity.relevance,
                evidence_text=entity.text,
                link=entity.link,
                provenance=entity.provenance,
                quality_reasons=reasons,
            )
        )
    forced_country_scopes: list[NewsAnalyzeCountryScope] = []
    for scope in [country_scope, *(country_scopes or [])]:
        if not scope:
            continue
        if any(
            existing.entity_ref.casefold() == scope.entity_ref.casefold()
            for existing in passive_entities
        ):
            continue
        if any(
            existing.entity_ref.casefold() == scope.entity_ref.casefold()
            for existing in forced_country_scopes
        ):
            continue
        forced_country_scopes.append(scope)
    for scope in reversed(forced_country_scopes):
        passive_entities.insert(
            0,
            NewsAnalyzePassiveEntity(
                entity_ref=scope.entity_ref,
                name=scope.name,
                entity_type="country",
                label="country",
                role="country_scope",
                quality="strong",
                display_eligible=True,
                aliases=[],
                mention_count=1,
                evidence_text=scope.name,
                quality_reasons=["forced_country_scope"],
            ),
        )
    return passive_entities[:max_entities]


def _build_unresolved_entities(
    *,
    passive_entities: list[NewsAnalyzePassiveEntity],
    terminal_candidates: list[object],
    impact_paths: ImpactExpansionResult | None,
    relationship_proposal_counts: dict[str, int] | None = None,
    country_scope: NewsAnalyzeCountryScope | None,
    topic_scope: NewsAnalyzeTopicScope | None,
    event_signals: list[object],
    max_entities: int = 64,
) -> list[NewsAnalyzeUnresolvedEntity]:
    terminal_source_refs: set[str] = set()
    terminal_entity_refs: set[str] = set()
    for candidate in terminal_candidates:
        candidate_ref = getattr(candidate, "entity_ref", "")
        if candidate_ref:
            terminal_entity_refs.add(candidate_ref.casefold())
        for source_ref in getattr(candidate, "source_entity_refs", []) or []:
            if source_ref:
                terminal_source_refs.add(str(source_ref).casefold())
        for relationship_path in getattr(candidate, "relationship_paths", []) or []:
            for edge in getattr(relationship_path, "edges", []) or []:
                source_ref = getattr(edge, "source_ref", "")
                if source_ref:
                    terminal_source_refs.add(str(source_ref).casefold())

    passive_path_counts: dict[str, int] = {}
    if impact_paths is not None:
        for passive_path in impact_paths.passive_paths:
            refs = [passive_path.entity_ref, *passive_path.source_entity_refs]
            for relationship_path in passive_path.relationship_paths:
                for edge in relationship_path.edges:
                    refs.extend([edge.source_ref, edge.target_ref])
            for ref in refs:
                if ref:
                    key = ref.casefold()
                    passive_path_counts[key] = passive_path_counts.get(key, 0) + 1

    event_types = _dedupe_string_values(
        [getattr(signal, "event_type", None) for signal in event_signals]
    )
    unresolved: list[NewsAnalyzeUnresolvedEntity] = []
    for passive in passive_entities:
        entity_ref_key = passive.entity_ref.casefold()
        has_terminal_candidate = (
            entity_ref_key in terminal_entity_refs or entity_ref_key in terminal_source_refs
        )
        if has_terminal_candidate:
            continue
        if not passive.display_eligible or passive.quality == "hidden_artifact":
            continue
        if passive.role == "source_outlet":
            continue

        if not event_types:
            candidate_proposals = 0
            missing_reason = "no_market_event_signal"
        else:
            candidate_proposals = (relationship_proposal_counts or {}).get(entity_ref_key)
            if candidate_proposals is None:
                candidate_proposals = passive_path_counts.get(entity_ref_key, 0)
            if impact_paths is None:
                missing_reason = "impact_expansion_unavailable"
            elif passive.role == "country_scope" and candidate_proposals == 0:
                missing_reason = "country_scope_without_terminal_candidate"
            elif candidate_proposals > 0:
                missing_reason = "passive_relationship_without_terminal_candidate"
            else:
                missing_reason = "no_source_backed_market_relationship"

        unresolved.append(
            NewsAnalyzeUnresolvedEntity(
                entity_ref=passive.entity_ref,
                name=passive.name,
                entity_type=passive.entity_type or passive.label,
                mention_count=passive.mention_count,
                story_count=1,
                country_scope=country_scope.country_code if country_scope else None,
                topic_scope=topic_scope.primary if topic_scope else None,
                event_types=event_types,
                has_terminal_candidate=False,
                missing_reason=missing_reason,
                reason=missing_reason,
                candidate_proposals=candidate_proposals,
            )
        )

    unresolved.sort(
        key=lambda entity: (
            -entity.mention_count,
            entity.missing_reason,
            entity.entity_ref.casefold(),
        )
    )
    return unresolved[:max_entities]


def _build_relationship_proposals(
    *,
    tag_responses: list[TagResponse],
    event_signals: list[object],
    impact_paths: ImpactExpansionResult | None = None,
    terminal_candidates: list[object],
    max_proposals: int = 64,
) -> list[NewsAnalyzeRelationshipProposal]:
    event_types = _dedupe_string_values(
        [getattr(signal, "event_type", None) for signal in event_signals]
    )
    if not event_types:
        return []

    terminal_refs = {
        str(getattr(candidate, "entity_ref", "")).casefold()
        for candidate in terminal_candidates
        if getattr(candidate, "entity_ref", "")
    }
    proposals_by_ref: dict[str, NewsAnalyzeRelationshipProposal] = {}
    for response in tag_responses:
        for entity in response.entities:
            if not _is_low_trust_entity_claim(entity):
                continue
            entity_ref = _entity_ref(entity).strip()
            name = _entity_name(entity).strip()
            if not entity_ref or not name or entity_ref.casefold() in terminal_refs:
                continue
            proposal = NewsAnalyzeRelationshipProposal(
                entity_ref=entity_ref,
                name=name,
                entity_type=entity.label,
                score=_score_entity(entity),
                provider=(
                    entity.link.provider
                    if entity.link
                    else (entity.provenance.match_source if entity.provenance else None)
                ),
                source="extracted_entity_claim",
                seed_entity_refs=[entity_ref],
                shared_seed_count=1,
                publication_allowed=False,
            )
            key = entity_ref.casefold()
            existing = proposals_by_ref.get(key)
            if existing is None or (proposal.score or 0.0) > (existing.score or 0.0):
                proposals_by_ref[key] = proposal
        for related in response.related_entities:
            entity_ref = related.entity_id.strip()
            name = related.canonical_text.strip()
            if not entity_ref or not name or entity_ref.casefold() in terminal_refs:
                continue
            seed_entity_refs = _dedupe_string_values(related.seed_entity_ids)[:12]
            proposal = NewsAnalyzeRelationshipProposal(
                entity_ref=entity_ref,
                name=name,
                entity_type=related.entity_type,
                score=related.score,
                provider=related.provider,
                seed_entity_refs=seed_entity_refs,
                shared_seed_count=related.shared_seed_count,
                publication_allowed=False,
            )
            key = entity_ref.casefold()
            existing = proposals_by_ref.get(key)
            if existing is None or (proposal.score or 0.0) > (existing.score or 0.0):
                proposals_by_ref[key] = proposal

    if impact_paths is not None:
        for passive_path in impact_paths.passive_paths:
            entity_ref = passive_path.entity_ref.strip()
            name = passive_path.name.strip()
            if not entity_ref or not name or entity_ref.casefold() in terminal_refs:
                continue
            seed_entity_refs = _dedupe_string_values(passive_path.source_entity_refs)[:12]
            proposal = NewsAnalyzeRelationshipProposal(
                entity_ref=entity_ref,
                name=name,
                entity_type=passive_path.entity_type,
                score=_relationship_path_proposal_score(passive_path.relationship_paths),
                provider="ades.impact.passive_path",
                source="impact_passive_path",
                seed_entity_refs=seed_entity_refs,
                shared_seed_count=len(seed_entity_refs),
                publication_allowed=False,
            )
            key = entity_ref.casefold()
            existing = proposals_by_ref.get(key)
            if existing is None or (proposal.score or 0.0) > (existing.score or 0.0):
                proposals_by_ref[key] = proposal

    proposals = list(proposals_by_ref.values())
    proposals.sort(
        key=lambda proposal: (
            -(proposal.score or 0.0),
            -proposal.shared_seed_count,
            proposal.entity_ref.casefold(),
        )
    )
    return proposals[:max_proposals]


def _relationship_path_proposal_score(paths: list[ImpactRelationshipPath]) -> float | None:
    path_scores: list[float] = []
    for path in paths:
        if not path.edges:
            continue
        edge_score = min(edge.confidence for edge in path.edges)
        depth_penalty = max(0.0, 1.0 - (max(path.path_depth - 1, 0) * 0.06))
        path_scores.append(max(0.0, min(1.0, edge_score * depth_penalty)))
    if not path_scores:
        return None
    return max(path_scores)


def _relationship_proposal_counts_by_seed(
    proposals: list[NewsAnalyzeRelationshipProposal],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for proposal in proposals:
        for seed_ref in proposal.seed_entity_refs:
            key = seed_ref.casefold()
            counts[key] = counts.get(key, 0) + 1
    return counts


def _merge_topic_matches(responses: list[TagResponse]) -> list[object]:
    by_label: dict[str, object] = {}
    for response in responses:
        for topic in response.topics:
            key = topic.label.casefold()
            existing = by_label.get(key)
            if existing is None:
                by_label[key] = topic
                continue
            existing_score = getattr(existing, "score", None)
            topic_score = topic.score
            by_label[key] = topic.model_copy(
                update={
                    "evidence_count": getattr(existing, "evidence_count", 0) + topic.evidence_count,
                    "score": max(
                        [value for value in (existing_score, topic_score) if value is not None],
                        default=None,
                    ),
                }
            )
    return list(by_label.values())[:16]


def _build_topic_scope(
    *,
    topics: list[object],
    request: NewsAnalyzeRequest,
    event_signals: list[object] | None = None,
    text: str | None = None,
) -> NewsAnalyzeTopicScope | None:
    labels = _dedupe_string_values(
        [
            *(getattr(topic, "label", None) for topic in topics),
            request.domain_hint,
            *((request.hints.topics if request.hints else []) or []),
        ]
    )
    signal_types = {
        str(getattr(signal, "event_type", "")).strip().casefold()
        for signal in event_signals or []
        if str(getattr(signal, "event_type", "")).strip()
    }
    route_text = "\n".join([text or "", " ".join(labels)]).strip()
    primary_override: str | None = None
    if signal_types & {"regulatory_enforcement", "sanctions", "export_control"} or _LEGAL_TOPIC_PATTERN.search(
        route_text
    ):
        primary_override = "Regulatory & Legal"
    elif signal_types & {"acquisition", "merger", "divestiture", "equity_listing"} or _MA_TOPIC_PATTERN.search(
        route_text
    ):
        primary_override = "Trading & Markets"
    elif "crypto_market_move" in signal_types or _CRYPTO_TOPIC_PATTERN.search(route_text):
        primary_override = "Trading & Markets"
    elif _TECH_TOPIC_PATTERN.search(route_text) and any(
        "commodit" in label.casefold() for label in labels
    ):
        primary_override = "General / Mixed"
    elif _NON_PHYSICAL_ASSET_PATTERN.search(route_text) and any(
        "commodit" in label.casefold() for label in labels
    ):
        primary_override = "General / Mixed"
    if primary_override:
        labels = _dedupe_string_values(
            [
                primary_override,
                *[
                    label
                    for label in labels
                    if label.casefold() != primary_override.casefold()
                ],
            ]
        )
    if not labels:
        return None

    normalized_labels = {label.casefold() for label in labels}

    def _has_any(*needles: str) -> bool:
        return any(
            needle in label or label in needle for label in normalized_labels for needle in needles
        )

    return NewsAnalyzeTopicScope(
        primary=labels[0],
        secondary=labels[1:8],
        finance_relevant=_has_any(
            "finance",
            "market",
            "stocks",
            "equity",
            "currency",
            "commodity",
            "crypto",
            "trading",
            "markets",
        ),
        politics_relevant=_has_any(
            "politics",
            "policy",
            "geopolitics",
            "government",
            "regulation",
            "sanctions",
        ),
        economics_relevant=_has_any(
            "economics",
            "economy",
            "inflation",
            "rates",
            "trade",
            "growth",
        ),
    )


def _candidate_direction_context(candidate: ImpactCandidate) -> str:
    parts: list[str] = [
        candidate.entity_ref,
        candidate.name,
        candidate.entity_type or "",
        *candidate.compatible_event_types,
    ]
    for path in candidate.relationship_paths:
        for edge in path.edges:
            parts.extend(
                [
                    edge.source_ref,
                    edge.target_ref,
                    edge.relation,
                    edge.direction_hint,
                    edge.source_name,
                    *edge.compatible_event_types,
                ]
            )
    return " ".join(part for part in parts if part).casefold()


def _negative_direction_hint_for_candidate(
    *,
    candidate: ImpactCandidate,
    text: str,
) -> str | None:
    candidate_context = _candidate_direction_context(candidate)
    if "airline" in candidate_context and _AIRLINE_NEGATIVE_COST_PATTERN.search(text):
        return "negative_cost_pressure"
    if (
        "semiconductor" in candidate_context
        or "chipmaker" in candidate_context
        or "chip" in candidate_context
    ) and _SEMICONDUCTOR_NEGATIVE_WARNING_PATTERN.search(text):
        return "negative_analyst_warning"
    return None


def _apply_story_direction_overrides(
    *,
    candidates: list[ImpactCandidate],
    text: str,
    warnings: list[str],
) -> list[ImpactCandidate]:
    updated_candidates: list[ImpactCandidate] = []
    for candidate in candidates:
        direction_hint = _negative_direction_hint_for_candidate(
            candidate=candidate,
            text=text,
        )
        if not direction_hint or not candidate.relationship_paths:
            updated_candidates.append(candidate)
            continue
        updated_paths: list[ImpactRelationshipPath] = []
        for path in candidate.relationship_paths:
            updated_edges = [
                edge.model_copy(
                    update={
                        "direction_hint": direction_hint,
                        "direction_preconditions": _dedupe_string_values(
                            [
                                *edge.direction_preconditions,
                                "story_negative_signal",
                            ]
                        ),
                    }
                )
                for edge in path.edges
            ]
            updated_paths.append(path.model_copy(update={"edges": updated_edges}))
        updated_candidates.append(
            candidate.model_copy(update={"relationship_paths": updated_paths})
        )
        warnings.append(
            f"ADES_NEWS_ANALYZE_DIRECTION_OVERRIDE:{candidate.entity_ref}:{direction_hint}"
        )
    return updated_candidates


def _raise_configuration_http_exception(exc: Exception) -> None:
    """Map one configuration/runtime failure onto the public HTTP contract."""

    if isinstance(exc, FileNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    raise HTTPException(status_code=400, detail=str(exc)) from exc


@dataclass
class _ServiceRuntimeState:
    """Resident settings, registry, and warm-pack state for the local service."""

    storage_root_override: str | Path | None
    settings: Settings | None = None
    registry: PackRegistry | None = None
    prewarmed_packs: set[str] = field(default_factory=set)

    def ensure_registry(self) -> PackRegistry:
        """Return one resident registry bound to the current service settings."""

        if self.registry is not None:
            return self.registry
        settings = _resolve_settings(storage_root=self.storage_root_override)
        self.settings = settings
        self.registry = PackRegistry(
            settings.storage_root,
            runtime_target=settings.runtime_target,
            metadata_backend=settings.metadata_backend,
            database_url=settings.database_url,
        )
        return self.registry

    def current_settings(self) -> Settings:
        """Return the resolved service settings, loading them lazily."""

        if self.settings is None:
            self.ensure_registry()
        assert self.settings is not None
        return self.settings

    def prewarm_pack(self, pack_id: str) -> None:
        """Load one runtime and warm its matcher sidecars."""

        registry = self.ensure_registry()
        settings = self.current_settings()
        runtime = load_pack_runtime(settings.storage_root, pack_id, registry=registry)
        if runtime is None:
            self.prewarmed_packs.discard(pack_id)
            return
        for matcher in runtime.matchers:
            load_runtime_matcher(matcher.artifact_path, matcher.entries_path)
        self.prewarmed_packs.add(pack_id)

    def prewarm_active_packs(self) -> None:
        """Warm every active installed pack."""

        registry = self.ensure_registry()
        for pack in registry.list_installed_packs(active_only=True):
            self.prewarm_pack(pack.pack_id)

    def invalidate(self) -> None:
        """Drop warm caches after a pack mutation."""

        clear_pack_runtime_cache()
        clear_runtime_matcher_cache()
        self.prewarmed_packs.clear()


def create_app(*, storage_root: str | Path | None = None) -> FastAPI:
    """Create a FastAPI application bound to a storage root."""

    runtime_state = _ServiceRuntimeState(storage_root)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        settings = runtime_state.current_settings()
        if settings.service_prewarm_enabled:
            try:
                runtime_state.prewarm_active_packs()
            except (FileNotFoundError, UnsupportedRuntimeConfigurationError, ValueError):
                # Preserve request-time config errors on the public endpoints.
                pass
        yield

    app = FastAPI(title="ades", version=__version__, lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Simple health endpoint."""

        return {"status": "ok", "version": __version__}

    @app.get("/v0/status", response_model=StatusResponse)
    def runtime_status() -> StatusResponse:
        """Report local runtime status."""

        try:
            response = status(storage_root=storage_root)
            return response.model_copy(
                update={"prewarmed_packs": sorted(runtime_state.prewarmed_packs)}
            )
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)

    @app.get("/v0/installers/npm", response_model=NpmInstallerInfo)
    def runtime_npm_installer_info() -> NpmInstallerInfo:
        """Return canonical npm-wrapper bootstrap metadata."""

        return npm_installer_info()

    @app.get("/v0/release/versions", response_model=ReleaseVersionState)
    def runtime_release_versions() -> ReleaseVersionState:
        """Return the current coordinated release version state."""

        try:
            return release_versions()
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/release/sync-version", response_model=ReleaseVersionSyncResponse)
    def runtime_sync_release_version(
        request: ReleaseVersionSyncRequest,
    ) -> ReleaseVersionSyncResponse:
        """Synchronize release versions across Python and npm metadata."""

        try:
            return sync_release_version(request.version)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/release/verify", response_model=ReleaseVerificationResponse)
    def runtime_verify_release(request: ReleaseVerificationRequest) -> ReleaseVerificationResponse:
        """Build and verify the local Python and npm release artifacts."""

        try:
            return verify_release(
                output_dir=request.output_dir,
                clean=request.clean,
                smoke_install=request.smoke_install,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/release/manifest", response_model=ReleaseManifestResponse)
    def runtime_write_release_manifest(
        request: ReleaseManifestRequest,
    ) -> ReleaseManifestResponse:
        """Build and persist the coordinated release manifest."""

        try:
            return write_release_manifest(
                output_dir=request.output_dir,
                manifest_path=request.manifest_path,
                version=request.version,
                clean=request.clean,
                smoke_install=request.smoke_install,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/release/validate", response_model=ReleaseValidationResponse)
    def runtime_validate_release(
        request: ReleaseValidationRequest,
    ) -> ReleaseValidationResponse:
        """Run tests, then build and persist one coordinated release manifest."""

        try:
            return validate_release(
                output_dir=request.output_dir,
                manifest_path=request.manifest_path,
                version=request.version,
                clean=request.clean,
                smoke_install=request.smoke_install,
                tests_command=request.tests_command or None,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/release/publish", response_model=ReleasePublishResponse)
    def runtime_publish_release(
        request: ReleasePublishRequest,
    ) -> ReleasePublishResponse:
        """Publish one validated release manifest to Python and npm registries."""

        try:
            return publish_release(
                manifest_path=request.manifest_path,
                dry_run=request.dry_run,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v0/packs", response_model=list[PackSummary])
    def runtime_list_packs(active_only: bool = Query(False)) -> list[PackSummary]:
        """List locally installed packs."""

        try:
            return list_packs(storage_root=storage_root, active_only=active_only)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)

    @app.get("/v0/packs/available", response_model=list[AvailablePackSummary])
    def runtime_list_available_packs(
        registry_url: str | None = Query(None),
    ) -> list[AvailablePackSummary]:
        """List installable packs from the configured registry."""

        try:
            return list_available_packs(registry_url=registry_url)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)

    @app.get("/v0/packs/{pack_id}", response_model=PackSummary)
    def runtime_get_pack(pack_id: str) -> PackSummary:
        """Get a single installed pack."""

        try:
            pack = get_pack(pack_id, storage_root=storage_root)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)
        if pack is None:
            raise HTTPException(status_code=404, detail=f"Pack not found: {pack_id}")
        return pack

    @app.get("/v0/packs/{pack_id}/health", response_model=PackHealthResponse)
    def runtime_get_pack_health(
        pack_id: str,
        limit: int = Query(100, ge=1, le=1000),
    ) -> PackHealthResponse:
        """Return aggregated recent extraction-health telemetry for one pack."""

        try:
            return get_pack_health(pack_id, storage_root=storage_root, limit=limit)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)

    @app.post("/v0/packs/{pack_id}/activate", response_model=PackSummary)
    def runtime_activate_pack(pack_id: str) -> PackSummary:
        """Activate a single installed pack."""

        try:
            pack = activate_pack(pack_id, storage_root=storage_root)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)
        if pack is None:
            raise HTTPException(status_code=404, detail=f"Pack not found: {pack_id}")
        runtime_state.invalidate()
        try:
            runtime_state.prewarm_active_packs()
        except (FileNotFoundError, UnsupportedRuntimeConfigurationError, ValueError):
            pass
        return pack

    @app.post("/v0/packs/{pack_id}/deactivate", response_model=PackSummary)
    def runtime_deactivate_pack(pack_id: str) -> PackSummary:
        """Deactivate a single installed pack."""

        try:
            pack = deactivate_pack(pack_id, storage_root=storage_root)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)
        if pack is None:
            raise HTTPException(status_code=404, detail=f"Pack not found: {pack_id}")
        runtime_state.invalidate()
        try:
            runtime_state.prewarm_active_packs()
        except (FileNotFoundError, UnsupportedRuntimeConfigurationError, ValueError):
            pass
        return pack

    @app.delete("/v0/packs/{pack_id}", response_model=PackSummary)
    def runtime_remove_pack(pack_id: str) -> PackSummary:
        """Remove a single installed pack."""

        try:
            pack = remove_pack(pack_id, storage_root=storage_root)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)
        if pack is None:
            raise HTTPException(status_code=404, detail=f"Pack not found: {pack_id}")
        runtime_state.invalidate()
        try:
            runtime_state.prewarm_active_packs()
        except (FileNotFoundError, UnsupportedRuntimeConfigurationError, ValueError):
            pass
        return pack

    @app.post("/v0/registry/build", response_model=RegistryBuildResponse)
    def runtime_build_registry(request: RegistryBuildRequest) -> RegistryBuildResponse:
        """Build a static file-based registry from local pack directories."""

        try:
            return build_registry(
                request.pack_dirs,
                output_dir=request.output_dir,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-qid-graph-store",
        response_model=QidGraphStoreBuildResponse,
    )
    def runtime_build_qid_graph_store(
        request: QidGraphStoreBuildRequest,
    ) -> QidGraphStoreBuildResponse:
        """Build one explicit QID graph store from local bundle directories."""

        try:
            return build_qid_graph_store(
                request.bundle_dirs,
                truthy_path=request.truthy_path,
                output_dir=request.output_dir,
                allowed_predicates=request.predicate or None,
                release_gate_commands=request.release_gate_commands,
                release_gate_working_dir=request.release_gate_working_dir,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-market-graph-store",
        response_model=MarketGraphStoreBuildResponse,
    )
    def runtime_build_market_graph_store(
        request: MarketGraphStoreBuildRequest,
    ) -> MarketGraphStoreBuildResponse:
        """Build one market impact graph store from normalized TSV source lanes."""

        try:
            return build_market_graph_store(
                edge_tsv_paths=request.edge_tsv_paths,
                output_dir=request.output_dir,
                node_tsv_paths=request.node_tsv_paths,
                graph_version=request.graph_version,
                artifact_version=request.artifact_version,
                release_gate_commands=request.release_gate_commands,
                release_gate_working_dir=request.release_gate_working_dir,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/fetch-finance-sources",
        response_model=RegistryFetchFinanceSourcesResponse,
    )
    def runtime_fetch_finance_sources(
        request: RegistryFetchFinanceSourcesRequest,
    ) -> RegistryFetchFinanceSourcesResponse:
        """Download one real `finance-en` source snapshot set."""

        try:
            return fetch_finance_source_snapshot(
                output_dir=request.output_dir,
                snapshot=request.snapshot,
                sec_companies_url=request.sec_companies_url,
                sec_submissions_url=request.sec_submissions_url,
                sec_companyfacts_url=request.sec_companyfacts_url,
                symbol_directory_url=request.symbol_directory_url,
                other_listed_url=request.other_listed_url,
                finance_people_url=request.finance_people_url,
                derive_finance_people_from_sec=request.derive_finance_people_from_sec,
                finance_people_archive_base_url=request.finance_people_archive_base_url,
                finance_people_max_companies=request.finance_people_max_companies,
                user_agent=request.user_agent,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-finance-bundle",
        response_model=RegistryBuildFinanceBundleResponse,
    )
    def runtime_build_finance_bundle(
        request: RegistryBuildFinanceBundleRequest,
    ) -> RegistryBuildFinanceBundleResponse:
        """Build one normalized `finance-en` source bundle from raw snapshot files."""

        try:
            return build_finance_source_bundle(
                sec_companies_path=request.sec_companies_path,
                sec_submissions_path=request.sec_submissions_path,
                sec_companyfacts_path=request.sec_companyfacts_path,
                symbol_directory_path=request.symbol_directory_path,
                other_listed_path=request.other_listed_path,
                finance_people_path=request.finance_people_path,
                curated_entities_path=request.curated_entities_path,
                output_dir=request.output_dir,
                version=request.version,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/fetch-finance-country-sources",
        response_model=RegistryFetchFinanceCountrySourcesResponse,
    )
    def runtime_fetch_finance_country_sources(
        request: RegistryFetchFinanceCountrySourcesRequest,
    ) -> RegistryFetchFinanceCountrySourcesResponse:
        """Download official source landing pages for country-scoped finance packs."""

        try:
            return fetch_finance_country_source_snapshots(
                output_dir=request.output_dir,
                snapshot=request.snapshot,
                country_codes=request.country_codes,
                user_agent=request.user_agent,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-finance-country-bundles",
        response_model=RegistryBuildFinanceCountryBundlesResponse,
    )
    def runtime_build_finance_country_bundles(
        request: RegistryBuildFinanceCountryBundlesRequest,
    ) -> RegistryBuildFinanceCountryBundlesResponse:
        """Build country-scoped finance bundles from downloaded source snapshots."""

        try:
            return build_finance_country_source_bundles(
                snapshot_dir=request.snapshot_dir,
                output_dir=request.output_dir,
                country_codes=request.country_codes,
                version=request.version,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-general-bundle",
        response_model=RegistryBuildGeneralBundleResponse,
    )
    def runtime_build_general_bundle(
        request: RegistryBuildGeneralBundleRequest,
    ) -> RegistryBuildGeneralBundleResponse:
        """Build one normalized `general-en` source bundle from raw snapshot files."""

        try:
            return build_general_source_bundle(
                wikidata_entities_path=request.wikidata_entities_path,
                geonames_places_path=request.geonames_places_path,
                curated_entities_path=request.curated_entities_path,
                output_dir=request.output_dir,
                version=request.version,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/fetch-general-sources",
        response_model=RegistryFetchGeneralSourcesResponse,
    )
    def runtime_fetch_general_sources(
        request: RegistryFetchGeneralSourcesRequest,
    ) -> RegistryFetchGeneralSourcesResponse:
        """Download one real `general-en` source snapshot set."""

        try:
            return fetch_general_source_snapshot(
                output_dir=request.output_dir,
                snapshot=request.snapshot,
                wikidata_url=request.wikidata_url,
                wikidata_truthy_url=request.wikidata_truthy_url,
                wikidata_entities_url=request.wikidata_entities_url,
                geonames_places_url=request.geonames_places_url,
                geonames_alternate_names_url=request.geonames_alternate_names_url,
                geonames_modifications_url=request.geonames_modifications_url,
                geonames_deletes_url=request.geonames_deletes_url,
                geonames_alternate_modifications_url=request.geonames_alternate_modifications_url,
                geonames_alternate_deletes_url=request.geonames_alternate_deletes_url,
                user_agent=request.user_agent,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/fetch-medical-sources",
        response_model=RegistryFetchMedicalSourcesResponse,
    )
    def runtime_fetch_medical_sources(
        request: RegistryFetchMedicalSourcesRequest,
    ) -> RegistryFetchMedicalSourcesResponse:
        """Download one real `medical-en` source snapshot set."""

        try:
            return fetch_medical_source_snapshot(
                output_dir=request.output_dir,
                snapshot=request.snapshot,
                disease_ontology_url=request.disease_ontology_url,
                hgnc_genes_url=request.hgnc_genes_url,
                uniprot_proteins_url=request.uniprot_proteins_url,
                clinical_trials_url=request.clinical_trials_url,
                orange_book_url=request.orange_book_url,
                user_agent=request.user_agent,
                uniprot_max_records=request.uniprot_max_records,
                clinical_trials_max_records=request.clinical_trials_max_records,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-medical-bundle",
        response_model=RegistryBuildMedicalBundleResponse,
    )
    def runtime_build_medical_bundle(
        request: RegistryBuildMedicalBundleRequest,
    ) -> RegistryBuildMedicalBundleResponse:
        """Build one normalized `medical-en` source bundle from raw snapshot files."""

        try:
            return build_medical_source_bundle(
                disease_ontology_path=request.disease_ontology_path,
                hgnc_genes_path=request.hgnc_genes_path,
                uniprot_proteins_path=request.uniprot_proteins_path,
                clinical_trials_path=request.clinical_trials_path,
                orange_book_products_path=request.orange_book_products_path,
                curated_entities_path=request.curated_entities_path,
                output_dir=request.output_dir,
                version=request.version,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/validate-finance-quality",
        response_model=RegistryValidateFinanceQualityResponse,
    )
    def runtime_validate_finance_quality(
        request: RegistryValidateFinanceQualityRequest,
    ) -> RegistryValidateFinanceQualityResponse:
        """Build, install, and evaluate one generated `finance-en` pack bundle."""

        try:
            return validate_finance_pack_quality(
                request.bundle_dir,
                output_dir=request.output_dir,
                fixture_profile=request.fixture_profile,
                version=request.version,
                min_expected_recall=request.min_expected_recall,
                max_unexpected_hits=request.max_unexpected_hits,
                max_ambiguous_aliases=request.max_ambiguous_aliases,
                max_dropped_alias_ratio=request.max_dropped_alias_ratio,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/validate-general-quality",
        response_model=RegistryPackQualityResponse,
    )
    def runtime_validate_general_quality(
        request: RegistryValidateGeneralQualityRequest,
    ) -> RegistryPackQualityResponse:
        """Build, install, and evaluate one generated `general-en` pack bundle."""

        try:
            return validate_general_pack_quality(
                request.bundle_dir,
                output_dir=request.output_dir,
                fixture_profile=request.fixture_profile,
                version=request.version,
                min_expected_recall=request.min_expected_recall,
                max_unexpected_hits=request.max_unexpected_hits,
                max_ambiguous_aliases=request.max_ambiguous_aliases,
                max_dropped_alias_ratio=request.max_dropped_alias_ratio,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/validate-medical-quality",
        response_model=RegistryPackQualityResponse,
    )
    def runtime_validate_medical_quality(
        request: RegistryValidateMedicalQualityRequest,
    ) -> RegistryPackQualityResponse:
        """Build, install, and evaluate one generated `medical-en` pack bundle."""

        try:
            return validate_medical_pack_quality(
                request.bundle_dir,
                general_bundle_dir=request.general_bundle_dir,
                output_dir=request.output_dir,
                fixture_profile=request.fixture_profile,
                version=request.version,
                min_expected_recall=request.min_expected_recall,
                max_unexpected_hits=request.max_unexpected_hits,
                max_ambiguous_aliases=request.max_ambiguous_aliases,
                max_dropped_alias_ratio=request.max_dropped_alias_ratio,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/evaluate-extraction-quality",
        response_model=RegistryEvaluateExtractionQualityResponse,
    )
    def runtime_evaluate_extraction_quality(
        request: RegistryEvaluateExtractionQualityRequest,
    ) -> RegistryEvaluateExtractionQualityResponse:
        """Evaluate one installed pack against one golden set."""

        try:
            return evaluate_extraction_quality(
                request.pack_id,
                storage_root=storage_root,
                golden_set_path=request.golden_set_path,
                profile=request.profile,
                hybrid=request.hybrid,
                write_report=request.write_report,
                report_path=request.report_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/evaluate-vector-quality",
        response_model=RegistryEvaluateVectorQualityResponse,
    )
    def runtime_evaluate_vector_quality(
        request: RegistryEvaluateVectorQualityRequest,
    ) -> RegistryEvaluateVectorQualityResponse:
        """Evaluate the hosted vector-quality lane against one golden set."""

        try:
            return evaluate_vector_quality(
                request.pack_id,
                storage_root=storage_root,
                golden_set_path=request.golden_set_path,
                profile=request.profile,
                refinement_depth=request.refinement_depth,
                top_k=request.top_k,
                write_report=request.write_report,
                report_path=request.report_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/benchmark-runtime",
        response_model=RegistryBenchmarkRuntimeResponse,
    )
    def runtime_benchmark_runtime(
        request: RegistryBenchmarkRuntimeRequest,
    ) -> RegistryBenchmarkRuntimeResponse:
        """Benchmark matcher load, warm tagging, operator lookup, and disk usage."""

        try:
            return benchmark_runtime(
                request.pack_id,
                storage_root=storage_root,
                golden_set_path=request.golden_set_path,
                profile=request.profile,
                hybrid=request.hybrid,
                warm_runs=request.warm_runs,
                lookup_limit=request.lookup_limit,
                write_report=request.write_report,
                report_path=request.report_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/benchmark-matcher-backends",
        response_model=RegistryBenchmarkMatcherBackendsResponse,
    )
    def runtime_benchmark_matcher_backends(
        request: RegistryBenchmarkMatcherBackendsRequest,
    ) -> RegistryBenchmarkMatcherBackendsResponse:
        """Benchmark candidate matcher backends on a bounded installed-pack alias slice."""

        try:
            return benchmark_matcher_backends(
                request.pack_id,
                storage_root=storage_root,
                golden_set_path=request.golden_set_path,
                profile=request.profile,
                alias_limit=request.alias_limit,
                scan_limit=request.scan_limit,
                min_alias_score=request.min_alias_score,
                exact_tier_min_token_count=request.exact_tier_min_token_count,
                query_runs=request.query_runs,
                write_report=request.write_report,
                report_path=request.report_path,
                output_root=request.output_root,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/compare-extraction-quality",
        response_model=RegistryCompareExtractionQualityResponse,
    )
    def runtime_compare_extraction_quality(
        request: RegistryCompareExtractionQualityRequest,
    ) -> RegistryCompareExtractionQualityResponse:
        """Compare two stored extraction-quality reports."""

        try:
            return compare_extraction_quality_reports(
                baseline_report_path=request.baseline_report_path,
                candidate_report_path=request.candidate_report_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/diff-pack",
        response_model=RegistryPackDiffResponse,
    )
    def runtime_diff_pack(
        request: RegistryDiffPackRequest,
    ) -> RegistryPackDiffResponse:
        """Diff two runtime pack directories."""

        try:
            return diff_pack_versions(
                request.old_pack_dir,
                request.new_pack_dir,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/evaluate-release-thresholds",
        response_model=RegistryEvaluateReleaseThresholdsResponse,
    )
    def runtime_evaluate_release_thresholds(
        request: RegistryEvaluateReleaseThresholdsRequest,
    ) -> RegistryEvaluateReleaseThresholdsResponse:
        """Evaluate one stored extraction-quality report against release thresholds."""

        try:
            return evaluate_extraction_release_thresholds(
                report_path=request.report_path,
                mode=request.mode,
                baseline_report_path=request.baseline_report_path,
                min_recall=request.min_recall,
                min_precision=request.min_precision,
                max_label_recall_drop=request.max_label_recall_drop,
                min_recall_lift=request.min_recall_lift,
                max_precision_drop=request.max_precision_drop,
                max_p95_latency_ms=request.max_p95_latency_ms,
                max_model_artifact_bytes=request.max_model_artifact_bytes,
                max_peak_memory_mb=request.max_peak_memory_mb,
                model_artifact_path=request.model_artifact_path,
                peak_memory_mb=request.peak_memory_mb,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/evaluate-vector-release-thresholds",
        response_model=RegistryEvaluateVectorReleaseThresholdsResponse,
    )
    def runtime_evaluate_vector_release_thresholds(
        request: RegistryEvaluateVectorReleaseThresholdsRequest,
    ) -> RegistryEvaluateVectorReleaseThresholdsResponse:
        """Evaluate one stored vector-quality report against release thresholds."""

        try:
            return evaluate_vector_release_thresholds_from_report(
                report_path=request.report_path,
                min_related_precision_at_k=request.min_related_precision_at_k,
                min_related_recall_at_k=request.min_related_recall_at_k,
                min_related_mrr=request.min_related_mrr,
                min_suppression_alignment_rate=request.min_suppression_alignment_rate,
                min_refinement_alignment_rate=request.min_refinement_alignment_rate,
                min_easy_case_pass_rate=request.min_easy_case_pass_rate,
                max_fallback_rate=request.max_fallback_rate,
                max_p95_latency_ms=request.max_p95_latency_ms,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/registry/generate-pack", response_model=RegistryGeneratePackResponse)
    def runtime_generate_pack(
        request: RegistryGeneratePackRequest,
    ) -> RegistryGeneratePackResponse:
        """Generate one runtime-compatible pack directory from a normalized bundle."""

        try:
            return generate_pack_source(
                request.bundle_dir,
                output_dir=request.output_dir,
                version=request.version,
                include_build_metadata=request.include_build_metadata,
                include_build_only=request.include_build_only,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/registry/report-pack", response_model=RegistryReportPackResponse)
    def runtime_report_pack(
        request: RegistryReportPackRequest,
    ) -> RegistryReportPackResponse:
        """Generate one runtime-compatible pack directory and report stable statistics."""

        try:
            return report_generated_pack(
                request.bundle_dir,
                output_dir=request.output_dir,
                version=request.version,
                include_build_metadata=request.include_build_metadata,
                include_build_only=request.include_build_only,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/refresh-generated-packs",
        response_model=RegistryRefreshGeneratedPacksResponse,
    )
    def runtime_refresh_generated_packs(
        request: RegistryRefreshGeneratedPacksRequest,
    ) -> RegistryRefreshGeneratedPacksResponse:
        """Refresh one or more generated bundles into a quality-gated registry release."""

        try:
            return refresh_generated_packs(
                request.bundle_dirs,
                output_dir=request.output_dir,
                general_bundle_dir=request.general_bundle_dir,
                materialize_registry=request.materialize_registry,
                min_expected_recall=request.min_expected_recall,
                max_unexpected_hits=request.max_unexpected_hits,
                max_ambiguous_aliases=request.max_ambiguous_aliases,
                max_dropped_alias_ratio=request.max_dropped_alias_ratio,
                release_gate_commands=request.release_gate_commands,
                release_gate_working_dir=request.release_gate_working_dir,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/publish-generated-release",
        response_model=RegistryPublishGeneratedReleaseResponse,
    )
    def runtime_publish_generated_release(
        request: RegistryPublishGeneratedReleaseRequest,
    ) -> RegistryPublishGeneratedReleaseResponse:
        """Publish one reviewed generated registry directory to object storage."""

        try:
            return publish_generated_registry_release(
                request.registry_dir,
                prefix=request.prefix,
                bucket=request.bucket,
                endpoint=request.endpoint,
                region=request.region,
                delete=request.delete,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/prepare-deploy-release",
        response_model=RegistryPrepareDeployReleaseResponse,
    )
    def runtime_prepare_deploy_release(
        request: RegistryPrepareDeployReleaseRequest,
    ) -> RegistryPrepareDeployReleaseResponse:
        """Prepare the deploy-owned registry payload from bundled packs or a promoted release."""

        try:
            return prepare_registry_deploy_release(
                output_dir=request.output_dir,
                pack_dirs=request.pack_dirs,
                promotion_spec_path=request.promotion_spec_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/smoke-published-release",
        response_model=RegistrySmokePublishedReleaseResponse,
    )
    def runtime_smoke_published_release(
        request: RegistrySmokePublishedReleaseRequest,
    ) -> RegistrySmokePublishedReleaseResponse:
        """Run clean pull/install/tag smoke against one published registry URL."""

        try:
            return smoke_test_published_generated_registry(
                request.registry_url,
                pack_ids=request.pack_ids,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/news/analyze", response_model=NewsAnalyzeResponse)
    def runtime_news_analyze(request: NewsAnalyzeRequest) -> NewsAnalyzeResponse:
        """Analyze one news article through the normalized ADES contract."""

        settings = runtime_state.current_settings()
        if not settings.news_analyze_enabled:
            raise HTTPException(status_code=404, detail="ADES_NEWS_ANALYZE_DISABLED")

        started_at = time.perf_counter()
        warnings: list[str] = []
        raw_analysis_text = _news_analysis_text(request)
        analysis_text = _clean_news_analysis_text(raw_analysis_text)
        warnings.extend(_story_quality_warnings(raw_analysis_text, analysis_text))
        event_signals = extract_news_event_signals(analysis_text)
        non_market_reasons = _non_market_news_rejection_reasons(
            request=request,
            text=analysis_text,
            event_signals=event_signals,
        )
        warnings.extend(
            f"ADES_NEWS_ANALYZE_NON_MARKET_DOMAIN:{reason}"
            for reason in non_market_reasons
        )
        country_hint, country_hint_source = _news_country_hint(request)
        tag_responses: list[TagResponse] = []
        registry = runtime_state.ensure_registry()
        include_relationship_proposals = request.options.include_relationship_proposals
        include_related_entities = (
            request.options.include_related_entities or include_relationship_proposals
        )
        include_graph_support = (
            request.options.include_graph_support or include_relationship_proposals
        )
        hybrid_option = _news_analyze_hybrid_option(request)
        hybrid_pack_limit = _news_analyze_hybrid_pack_limit()
        hybrid_pack_count = 0
        packs, pack_decisions = _build_initial_news_pack_plan(
            request=request,
            settings=settings,
            registry=registry,
        )

        def _run_pack_batch(batch_packs: list[str]) -> None:
            nonlocal hybrid_pack_count
            for pack in batch_packs:
                if any(response.pack == pack for response in tag_responses):
                    continue
                pack_hybrid = hybrid_option
                if hybrid_option is True and hybrid_pack_limit is not None:
                    if hybrid_pack_count >= hybrid_pack_limit:
                        pack_hybrid = False
                    else:
                        hybrid_pack_count += 1
                try:
                    runtime_state.prewarm_pack(pack)
                    response = tag(
                        analysis_text,
                        pack=pack,
                        content_type=request.content_type,
                        storage_root=storage_root,
                        debug=request.options.debug,
                        hybrid=pack_hybrid,
                        include_related_entities=include_related_entities,
                        include_graph_support=include_graph_support,
                        refine_links=request.options.refine_links,
                        refinement_depth=request.options.refinement_depth,
                        include_impact_paths=False,
                        domain_hint=request.domain_hint,
                        retrieval_profile=request.retrieval_profile,
                        country_hint=country_hint,
                        registry=registry,
                    )
                except FileNotFoundError as exc:
                    warnings.append(f"ADES_NEWS_ANALYZE_PACK_NOT_FOUND:{pack}:{str(exc)[:160]}")
                    continue
                except (IsADirectoryError, NotADirectoryError, ValueError) as exc:
                    warnings.append(f"ADES_NEWS_ANALYZE_PACK_FAILED:{pack}:{str(exc)[:160]}")
                    continue
                tag_responses.append(response)
                warnings.extend(response.warnings)

        _run_pack_batch(packs)
        entities = _merge_entity_matches(
            [entity for response in tag_responses for entity in response.entities]
        )
        country_packs = _add_country_finance_pack_plan(
            decisions=pack_decisions,
            packs=packs,
            registry=registry,
            request=request,
            text=analysis_text,
            entities=entities,
        )
        _run_pack_batch(country_packs)
        if not packs:
            raise HTTPException(status_code=400, detail="At least one pack is required.")
        entities = _merge_entity_matches(
            [entity for response in tag_responses for entity in response.entities]
        )
        production_entities = [
            entity for entity in entities if not _is_low_trust_entity_claim(entity)
        ]
        proposal_only_entity_refs = _dedupe_string_values(
            [
                entity.link.entity_id if entity.link else _entity_ref(entity)
                for entity in entities
                if _is_low_trust_entity_claim(entity)
            ]
        )
        if proposal_only_entity_refs:
            warnings.append(
                "ADES_NEWS_ANALYZE_LOW_TRUST_ENTITY_CLAIMS_PROPOSAL_ONLY:"
                f"{len(proposal_only_entity_refs)}"
            )
        entity_refs = _dedupe_string_values(
            [
                _production_entity_ref_for_news_seed(
                    entity,
                    text=analysis_text,
                    warnings=warnings,
                )
                for entity in production_entities
            ]
        )
        graph_enabled_packs = _news_graph_enabled_packs(
            tag_responses=tag_responses,
            pack_decisions=pack_decisions,
        )
        direct_terminal_seed_refs = (
            []
            if non_market_reasons
            else _direct_market_terminal_seed_refs(
                text=analysis_text,
                request=request,
                event_signals=event_signals,
            )
        )
        sector_seed_refs = (
            []
            if non_market_reasons
            else _market_sector_seed_refs_from_graph(
                text=analysis_text,
                event_signals=event_signals,
                country_codes=[
                    code
                    for code, _reason in _country_code_candidates_for_pack_plan(
                        request=request,
                        text=analysis_text,
                        entities=entities,
                    )
                ],
                graph_enabled_packs=graph_enabled_packs,
                settings=settings,
                warnings=warnings,
            )
        )
        entity_refs = _dedupe_string_values(
            [*entity_refs, *direct_terminal_seed_refs, *sector_seed_refs]
        )

        impact_paths: ImpactExpansionResult | None = None
        include_relationships = (
            request.options.include_impact_paths
            and request.options.include_relationship_paths
            and bool(entity_refs)
            and not non_market_reasons
        )
        if include_relationships:
            try:
                impact_paths = expand_impact_paths(
                    entity_refs,
                    enabled_packs=graph_enabled_packs,
                    max_depth=request.options.impact_max_depth,
                    impact_expansion_seed_limit=request.options.impact_seed_limit,
                    max_candidates=(
                        request.options.impact_max_candidates
                        if request.options.impact_max_candidates is not None
                        else request.options.max_terminal_candidates
                    ),
                    include_passive_paths=True,
                    vector_proposals_enabled=False,
                    compatible_event_types=[signal.event_type for signal in event_signals],
                    storage_root=storage_root,
                )
            except ValueError as exc:
                warnings.append(f"ADES_NEWS_ANALYZE_IMPACT_EXPAND_FAILED:{str(exc)[:180]}")

        if impact_paths is not None:
            warnings.extend(impact_paths.warnings)

        expanded_terminal_candidates = impact_paths.candidates if impact_paths else []
        if impact_paths is not None:
            expanded_terminal_candidates = _filter_unanchored_direct_terminal_candidates(
                expanded_terminal_candidates,
                impact_paths.source_entities,
                text=analysis_text,
                warnings=warnings,
            )
            direct_terminal_candidates = _direct_terminal_candidates_from_source_entities(
                impact_paths.source_entities,
                text=analysis_text,
                warnings=warnings,
            )
            if direct_terminal_candidates:
                expanded_terminal_candidates = _merge_direct_terminal_candidates(
                    direct_terminal_candidates,
                    expanded_terminal_candidates,
                )
            if expanded_terminal_candidates != impact_paths.candidates:
                impact_paths = impact_paths.model_copy(
                    update={"candidates": expanded_terminal_candidates}
                )
        pre_gate_terminal_candidates = list(expanded_terminal_candidates)
        if impact_paths is not None:
            expanded_terminal_candidates, event_gate_warnings = (
                gate_terminal_candidates_by_event_signals(
                    expanded_terminal_candidates,
                    event_signals,
                )
            )
            if event_gate_warnings:
                warnings.extend(event_gate_warnings)
                impact_paths = impact_paths.model_copy(
                    update={
                        "candidates": expanded_terminal_candidates,
                        "warnings": _dedupe_string_values(
                            [*impact_paths.warnings, *event_gate_warnings]
                        ),
                    }
                )
            else:
                impact_paths = impact_paths.model_copy(
                    update={"candidates": expanded_terminal_candidates}
                )
        if impact_paths is not None and expanded_terminal_candidates:
            direction_adjusted_candidates = _apply_story_direction_overrides(
                candidates=expanded_terminal_candidates,
                text=analysis_text,
                warnings=warnings,
            )
            if direction_adjusted_candidates != expanded_terminal_candidates:
                expanded_terminal_candidates = direction_adjusted_candidates
                impact_paths = impact_paths.model_copy(
                    update={
                        "candidates": expanded_terminal_candidates,
                        "warnings": _dedupe_string_values(
                            [*impact_paths.warnings, *warnings]
                        ),
                    }
                )
        terminal_candidates = (
            expanded_terminal_candidates[: request.options.max_terminal_candidates]
            if request.options.include_terminal_candidates
            else []
        )
        country_scope = _build_country_scope(
            country_hint=country_hint,
            hint_source=country_hint_source,
            entities=entities,
            impact_source_entities=impact_paths.source_entities if impact_paths else [],
            settings=settings,
            text=analysis_text,
        )
        country_scopes = [
            *([country_scope] if country_scope else []),
            *_additional_country_scopes_from_text(
                text=analysis_text,
                primary_scope=country_scope,
            ),
        ]
        passive_entities = (
            _build_passive_entities(
                entities=entities,
                terminal_candidates=expanded_terminal_candidates,
                country_scope=country_scope,
                country_scopes=country_scopes,
                max_entities=request.options.max_passive_entities,
                request=request,
            )
            if request.options.include_passive_entities
            else []
        )
        topics = _merge_topic_matches(tag_responses)
        topic_scope = _build_topic_scope(
            topics=topics,
            request=request,
            event_signals=event_signals,
            text=analysis_text,
        )
        relationship_proposals = (
            _build_relationship_proposals(
                tag_responses=tag_responses,
                event_signals=event_signals,
                impact_paths=impact_paths,
                terminal_candidates=expanded_terminal_candidates,
            )
            if include_relationship_proposals
            else []
        )
        unresolved_entities = _build_unresolved_entities(
            passive_entities=passive_entities,
            terminal_candidates=expanded_terminal_candidates,
            impact_paths=impact_paths,
            relationship_proposal_counts=_relationship_proposal_counts_by_seed(
                relationship_proposals
            ),
            country_scope=country_scope,
            topic_scope=topic_scope,
            event_signals=event_signals,
        )
        artifact_versions = NewsAnalyzeArtifactVersions(
            ades_version=__version__,
            packs={
                response.pack: response.pack_version for response in tag_responses if response.pack
            },
            impact_graph_version=impact_paths.graph_version if impact_paths else None,
            impact_artifact_version=(impact_paths.artifact_version if impact_paths else None),
            impact_artifact_hash=impact_paths.artifact_hash if impact_paths else None,
        )
        artifact_metadata = _build_artifact_metadata(artifact_versions)
        artifact_ref = _candidate_artifact_ref(artifact_versions)
        rejected_candidates = _build_rejected_candidates(
            pre_gate_candidates=pre_gate_terminal_candidates,
            gated_candidates=expanded_terminal_candidates,
            returned_candidates=terminal_candidates,
            artifact_ref=artifact_ref,
        )
        candidate_paths = _build_candidate_paths(
            terminal_candidates=terminal_candidates,
            artifact_ref=artifact_ref,
        )
        quality_flags: list[str] = []
        if non_market_reasons:
            quality_flags.append("NON_MARKET_NEWS_DOMAIN")
        if country_scope is None:
            quality_flags.append("COUNTRY_SCOPE_MISSING")
        if any(not passive.display_eligible for passive in passive_entities):
            quality_flags.append("PASSIVE_HIDDEN_ARTIFACTS_PRESENT")
        if not terminal_candidates:
            quality_flags.append("NO_TERMINAL_IMPACT_CANDIDATES")
        if unresolved_entities:
            quality_flags.append("UNRESOLVED_MARKET_RELATIONSHIPS_PRESENT")

        deduped_warnings = _dedupe_string_values(warnings)[:32]
        no_terminal_reasons = _build_no_terminal_reasons(
            terminal_candidates=terminal_candidates,
            production_entities=production_entities,
            entity_refs=entity_refs,
            impact_paths=impact_paths,
            pre_gate_candidates=pre_gate_terminal_candidates,
            unresolved_entities=unresolved_entities,
            rejected_candidates=rejected_candidates,
            pack_decisions=pack_decisions,
            warnings=deduped_warnings,
        )
        diagnostics = _build_diagnostics(
            warnings=deduped_warnings,
            quality_flags=quality_flags,
            unresolved_entities=unresolved_entities,
            rejected_candidates=rejected_candidates,
            no_terminal_reasons=no_terminal_reasons,
        )
        source_lane_coverage = _build_source_lane_coverage(
            tag_responses=tag_responses,
            pack_decisions=pack_decisions,
            artifact_versions=artifact_versions,
            source_entities=impact_paths.source_entities if impact_paths else [],
            terminal_candidates=terminal_candidates,
            unresolved_entities=unresolved_entities,
            rejected_candidates=rejected_candidates,
            warnings=deduped_warnings,
            impact_paths=impact_paths,
        )
        debug = (
            _build_news_analyze_debug(
                entity_refs=entity_refs,
                graph_enabled_packs=graph_enabled_packs,
                candidate_paths=candidate_paths,
                rejected_candidates=rejected_candidates,
                unresolved_entities=unresolved_entities,
                diagnostics=diagnostics,
                passive_entities=passive_entities,
                event_signals=event_signals,
                warnings=deduped_warnings,
            )
            if request.options.debug
            else None
        )

        timing_ms = int((time.perf_counter() - started_at) * 1000)
        return NewsAnalyzeResponse(
            version=__version__,
            language=tag_responses[0].language if tag_responses else "en",
            content_type=request.content_type,
            packs=packs,
            packs_used=_dedupe_string_values([response.pack for response in tag_responses]),
            pack_decisions=pack_decisions,
            country_scope=country_scope,
            topic_scope=topic_scope,
            passive_entities=passive_entities,
            unresolved_entities=unresolved_entities,
            relationship_proposals=relationship_proposals,
            event_signals=event_signals,
            source_entities=impact_paths.source_entities if impact_paths else [],
            terminal_impact_candidates=terminal_candidates,
            impact_paths=impact_paths,
            topics=topics,
            artifact_versions=artifact_versions,
            artifact_metadata=artifact_metadata,
            terminal_candidates=terminal_candidates,
            candidate_paths=candidate_paths,
            rejected_candidates=rejected_candidates,
            diagnostics=diagnostics,
            no_terminal_reasons=no_terminal_reasons,
            event_signal=event_signals[0] if event_signals else None,
            source_lane_coverage=source_lane_coverage,
            tag_responses=(tag_responses if request.options.include_tag_responses else []),
            warnings=deduped_warnings,
            quality_flags=quality_flags,
            debug=debug,
            timing_ms=timing_ms,
            total_time_ms=timing_ms,
        )

    @app.post("/v0/impact/expand", response_model=ImpactExpansionResult)
    def runtime_expand_impact_paths(
        request: ImpactExpansionRequest,
    ) -> ImpactExpansionResult:
        """Expand ADES entity refs into deterministic market impact paths."""

        try:
            return expand_impact_paths(
                request.entity_refs,
                language=request.language,
                enabled_packs=request.enabled_packs,
                max_depth=request.max_depth,
                impact_expansion_seed_limit=request.impact_expansion_seed_limit,
                max_candidates=request.max_candidates,
                include_passive_paths=request.include_passive_paths,
                vector_proposals_enabled=request.vector_proposals_enabled,
                storage_root=storage_root,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/tag", response_model=TagResponse)
    def runtime_tag(request: TagRequest) -> TagResponse:
        """Tag text through the local in-process pipeline."""

        try:
            resolved_pack = request.pack or runtime_state.current_settings().default_pack
            runtime_state.prewarm_pack(resolved_pack)
            return tag(
                request.text,
                pack=resolved_pack,
                content_type=request.content_type,
                output_path=request.output.path if request.output else None,
                output_dir=request.output.directory if request.output else None,
                pretty_output=request.output.pretty if request.output else True,
                storage_root=storage_root,
                debug=bool(_bool_option(request.options, "debug")),
                hybrid=_bool_option(request.options, "hybrid"),
                include_related_entities=bool(
                    _bool_option(request.options, "include_related_entities")
                ),
                include_graph_support=bool(_bool_option(request.options, "include_graph_support")),
                refine_links=bool(_bool_option(request.options, "refine_links")),
                refinement_depth=_refinement_depth_option(request.options),
                include_impact_paths=bool(_bool_option(request.options, "include_impact_paths")),
                impact_max_depth=_int_option(request.options, "impact_max_depth"),
                impact_seed_limit=_int_option(request.options, "impact_seed_limit"),
                impact_max_candidates=_int_option(
                    request.options,
                    "impact_max_candidates",
                ),
                domain_hint=_request_string_option(
                    request.domain_hint,
                    request.options,
                    "domain_hint",
                ),
                retrieval_profile=_request_string_option(
                    request.retrieval_profile,
                    request.options,
                    "retrieval_profile",
                ),
                country_hint=_string_option(request.options, "country_hint"),
                registry=runtime_state.ensure_registry(),
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/tag/file", response_model=TagResponse)
    def runtime_tag_file(request: FileTagRequest) -> TagResponse:
        """Tag a local file through the in-process pipeline."""

        try:
            resolved_pack = request.pack or runtime_state.current_settings().default_pack
            runtime_state.prewarm_pack(resolved_pack)
            return tag_file(
                request.path,
                pack=resolved_pack,
                content_type=request.content_type,
                output_path=request.output.path if request.output else None,
                output_dir=request.output.directory if request.output else None,
                pretty_output=request.output.pretty if request.output else True,
                storage_root=storage_root,
                debug=bool(_bool_option(request.options, "debug")),
                hybrid=_bool_option(request.options, "hybrid"),
                include_related_entities=bool(
                    _bool_option(request.options, "include_related_entities")
                ),
                include_graph_support=bool(_bool_option(request.options, "include_graph_support")),
                refine_links=bool(_bool_option(request.options, "refine_links")),
                refinement_depth=_refinement_depth_option(request.options),
                include_impact_paths=bool(_bool_option(request.options, "include_impact_paths")),
                impact_max_depth=_int_option(request.options, "impact_max_depth"),
                impact_seed_limit=_int_option(request.options, "impact_seed_limit"),
                impact_max_candidates=_int_option(
                    request.options,
                    "impact_max_candidates",
                ),
                domain_hint=_request_string_option(
                    request.domain_hint,
                    request.options,
                    "domain_hint",
                ),
                retrieval_profile=_request_string_option(
                    request.retrieval_profile,
                    request.options,
                    "retrieval_profile",
                ),
                country_hint=_string_option(request.options, "country_hint"),
                registry=runtime_state.ensure_registry(),
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/tag/files", response_model=BatchTagResponse)
    def runtime_tag_files(request: BatchFileTagRequest) -> BatchTagResponse:
        """Tag multiple local files through the in-process pipeline."""

        if request.output and request.output.path is not None:
            raise HTTPException(
                status_code=400,
                detail="Batch file tagging supports output.directory only.",
            )
        if (
            request.output
            and (request.output.write_manifest or request.output.manifest_path is not None)
            and request.output.directory is None
        ):
            raise HTTPException(
                status_code=400,
                detail="Batch manifest export requires output.directory.",
            )
        try:
            if request.pack is not None:
                runtime_state.prewarm_pack(request.pack)
            return tag_files(
                request.paths,
                pack=request.pack,
                content_type=request.content_type,
                output_dir=request.output.directory if request.output else None,
                pretty_output=request.output.pretty if request.output else True,
                storage_root=storage_root,
                directories=request.directories,
                glob_patterns=request.glob_patterns,
                manifest_input_path=request.manifest_input_path,
                manifest_replay_mode=request.manifest_replay_mode,
                skip_unchanged=request.skip_unchanged,
                reuse_unchanged_outputs=request.reuse_unchanged_outputs,
                repair_missing_reused_outputs=request.repair_missing_reused_outputs,
                recursive=request.recursive,
                include_patterns=request.include_patterns,
                exclude_patterns=request.exclude_patterns,
                max_files=request.max_files,
                max_input_bytes=request.max_input_bytes,
                write_manifest=request.output.write_manifest if request.output else False,
                manifest_output_path=request.output.manifest_path if request.output else None,
                debug=bool(_bool_option(request.options, "debug")),
                hybrid=_bool_option(request.options, "hybrid"),
                include_related_entities=bool(
                    _bool_option(request.options, "include_related_entities")
                ),
                include_graph_support=bool(_bool_option(request.options, "include_graph_support")),
                refine_links=bool(_bool_option(request.options, "refine_links")),
                refinement_depth=_refinement_depth_option(request.options),
                include_impact_paths=bool(_bool_option(request.options, "include_impact_paths")),
                impact_max_depth=_int_option(request.options, "impact_max_depth"),
                impact_seed_limit=_int_option(request.options, "impact_seed_limit"),
                impact_max_candidates=_int_option(
                    request.options,
                    "impact_max_candidates",
                ),
                domain_hint=_request_string_option(
                    request.domain_hint,
                    request.options,
                    "domain_hint",
                ),
                retrieval_profile=_request_string_option(
                    request.retrieval_profile,
                    request.options,
                    "retrieval_profile",
                ),
                country_hint=_string_option(request.options, "country_hint"),
                registry=runtime_state.ensure_registry(),
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v0/lookup", response_model=LookupResponse)
    def runtime_lookup(
        q: str = Query(..., min_length=1),
        pack_id: str | None = Query(None),
        exact_alias: bool = Query(False),
        fuzzy: bool = Query(False),
        active_only: bool = Query(True),
        limit: int = Query(20, ge=1, le=100),
    ) -> LookupResponse:
        """Search deterministic alias and rule metadata candidates."""

        try:
            return lookup_candidates(
                q,
                storage_root=storage_root,
                pack_id=pack_id,
                exact_alias=exact_alias,
                fuzzy=fuzzy,
                active_only=active_only,
                limit=limit,
            )
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)

    return app


app = create_app()
