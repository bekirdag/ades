"""Shared alias-analysis helpers for generated pack builds."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from functools import lru_cache
import hashlib
import json
from math import log10
import os
from pathlib import Path
import re
import shlex
import sqlite3
import subprocess
from typing import Any, Callable, Iterable, Iterator

try:
    from wordfreq import zipf_frequency as _wordfreq_zipf_frequency
except Exception:  # pragma: no cover - exercised by runtime dependency resolution.
    _wordfreq_zipf_frequency = None


DEFAULT_ALIAS_ANALYSIS_BACKEND = "sqlite"
DEFAULT_ALIAS_ANALYSIS_TOP_CLUSTERS = 20
DEFAULT_RETAINED_ALIAS_AUDIT_CHUNK_SIZE = 1000
DEFAULT_RETAINED_ALIAS_AUDIT_TIMEOUT_SECONDS = 120.0
DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME = (
    "general-retained-alias-exclusions.jsonl"
)
GENERAL_RETAINED_ALIAS_AUDIT_MODE_ENV = "ADES_GENERAL_ALIAS_AUDIT_MODE"
GENERAL_RETAINED_ALIAS_AUDIT_COMMAND_ENV = "ADES_GENERAL_ALIAS_AUDIT_COMMAND"
GENERAL_RETAINED_ALIAS_AUDIT_TIMEOUT_ENV = "ADES_GENERAL_ALIAS_AUDIT_TIMEOUT_SECONDS"
_GENERAL_RETAINED_ALIAS_AUDIT_MODE_AUTO = "auto"
_GENERAL_RETAINED_ALIAS_AUDIT_MODE_HEURISTIC = "heuristic"
_GENERAL_RETAINED_ALIAS_AUDIT_MODE_AI_COMMAND = "ai_command"
_GENERAL_RETAINED_ALIAS_AUDIT_MODE_DETERMINISTIC_COMMON_ENGLISH = (
    "deterministic_common_english"
)
_GENERAL_RETAINED_ALIAS_AUDIT_MODE_CALLABLE = "callable"
_GENERAL_RETAINED_ALIAS_AUDIT_ALLOWED_MODES = {
    _GENERAL_RETAINED_ALIAS_AUDIT_MODE_AUTO,
    _GENERAL_RETAINED_ALIAS_AUDIT_MODE_HEURISTIC,
    _GENERAL_RETAINED_ALIAS_AUDIT_MODE_AI_COMMAND,
    _GENERAL_RETAINED_ALIAS_AUDIT_MODE_DETERMINISTIC_COMMON_ENGLISH,
}
_EXACT_IDENTIFIER_LABELS = {"cik", "clinical_trial", "gene", "lei", "ticker"}
_DEFAULT_SOURCE_PRIORITIES = {
    "curated": 0.9,
    "wikidata": 0.8,
    "geonames": 0.82,
    "sec": 0.88,
    "nasdaq": 0.9,
    "mic": 0.78,
    "hgnc": 0.92,
    "uniprot": 0.9,
    "clinicaltrials": 0.94,
    "disease": 0.86,
    "internal": 0.2,
}
_LABEL_SPECIFICITY_WEIGHTS = {
    "cik": 1.0,
    "clinical_trial": 1.0,
    "ticker": 0.98,
    "gene": 0.94,
    "exchange": 0.86,
    "drug": 0.84,
    "protein": 0.82,
    "disease": 0.8,
    "organization": 0.72,
    "person": 0.72,
    "location": 0.7,
}
_RUNTIME_FUNCTION_WORDS = {
    "a",
    "an",
    "the",
    "this",
    "that",
    "these",
    "those",
    "some",
    "any",
    "each",
    "every",
    "my",
    "your",
    "his",
    "her",
    "its",
    "our",
    "their",
}
_RUNTIME_GENERIC_PHRASE_LEADS = _RUNTIME_FUNCTION_WORDS | {
    "him",
    "he",
    "i",
    "it",
    "me",
    "she",
    "they",
    "them",
    "us",
    "we",
    "you",
}
_RUNTIME_AUXILIARY_WORDS = {
    "am",
    "are",
    "be",
    "been",
    "being",
    "can",
    "could",
    "did",
    "do",
    "does",
    "had",
    "has",
    "have",
    "is",
    "may",
    "might",
    "must",
    "shall",
    "should",
    "was",
    "were",
    "will",
    "would",
}
_RUNTIME_CALENDAR_SINGLE_TOKEN_WORDS = {
    "april",
    "august",
    "december",
    "february",
    "friday",
    "january",
    "july",
    "june",
    "march",
    "may",
    "monday",
    "november",
    "october",
    "saturday",
    "september",
    "sunday",
    "thursday",
    "tuesday",
    "wednesday",
}
_GENERAL_AUDIT_DROPPABLE_CALENDAR_SINGLE_TOKEN_WORDS = {"march", "may"}
_RUNTIME_NUMBER_SINGLE_TOKEN_WORDS = {
    "eight",
    "eighteen",
    "eighty",
    "eleven",
    "fifteen",
    "fifty",
    "five",
    "forty",
    "four",
    "fourteen",
    "hundred",
    "nine",
    "nineteen",
    "ninety",
    "one",
    "seven",
    "seventeen",
    "seventy",
    "six",
    "sixteen",
    "sixty",
    "ten",
    "thirteen",
    "thirty",
    "thousand",
    "three",
    "twelve",
    "twenty",
    "two",
    "zero",
}
_RUNTIME_PERSON_PARTICLES = {
    "al",
    "ap",
    "ben",
    "bin",
    "da",
    "dal",
    "de",
    "del",
    "della",
    "der",
    "di",
    "do",
    "dos",
    "du",
    "el",
    "ibn",
    "la",
    "le",
    "st",
    "van",
    "von",
}
_RUNTIME_GENERIC_LOCATION_HEADS = {
    "city",
    "county",
    "district",
    "province",
    "region",
    "state",
    "town",
    "village",
    "world",
}
_RUNTIME_GENERIC_ENTITY_HEADS = {
    "bank",
    "company",
    "department",
    "founder",
    "fund",
    "market",
    "product",
    "report",
    "stock",
}
_RUNTIME_GENERIC_SINGLE_TOKEN_HEADS = (
    _RUNTIME_GENERIC_ENTITY_HEADS | _RUNTIME_GENERIC_LOCATION_HEADS | {"university"}
)
_GENERAL_AUDIT_COMMON_SINGLE_TOKEN_WORDS = (
    _RUNTIME_GENERIC_PHRASE_LEADS
    | _RUNTIME_AUXILIARY_WORDS
    | _RUNTIME_CALENDAR_SINGLE_TOKEN_WORDS
    | _RUNTIME_NUMBER_SINGLE_TOKEN_WORDS
    | _RUNTIME_GENERIC_SINGLE_TOKEN_HEADS
    | {
        "article",
        "articles",
        "book",
        "books",
        "call",
        "calls",
        "capital",
        "capitals",
        "car",
        "cars",
        "care",
        "case",
        "cases",
        "council",
        "councils",
        "day",
        "days",
        "field",
        "fields",
        "film",
        "films",
        "game",
        "games",
        "group",
        "groups",
        "home",
        "homes",
        "house",
        "houses",
        "information",
        "intelligence",
        "life",
        "lives",
        "line",
        "lines",
        "media",
        "month",
        "months",
        "news",
        "part",
        "parts",
        "place",
        "places",
        "research",
        "review",
        "reviews",
        "road",
        "roads",
        "service",
        "services",
        "side",
        "sides",
        "solution",
        "solutions",
        "story",
        "stories",
        "street",
        "streets",
        "system",
        "systems",
        "technology",
        "technologies",
        "time",
        "times",
        "town",
        "towns",
        "use",
        "uses",
        "value",
        "values",
        "way",
        "ways",
        "world",
        "worlds",
        "year",
        "years",
    }
)
_GENERAL_AUDIT_GENERIC_PHRASE_LEADS = (
    _RUNTIME_GENERIC_PHRASE_LEADS
    | _RUNTIME_AUXILIARY_WORDS
    | _RUNTIME_NUMBER_SINGLE_TOKEN_WORDS
)
_GENERAL_AUDIT_GENERIC_PHRASE_TAILS = {
    "day",
    "days",
    "month",
    "months",
    "way",
    "ways",
    "year",
    "years",
}
_GENERAL_AUDIT_COMMON_PHRASE_TOKENS = (
    _RUNTIME_AUXILIARY_WORDS
    | _RUNTIME_NUMBER_SINGLE_TOKEN_WORDS
    | _GENERAL_AUDIT_GENERIC_PHRASE_TAILS
)
_GENERAL_AUDIT_WORDFREQ_SINGLE_TOKEN_ZIPF_MIN = 4.8
_GENERAL_AUDIT_WORDFREQ_PHRASE_ZIPF_MIN = 4.8
_GENERAL_AUDIT_WORDFREQ_COMMON_TOKEN_ZIPF_MIN = 4.8
_GENERAL_AUDIT_ARTICLE_LEADS = {
    "a",
    "an",
    "the",
}
_GENERAL_AUDIT_EXECUTIVE_TITLE_ACRONYMS = {
    "cao",
    "cbo",
    "cco",
    "cdo",
    "ceo",
    "cfo",
    "cho",
    "cio",
    "cmo",
    "coo",
    "cpo",
    "cro",
    "cso",
    "cto",
}
_ACRONYM_CONNECTOR_TOKENS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "the",
    "to",
    "with",
}


@dataclass(frozen=True)
class AliasScoreFeatures:
    """Canonical score-feature contract for one alias candidate."""

    source_priority: float
    status_weight: float
    preferred_name_weight: float
    popularity_weight: float
    identifier_specificity_weight: float
    label_specificity_weight: float
    alias_form_penalty: float

    def to_dict(self) -> dict[str, float]:
        return {
            "source_priority": self.source_priority,
            "status_weight": self.status_weight,
            "preferred_name_weight": self.preferred_name_weight,
            "popularity_weight": self.popularity_weight,
            "identifier_specificity_weight": self.identifier_specificity_weight,
            "label_specificity_weight": self.label_specificity_weight,
            "alias_form_penalty": self.alias_form_penalty,
        }


@dataclass(frozen=True)
class AliasCandidate:
    """One normalized alias candidate ready for cluster analysis."""

    alias_key: str
    display_text: str
    label: str
    canonical_text: str
    entity_id: str
    source_name: str
    source_id: str
    lane: str
    generated: bool
    score: float
    features: AliasScoreFeatures


@dataclass(frozen=True)
class AliasClusterCandidate:
    """Best representative candidate for one label inside a cluster."""

    label: str
    display_text: str
    canonical_text: str
    lane: str
    generated: bool
    score: float
    features: AliasScoreFeatures
    source_name: str
    source_id: str
    entity_id: str
    label_candidate_count: int = 1
    non_generated_candidate_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "display_text": self.display_text,
            "canonical_text": self.canonical_text,
            "lane": self.lane,
            "generated": self.generated,
            "score": self.score,
            "features": self.features.to_dict(),
            "source_name": self.source_name,
            "source_id": self.source_id,
            "entity_id": self.entity_id,
            "label_candidate_count": self.label_candidate_count,
            "non_generated_candidate_count": self.non_generated_candidate_count,
        }


@dataclass(frozen=True)
class AliasClusterReport:
    """Decision report for one alias collision cluster."""

    alias_key: str
    lane: str
    candidate_count: int
    label_count: int
    ambiguous: bool
    retained_labels: list[str] = field(default_factory=list)
    blocked_labels: list[str] = field(default_factory=list)
    reason: str | None = None
    top_score: float | None = None
    runner_up_score: float | None = None
    candidates: list[AliasClusterCandidate] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "alias_key": self.alias_key,
            "lane": self.lane,
            "candidate_count": self.candidate_count,
            "label_count": self.label_count,
            "ambiguous": self.ambiguous,
            "retained_labels": self.retained_labels,
            "blocked_labels": self.blocked_labels,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }
        if self.reason is not None:
            payload["reason"] = self.reason
        if self.top_score is not None:
            payload["top_score"] = self.top_score
        if self.runner_up_score is not None:
            payload["runner_up_score"] = self.runner_up_score
        return payload


@dataclass(frozen=True)
class AliasAnalysisResult:
    """Persisted alias-analysis outcome for one generated pack build."""

    backend: str
    database_path: str | None
    candidate_alias_count: int
    retained_alias_count: int
    blocked_alias_count: int
    ambiguous_alias_count: int
    exact_identifier_alias_count: int
    natural_language_alias_count: int
    blocked_reason_counts: dict[str, int] = field(default_factory=dict)
    retained_label_counts: dict[str, int] = field(default_factory=dict)
    retained_aliases_materialized: bool = True
    retained_aliases: list[dict[str, Any]] = field(default_factory=list)
    top_collision_clusters: list[AliasClusterReport] = field(default_factory=list)
    retained_alias_audit_scanned_alias_count: int = 0
    retained_alias_audit_removed_alias_count: int = 0
    retained_alias_audit_chunk_size: int = 0
    retained_alias_audit_reason_counts: dict[str, int] = field(default_factory=dict)
    retained_alias_audit_mode: str = _GENERAL_RETAINED_ALIAS_AUDIT_MODE_HEURISTIC
    retained_alias_audit_reviewer_name: str | None = None
    retained_alias_audit_review_cache_path: str | None = None
    retained_alias_audit_invoked_review_count: int = 0
    retained_alias_audit_cached_review_count: int = 0
    retained_alias_exclusion_removed_alias_count: int = 0
    retained_alias_exclusion_source_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "database_path": self.database_path,
            "candidate_alias_count": self.candidate_alias_count,
            "retained_alias_count": self.retained_alias_count,
            "blocked_alias_count": self.blocked_alias_count,
            "ambiguous_alias_count": self.ambiguous_alias_count,
            "exact_identifier_alias_count": self.exact_identifier_alias_count,
            "natural_language_alias_count": self.natural_language_alias_count,
            "blocked_reason_counts": self.blocked_reason_counts,
            "retained_label_counts": self.retained_label_counts,
            "retained_aliases_materialized": self.retained_aliases_materialized,
            "retained_aliases": self.retained_aliases,
            "top_collision_clusters": [
                cluster.to_dict() for cluster in self.top_collision_clusters
            ],
            "retained_alias_audit_scanned_alias_count": self.retained_alias_audit_scanned_alias_count,
            "retained_alias_audit_removed_alias_count": self.retained_alias_audit_removed_alias_count,
            "retained_alias_audit_chunk_size": self.retained_alias_audit_chunk_size,
            "retained_alias_audit_reason_counts": self.retained_alias_audit_reason_counts,
            "retained_alias_audit_mode": self.retained_alias_audit_mode,
            "retained_alias_audit_reviewer_name": self.retained_alias_audit_reviewer_name,
            "retained_alias_audit_review_cache_path": self.retained_alias_audit_review_cache_path,
            "retained_alias_audit_invoked_review_count": self.retained_alias_audit_invoked_review_count,
            "retained_alias_audit_cached_review_count": self.retained_alias_audit_cached_review_count,
            "retained_alias_exclusion_removed_alias_count": self.retained_alias_exclusion_removed_alias_count,
            "retained_alias_exclusion_source_path": self.retained_alias_exclusion_source_path,
        }


@dataclass(frozen=True)
class RetainedAliasReviewDecision:
    """One keep/drop verdict for a retained general-en alias."""

    decision: str
    reason_code: str
    reason: str


def build_alias_candidate(
    *,
    alias_key: str,
    display_text: str,
    label: str,
    canonical_text: str,
    record: dict[str, Any],
    generated: bool = False,
) -> AliasCandidate:
    """Normalize one alias candidate into the canonical scoring contract."""

    lane = _resolve_alias_lane(display_text=display_text, label=label, record=record)
    features = _resolve_score_features(
        display_text=display_text,
        canonical_text=canonical_text,
        label=label,
        lane=lane,
        record=record,
    )
    return AliasCandidate(
        alias_key=alias_key,
        display_text=display_text,
        label=label,
        canonical_text=canonical_text,
        entity_id=_clean_text(record.get("entity_id")) or f"{label}:{canonical_text}",
        source_name=_clean_text(record.get("source_name")) or "unknown-source",
        source_id=_clean_text(record.get("source_id")) or canonical_text,
        lane=lane,
        generated=generated,
        score=_weighted_authority_score(features),
        features=features,
    )


def analyze_alias_candidates(
    candidates: Iterable[AliasCandidate],
    *,
    allowed_ambiguous_aliases: set[str],
    analysis_db_path: Path | None = None,
    top_cluster_limit: int = DEFAULT_ALIAS_ANALYSIS_TOP_CLUSTERS,
    materialize_retained_aliases: bool = True,
) -> AliasAnalysisResult:
    """Score and resolve alias candidates using an indexed SQLite analysis store."""

    if analysis_db_path is not None:
        analysis_db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(analysis_db_path) if analysis_db_path is not None else ":memory:")
    try:
        _configure_analysis_connection(
            connection,
            file_backed=analysis_db_path is not None,
        )
        _initialize_analysis_store(connection)
        candidate_alias_count = 0
        exact_identifier_alias_count = 0
        natural_language_alias_count = 0
        buffered_candidates: list[AliasCandidate] = []

        for candidate in candidates:
            buffered_candidates.append(candidate)
            candidate_alias_count += 1
            if candidate.lane == "exact":
                exact_identifier_alias_count += 1
            else:
                natural_language_alias_count += 1
            if len(buffered_candidates) >= 10_000:
                _insert_candidates(connection, buffered_candidates)
                buffered_candidates.clear()
        if buffered_candidates:
            _insert_candidates(connection, buffered_candidates)
        _finalize_analysis_store(connection)
        connection.commit()
        return _resolve_candidate_store(
            connection,
            allowed_ambiguous_aliases=allowed_ambiguous_aliases,
            analysis_db_path=analysis_db_path,
            candidate_alias_count=candidate_alias_count,
            exact_identifier_alias_count=exact_identifier_alias_count,
            natural_language_alias_count=natural_language_alias_count,
            top_cluster_limit=top_cluster_limit,
            materialize_retained_aliases=materialize_retained_aliases,
        )
    finally:
        connection.close()


def analyze_alias_candidates_from_db(
    source_analysis_db_path: str | Path,
    *,
    allowed_ambiguous_aliases: set[str],
    analysis_db_path: Path | None = None,
    top_cluster_limit: int = DEFAULT_ALIAS_ANALYSIS_TOP_CLUSTERS,
    materialize_retained_aliases: bool = True,
) -> AliasAnalysisResult:
    """Re-resolve aliases from an existing candidate-store SQLite database."""

    resolved_source_path = Path(source_analysis_db_path).expanduser().resolve()
    if not resolved_source_path.exists():
        raise FileNotFoundError(
            f"Alias analysis source database not found: {resolved_source_path}"
        )
    if analysis_db_path is not None:
        analysis_db_path = analysis_db_path.expanduser().resolve()
        analysis_db_path.parent.mkdir(parents=True, exist_ok=True)
        if analysis_db_path == resolved_source_path:
            raise ValueError("analysis_db_path must differ from source_analysis_db_path.")
    connection = sqlite3.connect(str(analysis_db_path) if analysis_db_path is not None else ":memory:")
    try:
        _configure_analysis_connection(
            connection,
            file_backed=analysis_db_path is not None,
        )
        _initialize_analysis_store(connection)
        connection.execute("ATTACH DATABASE ? AS source_db", (str(resolved_source_path),))
        try:
            counts = connection.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(CASE WHEN lane = 'exact' THEN 1 ELSE 0 END), 0)
                FROM source_db.alias_candidates
                """
            ).fetchone()
            if counts is None:
                candidate_alias_count = 0
                exact_identifier_alias_count = 0
            else:
                candidate_alias_count = int(counts[0])
                exact_identifier_alias_count = int(counts[1])
            natural_language_alias_count = (
                candidate_alias_count - exact_identifier_alias_count
            )
            connection.execute(
                """
                INSERT INTO alias_candidates (
                    alias_key,
                    display_text,
                    label,
                    canonical_text,
                    entity_id,
                    source_name,
                    source_id,
                    lane,
                    generated,
                    score,
                    source_priority,
                    status_weight,
                    preferred_name_weight,
                    popularity_weight,
                    identifier_specificity_weight,
                    label_specificity_weight,
                    alias_form_penalty
                )
                SELECT
                    alias_key,
                    display_text,
                    label,
                    canonical_text,
                    entity_id,
                    source_name,
                    source_id,
                    lane,
                    generated,
                    score,
                    source_priority,
                    status_weight,
                    preferred_name_weight,
                    popularity_weight,
                    identifier_specificity_weight,
                    label_specificity_weight,
                    alias_form_penalty
                FROM source_db.alias_candidates
                """
            )
            connection.commit()
        finally:
            connection.execute("DETACH DATABASE source_db")
        _finalize_analysis_store(connection)
        connection.commit()
        return _resolve_candidate_store(
            connection,
            allowed_ambiguous_aliases=allowed_ambiguous_aliases,
            analysis_db_path=analysis_db_path,
            candidate_alias_count=candidate_alias_count,
            exact_identifier_alias_count=exact_identifier_alias_count,
            natural_language_alias_count=natural_language_alias_count,
            top_cluster_limit=top_cluster_limit,
            materialize_retained_aliases=materialize_retained_aliases,
        )
    finally:
        connection.close()


def write_alias_analysis_report(path: Path, result: AliasAnalysisResult) -> None:
    """Persist the alias-analysis report as JSON."""

    path.write_text(json.dumps(result.to_dict(), indent=2) + "\n", encoding="utf-8")


def _resolve_cluster(
    *,
    alias_key: str,
    candidate_count: int,
    candidates: list[AliasClusterCandidate],
    allowed_ambiguous_aliases: set[str],
) -> AliasClusterReport:
    ordered_candidates = sorted(
        candidates,
        key=lambda candidate: (
            -candidate.score,
            candidate.label.casefold(),
            candidate.display_text.casefold(),
            candidate.display_text,
        ),
    )
    label_count = len(ordered_candidates)
    lane = "exact" if ordered_candidates and all(
        candidate.lane == "exact" for candidate in ordered_candidates
    ) else "natural"
    top_score = ordered_candidates[0].score if ordered_candidates else None
    runner_up_score = ordered_candidates[1].score if len(ordered_candidates) > 1 else None

    if alias_key in allowed_ambiguous_aliases:
        return AliasClusterReport(
            alias_key=alias_key,
            lane=lane,
            candidate_count=candidate_count,
            label_count=label_count,
            ambiguous=False,
            retained_labels=[candidate.label for candidate in ordered_candidates],
            blocked_labels=[],
            reason="allowed_ambiguous_alias",
            top_score=top_score,
            runner_up_score=runner_up_score,
            candidates=ordered_candidates,
        )

    if label_count <= 1:
        if ordered_candidates and _should_block_single_label_natural_alias(
            alias_key=alias_key,
            candidate=ordered_candidates[0],
        ):
            return AliasClusterReport(
                alias_key=alias_key,
                lane=lane,
                candidate_count=candidate_count,
                label_count=label_count,
                ambiguous=True,
                retained_labels=[],
                blocked_labels=[candidate.label for candidate in ordered_candidates],
                reason="low_information_single_label_alias",
                top_score=top_score,
                runner_up_score=runner_up_score,
                candidates=ordered_candidates,
            )
        return AliasClusterReport(
            alias_key=alias_key,
            lane=lane,
            candidate_count=candidate_count,
            label_count=label_count,
            ambiguous=False,
            retained_labels=[candidate.label for candidate in ordered_candidates],
            blocked_labels=[],
            reason="single_label_cluster",
            top_score=top_score,
            runner_up_score=runner_up_score,
            candidates=ordered_candidates,
        )

    top_candidate = ordered_candidates[0]
    runner_up_candidate = ordered_candidates[1]
    score_gap = round(top_candidate.score - runner_up_candidate.score, 4)

    if lane == "exact":
        if top_candidate.score >= 0.72 and score_gap >= 0.12:
            return AliasClusterReport(
                alias_key=alias_key,
                lane=lane,
                candidate_count=candidate_count,
                label_count=label_count,
                ambiguous=False,
                retained_labels=[top_candidate.label],
                blocked_labels=[candidate.label for candidate in ordered_candidates[1:]],
                reason="lower_authority_exact_identifier",
                top_score=top_score,
                runner_up_score=runner_up_score,
                candidates=ordered_candidates,
            )
        return AliasClusterReport(
            alias_key=alias_key,
            lane=lane,
            candidate_count=candidate_count,
            label_count=label_count,
            ambiguous=True,
            retained_labels=[],
            blocked_labels=[candidate.label for candidate in ordered_candidates],
            reason="exact_identifier_conflict",
            top_score=top_score,
            runner_up_score=runner_up_score,
            candidates=ordered_candidates,
        )

    short_geopolitical_candidate = _select_short_geopolitical_candidate(
        alias_key=alias_key,
        candidates=ordered_candidates,
    )
    if short_geopolitical_candidate is not None:
        return AliasClusterReport(
            alias_key=alias_key,
            lane=lane,
            candidate_count=candidate_count,
            label_count=label_count,
            ambiguous=False,
            retained_labels=[short_geopolitical_candidate.label],
            blocked_labels=[
                candidate.label
                for candidate in ordered_candidates
                if candidate.label != short_geopolitical_candidate.label
            ],
            reason="strong_short_geopolitical_alias",
            top_score=top_score,
            runner_up_score=runner_up_score,
            candidates=ordered_candidates,
        )
    if _is_short_token_like(alias_key):
        return AliasClusterReport(
            alias_key=alias_key,
            lane=lane,
            candidate_count=candidate_count,
            label_count=label_count,
            ambiguous=True,
            retained_labels=[],
            blocked_labels=[candidate.label for candidate in ordered_candidates],
            reason="short_cross_label_conflict",
            top_score=top_score,
            runner_up_score=runner_up_score,
            candidates=ordered_candidates,
        )
    if _is_generic_single_token_cluster(ordered_candidates):
        return AliasClusterReport(
            alias_key=alias_key,
            lane=lane,
            candidate_count=candidate_count,
            label_count=label_count,
            ambiguous=True,
            retained_labels=[],
            blocked_labels=[candidate.label for candidate in ordered_candidates],
            reason="common_word_conflict",
            top_score=top_score,
            runner_up_score=runner_up_score,
            candidates=ordered_candidates,
        )
    if _should_keep_dominant_location_alias(
        top_candidate=top_candidate,
        runner_up_candidate=runner_up_candidate,
    ):
        return AliasClusterReport(
            alias_key=alias_key,
            lane=lane,
            candidate_count=candidate_count,
            label_count=label_count,
            ambiguous=False,
            retained_labels=[top_candidate.label],
            blocked_labels=[candidate.label for candidate in ordered_candidates[1:]],
            reason="dominant_location_alias",
            top_score=top_score,
            runner_up_score=runner_up_score,
            candidates=ordered_candidates,
        )
    supported_location_candidate = _select_supported_location_candidate(
        ordered_candidates
    )
    if supported_location_candidate is not None:
        return AliasClusterReport(
            alias_key=alias_key,
            lane=lane,
            candidate_count=candidate_count,
            label_count=label_count,
            ambiguous=False,
            retained_labels=[supported_location_candidate.label],
            blocked_labels=[
                candidate.label
                for candidate in ordered_candidates
                if candidate.label != supported_location_candidate.label
            ],
            reason="supported_location_over_generated_competitors",
            top_score=top_score,
            runner_up_score=runner_up_score,
            candidates=ordered_candidates,
        )
    family_preferred_location_candidate = _select_family_preferred_location_candidate(
        alias_key=alias_key,
        candidates=ordered_candidates,
    )
    if family_preferred_location_candidate is not None:
        return AliasClusterReport(
            alias_key=alias_key,
            lane=lane,
            candidate_count=candidate_count,
            label_count=label_count,
            ambiguous=False,
            retained_labels=[family_preferred_location_candidate.label],
            blocked_labels=[
                candidate.label
                for candidate in ordered_candidates
                if candidate.label != family_preferred_location_candidate.label
            ],
            reason="family_preferred_location_alias",
            top_score=top_score,
            runner_up_score=runner_up_score,
            candidates=ordered_candidates,
        )
    if top_candidate.score >= 0.58 and score_gap >= 0.05:
        return AliasClusterReport(
            alias_key=alias_key,
            lane=lane,
            candidate_count=candidate_count,
            label_count=label_count,
            ambiguous=False,
            retained_labels=[top_candidate.label],
            blocked_labels=[candidate.label for candidate in ordered_candidates[1:]],
            reason="lower_authority_cross_label",
            top_score=top_score,
            runner_up_score=runner_up_score,
            candidates=ordered_candidates,
        )
    return AliasClusterReport(
        alias_key=alias_key,
        lane=lane,
        candidate_count=candidate_count,
        label_count=label_count,
        ambiguous=True,
        retained_labels=[],
        blocked_labels=[candidate.label for candidate in ordered_candidates],
        reason="ambiguous_natural_language_alias",
        top_score=top_score,
        runner_up_score=runner_up_score,
        candidates=ordered_candidates,
    )


def _resolve_candidate_store(
    connection: sqlite3.Connection,
    *,
    allowed_ambiguous_aliases: set[str],
    analysis_db_path: Path | None,
    candidate_alias_count: int,
    exact_identifier_alias_count: int,
    natural_language_alias_count: int,
    top_cluster_limit: int,
    materialize_retained_aliases: bool,
) -> AliasAnalysisResult:
    retain_aliases_in_memory = materialize_retained_aliases or analysis_db_path is None
    retained_aliases: list[dict[str, Any]] = []
    retained_alias_rows: list[
        tuple[str, str, str, str, int, float, str, str, str, float, float, str]
    ] = []
    retained_label_counts: dict[str, int] = {}
    blocked_reason_counts: dict[str, int] = {}
    blocked_alias_count = 0
    ambiguous_alias_count = 0
    retained_alias_count = 0
    top_clusters: list[AliasClusterReport] = []

    def flush_retained_alias_rows() -> None:
        nonlocal retained_alias_rows
        if not retained_alias_rows:
            return
        _insert_retained_aliases(connection, retained_alias_rows)
        retained_alias_rows = []

    for alias_key, candidate_count in connection.execute(
        """
        SELECT alias_key, COUNT(*)
        FROM alias_candidates
        GROUP BY alias_key
        ORDER BY alias_key ASC
        """
    ):
        rows = _load_cluster_candidates(connection, alias_key)
        cluster = _resolve_cluster(
            alias_key=alias_key,
            candidate_count=int(candidate_count),
            candidates=rows,
            allowed_ambiguous_aliases=allowed_ambiguous_aliases,
        )
        if cluster.ambiguous:
            ambiguous_alias_count += 1
        if cluster.label_count > 1:
            top_clusters.append(cluster)
            top_clusters.sort(
                key=lambda item: (
                    not item.ambiguous,
                    -item.label_count,
                    -item.candidate_count,
                    item.alias_key,
                )
            )
            if len(top_clusters) > top_cluster_limit:
                top_clusters = top_clusters[:top_cluster_limit]
        for label in cluster.retained_labels:
            representative = next(
                candidate for candidate in cluster.candidates if candidate.label == label
            )
            retained_alias_count += 1
            retained_label_counts[representative.label] = (
                retained_label_counts.get(representative.label, 0) + 1
            )
            runtime_tier = _resolve_runtime_tier(
                alias_key=alias_key,
                candidate=representative,
            )
            if retain_aliases_in_memory:
                retained_aliases.append(
                    {
                        "text": representative.display_text,
                        "label": representative.label,
                        "generated": representative.generated,
                        "score": round(representative.score, 4),
                        "canonical_text": representative.canonical_text,
                        "source_name": representative.source_name,
                        "entity_id": representative.entity_id,
                        "source_priority": round(
                            representative.features.source_priority,
                            4,
                        ),
                        "popularity_weight": round(
                            representative.features.popularity_weight,
                            4,
                        ),
                        "runtime_tier": runtime_tier,
                    }
                )
            else:
                retained_alias_rows.append(
                    (
                        representative.display_text,
                        representative.label,
                        representative.display_text.casefold(),
                        representative.label.casefold(),
                        1 if representative.generated else 0,
                        round(representative.score, 4),
                        representative.canonical_text,
                        representative.source_name,
                        representative.entity_id,
                        round(representative.features.source_priority, 4),
                        round(representative.features.popularity_weight, 4),
                        runtime_tier,
                    )
                )
                if len(retained_alias_rows) >= 20_000:
                    flush_retained_alias_rows()
        if cluster.reason is not None and cluster.blocked_labels:
            blocked_reason_counts[cluster.reason] = (
                blocked_reason_counts.get(cluster.reason, 0) + len(cluster.blocked_labels)
            )
            blocked_alias_count += len(cluster.blocked_labels)
        _insert_cluster_decision(connection, cluster)

    flush_retained_alias_rows()
    connection.commit()
    if retain_aliases_in_memory:
        ordered_aliases = sorted(
            retained_aliases,
            key=lambda item: (
                item["text"].casefold(),
                item["text"],
                item["label"].casefold(),
                item["label"],
            ),
        )
    else:
        ordered_aliases = []
    ordered_blocked_reason_counts = {
        reason: blocked_reason_counts[reason]
        for reason in sorted(blocked_reason_counts, key=lambda value: (value.casefold(), value))
    }
    ordered_retained_label_counts = {
        label: retained_label_counts[label]
        for label in sorted(retained_label_counts, key=lambda value: (value.casefold(), value))
    }
    return AliasAnalysisResult(
        backend=DEFAULT_ALIAS_ANALYSIS_BACKEND,
        database_path=str(analysis_db_path) if analysis_db_path is not None else None,
        candidate_alias_count=candidate_alias_count,
        retained_alias_count=retained_alias_count,
        blocked_alias_count=blocked_alias_count,
        ambiguous_alias_count=ambiguous_alias_count,
        exact_identifier_alias_count=exact_identifier_alias_count,
        natural_language_alias_count=natural_language_alias_count,
        blocked_reason_counts=ordered_blocked_reason_counts,
        retained_label_counts=ordered_retained_label_counts,
        retained_aliases_materialized=retain_aliases_in_memory,
        retained_aliases=ordered_aliases,
        top_collision_clusters=top_clusters,
    )


def _initialize_analysis_store(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS alias_candidates (
            alias_key TEXT NOT NULL,
            display_text TEXT NOT NULL,
            label TEXT NOT NULL,
            canonical_text TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_id TEXT NOT NULL,
            lane TEXT NOT NULL,
            generated INTEGER NOT NULL DEFAULT 0,
            score REAL NOT NULL,
            source_priority REAL NOT NULL,
            status_weight REAL NOT NULL,
            preferred_name_weight REAL NOT NULL,
            popularity_weight REAL NOT NULL,
            identifier_specificity_weight REAL NOT NULL,
            label_specificity_weight REAL NOT NULL,
            alias_form_penalty REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS alias_cluster_decisions (
            alias_key TEXT NOT NULL,
            lane TEXT NOT NULL,
            candidate_count INTEGER NOT NULL,
            label_count INTEGER NOT NULL,
            ambiguous INTEGER NOT NULL,
            retained_labels_json TEXT NOT NULL,
            blocked_labels_json TEXT NOT NULL,
            reason TEXT,
            top_score REAL,
            runner_up_score REAL
        );
        CREATE TABLE IF NOT EXISTS retained_aliases (
            display_text TEXT NOT NULL,
            label TEXT NOT NULL,
            display_sort_key TEXT NOT NULL,
            label_sort_key TEXT NOT NULL,
            generated INTEGER NOT NULL DEFAULT 0,
            score REAL NOT NULL DEFAULT 1.0,
            canonical_text TEXT NOT NULL DEFAULT '',
            source_name TEXT NOT NULL DEFAULT '',
            entity_id TEXT NOT NULL DEFAULT '',
            source_priority REAL NOT NULL DEFAULT 0.6,
            popularity_weight REAL NOT NULL DEFAULT 0.5,
            runtime_tier TEXT NOT NULL DEFAULT 'runtime_exact_high_precision'
        );
        DELETE FROM alias_cluster_decisions;
        DELETE FROM retained_aliases;
        DELETE FROM alias_candidates;
        """
    )


def _configure_analysis_connection(
    connection: sqlite3.Connection,
    *,
    file_backed: bool,
) -> None:
    """Relax durability for the disposable analysis store to reduce write amplification."""

    connection.execute("PRAGMA temp_store = MEMORY")
    connection.execute("PRAGMA cache_size = -262144")
    connection.execute("PRAGMA mmap_size = 268435456")
    if not file_backed:
        return
    connection.execute("PRAGMA journal_mode = MEMORY")
    connection.execute("PRAGMA synchronous = OFF")
    connection.execute("PRAGMA locking_mode = EXCLUSIVE")
    connection.execute("PRAGMA journal_size_limit = 0")


def _finalize_analysis_store(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_alias_candidates_alias_key
            ON alias_candidates(alias_key);
        CREATE INDEX IF NOT EXISTS idx_alias_candidates_label
            ON alias_candidates(alias_key, label);
        """
    )


def _insert_candidates(
    connection: sqlite3.Connection,
    candidates: list[AliasCandidate],
) -> None:
    connection.executemany(
        """
        INSERT INTO alias_candidates (
            alias_key,
            display_text,
            label,
            canonical_text,
            entity_id,
            source_name,
            source_id,
            lane,
            generated,
            score,
            source_priority,
            status_weight,
            preferred_name_weight,
            popularity_weight,
            identifier_specificity_weight,
            label_specificity_weight,
            alias_form_penalty
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                candidate.alias_key,
                candidate.display_text,
                candidate.label,
                candidate.canonical_text,
                candidate.entity_id,
                candidate.source_name,
                candidate.source_id,
                candidate.lane,
                1 if candidate.generated else 0,
                candidate.score,
                candidate.features.source_priority,
                candidate.features.status_weight,
                candidate.features.preferred_name_weight,
                candidate.features.popularity_weight,
                candidate.features.identifier_specificity_weight,
                candidate.features.label_specificity_weight,
                candidate.features.alias_form_penalty,
            )
            for candidate in candidates
        ],
    )


def _load_cluster_candidates(
    connection: sqlite3.Connection,
    alias_key: str,
) -> list[AliasClusterCandidate]:
    rows = connection.execute(
        """
        SELECT
            label,
            display_text,
            canonical_text,
            lane,
            generated,
            score,
            source_priority,
            status_weight,
            preferred_name_weight,
            popularity_weight,
            identifier_specificity_weight,
            label_specificity_weight,
            alias_form_penalty,
            source_name,
            source_id,
            entity_id
        FROM alias_candidates
        WHERE alias_key = ?
        ORDER BY
            label ASC,
            score DESC,
            preferred_name_weight DESC,
            length(display_text) ASC,
            display_text ASC
        """,
        (alias_key,),
    ).fetchall()
    label_rows: dict[str, list[tuple[Any, ...]]] = {}
    label_candidate_counts: dict[str, int] = {}
    label_non_generated_counts: dict[str, int] = {}
    for row in rows:
        label = str(row[0])
        label_rows.setdefault(label, []).append(row)
        label_candidate_counts[label] = label_candidate_counts.get(label, 0) + 1
        if not bool(row[4]):
            label_non_generated_counts[label] = label_non_generated_counts.get(label, 0) + 1
    best_by_label: dict[str, AliasClusterCandidate] = {}
    for label, label_specific_rows in label_rows.items():
        cluster_candidates = [
            AliasClusterCandidate(
                label=label,
                display_text=str(row[1]),
                canonical_text=str(row[2]),
                lane=str(row[3]),
                generated=bool(row[4]),
                score=round(float(row[5]), 4),
                features=AliasScoreFeatures(
                    source_priority=round(float(row[6]), 4),
                    status_weight=round(float(row[7]), 4),
                    preferred_name_weight=round(float(row[8]), 4),
                    popularity_weight=round(float(row[9]), 4),
                    identifier_specificity_weight=round(float(row[10]), 4),
                    label_specificity_weight=round(float(row[11]), 4),
                    alias_form_penalty=round(float(row[12]), 4),
                ),
                source_name=str(row[13]),
                source_id=str(row[14]),
                entity_id=str(row[15]),
                label_candidate_count=label_candidate_counts.get(label, 1),
                non_generated_candidate_count=label_non_generated_counts.get(label, 0),
            )
            for row in label_specific_rows
        ]
        best_by_label[label] = _select_best_cluster_candidate(
            alias_key=alias_key,
            candidates=cluster_candidates,
        )
    return list(best_by_label.values())


def _select_best_cluster_candidate(
    *,
    alias_key: str,
    candidates: list[AliasClusterCandidate],
) -> AliasClusterCandidate:
    if not candidates:
        raise ValueError("Cluster candidate selection requires at least one candidate.")

    preferred_expansion = _select_preferred_acronym_expansion_candidate(
        alias_key=alias_key,
        candidates=candidates,
    )
    if preferred_expansion is not None:
        return preferred_expansion

    best_candidate = candidates[0]
    best_rank = _cluster_candidate_rank(alias_key=alias_key, candidate=best_candidate)
    for candidate in candidates[1:]:
        rank = _cluster_candidate_rank(alias_key=alias_key, candidate=candidate)
        if rank > best_rank:
            best_candidate = candidate
            best_rank = rank
            continue
        if rank == best_rank and (
            candidate.canonical_text.casefold(),
            candidate.canonical_text,
            candidate.entity_id,
        ) < (
            best_candidate.canonical_text.casefold(),
            best_candidate.canonical_text,
            best_candidate.entity_id,
        ):
            best_candidate = candidate
            best_rank = rank
    return best_candidate


def _select_preferred_acronym_expansion_candidate(
    *,
    alias_key: str,
    candidates: list[AliasClusterCandidate],
) -> AliasClusterCandidate | None:
    if not _is_compact_alpha_acronym(alias_key):
        return None
    exact_short_candidates = [
        candidate
        for candidate in candidates
        if _is_short_exact_canonical_acronym_candidate(
            alias_key=alias_key,
            candidate=candidate,
        )
    ]
    expansion_candidates = [
        candidate
        for candidate in candidates
        if _is_multi_token_acronym_expansion_candidate(
            alias_key=alias_key,
            candidate=candidate,
        )
    ]
    if not exact_short_candidates or not expansion_candidates:
        return None
    best_exact_short = max(
        exact_short_candidates,
        key=lambda candidate: _cluster_candidate_rank(
            alias_key=alias_key,
            candidate=candidate,
        ),
    )
    best_expansion = max(
        expansion_candidates,
        key=lambda candidate: _cluster_candidate_rank(
            alias_key=alias_key,
            candidate=candidate,
        ),
    )
    if _should_prefer_acronym_expansion_candidate(
        expansion_candidate=best_expansion,
        exact_short_candidate=best_exact_short,
    ):
        return best_expansion
    return None


def _cluster_candidate_rank(
    *,
    alias_key: str,
    candidate: AliasClusterCandidate,
) -> tuple[float, float, tuple[int, int, int, int], int, float, float, int, int]:
    exact_canonical = int(
        candidate.display_text.casefold() == candidate.canonical_text.casefold()
    )
    acronym_quality = _acronym_expansion_quality(
        alias_key=alias_key,
        candidate=candidate,
    )
    return (
        candidate.score,
        candidate.features.preferred_name_weight,
        acronym_quality,
        exact_canonical,
        candidate.features.popularity_weight,
        candidate.features.source_priority,
        int(not candidate.generated),
        len(candidate.canonical_text),
    )


def _acronym_expansion_quality(
    *,
    alias_key: str,
    candidate: AliasClusterCandidate,
) -> tuple[int, int, int, int]:
    compact = "".join(character for character in alias_key if character.isalnum()).upper()
    if candidate.label != "organization":
        return (0, 0, 0, 0)
    if not candidate.display_text.isupper():
        return (0, 0, 0, 0)
    if not (2 <= len(compact) <= 6):
        return (0, 0, 0, 0)
    if not compact.isalpha():
        return (0, 0, 0, 0)
    tokens = _acronym_source_tokens(candidate.canonical_text)
    if not tokens:
        return (0, 0, 0, 0)
    initials = "".join(token[0].upper() for token in tokens if token[0].isalpha())
    exact_initials = int(initials == compact)
    prefix_initials = int(bool(initials) and (initials.startswith(compact) or compact.startswith(initials)))
    titlecase_initials = int(
        exact_initials
        and len(tokens) >= len(compact)
        and all(token[0].isupper() for token in tokens[: len(compact)])
    )
    multi_token = int(len(tokens) >= 2)
    return (
        exact_initials,
        titlecase_initials,
        prefix_initials,
        multi_token,
    )


def _is_compact_alpha_acronym(alias_key: str) -> bool:
    compact = "".join(character for character in alias_key if character.isalnum())
    return 2 <= len(compact) <= 6 and compact.isalpha()


def _is_short_exact_canonical_acronym_candidate(
    *,
    alias_key: str,
    candidate: AliasClusterCandidate,
) -> bool:
    compact = "".join(character for character in alias_key if character.isalnum())
    canonical_compact = "".join(
        character for character in candidate.canonical_text if character.isalnum()
    )
    return (
        candidate.label == "organization"
        and candidate.display_text.isupper()
        and candidate.display_text.casefold() == candidate.canonical_text.casefold()
        and len(re.findall(r"[A-Za-z]+", candidate.canonical_text)) == 1
        and canonical_compact.upper() == compact.upper()
    )


def _is_multi_token_acronym_expansion_candidate(
    *,
    alias_key: str,
    candidate: AliasClusterCandidate,
) -> bool:
    acronym_quality = _acronym_expansion_quality(
        alias_key=alias_key,
        candidate=candidate,
    )
    return (
        candidate.label == "organization"
        and acronym_quality[0] == 1
        and acronym_quality[3] == 1
    )


def _should_prefer_acronym_expansion_candidate(
    *,
    expansion_candidate: AliasClusterCandidate,
    exact_short_candidate: AliasClusterCandidate,
) -> bool:
    if expansion_candidate.score < exact_short_candidate.score - 0.05:
        return False
    if (
        expansion_candidate.features.popularity_weight >= 0.8
        and exact_short_candidate.features.popularity_weight < 0.8
    ):
        return True
    if (
        expansion_candidate.features.popularity_weight
        >= exact_short_candidate.features.popularity_weight + 0.1
    ):
        return True
    if (
        expansion_candidate.features.source_priority
        > exact_short_candidate.features.source_priority + 0.05
    ):
        return True
    return False


def _select_short_geopolitical_candidate(
    *,
    alias_key: str,
    candidates: list[AliasClusterCandidate],
) -> AliasClusterCandidate | None:
    strong_locations = [
        candidate
        for candidate in candidates
        if _is_strong_short_geopolitical_candidate(
            alias_key=alias_key,
            candidate=candidate,
        )
        or _is_strong_geopolitical_acronym_candidate(
            alias_key=alias_key,
            candidate=candidate,
        )
    ]
    if not strong_locations:
        return None
    location_candidate = max(
        strong_locations,
        key=lambda candidate: _cluster_candidate_rank(
            alias_key=alias_key,
            candidate=candidate,
        ),
    )
    top_candidate = candidates[0]
    geopolitical_acronym = _is_strong_geopolitical_acronym_candidate(
        alias_key=alias_key,
        candidate=location_candidate,
    )
    score_gap_limit = 0.05
    if geopolitical_acronym:
        score_gap_limit = 0.12
        if (
            top_candidate.label != "location"
            and _is_short_exact_canonical_acronym_candidate(
                alias_key=alias_key,
                candidate=top_candidate,
            )
        ):
            score_gap_limit = 0.16
    if top_candidate.score - location_candidate.score > score_gap_limit:
        return None
    if (
        top_candidate.label != "location"
        and top_candidate.features.source_priority
        > location_candidate.features.source_priority + 0.02
        and top_candidate.score - location_candidate.score > 0.02
    ):
        return None
    return location_candidate


def _insert_cluster_decision(
    connection: sqlite3.Connection,
    cluster: AliasClusterReport,
) -> None:
    connection.execute(
        """
        INSERT INTO alias_cluster_decisions (
            alias_key,
            lane,
            candidate_count,
            label_count,
            ambiguous,
            retained_labels_json,
            blocked_labels_json,
            reason,
            top_score,
            runner_up_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cluster.alias_key,
            cluster.lane,
            cluster.candidate_count,
            cluster.label_count,
            1 if cluster.ambiguous else 0,
            json.dumps(cluster.retained_labels, sort_keys=True),
            json.dumps(cluster.blocked_labels, sort_keys=True),
            cluster.reason,
            cluster.top_score,
            cluster.runner_up_score,
        ),
    )


def _insert_retained_aliases(
    connection: sqlite3.Connection,
    rows: list[tuple[str, str, str, str, int, float, str, str, str, float, float, str]],
) -> None:
    connection.executemany(
        """
        INSERT INTO retained_aliases (
            display_text,
            label,
            display_sort_key,
            label_sort_key,
            generated,
            score,
            canonical_text,
            source_name,
            entity_id,
            source_priority,
            popularity_weight,
            runtime_tier
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def iter_retained_aliases_from_db(
    path: str | Path,
    *,
    chunk_size: int = DEFAULT_RETAINED_ALIAS_AUDIT_CHUNK_SIZE,
) -> Iterator[dict[str, Any]]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Alias analysis database not found: {resolved}")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive.")
    connection = sqlite3.connect(str(resolved))
    try:
        table_info = connection.execute("PRAGMA table_info(retained_aliases)").fetchall()
        available_columns = {str(row[1]) for row in table_info}
        select_columns = [
            "display_text",
            "label",
            "generated",
            "score",
            "canonical_text",
            "source_name",
            "entity_id",
            "source_priority",
            "popularity_weight",
        ]
        has_runtime_tier = "runtime_tier" in available_columns
        if has_runtime_tier:
            select_columns.append("runtime_tier")
        cursor = connection.execute(
            f"""
            SELECT
                {", ".join(select_columns)}
            FROM retained_aliases
            ORDER BY
                display_sort_key ASC,
                display_text ASC,
                label_sort_key ASC,
                label ASC
            """
        )
        while rows := cursor.fetchmany(chunk_size):
            for row in rows:
                (
                    display_text,
                    label,
                    generated,
                    score,
                    canonical_text,
                    source_name,
                    entity_id,
                    source_priority,
                    popularity_weight,
                    *optional_fields,
                ) = row
                runtime_tier = (
                    str(optional_fields[0])
                    if has_runtime_tier and optional_fields
                    else "runtime_exact_high_precision"
                )
                yield {
                    "text": str(display_text),
                    "label": str(label),
                    "generated": bool(generated),
                    "score": round(float(score), 4),
                    "canonical_text": str(canonical_text),
                    "source_name": str(source_name),
                    "entity_id": str(entity_id),
                    "source_priority": round(float(source_priority), 4),
                    "popularity_weight": round(float(popularity_weight), 4),
                    "runtime_tier": str(runtime_tier),
                }
    finally:
        connection.close()


def build_retained_alias_review_payload(alias: dict[str, Any]) -> dict[str, Any]:
    return {
        "text": str(alias.get("text", "")),
        "label": str(alias.get("label", "")),
        "canonical_text": str(alias.get("canonical_text", "")),
        "entity_id": str(alias.get("entity_id", "")),
        "source_name": str(alias.get("source_name", "")),
        "generated": bool(alias.get("generated", False)),
        "score": float(alias.get("score", 0.0)),
        "source_priority": float(alias.get("source_priority", 0.0)),
        "popularity_weight": float(alias.get("popularity_weight", 0.0)),
        "runtime_tier": str(alias.get("runtime_tier", "")),
    }


def build_retained_alias_review_key(alias: dict[str, Any]) -> str:
    key_payload = {
        "text": str(alias.get("text", "")),
        "label": str(alias.get("label", "")),
        "canonical_text": str(alias.get("canonical_text", "")),
        "entity_id": str(alias.get("entity_id", "")),
        "source_name": str(alias.get("source_name", "")),
    }
    serialized = json.dumps(
        key_payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _resolve_alias_lane(
    *,
    display_text: str,
    label: str,
    record: dict[str, Any],
) -> str:
    if bool(record.get("exact_match_only")):
        return "exact"
    if _clean_text(record.get("identifier_family")):
        return "exact"
    label_key = label.casefold()
    if label_key in _EXACT_IDENTIFIER_LABELS:
        return "exact"
    compact = "".join(character for character in display_text if character.isalnum())
    if display_text.startswith("NCT") and compact[3:].isdigit():
        return "exact"
    if label_key == "ticker" and compact.isalnum() and 1 <= len(compact) <= 6:
        return "exact"
    return "natural"


def _resolve_score_features(
    *,
    display_text: str,
    canonical_text: str,
    label: str,
    lane: str,
    record: dict[str, Any],
) -> AliasScoreFeatures:
    label_key = label.casefold()
    exact_display_match = display_text == canonical_text
    normalized_display_match = display_text.casefold() == canonical_text.casefold()
    return AliasScoreFeatures(
        source_priority=_resolve_feature(
            record.get("source_priority"),
            default=_default_source_priority(_clean_text(record.get("source_name"))),
        ),
        status_weight=_resolve_feature(
            record.get("status_weight"),
            default=_default_status_weight(record),
        ),
        preferred_name_weight=_resolve_feature(
            record.get("preferred_name_weight"),
            default=_default_preferred_name_weight(
                display_text,
                canonical_text=canonical_text,
                label=label_key,
                exact_display_match=exact_display_match,
                normalized_display_match=normalized_display_match,
            ),
        ),
        popularity_weight=_resolve_feature(
            record.get("popularity_weight"),
            default=_default_popularity_weight(record),
        ),
        identifier_specificity_weight=_resolve_feature(
            record.get("identifier_specificity_weight"),
            default=1.0 if lane == "exact" else 0.45,
        ),
        label_specificity_weight=_resolve_feature(
            record.get("label_specificity_weight"),
            default=_LABEL_SPECIFICITY_WEIGHTS.get(label_key, 0.65),
        ),
        alias_form_penalty=_resolve_feature(
            record.get("alias_form_penalty"),
            default=_default_alias_form_penalty(
                display_text,
                label=label_key,
                exact_display_match=exact_display_match,
            ),
        ),
    )


def _weighted_authority_score(features: AliasScoreFeatures) -> float:
    raw_score = (
        features.source_priority * 0.24
        + features.status_weight * 0.12
        + features.preferred_name_weight * 0.16
        + features.popularity_weight * 0.14
        + features.identifier_specificity_weight * 0.16
        + features.label_specificity_weight * 0.18
        - features.alias_form_penalty * 0.18
    )
    return round(max(0.0, min(1.0, raw_score)), 4)


def _default_source_priority(source_name: str | None) -> float:
    if not source_name:
        return 0.6
    source_key = source_name.casefold()
    for prefix, priority in _DEFAULT_SOURCE_PRIORITIES.items():
        if prefix in source_key:
            return priority
    return 0.65


def _default_status_weight(record: dict[str, Any]) -> float:
    raw_status = _clean_text(record.get("status")) or _clean_text(record.get("state"))
    if not raw_status:
        return 0.5
    status_key = raw_status.casefold()
    if status_key in {"active", "approved", "current", "live", "trading"}:
        return 1.0
    if status_key in {"inactive", "blocked", "delisted", "deprecated", "retired"}:
        return 0.2
    return 0.55


def _default_popularity_weight(record: dict[str, Any]) -> float:
    for field_name in ("popularity", "population", "market_cap_rank", "record_rank"):
        raw_value = record.get(field_name)
        if raw_value is None:
            continue
        try:
            numeric = float(raw_value)
        except (TypeError, ValueError):
            continue
        if field_name == "population":
            if numeric <= 0:
                return 0.2
            return max(0.2, min(1.0, log10(max(numeric, 1.0)) / 7.0))
        if field_name.endswith("_rank"):
            return max(0.2, min(1.0, 1.0 - min(numeric, 1000.0) / 1000.0))
        return max(0.0, min(1.0, numeric))
    return 0.5


def _default_preferred_name_weight(
    display_text: str,
    *,
    canonical_text: str,
    label: str,
    exact_display_match: bool,
    normalized_display_match: bool,
) -> float:
    if label == "person" and _is_single_token_alpha(display_text):
        if exact_display_match:
            return 0.62
        if normalized_display_match or display_text.casefold() == canonical_text.casefold():
            return 0.7
    if label == "location" and _is_single_token_alpha(display_text):
        if exact_display_match:
            return 0.58
        if normalized_display_match or display_text.casefold() == canonical_text.casefold():
            return 0.68
    if exact_display_match:
        return 1.0
    if normalized_display_match:
        return 0.85
    return 0.6


def _default_alias_form_penalty(
    display_text: str,
    *,
    label: str,
    exact_display_match: bool,
) -> float:
    penalty = 0.0
    compact = "".join(character for character in display_text if character.isalnum())
    if len(compact) <= 2:
        penalty += 0.35
    elif len(compact) <= 4:
        penalty += 0.18
    if display_text.isupper() and len(compact) <= 6 and not exact_display_match:
        penalty += 0.1
    if "." in display_text or "@" in display_text or "/" in display_text:
        penalty += 0.08
    if _is_generic_single_token_like(display_text):
        penalty += 0.12 if exact_display_match else 0.18
    if label == "location" and _is_single_token_alpha(display_text):
        penalty += 0.24 if exact_display_match else 0.14
    return max(0.0, min(1.0, penalty))


def _resolve_feature(raw_value: Any, *, default: float) -> float:
    try:
        numeric = float(raw_value) if raw_value is not None else default
    except (TypeError, ValueError):
        numeric = default
    return round(max(0.0, min(1.0, numeric)), 4)


def _is_short_token_like(alias_key: str) -> bool:
    compact = "".join(character for character in alias_key if character.isalnum())
    return len(compact) <= 4 or (compact.isupper() and len(compact) <= 6)


def _is_generic_single_token_like(alias_key: str) -> bool:
    tokens = [
        token
        for token in re.split(r"[\s\-_./]+", alias_key.strip())
        if token
    ]
    if len(tokens) != 1:
        return False
    token = tokens[0]
    if not token.isalpha():
        return False
    if token.isupper():
        return False
    if any(character.isupper() for character in token):
        return False
    return token.islower() and len(token) >= 5


def _is_generic_single_token_cluster(candidates: list[AliasClusterCandidate]) -> bool:
    if not candidates:
        return False
    return all(_is_generic_single_token_like(candidate.display_text) for candidate in candidates)


def _is_strong_runtime_acronym_candidate(
    *,
    alias_key: str,
    candidate: AliasClusterCandidate,
) -> bool:
    compact = "".join(character for character in alias_key if character.isalnum())
    acronym_quality = _acronym_expansion_quality(
        alias_key=alias_key,
        candidate=candidate,
    )
    exact_canonical = (
        candidate.display_text.casefold() == candidate.canonical_text.casefold()
    )
    if candidate.label != "organization":
        return False
    if not (3 <= len(compact) <= 5):
        return False
    if not compact.isalpha():
        return False
    if alias_key in _RUNTIME_GENERIC_PHRASE_LEADS:
        return False
    if not candidate.display_text.isupper():
        return False
    if not exact_canonical and acronym_quality[0] == 0:
        return False
    return (
        candidate.features.source_priority >= 0.8
        and candidate.score >= 0.55
        and (
            (
                exact_canonical
                and candidate.non_generated_candidate_count >= 1
                and candidate.features.popularity_weight >= 0.8
            )
            or (
                acronym_quality[0] == 1
                and (
                    candidate.features.popularity_weight >= 0.8
                    or acronym_quality[1] == 1
                )
            )
        )
    )


def _is_strong_short_geopolitical_candidate(
    *,
    alias_key: str,
    candidate: AliasClusterCandidate,
) -> bool:
    if _is_blocklisted_runtime_single_token(alias_key):
        return False
    compact = "".join(character for character in alias_key if character.isalnum())
    if candidate.label != "location":
        return False
    if not (2 <= len(compact) <= 4):
        return False
    if not _is_single_token_alpha(alias_key):
        return False
    if candidate.generated:
        return False
    if candidate.display_text.casefold() != candidate.canonical_text.casefold():
        return False
    return (
        candidate.features.source_priority >= 0.8
        and candidate.features.popularity_weight >= 0.75
        and candidate.score >= 0.6
    )


def _is_strong_geopolitical_acronym_candidate(
    *,
    alias_key: str,
    candidate: AliasClusterCandidate,
) -> bool:
    compact = "".join(character for character in alias_key if character.isalnum()).upper()
    if candidate.label != "location":
        return False
    if not (2 <= len(compact) <= 4):
        return False
    if not compact.isalpha():
        return False
    if not candidate.display_text.isupper():
        return False
    tokens = _acronym_source_tokens(candidate.canonical_text)
    if len(tokens) < 2:
        return False
    initials = "".join(token[0].upper() for token in tokens if token[:1].isalpha())
    if initials != compact:
        return False
    return (
        candidate.features.source_priority >= 0.8
        and candidate.features.popularity_weight >= 0.5
        and candidate.score >= 0.55
    )


def _is_blocklisted_runtime_single_token(alias_key: str) -> bool:
    normalized_alias = alias_key.strip().casefold()
    if not normalized_alias or not _is_single_token_alpha(alias_key):
        return False
    return (
        normalized_alias in _RUNTIME_GENERIC_PHRASE_LEADS
        or normalized_alias in _RUNTIME_AUXILIARY_WORDS
        or normalized_alias in _RUNTIME_CALENDAR_SINGLE_TOKEN_WORDS
        or normalized_alias in _RUNTIME_NUMBER_SINGLE_TOKEN_WORDS
        or normalized_alias in _RUNTIME_GENERIC_SINGLE_TOKEN_HEADS
    )


def _acronym_source_tokens(value: str) -> list[str]:
    return [
        token
        for token in re.findall(r"[A-Za-z]+", value)
        if token.casefold() not in _ACRONYM_CONNECTOR_TOKENS
    ]


def _should_block_single_label_natural_alias(
    *,
    alias_key: str,
    candidate: AliasClusterCandidate,
) -> bool:
    if candidate.lane != "natural":
        return False
    if _is_strong_runtime_acronym_candidate(alias_key=alias_key, candidate=candidate):
        return False
    if _is_strong_short_geopolitical_candidate(
        alias_key=alias_key,
        candidate=candidate,
    ):
        return False
    if _is_strong_geopolitical_acronym_candidate(
        alias_key=alias_key,
        candidate=candidate,
    ):
        return False
    if _is_blocklisted_runtime_single_token(alias_key):
        return True
    if _is_single_token_alpha(alias_key):
        if candidate.label == "person":
            return True
        if (
            candidate.label in {"person", "location"}
            and candidate.display_text.casefold() != candidate.canonical_text.casefold()
        ):
            return True
        if (
            candidate.label == "location"
            and candidate.features.popularity_weight <= 0.3
        ):
            return True
        if (
            candidate.label == "organization"
            and candidate.display_text.casefold() != candidate.canonical_text.casefold()
            and not candidate.display_text.isupper()
        ):
            return candidate.score < 0.7
    if not _is_short_token_like(alias_key):
        return False
    return candidate.score < 0.7


def _select_family_preferred_location_candidate(
    *,
    alias_key: str,
    candidates: list[AliasClusterCandidate],
) -> AliasClusterCandidate | None:
    if _is_blocklisted_runtime_single_token(alias_key):
        return None
    location_candidates = [
        candidate
        for candidate in candidates
        if candidate.label == "location"
        and not candidate.generated
        and candidate.display_text.casefold() == candidate.canonical_text.casefold()
    ]
    if not location_candidates:
        return None
    location_candidate = max(
        location_candidates,
        key=lambda candidate: _cluster_candidate_rank(
            alias_key=alias_key,
            candidate=candidate,
        ),
    )
    top_candidate = candidates[0]
    if top_candidate.label == "location":
        return None
    if location_candidate.features.popularity_weight < 0.78:
        return None
    if location_candidate.features.source_priority + 0.05 < top_candidate.features.source_priority:
        return None
    score_gap_limit = 0.06
    if (
        _is_single_token_alpha(alias_key)
        and location_candidate.features.popularity_weight >= 0.9
        and location_candidate.display_text.casefold()
        == location_candidate.canonical_text.casefold()
    ):
        score_gap_limit = 0.12
    if top_candidate.score - location_candidate.score > score_gap_limit:
        return None
    if (
        top_candidate.label == "organization"
        and top_candidate.features.popularity_weight
        > location_candidate.features.popularity_weight + 0.08
        and top_candidate.score > location_candidate.score + 0.02
    ):
        return None
    return location_candidate


def _resolve_runtime_tier(
    *,
    alias_key: str,
    candidate: AliasClusterCandidate,
) -> str:
    tokens = [
        token
        for token in re.split(r"[\s\-_./]+", alias_key.strip())
        if token
    ]
    if not tokens:
        return "search_only"
    if candidate.lane == "exact":
        if (
            candidate.label == "person"
            and len(tokens) >= 2
            and (
                tokens[0] in _RUNTIME_GENERIC_PHRASE_LEADS
                or any(
                    token in _RUNTIME_AUXILIARY_WORDS
                    or (
                        token in _RUNTIME_FUNCTION_WORDS
                        and token not in _RUNTIME_PERSON_PARTICLES
                    )
                    for token in tokens[1:]
                )
            )
        ):
            return "search_only"
        if (
            candidate.label in {"organization", "location"}
            and tokens[0] in _RUNTIME_GENERIC_PHRASE_LEADS
            and any(
                token in _RUNTIME_AUXILIARY_WORDS or token in _RUNTIME_FUNCTION_WORDS
                for token in tokens[1:]
            )
        ):
            return "search_only"
        if (
            _is_single_token_alpha(alias_key)
            and _is_blocklisted_runtime_single_token(alias_key)
        ):
            return "search_only"
        return "runtime_exact_high_precision"
    if _is_strong_runtime_acronym_candidate(alias_key=alias_key, candidate=candidate):
        return "runtime_exact_acronym_high_precision"
    if _is_strong_short_geopolitical_candidate(
        alias_key=alias_key,
        candidate=candidate,
    ):
        return "runtime_exact_geopolitical_high_precision"
    if _is_strong_geopolitical_acronym_candidate(
        alias_key=alias_key,
        candidate=candidate,
    ):
        return "runtime_exact_geopolitical_high_precision"
    if candidate.label == "person" and len(tokens) < 2:
        return "search_only"
    if candidate.label == "person":
        if tokens[0] in _RUNTIME_GENERIC_PHRASE_LEADS:
            return "search_only"
        if any(
            token in _RUNTIME_AUXILIARY_WORDS
            or (
                token in _RUNTIME_FUNCTION_WORDS
                and token not in _RUNTIME_PERSON_PARTICLES
            )
            for token in tokens[1:]
        ):
            return "search_only"
        return "runtime_exact_high_precision"
    if tokens[0] in _RUNTIME_GENERIC_PHRASE_LEADS:
        if (
            candidate.label == "location"
            and candidate.features.popularity_weight >= 0.9
            and candidate.score >= 0.9
            and not any(token in _RUNTIME_AUXILIARY_WORDS for token in tokens[1:])
        ):
            return "runtime_exact_high_precision"
        return "search_only"
    if _is_single_token_alpha(alias_key):
        if _is_blocklisted_runtime_single_token(alias_key):
            return "search_only"
        if candidate.label == "location":
            if candidate.generated or candidate.features.popularity_weight < 0.8:
                return "search_only"
            return "runtime_exact_high_precision"
        if candidate.label == "organization":
            exact_canonical = (
                candidate.display_text.casefold() == candidate.canonical_text.casefold()
            )
            strong_exact_single_token_org = (
                exact_canonical
                and candidate.features.preferred_name_weight >= 0.95
                and candidate.features.popularity_weight >= 0.8
                and candidate.score >= 0.74
            )
            strong_fragment_single_token_org = (
                not exact_canonical
                and candidate.features.source_priority >= 0.88
                and candidate.features.popularity_weight >= 0.9
                and candidate.score >= 0.7
            )
            if _is_generic_single_token_like(alias_key) and not (
                strong_exact_single_token_org or strong_fragment_single_token_org
            ):
                return "search_only"
            if (
                not exact_canonical
                and not candidate.display_text.isupper()
                and not strong_fragment_single_token_org
            ):
                return "search_only"
            if candidate.score < 0.62:
                return "search_only"
            return "runtime_exact_high_precision"
        if candidate.score < 0.62:
            return "search_only"
        return "runtime_exact_high_precision"
    if (
        candidate.label == "location"
        and tokens[-1] in _RUNTIME_GENERIC_LOCATION_HEADS
        and candidate.features.popularity_weight < 0.9
    ):
        return "search_only"
    if (
        candidate.label in {"organization", "person"}
        and tokens[-1] in _RUNTIME_GENERIC_ENTITY_HEADS
        and candidate.score < 0.9
    ):
        return "search_only"
    if (
        candidate.label == "organization"
        and len(tokens) >= 2
        and not candidate.generated
        and candidate.features.source_priority >= 0.8
        and candidate.score >= 0.6
        and tokens[0] not in _RUNTIME_GENERIC_PHRASE_LEADS
        and tokens[-1] not in _RUNTIME_FUNCTION_WORDS
    ):
        return "runtime_exact_high_precision"
    if candidate.label in {"organization", "location"} and candidate.score < 0.62:
        return "search_only"
    return "runtime_exact_high_precision"


def _should_keep_dominant_location_alias(
    *,
    top_candidate: AliasClusterCandidate,
    runner_up_candidate: AliasClusterCandidate,
) -> bool:
    if top_candidate.label != "location" or runner_up_candidate.label == "location":
        return False
    if top_candidate.score < 0.72:
        return False
    if top_candidate.features.source_priority < runner_up_candidate.features.source_priority:
        return False
    return (
        top_candidate.features.popularity_weight + 0.02
        >= runner_up_candidate.features.popularity_weight
    )


def _select_supported_location_candidate(
    candidates: list[AliasClusterCandidate],
) -> AliasClusterCandidate | None:
    if not candidates:
        return None
    location_candidate = next(
        (candidate for candidate in candidates if candidate.label == "location"),
        None,
    )
    if location_candidate is None:
        return None
    if location_candidate.generated:
        return None
    if location_candidate.non_generated_candidate_count < 2:
        return None
    if location_candidate.score < 0.62:
        return None
    if any(
        candidate.label != "location" and candidate.non_generated_candidate_count > 0
        for candidate in candidates
    ):
        return None
    top_candidate = candidates[0]
    if top_candidate.label == "location":
        return None
    if top_candidate.score - location_candidate.score > 0.06:
        return None
    return location_candidate


def _is_single_token_alpha(alias_key: str) -> bool:
    tokens = [
        token
        for token in re.split(r"[\s\-_./]+", alias_key.strip())
        if token
    ]
    return len(tokens) == 1 and tokens[0].isalpha()


def _single_token_compact_length(alias_key: str) -> int:
    return len("".join(character for character in alias_key if character.isalnum()))


def _clean_text(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    return str(raw_value).strip()


@dataclass(frozen=True)
class _GeneralRetainedAliasAuditConfig:
    mode: str
    reviewer_name: str
    reviewer: Callable[[dict[str, Any]], RetainedAliasReviewDecision | dict[str, Any]] | None
    reviewer_command: str | None
    review_timeout_seconds: float
    review_cache_path: Path | None


def audit_general_retained_aliases(
    result: AliasAnalysisResult,
    *,
    chunk_size: int = DEFAULT_RETAINED_ALIAS_AUDIT_CHUNK_SIZE,
    review_mode: str | None = None,
    reviewer: Callable[[dict[str, Any]], RetainedAliasReviewDecision | dict[str, Any]]
    | None = None,
    reviewer_command: str | None = None,
    review_timeout_seconds: float | None = None,
    review_cache_path: str | Path | None = None,
) -> AliasAnalysisResult:
    """Remove overly generic retained aliases from the general-en pack output."""

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive.")
    audit_config = _resolve_general_retained_alias_audit_config(
        review_mode=review_mode,
        reviewer=reviewer,
        reviewer_command=reviewer_command,
        review_timeout_seconds=review_timeout_seconds,
        review_cache_path=review_cache_path,
    )
    if (
        audit_config.mode != _GENERAL_RETAINED_ALIAS_AUDIT_MODE_HEURISTIC
        and audit_config.review_cache_path is None
        and result.database_path
    ):
        database_path = Path(result.database_path).expanduser().resolve()
        audit_config = replace(
            audit_config,
            review_cache_path=database_path.with_name(
                f"{database_path.stem}-retained-alias-reviews.sqlite"
            ),
        )
    if result.retained_aliases_materialized:
        return _audit_materialized_general_retained_aliases(
            result,
            chunk_size=chunk_size,
            audit_config=audit_config,
        )
    if not result.database_path:
        raise ValueError("database_path is required for DB-backed retained alias audits.")
    return _audit_general_retained_aliases_in_db(
        result,
        analysis_db_path=Path(result.database_path),
        chunk_size=chunk_size,
        audit_config=audit_config,
    )


def _audit_materialized_general_retained_aliases(
    result: AliasAnalysisResult,
    *,
    chunk_size: int,
    audit_config: _GeneralRetainedAliasAuditConfig,
) -> AliasAnalysisResult:
    kept_aliases: list[dict[str, Any]] = []
    removed_reason_counts: dict[str, int] = {}
    removed_alias_count = 0
    scanned_alias_count = 0
    invoked_review_count = 0
    cached_review_count = 0
    review_connection = (
        _open_retained_alias_review_store(audit_config.review_cache_path)
        if audit_config.mode != _GENERAL_RETAINED_ALIAS_AUDIT_MODE_HEURISTIC
        else None
    )
    try:
        for alias in result.retained_aliases:
            scanned_alias_count += 1
            decision, reused_cache = _review_general_retained_alias(
                alias,
                audit_config=audit_config,
                review_connection=review_connection,
            )
            if reused_cache:
                cached_review_count += 1
            elif audit_config.mode != _GENERAL_RETAINED_ALIAS_AUDIT_MODE_HEURISTIC:
                invoked_review_count += 1
            if decision is None or decision.decision == "keep":
                kept_aliases.append(alias)
                continue
            removed_alias_count += 1
            removed_reason_counts[decision.reason_code] = (
                removed_reason_counts.get(decision.reason_code, 0) + 1
            )
    finally:
        if review_connection is not None:
            review_connection.close()
    retained_aliases = sorted(
        kept_aliases,
        key=lambda item: (
            str(item["text"]).casefold(),
            str(item["text"]),
            str(item["label"]).casefold(),
            str(item["label"]),
        ),
    )
    retained_label_counts = _build_retained_label_counts(
        (str(item["label"]) for item in retained_aliases)
    )
    retained_alias_audit_reason_counts = {
        reason: removed_reason_counts[reason]
        for reason in sorted(removed_reason_counts, key=lambda value: (value.casefold(), value))
    }
    return replace(
        result,
        retained_aliases=retained_aliases,
        retained_alias_count=len(retained_aliases),
        retained_label_counts=retained_label_counts,
        retained_alias_audit_scanned_alias_count=scanned_alias_count,
        retained_alias_audit_removed_alias_count=removed_alias_count,
        retained_alias_audit_chunk_size=chunk_size,
        retained_alias_audit_reason_counts=retained_alias_audit_reason_counts,
        retained_alias_audit_mode=audit_config.mode,
        retained_alias_audit_reviewer_name=audit_config.reviewer_name,
        retained_alias_audit_review_cache_path=(
            str(audit_config.review_cache_path) if audit_config.review_cache_path else None
        ),
        retained_alias_audit_invoked_review_count=invoked_review_count,
        retained_alias_audit_cached_review_count=cached_review_count,
    )


def _audit_general_retained_aliases_in_db(
    result: AliasAnalysisResult,
    *,
    analysis_db_path: Path,
    chunk_size: int,
    audit_config: _GeneralRetainedAliasAuditConfig,
) -> AliasAnalysisResult:
    resolved_path = analysis_db_path.expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Alias analysis database not found: {resolved_path}")
    connection = sqlite3.connect(str(resolved_path))
    review_connection = (
        _open_retained_alias_review_store(audit_config.review_cache_path)
        if audit_config.mode != _GENERAL_RETAINED_ALIAS_AUDIT_MODE_HEURISTIC
        else None
    )
    try:
        removed_reason_counts: dict[str, int] = {}
        removed_alias_count = 0
        scanned_alias_count = 0
        invoked_review_count = 0
        cached_review_count = 0
        last_rowid = 0
        while True:
            rows = connection.execute(
                """
                SELECT
                    rowid,
                    display_text,
                    label,
                    canonical_text,
                    entity_id,
                    source_name,
                    generated,
                    score,
                    source_priority,
                    popularity_weight,
                    runtime_tier
                FROM retained_aliases
                WHERE rowid > ?
                ORDER BY rowid ASC
                LIMIT ?
                """,
                (last_rowid, chunk_size),
            ).fetchall()
            if not rows:
                break
            rowids_to_delete: list[int] = []
            for (
                rowid,
                display_text,
                label,
                canonical_text,
                entity_id,
                source_name,
                generated,
                score,
                source_priority,
                popularity_weight,
                runtime_tier,
            ) in rows:
                scanned_alias_count += 1
                decision, reused_cache = _review_general_retained_alias(
                    {
                        "text": str(display_text),
                        "label": str(label),
                        "canonical_text": str(canonical_text),
                        "entity_id": str(entity_id),
                        "source_name": str(source_name),
                        "generated": bool(generated),
                        "score": float(score),
                        "source_priority": float(source_priority),
                        "popularity_weight": float(popularity_weight),
                        "runtime_tier": str(runtime_tier),
                    },
                    audit_config=audit_config,
                    review_connection=review_connection,
                )
                if reused_cache:
                    cached_review_count += 1
                elif audit_config.mode != _GENERAL_RETAINED_ALIAS_AUDIT_MODE_HEURISTIC:
                    invoked_review_count += 1
                if decision is None or decision.decision == "keep":
                    continue
                rowids_to_delete.append(int(rowid))
                removed_alias_count += 1
                removed_reason_counts[decision.reason_code] = (
                    removed_reason_counts.get(decision.reason_code, 0) + 1
                )
            if rowids_to_delete:
                connection.executemany(
                    "DELETE FROM retained_aliases WHERE rowid = ?",
                    ((rowid,) for rowid in rowids_to_delete),
                )
                connection.commit()
            last_rowid = int(rows[-1][0])
        retained_alias_count = int(
            connection.execute("SELECT COUNT(*) FROM retained_aliases").fetchone()[0]
        )
        retained_label_counts = _build_retained_label_counts(
            row[0]
            for row in connection.execute(
                """
                SELECT label
                FROM retained_aliases
                ORDER BY label_sort_key ASC, label ASC
                """
            )
        )
        retained_alias_audit_reason_counts = {
            reason: removed_reason_counts[reason]
            for reason in sorted(
                removed_reason_counts,
                key=lambda value: (value.casefold(), value),
            )
        }
        return replace(
            result,
            retained_alias_count=retained_alias_count,
            retained_label_counts=retained_label_counts,
            retained_alias_audit_scanned_alias_count=scanned_alias_count,
            retained_alias_audit_removed_alias_count=removed_alias_count,
            retained_alias_audit_chunk_size=chunk_size,
            retained_alias_audit_reason_counts=retained_alias_audit_reason_counts,
            retained_alias_audit_mode=audit_config.mode,
            retained_alias_audit_reviewer_name=audit_config.reviewer_name,
            retained_alias_audit_review_cache_path=(
                str(audit_config.review_cache_path) if audit_config.review_cache_path else None
            ),
            retained_alias_audit_invoked_review_count=invoked_review_count,
            retained_alias_audit_cached_review_count=cached_review_count,
        )
    finally:
        if review_connection is not None:
            review_connection.close()
        connection.close()


def apply_general_retained_alias_exclusions(
    result: AliasAnalysisResult,
    *,
    exclusions_path: str | Path,
    chunk_size: int = DEFAULT_RETAINED_ALIAS_AUDIT_CHUNK_SIZE,
) -> AliasAnalysisResult:
    """Apply a curated retained-alias exclusion list to a general-en analysis result."""

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive.")
    resolved_exclusions_path = Path(exclusions_path).expanduser().resolve()
    if not resolved_exclusions_path.exists():
        raise FileNotFoundError(
            f"Retained alias exclusion list not found: {resolved_exclusions_path}"
        )
    exclusion_keys = _load_general_retained_alias_exclusion_keys(resolved_exclusions_path)
    if not exclusion_keys:
        return replace(
            result,
            retained_alias_exclusion_removed_alias_count=0,
            retained_alias_exclusion_source_path=str(resolved_exclusions_path),
        )
    if result.retained_aliases_materialized:
        return _apply_general_retained_alias_exclusions_materialized(
            result,
            exclusion_keys=exclusion_keys,
            exclusions_path=resolved_exclusions_path,
        )
    if not result.database_path:
        raise ValueError(
            "database_path is required for DB-backed retained alias exclusion application."
        )
    return _apply_general_retained_alias_exclusions_in_db(
        result,
        analysis_db_path=Path(result.database_path),
        exclusion_keys=exclusion_keys,
        exclusions_path=resolved_exclusions_path,
        chunk_size=chunk_size,
    )


def _apply_general_retained_alias_exclusions_materialized(
    result: AliasAnalysisResult,
    *,
    exclusion_keys: set[str],
    exclusions_path: Path,
) -> AliasAnalysisResult:
    kept_aliases: list[dict[str, Any]] = []
    removed_alias_count = 0
    for alias in result.retained_aliases:
        if build_retained_alias_review_key(alias) in exclusion_keys:
            removed_alias_count += 1
            continue
        kept_aliases.append(alias)
    retained_aliases = sorted(
        kept_aliases,
        key=lambda item: (
            str(item["text"]).casefold(),
            str(item["text"]),
            str(item["label"]).casefold(),
            str(item["label"]),
        ),
    )
    retained_label_counts = _build_retained_label_counts(
        str(item["label"]) for item in retained_aliases
    )
    return replace(
        result,
        retained_aliases=retained_aliases,
        retained_alias_count=len(retained_aliases),
        retained_label_counts=retained_label_counts,
        retained_alias_exclusion_removed_alias_count=removed_alias_count,
        retained_alias_exclusion_source_path=str(exclusions_path),
    )


def _apply_general_retained_alias_exclusions_in_db(
    result: AliasAnalysisResult,
    *,
    analysis_db_path: Path,
    exclusion_keys: set[str],
    exclusions_path: Path,
    chunk_size: int,
) -> AliasAnalysisResult:
    resolved_path = analysis_db_path.expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Alias analysis database not found: {resolved_path}")
    connection = sqlite3.connect(str(resolved_path))
    try:
        removed_alias_count = 0
        last_rowid = 0
        while True:
            rows = connection.execute(
                """
                SELECT
                    rowid,
                    display_text,
                    label,
                    canonical_text,
                    entity_id,
                    source_name,
                    generated,
                    score,
                    source_priority,
                    popularity_weight,
                    runtime_tier
                FROM retained_aliases
                WHERE rowid > ?
                ORDER BY rowid ASC
                LIMIT ?
                """,
                (last_rowid, chunk_size),
            ).fetchall()
            if not rows:
                break
            rowids_to_delete: list[int] = []
            for (
                rowid,
                display_text,
                label,
                canonical_text,
                entity_id,
                source_name,
                generated,
                score,
                source_priority,
                popularity_weight,
                runtime_tier,
            ) in rows:
                alias = {
                    "text": str(display_text),
                    "label": str(label),
                    "canonical_text": str(canonical_text),
                    "entity_id": str(entity_id),
                    "source_name": str(source_name),
                    "generated": bool(generated),
                    "score": float(score),
                    "source_priority": float(source_priority),
                    "popularity_weight": float(popularity_weight),
                    "runtime_tier": str(runtime_tier),
                }
                if build_retained_alias_review_key(alias) not in exclusion_keys:
                    continue
                rowids_to_delete.append(int(rowid))
                removed_alias_count += 1
            if rowids_to_delete:
                connection.executemany(
                    "DELETE FROM retained_aliases WHERE rowid = ?",
                    ((rowid,) for rowid in rowids_to_delete),
                )
                connection.commit()
            last_rowid = int(rows[-1][0])
        retained_alias_count = int(
            connection.execute("SELECT COUNT(*) FROM retained_aliases").fetchone()[0]
        )
        retained_label_counts = _build_retained_label_counts(
            row[0]
            for row in connection.execute(
                """
                SELECT label
                FROM retained_aliases
                ORDER BY label_sort_key ASC, label ASC
                """
            )
        )
        return replace(
            result,
            retained_alias_count=retained_alias_count,
            retained_label_counts=retained_label_counts,
            retained_alias_exclusion_removed_alias_count=removed_alias_count,
            retained_alias_exclusion_source_path=str(exclusions_path),
        )
    finally:
        connection.close()


def _load_general_retained_alias_exclusion_keys(path: Path) -> set[str]:
    exclusion_keys: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(
                "Retained alias exclusion entries must be JSON objects in JSONL format."
            )
        decision = _clean_text(payload.get("decision")).casefold()
        if decision and decision != "drop":
            continue
        review_key = _clean_text(payload.get("review_key"))
        if not review_key:
            review_key = build_retained_alias_review_key(payload)
        exclusion_keys.add(review_key)
    return exclusion_keys


def _resolve_general_retained_alias_audit_config(
    *,
    review_mode: str | None,
    reviewer: Callable[[dict[str, Any]], RetainedAliasReviewDecision | dict[str, Any]]
    | None,
    reviewer_command: str | None,
    review_timeout_seconds: float | None,
    review_cache_path: str | Path | None,
) -> _GeneralRetainedAliasAuditConfig:
    if reviewer is not None:
        return _GeneralRetainedAliasAuditConfig(
            mode=_GENERAL_RETAINED_ALIAS_AUDIT_MODE_CALLABLE,
            reviewer_name=_resolve_reviewer_name(reviewer),
            reviewer=reviewer,
            reviewer_command=None,
            review_timeout_seconds=_resolve_general_retained_alias_audit_timeout(
                review_timeout_seconds
            ),
            review_cache_path=_resolve_general_retained_alias_review_cache_path(
                review_cache_path
            ),
        )
    raw_mode = (
        str(
            review_mode
            if review_mode is not None
            else os.getenv(
                GENERAL_RETAINED_ALIAS_AUDIT_MODE_ENV,
                _GENERAL_RETAINED_ALIAS_AUDIT_MODE_AUTO,
            )
        )
        .strip()
        .lower()
        or _GENERAL_RETAINED_ALIAS_AUDIT_MODE_AUTO
    )
    if raw_mode not in _GENERAL_RETAINED_ALIAS_AUDIT_ALLOWED_MODES:
        raise ValueError(
            f"Unsupported {GENERAL_RETAINED_ALIAS_AUDIT_MODE_ENV} value: {raw_mode!r}."
        )
    resolved_command = _clean_text(
        reviewer_command or os.getenv(GENERAL_RETAINED_ALIAS_AUDIT_COMMAND_ENV)
    )
    resolved_mode = raw_mode
    if resolved_mode == _GENERAL_RETAINED_ALIAS_AUDIT_MODE_AUTO:
        resolved_mode = (
            _GENERAL_RETAINED_ALIAS_AUDIT_MODE_AI_COMMAND
            if resolved_command
            else _GENERAL_RETAINED_ALIAS_AUDIT_MODE_DETERMINISTIC_COMMON_ENGLISH
        )
    if resolved_mode == _GENERAL_RETAINED_ALIAS_AUDIT_MODE_AI_COMMAND and not resolved_command:
        raise RuntimeError(
            "general-en retained alias AI review requires a configured reviewer command. "
            f"Set {GENERAL_RETAINED_ALIAS_AUDIT_COMMAND_ENV} or pass reviewer_command."
        )
    return _GeneralRetainedAliasAuditConfig(
        mode=resolved_mode,
        reviewer_name=(
            _resolve_command_reviewer_name(resolved_command)
            if resolved_mode == _GENERAL_RETAINED_ALIAS_AUDIT_MODE_AI_COMMAND
            else resolved_mode
        ),
        reviewer=None,
        reviewer_command=resolved_command or None,
        review_timeout_seconds=_resolve_general_retained_alias_audit_timeout(
            review_timeout_seconds
        ),
        review_cache_path=(
            _resolve_general_retained_alias_review_cache_path(review_cache_path)
            if resolved_mode
            not in {
                _GENERAL_RETAINED_ALIAS_AUDIT_MODE_HEURISTIC,
                _GENERAL_RETAINED_ALIAS_AUDIT_MODE_DETERMINISTIC_COMMON_ENGLISH,
            }
            else None
        ),
    )


def _resolve_general_retained_alias_audit_timeout(value: float | None) -> float:
    raw_value = value
    if raw_value is None:
        env_value = _clean_text(os.getenv(GENERAL_RETAINED_ALIAS_AUDIT_TIMEOUT_ENV))
        raw_value = (
            float(env_value)
            if env_value
            else DEFAULT_RETAINED_ALIAS_AUDIT_TIMEOUT_SECONDS
        )
    if raw_value <= 0:
        raise ValueError("review_timeout_seconds must be positive.")
    return float(raw_value)


def _resolve_general_retained_alias_review_cache_path(
    value: str | Path | None,
) -> Path | None:
    if value is None:
        return None
    return Path(value).expanduser().resolve()


def _resolve_reviewer_name(
    reviewer: Callable[[dict[str, Any]], RetainedAliasReviewDecision | dict[str, Any]],
) -> str:
    name = getattr(reviewer, "__name__", "")
    if name:
        return str(name)
    return reviewer.__class__.__name__


def _resolve_command_reviewer_name(command: str) -> str:
    tokens = shlex.split(command)
    if not tokens:
        raise RuntimeError(
            f"{GENERAL_RETAINED_ALIAS_AUDIT_COMMAND_ENV} did not resolve to an executable."
        )
    return tokens[0]


def _open_retained_alias_review_store(
    path: Path | None,
) -> sqlite3.Connection | None:
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS retained_alias_audit_reviews (
            review_key TEXT NOT NULL,
            reviewer_mode TEXT NOT NULL,
            reviewer_name TEXT NOT NULL,
            display_text TEXT NOT NULL,
            label TEXT NOT NULL,
            canonical_text TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            decision TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            reason TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            response_json TEXT NOT NULL,
            reviewed_at TEXT NOT NULL,
            PRIMARY KEY (review_key, reviewer_mode, reviewer_name)
        )
        """
    )
    connection.commit()
    return connection


def _review_general_retained_alias(
    alias: dict[str, Any],
    *,
    audit_config: _GeneralRetainedAliasAuditConfig,
    review_connection: sqlite3.Connection | None,
) -> tuple[RetainedAliasReviewDecision | None, bool]:
    if audit_config.mode == _GENERAL_RETAINED_ALIAS_AUDIT_MODE_HEURISTIC:
        reason = _classify_generic_general_retained_alias(
            text=str(alias.get("text", "")),
            label=str(alias.get("label", "")),
            canonical_text=str(alias.get("canonical_text", "")),
        )
        if reason is None:
            return None, False
        return (
            RetainedAliasReviewDecision(
                decision="drop",
                reason_code=reason,
                reason=reason,
            ),
            False,
        )
    if (
        audit_config.mode
        == _GENERAL_RETAINED_ALIAS_AUDIT_MODE_DETERMINISTIC_COMMON_ENGLISH
    ):
        reason = _classify_deterministic_common_english_retained_alias(
            text=str(alias.get("text", "")),
            label=str(alias.get("label", "")),
            canonical_text=str(alias.get("canonical_text", "")),
        )
        if reason is None:
            return None, False
        return (
            RetainedAliasReviewDecision(
                decision="drop",
                reason_code=reason,
                reason=reason,
            ),
            False,
        )
    cached = _load_cached_retained_alias_review(
        review_connection,
        alias,
        audit_config=audit_config,
    )
    if cached is not None:
        return cached, True
    if audit_config.mode == _GENERAL_RETAINED_ALIAS_AUDIT_MODE_CALLABLE:
        if audit_config.reviewer is None:
            raise RuntimeError("callable audit mode requires a reviewer callable.")
        raw_decision = audit_config.reviewer(dict(alias))
        response_payload = (
            raw_decision
            if isinstance(raw_decision, dict)
            else {
                "decision": raw_decision.decision,
                "reason_code": raw_decision.reason_code,
                "reason": raw_decision.reason,
            }
        )
    elif audit_config.mode == _GENERAL_RETAINED_ALIAS_AUDIT_MODE_AI_COMMAND:
        if not audit_config.reviewer_command:
            raise RuntimeError("ai_command audit mode requires reviewer_command.")
        response_payload = _run_general_retained_alias_ai_command(
            alias,
            command=audit_config.reviewer_command,
            timeout_seconds=audit_config.review_timeout_seconds,
        )
        raw_decision = response_payload
    else:
        raise RuntimeError(f"Unsupported general retained alias audit mode: {audit_config.mode}")
    decision = _coerce_retained_alias_review_decision(raw_decision)
    _store_cached_retained_alias_review(
        review_connection,
        alias,
        audit_config=audit_config,
        decision=decision,
        response_payload=response_payload,
    )
    return decision, False


def _retained_alias_review_payload(alias: dict[str, Any]) -> dict[str, Any]:
    return build_retained_alias_review_payload(alias)


def _retained_alias_review_key(alias: dict[str, Any]) -> str:
    return build_retained_alias_review_key(alias)


def _load_cached_retained_alias_review(
    connection: sqlite3.Connection | None,
    alias: dict[str, Any],
    *,
    audit_config: _GeneralRetainedAliasAuditConfig,
) -> RetainedAliasReviewDecision | None:
    if connection is None:
        return None
    row = connection.execute(
        """
        SELECT decision, reason_code, reason
        FROM retained_alias_audit_reviews
        WHERE review_key = ?
          AND reviewer_mode = ?
          AND reviewer_name = ?
        """,
        (
            _retained_alias_review_key(alias),
            audit_config.mode,
            audit_config.reviewer_name,
        ),
    ).fetchone()
    if row is None:
        return None
    return RetainedAliasReviewDecision(
        decision=str(row[0]),
        reason_code=str(row[1]),
        reason=str(row[2]),
    )


def _store_cached_retained_alias_review(
    connection: sqlite3.Connection | None,
    alias: dict[str, Any],
    *,
    audit_config: _GeneralRetainedAliasAuditConfig,
    decision: RetainedAliasReviewDecision,
    response_payload: dict[str, Any],
) -> None:
    if connection is None:
        return
    payload = _retained_alias_review_payload(alias)
    reviewed_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    connection.execute(
        """
        INSERT OR REPLACE INTO retained_alias_audit_reviews (
            review_key,
            reviewer_mode,
            reviewer_name,
            display_text,
            label,
            canonical_text,
            entity_id,
            source_name,
            decision,
            reason_code,
            reason,
            payload_json,
            response_json,
            reviewed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _retained_alias_review_key(alias),
            audit_config.mode,
            audit_config.reviewer_name,
            payload["text"],
            payload["label"],
            payload["canonical_text"],
            payload["entity_id"],
            payload["source_name"],
            decision.decision,
            decision.reason_code,
            decision.reason,
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
            json.dumps(response_payload, ensure_ascii=False, sort_keys=True),
            reviewed_at,
        ),
    )
    connection.commit()


def _run_general_retained_alias_ai_command(
    alias: dict[str, Any],
    *,
    command: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    payload = {
        "task": "general_retained_alias_review",
        "instructions": (
            "Review one retained alias from the general-en pack and return a keep/drop "
            "decision. Drop generic English words or phrases even if they correspond to "
            "obscure real entities. Prefer false negatives over broad false positives."
        ),
        "required_response_schema": {
            "decision": "keep|drop",
            "reason_code": "short_snake_case_code",
            "reason": "short_human_readable_reason",
        },
        "alias": _retained_alias_review_payload(alias),
    }
    process = subprocess.run(
        shlex.split(command),
        input=json.dumps(payload, ensure_ascii=False),
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if process.returncode != 0:
        error_output = _clean_text(process.stderr) or _clean_text(process.stdout)
        raise RuntimeError(
            "general-en retained alias AI review command failed: "
            f"{error_output or f'exit code {process.returncode}'}"
        )
    try:
        response = json.loads(process.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "general-en retained alias AI review command did not return JSON."
        ) from exc
    if not isinstance(response, dict):
        raise RuntimeError(
            "general-en retained alias AI review command must return a JSON object."
        )
    return response


def _coerce_retained_alias_review_decision(
    raw_value: RetainedAliasReviewDecision | dict[str, Any],
) -> RetainedAliasReviewDecision:
    if isinstance(raw_value, RetainedAliasReviewDecision):
        decision = raw_value
    elif isinstance(raw_value, dict):
        decision_value = _clean_text(raw_value.get("decision")).casefold()
        if decision_value not in {"keep", "drop"}:
            raise RuntimeError(
                "retained alias reviewer must return decision=keep or decision=drop."
            )
        reason_code = _clean_text(raw_value.get("reason_code"))
        if not reason_code:
            reason_code = (
                "ai_retained_alias_keep"
                if decision_value == "keep"
                else "ai_retained_alias_drop"
            )
        reason = _clean_text(raw_value.get("reason")) or reason_code
        decision = RetainedAliasReviewDecision(
            decision=decision_value,
            reason_code=reason_code,
            reason=reason,
        )
    else:
        raise RuntimeError(
            "retained alias reviewer must return a RetainedAliasReviewDecision or dict."
        )
    if decision.decision not in {"keep", "drop"}:
        raise RuntimeError("retained alias review decision must be keep or drop.")
    if not decision.reason_code:
        raise RuntimeError("retained alias review decision must include a reason_code.")
    if not decision.reason:
        raise RuntimeError("retained alias review decision must include a reason.")
    return decision


def _build_retained_label_counts(labels: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for label in labels:
        counts[label] = counts.get(label, 0) + 1
    return {
        label: counts[label]
        for label in sorted(counts, key=lambda value: (value.casefold(), value))
    }


def _classify_generic_general_retained_alias(
    *,
    text: str,
    label: str,
    canonical_text: str,
) -> str | None:
    del canonical_text
    tokens = _general_audit_alpha_tokens(text)
    if not tokens:
        return None
    if len(tokens) == 1 and label == "organization":
        token = tokens[0]
        if token in _GENERAL_AUDIT_EXECUTIVE_TITLE_ACRONYMS:
            return "generic_retained_single_token_alias"
    if _has_protected_uppercase_segment(text):
        return None
    if len(tokens) == 1:
        token = tokens[0]
        if (
            token in _RUNTIME_CALENDAR_SINGLE_TOKEN_WORDS
            and token not in _GENERAL_AUDIT_DROPPABLE_CALENDAR_SINGLE_TOKEN_WORDS
        ):
            return None
        if _is_blocklisted_runtime_single_token(text):
            return "generic_retained_single_token_alias"
        if token in _GENERAL_AUDIT_COMMON_SINGLE_TOKEN_WORDS:
            return "generic_retained_single_token_alias"
        return None
    if _has_parenthetical_disambiguation(text):
        return None
    if any(token in _RUNTIME_PERSON_PARTICLES for token in tokens):
        return None
    if len(tokens) > 2:
        return None
    if len(tokens) > 1 and tokens[0] in _RUNTIME_CALENDAR_SINGLE_TOKEN_WORDS:
        return None
    if tokens[0] in _GENERAL_AUDIT_ARTICLE_LEADS:
        return None
    if (
        len(tokens) == 2
        and tokens[1] in _GENERAL_AUDIT_GENERIC_PHRASE_TAILS
        and tokens[0] in _GENERAL_AUDIT_GENERIC_PHRASE_LEADS
    ):
        return "generic_retained_phrase_alias"
    if (
        len(tokens) == 2
        and tokens[0] in _GENERAL_AUDIT_GENERIC_PHRASE_TAILS
        and tokens[1] in _RUNTIME_NUMBER_SINGLE_TOKEN_WORDS
    ):
        return "generic_retained_phrase_alias"
    return None


def _classify_deterministic_common_english_retained_alias(
    *,
    text: str,
    label: str,
    canonical_text: str,
) -> str | None:
    if _has_non_ascii_alpha_character(text):
        return None
    if _has_parenthetical_disambiguation(text):
        return None
    if any(character.isdigit() for character in text):
        return None
    if any(character in "+/@&" for character in text):
        return None
    if _has_dotted_initial_segment(text):
        return None
    heuristic_reason = _classify_generic_general_retained_alias(
        text=text,
        label=label,
        canonical_text=canonical_text,
    )
    if heuristic_reason is not None:
        return heuristic_reason
    if _has_protected_uppercase_segment(text):
        return None
    tokens = _general_audit_alpha_tokens(text)
    if not tokens:
        return None
    normalized_text = _normalize_general_audit_phrase(text)
    if not normalized_text:
        return None
    if len(tokens) == 1:
        if not normalized_text.isalpha():
            return None
        if (
            tokens[0] in _RUNTIME_CALENDAR_SINGLE_TOKEN_WORDS
            and tokens[0] not in _GENERAL_AUDIT_DROPPABLE_CALENDAR_SINGLE_TOKEN_WORDS
        ):
            return None
        return None
    if tokens[0] in _GENERAL_AUDIT_ARTICLE_LEADS:
        return None
    if any(token in _RUNTIME_PERSON_PARTICLES for token in tokens):
        return None
    if tokens[0] in _RUNTIME_CALENDAR_SINGLE_TOKEN_WORDS:
        return None
    if len(tokens) > 2:
        return None
    phrase_zipf = _wordfreq_zipf_frequency_en(normalized_text)
    if phrase_zipf < _GENERAL_AUDIT_WORDFREQ_PHRASE_ZIPF_MIN:
        return None
    token_zipfs = [_wordfreq_zipf_frequency_en(token) for token in tokens]
    if min(token_zipfs) < _GENERAL_AUDIT_WORDFREQ_COMMON_TOKEN_ZIPF_MIN:
        return None
    if (
        len(tokens) == 2
        and tokens[1] in _GENERAL_AUDIT_GENERIC_PHRASE_TAILS
        and tokens[0] in _GENERAL_AUDIT_GENERIC_PHRASE_LEADS
    ):
        return "wordfreq_high_frequency_phrase_alias"
    if (
        len(tokens) == 2
        and tokens[0] in _GENERAL_AUDIT_GENERIC_PHRASE_TAILS
        and tokens[1] in _RUNTIME_NUMBER_SINGLE_TOKEN_WORDS
    ):
        return "wordfreq_high_frequency_phrase_alias"
    return None


@lru_cache(maxsize=200_000)
def _wordfreq_zipf_frequency_en(text: str) -> float:
    normalized = _normalize_general_audit_phrase(text)
    if not normalized:
        return 0.0
    if _wordfreq_zipf_frequency is None:
        raise RuntimeError(
            "wordfreq is required for deterministic common-English retained alias review."
        )
    return float(_wordfreq_zipf_frequency(normalized, "en"))


def _normalize_general_audit_phrase(text: str) -> str:
    return re.sub(r"\s+", " ", str(text).strip()).casefold()


def _general_audit_alpha_tokens(text: str) -> list[str]:
    return [token.casefold() for token in re.findall(r"[A-Za-z]+", text)]


def _has_protected_uppercase_segment(text: str) -> bool:
    return any(
        len(segment) > 1 and segment.isupper()
        for segment in re.split(r"[\s/._-]+", text.strip())
        if segment
    )


def _has_dotted_initial_segment(text: str) -> bool:
    return re.search(r"(?:^|[\s(])(?:[A-Za-z]\.){1,}(?:$|[\s)])", text.strip()) is not None


def _has_non_ascii_alpha_character(text: str) -> bool:
    return any(character.isalpha() and not character.isascii() for character in text)


def _has_parenthetical_disambiguation(text: str) -> bool:
    stripped = text.strip()
    return "(" in stripped and ")" in stripped
