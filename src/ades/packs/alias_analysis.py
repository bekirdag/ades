"""Shared alias-analysis helpers for generated pack builds."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from math import log10
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterable


DEFAULT_ALIAS_ANALYSIS_BACKEND = "sqlite"
DEFAULT_ALIAS_ANALYSIS_TOP_CLUSTERS = 20
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
        }


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

        retain_aliases_in_memory = materialize_retained_aliases or analysis_db_path is None
        retained_aliases: list[dict[str, Any]] = []
        retained_alias_rows: list[
            tuple[str, str, str, str, int, float, str, str, str, float, float]
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
            popularity_weight REAL NOT NULL DEFAULT 0.5
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
    best_by_label: dict[str, AliasClusterCandidate] = {}
    for row in rows:
        label = str(row[0])
        if label in best_by_label:
            continue
        best_by_label[label] = AliasClusterCandidate(
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
        )
    return list(best_by_label.values())


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
    rows: list[tuple[str, str, str, str, int, float, str, str, str, float, float]],
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
            popularity_weight
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def iter_retained_aliases_from_db(path: str | Path) -> Iterator[dict[str, Any]]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Alias analysis database not found: {resolved}")
    connection = sqlite3.connect(str(resolved))
    try:
        for (
            display_text,
            label,
            generated,
            score,
            canonical_text,
            source_name,
            entity_id,
            source_priority,
            popularity_weight,
        ) in connection.execute(
            """
            SELECT
                display_text,
                label,
                generated,
                score,
                canonical_text,
                source_name,
                entity_id,
                source_priority,
                popularity_weight
            FROM retained_aliases
            ORDER BY
                display_sort_key ASC,
                display_text ASC,
                label_sort_key ASC,
                label ASC
            """
        ):
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
            }
    finally:
        connection.close()


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


def _should_block_single_label_natural_alias(
    *,
    alias_key: str,
    candidate: AliasClusterCandidate,
) -> bool:
    if candidate.lane != "natural":
        return False
    if _is_single_token_alpha(alias_key):
        if (
            candidate.label in {"person", "location"}
            and candidate.display_text.casefold() != candidate.canonical_text.casefold()
        ):
            return True
        if (
            candidate.label == "organization"
            and candidate.display_text.casefold() != candidate.canonical_text.casefold()
            and not candidate.display_text.isupper()
            and _single_token_compact_length(alias_key) < 6
        ):
            return True
    if not _is_short_token_like(alias_key):
        return False
    return candidate.score < 0.7


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
