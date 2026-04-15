"""Phase-0 matcher backend benchmark helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import re
import sqlite3
import time
from typing import Any

from .runtime_matcher import (
    MATCHER_JSON_AHO_ALGORITHM,
    MATCHER_TOKEN_TRIE_ALGORITHM,
    build_matcher_artifact_from_aliases_json,
    find_exact_match_spans,
    load_runtime_matcher,
)
from .text_processing import normalize_lookup_text

_TOKEN_RE = re.compile(r"\b[\w][\w.+&/-]*\b")


@dataclass(frozen=True)
class MatcherBenchmarkCandidate:
    """Measured result for one candidate backend."""

    backend: str
    available: bool
    alias_count: int
    unique_pattern_count: int
    build_ms: int
    cold_load_ms: int
    warm_query_p50_ms: int
    warm_query_p95_ms: int
    artifact_bytes: int
    state_count: int = 0
    token_vocab_count: int = 0
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class MatcherBenchmarkSpikeReport:
    """Benchmark report comparing candidate matcher backends."""

    pack_id: str
    profile: str
    aliases_path: str
    scanned_alias_count: int
    retained_alias_count: int
    unique_pattern_count: int
    query_count: int
    query_runs: int
    min_alias_score: float
    alias_limit: int
    scan_limit: int
    exact_tier_min_token_count: int
    candidates: list[MatcherBenchmarkCandidate] = field(default_factory=list)
    recommended_backend: str | None = None
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _AliasSlice:
    scanned_alias_count: int
    retained_alias_count: int
    aliases: list[dict[str, Any]]


def benchmark_matcher_backends(
    *,
    pack_id: str,
    profile: str,
    aliases_path: str | Path,
    query_texts: list[str],
    output_dir: str | Path,
    alias_limit: int = 10000,
    scan_limit: int = 50000,
    min_alias_score: float = 0.8,
    exact_tier_min_token_count: int = 2,
    query_runs: int = 3,
) -> MatcherBenchmarkSpikeReport:
    """Benchmark candidate matcher backends on one bounded alias slice."""

    bounded_alias_limit = max(1, int(alias_limit))
    bounded_scan_limit = max(bounded_alias_limit, int(scan_limit))
    bounded_query_runs = max(1, int(query_runs))
    bounded_min_token_count = max(1, int(exact_tier_min_token_count))
    resolved_aliases_path = Path(aliases_path).expanduser().resolve()
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    alias_slice = _load_alias_slice(
        resolved_aliases_path,
        alias_limit=bounded_alias_limit,
        scan_limit=bounded_scan_limit,
        min_alias_score=float(min_alias_score),
        exact_tier_min_token_count=bounded_min_token_count,
    )
    warnings: list[str] = []
    if not alias_slice.aliases:
        warnings.append(f"empty_alias_slice:{pack_id}")
        return MatcherBenchmarkSpikeReport(
            pack_id=pack_id,
            profile=profile,
            aliases_path=str(resolved_aliases_path),
            scanned_alias_count=alias_slice.scanned_alias_count,
            retained_alias_count=alias_slice.retained_alias_count,
            unique_pattern_count=0,
            query_count=len(query_texts),
            query_runs=bounded_query_runs,
            min_alias_score=float(min_alias_score),
            alias_limit=bounded_alias_limit,
            scan_limit=bounded_scan_limit,
            exact_tier_min_token_count=bounded_min_token_count,
            candidates=[],
            recommended_backend=None,
            warnings=warnings,
        )

    slice_aliases_path = resolved_output_dir / "alias-slice.json"
    _write_alias_slice(slice_aliases_path, alias_slice.aliases)

    candidates = [
        _benchmark_backend(
            backend="current_json_aho",
            matcher_algorithm=MATCHER_JSON_AHO_ALGORITHM,
            aliases_path=slice_aliases_path,
            query_texts=query_texts,
            output_dir=resolved_output_dir / "current-json-aho",
            query_runs=bounded_query_runs,
            notes=["legacy_json_artifact"],
        ),
        _benchmark_backend(
            backend="token_trie_v1",
            matcher_algorithm=MATCHER_TOKEN_TRIE_ALGORITHM,
            aliases_path=slice_aliases_path,
            query_texts=query_texts,
            output_dir=resolved_output_dir / "token-trie-v1",
            query_runs=bounded_query_runs,
            notes=["production_candidate"],
        ),
    ]
    recommended_backend = _recommend_backend(candidates)
    return MatcherBenchmarkSpikeReport(
        pack_id=pack_id,
        profile=profile,
        aliases_path=str(resolved_aliases_path),
        scanned_alias_count=alias_slice.scanned_alias_count,
        retained_alias_count=alias_slice.retained_alias_count,
        unique_pattern_count=len(alias_slice.aliases),
        query_count=len(query_texts),
        query_runs=bounded_query_runs,
        min_alias_score=float(min_alias_score),
        alias_limit=bounded_alias_limit,
        scan_limit=bounded_scan_limit,
        exact_tier_min_token_count=bounded_min_token_count,
        candidates=candidates,
        recommended_backend=recommended_backend,
        warnings=warnings,
    )


def write_matcher_benchmark_spike_report(
    path: str | Path,
    report: MatcherBenchmarkSpikeReport,
) -> Path:
    """Persist one matcher benchmark spike report as JSON."""

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(_matcher_benchmark_spike_report_to_dict(report), indent=2) + "\n",
        encoding="utf-8",
    )
    return destination


def _load_alias_slice(
    aliases_path: Path,
    *,
    alias_limit: int,
    scan_limit: int,
    min_alias_score: float,
    exact_tier_min_token_count: int,
) -> _AliasSlice:
    analysis_db_path = aliases_path.with_name("analysis.sqlite")
    if analysis_db_path.exists():
        return _load_alias_slice_from_analysis_db(
            analysis_db_path,
            alias_limit=alias_limit,
            scan_limit=scan_limit,
            min_alias_score=min_alias_score,
            exact_tier_min_token_count=exact_tier_min_token_count,
        )

    selected_by_normalized_text: dict[str, dict[str, Any]] = {}
    scanned_alias_count = 0

    from .runtime_matcher import _iter_alias_payloads_from_aliases_json, _normalize_alias_payload

    for payload in _iter_alias_payloads_from_aliases_json(aliases_path):
        scanned_alias_count += 1
        if scanned_alias_count > scan_limit:
            break
        normalized = _normalize_alias_payload(payload)
        if bool(normalized.get("generated", False)):
            continue
        if float(normalized.get("score", 0.0)) < min_alias_score:
            continue
        tokens = _tokenize_lookup_text(str(normalized["normalized_text"]))
        if len(tokens) < exact_tier_min_token_count:
            continue
        key = str(normalized["normalized_text"])
        current = selected_by_normalized_text.get(key)
        if current is None or _alias_sort_key(normalized) > _alias_sort_key(current):
            selected_by_normalized_text[key] = normalized

    retained = sorted(
        selected_by_normalized_text.values(),
        key=_alias_sort_key,
        reverse=True,
    )[:alias_limit]
    return _AliasSlice(
        scanned_alias_count=scanned_alias_count,
        retained_alias_count=len(retained),
        aliases=retained,
    )


def _load_alias_slice_from_analysis_db(
    analysis_db_path: Path,
    *,
    alias_limit: int,
    scan_limit: int,
    min_alias_score: float,
    exact_tier_min_token_count: int,
) -> _AliasSlice:
    selected_by_normalized_text: dict[str, dict[str, Any]] = {}
    scanned_alias_count = 0
    connection = sqlite3.connect(str(analysis_db_path))
    try:
        cursor = connection.execute(
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
            LIMIT ?
            """,
            (int(scan_limit),),
        )
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
        ) in cursor:
            scanned_alias_count += 1
            if bool(generated):
                continue
            numeric_score = float(score)
            if numeric_score < min_alias_score:
                continue
            normalized_text = normalize_lookup_text(str(display_text))
            tokens = _tokenize_lookup_text(normalized_text)
            if len(tokens) < exact_tier_min_token_count:
                continue
            payload = {
                "text": str(display_text),
                "label": str(label),
                "generated": False,
                "score": numeric_score,
                "canonical_text": str(canonical_text),
                "source_name": str(source_name),
                "entity_id": str(entity_id),
                "source_priority": float(source_priority),
                "popularity_weight": float(popularity_weight),
                "normalized_text": normalized_text,
            }
            key = normalized_text
            current = selected_by_normalized_text.get(key)
            if current is None or _alias_sort_key(payload) > _alias_sort_key(current):
                selected_by_normalized_text[key] = payload
    finally:
        connection.close()

    retained = sorted(
        selected_by_normalized_text.values(),
        key=_alias_sort_key,
        reverse=True,
    )[:alias_limit]
    return _AliasSlice(
        scanned_alias_count=scanned_alias_count,
        retained_alias_count=len(retained),
        aliases=retained,
    )


def _alias_sort_key(payload: dict[str, Any]) -> tuple[float, int, float, str]:
    normalized_text = str(payload.get("normalized_text", ""))
    token_count = len(_tokenize_lookup_text(normalized_text))
    return (
        float(payload.get("score", 0.0)),
        token_count,
        float(payload.get("source_priority", 0.0)),
        str(payload.get("text", "")),
    )


def _write_alias_slice(path: Path, aliases: list[dict[str, Any]]) -> None:
    payload = {"aliases": aliases}
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _benchmark_backend(
    *,
    backend: str,
    matcher_algorithm: str,
    aliases_path: Path,
    query_texts: list[str],
    output_dir: Path,
    query_runs: int,
    notes: list[str] | None = None,
) -> MatcherBenchmarkCandidate:
    output_dir.mkdir(parents=True, exist_ok=True)
    build_started = time.perf_counter()
    build_result = build_matcher_artifact_from_aliases_json(
        aliases_path,
        output_dir=output_dir,
        algorithm=matcher_algorithm,
    )
    build_ms = int(round((time.perf_counter() - build_started) * 1000))

    load_started = time.perf_counter()
    matcher = load_runtime_matcher(build_result.artifact_path, build_result.entries_path)
    cold_load_ms = int(round((time.perf_counter() - load_started) * 1000))

    query_samples: list[int] = []
    for _ in range(query_runs):
        for query in query_texts:
            started = time.perf_counter()
            find_exact_match_spans(query, matcher)
            query_samples.append(int(round((time.perf_counter() - started) * 1000)))

    artifact_bytes = (
        Path(build_result.artifact_path).stat().st_size
        + Path(build_result.entries_path).stat().st_size
    )
    return MatcherBenchmarkCandidate(
        backend=backend,
        available=True,
        alias_count=build_result.entry_count,
        unique_pattern_count=build_result.entry_count,
        build_ms=build_ms,
        cold_load_ms=cold_load_ms,
        warm_query_p50_ms=_p50(query_samples),
        warm_query_p95_ms=_p95(query_samples),
        artifact_bytes=artifact_bytes,
        state_count=build_result.state_count,
        token_vocab_count=len(matcher.token_to_id),
        notes=list(notes or []),
    )


def _tokenize_lookup_text(value: str) -> tuple[str, ...]:
    return tuple(match.group(0) for match in _TOKEN_RE.finditer(value))


def _p95(values: list[int]) -> int:
    if not values:
        return 0
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
    return ordered[index]


def _p50(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return int(round((ordered[midpoint - 1] + ordered[midpoint]) / 2))


def _recommend_backend(candidates: list[MatcherBenchmarkCandidate]) -> str | None:
    available = [candidate for candidate in candidates if candidate.available]
    if not available:
        return None
    best_warm_p95 = min(candidate.warm_query_p95_ms for candidate in available)
    warm_tolerance_ms = 2
    shortlisted = [
        candidate
        for candidate in available
        if candidate.warm_query_p95_ms <= best_warm_p95 + warm_tolerance_ms
    ]
    return min(
        shortlisted,
        key=lambda item: (
            item.cold_load_ms,
            item.artifact_bytes,
            item.build_ms,
            item.warm_query_p95_ms,
        ),
    ).backend


def _matcher_benchmark_spike_report_to_dict(
    report: MatcherBenchmarkSpikeReport,
) -> dict[str, Any]:
    return {
        "pack_id": report.pack_id,
        "profile": report.profile,
        "aliases_path": report.aliases_path,
        "scanned_alias_count": report.scanned_alias_count,
        "retained_alias_count": report.retained_alias_count,
        "unique_pattern_count": report.unique_pattern_count,
        "query_count": report.query_count,
        "query_runs": report.query_runs,
        "min_alias_score": report.min_alias_score,
        "alias_limit": report.alias_limit,
        "scan_limit": report.scan_limit,
        "exact_tier_min_token_count": report.exact_tier_min_token_count,
        "candidates": [
            {
                "backend": candidate.backend,
                "available": candidate.available,
                "alias_count": candidate.alias_count,
                "unique_pattern_count": candidate.unique_pattern_count,
                "build_ms": candidate.build_ms,
                "cold_load_ms": candidate.cold_load_ms,
                "warm_query_p50_ms": candidate.warm_query_p50_ms,
                "warm_query_p95_ms": candidate.warm_query_p95_ms,
                "artifact_bytes": candidate.artifact_bytes,
                "state_count": candidate.state_count,
                "token_vocab_count": candidate.token_vocab_count,
                "notes": list(candidate.notes),
            }
            for candidate in report.candidates
        ],
        "recommended_backend": report.recommended_backend,
        "warnings": list(report.warnings),
    }
