"""Golden-set evaluation and release thresholds for hosted vector refinement."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import json
from pathlib import Path
from statistics import median
import time
from typing import Literal

from ..config import Settings
from ..service.models import EntityLink, EntityMatch, TagResponse
from ..storage import RuntimeTarget
from ..version import __version__
from .service import enrich_tag_response_with_related_entities

DEFAULT_VECTOR_QUALITY_ROOT = Path("/mnt/githubActions/ades_big_data/vector_quality")


@dataclass(frozen=True)
class VectorGoldenSeedEntity:
    """One pre-linked seed entity used to query the hosted vector lane."""

    entity_id: str
    canonical_text: str
    label: str
    score: float = 1.0


@dataclass(frozen=True)
class VectorGoldenCase:
    """One vector-quality case with expected related and refinement outcomes."""

    name: str
    case_kind: Literal["related", "low_confidence", "easy_exact", "mixed"] = "related"
    seed_entities: tuple[VectorGoldenSeedEntity, ...] = ()
    expected_related_entity_ids: tuple[str, ...] = ()
    expected_boosted_entity_ids: tuple[str, ...] = ()
    expected_downgraded_entity_ids: tuple[str, ...] = ()
    expected_suppressed_entity_ids: tuple[str, ...] = ()
    expected_refinement_applied: bool = False


@dataclass(frozen=True)
class VectorGoldenSet:
    """One hosted vector-quality golden set for one pack."""

    pack_id: str
    profile: str = "default"
    description: str | None = None
    refinement_depth: Literal["light", "deep"] = "light"
    cases: tuple[VectorGoldenCase, ...] = ()


@dataclass(frozen=True)
class VectorCaseResult:
    """One evaluated vector-quality case result."""

    name: str
    case_kind: str
    seed_entity_ids: list[str] = field(default_factory=list)
    expected_related_entity_ids: list[str] = field(default_factory=list)
    actual_related_entity_ids: list[str] = field(default_factory=list)
    matched_related_entity_ids: list[str] = field(default_factory=list)
    missing_related_entity_ids: list[str] = field(default_factory=list)
    unexpected_related_entity_ids: list[str] = field(default_factory=list)
    expected_boosted_entity_ids: list[str] = field(default_factory=list)
    actual_boosted_entity_ids: list[str] = field(default_factory=list)
    expected_downgraded_entity_ids: list[str] = field(default_factory=list)
    actual_downgraded_entity_ids: list[str] = field(default_factory=list)
    expected_suppressed_entity_ids: list[str] = field(default_factory=list)
    actual_suppressed_entity_ids: list[str] = field(default_factory=list)
    expected_refinement_applied: bool = False
    actual_refinement_applied: bool = False
    graph_support_applied: bool = False
    fallback_used: bool = False
    refinement_score: float | None = None
    related_precision_at_k: float = 1.0
    related_recall_at_k: float = 1.0
    reciprocal_rank: float = 0.0
    timing_ms: int = 0
    warnings: list[str] = field(default_factory=list)
    passed: bool = True


@dataclass(frozen=True)
class VectorQualityReport:
    """Aggregate vector-quality report across one hosted golden set."""

    pack_id: str
    profile: str
    refinement_depth: str
    collection_alias: str | None = None
    provider: str | None = None
    case_count: int = 0
    top_k: int = 0
    related_precision_at_k: float = 1.0
    related_recall_at_k: float = 1.0
    related_mrr: float = 0.0
    suppression_alignment_rate: float = 1.0
    refinement_alignment_rate: float = 1.0
    easy_case_pass_rate: float = 1.0
    graph_support_rate: float = 0.0
    refinement_trigger_rate: float = 0.0
    fallback_rate: float = 0.0
    p50_latency_ms: int = 0
    p95_latency_ms: int = 0
    warnings: list[str] = field(default_factory=list)
    cases: list[VectorCaseResult] = field(default_factory=list)


@dataclass(frozen=True)
class VectorReleaseThresholds:
    """Release thresholds for the hosted vector-quality lane."""

    min_related_precision_at_k: float = 0.75
    min_related_recall_at_k: float = 0.60
    min_related_mrr: float = 0.70
    min_suppression_alignment_rate: float = 0.75
    min_refinement_alignment_rate: float = 0.75
    min_easy_case_pass_rate: float = 1.0
    max_fallback_rate: float = 0.10
    max_p95_latency_ms: int | None = 150


@dataclass(frozen=True)
class VectorReleaseThresholdDecision:
    """Structured decision for one stored vector-quality report."""

    passed: bool
    reasons: list[str] = field(default_factory=list)


DEFAULT_VECTOR_RELEASE_THRESHOLDS = VectorReleaseThresholds()


def vector_golden_set_path(
    pack_id: str,
    *,
    root: Path = DEFAULT_VECTOR_QUALITY_ROOT,
    profile: str = "default",
) -> Path:
    """Return the default on-disk path for one vector golden set."""

    return root / "golden_sets" / pack_id / f"{profile}.json"


def vector_quality_report_path(
    pack_id: str,
    *,
    root: Path = DEFAULT_VECTOR_QUALITY_ROOT,
    profile: str = "default",
) -> Path:
    """Return the default on-disk path for one vector-quality report."""

    filename = f"{pack_id}.vector.json" if profile == "default" else f"{pack_id}.{profile}.vector.json"
    return root / "reports" / filename


def write_vector_golden_set(path: str | Path, golden_set: VectorGoldenSet) -> Path:
    """Persist one vector golden set as JSON."""

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "pack_id": golden_set.pack_id,
        "profile": golden_set.profile,
        "description": golden_set.description,
        "refinement_depth": golden_set.refinement_depth,
        "cases": [
            {
                "name": case.name,
                "case_kind": case.case_kind,
                "seed_entities": [
                    {
                        "entity_id": seed.entity_id,
                        "canonical_text": seed.canonical_text,
                        "label": seed.label,
                        "score": seed.score,
                    }
                    for seed in case.seed_entities
                ],
                "expected_related_entity_ids": list(case.expected_related_entity_ids),
                "expected_boosted_entity_ids": list(case.expected_boosted_entity_ids),
                "expected_downgraded_entity_ids": list(case.expected_downgraded_entity_ids),
                "expected_suppressed_entity_ids": list(case.expected_suppressed_entity_ids),
                "expected_refinement_applied": case.expected_refinement_applied,
            }
            for case in golden_set.cases
        ],
    }
    destination.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return destination


def load_vector_golden_set(path: str | Path) -> VectorGoldenSet:
    """Load one vector golden set from JSON."""

    payload = json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))
    return VectorGoldenSet(
        pack_id=str(payload["pack_id"]),
        profile=str(payload.get("profile") or "default"),
        description=payload.get("description"),
        refinement_depth=_normalize_refinement_depth(payload.get("refinement_depth")),
        cases=tuple(
            VectorGoldenCase(
                name=str(item["name"]),
                case_kind=str(item.get("case_kind") or "related"),
                seed_entities=tuple(
                    VectorGoldenSeedEntity(
                        entity_id=str(seed["entity_id"]),
                        canonical_text=str(seed.get("canonical_text") or seed["entity_id"]),
                        label=str(seed["label"]),
                        score=float(seed.get("score", 1.0)),
                    )
                    for seed in item.get("seed_entities", [])
                ),
                expected_related_entity_ids=tuple(
                    str(entity_id)
                    for entity_id in item.get("expected_related_entity_ids", [])
                ),
                expected_boosted_entity_ids=tuple(
                    str(entity_id)
                    for entity_id in item.get("expected_boosted_entity_ids", [])
                ),
                expected_downgraded_entity_ids=tuple(
                    str(entity_id)
                    for entity_id in item.get("expected_downgraded_entity_ids", [])
                ),
                expected_suppressed_entity_ids=tuple(
                    str(entity_id)
                    for entity_id in item.get("expected_suppressed_entity_ids", [])
                ),
                expected_refinement_applied=bool(item.get("expected_refinement_applied", False)),
            )
            for item in payload.get("cases", [])
        ),
    )


def write_vector_quality_report(path: str | Path, report: VectorQualityReport) -> Path:
    """Persist one vector-quality report as JSON."""

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(_vector_quality_report_to_dict(report), indent=2) + "\n",
        encoding="utf-8",
    )
    return destination


def load_vector_quality_report(path: str | Path) -> VectorQualityReport:
    """Load one vector-quality report from JSON."""

    payload = json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))
    return VectorQualityReport(
        pack_id=str(payload["pack_id"]),
        profile=str(payload.get("profile") or "default"),
        refinement_depth=_normalize_refinement_depth(payload.get("refinement_depth")),
        collection_alias=_optional_string(payload.get("collection_alias")),
        provider=_optional_string(payload.get("provider")),
        case_count=int(payload.get("case_count", 0)),
        top_k=int(payload.get("top_k", 0)),
        related_precision_at_k=float(payload.get("related_precision_at_k", 1.0)),
        related_recall_at_k=float(payload.get("related_recall_at_k", 1.0)),
        related_mrr=float(payload.get("related_mrr", 0.0)),
        suppression_alignment_rate=float(payload.get("suppression_alignment_rate", 1.0)),
        refinement_alignment_rate=float(payload.get("refinement_alignment_rate", 1.0)),
        easy_case_pass_rate=float(payload.get("easy_case_pass_rate", 1.0)),
        graph_support_rate=float(payload.get("graph_support_rate", 0.0)),
        refinement_trigger_rate=float(payload.get("refinement_trigger_rate", 0.0)),
        fallback_rate=float(payload.get("fallback_rate", 0.0)),
        p50_latency_ms=int(payload.get("p50_latency_ms", 0)),
        p95_latency_ms=int(payload.get("p95_latency_ms", 0)),
        warnings=[str(item) for item in payload.get("warnings", [])],
        cases=[
            VectorCaseResult(
                name=str(item["name"]),
                case_kind=str(item.get("case_kind") or "related"),
                seed_entity_ids=[str(value) for value in item.get("seed_entity_ids", [])],
                expected_related_entity_ids=[
                    str(value) for value in item.get("expected_related_entity_ids", [])
                ],
                actual_related_entity_ids=[
                    str(value) for value in item.get("actual_related_entity_ids", [])
                ],
                matched_related_entity_ids=[
                    str(value) for value in item.get("matched_related_entity_ids", [])
                ],
                missing_related_entity_ids=[
                    str(value) for value in item.get("missing_related_entity_ids", [])
                ],
                unexpected_related_entity_ids=[
                    str(value) for value in item.get("unexpected_related_entity_ids", [])
                ],
                expected_boosted_entity_ids=[
                    str(value) for value in item.get("expected_boosted_entity_ids", [])
                ],
                actual_boosted_entity_ids=[
                    str(value) for value in item.get("actual_boosted_entity_ids", [])
                ],
                expected_downgraded_entity_ids=[
                    str(value) for value in item.get("expected_downgraded_entity_ids", [])
                ],
                actual_downgraded_entity_ids=[
                    str(value) for value in item.get("actual_downgraded_entity_ids", [])
                ],
                expected_suppressed_entity_ids=[
                    str(value) for value in item.get("expected_suppressed_entity_ids", [])
                ],
                actual_suppressed_entity_ids=[
                    str(value) for value in item.get("actual_suppressed_entity_ids", [])
                ],
                expected_refinement_applied=bool(item.get("expected_refinement_applied", False)),
                actual_refinement_applied=bool(item.get("actual_refinement_applied", False)),
                graph_support_applied=bool(item.get("graph_support_applied", False)),
                fallback_used=bool(item.get("fallback_used", False)),
                refinement_score=(
                    float(item["refinement_score"])
                    if item.get("refinement_score") is not None
                    else None
                ),
                related_precision_at_k=float(item.get("related_precision_at_k", 1.0)),
                related_recall_at_k=float(item.get("related_recall_at_k", 1.0)),
                reciprocal_rank=float(item.get("reciprocal_rank", 0.0)),
                timing_ms=int(item.get("timing_ms", 0)),
                warnings=[str(value) for value in item.get("warnings", [])],
                passed=bool(item.get("passed", True)),
            )
            for item in payload.get("cases", [])
        ],
    )


def default_vector_release_thresholds() -> VectorReleaseThresholds:
    """Return the locked default release thresholds for hosted vector quality."""

    return DEFAULT_VECTOR_RELEASE_THRESHOLDS


def evaluate_vector_release_thresholds(
    report: VectorQualityReport,
    *,
    thresholds: VectorReleaseThresholds,
) -> VectorReleaseThresholdDecision:
    """Evaluate one vector-quality report against release thresholds."""

    reasons: list[str] = []
    if report.related_precision_at_k < thresholds.min_related_precision_at_k:
        reasons.append(
            "related_precision_at_k_below_threshold:"
            f"{report.related_precision_at_k:.4f}<{thresholds.min_related_precision_at_k:.4f}"
        )
    if report.related_recall_at_k < thresholds.min_related_recall_at_k:
        reasons.append(
            "related_recall_at_k_below_threshold:"
            f"{report.related_recall_at_k:.4f}<{thresholds.min_related_recall_at_k:.4f}"
        )
    if report.related_mrr < thresholds.min_related_mrr:
        reasons.append(
            f"related_mrr_below_threshold:{report.related_mrr:.4f}<{thresholds.min_related_mrr:.4f}"
        )
    if report.suppression_alignment_rate < thresholds.min_suppression_alignment_rate:
        reasons.append(
            "suppression_alignment_rate_below_threshold:"
            f"{report.suppression_alignment_rate:.4f}"
            f"<{thresholds.min_suppression_alignment_rate:.4f}"
        )
    if report.refinement_alignment_rate < thresholds.min_refinement_alignment_rate:
        reasons.append(
            "refinement_alignment_rate_below_threshold:"
            f"{report.refinement_alignment_rate:.4f}"
            f"<{thresholds.min_refinement_alignment_rate:.4f}"
        )
    if report.easy_case_pass_rate < thresholds.min_easy_case_pass_rate:
        reasons.append(
            f"easy_case_pass_rate_below_threshold:{report.easy_case_pass_rate:.4f}"
            f"<{thresholds.min_easy_case_pass_rate:.4f}"
        )
    if report.fallback_rate > thresholds.max_fallback_rate:
        reasons.append(
            f"fallback_rate_above_threshold:{report.fallback_rate:.4f}>{thresholds.max_fallback_rate:.4f}"
        )
    if (
        thresholds.max_p95_latency_ms is not None
        and report.p95_latency_ms > thresholds.max_p95_latency_ms
    ):
        reasons.append(
            f"p95_latency_above_threshold:{report.p95_latency_ms}>{thresholds.max_p95_latency_ms}"
        )
    return VectorReleaseThresholdDecision(
        passed=not reasons,
        reasons=reasons,
    )


def evaluate_vector_golden_set(
    golden_set: VectorGoldenSet,
    *,
    settings: Settings,
    refinement_depth: Literal["light", "deep"] | None = None,
    top_k: int | None = None,
) -> VectorQualityReport:
    """Evaluate one vector golden set against the hosted QID enrichment lane."""

    resolved_depth = refinement_depth or golden_set.refinement_depth
    vector_settings = replace(
        settings,
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
    )
    resolved_top_k = top_k or vector_settings.vector_search_related_limit
    cases: list[VectorCaseResult] = []
    warnings: set[str] = set()
    total_related_returned = 0
    total_related_matched = 0
    total_expected_related = 0
    total_suppression_alignment_passes = 0
    total_alignment_passes = 0
    total_graph_support = 0
    total_refinement_triggers = 0
    total_fallbacks = 0
    easy_case_total = 0
    easy_case_passes = 0
    reciprocal_ranks: list[float] = []
    latencies: list[int] = []

    for case in golden_set.cases:
        response = _seed_response(golden_set.pack_id, case)
        started_at = time.perf_counter()
        enriched = enrich_tag_response_with_related_entities(
            response,
            settings=vector_settings,
            include_related_entities=True,
            include_graph_support=True,
            refine_links=True,
            refinement_depth=resolved_depth,
        )
        timing_ms = max(0, int((time.perf_counter() - started_at) * 1000))
        latencies.append(timing_ms)

        actual_related_entity_ids = [
            item.entity_id
            for item in enriched.related_entities[:resolved_top_k]
        ]
        expected_related_entity_ids = sorted(set(case.expected_related_entity_ids))
        actual_related_set = set(actual_related_entity_ids)
        expected_related_set = set(expected_related_entity_ids)
        matched_related_entity_ids = sorted(expected_related_set & actual_related_set)
        missing_related_entity_ids = sorted(expected_related_set - actual_related_set)
        unexpected_related_entity_ids = [
            entity_id
            for entity_id in actual_related_entity_ids
            if entity_id not in expected_related_set
        ]
        related_precision_at_k = _safe_ratio(
            len(matched_related_entity_ids),
            len(actual_related_entity_ids),
            when_zero=1.0 if not expected_related_entity_ids else 0.0,
        )
        related_recall_at_k = _safe_ratio(
            len(matched_related_entity_ids),
            len(expected_related_entity_ids),
            when_zero=1.0,
        )
        reciprocal_rank = _reciprocal_rank(actual_related_entity_ids, expected_related_set)
        if expected_related_entity_ids:
            reciprocal_ranks.append(reciprocal_rank)

        graph_support = enriched.graph_support
        graph_support_applied = bool(graph_support is not None and graph_support.applied)
        case_warnings = list(graph_support.warnings) if graph_support is not None else []
        fallback_used = not graph_support_applied
        actual_boosted_entity_ids = (
            list(graph_support.boosted_entity_ids) if graph_support is not None else []
        )
        actual_downgraded_entity_ids = (
            list(graph_support.downgraded_entity_ids) if graph_support is not None else []
        )
        actual_suppressed_entity_ids = (
            list(graph_support.suppressed_entity_ids) if graph_support is not None else []
        )
        expected_boosted_entity_ids = sorted(set(case.expected_boosted_entity_ids))
        expected_downgraded_entity_ids = sorted(set(case.expected_downgraded_entity_ids))
        expected_suppressed_entity_ids = sorted(set(case.expected_suppressed_entity_ids))
        actual_refinement_applied = bool(enriched.refinement_applied)

        related_ok = expected_related_set.issubset(actual_related_set)
        boosted_ok = sorted(actual_boosted_entity_ids) == expected_boosted_entity_ids
        downgraded_ok = sorted(actual_downgraded_entity_ids) == expected_downgraded_entity_ids
        suppressed_ok = sorted(actual_suppressed_entity_ids) == expected_suppressed_entity_ids
        refinement_ok = actual_refinement_applied == case.expected_refinement_applied
        passed = related_ok and boosted_ok and downgraded_ok and suppressed_ok and refinement_ok

        cases.append(
            VectorCaseResult(
                name=case.name,
                case_kind=case.case_kind,
                seed_entity_ids=[seed.entity_id for seed in case.seed_entities],
                expected_related_entity_ids=expected_related_entity_ids,
                actual_related_entity_ids=actual_related_entity_ids,
                matched_related_entity_ids=matched_related_entity_ids,
                missing_related_entity_ids=missing_related_entity_ids,
                unexpected_related_entity_ids=unexpected_related_entity_ids,
                expected_boosted_entity_ids=expected_boosted_entity_ids,
                actual_boosted_entity_ids=sorted(actual_boosted_entity_ids),
                expected_downgraded_entity_ids=expected_downgraded_entity_ids,
                actual_downgraded_entity_ids=sorted(actual_downgraded_entity_ids),
                expected_suppressed_entity_ids=expected_suppressed_entity_ids,
                actual_suppressed_entity_ids=sorted(actual_suppressed_entity_ids),
                expected_refinement_applied=case.expected_refinement_applied,
                actual_refinement_applied=actual_refinement_applied,
                graph_support_applied=graph_support_applied,
                fallback_used=fallback_used,
                refinement_score=enriched.refinement_score,
                related_precision_at_k=related_precision_at_k,
                related_recall_at_k=related_recall_at_k,
                reciprocal_rank=reciprocal_rank,
                timing_ms=timing_ms,
                warnings=case_warnings,
                passed=passed,
            )
        )

        warnings.update(case_warnings)
        total_related_returned += len(actual_related_entity_ids)
        total_related_matched += len(matched_related_entity_ids)
        total_expected_related += len(expected_related_entity_ids)
        total_suppression_alignment_passes += int(suppressed_ok)
        total_alignment_passes += int(boosted_ok and downgraded_ok and refinement_ok)
        total_graph_support += int(graph_support_applied)
        total_refinement_triggers += int(actual_refinement_applied)
        total_fallbacks += int(fallback_used)
        if case.case_kind == "easy_exact":
            easy_case_total += 1
            easy_case_passes += int(passed)

    case_count = len(cases)
    report = VectorQualityReport(
        pack_id=golden_set.pack_id,
        profile=golden_set.profile,
        refinement_depth=resolved_depth,
        collection_alias=vector_settings.vector_search_collection_alias,
        provider="qdrant.qid_graph",
        case_count=case_count,
        top_k=resolved_top_k,
        related_precision_at_k=_safe_ratio(
            total_related_matched,
            total_related_returned,
            when_zero=1.0 if total_expected_related == 0 else 0.0,
        ),
        related_recall_at_k=_safe_ratio(
            total_related_matched,
            total_expected_related,
            when_zero=1.0,
        ),
        related_mrr=round(
            _safe_ratio(sum(reciprocal_ranks), len(reciprocal_ranks), when_zero=0.0),
            6,
        ),
        suppression_alignment_rate=round(
            _safe_ratio(total_suppression_alignment_passes, case_count, when_zero=1.0),
            6,
        ),
        refinement_alignment_rate=round(
            _safe_ratio(total_alignment_passes, case_count, when_zero=1.0),
            6,
        ),
        easy_case_pass_rate=round(
            _safe_ratio(easy_case_passes, easy_case_total, when_zero=1.0),
            6,
        ),
        graph_support_rate=round(
            _safe_ratio(total_graph_support, case_count, when_zero=0.0),
            6,
        ),
        refinement_trigger_rate=round(
            _safe_ratio(total_refinement_triggers, case_count, when_zero=0.0),
            6,
        ),
        fallback_rate=round(
            _safe_ratio(total_fallbacks, case_count, when_zero=0.0),
            6,
        ),
        p50_latency_ms=_latency_percentile_ms(latencies, 50),
        p95_latency_ms=_latency_percentile_ms(latencies, 95),
        warnings=sorted(warnings),
        cases=cases,
    )
    return report


def _normalize_refinement_depth(value: object) -> Literal["light", "deep"]:
    normalized = str(value or "light").strip().casefold()
    if normalized not in {"light", "deep"}:
        raise ValueError("refinement_depth must be light or deep.")
    return normalized  # type: ignore[return-value]


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _seed_response(pack_id: str, case: VectorGoldenCase) -> TagResponse:
    entities: list[EntityMatch] = []
    cursor = 0
    for seed in case.seed_entities:
        start = cursor
        end = start + len(seed.canonical_text)
        cursor = end + 1
        entities.append(
            EntityMatch(
                text=seed.canonical_text,
                label=seed.label,
                start=start,
                end=end,
                confidence=seed.score,
                relevance=seed.score,
                link=EntityLink(
                    entity_id=seed.entity_id,
                    canonical_text=seed.canonical_text,
                    provider="golden.seed",
                ),
            )
        )
    return TagResponse(
        version=__version__,
        pack=pack_id,
        pack_version=None,
        language="en",
        content_type="text/plain",
        entities=entities,
        topics=[],
        warnings=[],
        timing_ms=0,
    )


def _safe_ratio(numerator: float, denominator: int | float, *, when_zero: float) -> float:
    if not denominator:
        return round(when_zero, 6)
    return round(float(numerator) / float(denominator), 6)


def _reciprocal_rank(actual_entity_ids: list[str], expected_entity_ids: set[str]) -> float:
    if not expected_entity_ids:
        return 0.0
    for index, entity_id in enumerate(actual_entity_ids, start=1):
        if entity_id in expected_entity_ids:
            return round(1.0 / index, 6)
    return 0.0


def _latency_percentile_ms(latencies: list[int], percentile: int) -> int:
    if not latencies:
        return 0
    values = sorted(latencies)
    if percentile <= 50:
        return int(median(values))
    if len(values) == 1:
        return int(values[0])
    rank = max(0, min(len(values) - 1, int(round((percentile / 100) * (len(values) - 1)))))
    return int(values[rank])


def _vector_quality_report_to_dict(report: VectorQualityReport) -> dict[str, object]:
    return {
        "pack_id": report.pack_id,
        "profile": report.profile,
        "refinement_depth": report.refinement_depth,
        "collection_alias": report.collection_alias,
        "provider": report.provider,
        "case_count": report.case_count,
        "top_k": report.top_k,
        "related_precision_at_k": report.related_precision_at_k,
        "related_recall_at_k": report.related_recall_at_k,
        "related_mrr": report.related_mrr,
        "suppression_alignment_rate": report.suppression_alignment_rate,
        "refinement_alignment_rate": report.refinement_alignment_rate,
        "easy_case_pass_rate": report.easy_case_pass_rate,
        "graph_support_rate": report.graph_support_rate,
        "refinement_trigger_rate": report.refinement_trigger_rate,
        "fallback_rate": report.fallback_rate,
        "p50_latency_ms": report.p50_latency_ms,
        "p95_latency_ms": report.p95_latency_ms,
        "warnings": list(report.warnings),
        "cases": [
            {
                "name": case.name,
                "case_kind": case.case_kind,
                "seed_entity_ids": list(case.seed_entity_ids),
                "expected_related_entity_ids": list(case.expected_related_entity_ids),
                "actual_related_entity_ids": list(case.actual_related_entity_ids),
                "matched_related_entity_ids": list(case.matched_related_entity_ids),
                "missing_related_entity_ids": list(case.missing_related_entity_ids),
                "unexpected_related_entity_ids": list(case.unexpected_related_entity_ids),
                "expected_boosted_entity_ids": list(case.expected_boosted_entity_ids),
                "actual_boosted_entity_ids": list(case.actual_boosted_entity_ids),
                "expected_downgraded_entity_ids": list(case.expected_downgraded_entity_ids),
                "actual_downgraded_entity_ids": list(case.actual_downgraded_entity_ids),
                "expected_suppressed_entity_ids": list(case.expected_suppressed_entity_ids),
                "actual_suppressed_entity_ids": list(case.actual_suppressed_entity_ids),
                "expected_refinement_applied": case.expected_refinement_applied,
                "actual_refinement_applied": case.actual_refinement_applied,
                "graph_support_applied": case.graph_support_applied,
                "fallback_used": case.fallback_used,
                "refinement_score": case.refinement_score,
                "related_precision_at_k": case.related_precision_at_k,
                "related_recall_at_k": case.related_recall_at_k,
                "reciprocal_rank": case.reciprocal_rank,
                "timing_ms": case.timing_ms,
                "warnings": list(case.warnings),
                "passed": case.passed,
            }
            for case in report.cases
        ],
    }
