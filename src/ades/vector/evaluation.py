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
from .qdrant import QdrantNearestPoint, QdrantVectorSearchClient, QdrantVectorSearchError
from .service import (
    _collection_alias_candidates,
    _query_filter_payload,
    enrich_tag_response_with_related_entities,
)

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
class VectorSeedAuditCase:
    """One direct seed-neighbor audit case across routed collection aliases."""

    name: str
    pack_id: str = "general-en"
    seed_entities: tuple[VectorGoldenSeedEntity, ...] = ()
    domain_hint: str | None = None
    country_hint: str | None = None
    top_k: int | None = None


@dataclass(frozen=True)
class VectorSeedAuditNeighbor:
    """One returned vector neighbor for one queried seed."""

    entity_id: str
    canonical_text: str | None = None
    entity_type: str | None = None
    source_name: str | None = None
    score: float = 0.0
    packs: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class VectorSeedAuditSeedResult:
    """Neighbors returned for one queried seed under one collection alias."""

    seed_entity_id: str
    seed_canonical_text: str
    seed_label: str
    neighbors: list[VectorSeedAuditNeighbor] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class VectorSeedAuditAliasResult:
    """Direct seed-neighbor audit result for one collection alias."""

    collection_alias: str
    filter_payload: dict[str, object] | None = None
    seed_results: list[VectorSeedAuditSeedResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class VectorSeedAuditCaseResult:
    """Audit result for one case across routed aliases."""

    name: str
    pack_id: str
    domain_hint: str | None = None
    country_hint: str | None = None
    alias_results: list[VectorSeedAuditAliasResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class VectorSeedAuditReport:
    """Aggregate direct seed-neighbor audit report."""

    provider: str | None = None
    case_count: int = 0
    top_k: int = 0
    collection_aliases_seen: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    cases: list[VectorSeedAuditCaseResult] = field(default_factory=list)


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


@dataclass(frozen=True)
class SavedVectorAuditReleaseThresholds:
    """Release thresholds for saved unseen-RSS vector-audit bundles."""

    min_case_count: int = 10
    min_neighbor_seed_hit_rate: float = 0.70
    min_related_entity_hit_rate: float = 0.10
    min_cases_with_related_entities: int = 3
    max_warning_case_rate: float = 0.10
    max_p95_hinted_timing_ms: int | None = 150
    max_location_seed_query_share: float = 0.40


@dataclass(frozen=True)
class SavedVectorAuditReleaseDecision:
    """Structured decision for one saved unseen-RSS vector-audit summary."""

    passed: bool
    reasons: list[str] = field(default_factory=list)


DEFAULT_SAVED_VECTOR_AUDIT_RELEASE_THRESHOLDS = SavedVectorAuditReleaseThresholds()


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


def write_vector_seed_audit_report(path: str | Path, report: VectorSeedAuditReport) -> Path:
    """Persist one direct seed-neighbor audit report as JSON."""

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(_vector_seed_audit_report_to_dict(report), indent=2) + "\n",
        encoding="utf-8",
    )
    return destination


def default_vector_release_thresholds() -> VectorReleaseThresholds:
    """Return the locked default release thresholds for hosted vector quality."""

    return DEFAULT_VECTOR_RELEASE_THRESHOLDS


def default_saved_vector_audit_release_thresholds() -> SavedVectorAuditReleaseThresholds:
    """Return the locked default thresholds for saved unseen-RSS promotion gates."""

    return DEFAULT_SAVED_VECTOR_AUDIT_RELEASE_THRESHOLDS


def _load_saved_vector_seed_audit_summary(
    summary: str | Path | dict[str, object],
) -> dict[str, object]:
    if isinstance(summary, dict):
        return summary
    return json.loads(Path(summary).expanduser().resolve().read_text(encoding="utf-8"))


def _summary_bucket_share(rows: object, *, value: str) -> float | None:
    if not isinstance(rows, list):
        return None
    total = 0
    matched = 0
    normalized_value = value.casefold()
    for item in rows:
        if not isinstance(item, dict):
            continue
        count = item.get("count")
        item_value = item.get("value")
        try:
            resolved_count = int(count)
        except (TypeError, ValueError):
            continue
        if resolved_count <= 0:
            continue
        total += resolved_count
        if str(item_value or "").strip().casefold() == normalized_value:
            matched += resolved_count
    if total <= 0:
        return None
    return matched / total


def evaluate_saved_vector_seed_audit_release(
    summary: str | Path | dict[str, object],
    *,
    thresholds: SavedVectorAuditReleaseThresholds,
) -> SavedVectorAuditReleaseDecision:
    """Evaluate one saved unseen-RSS vector-audit summary against promotion thresholds."""

    payload = _load_saved_vector_seed_audit_summary(summary)
    baseline_metrics = payload.get("baseline_metrics")
    seed_shape_analysis = payload.get("seed_shape_analysis")
    reasons: list[str] = []

    if not isinstance(baseline_metrics, dict):
        reasons.append("missing_baseline_metrics")
        return SavedVectorAuditReleaseDecision(passed=False, reasons=reasons)
    if not isinstance(seed_shape_analysis, dict):
        reasons.append("missing_seed_shape_analysis")
        return SavedVectorAuditReleaseDecision(passed=False, reasons=reasons)

    def _summary_int(value: object, *, default: int = 0) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    def _summary_float(value: object, *, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _summary_optional_int(value: object) -> int | None:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    case_count = _summary_int(baseline_metrics.get("case_count", 0))
    neighbor_seed_hit_rate = _summary_float(
        baseline_metrics.get("neighbor_seed_hit_rate", 0.0)
    )
    related_entity_hit_rate = _summary_float(
        baseline_metrics.get("hinted_related_entity_hit_rate", 0.0)
    )
    related_case_count = _summary_int(baseline_metrics.get("hinted_related_case_count", 0))
    warning_case_rate = _summary_float(
        baseline_metrics.get("hinted_warning_case_rate", 0.0)
    )
    p95_hinted_timing_ms = _summary_optional_int(
        baseline_metrics.get("p95_hinted_timing_ms")
    )
    location_seed_query_share = _summary_bucket_share(
        seed_shape_analysis.get("seed_query_label_counts"),
        value="location",
    )

    if case_count < thresholds.min_case_count:
        reasons.append(
            f"case_count_below_threshold:{case_count}<{thresholds.min_case_count}"
        )
    if neighbor_seed_hit_rate < thresholds.min_neighbor_seed_hit_rate:
        reasons.append(
            "neighbor_seed_hit_rate_below_threshold:"
            f"{neighbor_seed_hit_rate:.4f}<{thresholds.min_neighbor_seed_hit_rate:.4f}"
        )
    if related_entity_hit_rate < thresholds.min_related_entity_hit_rate:
        reasons.append(
            "related_entity_hit_rate_below_threshold:"
            f"{related_entity_hit_rate:.4f}<{thresholds.min_related_entity_hit_rate:.4f}"
        )
    if related_case_count < thresholds.min_cases_with_related_entities:
        reasons.append(
            "related_case_count_below_threshold:"
            f"{related_case_count}<{thresholds.min_cases_with_related_entities}"
        )
    if warning_case_rate > thresholds.max_warning_case_rate:
        reasons.append(
            "warning_case_rate_above_threshold:"
            f"{warning_case_rate:.4f}>{thresholds.max_warning_case_rate:.4f}"
        )
    if thresholds.max_p95_hinted_timing_ms is not None:
        if p95_hinted_timing_ms is None:
            reasons.append("p95_hinted_timing_ms_missing")
        elif p95_hinted_timing_ms > thresholds.max_p95_hinted_timing_ms:
            reasons.append(
                "p95_hinted_timing_ms_above_threshold:"
                f"{p95_hinted_timing_ms}>{thresholds.max_p95_hinted_timing_ms}"
            )
    if location_seed_query_share is None:
        reasons.append("location_seed_query_share_missing")
    elif location_seed_query_share > thresholds.max_location_seed_query_share:
        reasons.append(
            "location_seed_query_share_above_threshold:"
            f"{location_seed_query_share:.4f}>{thresholds.max_location_seed_query_share:.4f}"
        )

    return SavedVectorAuditReleaseDecision(
        passed=not reasons,
        reasons=reasons,
    )


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


def audit_vector_seed_neighbors(
    cases: VectorSeedAuditCase | list[VectorSeedAuditCase] | tuple[VectorSeedAuditCase, ...],
    *,
    settings: Settings,
    top_k: int | None = None,
    collection_alias_overrides: dict[str, str] | None = None,
) -> VectorSeedAuditReport:
    """Query routed Qdrant aliases directly for one set of linked seed entities."""

    vector_settings = replace(
        settings,
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
    )
    if not vector_settings.vector_search_enabled or not vector_settings.vector_search_url:
        raise ValueError("Vector seed audits require an enabled vector_search_url.")

    resolved_cases = (cases,) if isinstance(cases, VectorSeedAuditCase) else tuple(cases)
    default_top_k = max(1, int(top_k or vector_settings.vector_search_related_limit))
    alias_seen: list[str] = []
    report_warnings: set[str] = set()
    case_results: list[VectorSeedAuditCaseResult] = []

    with QdrantVectorSearchClient(
        vector_settings.vector_search_url,
        api_key=vector_settings.vector_search_api_key,
    ) as client:
        for case in resolved_cases:
            synthetic_response = _seed_response(
                case.pack_id,
                VectorGoldenCase(
                    name=case.name,
                    seed_entities=case.seed_entities,
                ),
            )
            filter_payload = _query_filter_payload(
                synthetic_response,
                settings=vector_settings,
                domain_hint=case.domain_hint,
                country_hint=case.country_hint,
            )
            collection_aliases = _collection_alias_candidates(
                synthetic_response,
                settings=vector_settings,
                domain_hint=case.domain_hint,
                country_hint=case.country_hint,
            )
            if collection_alias_overrides:
                remapped_aliases: list[str] = []
                for collection_alias in collection_aliases:
                    resolved_alias = (
                        collection_alias_overrides.get(collection_alias, collection_alias).strip()
                    )
                    if resolved_alias and resolved_alias not in remapped_aliases:
                        remapped_aliases.append(resolved_alias)
                collection_aliases = remapped_aliases
            alias_results: list[VectorSeedAuditAliasResult] = []
            case_warnings: set[str] = set()
            resolved_top_k = max(1, int(case.top_k or default_top_k))

            if not case.seed_entities:
                case_warnings.add("no_seed_entities")
            if not collection_aliases:
                case_warnings.add("no_collection_aliases")

            for collection_alias in collection_aliases:
                if collection_alias not in alias_seen:
                    alias_seen.append(collection_alias)
                alias_seed_results: list[VectorSeedAuditSeedResult] = []
                alias_warnings: set[str] = set()
                for seed in case.seed_entities:
                    try:
                        raw_neighbors = client.query_similar_by_id(
                            collection_alias,
                            point_id=seed.entity_id,
                            limit=resolved_top_k + 1,
                            filter_payload=filter_payload,
                        )
                    except QdrantVectorSearchError as exc:
                        warning = (
                            f"seed_missing:{collection_alias}:{seed.entity_id}"
                            if exc.status_code == 404
                            else f"query_failed:{collection_alias}:{seed.entity_id}:{str(exc).strip()}"
                        )
                        alias_warnings.add(warning)
                        case_warnings.add(warning)
                        alias_seed_results.append(
                            VectorSeedAuditSeedResult(
                                seed_entity_id=seed.entity_id,
                                seed_canonical_text=seed.canonical_text,
                                seed_label=seed.label,
                                neighbors=[],
                                warnings=[warning],
                            )
                        )
                        continue

                    neighbors = _audit_neighbors_from_points(
                        raw_neighbors,
                        seed_entity_id=seed.entity_id,
                        top_k=resolved_top_k,
                    )
                    alias_seed_results.append(
                        VectorSeedAuditSeedResult(
                            seed_entity_id=seed.entity_id,
                            seed_canonical_text=seed.canonical_text,
                            seed_label=seed.label,
                            neighbors=neighbors,
                            warnings=[],
                        )
                    )

                alias_results.append(
                    VectorSeedAuditAliasResult(
                        collection_alias=collection_alias,
                        filter_payload=dict(filter_payload) if filter_payload is not None else None,
                        seed_results=alias_seed_results,
                        warnings=sorted(alias_warnings),
                    )
                )

            case_results.append(
                VectorSeedAuditCaseResult(
                    name=case.name,
                    pack_id=case.pack_id,
                    domain_hint=case.domain_hint,
                    country_hint=case.country_hint,
                    alias_results=alias_results,
                    warnings=sorted(case_warnings),
                )
            )
            report_warnings.update(case_warnings)

    return VectorSeedAuditReport(
        provider="qdrant.qid_graph",
        case_count=len(case_results),
        top_k=default_top_k,
        collection_aliases_seen=alias_seen,
        warnings=sorted(report_warnings),
        cases=case_results,
    )


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


def _audit_neighbors_from_points(
    points: list[QdrantNearestPoint],
    *,
    seed_entity_id: str,
    top_k: int,
) -> list[VectorSeedAuditNeighbor]:
    neighbors: list[VectorSeedAuditNeighbor] = []
    for item in points:
        payload = item.payload if isinstance(item.payload, dict) else {}
        payload_entity_id = _optional_string(payload.get("entity_id"))
        entity_id = payload_entity_id or item.point_id
        if entity_id == seed_entity_id:
            continue
        packs = payload.get("packs")
        neighbors.append(
            VectorSeedAuditNeighbor(
                entity_id=entity_id,
                canonical_text=_optional_string(payload.get("canonical_text")),
                entity_type=_optional_string(payload.get("entity_type")),
                source_name=_optional_string(payload.get("source_name")),
                score=float(item.score),
                packs=[str(value) for value in packs] if isinstance(packs, list) else [],
            )
        )
        if len(neighbors) >= top_k:
            break
    return neighbors


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


def _vector_seed_audit_report_to_dict(report: VectorSeedAuditReport) -> dict[str, object]:
    return {
        "provider": report.provider,
        "case_count": report.case_count,
        "top_k": report.top_k,
        "collection_aliases_seen": list(report.collection_aliases_seen),
        "warnings": list(report.warnings),
        "cases": [
            {
                "name": case.name,
                "pack_id": case.pack_id,
                "domain_hint": case.domain_hint,
                "country_hint": case.country_hint,
                "warnings": list(case.warnings),
                "alias_results": [
                    {
                        "collection_alias": alias.collection_alias,
                        "filter_payload": alias.filter_payload,
                        "warnings": list(alias.warnings),
                        "seed_results": [
                            {
                                "seed_entity_id": seed.seed_entity_id,
                                "seed_canonical_text": seed.seed_canonical_text,
                                "seed_label": seed.seed_label,
                                "warnings": list(seed.warnings),
                                "neighbors": [
                                    {
                                        "entity_id": neighbor.entity_id,
                                        "canonical_text": neighbor.canonical_text,
                                        "entity_type": neighbor.entity_type,
                                        "source_name": neighbor.source_name,
                                        "score": neighbor.score,
                                        "packs": list(neighbor.packs),
                                    }
                                    for neighbor in seed.neighbors
                                ],
                            }
                            for seed in alias.seed_results
                        ],
                    }
                    for alias in case.alias_results
                ],
            }
            for case in report.cases
        ],
    }
