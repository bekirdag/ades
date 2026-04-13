"""Golden-set evaluation helpers for entity extraction quality."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from statistics import quantiles
import time

from .pipeline.tagger import tag_text
from .service.models import TagResponse
from .text_processing import normalize_lookup_text

DEFAULT_EXTRACTION_QUALITY_ROOT = Path("/mnt/githubActions/ades_big_data/extraction_quality")


@dataclass(frozen=True)
class GoldenEntity:
    """One expected extracted entity inside a golden document."""

    text: str
    label: str


@dataclass(frozen=True)
class GoldenDocument:
    """One golden-set document with expected entities."""

    name: str
    text: str
    expected_entities: tuple[GoldenEntity, ...] = ()
    content_type: str = "text/plain"


@dataclass(frozen=True)
class GoldenSet:
    """One extraction-quality golden set for one pack and mode."""

    pack_id: str
    profile: str = "default"
    description: str | None = None
    documents: tuple[GoldenDocument, ...] = ()


@dataclass(frozen=True)
class ExtractionCaseResult:
    """One evaluated document result."""

    name: str
    expected_entities: list[GoldenEntity] = field(default_factory=list)
    actual_entities: list[GoldenEntity] = field(default_factory=list)
    matched_entities: list[GoldenEntity] = field(default_factory=list)
    missing_entities: list[GoldenEntity] = field(default_factory=list)
    unexpected_entities: list[GoldenEntity] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    token_count: int = 0
    entity_count: int = 0
    entities_per_100_tokens: float = 0.0
    overlap_drop_count: int = 0
    chunk_count: int = 0
    timing_ms: int = 0
    passed: bool = True


@dataclass(frozen=True)
class ExtractionQualityReport:
    """Aggregate extraction-quality result across one golden set."""

    pack_id: str
    profile: str
    hybrid: bool
    document_count: int
    expected_entity_count: int
    actual_entity_count: int
    matched_entity_count: int
    missing_entity_count: int
    unexpected_entity_count: int
    recall: float
    precision: float
    entities_per_100_tokens: float
    overlap_drop_count: int
    chunk_count: int
    low_density_warning_count: int
    p50_latency_ms: int = 0
    p95_latency_ms: int = 0
    per_label_expected: dict[str, int] = field(default_factory=dict)
    per_label_actual: dict[str, int] = field(default_factory=dict)
    per_label_matched: dict[str, int] = field(default_factory=dict)
    per_label_recall: dict[str, float] = field(default_factory=dict)
    per_lane_counts: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    cases: list[ExtractionCaseResult] = field(default_factory=list)


@dataclass(frozen=True)
class LabelQualityDelta:
    """Per-label delta between two quality reports."""

    label: str
    baseline_recall: float
    candidate_recall: float
    recall_delta: float


@dataclass(frozen=True)
class ExtractionQualityDelta:
    """Aggregate delta between two quality reports."""

    pack_id: str
    baseline_mode: str
    candidate_mode: str
    recall_delta: float
    precision_delta: float
    p95_latency_delta_ms: int
    per_label_deltas: list[LabelQualityDelta] = field(default_factory=list)


@dataclass(frozen=True)
class PackHealthSummary:
    """Aggregated recent extraction-health telemetry for one installed pack."""

    pack_id: str
    requested_window: int
    observation_count: int
    latest_pack_version: str | None = None
    latest_observed_at: str | None = None
    average_entity_count: float = 0.0
    average_entities_per_100_tokens: float = 0.0
    zero_entity_rate: float = 0.0
    warning_rate: float = 0.0
    low_density_warning_rate: float = 0.0
    p95_timing_ms: int = 0
    per_label_counts: dict[str, int] = field(default_factory=dict)
    per_lane_counts: dict[str, int] = field(default_factory=dict)
    warning_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeLatencyBenchmark:
    """One benchmark latency summary."""

    sample_count: int
    p50_latency_ms: int
    p95_latency_ms: int


@dataclass(frozen=True)
class RuntimeDiskBenchmark:
    """Disk-usage summary for runtime benchmark reporting."""

    runtime_pack_bytes: int
    matcher_artifact_bytes: int
    matcher_entries_bytes: int
    matcher_total_bytes: int
    metadata_store_bytes: int | None = None


@dataclass(frozen=True)
class RuntimeBenchmarkReport:
    """Consolidated runtime benchmark report for one installed pack."""

    pack_id: str
    profile: str
    hybrid: bool
    metadata_backend: str
    exact_extraction_backend: str
    operator_lookup_backend: str
    matcher_entry_count: int
    matcher_state_count: int
    matcher_cold_load_ms: int
    lookup_query_count: int
    warm_tagging: RuntimeLatencyBenchmark
    operator_lookup: RuntimeLatencyBenchmark
    disk_usage: RuntimeDiskBenchmark
    quality_report: ExtractionQualityReport
    warnings: list[str] = field(default_factory=list)


def golden_set_path(
    pack_id: str,
    *,
    root: Path = DEFAULT_EXTRACTION_QUALITY_ROOT,
    profile: str = "default",
    ) -> Path:
    """Return the default on-disk path for one golden set."""

    return root / "golden_sets" / pack_id / f"{profile}.json"


def quality_report_path(
    pack_id: str,
    *,
    root: Path = DEFAULT_EXTRACTION_QUALITY_ROOT,
    profile: str = "default",
    hybrid: bool = False,
) -> Path:
    """Return the default on-disk path for one quality report."""

    mode = "hybrid" if hybrid else "deterministic"
    if profile == "default":
        filename = f"{pack_id}.{mode}.json"
    else:
        filename = f"{pack_id}.{profile}.{mode}.json"
    return root / "reports" / filename


def benchmark_report_path(
    pack_id: str,
    *,
    root: Path = DEFAULT_EXTRACTION_QUALITY_ROOT,
    profile: str = "default",
    hybrid: bool = False,
) -> Path:
    """Return the default on-disk path for one runtime benchmark report."""

    mode = "hybrid" if hybrid else "deterministic"
    if profile == "default":
        filename = f"{pack_id}.{mode}.benchmark.json"
    else:
        filename = f"{pack_id}.{profile}.{mode}.benchmark.json"
    return root / "reports" / filename


def write_golden_set(path: str | Path, golden_set: GoldenSet) -> Path:
    """Persist one golden set as JSON."""

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "pack_id": golden_set.pack_id,
        "profile": golden_set.profile,
        "description": golden_set.description,
        "documents": [
            {
                "name": item.name,
                "document_id": item.name,
                "text": item.text,
                "content_type": item.content_type,
                "expected_entities": [
                    {"text": entity.text, "label": entity.label}
                    for entity in item.expected_entities
                ],
            }
            for item in golden_set.documents
        ],
    }
    destination.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return destination


def load_golden_set(path: str | Path) -> GoldenSet:
    """Load one golden set from JSON."""

    payload = json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))
    return GoldenSet(
        pack_id=str(payload["pack_id"]),
        profile=str(payload.get("profile") or "default"),
        description=payload.get("description"),
        documents=tuple(
            GoldenDocument(
                name=str(item.get("name") or item.get("document_id")),
                text=str(item["text"]),
                content_type=str(item.get("content_type") or "text/plain"),
                expected_entities=tuple(
                    GoldenEntity(text=str(entity["text"]), label=str(entity["label"]))
                    for entity in item.get("expected_entities", [])
                ),
            )
            for item in payload.get("documents", [])
        ),
    )


def write_quality_report(path: str | Path, report: ExtractionQualityReport) -> Path:
    """Persist one extraction-quality report as JSON."""

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(_quality_report_to_dict(report), indent=2) + "\n",
        encoding="utf-8",
    )
    return destination


def write_runtime_benchmark_report(
    path: str | Path,
    report: RuntimeBenchmarkReport,
) -> Path:
    """Persist one runtime benchmark report as JSON."""

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(_runtime_benchmark_report_to_dict(report), indent=2) + "\n",
        encoding="utf-8",
    )
    return destination


def load_quality_report(path: str | Path) -> ExtractionQualityReport:
    """Load one extraction-quality report from JSON."""

    payload = json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))
    return ExtractionQualityReport(
        pack_id=str(payload["pack_id"]),
        profile=str(payload.get("profile") or "default"),
        hybrid=bool(payload.get("hybrid", False)),
        document_count=int(payload.get("document_count", 0)),
        expected_entity_count=int(payload.get("expected_entity_count", 0)),
        actual_entity_count=int(payload.get("actual_entity_count", 0)),
        matched_entity_count=int(payload.get("matched_entity_count", 0)),
        missing_entity_count=int(payload.get("missing_entity_count", 0)),
        unexpected_entity_count=int(payload.get("unexpected_entity_count", 0)),
        recall=float(payload.get("recall", 1.0)),
        precision=float(payload.get("precision", 1.0)),
        entities_per_100_tokens=float(payload.get("entities_per_100_tokens", 0.0)),
        overlap_drop_count=int(payload.get("overlap_drop_count", 0)),
        chunk_count=int(payload.get("chunk_count", 0)),
        low_density_warning_count=int(payload.get("low_density_warning_count", 0)),
        p50_latency_ms=int(payload.get("p50_latency_ms", 0)),
        p95_latency_ms=int(payload.get("p95_latency_ms", 0)),
        per_label_expected=_int_dict(payload.get("per_label_expected")),
        per_label_actual=_int_dict(payload.get("per_label_actual")),
        per_label_matched=_int_dict(payload.get("per_label_matched")),
        per_label_recall=_float_dict(payload.get("per_label_recall")),
        per_lane_counts=_int_dict(payload.get("per_lane_counts")),
        warnings=[str(item) for item in payload.get("warnings", [])],
        cases=[
            ExtractionCaseResult(
                name=str(item["name"]),
                expected_entities=_load_entities(item.get("expected_entities")),
                actual_entities=_load_entities(item.get("actual_entities")),
                matched_entities=_load_entities(item.get("matched_entities")),
                missing_entities=_load_entities(item.get("missing_entities")),
                unexpected_entities=_load_entities(item.get("unexpected_entities")),
                warnings=[str(warning) for warning in item.get("warnings", [])],
                token_count=int(item.get("token_count", 0)),
                entity_count=int(item.get("entity_count", 0)),
                entities_per_100_tokens=float(item.get("entities_per_100_tokens", 0.0)),
                overlap_drop_count=int(item.get("overlap_drop_count", 0)),
                chunk_count=int(item.get("chunk_count", 0)),
                timing_ms=int(item.get("timing_ms", 0)),
                passed=bool(item.get("passed", True)),
            )
            for item in payload.get("cases", [])
        ],
    )


def evaluate_golden_set(
    golden_set: GoldenSet,
    *,
    storage_root: str | Path,
    hybrid: bool | None = None,
) -> ExtractionQualityReport:
    """Evaluate one installed pack against one golden set."""

    storage_path = Path(storage_root).expanduser().resolve()
    case_results: list[ExtractionCaseResult] = []
    expected_total = 0
    actual_total = 0
    matched_total = 0
    missing_total = 0
    unexpected_total = 0
    overlap_drop_total = 0
    chunk_total = 0
    low_density_warning_count = 0
    latency_values: list[int] = []
    token_total = 0
    entity_density_total = 0.0
    per_label_expected: Counter[str] = Counter()
    per_label_actual: Counter[str] = Counter()
    per_label_matched: Counter[str] = Counter()
    per_lane_counts: Counter[str] = Counter()
    warnings: list[str] = []

    for document in golden_set.documents:
        response = tag_text(
            text=document.text,
            pack=golden_set.pack_id,
            content_type=document.content_type,
            storage_root=storage_path,
            hybrid=hybrid,
        )
        case_result = _evaluate_case(document, response)
        case_results.append(case_result)
        expected_total += len(case_result.expected_entities)
        actual_total += len(case_result.actual_entities)
        matched_total += len(case_result.matched_entities)
        missing_total += len(case_result.missing_entities)
        unexpected_total += len(case_result.unexpected_entities)
        overlap_drop_total += case_result.overlap_drop_count
        chunk_total += case_result.chunk_count
        token_total += case_result.token_count
        entity_density_total += case_result.entities_per_100_tokens
        latency_values.append(case_result.timing_ms)
        warnings.extend(case_result.warnings)
        low_density_warning_count += sum(
            1 for warning in case_result.warnings if warning.startswith("low_entity_density:")
        )
        for entity in case_result.expected_entities:
            per_label_expected[entity.label] += 1
        for entity in case_result.actual_entities:
            per_label_actual[entity.label] += 1
        for entity in case_result.matched_entities:
            per_label_matched[entity.label] += 1
        if response.metrics is not None:
            for lane, count in response.metrics.per_lane_counts.items():
                per_lane_counts[lane] += int(count)

    recall = round(matched_total / expected_total, 4) if expected_total else 1.0
    precision = round(matched_total / actual_total, 4) if actual_total else 1.0
    per_label_recall = {
        label: round(per_label_matched[label] / count, 4) if count else 1.0
        for label, count in sorted(per_label_expected.items())
    }
    p50_latency_ms = _p50(latency_values)
    p95_latency_ms = _p95(latency_values)
    average_density = round(entity_density_total / len(case_results), 4) if case_results else 0.0
    return ExtractionQualityReport(
        pack_id=golden_set.pack_id,
        profile=golden_set.profile,
        hybrid=bool(hybrid),
        document_count=len(case_results),
        expected_entity_count=expected_total,
        actual_entity_count=actual_total,
        matched_entity_count=matched_total,
        missing_entity_count=missing_total,
        unexpected_entity_count=unexpected_total,
        recall=recall,
        precision=precision,
        entities_per_100_tokens=average_density,
        overlap_drop_count=overlap_drop_total,
        chunk_count=chunk_total,
        low_density_warning_count=low_density_warning_count,
        p50_latency_ms=p50_latency_ms,
        p95_latency_ms=p95_latency_ms,
        per_label_expected=dict(sorted(per_label_expected.items())),
        per_label_actual=dict(sorted(per_label_actual.items())),
        per_label_matched=dict(sorted(per_label_matched.items())),
        per_label_recall=per_label_recall,
        per_lane_counts=dict(sorted(per_lane_counts.items())),
        warnings=warnings,
        cases=case_results,
    )


def benchmark_runtime_pack(
    golden_set: GoldenSet,
    *,
    storage_root: str | Path,
    runtime_target=None,
    metadata_backend=None,
    database_url: str | None = None,
    hybrid: bool | None = None,
    warm_runs: int = 3,
    lookup_limit: int = 20,
) -> RuntimeBenchmarkReport:
    """Benchmark the runtime matcher, tagger hot path, and operator lookup surfaces."""

    from .packs.registry import PackRegistry
    from .packs.runtime import load_pack_runtime
    from .runtime_matcher import clear_runtime_matcher_cache, load_runtime_matcher

    storage_path = Path(storage_root).expanduser().resolve()
    registry = PackRegistry(
        storage_path,
        runtime_target=runtime_target,
        metadata_backend=metadata_backend,
        database_url=database_url,
    )
    runtime = load_pack_runtime(
        storage_path,
        golden_set.pack_id,
        registry=registry,
    )
    if runtime is None:
        raise ValueError(f"Installed runtime pack not found or inactive: {golden_set.pack_id}")

    quality_report = evaluate_golden_set(
        golden_set,
        storage_root=storage_path,
        hybrid=hybrid,
    )
    warnings: list[str] = []

    matcher_entry_count = 0
    matcher_state_count = 0
    matcher_cold_load_ms = 0
    if runtime.matchers:
        clear_runtime_matcher_cache()
        load_started = time.perf_counter()
        loaded_matchers = [
            load_runtime_matcher(matcher.artifact_path, matcher.entries_path)
            for matcher in runtime.matchers
        ]
        matcher_cold_load_ms = int(round((time.perf_counter() - load_started) * 1000))
        matcher_entry_count = sum(item.entry_count for item in loaded_matchers)
        matcher_state_count = sum(item.state_count for item in loaded_matchers)
    else:
        warnings.append(f"matcher_not_available:{golden_set.pack_id}")

    warm_samples: list[int] = []
    for _ in range(max(1, int(warm_runs))):
        for document in golden_set.documents:
            started = time.perf_counter()
            tag_text(
                text=document.text,
                pack=golden_set.pack_id,
                content_type=document.content_type,
                storage_root=storage_path,
                hybrid=hybrid,
            )
            warm_samples.append(int(round((time.perf_counter() - started) * 1000)))

    lookup_queries = _benchmark_lookup_queries(golden_set)
    lookup_samples: list[int] = []
    if lookup_queries:
        for query in lookup_queries:
            started = time.perf_counter()
            registry.lookup_candidates(
                query,
                pack_id=golden_set.pack_id,
                exact_alias=False,
                active_only=True,
                limit=max(1, int(lookup_limit)),
            )
            lookup_samples.append(int(round((time.perf_counter() - started) * 1000)))
    else:
        warnings.append(f"no_lookup_queries:{golden_set.pack_id}")

    pack_dir = registry.layout.packs_dir / golden_set.pack_id
    matcher_artifact_bytes = sum(
        _path_size(Path(item.artifact_path))
        for item in runtime.matchers
    )
    matcher_entries_bytes = sum(
        _path_size(Path(item.entries_path))
        for item in runtime.matchers
    )
    disk_usage = RuntimeDiskBenchmark(
        runtime_pack_bytes=_path_size(pack_dir),
        matcher_artifact_bytes=matcher_artifact_bytes,
        matcher_entries_bytes=matcher_entries_bytes,
        matcher_total_bytes=matcher_artifact_bytes + matcher_entries_bytes,
        metadata_store_bytes=_metadata_store_bytes(
            storage_root=storage_path,
            metadata_backend=metadata_backend,
            database_url=database_url,
        ),
    )
    return RuntimeBenchmarkReport(
        pack_id=golden_set.pack_id,
        profile=golden_set.profile,
        hybrid=bool(hybrid),
        metadata_backend=_backend_name(metadata_backend, default="sqlite"),
        exact_extraction_backend="compiled_matcher" if runtime.matchers else "metadata_store_lookup",
        operator_lookup_backend=_operator_lookup_backend_name(metadata_backend),
        matcher_entry_count=matcher_entry_count,
        matcher_state_count=matcher_state_count,
        matcher_cold_load_ms=matcher_cold_load_ms,
        lookup_query_count=len(lookup_queries),
        warm_tagging=RuntimeLatencyBenchmark(
            sample_count=len(warm_samples),
            p50_latency_ms=_p50(warm_samples),
            p95_latency_ms=_p95(warm_samples),
        ),
        operator_lookup=RuntimeLatencyBenchmark(
            sample_count=len(lookup_samples),
            p50_latency_ms=_p50(lookup_samples),
            p95_latency_ms=_p95(lookup_samples),
        ),
        disk_usage=disk_usage,
        quality_report=quality_report,
        warnings=warnings,
    )


def compare_quality_reports(
    baseline: ExtractionQualityReport,
    candidate: ExtractionQualityReport,
) -> ExtractionQualityDelta:
    """Return one delta object between two evaluation reports."""

    labels = sorted(set(baseline.per_label_expected) | set(candidate.per_label_expected))
    per_label_deltas = [
        LabelQualityDelta(
            label=label,
            baseline_recall=baseline.per_label_recall.get(label, 1.0),
            candidate_recall=candidate.per_label_recall.get(label, 1.0),
            recall_delta=round(
                candidate.per_label_recall.get(label, 1.0)
                - baseline.per_label_recall.get(label, 1.0),
                4,
            ),
        )
        for label in labels
    ]
    return ExtractionQualityDelta(
        pack_id=candidate.pack_id,
        baseline_mode="hybrid" if baseline.hybrid else "deterministic",
        candidate_mode="hybrid" if candidate.hybrid else "deterministic",
        recall_delta=round(candidate.recall - baseline.recall, 4),
        precision_delta=round(candidate.precision - baseline.precision, 4),
        p95_latency_delta_ms=candidate.p95_latency_ms - baseline.p95_latency_ms,
        per_label_deltas=per_label_deltas,
    )


def pack_health_db_path(storage_root: str | Path) -> Path:
    """Return the on-disk SQLite path used for pack-health telemetry."""

    root = Path(storage_root).expanduser().resolve()
    return root / "registry" / "pack_health.sqlite3"


def record_tag_observation(storage_root: str | Path, response: TagResponse) -> None:
    """Persist one compact tag-observation row for later pack-health aggregation."""

    if response.pack_version is None or response.metrics is None:
        return
    warnings = [str(item) for item in response.warnings]
    if any(item.startswith("pack_not_installed:") for item in warnings):
        return
    connection = _connect_pack_health_db(storage_root)
    try:
        metrics = response.metrics
        low_density_warning = any(
            item.startswith("low_entity_density:") for item in warnings
        )
        connection.execute(
            """
            INSERT INTO tag_observations (
                observed_at,
                pack_id,
                pack_version,
                token_count,
                entity_count,
                entities_per_100_tokens,
                overlap_drop_count,
                chunk_count,
                timing_ms,
                low_density_warning,
                warnings_json,
                per_label_counts_json,
                per_lane_counts_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                response.pack,
                response.pack_version,
                metrics.token_count,
                metrics.entity_count,
                metrics.entities_per_100_tokens,
                metrics.overlap_drop_count,
                metrics.chunk_count,
                response.timing_ms,
                1 if low_density_warning else 0,
                json.dumps(warnings, sort_keys=True),
                json.dumps(metrics.per_label_counts, sort_keys=True),
                json.dumps(metrics.per_lane_counts, sort_keys=True),
            ),
        )
        connection.commit()
    finally:
        connection.close()


def get_pack_health_summary(
    pack_id: str,
    *,
    storage_root: str | Path,
    limit: int = 100,
) -> dict[str, object]:
    """Return aggregated recent extraction-health telemetry for one installed pack."""

    bounded_limit = max(1, min(int(limit), 1000))
    connection = _connect_pack_health_db(storage_root)
    try:
        rows = connection.execute(
            """
            SELECT
                observed_at,
                pack_version,
                entity_count,
                entities_per_100_tokens,
                timing_ms,
                low_density_warning,
                warnings_json,
                per_label_counts_json,
                per_lane_counts_json
            FROM tag_observations
            WHERE pack_id = ?
            ORDER BY observed_at DESC, id DESC
            LIMIT ?
            """,
            (pack_id, bounded_limit),
        ).fetchall()
    finally:
        connection.close()

    if not rows:
        return PackHealthSummary(
            pack_id=pack_id,
            requested_window=bounded_limit,
            observation_count=0,
        ).__dict__

    per_label_counts: Counter[str] = Counter()
    per_lane_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    entity_counts: list[int] = []
    density_values: list[float] = []
    timing_values: list[int] = []
    zero_entity_count = 0
    warned_count = 0
    low_density_count = 0

    for row in rows:
        entity_count = int(row["entity_count"])
        density_value = float(row["entities_per_100_tokens"])
        timing_value = int(row["timing_ms"])
        entity_counts.append(entity_count)
        density_values.append(density_value)
        timing_values.append(timing_value)
        if entity_count == 0:
            zero_entity_count += 1
        if bool(row["low_density_warning"]):
            low_density_count += 1

        warnings_payload = _list_str(json.loads(str(row["warnings_json"] or "[]")))
        if warnings_payload:
            warned_count += 1
            for warning in warnings_payload:
                warning_counts[warning] += 1

        for label, count in _int_dict(json.loads(str(row["per_label_counts_json"] or "{}"))).items():
            per_label_counts[label] += count
        for lane, count in _int_dict(json.loads(str(row["per_lane_counts_json"] or "{}"))).items():
            per_lane_counts[lane] += count

    observation_count = len(rows)
    summary = PackHealthSummary(
        pack_id=pack_id,
        requested_window=bounded_limit,
        observation_count=observation_count,
        latest_pack_version=str(rows[0]["pack_version"]) if rows[0]["pack_version"] is not None else None,
        latest_observed_at=str(rows[0]["observed_at"]) if rows[0]["observed_at"] is not None else None,
        average_entity_count=round(sum(entity_counts) / observation_count, 4),
        average_entities_per_100_tokens=round(sum(density_values) / observation_count, 4),
        zero_entity_rate=round(zero_entity_count / observation_count, 4),
        warning_rate=round(warned_count / observation_count, 4),
        low_density_warning_rate=round(low_density_count / observation_count, 4),
        p95_timing_ms=_p95(timing_values),
        per_label_counts=dict(sorted(per_label_counts.items())),
        per_lane_counts=dict(sorted(per_lane_counts.items())),
        warning_counts=dict(sorted(warning_counts.items())),
    )
    return summary.__dict__


def _connect_pack_health_db(storage_root: str | Path) -> sqlite3.Connection:
    path = pack_health_db_path(storage_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS tag_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_at TEXT NOT NULL,
            pack_id TEXT NOT NULL,
            pack_version TEXT,
            token_count INTEGER NOT NULL,
            entity_count INTEGER NOT NULL,
            entities_per_100_tokens REAL NOT NULL,
            overlap_drop_count INTEGER NOT NULL,
            chunk_count INTEGER NOT NULL,
            timing_ms INTEGER NOT NULL,
            low_density_warning INTEGER NOT NULL DEFAULT 0,
            warnings_json TEXT NOT NULL,
            per_label_counts_json TEXT NOT NULL,
            per_lane_counts_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tag_observations_pack_time
        ON tag_observations(pack_id, observed_at DESC, id DESC)
        """
    )
    return connection


def _evaluate_case(document: GoldenDocument, response: TagResponse) -> ExtractionCaseResult:
    expected_keys = _entity_keys(document.expected_entities)
    actual_keys = _entity_keys(
        GoldenEntity(text=entity.text, label=entity.label)
        for entity in response.entities
    )
    expected_entities = _sorted_entities(expected_keys)
    actual_entities = _sorted_entities(actual_keys)
    matched_entities = _sorted_entities(expected_keys & actual_keys)
    missing_entities = _sorted_entities(expected_keys - actual_keys)
    unexpected_entities = _sorted_entities(actual_keys - expected_keys)
    metrics = response.metrics
    return ExtractionCaseResult(
        name=document.name,
        expected_entities=expected_entities,
        actual_entities=actual_entities,
        matched_entities=matched_entities,
        missing_entities=missing_entities,
        unexpected_entities=unexpected_entities,
        warnings=list(response.warnings),
        token_count=metrics.token_count if metrics is not None else 0,
        entity_count=metrics.entity_count if metrics is not None else len(response.entities),
        entities_per_100_tokens=metrics.entities_per_100_tokens if metrics is not None else 0.0,
        overlap_drop_count=metrics.overlap_drop_count if metrics is not None else 0,
        chunk_count=metrics.chunk_count if metrics is not None else 0,
        timing_ms=response.timing_ms,
        passed=not missing_entities and not unexpected_entities,
    )


def _entity_keys(entities) -> set[tuple[str, str]]:
    return {
        (normalize_lookup_text(entity.text), entity.label.casefold())
        for entity in entities
    }


def _sorted_entities(values: set[tuple[str, str]]) -> list[GoldenEntity]:
    return [
        GoldenEntity(text=text, label=label)
        for text, label in sorted(values, key=lambda item: (item[1], item[0]))
    ]


def _p95(values: list[int]) -> int:
    if not values:
        return 0
    if len(values) == 1:
        return values[0]
    return int(round(quantiles(values, n=100, method="inclusive")[94]))


def _p50(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return int(round((ordered[midpoint - 1] + ordered[midpoint]) / 2))


def _quality_report_to_dict(report: ExtractionQualityReport) -> dict[str, object]:
    return {
        "pack_id": report.pack_id,
        "profile": report.profile,
        "hybrid": report.hybrid,
        "document_count": report.document_count,
        "expected_entity_count": report.expected_entity_count,
        "actual_entity_count": report.actual_entity_count,
        "matched_entity_count": report.matched_entity_count,
        "missing_entity_count": report.missing_entity_count,
        "unexpected_entity_count": report.unexpected_entity_count,
        "recall": report.recall,
        "precision": report.precision,
        "entities_per_100_tokens": report.entities_per_100_tokens,
        "overlap_drop_count": report.overlap_drop_count,
        "chunk_count": report.chunk_count,
        "low_density_warning_count": report.low_density_warning_count,
        "p50_latency_ms": report.p50_latency_ms,
        "p95_latency_ms": report.p95_latency_ms,
        "per_label_expected": report.per_label_expected,
        "per_label_actual": report.per_label_actual,
        "per_label_matched": report.per_label_matched,
        "per_label_recall": report.per_label_recall,
        "per_lane_counts": report.per_lane_counts,
        "warnings": list(report.warnings),
        "cases": [_case_result_to_dict(case) for case in report.cases],
    }


def _runtime_benchmark_report_to_dict(report: RuntimeBenchmarkReport) -> dict[str, object]:
    return {
        "pack_id": report.pack_id,
        "profile": report.profile,
        "hybrid": report.hybrid,
        "metadata_backend": report.metadata_backend,
        "exact_extraction_backend": report.exact_extraction_backend,
        "operator_lookup_backend": report.operator_lookup_backend,
        "matcher_entry_count": report.matcher_entry_count,
        "matcher_state_count": report.matcher_state_count,
        "matcher_cold_load_ms": report.matcher_cold_load_ms,
        "lookup_query_count": report.lookup_query_count,
        "warm_tagging": {
            "sample_count": report.warm_tagging.sample_count,
            "p50_latency_ms": report.warm_tagging.p50_latency_ms,
            "p95_latency_ms": report.warm_tagging.p95_latency_ms,
        },
        "operator_lookup": {
            "sample_count": report.operator_lookup.sample_count,
            "p50_latency_ms": report.operator_lookup.p50_latency_ms,
            "p95_latency_ms": report.operator_lookup.p95_latency_ms,
        },
        "disk_usage": {
            "runtime_pack_bytes": report.disk_usage.runtime_pack_bytes,
            "matcher_artifact_bytes": report.disk_usage.matcher_artifact_bytes,
            "matcher_entries_bytes": report.disk_usage.matcher_entries_bytes,
            "matcher_total_bytes": report.disk_usage.matcher_total_bytes,
            "metadata_store_bytes": report.disk_usage.metadata_store_bytes,
        },
        "quality_report": _quality_report_to_dict(report.quality_report),
        "warnings": list(report.warnings),
    }


def _case_result_to_dict(case: ExtractionCaseResult) -> dict[str, object]:
    return {
        "name": case.name,
        "expected_entities": _entities_to_dict(case.expected_entities),
        "actual_entities": _entities_to_dict(case.actual_entities),
        "matched_entities": _entities_to_dict(case.matched_entities),
        "missing_entities": _entities_to_dict(case.missing_entities),
        "unexpected_entities": _entities_to_dict(case.unexpected_entities),
        "warnings": list(case.warnings),
        "token_count": case.token_count,
        "entity_count": case.entity_count,
        "entities_per_100_tokens": case.entities_per_100_tokens,
        "overlap_drop_count": case.overlap_drop_count,
        "chunk_count": case.chunk_count,
        "timing_ms": case.timing_ms,
        "passed": case.passed,
    }


def _entities_to_dict(entities: list[GoldenEntity]) -> list[dict[str, str]]:
    return [{"text": entity.text, "label": entity.label} for entity in entities]


def _load_entities(payload: object) -> list[GoldenEntity]:
    if not isinstance(payload, list):
        return []
    return [
        GoldenEntity(text=str(item["text"]), label=str(item["label"]))
        for item in payload
        if isinstance(item, dict) and "text" in item and "label" in item
    ]


def _int_dict(payload: object) -> dict[str, int]:
    if not isinstance(payload, dict):
        return {}
    return {str(key): int(value) for key, value in payload.items()}


def _float_dict(payload: object) -> dict[str, float]:
    if not isinstance(payload, dict):
        return {}
    return {str(key): float(value) for key, value in payload.items()}


def _list_str(payload: object) -> list[str]:
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload]


def _benchmark_lookup_queries(golden_set: GoldenSet) -> list[str]:
    queries = {
        entity.text.strip()
        for document in golden_set.documents
        for entity in document.expected_entities
        if entity.text.strip()
    }
    return sorted(queries, key=lambda item: (len(item), item.casefold(), item))


def _path_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(
        item.stat().st_size
        for item in path.rglob("*")
        if item.is_file()
    )


def _metadata_store_bytes(
    *,
    storage_root: Path,
    metadata_backend,
    database_url: str | None,
) -> int | None:
    backend_name = _backend_name(metadata_backend, default="sqlite")
    if backend_name == "sqlite":
        registry_db = storage_root / "registry" / "ades.db"
        return sum(
            path.stat().st_size
            for path in (
                registry_db,
                registry_db.with_name(registry_db.name + "-wal"),
                registry_db.with_name(registry_db.name + "-shm"),
            )
            if path.exists()
        )
    if backend_name != "postgresql" or not database_url:
        return None
    try:
        import psycopg
    except ImportError:
        return None
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            "SELECT pg_database_size(current_database())"
        ).fetchone()
    if not row:
        return None
    return int(row[0])


def _backend_name(value, *, default: str) -> str:
    if value is None:
        return default
    return str(getattr(value, "value", value))


def _operator_lookup_backend_name(metadata_backend) -> str:
    backend_name = _backend_name(metadata_backend, default="sqlite")
    if backend_name == "postgresql":
        return "postgresql_search"
    return "sqlite_search"
