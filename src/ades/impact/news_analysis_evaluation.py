"""Golden-set evaluation for ADES news-analysis impact output."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import time
from typing import Any, Callable

GOLDEN_SCHEMA = "ades.impact_path_golden.v1"


@dataclass(frozen=True)
class ImpactNewsAnalysisGoldenCase:
    """One BDYA-derived news-analysis impact golden case."""

    case_id: str
    text: str
    title: str | None = None
    summary: str | None = None
    expected_terminal_refs: tuple[str, ...] = ()
    accepted_broad_proxy_refs: tuple[str, ...] = ()
    forbidden_terminal_refs: tuple[str, ...] = ()
    expected_source_refs: tuple[str, ...] = ()
    expected_event_types: tuple[str, ...] = ()
    review_status: str | None = None
    needs_review: bool = False


@dataclass(frozen=True)
class ImpactNewsAnalysisGoldenCaseResult:
    """Evaluation result for one news-analysis impact golden case."""

    case_id: str
    expected_terminal_refs: list[str] = field(default_factory=list)
    accepted_broad_proxy_refs: list[str] = field(default_factory=list)
    actual_terminal_refs: list[str] = field(default_factory=list)
    missing_terminal_refs: list[str] = field(default_factory=list)
    unexpected_terminal_refs: list[str] = field(default_factory=list)
    forbidden_terminal_refs: list[str] = field(default_factory=list)
    forbidden_terminal_hit_refs: list[str] = field(default_factory=list)
    expected_source_refs: list[str] = field(default_factory=list)
    actual_source_refs: list[str] = field(default_factory=list)
    missing_source_refs: list[str] = field(default_factory=list)
    expected_event_types: list[str] = field(default_factory=list)
    actual_event_types: list[str] = field(default_factory=list)
    missing_event_types: list[str] = field(default_factory=list)
    warning_count: int = 0
    warnings: list[str] = field(default_factory=list)
    terminal_candidate_recall: float = 1.0
    terminal_candidate_precision: float = 1.0
    source_entity_recall: float = 1.0
    event_type_recall: float = 1.0
    timing_ms: int = 0
    passed: bool = True


@dataclass(frozen=True)
class ImpactNewsAnalysisGoldenSetReport:
    """Aggregate report for a news-analysis impact golden set."""

    schema: str
    golden_set_path: str
    case_count: int
    passed_case_count: int
    failed_case_count: int
    terminal_candidate_recall: float
    terminal_candidate_precision: float
    source_entity_recall: float
    event_type_recall: float
    forbidden_candidate_hit_count: int
    unexpected_candidate_count: int
    warning_count: int
    p95_latency_ms: int
    warnings: list[str] = field(default_factory=list)
    cases: list[ImpactNewsAnalysisGoldenCaseResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Return whether every evaluated golden case passed."""

        return self.case_count > 0 and self.failed_case_count == 0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable report payload."""

        return {
            "schema": self.schema,
            "golden_set_path": self.golden_set_path,
            "case_count": self.case_count,
            "passed_case_count": self.passed_case_count,
            "failed_case_count": self.failed_case_count,
            "terminal_candidate_recall": self.terminal_candidate_recall,
            "terminal_candidate_precision": self.terminal_candidate_precision,
            "source_entity_recall": self.source_entity_recall,
            "event_type_recall": self.event_type_recall,
            "forbidden_candidate_hit_count": self.forbidden_candidate_hit_count,
            "unexpected_candidate_count": self.unexpected_candidate_count,
            "warning_count": self.warning_count,
            "p95_latency_ms": self.p95_latency_ms,
            "warnings": list(self.warnings),
            "passed": self.passed,
            "cases": [case.__dict__ for case in self.cases],
        }


def _as_record(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string_value(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _object_field(value: object, key: str) -> object:
    if isinstance(value, dict):
        return value.get(key)
    return getattr(value, key, None)


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        text = _string_value(item)
        if text is not None:
            result.append(text)
    return result


def _candidate_ref(value: object) -> str | None:
    if isinstance(value, str):
        return _string_value(value)
    record = _as_record(value)
    return _string_value(
        record.get("entity_ref")
        or record.get("entityRef")
        or record.get("asset_ref")
        or record.get("assetRef")
        or record.get("ref")
    )


def _candidate_refs(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    refs: list[str] = []
    seen: set[str] = set()
    for item in value:
        ref = _candidate_ref(item)
        if ref and ref not in seen:
            refs.append(ref)
            seen.add(ref)
    return tuple(refs)


def _refs_from_objects(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    refs: list[str] = []
    seen: set[str] = set()
    for item in value:
        ref = _candidate_ref(item) or _string_value(_object_field(item, "entity_ref"))
        if ref and ref not in seen:
            refs.append(ref)
            seen.add(ref)
    return tuple(refs)


def _event_types_from_objects(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    event_types: list[str] = []
    seen: set[str] = set()
    for item in value:
        event_type = _string_value(_object_field(item, "event_type"))
        if event_type and event_type not in seen:
            event_types.append(event_type)
            seen.add(event_type)
    return tuple(event_types)


def _p95(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((len(ordered) * 0.95) + 0.999999) - 1))
    return ordered[index]


def _ratio(matched: int, expected: int) -> float:
    return round(matched / expected, 4) if expected else 1.0


def _load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSONL record at line {line_number}: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"Golden case at line {line_number} must be a JSON object.")
        records.append(parsed)
    return records


def load_impact_news_analysis_golden_cases(
    path: str | Path,
    *,
    reviewed_only: bool = False,
) -> tuple[ImpactNewsAnalysisGoldenCase, ...]:
    """Load BDYA-derived impact-path golden cases from JSONL."""

    resolved_path = Path(path).expanduser()
    records = _load_jsonl_records(resolved_path)
    cases: list[ImpactNewsAnalysisGoldenCase] = []
    for index, record in enumerate(records, start=1):
        if record.get("schema") != GOLDEN_SCHEMA:
            raise ValueError(
                f"Unsupported impact golden schema at line {index}: {record.get('schema')!r}"
            )
        input_record = _as_record(record.get("input"))
        expected_record = _as_record(record.get("expected"))
        review_record = _as_record(record.get("review"))
        needs_review = bool(review_record.get("needs_review", False))
        if reviewed_only and needs_review:
            continue
        text = _string_value(input_record.get("text"))
        title = _string_value(input_record.get("title"))
        summary = _string_value(input_record.get("summary"))
        if not text:
            text = "\n\n".join(item for item in (title, summary) if item)
        case_id = _string_value(record.get("case_id")) or f"case-{index}"
        if not text:
            raise ValueError(f"Impact golden case {case_id} is missing input text.")
        cases.append(
            ImpactNewsAnalysisGoldenCase(
                case_id=case_id,
                text=text,
                title=title,
                summary=summary,
                expected_terminal_refs=_candidate_refs(expected_record.get("terminal_candidates")),
                accepted_broad_proxy_refs=_candidate_refs(
                    expected_record.get("accepted_broad_proxies")
                ),
                forbidden_terminal_refs=_candidate_refs(
                    expected_record.get("forbidden_terminal_candidates")
                ),
                expected_source_refs=_candidate_refs(expected_record.get("source_entities")),
                expected_event_types=tuple(
                    dict.fromkeys(_string_list(expected_record.get("event_types")))
                ),
                review_status=_string_value(review_record.get("status")),
                needs_review=needs_review,
            )
        )
    if not cases:
        raise ValueError("Impact news-analysis golden set did not include any cases.")
    return tuple(cases)


def evaluate_impact_news_analysis_golden_set(
    *,
    golden_set_path: str | Path,
    analyzer: Callable[[ImpactNewsAnalysisGoldenCase], Any],
    reviewed_only: bool = False,
    min_terminal_candidate_recall: float = 1.0,
    min_terminal_candidate_precision: float = 0.95,
    min_source_entity_recall: float = 0.8,
    min_event_type_recall: float = 0.8,
    fail_on_warnings: bool = True,
    max_cases: int | None = None,
) -> ImpactNewsAnalysisGoldenSetReport:
    """Evaluate `/v0/news/analyze`-shaped impact output against BDYA golden cases."""

    cases = list(
        load_impact_news_analysis_golden_cases(
            golden_set_path,
            reviewed_only=reviewed_only,
        )
    )
    if max_cases is not None:
        cases = cases[: max(max_cases, 0)]
    if not cases:
        raise ValueError("Impact news-analysis golden set did not include any cases.")

    case_results: list[ImpactNewsAnalysisGoldenCaseResult] = []
    latencies: list[int] = []
    warnings: list[str] = []
    matched_terminal_total = 0
    expected_terminal_total = 0
    allowed_actual_terminal_total = 0
    actual_terminal_total = 0
    matched_source_total = 0
    expected_source_total = 0
    matched_event_total = 0
    expected_event_total = 0
    forbidden_hit_count = 0
    unexpected_candidate_count = 0
    warning_count = 0

    for case in cases:
        start = time.perf_counter()
        try:
            response = analyzer(case)
            analyzer_warnings = _string_list(_object_field(response, "warnings"))
        except Exception as exc:
            response = {}
            analyzer_warnings = [
                f"ADES_NEWS_ANALYZE_EVALUATOR_FAILED:{type(exc).__name__}:{str(exc)[:180]}"
            ]
        elapsed_ms = max(0, round((time.perf_counter() - start) * 1000))
        response_timing = _object_field(response, "total_time_ms") or _object_field(
            response, "timing_ms"
        )
        if isinstance(response_timing, int):
            elapsed_ms = max(elapsed_ms, response_timing)
        latencies.append(elapsed_ms)

        actual_terminals = _refs_from_objects(_object_field(response, "terminal_impact_candidates"))
        actual_sources = _refs_from_objects(_object_field(response, "source_entities"))
        actual_events = _event_types_from_objects(_object_field(response, "event_signals"))

        expected_terminals = set(case.expected_terminal_refs)
        accepted_broad_proxies = set(case.accepted_broad_proxy_refs)
        allowed_terminals = expected_terminals | accepted_broad_proxies
        forbidden_terminals = set(case.forbidden_terminal_refs)
        actual_terminal_set = set(actual_terminals)
        expected_sources = set(case.expected_source_refs)
        actual_source_set = set(actual_sources)
        expected_events = set(case.expected_event_types)
        actual_event_set = set(actual_events)

        missing_terminals = sorted(expected_terminals - actual_terminal_set)
        unexpected_terminals = sorted(actual_terminal_set - allowed_terminals)
        forbidden_hits = sorted(actual_terminal_set & forbidden_terminals)
        missing_sources = sorted(expected_sources - actual_source_set)
        missing_events = sorted(expected_events - actual_event_set)

        matched_terminals = len(expected_terminals & actual_terminal_set)
        allowed_actual_terminals = len(actual_terminal_set & allowed_terminals)
        matched_sources = len(expected_sources & actual_source_set)
        matched_events = len(expected_events & actual_event_set)

        expected_terminal_total += len(expected_terminals)
        matched_terminal_total += matched_terminals
        actual_terminal_total += len(actual_terminal_set)
        allowed_actual_terminal_total += allowed_actual_terminals
        expected_source_total += len(expected_sources)
        matched_source_total += matched_sources
        expected_event_total += len(expected_events)
        matched_event_total += matched_events
        forbidden_hit_count += len(forbidden_hits)
        unexpected_candidate_count += len(unexpected_terminals)
        warning_count += len(analyzer_warnings)
        if analyzer_warnings:
            warnings.extend(f"{case.case_id}:{warning}" for warning in analyzer_warnings)

        terminal_recall = _ratio(matched_terminals, len(expected_terminals))
        terminal_precision = _ratio(allowed_actual_terminals, len(actual_terminal_set))
        source_recall = _ratio(matched_sources, len(expected_sources))
        event_recall = _ratio(matched_events, len(expected_events))
        passed = (
            terminal_recall >= min_terminal_candidate_recall
            and terminal_precision >= min_terminal_candidate_precision
            and source_recall >= min_source_entity_recall
            and event_recall >= min_event_type_recall
            and not forbidden_hits
            and (not fail_on_warnings or not analyzer_warnings)
        )
        case_results.append(
            ImpactNewsAnalysisGoldenCaseResult(
                case_id=case.case_id,
                expected_terminal_refs=list(case.expected_terminal_refs),
                accepted_broad_proxy_refs=list(case.accepted_broad_proxy_refs),
                actual_terminal_refs=list(actual_terminals),
                missing_terminal_refs=missing_terminals,
                unexpected_terminal_refs=unexpected_terminals,
                forbidden_terminal_refs=list(case.forbidden_terminal_refs),
                forbidden_terminal_hit_refs=forbidden_hits,
                expected_source_refs=list(case.expected_source_refs),
                actual_source_refs=list(actual_sources),
                missing_source_refs=missing_sources,
                expected_event_types=list(case.expected_event_types),
                actual_event_types=list(actual_events),
                missing_event_types=missing_events,
                warning_count=len(analyzer_warnings),
                warnings=analyzer_warnings,
                terminal_candidate_recall=terminal_recall,
                terminal_candidate_precision=terminal_precision,
                source_entity_recall=source_recall,
                event_type_recall=event_recall,
                timing_ms=elapsed_ms,
                passed=passed,
            )
        )

    passed_case_count = sum(1 for result in case_results if result.passed)
    case_count = len(case_results)
    aggregate_terminal_precision = _ratio(
        allowed_actual_terminal_total,
        actual_terminal_total,
    )
    return ImpactNewsAnalysisGoldenSetReport(
        schema="ades.impact_news_analysis_golden_report.v1",
        golden_set_path=str(Path(golden_set_path).expanduser()),
        case_count=case_count,
        passed_case_count=passed_case_count,
        failed_case_count=case_count - passed_case_count,
        terminal_candidate_recall=_ratio(
            matched_terminal_total,
            expected_terminal_total,
        ),
        terminal_candidate_precision=aggregate_terminal_precision,
        source_entity_recall=_ratio(matched_source_total, expected_source_total),
        event_type_recall=_ratio(matched_event_total, expected_event_total),
        forbidden_candidate_hit_count=forbidden_hit_count,
        unexpected_candidate_count=unexpected_candidate_count,
        warning_count=warning_count,
        p95_latency_ms=_p95(latencies),
        warnings=warnings,
        cases=case_results,
    )
