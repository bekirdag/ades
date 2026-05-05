"""Golden-set evaluation for market impact expansion."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import json
from pathlib import Path
import time
from typing import Any

from ..config import Settings
from .expansion import expand_impact_paths


@dataclass(frozen=True)
class ImpactGoldenCase:
    """One refs-only impact expansion evaluation case."""

    name: str
    entity_refs: tuple[str, ...]
    expected_candidate_refs: tuple[str, ...] = ()
    expected_passive_refs: tuple[str, ...] = ()
    expected_relation_families: tuple[str, ...] = ()
    enabled_packs: tuple[str, ...] = ()
    negative: bool = False


@dataclass(frozen=True)
class ImpactGoldenSet:
    """A refs-only impact expansion golden set."""

    name: str
    description: str | None = None
    default_enabled_packs: tuple[str, ...] = ()
    cases: tuple[ImpactGoldenCase, ...] = ()


@dataclass(frozen=True)
class ImpactGoldenCaseResult:
    """Evaluation result for one impact golden case."""

    name: str
    entity_refs: list[str]
    expected_candidate_refs: list[str] = field(default_factory=list)
    actual_candidate_refs: list[str] = field(default_factory=list)
    missing_candidate_refs: list[str] = field(default_factory=list)
    unexpected_candidate_refs: list[str] = field(default_factory=list)
    expected_passive_refs: list[str] = field(default_factory=list)
    actual_passive_refs: list[str] = field(default_factory=list)
    missing_passive_refs: list[str] = field(default_factory=list)
    unexpected_passive_refs: list[str] = field(default_factory=list)
    expected_relation_families: list[str] = field(default_factory=list)
    actual_relation_families: list[str] = field(default_factory=list)
    missing_relation_families: list[str] = field(default_factory=list)
    unexpected_relation_families: list[str] = field(default_factory=list)
    warning_count: int = 0
    timing_ms: int = 0
    negative: bool = False
    passed: bool = True


@dataclass(frozen=True)
class ImpactGoldenSetReport:
    """Aggregate impact expansion golden-set report."""

    name: str
    case_count: int
    useful_path_rate: float
    empty_path_rate: float
    unrelated_asset_rate: float
    p95_latency_ms: int
    per_relation_family_recall: dict[str, float] = field(default_factory=dict)
    per_relation_family_false_positives: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    cases: list[ImpactGoldenCaseResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Return whether every golden case passed its expected outcome."""

        return all(case.passed for case in self.cases)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable report payload."""

        return {
            "name": self.name,
            "case_count": self.case_count,
            "useful_path_rate": self.useful_path_rate,
            "empty_path_rate": self.empty_path_rate,
            "unrelated_asset_rate": self.unrelated_asset_rate,
            "p95_latency_ms": self.p95_latency_ms,
            "per_relation_family_recall": dict(self.per_relation_family_recall),
            "per_relation_family_false_positives": dict(
                self.per_relation_family_false_positives
            ),
            "warnings": list(self.warnings),
            "passed": self.passed,
            "cases": [case.__dict__ for case in self.cases],
        }


def _string_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"Expected list value, got {type(value).__name__}")
    return tuple(str(item).strip() for item in value if str(item).strip())


def load_impact_golden_set(path: str | Path) -> ImpactGoldenSet:
    """Load one impact expansion golden set from JSON."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Impact golden set must be a JSON object.")
    cases_payload = payload.get("cases")
    if not isinstance(cases_payload, list) or not cases_payload:
        raise ValueError("Impact golden set must include at least one case.")
    cases: list[ImpactGoldenCase] = []
    for index, item in enumerate(cases_payload, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Impact golden case {index} must be an object.")
        name = str(item.get("name") or "").strip()
        entity_refs = _string_tuple(item.get("entity_refs"))
        if not name or not entity_refs:
            raise ValueError(f"Impact golden case {index} is missing name or entity_refs.")
        cases.append(
            ImpactGoldenCase(
                name=name,
                entity_refs=entity_refs,
                expected_candidate_refs=_string_tuple(item.get("expected_candidate_refs")),
                expected_passive_refs=_string_tuple(item.get("expected_passive_refs")),
                expected_relation_families=_string_tuple(
                    item.get("expected_relation_families")
                ),
                enabled_packs=_string_tuple(item.get("enabled_packs")),
                negative=bool(item.get("negative", False)),
            )
        )
    return ImpactGoldenSet(
        name=str(payload.get("name") or Path(path).stem),
        description=str(payload.get("description") or "").strip() or None,
        default_enabled_packs=_string_tuple(payload.get("default_enabled_packs")),
        cases=tuple(cases),
    )


def _p95(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((len(ordered) * 0.95) + 0.999999) - 1))
    return ordered[index]


def _path_relation_families(paths: list[Any]) -> set[str]:
    relations: set[str] = set()
    for path in paths:
        for edge in path.edges:
            if edge.relation:
                relations.add(edge.relation)
    return relations


def evaluate_impact_golden_set(
    *,
    golden_set_path: str | Path,
    artifact_path: str | Path | None = None,
    enabled_packs: list[str] | tuple[str, ...] | None = None,
    max_depth: int = 2,
    max_candidates: int = 25,
    max_paths_per_candidate: int = 3,
) -> ImpactGoldenSetReport:
    """Evaluate impact expansion against one refs-only golden set."""

    golden_set = load_impact_golden_set(golden_set_path)
    case_results: list[ImpactGoldenCaseResult] = []
    latencies: list[int] = []
    expected_relation_counts: Counter[str] = Counter()
    matched_relation_counts: Counter[str] = Counter()
    false_positive_relations: Counter[str] = Counter()
    useful_case_count = 0
    empty_case_count = 0
    unexpected_candidate_count = 0
    actual_candidate_count = 0
    warnings: list[str] = []
    eval_settings = Settings(impact_expansion_enabled=True)

    default_packs = tuple(enabled_packs or golden_set.default_enabled_packs)
    for case in golden_set.cases:
        packs = list(case.enabled_packs or default_packs)
        start = time.perf_counter()
        result = expand_impact_paths(
            entity_refs=case.entity_refs,
            settings=eval_settings,
            artifact_path=artifact_path,
            enabled_packs=packs or None,
            max_depth=max_depth,
            max_candidates=max_candidates,
            max_paths_per_candidate=max_paths_per_candidate,
        )
        elapsed_ms = max(0, round((time.perf_counter() - start) * 1000))
        latencies.append(elapsed_ms)

        actual_candidates = [candidate.entity_ref for candidate in result.candidates]
        actual_passives = [passive.entity_ref for passive in result.passive_paths]
        actual_relations: set[str] = set()
        for candidate in result.candidates:
            actual_relations.update(_path_relation_families(candidate.relationship_paths))
        for passive in result.passive_paths:
            actual_relations.update(_path_relation_families(passive.relationship_paths))

        expected_candidates = set(case.expected_candidate_refs)
        expected_passives = set(case.expected_passive_refs)
        expected_relations = set(case.expected_relation_families)
        actual_candidate_set = set(actual_candidates)
        actual_passive_set = set(actual_passives)

        missing_candidates = sorted(expected_candidates - actual_candidate_set)
        missing_passives = sorted(expected_passives - actual_passive_set)
        unexpected_candidates = sorted(actual_candidate_set - expected_candidates)
        unexpected_passives = sorted(actual_passive_set - expected_passives)
        missing_relations = sorted(expected_relations - actual_relations)
        unexpected_relations = sorted(actual_relations - expected_relations)

        for relation in expected_relations:
            expected_relation_counts[relation] += 1
            if relation in actual_relations:
                matched_relation_counts[relation] += 1
        for relation in unexpected_relations:
            false_positive_relations[relation] += 1

        actual_candidate_count += len(actual_candidate_set)
        unexpected_candidate_count += len(unexpected_candidates)
        if actual_candidate_set or actual_passive_set:
            useful_case_count += 1
        else:
            empty_case_count += 1
        if result.warnings:
            warnings.extend(f"{case.name}:{warning}" for warning in result.warnings)

        passed = not missing_candidates and not missing_passives and not missing_relations
        if case.negative:
            passed = not actual_candidate_set and not actual_passive_set
        else:
            passed = passed and not unexpected_candidates

        case_results.append(
            ImpactGoldenCaseResult(
                name=case.name,
                entity_refs=list(case.entity_refs),
                expected_candidate_refs=list(case.expected_candidate_refs),
                actual_candidate_refs=actual_candidates,
                missing_candidate_refs=missing_candidates,
                unexpected_candidate_refs=unexpected_candidates,
                expected_passive_refs=list(case.expected_passive_refs),
                actual_passive_refs=actual_passives,
                missing_passive_refs=missing_passives,
                unexpected_passive_refs=unexpected_passives,
                expected_relation_families=list(case.expected_relation_families),
                actual_relation_families=sorted(actual_relations),
                missing_relation_families=missing_relations,
                unexpected_relation_families=unexpected_relations,
                warning_count=len(result.warnings),
                timing_ms=elapsed_ms,
                negative=case.negative,
                passed=passed,
            )
        )

    relation_recall = {
        relation: round(matched_relation_counts[relation] / count, 4)
        for relation, count in sorted(expected_relation_counts.items())
    }
    unrelated_asset_rate = (
        round(unexpected_candidate_count / actual_candidate_count, 4)
        if actual_candidate_count
        else 0.0
    )
    case_count = len(golden_set.cases)
    return ImpactGoldenSetReport(
        name=golden_set.name,
        case_count=case_count,
        useful_path_rate=round(useful_case_count / case_count, 4) if case_count else 0.0,
        empty_path_rate=round(empty_case_count / case_count, 4) if case_count else 0.0,
        unrelated_asset_rate=unrelated_asset_rate,
        p95_latency_ms=_p95(latencies),
        per_relation_family_recall=relation_recall,
        per_relation_family_false_positives=dict(sorted(false_positive_relations.items())),
        warnings=warnings,
        cases=case_results,
    )
