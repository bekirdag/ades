"""Shared helpers for validating generated pack quality."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
import shutil

from ..pipeline.tagger import tag_text
from .installer import PackInstaller
from .publish import build_static_registry
from .reporting import report_generated_pack


@dataclass(frozen=True)
class PackQualityEntity:
    """One tagged or expected entity inside a generated-pack quality case."""

    text: str
    label: str


@dataclass(frozen=True)
class PackQualityCase:
    """One deterministic validation case for a generated pack."""

    name: str
    text: str
    expected_entities: tuple[PackQualityEntity, ...] = ()


@dataclass(frozen=True)
class PackQualityCaseResult:
    """Outcome for one generated-pack quality case."""

    name: str
    text: str
    expected_entities: list[PackQualityEntity] = field(default_factory=list)
    actual_entities: list[PackQualityEntity] = field(default_factory=list)
    matched_entities: list[PackQualityEntity] = field(default_factory=list)
    missing_entities: list[PackQualityEntity] = field(default_factory=list)
    unexpected_entities: list[PackQualityEntity] = field(default_factory=list)
    passed: bool = True


@dataclass(frozen=True)
class PackQualityResult:
    """Aggregate quality report for one generated pack."""

    pack_id: str
    version: str
    bundle_dir: str
    output_dir: str
    generated_pack_dir: str
    registry_dir: str
    registry_index_path: str
    registry_index_url: str
    storage_root: str
    evaluated_at: str
    fixture_profile: str
    fixture_count: int
    passed_case_count: int
    failed_case_count: int
    expected_entity_count: int
    actual_entity_count: int
    matched_expected_entity_count: int
    missing_expected_entity_count: int
    unexpected_entity_count: int
    expected_recall: float
    precision: float
    alias_count: int
    unique_canonical_count: int
    rule_count: int
    dropped_alias_count: int
    ambiguous_alias_count: int
    dropped_alias_ratio: float
    passed: bool
    failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    cases: list[PackQualityCaseResult] = field(default_factory=list)


def validate_generated_pack_quality(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    quality_dir_name: str,
    cases: tuple[PackQualityCase, ...],
    dependency_bundle_dirs: tuple[str | Path, ...] = (),
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = 0,
    max_dropped_alias_ratio: float = 0.5,
) -> PackQualityResult:
    """Build, install, and evaluate one generated pack with optional dependencies."""

    if not 0.0 <= min_expected_recall <= 1.0:
        raise ValueError("min_expected_recall must be between 0.0 and 1.0.")
    if max_unexpected_hits < 0:
        raise ValueError("max_unexpected_hits must be >= 0.")
    if max_ambiguous_aliases < 0:
        raise ValueError("max_ambiguous_aliases must be >= 0.")
    if not 0.0 <= max_dropped_alias_ratio <= 1.0:
        raise ValueError("max_dropped_alias_ratio must be between 0.0 and 1.0.")

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    quality_root = resolved_output_dir / quality_dir_name
    generated_root = quality_root / "generated-pack"
    registry_root = quality_root / "registry"
    install_root = quality_root / "install"
    for path in (quality_root, generated_root, registry_root, install_root):
        _ensure_clean_dir(path)

    dependency_reports = [
        report_generated_pack(dependency_bundle_dir, output_dir=generated_root)
        for dependency_bundle_dir in dependency_bundle_dirs
    ]
    generated = report_generated_pack(
        bundle_dir,
        output_dir=generated_root,
        version=version,
    )
    registry = build_static_registry(
        [*(report.pack_dir for report in dependency_reports), generated.pack_dir],
        output_dir=registry_root,
    )
    installer = PackInstaller(install_root, registry_url=registry.index_url)
    installer.install(generated.pack_id)

    warnings = list(generated.warnings)
    for dependency in dependency_reports:
        warnings.extend(f"{dependency.pack_id}: {warning}" for warning in dependency.warnings)
    failures: list[str] = []
    case_results: list[PackQualityCaseResult] = []
    expected_entity_count = 0
    actual_entity_count = 0
    matched_expected_entity_count = 0
    missing_expected_entity_count = 0
    unexpected_entity_count = 0
    strict_pass_count = 0

    for case in cases:
        response = tag_text(
            text=case.text,
            pack=generated.pack_id,
            content_type="text/plain",
            storage_root=install_root,
        )
        actual_entities = _sorted_entities({(entity.text, entity.label) for entity in response.entities})
        expected_entities = _sorted_entities(
            {(entity.text, entity.label) for entity in case.expected_entities}
        )
        actual_keys = {(item.text, item.label) for item in actual_entities}
        expected_keys = {(item.text, item.label) for item in expected_entities}
        matched_entities = _sorted_entities(actual_keys & expected_keys)
        missing_entities = _sorted_entities(expected_keys - actual_keys)
        unexpected_entities = _sorted_entities(actual_keys - expected_keys)
        passed = not missing_entities and not unexpected_entities
        if passed:
            strict_pass_count += 1
        else:
            if missing_entities:
                warnings.append(
                    f"Case {case.name} is missing expected entities: "
                    + ", ".join(_format_entity(item) for item in missing_entities)
                )
            if unexpected_entities:
                warnings.append(
                    f"Case {case.name} produced unexpected entities: "
                    + ", ".join(_format_entity(item) for item in unexpected_entities)
                )

        case_results.append(
            PackQualityCaseResult(
                name=case.name,
                text=case.text,
                expected_entities=expected_entities,
                actual_entities=actual_entities,
                matched_entities=matched_entities,
                missing_entities=missing_entities,
                unexpected_entities=unexpected_entities,
                passed=passed,
            )
        )
        expected_entity_count += len(expected_entities)
        actual_entity_count += len(actual_entities)
        matched_expected_entity_count += len(matched_entities)
        missing_expected_entity_count += len(missing_entities)
        unexpected_entity_count += len(unexpected_entities)

    expected_recall = (
        round(matched_expected_entity_count / expected_entity_count, 4)
        if expected_entity_count
        else 1.0
    )
    precision = (
        round(matched_expected_entity_count / actual_entity_count, 4)
        if actual_entity_count
        else 1.0
    )

    if expected_recall < min_expected_recall:
        failures.append(
            f"Expected recall {expected_recall:.4f} is below threshold {min_expected_recall:.4f}."
        )
    if unexpected_entity_count > max_unexpected_hits:
        failures.append(
            f"Unexpected entity count {unexpected_entity_count} exceeds threshold {max_unexpected_hits}."
        )
    if generated.ambiguous_alias_count > max_ambiguous_aliases:
        failures.append(
            "Ambiguous alias count "
            f"{generated.ambiguous_alias_count} exceeds threshold {max_ambiguous_aliases}."
        )
    if generated.dropped_alias_ratio > max_dropped_alias_ratio:
        failures.append(
            f"Dropped alias ratio {generated.dropped_alias_ratio:.4f} exceeds threshold "
            f"{max_dropped_alias_ratio:.4f}."
        )

    return PackQualityResult(
        pack_id=generated.pack_id,
        version=generated.version,
        bundle_dir=generated.bundle_dir,
        output_dir=str(quality_root),
        generated_pack_dir=generated.pack_dir,
        registry_dir=registry.output_dir,
        registry_index_path=registry.index_path,
        registry_index_url=registry.index_url,
        storage_root=str(install_root),
        evaluated_at=_utc_timestamp(),
        fixture_profile="default",
        fixture_count=len(cases),
        passed_case_count=strict_pass_count,
        failed_case_count=len(cases) - strict_pass_count,
        expected_entity_count=expected_entity_count,
        actual_entity_count=actual_entity_count,
        matched_expected_entity_count=matched_expected_entity_count,
        missing_expected_entity_count=missing_expected_entity_count,
        unexpected_entity_count=unexpected_entity_count,
        expected_recall=expected_recall,
        precision=precision,
        alias_count=generated.alias_count,
        unique_canonical_count=generated.unique_canonical_count,
        rule_count=generated.rule_count,
        dropped_alias_count=generated.dropped_alias_count,
        ambiguous_alias_count=generated.ambiguous_alias_count,
        dropped_alias_ratio=generated.dropped_alias_ratio,
        passed=not failures,
        failures=failures,
        warnings=warnings,
        cases=case_results,
    )


def _ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _sorted_entities(values: set[tuple[str, str]]) -> list[PackQualityEntity]:
    return [
        PackQualityEntity(text=text, label=label)
        for text, label in sorted(values, key=lambda item: (item[0].casefold(), item[1].casefold()))
    ]


def _format_entity(entity: PackQualityEntity) -> str:
    return f"{entity.text}:{entity.label}"


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
