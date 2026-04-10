"""Quality validation for generated `finance-en` pack bundles."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
from pathlib import Path
import shutil

from .generation import SourceBundleManifest, generate_pack_source
from .installer import PackInstaller
from .publish import build_static_registry
from ..pipeline.tagger import tag_text


DEFAULT_FINANCE_MAX_AMBIGUOUS_ALIASES = 300


@dataclass(frozen=True)
class FinanceQualityEntity:
    """One tagged or expected entity inside a finance quality case."""

    text: str
    label: str


@dataclass(frozen=True)
class FinanceQualityCase:
    """One deterministic validation case for generated finance packs."""

    name: str
    text: str
    expected_entities: tuple[FinanceQualityEntity, ...] = ()


@dataclass(frozen=True)
class FinanceQualityCaseResult:
    """Outcome for one finance quality case."""

    name: str
    text: str
    expected_entities: list[FinanceQualityEntity] = field(default_factory=list)
    actual_entities: list[FinanceQualityEntity] = field(default_factory=list)
    matched_entities: list[FinanceQualityEntity] = field(default_factory=list)
    missing_entities: list[FinanceQualityEntity] = field(default_factory=list)
    unexpected_entities: list[FinanceQualityEntity] = field(default_factory=list)
    passed: bool = True


@dataclass(frozen=True)
class FinancePackQualityResult:
    """Aggregate quality report for one generated finance pack."""

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
    cases: list[FinanceQualityCaseResult] = field(default_factory=list)


DEFAULT_FINANCE_QUALITY_CASES: tuple[FinanceQualityCase, ...] = (
    FinanceQualityCase(
        name="earnings-update",
        text="Apple said AAPL traded on NASDAQ after USD 12.5 guidance and $MSFT rose.",
        expected_entities=(
            FinanceQualityEntity(text="Apple", label="organization"),
            FinanceQualityEntity(text="AAPL", label="ticker"),
            FinanceQualityEntity(text="NASDAQ", label="exchange"),
            FinanceQualityEntity(text="MSFT", label="ticker"),
            FinanceQualityEntity(text="USD 12.5", label="currency_amount"),
            FinanceQualityEntity(text="$MSFT", label="ticker"),
        ),
    ),
    FinanceQualityCase(
        name="exchange-alias",
        text="Microsoft rose on New York Stock Exchange after USD 22 guidance.",
        expected_entities=(
            FinanceQualityEntity(text="Microsoft", label="organization"),
            FinanceQualityEntity(text="New York Stock Exchange", label="exchange"),
            FinanceQualityEntity(text="USD 22", label="currency_amount"),
        ),
    ),
    FinanceQualityCase(
        name="produce-market-non-hit",
        text="Farmers sold apples and oranges at the market on Tuesday.",
        expected_entities=(),
    ),
)


def validate_finance_pack_quality(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = DEFAULT_FINANCE_MAX_AMBIGUOUS_ALIASES,
    max_dropped_alias_ratio: float = 0.5,
) -> FinancePackQualityResult:
    """Build, install, and evaluate one generated `finance-en` pack bundle."""

    if not 0.0 <= min_expected_recall <= 1.0:
        raise ValueError("min_expected_recall must be between 0.0 and 1.0.")
    if max_unexpected_hits < 0:
        raise ValueError("max_unexpected_hits must be >= 0.")
    if max_ambiguous_aliases < 0:
        raise ValueError("max_ambiguous_aliases must be >= 0.")
    if not 0.0 <= max_dropped_alias_ratio <= 1.0:
        raise ValueError("max_dropped_alias_ratio must be between 0.0 and 1.0.")

    resolved_bundle_dir = Path(bundle_dir).expanduser().resolve()
    if not resolved_bundle_dir.exists():
        raise FileNotFoundError(f"Bundle directory not found: {resolved_bundle_dir}")
    if not resolved_bundle_dir.is_dir():
        raise NotADirectoryError(f"Bundle directory is not a directory: {resolved_bundle_dir}")

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    quality_root = resolved_output_dir / "finance-quality"
    generated_root = quality_root / "generated-pack"
    registry_root = quality_root / "registry"
    install_root = quality_root / "install"
    for path in (quality_root, generated_root, registry_root, install_root):
        _ensure_clean_dir(path)

    generated = generate_pack_source(
        resolved_bundle_dir,
        output_dir=generated_root,
        version=version,
    )
    registry = build_static_registry([generated.pack_dir], output_dir=registry_root)
    installer = PackInstaller(install_root, registry_url=registry.index_url)
    installer.install(generated.pack_id)

    unique_canonical_count = _count_unique_canonicals(resolved_bundle_dir)
    dropped_alias_total = generated.alias_count + generated.dropped_alias_count
    dropped_alias_ratio = (
        round(generated.dropped_alias_count / dropped_alias_total, 4)
        if dropped_alias_total
        else 0.0
    )
    evaluated_at = _utc_timestamp()

    case_results: list[FinanceQualityCaseResult] = []
    warnings = list(generated.warnings)
    failures: list[str] = []
    expected_entity_count = 0
    actual_entity_count = 0
    matched_expected_entity_count = 0
    missing_expected_entity_count = 0
    unexpected_entity_count = 0
    strict_pass_count = 0

    for case in DEFAULT_FINANCE_QUALITY_CASES:
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
            FinanceQualityCaseResult(
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
    if dropped_alias_ratio > max_dropped_alias_ratio:
        failures.append(
            f"Dropped alias ratio {dropped_alias_ratio:.4f} exceeds threshold {max_dropped_alias_ratio:.4f}."
        )

    return FinancePackQualityResult(
        pack_id=generated.pack_id,
        version=generated.version,
        bundle_dir=str(resolved_bundle_dir),
        output_dir=str(quality_root),
        generated_pack_dir=generated.pack_dir,
        registry_dir=registry.output_dir,
        registry_index_path=registry.index_path,
        registry_index_url=registry.index_url,
        storage_root=str(install_root),
        evaluated_at=evaluated_at,
        fixture_profile="default",
        fixture_count=len(DEFAULT_FINANCE_QUALITY_CASES),
        passed_case_count=strict_pass_count,
        failed_case_count=len(DEFAULT_FINANCE_QUALITY_CASES) - strict_pass_count,
        expected_entity_count=expected_entity_count,
        actual_entity_count=actual_entity_count,
        matched_expected_entity_count=matched_expected_entity_count,
        missing_expected_entity_count=missing_expected_entity_count,
        unexpected_entity_count=unexpected_entity_count,
        expected_recall=expected_recall,
        precision=precision,
        alias_count=generated.alias_count,
        unique_canonical_count=unique_canonical_count,
        rule_count=generated.rule_count,
        dropped_alias_count=generated.dropped_alias_count,
        ambiguous_alias_count=generated.ambiguous_alias_count,
        dropped_alias_ratio=dropped_alias_ratio,
        passed=not failures,
        failures=failures,
        warnings=warnings,
        cases=case_results,
    )


def _count_unique_canonicals(bundle_dir: Path) -> int:
    bundle = SourceBundleManifest.from_path(bundle_dir / "bundle.json")
    records_path = bundle_dir / bundle.entities_path
    canonicals: set[str] = set()
    for line in records_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        canonical_text = str(payload.get("canonical_text", "")).strip()
        if canonical_text:
            canonicals.add(canonical_text.casefold())
    return len(canonicals)


def _ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _sorted_entities(values: set[tuple[str, str]]) -> list[FinanceQualityEntity]:
    return [
        FinanceQualityEntity(text=text, label=label)
        for text, label in sorted(values, key=lambda item: (item[0].casefold(), item[1].casefold()))
    ]


def _format_entity(entity: FinanceQualityEntity) -> str:
    return f"{entity.text}:{entity.label}"


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
