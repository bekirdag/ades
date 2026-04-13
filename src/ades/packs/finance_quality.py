"""Quality validation for generated `finance-en` pack bundles."""

from __future__ import annotations

from pathlib import Path

from .quality_common import (
    PackQualityCase,
    PackQualityEntity,
    PackQualityResult,
    validate_generated_pack_quality,
)


DEFAULT_FINANCE_MAX_AMBIGUOUS_ALIASES = 300


DEFAULT_FINANCE_SMOKE_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    PackQualityCase(
        name="earnings-update",
        text="Issuer Alpha said TICKA traded on EXCHX after USD 12.5 guidance and $TICKB rose.",
        expected_entities=(
            PackQualityEntity(text="Issuer Alpha", label="organization"),
            PackQualityEntity(text="TICKA", label="ticker"),
            PackQualityEntity(text="EXCHX", label="exchange"),
            PackQualityEntity(text="TICKB", label="ticker"),
            PackQualityEntity(text="USD 12.5", label="currency_amount"),
            PackQualityEntity(text="$TICKB", label="ticker"),
        ),
    ),
    PackQualityCase(
        name="exchange-alias",
        text="Issuer Beta rose on Exchange Beta after USD 22 guidance.",
        expected_entities=(
            PackQualityEntity(text="Issuer Beta", label="organization"),
            PackQualityEntity(text="Exchange Beta", label="exchange"),
            PackQualityEntity(text="USD 22", label="currency_amount"),
        ),
    ),
    PackQualityCase(
        name="produce-market-non-hit",
        text="Vendors sold produce and tools at the public market on Tuesday.",
        expected_entities=(),
    ),
)


DEFAULT_FINANCE_BENCHMARK_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    *DEFAULT_FINANCE_SMOKE_QUALITY_CASES[:2],
    PackQualityCase(
        name="exchange-canonical-and-symbol-mix",
        text="EXCHX and EXCHY moved while TICKA and TICKB stayed active after USD 18 guidance.",
        expected_entities=(
            PackQualityEntity(text="EXCHX", label="exchange"),
            PackQualityEntity(text="EXCHY", label="exchange"),
            PackQualityEntity(text="TICKA", label="ticker"),
            PackQualityEntity(text="TICKB", label="ticker"),
            PackQualityEntity(text="USD 18", label="currency_amount"),
        ),
    ),
    PackQualityCase(
        name="curated-exchange-alias-expansion",
        text="EXCHZ and Exchange Epsilon opened higher after USD 7.5 guidance.",
        expected_entities=(
            PackQualityEntity(text="EXCHZ", label="exchange"),
            PackQualityEntity(text="Exchange Epsilon", label="exchange"),
            PackQualityEntity(text="USD 7.5", label="currency_amount"),
        ),
    ),
    DEFAULT_FINANCE_SMOKE_QUALITY_CASES[2],
)


def validate_finance_pack_quality(
    bundle_dir: str | Path,
    *,
    output_dir: str,
    fixture_profile: str = "benchmark",
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = DEFAULT_FINANCE_MAX_AMBIGUOUS_ALIASES,
    max_dropped_alias_ratio: float = 0.5,
) -> PackQualityResult:
    """Build, install, and evaluate one generated `finance-en` pack bundle."""

    cases = _resolve_finance_quality_cases(fixture_profile)
    return validate_generated_pack_quality(
        bundle_dir,
        output_dir=output_dir,
        quality_dir_name="finance-quality",
        cases=cases,
        fixture_profile=fixture_profile,
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )


def _resolve_finance_quality_cases(
    fixture_profile: str,
) -> tuple[PackQualityCase, ...]:
    normalized_profile = fixture_profile.strip().casefold()
    if normalized_profile == "benchmark":
        return DEFAULT_FINANCE_BENCHMARK_QUALITY_CASES
    if normalized_profile == "smoke":
        return DEFAULT_FINANCE_SMOKE_QUALITY_CASES
    raise ValueError("fixture_profile must be one of: benchmark, smoke.")
