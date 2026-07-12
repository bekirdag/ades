"""Quality validation for generated `business-vector-en` pack bundles."""

from __future__ import annotations

from pathlib import Path

from .quality_common import (
    PackQualityCase,
    PackQualityEntity,
    PackQualityResult,
    validate_generated_pack_quality,
)


DEFAULT_BUSINESS_MAX_AMBIGUOUS_ALIASES = 0


DEFAULT_BUSINESS_SMOKE_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    PackQualityCase(
        name="source-backed-event-mix",
        text=(
            "The company announced a merger, then filed for bankruptcy after "
            "a guidance cut."
        ),
        expected_entities=(
            PackQualityEntity(text="merger", label="business_concept"),
            PackQualityEntity(text="bankruptcy", label="business_concept"),
            PackQualityEntity(text="guidance cut", label="business_concept"),
        ),
    ),
    PackQualityCase(
        name="blocked-shortforms-and-generic-prose",
        text=(
            "Ordinary articles mention IPO, M&A, a recall, a walkout, a lawsuit, "
            "a court ruling, and a strategic partnership."
        ),
        expected_entities=(),
    ),
)


DEFAULT_BUSINESS_BENCHMARK_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    DEFAULT_BUSINESS_SMOKE_QUALITY_CASES[0],
    PackQualityCase(
        name="shareholder-and-credit-actions",
        text=(
            "Management approved a stock buyback, a dividend declaration, "
            "and a rating downgrade."
        ),
        expected_entities=(
            PackQualityEntity(text="stock buyback", label="business_concept"),
            PackQualityEntity(text="dividend declaration", label="business_concept"),
            PackQualityEntity(text="rating downgrade", label="business_concept"),
        ),
    ),
    PackQualityCase(
        name="workforce-cyber-and-legal-events",
        text=(
            "The board reported a mass layoff, plant closing, data breach, "
            "and material litigation."
        ),
        expected_entities=(
            PackQualityEntity(text="mass layoff", label="business_concept"),
            PackQualityEntity(text="plant closing", label="business_concept"),
            PackQualityEntity(text="data breach", label="business_concept"),
            PackQualityEntity(text="material litigation", label="business_concept"),
        ),
    ),
    PackQualityCase(
        name="offering-recall-and-venture-events",
        text=(
            "A product recall followed an initial public offering and a joint venture."
        ),
        expected_entities=(
            PackQualityEntity(text="product recall", label="business_concept"),
            PackQualityEntity(text="initial public offering", label="business_concept"),
            PackQualityEntity(text="joint venture", label="business_concept"),
        ),
    ),
    DEFAULT_BUSINESS_SMOKE_QUALITY_CASES[1],
)


def validate_business_pack_quality(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    fixture_profile: str = "benchmark",
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = DEFAULT_BUSINESS_MAX_AMBIGUOUS_ALIASES,
    max_dropped_alias_ratio: float = 0.5,
) -> PackQualityResult:
    """Build, install, and evaluate one generated `business-vector-en` bundle."""

    cases = _resolve_business_quality_cases(fixture_profile)
    return validate_generated_pack_quality(
        bundle_dir,
        output_dir=output_dir,
        quality_dir_name="business-quality",
        cases=cases,
        fixture_profile=fixture_profile,
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )


def _resolve_business_quality_cases(
    fixture_profile: str,
) -> tuple[PackQualityCase, ...]:
    normalized_profile = fixture_profile.strip().casefold()
    if normalized_profile == "benchmark":
        return DEFAULT_BUSINESS_BENCHMARK_QUALITY_CASES
    if normalized_profile == "smoke":
        return DEFAULT_BUSINESS_SMOKE_QUALITY_CASES
    raise ValueError("fixture_profile must be one of: benchmark, smoke.")
