"""Quality validation for generated `economics-vector-en` pack bundles."""

from __future__ import annotations

from pathlib import Path

from .quality_common import (
    PackQualityCase,
    PackQualityEntity,
    PackQualityResult,
    validate_generated_pack_quality,
)


DEFAULT_ECONOMICS_MAX_AMBIGUOUS_ALIASES = 0


DEFAULT_ECONOMICS_SMOKE_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    PackQualityCase(
        name="price-labor-and-growth-indicators",
        text=(
            "CPI and PPI data pointed to consumer inflation while GDP growth, "
            "nonfarm payrolls, and the unemployment rate moved markets."
        ),
        expected_entities=(
            PackQualityEntity(text="CPI", label="economics_concept"),
            PackQualityEntity(text="PPI", label="economics_concept"),
            PackQualityEntity(text="consumer inflation", label="economics_concept"),
            PackQualityEntity(text="GDP", label="economics_concept"),
            PackQualityEntity(text="payrolls", label="economics_concept"),
            PackQualityEntity(text="unemployment rate", label="economics_concept"),
        ),
    ),
    PackQualityCase(
        name="blocked-noisy-macro-language",
        text=(
            "Commentary mentioned price pressures, a growth outlook, a downturn, "
            "rate policy, a budget plan, home prices, mortgage rates, consumer "
            "sentiment, private mortgage insurance PMI, currency depreciation, "
            "spending cuts, and liquidity growth."
        ),
        expected_entities=(),
    ),
)


DEFAULT_ECONOMICS_BENCHMARK_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    DEFAULT_ECONOMICS_SMOKE_QUALITY_CASES[0],
    PackQualityCase(
        name="monetary-and-sovereign-rates",
        text=(
            "The Federal Reserve monetary policy statement referenced the federal "
            "funds rate, M2 money supply, reserve requirements, the yield curve, "
            "a Treasury auction, and public debt."
        ),
        expected_entities=(
            PackQualityEntity(text="monetary policy", label="economics_concept"),
            PackQualityEntity(text="M2 money supply", label="economics_concept"),
            PackQualityEntity(text="reserve requirements", label="economics_concept"),
            PackQualityEntity(text="yield curve", label="economics_concept"),
            PackQualityEntity(text="Treasury auction", label="economics_concept"),
            PackQualityEntity(text="public debt", label="economics_concept"),
        ),
    ),
    PackQualityCase(
        name="trade-external-and-fiscal-policy",
        text=(
            "Tariffs and customs duties affected imports and exports as the trade "
            "balance, current-account deficit, budget deficit, tax reform, and "
            "fiscal stimulus dominated the forecast."
        ),
        expected_entities=(
            PackQualityEntity(text="Tariffs", label="economics_concept"),
            PackQualityEntity(text="customs duties", label="economics_concept"),
            PackQualityEntity(text="imports", label="economics_concept"),
            PackQualityEntity(text="exports", label="economics_concept"),
            PackQualityEntity(text="trade balance", label="economics_concept"),
            PackQualityEntity(text="current-account", label="economics_concept"),
            PackQualityEntity(text="budget deficit", label="economics_concept"),
            PackQualityEntity(text="tax reform", label="economics_concept"),
            PackQualityEntity(text="fiscal stimulus", label="economics_concept"),
        ),
    ),
    PackQualityCase(
        name="activity-housing-and-intervention",
        text=(
            "Core CPI, core PCE, retail sales, housing starts, industrial "
            "production, capacity utilization, and FX intervention all appeared "
            "in the morning macro calendar."
        ),
        expected_entities=(
            PackQualityEntity(text="Core CPI", label="economics_concept"),
            PackQualityEntity(text="core PCE", label="economics_concept"),
            PackQualityEntity(text="retail sales", label="economics_concept"),
            PackQualityEntity(text="housing starts", label="economics_concept"),
            PackQualityEntity(text="industrial production", label="economics_concept"),
            PackQualityEntity(text="FX intervention", label="economics_concept"),
        ),
    ),
    PackQualityCase(
        name="subsidies-and-wages",
        text=(
            "Government subsidies and countervailing measures came alongside "
            "average hourly earnings and wage growth."
        ),
        expected_entities=(
            PackQualityEntity(text="Government subsidies", label="economics_concept"),
            PackQualityEntity(text="countervailing measures", label="economics_concept"),
            PackQualityEntity(text="average hourly earnings", label="economics_concept"),
            PackQualityEntity(text="wage growth", label="economics_concept"),
        ),
    ),
    DEFAULT_ECONOMICS_SMOKE_QUALITY_CASES[1],
)


def validate_economics_pack_quality(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    fixture_profile: str = "benchmark",
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = DEFAULT_ECONOMICS_MAX_AMBIGUOUS_ALIASES,
    max_dropped_alias_ratio: float = 0.5,
) -> PackQualityResult:
    """Build, install, and evaluate one generated `economics-vector-en` bundle."""

    cases = _resolve_economics_quality_cases(fixture_profile)
    return validate_generated_pack_quality(
        bundle_dir,
        output_dir=output_dir,
        quality_dir_name="economics-quality",
        cases=cases,
        fixture_profile=fixture_profile,
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )


def _resolve_economics_quality_cases(
    fixture_profile: str,
) -> tuple[PackQualityCase, ...]:
    normalized_profile = fixture_profile.strip().casefold()
    if normalized_profile == "benchmark":
        return DEFAULT_ECONOMICS_BENCHMARK_QUALITY_CASES
    if normalized_profile == "smoke":
        return DEFAULT_ECONOMICS_SMOKE_QUALITY_CASES
    raise ValueError("fixture_profile must be one of: benchmark, smoke.")
