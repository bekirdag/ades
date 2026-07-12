"""Quality validation for generated `politics-vector-en` pack bundles."""

from __future__ import annotations

from pathlib import Path

from .quality_common import (
    PackQualityCase,
    PackQualityEntity,
    PackQualityResult,
    validate_generated_pack_quality,
)


DEFAULT_POLITICS_MAX_AMBIGUOUS_ALIASES = 0


DEFAULT_POLITICS_SMOKE_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    PackQualityCase(
        name="sanctions-and-export-controls",
        text=(
            "OFAC sanctions and export controls hit entities on the Entity List "
            "after travel ban measures."
        ),
        expected_entities=(
            PackQualityEntity(text="OFAC sanctions", label="politics_concept"),
            PackQualityEntity(text="export controls", label="politics_concept"),
            PackQualityEntity(text="Entity List", label="politics_concept"),
            PackQualityEntity(text="travel ban", label="politics_concept"),
        ),
    ),
    PackQualityCase(
        name="wikidata-organization-anchors",
        text=(
            "The North Atlantic Treaty Organization, United Nations, the U.S. "
            "State Department, the Bureau of Industry and Security, UK "
            "Parliament, the Electoral Commission, the International Energy "
            "Agency, and the United States Geological Survey issued public "
            "updates."
        ),
        expected_entities=(
            PackQualityEntity(
                text="North Atlantic Treaty Organization",
                label="organization",
            ),
            PackQualityEntity(text="United Nations", label="organization"),
            PackQualityEntity(text="U.S. State Department", label="organization"),
            PackQualityEntity(text="Bureau of Industry and Security", label="organization"),
            PackQualityEntity(text="UK Parliament", label="organization"),
            PackQualityEntity(text="Electoral Commission", label="organization"),
            PackQualityEntity(text="International Energy Agency", label="organization"),
            PackQualityEntity(text="United States Geological Survey", label="organization"),
        ),
    ),
    PackQualityCase(
        name="blocked-noisy-politics-language",
        text=(
            "The article mentioned a kitchen cabinet, a network border, shipping "
            "security software, a survey poll, sanction approval, coalition talks, "
            "diplomatic dispute, and a resource tax debate."
        ),
        expected_entities=(),
    ),
)


DEFAULT_POLITICS_BENCHMARK_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    DEFAULT_POLITICS_SMOKE_QUALITY_CASES[0],
    DEFAULT_POLITICS_SMOKE_QUALITY_CASES[1],
    PackQualityCase(
        name="government-stability-and-shutdown",
        text=(
            "A coalition government faced a no-confidence vote as the government "
            "shutdown continued after a state of emergency."
        ),
        expected_entities=(
            PackQualityEntity(text="coalition government", label="politics_concept"),
            PackQualityEntity(text="no-confidence vote", label="politics_concept"),
            PackQualityEntity(text="government shutdown", label="politics_concept"),
            PackQualityEntity(text="state of emergency", label="politics_concept"),
        ),
    ),
    PackQualityCase(
        name="peace-border-and-referendum",
        text=(
            "Peace talks produced a ceasefire agreement while border restrictions "
            "and a referendum advanced."
        ),
        expected_entities=(
            PackQualityEntity(text="Peace talks", label="politics_concept"),
            PackQualityEntity(text="ceasefire agreement", label="politics_concept"),
            PackQualityEntity(text="border restrictions", label="politics_concept"),
            PackQualityEntity(text="referendum", label="politics_concept"),
        ),
    ),
    PackQualityCase(
        name="trade-diplomacy-and-resource-policy",
        text=(
            "Trade negotiations resumed after diplomatic relations worsened, "
            "energy policy shifted, and critical minerals policy changed as "
            "mining regulation tightened."
        ),
        expected_entities=(
            PackQualityEntity(text="Trade negotiations", label="politics_concept"),
            PackQualityEntity(text="diplomatic relations", label="politics_concept"),
            PackQualityEntity(text="energy policy", label="politics_concept"),
            PackQualityEntity(text="critical minerals policy", label="politics_concept"),
            PackQualityEntity(text="mining regulation", label="politics_concept"),
        ),
    ),
    DEFAULT_POLITICS_SMOKE_QUALITY_CASES[2],
)


def validate_politics_pack_quality(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    fixture_profile: str = "benchmark",
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = DEFAULT_POLITICS_MAX_AMBIGUOUS_ALIASES,
    max_dropped_alias_ratio: float = 0.5,
) -> PackQualityResult:
    """Build, install, and evaluate one generated `politics-vector-en` bundle."""

    cases = _resolve_politics_quality_cases(fixture_profile)
    return validate_generated_pack_quality(
        bundle_dir,
        output_dir=output_dir,
        quality_dir_name="politics-quality",
        cases=cases,
        fixture_profile=fixture_profile,
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )


def _resolve_politics_quality_cases(
    fixture_profile: str,
) -> tuple[PackQualityCase, ...]:
    normalized_profile = fixture_profile.strip().casefold()
    if normalized_profile == "benchmark":
        return DEFAULT_POLITICS_BENCHMARK_QUALITY_CASES
    if normalized_profile == "smoke":
        return DEFAULT_POLITICS_SMOKE_QUALITY_CASES
    raise ValueError("fixture_profile must be one of: benchmark, smoke.")
