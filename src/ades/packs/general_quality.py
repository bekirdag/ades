"""Quality validation for generated `general-en` pack bundles."""

from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

from .general_structure import evaluate_general_pack_structure
from .quality_defaults import DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES
from .quality_common import (
    PackQualityCase,
    PackQualityEntity,
    PackQualityResult,
    validate_generated_pack_quality,
)


DEFAULT_GENERAL_SMOKE_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    PackQualityCase(
        name="entity-and-pattern-match",
        text="Person Alpha emailed contact@example.test about Org Alpha in Metro Alpha. Visit https://portal.example.test now.",
        expected_entities=(
            PackQualityEntity(text="Person Alpha", label="person"),
            PackQualityEntity(text="contact@example.test", label="email_address"),
            PackQualityEntity(text="Org Alpha", label="organization"),
            PackQualityEntity(text="Metro Alpha", label="location"),
            PackQualityEntity(text="https://portal.example.test", label="url"),
        ),
    ),
    PackQualityCase(
        name="alias-match",
        text="Person Alpha Variant met Org Alpha Variant in Metro Alpha Historic before lunch.",
        expected_entities=(
            PackQualityEntity(text="Person Alpha Variant", label="person"),
            PackQualityEntity(text="Org Alpha Variant", label="organization"),
            PackQualityEntity(text="Metro Alpha Historic", label="location"),
        ),
    ),
    PackQualityCase(
        name="non-hit",
        text="Ordinary weather notes and lunch plans stay untagged here.",
        expected_entities=(),
    ),
)


DEFAULT_GENERAL_BENCHMARK_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    *DEFAULT_GENERAL_SMOKE_QUALITY_CASES[:2],
    PackQualityCase(
        name="expanded-location-alias-match",
        text="Org Beta opened a store in Metro Beta Alt before lunch.",
        expected_entities=(
            PackQualityEntity(text="Org Beta", label="organization"),
            PackQualityEntity(text="Metro Beta Alt", label="location"),
        ),
    ),
    PackQualityCase(
        name="bounded-wikidata-person-expansion",
        text="Person Beta met Person Gamma in Metro Beta after the keynote.",
        expected_entities=(
            PackQualityEntity(text="Person Beta", label="person"),
            PackQualityEntity(text="Person Gamma", label="person"),
            PackQualityEntity(text="Metro Beta", label="location"),
        ),
    ),
    PackQualityCase(
        name="bounded-wikidata-organization-expansion",
        text="Org Delta partnered with Org Epsilon on a new release.",
        expected_entities=(
            PackQualityEntity(text="Org Delta", label="organization"),
            PackQualityEntity(text="Org Epsilon", label="organization"),
        ),
    ),
    PackQualityCase(
        name="bounded-wikidata-follow-on-expansion",
        text="Person Delta visited Org Zeta Variant after the launch.",
        expected_entities=(
            PackQualityEntity(text="Person Delta", label="person"),
            PackQualityEntity(text="Org Zeta Variant", label="organization"),
        ),
    ),
    PackQualityCase(
        name="bounded-wikidata-person-alias-pruning-expansion",
        text="Person Epsilon Variant Three met Person Zeta after lunch.",
        expected_entities=(
            PackQualityEntity(text="Person Epsilon Variant Three", label="person"),
            PackQualityEntity(text="Person Zeta", label="person"),
        ),
    ),
    PackQualityCase(
        name="bounded-wikidata-retained-alias-expansion",
        text="Person Eta Variant met Person Theta Variant after the keynote.",
        expected_entities=(
            PackQualityEntity(text="Person Eta Variant", label="person"),
            PackQualityEntity(text="Person Theta Variant", label="person"),
        ),
    ),
    PackQualityCase(
        name="curated-location-alias-expansion",
        text="Metro Gamma Variant hosted the summit overnight.",
        expected_entities=(
            PackQualityEntity(text="Metro Gamma Variant", label="location"),
        ),
    ),
    PackQualityCase(
        name="generic-prose-noise-regression",
        text=(
            "This market note in March said stock traders in Europe were "
            "cautious."
        ),
        expected_entities=(
            PackQualityEntity(text="Europe", label="location"),
        ),
    ),
    PackQualityCase(
        name="document-acronym-backfill",
        text=(
            "International Energy Agency (IEA) officials met delegates from "
            "the United States (US). Later, the IEA said the US would respond."
        ),
        expected_entities=(
            PackQualityEntity(text="International Energy Agency", label="organization"),
            PackQualityEntity(text="United States", label="location"),
            PackQualityEntity(text="IEA", label="organization"),
            PackQualityEntity(text="US", label="location"),
        ),
    ),
    PackQualityCase(
        name="prefer-longest-valid-span",
        text=(
            "The Strait of Talora reopened after the National Bank of Talora "
            "published guidance."
        ),
        expected_entities=(
            PackQualityEntity(text="Strait of Talora", label="location"),
            PackQualityEntity(text="National Bank of Talora", label="organization"),
        ),
        normalize_subsumed_hits=False,
    ),
    PackQualityCase(
        name="geopolitical-and-structured-organization-recall",
        text=(
            "Four analysts in China said Israel would rely on Shanghai-based "
            "Northwind Solutions while Harbor China Information reviewed the "
            "supply outlook."
        ),
        expected_entities=(
            PackQualityEntity(text="China", label="location"),
            PackQualityEntity(text="Israel", label="location"),
            PackQualityEntity(text="Shanghai", label="location"),
            PackQualityEntity(text="Northwind Solutions", label="organization"),
            PackQualityEntity(text="Harbor China Information", label="organization"),
        ),
    ),
    DEFAULT_GENERAL_SMOKE_QUALITY_CASES[2],
)


def validate_general_pack_quality(
    bundle_dir: str,
    *,
    output_dir: str,
    fixture_profile: str = "benchmark",
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES,
    max_dropped_alias_ratio: float = 0.5,
) -> PackQualityResult:
    """Build, install, and evaluate one generated `general-en` pack bundle."""

    cases = _resolve_general_quality_cases(fixture_profile)
    result = validate_generated_pack_quality(
        bundle_dir,
        output_dir=output_dir,
        quality_dir_name="general-quality",
        cases=cases,
        fixture_profile=fixture_profile,
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )
    return _apply_general_structural_gates(result)


def _resolve_general_quality_cases(
    fixture_profile: str,
) -> tuple[PackQualityCase, ...]:
    normalized_profile = fixture_profile.strip().casefold()
    if normalized_profile == "benchmark":
        return DEFAULT_GENERAL_BENCHMARK_QUALITY_CASES
    if normalized_profile == "smoke":
        return DEFAULT_GENERAL_SMOKE_QUALITY_CASES
    raise ValueError(
        "fixture_profile must be one of: benchmark, smoke."
    )


def _apply_general_structural_gates(result: PackQualityResult) -> PackQualityResult:
    build_path = Path(result.generated_pack_dir) / "build.json"
    if not build_path.exists():
        return result
    build_payload = json.loads(build_path.read_text(encoding="utf-8"))
    new_failures, new_warnings = evaluate_general_pack_structure(build_payload)
    failures = [*result.failures, *new_failures]
    warnings = [*result.warnings, *new_warnings]
    return replace(
        result,
        passed=not failures,
        failures=failures,
        warnings=warnings,
    )
