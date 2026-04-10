"""Quality validation for generated `general-en` pack bundles."""

from __future__ import annotations

from .quality_common import (
    PackQualityCase,
    PackQualityEntity,
    PackQualityResult,
    validate_generated_pack_quality,
)


DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES = 0


DEFAULT_GENERAL_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    PackQualityCase(
        name="entity-and-pattern-match",
        text="Tim Cook emailed jane@example.com about OpenAI in Istanbul. Visit https://openai.com now.",
        expected_entities=(
            PackQualityEntity(text="Tim Cook", label="person"),
            PackQualityEntity(text="jane@example.com", label="email_address"),
            PackQualityEntity(text="OpenAI", label="organization"),
            PackQualityEntity(text="Istanbul", label="location"),
            PackQualityEntity(text="https://openai.com", label="url"),
        ),
    ),
    PackQualityCase(
        name="alias-match",
        text="Timothy Cook met Open AI in Constantinople before lunch.",
        expected_entities=(
            PackQualityEntity(text="Timothy Cook", label="person"),
            PackQualityEntity(text="Open AI", label="organization"),
            PackQualityEntity(text="Constantinople", label="location"),
        ),
    ),
    PackQualityCase(
        name="non-hit",
        text="Ordinary weather notes and lunch plans stay untagged here.",
        expected_entities=(),
    ),
)


def validate_general_pack_quality(
    bundle_dir: str,
    *,
    output_dir: str,
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES,
    max_dropped_alias_ratio: float = 0.5,
) -> PackQualityResult:
    """Build, install, and evaluate one generated `general-en` pack bundle."""

    return validate_generated_pack_quality(
        bundle_dir,
        output_dir=output_dir,
        quality_dir_name="general-quality",
        cases=DEFAULT_GENERAL_QUALITY_CASES,
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )
