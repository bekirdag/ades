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
        name="expanded-location-alias-match",
        text="Apple opened a store in Londres before lunch.",
        expected_entities=(
            PackQualityEntity(text="Apple", label="organization"),
            PackQualityEntity(text="Londres", label="location"),
        ),
    ),
    PackQualityCase(
        name="bounded-wikidata-person-expansion",
        text="Satya Nadella met Jensen Huang in London after the keynote.",
        expected_entities=(
            PackQualityEntity(text="Satya Nadella", label="person"),
            PackQualityEntity(text="Jensen Huang", label="person"),
            PackQualityEntity(text="London", label="location"),
        ),
    ),
    PackQualityCase(
        name="bounded-wikidata-organization-expansion",
        text="Nvidia partnered with Anthropic on a new release.",
        expected_entities=(
            PackQualityEntity(text="Nvidia", label="organization"),
            PackQualityEntity(text="Anthropic", label="organization"),
        ),
    ),
    PackQualityCase(
        name="bounded-wikidata-follow-on-expansion",
        text="Sam Altman visited DeepMind Technologies after the launch.",
        expected_entities=(
            PackQualityEntity(text="Sam Altman", label="person"),
            PackQualityEntity(text="DeepMind Technologies", label="organization"),
        ),
    ),
    PackQualityCase(
        name="bounded-wikidata-person-alias-pruning-expansion",
        text="Sundara Pichai met Demis Hassabis after lunch.",
        expected_entities=(
            PackQualityEntity(text="Sundara Pichai", label="person"),
            PackQualityEntity(text="Demis Hassabis", label="person"),
        ),
    ),
    PackQualityCase(
        name="bounded-wikidata-retained-alias-expansion",
        text="Ermira Murati met Ilya Efimovich Sutskever after the keynote.",
        expected_entities=(
            PackQualityEntity(text="Ermira Murati", label="person"),
            PackQualityEntity(text="Ilya Efimovich Sutskever", label="person"),
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
