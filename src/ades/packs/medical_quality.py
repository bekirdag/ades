"""Quality validation for generated `medical-en` pack bundles."""

from __future__ import annotations

from pathlib import Path

from .quality_common import (
    PackQualityCase,
    PackQualityEntity,
    PackQualityResult,
    validate_generated_pack_quality,
)


DEFAULT_MEDICAL_MAX_AMBIGUOUS_ALIASES = 25


DEFAULT_MEDICAL_SMOKE_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    PackQualityCase(
        name="canonical-match",
        text="GENEA1 and Protein Alpha were studied in NCT00000001 after Compound Alpha 10 mg dosing for disease alpha.",
        expected_entities=(
            PackQualityEntity(text="GENEA1", label="gene"),
            PackQualityEntity(text="Protein Alpha", label="protein"),
            PackQualityEntity(text="NCT00000001", label="clinical_trial"),
            PackQualityEntity(text="Compound Alpha", label="drug"),
            PackQualityEntity(text="10 mg", label="dosage"),
            PackQualityEntity(text="disease alpha", label="disease"),
        ),
    ),
    PackQualityCase(
        name="alias-match",
        text="GENEA1ALT and Protein Alpha Long Form were tracked in NCT00000001 after Compound Alpha Acid 10 mg for disease beta alias.",
        expected_entities=(
            PackQualityEntity(text="GENEA1ALT", label="gene"),
            PackQualityEntity(text="Protein Alpha Long Form", label="protein"),
            PackQualityEntity(text="NCT00000001", label="clinical_trial"),
            PackQualityEntity(text="Compound Alpha Acid", label="drug"),
            PackQualityEntity(text="10 mg", label="dosage"),
            PackQualityEntity(text="disease beta alias", label="disease"),
        ),
    ),
    PackQualityCase(
        name="non-hit",
        text="The patient rested quietly after breakfast yesterday.",
        expected_entities=(),
    ),
)


DEFAULT_MEDICAL_BENCHMARK_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    *DEFAULT_MEDICAL_SMOKE_QUALITY_CASES[:2],
    PackQualityCase(
        name="dependency-plus-domain-compatibility",
        text="Person Alpha discussed GENEA1 and disease alpha after Compound Alpha dosing in Metro Alpha.",
        expected_entities=(
            PackQualityEntity(text="Person Alpha", label="person"),
            PackQualityEntity(text="GENEA1", label="gene"),
            PackQualityEntity(text="disease alpha", label="disease"),
            PackQualityEntity(text="Compound Alpha", label="drug"),
            PackQualityEntity(text="Metro Alpha", label="location"),
        ),
    ),
    PackQualityCase(
        name="trial-and-protein-expansion",
        text="NCT00000001 tracked Protein Alpha after Compound Alpha Acid dosing for disease beta.",
        expected_entities=(
            PackQualityEntity(text="NCT00000001", label="clinical_trial"),
            PackQualityEntity(text="Protein Alpha", label="protein"),
            PackQualityEntity(text="Compound Alpha Acid", label="drug"),
            PackQualityEntity(text="disease beta", label="disease"),
        ),
    ),
    DEFAULT_MEDICAL_SMOKE_QUALITY_CASES[2],
)


def validate_medical_pack_quality(
    bundle_dir: str | Path,
    *,
    general_bundle_dir: str | Path,
    output_dir: str,
    fixture_profile: str = "benchmark",
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = DEFAULT_MEDICAL_MAX_AMBIGUOUS_ALIASES,
    max_dropped_alias_ratio: float = 0.5,
) -> PackQualityResult:
    """Build, install, and evaluate one generated `medical-en` pack bundle."""

    cases = _resolve_medical_quality_cases(fixture_profile)
    return validate_generated_pack_quality(
        bundle_dir,
        output_dir=output_dir,
        quality_dir_name="medical-quality",
        cases=cases,
        fixture_profile=fixture_profile,
        dependency_bundle_dirs=(general_bundle_dir,),
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )


def _resolve_medical_quality_cases(
    fixture_profile: str,
) -> tuple[PackQualityCase, ...]:
    normalized_profile = fixture_profile.strip().casefold()
    if normalized_profile == "benchmark":
        return DEFAULT_MEDICAL_BENCHMARK_QUALITY_CASES
    if normalized_profile == "smoke":
        return DEFAULT_MEDICAL_SMOKE_QUALITY_CASES
    raise ValueError("fixture_profile must be one of: benchmark, smoke.")
