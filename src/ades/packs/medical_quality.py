"""Quality validation for generated `medical-en` pack bundles."""

from __future__ import annotations

from .quality_common import (
    PackQualityCase,
    PackQualityEntity,
    PackQualityResult,
    validate_generated_pack_quality,
)


DEFAULT_MEDICAL_QUALITY_CASES: tuple[PackQualityCase, ...] = (
    PackQualityCase(
        name="canonical-match",
        text="BRCA1 and p53 protein were studied in NCT04280705 after Aspirin 10 mg dosing for diabetes.",
        expected_entities=(
            PackQualityEntity(text="BRCA1", label="gene"),
            PackQualityEntity(text="p53 protein", label="protein"),
            PackQualityEntity(text="NCT04280705", label="clinical_trial"),
            PackQualityEntity(text="Aspirin", label="drug"),
            PackQualityEntity(text="10 mg", label="dosage"),
            PackQualityEntity(text="diabetes", label="disease"),
        ),
    ),
    PackQualityCase(
        name="alias-match",
        text="RNF53 and cellular tumor antigen p53 were tracked in NCT04280705 after acetylsalicylic acid 10 mg for flu.",
        expected_entities=(
            PackQualityEntity(text="RNF53", label="gene"),
            PackQualityEntity(text="cellular tumor antigen p53", label="protein"),
            PackQualityEntity(text="NCT04280705", label="clinical_trial"),
            PackQualityEntity(text="acetylsalicylic acid", label="drug"),
            PackQualityEntity(text="10 mg", label="dosage"),
            PackQualityEntity(text="flu", label="disease"),
        ),
    ),
    PackQualityCase(
        name="non-hit",
        text="The patient rested quietly after breakfast yesterday.",
        expected_entities=(),
    ),
)


def validate_medical_pack_quality(
    bundle_dir: str,
    *,
    general_bundle_dir: str,
    output_dir: str,
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = 0,
    max_dropped_alias_ratio: float = 0.5,
) -> PackQualityResult:
    """Build, install, and evaluate one generated `medical-en` pack bundle."""

    return validate_generated_pack_quality(
        bundle_dir,
        output_dir=output_dir,
        quality_dir_name="medical-quality",
        cases=DEFAULT_MEDICAL_QUALITY_CASES,
        dependency_bundle_dirs=(general_bundle_dir,),
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )
