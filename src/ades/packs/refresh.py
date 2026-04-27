"""Offline helpers for refreshing quality-gated generated pack registry releases."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
import shutil

from .finance_quality import (
    DEFAULT_FINANCE_MAX_AMBIGUOUS_ALIASES,
    DEFAULT_FINANCE_MAX_DROPPED_ALIAS_RATIO,
    DEFAULT_FINANCE_REGIONAL_MAX_DROPPED_ALIAS_RATIO,
    validate_finance_pack_quality,
)
from .general_quality import (
    DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES,
    validate_general_pack_quality,
)
from .generation import SourceBundleManifest
from .medical_quality import (
    DEFAULT_MEDICAL_MAX_AMBIGUOUS_ALIASES,
    validate_medical_pack_quality,
)
from .publish import RegistryBuildResult, build_static_registry
from .quality_common import PackQualityResult
from .reporting import GeneratedPackReport, report_generated_pack


@dataclass(frozen=True)
class GeneratedPackRefreshItem:
    """Reported and validated state for one bundle inside a refresh run."""

    pack_id: str
    bundle_dir: str
    report: GeneratedPackReport
    quality: PackQualityResult


@dataclass(frozen=True)
class GeneratedPackRefreshResult:
    """Aggregate result for one generated-pack refresh run."""

    output_dir: str
    candidate_dir: str
    report_dir: str
    quality_dir: str
    generated_at: str
    pack_count: int
    passed: bool
    materialize_registry: bool
    registry_materialized: bool
    registry: RegistryBuildResult | None = None
    warnings: list[str] = field(default_factory=list)
    packs: list[GeneratedPackRefreshItem] = field(default_factory=list)


def refresh_generated_pack_registry(
    bundle_dirs: list[str | Path],
    *,
    dependency_bundle_dirs: list[str | Path] | None = None,
    output_dir: str | Path,
    general_bundle_dir: str | Path | None = None,
    materialize_registry: bool = False,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int | None = None,
    max_dropped_alias_ratio: float | None = None,
) -> GeneratedPackRefreshResult:
    """Refresh one or more generated packs into candidate bundles and optional registry output."""

    if not bundle_dirs:
        raise ValueError("At least one bundle directory is required.")
    _validate_thresholds(
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    _ensure_clean_dir(resolved_output_dir)
    candidate_dir = resolved_output_dir / "candidates"
    report_dir = candidate_dir
    quality_dir = resolved_output_dir / "quality"
    candidate_dir.mkdir(parents=True, exist_ok=True)
    quality_dir.mkdir(parents=True, exist_ok=True)

    manifests_by_dir: dict[Path, SourceBundleManifest] = {}
    bundle_dirs_by_pack_id: dict[str, Path] = {}
    resolved_bundle_dirs: list[Path] = []
    for raw_bundle_dir in bundle_dirs:
        resolved_bundle_dir = _resolve_bundle_dir(raw_bundle_dir)
        manifest = SourceBundleManifest.from_path(resolved_bundle_dir / "bundle.json")
        if manifest.pack_id in bundle_dirs_by_pack_id:
            raise ValueError(
                f"Duplicate pack id in generated-pack refresh request: {manifest.pack_id}"
            )
        resolved_bundle_dirs.append(resolved_bundle_dir)
        manifests_by_dir[resolved_bundle_dir] = manifest
        bundle_dirs_by_pack_id[manifest.pack_id] = resolved_bundle_dir
    for raw_bundle_dir in dependency_bundle_dirs or []:
        resolved_bundle_dir = _resolve_bundle_dir(raw_bundle_dir)
        manifest = SourceBundleManifest.from_path(resolved_bundle_dir / "bundle.json")
        existing_bundle_dir = bundle_dirs_by_pack_id.get(manifest.pack_id)
        if existing_bundle_dir is not None:
            if existing_bundle_dir != resolved_bundle_dir:
                raise ValueError(
                    f"Duplicate pack id in generated-pack refresh dependencies: {manifest.pack_id}"
                )
            continue
        manifests_by_dir[resolved_bundle_dir] = manifest
        bundle_dirs_by_pack_id[manifest.pack_id] = resolved_bundle_dir

    resolved_general_bundle_dir = _resolve_general_bundle_dir(
        general_bundle_dir=general_bundle_dir,
        bundle_dirs_by_pack_id=bundle_dirs_by_pack_id,
    )

    pack_results: list[GeneratedPackRefreshItem] = []
    warnings: list[str] = []
    for resolved_bundle_dir in resolved_bundle_dirs:
        manifest = manifests_by_dir[resolved_bundle_dir]
        report = report_generated_pack(resolved_bundle_dir, output_dir=candidate_dir)
        quality = _validate_quality_for_pack(
            manifest=manifest,
            bundle_dir=resolved_bundle_dir,
            bundle_dirs_by_pack_id=bundle_dirs_by_pack_id,
            general_bundle_dir=resolved_general_bundle_dir,
            output_dir=quality_dir,
            min_expected_recall=min_expected_recall,
            max_unexpected_hits=max_unexpected_hits,
            max_ambiguous_aliases=max_ambiguous_aliases,
            max_dropped_alias_ratio=max_dropped_alias_ratio,
        )
        warnings.extend(f"{report.pack_id}: {warning}" for warning in report.warnings)
        warnings.extend(f"{quality.pack_id}: {warning}" for warning in quality.warnings)
        pack_results.append(
            GeneratedPackRefreshItem(
                pack_id=manifest.pack_id,
                bundle_dir=str(resolved_bundle_dir),
                report=report,
                quality=quality,
            )
        )

    quality_passed = all(item.quality.passed for item in pack_results)
    source_governance_passed = all(
        item.report.publishable_sources_only for item in pack_results
    )
    passed = quality_passed and source_governance_passed
    registry_result: RegistryBuildResult | None = None
    if passed and materialize_registry:
        registry_result = build_static_registry(
            [item.report.pack_dir for item in pack_results],
            output_dir=resolved_output_dir / "registry",
        )
    elif passed:
        warnings.append("registry_build_skipped:candidate_only")
    else:
        if not quality_passed:
            warnings.append("registry_build_skipped:quality_failed")
        if not source_governance_passed:
            warnings.append("registry_build_skipped:source_governance_failed")

    return GeneratedPackRefreshResult(
        output_dir=str(resolved_output_dir),
        candidate_dir=str(candidate_dir),
        report_dir=str(report_dir),
        quality_dir=str(quality_dir),
        generated_at=_utc_timestamp(),
        pack_count=len(pack_results),
        passed=passed,
        materialize_registry=materialize_registry,
        registry_materialized=registry_result is not None,
        registry=registry_result,
        warnings=warnings,
        packs=pack_results,
    )


def _resolve_bundle_dir(bundle_dir: str | Path) -> Path:
    resolved_bundle_dir = Path(bundle_dir).expanduser().resolve()
    if not resolved_bundle_dir.exists():
        raise FileNotFoundError(f"Bundle directory not found: {resolved_bundle_dir}")
    if not resolved_bundle_dir.is_dir():
        raise NotADirectoryError(
            f"Bundle directory is not a directory: {resolved_bundle_dir}"
        )
    bundle_manifest_path = resolved_bundle_dir / "bundle.json"
    if not bundle_manifest_path.exists():
        raise FileNotFoundError(f"Bundle manifest not found: {bundle_manifest_path}")
    return resolved_bundle_dir


def _resolve_general_bundle_dir(
    *,
    general_bundle_dir: str | Path | None,
    bundle_dirs_by_pack_id: dict[str, Path],
) -> Path | None:
    if general_bundle_dir is None:
        return bundle_dirs_by_pack_id.get("general-en")

    resolved_bundle_dir = _resolve_bundle_dir(general_bundle_dir)
    manifest = SourceBundleManifest.from_path(resolved_bundle_dir / "bundle.json")
    if manifest.pack_id != "general-en":
        raise ValueError(
            f"general_bundle_dir must point at a general-en bundle, got: {manifest.pack_id}"
        )
    return resolved_bundle_dir


def _validate_quality_for_pack(
    *,
    manifest: SourceBundleManifest,
    bundle_dir: Path,
    bundle_dirs_by_pack_id: dict[str, Path],
    general_bundle_dir: Path | None,
    output_dir: Path,
    min_expected_recall: float,
    max_unexpected_hits: int,
    max_ambiguous_aliases: int | None,
    max_dropped_alias_ratio: float | None,
) -> PackQualityResult:
    resolved_max_ambiguous_aliases = _resolve_max_ambiguous_aliases(
        manifest.pack_id,
        max_ambiguous_aliases=max_ambiguous_aliases,
    )
    resolved_max_dropped_alias_ratio = _resolve_max_dropped_alias_ratio(
        manifest.pack_id,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )
    dependency_bundle_dirs = tuple(
        bundle_dirs_by_pack_id[dependency]
        for dependency in manifest.dependencies
        if dependency in bundle_dirs_by_pack_id and dependency != manifest.pack_id
    )
    if manifest.pack_id == "finance-en" or (
        manifest.pack_id.startswith("finance-") and manifest.pack_id.endswith("-en")
    ):
        return validate_finance_pack_quality(
            str(bundle_dir),
            output_dir=str(output_dir),
            fixture_profile=(
                "benchmark" if manifest.pack_id == "finance-en" else "regional"
            ),
            dependency_bundle_dirs=dependency_bundle_dirs,
            min_expected_recall=min_expected_recall,
            max_unexpected_hits=max_unexpected_hits,
            max_ambiguous_aliases=resolved_max_ambiguous_aliases,
            max_dropped_alias_ratio=resolved_max_dropped_alias_ratio,
        )
    if manifest.pack_id == "general-en":
        return validate_general_pack_quality(
            str(bundle_dir),
            output_dir=str(output_dir),
            min_expected_recall=min_expected_recall,
            max_unexpected_hits=max_unexpected_hits,
            max_ambiguous_aliases=resolved_max_ambiguous_aliases,
            max_dropped_alias_ratio=resolved_max_dropped_alias_ratio,
        )
    if manifest.pack_id == "medical-en":
        if general_bundle_dir is None:
            raise ValueError(
                "medical-en refresh requires a general-en bundle in bundle_dirs or --general-bundle-dir."
            )
        return validate_medical_pack_quality(
            str(bundle_dir),
            general_bundle_dir=str(general_bundle_dir),
            output_dir=str(output_dir),
            min_expected_recall=min_expected_recall,
            max_unexpected_hits=max_unexpected_hits,
            max_ambiguous_aliases=resolved_max_ambiguous_aliases,
            max_dropped_alias_ratio=resolved_max_dropped_alias_ratio,
        )
    raise ValueError(
        f"No generated-pack quality profile is registered for pack: {manifest.pack_id}"
    )


def _ensure_clean_dir(path: Path) -> None:
    if path.exists():
        if not path.is_dir():
            raise NotADirectoryError(f"Output directory is not a directory: {path}")
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _validate_thresholds(
    *,
    min_expected_recall: float,
    max_unexpected_hits: int,
    max_ambiguous_aliases: int | None,
    max_dropped_alias_ratio: float | None,
) -> None:
    if not 0.0 <= min_expected_recall <= 1.0:
        raise ValueError("min_expected_recall must be between 0.0 and 1.0.")
    if max_unexpected_hits < 0:
        raise ValueError("max_unexpected_hits must be >= 0.")
    if max_ambiguous_aliases is not None and max_ambiguous_aliases < 0:
        raise ValueError("max_ambiguous_aliases must be >= 0.")
    if max_dropped_alias_ratio is not None and not 0.0 <= max_dropped_alias_ratio <= 1.0:
        raise ValueError("max_dropped_alias_ratio must be between 0.0 and 1.0.")


def _resolve_max_ambiguous_aliases(
    pack_id: str,
    *,
    max_ambiguous_aliases: int | None,
) -> int:
    if max_ambiguous_aliases is not None:
        return max_ambiguous_aliases
    if pack_id == "finance-en" or (
        pack_id.startswith("finance-") and pack_id.endswith("-en")
    ):
        return DEFAULT_FINANCE_MAX_AMBIGUOUS_ALIASES
    if pack_id == "medical-en":
        return DEFAULT_MEDICAL_MAX_AMBIGUOUS_ALIASES
    return DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES


def _resolve_max_dropped_alias_ratio(
    pack_id: str,
    *,
    max_dropped_alias_ratio: float | None,
) -> float:
    if max_dropped_alias_ratio is not None:
        return max_dropped_alias_ratio
    if pack_id.startswith("finance-") and pack_id.endswith("-en") and pack_id != "finance-en":
        return DEFAULT_FINANCE_REGIONAL_MAX_DROPPED_ALIAS_RATIO
    return DEFAULT_FINANCE_MAX_DROPPED_ALIAS_RATIO


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
