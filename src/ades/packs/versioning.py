"""Pack diffing and release-threshold helpers for extraction stability."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path

from ..extraction_quality import ExtractionQualityDelta, ExtractionQualityReport, compare_quality_reports
from ..text_processing import normalize_lookup_text


@dataclass(frozen=True)
class PackDiff:
    """Machine-readable diff between two pack directories."""

    pack_id: str
    from_version: str | None
    to_version: str | None
    severity: str
    requires_retag: bool
    added_labels: list[str] = field(default_factory=list)
    removed_labels: list[str] = field(default_factory=list)
    added_rules: list[str] = field(default_factory=list)
    removed_rules: list[str] = field(default_factory=list)
    added_aliases: list[str] = field(default_factory=list)
    removed_aliases: list[str] = field(default_factory=list)
    changed_canonical_aliases: list[str] = field(default_factory=list)
    changed_metadata_fields: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReleaseThresholds:
    """Quality gates for deterministic or hybrid pack release decisions."""

    min_recall: float = 0.85
    min_precision: float = 0.85
    max_label_recall_drop: float = 0.0
    min_recall_lift: float = 0.10
    max_precision_drop: float = 0.05
    max_p95_latency_ms: int | None = None
    max_model_artifact_bytes: int | None = None
    max_peak_memory_mb: int | None = None


DEFAULT_DETERMINISTIC_RELEASE_THRESHOLDS = ReleaseThresholds(
    min_recall=0.85,
    min_precision=0.85,
    max_label_recall_drop=0.0,
    max_p95_latency_ms=100,
)

DEFAULT_HYBRID_RELEASE_THRESHOLDS = ReleaseThresholds(
    min_recall=0.85,
    min_precision=0.85,
    max_label_recall_drop=0.0,
    min_recall_lift=0.10,
    max_precision_drop=0.05,
    max_p95_latency_ms=150,
    max_model_artifact_bytes=1_000_000_000,
    max_peak_memory_mb=500,
)


@dataclass(frozen=True)
class ReleaseThresholdDecision:
    """Structured release decision for one extraction-quality report."""

    mode: str
    passed: bool
    reasons: list[str] = field(default_factory=list)
    quality_delta: ExtractionQualityDelta | None = None


def default_release_thresholds(mode: str) -> ReleaseThresholds:
    """Return the locked default release thresholds for one evaluation mode."""

    if mode == "deterministic":
        return DEFAULT_DETERMINISTIC_RELEASE_THRESHOLDS
    if mode == "hybrid":
        return DEFAULT_HYBRID_RELEASE_THRESHOLDS
    raise ValueError("mode must be deterministic or hybrid.")


def diff_pack_directories(
    old_pack_dir: str | Path,
    new_pack_dir: str | Path,
) -> PackDiff:
    """Diff two pack directories and classify the semantic update severity."""

    old_pack = _load_pack(Path(old_pack_dir).expanduser().resolve())
    new_pack = _load_pack(Path(new_pack_dir).expanduser().resolve())
    alias_diff = _diff_aliases(old_pack["aliases"], new_pack["aliases"])
    rule_diff = _diff_rules(old_pack["rules"], new_pack["rules"])
    old_labels = set(old_pack["labels"])
    new_labels = set(new_pack["labels"])
    added_labels = sorted(new_labels - old_labels)
    removed_labels = sorted(old_labels - new_labels)
    changed_metadata_fields = sorted(
        key
        for key in {"description", "tier", "tags", "dependencies", "min_entities_per_100_tokens_warning"}
        if old_pack["manifest"].get(key) != new_pack["manifest"].get(key)
    )

    major = bool(
        removed_labels
        or alias_diff["removed"]
        or alias_diff["changed"]
        or rule_diff["removed"]
        or old_pack["manifest"].get("pack_id") != new_pack["manifest"].get("pack_id")
    )
    minor = bool(
        added_labels
        or alias_diff["added"]
        or rule_diff["added"]
    )
    severity = "major" if major else "minor" if minor else "patch"
    requires_retag = bool(
        major
        or minor
    )
    return PackDiff(
        pack_id=str(new_pack["manifest"]["pack_id"]),
        from_version=old_pack["manifest"].get("version"),
        to_version=new_pack["manifest"].get("version"),
        severity=severity,
        requires_retag=requires_retag,
        added_labels=added_labels,
        removed_labels=removed_labels,
        added_rules=rule_diff["added"],
        removed_rules=rule_diff["removed"],
        added_aliases=alias_diff["added"],
        removed_aliases=alias_diff["removed"],
        changed_canonical_aliases=alias_diff["changed"],
        changed_metadata_fields=changed_metadata_fields,
    )


def evaluate_release_thresholds(
    report: ExtractionQualityReport,
    *,
    mode: str,
    thresholds: ReleaseThresholds,
    baseline_report: ExtractionQualityReport | None = None,
    model_artifact_bytes: int | None = None,
    peak_memory_mb: int | None = None,
) -> ReleaseThresholdDecision:
    """Evaluate deterministic or hybrid release thresholds."""

    reasons: list[str] = []
    quality_delta: ExtractionQualityDelta | None = None
    if report.recall < thresholds.min_recall:
        reasons.append(
            f"recall_below_threshold:{report.recall:.4f}<{thresholds.min_recall:.4f}"
        )
    if report.precision < thresholds.min_precision:
        reasons.append(
            f"precision_below_threshold:{report.precision:.4f}<{thresholds.min_precision:.4f}"
        )
    if thresholds.max_p95_latency_ms is not None and report.p95_latency_ms > thresholds.max_p95_latency_ms:
        reasons.append(
            f"p95_latency_above_threshold:{report.p95_latency_ms}>{thresholds.max_p95_latency_ms}"
        )

    if mode == "hybrid":
        if baseline_report is None:
            reasons.append("hybrid_requires_baseline_report")
        else:
            quality_delta = compare_quality_reports(baseline_report, report)
            if quality_delta.recall_delta < thresholds.min_recall_lift:
                reasons.append(
                    f"recall_lift_below_threshold:{quality_delta.recall_delta:.4f}<{thresholds.min_recall_lift:.4f}"
                )
            precision_drop = baseline_report.precision - report.precision
            if precision_drop > thresholds.max_precision_drop:
                reasons.append(
                    f"precision_drop_above_threshold:{precision_drop:.4f}>{thresholds.max_precision_drop:.4f}"
                )
            for label_delta in quality_delta.per_label_deltas:
                if label_delta.recall_delta < -thresholds.max_label_recall_drop:
                    reasons.append(
                        "label_recall_drop:"
                        f"{label_delta.label}:{label_delta.recall_delta:.4f}"
                    )
        if thresholds.max_model_artifact_bytes is not None and model_artifact_bytes is not None:
            if model_artifact_bytes > thresholds.max_model_artifact_bytes:
                reasons.append(
                    "model_artifact_above_threshold:"
                    f"{model_artifact_bytes}>{thresholds.max_model_artifact_bytes}"
                )
        if thresholds.max_peak_memory_mb is not None and peak_memory_mb is not None:
            if peak_memory_mb > thresholds.max_peak_memory_mb:
                reasons.append(
                    f"peak_memory_above_threshold:{peak_memory_mb}>{thresholds.max_peak_memory_mb}"
                )
    elif baseline_report is not None:
        quality_delta = compare_quality_reports(baseline_report, report)
        for label_delta in quality_delta.per_label_deltas:
            if label_delta.recall_delta < -thresholds.max_label_recall_drop:
                reasons.append(
                    "label_recall_drop:"
                    f"{label_delta.label}:{label_delta.recall_delta:.4f}"
                )

    return ReleaseThresholdDecision(
        mode=mode,
        passed=not reasons,
        reasons=reasons,
        quality_delta=quality_delta,
    )


def _load_pack(pack_dir: Path) -> dict[str, object]:
    manifest = json.loads((pack_dir / "manifest.json").read_text(encoding="utf-8"))
    aliases = json.loads((pack_dir / "aliases.json").read_text(encoding="utf-8")).get("aliases", [])
    rules = json.loads((pack_dir / "rules.json").read_text(encoding="utf-8")).get("patterns", [])
    labels = json.loads((pack_dir / "labels.json").read_text(encoding="utf-8"))
    return {
        "manifest": manifest,
        "aliases": aliases,
        "rules": rules,
        "labels": labels,
    }


def _diff_aliases(
    old_aliases: list[dict[str, object]],
    new_aliases: list[dict[str, object]],
) -> dict[str, list[str]]:
    old_index = {
        _alias_key(item): item
        for item in old_aliases
    }
    new_index = {
        _alias_key(item): item
        for item in new_aliases
    }
    added = sorted(_render_alias_key(key) for key in new_index.keys() - old_index.keys())
    removed = sorted(_render_alias_key(key) for key in old_index.keys() - new_index.keys())
    changed: list[str] = []
    for key in sorted(old_index.keys() & new_index.keys()):
        old_item = old_index[key]
        new_item = new_index[key]
        if (
            str(old_item.get("canonical_text") or old_item.get("text") or "")
            != str(new_item.get("canonical_text") or new_item.get("text") or "")
            or str(old_item.get("entity_id") or "")
            != str(new_item.get("entity_id") or "")
        ):
            changed.append(_render_alias_key(key))
    return {"added": added, "removed": removed, "changed": changed}


def _diff_rules(
    old_rules: list[dict[str, object]],
    new_rules: list[dict[str, object]],
) -> dict[str, list[str]]:
    old_index = {_rule_key(item) for item in old_rules}
    new_index = {_rule_key(item) for item in new_rules}
    return {
        "added": sorted(new_index - old_index),
        "removed": sorted(old_index - new_index),
    }


def _alias_key(item: dict[str, object]) -> tuple[str, str]:
    return (
        normalize_lookup_text(str(item.get("text") or "")),
        str(item.get("label") or "").casefold(),
    )


def _render_alias_key(key: tuple[str, str]) -> str:
    return f"{key[0]}:{key[1]}"


def _rule_key(item: dict[str, object]) -> str:
    return (
        f"{str(item.get('name') or '').casefold()}:"
        f"{str(item.get('label') or '').casefold()}:"
        f"{str(item.get('pattern') or '')}"
    )
