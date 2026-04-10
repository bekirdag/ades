"""Shared reporting helpers for generated pack bundles."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

from .generation import SourceBundleManifest, generate_pack_source


@dataclass(frozen=True)
class GeneratedPackReport:
    """Aggregate statistics for one generated runtime-compatible pack."""

    pack_id: str
    version: str
    bundle_dir: str
    output_dir: str
    pack_dir: str
    manifest_path: str
    labels_path: str
    aliases_path: str
    rules_path: str
    sources_path: str | None
    build_path: str | None
    generated_at: str
    source_count: int
    label_count: int
    alias_count: int
    unique_canonical_count: int
    rule_count: int
    included_entity_count: int
    included_rule_count: int
    dropped_record_count: int
    dropped_alias_count: int
    ambiguous_alias_count: int
    dropped_alias_ratio: float
    label_distribution: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def report_generated_pack(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    version: str | None = None,
    include_build_metadata: bool = True,
    include_build_only: bool = False,
) -> GeneratedPackReport:
    """Generate one pack directory and report stable output statistics."""

    resolved_bundle_dir = Path(bundle_dir).expanduser().resolve()
    if not resolved_bundle_dir.exists():
        raise FileNotFoundError(f"Bundle directory not found: {resolved_bundle_dir}")
    if not resolved_bundle_dir.is_dir():
        raise NotADirectoryError(f"Bundle directory is not a directory: {resolved_bundle_dir}")

    bundle_manifest_path = resolved_bundle_dir / "bundle.json"
    if not bundle_manifest_path.exists():
        raise FileNotFoundError(f"Bundle manifest not found: {bundle_manifest_path}")

    bundle = SourceBundleManifest.from_path(bundle_manifest_path)
    generated = generate_pack_source(
        resolved_bundle_dir,
        output_dir=output_dir,
        version=version,
        include_build_metadata=include_build_metadata,
        include_build_only=include_build_only,
    )

    unique_canonical_count = _count_unique_canonicals(
        resolved_bundle_dir / bundle.entities_path,
        include_build_only=include_build_only,
    )
    label_distribution = _load_label_distribution(
        Path(generated.aliases_path),
        Path(generated.rules_path),
    )
    dropped_alias_total = generated.alias_count + generated.dropped_alias_count
    dropped_alias_ratio = (
        round(generated.dropped_alias_count / dropped_alias_total, 4)
        if dropped_alias_total
        else 0.0
    )

    warnings = list(generated.warnings)
    if unique_canonical_count == 0:
        warnings.append("Generated pack does not include any canonical entities.")

    return GeneratedPackReport(
        pack_id=generated.pack_id,
        version=generated.version,
        bundle_dir=generated.bundle_dir,
        output_dir=generated.output_dir,
        pack_dir=generated.pack_dir,
        manifest_path=generated.manifest_path,
        labels_path=generated.labels_path,
        aliases_path=generated.aliases_path,
        rules_path=generated.rules_path,
        sources_path=generated.sources_path,
        build_path=generated.build_path,
        generated_at=generated.generated_at,
        source_count=generated.source_count,
        label_count=generated.label_count,
        alias_count=generated.alias_count,
        unique_canonical_count=unique_canonical_count,
        rule_count=generated.rule_count,
        included_entity_count=generated.included_entity_count,
        included_rule_count=generated.included_rule_count,
        dropped_record_count=generated.dropped_record_count,
        dropped_alias_count=generated.dropped_alias_count,
        ambiguous_alias_count=generated.ambiguous_alias_count,
        dropped_alias_ratio=dropped_alias_ratio,
        label_distribution=label_distribution,
        warnings=warnings,
    )


def _count_unique_canonicals(
    entities_path: Path,
    *,
    include_build_only: bool,
) -> int:
    canonicals: set[str] = set()
    for record in _read_jsonl_records(entities_path):
        if _coerce_bool(record.get("build_only")) and not include_build_only:
            continue
        canonical = _clean_text(record.get("canonical_text"))
        if canonical:
            canonicals.add(canonical.casefold())
    return len(canonicals)


def _load_label_distribution(
    aliases_path: Path,
    rules_path: Path,
) -> dict[str, int]:
    distribution: dict[str, int] = {}

    aliases_payload = json.loads(aliases_path.read_text(encoding="utf-8"))
    for item in aliases_payload.get("aliases", []):
        label = _clean_text(item.get("label"))
        if label:
            distribution[label] = distribution.get(label, 0) + 1

    rules_payload = json.loads(rules_path.read_text(encoding="utf-8"))
    for item in rules_payload.get("patterns", []):
        label = _clean_text(item.get("label"))
        if label:
            distribution[label] = distribution.get(label, 0) + 1

    return {
        label: distribution[label]
        for label in sorted(distribution, key=lambda value: (value.casefold(), value))
    }


def _read_jsonl_records(path: Path) -> list[dict[str, Any]]:
    resolved = path.expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Normalized input file not found: {resolved}")
    if not resolved.is_file():
        raise FileExistsError(f"Normalized input path is not a file: {resolved}")

    records: list[dict[str, Any]] = []
    for index, line in enumerate(resolved.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSONL record in {resolved} line {index}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"JSONL record in {resolved} line {index} must be an object.")
        records.append(payload)
    return records


def _coerce_bool(raw_value: Any) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    if raw_value is None:
        return False
    if isinstance(raw_value, str):
        return raw_value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(raw_value)


def _clean_text(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    return str(raw_value).strip()
