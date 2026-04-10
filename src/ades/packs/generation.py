"""Offline helpers for turning normalized source bundles into pack directories."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import shutil
from typing import Any
import unicodedata

from .manifest import PackArtifact, PackManifest


_WHITESPACE_RE = re.compile(r"\s+")
_ORG_SUFFIX_RE = re.compile(
    r"(?:,\s*)?(?:inc\.?|corp\.?|corporation|co\.?|company|llc|ltd\.?|limited|plc|group|holdings?|sa|ag|nv)$",
    flags=re.IGNORECASE,
)
_ORG_LIKE_LABELS = {
    "bank",
    "company",
    "corporation",
    "exchange",
    "fund",
    "issuer",
    "organization",
}
_QUOTE_TRANSLATION = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
    }
)
_DASH_TRANSLATION = str.maketrans(
    {
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
    }
)
_ALLOWED_SOURCE_LICENSE_CLASSES = {
    "ship-now",
    "build-only",
    "blocked-pending-license-review",
}


@dataclass(frozen=True)
class SourceSnapshot:
    """One recorded upstream snapshot inside a normalized source bundle."""

    name: str
    snapshot_uri: str | None = None
    version: str | None = None
    license_class: str | None = None
    license: str | None = None
    retrieved_at: str | None = None
    record_count: int | None = None
    notes: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceSnapshot":
        if not data.get("name"):
            raise ValueError("Bundle source entries must include name.")
        license_class = (
            str(data["license_class"]).strip()
            if data.get("license_class") is not None
            else None
        )
        license_name = (
            str(data["license"]).strip() if data.get("license") is not None else None
        )
        if license_class is None and license_name in _ALLOWED_SOURCE_LICENSE_CLASSES:
            license_class = license_name
        if license_class is None:
            raise ValueError(
                f"Bundle source entry '{data['name']}' must include license_class."
            )
        if license_class not in _ALLOWED_SOURCE_LICENSE_CLASSES:
            allowed = ", ".join(sorted(_ALLOWED_SOURCE_LICENSE_CLASSES))
            raise ValueError(
                f"Bundle source entry '{data['name']}' uses unsupported license_class "
                f"'{license_class}'. Expected one of: {allowed}"
            )
        record_count = data.get("record_count")
        return cls(
            name=str(data["name"]),
            snapshot_uri=str(data["snapshot_uri"]) if data.get("snapshot_uri") is not None else None,
            version=str(data["version"]) if data.get("version") is not None else None,
            license_class=license_class,
            license=license_name,
            retrieved_at=str(data["retrieved_at"]) if data.get("retrieved_at") is not None else None,
            record_count=int(record_count) if record_count is not None else None,
            notes=str(data["notes"]) if data.get("notes") is not None else None,
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": self.name}
        if self.snapshot_uri is not None:
            payload["snapshot_uri"] = self.snapshot_uri
        if self.version is not None:
            payload["version"] = self.version
        if self.license_class is not None:
            payload["license_class"] = self.license_class
        if self.license is not None:
            payload["license"] = self.license
        if self.retrieved_at is not None:
            payload["retrieved_at"] = self.retrieved_at
        if self.record_count is not None:
            payload["record_count"] = self.record_count
        if self.notes is not None:
            payload["notes"] = self.notes
        return payload


@dataclass(frozen=True)
class SourceBundleManifest:
    """Top-level manifest that describes one normalized bundle."""

    schema_version: int
    pack_id: str
    version: str | None
    language: str
    domain: str
    tier: str
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    min_ades_version: str = "0.1.0"
    entities_path: str = "normalized/entities.jsonl"
    rules_path: str | None = "normalized/rules.jsonl"
    label_mappings: dict[str, str] = field(default_factory=dict)
    stoplisted_aliases: list[str] = field(default_factory=list)
    allowed_ambiguous_aliases: list[str] = field(default_factory=list)
    sources: list[SourceSnapshot] = field(default_factory=list)

    @classmethod
    def from_path(cls, path: Path) -> "SourceBundleManifest":
        return cls.from_dict(json.loads(path.read_text(encoding="utf-8")))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceBundleManifest":
        for field_name in ("pack_id", "language", "domain", "tier"):
            if not data.get(field_name):
                raise ValueError(f"Bundle manifest is missing required field: {field_name}")
        sources = [SourceSnapshot.from_dict(item) for item in data.get("sources", [])]
        return cls(
            schema_version=int(data.get("schema_version", 1)),
            pack_id=str(data["pack_id"]),
            version=str(data["version"]) if data.get("version") is not None else None,
            language=str(data["language"]),
            domain=str(data["domain"]),
            tier=str(data["tier"]),
            description=str(data["description"]) if data.get("description") is not None else None,
            tags=[str(item) for item in data.get("tags", [])],
            dependencies=[str(item) for item in data.get("dependencies", [])],
            min_ades_version=str(data.get("min_ades_version", "0.1.0")),
            entities_path=str(data.get("entities_path", "normalized/entities.jsonl")),
            rules_path=(
                str(data["rules_path"])
                if "rules_path" in data and data.get("rules_path") is not None
                else ("normalized/rules.jsonl" if "rules_path" not in data else None)
            ),
            label_mappings={
                str(key): str(value)
                for key, value in data.get("label_mappings", {}).items()
            },
            stoplisted_aliases=[str(item) for item in data.get("stoplisted_aliases", [])],
            allowed_ambiguous_aliases=[
                str(item) for item in data.get("allowed_ambiguous_aliases", [])
            ],
            sources=sources,
        )


@dataclass(frozen=True)
class SourceGovernanceSummary:
    """Publication-oriented governance state for bundle sources."""

    source_license_classes: dict[str, int] = field(default_factory=dict)
    publishable_source_count: int = 0
    restricted_source_count: int = 0
    publishable_sources_only: bool = False
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class GeneratedPackSourceResult:
    """Summary of one generated pack directory."""

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
    label_count: int
    alias_count: int
    rule_count: int
    source_count: int
    publishable_source_count: int
    restricted_source_count: int
    publishable_sources_only: bool
    included_entity_count: int
    included_rule_count: int
    dropped_record_count: int
    dropped_alias_count: int
    ambiguous_alias_count: int
    source_license_classes: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def generate_pack_source(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    version: str | None = None,
    include_build_metadata: bool = True,
    include_build_only: bool = False,
) -> GeneratedPackSourceResult:
    """Generate a runtime-compatible pack directory from a normalized source bundle."""

    resolved_bundle_dir = Path(bundle_dir).expanduser().resolve()
    if not resolved_bundle_dir.exists():
        raise FileNotFoundError(f"Bundle directory not found: {resolved_bundle_dir}")
    if not resolved_bundle_dir.is_dir():
        raise NotADirectoryError(f"Bundle directory is not a directory: {resolved_bundle_dir}")

    bundle_manifest_path = resolved_bundle_dir / "bundle.json"
    if not bundle_manifest_path.exists():
        raise FileNotFoundError(f"Bundle manifest not found: {bundle_manifest_path}")

    bundle = SourceBundleManifest.from_path(bundle_manifest_path)
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    pack_version = version or bundle.version or "0.1.0"
    generated_at = _utc_timestamp()
    pack_dir = resolved_output_dir / bundle.pack_id
    if pack_dir.exists():
        shutil.rmtree(pack_dir)
    pack_dir.mkdir(parents=True, exist_ok=True)

    entity_records = _load_jsonl_records(
        resolved_bundle_dir / bundle.entities_path,
        required=True,
    )
    rule_records = _load_jsonl_records(
        resolved_bundle_dir / bundle.rules_path,
        required=False,
    )
    source_governance = summarize_source_governance(bundle.sources)

    label_mappings = {
        key.casefold(): value
        for key, value in bundle.label_mappings.items()
    }
    stoplisted_aliases = {
        _normalized_key(item)
        for item in bundle.stoplisted_aliases
        if _normalized_key(item)
    }
    allowed_ambiguous_aliases = {
        _normalized_key(item)
        for item in bundle.allowed_ambiguous_aliases
        if _normalized_key(item)
    }

    labels: set[str] = set()
    alias_labels: dict[str, set[str]] = {}
    alias_entries: dict[tuple[str, str], str] = {}
    included_entity_count = 0
    included_rule_count = 0
    dropped_record_count = 0
    dropped_alias_count = 0

    for index, record in enumerate(entity_records, start=1):
        if _coerce_bool(record.get("build_only")) and not include_build_only:
            dropped_record_count += 1
            continue
        label = _resolve_record_label(
            record,
            label_mappings=label_mappings,
            kind="entity",
            record_index=index,
        )
        labels.add(label)
        included_entity_count += 1

        canonical_text = _require_clean_text(
            record.get("canonical_text"),
            kind="entity canonical_text",
            record_index=index,
        )
        for candidate in _iter_entity_alias_candidates(canonical_text, record, label):
            normalized_key = _normalized_key(candidate)
            if not normalized_key:
                continue
            if _should_drop_alias(normalized_key, candidate, stoplisted_aliases):
                dropped_alias_count += 1
                continue
            alias_labels.setdefault(normalized_key, set()).add(label)
            alias_entries.setdefault((normalized_key, label), candidate)

    patterns: list[dict[str, str]] = []
    for index, record in enumerate(rule_records, start=1):
        if _coerce_bool(record.get("build_only")) and not include_build_only:
            dropped_record_count += 1
            continue
        name = _require_clean_text(
            record.get("name"),
            kind="rule name",
            record_index=index,
        )
        pattern = _require_clean_text(
            record.get("pattern"),
            kind="rule pattern",
            record_index=index,
        )
        kind = str(record.get("kind", "regex")).strip() or "regex"
        if kind != "regex":
            raise ValueError(
                f"Rule record {index} in {bundle.rules_path} uses unsupported kind: {kind}"
            )
        try:
            re.compile(pattern, flags=re.IGNORECASE)
        except re.error as exc:
            raise ValueError(
                f"Rule record {index} in {bundle.rules_path} has invalid regex pattern: {exc}"
            ) from exc
        label = _resolve_record_label(
            record,
            label_mappings=label_mappings,
            kind="rule",
            record_index=index,
        )
        labels.add(label)
        included_rule_count += 1
        patterns.append(
            {
                "name": name,
                "kind": "regex",
                "pattern": pattern,
                "label": label,
            }
        )

    ambiguous_alias_count = 0
    aliases: list[dict[str, str]] = []
    for normalized_key in sorted(alias_labels, key=_stable_sort_key):
        labels_for_alias = alias_labels[normalized_key]
        if len(labels_for_alias) > 1 and normalized_key not in allowed_ambiguous_aliases:
            ambiguous_alias_count += 1
            dropped_alias_count += len(labels_for_alias)
            continue
        for label in sorted(labels_for_alias, key=_stable_sort_key):
            aliases.append(
                {
                    "text": alias_entries[(normalized_key, label)],
                    "label": label,
                }
            )

    labels_payload = sorted(labels, key=_stable_sort_key)
    patterns_payload = sorted(
        patterns,
        key=lambda item: (
            _stable_sort_key(item["name"]),
            _stable_sort_key(item["label"]),
            item["pattern"],
        ),
    )
    aliases_payload = sorted(
        aliases,
        key=lambda item: (
            _stable_sort_key(item["text"]),
            _stable_sort_key(item["label"]),
        ),
    )

    if not labels_payload:
        raise ValueError("Bundle did not yield any runtime labels.")

    labels_path = pack_dir / "labels.json"
    aliases_path = pack_dir / "aliases.json"
    rules_path = pack_dir / "rules.json"
    manifest_path = pack_dir / "manifest.json"
    sources_path = pack_dir / "sources.json" if include_build_metadata else None
    build_path = pack_dir / "build.json" if include_build_metadata else None

    labels_path.write_text(
        json.dumps(labels_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    aliases_path.write_text(
        json.dumps({"aliases": aliases_payload}, indent=2) + "\n",
        encoding="utf-8",
    )
    rules_path.write_text(
        json.dumps({"patterns": patterns_payload}, indent=2) + "\n",
        encoding="utf-8",
    )

    manifest = PackManifest(
        schema_version=1,
        pack_id=bundle.pack_id,
        version=pack_version,
        language=bundle.language,
        domain=bundle.domain,
        tier=bundle.tier,
        description=(
            bundle.description
            or f"{bundle.pack_id} generated pack for {bundle.domain} tagging."
        ),
        tags=list(bundle.tags),
        dependencies=list(bundle.dependencies),
        artifacts=[
            PackArtifact(
                name="pack",
                url=f"../../artifacts/{bundle.pack_id}-{pack_version}.tar.zst",
                sha256="source-placeholder",
            )
        ],
        models=[],
        rules=["rules.json"],
        labels=["labels.json"],
        min_ades_version=bundle.min_ades_version,
    )
    manifest_path.write_text(
        json.dumps(manifest.to_dict(), indent=2) + "\n",
        encoding="utf-8",
    )

    if sources_path is not None:
        sources_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "pack_id": bundle.pack_id,
                    "version": pack_version,
                    "bundle_manifest_path": str(bundle_manifest_path),
                    "entities_path": str((resolved_bundle_dir / bundle.entities_path).resolve()),
                    "rules_path": (
                        str((resolved_bundle_dir / bundle.rules_path).resolve())
                        if bundle.rules_path is not None
                        and (resolved_bundle_dir / bundle.rules_path).exists()
                        else None
                    ),
                    "sources": [source.to_dict() for source in bundle.sources],
                    "source_license_classes": source_governance.source_license_classes,
                    "publishable_source_count": source_governance.publishable_source_count,
                    "restricted_source_count": source_governance.restricted_source_count,
                    "publishable_sources_only": source_governance.publishable_sources_only,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    if build_path is not None:
        build_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "generated_at": generated_at,
                    "bundle_dir": str(resolved_bundle_dir),
                    "bundle_manifest_path": str(bundle_manifest_path),
                    "include_build_only": include_build_only,
                    "input_entity_count": len(entity_records),
                    "input_rule_count": len(rule_records),
                    "included_entity_count": included_entity_count,
                    "included_rule_count": included_rule_count,
                    "dropped_record_count": dropped_record_count,
                    "dropped_alias_count": dropped_alias_count,
                    "ambiguous_alias_count": ambiguous_alias_count,
                    "label_count": len(labels_payload),
                    "alias_count": len(aliases_payload),
                    "rule_count": len(patterns_payload),
                    "source_count": len(bundle.sources),
                    "source_license_classes": source_governance.source_license_classes,
                    "publishable_source_count": source_governance.publishable_source_count,
                    "restricted_source_count": source_governance.restricted_source_count,
                    "publishable_sources_only": source_governance.publishable_sources_only,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    warnings = list(source_governance.warnings)
    if not aliases_payload:
        warnings.append("Generated pack does not include any aliases.")
    if not patterns_payload:
        warnings.append("Generated pack does not include any rules.")

    return GeneratedPackSourceResult(
        pack_id=bundle.pack_id,
        version=pack_version,
        bundle_dir=str(resolved_bundle_dir),
        output_dir=str(resolved_output_dir),
        pack_dir=str(pack_dir),
        manifest_path=str(manifest_path),
        labels_path=str(labels_path),
        aliases_path=str(aliases_path),
        rules_path=str(rules_path),
        sources_path=str(sources_path) if sources_path is not None else None,
        build_path=str(build_path) if build_path is not None else None,
        generated_at=generated_at,
        label_count=len(labels_payload),
        alias_count=len(aliases_payload),
        rule_count=len(patterns_payload),
        source_count=len(bundle.sources),
        publishable_source_count=source_governance.publishable_source_count,
        restricted_source_count=source_governance.restricted_source_count,
        publishable_sources_only=source_governance.publishable_sources_only,
        included_entity_count=included_entity_count,
        included_rule_count=included_rule_count,
        dropped_record_count=dropped_record_count,
        dropped_alias_count=dropped_alias_count,
        ambiguous_alias_count=ambiguous_alias_count,
        source_license_classes=source_governance.source_license_classes,
        warnings=warnings,
    )


def _load_jsonl_records(path: Path | None, *, required: bool) -> list[dict[str, Any]]:
    if path is None:
        return []
    resolved = path.expanduser().resolve()
    if not resolved.exists():
        if required:
            raise FileNotFoundError(f"Normalized input file not found: {resolved}")
        return []
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


def _resolve_record_label(
    record: dict[str, Any],
    *,
    label_mappings: dict[str, str],
    kind: str,
    record_index: int,
) -> str:
    for key in ("label", "entity_type", "rule_type", "type"):
        raw_value = record.get(key)
        if raw_value is None:
            continue
        value = _clean_text(raw_value)
        if value:
            return label_mappings.get(value.casefold(), value)
    raise ValueError(f"{kind.title()} record {record_index} is missing label or entity_type.")


def summarize_source_governance(
    sources: list[SourceSnapshot],
) -> SourceGovernanceSummary:
    """Summarize whether bundle sources are publishable through the hosted registry."""

    if not sources:
        return SourceGovernanceSummary(
            publishable_sources_only=False,
            warnings=[
                "Bundle does not declare any source snapshots and blocks registry publication."
            ],
        )

    license_class_counts: dict[str, int] = {}
    publishable_source_count = 0
    restricted_source_count = 0
    warnings: list[str] = []
    for source in sorted(sources, key=lambda item: _stable_sort_key(item.name)):
        license_class = source.license_class or "blocked-pending-license-review"
        license_class_counts[license_class] = license_class_counts.get(license_class, 0) + 1
        if license_class == "ship-now":
            publishable_source_count += 1
            continue
        restricted_source_count += 1
        warnings.append(
            f"Source '{source.name}' uses license_class '{license_class}' and blocks registry publication."
        )
    ordered_counts = {
        key: license_class_counts[key]
        for key in sorted(license_class_counts, key=_stable_sort_key)
    }
    return SourceGovernanceSummary(
        source_license_classes=ordered_counts,
        publishable_source_count=publishable_source_count,
        restricted_source_count=restricted_source_count,
        publishable_sources_only=restricted_source_count == 0,
        warnings=warnings,
    )


def _iter_entity_alias_candidates(
    canonical_text: str,
    record: dict[str, Any],
    label: str,
) -> list[str]:
    seen: set[str] = set()
    candidates: list[str] = []
    raw_aliases = record.get("aliases", [])
    if raw_aliases is None:
        raw_aliases = []
    if not isinstance(raw_aliases, list):
        raise ValueError("Normalized entity aliases must be a list.")
    for raw_value in [canonical_text, *raw_aliases]:
        cleaned = _clean_text(raw_value)
        if not cleaned:
            continue
        for variant in _expand_alias_variants(cleaned, label):
            if variant not in seen:
                seen.add(variant)
                candidates.append(variant)
    return candidates


def _expand_alias_variants(text: str, label: str) -> list[str]:
    variants = [text]
    if label.casefold() in _ORG_LIKE_LABELS:
        stripped = _clean_text(_ORG_SUFFIX_RE.sub("", text))
        if stripped and stripped != text:
            variants.append(stripped)
    return variants


def _should_drop_alias(
    normalized_key: str,
    display_value: str,
    stoplisted_aliases: set[str],
) -> bool:
    if normalized_key in stoplisted_aliases:
        return True
    if len(display_value) < 2:
        return True
    if not any(character.isalnum() for character in display_value):
        return True
    return False


def _require_clean_text(
    raw_value: Any,
    *,
    kind: str,
    record_index: int,
) -> str:
    cleaned = _clean_text(raw_value)
    if not cleaned:
        raise ValueError(f"{kind.title()} is missing in record {record_index}.")
    return cleaned


def _clean_text(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    value = unicodedata.normalize("NFKC", str(raw_value))
    value = value.translate(_QUOTE_TRANSLATION)
    value = value.translate(_DASH_TRANSLATION)
    value = _WHITESPACE_RE.sub(" ", value).strip()
    return value


def _coerce_bool(raw_value: Any) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    if raw_value is None:
        return False
    if isinstance(raw_value, str):
        return raw_value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(raw_value)


def _normalized_key(value: str) -> str:
    return _clean_text(value).casefold()


def _stable_sort_key(value: str) -> tuple[str, str]:
    return (value.casefold(), value)


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
