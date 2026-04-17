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

from ..runtime_matcher import build_matcher_artifact_from_aliases_json
from ..text_processing import canonicalize_text, normalize_lookup_text
from .alias_analysis import (
    AliasAnalysisResult,
    DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME,
    apply_general_retained_alias_exclusions,
    audit_general_retained_aliases,
    analyze_alias_candidates,
    analyze_alias_candidates_from_db,
    build_alias_candidate,
    iter_retained_aliases_from_db,
    write_alias_analysis_report,
)
from .manifest import PackArtifact, PackManifest
from .manifest import PackMatcher
from .rule_validation import validate_rule_pattern


_WHITESPACE_RE = re.compile(r"\s+")
_ORG_LIKE_LABELS = {
    "bank",
    "company",
    "corporation",
    "exchange",
    "fund",
    "issuer",
    "organization",
}
_ORG_SUFFIX_TOKENS = {
    "ag",
    "adviser",
    "advisers",
    "advisor",
    "advisors",
    "associate",
    "associates",
    "capital",
    "co",
    "company",
    "corp",
    "corporation",
    "group",
    "holding",
    "holdings",
    "inc",
    "limited",
    "llc",
    "ltd",
    "management",
    "nv",
    "partner",
    "partners",
    "plc",
    "sa",
    "venture",
    "ventures",
}
_ORG_FRAGMENT_SUFFIX_TOKENS = _ORG_SUFFIX_TOKENS | {
    "analytics",
    "consultancy",
    "consulting",
    "information",
    "intelligence",
    "media",
    "research",
    "solutions",
    "systems",
    "technologies",
    "technology",
}
_ACRONYM_CONNECTOR_TOKENS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "the",
    "to",
    "with",
}
_GEOPOLITICAL_ACRONYM_TOKENS = {
    "arab",
    "democratic",
    "emirates",
    "european",
    "federal",
    "kingdom",
    "republic",
    "state",
    "states",
    "union",
    "united",
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
_ALLOWED_ALIAS_RESOLUTION_MODES = {"analyze", "pre_resolved_explicit"}


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
    min_entities_per_100_tokens_warning: float | None = None
    entities_path: str = "normalized/entities.jsonl"
    rules_path: str | None = "normalized/rules.jsonl"
    label_mappings: dict[str, str] = field(default_factory=dict)
    stoplisted_aliases: list[str] = field(default_factory=list)
    allowed_ambiguous_aliases: list[str] = field(default_factory=list)
    alias_resolution: str = "analyze"
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
        alias_resolution = str(data.get("alias_resolution", "analyze")).strip() or "analyze"
        if alias_resolution not in _ALLOWED_ALIAS_RESOLUTION_MODES:
            allowed = ", ".join(sorted(_ALLOWED_ALIAS_RESOLUTION_MODES))
            raise ValueError(
                f"Bundle manifest uses unsupported alias_resolution '{alias_resolution}'. "
                f"Expected one of: {allowed}"
            )
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
            min_entities_per_100_tokens_warning=(
                float(data["min_entities_per_100_tokens_warning"])
                if data.get("min_entities_per_100_tokens_warning") is not None
                else None
            ),
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
            alias_resolution=alias_resolution,
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
    matcher_artifact_path: str
    matcher_entries_path: str
    matcher_algorithm: str
    matcher_entry_count: int
    matcher_state_count: int
    matcher_max_alias_length: int
    matcher_artifact_sha256: str
    matcher_entries_sha256: str
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
    entity_label_distribution: dict[str, int] = field(default_factory=dict)
    label_distribution: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def _should_audit_general_retained_aliases(*, domain: str, language: str) -> bool:
    return domain.casefold() == "general" and language.casefold() == "en"


def _default_general_retained_alias_review_cache_path(root: Path) -> Path:
    return root / "general-retained-alias-review.sqlite"


def _default_general_retained_alias_exclusions_path(root: Path) -> Path:
    return root / DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME


def generate_pack_source(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    version: str | None = None,
    include_build_metadata: bool = True,
    include_build_only: bool = False,
    general_retained_alias_review_mode: str | None = None,
    general_retained_alias_reviewer: Any | None = None,
    general_retained_alias_review_cache_path: str | Path | None = None,
    general_retained_alias_exclusions_path: str | Path | None = None,
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
    labels_path = pack_dir / "labels.json"
    aliases_path = pack_dir / "aliases.json"
    rules_path = pack_dir / "rules.json"
    manifest_path = pack_dir / "manifest.json"
    matcher_dir = pack_dir / "matcher"
    sources_path = pack_dir / "sources.json" if include_build_metadata else None
    build_path = pack_dir / "build.json" if include_build_metadata else None
    analysis_db_path = (
        pack_dir / "analysis.sqlite"
        if include_build_metadata and bundle.alias_resolution == "analyze"
        else None
    )
    alias_analysis_path = pack_dir / "alias-analysis.json" if include_build_metadata else None

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
    entity_label_distribution: dict[str, int] = {}
    included_entity_count = 0
    included_rule_count = 0
    dropped_record_count = 0
    dropped_alias_count = 0
    input_entity_count = 0

    def iter_alias_candidates() -> Any:
        nonlocal dropped_alias_count
        nonlocal dropped_record_count
        nonlocal included_entity_count
        nonlocal input_entity_count
        for index, record in enumerate(
            _iter_jsonl_records(
                resolved_bundle_dir / bundle.entities_path,
                required=True,
            ),
            start=1,
        ):
            input_entity_count += 1
            if _coerce_bool(record.get("build_only")) and not include_build_only:
                dropped_record_count += 1
                continue
            raw_blocked_aliases = record.get("blocked_aliases", [])
            if raw_blocked_aliases is None:
                raw_blocked_aliases = []
            if not isinstance(raw_blocked_aliases, list):
                raise ValueError("Normalized entity blocked_aliases must be a list.")
            blocked_aliases = {
                _normalized_key(item)
                for item in raw_blocked_aliases
                if _normalized_key(item)
            }
            label = _resolve_record_label(
                record,
                label_mappings=label_mappings,
                kind="entity",
                record_index=index,
            )
            labels.add(label)
            included_entity_count += 1
            entity_label_distribution[label] = entity_label_distribution.get(label, 0) + 1

            canonical_text = _require_clean_text(
                record.get("canonical_text"),
                kind="entity canonical_text",
                record_index=index,
            )
            for candidate, generated in _iter_entity_alias_candidates(
                canonical_text,
                record,
                label,
                pack_domain=bundle.domain,
            ):
                normalized_key = _normalized_key(candidate)
                if not normalized_key:
                    continue
                if normalized_key in blocked_aliases:
                    dropped_alias_count += 1
                    continue
                if _should_drop_alias(
                    normalized_key,
                    candidate,
                    stoplisted_aliases,
                    label=label,
                    canonical_text=canonical_text,
                    generated=generated,
                    pack_domain=bundle.domain,
                ):
                    dropped_alias_count += 1
                    continue
                yield build_alias_candidate(
                    alias_key=normalized_key,
                    display_text=candidate,
                    label=label,
                    canonical_text=canonical_text,
                    record=record,
                    generated=generated,
                )

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
            validate_rule_pattern(pattern)
        except ValueError as exc:
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

    if bundle.alias_resolution == "pre_resolved_explicit":
        (
            alias_analysis,
            pre_resolved_labels,
            pre_resolved_entity_label_distribution,
            included_entity_count,
            input_entity_count,
            dropped_record_count,
            dropped_alias_count,
        ) = _write_pre_resolved_aliases_json(
            aliases_path,
            bundle_dir=resolved_bundle_dir,
            bundle=bundle,
            label_mappings=label_mappings,
            stoplisted_aliases=stoplisted_aliases,
            include_build_only=include_build_only,
        )
        labels.update(pre_resolved_labels)
        entity_label_distribution = pre_resolved_entity_label_distribution
    else:
        alias_analysis = analyze_alias_candidates(
            iter_alias_candidates(),
            allowed_ambiguous_aliases=allowed_ambiguous_aliases,
            analysis_db_path=analysis_db_path,
            materialize_retained_aliases=analysis_db_path is None,
        )
        if _should_audit_general_retained_aliases(
            domain=bundle.domain,
            language=bundle.language,
        ):
            alias_analysis = audit_general_retained_aliases(
                alias_analysis,
                review_mode=general_retained_alias_review_mode,
                reviewer=general_retained_alias_reviewer,
                review_cache_path=(
                    general_retained_alias_review_cache_path
                    if general_retained_alias_review_cache_path is not None
                    else _default_general_retained_alias_review_cache_path(
                        resolved_bundle_dir
                    )
                ),
            )
            resolved_exclusions_path = (
                Path(general_retained_alias_exclusions_path).expanduser().resolve()
                if general_retained_alias_exclusions_path is not None
                else _default_general_retained_alias_exclusions_path(resolved_bundle_dir)
            )
            if resolved_exclusions_path.exists():
                alias_analysis = apply_general_retained_alias_exclusions(
                    alias_analysis,
                    exclusions_path=resolved_exclusions_path,
                )
    if alias_analysis_path is not None:
        write_alias_analysis_report(alias_analysis_path, alias_analysis)
    ambiguous_alias_count = alias_analysis.ambiguous_alias_count
    dropped_alias_count += (
        alias_analysis.blocked_alias_count
        + alias_analysis.retained_alias_audit_removed_alias_count
        + alias_analysis.retained_alias_exclusion_removed_alias_count
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
    label_distribution = _build_label_distribution(
        alias_analysis.retained_label_counts,
        patterns_payload,
    )
    entity_label_distribution = {
        label: entity_label_distribution[label]
        for label in sorted(entity_label_distribution, key=_stable_sort_key)
    }

    if not labels_payload:
        raise ValueError("Bundle did not yield any runtime labels.")

    labels_path.write_text(
        json.dumps(labels_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    if bundle.alias_resolution == "pre_resolved_explicit":
        aliases_payload = None
    elif alias_analysis.retained_aliases_materialized:
        aliases_payload = sorted(
            alias_analysis.retained_aliases,
            key=lambda item: (
                _stable_sort_key(item["text"]),
                _stable_sort_key(item["label"]),
            ),
        )
        _write_aliases_json(aliases_path, aliases_payload)
    else:
        if analysis_db_path is None:
            raise ValueError(
                "Alias analysis database is required when retained aliases are not materialized."
            )
        aliases_payload = None
        _write_aliases_json_from_analysis_db(
            aliases_path,
            analysis_db_path,
            source_domain=bundle.domain,
        )
    rules_path.write_text(
        json.dumps({"patterns": patterns_payload}, indent=2) + "\n",
        encoding="utf-8",
    )
    matcher_artifact = build_matcher_artifact_from_aliases_json(
        aliases_path,
        output_dir=matcher_dir,
        include_runtime_tiers={
            "runtime_exact_acronym_high_precision",
            "runtime_exact_geopolitical_high_precision",
            "runtime_exact_high_precision",
        },
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
        matcher=PackMatcher(
            schema_version=1,
            algorithm=matcher_artifact.algorithm,
            normalization=matcher_artifact.normalization,
            artifact_path=(
                Path(matcher_dir.name) / Path(matcher_artifact.artifact_path).name
            ).as_posix(),
            entries_path=(
                Path(matcher_dir.name) / Path(matcher_artifact.entries_path).name
            ).as_posix(),
            entry_count=matcher_artifact.entry_count,
            state_count=matcher_artifact.state_count,
            max_alias_length=matcher_artifact.max_alias_length,
            artifact_sha256=matcher_artifact.artifact_sha256,
            entries_sha256=matcher_artifact.entries_sha256,
        ),
        min_ades_version=bundle.min_ades_version,
        min_entities_per_100_tokens_warning=bundle.min_entities_per_100_tokens_warning,
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
                    "input_entity_count": input_entity_count,
                    "input_rule_count": len(rule_records),
                    "included_entity_count": included_entity_count,
                    "included_rule_count": included_rule_count,
                    "dropped_record_count": dropped_record_count,
                    "dropped_alias_count": dropped_alias_count,
                    "ambiguous_alias_count": ambiguous_alias_count,
                    "analysis_backend": alias_analysis.backend,
                    "analysis_db_path": (
                        str(analysis_db_path) if analysis_db_path is not None else None
                    ),
                    "alias_analysis_report_path": (
                        str(alias_analysis_path) if alias_analysis_path is not None else None
                    ),
                    "candidate_alias_count": alias_analysis.candidate_alias_count,
                    "retained_alias_count": alias_analysis.retained_alias_count,
                    "collision_blocked_alias_count": alias_analysis.blocked_alias_count,
                    "retained_alias_audit_scanned_alias_count": alias_analysis.retained_alias_audit_scanned_alias_count,
                    "retained_alias_audit_removed_alias_count": alias_analysis.retained_alias_audit_removed_alias_count,
                    "retained_alias_audit_chunk_size": alias_analysis.retained_alias_audit_chunk_size,
                    "retained_alias_audit_reason_counts": alias_analysis.retained_alias_audit_reason_counts,
                    "retained_alias_audit_mode": alias_analysis.retained_alias_audit_mode,
                "retained_alias_audit_reviewer_name": alias_analysis.retained_alias_audit_reviewer_name,
                "retained_alias_audit_review_cache_path": alias_analysis.retained_alias_audit_review_cache_path,
                "retained_alias_audit_invoked_review_count": alias_analysis.retained_alias_audit_invoked_review_count,
                "retained_alias_audit_cached_review_count": alias_analysis.retained_alias_audit_cached_review_count,
                "retained_alias_exclusion_removed_alias_count": alias_analysis.retained_alias_exclusion_removed_alias_count,
                "retained_alias_exclusion_source_path": alias_analysis.retained_alias_exclusion_source_path,
                    "exact_identifier_alias_count": alias_analysis.exact_identifier_alias_count,
                    "natural_language_alias_count": alias_analysis.natural_language_alias_count,
                    "blocked_reason_counts": alias_analysis.blocked_reason_counts,
                    "retained_label_counts": alias_analysis.retained_label_counts,
                    "label_count": len(labels_payload),
                    "alias_count": alias_analysis.retained_alias_count,
                    "rule_count": len(patterns_payload),
                    "matcher": {
                        "algorithm": matcher_artifact.algorithm,
                        "normalization": matcher_artifact.normalization,
                        "artifact_path": str(Path(matcher_artifact.artifact_path)),
                        "entries_path": str(Path(matcher_artifact.entries_path)),
                        "entry_count": matcher_artifact.entry_count,
                        "state_count": matcher_artifact.state_count,
                        "max_alias_length": matcher_artifact.max_alias_length,
                        "artifact_sha256": matcher_artifact.artifact_sha256,
                        "entries_sha256": matcher_artifact.entries_sha256,
                    },
                    "source_count": len(bundle.sources),
                    "source_license_classes": source_governance.source_license_classes,
                    "publishable_source_count": source_governance.publishable_source_count,
                    "restricted_source_count": source_governance.restricted_source_count,
                    "publishable_sources_only": source_governance.publishable_sources_only,
                    "entity_label_distribution": entity_label_distribution,
                    "label_distribution": label_distribution,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    warnings = list(source_governance.warnings)
    if alias_analysis.retained_alias_count == 0:
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
        matcher_artifact_path=matcher_artifact.artifact_path,
        matcher_entries_path=matcher_artifact.entries_path,
        matcher_algorithm=matcher_artifact.algorithm,
        matcher_entry_count=matcher_artifact.entry_count,
        matcher_state_count=matcher_artifact.state_count,
        matcher_max_alias_length=matcher_artifact.max_alias_length,
        matcher_artifact_sha256=matcher_artifact.artifact_sha256,
        matcher_entries_sha256=matcher_artifact.entries_sha256,
        sources_path=str(sources_path) if sources_path is not None else None,
        build_path=str(build_path) if build_path is not None else None,
        generated_at=generated_at,
        label_count=len(labels_payload),
        alias_count=alias_analysis.retained_alias_count,
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
        entity_label_distribution=entity_label_distribution,
        label_distribution=label_distribution,
        warnings=warnings,
    )


def refresh_pack_from_analysis_db(
    source_pack_dir: str | Path,
    *,
    output_dir: str | Path,
    version: str | None = None,
    include_build_metadata: bool = True,
    general_retained_alias_review_mode: str | None = None,
    general_retained_alias_reviewer: Any | None = None,
    general_retained_alias_review_cache_path: str | Path | None = None,
    general_retained_alias_exclusions_path: str | Path | None = None,
) -> GeneratedPackSourceResult:
    """Refresh a generated pack by re-resolving aliases from an existing analysis DB."""

    resolved_source_pack_dir = Path(source_pack_dir).expanduser().resolve()
    if not resolved_source_pack_dir.exists():
        raise FileNotFoundError(f"Pack directory not found: {resolved_source_pack_dir}")
    if not resolved_source_pack_dir.is_dir():
        raise NotADirectoryError(
            f"Pack directory is not a directory: {resolved_source_pack_dir}"
        )

    manifest_path = resolved_source_pack_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Pack manifest not found: {manifest_path}")
    source_analysis_db_path = resolved_source_pack_dir / "analysis.sqlite"
    if not source_analysis_db_path.exists():
        raise FileNotFoundError(
            f"Pack analysis database not found: {source_analysis_db_path}"
        )

    source_manifest = PackManifest.load(manifest_path)
    build_metadata_path = resolved_source_pack_dir / "build.json"
    build_metadata = (
        json.loads(build_metadata_path.read_text(encoding="utf-8"))
        if build_metadata_path.exists()
        else {}
    )
    sources_metadata_path = resolved_source_pack_dir / "sources.json"
    sources_metadata = (
        json.loads(sources_metadata_path.read_text(encoding="utf-8"))
        if sources_metadata_path.exists()
        else {}
    )
    bundle_manifest_path = (
        Path(str(build_metadata["bundle_manifest_path"])).expanduser().resolve()
        if build_metadata.get("bundle_manifest_path")
        else None
    )
    allowed_ambiguous_aliases: set[str] = set()
    if bundle_manifest_path is not None and bundle_manifest_path.exists():
        bundle_manifest = SourceBundleManifest.from_path(bundle_manifest_path)
        allowed_ambiguous_aliases = {
            _normalized_key(item)
            for item in bundle_manifest.allowed_ambiguous_aliases
            if _normalized_key(item)
        }

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    pack_version = version or source_manifest.version
    generated_at = _utc_timestamp()
    pack_dir = resolved_output_dir / source_manifest.pack_id
    if pack_dir.exists():
        shutil.rmtree(pack_dir)
    pack_dir.mkdir(parents=True, exist_ok=True)

    labels_path = pack_dir / "labels.json"
    aliases_path = pack_dir / "aliases.json"
    rules_path = pack_dir / "rules.json"
    manifest_output_path = pack_dir / "manifest.json"
    matcher_dir = pack_dir / "matcher"
    sources_path = pack_dir / "sources.json" if include_build_metadata else None
    build_path = pack_dir / "build.json" if include_build_metadata else None
    alias_analysis_path = pack_dir / "alias-analysis.json" if include_build_metadata else None
    analysis_db_path = pack_dir / "analysis.sqlite" if include_build_metadata else None

    shutil.copy2(resolved_source_pack_dir / "labels.json", labels_path)
    if (resolved_source_pack_dir / "rules.json").exists():
        shutil.copy2(resolved_source_pack_dir / "rules.json", rules_path)
    else:
        rules_path.write_text(json.dumps({"patterns": []}, indent=2) + "\n", encoding="utf-8")

    alias_analysis = analyze_alias_candidates_from_db(
        source_analysis_db_path,
        allowed_ambiguous_aliases=allowed_ambiguous_aliases,
        analysis_db_path=analysis_db_path,
        materialize_retained_aliases=analysis_db_path is None,
    )
    if _should_audit_general_retained_aliases(
        domain=source_manifest.domain,
        language=source_manifest.language,
    ):
        review_cache_root = (
            bundle_manifest_path.parent
            if bundle_manifest_path is not None
            else resolved_source_pack_dir
        )
        alias_analysis = audit_general_retained_aliases(
            alias_analysis,
            review_mode=general_retained_alias_review_mode,
            reviewer=general_retained_alias_reviewer,
            review_cache_path=(
                general_retained_alias_review_cache_path
                if general_retained_alias_review_cache_path is not None
                else _default_general_retained_alias_review_cache_path(review_cache_root)
            ),
        )
        resolved_exclusions_path = (
            Path(general_retained_alias_exclusions_path).expanduser().resolve()
            if general_retained_alias_exclusions_path is not None
            else _default_general_retained_alias_exclusions_path(review_cache_root)
        )
        if resolved_exclusions_path.exists():
            alias_analysis = apply_general_retained_alias_exclusions(
                alias_analysis,
                exclusions_path=resolved_exclusions_path,
            )
    if alias_analysis_path is not None:
        write_alias_analysis_report(alias_analysis_path, alias_analysis)
    if alias_analysis.retained_aliases_materialized:
        _write_aliases_json(
            aliases_path,
            sorted(
                alias_analysis.retained_aliases,
                key=lambda item: (
                    _stable_sort_key(item["text"]),
                    _stable_sort_key(item["label"]),
                ),
            ),
        )
    else:
        if analysis_db_path is None:
            raise ValueError("analysis_db_path is required for DB-backed alias refresh.")
        _write_aliases_json_from_analysis_db(
            aliases_path,
            analysis_db_path,
            source_domain=source_manifest.domain,
        )

    matcher_artifact = build_matcher_artifact_from_aliases_json(
        aliases_path,
        output_dir=matcher_dir,
        include_runtime_tiers={
            "runtime_exact_acronym_high_precision",
            "runtime_exact_geopolitical_high_precision",
            "runtime_exact_high_precision",
        },
    )

    artifact_url = f"../../artifacts/{source_manifest.pack_id}-{pack_version}.tar.zst"
    refreshed_manifest = PackManifest(
        schema_version=source_manifest.schema_version,
        pack_id=source_manifest.pack_id,
        version=pack_version,
        language=source_manifest.language,
        domain=source_manifest.domain,
        tier=source_manifest.tier,
        description=source_manifest.description,
        tags=list(source_manifest.tags),
        dependencies=list(source_manifest.dependencies),
        artifacts=[
            PackArtifact(
                name="pack",
                url=artifact_url,
                sha256="source-placeholder",
            )
        ],
        models=list(source_manifest.models),
        rules=list(source_manifest.rules),
        labels=list(source_manifest.labels),
        matcher=PackMatcher(
            schema_version=1,
            algorithm=matcher_artifact.algorithm,
            normalization=matcher_artifact.normalization,
            artifact_path=(Path(matcher_dir.name) / Path(matcher_artifact.artifact_path).name).as_posix(),
            entries_path=(Path(matcher_dir.name) / Path(matcher_artifact.entries_path).name).as_posix(),
            entry_count=matcher_artifact.entry_count,
            state_count=matcher_artifact.state_count,
            max_alias_length=matcher_artifact.max_alias_length,
            artifact_sha256=matcher_artifact.artifact_sha256,
            entries_sha256=matcher_artifact.entries_sha256,
        ),
        min_ades_version=source_manifest.min_ades_version,
        min_entities_per_100_tokens_warning=source_manifest.min_entities_per_100_tokens_warning,
    )
    manifest_output_path.write_text(
        json.dumps(refreshed_manifest.to_dict(), indent=2) + "\n",
        encoding="utf-8",
    )

    if sources_path is not None:
        refreshed_sources = dict(sources_metadata)
        if refreshed_sources:
            refreshed_sources["version"] = pack_version
            refreshed_sources["pack_id"] = source_manifest.pack_id
        sources_path.write_text(
            json.dumps(refreshed_sources, indent=2) + "\n",
            encoding="utf-8",
        )

    labels_payload = json.loads(labels_path.read_text(encoding="utf-8"))
    rules_payload = json.loads(rules_path.read_text(encoding="utf-8")).get("patterns", [])
    label_distribution = _build_label_distribution(
        alias_analysis.retained_label_counts,
        rules_payload,
    )
    if build_path is not None:
        refreshed_build_metadata = dict(build_metadata)
        refreshed_build_metadata.update(
            {
                "schema_version": 1,
                "generated_at": generated_at,
                "analysis_backend": alias_analysis.backend,
                "analysis_db_path": str(analysis_db_path) if analysis_db_path is not None else None,
                "analysis_seed_db_path": str(source_analysis_db_path),
                "alias_analysis_report_path": (
                    str(alias_analysis_path) if alias_analysis_path is not None else None
                ),
                "dropped_alias_count": int(build_metadata.get("dropped_alias_count", 0))
                + alias_analysis.retained_alias_audit_removed_alias_count
                + alias_analysis.retained_alias_exclusion_removed_alias_count,
                "candidate_alias_count": alias_analysis.candidate_alias_count,
                "retained_alias_count": alias_analysis.retained_alias_count,
                "collision_blocked_alias_count": alias_analysis.blocked_alias_count,
                "retained_alias_audit_scanned_alias_count": alias_analysis.retained_alias_audit_scanned_alias_count,
                "retained_alias_audit_removed_alias_count": alias_analysis.retained_alias_audit_removed_alias_count,
                "retained_alias_audit_chunk_size": alias_analysis.retained_alias_audit_chunk_size,
                "retained_alias_audit_reason_counts": alias_analysis.retained_alias_audit_reason_counts,
                "retained_alias_audit_mode": alias_analysis.retained_alias_audit_mode,
                "retained_alias_audit_reviewer_name": alias_analysis.retained_alias_audit_reviewer_name,
                "retained_alias_audit_review_cache_path": alias_analysis.retained_alias_audit_review_cache_path,
                "retained_alias_audit_invoked_review_count": alias_analysis.retained_alias_audit_invoked_review_count,
                "retained_alias_audit_cached_review_count": alias_analysis.retained_alias_audit_cached_review_count,
                "retained_alias_exclusion_removed_alias_count": alias_analysis.retained_alias_exclusion_removed_alias_count,
                "retained_alias_exclusion_source_path": alias_analysis.retained_alias_exclusion_source_path,
                "ambiguous_alias_count": alias_analysis.ambiguous_alias_count,
                "exact_identifier_alias_count": alias_analysis.exact_identifier_alias_count,
                "natural_language_alias_count": alias_analysis.natural_language_alias_count,
                "blocked_reason_counts": alias_analysis.blocked_reason_counts,
                "retained_label_counts": alias_analysis.retained_label_counts,
                "alias_count": alias_analysis.retained_alias_count,
                "label_count": len(labels_payload),
                "rule_count": len(rules_payload),
                "matcher": {
                    "algorithm": matcher_artifact.algorithm,
                    "normalization": matcher_artifact.normalization,
                    "artifact_path": str(Path(matcher_artifact.artifact_path)),
                    "entries_path": str(Path(matcher_artifact.entries_path)),
                    "entry_count": matcher_artifact.entry_count,
                    "state_count": matcher_artifact.state_count,
                    "max_alias_length": matcher_artifact.max_alias_length,
                    "artifact_sha256": matcher_artifact.artifact_sha256,
                    "entries_sha256": matcher_artifact.entries_sha256,
                },
                "label_distribution": label_distribution,
            }
        )
        build_path.write_text(
            json.dumps(refreshed_build_metadata, indent=2) + "\n",
            encoding="utf-8",
        )

    warnings: list[str] = []
    if alias_analysis.retained_alias_count == 0:
        warnings.append("Generated pack does not include any aliases.")
    if not rules_payload:
        warnings.append("Generated pack does not include any rules.")

    return GeneratedPackSourceResult(
        pack_id=source_manifest.pack_id,
        version=pack_version,
        bundle_dir=str(build_metadata.get("bundle_dir") or resolved_source_pack_dir),
        output_dir=str(resolved_output_dir),
        pack_dir=str(pack_dir),
        manifest_path=str(manifest_output_path),
        labels_path=str(labels_path),
        aliases_path=str(aliases_path),
        rules_path=str(rules_path),
        matcher_artifact_path=matcher_artifact.artifact_path,
        matcher_entries_path=matcher_artifact.entries_path,
        matcher_algorithm=matcher_artifact.algorithm,
        matcher_entry_count=matcher_artifact.entry_count,
        matcher_state_count=matcher_artifact.state_count,
        matcher_max_alias_length=matcher_artifact.max_alias_length,
        matcher_artifact_sha256=matcher_artifact.artifact_sha256,
        matcher_entries_sha256=matcher_artifact.entries_sha256,
        sources_path=str(sources_path) if sources_path is not None else None,
        build_path=str(build_path) if build_path is not None else None,
        generated_at=generated_at,
        label_count=len(labels_payload),
        alias_count=alias_analysis.retained_alias_count,
        rule_count=len(rules_payload),
        source_count=int(build_metadata.get("source_count", len(sources_metadata.get("sources", [])))),
        publishable_source_count=int(build_metadata.get("publishable_source_count", 0)),
        restricted_source_count=int(build_metadata.get("restricted_source_count", 0)),
        publishable_sources_only=bool(build_metadata.get("publishable_sources_only", False)),
        included_entity_count=int(build_metadata.get("included_entity_count", 0)),
        included_rule_count=int(build_metadata.get("included_rule_count", len(rules_payload))),
        dropped_record_count=int(build_metadata.get("dropped_record_count", 0)),
        dropped_alias_count=int(build_metadata.get("dropped_alias_count", 0))
        + alias_analysis.retained_alias_audit_removed_alias_count,
        ambiguous_alias_count=alias_analysis.ambiguous_alias_count,
        source_license_classes=dict(build_metadata.get("source_license_classes", {})),
        entity_label_distribution=dict(build_metadata.get("entity_label_distribution", {})),
        label_distribution=label_distribution,
        warnings=warnings,
    )


def _iter_jsonl_records(
    path: Path | None,
    *,
    required: bool,
) -> Any:
    if path is None:
        return
    resolved = path.expanduser().resolve()
    if not resolved.exists():
        if required:
            raise FileNotFoundError(f"Normalized input file not found: {resolved}")
        return
    if not resolved.is_file():
        raise FileExistsError(f"Normalized input path is not a file: {resolved}")

    with resolved.open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL record in {resolved} line {index}") from exc
            if not isinstance(payload, dict):
                raise ValueError(
                    f"JSONL record in {resolved} line {index} must be an object."
                )
            yield payload


def _load_jsonl_records(path: Path | None, *, required: bool) -> list[dict[str, Any]]:
    return list(_iter_jsonl_records(path, required=required))


def _write_pre_resolved_aliases_json(
    path: Path,
    *,
    bundle_dir: Path,
    bundle: SourceBundleManifest,
    label_mappings: dict[str, str],
    stoplisted_aliases: set[str],
    include_build_only: bool,
) -> tuple[
    AliasAnalysisResult,
    set[str],
    dict[str, int],
    int,
    int,
    int,
    int,
]:
    labels: set[str] = set()
    entity_label_distribution: dict[str, int] = {}
    included_entity_count = 0
    input_entity_count = 0
    dropped_record_count = 0
    dropped_alias_count = 0
    candidate_alias_count = 0
    exact_identifier_alias_count = 0
    natural_language_alias_count = 0
    retained_label_counts: dict[str, int] = {}

    with path.open("w", encoding="utf-8") as handle:
        handle.write('{\n  "aliases": [\n')
        first = True
        for index, record in enumerate(
            _iter_jsonl_records(bundle_dir / bundle.entities_path, required=True),
            start=1,
        ):
            input_entity_count += 1
            if _coerce_bool(record.get("build_only")) and not include_build_only:
                dropped_record_count += 1
                continue
            raw_blocked_aliases = record.get("blocked_aliases", [])
            if raw_blocked_aliases is None:
                raw_blocked_aliases = []
            if not isinstance(raw_blocked_aliases, list):
                raise ValueError("Normalized entity blocked_aliases must be a list.")
            blocked_aliases = {
                _normalized_key(item)
                for item in raw_blocked_aliases
                if _normalized_key(item)
            }
            label = _resolve_record_label(
                record,
                label_mappings=label_mappings,
                kind="entity",
                record_index=index,
            )
            labels.add(label)
            included_entity_count += 1
            entity_label_distribution[label] = entity_label_distribution.get(label, 0) + 1
            canonical_text = _require_clean_text(
                record.get("canonical_text"),
                kind="entity canonical_text",
                record_index=index,
            )
            seen_alias_keys: set[str] = set()
            source_domain = _clean_text(record.get("source_domain")) or bundle.domain
            for candidate_text in _iter_entity_explicit_aliases(canonical_text, record):
                normalized_key = _normalized_key(candidate_text)
                if not normalized_key:
                    continue
                if normalized_key in seen_alias_keys:
                    continue
                seen_alias_keys.add(normalized_key)
                if normalized_key in blocked_aliases:
                    dropped_alias_count += 1
                    continue
                if _should_drop_alias(
                    normalized_key,
                    candidate_text,
                    stoplisted_aliases,
                    label=label,
                    canonical_text=canonical_text,
                    generated=False,
                    pack_domain=bundle.domain,
                ):
                    dropped_alias_count += 1
                    continue
                candidate = build_alias_candidate(
                    alias_key=normalized_key,
                    display_text=candidate_text,
                    label=label,
                    canonical_text=canonical_text,
                    record=record,
                    generated=False,
                )
                payload = _serialize_alias_payload(candidate, source_domain=source_domain)
                if not first:
                    handle.write(",\n")
                handle.write(f"    {json.dumps(payload, sort_keys=True)}")
                first = False
                candidate_alias_count += 1
                retained_label_counts[label] = retained_label_counts.get(label, 0) + 1
                if candidate.lane == "exact":
                    exact_identifier_alias_count += 1
                else:
                    natural_language_alias_count += 1
        handle.write("\n  ]\n}\n")

    alias_analysis = AliasAnalysisResult(
        backend="pre_resolved_bundle",
        database_path=None,
        candidate_alias_count=candidate_alias_count,
        retained_alias_count=candidate_alias_count,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=exact_identifier_alias_count,
        natural_language_alias_count=natural_language_alias_count,
        blocked_reason_counts={},
        retained_label_counts={
            label: retained_label_counts[label]
            for label in sorted(retained_label_counts, key=_stable_sort_key)
        },
        retained_aliases_materialized=False,
        retained_aliases=[],
        top_collision_clusters=[],
    )
    return (
        alias_analysis,
        labels,
        {
            label: entity_label_distribution[label]
            for label in sorted(entity_label_distribution, key=_stable_sort_key)
        },
        included_entity_count,
        input_entity_count,
        dropped_record_count,
        dropped_alias_count,
    )


def _write_aliases_json(path: Path, aliases: list[dict[str, Any]]) -> None:
    path.write_text(
        json.dumps({"aliases": aliases}, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_aliases_json_from_analysis_db(
    path: Path,
    analysis_db_path: Path,
    *,
    source_domain: str,
) -> None:
    with path.open("w", encoding="utf-8") as handle:
        handle.write('{\n  "aliases": [\n')
        first = True
        for item in iter_retained_aliases_from_db(analysis_db_path):
            item = {
                **item,
                "alias_score": round(float(item.get("alias_score", item["score"])), 4),
                "source_domain": str(item.get("source_domain") or source_domain),
            }
            if not first:
                handle.write(",\n")
            handle.write(f"    {json.dumps(item, sort_keys=True)}")
            first = False
        handle.write("\n  ]\n}\n")


def _build_label_distribution(
    alias_label_counts: dict[str, int],
    patterns: list[dict[str, str]],
) -> dict[str, int]:
    distribution = dict(alias_label_counts)
    for item in patterns:
        label = _clean_text(item.get("label"))
        if label:
            distribution[label] = distribution.get(label, 0) + 1
    return {
        label: distribution[label]
        for label in sorted(distribution, key=_stable_sort_key)
    }


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
    *,
    pack_domain: str,
) -> list[tuple[str, bool]]:
    seen: set[str] = set()
    candidates: list[tuple[str, bool]] = []
    raw_aliases = record.get("aliases", [])
    if raw_aliases is None:
        raw_aliases = []
    if not isinstance(raw_aliases, list):
        raise ValueError("Normalized entity aliases must be a list.")
    explicit_values = [canonical_text, *raw_aliases]
    for raw_value in explicit_values:
        cleaned = _clean_text(raw_value)
        if not cleaned:
            continue
        for variant, generated in _expand_alias_variants(
            cleaned,
            label,
            pack_domain=pack_domain,
        ):
            if variant not in seen:
                seen.add(variant)
                candidates.append((variant, generated))
    return candidates


def _iter_entity_explicit_aliases(
    canonical_text: str,
    record: dict[str, Any],
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
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        candidates.append(cleaned)
    return candidates


def _serialize_alias_payload(candidate: Any, *, source_domain: str) -> dict[str, Any]:
    rounded_score = round(float(candidate.score), 4)
    return {
        "text": str(candidate.display_text),
        "label": str(candidate.label),
        "normalized_text": str(candidate.alias_key),
        "canonical_text": str(candidate.canonical_text),
        "score": rounded_score,
        "alias_score": rounded_score,
        "generated": bool(candidate.generated),
        "source_name": str(candidate.source_name),
        "entity_id": str(candidate.entity_id),
        "source_priority": round(float(candidate.features.source_priority), 4),
        "popularity_weight": round(float(candidate.features.popularity_weight), 4),
        "source_domain": str(source_domain),
        "runtime_tier": "runtime_exact_high_precision",
    }


def _expand_alias_variants(
    text: str,
    label: str,
    *,
    pack_domain: str,
) -> list[tuple[str, bool]]:
    variants: list[tuple[str, bool]] = [(text, False)]
    canonical = canonicalize_text(text)
    if canonical and canonical != text:
        variants.append((canonical, True))
    if label.casefold() in _ORG_LIKE_LABELS:
        stripped = _strip_org_suffix_tokens(text, pack_domain=pack_domain)
        if stripped and stripped != text:
            variants.append((stripped, True))
    if label.casefold() == "person":
        short_person_name = _build_person_first_last_variant(text)
        if short_person_name and short_person_name != text:
            variants.append((short_person_name, True))
    compact = _clean_text(re.sub(r"\s*[-/]\s*", " ", text))
    if compact and compact != text:
        variants.append((compact, True))
    initialism_acronym = _build_initialism_acronym_variant(text, label=label)
    if initialism_acronym and initialism_acronym != text:
        variants.append((initialism_acronym, True))
    compact_acronym = _build_compact_acronym_variant(text)
    if compact_acronym and compact_acronym != text:
        variants.append((compact_acronym, True))
    article_stripped_acronym = _build_article_stripped_acronym_variant(text)
    if article_stripped_acronym and article_stripped_acronym != text:
        variants.append((article_stripped_acronym, True))
    return variants


def _strip_org_suffix_tokens(text: str, *, pack_domain: str) -> str:
    tokens = text.split()
    if len(tokens) < 2:
        return ""
    stripped_tokens = list(tokens)
    while len(stripped_tokens) > 1:
        normalized = stripped_tokens[-1].rstrip(".,").casefold()
        if normalized not in _ORG_SUFFIX_TOKENS:
            break
        stripped_tokens.pop()
    if not stripped_tokens or len(stripped_tokens) == len(tokens):
        return ""
    stripped = _clean_text(" ".join(stripped_tokens))
    if (
        pack_domain == "general"
        and _is_single_token_alpha_alias(stripped)
        and _compact_alnum_length(stripped) < 6
    ):
        return ""
    return stripped


def _build_person_first_last_variant(text: str) -> str:
    tokens = text.split()
    if len(tokens) < 3 or len(tokens) > 4:
        return ""
    if not _looks_like_name_edge_token(tokens[0]) or not _looks_like_name_edge_token(tokens[-1]):
        return ""
    middle_tokens = tokens[1:-1]
    if not middle_tokens or not all(_looks_like_middle_name_token(token) for token in middle_tokens):
        return ""
    return _clean_text(f"{tokens[0]} {tokens[-1]}")


def _build_compact_acronym_variant(text: str) -> str:
    compact = "".join(character for character in text if character.isalnum())
    if not (2 <= len(compact) <= 6):
        return ""
    if not compact.isalpha():
        return ""
    if compact == text:
        return ""
    if not any(character in text for character in ". "):
        return ""
    tokens = [token for token in re.split(r"[\s\-./]+", text.strip()) if token]
    if not tokens:
        return ""
    if not all(token.isalpha() for token in tokens):
        return ""
    if not all(token.isupper() or len(token) == 1 for token in tokens):
        return ""
    return compact.upper()


def _build_initialism_acronym_variant(text: str, *, label: str) -> str:
    tokens = [
        token
        for token in re.findall(r"[A-Za-z]+", text)
        if token.casefold() not in _ACRONYM_CONNECTOR_TOKENS
    ]
    normalized_label = label.casefold()
    if normalized_label in _ORG_LIKE_LABELS:
        if not (3 <= len(tokens) <= 6):
            return ""
        if not all(token.isupper() or token[:1].isupper() for token in tokens):
            return ""
        acronym = "".join(token[0].upper() for token in tokens if token[:1].isalpha())
        if not (3 <= len(acronym) <= 6):
            return ""
        return acronym
    if normalized_label != "location":
        return ""
    if not all(token.isupper() or token[:1].isupper() for token in tokens):
        return ""
    acronym = "".join(token[0].upper() for token in tokens if token[:1].isalpha())
    if not (2 <= len(tokens) <= 4):
        return ""
    if not (2 <= len(acronym) <= 4):
        return ""
    if _GEOPOLITICAL_ACRONYM_TOKENS.isdisjoint(token.casefold() for token in tokens):
        return ""
    return acronym


def _build_article_stripped_acronym_variant(text: str) -> str:
    tokens = text.split()
    if len(tokens) < 2:
        return ""
    if tokens[0].casefold() not in {"a", "an", "the"}:
        return ""
    remainder = _clean_text(" ".join(tokens[1:]))
    if not remainder:
        return ""
    compact_remainder = _build_compact_acronym_variant(remainder)
    if compact_remainder:
        return compact_remainder
    if remainder.isupper() and remainder.isalpha() and 2 <= len(remainder) <= 6:
        return remainder
    return ""


def _looks_like_name_edge_token(token: str) -> bool:
    stripped = token.strip(".,")
    return len(stripped) >= 2 and stripped[:1].isupper()


def _looks_like_middle_name_token(token: str) -> bool:
    stripped = token.strip(".,")
    if not stripped:
        return False
    if len(stripped) == 1 and stripped.isalpha():
        return True
    return stripped[:1].isupper() and stripped[1:].islower()


def _should_drop_alias(
    normalized_key: str,
    display_value: str,
    stoplisted_aliases: set[str],
    *,
    label: str,
    canonical_text: str,
    generated: bool,
    pack_domain: str,
) -> bool:
    if normalized_key in stoplisted_aliases:
        return True
    if len(display_value) < 2:
        return True
    if not any(character.isalnum() for character in display_value):
        return True
    if pack_domain == "general" and _is_single_token_alpha_alias(display_value):
        if (
            label.casefold() in _ORG_LIKE_LABELS
            and _is_structural_org_fragment_alias(
                display_value,
                canonical_text=canonical_text,
            )
        ):
            return True
        if label.casefold() in {"person", "location"}:
            if (
                label.casefold() == "location"
                and generated
                and display_value.isupper()
                and _build_initialism_acronym_variant(
                    canonical_text,
                    label=label,
                )
                == display_value
            ):
                return False
            return display_value.casefold() != canonical_text.casefold()
        if (
            label.casefold() in _ORG_LIKE_LABELS
            and display_value.casefold() != canonical_text.casefold()
            and not display_value.isupper()
            and _compact_alnum_length(display_value) < 6
        ):
            return True
        if generated and label.casefold() in _ORG_LIKE_LABELS and not display_value.isupper():
            return _compact_alnum_length(display_value) < 6
    return False


def _is_structural_org_fragment_alias(value: str, *, canonical_text: str) -> bool:
    canonical_tokens = [
        token.rstrip(".,").casefold() for token in canonical_text.split() if token.strip()
    ]
    if len(canonical_tokens) < 3:
        return False
    if canonical_tokens[-1] not in _ORG_FRAGMENT_SUFFIX_TOKENS:
        return False
    normalized_value = value.strip().rstrip(".,").casefold()
    if not normalized_value or not _is_single_token_alpha_alias(value):
        return False
    return normalized_value in {canonical_tokens[0], canonical_tokens[-1]}


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
    return canonicalize_text(str(raw_value))


def _coerce_bool(raw_value: Any) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    if raw_value is None:
        return False
    if isinstance(raw_value, str):
        return raw_value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(raw_value)


def _normalized_key(value: str) -> str:
    return normalize_lookup_text(value)


def _stable_sort_key(value: str) -> tuple[str, str]:
    return (value.casefold(), value)


def _is_single_token_alpha_alias(value: str) -> bool:
    tokens = [token for token in re.split(r"[\s\-_./]+", value.strip()) if token]
    return len(tokens) == 1 and tokens[0].isalpha()


def _compact_alnum_length(value: str) -> int:
    return len("".join(character for character in value if character.isalnum()))


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
