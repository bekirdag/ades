#!/usr/bin/env python3
"""Audit direct vector neighbors for saved unseen-RSS response bundles."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any

from ades.config import get_settings
from ades.packs.registry import PackRegistry
from ades.vector.evaluation import (
    VectorGoldenSeedEntity,
    VectorSeedAuditCase,
    VectorSeedAuditCaseResult,
    VectorSeedAuditNeighbor,
    VectorSeedAuditReport,
    audit_vector_seed_neighbors,
    write_vector_seed_audit_report,
)
from ades.vector.qdrant import QdrantVectorSearchClient, QdrantVectorSearchError


@dataclass(frozen=True)
class _SavedResponseSnapshot:
    timing_ms: int | None
    warnings: tuple[str, ...]
    entity_ids: tuple[str, ...]
    related_entity_ids: tuple[str, ...]
    entity_count: int
    related_entity_count: int
    graph_support_applied: bool | None


@dataclass(frozen=True)
class _SavedResponseCase:
    case_id: str
    source_summary_path: Path
    source_summary_name: str
    slug: str
    title: str
    source_name: str | None
    pack_id: str
    domain_hint: str | None
    country_hint: str | None
    response_file_path: Path
    plain: _SavedResponseSnapshot
    hinted: _SavedResponseSnapshot
    seed_entities: tuple[VectorGoldenSeedEntity, ...]


@dataclass(frozen=True)
class _LoadedSummary:
    summary_path: Path
    source_kind: str
    compatible: bool
    raw_case_count: int
    loaded_case_count: int
    warnings: tuple[str, ...]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit direct vector neighbors from one or more saved unseen-RSS "
            "response-bundle summaries."
        ),
    )
    parser.add_argument(
        "--summary",
        action="append",
        required=True,
        help=(
            "Path to one saved summary.json with case response_file_path entries. "
            "Repeat for multiple compatible runs."
        ),
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Optional output directory. Defaults under "
            "docs/planning/live-rss-vector-reviews/."
        ),
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Neighbor count to keep per seed/alias.",
    )
    parser.add_argument(
        "--max-cases",
        type=int,
        help="Optional cap on loaded cases after summary ingestion.",
    )
    parser.add_argument(
        "--alias-override",
        action="append",
        default=[],
        help=(
            "Optional alias remap for side-by-side rebuild comparison, in the form "
            "current_alias=rebuild_alias. Repeat for multiple aliases."
        ),
    )
    return parser.parse_args()


def _parse_alias_overrides(values: list[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for raw_value in values:
        normalized = raw_value.strip()
        if not normalized:
            continue
        if "=" not in normalized:
            raise SystemExit(
                f"Invalid --alias-override value {raw_value!r}; expected current_alias=rebuild_alias."
            )
        source_alias, target_alias = (part.strip() for part in normalized.split("=", 1))
        if not source_alias or not target_alias:
            raise SystemExit(
                f"Invalid --alias-override value {raw_value!r}; expected current_alias=rebuild_alias."
            )
        overrides[source_alias] = target_alias
    return overrides


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected one JSON object in {path}.")
    return payload


def _safe_slug(value: str) -> str:
    cleaned = "".join(
        character if character.isalnum() or character in {"-", "_"} else "-"
        for character in value.strip().casefold()
    )
    normalized = "-".join(part for part in cleaned.split("-") if part)
    return normalized or "case"


def _coerced_str_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(
        normalized
        for item in value
        if isinstance(item, str)
        if (normalized := item.strip())
    )


def _entity_id_from_payload(item: dict[str, Any]) -> str | None:
    entity_id = item.get("entity_id")
    if isinstance(entity_id, str) and entity_id.strip():
        return entity_id.strip()
    link = item.get("link")
    if isinstance(link, dict):
        linked_entity_id = link.get("entity_id")
        if isinstance(linked_entity_id, str) and linked_entity_id.strip():
            return linked_entity_id.strip()
    return None


def _canonical_text_from_payload(item: dict[str, Any], *, entity_id: str) -> str:
    canonical_text = item.get("canonical_text")
    if isinstance(canonical_text, str) and canonical_text.strip():
        return canonical_text.strip()
    link = item.get("link")
    if isinstance(link, dict):
        linked_canonical = link.get("canonical_text")
        if isinstance(linked_canonical, str) and linked_canonical.strip():
            return linked_canonical.strip()
    text = item.get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    return entity_id


def _label_from_payload(item: dict[str, Any]) -> str:
    label = item.get("label")
    if isinstance(label, str) and label.strip():
        return label.strip()
    return "unknown"


def _entity_ids_from_items(items: object) -> tuple[str, ...]:
    if not isinstance(items, list):
        return ()
    values: list[str] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        entity_id = _entity_id_from_payload(item)
        if entity_id is None or entity_id in seen:
            continue
        seen.add(entity_id)
        values.append(entity_id)
    return tuple(values)


def _extract_response_snapshot(payload: object) -> _SavedResponseSnapshot:
    if not isinstance(payload, dict):
        return _SavedResponseSnapshot(
            timing_ms=None,
            warnings=(),
            entity_ids=(),
            related_entity_ids=(),
            entity_count=0,
            related_entity_count=0,
            graph_support_applied=None,
        )
    timing_ms = payload.get("timing_ms")
    graph_support = payload.get("graph_support")
    graph_support_applied: bool | None = None
    if isinstance(graph_support, dict):
        applied = graph_support.get("applied")
        if isinstance(applied, bool):
            graph_support_applied = applied
    entity_ids = _entity_ids_from_items(payload.get("entities"))
    related_entity_ids = _entity_ids_from_items(payload.get("related_entities"))
    resolved_timing_ms = int(timing_ms) if isinstance(timing_ms, (int, float)) else None
    entity_count = payload.get("entity_count")
    related_entity_count = payload.get("related_entity_count")
    return _SavedResponseSnapshot(
        timing_ms=resolved_timing_ms,
        warnings=_coerced_str_tuple(payload.get("warnings")),
        entity_ids=entity_ids,
        related_entity_ids=related_entity_ids,
        entity_count=int(entity_count) if isinstance(entity_count, (int, float)) else len(entity_ids),
        related_entity_count=(
            int(related_entity_count)
            if isinstance(related_entity_count, (int, float))
            else len(related_entity_ids)
        ),
        graph_support_applied=graph_support_applied,
    )


def _extract_seed_entities(hinted_payload: dict[str, Any]) -> tuple[VectorGoldenSeedEntity, ...]:
    entities = hinted_payload.get("entities")
    if not isinstance(entities, list):
        return ()
    seed_entities: list[VectorGoldenSeedEntity] = []
    seen: set[str] = set()
    for item in entities:
        if not isinstance(item, dict):
            continue
        entity_id = _entity_id_from_payload(item)
        if entity_id is None or not entity_id.startswith("wikidata:Q") or entity_id in seen:
            continue
        seen.add(entity_id)
        seed_entities.append(
            VectorGoldenSeedEntity(
                entity_id=entity_id,
                canonical_text=_canonical_text_from_payload(item, entity_id=entity_id),
                label=_label_from_payload(item),
                score=1.0,
            )
        )
    return tuple(seed_entities)


def _slug_from_legacy_report(report: dict[str, Any], *, fallback_index: int) -> str:
    for key in ("report_file_name", "news_file_name"):
        value = report.get(key)
        if isinstance(value, str) and value.strip():
            return Path(value.strip()).stem
    title = report.get("title")
    if isinstance(title, str) and title.strip():
        return _safe_slug(title)
    return f"report-{fallback_index:04d}"


def _snapshot_from_legacy_report(
    report: dict[str, Any],
    *,
    entity_key: str,
    entity_count_key: str,
    timing_key: str,
    include_related_entities: bool,
) -> _SavedResponseSnapshot:
    synthetic_payload: dict[str, Any] = {
        "timing_ms": report.get(timing_key),
        "entities": report.get(entity_key),
        "entity_count": report.get(entity_count_key),
    }
    if include_related_entities:
        synthetic_payload["related_entities"] = report.get("related_entities")
        synthetic_payload["related_entity_count"] = report.get("related_entity_count")
    return _extract_response_snapshot(synthetic_payload)


def _load_report_summary_case(
    report: dict[str, Any],
    *,
    source_summary_path: Path,
    index: int,
) -> _SavedResponseCase:
    slug = _slug_from_legacy_report(report, fallback_index=index)
    title = str(report.get("title") or slug).strip()
    source_name_value = report.get("source")
    source_name = (
        str(source_name_value).strip()
        if isinstance(source_name_value, str) and source_name_value.strip()
        else None
    )
    report_file_path_value = report.get("report_file_path")
    news_file_path_value = report.get("news_file_path")
    if isinstance(report_file_path_value, str) and report_file_path_value.strip():
        response_file_path = Path(report_file_path_value).expanduser().resolve()
    elif isinstance(news_file_path_value, str) and news_file_path_value.strip():
        response_file_path = Path(news_file_path_value).expanduser().resolve()
    else:
        response_file_path = source_summary_path
    source_summary_name = source_summary_path.parent.name
    case_id = f"{_safe_slug(source_summary_name)}__{_safe_slug(slug)}"
    hinted_payload = {"entities": report.get("flagged_entities")}
    return _SavedResponseCase(
        case_id=case_id,
        source_summary_path=source_summary_path,
        source_summary_name=source_summary_name,
        slug=slug,
        title=title,
        source_name=source_name,
        pack_id="general-en",
        domain_hint=None,
        country_hint=None,
        response_file_path=response_file_path,
        plain=_snapshot_from_legacy_report(
            report,
            entity_key="plain_entities",
            entity_count_key="plain_entity_count",
            timing_key="plain_timing_ms",
            include_related_entities=False,
        ),
        hinted=_snapshot_from_legacy_report(
            report,
            entity_key="flagged_entities",
            entity_count_key="flagged_entity_count",
            timing_key="flagged_timing_ms",
            include_related_entities=True,
        ),
        seed_entities=_extract_seed_entities(hinted_payload),
    )


def _load_saved_response_case(path: Path, *, source_summary_path: Path) -> _SavedResponseCase:
    payload = _load_json(path)
    case_payload = payload.get("case")
    if isinstance(case_payload, dict):
        record = case_payload
    else:
        record = payload
    hinted_payload = payload.get("new_hinted")
    if not isinstance(hinted_payload, dict):
        hinted_payload = payload.get("hinted")
    if not isinstance(hinted_payload, dict):
        hinted_payload = record.get("hinted")
    if not isinstance(hinted_payload, dict):
        hinted_payload = record
    plain_payload = payload.get("new_plain")
    if not isinstance(plain_payload, dict):
        plain_payload = payload.get("plain")
    if not isinstance(plain_payload, dict):
        plain_payload = record.get("plain")
    slug = str(record.get("slug") or path.stem.replace(".responses", "")).strip()
    title = str(record.get("title") or slug).strip()
    source_name = record.get("source")
    pack_id = str(record.get("pack") or hinted_payload.get("pack") or "general-en").strip()
    domain_hint = record.get("domain_hint")
    country_hint = record.get("country_hint")
    source_summary_name = source_summary_path.parent.name
    case_id = f"{_safe_slug(source_summary_name)}__{_safe_slug(slug)}"
    return _SavedResponseCase(
        case_id=case_id,
        source_summary_path=source_summary_path,
        source_summary_name=source_summary_name,
        slug=slug,
        title=title,
        source_name=str(source_name).strip() if isinstance(source_name, str) and source_name.strip() else None,
        pack_id=pack_id or "general-en",
        domain_hint=str(domain_hint).strip() if isinstance(domain_hint, str) and domain_hint.strip() else None,
        country_hint=str(country_hint).strip() if isinstance(country_hint, str) and country_hint.strip() else None,
        response_file_path=path,
        plain=_extract_response_snapshot(plain_payload),
        hinted=_extract_response_snapshot(hinted_payload),
        seed_entities=_extract_seed_entities(hinted_payload),
    )


def _load_inline_summary_case(
    item: dict[str, Any],
    *,
    source_summary_path: Path,
    index: int,
) -> _SavedResponseCase:
    slug_value = item.get("slug")
    slug = (
        str(slug_value).strip()
        if isinstance(slug_value, str) and slug_value.strip()
        else f"case-{index:04d}"
    )
    title_value = item.get("title")
    title = str(title_value or slug).strip()
    source_name_value = item.get("source")
    source_name = (
        str(source_name_value).strip()
        if isinstance(source_name_value, str) and source_name_value.strip()
        else None
    )
    pack_value = item.get("pack")
    pack_id = (
        str(pack_value).strip()
        if isinstance(pack_value, str) and pack_value.strip()
        else "general-en"
    )
    domain_hint_value = item.get("domain_hint")
    country_hint_value = item.get("country_hint")
    news_path_value = item.get("news_path")
    if isinstance(news_path_value, str) and news_path_value.strip():
        response_file_path = Path(news_path_value).expanduser().resolve()
    else:
        response_file_path = source_summary_path
    hinted_payload = item.get("hinted")
    plain_payload = item.get("plain")
    if not isinstance(hinted_payload, dict):
        hinted_payload = {}
    if not isinstance(plain_payload, dict):
        plain_payload = {}
    source_summary_name = source_summary_path.parent.name
    case_id = f"{_safe_slug(source_summary_name)}__{_safe_slug(slug)}"
    return _SavedResponseCase(
        case_id=case_id,
        source_summary_path=source_summary_path,
        source_summary_name=source_summary_name,
        slug=slug,
        title=title,
        source_name=source_name,
        pack_id=pack_id,
        domain_hint=(
            str(domain_hint_value).strip()
            if isinstance(domain_hint_value, str) and domain_hint_value.strip()
            else None
        ),
        country_hint=(
            str(country_hint_value).strip()
            if isinstance(country_hint_value, str) and country_hint_value.strip()
            else None
        ),
        response_file_path=response_file_path,
        plain=_extract_response_snapshot(plain_payload),
        hinted=_extract_response_snapshot(hinted_payload),
        seed_entities=_extract_seed_entities(hinted_payload),
    )


def _load_summary_source(summary_path: Path) -> tuple[_LoadedSummary, list[_SavedResponseCase]]:
    summary = _load_json(summary_path)
    raw_cases = summary.get("cases")
    raw_reports = summary.get("reports")

    cases: list[_SavedResponseCase] = []
    warnings: list[str] = []
    source_kind = "unknown"

    if isinstance(raw_cases, list):
        source_kind = "cases"
        for index, item in enumerate(raw_cases):
            if not isinstance(item, dict):
                warnings.append(f"skipped_case:{index}:non_object_case")
                continue
            response_file_path = item.get("response_file_path")
            if isinstance(response_file_path, str) and response_file_path.strip():
                try:
                    cases.append(
                        _load_saved_response_case(
                            Path(response_file_path).expanduser().resolve(),
                            source_summary_path=summary_path,
                        )
                    )
                except Exception as exc:  # pragma: no cover - exercised via script execution
                    warnings.append(f"skipped_case:{index}:{type(exc).__name__}:{exc}")
                continue
            if isinstance(item.get("plain"), dict) or isinstance(item.get("hinted"), dict):
                try:
                    cases.append(
                        _load_inline_summary_case(
                            item,
                            source_summary_path=summary_path,
                            index=index,
                        )
                    )
                except Exception as exc:  # pragma: no cover - exercised via script execution
                    warnings.append(f"skipped_case:{index}:{type(exc).__name__}:{exc}")
                continue
            warnings.append(f"skipped_case:{index}:missing_response_file_path")
        if not cases:
            warnings.insert(0, "incompatible_summary:cases_without_response_file_path")
    elif isinstance(raw_reports, list):
        source_kind = "reports"
        for index, item in enumerate(raw_reports):
            if not isinstance(item, dict):
                warnings.append(f"skipped_report:{index}:non_object_report")
                continue
            status = item.get("status")
            if isinstance(status, str) and status.strip() and status.strip() != "success":
                warnings.append(f"skipped_report:{index}:status={status.strip()}")
                continue
            try:
                cases.append(
                    _load_report_summary_case(
                        item,
                        source_summary_path=summary_path,
                        index=index,
                    )
                )
            except Exception as exc:  # pragma: no cover - exercised via script execution
                warnings.append(f"skipped_report:{index}:{type(exc).__name__}:{exc}")
        if not cases:
            warnings.insert(0, "incompatible_summary:reports_without_saved_entities")
    elif isinstance(summary.get("articles"), list):
        source_kind = "articles"
        warnings.append("incompatible_summary:articles")
    else:
        warnings.append("incompatible_summary:unknown")

    raw_case_count = 0
    raw_source = summary.get(source_kind)
    if isinstance(raw_source, list):
        raw_case_count = len(raw_source)
    return (
        _LoadedSummary(
            summary_path=summary_path,
            source_kind=source_kind,
            compatible=bool(cases),
            raw_case_count=raw_case_count,
            loaded_case_count=len(cases),
            warnings=tuple(warnings),
        ),
        cases,
    )


def _default_output_dir(summary_paths: list[Path]) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    first_summary = summary_paths[0].resolve()
    root = first_summary.parent.parent
    return root / f"seed-neighbor-baseline-{timestamp}"


def _case_from_saved(saved_case: _SavedResponseCase, *, top_k: int) -> VectorSeedAuditCase:
    return VectorSeedAuditCase(
        name=saved_case.case_id,
        pack_id=saved_case.pack_id,
        seed_entities=saved_case.seed_entities,
        domain_hint=saved_case.domain_hint,
        country_hint=saved_case.country_hint,
        top_k=top_k,
    )


def _neighbor_line(neighbor: VectorSeedAuditNeighbor) -> str:
    label = neighbor.entity_type or "unknown"
    canonical = neighbor.canonical_text or neighbor.entity_id
    packs = f" packs={','.join(neighbor.packs)}" if neighbor.packs else ""
    return f"- `{neighbor.entity_id}` | `{canonical}` | `{label}` | score={neighbor.score:.4f}{packs}"


def _serialize_seed(seed: VectorGoldenSeedEntity) -> dict[str, Any]:
    return {
        "entity_id": seed.entity_id,
        "canonical_text": seed.canonical_text,
        "label": seed.label,
        "score": seed.score,
    }


def _serialize_snapshot(snapshot: _SavedResponseSnapshot) -> dict[str, Any]:
    return {
        "timing_ms": snapshot.timing_ms,
        "warning_count": len(snapshot.warnings),
        "warnings": list(snapshot.warnings),
        "entity_ids": list(snapshot.entity_ids),
        "related_entity_ids": list(snapshot.related_entity_ids),
        "entity_count": snapshot.entity_count,
        "related_entity_count": snapshot.related_entity_count,
        "graph_support_applied": snapshot.graph_support_applied,
    }


def _serialize_case_result(case_result: VectorSeedAuditCaseResult) -> dict[str, Any]:
    return {
        "name": case_result.name,
        "pack_id": case_result.pack_id,
        "domain_hint": case_result.domain_hint,
        "country_hint": case_result.country_hint,
        "warnings": list(case_result.warnings),
        "alias_results": [
            {
                "collection_alias": alias_result.collection_alias,
                "filter_payload": alias_result.filter_payload,
                "warnings": list(alias_result.warnings),
                "seed_results": [
                    {
                        "seed_entity_id": seed_result.seed_entity_id,
                        "seed_canonical_text": seed_result.seed_canonical_text,
                        "seed_label": seed_result.seed_label,
                        "warnings": list(seed_result.warnings),
                        "neighbors": [
                            {
                                "entity_id": neighbor.entity_id,
                                "canonical_text": neighbor.canonical_text,
                                "entity_type": neighbor.entity_type,
                                "source_name": neighbor.source_name,
                                "score": neighbor.score,
                                "packs": list(neighbor.packs),
                            }
                            for neighbor in seed_result.neighbors
                        ],
                    }
                    for seed_result in alias_result.seed_results
                ],
            }
            for alias_result in case_result.alias_results
        ],
    }


def _write_case_report(
    output_dir: Path,
    case_result: VectorSeedAuditCaseResult,
    saved_case: _SavedResponseCase,
) -> Path:
    lines = [
        f"# {saved_case.title}",
        "",
        f"- case_id: `{saved_case.case_id}`",
        f"- slug: `{saved_case.slug}`",
        f"- source_summary: `{saved_case.source_summary_path}`",
        f"- source response: `{saved_case.response_file_path}`",
        f"- pack: `{saved_case.pack_id}`",
        f"- domain_hint: `{saved_case.domain_hint or ''}`",
        f"- country_hint: `{saved_case.country_hint or ''}`",
        f"- source_name: `{saved_case.source_name or ''}`",
        f"- seed_count: `{len(saved_case.seed_entities)}`",
        f"- plain_timing_ms: `{saved_case.plain.timing_ms}`",
        f"- hinted_timing_ms: `{saved_case.hinted.timing_ms}`",
        f"- final_related_entity_count: `{saved_case.hinted.related_entity_count}`",
    ]
    if case_result.warnings:
        lines.extend(["- warnings:"] + [f"  - `{warning}`" for warning in case_result.warnings])
    lines.extend(
        [
            "",
            "## Final Output Snapshot",
            "",
            f"- plain_entity_ids: `{', '.join(saved_case.plain.entity_ids)}`",
            f"- hinted_entity_ids: `{', '.join(saved_case.hinted.entity_ids)}`",
            f"- hinted_related_entity_ids: `{', '.join(saved_case.hinted.related_entity_ids)}`",
            "",
        ]
    )
    for alias_result in case_result.alias_results:
        lines.append(f"## {alias_result.collection_alias}")
        lines.append("")
        if alias_result.filter_payload is not None:
            lines.append(f"- filter_payload: `{json.dumps(alias_result.filter_payload, sort_keys=True)}`")
        if alias_result.warnings:
            lines.extend([f"- warning: `{warning}`" for warning in alias_result.warnings])
        lines.append("")
        for seed_result in alias_result.seed_results:
            lines.append(
                f"### {seed_result.seed_canonical_text} ({seed_result.seed_entity_id})"
            )
            lines.append("")
            if seed_result.warnings:
                lines.extend([f"- warning: `{warning}`" for warning in seed_result.warnings])
            elif seed_result.neighbors:
                lines.extend(_neighbor_line(neighbor) for neighbor in seed_result.neighbors)
            else:
                lines.append("- no neighbors")
            lines.append("")
    destination = output_dir / "reports" / f"{saved_case.case_id}.md"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return destination


def _p95(values: list[int]) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return ordered[index]


def _mean(values: list[int]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _collection_aliases_of_interest(settings) -> list[str]:
    alias_names = {
        settings.vector_search_collection_alias,
        *settings.vector_search_pack_collection_aliases.values(),
    }
    return sorted(alias for alias in alias_names if alias)


def _graph_store_inventory(settings) -> dict[str, Any]:
    artifact_path = settings.graph_context_artifact_path
    inventory: dict[str, Any] = {
        "configured_path": str(artifact_path) if artifact_path is not None else None,
        "exists": False,
        "size_bytes": None,
        "manifest_path": None,
        "manifest_exists": False,
        "manifest_excerpt": None,
    }
    if artifact_path is None:
        return inventory
    resolved_path = artifact_path.expanduser().resolve()
    inventory["configured_path"] = str(resolved_path)
    inventory["exists"] = resolved_path.exists()
    if resolved_path.exists():
        inventory["size_bytes"] = resolved_path.stat().st_size
    manifest_path = resolved_path.with_name("qid_graph_store_manifest.json")
    inventory["manifest_path"] = str(manifest_path)
    inventory["manifest_exists"] = manifest_path.exists()
    if manifest_path.exists():
        manifest = _load_json(manifest_path)
        excerpt_keys = (
            "generated_at",
            "ades_version",
            "pack_ids",
            "target_node_count",
            "stored_node_count",
            "stored_edge_count",
            "matched_statement_count",
            "processed_line_count",
        )
        inventory["manifest_excerpt"] = {
            key: manifest.get(key)
            for key in excerpt_keys
            if key in manifest
        }
    return inventory


def _qdrant_inventory(settings) -> dict[str, Any]:
    inventory: dict[str, Any] = {
        "enabled": settings.vector_search_enabled,
        "url": settings.vector_search_url,
        "configured_default_alias": settings.vector_search_collection_alias,
        "aliases": [],
        "warnings": [],
    }
    if not settings.vector_search_enabled or not settings.vector_search_url:
        return inventory
    try:
        with QdrantVectorSearchClient(
            settings.vector_search_url,
            api_key=settings.vector_search_api_key,
        ) as client:
            alias_map = client.list_aliases()
            alias_names = sorted({*alias_map.keys(), *_collection_aliases_of_interest(settings)})
            for alias_name in alias_names:
                collection_name = alias_map.get(alias_name)
                entry: dict[str, Any] = {
                    "alias_name": alias_name,
                    "present": collection_name is not None,
                    "collection_name": collection_name,
                    "point_count": None,
                    "vectors_count": None,
                    "indexed_vectors_count": None,
                    "status": None,
                    "optimizer_status": None,
                    "error": None,
                }
                if collection_name is None:
                    inventory["aliases"].append(entry)
                    continue
                try:
                    collection_info = client.get_collection_info(collection_name)
                except QdrantVectorSearchError as exc:
                    entry["error"] = str(exc)
                    inventory["warnings"].append(
                        f"collection_info_failed:{alias_name}:{collection_name}:{exc}"
                    )
                    inventory["aliases"].append(entry)
                    continue
                entry["point_count"] = client.get_collection_point_count(collection_name)
                entry["vectors_count"] = collection_info.get("vectors_count")
                entry["indexed_vectors_count"] = collection_info.get("indexed_vectors_count")
                entry["status"] = collection_info.get("status")
                entry["optimizer_status"] = collection_info.get("optimizer_status")
                inventory["aliases"].append(entry)
    except QdrantVectorSearchError as exc:
        inventory["warnings"].append(f"qdrant_inventory_failed:{exc}")
    return inventory


def _installed_pack_inventory(settings) -> dict[str, Any]:
    inventory: dict[str, Any] = {
        "storage_root": str(settings.storage_root),
        "runtime_target": settings.runtime_target.value,
        "metadata_backend": settings.metadata_backend.value,
        "database_url_configured": settings.database_url is not None,
        "packs": [],
        "warnings": [],
    }
    try:
        registry = PackRegistry(
            settings.storage_root,
            runtime_target=settings.runtime_target,
            metadata_backend=settings.metadata_backend,
            database_url=settings.database_url,
        )
        packs = registry.list_installed_packs(active_only=False)
    except Exception as exc:  # pragma: no cover - exercised via script execution
        inventory["warnings"].append(f"pack_registry_failed:{type(exc).__name__}:{exc}")
        return inventory
    inventory["packs"] = [
        {
            "pack_id": pack.pack_id,
            "version": pack.version,
            "active": pack.active,
            "domain": pack.domain,
            "language": pack.language,
        }
        for pack in sorted(packs, key=lambda item: item.pack_id)
    ]
    return inventory


def _artifact_inventory(settings) -> dict[str, Any]:
    return {
        "settings": {
            "runtime_target": settings.runtime_target.value,
            "metadata_backend": settings.metadata_backend.value,
            "vector_search_enabled": settings.vector_search_enabled,
            "vector_search_url": settings.vector_search_url,
            "vector_search_collection_alias": settings.vector_search_collection_alias,
            "vector_search_related_limit": settings.vector_search_related_limit,
            "graph_context_enabled": settings.graph_context_enabled,
            "graph_context_artifact_path": (
                str(settings.graph_context_artifact_path)
                if settings.graph_context_artifact_path is not None
                else None
            ),
            "graph_context_vector_proposals_enabled": (
                settings.graph_context_vector_proposals_enabled
            ),
        },
        "graph_store": _graph_store_inventory(settings),
        "qdrant": _qdrant_inventory(settings),
        "installed_packs": _installed_pack_inventory(settings),
    }


def _label_queue_rows(
    saved_case: _SavedResponseCase,
    case_result: VectorSeedAuditCaseResult,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for alias_result in case_result.alias_results:
        for seed_result in alias_result.seed_results:
            for rank, neighbor in enumerate(seed_result.neighbors, start=1):
                rows.append(
                    {
                        "source_summary": str(saved_case.source_summary_path),
                        "case_id": saved_case.case_id,
                        "slug": saved_case.slug,
                        "title": saved_case.title,
                        "source_name": saved_case.source_name,
                        "pack_id": saved_case.pack_id,
                        "domain_hint": saved_case.domain_hint,
                        "country_hint": saved_case.country_hint,
                        "response_file_path": str(saved_case.response_file_path),
                        "collection_alias": alias_result.collection_alias,
                        "filter_payload": alias_result.filter_payload,
                        "seed_entity_id": seed_result.seed_entity_id,
                        "seed_canonical_text": seed_result.seed_canonical_text,
                        "seed_label": seed_result.seed_label,
                        "neighbor_rank": rank,
                        "neighbor_entity_id": neighbor.entity_id,
                        "neighbor_canonical_text": neighbor.canonical_text,
                        "neighbor_entity_type": neighbor.entity_type,
                        "neighbor_source_name": neighbor.source_name,
                        "neighbor_score": neighbor.score,
                        "neighbor_packs": list(neighbor.packs),
                        "final_related_entity_ids": list(saved_case.hinted.related_entity_ids),
                        "label": None,
                        "notes": "",
                    }
                )
    return rows


def _build_alias_stats(report: VectorSeedAuditReport) -> list[dict[str, Any]]:
    alias_stats: dict[str, dict[str, int]] = {}
    for case_result in report.cases:
        for alias_result in case_result.alias_results:
            stats = alias_stats.setdefault(
                alias_result.collection_alias,
                {
                    "seed_query_count": 0,
                    "seed_missing_count": 0,
                    "nonempty_neighbor_seed_count": 0,
                    "neighbor_count": 0,
                },
            )
            for seed_result in alias_result.seed_results:
                stats["seed_query_count"] += 1
                if any(warning.startswith("seed_missing:") for warning in seed_result.warnings):
                    stats["seed_missing_count"] += 1
                if seed_result.neighbors:
                    stats["nonempty_neighbor_seed_count"] += 1
                    stats["neighbor_count"] += len(seed_result.neighbors)
    return [
        {"collection_alias": alias_name, **stats}
        for alias_name, stats in sorted(alias_stats.items())
    ]


def _top_counter_items(counter: Counter[str], *, limit: int = 20) -> list[dict[str, Any]]:
    return [
        {"value": value, "count": count}
        for value, count in counter.most_common(limit)
    ]


def _seed_shape_analysis(report: VectorSeedAuditReport) -> dict[str, Any]:
    seed_query_label_counts: Counter[str] = Counter()
    neighbor_entity_type_counts: Counter[str] = Counter()
    top_seed_query_counts: Counter[str] = Counter()
    missing_seed_counts: Counter[str] = Counter()
    missing_seed_meta: dict[str, dict[str, str]] = {}

    for case_result in report.cases:
        for alias_result in case_result.alias_results:
            for seed_result in alias_result.seed_results:
                seed_label = seed_result.seed_label or "unknown"
                seed_query_label_counts[seed_label] += 1
                top_seed_query_counts[seed_result.seed_canonical_text] += 1
                for neighbor in seed_result.neighbors:
                    neighbor_entity_type_counts[neighbor.entity_type or "unknown"] += 1
                if any(warning.startswith("seed_missing:") for warning in seed_result.warnings):
                    missing_seed_counts[seed_result.seed_entity_id] += 1
                    missing_seed_meta.setdefault(
                        seed_result.seed_entity_id,
                        {
                            "canonical_text": seed_result.seed_canonical_text,
                            "label": seed_label,
                        },
                    )

    return {
        "seed_query_label_counts": _top_counter_items(seed_query_label_counts),
        "neighbor_entity_type_counts": _top_counter_items(neighbor_entity_type_counts),
        "top_seed_queries": _top_counter_items(top_seed_query_counts),
        "top_missing_seeds": [
            {
                "entity_id": entity_id,
                "canonical_text": missing_seed_meta.get(entity_id, {}).get("canonical_text", entity_id),
                "label": missing_seed_meta.get(entity_id, {}).get("label", "unknown"),
                "count": count,
            }
            for entity_id, count in missing_seed_counts.most_common(20)
        ],
    }


def _baseline_metrics(
    loaded_summaries: list[_LoadedSummary],
    saved_cases: list[_SavedResponseCase],
    report: VectorSeedAuditReport,
    *,
    label_queue_row_count: int,
) -> dict[str, Any]:
    hinted_timings = [
        timing
        for saved_case in saved_cases
        if (timing := saved_case.hinted.timing_ms) is not None
    ]
    hinted_warning_case_count = sum(1 for saved_case in saved_cases if saved_case.hinted.warnings)
    hinted_related_case_count = sum(
        1 for saved_case in saved_cases if saved_case.hinted.related_entity_ids
    )
    alias_stats = _build_alias_stats(report)
    seed_query_count = sum(item["seed_query_count"] for item in alias_stats)
    nonempty_neighbor_seed_count = sum(
        item["nonempty_neighbor_seed_count"] for item in alias_stats
    )
    return {
        "compatible_summary_count": sum(1 for item in loaded_summaries if item.compatible),
        "incompatible_summary_count": sum(1 for item in loaded_summaries if not item.compatible),
        "case_count": len(saved_cases),
        "seed_count": sum(len(saved_case.seed_entities) for saved_case in saved_cases),
        "label_queue_row_count": label_queue_row_count,
        "hinted_related_case_count": hinted_related_case_count,
        "hinted_related_entity_hit_rate": (
            hinted_related_case_count / len(saved_cases) if saved_cases else 0.0
        ),
        "hinted_warning_case_count": hinted_warning_case_count,
        "hinted_warning_case_rate": (
            hinted_warning_case_count / len(saved_cases) if saved_cases else 0.0
        ),
        "avg_hinted_timing_ms": _mean(hinted_timings),
        "p95_hinted_timing_ms": _p95(hinted_timings),
        "neighbor_seed_hit_rate": (
            nonempty_neighbor_seed_count / seed_query_count if seed_query_count else 0.0
        ),
    }


def _build_benchmark_bundle(
    *,
    loaded_summaries: list[_LoadedSummary],
    artifact_inventory: dict[str, Any],
    saved_cases: list[_SavedResponseCase],
    report: VectorSeedAuditReport,
    collection_alias_overrides: dict[str, str],
) -> dict[str, Any]:
    bundle_cases: list[dict[str, Any]] = []
    for saved_case, case_result in zip(saved_cases, report.cases, strict=True):
        bundle_cases.append(
            {
                "case_id": saved_case.case_id,
                "source_summary": str(saved_case.source_summary_path),
                "slug": saved_case.slug,
                "title": saved_case.title,
                "source_name": saved_case.source_name,
                "pack_id": saved_case.pack_id,
                "domain_hint": saved_case.domain_hint,
                "country_hint": saved_case.country_hint,
                "response_file_path": str(saved_case.response_file_path),
                "plain": _serialize_snapshot(saved_case.plain),
                "hinted": _serialize_snapshot(saved_case.hinted),
                "seed_entities": [_serialize_seed(seed) for seed in saved_case.seed_entities],
                "neighbor_audit": _serialize_case_result(case_result),
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_summaries": [
            {
                "summary_path": str(item.summary_path),
                "source_kind": item.source_kind,
                "compatible": item.compatible,
                "raw_case_count": item.raw_case_count,
                "loaded_case_count": item.loaded_case_count,
                "warnings": list(item.warnings),
            }
            for item in loaded_summaries
        ],
        "artifact_inventory": artifact_inventory,
        "provider": report.provider,
        "top_k": report.top_k,
        "collection_aliases_seen": report.collection_aliases_seen,
        "collection_alias_overrides": dict(collection_alias_overrides),
        "cases": bundle_cases,
    }


def _write_label_queue(
    output_dir: Path,
    saved_cases: list[_SavedResponseCase],
    report: VectorSeedAuditReport,
) -> Path:
    destination = output_dir / "label-queue.jsonl"
    rows: list[str] = []
    for saved_case, case_result in zip(saved_cases, report.cases, strict=True):
        for row in _label_queue_rows(saved_case, case_result):
            rows.append(json.dumps(row, sort_keys=True))
    destination.write_text("\n".join(rows) + ("\n" if rows else ""), encoding="utf-8")
    return destination


def _write_index(
    output_dir: Path,
    *,
    summary: dict[str, Any],
    report: VectorSeedAuditReport,
    saved_cases: list[_SavedResponseCase],
    report_paths: dict[str, Path],
) -> Path:
    metrics = summary["baseline_metrics"]
    artifact_inventory = summary["artifact_inventory"]
    lines = [
        "# Seed Neighbor Baseline Audit",
        "",
        f"- provider: `{report.provider}`",
        f"- case_count: `{report.case_count}`",
        f"- top_k: `{report.top_k}`",
        f"- collection_aliases_seen: `{', '.join(report.collection_aliases_seen)}`",
        f"- collection_alias_overrides: `{summary.get('collection_alias_overrides', {})}`",
        f"- compatible_summary_count: `{metrics['compatible_summary_count']}`",
        f"- incompatible_summary_count: `{metrics['incompatible_summary_count']}`",
        f"- hinted_related_entity_hit_rate: `{metrics['hinted_related_entity_hit_rate']:.4f}`",
        f"- hinted_warning_case_rate: `{metrics['hinted_warning_case_rate']:.4f}`",
        f"- neighbor_seed_hit_rate: `{metrics['neighbor_seed_hit_rate']:.4f}`",
        f"- avg_hinted_timing_ms: `{metrics['avg_hinted_timing_ms']}`",
        f"- p95_hinted_timing_ms: `{metrics['p95_hinted_timing_ms']}`",
        f"- label_queue_row_count: `{metrics['label_queue_row_count']}`",
        "",
        "## Source Summaries",
        "",
    ]
    for source_summary in summary["source_summaries"]:
        lines.append(
            "- "
            f"`{source_summary['summary_path']}` | "
            f"kind={source_summary['source_kind']} | "
            f"compatible={source_summary['compatible']} | "
            f"loaded={source_summary['loaded_case_count']}/{source_summary['raw_case_count']}"
        )
        for warning in source_summary["warnings"]:
            lines.append(f"  - `{warning}`")
    lines.extend(
        [
            "",
            "## Artifact Inventory",
            "",
            f"- graph_store_path: `{artifact_inventory['graph_store']['configured_path']}`",
            f"- graph_store_exists: `{artifact_inventory['graph_store']['exists']}`",
            f"- graph_store_manifest_exists: `{artifact_inventory['graph_store']['manifest_exists']}`",
            f"- installed_pack_count: `{len(artifact_inventory['installed_packs']['packs'])}`",
            "",
            "### Qdrant Aliases",
            "",
        ]
    )
    for alias_entry in artifact_inventory["qdrant"]["aliases"]:
        lines.append(
            "- "
            f"`{alias_entry['alias_name']}` | "
            f"present={alias_entry['present']} | "
            f"collection=`{alias_entry['collection_name']}` | "
            f"point_count={alias_entry['point_count']}"
        )
    lines.extend(
        [
            "",
            "## Alias Stats",
            "",
        ]
    )
    for item in summary.get("alias_stats", []):
        lines.append(
            "- "
            f"`{item['collection_alias']}` | "
            f"seed_queries={item['seed_query_count']} | "
            f"seed_missing={item['seed_missing_count']} | "
            f"seed_queries_with_neighbors={item['nonempty_neighbor_seed_count']} | "
            f"neighbor_count={item['neighbor_count']}"
        )
    lines.extend(
        [
            "",
            "## Cases",
            "",
        ]
    )
    for saved_case, case_result in zip(saved_cases, report.cases, strict=True):
        lines.append(
            "- "
            f"[{saved_case.case_id}]({report_paths[saved_case.case_id]}) | "
            f"slug={saved_case.slug} | "
            f"seeds={len(saved_case.seed_entities)} | "
            f"hinted_related={saved_case.hinted.related_entity_count} | "
            f"warnings={len(case_result.warnings)}"
        )
    destination = output_dir / "index.md"
    destination.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return destination


def _build_summary(
    *,
    loaded_summaries: list[_LoadedSummary],
    artifact_inventory: dict[str, Any],
    report: VectorSeedAuditReport,
    saved_cases: list[_SavedResponseCase],
    report_paths: dict[str, Path],
    benchmark_bundle_path: Path,
    label_queue_path: Path,
    collection_alias_overrides: dict[str, str],
) -> dict[str, Any]:
    alias_stats = _build_alias_stats(report)
    label_queue_row_count = 0
    if label_queue_path.exists():
        label_queue_row_count = sum(
            1 for line in label_queue_path.read_text(encoding="utf-8").splitlines() if line.strip()
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provider": report.provider,
        "case_count": report.case_count,
        "top_k": report.top_k,
        "collection_aliases_seen": report.collection_aliases_seen,
        "collection_alias_overrides": dict(collection_alias_overrides),
        "warning_count": len(report.warnings),
        "warnings": report.warnings,
        "source_summaries": [
            {
                "summary_path": str(item.summary_path),
                "source_kind": item.source_kind,
                "compatible": item.compatible,
                "raw_case_count": item.raw_case_count,
                "loaded_case_count": item.loaded_case_count,
                "warnings": list(item.warnings),
            }
            for item in loaded_summaries
        ],
        "artifact_inventory": artifact_inventory,
        "baseline_metrics": _baseline_metrics(
            loaded_summaries,
            saved_cases,
            report,
            label_queue_row_count=label_queue_row_count,
        ),
        "alias_stats": alias_stats,
        "benchmark_bundle_path": str(benchmark_bundle_path),
        "label_queue_path": str(label_queue_path),
        "seed_shape_analysis": _seed_shape_analysis(report),
        "cases": [
            {
                "case_id": saved_case.case_id,
                "slug": saved_case.slug,
                "title": saved_case.title,
                "source_summary": str(saved_case.source_summary_path),
                "pack_id": saved_case.pack_id,
                "domain_hint": saved_case.domain_hint,
                "country_hint": saved_case.country_hint,
                "seed_count": len(saved_case.seed_entities),
                "response_file_path": str(saved_case.response_file_path),
                "report_file_path": str(report_paths[saved_case.case_id]),
                "plain_timing_ms": saved_case.plain.timing_ms,
                "hinted_timing_ms": saved_case.hinted.timing_ms,
                "hinted_warning_count": len(saved_case.hinted.warnings),
                "hinted_related_count": saved_case.hinted.related_entity_count,
                "case_warning_count": len(case_result.warnings),
                "alias_count": len(case_result.alias_results),
            }
            for saved_case, case_result in zip(saved_cases, report.cases, strict=True)
        ],
    }


def main() -> int:
    args = _parse_args()
    alias_overrides = _parse_alias_overrides(args.alias_override)
    summary_paths = [Path(item).expanduser().resolve() for item in args.summary]
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else _default_output_dir(summary_paths)
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    loaded_summaries: list[_LoadedSummary] = []
    saved_cases: list[_SavedResponseCase] = []
    for summary_path in summary_paths:
        loaded_summary, cases = _load_summary_source(summary_path)
        loaded_summaries.append(loaded_summary)
        saved_cases.extend(cases)
    if args.max_cases is not None and args.max_cases > 0:
        saved_cases = saved_cases[: args.max_cases]
    if not saved_cases:
        raise SystemExit("No compatible response-bundle cases were loaded from the provided summaries.")

    settings = get_settings()
    artifact_inventory = _artifact_inventory(settings)
    audit_cases = [_case_from_saved(item, top_k=args.top_k) for item in saved_cases]
    report = audit_vector_seed_neighbors(
        audit_cases,
        settings=settings,
        top_k=args.top_k,
        collection_alias_overrides=alias_overrides or None,
    )

    audit_report_path = write_vector_seed_audit_report(output_dir / "audit-report.json", report)
    report_paths: dict[str, Path] = {}
    for saved_case, case_result in zip(saved_cases, report.cases, strict=True):
        report_paths[saved_case.case_id] = _write_case_report(output_dir, case_result, saved_case)

    benchmark_bundle = _build_benchmark_bundle(
        loaded_summaries=loaded_summaries,
        artifact_inventory=artifact_inventory,
        saved_cases=saved_cases,
        report=report,
        collection_alias_overrides=alias_overrides,
    )
    benchmark_bundle_path = output_dir / "benchmark-bundle.json"
    benchmark_bundle_path.write_text(
        json.dumps(benchmark_bundle, indent=2) + "\n",
        encoding="utf-8",
    )

    label_queue_path = _write_label_queue(output_dir, saved_cases, report)

    summary = _build_summary(
        loaded_summaries=loaded_summaries,
        artifact_inventory=artifact_inventory,
        report=report,
        saved_cases=saved_cases,
        report_paths=report_paths,
        benchmark_bundle_path=benchmark_bundle_path,
        label_queue_path=label_queue_path,
        collection_alias_overrides=alias_overrides,
    )
    summary_path_out = output_dir / "summary.json"
    summary_path_out.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    index_path = _write_index(
        output_dir,
        summary=summary,
        report=report,
        saved_cases=saved_cases,
        report_paths=report_paths,
    )

    print(
        json.dumps(
            {
                "audit_report": str(audit_report_path),
                "benchmark_bundle": str(benchmark_bundle_path),
                "label_queue": str(label_queue_path),
                "summary": str(summary_path_out),
                "index": str(index_path),
                "case_count": report.case_count,
                "compatible_summary_count": sum(
                    1 for item in loaded_summaries if item.compatible
                ),
                "incompatible_summary_count": sum(
                    1 for item in loaded_summaries if not item.compatible
                ),
                "collection_aliases_seen": report.collection_aliases_seen,
                "collection_alias_overrides": alias_overrides,
                "warning_count": len(report.warnings),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
