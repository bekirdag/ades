#!/usr/bin/env python3
"""Rerun saved unseen-RSS response bundles through the warm local service."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

from ades.service.client import tag_via_local_service
from ades.service.models import TagResponse

REPO_ROOT = Path(__file__).resolve().parents[1]
LIVE_REVIEW_ROOT = REPO_ROOT / "docs" / "planning" / "live-rss-vector-reviews"


@dataclass(frozen=True)
class _SavedResponseCase:
    slug: str
    title: str
    source: str
    url: str
    pack_id: str
    domain_hint: str | None
    country_hint: str | None
    response_file_path: Path
    article_path: Path
    baseline_plain: dict[str, Any]
    baseline_hinted: dict[str, Any]
    graph_context_artifact_path: Path | None = None
    news_context_artifact_path: Path | None = None


def _json_object_argument(value: str | None, *, name: str) -> dict[str, str]:
    if value is None or not value.strip():
        return {}
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise argparse.ArgumentTypeError(f"{name} must be one JSON object.")
    normalized: dict[str, str] = {}
    for key, raw_value in payload.items():
        normalized_key = str(key).strip()
        normalized_value = str(raw_value).strip()
        if not normalized_key or not normalized_value:
            continue
        normalized[normalized_key] = normalized_value
    return normalized


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rerun one saved unseen-RSS summary bundle through the warm local service.",
    )
    parser.add_argument(
        "--summary",
        required=True,
        help="Path to one saved summary.json with case response_file_path entries.",
    )
    parser.add_argument(
        "--output-dir",
        help="Optional output directory. Defaults under docs/planning/live-rss-vector-reviews/.",
    )
    parser.add_argument(
        "--pack",
        default="general-en",
        help="Pack to use when a saved case does not specify one.",
    )
    parser.add_argument(
        "--refinement-depth",
        default="deep",
        choices=("light", "deep"),
        help="Refinement depth for the local service call.",
    )
    parser.add_argument(
        "--skip-warmup",
        action="store_true",
        help="Skip the initial warmup request.",
    )
    parser.add_argument(
        "--mode",
        default="service",
        choices=("service", "in-process"),
        help="Use the warm local service or the in-process public API.",
    )
    parser.add_argument(
        "--collection-alias",
        help=(
            "Optional override for ADES_VECTOR_SEARCH_COLLECTION_ALIAS during "
            "in-process reruns."
        ),
    )
    parser.add_argument(
        "--pack-collection-aliases-json",
        help=(
            "Optional JSON object for "
            "ADES_VECTOR_SEARCH_PACK_COLLECTION_ALIASES during in-process reruns."
        ),
    )
    parser.add_argument(
        "--graph-context-artifact-path",
        help=(
            "Optional override for ADES_GRAPH_CONTEXT_ARTIFACT_PATH during "
            "in-process reruns."
        ),
    )
    parser.add_argument(
        "--news-context-artifact-path",
        help=(
            "Optional override for ADES_NEWS_CONTEXT_ARTIFACT_PATH during "
            "in-process reruns."
        ),
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected one JSON object in {path}.")
    return payload


def _normalize_optional_string(value: Any) -> str | None:
    if isinstance(value, str):
        normalized = value.strip()
        if normalized:
            return normalized
    return None


def _path_from_optional_string(value: Any) -> Path | None:
    normalized = _normalize_optional_string(value)
    if normalized is None:
        return None
    return Path(normalized).expanduser().resolve()


def _record_url(record: dict[str, Any]) -> str:
    return (
        _normalize_optional_string(record.get("url"))
        or _normalize_optional_string(record.get("article_url"))
        or ""
    )


def _resolve_direct_article_path_from_record(
    record: dict[str, Any],
    *,
    summary_path: Path,
) -> Path | None:
    for key in ("path", "news_path", "news_file_path", "text_path"):
        raw_path = record.get(key)
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        article_path = Path(raw_path).expanduser().resolve()
        if not article_path.exists():
            raise FileNotFoundError(
                f"Could not locate article text path {article_path} from {summary_path}."
            )
        return article_path
    return None


def _summary_runtime_artifact_paths(
    summary_path: Path,
    payload: dict[str, Any] | None = None,
) -> tuple[Path | None, Path | None]:
    resolved_payload = payload if payload is not None else _load_json(summary_path)
    runtime_provenance = resolved_payload.get("runtime_provenance")
    runtime_map = runtime_provenance if isinstance(runtime_provenance, dict) else {}
    graph_context_artifact_path = (
        _path_from_optional_string(resolved_payload.get("graph_context_artifact_path"))
        or _path_from_optional_string(runtime_map.get("graph_context_artifact_path"))
    )
    news_context_artifact_path = (
        _path_from_optional_string(resolved_payload.get("news_context_artifact_path"))
        or _path_from_optional_string(runtime_map.get("news_context_artifact_path"))
    )
    return graph_context_artifact_path, news_context_artifact_path


def _report_mode_payload(
    report: dict[str, Any],
    *,
    timing_keys: tuple[str, ...],
    entity_keys: tuple[str, ...],
    entity_count_keys: tuple[str, ...],
    related_keys: tuple[str, ...] = (),
    related_count_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    entities: list[dict[str, Any]] = []
    for key in entity_keys:
        value = report.get(key)
        if isinstance(value, list):
            entities = value
            break
    related_entities: list[dict[str, Any]] = []
    for key in related_keys:
        value = report.get(key)
        if isinstance(value, list):
            related_entities = value
            break
    timing_ms = 0
    for key in timing_keys:
        value = report.get(key)
        if value is not None:
            timing_ms = int(value or 0)
            break
    entity_count = len(entities)
    for key in entity_count_keys:
        value = report.get(key)
        if value is not None:
            entity_count = int(value or 0)
            break
    related_entity_count = len(related_entities)
    for key in related_count_keys:
        value = report.get(key)
        if value is not None:
            related_entity_count = int(value or 0)
            break
    return {
        "timing_ms": timing_ms,
        "warnings": [],
        "entity_count": entity_count,
        "related_entity_count": related_entity_count,
        "entities": entities,
        "related_entities": related_entities,
    }


def _saved_case_from_report_item(
    report: dict[str, Any],
    *,
    summary_path: Path,
    default_pack: str,
    domain_hint: str | None,
    country_hint: str | None,
    graph_context_artifact_path: Path | None,
    news_context_artifact_path: Path | None,
) -> _SavedResponseCase:
    article_path = _resolve_direct_article_path_from_record(
        report,
        summary_path=summary_path,
    )
    if article_path is None:
        raise FileNotFoundError(
            f"Could not locate article text path for report item from {summary_path}."
        )
    return _SavedResponseCase(
        slug=article_path.stem,
        title=_normalize_optional_string(report.get("title")) or article_path.stem,
        source=_normalize_optional_string(report.get("source")) or "",
        url=_record_url(report),
        pack_id=_normalize_optional_string(report.get("pack")) or default_pack,
        domain_hint=domain_hint,
        country_hint=country_hint,
        response_file_path=summary_path,
        article_path=article_path,
        baseline_plain=_report_mode_payload(
            report,
            timing_keys=("plain_timing_ms",),
            entity_keys=("plain_entities",),
            entity_count_keys=("plain_entity_count",),
        ),
        baseline_hinted=_report_mode_payload(
            report,
            timing_keys=("flagged_timing_ms", "hybrid_timing_ms"),
            entity_keys=("flagged_entities", "hybrid_entities"),
            entity_count_keys=("flagged_entity_count", "hybrid_entity_count"),
            related_keys=("related_entities", "hybrid_related_entities"),
            related_count_keys=("related_entity_count", "hybrid_related_count"),
        ),
        graph_context_artifact_path=graph_context_artifact_path,
        news_context_artifact_path=news_context_artifact_path,
    )


def _candidate_article_paths(
    *,
    slug: str,
    response_path: Path,
    payload: dict[str, Any],
    case_payload: dict[str, Any],
) -> list[Path]:
    candidates: list[Path] = []
    for key in ("article_path", "article_text_path", "text_path"):
        raw_path = payload.get(key)
        if isinstance(raw_path, str) and raw_path.strip():
            candidates.append(Path(raw_path).expanduser())
        raw_case_path = case_payload.get(key)
        if isinstance(raw_case_path, str) and raw_case_path.strip():
            candidates.append(Path(raw_case_path).expanduser())
    candidates.append(response_path.with_name(f"{slug}.article.txt"))
    previous_run = payload.get("previous_run")
    if isinstance(previous_run, str) and previous_run.strip():
        candidates.append(Path(previous_run).expanduser() / f"{slug}.article.txt")
    source_summary = payload.get("source_summary")
    if isinstance(source_summary, str) and source_summary.strip():
        candidates.append(
            Path(source_summary).expanduser().resolve().parent / f"{slug}.article.txt"
        )
    return candidates


def _resolve_article_path(
    *,
    slug: str,
    response_path: Path,
    payload: dict[str, Any],
    case_payload: dict[str, Any],
) -> Path:
    for candidate in _candidate_article_paths(
        slug=slug,
        response_path=response_path,
        payload=payload,
        case_payload=case_payload,
    ):
        resolved = candidate.resolve()
        if resolved.exists():
            return resolved
    global_matches = sorted(LIVE_REVIEW_ROOT.rglob(f"{slug}.article.txt"))
    if global_matches:
        return global_matches[0].resolve()
    raise FileNotFoundError(
        f"Could not locate article text for {slug} from {response_path}."
    )


def _pick_baseline_payload(payload: dict[str, Any], mode: str) -> dict[str, Any]:
    keys = (
        f"new_{mode}",
        mode,
        f"old_{mode}",
        f"previous_{mode}",
    )
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _load_saved_response_case(path: Path, *, default_pack: str) -> _SavedResponseCase:
    payload = _load_json(path)
    case_payload = payload.get("case")
    record = case_payload if isinstance(case_payload, dict) else payload
    graph_context_artifact_path = _path_from_optional_string(
        payload.get("graph_context_artifact_path")
    )
    news_context_artifact_path = _path_from_optional_string(
        payload.get("news_context_artifact_path")
    )
    if graph_context_artifact_path is None or news_context_artifact_path is None:
        source_summary_path = _path_from_optional_string(payload.get("source_summary"))
        if source_summary_path is not None and source_summary_path.exists():
            (
                inferred_graph_context_artifact_path,
                inferred_news_context_artifact_path,
            ) = _summary_runtime_artifact_paths(source_summary_path)
            if graph_context_artifact_path is None:
                graph_context_artifact_path = inferred_graph_context_artifact_path
            if news_context_artifact_path is None:
                news_context_artifact_path = inferred_news_context_artifact_path
    slug = str(record.get("slug") or path.stem.replace(".responses", "")).strip()
    if not slug:
        raise ValueError(f"Missing slug in {path}.")
    title = str(record.get("title") or slug).strip()
    source = str(record.get("source") or "").strip()
    url = str(record.get("url") or "").strip()
    pack_id = str(record.get("pack") or default_pack).strip() or default_pack
    return _SavedResponseCase(
        slug=slug,
        title=title,
        source=source,
        url=url,
        pack_id=pack_id,
        domain_hint=_normalize_optional_string(record.get("domain_hint")),
        country_hint=_normalize_optional_string(record.get("country_hint")),
        response_file_path=path,
        article_path=_resolve_article_path(
            slug=slug,
            response_path=path,
            payload=payload,
            case_payload=record,
        ),
        baseline_plain=_pick_baseline_payload(payload, "plain"),
        baseline_hinted=_pick_baseline_payload(payload, "hinted"),
        graph_context_artifact_path=graph_context_artifact_path,
        news_context_artifact_path=news_context_artifact_path,
    )


def _load_case_from_summary_selector(
    *,
    summary_path: Path,
    selector_kind: str,
    selector_value: str | int,
    default_pack: str,
    domain_hint: str | None,
    country_hint: str | None,
) -> _SavedResponseCase:
    summary = _load_json(summary_path)
    (
        graph_context_artifact_path,
        news_context_artifact_path,
    ) = _summary_runtime_artifact_paths(summary_path, summary)
    if selector_kind == "case_slug":
        raw_cases = summary.get("cases")
        if not isinstance(raw_cases, list):
            raise ValueError(f"Expected summary.cases in {summary_path}.")
        for item in raw_cases:
            if not isinstance(item, dict):
                continue
            if item.get("slug") != selector_value:
                continue
            response_file_path = item.get("response_file_path")
            if isinstance(response_file_path, str) and response_file_path.strip():
                loaded = _load_saved_response_case(
                    Path(response_file_path).expanduser().resolve(),
                    default_pack=default_pack,
                )
                return _SavedResponseCase(
                    slug=loaded.slug,
                    title=loaded.title,
                    source=loaded.source,
                    url=loaded.url,
                    pack_id=loaded.pack_id,
                    domain_hint=domain_hint if domain_hint is not None else loaded.domain_hint,
                    country_hint=(country_hint if country_hint is not None else loaded.country_hint),
                    response_file_path=loaded.response_file_path,
                    article_path=loaded.article_path,
                    baseline_plain=loaded.baseline_plain,
                    baseline_hinted=loaded.baseline_hinted,
                    graph_context_artifact_path=(
                        loaded.graph_context_artifact_path
                        or graph_context_artifact_path
                    ),
                    news_context_artifact_path=(
                        loaded.news_context_artifact_path
                        or news_context_artifact_path
                    ),
                )
            article_path = _resolve_direct_article_path_from_record(
                item,
                summary_path=summary_path,
            )
            if article_path is not None:
                plain = item.get("plain") if isinstance(item.get("plain"), dict) else {}
                hinted = item.get("hinted") if isinstance(item.get("hinted"), dict) else {}
                return _SavedResponseCase(
                    slug=_normalize_optional_string(item.get("case_id")) or article_path.stem,
                    title=(
                        _normalize_optional_string(item.get("title"))
                        or _normalize_optional_string(item.get("case_id"))
                        or article_path.stem
                    ),
                    source=_normalize_optional_string(item.get("source")) or "",
                    url=_record_url(item),
                    pack_id=_normalize_optional_string(item.get("pack")) or default_pack,
                    domain_hint=domain_hint,
                    country_hint=country_hint,
                    response_file_path=summary_path,
                    article_path=article_path,
                    baseline_plain=plain,
                    baseline_hinted=hinted,
                    graph_context_artifact_path=graph_context_artifact_path,
                    news_context_artifact_path=news_context_artifact_path,
                )
            slug = _normalize_optional_string(item.get("slug"))
            if slug is None:
                break
            case_response_path = (summary_path.parent / f"{slug}.responses.json").resolve()
            case_article_path = (summary_path.parent / f"{slug}.article.txt").resolve()
            if case_response_path.exists() and case_article_path.exists():
                loaded = _load_saved_response_case(
                    case_response_path,
                    default_pack=default_pack,
                )
                return _SavedResponseCase(
                    slug=loaded.slug,
                    title=loaded.title,
                    source=loaded.source,
                    url=loaded.url,
                    pack_id=loaded.pack_id,
                    domain_hint=domain_hint if domain_hint is not None else loaded.domain_hint,
                    country_hint=(country_hint if country_hint is not None else loaded.country_hint),
                    response_file_path=loaded.response_file_path,
                    article_path=loaded.article_path,
                    baseline_plain=loaded.baseline_plain,
                    baseline_hinted=loaded.baseline_hinted,
                    graph_context_artifact_path=(
                        loaded.graph_context_artifact_path
                        or graph_context_artifact_path
                    ),
                    news_context_artifact_path=(
                        loaded.news_context_artifact_path
                        or news_context_artifact_path
                    ),
                )
            article_path = (summary_path.parent / "news" / f"{slug}.txt").resolve()
            if not article_path.exists():
                raise FileNotFoundError(
                    f"Could not locate article text path {article_path} derived from {summary_path}."
                )
            plain = item.get("plain") if isinstance(item.get("plain"), dict) else {}
            hinted = item.get("hinted") if isinstance(item.get("hinted"), dict) else {}
            return _SavedResponseCase(
                slug=slug,
                title=_normalize_optional_string(item.get("title")) or slug,
                source=_normalize_optional_string(item.get("source")) or "",
                url=_normalize_optional_string(item.get("url")) or "",
                pack_id=_normalize_optional_string(item.get("pack")) or default_pack,
                domain_hint=domain_hint,
                country_hint=country_hint,
                response_file_path=summary_path,
                article_path=article_path,
                baseline_plain=plain,
                baseline_hinted=hinted,
            )
        raise ValueError(
            f"Could not resolve case slug {selector_value!r} from {summary_path}."
        )

    if selector_kind == "report_index":
        raw_reports = summary.get("reports")
        if not isinstance(raw_reports, list):
            raise ValueError(f"Expected summary.reports in {summary_path}.")
        for item in raw_reports:
            if not isinstance(item, dict):
                continue
            if item.get("index") != selector_value:
                continue
            return _saved_case_from_report_item(
                item,
                summary_path=summary_path,
                default_pack=default_pack,
                domain_hint=domain_hint,
                country_hint=country_hint,
                graph_context_artifact_path=graph_context_artifact_path,
                news_context_artifact_path=news_context_artifact_path,
            )
        raise ValueError(
            f"Could not resolve report index {selector_value!r} from {summary_path}."
        )

    if selector_kind == "article_index":
        raw_articles = summary.get("articles")
        if not isinstance(raw_articles, list):
            raise ValueError(f"Expected summary.articles in {summary_path}.")
        news_dirs = [summary_path.parent / "news"]
        previous_run = summary.get("previous_run")
        if isinstance(previous_run, str) and previous_run.strip():
            news_dirs.append(Path(previous_run).expanduser().resolve() / "news")
        for item in raw_articles:
            if not isinstance(item, dict):
                continue
            if item.get("index") != selector_value:
                continue
            direct_text_path = _normalize_optional_string(item.get("text_path"))
            if direct_text_path is not None:
                article_path = Path(direct_text_path).expanduser().resolve()
                if not article_path.exists():
                    raise FileNotFoundError(
                        f"Could not locate article text path {article_path} from {summary_path}."
                    )
                return _SavedResponseCase(
                    slug=article_path.stem,
                    title=_normalize_optional_string(item.get("title")) or article_path.stem,
                    source=_normalize_optional_string(item.get("source")) or "",
                    url=_normalize_optional_string(item.get("url")) or "",
                    pack_id=_normalize_optional_string(item.get("pack")) or default_pack,
                    domain_hint=domain_hint,
                    country_hint=country_hint,
                    response_file_path=summary_path,
                    article_path=article_path,
                    baseline_plain={},
                    baseline_hinted={},
                    graph_context_artifact_path=graph_context_artifact_path,
                    news_context_artifact_path=news_context_artifact_path,
                )
            stem = _normalize_optional_string(item.get("stem"))
            if stem is None:
                raise ValueError(
                    f"Article index {selector_value!r} in {summary_path} is missing stem."
                )
            article_path = None
            for news_dir in news_dirs:
                candidate = (news_dir / f"{stem}.txt").resolve()
                if candidate.exists():
                    article_path = candidate
                    break
            if article_path is None:
                raise FileNotFoundError(
                    f"Could not locate article text path for stem {stem!r} derived from {summary_path}."
                )
            return _SavedResponseCase(
                slug=stem,
                title=_normalize_optional_string(item.get("title")) or stem,
                source=_normalize_optional_string(item.get("source")) or "",
                url=_normalize_optional_string(item.get("url")) or "",
                pack_id=_normalize_optional_string(item.get("pack")) or default_pack,
                domain_hint=domain_hint,
                country_hint=country_hint,
                response_file_path=summary_path,
                article_path=article_path,
                baseline_plain={},
                baseline_hinted={},
                graph_context_artifact_path=graph_context_artifact_path,
                news_context_artifact_path=news_context_artifact_path,
            )
        raise ValueError(
            f"Could not resolve article index {selector_value!r} from {summary_path}."
        )

    raise ValueError(f"Unsupported selector kind {selector_kind!r} in {summary_path}.")


def _load_cases(summary_path: Path, *, default_pack: str) -> list[_SavedResponseCase]:
    summary = _load_json(summary_path)
    (
        graph_context_artifact_path,
        news_context_artifact_path,
    ) = _summary_runtime_artifact_paths(summary_path, summary)
    cases: list[_SavedResponseCase] = []
    raw_cases = summary.get("cases")
    if isinstance(raw_cases, list):
        for item in raw_cases:
            if not isinstance(item, dict):
                continue
            response_file_path = item.get("response_file_path")
            if not isinstance(response_file_path, str) or not response_file_path.strip():
                selector_kind = _normalize_optional_string(item.get("selector_kind"))
                selector_value = item.get("selector_value")
                selector_summary_path = item.get("summary_path")
                if (
                    selector_kind in {"case_slug", "report_index", "article_index"}
                    and isinstance(selector_summary_path, str)
                    and selector_summary_path.strip()
                ):
                    cases.append(
                        _load_case_from_summary_selector(
                            summary_path=Path(selector_summary_path).expanduser().resolve(),
                            selector_kind=selector_kind,
                            selector_value=selector_value,
                            default_pack=(
                                _normalize_optional_string(item.get("pack"))
                                or default_pack
                            ),
                            domain_hint=_normalize_optional_string(item.get("domain_hint")),
                            country_hint=_normalize_optional_string(item.get("country_hint")),
                        )
                    )
                    continue
                article_path = _resolve_direct_article_path_from_record(
                    item,
                    summary_path=summary_path,
                )
                if article_path is None:
                    continue
                case_slug = _normalize_optional_string(item.get("case_id")) or article_path.stem
                case_title = (
                    _normalize_optional_string(item.get("title"))
                    or _normalize_optional_string(item.get("case_id"))
                    or case_slug
                )
                plain = item.get("plain") if isinstance(item.get("plain"), dict) else {}
                hinted = item.get("hinted") if isinstance(item.get("hinted"), dict) else {}
                cases.append(
                    _SavedResponseCase(
                        slug=case_slug,
                        title=case_title,
                        source=_normalize_optional_string(item.get("source")) or "",
                        url=_record_url(item),
                        pack_id=_normalize_optional_string(item.get("pack")) or default_pack,
                        domain_hint=_normalize_optional_string(item.get("domain_hint")),
                        country_hint=_normalize_optional_string(item.get("country_hint")),
                        response_file_path=summary_path,
                        article_path=article_path,
                        baseline_plain=plain,
                        baseline_hinted=hinted,
                        graph_context_artifact_path=graph_context_artifact_path,
                        news_context_artifact_path=news_context_artifact_path,
                    )
                )
                continue
            cases.append(
                _load_saved_response_case(
                    Path(response_file_path).expanduser().resolve(),
                    default_pack=default_pack,
                )
            )
        return cases

    raw_reports = summary.get("reports")
    if isinstance(raw_reports, list):
        for item in raw_reports:
            if not isinstance(item, dict):
                continue
            if _resolve_direct_article_path_from_record(item, summary_path=summary_path) is None:
                continue
            cases.append(
                _saved_case_from_report_item(
                    item,
                    summary_path=summary_path,
                    default_pack=default_pack,
                    domain_hint=_normalize_optional_string(item.get("domain_hint")),
                    country_hint=_normalize_optional_string(item.get("country_hint")),
                    graph_context_artifact_path=graph_context_artifact_path,
                    news_context_artifact_path=news_context_artifact_path,
                )
            )
        return cases

    raw_articles = summary.get("articles")
    if not isinstance(raw_articles, list):
        raise ValueError(
            f"Expected summary.cases, summary.reports, or summary.articles in {summary_path}."
        )
    news_dirs = [summary_path.parent / "news"]
    previous_run = summary.get("previous_run")
    if isinstance(previous_run, str) and previous_run.strip():
        news_dirs.append(Path(previous_run).expanduser().resolve() / "news")
    for item in raw_articles:
        if not isinstance(item, dict):
            continue
        stem = _normalize_optional_string(item.get("stem"))
        if stem is None:
            continue
        article_path = None
        for news_dir in news_dirs:
            candidate = (news_dir / f"{stem}.txt").resolve()
            if candidate.exists():
                article_path = candidate
                break
        if article_path is None:
            raise FileNotFoundError(
                f"Could not locate article text path for stem {stem!r} derived from {summary_path}."
            )
        cases.append(
            _SavedResponseCase(
                slug=stem,
                title=_normalize_optional_string(item.get("title")) or stem,
                source=_normalize_optional_string(item.get("source")) or "",
                url=_normalize_optional_string(item.get("article_url")) or "",
                pack_id=_normalize_optional_string(item.get("pack")) or default_pack,
                domain_hint=_normalize_optional_string(item.get("domain_hint")),
                country_hint=_normalize_optional_string(item.get("country_hint")),
                response_file_path=summary_path,
                article_path=article_path,
                baseline_plain={},
                baseline_hinted={},
                graph_context_artifact_path=graph_context_artifact_path,
                news_context_artifact_path=news_context_artifact_path,
            )
        )
    return cases


def _default_output_dir(summary_path: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    resolved_summary = summary_path.resolve()
    if LIVE_REVIEW_ROOT in resolved_summary.parents:
        base_dir = resolved_summary.parent.parent
        label = resolved_summary.parent.name
    else:
        base_dir = LIVE_REVIEW_ROOT
        label = resolved_summary.stem
    return base_dir / f"rerun-saved-{label}-{timestamp}"


def _entity_summary(payload: dict[str, Any]) -> list[dict[str, Any]]:
    entities = payload.get("entities")
    if not isinstance(entities, list):
        return []
    rows: list[dict[str, Any]] = []
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        link = entity.get("link")
        link_dict = link if isinstance(link, dict) else {}
        rows.append(
            {
                "text": str(entity.get("text") or "").strip(),
                "label": str(entity.get("label") or "").strip(),
                "entity_id": str(link_dict.get("entity_id") or entity.get("entity_id") or "").strip(),
                "canonical_text": str(
                    link_dict.get("canonical_text")
                    or entity.get("canonical_text")
                    or entity.get("text")
                    or ""
                ).strip(),
                "score": float(entity.get("score") or 0.0),
            }
        )
    return rows


def _related_entity_summary(payload: dict[str, Any]) -> list[dict[str, Any]]:
    related = payload.get("related_entities")
    if not isinstance(related, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in related:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "entity_id": str(item.get("entity_id") or "").strip(),
                "canonical_text": str(item.get("canonical_text") or "").strip(),
                "entity_type": str(item.get("entity_type") or "").strip(),
                "score": float(item.get("score") or 0.0),
            }
        )
    return rows


def _response_summary(response: TagResponse) -> dict[str, Any]:
    payload = response.model_dump(mode="json")
    graph_support = payload.get("graph_support")
    refinement_debug = payload.get("refinement_debug")
    related_debug = (
        refinement_debug.get("related_debug")
        if isinstance(refinement_debug, dict)
        else None
    )
    related_debug_reason = (
        str(related_debug.get("reason") or "").strip()
        if isinstance(related_debug, dict)
        else ""
    )
    return {
        "timing_ms": int(payload.get("timing_ms") or 0),
        "warnings": [str(item) for item in payload.get("warnings") or []],
        "entity_count": len(payload.get("entities") or []),
        "related_entity_count": len(payload.get("related_entities") or []),
        "entities": _entity_summary(payload),
        "related_entities": _related_entity_summary(payload),
        "graph_support": graph_support if isinstance(graph_support, dict) else None,
        "refinement_applied": bool(payload.get("refinement_applied")),
        "refinement_strategy": str(payload.get("refinement_strategy") or "").strip(),
        "refinement_score": float(payload.get("refinement_score") or 0.0),
        "refinement_debug": refinement_debug if isinstance(refinement_debug, dict) else None,
        "related_debug_reason": related_debug_reason or None,
    }


def _entity_ids(summary: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for entity in summary.get("entities") or []:
        if not isinstance(entity, dict):
            continue
        entity_id = entity.get("entity_id")
        if isinstance(entity_id, str) and entity_id.strip():
            ids.add(entity_id.strip())
    return ids


def _related_entity_ids(summary: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for entity in summary.get("related_entities") or []:
        if not isinstance(entity, dict):
            continue
        entity_id = entity.get("entity_id")
        if isinstance(entity_id, str) and entity_id.strip():
            ids.add(entity_id.strip())
    return ids


def _diff_summary(*, baseline: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    baseline_entity_ids = _entity_ids(baseline)
    current_entity_ids = _entity_ids(current)
    baseline_related_ids = _related_entity_ids(baseline)
    current_related_ids = _related_entity_ids(current)
    return {
        "timing_delta_ms": int(current.get("timing_ms") or 0) - int(baseline.get("timing_ms") or 0),
        "related_delta": int(current.get("related_entity_count") or 0)
        - int(baseline.get("related_entity_count") or 0),
        "new_entity_ids": sorted(current_entity_ids - baseline_entity_ids),
        "removed_entity_ids": sorted(baseline_entity_ids - current_entity_ids),
        "new_related_entity_ids": sorted(current_related_ids - baseline_related_ids),
        "removed_related_entity_ids": sorted(baseline_related_ids - current_related_ids),
    }


def _entity_line(index: int, entity: dict[str, Any]) -> str:
    text = entity.get("text") or entity.get("canonical_text") or ""
    label = entity.get("label") or entity.get("entity_type") or "unknown"
    entity_id = entity.get("entity_id") or ""
    return f"{index}. `{text}` | `{label}` | `{entity_id}`"


def _write_case_report(output_dir: Path, record: dict[str, Any]) -> Path:
    plain = record["new_plain"]
    hinted = record["new_hinted"]
    diff = record["diff"]
    lines = [
        f"# {record['case']['title']}",
        "",
        f"- Hints: `domain_hint={record['case']['domain_hint'] or ''}` `country_hint={record['case']['country_hint'] or ''}`",
        f"- Article text: `{record['case']['article_path']}`",
        f"- Source response: `{record['case']['response_file_path']}`",
        f"- Old hinted timing: `{record['baseline_hinted'].get('timing_ms', 0)}`ms",
        f"- New hinted timing: `{hinted.get('timing_ms', 0)}`ms",
        f"- Old hinted warnings: `{record['baseline_hinted'].get('warnings', [])}`",
        f"- New hinted warnings: `{hinted.get('warnings', [])}`",
        f"- Old hinted related count: `{record['baseline_hinted'].get('related_entity_count', 0)}`",
        f"- New hinted related count: `{hinted.get('related_entity_count', 0)}`",
        f"- New hinted coherence: `{(hinted.get('graph_support') or {}).get('coherence_score')}`",
        f"- New hinted related-debug reason: `{hinted.get('related_debug_reason') or ''}`",
        f"- New hinted refinement applied: `{hinted.get('refinement_applied', False)}`",
        f"- New hinted refinement strategy: `{hinted.get('refinement_strategy') or ''}`",
        f"- New entity ids in hinted: `{diff['hinted']['new_entity_ids']}`",
        f"- Removed entity ids in hinted: `{diff['hinted']['removed_entity_ids']}`",
        f"- New related entity ids in hinted: `{diff['hinted']['new_related_entity_ids']}`",
        f"- Removed related entity ids in hinted: `{diff['hinted']['removed_related_entity_ids']}`",
        "",
        "**New Plain Entities**",
        "",
    ]
    if plain["entities"]:
        lines.extend(_entity_line(index, entity) for index, entity in enumerate(plain["entities"], start=1))
    else:
        lines.append("None")
    lines.extend(["", "**New Hinted Entities**", ""])
    if hinted["entities"]:
        lines.extend(_entity_line(index, entity) for index, entity in enumerate(hinted["entities"], start=1))
    else:
        lines.append("None")
    lines.extend(["", "**New Hinted Related Entities**", ""])
    if hinted["related_entities"]:
        lines.extend(
            _entity_line(index, entity)
            for index, entity in enumerate(hinted["related_entities"], start=1)
        )
    else:
        lines.append("None")
    lines.extend(
        [
            "",
            f"- Full JSON: `{output_dir / f'{record['case']['slug']}.responses.json'}`",
        ]
    )
    destination = output_dir / f"{record['case']['slug']}.report.md"
    destination.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return destination


def _warmup(pack: str) -> None:
    tag_via_local_service(
        "Warm service check for saved unseen RSS rerun.",
        pack=pack,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=True,
        refinement_depth="deep",
    )


@contextmanager
def _temporary_env(updates: dict[str, str]) -> Any:
    original: dict[str, str | None] = {
        key: os.environ.get(key)
        for key in updates
    }
    try:
        for key, value in updates.items():
            os.environ[key] = value
        yield
    finally:
        for key, previous in original.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


def _run_tag_request(
    article_text: str,
    *,
    saved_case: _SavedResponseCase,
    use_hints: bool,
    mode: str,
    refinement_depth: str,
    collection_alias: str | None,
    pack_collection_aliases: dict[str, str],
    graph_context_artifact_path: Path | None,
    news_context_artifact_path: Path | None,
) -> TagResponse:
    request_kwargs = {
        "pack": saved_case.pack_id,
        "include_related_entities": True,
        "include_graph_support": True,
        "refine_links": True,
        "refinement_depth": refinement_depth,
    }
    if use_hints:
        request_kwargs["domain_hint"] = saved_case.domain_hint
        request_kwargs["country_hint"] = saved_case.country_hint
    if mode == "service":
        return tag_via_local_service(article_text, **request_kwargs)

    from ades.api import tag as tag_in_process

    env_updates: dict[str, str] = {}
    if collection_alias is not None and collection_alias.strip():
        env_updates["ADES_VECTOR_SEARCH_COLLECTION_ALIAS"] = collection_alias.strip()
    if pack_collection_aliases:
        env_updates["ADES_VECTOR_SEARCH_PACK_COLLECTION_ALIASES"] = json.dumps(
            pack_collection_aliases,
            sort_keys=True,
        )
    if graph_context_artifact_path is not None:
        env_updates["ADES_GRAPH_CONTEXT_ENABLED"] = "true"
        env_updates["ADES_GRAPH_CONTEXT_ARTIFACT_PATH"] = str(
            graph_context_artifact_path
        )
    if news_context_artifact_path is not None:
        env_updates["ADES_NEWS_CONTEXT_ARTIFACT_PATH"] = str(
            news_context_artifact_path
        )
    with _temporary_env(env_updates):
        return tag_in_process(
            article_text,
            content_type="text/plain",
            hybrid=None,
            **request_kwargs,
        )


def main() -> int:
    args = _parse_args()
    summary_path = Path(args.summary).expanduser().resolve()
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else _default_output_dir(summary_path)
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    cases = _load_cases(summary_path, default_pack=args.pack)
    if not cases:
        raise ValueError(f"No saved cases found in {summary_path}.")
    if not args.skip_warmup and args.mode == "service":
        _warmup(cases[0].pack_id)
    pack_collection_aliases = _json_object_argument(
        args.pack_collection_aliases_json,
        name="--pack-collection-aliases-json",
    )
    graph_context_artifact_override = _path_from_optional_string(
        args.graph_context_artifact_path
    )
    news_context_artifact_override = _path_from_optional_string(
        args.news_context_artifact_path
    )

    summary_rows: list[dict[str, Any]] = []
    index_lines = [
        f"# rerun-saved-{summary_path.parent.name}",
        "",
        f"- source_summary: `{summary_path}`",
        f"- output_dir: `{output_dir}`",
        f"- case_count: `{len(cases)}`",
        f"- mode: `{args.mode}`",
        f"- collection_alias_override: `{args.collection_alias or ''}`",
        f"- pack_collection_alias_overrides: `{pack_collection_aliases}`",
        f"- graph_context_artifact_override: `{graph_context_artifact_override or ''}`",
        f"- news_context_artifact_override: `{news_context_artifact_override or ''}`",
        "",
    ]

    for saved_case in cases:
        article_text = saved_case.article_path.read_text(encoding="utf-8")
        new_plain = _response_summary(
            _run_tag_request(
                article_text,
                saved_case=saved_case,
                use_hints=False,
                mode=args.mode,
                refinement_depth=args.refinement_depth,
                collection_alias=args.collection_alias,
                pack_collection_aliases=pack_collection_aliases,
                graph_context_artifact_path=(
                    graph_context_artifact_override
                    or saved_case.graph_context_artifact_path
                ),
                news_context_artifact_path=(
                    news_context_artifact_override
                    or saved_case.news_context_artifact_path
                ),
            )
        )
        new_hinted = _response_summary(
            _run_tag_request(
                article_text,
                saved_case=saved_case,
                use_hints=True,
                mode=args.mode,
                refinement_depth=args.refinement_depth,
                collection_alias=args.collection_alias,
                pack_collection_aliases=pack_collection_aliases,
                graph_context_artifact_path=(
                    graph_context_artifact_override
                    or saved_case.graph_context_artifact_path
                ),
                news_context_artifact_path=(
                    news_context_artifact_override
                    or saved_case.news_context_artifact_path
                ),
            )
        )
        record = {
            "case": {
                "slug": saved_case.slug,
                "title": saved_case.title,
                "source": saved_case.source,
                "url": saved_case.url,
                "pack": saved_case.pack_id,
                "domain_hint": saved_case.domain_hint,
                "country_hint": saved_case.country_hint,
                "response_file_path": str(saved_case.response_file_path),
                "article_path": str(saved_case.article_path),
                "graph_context_artifact_path": (
                    str(saved_case.graph_context_artifact_path)
                    if saved_case.graph_context_artifact_path is not None
                    else None
                ),
                "news_context_artifact_path": (
                    str(saved_case.news_context_artifact_path)
                    if saved_case.news_context_artifact_path is not None
                    else None
                ),
            },
            "baseline_plain": saved_case.baseline_plain,
            "baseline_hinted": saved_case.baseline_hinted,
            "new_plain": new_plain,
            "new_hinted": new_hinted,
            "diff": {
                "plain": _diff_summary(
                    baseline=saved_case.baseline_plain,
                    current=new_plain,
                ),
                "hinted": _diff_summary(
                    baseline=saved_case.baseline_hinted,
                    current=new_hinted,
                ),
            },
        }
        response_path = output_dir / f"{saved_case.slug}.responses.json"
        response_path.write_text(
            json.dumps(record, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
        report_path = _write_case_report(output_dir, record)
        summary_rows.append(
            {
                "slug": saved_case.slug,
                "title": saved_case.title,
                "domain_hint": saved_case.domain_hint,
                "country_hint": saved_case.country_hint,
                "response_file_path": str(response_path),
                "report_path": str(report_path),
                "new_hinted_timing_ms": new_hinted["timing_ms"],
                "new_hinted_related_entity_count": new_hinted["related_entity_count"],
                "new_hinted_warning_count": len(new_hinted["warnings"]),
                "new_hinted_coherence_score": (
                    (new_hinted.get("graph_support") or {}).get("coherence_score")
                ),
                "new_hinted_related_debug_reason": new_hinted.get("related_debug_reason"),
                "new_hinted_refinement_applied": new_hinted.get("refinement_applied", False),
                "new_hinted_refinement_strategy": new_hinted.get("refinement_strategy") or None,
            }
        )
        index_lines.append(
            f"- [{saved_case.slug}.report.md]({report_path}) | hinted={new_hinted['timing_ms']}ms | related={new_hinted['related_entity_count']} | warnings={len(new_hinted['warnings'])} | coherence={(new_hinted.get('graph_support') or {}).get('coherence_score')} | related_reason={new_hinted.get('related_debug_reason') or ''}"
        )

    summary_payload = {
        "source_summary": str(summary_path),
        "output_dir": str(output_dir),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "case_count": len(summary_rows),
        "mode": args.mode,
        "collection_alias_override": args.collection_alias,
        "pack_collection_alias_overrides": pack_collection_aliases,
        "graph_context_artifact_override": (
            str(graph_context_artifact_override)
            if graph_context_artifact_override is not None
            else None
        ),
        "news_context_artifact_override": (
            str(news_context_artifact_override)
            if news_context_artifact_override is not None
            else None
        ),
        "graph_context_artifact_path": (
            str(graph_context_artifact_override or cases[0].graph_context_artifact_path)
            if (graph_context_artifact_override or cases[0].graph_context_artifact_path)
            is not None
            else None
        ),
        "news_context_artifact_path": (
            str(news_context_artifact_override or cases[0].news_context_artifact_path)
            if (news_context_artifact_override or cases[0].news_context_artifact_path)
            is not None
            else None
        ),
        "cases": summary_rows,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary_payload, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    (output_dir / "index.md").write_text(
        "\n".join(index_lines).rstrip() + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
