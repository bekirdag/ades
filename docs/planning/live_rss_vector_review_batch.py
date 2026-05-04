#!/usr/bin/env python3
"""Collect live RSS article text plus plain/flagged entity reports locally."""

# ruff: noqa: E402

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from datetime import datetime, timezone
import argparse
import json
import os
from pathlib import Path
import re
import sys
from typing import Any
from urllib.parse import urlsplit

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import ades
from ades.api import tag as api_tag
from ades.news_feedback import (
    DEFAULT_NEWS_FEEDBACK_ROOT,
    DEFAULT_NEWS_HTTP_HEADERS,
    DEFAULT_NEWS_HTTP_TIMEOUT_S,
    DEFAULT_LIVE_NEWS_FEEDS,
    _extract_article_text,
    _fetch_text,
    _load_feed_items,
    _merge_processed_live_news_article,
    _resolve_live_news_feeds,
    LiveNewsProcessedArticle,
    live_news_processed_articles_path,
    load_live_news_processed_articles,
    write_live_news_processed_articles,
)
from ades.packs.registry import PackRegistry
from ades.packs.runtime import PackRuntime, load_pack_runtime
from ades.service.models import TagResponse
from ades.vector import context_engine as vector_context_engine
from ades.vector.graph_store import QidGraphStore

DEFAULT_OUTPUT_PARENT = REPO_ROOT / "docs" / "planning" / "live-rss-vector-reviews"
DEFAULT_PACK_ID = "general-en"
DEFAULT_TARGET_COUNT = 1000
DEFAULT_PER_FEED_LIMIT = 80
DEFAULT_WORKERS = 4
DEFAULT_GRAPH_ARTIFACT_GLOB = "/mnt/githubActions/ades_big_data/vector_builds/*/qid_graph_store.sqlite"
DEFAULT_NEWS_CONTEXT_ARTIFACT_GLOB = (
    "/mnt/githubActions/ades_big_data/vector_builds/*/recent_news_support.json"
)

_WORKER_CLIENT: httpx.Client | None = None
_WORKER_REGISTRY: PackRegistry | None = None
_WORKER_RUNTIME: PackRuntime | None = None
_WORKER_STORAGE_ROOT: Path | None = None
_WORKER_PACK_ID: str | None = None


def _slugify(value: str, *, max_length: int = 96) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    if not cleaned:
        cleaned = "item"
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rstrip("-")
    return cleaned or "item"


def _detect_graph_artifact(explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"graph artifact not found: {path}")
        return path

    candidates = [Path(path) for path in sorted(REPO_ROOT.glob("does-not-exist"))]
    del candidates
    globbed = sorted(Path("/").glob(DEFAULT_GRAPH_ARTIFACT_GLOB.lstrip("/")))
    if not globbed:
        raise FileNotFoundError(
            "no qid_graph_store.sqlite artifacts found under /mnt/githubActions/ades_big_data/vector_builds"
        )
    return max(globbed, key=lambda path: (path.stat().st_mtime, str(path)))


def _detect_news_context_artifact(explicit: str | None) -> Path | None:
    candidate = explicit or os.environ.get("ADES_NEWS_CONTEXT_ARTIFACT_PATH")
    if candidate:
        path = Path(candidate).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"news context artifact not found: {path}")
        return path

    globbed = sorted(Path("/").glob(DEFAULT_NEWS_CONTEXT_ARTIFACT_GLOB.lstrip("/")))
    if not globbed:
        return None
    return max(globbed, key=lambda path: (path.stat().st_mtime, str(path)))


def _storage_root_from_env() -> Path:
    return Path(os.environ.get("ADES_STORAGE_ROOT", "/mnt/githubActions/ades_big_data")).expanduser().resolve()


def _ensure_dirs(root: Path) -> dict[str, Path]:
    news_dir = root / "news"
    reports_dir = root / "reports"
    news_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    return {
        "news": news_dir,
        "reports": reports_dir,
        "manifest": root / "manifest.jsonl",
        "summary": root / "summary.json",
        "progress": root / "progress.md",
        "index": root / "index.md",
        "log": root / "run.log",
    }


def _load_existing_manifest(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def _append_manifest(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _entity_summary(payload: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for entity in payload.get("entities", []):
        link = entity.get("link") or {}
        results.append(
            {
                "text": str(entity.get("text") or ""),
                "label": str(entity.get("label") or ""),
                "entity_id": str(link.get("entity_id") or ""),
                "canonical_text": str(link.get("canonical_text") or ""),
                "source_name": str(link.get("source_name") or ""),
            }
        )
    return results


def _related_entity_summary(payload: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for entity in payload.get("related_entities", []):
        results.append(
            {
                "text": str(entity.get("text") or ""),
                "label": str(entity.get("label") or ""),
                "entity_id": str(entity.get("entity_id") or ""),
                "canonical_text": str(entity.get("canonical_text") or ""),
                "shared_seed_count": int(entity.get("shared_seed_count") or 0),
                "seed_entity_ids": [str(item) for item in entity.get("seed_entity_ids") or []],
            }
        )
    return results


def _response_summary(response: TagResponse) -> dict[str, Any]:
    payload = response.model_dump(mode="json")
    return {
        "timing_ms": int(payload.get("timing_ms") or 0),
        "warnings": [str(item) for item in payload.get("warnings") or []],
        "entities": _entity_summary(payload),
        "related_entities": _related_entity_summary(payload),
        "graph_support": payload.get("graph_support"),
    }


def _entity_key(entity: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(entity.get("text") or "").strip(),
        str(entity.get("entity_id") or "").strip(),
        str(entity.get("label") or "").strip(),
    )


def _word_count(text: str) -> int:
    return len(re.findall(r"\b[\w'-]+\b", text))


def _runtime_provenance() -> dict[str, str]:
    payload = {
        "python_executable": sys.executable,
        "ades_file": str(Path(ades.__file__).resolve()),
        "api_tag_module": str(Path(sys.modules[api_tag.__module__].__file__).resolve()),
        "context_engine_file": str(Path(vector_context_engine.__file__).resolve()),
    }
    graph_artifact_path = os.environ.get("ADES_GRAPH_CONTEXT_ARTIFACT_PATH", "").strip()
    if graph_artifact_path:
        payload["graph_context_artifact_path"] = graph_artifact_path
    news_context_artifact_path = os.environ.get("ADES_NEWS_CONTEXT_ARTIFACT_PATH", "").strip()
    if news_context_artifact_path:
        payload["news_context_artifact_path"] = news_context_artifact_path
    return payload


def _worker_init(
    pack_id: str,
    storage_root: str,
    graph_artifact_path: str,
    news_context_artifact_path: str | None = None,
) -> None:
    global _WORKER_CLIENT, _WORKER_PACK_ID, _WORKER_REGISTRY, _WORKER_RUNTIME, _WORKER_STORAGE_ROOT

    os.environ["ADES_STORAGE_ROOT"] = storage_root
    os.environ["ADES_GRAPH_CONTEXT_ENABLED"] = "1"
    os.environ["ADES_GRAPH_CONTEXT_ARTIFACT_PATH"] = graph_artifact_path
    if news_context_artifact_path:
        os.environ["ADES_NEWS_CONTEXT_ARTIFACT_PATH"] = news_context_artifact_path
    else:
        os.environ.pop("ADES_NEWS_CONTEXT_ARTIFACT_PATH", None)

    _WORKER_PACK_ID = pack_id
    _WORKER_STORAGE_ROOT = Path(storage_root)
    _WORKER_CLIENT = httpx.Client(
        follow_redirects=True,
        timeout=DEFAULT_NEWS_HTTP_TIMEOUT_S,
        headers=DEFAULT_NEWS_HTTP_HEADERS,
    )
    _WORKER_REGISTRY = PackRegistry(_WORKER_STORAGE_ROOT)
    _WORKER_RUNTIME = load_pack_runtime(_WORKER_STORAGE_ROOT, pack_id, registry=_WORKER_REGISTRY)
    # Touch the graph artifact once at worker init so missing/invalid files fail early.
    QidGraphStore(Path(graph_artifact_path)).close()


def _worker_close() -> None:
    global _WORKER_CLIENT, _WORKER_PACK_ID, _WORKER_REGISTRY, _WORKER_RUNTIME, _WORKER_STORAGE_ROOT
    if _WORKER_CLIENT is not None:
        _WORKER_CLIENT.close()
    _WORKER_CLIENT = None
    _WORKER_PACK_ID = None
    _WORKER_REGISTRY = None
    _WORKER_RUNTIME = None
    _WORKER_STORAGE_ROOT = None


def _process_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    if (
        _WORKER_CLIENT is None
        or _WORKER_PACK_ID is None
        or _WORKER_REGISTRY is None
        or _WORKER_RUNTIME is None
        or _WORKER_STORAGE_ROOT is None
    ):
        raise RuntimeError("worker not initialized")

    base = {
        "source": candidate["source"],
        "title": candidate["title"],
        "article_url": candidate["article_url"],
        "published_at": candidate.get("published_at"),
    }
    try:
        html_text = _fetch_text(_WORKER_CLIENT, candidate["article_url"])
    except Exception as exc:  # pragma: no cover - exercised in live runs
        return {**base, "status": "article_fetch", "message": f"{type(exc).__name__}: {exc}"}

    text_source, text = _extract_article_text(html_text)
    text = text.strip()
    if _word_count(text) < 80:
        return {
            **base,
            "status": "article_extract",
            "message": f"insufficient_text:{_word_count(text)}",
            "text_source": text_source,
            "word_count": _word_count(text),
        }

    try:
        plain = api_tag(
            text,
            pack=_WORKER_PACK_ID,
            storage_root=_WORKER_STORAGE_ROOT,
            registry=_WORKER_REGISTRY,
            runtime=_WORKER_RUNTIME,
        )
        flagged = api_tag(
            text,
            pack=_WORKER_PACK_ID,
            storage_root=_WORKER_STORAGE_ROOT,
            include_related_entities=True,
            include_graph_support=True,
            refine_links=True,
            registry=_WORKER_REGISTRY,
            runtime=_WORKER_RUNTIME,
        )
    except Exception as exc:  # pragma: no cover - exercised in live runs
        return {**base, "status": "tag_error", "message": f"{type(exc).__name__}: {exc}"}

    return {
        **base,
        "status": "success",
        "runtime_provenance": _runtime_provenance(),
        "text_source": text_source,
        "word_count": _word_count(text),
        "text": text,
        "plain": _response_summary(plain),
        "flagged": _response_summary(flagged),
    }


def _looks_like_email(text: str) -> bool:
    return bool(re.search(r"[^@\s]+@[^@\s]+\.[^@\s]+", text))


def _looks_like_numeric_fragment(text: str) -> bool:
    normalized = text.strip().lower()
    return bool(
        re.fullmatch(r"\d+(?:st|nd|rd|th)?", normalized)
        or re.fullmatch(r"\d+(?:\s+\w+){0,2}", normalized)
        or re.fullmatch(r"(?:number\s+)?\d+", normalized)
    )


def _looks_like_generic_short_phrase(text: str) -> bool:
    normalized = re.sub(r"\s+", " ", text.strip().lower())
    generic = {
        "authority",
        "best",
        "command",
        "committee",
        "group",
        "news",
        "pm",
        "public affairs",
        "secretary",
        "task force",
        "the joint",
    }
    if normalized in generic:
        return True
    tokens = normalized.split()
    if not tokens:
        return True
    if len(tokens) == 1 and tokens[0] in {"best", "news", "pm", "secretary"}:
        return True
    return False


def _suspicious_entity(entity: dict[str, Any], *, source: str) -> bool:
    text = str(entity.get("text") or "")
    if not text.strip():
        return True
    if _looks_like_email(text) or _looks_like_numeric_fragment(text) or _looks_like_generic_short_phrase(text):
        return True
    source_tokens = {token for token in re.findall(r"[a-z]{3,}", source.lower())}
    text_tokens = {token for token in re.findall(r"[a-z]{3,}", text.lower())}
    return bool(source_tokens and text_tokens and text_tokens <= source_tokens)


def _possible_overpruned(entity: dict[str, Any]) -> bool:
    text = str(entity.get("text") or "").strip()
    label = str(entity.get("label") or "").strip().lower()
    if not text:
        return False
    if label in {"person", "organization"} and len(re.findall(r"\b[\w'-]+\b", text)) >= 2:
        return True
    return text.isupper() and 2 <= len(text) <= 8


def _entity_line(index: int, entity: dict[str, Any]) -> str:
    entity_id = entity.get("entity_id") or "unlinked"
    label = entity.get("label") or "unknown"
    return f"{index}. `{entity.get('text')}` -> `{entity_id}` ({label})"


def _related_entity_line(index: int, entity: dict[str, Any]) -> str:
    entity_id = entity.get("entity_id") or "unlinked"
    seeds = ", ".join(f"`{item}`" for item in entity.get("seed_entity_ids") or [])
    if seeds:
        return (
            f"{index}. `{entity.get('text')}` -> `{entity_id}` "
            f"(shared_seed_count={entity.get('shared_seed_count', 0)}; seeds: {seeds})"
        )
    return f"{index}. `{entity.get('text')}` -> `{entity_id}`"


def _review_lines(result: dict[str, Any], suppressed: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    plain_entities = result["plain"]["entities"]
    flagged_entities = result["flagged"]["entities"]
    related_entities = result["flagged"]["related_entities"]
    source = result["source"]

    suspicious_suppressed = [entity for entity in suppressed if _suspicious_entity(entity, source=source)]
    suspicious_survivors = [
        entity for entity in flagged_entities if _suspicious_entity(entity, source=source)
    ]
    possible_overpruned = [entity for entity in suppressed if _possible_overpruned(entity)]

    good: list[str] = []
    bad: list[str] = []

    if len(flagged_entities) < len(plain_entities):
        good.append(
            f"Flagged mode reduced extracted entities from {len(plain_entities)} to {len(flagged_entities)}."
        )
    if suspicious_suppressed:
        examples = ", ".join(f"`{entity['text']}`" for entity in suspicious_suppressed[:6])
        good.append(f"Suppressed suspicious plain-mode entities such as {examples}.")
    if related_entities:
        examples = ", ".join(f"`{entity['text']}`" for entity in related_entities[:5])
        good.append(f"Added related entities in flagged mode: {examples}.")

    if suspicious_survivors:
        examples = ", ".join(f"`{entity['text']}`" for entity in suspicious_survivors[:6])
        bad.append(f"Questionable survivors remain in flagged mode: {examples}.")
    if possible_overpruned:
        examples = ", ".join(f"`{entity['text']}`" for entity in possible_overpruned[:6])
        bad.append(f"Possible over-pruning risk on named entities: {examples}.")
    if not related_entities:
        bad.append("Flagged mode returned no related entities.")
    if len(flagged_entities) == len(plain_entities) and not suppressed:
        bad.append("Flagged mode made no extracted-entity changes.")

    if not good:
        good.append("No clear automatic quality win was detected beyond preserving the current entity set.")
    if not bad:
        bad.append("No obvious automatic regression signal was detected in this article.")
    return good, bad


def _write_article_outputs(
    *,
    result: dict[str, Any],
    index: int,
    paths: dict[str, Path],
) -> dict[str, Any]:
    slug = _slugify(f"{result['source']}-{result['title']}", max_length=80)
    stem = f"{index:04d}-{slug}"
    news_path = paths["news"] / f"{stem}.txt"
    report_path = paths["reports"] / f"{stem}.md"

    plain_entities = result["plain"]["entities"]
    flagged_entities = result["flagged"]["entities"]
    related_entities = result["flagged"]["related_entities"]

    flagged_keys = {_entity_key(entity) for entity in flagged_entities}
    suppressed = [entity for entity in plain_entities if _entity_key(entity) not in flagged_keys]

    news_path.write_text(result["text"].rstrip() + "\n", encoding="utf-8")

    good_review, bad_review = _review_lines(result, suppressed)
    lines = [
        f"# {result['title']}",
        "",
        f"- Source: `{result['source']}`",
        f"- Article URL: {result['article_url']}",
        f"- Published at: `{result.get('published_at') or 'unknown'}`",
        f"- News file name: `{news_path.name}`",
        f"- News text file: [news/{news_path.name}](../news/{news_path.name})",
        f"- Text source: `{result.get('text_source') or 'unknown'}`",
        f"- Word count: `{result.get('word_count') or 0}`",
        f"- Runtime provenance: `{json.dumps(result.get('runtime_provenance') or {}, ensure_ascii=False, sort_keys=True)}`",
        f"- Plain entity count: `{len(plain_entities)}`",
        f"- Flagged extracted entity count: `{len(flagged_entities)}`",
        f"- Flagged related entity count: `{len(related_entities)}`",
        f"- Suppressed count: `{len(suppressed)}`",
        "",
        "**Without Vector Flags**",
        "",
    ]
    if plain_entities:
        lines.extend(_entity_line(i, entity) for i, entity in enumerate(plain_entities, start=1))
    else:
        lines.append("None")

    lines.extend(["", "**With Vector Flags**", ""])
    if flagged_entities:
        lines.extend(_entity_line(i, entity) for i, entity in enumerate(flagged_entities, start=1))
    else:
        lines.append("None")

    lines.extend(["", "**Related Entities (Flagged)**", ""])
    if related_entities:
        lines.extend(_related_entity_line(i, entity) for i, entity in enumerate(related_entities, start=1))
    else:
        lines.append("None")

    lines.extend(["", "**Suppressed Ones**", ""])
    if suppressed:
        lines.extend(_entity_line(i, entity) for i, entity in enumerate(suppressed, start=1))
    else:
        lines.append("None")

    lines.extend(["", "**Review**", "", "Good:"])
    lines.extend(f"- {item}" for item in good_review)
    lines.append("Bad:")
    lines.extend(f"- {item}" for item in bad_review)
    report_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    return {
        "index": index,
        "source": result["source"],
        "title": result["title"],
        "article_url": result["article_url"],
        "published_at": result.get("published_at"),
        "status": "success",
        "runtime_provenance": result.get("runtime_provenance") or {},
        "news_file_name": news_path.name,
        "news_file_path": str(news_path),
        "report_file_name": report_path.name,
        "report_file_path": str(report_path),
        "word_count": result.get("word_count") or 0,
        "plain_entity_count": len(plain_entities),
        "flagged_entity_count": len(flagged_entities),
        "related_entity_count": len(related_entities),
        "suppressed_count": len(suppressed),
        "plain_timing_ms": result["plain"].get("timing_ms") or 0,
        "flagged_timing_ms": result["flagged"].get("timing_ms") or 0,
        "plain_entities": plain_entities,
        "flagged_entities": flagged_entities,
        "related_entities": related_entities,
        "suppressed_entities": suppressed,
    }


def _write_progress(
    *,
    path: Path,
    run_root: Path,
    target_count: int,
    success_records: list[dict[str, Any]],
    failure_counts: dict[str, int],
    candidate_count: int,
    selected_count: int,
) -> None:
    latest = success_records[-1] if success_records else None
    lines = [
        "# Live RSS Vector Review Progress",
        "",
        f"- Run root: `{run_root}`",
        f"- Target article count: `{target_count}`",
        f"- Candidate URLs discovered: `{candidate_count}`",
        f"- Candidate URLs selected: `{selected_count}`",
        f"- Successful article reports written: `{len(success_records)}`",
        f"- Failure counts: `{json.dumps(failure_counts, sort_keys=True)}`",
    ]
    if latest:
        lines.extend(
            [
                f"- Latest report: `{latest['report_file_name']}`",
                f"- Latest news file: `{latest['news_file_name']}`",
                f"- Latest article URL: {latest['article_url']}",
            ]
        )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _write_index(path: Path, success_records: list[dict[str, Any]]) -> None:
    lines = [
        "# Live RSS Vector Review Index",
        "",
        f"- Generated at: `{datetime.now(timezone.utc).isoformat()}`",
        f"- Article report count: `{len(success_records)}`",
        "",
    ]
    for record in success_records:
        report_rel = f"reports/{record['report_file_name']}"
        news_rel = f"news/{record['news_file_name']}"
        lines.extend(
            [
                f"## {record['index']:04d} {record['title']}",
                "",
                f"- Source: `{record['source']}`",
                f"- Article URL: {record['article_url']}",
                f"- News file: [{record['news_file_name']}]({news_rel})",
                f"- Report file: [{record['report_file_name']}]({report_rel})",
                f"- Plain / flagged / suppressed / related: `{record['plain_entity_count']}` / `{record['flagged_entity_count']}` / `{record['suppressed_count']}` / `{record['related_entity_count']}`",
                "",
            ]
        )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _write_summary(
    *,
    path: Path,
    run_root: Path,
    graph_artifact_path: Path,
    news_context_artifact_path: Path | None,
    target_count: int,
    feeds: int,
    per_feed_limit: int,
    candidate_count: int,
    selected_count: int,
    success_records: list[dict[str, Any]],
    failure_counts: dict[str, int],
    feed_failures: list[dict[str, str]],
) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_root": str(run_root),
        "runtime_provenance": _runtime_provenance(),
        "graph_artifact_path": str(graph_artifact_path),
        "news_context_artifact_path": (
            str(news_context_artifact_path)
            if news_context_artifact_path is not None
            else None
        ),
        "target_count": target_count,
        "feed_count": feeds,
        "per_feed_limit": per_feed_limit,
        "candidate_count": candidate_count,
        "selected_count": selected_count,
        "success_count": len(success_records),
        "failure_counts": failure_counts,
        "feed_failures": feed_failures,
        "reports": success_records,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _persist_processed_result(
    *,
    pack_id: str,
    root: Path,
    records: dict[str, LiveNewsProcessedArticle],
    result: dict[str, Any],
    processed_at: str | None = None,
) -> None:
    article_url = str(result.get("article_url") or "").strip()
    if not article_url:
        return

    timestamp = processed_at or datetime.now(timezone.utc).isoformat()
    _merge_processed_live_news_article(
        records,
        article_url=article_url,
        source=str(result.get("source") or ""),
        title=str(result.get("title") or ""),
        published_at=(
            str(result.get("published_at"))
            if result.get("published_at") is not None
            else None
        ),
        processed_at=timestamp,
        status=str(result.get("status") or "unknown"),
        message=(
            str(result.get("message"))
            if result.get("message") is not None
            else None
        ),
        increment_count=True,
    )
    destination = live_news_processed_articles_path(pack_id, root=root)
    write_live_news_processed_articles(
        destination,
        pack_id=pack_id,
        generated_at=timestamp,
        articles=list(records.values()),
    )


def _looks_like_low_value_video_candidate(*, article_url: str, title: str) -> bool:
    path = urlsplit(article_url).path.strip().lower()
    if re.search(r"(?:^|/)videos?(?:/|$)", path):
        return True
    normalized_title = re.sub(r"\s+", " ", title.strip().lower())
    if normalized_title.startswith("watch:") and "video" in path:
        return True
    return False


def _load_candidates(
    *,
    pack_id: str,
    per_feed_limit: int,
    include_seen: bool,
    limit: int | None,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    seen_urls = set()
    if not include_seen:
        seen_urls = set(load_live_news_processed_articles(pack_id, root=DEFAULT_NEWS_FEEDBACK_ROOT))

    candidates: list[dict[str, Any]] = []
    feed_failures: list[dict[str, str]] = []
    unique_urls: set[str] = set()
    feeds = _resolve_live_news_feeds(None)
    with httpx.Client(
        follow_redirects=True,
        timeout=DEFAULT_NEWS_HTTP_TIMEOUT_S,
        headers=DEFAULT_NEWS_HTTP_HEADERS,
    ) as client:
        for feed in feeds:
            try:
                items = _load_feed_items(client, feed, per_feed_limit=per_feed_limit)
            except Exception as exc:
                feed_failures.append(
                    {
                        "source": feed.source,
                        "feed_url": feed.feed_url,
                        "message": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            for item in items:
                if item.article_url in unique_urls:
                    continue
                unique_urls.add(item.article_url)
                if item.article_url in seen_urls:
                    continue
                if _looks_like_low_value_video_candidate(
                    article_url=item.article_url,
                    title=item.title,
                ):
                    continue
                candidates.append(
                    {
                        "source": item.source,
                        "title": item.title,
                        "article_url": item.article_url,
                        "published_at": item.published_at,
                    }
                )
                if limit is not None and len(candidates) >= limit:
                    return candidates, feed_failures
    return candidates, feed_failures


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pack", default=DEFAULT_PACK_ID)
    parser.add_argument("--target-count", type=int, default=DEFAULT_TARGET_COUNT)
    parser.add_argument("--per-feed-limit", type=int, default=DEFAULT_PER_FEED_LIMIT)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--graph-artifact-path")
    parser.add_argument("--news-context-artifact-path")
    parser.add_argument("--output-root")
    parser.add_argument("--include-seen", action="store_true")
    parser.add_argument("--candidate-limit", type=int)
    return parser


def main() -> int:
    args = _build_arg_parser().parse_args()

    output_root = (
        Path(args.output_root).expanduser().resolve()
        if args.output_root
        else DEFAULT_OUTPUT_PARENT / datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ")
    )
    graph_artifact_path = _detect_graph_artifact(args.graph_artifact_path)
    news_context_artifact_path = _detect_news_context_artifact(args.news_context_artifact_path)
    storage_root = _storage_root_from_env()
    paths = _ensure_dirs(output_root)

    existing_records = _load_existing_manifest(paths["manifest"])
    success_records = [record for record in existing_records if record.get("status") == "success"]
    failure_counts: dict[str, int] = {}
    seen_run_urls = {str(record.get("article_url") or "") for record in existing_records}
    processed_records = load_live_news_processed_articles(
        args.pack,
        root=DEFAULT_NEWS_FEEDBACK_ROOT,
    )
    for record in existing_records:
        if record.get("status") != "success":
            status = str(record.get("status") or "unknown")
            failure_counts[status] = failure_counts.get(status, 0) + 1

    candidates, feed_failures = _load_candidates(
        pack_id=args.pack,
        per_feed_limit=args.per_feed_limit,
        include_seen=args.include_seen,
        limit=args.candidate_limit,
    )
    candidates = [candidate for candidate in candidates if candidate["article_url"] not in seen_run_urls]
    selected_candidates = candidates[: max(args.target_count * 2, args.target_count)]

    _write_progress(
        path=paths["progress"],
        run_root=output_root,
        target_count=args.target_count,
        success_records=success_records,
        failure_counts=failure_counts,
        candidate_count=len(candidates),
        selected_count=len(selected_candidates),
    )

    next_index = len(success_records) + 1
    if len(success_records) >= args.target_count:
        _write_index(paths["index"], success_records[: args.target_count])
        _write_summary(
            path=paths["summary"],
            run_root=output_root,
            graph_artifact_path=graph_artifact_path,
            news_context_artifact_path=news_context_artifact_path,
            target_count=args.target_count,
            feeds=len(DEFAULT_LIVE_NEWS_FEEDS),
            per_feed_limit=args.per_feed_limit,
            candidate_count=len(candidates),
            selected_count=len(selected_candidates),
            success_records=success_records[: args.target_count],
            failure_counts=failure_counts,
            feed_failures=feed_failures,
        )
        return 0

    def _handle_result(result: dict[str, Any]) -> None:
        nonlocal next_index
        if result.get("status") == "success":
            record = _write_article_outputs(
                result=result,
                index=next_index,
                paths=paths,
            )
            success_records.append(record)
            _append_manifest(paths["manifest"], record)
            next_index += 1
        else:
            status = str(result.get("status") or "unknown")
            failure_counts[status] = failure_counts.get(status, 0) + 1
            _append_manifest(paths["manifest"], result)

        _persist_processed_result(
            pack_id=args.pack,
            root=DEFAULT_NEWS_FEEDBACK_ROOT,
            records=processed_records,
            result=result,
        )

        if len(success_records) % 25 == 0 or result.get("status") != "success":
            _write_progress(
                path=paths["progress"],
                run_root=output_root,
                target_count=args.target_count,
                success_records=success_records,
                failure_counts=failure_counts,
                candidate_count=len(candidates),
                selected_count=len(selected_candidates),
            )

    if args.workers <= 1:
        _worker_init(
            args.pack,
            str(storage_root),
            str(graph_artifact_path),
            str(news_context_artifact_path) if news_context_artifact_path is not None else None,
        )
        try:
            for candidate in selected_candidates:
                if len(success_records) >= args.target_count:
                    break
                try:
                    result = _process_candidate(candidate)
                except Exception as exc:  # pragma: no cover - live fallback
                    result = {
                        **candidate,
                        "status": "worker_error",
                        "message": f"{type(exc).__name__}: {exc}",
                        "runtime_provenance": _runtime_provenance(),
                    }
                _handle_result(result)
        finally:
            _worker_close()
    else:
        pending: dict[Any, dict[str, Any]] = {}
        candidate_iter = iter(selected_candidates)
        with ProcessPoolExecutor(
            max_workers=max(args.workers, 1),
            initializer=_worker_init,
            initargs=(
                args.pack,
                str(storage_root),
                str(graph_artifact_path),
                str(news_context_artifact_path) if news_context_artifact_path is not None else None,
            ),
        ) as executor:
            for _ in range(min(args.workers * 3, len(selected_candidates))):
                try:
                    candidate = next(candidate_iter)
                except StopIteration:
                    break
                pending[executor.submit(_process_candidate, candidate)] = candidate

            while pending and len(success_records) < args.target_count:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    candidate = pending.pop(future)
                    try:
                        result = future.result()
                    except Exception as exc:  # pragma: no cover - live fallback
                        result = {
                            **candidate,
                            "status": "worker_error",
                            "message": f"{type(exc).__name__}: {exc}",
                        }

                    _handle_result(result)

                    if len(success_records) >= args.target_count:
                        break

                    try:
                        next_candidate = next(candidate_iter)
                    except StopIteration:
                        continue
                    pending[executor.submit(_process_candidate, next_candidate)] = next_candidate

            for future in pending:
                future.cancel()

    success_records = success_records[: args.target_count]
    _write_progress(
        path=paths["progress"],
        run_root=output_root,
        target_count=args.target_count,
        success_records=success_records,
        failure_counts=failure_counts,
        candidate_count=len(candidates),
        selected_count=len(selected_candidates),
    )
    _write_index(paths["index"], success_records)
    _write_summary(
        path=paths["summary"],
        run_root=output_root,
        graph_artifact_path=graph_artifact_path,
        news_context_artifact_path=news_context_artifact_path,
        target_count=args.target_count,
        feeds=len(DEFAULT_LIVE_NEWS_FEEDS),
        per_feed_limit=args.per_feed_limit,
        candidate_count=len(candidates),
        selected_count=len(selected_candidates),
        success_records=success_records,
        failure_counts=failure_counts,
        feed_failures=feed_failures,
    )

    print(
        json.dumps(
            {
                "run_root": str(output_root),
                "graph_artifact_path": str(graph_artifact_path),
                "news_context_artifact_path": (
                    str(news_context_artifact_path)
                    if news_context_artifact_path is not None
                    else None
                ),
                "success_count": len(success_records),
                "target_count": args.target_count,
                "candidate_count": len(candidates),
                "selected_count": len(selected_candidates),
                "failure_counts": failure_counts,
            },
            indent=2,
        )
    )
    return 0 if len(success_records) >= args.target_count else 1


if __name__ == "__main__":
    raise SystemExit(main())
