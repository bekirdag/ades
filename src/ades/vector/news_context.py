"""Recent-news support artifact helpers for hybrid related-entity gating."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
import json
from pathlib import Path
import re
from threading import Lock
from typing import Any, Iterable

_RECENT_NEWS_SOURCE_OUTLET_EXACT = {
    "associated press",
    "bbc",
    "guardian",
    "mailonline",
    "radio 4",
    "wikimedia commons",
}
_RECENT_NEWS_SOURCE_OUTLET_SUFFIXES = (
    " online",
    " times",
    " telegraph",
    " standard",
    " news",
)
_RECENT_NEWS_SUPPORT_WINDOWS_HOURS = (
    ("24h", 24.0),
    ("7d", 24.0 * 7.0),
    ("30d", 24.0 * 30.0),
)
_RECENT_NEWS_REJECT_STATUS_VALUES = {
    "discarded",
    "duplicate",
    "failed",
    "filtered",
    "ignored",
    "not_accepted",
    "rejected",
    "skipped",
    "tabloid",
}
_RECENT_NEWS_ACCEPT_STATUS_VALUES = {
    "accepted",
    "created",
    "ok",
    "processed",
    "published",
    "success",
}
_RECENT_NEWS_MARKET_TOPIC_KEYWORDS = {
    "bank",
    "business",
    "central bank",
    "commodity",
    "commodities",
    "company",
    "currency",
    "debt",
    "earnings",
    "economic",
    "economics",
    "economy",
    "energy",
    "finance",
    "financial",
    "fiscal",
    "geopolitical",
    "geopolitics",
    "inflation",
    "interest rate",
    "market",
    "markets",
    "monetary",
    "oil",
    "policy",
    "political",
    "politics",
    "rate",
    "rates",
    "regulation",
    "sanction",
    "security",
    "stock",
    "stocks",
    "tariff",
    "tax",
    "trade",
}
_RECENT_NEWS_REJECT_TOPIC_KEYWORDS = {
    "celebrity",
    "crime",
    "entertainment",
    "fashion",
    "film",
    "gossip",
    "health",
    "human interest",
    "lifestyle",
    "magazine",
    "movie",
    "music",
    "royal",
    "showbiz",
    "sport",
    "sports",
    "tabloid",
    "television",
    "travel",
    "weather",
}
_RECENT_NEWS_TOPIC_KEYS = (
    "category",
    "categories",
    "classification",
    "content_category",
    "domain",
    "domains",
    "primary_category",
    "section",
    "sections",
    "story_disposition",
    "subtopic",
    "subtopics",
    "topic",
    "topics",
)
_RECENT_NEWS_ENTITY_LIST_KEYS = (
    "entities",
    "linked_entities",
    "extracted_entities",
    "ades_entities",
    "passive_entities",
    "impact_entities",
    "terminal_impact_candidates",
    "entity_refs",
)
_RECENT_NEWS_ENTITY_REF_KEYS = (
    "canonical_ref",
    "entity_id",
    "entity_ref",
    "id",
    "qid",
    "ref",
)
_RECENT_NEWS_ENTITY_TEXT_KEYS = (
    "canonical_text",
    "displayName",
    "display_name",
    "label_text",
    "name",
    "text",
    "title",
)
_RECENT_NEWS_ENTITY_TYPE_KEYS = ("entity_type", "kind", "label", "type")
_RECENT_NEWS_SOURCE_KEYS = ("source_name", "pack", "source", "library")
_RECENT_NEWS_TIMESTAMP_KEYS = (
    "accepted_at",
    "created_at",
    "digested_at",
    "generated_at",
    "published_at",
    "story_created_at",
    "timestamp",
    "updated_at",
)


def is_probable_source_outlet_candidate(
    canonical_text: str,
    *,
    candidate_type: str | None,
) -> bool:
    """Return True when recent-news support should treat the entity as outlet noise."""

    if str(candidate_type or "").strip().casefold() != "organization":
        return False
    normalized = re.sub(r"\s+", " ", canonical_text.casefold()).strip()
    if not normalized:
        return False
    normalized_variants = {normalized}
    if normalized.startswith("the "):
        stripped = normalized.removeprefix("the ").strip()
        if stripped:
            normalized_variants.add(stripped)
    if any(value in _RECENT_NEWS_SOURCE_OUTLET_EXACT for value in normalized_variants):
        return True
    return any(
        value.endswith(suffix)
        for value in normalized_variants
        for suffix in _RECENT_NEWS_SOURCE_OUTLET_SUFFIXES
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalized_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _casefolded_topic(value: object) -> str:
    normalized = _normalized_text(value).casefold()
    return re.sub(r"[_\\/\-]+", " ", normalized)


def _iter_nested_text_values(value: object) -> Iterable[str]:
    if value is None:
        return
    if isinstance(value, str):
        normalized = _normalized_text(value)
        if normalized:
            yield normalized
        return
    if isinstance(value, dict):
        for nested_value in value.values():
            yield from _iter_nested_text_values(nested_value)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _iter_nested_text_values(item)
        return
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        yield str(value)


def _record_topic_texts(record: dict[str, Any]) -> list[str]:
    topic_texts: list[str] = []
    for key in _RECENT_NEWS_TOPIC_KEYS:
        if key in record:
            topic_texts.extend(_iter_nested_text_values(record.get(key)))
    return topic_texts


def _topic_contains(topic_texts: Iterable[str], keywords: set[str]) -> bool:
    for text in topic_texts:
        normalized = _casefolded_topic(text)
        if not normalized:
            continue
        if normalized in keywords:
            return True
        for keyword in keywords:
            if re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", normalized):
                return True
    return False


def _record_status_value(record: dict[str, Any]) -> str:
    for key in ("status", "decision", "disposition", "story_disposition"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().casefold()
    return ""


def _record_is_accepted(record: dict[str, Any]) -> bool:
    if record.get("accepted") is False or record.get("is_accepted") is False:
        return False
    if record.get("rejected") is True or record.get("is_rejected") is True:
        return False
    status = _record_status_value(record)
    if status in _RECENT_NEWS_REJECT_STATUS_VALUES:
        return False
    if any(keyword in status for keyword in ("reject", "discard", "filter", "skip")):
        return False
    if status and status not in _RECENT_NEWS_ACCEPT_STATUS_VALUES:
        if "accepted" not in status and "success" not in status:
            return False
    rejected_reason = record.get("rejected_reason") or record.get("rejectedReason")
    if isinstance(rejected_reason, str) and rejected_reason.strip():
        return False
    return True


def _record_market_relevance(record: dict[str, Any]) -> tuple[bool, float, list[str]]:
    topic_texts = _record_topic_texts(record)
    warnings: list[str] = []
    if _topic_contains(topic_texts, _RECENT_NEWS_REJECT_TOPIC_KEYWORDS):
        warnings.append("rejected_topic")
        return False, 0.0, warnings
    if _topic_contains(topic_texts, _RECENT_NEWS_MARKET_TOPIC_KEYWORDS):
        return True, 1.25, warnings
    return True, 1.0, warnings


def _parse_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        raw_number = float(value)
        if raw_number > 10_000_000_000:
            raw_number = raw_number / 1000.0
        try:
            parsed = datetime.fromtimestamp(raw_number, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    elif isinstance(value, str):
        raw_value = value.strip()
        if not raw_value:
            return None
        if raw_value.endswith("Z"):
            raw_value = raw_value[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw_value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _record_timestamp(record: dict[str, Any]) -> tuple[datetime | None, bool]:
    for key in _RECENT_NEWS_TIMESTAMP_KEYS:
        parsed = _parse_datetime(record.get(key))
        if parsed is not None:
            return parsed, True
    return None, False


def _source_reliability_weight(record: dict[str, Any]) -> float:
    for key in ("source_reliability", "sourceReliability", "reliability", "credibility"):
        value = record.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return min(max(float(value), 0.25), 1.5)
        if isinstance(value, str):
            normalized = value.strip().casefold()
            if normalized in {"high", "trusted", "reliable", "tier1", "tier_1"}:
                return 1.25
            if normalized in {"low", "weak", "unknown", "untrusted"}:
                return 0.75
            if normalized in {"medium", "normal", "standard"}:
                return 1.0
    return 1.0


def _story_quality_weight(record: dict[str, Any]) -> float:
    for key in ("story_quality", "storyQuality", "quality_score", "qualityScore"):
        value = record.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return min(max(float(value), 0.25), 1.5)
        if isinstance(value, str):
            normalized = value.strip().casefold()
            if normalized in {"high", "strong", "reviewed"}:
                return 1.2
            if normalized in {"low", "weak", "noisy"}:
                return 0.75
            if normalized in {"medium", "normal", "standard"}:
                return 1.0
    return 1.0


def _entity_quality_weight(entity: dict[str, str | None]) -> float:
    for key in ("alias_quality", "quality", "match_quality", "confidence_label"):
        raw_value = entity.get(key)
        if raw_value is None:
            continue
        normalized = str(raw_value).strip().casefold()
        if normalized in {"weak", "low", "ambiguous"}:
            return 0.6
        if normalized in {"strong", "exact", "high"}:
            return 1.15
    return 1.0


def _cooccurrence_weight(
    *,
    topic_relevance: float,
    source_reliability: float,
    story_quality: float,
    entity_quality: float,
    age_hours: float | None,
    has_timestamp: bool,
) -> int:
    recency_weight = 1.0
    if has_timestamp and age_hours is not None:
        if age_hours <= 24.0:
            recency_weight = 1.45
        elif age_hours <= 24.0 * 7.0:
            recency_weight = 1.2
        elif age_hours <= 24.0 * 30.0:
            recency_weight = 1.0
    raw_weight = (
        topic_relevance
        * source_reliability
        * story_quality
        * entity_quality
        * recency_weight
    )
    return max(1, min(5, round(raw_weight)))


def _wikidata_entity_id(value: object) -> str | None:
    if value is None:
        return None
    raw_value = _normalized_text(value)
    if not raw_value:
        return None
    if raw_value.startswith("wikidata:Q") and raw_value[10:].isdigit():
        return raw_value
    if raw_value.startswith("Q") and raw_value[1:].isdigit():
        return f"wikidata:{raw_value}"
    return None


def _first_string_value(raw_item: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = raw_item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _normalize_entity_item(raw_item: object) -> dict[str, str | None] | None:
    if isinstance(raw_item, str):
        entity_id = _wikidata_entity_id(raw_item)
        if entity_id is None:
            return None
        return {
            "entity_id": entity_id,
            "canonical_text": entity_id,
            "entity_type": None,
            "source_name": None,
        }
    if not isinstance(raw_item, dict):
        return None
    entity_id = None
    for key in _RECENT_NEWS_ENTITY_REF_KEYS:
        entity_id = _wikidata_entity_id(raw_item.get(key))
        if entity_id is not None:
            break
    if entity_id is None and isinstance(raw_item.get("link"), dict):
        link = raw_item["link"]
        for key in _RECENT_NEWS_ENTITY_REF_KEYS:
            entity_id = _wikidata_entity_id(link.get(key))
            if entity_id is not None:
                break
    if entity_id is None:
        return None
    canonical_text = _first_string_value(raw_item, _RECENT_NEWS_ENTITY_TEXT_KEYS)
    if canonical_text is None and isinstance(raw_item.get("link"), dict):
        canonical_text = _first_string_value(raw_item["link"], _RECENT_NEWS_ENTITY_TEXT_KEYS)
    entity_type = _first_string_value(raw_item, _RECENT_NEWS_ENTITY_TYPE_KEYS)
    if entity_type is None and isinstance(raw_item.get("link"), dict):
        entity_type = _first_string_value(raw_item["link"], _RECENT_NEWS_ENTITY_TYPE_KEYS)
    source_name = _first_string_value(raw_item, _RECENT_NEWS_SOURCE_KEYS)
    if source_name is None and isinstance(raw_item.get("link"), dict):
        source_name = _first_string_value(raw_item["link"], _RECENT_NEWS_SOURCE_KEYS)
    normalized: dict[str, str | None] = {
        "entity_id": entity_id,
        "canonical_text": canonical_text or entity_id,
        "entity_type": entity_type,
        "source_name": source_name,
    }
    for key in ("alias_quality", "quality", "match_quality", "confidence_label"):
        value = raw_item.get(key)
        if isinstance(value, str) and value.strip():
            normalized[key] = value.strip()
    return normalized


def _iter_record_entity_lists(
    record: dict[str, Any],
    *,
    entity_source: str,
) -> Iterable[object]:
    if entity_source == "flagged":
        for key in ("flagged_entities", "hinted_entities"):
            raw_entities = record.get(key)
            if isinstance(raw_entities, list):
                yield from raw_entities
        hinted = record.get("hinted")
        if isinstance(hinted, dict) and isinstance(hinted.get("entities"), list):
            yield from hinted["entities"]
    else:
        raw_entities = record.get("plain_entities")
        if isinstance(raw_entities, list):
            yield from raw_entities
        plain = record.get("plain")
        if isinstance(plain, dict) and isinstance(plain.get("entities"), list):
            yield from plain["entities"]
    for key in _RECENT_NEWS_ENTITY_LIST_KEYS:
        raw_entities = record.get(key)
        if isinstance(raw_entities, list):
            yield from raw_entities
    ades_payload = record.get("ades")
    if isinstance(ades_payload, dict):
        for key in _RECENT_NEWS_ENTITY_LIST_KEYS:
            raw_entities = ades_payload.get(key)
            if isinstance(raw_entities, list):
                yield from raw_entities


def normalize_recent_news_entity_items(
    record: dict[str, Any],
    *,
    entity_source: str = "plain",
) -> list[dict[str, str | None]]:
    """Normalize ADES/BDYA news entity shapes into canonical Wikidata refs."""

    normalized: list[dict[str, str | None]] = []
    seen_entity_ids: set[str] = set()
    for raw_item in _iter_record_entity_lists(record, entity_source=entity_source):
        entity = _normalize_entity_item(raw_item)
        if entity is None:
            continue
        entity_id = str(entity["entity_id"])
        if entity_id in seen_entity_ids:
            continue
        seen_entity_ids.add(entity_id)
        normalized.append(entity)
    return normalized


def _record_identity(record: dict[str, Any]) -> str | None:
    for key in (
        "article_url",
        "canonical_url",
        "news_file_path",
        "news_id",
        "news_path",
        "path",
        "story_id",
        "text_path",
        "url",
    ):
        value = record.get(key)
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
    title = record.get("title")
    if isinstance(title, str):
        normalized = title.strip()
        if normalized:
            return normalized
    return None


def _empty_window_payload() -> dict[str, object]:
    return {
        "article_count": 0,
        "qid_metadata": {},
        "cooccurrence": {},
        "pair_document_count": {},
    }


def _materialize_window_payload(
    *,
    article_count: int,
    document_count_by_qid: Counter[str],
    metadata_by_qid: dict[str, dict[str, str | None]],
    cooccurrence_counts: dict[str, Counter[str]],
    pair_document_counts: dict[str, Counter[str]],
    min_document_count: int,
    max_neighbors_per_qid: int,
) -> dict[str, object]:
    kept_qids = {
        entity_id
        for entity_id, count in document_count_by_qid.items()
        if count >= min_document_count
    }
    qid_metadata = {
        entity_id: {
            "canonical_text": str(
                metadata_by_qid.get(entity_id, {}).get("canonical_text") or entity_id
            ),
            "entity_type": metadata_by_qid.get(entity_id, {}).get("entity_type"),
            "source_name": metadata_by_qid.get(entity_id, {}).get("source_name"),
            "document_count": int(document_count_by_qid[entity_id]),
        }
        for entity_id in sorted(kept_qids)
    }
    cooccurrence: dict[str, dict[str, int]] = {}
    pair_document_count: dict[str, dict[str, int]] = {}
    for entity_id in sorted(kept_qids):
        neighbors = [
            (candidate_id, count)
            for candidate_id, count in cooccurrence_counts.get(entity_id, {}).items()
            if candidate_id in kept_qids and count > 0
        ]
        neighbors.sort(key=lambda item: (-item[1], item[0]))
        limited_neighbors = neighbors[:max_neighbors_per_qid]
        if limited_neighbors:
            cooccurrence[entity_id] = {
                candidate_id: int(count) for candidate_id, count in limited_neighbors
            }
        document_neighbors = [
            (candidate_id, count)
            for candidate_id, count in pair_document_counts.get(entity_id, {}).items()
            if candidate_id in kept_qids and count > 0
        ]
        document_neighbors.sort(key=lambda item: (-item[1], item[0]))
        limited_document_neighbors = document_neighbors[:max_neighbors_per_qid]
        if limited_document_neighbors:
            pair_document_count[entity_id] = {
                candidate_id: int(count)
                for candidate_id, count in limited_document_neighbors
            }
    return {
        "article_count": int(article_count),
        "qid_metadata": qid_metadata,
        "cooccurrence": cooccurrence,
        "pair_document_count": pair_document_count,
    }


def build_recent_news_support_payload(
    records: Iterable[dict[str, Any]],
    *,
    entity_source: str = "plain",
    min_document_count: int = 1,
    max_neighbors_per_qid: int = 256,
    excluded_report_sources: set[str] | None = None,
    keep_probable_source_outlets: bool = False,
    generated_at: datetime | None = None,
    source_summaries: list[str] | None = None,
    source_manifests: list[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build a recent-news support artifact from accepted market-news records."""

    if min_document_count <= 0:
        raise ValueError("min_document_count must be positive.")
    if max_neighbors_per_qid <= 0:
        raise ValueError("max_neighbors_per_qid must be positive.")
    now = generated_at or _utc_now()
    excluded_sources = {
        value.strip().casefold()
        for value in (excluded_report_sources or set())
        if value and value.strip()
    }
    metadata_by_qid: dict[str, dict[str, str | None]] = {}
    window_article_counts: dict[str, int] = {
        window_name: 0 for window_name, _hours in _RECENT_NEWS_SUPPORT_WINDOWS_HOURS
    }
    window_document_counts: dict[str, Counter[str]] = {
        window_name: Counter()
        for window_name, _hours in _RECENT_NEWS_SUPPORT_WINDOWS_HOURS
    }
    window_cooccurrence_counts: dict[str, dict[str, Counter[str]]] = {
        window_name: defaultdict(Counter)
        for window_name, _hours in _RECENT_NEWS_SUPPORT_WINDOWS_HOURS
    }
    window_pair_document_counts: dict[str, dict[str, Counter[str]]] = {
        window_name: defaultdict(Counter)
        for window_name, _hours in _RECENT_NEWS_SUPPORT_WINDOWS_HOURS
    }
    success_record_count = 0
    duplicate_record_count = 0
    filtered_probable_source_outlet_count = 0
    filtered_status_count = 0
    filtered_source_count = 0
    filtered_topic_count = 0
    filtered_empty_entity_count = 0
    seen_record_ids: set[str] = set()

    for record in records:
        if not isinstance(record, dict):
            continue
        if not _record_is_accepted(record):
            filtered_status_count += 1
            continue
        report_source = str(record.get("source") or "").strip().casefold()
        if report_source and report_source in excluded_sources:
            filtered_source_count += 1
            continue
        is_market_relevant, topic_relevance, topic_warnings = _record_market_relevance(record)
        if not is_market_relevant:
            filtered_topic_count += 1
            continue
        record_id = _record_identity(record)
        if record_id is not None:
            if record_id in seen_record_ids:
                duplicate_record_count += 1
                continue
            seen_record_ids.add(record_id)
        success_record_count += 1
        entities = normalize_recent_news_entity_items(record, entity_source=entity_source)
        if not keep_probable_source_outlets:
            filtered_entities: list[dict[str, str | None]] = []
            for entity in entities:
                canonical_text = str(entity.get("canonical_text") or "").strip()
                entity_type = str(entity.get("entity_type") or "").strip() or None
                if canonical_text and is_probable_source_outlet_candidate(
                    canonical_text,
                    candidate_type=entity_type,
                ):
                    filtered_probable_source_outlet_count += 1
                    continue
                filtered_entities.append(entity)
            entities = filtered_entities
        if len(entities) < 2:
            filtered_empty_entity_count += 1
            continue
        record_timestamp, has_timestamp = _record_timestamp(record)
        if record_timestamp is None:
            record_timestamp = now
        age_hours = max((now - record_timestamp).total_seconds() / 3600.0, 0.0)
        per_article_qids = [
            str(entity["entity_id"])
            for entity in entities
            if str(entity.get("entity_id") or "").startswith("wikidata:Q")
        ]
        if len(per_article_qids) < 2:
            filtered_empty_entity_count += 1
            continue
        per_article_entity_by_qid = {
            str(entity["entity_id"]): entity for entity in entities
        }
        for entity_id in per_article_qids:
            entity = per_article_entity_by_qid[entity_id]
            metadata = metadata_by_qid.setdefault(
                entity_id,
                {
                    "canonical_text": entity_id,
                    "entity_type": None,
                    "source_name": None,
                },
            )
            canonical_text = str(entity.get("canonical_text") or "").strip()
            entity_type = str(entity.get("entity_type") or "").strip() or None
            source_name = str(entity.get("source_name") or "").strip() or None
            if canonical_text and metadata["canonical_text"] == entity_id:
                metadata["canonical_text"] = canonical_text
            if entity_type and not metadata["entity_type"]:
                metadata["entity_type"] = entity_type
            if source_name and not metadata["source_name"]:
                metadata["source_name"] = source_name
        entity_quality = min(_entity_quality_weight(entity) for entity in entities)
        pair_weight = _cooccurrence_weight(
            topic_relevance=topic_relevance,
            source_reliability=_source_reliability_weight(record),
            story_quality=_story_quality_weight(record),
            entity_quality=entity_quality,
            age_hours=age_hours,
            has_timestamp=has_timestamp,
        )
        for window_name, max_age_hours in _RECENT_NEWS_SUPPORT_WINDOWS_HOURS:
            if age_hours > max_age_hours:
                continue
            window_article_counts[window_name] += 1
            document_counts = window_document_counts[window_name]
            cooccurrence_counts = window_cooccurrence_counts[window_name]
            pair_document_counts = window_pair_document_counts[window_name]
            for entity_id in per_article_qids:
                document_counts[entity_id] += 1
            for left_qid, right_qid in combinations(sorted(per_article_qids), 2):
                cooccurrence_counts[left_qid][right_qid] += pair_weight
                cooccurrence_counts[right_qid][left_qid] += pair_weight
                pair_document_counts[left_qid][right_qid] += 1
                pair_document_counts[right_qid][left_qid] += 1
        _ = topic_warnings

    windows: dict[str, dict[str, object]] = {}
    for window_name, _max_age_hours in _RECENT_NEWS_SUPPORT_WINDOWS_HOURS:
        if window_article_counts[window_name] <= 0:
            windows[window_name] = _empty_window_payload()
            continue
        windows[window_name] = _materialize_window_payload(
            article_count=window_article_counts[window_name],
            document_count_by_qid=window_document_counts[window_name],
            metadata_by_qid=metadata_by_qid,
            cooccurrence_counts=window_cooccurrence_counts[window_name],
            pair_document_counts=window_pair_document_counts[window_name],
            min_document_count=min_document_count,
            max_neighbors_per_qid=max_neighbors_per_qid,
        )
    default_window = windows["30d"]
    qid_metadata = default_window.get("qid_metadata")
    cooccurrence = default_window.get("cooccurrence")
    artifact_payload: dict[str, Any] = {
        "schema_version": 2,
        "generated_at": now.isoformat(),
        "article_count": int(default_window.get("article_count") or 0),
        "window_count": len(windows),
        "windows": windows,
        "entity_source": entity_source,
        "min_document_count": min_document_count,
        "max_neighbors_per_qid": max_neighbors_per_qid,
        "excluded_report_sources": sorted(excluded_sources),
        "keep_probable_source_outlets": bool(keep_probable_source_outlets),
        "source_summaries": source_summaries or [],
        "source_manifests": source_manifests or [],
        "qid_metadata": qid_metadata if isinstance(qid_metadata, dict) else {},
        "cooccurrence": cooccurrence if isinstance(cooccurrence, dict) else {},
    }
    top_entities = []
    default_metadata = artifact_payload["qid_metadata"]
    if isinstance(default_metadata, dict):
        sorted_entities = sorted(
            default_metadata.items(),
            key=lambda item: (
                -int(item[1].get("document_count") or 0)
                if isinstance(item[1], dict)
                else 0,
                item[0],
            ),
        )
        for entity_id, metadata in sorted_entities[:20]:
            if not isinstance(metadata, dict):
                continue
            top_entities.append(
                {
                    "entity_id": entity_id,
                    "canonical_text": metadata.get("canonical_text") or entity_id,
                    "document_count": int(metadata.get("document_count") or 0),
                }
            )
    top_pairs: list[dict[str, Any]] = []
    default_cooccurrence = artifact_payload["cooccurrence"]
    if isinstance(default_cooccurrence, dict):
        seen_pairs: set[tuple[str, str]] = set()
        for entity_id, neighbors in default_cooccurrence.items():
            if not isinstance(neighbors, dict):
                continue
            for candidate_id, count in neighbors.items():
                pair = tuple(sorted((str(entity_id), str(candidate_id))))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                top_pairs.append(
                    {
                        "left_entity_id": pair[0],
                        "right_entity_id": pair[1],
                        "count": int(count),
                    }
                )
    top_pairs.sort(
        key=lambda item: (-item["count"], item["left_entity_id"], item["right_entity_id"])
    )
    build_summary = {
        "entity_source": entity_source,
        "source_summary_count": len(source_summaries or []),
        "source_manifest_count": len(source_manifests or []),
        "excluded_report_sources": sorted(excluded_sources),
        "keep_probable_source_outlets": bool(keep_probable_source_outlets),
        "success_record_count": success_record_count,
        "duplicate_record_count": duplicate_record_count,
        "filtered_status_count": filtered_status_count,
        "filtered_source_count": filtered_source_count,
        "filtered_topic_count": filtered_topic_count,
        "filtered_empty_entity_count": filtered_empty_entity_count,
        "filtered_probable_source_outlet_count": filtered_probable_source_outlet_count,
        "article_count": artifact_payload["article_count"],
        "window_article_counts": dict(window_article_counts),
        "kept_qid_count": len(artifact_payload["qid_metadata"]),
        "cooccurrence_seed_count": len(artifact_payload["cooccurrence"]),
        "top_entities": top_entities,
        "top_pairs": top_pairs[:20],
    }
    return artifact_payload, build_summary


def write_recent_news_support_artifact(
    records: Iterable[dict[str, Any]],
    output_path: str | Path,
    **kwargs: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build and write a recent-news support artifact plus build summary."""

    resolved_output_path = Path(output_path).expanduser().resolve()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_payload, build_summary = build_recent_news_support_payload(
        records,
        **kwargs,
    )
    resolved_output_path.write_text(
        json.dumps(artifact_payload, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    summary_payload = {**build_summary, "output_path": str(resolved_output_path)}
    resolved_output_path.with_name("build_summary.json").write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return artifact_payload, summary_payload


@dataclass(frozen=True)
class RecentNewsEntityMetadata:
    """Compact per-QID metadata stored in the recent-news support artifact."""

    entity_id: str
    canonical_text: str
    entity_type: str | None = None
    source_name: str | None = None
    document_count: int = 0


@dataclass(frozen=True)
class RecentNewsCandidateSupport:
    """Support evidence for one candidate against the current strong seeds."""

    entity_id: str
    supporting_seed_ids: tuple[str, ...]
    pair_support_counts: dict[str, int]
    shared_seed_count: int
    total_support_count: int
    max_pair_count: int


@dataclass(frozen=True)
class _ArtifactCacheKey:
    path: str
    modified_time_ns: int


class RecentNewsSupportArtifact:
    """In-memory view over one recent-news co-occurrence artifact."""

    def __init__(
        self,
        *,
        schema_version: int,
        generated_at: str | None,
        article_count: int,
        qid_metadata: dict[str, RecentNewsEntityMetadata],
        cooccurrence: dict[str, dict[str, int]],
    ) -> None:
        self.schema_version = int(schema_version)
        self.generated_at = generated_at
        self.article_count = max(int(article_count), 0)
        self._qid_metadata = qid_metadata
        self._cooccurrence = cooccurrence

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "RecentNewsSupportArtifact":
        raw_metadata = payload.get("qid_metadata")
        metadata: dict[str, RecentNewsEntityMetadata] = {}
        if isinstance(raw_metadata, dict):
            for raw_entity_id, raw_item in raw_metadata.items():
                entity_id = str(raw_entity_id or "").strip()
                if not entity_id.startswith("wikidata:Q") or not isinstance(raw_item, dict):
                    continue
                metadata[entity_id] = RecentNewsEntityMetadata(
                    entity_id=entity_id,
                    canonical_text=str(raw_item.get("canonical_text") or entity_id).strip()
                    or entity_id,
                    entity_type=(
                        str(raw_item.get("entity_type")).strip()
                        if raw_item.get("entity_type") is not None
                        else None
                    ),
                    source_name=(
                        str(raw_item.get("source_name")).strip()
                        if raw_item.get("source_name") is not None
                        else None
                    ),
                    document_count=max(int(raw_item.get("document_count") or 0), 0),
                )

        raw_cooccurrence = payload.get("cooccurrence")
        cooccurrence: dict[str, dict[str, int]] = {}
        if isinstance(raw_cooccurrence, dict):
            for raw_seed_id, raw_neighbors in raw_cooccurrence.items():
                seed_id = str(raw_seed_id or "").strip()
                if not seed_id.startswith("wikidata:Q") or not isinstance(raw_neighbors, dict):
                    continue
                normalized_neighbors: dict[str, int] = {}
                for raw_candidate_id, raw_count in raw_neighbors.items():
                    candidate_id = str(raw_candidate_id or "").strip()
                    if not candidate_id.startswith("wikidata:Q") or candidate_id == seed_id:
                        continue
                    try:
                        count = int(raw_count)
                    except (TypeError, ValueError):
                        continue
                    if count <= 0:
                        continue
                    normalized_neighbors[candidate_id] = count
                if normalized_neighbors:
                    cooccurrence[seed_id] = normalized_neighbors

        return cls(
            schema_version=int(payload.get("schema_version") or 1),
            generated_at=(
                str(payload.get("generated_at")).strip()
                if payload.get("generated_at") is not None
                else None
            ),
            article_count=max(int(payload.get("article_count") or 0), 0),
            qid_metadata=metadata,
            cooccurrence=cooccurrence,
        )

    def metadata_for(self, entity_id: str) -> RecentNewsEntityMetadata | None:
        return self._qid_metadata.get(entity_id)

    def support_for_candidate(
        self,
        candidate_entity_id: str,
        strong_seed_entity_ids: list[str] | tuple[str, ...],
        *,
        min_supporting_seeds: int = 2,
        min_pair_count: int = 1,
    ) -> RecentNewsCandidateSupport | None:
        candidate_id = str(candidate_entity_id or "").strip()
        if not candidate_id.startswith("wikidata:Q"):
            return None
        unique_seed_ids = tuple(
            dict.fromkeys(
                seed_entity_id
                for seed_entity_id in strong_seed_entity_ids
                if isinstance(seed_entity_id, str)
                and seed_entity_id.startswith("wikidata:Q")
                and seed_entity_id != candidate_id
            )
        )
        if len(unique_seed_ids) < min_supporting_seeds:
            return None
        pair_support_counts: dict[str, int] = {}
        for seed_entity_id in unique_seed_ids:
            count = int(
                self._cooccurrence.get(seed_entity_id, {}).get(candidate_id, 0)
            )
            if count < min_pair_count:
                continue
            pair_support_counts[seed_entity_id] = count
        if len(pair_support_counts) < min_supporting_seeds:
            return None
        return RecentNewsCandidateSupport(
            entity_id=candidate_id,
            supporting_seed_ids=tuple(
                seed_entity_id
                for seed_entity_id in unique_seed_ids
                if seed_entity_id in pair_support_counts
            ),
            pair_support_counts=pair_support_counts,
            shared_seed_count=len(pair_support_counts),
            total_support_count=sum(pair_support_counts.values()),
            max_pair_count=max(pair_support_counts.values()),
        )

    def candidate_supports_for_seeds(
        self,
        strong_seed_entity_ids: list[str] | tuple[str, ...],
        *,
        min_supporting_seeds: int = 2,
        min_pair_count: int = 1,
        limit: int | None = None,
    ) -> list[RecentNewsCandidateSupport]:
        unique_seed_ids = tuple(
            dict.fromkeys(
                seed_entity_id
                for seed_entity_id in strong_seed_entity_ids
                if isinstance(seed_entity_id, str) and seed_entity_id.startswith("wikidata:Q")
            )
        )
        if len(unique_seed_ids) < min_supporting_seeds:
            return []
        candidate_pair_counts: dict[str, dict[str, int]] = {}
        for seed_entity_id in unique_seed_ids:
            for candidate_id, raw_count in self._cooccurrence.get(seed_entity_id, {}).items():
                if candidate_id == seed_entity_id:
                    continue
                count = int(raw_count)
                if count < min_pair_count:
                    continue
                candidate_pair_counts.setdefault(candidate_id, {})[seed_entity_id] = count
        supports: list[RecentNewsCandidateSupport] = []
        for candidate_id, pair_support_counts in candidate_pair_counts.items():
            if len(pair_support_counts) < min_supporting_seeds:
                continue
            supporting_seed_ids = tuple(
                seed_entity_id
                for seed_entity_id in unique_seed_ids
                if seed_entity_id in pair_support_counts
            )
            supports.append(
                RecentNewsCandidateSupport(
                    entity_id=candidate_id,
                    supporting_seed_ids=supporting_seed_ids,
                    pair_support_counts=dict(pair_support_counts),
                    shared_seed_count=len(pair_support_counts),
                    total_support_count=sum(pair_support_counts.values()),
                    max_pair_count=max(pair_support_counts.values()),
                )
            )
        supports.sort(
            key=lambda support: (
                -support.shared_seed_count,
                -support.total_support_count,
                -support.max_pair_count,
                -self.metadata_for(support.entity_id).document_count
                if self.metadata_for(support.entity_id) is not None
                else 0,
                support.entity_id,
            )
        )
        if limit is not None and limit >= 0:
            return supports[:limit]
        return supports


_ARTIFACT_CACHE: dict[_ArtifactCacheKey, RecentNewsSupportArtifact] = {}
_ARTIFACT_CACHE_LOCK = Lock()


def load_recent_news_support_artifact(
    path: str | Path,
) -> RecentNewsSupportArtifact:
    """Load one recent-news support artifact with a small mtime-based cache."""

    resolved_path = Path(path).expanduser().resolve()
    stat = resolved_path.stat()
    cache_key = _ArtifactCacheKey(
        path=str(resolved_path),
        modified_time_ns=stat.st_mtime_ns,
    )
    with _ARTIFACT_CACHE_LOCK:
        cached = _ARTIFACT_CACHE.get(cache_key)
        if cached is not None:
            return cached
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected one JSON object in {resolved_path}.")
    artifact = RecentNewsSupportArtifact.from_payload(payload)
    with _ARTIFACT_CACHE_LOCK:
        stale_keys = [
            key for key in _ARTIFACT_CACHE if key.path == str(resolved_path)
        ]
        for stale_key in stale_keys:
            del _ARTIFACT_CACHE[stale_key]
        _ARTIFACT_CACHE[cache_key] = artifact
    return artifact
