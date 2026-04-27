"""Hosted post-link enrichment and graph-aware refinement."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from statistics import mean

from ..config import Settings
from ..service.models import (
    GraphSupport,
    RelatedEntityMatch,
    TagResponse,
    _wordfreq_zipf_en,
)
from ..storage import RuntimeTarget
from .builder import (
    DEFAULT_QID_GRAPH_ALLOWED_PREDICATES,
    DEFAULT_QID_GRAPH_DIMENSIONS,
    build_dense_vector_from_graph_store,
)
from .context_engine import _is_graph_seed_eligible, apply_graph_context
from .graph_store import QidGraphStore
from .news_context import (
    is_probable_source_outlet_candidate,
    load_recent_news_support_artifact,
)
from .qdrant import QdrantNearestPoint, QdrantVectorSearchClient, QdrantVectorSearchError

_PROVIDER_NAME = "qdrant.qid_graph"
_REFINEMENT_STRATEGY = "qid_graph_coherence_v1"
_HYBRID_VECTOR_PROPOSAL_MIN_COHERENCE = 0.12
_HYBRID_VECTOR_ORG_ONLY_NO_DIRECT_MIN_SHARED_CONTEXT = 3
_ROUTED_VECTOR_PRIMARY_SEED_LABELS = frozenset({"organization", "person"})
_ROUTED_VECTOR_SECONDARY_SEED_LABELS = frozenset({"product"})
_SHARED_VECTOR_LOCATION_FALLBACK_LIMIT = 2
_RECENT_NEWS_PERSON_ONLY_MIN_TOTAL_SUPPORT = 4
_RECENT_NEWS_DIRECT_PROPOSAL_MAX_SCORE = 0.74
_RECENT_NEWS_DIRECT_LOW_COHERENCE_MIN_SHARED_SEEDS = 3
_RECENT_NEWS_DIRECT_LOW_COHERENCE_MIN_ANCHOR_PAIR_COUNT = 3
_RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MIN_SHARED_SEEDS = 2
_RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MIN_TOTAL_SUPPORT = 6
_RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MIN_MAX_PAIR_COUNT = 4
_RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MIN_DOCUMENT_COUNT = 20
_RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MAX_COHERENCE = 0.02
_RECENT_NEWS_DIRECT_LOW_COHERENCE_PERSON_NON_ANCHOR_MIN_DOCUMENT_COUNT = 5
_RECENT_NEWS_DIRECT_LOW_COHERENCE_PERSON_NON_ANCHOR_MAX_COHERENCE = 0.06
_RECENT_NEWS_DIRECT_PERSON_ONLY_MAX_COHERENCE = 0.08
_RECENT_NEWS_DIRECT_EXTRA_PERSON_SEED_MIN_RELEVANCE = 0.6
_RECENT_NEWS_DIRECT_EXTRA_PERSON_LIMIT = 1
_RECENT_NEWS_DIRECT_EXTRA_NON_LOCATION_SEED_MIN_RELEVANCE = 0.58
_RECENT_NEWS_DIRECT_EXTRA_LOCATION_SEED_MIN_RELEVANCE = 0.7
_RECENT_NEWS_DIRECT_EXTRA_NON_LOCATION_LIMIT = 8
_RECENT_NEWS_DIRECT_EXTRA_LOCATION_LIMIT = 1
_RECENT_NEWS_PERSON_ONLY_GRAPH_ALLOWED_PREDICATES = ("P102",)
_RECENT_NEWS_PERSON_ONLY_GRAPH_SINGLE_SEED_MAX_CONTEXT_SEEDS = 3
_RECENT_NEWS_DIRECT_LOCATION_AUGMENTED_DOMAINS = {"business", "economics", "finance"}
_RECENT_NEWS_GENERIC_ORG_SEED_MAX_TOKEN_COUNT = 3
_RECENT_NEWS_GENERIC_ORG_SEED_MIN_MEAN_ZIPF = 4.9
_LOW_CONFIDENCE_THRESHOLD_BY_DEPTH = {
    "light": 0.65,
    "deep": 0.8,
}
_DOWNGRADE_THRESHOLD = 0.45
_MIN_REFINEMENT_DELTA = 0.02


def _normalized_news_context_domain_hint(domain_hint: str | None) -> str | None:
    normalized = str(domain_hint or "").strip().casefold()
    return normalized or None


@dataclass
class _CandidateAggregate:
    entity_id: str
    canonical_text: str
    provider: str
    entity_type: str | None = None
    source_name: str | None = None
    packs: set[str] = field(default_factory=set)
    seed_entity_ids: set[str] = field(default_factory=set)
    scores: list[float] = field(default_factory=list)


@dataclass
class _SeedGraphSupport:
    supporting_seed_ids: set[str] = field(default_factory=set)
    scores: list[float] = field(default_factory=list)


def _linked_seed_entity_ids(response: TagResponse) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    for entity in response.entities:
        if not _is_graph_seed_eligible(entity):
            continue
        entity_id = entity.link.entity_id.strip() if entity.link is not None else ""
        if not entity_id.startswith("wikidata:Q"):
            continue
        if entity_id in seen:
            continue
        seen.add(entity_id)
        values.append(entity_id)
    return values


def _linked_seed_entity_set(response: TagResponse) -> set[str]:
    return set(_linked_seed_entity_ids(response))


def _seed_entity_label(entity) -> str:
    return str(entity.label or "").strip().casefold()


def _is_recent_news_generic_org_seed(entity) -> bool:
    if _seed_entity_label(entity) != "organization":
        return False
    canonical_text = str(
        entity.link.canonical_text if entity.link is not None else entity.text
    ).strip()
    if not canonical_text or canonical_text.casefold() != canonical_text:
        return False
    tokens = re.findall(r"[A-Za-z]+", canonical_text.casefold())
    if not tokens or len(tokens) > _RECENT_NEWS_GENERIC_ORG_SEED_MAX_TOKEN_COUNT:
        return False
    mean_zipf = mean(_wordfreq_zipf_en(token) for token in tokens)
    return mean_zipf >= _RECENT_NEWS_GENERIC_ORG_SEED_MIN_MEAN_ZIPF


def _recent_news_low_coherence_has_anchor_support(
    support,
    *,
    seed_entity_by_id: dict[str, object],
) -> bool:
    for seed_entity_id in support.supporting_seed_ids:
        seed_entity = seed_entity_by_id.get(seed_entity_id)
        if seed_entity is None:
            continue
        if _seed_entity_label(seed_entity) in {"person", "location"}:
            continue
        if _is_recent_news_generic_org_seed(seed_entity):
            continue
        if (
            int(support.pair_support_counts.get(seed_entity_id, 0))
            >= _RECENT_NEWS_DIRECT_LOW_COHERENCE_MIN_ANCHOR_PAIR_COUNT
        ):
            return True
    return False


def _recent_news_low_coherence_has_person_support(
    support,
    *,
    seed_entity_by_id: dict[str, object],
) -> bool:
    for seed_entity_id in support.supporting_seed_ids:
        seed_entity = seed_entity_by_id.get(seed_entity_id)
        if seed_entity is None:
            continue
        if _seed_entity_label(seed_entity) == "person":
            return True
    return False


def _recent_news_low_coherence_min_shared_seeds(
    *,
    candidate_type: str,
    settings: Settings,
    has_anchor_support: bool,
) -> int:
    if candidate_type == "organization" and has_anchor_support:
        return max(2, settings.news_context_min_supporting_seeds)
    return _RECENT_NEWS_DIRECT_LOW_COHERENCE_MIN_SHARED_SEEDS


def _recent_news_low_coherence_allows_strong_non_anchor_org(
    support,
    *,
    candidate_type: str,
    document_count: int,
    coherence_score: float | None,
    has_person_support: bool,
) -> bool:
    max_coherence = (
        _RECENT_NEWS_DIRECT_LOW_COHERENCE_PERSON_NON_ANCHOR_MAX_COHERENCE
        if has_person_support
        else _RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MAX_COHERENCE
    )
    min_document_count = (
        _RECENT_NEWS_DIRECT_LOW_COHERENCE_PERSON_NON_ANCHOR_MIN_DOCUMENT_COUNT
        if has_person_support
        else _RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MIN_DOCUMENT_COUNT
    )
    return (
        coherence_score is not None
        and coherence_score <= max_coherence
        and
        candidate_type == "organization"
        and support.shared_seed_count
        >= _RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MIN_SHARED_SEEDS
        and support.total_support_count
        >= _RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MIN_TOTAL_SUPPORT
        and support.max_pair_count
        >= _RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MIN_MAX_PAIR_COUNT
        and document_count
        >= min_document_count
    )


def _recent_news_low_coherence_allows_person_only_org(
    support,
    *,
    candidate_type: str,
    document_count: int,
    coherence_score: float | None,
) -> bool:
    return (
        coherence_score is not None
        and coherence_score <= _RECENT_NEWS_DIRECT_PERSON_ONLY_MAX_COHERENCE
        and candidate_type == "organization"
        and support.total_support_count
        >= _RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MIN_TOTAL_SUPPORT
        and support.max_pair_count
        >= _RECENT_NEWS_DIRECT_LOW_COHERENCE_NON_ANCHOR_MIN_MAX_PAIR_COUNT
        and document_count
        >= _RECENT_NEWS_DIRECT_LOW_COHERENCE_PERSON_NON_ANCHOR_MIN_DOCUMENT_COUNT
    )


def _recent_news_canonical_key(value: str | None) -> str:
    normalized = re.sub(r"\s+", " ", str(value or "").strip())
    return normalized.casefold()


def _shared_location_seed_sort_key(entity) -> tuple[int, float, int, str]:
    mention_count = (
        int(entity.mention_count)
        if isinstance(entity.mention_count, int)
        else 1
    )
    baseline_score = _entity_baseline_score(entity)
    start = int(entity.start) if isinstance(entity.start, int) else 0
    entity_id = entity.link.entity_id.strip() if entity.link is not None else ""
    return (-mention_count, -baseline_score, start, entity_id)


def _seed_entity_ids_for_collection_alias(
    response: TagResponse,
    *,
    collection_alias: str,
    settings: Settings,
) -> list[str]:
    seed_entity_ids = _linked_seed_entity_ids(response)
    if not seed_entity_ids:
        return seed_entity_ids

    primary_seed_ids: list[str] = []
    secondary_seed_ids: list[str] = []
    location_entities: list = []
    fallback_seed_ids: list[str] = []
    seen: set[str] = set()
    for entity in response.entities:
        entity_id = entity.link.entity_id.strip() if entity.link is not None else ""
        if not entity_id.startswith("wikidata:Q") or entity_id in seen:
            continue
        seen.add(entity_id)
        label = _seed_entity_label(entity)
        if label in _ROUTED_VECTOR_PRIMARY_SEED_LABELS:
            primary_seed_ids.append(entity_id)
            continue
        if label in _ROUTED_VECTOR_SECONDARY_SEED_LABELS:
            secondary_seed_ids.append(entity_id)
            continue
        if label == "location":
            location_entities.append(entity)
            continue
        fallback_seed_ids.append(entity_id)
    # Use the same strong-anchor preference for the shared collection and the
    # routed aliases. When a document already has person/organization anchors,
    # sending broad location seeds into the vector lane mostly increases junk
    # neighbors without improving related-entity admission.
    routed_vector_collection_aliases = frozenset(
        settings.vector_search_pack_collection_aliases.values()
    )
    if (
        collection_alias == settings.vector_search_collection_alias
        or collection_alias in routed_vector_collection_aliases
    ):
        if primary_seed_ids:
            return primary_seed_ids
        if secondary_seed_ids:
            return secondary_seed_ids
    if collection_alias == settings.vector_search_collection_alias and location_entities:
        if fallback_seed_ids:
            return fallback_seed_ids
        ranked_location_seed_ids = [
            entity.link.entity_id.strip()
            for entity in sorted(location_entities, key=_shared_location_seed_sort_key)[
                :_SHARED_VECTOR_LOCATION_FALLBACK_LIMIT
            ]
            if entity.link is not None and entity.link.entity_id.strip().startswith("wikidata:Q")
        ]
        return fallback_seed_ids + ranked_location_seed_ids
    return seed_entity_ids


def _normalize_payload_packs(payload: dict[str, object]) -> list[str]:
    raw_value = payload.get("packs")
    if not isinstance(raw_value, list):
        return []
    return [str(item).strip() for item in raw_value if str(item).strip()]


def _normalized_hint(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().casefold()
    if not normalized:
        return None
    return normalized.replace("_", "-").replace(" ", "-")


def _canonical_country_hint(
    country_hint: str | None,
    *,
    settings: Settings,
) -> str | None:
    normalized = _normalized_hint(country_hint)
    if normalized is None:
        return None
    return settings.vector_search_country_aliases.get(
        normalized,
        normalized if len(normalized) == 2 else None,
    )


def _routed_pack_ids(
    response: TagResponse,
    *,
    settings: Settings,
    domain_hint: str | None = None,
    country_hint: str | None = None,
) -> list[str]:
    pack_ids: list[str] = []
    if response.pack:
        pack_ids.append(response.pack)
    normalized_domain_hint = _normalized_hint(domain_hint)
    if normalized_domain_hint is not None:
        routed_domain_pack = settings.vector_search_domain_pack_routes.get(
            normalized_domain_hint
        )
        if routed_domain_pack is not None:
            pack_ids.append(routed_domain_pack)
    canonical_country_hint = _canonical_country_hint(
        country_hint,
        settings=settings,
    )
    if canonical_country_hint is not None:
        pack_ids.append(f"finance-{canonical_country_hint}-en")
    return list(dict.fromkeys(pack_id for pack_id in pack_ids if pack_id))


def _entity_baseline_score(entity) -> float:
    if entity.relevance is not None:
        return float(entity.relevance)
    if entity.confidence is not None:
        return float(entity.confidence)
    return 0.0


def _score_candidate(aggregate: _CandidateAggregate) -> float:
    base_score = max(aggregate.scores) if aggregate.scores else 0.0
    shared_seed_count = len(aggregate.seed_entity_ids)
    coherence_bonus = min(0.25, max(0.0, (shared_seed_count - 1) * 0.05))
    return round(base_score + coherence_bonus, 6)


def _merge_nearest_point_results(
    results: list[QdrantNearestPoint],
    *,
    limit: int,
) -> list[QdrantNearestPoint]:
    merged: dict[str, QdrantNearestPoint] = {}
    for item in results:
        existing = merged.get(item.point_id)
        if existing is None:
            merged[item.point_id] = item
            continue
        existing_payload = existing.payload or {}
        item_payload = item.payload or {}
        if float(item.score) > float(existing.score):
            merged[item.point_id] = item
            continue
        if (
            float(item.score) == float(existing.score)
            and item_payload
            and len(item_payload) > len(existing_payload)
        ):
            merged[item.point_id] = item
    ranked = sorted(
        merged.values(),
        key=lambda item: (-float(item.score), item.point_id),
    )
    return ranked[:limit]


def _query_filter_payload(
    response: TagResponse,
    *,
    settings: Settings,
    domain_hint: str | None = None,
    country_hint: str | None = None,
) -> dict[str, object] | None:
    routed_pack_ids = _routed_pack_ids(
        response,
        settings=settings,
        domain_hint=domain_hint,
        country_hint=country_hint,
    )
    if not routed_pack_ids:
        return None
    return {"packs": routed_pack_ids}


def _collection_alias_candidates(
    response: TagResponse,
    *,
    settings: Settings,
    domain_hint: str | None = None,
    country_hint: str | None = None,
) -> list[str]:
    aliases: list[str] = []
    shared_alias = str(settings.vector_search_collection_alias or "").strip()
    for pack_id in _routed_pack_ids(
        response,
        settings=settings,
        domain_hint=domain_hint,
        country_hint=country_hint,
    ):
        routed_alias = str(
            settings.vector_search_pack_collection_aliases.get(pack_id, "")
        ).strip()
        if not routed_alias or routed_alias == shared_alias or routed_alias in aliases:
            continue
        aliases.append(routed_alias)
    if shared_alias and shared_alias not in aliases:
        aliases.append(shared_alias)
    return aliases


def _seed_labels(response: TagResponse) -> set[str]:
    return {
        entity.label.strip()
        for entity in response.entities
        if entity.label.strip()
    }


def _build_seed_support(
    results_by_seed: dict[str, list[QdrantNearestPoint]],
    *,
    seed_entity_ids: set[str],
) -> dict[str, _SeedGraphSupport]:
    support: dict[str, _SeedGraphSupport] = {}
    for seed_entity_id, results in results_by_seed.items():
        for item in results:
            candidate_id = item.point_id
            if candidate_id == seed_entity_id or candidate_id not in seed_entity_ids:
                continue
            candidate_support = support.setdefault(candidate_id, _SeedGraphSupport())
            candidate_support.supporting_seed_ids.add(seed_entity_id)
            candidate_support.scores.append(float(item.score))
    return support


def _document_coherence_score(
    support_by_seed: dict[str, _SeedGraphSupport],
    *,
    seed_entity_ids: list[str],
) -> float | None:
    if not seed_entity_ids:
        return None
    values: list[float] = []
    for seed_entity_id in seed_entity_ids:
        support = support_by_seed.get(seed_entity_id)
        if support is None or not support.scores:
            values.append(0.0)
            continue
        values.append(float(mean(support.scores)))
    return round(float(mean(values)), 6)


def _refine_entities(
    response: TagResponse,
    *,
    support_by_seed: dict[str, _SeedGraphSupport],
    seed_entity_ids: list[str],
    refinement_depth: str,
    refine_links: bool,
) -> tuple[list, bool, list[str], list[str], float | None, dict[str, object] | None]:
    if not refine_links:
        return response.entities, False, [], [], None, None
    threshold = _LOW_CONFIDENCE_THRESHOLD_BY_DEPTH.get(refinement_depth, 0.65)
    boosted_entity_ids: list[str] = []
    downgraded_entity_ids: list[str] = []
    refined_entities: list = []
    changed = False
    support_debug: dict[str, dict[str, object]] = {}
    seed_count = len(seed_entity_ids)
    for entity in response.entities:
        link = entity.link
        entity_id = link.entity_id if link is not None else ""
        if entity_id not in seed_entity_ids:
            refined_entities.append(entity)
            continue
        baseline = _entity_baseline_score(entity)
        support = support_by_seed.get(entity_id)
        supporter_ids = sorted(support.supporting_seed_ids) if support is not None else []
        mean_score = float(mean(support.scores)) if support is not None and support.scores else 0.0
        support_debug[entity_id] = {
            "supporting_seed_ids": supporter_ids,
            "support_score_mean": round(mean_score, 6),
            "baseline_score": round(baseline, 6),
        }
        updated_score = baseline
        if support is not None and baseline <= threshold:
            support_count = len(support.supporting_seed_ids)
            coherence_bonus = min(
                0.18,
                (support_count * 0.04) + max(0.0, mean_score - 0.55) * 0.25,
            )
            updated_score = min(1.0, baseline + coherence_bonus)
        elif support is None and seed_count >= 3 and baseline < _DOWNGRADE_THRESHOLD:
            updated_score = max(0.0, baseline - 0.03)
        delta = round(updated_score - baseline, 6)
        if abs(delta) < _MIN_REFINEMENT_DELTA:
            refined_entities.append(entity)
            continue
        changed = True
        update: dict[str, object] = {}
        if entity.relevance is not None or entity.confidence is None:
            update["relevance"] = round(updated_score, 6)
        if entity.confidence is not None:
            update["confidence"] = round(updated_score, 6)
        refined_entities.append(entity.model_copy(update=update))
        if delta > 0:
            boosted_entity_ids.append(entity_id)
        else:
            downgraded_entity_ids.append(entity_id)
    refinement_score = _document_coherence_score(
        support_by_seed,
        seed_entity_ids=seed_entity_ids,
    )
    debug_payload = (
        {
            "strategy": _REFINEMENT_STRATEGY,
            "refinement_depth": refinement_depth,
            "low_confidence_threshold": threshold,
            "seed_support": support_debug,
        }
        if refine_links
        else None
    )
    return (
        refined_entities,
        changed,
        boosted_entity_ids,
        downgraded_entity_ids,
        refinement_score,
        debug_payload,
    )


def _aggregate_related_entities(
    results_by_seed: dict[str, list[QdrantNearestPoint]],
    *,
    response: TagResponse,
    settings: Settings,
    limit: int | None = None,
    domain_hint: str | None = None,
    country_hint: str | None = None,
) -> list[RelatedEntityMatch]:
    seed_labels = _seed_labels(response)
    allowed_packs = set(
        _routed_pack_ids(
            response,
            settings=settings,
            domain_hint=domain_hint,
            country_hint=country_hint,
        )
    )
    excluded_entity_ids = {
        entity.link.entity_id
        for entity in response.entities
        if entity.link is not None and entity.link.entity_id
    }
    aggregates: dict[str, _CandidateAggregate] = {}
    for seed_entity_id, results in results_by_seed.items():
        for item in results:
            candidate_id = item.point_id
            if candidate_id in excluded_entity_ids:
                continue
            payload = item.payload
            candidate_packs = _normalize_payload_packs(payload)
            if (
                allowed_packs
                and candidate_packs
                and not allowed_packs.intersection(candidate_packs)
            ):
                continue
            aggregate = aggregates.get(candidate_id)
            if aggregate is None:
                aggregate = _CandidateAggregate(
                    entity_id=candidate_id,
                    canonical_text=str(
                        payload.get("canonical_text") or payload.get("entity_id") or candidate_id
                    ),
                    provider=_PROVIDER_NAME,
                    entity_type=(
                        str(payload.get("entity_type")).strip()
                        if payload.get("entity_type") is not None
                        else None
                    ),
                    source_name=(
                        str(payload.get("source_name")).strip()
                        if payload.get("source_name") is not None
                        else None
                    ),
                )
                aggregates[candidate_id] = aggregate
            aggregate.packs.update(candidate_packs)
            aggregate.seed_entity_ids.add(seed_entity_id)
            aggregate.scores.append(float(item.score))
            if aggregate.entity_type and aggregate.entity_type in seed_labels:
                aggregate.scores.append(float(item.score) + 0.03)
    ranked = sorted(
        aggregates.values(),
        key=lambda item: (
            -_score_candidate(item),
            -len(item.seed_entity_ids),
            item.canonical_text.casefold(),
            item.entity_id,
        ),
    )
    if settings.vector_search_score_threshold is not None:
        ranked = [
            item
            for item in ranked
            if _score_candidate(item) >= settings.vector_search_score_threshold
        ]
    resolved_limit = settings.vector_search_related_limit if limit is None else limit
    limited = ranked[:resolved_limit]
    return [
        RelatedEntityMatch(
            entity_id=item.entity_id,
            canonical_text=item.canonical_text,
            score=_score_candidate(item),
            provider=item.provider,
            entity_type=item.entity_type,
            source_name=item.source_name,
            packs=sorted(item.packs),
            seed_entity_ids=sorted(item.seed_entity_ids),
            shared_seed_count=len(item.seed_entity_ids),
        )
        for item in limited
    ]


def _query_vector_results_by_seed(
    *,
    response: TagResponse,
    settings: Settings,
    seed_entity_ids: list[str],
    query_limit: int,
    allow_non_production: bool = False,
    allow_shared_fallback: bool = True,
    domain_hint: str | None = None,
    country_hint: str | None = None,
) -> tuple[dict[str, list[QdrantNearestPoint]], list[str]]:
    if (
        not allow_non_production
        and settings.runtime_target is not RuntimeTarget.PRODUCTION_SERVER
    ):
        return {}, ["vector_search_production_only"]
    if not settings.vector_search_enabled:
        return {}, ["vector_search_disabled"]
    if not settings.vector_search_url:
        return {}, ["vector_search_url_missing"]

    filter_payload = _query_filter_payload(
        response,
        settings=settings,
        domain_hint=domain_hint,
        country_hint=country_hint,
    )
    warnings: list[str] = []
    results_by_seed: dict[str, list[QdrantNearestPoint]] = {}
    collection_aliases = _collection_alias_candidates(
        response,
        settings=settings,
        domain_hint=domain_hint,
        country_hint=country_hint,
    )
    shared_alias = str(settings.vector_search_collection_alias or "").strip()
    if (
        not allow_shared_fallback
        and shared_alias
        and len(collection_aliases) > 1
    ):
        collection_aliases = [
            alias for alias in collection_aliases if alias != shared_alias
        ]
    eligible_seed_ids_by_alias = {
        collection_alias: set(
            _seed_entity_ids_for_collection_alias(
                response,
                collection_alias=collection_alias,
                settings=settings,
            )
        )
        for collection_alias in collection_aliases
    }
    eligible_seed_ids = {
        seed_entity_id
        for alias_seed_ids in eligible_seed_ids_by_alias.values()
        for seed_entity_id in alias_seed_ids
    }
    query_seed_entity_ids = [
        seed_entity_id
        for seed_entity_id in seed_entity_ids
        if seed_entity_id in eligible_seed_ids
    ]
    graph_store: QidGraphStore | None = None
    try:
        if settings.graph_context_artifact_path is not None:
            graph_store = QidGraphStore(settings.graph_context_artifact_path)
        with QdrantVectorSearchClient(
            settings.vector_search_url,
            api_key=settings.vector_search_api_key,
        ) as client:
            for seed_entity_id in query_seed_entity_ids:
                resolved_results: list[QdrantNearestPoint] | None = None
                merged_results: list[QdrantNearestPoint] = []
                alias_resolved = False
                for collection_alias in collection_aliases:
                    if seed_entity_id not in eligible_seed_ids_by_alias.get(
                        collection_alias, set()
                    ):
                        continue
                    try:
                        alias_results = client.query_similar_by_id(
                            collection_alias,
                            point_id=seed_entity_id,
                            limit=query_limit,
                            filter_payload=filter_payload,
                        )
                    except QdrantVectorSearchError as exc:
                        if exc.status_code == 404:
                            continue
                        raise
                    alias_resolved = True
                    resolved_results = alias_results
                    if alias_results:
                        merged_results.extend(alias_results)
                    if (
                        not allow_shared_fallback
                        and collection_alias == shared_alias
                        and alias_results
                    ):
                        break
                if alias_resolved:
                    if merged_results:
                        results_by_seed[seed_entity_id] = _merge_nearest_point_results(
                            merged_results,
                            limit=query_limit,
                        )
                    else:
                        results_by_seed[seed_entity_id] = []
                    continue

                fallback_vector = None
                if graph_store is not None:
                    fallback_vector = build_dense_vector_from_graph_store(
                        graph_store,
                        seed_entity_id.removeprefix("wikidata:"),
                        dimensions=DEFAULT_QID_GRAPH_DIMENSIONS,
                        allowed_predicates=DEFAULT_QID_GRAPH_ALLOWED_PREDICATES,
                    )
                if fallback_vector is not None:
                    fallback_results: list[QdrantNearestPoint] | None = None
                    for collection_alias in collection_aliases:
                        if seed_entity_id not in eligible_seed_ids_by_alias.get(
                            collection_alias, set()
                        ):
                            continue
                        try:
                            alias_results = client.query_similar_by_vector(
                                collection_alias,
                                vector=fallback_vector,
                                limit=query_limit,
                                filter_payload=filter_payload,
                            )
                        except QdrantVectorSearchError as fallback_exc:
                            if fallback_exc.status_code == 404:
                                continue
                            warnings.append(f"vector_search_failed:{fallback_exc}")
                            return {}, warnings
                        if fallback_results is None:
                            fallback_results = alias_results
                        if alias_results:
                            merged_results.extend(alias_results)
                    if fallback_results is not None:
                        if merged_results:
                            results_by_seed[seed_entity_id] = _merge_nearest_point_results(
                                merged_results,
                                limit=query_limit,
                            )
                        else:
                            results_by_seed[seed_entity_id] = []
                        warnings.append(
                            f"vector_search_seed_graph_fallback:{seed_entity_id}"
                        )
                        continue
                warnings.append(f"vector_search_seed_missing:{seed_entity_id}")
    except QdrantVectorSearchError as exc:
        warnings.append(f"vector_search_failed:{exc}")
        return {}, warnings
    finally:
        if graph_store is not None:
            graph_store.close()
    if not results_by_seed:
        warnings.append("vector_search_no_available_seed_points")
    return results_by_seed, warnings


def _graph_context_strong_seed_entity_ids(response: TagResponse) -> list[str]:
    seen: set[str] = set()
    linked_entity_by_id = {
        entity.link.entity_id: entity
        for entity in response.entities
        if entity.link is not None and entity.link.entity_id
    }

    def _is_probable_source_outlet_seed(entity_id: str) -> bool:
        entity = linked_entity_by_id.get(entity_id)
        if entity is None:
            return False
        canonical_text = str(
            entity.link.canonical_text if entity.link is not None else entity.text
        ).strip()
        return is_probable_source_outlet_candidate(
            canonical_text or entity.text,
            candidate_type=entity.label,
        )

    refinement_debug = response.refinement_debug
    graph_support = response.graph_support
    if isinstance(refinement_debug, dict):
        decision_action_by_entity_id: dict[str, str] = {}
        raw_seed_decisions = refinement_debug.get("seed_decisions")
        if isinstance(raw_seed_decisions, list):
            for item in raw_seed_decisions:
                if not isinstance(item, dict):
                    continue
                entity_id = str(item.get("entity_id") or "").strip()
                action = str(item.get("action") or "").strip()
                if not entity_id.startswith("wikidata:Q"):
                    continue
                decision_action_by_entity_id[entity_id] = action
        filtered_anchor_seed_ids: list[str] = []
        anchor_seed_ids: list[str] = []
        raw_anchor_seed_ids = refinement_debug.get("graph_anchor_seed_ids")
        if isinstance(raw_anchor_seed_ids, list):
            for item in raw_anchor_seed_ids:
                entity_id = str(item or "").strip()
                action = decision_action_by_entity_id.get(entity_id)
                if action not in {None, "keep", "boost"}:
                    if entity_id.startswith("wikidata:Q"):
                        filtered_anchor_seed_ids.append(entity_id)
                    continue
                if _is_probable_source_outlet_seed(entity_id):
                    filtered_anchor_seed_ids.append(entity_id)
                    continue
                if not entity_id.startswith("wikidata:Q") or entity_id in seen:
                    continue
                seen.add(entity_id)
                anchor_seed_ids.append(entity_id)
        if isinstance(raw_seed_decisions, list):
            should_supplement_anchor_seed_ids = bool(filtered_anchor_seed_ids) or (
                graph_support is not None
                and graph_support.coherence_score is not None
                and graph_support.coherence_score
                < _HYBRID_VECTOR_PROPOSAL_MIN_COHERENCE
            )
            supplemental_non_location_seed_ids: list[str] = []
            supplemental_location_seed_ids: list[str] = []
            strong_seed_ids: list[str] = []
            for item in raw_seed_decisions:
                if not isinstance(item, dict):
                    continue
                entity_id = str(item.get("entity_id") or "").strip()
                action = str(item.get("action") or "").strip()
                if action not in {"keep", "boost"}:
                    continue
                if _is_probable_source_outlet_seed(entity_id):
                    continue
                if not entity_id.startswith("wikidata:Q") or entity_id in seen:
                    continue
                seen.add(entity_id)
                if anchor_seed_ids and should_supplement_anchor_seed_ids:
                    entity = linked_entity_by_id.get(entity_id)
                    if entity is not None and _seed_entity_label(entity) == "location":
                        supplemental_location_seed_ids.append(entity_id)
                    else:
                        supplemental_non_location_seed_ids.append(entity_id)
                    continue
                strong_seed_ids.append(entity_id)
            if anchor_seed_ids:
                target_seed_count = max(
                    len(anchor_seed_ids),
                    _RECENT_NEWS_DIRECT_LOW_COHERENCE_MIN_SHARED_SEEDS,
                )
                supplemental_seed_ids = (
                    supplemental_non_location_seed_ids + supplemental_location_seed_ids
                )
                supplemental_limit = max(0, target_seed_count - len(anchor_seed_ids))
                return anchor_seed_ids + supplemental_seed_ids[:supplemental_limit]
            if strong_seed_ids:
                return strong_seed_ids
    graph_support = response.graph_support
    suppressed = (
        set(graph_support.suppressed_entity_ids) if graph_support is not None else set()
    )
    downgraded = (
        set(graph_support.downgraded_entity_ids) if graph_support is not None else set()
    )
    strong_seed_ids: list[str] = []
    for entity_id in _linked_seed_entity_ids(response):
        if _is_probable_source_outlet_seed(entity_id):
            continue
        if entity_id in suppressed or entity_id in downgraded or entity_id in seen:
            continue
        seen.add(entity_id)
        strong_seed_ids.append(entity_id)
    return strong_seed_ids


def _graph_gate_vector_related_entities(
    vector_related_entities: list[RelatedEntityMatch],
    *,
    response: TagResponse,
    strong_seed_entity_ids: list[str],
    settings: Settings,
) -> list[RelatedEntityMatch]:
    graph_support = response.graph_support
    if not vector_related_entities or settings.graph_context_artifact_path is None:
        return []
    if graph_support is None or graph_support.coherence_score is None:
        return []
    if graph_support.coherence_score < _HYBRID_VECTOR_PROPOSAL_MIN_COHERENCE:
        return []
    if len(strong_seed_entity_ids) < settings.graph_context_min_supporting_seeds:
        return []

    extracted_entity_ids = {
        entity.link.entity_id
        for entity in response.entities
        if entity.link is not None and entity.link.entity_id
    }
    seed_label_by_entity_id = {
        entity.link.entity_id: entity.label.strip().casefold()
        for entity in response.entities
        if entity.link is not None
        and entity.link.entity_id
        and entity.label.strip()
    }
    existing_related_ids = {item.entity_id for item in response.related_entities}
    candidate_qids = tuple(
        dict.fromkeys(
            item.entity_id.removeprefix("wikidata:")
            for item in vector_related_entities
            if item.entity_id.startswith("wikidata:Q")
            and item.entity_id not in extracted_entity_ids
            and item.entity_id not in existing_related_ids
        )
    )
    if not candidate_qids:
        return []

    strong_seed_qids = tuple(
        entity_id.removeprefix("wikidata:") for entity_id in strong_seed_entity_ids
    )
    graph_related_qids = tuple(
        related.entity_id.removeprefix("wikidata:")
        for related in response.related_entities
        if related.entity_id.startswith("wikidata:Q")
    )
    with QidGraphStore(settings.graph_context_artifact_path) as store:
        metadata_by_qid = store.node_metadata_batch(
            (*strong_seed_qids, *candidate_qids, *graph_related_qids)
        )
        neighbors_by_qid = store.neighbors_batch(
            (*strong_seed_qids, *candidate_qids, *graph_related_qids),
            direction="both",
            limit_per_qid=settings.graph_context_seed_neighbor_limit,
        )

    seed_neighbor_sets = {
        seed_qid: {neighbor.qid for neighbor in neighbors_by_qid.get(seed_qid, [])}
        for seed_qid in strong_seed_qids
    }
    graph_related_neighbor_sets = {
        related_qid: {
            neighbor.qid for neighbor in neighbors_by_qid.get(related_qid, [])
        }
        for related_qid in graph_related_qids
    }
    admitted: list[RelatedEntityMatch] = []
    for item in vector_related_entities:
        if not item.entity_id.startswith("wikidata:Q"):
            continue
        if item.entity_id in extracted_entity_ids or item.entity_id in existing_related_ids:
            continue
        candidate_qid = item.entity_id.removeprefix("wikidata:")
        metadata = metadata_by_qid.get(candidate_qid)
        if metadata is None or metadata.entity_id != item.entity_id:
            continue
        display_text = metadata.canonical_text.strip() or item.canonical_text.strip()
        if not display_text or display_text == candidate_qid:
            continue
        candidate_neighbor_set = {
            neighbor.qid for neighbor in neighbors_by_qid.get(candidate_qid, [])
        }
        supporting_seed_ids: list[str] = []
        direct_support_count = 0
        shared_context_qids: set[str] = set()
        for seed_entity_id, seed_qid in zip(
            strong_seed_entity_ids,
            strong_seed_qids,
            strict=True,
        ):
            seed_neighbor_set = seed_neighbor_sets.get(seed_qid, set())
            direct_support = (
                candidate_qid in seed_neighbor_set or seed_qid in candidate_neighbor_set
            )
            shared_neighbors = (
                candidate_neighbor_set.intersection(seed_neighbor_set)
                - {candidate_qid, seed_qid}
            )
            if not direct_support and not shared_neighbors:
                continue
            supporting_seed_ids.append(seed_entity_id)
            if direct_support:
                direct_support_count += 1
            shared_context_qids.update(shared_neighbors)
        if graph_related_qids:
            has_graph_related_overlap = False
            for related_qid in graph_related_qids:
                related_neighbor_set = graph_related_neighbor_sets.get(related_qid, set())
                shared_related_neighbors = (
                    candidate_neighbor_set.intersection(related_neighbor_set)
                    - {candidate_qid, related_qid}
                )
                if (
                    candidate_qid in related_neighbor_set
                    or related_qid in candidate_neighbor_set
                    or shared_related_neighbors
                ):
                    has_graph_related_overlap = True
                    break
            if not has_graph_related_overlap:
                continue
        if len(supporting_seed_ids) < settings.graph_context_min_supporting_seeds:
            continue
        supporting_seed_labels = {
            seed_label
            for seed_entity_id in supporting_seed_ids
            if (seed_label := seed_label_by_entity_id.get(seed_entity_id))
        }
        candidate_type = (metadata.entity_type or item.entity_type or "").strip().casefold()
        if (
            not graph_related_qids
            and direct_support_count == 0
            and supporting_seed_labels == {"organization"}
        ):
            if candidate_type == "location":
                continue
            if (
                candidate_type == "organization"
                and len(shared_context_qids)
                < _HYBRID_VECTOR_ORG_ONLY_NO_DIRECT_MIN_SHARED_CONTEXT
            ):
                continue
        if (
            candidate_type == "person"
            and direct_support_count == 0
            and supporting_seed_labels == {"person"}
        ):
            continue
        graph_bonus = min(
            0.18,
            max(
                0.0,
                (len(supporting_seed_ids) - 1) * 0.05
                + (direct_support_count * 0.03)
                + min(len(shared_context_qids), 3) * 0.01,
            ),
        )
        admitted.append(
            RelatedEntityMatch(
                entity_id=item.entity_id,
                canonical_text=display_text,
                score=round(float(item.score) + graph_bonus, 6),
                provider=item.provider,
                entity_type=metadata.entity_type or item.entity_type,
                source_name=metadata.source_name or item.source_name,
                packs=sorted(set(item.packs).union(metadata.packs)),
                seed_entity_ids=supporting_seed_ids,
                shared_seed_count=len(supporting_seed_ids),
            )
        )
    admitted.sort(
        key=lambda related: (
            -related.score,
            -related.shared_seed_count,
            related.canonical_text.casefold(),
            related.entity_id,
        )
    )
    return admitted[: settings.graph_context_related_limit]


def _recent_news_gate_vector_related_entities(
    vector_related_entities: list[RelatedEntityMatch],
    *,
    response: TagResponse,
    strong_seed_entity_ids: list[str],
    settings: Settings,
) -> list[RelatedEntityMatch]:
    if (
        not vector_related_entities
        or settings.news_context_artifact_path is None
        or len(strong_seed_entity_ids) < settings.news_context_min_supporting_seeds
    ):
        return []

    artifact = load_recent_news_support_artifact(settings.news_context_artifact_path)
    extracted_entity_ids = {
        entity.link.entity_id
        for entity in response.entities
        if entity.link is not None and entity.link.entity_id
    }
    existing_related_ids = {item.entity_id for item in response.related_entities}
    seed_label_by_entity_id = {
        entity.link.entity_id: entity.label.strip().casefold()
        for entity in response.entities
        if entity.link is not None
        and entity.link.entity_id
        and entity.label.strip()
    }
    admitted: list[RelatedEntityMatch] = []
    for item in vector_related_entities:
        if not item.entity_id.startswith("wikidata:Q"):
            continue
        if item.entity_id in extracted_entity_ids or item.entity_id in existing_related_ids:
            continue
        support = artifact.support_for_candidate(
            item.entity_id,
            strong_seed_entity_ids,
            min_supporting_seeds=settings.news_context_min_supporting_seeds,
            min_pair_count=settings.news_context_min_pair_count,
        )
        if support is None:
            continue
        metadata = artifact.metadata_for(item.entity_id)
        display_text = (
            metadata.canonical_text.strip()
            if metadata is not None and metadata.canonical_text.strip()
            else item.canonical_text.strip()
        )
        if not display_text or display_text == item.entity_id:
            continue
        supporting_seed_labels = {
            seed_label
            for seed_entity_id in support.supporting_seed_ids
            if (seed_label := seed_label_by_entity_id.get(seed_entity_id))
        }
        candidate_type = (
            metadata.entity_type
            if metadata is not None and metadata.entity_type
            else item.entity_type
        )
        if (
            str(candidate_type or "").strip().casefold() == "person"
            and supporting_seed_labels == {"person"}
            and support.total_support_count < _RECENT_NEWS_PERSON_ONLY_MIN_TOTAL_SUPPORT
        ):
            continue
        news_bonus = min(
            0.12,
            max(
                0.0,
                (support.shared_seed_count - 1) * 0.03
                + min(support.total_support_count, 4) * 0.01
                + min(support.max_pair_count, 3) * 0.01,
            ),
        )
        admitted.append(
            RelatedEntityMatch(
                entity_id=item.entity_id,
                canonical_text=display_text,
                score=round(float(item.score) + news_bonus, 6),
                provider=item.provider,
                entity_type=candidate_type,
                source_name=(
                    metadata.source_name
                    if metadata is not None and metadata.source_name
                    else item.source_name
                ),
                packs=sorted(set(item.packs)),
                seed_entity_ids=list(support.supporting_seed_ids),
                shared_seed_count=support.shared_seed_count,
            )
        )
    admitted.sort(
        key=lambda related: (
            -related.score,
            -related.shared_seed_count,
            related.canonical_text.casefold(),
            related.entity_id,
        )
    )
    return admitted[: settings.graph_context_related_limit]


def _recent_news_direct_fallback_seed_entity_ids(
    response: TagResponse,
    *,
    strong_seed_entity_ids: list[str],
    domain_hint: str | None,
) -> list[str]:
    graph_support = response.graph_support
    if (
        graph_support is None
        or graph_support.coherence_score is None
        or graph_support.coherence_score >= _HYBRID_VECTOR_PROPOSAL_MIN_COHERENCE
    ):
        return list(dict.fromkeys(strong_seed_entity_ids))

    suppressed = set(graph_support.suppressed_entity_ids)
    downgraded = set(graph_support.downgraded_entity_ids)
    graph_seed_entity_ids = set(graph_support.seed_entity_ids)
    best_entity_by_id: dict[str, object] = {}
    for entity in response.entities:
        if entity.link is None or not entity.link.entity_id:
            continue
        entity_id = entity.link.entity_id.strip()
        if (
            entity_id not in graph_seed_entity_ids
            or entity_id in suppressed
            or entity_id in downgraded
        ):
            continue
        existing = best_entity_by_id.get(entity_id)
        if existing is None or float(entity.relevance or 0.0) > float(existing.relevance or 0.0):
            best_entity_by_id[entity_id] = entity
    base_seed_ids = [
        entity_id
        for entity_id in dict.fromkeys(strong_seed_entity_ids)
        if not _is_recent_news_generic_org_seed(best_entity_by_id.get(entity_id))
    ]
    seen = set(base_seed_ids)
    base_has_person_seed = any(
        _seed_entity_label(best_entity_by_id.get(entity_id)) == "person"
        for entity_id in base_seed_ids
    )

    extra_person_ids: list[str] = []
    if not base_has_person_seed:
        extra_person_ids = [
            entity.link.entity_id
            for entity in sorted(
                best_entity_by_id.values(),
                key=lambda item: (
                    -float(item.relevance or 0.0),
                    -(item.mention_count or 0),
                    (item.link.canonical_text if item.link is not None else item.text).casefold(),
                    item.text.casefold(),
                ),
            )
            if entity.link is not None
            and entity.link.entity_id not in seen
            and _seed_entity_label(entity) == "person"
            and float(entity.relevance or 0.0)
            >= _RECENT_NEWS_DIRECT_EXTRA_PERSON_SEED_MIN_RELEVANCE
        ][: _RECENT_NEWS_DIRECT_EXTRA_PERSON_LIMIT]
    for entity_id in extra_person_ids:
        seen.add(entity_id)

    extra_non_location_ids = [
        entity.link.entity_id
        for entity in sorted(
            best_entity_by_id.values(),
            key=lambda item: (
                -float(item.relevance or 0.0),
                -(item.mention_count or 0),
                (item.link.canonical_text if item.link is not None else item.text).casefold(),
                item.text.casefold(),
            ),
        )
        if entity.link is not None
        and entity.link.entity_id not in seen
        and _seed_entity_label(entity) == "organization"
        and not _is_recent_news_generic_org_seed(entity)
        and not is_probable_source_outlet_candidate(
            str(entity.link.canonical_text or entity.text).strip(),
            candidate_type="organization",
        )
        and float(entity.relevance or 0.0)
        >= _RECENT_NEWS_DIRECT_EXTRA_NON_LOCATION_SEED_MIN_RELEVANCE
    ][: _RECENT_NEWS_DIRECT_EXTRA_NON_LOCATION_LIMIT]
    for entity_id in extra_non_location_ids:
        seen.add(entity_id)

    should_augment_location_seed = (
        _normalized_news_context_domain_hint(domain_hint)
        in _RECENT_NEWS_DIRECT_LOCATION_AUGMENTED_DOMAINS
    ) or domain_hint is None
    extra_location_ids: list[str] = []
    if should_augment_location_seed:
        extra_location_ids = [
            entity.link.entity_id
            for entity in sorted(
                best_entity_by_id.values(),
                key=lambda item: (
                    -float(item.relevance or 0.0),
                    -(item.mention_count or 0),
                    (
                        item.link.canonical_text if item.link is not None else item.text
                    ).casefold(),
                    item.text.casefold(),
                ),
            )
            if entity.link is not None
            and entity.link.entity_id not in seen
            and _seed_entity_label(entity) == "location"
            and float(entity.relevance or 0.0)
            >= _RECENT_NEWS_DIRECT_EXTRA_LOCATION_SEED_MIN_RELEVANCE
        ][: _RECENT_NEWS_DIRECT_EXTRA_LOCATION_LIMIT]

    return [
        *base_seed_ids,
        *extra_person_ids,
        *extra_non_location_ids,
        *extra_location_ids,
    ]


def _recent_news_propose_related_entities(
    *,
    response: TagResponse,
    strong_seed_entity_ids: list[str],
    settings: Settings,
) -> list[RelatedEntityMatch]:
    if (
        settings.news_context_artifact_path is None
        or len(strong_seed_entity_ids) < settings.news_context_min_supporting_seeds
    ):
        return []

    artifact = load_recent_news_support_artifact(settings.news_context_artifact_path)
    extracted_entity_ids = {
        entity.link.entity_id
        for entity in response.entities
        if entity.link is not None and entity.link.entity_id
    }
    existing_related_ids = {item.entity_id for item in response.related_entities}
    direct_candidates: list[RelatedEntityMatch] = []
    coherence_score = (
        response.graph_support.coherence_score
        if response.graph_support is not None
        else None
    )
    seed_entity_by_id: dict[str, object] = {}
    for entity in response.entities:
        if entity.link is None or not entity.link.entity_id:
            continue
        entity_id = entity.link.entity_id.strip()
        existing = seed_entity_by_id.get(entity_id)
        if existing is None or float(entity.relevance or 0.0) > float(existing.relevance or 0.0):
                seed_entity_by_id[entity_id] = entity
    limit = max(settings.graph_context_related_limit * 4, settings.graph_context_related_limit)
    low_coherence_recent_news = (
        coherence_score is not None
        and coherence_score < _HYBRID_VECTOR_PROPOSAL_MIN_COHERENCE
    )

    def _graph_supported_person_only_affiliations(
        person_seed_entity_ids: list[str],
    ) -> dict[str, dict[str, object]] | None:
        if (
            not person_seed_entity_ids
            or settings.graph_context_artifact_path is None
            or not _RECENT_NEWS_PERSON_ONLY_GRAPH_ALLOWED_PREDICATES
        ):
            return None
        person_seed_qids = [
            seed_entity_id.removeprefix("wikidata:")
            for seed_entity_id in person_seed_entity_ids
            if seed_entity_id.startswith("wikidata:Q")
        ]
        if not person_seed_qids:
            return None
        with QidGraphStore(settings.graph_context_artifact_path) as graph_store:
            neighbors_by_qid = graph_store.neighbors_batch(
                person_seed_qids,
                direction="both",
                predicates=_RECENT_NEWS_PERSON_ONLY_GRAPH_ALLOWED_PREDICATES,
                limit_per_qid=32,
            )
        affiliations: dict[str, dict[str, object]] = {}
        for seed_qid in person_seed_qids:
            seed_entity_id = f"wikidata:{seed_qid}"
            for neighbor in neighbors_by_qid.get(seed_qid, []):
                if neighbor.entity_type != "organization":
                    continue
                entry = affiliations.get(neighbor.entity_id)
                if entry is None:
                    affiliations[neighbor.entity_id] = {
                        "canonical_text": neighbor.canonical_text,
                        "entity_type": neighbor.entity_type,
                        "source_name": neighbor.source_name,
                        "packs": list(neighbor.packs),
                        "supporting_seed_ids": [seed_entity_id],
                    }
                    continue
                supporting_seed_ids = entry.get("supporting_seed_ids")
                if isinstance(supporting_seed_ids, list) and seed_entity_id not in supporting_seed_ids:
                    supporting_seed_ids.append(seed_entity_id)
        return affiliations

    def _collect_direct_candidates(
        supports,
        *,
        allow_person_only_fallback: bool = False,
        graph_supported_candidate_ids: set[str] | None = None,
    ) -> list[RelatedEntityMatch]:
        collected: list[RelatedEntityMatch] = []
        for support in supports:
            if (
                support.entity_id in extracted_entity_ids
                or support.entity_id in existing_related_ids
            ):
                continue
            metadata = artifact.metadata_for(support.entity_id)
            if metadata is None:
                continue
            display_text = metadata.canonical_text.strip()
            if not display_text or display_text == support.entity_id:
                continue
            aligned_entity_id = support.entity_id
            aligned_affiliation = graph_supported_affiliation_by_name.get(
                _recent_news_canonical_key(display_text)
            )
            if aligned_affiliation is not None:
                candidate_entity_id = str(
                    aligned_affiliation.get("entity_id") or ""
                ).strip()
                if candidate_entity_id:
                    if (
                        candidate_entity_id in extracted_entity_ids
                        or candidate_entity_id in existing_related_ids
                    ):
                        continue
                    aligned_entity_id = candidate_entity_id
                    display_text = str(
                        aligned_affiliation.get("canonical_text") or display_text
                    ).strip()
            candidate_type = str(metadata.entity_type or "").strip().casefold()
            if aligned_affiliation is not None:
                candidate_type = str(
                    aligned_affiliation.get("entity_type") or candidate_type
                ).strip().casefold()
            document_count = max(int(metadata.document_count), 0)
            has_anchor_support = False
            has_person_support = False
            person_only_support = False
            support_labels: set[str] = set()
            if coherence_score is not None:
                has_anchor_support = _recent_news_low_coherence_has_anchor_support(
                    support,
                    seed_entity_by_id=seed_entity_by_id,
                )
                has_person_support = _recent_news_low_coherence_has_person_support(
                    support,
                    seed_entity_by_id=seed_entity_by_id,
                )
                support_labels = {
                    _seed_entity_label(seed_entity)
                    for seed_entity_id in support.supporting_seed_ids
                    if (seed_entity := seed_entity_by_id.get(seed_entity_id)) is not None
                }
                person_only_support = bool(support_labels) and support_labels == {"person"}
            if (
                coherence_score is not None
                and coherence_score < _HYBRID_VECTOR_PROPOSAL_MIN_COHERENCE
            ):
                # Keep the low-coherence escape hatch focused on person-centric
                # breaking-news cases. Without a person seed, local location/agency
                # pairs are too prone to absurd support-driven neighbors.
                if not has_person_support:
                    continue
                if has_anchor_support:
                    min_shared_seed_count = _recent_news_low_coherence_min_shared_seeds(
                        candidate_type=candidate_type,
                        settings=settings,
                        has_anchor_support=has_anchor_support,
                    )
                    if support.shared_seed_count < min_shared_seed_count:
                        continue
                elif allow_person_only_fallback and person_only_support:
                    if not _recent_news_low_coherence_allows_person_only_org(
                        support,
                        candidate_type=candidate_type,
                        document_count=document_count,
                        coherence_score=coherence_score,
                    ):
                        continue
                    if (
                        graph_supported_candidate_ids is not None
                        and aligned_entity_id not in graph_supported_candidate_ids
                    ):
                        continue
                elif (
                    "organization" not in support_labels
                    or not _recent_news_low_coherence_allows_strong_non_anchor_org(
                        support,
                        candidate_type=candidate_type,
                        document_count=document_count,
                        coherence_score=coherence_score,
                        has_person_support=has_person_support,
                    )
                ):
                    continue
            elif coherence_score is not None and not has_anchor_support:
                continue
            if candidate_type == "location":
                continue
            if candidate_type == "person":
                continue
            if is_probable_source_outlet_candidate(
                display_text,
                candidate_type=candidate_type,
            ):
                continue
            base_score = min(
                _RECENT_NEWS_DIRECT_PROPOSAL_MAX_SCORE,
                max(
                    0.36,
                    0.36
                    + (support.shared_seed_count - 1) * 0.06
                    + min(support.total_support_count, 12) * 0.015
                    + min(support.max_pair_count, 6) * 0.01
                    + min(document_count, 8) * 0.01,
                ),
            )
            collected.append(
                RelatedEntityMatch(
                    entity_id=aligned_entity_id,
                    canonical_text=display_text,
                    score=round(base_score, 6),
                    provider="recent_news.support",
                    entity_type=(
                        str(aligned_affiliation.get("entity_type") or "")
                        if aligned_affiliation is not None
                        else metadata.entity_type
                    ),
                    source_name=(
                        str(
                            aligned_affiliation.get("source_name")
                            or metadata.source_name
                            or "recent-news-support"
                        )
                        if aligned_affiliation is not None
                        else metadata.source_name or "recent-news-support"
                    ),
                    packs=(
                        list(aligned_affiliation.get("packs") or [])
                        if aligned_affiliation is not None
                        else []
                    ),
                    seed_entity_ids=list(support.supporting_seed_ids),
                    shared_seed_count=support.shared_seed_count,
                )
            )
        return collected

    person_seed_entity_ids = [
        seed_entity_id
        for seed_entity_id in strong_seed_entity_ids
        if (
            seed_entity := seed_entity_by_id.get(seed_entity_id)
        ) is not None
        and _seed_entity_label(seed_entity) == "person"
    ]
    graph_supported_affiliations = (
        _graph_supported_person_only_affiliations(person_seed_entity_ids)
        if low_coherence_recent_news and person_seed_entity_ids
        else None
    )
    graph_supported_candidate_ids = (
        set(graph_supported_affiliations)
        if graph_supported_affiliations is not None
        else None
    )
    graph_supported_affiliation_by_name: dict[str, dict[str, object]] = {}
    if graph_supported_affiliations:
        grouped_affiliations: dict[str, list[tuple[str, dict[str, object]]]] = {}
        for entity_id, affiliation in graph_supported_affiliations.items():
            name_key = _recent_news_canonical_key(affiliation.get("canonical_text"))
            if not name_key:
                continue
            grouped_affiliations.setdefault(name_key, []).append((entity_id, affiliation))
        for name_key, entries in grouped_affiliations.items():
            unique_entity_ids = {entity_id for entity_id, _ in entries}
            if len(unique_entity_ids) != 1:
                continue
            entity_id, affiliation = entries[0]
            graph_supported_affiliation_by_name[name_key] = {
                "entity_id": entity_id,
                **affiliation,
            }

    supports = artifact.candidate_supports_for_seeds(
        strong_seed_entity_ids,
        min_supporting_seeds=settings.news_context_min_supporting_seeds,
        min_pair_count=settings.news_context_min_pair_count,
        limit=limit,
    )
    direct_candidates = _collect_direct_candidates(supports)
    if low_coherence_recent_news and graph_supported_candidate_ids:
        graph_supported_direct_candidates = [
            item
            for item in direct_candidates
            if item.entity_id in graph_supported_candidate_ids
        ]
        if graph_supported_direct_candidates:
            direct_candidates = graph_supported_direct_candidates
    if (
        not direct_candidates
        and low_coherence_recent_news
    ):
        if person_seed_entity_ids:
            person_only_supports = artifact.candidate_supports_for_seeds(
                person_seed_entity_ids,
                min_supporting_seeds=1,
                min_pair_count=settings.news_context_min_pair_count,
                limit=limit,
            )
            direct_candidates = _collect_direct_candidates(
                person_only_supports,
                allow_person_only_fallback=True,
                graph_supported_candidate_ids=graph_supported_candidate_ids,
            )
            if not direct_candidates and graph_supported_affiliations:
                for entity_id, affiliation in graph_supported_affiliations.items():
                    if (
                        entity_id in extracted_entity_ids
                        or entity_id in existing_related_ids
                    ):
                        continue
                    display_text = str(
                        affiliation.get("canonical_text") or entity_id
                    ).strip()
                    candidate_type = str(
                        affiliation.get("entity_type") or ""
                    ).strip().casefold()
                    if (
                        not display_text
                        or display_text == entity_id
                        or candidate_type != "organization"
                        or is_probable_source_outlet_candidate(
                            display_text,
                            candidate_type=candidate_type,
                        )
                    ):
                        continue
                    supporting_seed_ids = list(
                        affiliation.get("supporting_seed_ids") or []
                    )
                    if not supporting_seed_ids:
                        continue
                    if (
                        len(supporting_seed_ids) == 1
                        and response.graph_support is not None
                        and len(response.graph_support.seed_entity_ids)
                        > _RECENT_NEWS_PERSON_ONLY_GRAPH_SINGLE_SEED_MAX_CONTEXT_SEEDS
                    ):
                        continue
                    direct_candidates.append(
                        RelatedEntityMatch(
                            entity_id=entity_id,
                            canonical_text=display_text,
                            score=0.66,
                            provider="qid_graph.affiliation",
                            entity_type=str(affiliation.get("entity_type") or ""),
                            source_name=str(
                                affiliation.get("source_name")
                                or "wikidata-general-entities"
                            ),
                            packs=list(affiliation.get("packs") or []),
                            seed_entity_ids=supporting_seed_ids,
                            shared_seed_count=len(supporting_seed_ids),
                        )
                    )
    return _merge_related_entities(
        [],
        direct_candidates,
        limit=settings.graph_context_related_limit,
    )


def _should_probe_recent_news_direct_support(
    *,
    response: TagResponse,
    strong_seed_entity_ids: list[str],
    settings: Settings,
    vetted_candidates: list[RelatedEntityMatch],
    recent_news_candidates: list[RelatedEntityMatch],
) -> bool:
    if (
        settings.news_context_artifact_path is None
        or len(strong_seed_entity_ids) < settings.news_context_min_supporting_seeds
    ):
        return False
    graph_support = response.graph_support
    coherence_score = (
        graph_support.coherence_score if graph_support is not None else None
    )
    if coherence_score is None:
        return False
    if coherence_score < _HYBRID_VECTOR_PROPOSAL_MIN_COHERENCE:
        return True
    if response.related_entities:
        return False
    return not vetted_candidates and not recent_news_candidates


def _merge_related_entities(
    primary: list[RelatedEntityMatch],
    secondary: list[RelatedEntityMatch],
    *,
    limit: int,
) -> list[RelatedEntityMatch]:
    merged: dict[str, RelatedEntityMatch] = {
        item.entity_id: item.model_copy(deep=True) for item in primary
    }
    for item in secondary:
        existing = merged.get(item.entity_id)
        if existing is None:
            merged[item.entity_id] = item.model_copy(deep=True)
            continue
        merged[item.entity_id] = existing.model_copy(
            update={
                "score": max(existing.score, item.score),
                "entity_type": existing.entity_type or item.entity_type,
                "source_name": existing.source_name or item.source_name,
                "packs": sorted(set(existing.packs).union(item.packs)),
                "seed_entity_ids": sorted(
                    set(existing.seed_entity_ids).union(item.seed_entity_ids)
                ),
                "shared_seed_count": len(
                    set(existing.seed_entity_ids).union(item.seed_entity_ids)
                ),
            }
        )
    ranked = sorted(
        merged.values(),
        key=lambda related: (
            -related.score,
            -related.shared_seed_count,
            related.canonical_text.casefold(),
            related.entity_id,
        ),
    )
    return ranked[:limit]


def _should_probe_low_coherence_recent_news_support(
    *,
    graph_support: GraphSupport,
    strong_seed_entity_ids: list[str],
    settings: Settings,
) -> bool:
    coherence_score = graph_support.coherence_score
    if (
        settings.news_context_artifact_path is None
        or coherence_score is None
        or coherence_score >= _HYBRID_VECTOR_PROPOSAL_MIN_COHERENCE
    ):
        return False
    return len(strong_seed_entity_ids) >= max(
        settings.graph_context_min_supporting_seeds,
        settings.news_context_min_supporting_seeds,
    )


def _enrich_graph_context_response_with_vector_proposals(
    response: TagResponse,
    *,
    settings: Settings,
    domain_hint: str | None = None,
    country_hint: str | None = None,
) -> TagResponse:
    graph_support = response.graph_support
    if graph_support is None or not graph_support.applied:
        return response
    strong_seed_entity_ids = _graph_context_strong_seed_entity_ids(response)
    if len(strong_seed_entity_ids) < settings.graph_context_min_supporting_seeds:
        return response
    # Keep the expensive vector lane asleep when the later graph gate would
    # reject every proposal anyway.
    if graph_support.coherence_score is None:
        return response
    low_coherence_recent_news_probe = (
        _should_probe_low_coherence_recent_news_support(
            graph_support=graph_support,
            strong_seed_entity_ids=strong_seed_entity_ids,
            settings=settings,
        )
    )
    if (
        graph_support.coherence_score < _HYBRID_VECTOR_PROPOSAL_MIN_COHERENCE
        and not low_coherence_recent_news_probe
    ):
        return response
    query_limit = max(
        settings.graph_context_vector_proposal_limit * 4,
        settings.graph_context_vector_proposal_limit + 4,
    )
    results_by_seed, warnings = _query_vector_results_by_seed(
        response=response,
        settings=settings,
        seed_entity_ids=strong_seed_entity_ids,
        query_limit=query_limit,
        allow_non_production=True,
        # Keep routed aliases first, but still allow the shared lane as a
        # last-resort coverage fallback when a hinted domain/country alias does
        # not actually contain the seed point.
        allow_shared_fallback=True,
        domain_hint=domain_hint,
        country_hint=country_hint,
    )
    merged_graph_support = graph_support.model_copy(deep=True)
    if low_coherence_recent_news_probe:
        warning = "vector_search_low_coherence_recent_news_probe"
        if warning not in merged_graph_support.warnings:
            merged_graph_support.warnings.append(warning)
    for warning in warnings:
        if warning not in merged_graph_support.warnings:
            merged_graph_support.warnings.append(warning)
    if not results_by_seed:
        return response.model_copy(update={"graph_support": merged_graph_support})
    vector_candidates = _aggregate_related_entities(
        results_by_seed,
        response=response,
        settings=settings,
        limit=settings.graph_context_vector_proposal_limit,
        domain_hint=domain_hint,
        country_hint=country_hint,
    )
    vetted_candidates = _graph_gate_vector_related_entities(
        vector_candidates,
        response=response,
        strong_seed_entity_ids=strong_seed_entity_ids,
        settings=settings,
    )
    recent_news_candidates: list[RelatedEntityMatch] = []
    recent_news_direct_candidates: list[RelatedEntityMatch] = []
    if settings.news_context_artifact_path is not None:
        try:
            recent_news_candidates = _recent_news_gate_vector_related_entities(
                vector_candidates,
                response=response,
                strong_seed_entity_ids=strong_seed_entity_ids,
                settings=settings,
            )
            if _should_probe_recent_news_direct_support(
                response=response,
                strong_seed_entity_ids=strong_seed_entity_ids,
                settings=settings,
                vetted_candidates=vetted_candidates,
                recent_news_candidates=recent_news_candidates,
            ):
                recent_news_direct_candidates = _recent_news_propose_related_entities(
                    response=response,
                    strong_seed_entity_ids=strong_seed_entity_ids,
                    settings=settings,
                )
                if not recent_news_direct_candidates:
                    fallback_seed_entity_ids = _recent_news_direct_fallback_seed_entity_ids(
                        response,
                        strong_seed_entity_ids=strong_seed_entity_ids,
                        domain_hint=domain_hint,
                    )
                    if fallback_seed_entity_ids != strong_seed_entity_ids:
                        recent_news_direct_candidates = (
                            _recent_news_propose_related_entities(
                                response=response,
                                strong_seed_entity_ids=fallback_seed_entity_ids,
                                settings=settings,
                            )
                        )
        except (OSError, ValueError) as exc:
            warning = f"news_context_failed:{exc}"
            if warning not in merged_graph_support.warnings:
                merged_graph_support.warnings.append(warning)
    combined_candidates = _merge_related_entities(
        vetted_candidates,
        recent_news_candidates,
        limit=settings.graph_context_related_limit,
    )
    combined_candidates = _merge_related_entities(
        combined_candidates,
        recent_news_direct_candidates,
        limit=settings.graph_context_related_limit,
    )
    merged_related_entities = _merge_related_entities(
        response.related_entities,
        combined_candidates,
        limit=settings.graph_context_related_limit,
    )
    merged_graph_support.related_entity_count = len(merged_related_entities)
    return response.model_copy(
        update={
            "related_entities": merged_related_entities,
            "graph_support": merged_graph_support,
        }
    )


def _enrich_tag_response_with_vector_search(
    response: TagResponse,
    *,
    settings: Settings,
    include_related_entities: bool = False,
    include_graph_support: bool = False,
    refine_links: bool = False,
    refinement_depth: str = "light",
    domain_hint: str | None = None,
    country_hint: str | None = None,
) -> TagResponse:
    """Optionally enrich one tag response with related QIDs and vector refinement."""

    if not include_related_entities and not include_graph_support and not refine_links:
        return response
    graph_support = GraphSupport(
        requested=True,
        applied=False,
        provider=_PROVIDER_NAME,
        collection_alias=settings.vector_search_collection_alias,
        requested_related_entities=include_related_entities,
        requested_refinement=refine_links,
        refinement_depth=refinement_depth if refine_links else None,
    )
    if settings.runtime_target is not RuntimeTarget.PRODUCTION_SERVER:
        graph_support.warnings.append("vector_search_production_only")
        return response.model_copy(update={"graph_support": graph_support})
    if not settings.vector_search_enabled:
        graph_support.warnings.append("vector_search_disabled")
        return response.model_copy(update={"graph_support": graph_support})
    if not settings.vector_search_url:
        graph_support.warnings.append("vector_search_url_missing")
        return response.model_copy(update={"graph_support": graph_support})
    seed_entity_ids = _linked_seed_entity_ids(response)
    graph_support.seed_entity_ids = seed_entity_ids
    if not seed_entity_ids:
        graph_support.warnings.append("no_wikidata_seed_entities")
        return response.model_copy(update={"graph_support": graph_support})

    query_limit = max(
        settings.vector_search_related_limit * 4,
        settings.vector_search_related_limit + 4,
    )
    results_by_seed, warnings = _query_vector_results_by_seed(
        response=response,
        settings=settings,
        seed_entity_ids=seed_entity_ids,
        query_limit=query_limit,
        domain_hint=domain_hint,
        country_hint=country_hint,
    )
    graph_support.warnings.extend(warnings)
    if not results_by_seed:
        return response.model_copy(update={"graph_support": graph_support})

    related_entities = (
        _aggregate_related_entities(
            results_by_seed,
            response=response,
            settings=settings,
            domain_hint=domain_hint,
            country_hint=country_hint,
        )
        if include_related_entities
        else []
    )
    support_by_seed = _build_seed_support(
        results_by_seed,
        seed_entity_ids=_linked_seed_entity_set(response),
    )
    refined_entities, refinement_applied, boosted_entity_ids, downgraded_entity_ids, refinement_score, refinement_debug = _refine_entities(
        response,
        support_by_seed=support_by_seed,
        seed_entity_ids=seed_entity_ids,
        refinement_depth=refinement_depth,
        refine_links=refine_links,
    )
    graph_support.applied = True
    graph_support.coherence_score = _document_coherence_score(
        support_by_seed,
        seed_entity_ids=seed_entity_ids,
    )
    graph_support.related_entity_count = len(related_entities)
    graph_support.refined_entity_count = len(boosted_entity_ids) + len(downgraded_entity_ids)
    graph_support.boosted_entity_ids = boosted_entity_ids
    graph_support.downgraded_entity_ids = downgraded_entity_ids
    return response.model_copy(
        update={
            "entities": refined_entities,
            "related_entities": related_entities,
            "refinement_applied": refinement_applied,
            "refinement_strategy": _REFINEMENT_STRATEGY if refine_links else None,
            "refinement_score": refinement_score,
            "refinement_debug": refinement_debug,
            "graph_support": graph_support,
        }
    )


def enrich_tag_response_with_related_entities(
    response: TagResponse,
    *,
    settings: Settings,
    include_related_entities: bool = False,
    include_graph_support: bool = False,
    refine_links: bool = False,
    refinement_depth: str = "light",
    domain_hint: str | None = None,
    country_hint: str | None = None,
) -> TagResponse:
    """Optionally enrich one tag response with related QIDs and graph refinement."""

    if not include_related_entities and not include_graph_support and not refine_links:
        return response

    working_response = response
    graph_context_response: TagResponse | None = None
    if settings.graph_context_enabled and (
        include_related_entities or include_graph_support or refine_links
    ):
        graph_context_response = apply_graph_context(
            response,
            settings=settings,
            include_related_entities=include_related_entities,
            include_graph_support=include_graph_support,
            refine_links=refine_links,
            refinement_depth=refinement_depth,
        )
        working_response = graph_context_response

    if graph_context_response is not None and graph_context_response.graph_support is not None:
        if graph_context_response.graph_support.applied:
            if (
                include_related_entities
                and settings.graph_context_vector_proposals_enabled
            ):
                return _enrich_graph_context_response_with_vector_proposals(
                    graph_context_response,
                    settings=settings,
                    domain_hint=domain_hint,
                    country_hint=country_hint,
                )
            return graph_context_response

    if not include_related_entities:
        if graph_context_response is not None:
            return graph_context_response
        return _enrich_tag_response_with_vector_search(
            working_response,
            settings=settings,
            include_related_entities=include_related_entities,
            include_graph_support=include_graph_support,
            refine_links=refine_links,
            refinement_depth=refinement_depth,
            domain_hint=domain_hint,
            country_hint=country_hint,
        )

    vector_response = _enrich_tag_response_with_vector_search(
        working_response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=graph_context_response is None and include_graph_support,
        refine_links=graph_context_response is None and refine_links,
        refinement_depth=refinement_depth,
        domain_hint=domain_hint,
        country_hint=country_hint,
    )
    if graph_context_response is None:
        return vector_response

    merged_graph_support = (
        graph_context_response.graph_support.model_copy(deep=True)
        if graph_context_response.graph_support is not None
        else None
    )
    if merged_graph_support is not None:
        merged_graph_support.related_entity_count = len(vector_response.related_entities)
    return vector_response.model_copy(
        update={
            "entities": graph_context_response.entities,
            "refinement_applied": graph_context_response.refinement_applied,
            "refinement_strategy": graph_context_response.refinement_strategy,
            "refinement_score": graph_context_response.refinement_score,
            "refinement_debug": graph_context_response.refinement_debug,
            "graph_support": merged_graph_support,
        }
    )
