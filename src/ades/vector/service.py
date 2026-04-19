"""Hosted post-link enrichment and graph-aware refinement."""

from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean

from ..config import Settings
from ..service.models import GraphSupport, RelatedEntityMatch, TagResponse
from ..storage import RuntimeTarget
from .qdrant import QdrantNearestPoint, QdrantVectorSearchClient, QdrantVectorSearchError

_PROVIDER_NAME = "qdrant.qid_graph"
_REFINEMENT_STRATEGY = "qid_graph_coherence_v1"
_LOW_CONFIDENCE_THRESHOLD_BY_DEPTH = {
    "light": 0.65,
    "deep": 0.8,
}
_DOWNGRADE_THRESHOLD = 0.45
_MIN_REFINEMENT_DELTA = 0.02


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


def _normalize_payload_packs(payload: dict[str, object]) -> list[str]:
    raw_value = payload.get("packs")
    if not isinstance(raw_value, list):
        return []
    return [str(item).strip() for item in raw_value if str(item).strip()]


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


def _query_filter_payload(response: TagResponse) -> dict[str, object] | None:
    if not response.pack:
        return None
    return {"packs": [response.pack]}


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
) -> list[RelatedEntityMatch]:
    seed_labels = _seed_labels(response)
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
            if response.pack and candidate_packs and response.pack not in candidate_packs:
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
    limited = ranked[: settings.vector_search_related_limit]
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


def enrich_tag_response_with_related_entities(
    response: TagResponse,
    *,
    settings: Settings,
    include_related_entities: bool = False,
    include_graph_support: bool = False,
    refine_links: bool = False,
    refinement_depth: str = "light",
) -> TagResponse:
    """Optionally enrich one tag response with related QIDs and graph refinement."""

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

    query_limit = max(settings.vector_search_related_limit * 4, settings.vector_search_related_limit + 4)
    results_by_seed: dict[str, list[QdrantNearestPoint]] = {}
    filter_payload = _query_filter_payload(response)
    try:
        with QdrantVectorSearchClient(
            settings.vector_search_url,
            api_key=settings.vector_search_api_key,
        ) as client:
            for seed_entity_id in seed_entity_ids:
                results_by_seed[seed_entity_id] = client.query_similar_by_id(
                    settings.vector_search_collection_alias,
                    point_id=seed_entity_id,
                    limit=query_limit,
                    filter_payload=filter_payload,
                )
    except QdrantVectorSearchError as exc:
        graph_support.warnings.append(f"vector_search_failed:{exc}")
        return response.model_copy(update={"graph_support": graph_support})

    related_entities = (
        _aggregate_related_entities(
            results_by_seed,
            response=response,
            settings=settings,
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
