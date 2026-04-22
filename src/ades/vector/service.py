"""Hosted post-link enrichment and graph-aware refinement."""

from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean

from ..config import Settings
from ..service.models import GraphSupport, RelatedEntityMatch, TagResponse
from ..storage import RuntimeTarget
from .builder import (
    DEFAULT_QID_GRAPH_ALLOWED_PREDICATES,
    DEFAULT_QID_GRAPH_DIMENSIONS,
    build_dense_vector_from_graph_store,
)
from .context_engine import apply_graph_context
from .graph_store import QidGraphStore
from .qdrant import QdrantNearestPoint, QdrantVectorSearchClient, QdrantVectorSearchError

_PROVIDER_NAME = "qdrant.qid_graph"
_REFINEMENT_STRATEGY = "qid_graph_coherence_v1"
_HYBRID_VECTOR_PROPOSAL_MIN_COHERENCE = 0.12
_HYBRID_VECTOR_QUERY_SEED_LIMIT = 4
_GENERIC_SHARED_CONTEXT_QIDS = {"Q5"}
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
    limit: int | None = None,
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

    filter_payload = _query_filter_payload(response)
    warnings: list[str] = []
    results_by_seed: dict[str, list[QdrantNearestPoint]] = {}
    graph_store: QidGraphStore | None = None
    try:
        if settings.graph_context_artifact_path is not None:
            graph_store = QidGraphStore(settings.graph_context_artifact_path)
        with QdrantVectorSearchClient(
            settings.vector_search_url,
            api_key=settings.vector_search_api_key,
        ) as client:
            for seed_entity_id in seed_entity_ids:
                try:
                    results_by_seed[seed_entity_id] = client.query_similar_by_id(
                        settings.vector_search_collection_alias,
                        point_id=seed_entity_id,
                        limit=query_limit,
                        filter_payload=filter_payload,
                    )
                except QdrantVectorSearchError as exc:
                    if exc.status_code == 404:
                        fallback_vector = None
                        if graph_store is not None:
                            fallback_vector = build_dense_vector_from_graph_store(
                                graph_store,
                                seed_entity_id.removeprefix("wikidata:"),
                                dimensions=DEFAULT_QID_GRAPH_DIMENSIONS,
                                allowed_predicates=DEFAULT_QID_GRAPH_ALLOWED_PREDICATES,
                            )
                        if fallback_vector is not None:
                            try:
                                results_by_seed[seed_entity_id] = client.query_similar_by_vector(
                                    settings.vector_search_collection_alias,
                                    vector=fallback_vector,
                                    limit=query_limit,
                                    filter_payload=filter_payload,
                                )
                            except QdrantVectorSearchError as fallback_exc:
                                warnings.append(f"vector_search_failed:{fallback_exc}")
                                return {}, warnings
                            continue
                        warnings.append(f"vector_search_seed_missing:{seed_entity_id}")
                        continue
                    raise
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
    refinement_debug = response.refinement_debug
    if isinstance(refinement_debug, dict):
        raw_seed_decisions = refinement_debug.get("seed_decisions")
        if isinstance(raw_seed_decisions, list):
            strong_seed_ids: list[str] = []
            for item in raw_seed_decisions:
                if not isinstance(item, dict):
                    continue
                entity_id = str(item.get("entity_id") or "").strip()
                action = str(item.get("action") or "").strip()
                if action not in {"keep", "boost"}:
                    continue
                if not entity_id.startswith("wikidata:Q") or entity_id in seen:
                    continue
                seen.add(entity_id)
                strong_seed_ids.append(entity_id)
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
        if entity_id in suppressed or entity_id in downgraded or entity_id in seen:
            continue
        seen.add(entity_id)
        strong_seed_ids.append(entity_id)
    return strong_seed_ids


def _graph_context_vector_query_seed_entity_ids(response: TagResponse) -> list[str]:
    entity_label_by_id = {
        entity.link.entity_id: entity.label
        for entity in response.entities
        if entity.link is not None and entity.link.entity_id
    }
    refinement_debug = response.refinement_debug
    if not isinstance(refinement_debug, dict):
        return _graph_context_strong_seed_entity_ids(response)[
            :_HYBRID_VECTOR_QUERY_SEED_LIMIT
        ]

    raw_seed_decisions = refinement_debug.get("seed_decisions")
    if not isinstance(raw_seed_decisions, list):
        return _graph_context_strong_seed_entity_ids(response)[
            :_HYBRID_VECTOR_QUERY_SEED_LIMIT
        ]

    scored_non_person: list[tuple[float, str]] = []
    scored_fallback: list[tuple[float, str]] = []
    seen: set[str] = set()
    for item in raw_seed_decisions:
        if not isinstance(item, dict):
            continue
        entity_id = str(item.get("entity_id") or "").strip()
        action = str(item.get("action") or "").strip()
        if action not in {"keep", "boost"}:
            continue
        if not entity_id.startswith("wikidata:Q") or entity_id in seen:
            continue
        seen.add(entity_id)
        shared_context = {
            str(value).strip()
            for value in item.get("shared_context_qids") or []
            if str(value).strip()
        }
        generic_shared_context_only = (
            bool(shared_context)
            and shared_context.issubset(_GENERIC_SHARED_CONTEXT_QIDS)
        )
        quality = float(item.get("cluster_fit") or 0.0)
        quality += float(item.get("mean_peer_support") or 0.0)
        quality += min(int(item.get("supporting_seed_count") or 0), 4) * 0.08
        quality -= float(item.get("genericity_penalty") or 0.0)
        if action == "boost":
            quality += 0.2
        if generic_shared_context_only:
            quality -= 0.2
        scored_fallback.append((quality, entity_id))
        if entity_label_by_id.get(entity_id) == "person":
            continue
        if quality < 0.1:
            continue
        scored_non_person.append((quality, entity_id))

    selected: list[str] = []
    for _, entity_id in sorted(scored_non_person, reverse=True):
        if entity_id not in selected:
            selected.append(entity_id)
        if len(selected) >= _HYBRID_VECTOR_QUERY_SEED_LIMIT:
            return selected
    for _, entity_id in sorted(scored_fallback, reverse=True):
        if entity_id not in selected:
            selected.append(entity_id)
        if len(selected) >= _HYBRID_VECTOR_QUERY_SEED_LIMIT:
            break
    return selected


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
    with QidGraphStore(settings.graph_context_artifact_path) as store:
        metadata_by_qid = store.node_metadata_batch((*strong_seed_qids, *candidate_qids))
        neighbors_by_qid = store.neighbors_batch(
            (*strong_seed_qids, *candidate_qids),
            direction="both",
            limit_per_qid=settings.graph_context_seed_neighbor_limit,
        )

    seed_neighbor_sets = {
        seed_qid: {neighbor.qid for neighbor in neighbors_by_qid.get(seed_qid, [])}
        for seed_qid in strong_seed_qids
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
        if len(supporting_seed_ids) < settings.graph_context_min_supporting_seeds:
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


def _enrich_graph_context_response_with_vector_proposals(
    response: TagResponse,
    *,
    settings: Settings,
) -> TagResponse:
    graph_support = response.graph_support
    if graph_support is None or not graph_support.applied:
        return response
    strong_seed_entity_ids = _graph_context_strong_seed_entity_ids(response)
    if len(strong_seed_entity_ids) < settings.graph_context_min_supporting_seeds:
        return response
    proposal_seed_entity_ids = _graph_context_vector_query_seed_entity_ids(response)
    if len(proposal_seed_entity_ids) < settings.graph_context_min_supporting_seeds:
        return response
    query_limit = max(
        settings.graph_context_vector_proposal_limit * 2,
        settings.graph_context_vector_proposal_limit + 2,
    )
    results_by_seed, warnings = _query_vector_results_by_seed(
        response=response,
        settings=settings,
        seed_entity_ids=proposal_seed_entity_ids,
        query_limit=query_limit,
        allow_non_production=True,
    )
    merged_graph_support = graph_support.model_copy(deep=True)
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
    )
    vetted_candidates = _graph_gate_vector_related_entities(
        vector_candidates,
        response=response,
        strong_seed_entity_ids=strong_seed_entity_ids,
        settings=settings,
    )
    merged_related_entities = _merge_related_entities(
        response.related_entities,
        vetted_candidates,
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
    )
    graph_support.warnings.extend(warnings)
    if not results_by_seed:
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
        )

    vector_response = _enrich_tag_response_with_vector_search(
        working_response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=graph_context_response is None and include_graph_support,
        refine_links=graph_context_response is None and refine_links,
        refinement_depth=refinement_depth,
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
