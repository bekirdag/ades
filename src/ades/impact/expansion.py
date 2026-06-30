"""Deterministic market impact path expansion."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Iterable

from ..config import Settings, get_settings
from ..service.models import (
    EntityMatch,
    ImpactCandidate,
    ImpactExpansionResult,
    ImpactPassivePath,
    ImpactPathEdge,
    ImpactRelationshipPath,
    ImpactSourceEntity,
    TagResponse,
)
from .graph_store import MarketGraphEdge, MarketGraphNode, MarketGraphStore
from .source_catalog import classify_source_tier

_DEPTH_DECAY = {0: 1.0, 1: 0.9, 2: 0.75}
_MAX_DEPTH_CAP = 4
_DIRECT_TERMINAL_REF_TYPES = {
    "ades:impact:commodity:": "commodity",
    "ades:impact:currency:": "currency",
    "ades:impact:index:": "market_index",
    "ades:impact:rate:": "policy_rate",
}
_EVENT_GATED_PROXY_RELATIONS = {
    "country_affects_country_risk_proxy",
    "country_affects_currency_index",
    "country_affects_dxy_proxy",
    "country_affects_global_equity_proxy",
    "country_affects_policy_rate_proxy",
    "entity_affects_country_risk_proxy",
    "entity_affects_dxy_proxy",
    "entity_affects_global_equity_proxy",
    "entity_affects_global_policy_risk_proxy",
    "entity_affects_policy_rate_proxy",
}


@dataclass(frozen=True)
class _MentionMetadata:
    entity_ref: str
    name: str
    entity_type: str | None = None
    library_id: str | None = None
    confidence: float = 1.0


@dataclass(frozen=True)
class _TraversalState:
    seed_ref: str
    current_ref: str
    depth: int
    edges: tuple[MarketGraphEdge, ...]
    visited_refs: tuple[str, ...]
    edge_confidence: float


def _normalize_refs(entity_refs: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(ref).strip() for ref in entity_refs if str(ref).strip()))


def _heuristic_structural_equivalent_refs(entity_ref: str) -> tuple[str, ...]:
    parts = entity_ref.split(":")
    if len(parts) < 5 or parts[0] != "ades":
        return ()
    if not parts[1].startswith("heuristic_structural_"):
        return ()
    equivalent_refs: list[str] = []
    for pack_id in ("general-en", "finance-en"):
        if parts[2] == pack_id:
            continue
        candidate_parts = list(parts)
        candidate_parts[2] = pack_id
        equivalent_refs.append(":".join(candidate_parts))
    return tuple(equivalent_refs)


def _candidate_refs_for_requested(
    requested_refs: tuple[str, ...],
    *,
    store: MarketGraphStore,
) -> dict[str, tuple[str, ...]]:
    base_candidates_by_requested = {
        entity_ref: _normalize_refs((entity_ref, *_heuristic_structural_equivalent_refs(entity_ref)))
        for entity_ref in requested_refs
    }
    bridge_lookup_refs = _normalize_refs(
        candidate
        for candidates in base_candidates_by_requested.values()
        for candidate in candidates
    )
    bridge_refs_by_lookup = store.bridge_refs_batch(bridge_lookup_refs)

    requested_ref_candidates: dict[str, tuple[str, ...]] = {}
    for entity_ref, base_candidates in base_candidates_by_requested.items():
        candidate_refs: list[str] = []
        for candidate_ref in base_candidates:
            candidate_refs.append(candidate_ref)
            for bridge_ref in bridge_refs_by_lookup.get(candidate_ref, ()):
                candidate_refs.append(bridge_ref)
                candidate_refs.extend(_heuristic_structural_equivalent_refs(bridge_ref))
        requested_ref_candidates[entity_ref] = _normalize_refs(candidate_refs)
    return requested_ref_candidates


def _clamp_confidence(value: float | None) -> float:
    if value is None:
        return 1.0
    return min(max(float(value), 0.0), 1.0)


def _candidate_confidence(seed_confidence: float, edge_confidence: float, depth: int) -> float:
    decay = _DEPTH_DECAY.get(depth, _DEPTH_DECAY[2] * (0.5 ** max(depth - 2, 0)))
    return round(_clamp_confidence(seed_confidence) * edge_confidence * decay, 6)


def _edge_effective_confidence(edge: MarketGraphEdge) -> float:
    evidence_weight = {
        "direct": 1.0,
        "shallow": 0.92,
        "inferred": 0.78,
    }.get(edge.evidence_level, 0.75)
    source_weight = 1.0
    source_url = edge.source_url.casefold()
    source_name = edge.source_name.casefold()
    if not (
        ".gov" in source_url
        or ".int" in source_url
        or "central bank" in source_name
        or "federal reserve" in source_name
        or "cme " in source_name
        or "eia " in source_name
        or "ofac" in source_name
        or "opec" in source_name
        or "usda" in source_name
    ):
        source_weight = 0.94
    return _clamp_confidence(edge.confidence * evidence_weight * source_weight)


def _mentions_from_entities(entities: Iterable[EntityMatch]) -> dict[str, _MentionMetadata]:
    mentions: dict[str, _MentionMetadata] = {}
    for entity in entities:
        if entity.link is None:
            continue
        entity_ref = entity.link.entity_id.strip()
        if not entity_ref:
            continue
        confidence = entity.confidence if entity.confidence is not None else entity.relevance
        current = mentions.get(entity_ref)
        metadata = _MentionMetadata(
            entity_ref=entity_ref,
            name=entity.link.canonical_text or entity.text,
            entity_type=entity.label,
            library_id=entity.provenance.source_pack if entity.provenance is not None else None,
            confidence=_clamp_confidence(confidence),
        )
        if current is None or metadata.confidence > current.confidence:
            mentions[entity_ref] = metadata
    return mentions


def _metadata_for_ref(
    entity_ref: str,
    *,
    node: MarketGraphNode | None,
    mentions: dict[str, _MentionMetadata],
) -> _MentionMetadata:
    mention = mentions.get(entity_ref)
    if mention is not None:
        return mention
    if node is not None:
        return _MentionMetadata(
            entity_ref=entity_ref,
            name=node.canonical_name,
            entity_type=node.entity_type,
            library_id=node.library_id,
            confidence=1.0,
        )
    return _MentionMetadata(entity_ref=entity_ref, name=entity_ref)


def _direct_terminal_type_for_ref(entity_ref: str) -> str | None:
    normalized = entity_ref.casefold()
    for prefix, entity_type in _DIRECT_TERMINAL_REF_TYPES.items():
        if normalized.startswith(prefix):
            return entity_type
    return None


def _direct_terminal_name_for_ref(entity_ref: str, entity_type: str) -> str:
    slug = entity_ref.rsplit(":", 1)[-1].replace("-", " ").strip()
    if not slug:
        return entity_ref
    if entity_type == "currency":
        return slug.upper()
    return slug.title()


def _edge_effective_from(edge: MarketGraphEdge) -> str | None:
    if edge.source_year is None:
        return None
    return f"{edge.source_year:04d}-01-01"


def _path_edges(edges: tuple[MarketGraphEdge, ...]) -> list[ImpactPathEdge]:
    return [
        ImpactPathEdge(
            source_ref=edge.source_ref,
            target_ref=edge.target_ref,
            relation=edge.relation,
            evidence_level=edge.evidence_level,
            confidence=edge.confidence,
            direction_hint=edge.direction_hint,
            source_name=edge.source_name,
            source_url=edge.source_url,
            source_snapshot=edge.source_snapshot,
            source_year=edge.source_year,
            source_tier=classify_source_tier(edge.source_name, edge.source_url),
            effective_from=_edge_effective_from(edge),
            effective_to=None,
            compatible_event_types=list(edge.compatible_event_types),
            direction_preconditions=list(edge.direction_preconditions),
        )
        for edge in edges
    ]


def _relationship_path(edges: tuple[MarketGraphEdge, ...]) -> ImpactRelationshipPath:
    return ImpactRelationshipPath(path_depth=len(edges), edges=_path_edges(edges))


def _path_sort_key(path: ImpactRelationshipPath) -> tuple[object, ...]:
    edge_key = tuple(
        (edge.source_ref, edge.relation, edge.target_ref, edge.source_snapshot)
        for edge in path.edges
    )
    min_edge_confidence = min((edge.confidence for edge in path.edges), default=1.0)
    return (path.path_depth, -min_edge_confidence, edge_key)


def _merge_path(
    paths: list[ImpactRelationshipPath],
    path: ImpactRelationshipPath,
    *,
    max_paths: int,
) -> list[ImpactRelationshipPath]:
    path_signature = tuple(
        (edge.source_ref, edge.relation, edge.target_ref, edge.source_snapshot)
        for edge in path.edges
    )
    existing_signatures = {
        tuple((edge.source_ref, edge.relation, edge.target_ref, edge.source_snapshot) for edge in item.edges)
        for item in paths
    }
    if path_signature not in existing_signatures:
        paths.append(path)
    paths.sort(key=_path_sort_key)
    return paths[:max_paths]


def _event_types_from_paths(paths: Iterable[ImpactRelationshipPath]) -> list[str]:
    return sorted(
        {
            event_type
            for path in paths
            for edge in path.edges
            for event_type in edge.compatible_event_types
        }
    )


def _edge_allowed_by_event_context(
    edge: MarketGraphEdge,
    compatible_event_types: set[str],
) -> bool:
    if edge.relation not in _EVENT_GATED_PROXY_RELATIONS:
        return True
    event_context = {event.casefold() for event in compatible_event_types}
    edge_event_types = {event.casefold() for event in edge.compatible_event_types}
    if not edge_event_types:
        return False
    return bool(edge_event_types & event_context)


def _candidate_sort_key(candidate: ImpactCandidate) -> tuple[object, ...]:
    shortest_depth = min(
        (path.path_depth for path in candidate.relationship_paths),
        default=0,
    )
    return (
        -len(candidate.source_entity_refs),
        shortest_depth,
        -candidate.confidence,
        candidate.entity_ref,
    )


def _artifact_warning_result(warning: str) -> ImpactExpansionResult:
    return ImpactExpansionResult(warnings=[warning], source_entities=[], candidates=[], passive_paths=[])


def _resolve_artifact_path(
    *,
    settings: Settings | None,
    artifact_path: str | Path | None,
) -> Path | None:
    if artifact_path is not None:
        return Path(artifact_path).expanduser()
    if settings is not None:
        return settings.impact_expansion_artifact_path
    return None


def expand_impact_paths(
    entity_refs: Iterable[str],
    *,
    language: str = "en",
    enabled_packs: Iterable[str] | None = None,
    max_depth: int | None = None,
    impact_expansion_seed_limit: int | None = None,
    max_candidates: int | None = None,
    include_passive_paths: bool = True,
    vector_proposals_enabled: bool | None = None,
    settings: Settings | None = None,
    artifact_path: str | Path | None = None,
    source_mentions: dict[str, _MentionMetadata] | None = None,
    max_edges_per_seed: int | None = None,
    max_paths_per_candidate: int | None = None,
    compatible_event_types: Iterable[str] | None = None,
    storage_root: str | Path | None = None,
) -> ImpactExpansionResult:
    """Expand extracted ADES entity refs into deterministic market impact candidates."""

    del language, storage_root
    resolved_settings = settings if settings is not None else get_settings()
    if not resolved_settings.impact_expansion_enabled:
        return _artifact_warning_result("impact_expansion_disabled")

    resolved_artifact_path = _resolve_artifact_path(
        settings=resolved_settings,
        artifact_path=artifact_path,
    )
    if resolved_artifact_path is None:
        return _artifact_warning_result("market_graph_artifact_missing")

    requested_refs = _normalize_refs(entity_refs)
    resolved_max_depth = min(
        max(max_depth if max_depth is not None else resolved_settings.impact_expansion_max_depth, 1),
        _MAX_DEPTH_CAP,
    )
    resolved_seed_limit = max(
        impact_expansion_seed_limit
        if impact_expansion_seed_limit is not None
        else resolved_settings.impact_expansion_seed_limit,
        1,
    )
    resolved_max_candidates = max(
        max_candidates
        if max_candidates is not None
        else resolved_settings.impact_expansion_max_candidates,
        0,
    )
    resolved_edge_limit = max(
        max_edges_per_seed
        if max_edges_per_seed is not None
        else resolved_settings.impact_expansion_max_edges_per_seed,
        1,
    )
    resolved_path_limit = max(
        max_paths_per_candidate
        if max_paths_per_candidate is not None
        else resolved_settings.impact_expansion_max_paths_per_candidate,
        1,
    )
    resolved_vector_proposals_enabled = (
        vector_proposals_enabled
        if vector_proposals_enabled is not None
        else resolved_settings.impact_expansion_vector_proposals_enabled
    )
    warnings: list[str] = []
    event_context = {event.casefold() for event in compatible_event_types or () if event}
    if resolved_vector_proposals_enabled:
        warnings.append("impact_vector_proposals_not_implemented")

    mentions = dict(source_mentions or {})
    try:
        with MarketGraphStore(resolved_artifact_path) as store:
            metadata = store.metadata()
            return _expand_with_store(
                store,
                metadata=metadata,
                requested_refs=requested_refs,
                mentions=mentions,
                enabled_packs=enabled_packs,
                max_depth=resolved_max_depth,
                seed_limit=resolved_seed_limit,
                max_candidates=resolved_max_candidates,
                max_edges_per_seed=resolved_edge_limit,
                max_paths_per_candidate=resolved_path_limit,
                include_passive_paths=include_passive_paths,
                warnings=warnings,
                compatible_event_types=event_context,
            )
    except FileNotFoundError:
        return _artifact_warning_result("market_graph_artifact_missing")
    except (sqlite3.Error, ValueError):
        return _artifact_warning_result("market_graph_artifact_unreadable")


def _expand_with_store(
    store: MarketGraphStore,
    *,
    metadata: dict[str, object],
    requested_refs: tuple[str, ...],
    mentions: dict[str, _MentionMetadata],
    enabled_packs: Iterable[str] | None,
    max_depth: int,
    seed_limit: int,
    max_candidates: int,
    max_edges_per_seed: int,
    max_paths_per_candidate: int,
    include_passive_paths: bool,
    warnings: list[str],
    compatible_event_types: set[str],
) -> ImpactExpansionResult:
    requested_ref_candidates = _candidate_refs_for_requested(requested_refs, store=store)
    lookup_refs = _normalize_refs(
        candidate
        for candidates in requested_ref_candidates.values()
        for candidate in candidates
    )
    nodes = store.node_batch(lookup_refs)
    graph_ref_candidates_by_requested = {
        entity_ref: tuple(candidate for candidate in candidates if candidate in nodes)
        for entity_ref, candidates in requested_ref_candidates.items()
    }
    primary_graph_ref_by_requested = {
        entity_ref: next(
            (candidate for candidate in candidates if candidate in nodes),
            entity_ref,
        )
        for entity_ref, candidates in requested_ref_candidates.items()
    }
    identity_refs_by_entity = store.identity_refs_for_entity_batch(
        candidate
        for candidates in graph_ref_candidates_by_requested.values()
        for candidate in candidates
    )
    same_as_refs_by_requested: dict[str, tuple[str, ...]] = {}
    for entity_ref, graph_ref_candidates in graph_ref_candidates_by_requested.items():
        same_as_values: list[str] = [
            candidate
            for candidate in graph_ref_candidates
            if candidate != entity_ref
        ]
        for graph_ref in graph_ref_candidates:
            same_as_values.extend(identity_refs_by_entity.get(graph_ref, ()))
        same_as_refs_by_requested[entity_ref] = tuple(
            ref for ref in _normalize_refs(same_as_values) if ref != entity_ref
        )
    source_entities: list[ImpactSourceEntity] = []
    candidates_by_ref: dict[str, ImpactCandidate] = {}
    passive_by_ref: dict[str, ImpactPassivePath] = {}
    seed_confidence: dict[str, float] = {}
    eligible_seed_refs = [
        entity_ref
        for entity_ref in requested_refs
        if any(
            (node := nodes.get(graph_ref)) is not None
            and node.is_seed_eligible
            and node.seed_degree > 0
            for graph_ref in graph_ref_candidates_by_requested.get(entity_ref, ())
        )
    ]
    seed_refs = eligible_seed_refs[:seed_limit]
    seed_ref_set = set(seed_refs)
    if len(eligible_seed_refs) > len(seed_refs):
        warnings.append("impact_seed_limit_truncated")

    for entity_ref in requested_refs:
        traversal_ref = primary_graph_ref_by_requested.get(entity_ref, entity_ref)
        graph_ref_candidates = graph_ref_candidates_by_requested.get(entity_ref, ())
        node = nodes.get(entity_ref) or nodes.get(traversal_ref)
        metadata_for_ref = _metadata_for_ref(entity_ref, node=node, mentions=mentions)
        direct_terminal_type = _direct_terminal_type_for_ref(entity_ref) if node is None else None
        direct_terminal_name = (
            metadata_for_ref.name
            if direct_terminal_type is None or metadata_for_ref.name != entity_ref
            else _direct_terminal_name_for_ref(entity_ref, direct_terminal_type)
        )
        is_graph_seed = entity_ref in seed_ref_set
        seed_confidence[entity_ref] = metadata_for_ref.confidence
        seed_degree = max(
            (nodes[graph_ref].seed_degree for graph_ref in graph_ref_candidates if graph_ref in nodes),
            default=0,
        )
        is_tradable = any(
            nodes[graph_ref].is_tradable for graph_ref in graph_ref_candidates if graph_ref in nodes
        ) or direct_terminal_type is not None
        source_entities.append(
            ImpactSourceEntity(
                entity_ref=entity_ref,
                name=direct_terminal_name,
                entity_type=metadata_for_ref.entity_type
                or (node.entity_type if node else direct_terminal_type),
                library_id=metadata_for_ref.library_id or (node.library_id if node else None),
                is_graph_seed=is_graph_seed,
                seed_degree=seed_degree,
                is_tradable=is_tradable,
                same_as_refs=list(same_as_refs_by_requested.get(entity_ref, ())),
            )
        )
        if direct_terminal_type is not None and not graph_ref_candidates:
            candidates_by_ref[entity_ref] = ImpactCandidate(
                entity_ref=entity_ref,
                name=direct_terminal_name,
                entity_type=direct_terminal_type,
                is_tradable=True,
                evidence_level="direct",
                confidence=metadata_for_ref.confidence,
                source_entity_refs=[entity_ref],
                relationship_paths=[],
            )
            continue
        for graph_ref in graph_ref_candidates:
            graph_node = nodes.get(graph_ref)
            if graph_node is None or not graph_node.is_tradable:
                continue
            existing = candidates_by_ref.get(graph_ref)
            if existing is None:
                candidates_by_ref[graph_ref] = ImpactCandidate(
                    entity_ref=graph_ref,
                    name=graph_node.canonical_name,
                    entity_type=graph_node.entity_type,
                    is_tradable=True,
                    evidence_level="direct",
                    confidence=metadata_for_ref.confidence,
                    source_entity_refs=[entity_ref],
                    relationship_paths=[],
                )
                continue
            candidates_by_ref[graph_ref] = existing.model_copy(
                update={
                    "confidence": max(existing.confidence, metadata_for_ref.confidence),
                    "evidence_level": "direct",
                    "source_entity_refs": sorted(
                        set(existing.source_entity_refs) | {entity_ref}
                    ),
                }
            )

    queue: list[_TraversalState] = []
    for seed_ref in seed_refs:
        for graph_ref in graph_ref_candidates_by_requested.get(seed_ref, ()):
            graph_node = nodes.get(graph_ref)
            if graph_node is None or not graph_node.is_seed_eligible or graph_node.seed_degree <= 0:
                continue
            queue.append(
                _TraversalState(
                    seed_ref=seed_ref,
                    current_ref=graph_ref,
                    depth=0,
                    edges=(),
                    visited_refs=(graph_ref,),
                    edge_confidence=1.0,
                )
            )
    while queue:
        state = queue.pop(0)
        if state.depth >= max_depth:
            continue
        outbound_edges = store.outbound_edges_batch(
            [state.current_ref],
            enabled_packs=enabled_packs,
            limit_per_source=max_edges_per_seed,
        ).get(state.current_ref, [])
        for edge in outbound_edges:
            if not _edge_allowed_by_event_context(edge, compatible_event_types):
                continue
            if edge.target_ref in state.visited_refs:
                continue
            next_depth = state.depth + 1
            next_edges = (*state.edges, edge)
            next_confidence = min(state.edge_confidence, _edge_effective_confidence(edge))
            target_node = store.node(edge.target_ref)
            if target_node is None:
                continue
            relationship_path = _relationship_path(next_edges)
            source_refs = [state.seed_ref]
            if target_node.is_tradable:
                candidate_confidence = _candidate_confidence(
                    seed_confidence.get(state.seed_ref, 1.0),
                    next_confidence,
                    next_depth,
                )
                existing = candidates_by_ref.get(edge.target_ref)
                evidence_level = "shallow" if next_depth == 1 else "inferred"
                if existing is None:
                    candidate_event_types = _event_types_from_paths([relationship_path])
                    candidates_by_ref[edge.target_ref] = ImpactCandidate(
                        entity_ref=edge.target_ref,
                        name=target_node.canonical_name,
                        entity_type=target_node.entity_type,
                        is_tradable=True,
                        evidence_level=evidence_level,
                        confidence=candidate_confidence,
                        source_entity_refs=source_refs,
                        relationship_paths=[relationship_path],
                        compatible_event_types=candidate_event_types,
                    )
                else:
                    source_entity_refs = sorted(set(existing.source_entity_refs) | set(source_refs))
                    relationship_paths = _merge_path(
                        list(existing.relationship_paths),
                        relationship_path,
                        max_paths=max_paths_per_candidate,
                    )
                    merged_evidence = (
                        "direct"
                        if existing.evidence_level == "direct"
                        else ("shallow" if any(path.path_depth == 1 for path in relationship_paths) else "inferred")
                    )
                    candidates_by_ref[edge.target_ref] = existing.model_copy(
                        update={
                            "confidence": max(existing.confidence, candidate_confidence),
                            "source_entity_refs": source_entity_refs,
                            "relationship_paths": relationship_paths,
                            "evidence_level": merged_evidence,
                            "compatible_event_types": _event_types_from_paths(
                                relationship_paths
                            ),
                        }
                    )
            elif include_passive_paths:
                existing_passive = passive_by_ref.get(edge.target_ref)
                if existing_passive is None:
                    passive_by_ref[edge.target_ref] = ImpactPassivePath(
                        entity_ref=edge.target_ref,
                        name=target_node.canonical_name,
                        entity_type=target_node.entity_type,
                        source_entity_refs=source_refs,
                        relationship_paths=[relationship_path],
                    )
                else:
                    passive_by_ref[edge.target_ref] = existing_passive.model_copy(
                        update={
                            "source_entity_refs": sorted(
                                set(existing_passive.source_entity_refs) | set(source_refs)
                            ),
                            "relationship_paths": _merge_path(
                                list(existing_passive.relationship_paths),
                                relationship_path,
                                max_paths=max_paths_per_candidate,
                            ),
                        }
                    )
            if next_depth < max_depth and target_node.seed_degree > 0:
                queue.append(
                    _TraversalState(
                        seed_ref=state.seed_ref,
                        current_ref=edge.target_ref,
                        depth=next_depth,
                        edges=next_edges,
                        visited_refs=(*state.visited_refs, edge.target_ref),
                        edge_confidence=next_confidence,
                    )
                )

    candidates = sorted(candidates_by_ref.values(), key=_candidate_sort_key)
    if max_candidates >= 0:
        candidates = candidates[:max_candidates]
    passive_paths = sorted(
        passive_by_ref.values(),
        key=lambda item: (
            min((path.path_depth for path in item.relationship_paths), default=0),
            item.entity_ref,
        ),
    )
    return ImpactExpansionResult(
        graph_version=str(metadata["graph_version"]),
        artifact_version=str(metadata["artifact_version"]),
        artifact_hash=str(metadata["artifact_hash"]),
        warnings=warnings,
        source_entities=source_entities,
        candidates=candidates,
        passive_paths=passive_paths,
    )


def enrich_tag_response_with_impact_paths(
    response: TagResponse,
    *,
    settings: Settings,
    include_impact_paths: bool = False,
    enabled_packs: Iterable[str] | None = None,
    max_depth: int | None = None,
    impact_expansion_seed_limit: int | None = None,
    max_candidates: int | None = None,
    include_passive_paths: bool = True,
    vector_proposals_enabled: bool | None = None,
) -> TagResponse:
    """Optionally attach impact path expansion to a tag response."""

    if not include_impact_paths:
        return response
    mentions = _mentions_from_entities(response.entities)
    result = expand_impact_paths(
        mentions.keys(),
        enabled_packs=enabled_packs,
        max_depth=max_depth,
        impact_expansion_seed_limit=impact_expansion_seed_limit,
        max_candidates=max_candidates,
        include_passive_paths=include_passive_paths,
        vector_proposals_enabled=vector_proposals_enabled,
        settings=settings,
        source_mentions=mentions,
    )
    if result.warnings:
        merged_warnings = list(response.warnings)
        for warning in result.warnings:
            if warning not in merged_warnings:
                merged_warnings.append(warning)
        return response.model_copy(update={"impact_paths": result, "warnings": merged_warnings})
    return response.model_copy(update={"impact_paths": result})
