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

_DEPTH_DECAY = {0: 1.0, 1: 0.9, 2: 0.75}
_MAX_DEPTH_CAP = 4


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


def _clamp_confidence(value: float | None) -> float:
    if value is None:
        return 1.0
    return min(max(float(value), 0.0), 1.0)


def _candidate_confidence(seed_confidence: float, edge_confidence: float, depth: int) -> float:
    decay = _DEPTH_DECAY.get(depth, _DEPTH_DECAY[2] * (0.5 ** max(depth - 2, 0)))
    return round(_clamp_confidence(seed_confidence) * edge_confidence * decay, 6)


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
) -> ImpactExpansionResult:
    """Expand extracted ADES entity refs into deterministic market impact candidates."""

    del language
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
) -> ImpactExpansionResult:
    nodes = store.node_batch(requested_refs)
    source_entities: list[ImpactSourceEntity] = []
    seed_refs: list[str] = []
    candidates_by_ref: dict[str, ImpactCandidate] = {}
    passive_by_ref: dict[str, ImpactPassivePath] = {}
    seed_confidence: dict[str, float] = {}

    for entity_ref in requested_refs:
        node = nodes.get(entity_ref)
        metadata_for_ref = _metadata_for_ref(entity_ref, node=node, mentions=mentions)
        is_graph_seed = bool(node and node.is_seed_eligible and node.seed_degree > 0)
        if is_graph_seed and len(seed_refs) < seed_limit:
            seed_refs.append(entity_ref)
        seed_confidence[entity_ref] = metadata_for_ref.confidence
        source_entities.append(
            ImpactSourceEntity(
                entity_ref=entity_ref,
                name=metadata_for_ref.name,
                entity_type=metadata_for_ref.entity_type or (node.entity_type if node else None),
                library_id=metadata_for_ref.library_id or (node.library_id if node else None),
                is_graph_seed=is_graph_seed,
                seed_degree=node.seed_degree if node is not None else 0,
                is_tradable=node.is_tradable if node is not None else False,
            )
        )
        if node is not None and node.is_tradable:
            candidates_by_ref[entity_ref] = ImpactCandidate(
                entity_ref=entity_ref,
                name=metadata_for_ref.name,
                entity_type=metadata_for_ref.entity_type or node.entity_type,
                is_tradable=True,
                evidence_level="direct",
                confidence=metadata_for_ref.confidence,
                source_entity_refs=[entity_ref],
                relationship_paths=[],
            )

    queue: list[_TraversalState] = [
        _TraversalState(
            seed_ref=seed_ref,
            current_ref=seed_ref,
            depth=0,
            edges=(),
            visited_refs=(seed_ref,),
            edge_confidence=1.0,
        )
        for seed_ref in seed_refs
    ]
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
            if edge.target_ref in state.visited_refs:
                continue
            next_depth = state.depth + 1
            next_edges = (*state.edges, edge)
            next_confidence = min(state.edge_confidence, _clamp_confidence(edge.confidence))
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
                    candidates_by_ref[edge.target_ref] = ImpactCandidate(
                        entity_ref=edge.target_ref,
                        name=target_node.canonical_name,
                        entity_type=target_node.entity_type,
                        is_tradable=True,
                        evidence_level=evidence_level,
                        confidence=candidate_confidence,
                        source_entity_refs=source_refs,
                        relationship_paths=[relationship_path],
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
        enabled_packs=enabled_packs if enabled_packs is not None else [response.pack],
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
