"""Graph-only document context scoring over the explicit QID graph store."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from statistics import mean

from ..config import Settings
from ..service.models import (
    EntityMatch,
    GraphSupport,
    RelatedEntityMatch,
    TagResponse,
    _wordfreq_zipf_en,
)
from .graph_store import GraphNeighbor, GraphNodeStats, QidGraphStore, SharedGraphNode

_GRAPH_CONTEXT_PROVIDER = "sqlite.qid_graph_context"
_GRAPH_CONTEXT_REFINEMENT_STRATEGY = "qid_graph_context_v1"
_PAIRWISE_SUPPORT_THRESHOLD = 0.12
_SUPPRESS_CLUSTER_FIT_THRESHOLD = 0.28
_DOWNGRADE_CLUSTER_FIT_THRESHOLD = 0.38
_BOOST_SUPPORT_THRESHOLD = 0.45
_ISOLATED_SEED_SUPPRESS_CLUSTER_FIT_THRESHOLD = 0.26
_ISOLATED_SEED_SUPPRESS_LOCAL_SCORE_THRESHOLD = 0.56
_ISOLATED_SEED_SUPPRESS_MEAN_SUPPORT_THRESHOLD = 0.03
_RELATED_ENTITY_MIN_COHERENCE = 0.1
_RELATED_DISCOVERY_MIN_CLUSTER_FIT = 0.35
_RELATED_DISCOVERY_MIN_CLUSTER_FIT_WITH_SUPPORT = 0.3
_RELATED_DISCOVERY_MIN_MEAN_SUPPORT = 0.1
_SURFACE_CONFLICT_SCORE_MARGIN = 0.08
_SURFACE_CONFLICT_CLUSTER_FIT_THRESHOLD = 0.36
_LOWERCASE_GEONAMES_SUPPRESS_RELEVANCE_THRESHOLD = 0.58
_LOWERCASE_GENERIC_LOCATION_SUPPRESS_RELEVANCE_THRESHOLD = 0.66
_NUMERIC_FRAGMENT_SUPPRESS_RELEVANCE_THRESHOLD = 0.62
_TITLECASE_COMMON_ALIAS_SUPPRESS_RELEVANCE_THRESHOLD = 0.6
_WEAK_LED_EXACT_ALIAS_SUPPRESS_RELEVANCE_THRESHOLD = 0.5
_LATE_TAIL_ALIAS_SUPPRESS_RELEVANCE_THRESHOLD = 0.58
_COMMON_ALIAS_WORD_ZIPF_MIN = 4.85
_BODY_ACTOR_MIN_START_RATIO = 0.08
_BODY_ACTOR_MAX_START_RATIO = 0.74
_BODY_ACTOR_MIN_BASELINE = 0.34
_BODY_ACTOR_MIN_PEER_SUPPORT = 0.02
_BODY_ACTOR_LOCAL_BONUS = 0.04
_LATE_TAIL_START_RATIO = 0.8
_LATE_TAIL_SINGLE_MENTION_PENALTY = 0.08
_SURFACE_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "dr",
        "hon",
        "lady",
        "lord",
        "mr",
        "mrs",
        "ms",
        "rt",
        "sir",
        "the",
    }
)
_PERSON_TITLE_SURFACE_TOKENS = frozenset(
    {
        "dr",
        "hon",
        "lady",
        "lord",
        "mr",
        "mrs",
        "ms",
        "rt",
        "sir",
    }
)
_EMAIL_ENTITY_ID_MARKER = ":email_address:"
_NUMBER_WORDS = frozenset(
    {
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
        "ten",
        "eleven",
        "twelve",
        "thirteen",
        "fourteen",
        "fifteen",
        "sixteen",
        "seventeen",
        "eighteen",
        "nineteen",
        "twenty",
        "thirty",
        "forty",
        "fifty",
        "sixty",
        "seventy",
        "eighty",
        "ninety",
        "hundred",
        "thousand",
    }
)
_WEAK_ALIAS_LEAD_TOKENS = frozenset(
    {
        "after",
        "at",
        "before",
        "by",
        "for",
        "from",
        "in",
        "into",
        "of",
        "on",
        "over",
        "to",
        "under",
        "via",
        "with",
    }
)
_GENERIC_STRUCTURAL_HEAD_TOKENS = frozenset(
    {
        "association",
        "bank",
        "banks",
        "capital",
        "city",
        "committee",
        "council",
        "court",
        "county",
        "district",
        "ministry",
        "province",
        "state",
        "street",
    }
)
_GRAPH_CONTEXT_ANCHOR_LABEL_RANK: dict[str, int] = {
    "organization": 0,
    "person": 1,
    "product": 2,
    "location": 3,
}
_GRAPH_CONTEXT_ANCHOR_SEED_LIMIT = 3
_GRAPH_SEED_WEAK_LEAD_BASELINE_THRESHOLD = 0.82
_GRAPH_SEED_ZERO_OVERLAP_BASELINE_THRESHOLD = 0.88
_GRAPH_SEED_COLLAPSED_CANONICAL_BASELINE_THRESHOLD = 0.82
_GRAPH_SEED_GENERIC_HEAD_BASELINE_THRESHOLD = 0.72
_GRAPH_SEED_COLLECTIVE_HEAD_BASELINE_THRESHOLD = 0.78
_ORG_FRAGMENT_TOKENS = frozenset(
    {"joint", "task", "force", "committee", "council", "command", "affairs", "authority", "service", "services"}
)
_COLLECTIVE_GROUP_HEAD_TOKENS = frozenset(
    {
        "citizens",
        "community",
        "members",
        "people",
        "population",
        "public",
        "residents",
        "society",
    }
)
_SUPPORT_PREDICATES = (
    "P31",
    "P279",
    "P361",
    "P527",
    "P131",
    "P17",
    "P495",
    "P463",
    "P102",
    "P171",
    "P641",
    "P136",
    "P921",
)
_ANCESTOR_PREDICATES = ("P31", "P279", "P361")
_PREDICATE_WEIGHTS: dict[str, float] = {
    "P31": 0.55,
    "P279": 0.45,
    "P361": 0.48,
    "P527": 0.34,
    "P131": 0.24,
    "P17": 0.22,
    "P495": 0.22,
    "P463": 0.50,
    "P102": 0.44,
    "P171": 0.38,
    "P641": 0.44,
    "P136": 0.34,
    "P921": 0.48,
}


@dataclass(frozen=True)
class _SeedRecord:
    entity_index: int
    entity_id: str
    qid: str
    entity: EntityMatch


@dataclass(frozen=True)
class _SeedDecision:
    entity_index: int
    entity_id: str
    qid: str
    baseline_score: float
    local_score: float
    mean_peer_support: float
    supporting_seed_count: int
    genericity_penalty: float
    cluster_fit: float
    action: str
    score_delta: float
    supporting_seed_ids: tuple[str, ...]
    shared_context_qids: tuple[str, ...]


@dataclass
class _PairSupport:
    score: float = 0.0
    shared_context_qids: set[str] = field(default_factory=set)


@dataclass
class _RelatedCandidateAggregate:
    qid: str
    entity_id: str
    canonical_text: str
    entity_type: str | None
    source_name: str | None
    popularity: float | None
    packs: tuple[str, ...]
    supporting_qids: set[str] = field(default_factory=set)
    support_score: float = 0.0
    min_depth: int = 99
    support_sources: set[str] = field(default_factory=set)


def _linked_seed_records(response: TagResponse) -> list[_SeedRecord]:
    seen_entity_ids: set[str] = set()
    seeds: list[_SeedRecord] = []
    for index, entity in enumerate(response.entities):
        link = entity.link
        entity_id = link.entity_id.strip() if link is not None else ""
        if not entity_id.startswith("wikidata:Q"):
            continue
        if not _is_graph_seed_eligible(entity):
            continue
        if entity_id in seen_entity_ids:
            continue
        seen_entity_ids.add(entity_id)
        seeds.append(
            _SeedRecord(
                entity_index=index,
                entity_id=entity_id,
                qid=entity_id.removeprefix("wikidata:"),
                entity=entity,
            )
        )
    return seeds


def _entity_baseline_score(entity: EntityMatch) -> float:
    if entity.relevance is not None:
        return float(entity.relevance)
    if entity.confidence is not None:
        return float(entity.confidence)
    return 0.0


def _document_extent(entities: list[EntityMatch]) -> int:
    return max((entity.end for entity in entities), default=0)


def _entity_start_ratio(entity: EntityMatch, *, document_extent: int) -> float:
    if document_extent <= 0:
        return 0.0
    return min(1.0, max(0.0, entity.start / document_extent))


def _entity_local_score(entity: EntityMatch, *, document_extent: int | None = None) -> float:
    baseline = _entity_baseline_score(entity)
    mention_bonus = min(0.12, max(0, entity.mention_count - 1) * 0.04)
    position_adjustment = 0.0
    if document_extent is not None and document_extent > 0:
        start_ratio = _entity_start_ratio(entity, document_extent=document_extent)
        label_key = entity.label.casefold()
        if entity.mention_count == 1 and start_ratio >= _LATE_TAIL_START_RATIO:
            position_adjustment -= _LATE_TAIL_SINGLE_MENTION_PENALTY
        elif (
            label_key in {"person", "organization"}
            and _BODY_ACTOR_MIN_START_RATIO <= start_ratio <= _BODY_ACTOR_MAX_START_RATIO
        ):
            position_adjustment += _BODY_ACTOR_LOCAL_BONUS
    return min(1.0, max(0.0, baseline + mention_bonus + position_adjustment))


def _graph_context_anchor_seed_sort_key(
    seed: _SeedRecord,
    *,
    document_extent: int,
) -> tuple[int, float, int, float, int, str]:
    label_rank = _GRAPH_CONTEXT_ANCHOR_LABEL_RANK.get(
        seed.entity.label.casefold(),
        len(_GRAPH_CONTEXT_ANCHOR_LABEL_RANK),
    )
    local_score = _entity_local_score(seed.entity, document_extent=document_extent)
    mention_count = (
        int(seed.entity.mention_count)
        if isinstance(seed.entity.mention_count, int)
        else 1
    )
    baseline = _entity_baseline_score(seed.entity)
    start = int(seed.entity.start) if isinstance(seed.entity.start, int) else 0
    return (
        label_rank,
        -local_score,
        -mention_count,
        -baseline,
        start,
        seed.entity_id,
    )


def _graph_context_anchor_seeds(
    seeds: list[_SeedRecord],
    *,
    document_extent: int,
) -> list[_SeedRecord]:
    if len(seeds) <= _GRAPH_CONTEXT_ANCHOR_SEED_LIMIT:
        return seeds
    ranked = sorted(
        seeds,
        key=lambda seed: _graph_context_anchor_seed_sort_key(
            seed,
            document_extent=document_extent,
        ),
    )
    selected_ids = {
        seed.entity_id for seed in ranked[:_GRAPH_CONTEXT_ANCHOR_SEED_LIMIT]
    }
    return [seed for seed in seeds if seed.entity_id in selected_ids]


def _graph_context_anchor_seed_sort_key_from_decision(
    seed: _SeedRecord,
    decision: _SeedDecision | None,
    *,
    document_extent: int,
) -> tuple[int, int, int, int, float, float, float, int, str]:
    label_rank = _GRAPH_CONTEXT_ANCHOR_LABEL_RANK.get(
        seed.entity.label.casefold(),
        len(_GRAPH_CONTEXT_ANCHOR_LABEL_RANK),
    )
    start = int(seed.entity.start) if isinstance(seed.entity.start, int) else 0
    if decision is None:
        return (
            3,
            1,
            label_rank,
            0,
            0.0,
            0.0,
            0.0,
            start,
            seed.entity_id,
        )

    action_rank = {
        "boost": 0,
        "keep": 1,
        "downgrade": 2,
        "suppress": 3,
    }.get(decision.action, 3)
    unsupported_rank = 1 if decision.supporting_seed_count <= 0 else 0
    actor_penalty = 1 if seed.entity.label.casefold() == "location" else 0
    return (
        action_rank,
        unsupported_rank,
        label_rank,
        actor_penalty,
        -float(decision.supporting_seed_count),
        -decision.mean_peer_support,
        -decision.cluster_fit,
        start,
        seed.entity_id,
    )


def _graph_context_anchor_seeds_from_decisions(
    seeds: list[_SeedRecord],
    *,
    decisions: list[_SeedDecision],
    document_extent: int,
) -> list[_SeedRecord]:
    if len(seeds) <= _GRAPH_CONTEXT_ANCHOR_SEED_LIMIT:
        return seeds
    decision_by_entity_id = {
        decision.entity_id: decision for decision in decisions
    }
    candidate_seeds = [
        seed
        for seed in seeds
        if (decision := decision_by_entity_id.get(seed.entity_id)) is not None
        and decision.action in {"keep", "boost", "downgrade"}
    ]
    if not candidate_seeds:
        return _graph_context_anchor_seeds(
            seeds,
            document_extent=document_extent,
        )
    ranked = sorted(
        candidate_seeds,
        key=lambda seed: _graph_context_anchor_seed_sort_key_from_decision(
            seed,
            decision_by_entity_id.get(seed.entity_id),
            document_extent=document_extent,
        ),
    )
    selected_ids = {
        seed.entity_id for seed in ranked[:_GRAPH_CONTEXT_ANCHOR_SEED_LIMIT]
    }
    return [seed for seed in seeds if seed.entity_id in selected_ids]


def _surface_tokens(text: str) -> tuple[str, ...]:
    tokens = [
        token
        for token in re.findall(r"[a-z0-9]+", text.casefold())
        if token and token not in _SURFACE_STOPWORDS
    ]
    return tuple(tokens)


def _link_entity_id(entity: EntityMatch) -> str:
    link = entity.link
    if link is None:
        return ""
    return link.entity_id.strip()


def _match_path(entity: EntityMatch) -> str:
    provenance = entity.provenance
    if not isinstance(provenance, dict):
        return ""
    value = provenance.get("match_path")
    return str(value).strip()


def _is_acronym_support_entity(entity: EntityMatch) -> bool:
    link = entity.link
    provider = link.provider.casefold() if link is not None and link.provider is not None else ""
    match_path = _match_path(entity).casefold()
    return "acronym" in provider or "acronym" in match_path


def _should_preserve_body_actor(
    entity: EntityMatch,
    *,
    document_extent: int,
    mean_peer_support: float,
    genericity_penalty: float,
) -> bool:
    if document_extent <= 0:
        return False
    if entity.label.casefold() not in {"person", "organization"}:
        return False
    if _entity_baseline_score(entity) < _BODY_ACTOR_MIN_BASELINE:
        return False
    if mean_peer_support < _BODY_ACTOR_MIN_PEER_SUPPORT:
        return False
    if genericity_penalty >= 0.12:
        return False
    start_ratio = _entity_start_ratio(entity, document_extent=document_extent)
    return _BODY_ACTOR_MIN_START_RATIO <= start_ratio <= _BODY_ACTOR_MAX_START_RATIO


def _should_preserve_named_exact_organization(
    entity: EntityMatch,
    *,
    baseline_score: float,
    genericity_penalty: float,
) -> bool:
    if entity.label.casefold() != "organization":
        return False
    if not _is_exact_alias(entity):
        return False
    if entity.link is None:
        return False
    if baseline_score < 0.5:
        return False
    if genericity_penalty >= 0.08:
        return False
    if entity.link.canonical_text.casefold() != entity.text.casefold():
        return False
    raw_tokens = [token for token in re.findall(r"[A-Za-z0-9&']+", entity.text) if token]
    if not 2 <= len(raw_tokens) <= 6:
        return False
    if any(token.isdigit() for token in raw_tokens):
        return False
    if any(token.isupper() and len(token) >= 2 for token in raw_tokens):
        return True
    lexical_tokens = [token for token in _surface_tokens(entity.text) if len(token) >= 4]
    return any(_wordfreq_zipf_en(token) < _COMMON_ALIAS_WORD_ZIPF_MIN for token in lexical_tokens)


def _token_overlap(left: tuple[str, ...], right: tuple[str, ...]) -> float:
    if not left or not right:
        return 0.0
    left_set = set(left)
    right_set = set(right)
    shared = left_set.intersection(right_set)
    if not shared:
        return 0.0
    return len(shared) / min(len(left_set), len(right_set))


def _predicate_weight(predicate: str) -> float:
    return _PREDICATE_WEIGHTS.get(str(predicate).strip().upper(), 0.2)


def _specificity_weight(stats: GraphNodeStats) -> float:
    degree = max(stats.degree_total, 0)
    if degree <= 4:
        return 1.0
    if degree <= 16:
        return 0.82
    if degree <= 64:
        return 0.58
    if degree <= 256:
        return 0.32
    return 0.12


def _genericity_penalty(stats: GraphNodeStats, *, enabled: bool) -> float:
    if not enabled:
        return 0.0
    degree = max(stats.degree_total, 0)
    if degree <= 16:
        return 0.0
    if degree <= 48:
        return 0.04
    if degree <= 128:
        return 0.08
    if degree <= 256:
        return 0.12
    return 0.18


def _neighbor_weight(neighbors: list[GraphNeighbor]) -> float:
    if not neighbors:
        return 0.0
    return max(_predicate_weight(neighbor.predicate) for neighbor in neighbors)


def _build_neighbor_maps(
    store: QidGraphStore,
    *,
    seed_qids: tuple[str, ...],
    settings: Settings,
) -> tuple[dict[str, dict[str, list[GraphNeighbor]]], dict[str, GraphNodeStats]]:
    neighbors_by_seed = store.neighbors_batch(
        seed_qids,
        direction="both",
        predicates=_SUPPORT_PREDICATES,
        limit_per_qid=settings.graph_context_seed_neighbor_limit,
    )
    frontier_qids = sorted(
        {
            neighbor.qid
            for neighbors in neighbors_by_seed.values()
            for neighbor in neighbors
            if neighbor.qid not in seed_qids
        }
    )
    frontier_stats = store.node_stats_batch(frontier_qids)
    if frontier_qids and len(frontier_qids) > settings.graph_context_candidate_limit:
        allowed_frontier_qids = {
            qid
            for qid, _ in sorted(
                frontier_stats.items(),
                key=lambda item: (item[1].degree_total, item[0]),
            )[: settings.graph_context_candidate_limit]
        }
    else:
        allowed_frontier_qids = set(frontier_qids)

    filtered_maps: dict[str, dict[str, list[GraphNeighbor]]] = {}
    retained_frontier_qids: set[str] = set()
    for seed_qid, neighbors in neighbors_by_seed.items():
        neighbor_map: dict[str, list[GraphNeighbor]] = {}
        for neighbor in neighbors:
            if neighbor.qid not in seed_qids and neighbor.qid not in allowed_frontier_qids:
                continue
            retained_frontier_qids.add(neighbor.qid)
            neighbor_map.setdefault(neighbor.qid, []).append(neighbor)
        filtered_maps[seed_qid] = neighbor_map

    node_stats = store.node_stats_batch([*seed_qids, *sorted(retained_frontier_qids)])
    return filtered_maps, node_stats


def _pair_supports(
    seed_qids: tuple[str, ...],
    neighbor_maps: dict[str, dict[str, list[GraphNeighbor]]],
    node_stats: dict[str, GraphNodeStats],
    store: QidGraphStore,
    *,
    settings: Settings,
    max_depth: int,
    candidate_limit: int,
) -> dict[tuple[str, str], _PairSupport]:
    pair_support: dict[tuple[str, str], _PairSupport] = {}
    shared_ancestors = store.shared_ancestors(
        seed_qids,
        predicates=_ANCESTOR_PREDICATES,
        max_depth=max_depth,
        limit=candidate_limit,
        limit_per_hop=settings.graph_context_seed_neighbor_limit,
    )
    ancestor_pairs: dict[tuple[str, str], list[str]] = {}
    for ancestor in shared_ancestors:
        supporting = sorted(ancestor.supporting_qids)
        for left_index, left_qid in enumerate(supporting):
            for right_qid in supporting[left_index + 1 :]:
                ancestor_pairs.setdefault((left_qid, right_qid), []).append(ancestor.qid)
                ancestor_pairs.setdefault((right_qid, left_qid), []).append(ancestor.qid)

    for left_index, left_qid in enumerate(seed_qids):
        left_neighbors = neighbor_maps.get(left_qid, {})
        for right_qid in seed_qids[left_index + 1 :]:
            right_neighbors = neighbor_maps.get(right_qid, {})
            shared_nodes = (
                set(left_neighbors).intersection(right_neighbors)
                - {left_qid, right_qid}
            )
            direct_score = 0.0
            if right_qid in left_neighbors:
                direct_score = max(direct_score, _neighbor_weight(left_neighbors[right_qid]) * 0.75)
            if left_qid in right_neighbors:
                direct_score = max(direct_score, _neighbor_weight(right_neighbors[left_qid]) * 0.75)
            shared_score = 0.0
            shared_context_qids: set[str] = set()
            for shared_qid in shared_nodes:
                stats = node_stats.get(shared_qid, GraphNodeStats(qid=shared_qid))
                specificity = _specificity_weight(stats)
                shared_weight = min(
                    _neighbor_weight(left_neighbors[shared_qid]),
                    _neighbor_weight(right_neighbors[shared_qid]),
                )
                node_score = shared_weight * specificity
                if node_score <= 0:
                    continue
                shared_score += node_score
                shared_context_qids.add(shared_qid)
            ancestor_score = 0.0
            for ancestor_qid in ancestor_pairs.get((left_qid, right_qid), []):
                stats = node_stats.get(ancestor_qid, GraphNodeStats(qid=ancestor_qid))
                ancestor_score += 0.3 * _specificity_weight(stats)
                shared_context_qids.add(ancestor_qid)
            total_score = min(1.0, direct_score + min(shared_score, 0.75) + min(ancestor_score, 0.4))
            pair_support[(left_qid, right_qid)] = _PairSupport(
                score=round(total_score, 6),
                shared_context_qids=shared_context_qids,
            )
            pair_support[(right_qid, left_qid)] = _PairSupport(
                score=round(total_score, 6),
                shared_context_qids=set(shared_context_qids),
            )
    return pair_support


def _seed_decisions(
    seeds: list[_SeedRecord],
    *,
    pair_support: dict[tuple[str, str], _PairSupport],
    node_stats: dict[str, GraphNodeStats],
    settings: Settings,
    refinement_depth: str,
    document_extent: int,
) -> tuple[list[_SeedDecision], float | None]:
    seed_count = len(seeds)
    required_supporting_peers = min(
        max(settings.graph_context_min_supporting_seeds - 1, 1),
        max(seed_count - 1, 1),
    )
    decisions: list[_SeedDecision] = []
    for seed in seeds:
        local_score = _entity_local_score(seed.entity, document_extent=document_extent)
        baseline_score = _entity_baseline_score(seed.entity)
        stats = node_stats.get(seed.qid, GraphNodeStats(qid=seed.qid))
        genericity_penalty = _genericity_penalty(
            stats,
            enabled=settings.graph_context_genericity_penalty_enabled,
        )
        pair_values: list[float] = []
        supporting_seed_ids: list[str] = []
        shared_context_qids: set[str] = set()
        for other_seed in seeds:
            if other_seed.qid == seed.qid:
                continue
            support = pair_support.get((seed.qid, other_seed.qid))
            pair_score = support.score if support is not None else 0.0
            pair_values.append(pair_score)
            if pair_score >= _PAIRWISE_SUPPORT_THRESHOLD:
                supporting_seed_ids.append(other_seed.entity_id)
            if support is not None:
                shared_context_qids.update(support.shared_context_qids)
        mean_peer_support = round(float(mean(pair_values)), 6) if pair_values else 0.0
        cluster_fit = round(
            max(0.0, (0.45 * local_score) + (0.55 * mean_peer_support) - genericity_penalty),
            6,
        )
        score_delta = 0.0
        action = "keep"
        if seed_count >= 3:
            if (
                not supporting_seed_ids
                and mean_peer_support <= _ISOLATED_SEED_SUPPRESS_MEAN_SUPPORT_THRESHOLD
                and local_score < _ISOLATED_SEED_SUPPRESS_LOCAL_SCORE_THRESHOLD
                and cluster_fit < _ISOLATED_SEED_SUPPRESS_CLUSTER_FIT_THRESHOLD
            ):
                action = "suppress"
            elif (
                len(supporting_seed_ids) < required_supporting_peers
                and mean_peer_support < _PAIRWISE_SUPPORT_THRESHOLD
                and local_score < (0.52 if refinement_depth == "light" else 0.58)
                and cluster_fit < _SUPPRESS_CLUSTER_FIT_THRESHOLD
            ):
                action = "suppress"
            elif mean_peer_support < 0.14 and local_score < 0.56 and cluster_fit < _DOWNGRADE_CLUSTER_FIT_THRESHOLD:
                action = "downgrade"
                score_delta = -0.06 if refinement_depth == "light" else -0.08
            elif (
                len(supporting_seed_ids) >= required_supporting_peers
                and mean_peer_support >= _BOOST_SUPPORT_THRESHOLD
                and baseline_score < 0.82
            ):
                action = "boost"
                score_delta = min(0.08 if refinement_depth == "light" else 0.11, mean_peer_support * 0.18)
        elif seed_count == 2:
            if (
                not supporting_seed_ids
                and local_score < 0.45
                and genericity_penalty >= 0.08
                and cluster_fit < _DOWNGRADE_CLUSTER_FIT_THRESHOLD
            ):
                action = "downgrade"
                score_delta = -0.05
            elif supporting_seed_ids and mean_peer_support >= 0.55 and baseline_score < 0.82:
                action = "boost"
                score_delta = min(0.07, mean_peer_support * 0.16)
        if action == "suppress" and _should_preserve_body_actor(
            seed.entity,
            document_extent=document_extent,
            mean_peer_support=mean_peer_support,
            genericity_penalty=genericity_penalty,
        ):
            action = "downgrade"
            score_delta = -0.04 if refinement_depth == "light" else -0.06
        if action == "suppress" and _should_preserve_named_exact_organization(
            seed.entity,
            baseline_score=baseline_score,
            genericity_penalty=genericity_penalty,
        ):
            action = "downgrade"
            score_delta = -0.04 if refinement_depth == "light" else -0.06
        decisions.append(
            _SeedDecision(
                entity_index=seed.entity_index,
                entity_id=seed.entity_id,
                qid=seed.qid,
                baseline_score=round(baseline_score, 6),
                local_score=round(local_score, 6),
                mean_peer_support=mean_peer_support,
                supporting_seed_count=len(supporting_seed_ids),
                genericity_penalty=round(genericity_penalty, 6),
                cluster_fit=cluster_fit,
                action=action,
                score_delta=round(score_delta, 6),
                supporting_seed_ids=tuple(sorted(supporting_seed_ids)),
                shared_context_qids=tuple(sorted(shared_context_qids))[:12],
            )
        )
    coherence_score = _document_coherence_from_seed_decisions(decisions)
    return decisions, coherence_score


def _related_discovery_seed_qids(decisions: list[_SeedDecision]) -> tuple[str, ...]:
    strong_seed_qids: list[str] = []
    for decision in decisions:
        if decision.action not in {"keep", "boost"}:
            continue
        if decision.action == "boost":
            strong_seed_qids.append(decision.qid)
            continue
        if (
            decision.cluster_fit >= _RELATED_DISCOVERY_MIN_CLUSTER_FIT
            or decision.mean_peer_support >= _RELATED_DISCOVERY_MIN_MEAN_SUPPORT
            or (
                decision.supporting_seed_count > 0
                and decision.cluster_fit
                >= _RELATED_DISCOVERY_MIN_CLUSTER_FIT_WITH_SUPPORT
            )
        ):
            strong_seed_qids.append(decision.qid)
    return tuple(strong_seed_qids)


def _document_coherence_from_seed_decisions(decisions: list[_SeedDecision]) -> float | None:
    if not decisions:
        return None
    coherence_values = [decision.mean_peer_support for decision in decisions]
    strong_related_decisions = [
        decision
        for decision in decisions
        if decision.action in {"keep", "boost"}
        and (
            decision.cluster_fit >= _RELATED_DISCOVERY_MIN_CLUSTER_FIT
            or decision.mean_peer_support >= _RELATED_DISCOVERY_MIN_MEAN_SUPPORT
        )
    ]
    if len(strong_related_decisions) >= 3:
        coherence_values = [decision.mean_peer_support for decision in strong_related_decisions]
    return round(float(mean(coherence_values)), 6) if coherence_values else None


def _should_suppress_lowercase_geonames(entity: EntityMatch) -> bool:
    entity_id = _link_entity_id(entity)
    if not entity_id.startswith("geonames:"):
        return False
    if entity.text != entity.text.casefold():
        return False
    if entity.mention_count > 1:
        return False
    if _entity_baseline_score(entity) >= _LOWERCASE_GEONAMES_SUPPRESS_RELEVANCE_THRESHOLD:
        return False
    text_tokens = _surface_tokens(entity.text)
    if len(text_tokens) < 2:
        return False
    canonical_tokens = _surface_tokens(entity.link.canonical_text if entity.link is not None else "")
    return _token_overlap(text_tokens, canonical_tokens) == 0.0


def _has_strong_seed_support(decision: _SeedDecision | None) -> bool:
    if decision is None:
        return False
    if decision.action == "boost":
        return True
    return (
        decision.action == "keep"
        and decision.supporting_seed_count > 0
        and decision.cluster_fit >= _SURFACE_CONFLICT_CLUSTER_FIT_THRESHOLD
    )


def _is_low_value_email_entity(entity: EntityMatch) -> bool:
    entity_id = _link_entity_id(entity)
    return entity.label == "email_address" or _EMAIL_ENTITY_ID_MARKER in entity_id


def _is_generic_lowercase_location(entity: EntityMatch, *, decision: _SeedDecision | None) -> bool:
    if _has_strong_seed_support(decision):
        return False
    if entity.label != "location":
        return False
    if entity.mention_count > 1:
        return False
    if _entity_baseline_score(entity) >= _LOWERCASE_GENERIC_LOCATION_SUPPRESS_RELEVANCE_THRESHOLD:
        return False
    text = entity.text.strip()
    if not text or text != text.casefold():
        return False
    tokens = _surface_tokens(text)
    if not 1 <= len(tokens) <= 3:
        return False
    if not all(token.isalpha() for token in tokens):
        return False
    return True


def _is_numeric_fragment_location(entity: EntityMatch, *, decision: _SeedDecision | None) -> bool:
    if _has_strong_seed_support(decision):
        return False
    if entity.label != "location":
        return False
    if entity.mention_count > 1:
        return False
    if _entity_baseline_score(entity) >= _NUMERIC_FRAGMENT_SUPPRESS_RELEVANCE_THRESHOLD:
        return False
    stripped = entity.text.strip().casefold()
    if re.fullmatch(r"\d+(st|nd|rd|th)", stripped):
        return True
    tokens = _surface_tokens(stripped)
    if not tokens:
        return False
    return tokens[0] in _NUMBER_WORDS and len(tokens) <= 3


def _is_exact_alias(entity: EntityMatch) -> bool:
    if entity.link is None or entity.provenance is None:
        return False
    return entity.provenance.match_path == "lookup.alias.exact"


def _is_graph_seed_eligible(entity: EntityMatch) -> bool:
    entity_id = _link_entity_id(entity)
    if not entity_id.startswith("wikidata:Q"):
        return False
    if entity.link is None:
        return False
    exact_alias = _is_exact_alias(entity)
    if entity.mention_count > 1 and not exact_alias:
        return True
    text_tokens = _surface_tokens(entity.text)
    canonical_tokens = _surface_tokens(entity.link.canonical_text)
    if not text_tokens or not canonical_tokens:
        return True
    baseline = _entity_baseline_score(entity)
    raw_tokens = [token.casefold() for token in entity.text.split() if token]
    if (
        exact_alias
        and raw_tokens
        and raw_tokens[0] in _WEAK_ALIAS_LEAD_TOKENS
        and baseline < _GRAPH_SEED_WEAK_LEAD_BASELINE_THRESHOLD
    ):
        return False
    overlap = _token_overlap(text_tokens, canonical_tokens)
    if (
        exact_alias
        and overlap == 0.0
        and baseline < _GRAPH_SEED_ZERO_OVERLAP_BASELINE_THRESHOLD
    ):
        return False
    if (
        exact_alias
        and len(text_tokens) >= 3
        and len(canonical_tokens) <= 2
        and overlap < 0.5
        and baseline < _GRAPH_SEED_COLLAPSED_CANONICAL_BASELINE_THRESHOLD
    ):
        return False
    if (
        exact_alias
        and len(text_tokens) >= 3
        and len(canonical_tokens) <= 2
        and len(text_tokens) > len(canonical_tokens)
        and overlap == 1.0
        and any(token in _GENERIC_STRUCTURAL_HEAD_TOKENS for token in canonical_tokens)
        and baseline < _GRAPH_SEED_COLLAPSED_CANONICAL_BASELINE_THRESHOLD
    ):
        return False
    if (
        exact_alias
        and len(text_tokens) < len(canonical_tokens)
        and overlap == 1.0
        and any(token in _GENERIC_STRUCTURAL_HEAD_TOKENS for token in text_tokens)
        and baseline < _GRAPH_SEED_GENERIC_HEAD_BASELINE_THRESHOLD
    ):
        return False
    if (
        entity.label == "location"
        and exact_alias
        and any(token in _GENERIC_STRUCTURAL_HEAD_TOKENS for token in text_tokens)
        and overlap < 1.0
        and baseline < _GRAPH_SEED_GENERIC_HEAD_BASELINE_THRESHOLD
    ):
        return False
    if (
        entity.label == "organization"
        and exact_alias
        and len(text_tokens) == len(canonical_tokens) >= 2
        and text_tokens[:-1] == canonical_tokens[:-1]
        and text_tokens[-1] != canonical_tokens[-1]
        and text_tokens[-1] in _COLLECTIVE_GROUP_HEAD_TOKENS
        and canonical_tokens[-1] in _COLLECTIVE_GROUP_HEAD_TOKENS
        and baseline < _GRAPH_SEED_COLLECTIVE_HEAD_BASELINE_THRESHOLD
    ):
        return False
    return True


def _preseed_surface_filtered_entities(
    entities: list[EntityMatch],
    *,
    document_extent: int | None,
) -> tuple[list[EntityMatch], list[str], dict[str, object]]:
    filtered_entities, suppressed_entity_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
        document_extent=document_extent,
    )
    late_tail_suppressed_ids = set(debug.get("late_tail_exact_alias_suppressed", []))
    if not late_tail_suppressed_ids:
        return filtered_entities, suppressed_entity_ids, debug
    other_suppressed_ids = set(suppressed_entity_ids) - late_tail_suppressed_ids
    restored_late_tail_ids = late_tail_suppressed_ids - other_suppressed_ids
    if not restored_late_tail_ids:
        restored_entities = filtered_entities
        restored_suppressed_ids = suppressed_entity_ids
    else:
        restored_entities = []
        restored_suppressed_ids = []
        for entity in entities:
            entity_id = _link_entity_id(entity)
            if entity_id in other_suppressed_ids:
                if entity_id not in restored_suppressed_ids:
                    restored_suppressed_ids.append(entity_id)
                continue
            restored_entities.append(entity)
        debug = dict(debug)
        debug["late_tail_exact_alias_suppressed"] = [
            entity_id
            for entity_id in debug.get("late_tail_exact_alias_suppressed", [])
            if entity_id not in restored_late_tail_ids
        ]
    titled_fragment_suppressions: list[str] = []
    suppressed_preseed_indexes: set[int] = set()
    token_cache = [_surface_tokens(entity.text) for entity in restored_entities]
    for index, entity in enumerate(restored_entities):
        if index in suppressed_preseed_indexes:
            continue
        entity_id = _link_entity_id(entity)
        if not entity_id or entity.label != "person" or not _is_exact_alias(entity):
            continue
        lexical_tokens = token_cache[index]
        raw_tokens = [token.casefold() for token in entity.text.split() if token]
        if (
            len(lexical_tokens) != 1
            or not raw_tokens
            or raw_tokens[0] not in _PERSON_TITLE_SURFACE_TOKENS
        ):
            continue
        baseline = _entity_baseline_score(entity)
        for other_index, other_entity in enumerate(restored_entities):
            if other_index == index or other_index in suppressed_preseed_indexes:
                continue
            other_id = _link_entity_id(other_entity)
            if (
                not other_id
                or other_id == entity_id
                or other_entity.label != "person"
            ):
                continue
            other_tokens = token_cache[other_index]
            if len(other_tokens) <= len(lexical_tokens):
                continue
            if not set(lexical_tokens).issubset(set(other_tokens)):
                continue
            if len(other_entity.text) <= len(entity.text):
                continue
            if _entity_baseline_score(other_entity) + 0.02 < baseline:
                continue
            suppressed_preseed_indexes.add(index)
            titled_fragment_suppressions.append(entity_id)
            if entity_id not in restored_suppressed_ids:
                restored_suppressed_ids.append(entity_id)
            break
    if not titled_fragment_suppressions:
        return restored_entities, restored_suppressed_ids, debug
    debug = dict(debug)
    debug["surface_conflict_suppressed"] = [
        *debug.get("surface_conflict_suppressed", []),
        *titled_fragment_suppressions,
    ]
    restored_entities = [
        entity
        for index, entity in enumerate(restored_entities)
        if index not in suppressed_preseed_indexes
    ]
    return restored_entities, restored_suppressed_ids, debug


def _is_titlecase_common_alias(entity: EntityMatch, *, decision: _SeedDecision | None) -> bool:
    if _has_strong_seed_support(decision):
        return False
    if entity.label not in {"location", "organization"}:
        return False
    if entity.mention_count > 1:
        return False
    if _entity_baseline_score(entity) >= _TITLECASE_COMMON_ALIAS_SUPPRESS_RELEVANCE_THRESHOLD:
        return False
    if not _is_exact_alias(entity):
        return False
    if entity.link is None or entity.link.canonical_text.casefold() != entity.text.casefold():
        return False
    text = entity.text.strip()
    if not text or not text.isascii():
        return False
    raw_tokens = [token for token in text.split() if token]
    if not 1 <= len(raw_tokens) <= 2:
        return False
    if any(not token.isalpha() for token in raw_tokens):
        return False
    if len(raw_tokens) == 1:
        return raw_tokens[0][0].isupper() and _wordfreq_zipf_en(raw_tokens[0]) >= _COMMON_ALIAS_WORD_ZIPF_MIN
    first, second = raw_tokens
    if not first[0].isupper():
        return False
    if second != second.casefold():
        return False
    return _wordfreq_zipf_en(second) >= _COMMON_ALIAS_WORD_ZIPF_MIN


def _is_weak_led_exact_alias(entity: EntityMatch, *, decision: _SeedDecision | None) -> bool:
    if _has_strong_seed_support(decision):
        return False
    if entity.label not in {"location", "organization"}:
        return False
    if _entity_baseline_score(entity) >= _WEAK_LED_EXACT_ALIAS_SUPPRESS_RELEVANCE_THRESHOLD:
        return False
    if not _is_exact_alias(entity):
        return False
    text_tokens = _surface_tokens(entity.text)
    if not text_tokens or len(text_tokens) > 3:
        return False
    raw_tokens = [token.casefold() for token in entity.text.split() if token]
    if not raw_tokens:
        return False
    if raw_tokens[0] in _WEAK_ALIAS_LEAD_TOKENS:
        if entity.mention_count > 1:
            return False
        return True
    if raw_tokens[0] == "the" and entity.link is not None:
        if entity.mention_count > 2:
            return False
        return entity.link.canonical_text.casefold() != entity.text.casefold()
    return False


def _is_late_tail_exact_alias(
    entity: EntityMatch,
    *,
    decision: _SeedDecision | None,
    document_extent: int | None,
) -> bool:
    if document_extent is None or document_extent <= 0:
        return False
    if _has_strong_seed_support(decision):
        return False
    if entity.label not in {"organization", "location"}:
        return False
    if (
        decision is not None
        and decision.action == "downgrade"
        and entity.label == "organization"
        and (
            decision.supporting_seed_count > 0
            or _should_preserve_named_exact_organization(
                entity,
                baseline_score=decision.baseline_score,
                genericity_penalty=decision.genericity_penalty,
            )
        )
        and len(_surface_tokens(entity.text)) >= 2
    ):
        return False
    if entity.mention_count > 1:
        return False
    if _entity_baseline_score(entity) >= _LATE_TAIL_ALIAS_SUPPRESS_RELEVANCE_THRESHOLD:
        return False
    if not _is_exact_alias(entity):
        return False
    return _entity_start_ratio(entity, document_extent=document_extent) >= _LATE_TAIL_START_RATIO


def _looks_like_acronym_fragment_alias(entity: EntityMatch, *, support_entity: EntityMatch) -> bool:
    if entity.label != "organization" or support_entity.label != "organization":
        return False
    if not _is_exact_alias(entity):
        return False
    if entity.mention_count > 2:
        return False
    if not _is_acronym_support_entity(support_entity):
        return False
    support_link = support_entity.link
    if support_link is None:
        return False
    fragment_tokens = _surface_tokens(entity.text)
    if not fragment_tokens or len(fragment_tokens) > 2:
        return False
    support_tokens = _surface_tokens(support_link.canonical_text or support_entity.text)
    if len(support_tokens) <= len(fragment_tokens):
        return False
    if not set(fragment_tokens).issubset(set(support_tokens)):
        return False
    return any(token in _ORG_FRAGMENT_TOKENS for token in fragment_tokens)


def _apply_surface_conflict_filters(
    entities: list[EntityMatch],
    *,
    decision_by_entity_id: dict[str, _SeedDecision],
    document_extent: int | None = None,
) -> tuple[list[EntityMatch], list[str], dict[str, object]]:
    if not entities:
        return entities, [], {
            "email_address_suppressed": [],
            "lowercase_geonames_suppressed": [],
            "generic_lowercase_location_suppressed": [],
            "numeric_fragment_suppressed": [],
            "titlecase_common_alias_suppressed": [],
            "weak_led_exact_alias_suppressed": [],
            "late_tail_exact_alias_suppressed": [],
            "acronym_fragment_suppressed": [],
            "surface_conflict_suppressed": [],
        }

    token_cache: list[tuple[str, ...]] = [_surface_tokens(entity.text) for entity in entities]
    email_suppressions: list[str] = []
    lexical_suppressions: list[str] = []
    generic_location_suppressions: list[str] = []
    numeric_fragment_suppressions: list[str] = []
    titlecase_common_alias_suppressions: list[str] = []
    weak_led_alias_suppressions: list[str] = []
    late_tail_alias_suppressions: list[str] = []
    acronym_fragment_suppressions: list[str] = []
    surface_conflict_suppressions: list[str] = []
    suppressed_indexes: set[int] = set()
    candidates: list[tuple[int, EntityMatch, str, tuple[str, ...], float]] = []

    for index, entity in enumerate(entities):
        entity_id = _link_entity_id(entity)
        if not entity_id:
            continue
        decision = decision_by_entity_id.get(entity_id)
        if _is_low_value_email_entity(entity):
            email_suppressions.append(entity_id)
            suppressed_indexes.add(index)
            continue
        if _should_suppress_lowercase_geonames(entity):
            lexical_suppressions.append(entity_id)
            suppressed_indexes.add(index)
            continue
        if _is_generic_lowercase_location(entity, decision=decision):
            generic_location_suppressions.append(entity_id)
            suppressed_indexes.add(index)
            continue
        if _is_numeric_fragment_location(entity, decision=decision):
            numeric_fragment_suppressions.append(entity_id)
            suppressed_indexes.add(index)
            continue
        if _is_titlecase_common_alias(entity, decision=decision):
            titlecase_common_alias_suppressions.append(entity_id)
            suppressed_indexes.add(index)
            continue
        if _is_weak_led_exact_alias(entity, decision=decision):
            weak_led_alias_suppressions.append(entity_id)
            suppressed_indexes.add(index)
            continue
        if _is_late_tail_exact_alias(
            entity,
            decision=decision,
            document_extent=document_extent,
        ):
            late_tail_alias_suppressions.append(entity_id)
            suppressed_indexes.add(index)
            continue
        candidates.append(
            (
                index,
                entity,
                entity_id,
                token_cache[index],
                _entity_baseline_score(entity),
            )
        )

    for index, entity, entity_id, tokens, baseline in candidates:
        if index in suppressed_indexes or not tokens:
            continue
        decision = decision_by_entity_id.get(entity_id)
        if _has_strong_seed_support(decision):
            continue
        for other_index, other_entity, _, _, other_baseline in candidates:
            if other_index == index or other_index in suppressed_indexes:
                continue
            if other_baseline + 0.02 < baseline:
                continue
            if not _looks_like_acronym_fragment_alias(entity, support_entity=other_entity):
                continue
            acronym_fragment_suppressions.append(entity_id)
            suppressed_indexes.add(index)
            break

    for index, entity, entity_id, tokens, baseline in candidates:
        if index in suppressed_indexes or not tokens:
            continue
        decision = decision_by_entity_id.get(entity_id)
        if _has_strong_seed_support(decision):
            continue
        for other_index, other_entity, other_id, other_tokens, other_baseline in candidates:
            if other_index == index or other_index in suppressed_indexes:
                continue
            if other_id == entity_id:
                continue
            if other_entity.label != entity.label:
                continue
            if len(other_tokens) <= len(tokens):
                continue
            if not set(tokens).issubset(set(other_tokens)):
                continue
            if other_baseline < baseline + _SURFACE_CONFLICT_SCORE_MARGIN:
                continue
            if len(other_entity.text) <= len(entity.text):
                continue
            surface_conflict_suppressions.append(entity_id)
            suppressed_indexes.add(index)
            break

    filtered_entities = [
        entity
        for index, entity in enumerate(entities)
        if index not in suppressed_indexes
    ]
    suppressed_entity_ids = [
        *email_suppressions,
        *lexical_suppressions,
        *generic_location_suppressions,
        *numeric_fragment_suppressions,
        *titlecase_common_alias_suppressions,
        *weak_led_alias_suppressions,
        *late_tail_alias_suppressions,
        *acronym_fragment_suppressions,
        *surface_conflict_suppressions,
    ]
    return filtered_entities, suppressed_entity_ids, {
        "email_address_suppressed": email_suppressions,
        "lowercase_geonames_suppressed": lexical_suppressions,
        "generic_lowercase_location_suppressed": generic_location_suppressions,
        "numeric_fragment_suppressed": numeric_fragment_suppressions,
        "titlecase_common_alias_suppressed": titlecase_common_alias_suppressions,
        "weak_led_exact_alias_suppressed": weak_led_alias_suppressions,
        "late_tail_exact_alias_suppressed": late_tail_alias_suppressions,
        "acronym_fragment_suppressed": acronym_fragment_suppressions,
        "surface_conflict_suppressed": surface_conflict_suppressions,
    }


def _score_related_candidate(
    candidate: _RelatedCandidateAggregate,
    *,
    stats: GraphNodeStats,
    genericity_penalty_enabled: bool,
) -> float:
    label = (candidate.entity_type or "").strip().casefold()
    pack_ids = {pack.strip().casefold() for pack in candidate.packs if str(pack).strip()}
    has_specialized_pack = any(pack != "general-en" for pack in pack_ids)
    canonical_tokens = _surface_tokens(candidate.canonical_text)
    if (
        label == "location"
        and stats.degree_total <= 0
        and len(canonical_tokens) <= 3
        and not has_specialized_pack
    ):
        return 0.0
    specificity = _specificity_weight(stats)
    genericity_penalty = _genericity_penalty(
        stats,
        enabled=genericity_penalty_enabled,
    )
    seed_support_bonus = min(0.36, max(0, len(candidate.supporting_qids) - 1) * 0.18)
    path_bonus = 0.08 if candidate.min_depth <= 1 else 0.04
    multi_source_bonus = 0.06 if len(candidate.support_sources) >= 2 else 0.0
    ancestor_only_penalty = 0.0
    if candidate.support_sources == {"shared_ancestor"}:
        ancestor_only_penalty += 0.06 if len(candidate.supporting_qids) <= 2 else 0.03
        if (candidate.entity_type or "").strip().casefold() == "location":
            ancestor_only_penalty += 0.12
    score = candidate.support_score + seed_support_bonus + path_bonus + multi_source_bonus
    base_specific_score = score * specificity
    resolved_score = base_specific_score - genericity_penalty - ancestor_only_penalty
    if (
        label == "organization"
        and len(candidate.supporting_qids) >= 2
        and candidate.min_depth <= 1
        and "shared_neighbor" in candidate.support_sources
    ):
        # Keep strongly shared organization candidates alive even when global
        # degree-based genericity would otherwise zero them out.
        resolved_score = max(resolved_score, min(0.18, base_specific_score))
    return round(
        max(
            0.0,
            min(
                1.0,
                resolved_score,
            ),
        ),
        6,
    )


def _add_related_support(
    aggregates: dict[str, _RelatedCandidateAggregate],
    candidate: SharedGraphNode,
    *,
    source_kind: str,
    support_weight: float,
) -> None:
    aggregate = aggregates.get(candidate.qid)
    if aggregate is None:
        aggregate = _RelatedCandidateAggregate(
            qid=candidate.qid,
            entity_id=candidate.entity_id,
            canonical_text=candidate.canonical_text,
            entity_type=candidate.entity_type,
            source_name=candidate.source_name,
            popularity=candidate.popularity,
            packs=candidate.packs,
        )
        aggregates[candidate.qid] = aggregate
    aggregate.supporting_qids.update(candidate.supporting_qids)
    aggregate.support_score += support_weight
    aggregate.min_depth = min(aggregate.min_depth, candidate.min_depth)
    aggregate.support_sources.add(source_kind)


def _is_metadata_backed_related_candidate(candidate: SharedGraphNode) -> bool:
    canonical_text = candidate.canonical_text.strip()
    if not canonical_text or canonical_text == candidate.qid:
        return False
    if not re.search(r"[a-z0-9]", canonical_text.casefold()):
        return False
    if candidate.source_name is not None or candidate.entity_type is not None:
        return True
    if candidate.packs:
        return True
    return True


def _discover_related_entities(
    store: QidGraphStore,
    *,
    surviving_seed_qids: tuple[str, ...],
    related_seed_qids: tuple[str, ...],
    coherence_score: float | None,
    extracted_entity_ids: set[str],
    settings: Settings,
) -> tuple[list[RelatedEntityMatch], dict[str, object] | None]:
    if coherence_score is not None and coherence_score < _RELATED_ENTITY_MIN_COHERENCE:
        return [], {
            "reason": "low_document_coherence",
            "coherence_score": coherence_score,
            "surviving_seed_qids": list(surviving_seed_qids),
            "related_seed_qids": list(related_seed_qids),
        }
    if len(related_seed_qids) < settings.graph_context_min_supporting_seeds:
        return [], {
            "reason": "insufficient_strong_surviving_seeds",
            "surviving_seed_qids": list(surviving_seed_qids),
            "related_seed_qids": list(related_seed_qids),
        }
    shared_neighbors = store.shared_neighbors(
        related_seed_qids,
        predicates=_SUPPORT_PREDICATES,
        direction="both",
        limit=settings.graph_context_candidate_limit,
        limit_per_seed=settings.graph_context_seed_neighbor_limit,
    )
    shared_ancestors = store.shared_ancestors(
        related_seed_qids,
        predicates=_ANCESTOR_PREDICATES,
        max_depth=settings.graph_context_max_depth,
        limit=settings.graph_context_candidate_limit,
        limit_per_hop=settings.graph_context_seed_neighbor_limit,
    )
    aggregates: dict[str, _RelatedCandidateAggregate] = {}
    for candidate in shared_neighbors:
        if len(candidate.supporting_qids) < settings.graph_context_min_supporting_seeds:
            continue
        if candidate.entity_id in extracted_entity_ids:
            continue
        if not _is_metadata_backed_related_candidate(candidate):
            continue
        _add_related_support(
            aggregates,
            candidate,
            source_kind="shared_neighbor",
            support_weight=min(0.55, 0.18 * candidate.support_count),
        )
    for candidate in shared_ancestors:
        if len(candidate.supporting_qids) < settings.graph_context_min_supporting_seeds:
            continue
        if candidate.entity_id in extracted_entity_ids:
            continue
        if not _is_metadata_backed_related_candidate(candidate):
            continue
        _add_related_support(
            aggregates,
            candidate,
            source_kind="shared_ancestor",
            support_weight=min(0.42, 0.14 * candidate.support_count),
        )
    if not aggregates:
        return [], {
            "reason": "no_graph_supported_related_candidates",
            "surviving_seed_qids": list(surviving_seed_qids),
            "related_seed_qids": list(related_seed_qids),
        }
    candidate_stats = store.node_stats_batch(aggregates.keys())
    ranked_candidates = sorted(
        aggregates.values(),
        key=lambda item: (
            -_score_related_candidate(
                item,
                stats=candidate_stats.get(item.qid, GraphNodeStats(qid=item.qid)),
                genericity_penalty_enabled=settings.graph_context_genericity_penalty_enabled,
            ),
            -len(item.supporting_qids),
            -(item.popularity or 0.0),
            item.min_depth,
            item.canonical_text.casefold(),
            item.entity_id,
        ),
    )
    related_entities: list[RelatedEntityMatch] = []
    for item in ranked_candidates[: settings.graph_context_related_limit]:
        display_text = item.canonical_text.strip()
        if not display_text:
            continue
        score = _score_related_candidate(
            item,
            stats=candidate_stats.get(item.qid, GraphNodeStats(qid=item.qid)),
            genericity_penalty_enabled=settings.graph_context_genericity_penalty_enabled,
        )
        if score <= 0:
            continue
        related_entities.append(
            RelatedEntityMatch(
                entity_id=item.entity_id,
                canonical_text=display_text,
                score=score,
                provider=_GRAPH_CONTEXT_PROVIDER,
                entity_type=item.entity_type,
                source_name=item.source_name,
                packs=list(item.packs),
                seed_entity_ids=[f"wikidata:{qid}" for qid in sorted(item.supporting_qids)],
                shared_seed_count=len(item.supporting_qids),
            )
        )
    return related_entities, {
        "coherence_score": coherence_score,
        "shared_neighbor_candidates": len(shared_neighbors),
        "shared_ancestor_candidates": len(shared_ancestors),
        "related_seed_qids": list(related_seed_qids),
        "ranked_candidate_qids": [item.qid for item in ranked_candidates[: settings.graph_context_related_limit]],
    }


def apply_graph_context(
    response: TagResponse,
    *,
    settings: Settings,
    include_related_entities: bool = False,
    include_graph_support: bool = False,
    refine_links: bool = False,
    refinement_depth: str = "light",
) -> TagResponse:
    """Apply graph-only context refinement over extracted Wikidata-linked entities."""

    if not include_related_entities and not include_graph_support and not refine_links:
        return response
    graph_support = GraphSupport(
        requested=True,
        applied=False,
        provider=_GRAPH_CONTEXT_PROVIDER,
        requested_related_entities=include_related_entities,
        requested_refinement=refine_links,
        refinement_depth=refinement_depth if refine_links else None,
    )
    if not settings.graph_context_enabled:
        graph_support.warnings.append("graph_context_disabled")
        return response.model_copy(update={"graph_support": graph_support})
    if settings.graph_context_artifact_path is None:
        graph_support.warnings.append("graph_context_artifact_missing")
        return response.model_copy(update={"graph_support": graph_support})

    document_extent = _document_extent(response.entities)
    (
        preseed_entities,
        preseed_suppressed_entity_ids,
        preseed_surface_filter_debug,
    ) = _preseed_surface_filtered_entities(
        response.entities,
        document_extent=document_extent,
    )
    seed_response = (
        response
        if preseed_entities is response.entities
        else response.model_copy(update={"entities": preseed_entities})
    )
    seeds = _linked_seed_records(seed_response)
    graph_support.seed_entity_ids = [seed.entity_id for seed in seeds]
    if not seeds:
        graph_support.warnings.append("no_wikidata_seed_entities")
        return response.model_copy(update={"graph_support": graph_support})

    try:
        with QidGraphStore(settings.graph_context_artifact_path) as store:
            seed_qids = tuple(seed.qid for seed in seeds)
            neighbor_maps, node_stats = _build_neighbor_maps(
                store,
                seed_qids=seed_qids,
                settings=settings,
            )
            pair_support = _pair_supports(
                seed_qids,
                neighbor_maps,
                node_stats,
                store,
                settings=settings,
                max_depth=settings.graph_context_max_depth,
                candidate_limit=settings.graph_context_candidate_limit,
            )
            extracted_entity_ids = {
                entity.link.entity_id
                for entity in response.entities
                if entity.link is not None and entity.link.entity_id
            }
            decisions, _ = _seed_decisions(
                seeds,
                pair_support=pair_support,
                node_stats=node_stats,
                settings=settings,
                refinement_depth=refinement_depth,
                document_extent=document_extent,
            )
            anchor_seeds = _graph_context_anchor_seeds_from_decisions(
                seeds,
                decisions=decisions,
                document_extent=document_extent,
            )
            anchor_seed_qids = tuple(seed.qid for seed in anchor_seeds)
            if anchor_seed_qids == seed_qids:
                anchor_neighbor_maps = neighbor_maps
                anchor_node_stats = node_stats
            else:
                anchor_neighbor_maps, anchor_node_stats = _build_neighbor_maps(
                    store,
                    seed_qids=anchor_seed_qids,
                    settings=settings,
                )
            anchor_pair_support = _pair_supports(
                anchor_seed_qids,
                anchor_neighbor_maps,
                anchor_node_stats,
                store,
                settings=settings,
                max_depth=settings.graph_context_max_depth,
                candidate_limit=settings.graph_context_candidate_limit,
            )
            anchor_decisions, coherence_score = _seed_decisions(
                anchor_seeds,
                pair_support=anchor_pair_support,
                node_stats=anchor_node_stats,
                settings=settings,
                refinement_depth=refinement_depth,
                document_extent=document_extent,
            )
            surviving_seed_qids = tuple(
                decision.qid
                for decision in anchor_decisions
                if decision.action != "suppress"
            )
            related_seed_qids = _related_discovery_seed_qids(anchor_decisions)
            related_entities, related_debug = (
                _discover_related_entities(
                    store,
                    surviving_seed_qids=surviving_seed_qids,
                    related_seed_qids=related_seed_qids,
                    coherence_score=coherence_score,
                    extracted_entity_ids=extracted_entity_ids,
                    settings=settings,
                )
                if include_related_entities
                else ([], None)
            )
    except FileNotFoundError:
        graph_support.warnings.append("graph_context_artifact_missing")
        return response.model_copy(update={"graph_support": graph_support})
    graph_support.applied = True
    graph_support.coherence_score = coherence_score
    graph_support.related_entity_count = len(related_entities)

    if not refine_links:
        return response.model_copy(
            update={
                "related_entities": related_entities,
                "graph_support": graph_support,
                "refinement_applied": False,
                "refinement_strategy": None,
                "refinement_score": None,
                "refinement_debug": {
                    "strategy": _GRAPH_CONTEXT_REFINEMENT_STRATEGY,
                    "mode": "support_only",
                    "graph_anchor_seed_ids": [seed.entity_id for seed in anchor_seeds],
                    "preseed_suppressed_entity_ids": preseed_suppressed_entity_ids,
                    "preseed_surface_filter_debug": preseed_surface_filter_debug,
                    "related_debug": related_debug,
                    "seed_decisions": [
                        {
                            "entity_id": decision.entity_id,
                            "local_score": decision.local_score,
                            "mean_peer_support": decision.mean_peer_support,
                            "supporting_seed_ids": list(decision.supporting_seed_ids),
                            "genericity_penalty": decision.genericity_penalty,
                            "cluster_fit": decision.cluster_fit,
                            "action": decision.action,
                            "shared_context_qids": list(decision.shared_context_qids),
                        }
                        for decision in decisions
                    ],
                },
            }
        )

    decision_by_entity_id = {decision.entity_id: decision for decision in decisions}
    boosted_entity_ids: list[str] = []
    downgraded_entity_ids: list[str] = []
    suppressed_entity_ids: list[str] = []
    refined_entities: list[EntityMatch] = []
    for entity in response.entities:
        link = entity.link
        entity_id = link.entity_id.strip() if link is not None else ""
        decision = decision_by_entity_id.get(entity_id)
        if decision is None:
            refined_entities.append(entity)
            continue
        if decision.action == "suppress":
            suppressed_entity_ids.append(entity_id)
            continue
        if decision.action not in {"boost", "downgrade"} or abs(decision.score_delta) < 0.01:
            refined_entities.append(entity)
            continue
        updated_score = round(
            min(1.0, max(0.0, decision.baseline_score + decision.score_delta)),
            6,
        )
        update: dict[str, object] = {}
        if entity.relevance is not None or entity.confidence is None:
            update["relevance"] = updated_score
        if entity.confidence is not None:
            update["confidence"] = updated_score
        refined_entities.append(entity.model_copy(update=update))
        if decision.action == "boost":
            boosted_entity_ids.append(entity_id)
        else:
            downgraded_entity_ids.append(entity_id)

    refined_entities, post_filter_suppressed_entity_ids, post_filter_debug = (
        _apply_surface_conflict_filters(
            refined_entities,
            decision_by_entity_id=decision_by_entity_id,
            document_extent=document_extent,
        )
    )
    for entity_id in post_filter_suppressed_entity_ids:
        if entity_id not in suppressed_entity_ids:
            suppressed_entity_ids.append(entity_id)

    graph_support.refined_entity_count = (
        len(boosted_entity_ids) + len(downgraded_entity_ids) + len(suppressed_entity_ids)
    )
    graph_support.boosted_entity_ids = boosted_entity_ids
    graph_support.downgraded_entity_ids = downgraded_entity_ids
    graph_support.suppressed_entity_ids = suppressed_entity_ids
    graph_support.suppressed_entity_count = len(suppressed_entity_ids)
    return response.model_copy(
        update={
            "entities": refined_entities,
            "related_entities": related_entities,
            "refinement_applied": bool(
                boosted_entity_ids or downgraded_entity_ids or suppressed_entity_ids
            ),
            "refinement_strategy": _GRAPH_CONTEXT_REFINEMENT_STRATEGY,
            "refinement_score": coherence_score,
            "refinement_debug": {
                "strategy": _GRAPH_CONTEXT_REFINEMENT_STRATEGY,
                "graph_anchor_seed_ids": [seed.entity_id for seed in anchor_seeds],
                "preseed_suppressed_entity_ids": preseed_suppressed_entity_ids,
                "preseed_surface_filter_debug": preseed_surface_filter_debug,
                "related_debug": related_debug,
                "seed_decisions": [
                    {
                        "entity_id": decision.entity_id,
                        "baseline_score": decision.baseline_score,
                        "local_score": decision.local_score,
                        "mean_peer_support": decision.mean_peer_support,
                        "supporting_seed_count": decision.supporting_seed_count,
                        "supporting_seed_ids": list(decision.supporting_seed_ids),
                        "genericity_penalty": decision.genericity_penalty,
                        "cluster_fit": decision.cluster_fit,
                        "action": decision.action,
                        "score_delta": decision.score_delta,
                        "shared_context_qids": list(decision.shared_context_qids),
                    }
                    for decision in decisions
                ],
                "post_filter_debug": post_filter_debug,
            },
            "graph_support": graph_support,
        }
    )
