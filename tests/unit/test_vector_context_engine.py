import json
from pathlib import Path
from types import SimpleNamespace

from ades.config import Settings
from ades.service.models import EntityLink, EntityMatch, TagResponse
from ades.vector import graph_builder as graph_builder_module
from ades.vector.context_engine import (
    _SeedDecision,
    _apply_surface_conflict_filters,
    _discover_related_entities,
    _graph_context_anchor_seeds_from_decisions,
    _is_graph_seed_eligible,
    _linked_seed_records,
    _related_discovery_seed_qids,
    apply_graph_context,
)
from ades.vector.graph_store import GraphNodeStats, SharedGraphNode


def _bundle_dir(root: Path, *, pack_id: str = "general-en") -> Path:
    bundle_dir = root / pack_id
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True)
    (bundle_dir / "bundle.json").write_text(
        json.dumps(
            {
                "pack_id": pack_id,
                "entities_path": "normalized/entities.jsonl",
            }
        ),
        encoding="utf-8",
    )
    (normalized_dir / "entities.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "entity_id": "wikidata:Q1",
                        "canonical_text": "OpenAI",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 100,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q2",
                        "canonical_text": "Microsoft",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 95,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q3",
                        "canonical_text": "the Second",
                        "entity_type": "person",
                        "source_name": "wikidata-general-entities",
                        "popularity": 2,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q4",
                        "canonical_text": "AI Consortium",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 80,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q5",
                        "canonical_text": "Applied AI Guild",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 7,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q6",
                        "canonical_text": "Noise Alpha",
                        "entity_type": "person",
                        "source_name": "wikidata-general-entities",
                        "popularity": 3,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q7",
                        "canonical_text": "Noise Beta",
                        "entity_type": "person",
                        "source_name": "wikidata-general-entities",
                        "popularity": 3,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q287171",
                        "canonical_text": "ABC News",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 60,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return bundle_dir


def _build_graph_artifact(tmp_path: Path) -> Path:
    bundle_dir = _bundle_dir(tmp_path)
    truthy_lines = [
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q4> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q4> .",
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q5> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q5> .",
    ]
    noisy_predicates = [
        "P17",
        "P131",
        "P495",
        "P361",
        "P527",
        "P463",
        "P102",
        "P171",
        "P641",
        "P136",
        "P921",
        "P31",
        "P17",
        "P131",
        "P495",
        "P361",
        "P527",
        "P463",
    ]
    truthy_lines.extend(
        f"<http://www.wikidata.org/entity/Q3> <http://www.wikidata.org/prop/direct/{predicate}> "
        f"<http://www.wikidata.org/entity/Q9{index:02d}> ."
        for index, predicate in enumerate(noisy_predicates, start=1)
    )
    truthy_path = tmp_path / "truthy.nt"
    truthy_path.write_text("\n".join(truthy_lines) + "\n", encoding="utf-8")
    response = graph_builder_module.build_qid_graph_store(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact",
        allowed_predicates=[
            "P31",
            "P463",
            "P17",
            "P131",
            "P495",
            "P361",
            "P527",
            "P102",
            "P171",
            "P641",
            "P136",
            "P921",
        ],
    )
    return Path(response.artifact_path)


def _build_clustered_noise_graph_artifact(tmp_path: Path) -> Path:
    bundle_dir = tmp_path / "cluster-en"
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True)
    (bundle_dir / "bundle.json").write_text(
        json.dumps(
            {
                "pack_id": "cluster-en",
                "entities_path": "normalized/entities.jsonl",
            }
        ),
        encoding="utf-8",
    )
    (normalized_dir / "entities.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "entity_id": "wikidata:Q1",
                        "canonical_text": "OpenAI",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 100,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q2",
                        "canonical_text": "Microsoft",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 95,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q4",
                        "canonical_text": "AI Consortium",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 80,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q5",
                        "canonical_text": "Applied AI Guild",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 70,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q6",
                        "canonical_text": "Noise Alpha",
                        "entity_type": "person",
                        "source_name": "wikidata-general-entities",
                        "popularity": 3,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q7",
                        "canonical_text": "Noise Beta",
                        "entity_type": "person",
                        "source_name": "wikidata-general-entities",
                        "popularity": 3,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q8",
                        "canonical_text": "Anthropic",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 90,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    truthy_lines = [
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
        "<http://www.wikidata.org/entity/Q8> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
        "<http://www.wikidata.org/entity/Q8> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q4> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q4> .",
        "<http://www.wikidata.org/entity/Q8> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q4> .",
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q5> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q5> .",
        "<http://www.wikidata.org/entity/Q8> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q5> .",
        "<http://www.wikidata.org/entity/Q6> <http://www.wikidata.org/prop/direct/P17> <http://www.wikidata.org/entity/Q901> .",
        "<http://www.wikidata.org/entity/Q7> <http://www.wikidata.org/prop/direct/P17> <http://www.wikidata.org/entity/Q902> .",
    ]
    truthy_path = tmp_path / "cluster-truthy.nt"
    truthy_path.write_text("\n".join(truthy_lines) + "\n", encoding="utf-8")
    response = graph_builder_module.build_qid_graph_store(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "cluster-artifact",
        allowed_predicates=[
            "P31",
            "P463",
            "P17",
            "P131",
            "P495",
            "P361",
            "P527",
            "P102",
            "P171",
            "P641",
            "P136",
            "P921",
        ],
    )
    return Path(response.artifact_path)


class _FakeRelatedEntityStore:
    def __init__(
        self,
        *,
        shared_neighbors: list[SharedGraphNode] | None = None,
        shared_ancestors: list[SharedGraphNode] | None = None,
        node_stats_by_qid: dict[str, GraphNodeStats] | None = None,
    ) -> None:
        self._shared_neighbors = list(shared_neighbors or [])
        self._shared_ancestors = list(shared_ancestors or [])
        self._node_stats_by_qid = dict(node_stats_by_qid or {})

    def shared_neighbors(self, *args, **kwargs) -> list[SharedGraphNode]:
        del args, kwargs
        return list(self._shared_neighbors)

    def shared_ancestors(self, *args, **kwargs) -> list[SharedGraphNode]:
        del args, kwargs
        return list(self._shared_ancestors)

    def node_stats_batch(self, qids) -> dict[str, GraphNodeStats]:
        return {
            qid: self._node_stats_by_qid.get(qid, GraphNodeStats(qid=qid))
            for qid in qids
        }


def _response_with_three_linked_entities() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="OpenAI",
                label="organization",
                start=0,
                end=6,
                confidence=0.46,
                relevance=0.46,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="OpenAI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Microsoft",
                label="organization",
                start=10,
                end=19,
                confidence=0.44,
                relevance=0.44,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Microsoft",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="the Second",
                label="person",
                start=30,
                end=40,
                confidence=0.24,
                relevance=0.24,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="the Second",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _response_with_low_coherence_linked_entities() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="OpenAI",
                label="organization",
                start=0,
                end=6,
                confidence=0.46,
                relevance=0.46,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="OpenAI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Microsoft",
                label="organization",
                start=10,
                end=19,
                confidence=0.44,
                relevance=0.44,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Microsoft",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="the Second",
                label="person",
                start=30,
                end=40,
                confidence=0.24,
                relevance=0.24,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="the Second",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Noise Alpha",
                label="person",
                start=50,
                end=61,
                confidence=0.41,
                relevance=0.41,
                link=EntityLink(
                    entity_id="wikidata:Q6",
                    canonical_text="Noise Alpha",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Noise Beta",
                label="person",
                start=70,
                end=80,
                confidence=0.4,
                relevance=0.4,
                link=EntityLink(
                    entity_id="wikidata:Q7",
                    canonical_text="Noise Beta",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _exact_alias_provenance(match_source: str) -> dict[str, str]:
    return {
        "match_kind": "alias",
        "match_path": "lookup.alias.exact",
        "match_source": match_source,
        "source_pack": "general-en",
        "source_domain": "general",
    }


def _response_with_low_fidelity_seed_aliases() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Formula 1",
                label="location",
                start=0,
                end=9,
                confidence=0.61,
                relevance=0.61,
                provenance=_exact_alias_provenance("Formula 1"),
                link=EntityLink(
                    entity_id="wikidata:Q25413302",
                    canonical_text="Formuła",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="in Berlin",
                label="organization",
                start=20,
                end=29,
                confidence=0.55,
                relevance=0.55,
                provenance=_exact_alias_provenance("in Berlin"),
                link=EntityLink(
                    entity_id="wikidata:Q1661620",
                    canonical_text="Individual Network Berlin",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="the Association of German Banks",
                label="organization",
                start=40,
                end=72,
                confidence=0.63,
                relevance=0.63,
                provenance=_exact_alias_provenance("the Association of German Banks"),
                link=EntityLink(
                    entity_id="wikidata:Q2118895",
                    canonical_text="The Association",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="High Court",
                label="organization",
                start=80,
                end=90,
                confidence=0.58,
                relevance=0.58,
                provenance=_exact_alias_provenance("High Court"),
                link=EntityLink(
                    entity_id="wikidata:Q5755253",
                    canonical_text="High Court of South Africa",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Friedrich Merz",
                label="person",
                start=100,
                end=114,
                confidence=0.74,
                relevance=0.74,
                provenance=_exact_alias_provenance("Friedrich Merz"),
                link=EntityLink(
                    entity_id="wikidata:Q1692388",
                    canonical_text="Friedrich Merz",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _response_with_clustered_coherent_and_noise_entities() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="cluster-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="OpenAI",
                label="organization",
                start=0,
                end=6,
                confidence=0.46,
                relevance=0.46,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="OpenAI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Microsoft",
                label="organization",
                start=10,
                end=19,
                confidence=0.44,
                relevance=0.44,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Microsoft",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Anthropic",
                label="organization",
                start=24,
                end=33,
                confidence=0.45,
                relevance=0.45,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q8",
                    canonical_text="Anthropic",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Noise Alpha",
                label="person",
                start=44,
                end=55,
                confidence=0.41,
                relevance=0.41,
                link=EntityLink(
                    entity_id="wikidata:Q6",
                    canonical_text="Noise Alpha",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Noise Beta",
                label="person",
                start=60,
                end=70,
                confidence=0.4,
                relevance=0.4,
                link=EntityLink(
                    entity_id="wikidata:Q7",
                    canonical_text="Noise Beta",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _response_with_isolated_named_exact_org() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="OpenAI",
                label="organization",
                start=0,
                end=6,
                confidence=0.46,
                relevance=0.46,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="OpenAI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Microsoft",
                label="organization",
                start=12,
                end=21,
                confidence=0.44,
                relevance=0.44,
                mention_count=1,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Microsoft",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="ABC News",
                label="organization",
                start=930,
                end=938,
                confidence=0.52,
                relevance=0.52,
                mention_count=1,
                provenance={
                    "match_kind": "alias",
                    "match_path": "lookup.alias.exact",
                    "match_source": "ABC News",
                    "source_pack": "general-en",
                    "source_domain": "general",
                },
                link=EntityLink(
                    entity_id="wikidata:Q287171",
                    canonical_text="ABC News",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _response_with_lowercase_geonames_noise() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="OpenAI",
                label="organization",
                start=0,
                end=6,
                confidence=0.46,
                relevance=0.46,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="OpenAI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Microsoft",
                label="organization",
                start=10,
                end=19,
                confidence=0.44,
                relevance=0.44,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Microsoft",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="number two",
                label="location",
                start=30,
                end=40,
                confidence=0.5,
                relevance=0.5,
                mention_count=1,
                link=EntityLink(
                    entity_id="geonames:4955258",
                    canonical_text="Town of Westminster",
                    provider="lookup.alias.geonames",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def test_apply_graph_context_suppresses_isolated_low_fit_seed(tmp_path: Path) -> None:
    artifact_path = _build_graph_artifact(tmp_path)
    response = _response_with_three_linked_entities()
    settings = Settings(
        graph_context_enabled=True,
        graph_context_artifact_path=artifact_path,
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
        graph_context_min_supporting_seeds=2,
    )

    refined = apply_graph_context(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=True,
        refinement_depth="deep",
    )

    assert [entity.link.entity_id for entity in refined.entities if entity.link is not None] == [
        "wikidata:Q1",
        "wikidata:Q2",
    ]
    assert refined.refinement_applied is True
    assert refined.refinement_strategy == "qid_graph_context_v1"
    assert refined.graph_support is not None
    assert refined.graph_support.applied is True
    assert refined.graph_support.suppressed_entity_ids == ["wikidata:Q3"]
    assert refined.graph_support.suppressed_entity_count == 1
    assert [item.entity_id for item in refined.related_entities] == [
        "wikidata:Q4",
        "wikidata:Q5",
    ]
    assert refined.refinement_debug is not None
    assert any(
        item["entity_id"] == "wikidata:Q3" and item["action"] == "suppress"
        for item in refined.refinement_debug["seed_decisions"]
    )


def test_apply_graph_context_support_only_mode_keeps_entities(tmp_path: Path) -> None:
    artifact_path = _build_graph_artifact(tmp_path)
    response = _response_with_three_linked_entities()
    settings = Settings(
        graph_context_enabled=True,
        graph_context_artifact_path=artifact_path,
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
    )

    refined = apply_graph_context(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=False,
    )

    assert len(refined.entities) == 3
    assert refined.refinement_applied is False
    assert refined.graph_support is not None
    assert refined.graph_support.applied is True
    assert refined.graph_support.suppressed_entity_count == 0
    assert [item.entity_id for item in refined.related_entities] == [
        "wikidata:Q4",
        "wikidata:Q5",
    ]
    assert refined.refinement_debug is not None
    assert refined.refinement_debug["mode"] == "support_only"


def test_apply_graph_context_support_only_mode_uses_anchor_seed_subset_for_noisy_document(
    tmp_path: Path,
) -> None:
    artifact_path = _build_graph_artifact(tmp_path)
    response = _response_with_low_coherence_linked_entities()
    settings = Settings(
        graph_context_enabled=True,
        graph_context_artifact_path=artifact_path,
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
        graph_context_min_supporting_seeds=2,
    )

    refined = apply_graph_context(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=False,
    )

    assert len(refined.entities) == 5
    assert refined.graph_support is not None
    assert refined.graph_support.applied is True
    assert refined.graph_support.coherence_score is not None
    assert set(refined.graph_support.seed_entity_ids) == {
        "wikidata:Q1",
        "wikidata:Q2",
        "wikidata:Q3",
        "wikidata:Q6",
        "wikidata:Q7",
    }
    assert refined.graph_support.coherence_score >= 0.12
    assert [item.entity_id for item in refined.related_entities] == [
        "wikidata:Q4",
        "wikidata:Q5",
    ]
    assert refined.refinement_debug is not None
    anchor_seed_ids = set(refined.refinement_debug["graph_anchor_seed_ids"])
    assert anchor_seed_ids == {"wikidata:Q1", "wikidata:Q2"}


def test_apply_graph_context_support_only_mode_uses_strong_related_seed_cluster_for_coherence(
    tmp_path: Path,
) -> None:
    artifact_path = _build_clustered_noise_graph_artifact(tmp_path)
    response = _response_with_clustered_coherent_and_noise_entities()
    settings = Settings(
        graph_context_enabled=True,
        graph_context_artifact_path=artifact_path,
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
        graph_context_min_supporting_seeds=2,
    )

    refined = apply_graph_context(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=False,
    )

    assert len(refined.entities) == 5
    assert refined.graph_support is not None
    assert refined.graph_support.applied is True
    assert refined.graph_support.coherence_score is not None
    assert refined.graph_support.coherence_score >= 0.12
    assert set(refined.graph_support.seed_entity_ids) == {
        "wikidata:Q1",
        "wikidata:Q2",
        "wikidata:Q6",
        "wikidata:Q7",
        "wikidata:Q8",
    }
    assert [item.entity_id for item in refined.related_entities] == [
        "wikidata:Q4",
        "wikidata:Q5",
    ]
    assert refined.refinement_debug is not None
    assert set(refined.refinement_debug["graph_anchor_seed_ids"]) == {
        "wikidata:Q1",
        "wikidata:Q2",
        "wikidata:Q8",
    }
    assert refined.refinement_debug["related_debug"]["coherence_score"] >= 0.12


def test_anchor_seed_reselection_prefers_supported_people_over_weak_orgs() -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Popular",
                label="organization",
                start=190,
                end=197,
                confidence=0.52,
                relevance=0.52,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Popular",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Catholic Church",
                label="organization",
                start=280,
                end=296,
                confidence=0.49,
                relevance=0.49,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Catholic Church",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="President Donald Trump",
                label="person",
                start=3,
                end=25,
                confidence=0.72,
                relevance=0.72,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Donald Trump",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Karoline Leavitt",
                label="person",
                start=32,
                end=48,
                confidence=0.64,
                relevance=0.64,
                link=EntityLink(
                    entity_id="wikidata:Q4",
                    canonical_text="Karoline Leavitt",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="JD Vance",
                label="person",
                start=55,
                end=63,
                confidence=0.61,
                relevance=0.61,
                link=EntityLink(
                    entity_id="wikidata:Q5",
                    canonical_text="JD Vance",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )
    seeds = _linked_seed_records(response)
    decisions = [
        _SeedDecision(
            entity_index=0,
            entity_id="wikidata:Q1",
            qid="Q1",
            baseline_score=0.52,
            local_score=0.56,
            mean_peer_support=0.0,
            supporting_seed_count=0,
            genericity_penalty=0.0,
            cluster_fit=0.25,
            action="keep",
            score_delta=0.0,
            supporting_seed_ids=(),
            shared_context_qids=(),
        ),
        _SeedDecision(
            entity_index=1,
            entity_id="wikidata:Q2",
            qid="Q2",
            baseline_score=0.49,
            local_score=0.41,
            mean_peer_support=0.0,
            supporting_seed_count=0,
            genericity_penalty=0.0,
            cluster_fit=0.2,
            action="downgrade",
            score_delta=-0.04,
            supporting_seed_ids=(),
            shared_context_qids=(),
        ),
        _SeedDecision(
            entity_index=2,
            entity_id="wikidata:Q3",
            qid="Q3",
            baseline_score=0.72,
            local_score=0.76,
            mean_peer_support=0.03,
            supporting_seed_count=2,
            genericity_penalty=0.0,
            cluster_fit=0.36,
            action="keep",
            score_delta=0.0,
            supporting_seed_ids=("wikidata:Q4", "wikidata:Q5"),
            shared_context_qids=("Qx",),
        ),
        _SeedDecision(
            entity_index=3,
            entity_id="wikidata:Q4",
            qid="Q4",
            baseline_score=0.64,
            local_score=0.68,
            mean_peer_support=0.05,
            supporting_seed_count=3,
            genericity_penalty=0.0,
            cluster_fit=0.34,
            action="keep",
            score_delta=0.0,
            supporting_seed_ids=("wikidata:Q3", "wikidata:Q5", "wikidata:Q1"),
            shared_context_qids=("Qx",),
        ),
        _SeedDecision(
            entity_index=4,
            entity_id="wikidata:Q5",
            qid="Q5",
            baseline_score=0.61,
            local_score=0.65,
            mean_peer_support=0.02,
            supporting_seed_count=1,
            genericity_penalty=0.0,
            cluster_fit=0.3,
            action="keep",
            score_delta=0.0,
            supporting_seed_ids=("wikidata:Q4",),
            shared_context_qids=("Qx",),
        ),
    ]

    anchor_seeds = _graph_context_anchor_seeds_from_decisions(
        seeds,
        decisions=decisions,
        document_extent=300,
    )

    assert [seed.entity_id for seed in anchor_seeds] == [
        "wikidata:Q3",
        "wikidata:Q4",
        "wikidata:Q5",
    ]


def test_anchor_seed_reselection_prefers_supported_actors_over_supported_locations() -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="National Security Service",
                label="organization",
                start=0,
                end=25,
                confidence=0.63,
                relevance=0.63,
                mention_count=4,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="National Security Service",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="President Jane Doe",
                label="person",
                start=32,
                end=50,
                confidence=0.66,
                relevance=0.66,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Jane Doe",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Grand Ballroom Hotel",
                label="location",
                start=58,
                end=78,
                confidence=0.74,
                relevance=0.74,
                mention_count=3,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Grand Ballroom Hotel",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Alex Smith",
                label="person",
                start=86,
                end=96,
                confidence=0.6,
                relevance=0.6,
                mention_count=1,
                link=EntityLink(
                    entity_id="wikidata:Q4",
                    canonical_text="Alex Smith",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )
    seeds = _linked_seed_records(response)
    decisions = [
        _SeedDecision(
            entity_index=0,
            entity_id="wikidata:Q1",
            qid="Q1",
            baseline_score=0.63,
            local_score=0.71,
            mean_peer_support=0.08,
            supporting_seed_count=6,
            genericity_penalty=0.0,
            cluster_fit=0.38,
            action="keep",
            score_delta=0.0,
            supporting_seed_ids=("wikidata:Q2", "wikidata:Q3", "wikidata:Q4"),
            shared_context_qids=("Qx",),
        ),
        _SeedDecision(
            entity_index=1,
            entity_id="wikidata:Q2",
            qid="Q2",
            baseline_score=0.66,
            local_score=0.7,
            mean_peer_support=0.11,
            supporting_seed_count=5,
            genericity_penalty=0.0,
            cluster_fit=0.37,
            action="keep",
            score_delta=0.0,
            supporting_seed_ids=("wikidata:Q1", "wikidata:Q3", "wikidata:Q4"),
            shared_context_qids=("Qx",),
        ),
        _SeedDecision(
            entity_index=2,
            entity_id="wikidata:Q3",
            qid="Q3",
            baseline_score=0.74,
            local_score=0.82,
            mean_peer_support=0.06,
            supporting_seed_count=6,
            genericity_penalty=0.0,
            cluster_fit=0.41,
            action="keep",
            score_delta=0.0,
            supporting_seed_ids=("wikidata:Q1", "wikidata:Q2", "wikidata:Q4"),
            shared_context_qids=("Qx",),
        ),
        _SeedDecision(
            entity_index=3,
            entity_id="wikidata:Q4",
            qid="Q4",
            baseline_score=0.6,
            local_score=0.64,
            mean_peer_support=0.07,
            supporting_seed_count=4,
            genericity_penalty=0.0,
            cluster_fit=0.33,
            action="keep",
            score_delta=0.0,
            supporting_seed_ids=("wikidata:Q1", "wikidata:Q2"),
            shared_context_qids=("Qx",),
        ),
    ]

    anchor_seeds = _graph_context_anchor_seeds_from_decisions(
        seeds,
        decisions=decisions,
        document_extent=300,
    )

    assert [seed.entity_id for seed in anchor_seeds] == [
        "wikidata:Q1",
        "wikidata:Q2",
        "wikidata:Q4",
    ]


def test_discover_related_entities_suppresses_zero_degree_graph_only_location_candidate() -> None:
    store = _FakeRelatedEntityStore(
        shared_ancestors=[
            SharedGraphNode(
                qid="Q30",
                entity_id="wikidata:Q30",
                canonical_text="United States",
                support_count=2,
                supporting_qids=("Q1", "Q2"),
                min_depth=1,
                entity_type="location",
                source_name="wikidata-general-entities",
                popularity=100.0,
                packs=("general-en",),
            )
        ]
    )
    settings = Settings(
        graph_context_enabled=True,
        graph_context_min_supporting_seeds=2,
        graph_context_candidate_limit=64,
        graph_context_seed_neighbor_limit=24,
    )

    related_entities, debug = _discover_related_entities(
        store,
        surviving_seed_qids=("Q1", "Q2"),
        related_seed_qids=("Q1", "Q2"),
        coherence_score=0.22,
        extracted_entity_ids=set(),
        settings=settings,
    )

    assert related_entities == []
    assert debug is not None
    assert debug["ranked_candidate_qids"] == ["Q30"]


def test_discover_related_entities_keeps_zero_degree_graph_only_organization_candidate() -> None:
    store = _FakeRelatedEntityStore(
        shared_ancestors=[
            SharedGraphNode(
                qid="Q400",
                entity_id="wikidata:Q400",
                canonical_text="Applied AI Guild",
                support_count=2,
                supporting_qids=("Q1", "Q2"),
                min_depth=1,
                entity_type="organization",
                source_name="wikidata-general-entities",
                popularity=20.0,
                packs=("general-en",),
            )
        ]
    )
    settings = Settings(
        graph_context_enabled=True,
        graph_context_min_supporting_seeds=2,
        graph_context_candidate_limit=64,
        graph_context_seed_neighbor_limit=24,
    )

    related_entities, debug = _discover_related_entities(
        store,
        surviving_seed_qids=("Q1", "Q2"),
        related_seed_qids=("Q1", "Q2"),
        coherence_score=0.22,
        extracted_entity_ids=set(),
        settings=settings,
    )

    assert [item.entity_id for item in related_entities] == ["wikidata:Q400"]
    assert debug is not None
    assert debug["ranked_candidate_qids"] == ["Q400"]


def test_related_discovery_seed_qids_keeps_supported_kept_seed() -> None:
    decisions = [
        _SeedDecision(
            entity_index=0,
            entity_id="wikidata:Q1",
            qid="Q1",
            baseline_score=0.74,
            local_score=0.62,
            mean_peer_support=0.03096,
            supporting_seed_count=1,
            genericity_penalty=0.0,
            cluster_fit=0.321453,
            action="keep",
            score_delta=0.0,
            supporting_seed_ids=("wikidata:Q2",),
            shared_context_qids=("Q100",),
        ),
        _SeedDecision(
            entity_index=1,
            entity_id="wikidata:Q2",
            qid="Q2",
            baseline_score=0.73,
            local_score=0.61,
            mean_peer_support=0.03096,
            supporting_seed_count=1,
            genericity_penalty=0.0,
            cluster_fit=0.306288,
            action="keep",
            score_delta=0.0,
            supporting_seed_ids=("wikidata:Q1",),
            shared_context_qids=("Q100",),
        ),
        _SeedDecision(
            entity_index=2,
            entity_id="wikidata:Q3",
            qid="Q3",
            baseline_score=0.72,
            local_score=0.28,
            mean_peer_support=0.0,
            supporting_seed_count=0,
            genericity_penalty=0.0,
            cluster_fit=0.12,
            action="keep",
            score_delta=0.0,
            supporting_seed_ids=(),
            shared_context_qids=(),
        ),
    ]

    assert _related_discovery_seed_qids(decisions) == ("Q1", "Q2")


def test_discover_related_entities_keeps_high_degree_supported_organization_candidate() -> None:
    store = _FakeRelatedEntityStore(
        shared_neighbors=[
            SharedGraphNode(
                qid="Q400",
                entity_id="wikidata:Q400",
                canonical_text="Republican Party",
                support_count=2,
                supporting_qids=("Q1", "Q2"),
                min_depth=1,
                entity_type="organization",
                source_name="wikidata-general-entities",
                popularity=120.0,
                packs=("general-en",),
            )
        ],
        node_stats_by_qid={
            "Q400": GraphNodeStats(
                qid="Q400",
                out_degree=6,
                in_degree=24929,
                degree_total=24935,
            )
        },
    )
    settings = Settings(
        graph_context_enabled=True,
        graph_context_min_supporting_seeds=2,
        graph_context_candidate_limit=64,
        graph_context_seed_neighbor_limit=24,
    )

    related_entities, debug = _discover_related_entities(
        store,
        surviving_seed_qids=("Q1", "Q2"),
        related_seed_qids=("Q1", "Q2"),
        coherence_score=0.1196,
        extracted_entity_ids=set(),
        settings=settings,
    )

    assert [item.entity_id for item in related_entities] == ["wikidata:Q400"]
    assert related_entities[0].score == 0.0744
    assert debug is not None
    assert debug["ranked_candidate_qids"] == ["Q400"]


def test_discover_related_entities_rejects_supported_organization_below_low_coherence_floor() -> None:
    store = _FakeRelatedEntityStore(
        shared_neighbors=[
            SharedGraphNode(
                qid="Q400",
                entity_id="wikidata:Q400",
                canonical_text="Republican Party",
                support_count=2,
                supporting_qids=("Q1", "Q2"),
                min_depth=1,
                entity_type="organization",
                source_name="wikidata-general-entities",
                popularity=120.0,
                packs=("general-en",),
            )
        ],
        node_stats_by_qid={
            "Q400": GraphNodeStats(
                qid="Q400",
                out_degree=6,
                in_degree=24929,
                degree_total=24935,
            )
        },
    )
    settings = Settings(
        graph_context_enabled=True,
        graph_context_min_supporting_seeds=2,
        graph_context_candidate_limit=64,
        graph_context_seed_neighbor_limit=24,
    )

    related_entities, debug = _discover_related_entities(
        store,
        surviving_seed_qids=("Q1", "Q2"),
        related_seed_qids=("Q1", "Q2"),
        coherence_score=0.0516,
        extracted_entity_ids=set(),
        settings=settings,
    )

    assert related_entities == []
    assert debug is not None
    assert debug["reason"] == "low_document_coherence"


def test_linked_seed_records_skip_low_fidelity_exact_alias_seeds() -> None:
    response = _response_with_low_fidelity_seed_aliases()

    seeds = _linked_seed_records(response)

    assert [seed.entity_id for seed in seeds] == ["wikidata:Q1692388"]


def test_graph_seed_eligibility_rejects_collective_head_alias_mismatch() -> None:
    entity = EntityMatch(
        text="Jewish community",
        label="organization",
        start=0,
        end=17,
        confidence=0.6896,
        relevance=0.6008,
        mention_count=2,
        provenance=_exact_alias_provenance("Jewish community"),
        link=EntityLink(
            entity_id="wikidata:Q7325",
            canonical_text="Jewish people",
            provider="lookup.alias.exact",
        ),
    )

    assert _is_graph_seed_eligible(entity) is False


def test_apply_graph_context_preserves_isolated_named_exact_org_as_downgraded_seed(
    tmp_path: Path,
) -> None:
    artifact_path = _build_graph_artifact(tmp_path)
    response = _response_with_isolated_named_exact_org()
    settings = Settings(
        graph_context_enabled=True,
        graph_context_artifact_path=artifact_path,
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
        graph_context_min_supporting_seeds=2,
    )

    refined = apply_graph_context(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=True,
        refinement_depth="light",
    )

    assert [entity.link.entity_id for entity in refined.entities if entity.link is not None] == [
        "wikidata:Q1",
        "wikidata:Q2",
        "wikidata:Q287171",
    ]
    assert refined.graph_support is not None
    assert refined.graph_support.applied is True
    assert refined.graph_support.suppressed_entity_ids == []
    assert refined.graph_support.downgraded_entity_ids == ["wikidata:Q287171"]
    assert refined.refinement_debug is not None
    assert any(
        item["entity_id"] == "wikidata:Q287171" and item["action"] == "downgrade"
        for item in refined.refinement_debug["seed_decisions"]
    )


def test_surface_conflict_filter_suppresses_shorter_conflicting_alias() -> None:
    entities = [
        EntityMatch(
            text="Sir Keir Starmer",
            label="person",
            start=0,
            end=17,
            confidence=0.72,
            relevance=0.72,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q1",
                canonical_text="Keir Starmer",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Sir Keir",
            label="person",
            start=25,
            end=33,
            confidence=0.61,
            relevance=0.61,
            mention_count=7,
            link=EntityLink(
                entity_id="wikidata:Q2",
                canonical_text="David Keir",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Sir Keir Starmer"]
    assert suppressed_ids == ["wikidata:Q2"]
    assert debug["surface_conflict_suppressed"] == ["wikidata:Q2"]


def test_apply_graph_context_support_only_mode_preseed_filters_conflicting_aliases(
    tmp_path: Path,
) -> None:
    artifact_path = _build_graph_artifact(tmp_path)
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Sir Keir Starmer",
                label="person",
                start=0,
                end=17,
                confidence=0.72,
                relevance=0.72,
                mention_count=2,
                provenance=_exact_alias_provenance("Sir Keir Starmer"),
                link=EntityLink(
                    entity_id="wikidata:Q6383803",
                    canonical_text="Keir Starmer",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Sir Keir",
                label="person",
                start=25,
                end=33,
                confidence=0.61,
                relevance=0.61,
                mention_count=7,
                provenance=_exact_alias_provenance("Sir Keir"),
                link=EntityLink(
                    entity_id="wikidata:Q5236641",
                    canonical_text="David Keir",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )
    settings = Settings(
        graph_context_enabled=True,
        graph_context_artifact_path=artifact_path,
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
    )

    refined = apply_graph_context(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=False,
    )

    assert refined.graph_support is not None
    assert refined.graph_support.seed_entity_ids == ["wikidata:Q6383803"]
    assert refined.refinement_debug is not None
    assert refined.refinement_debug["preseed_suppressed_entity_ids"] == ["wikidata:Q5236641"]
    assert refined.refinement_debug["preseed_surface_filter_debug"]["surface_conflict_suppressed"] == [
        "wikidata:Q5236641"
    ]


def test_surface_conflict_filter_suppresses_low_value_email_entity() -> None:
    entities = [
        EntityMatch(
            text="life@newsweek.com",
            label="email_address",
            start=0,
            end=17,
            confidence=0.85,
            relevance=0.61,
            mention_count=1,
            link=EntityLink(
                entity_id="ades:rule_regex:general-en:email_address:life-newsweek-com",
                canonical_text="life@newsweek.com",
                provider="rule.regex",
            ),
        ),
        EntityMatch(
            text="Newsweek",
            label="organization",
            start=20,
            end=28,
            confidence=0.72,
            relevance=0.72,
            mention_count=1,
            link=EntityLink(
                entity_id="wikidata:Q18887",
                canonical_text="Newsweek",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Newsweek"]
    assert suppressed_ids == ["ades:rule_regex:general-en:email_address:life-newsweek-com"]
    assert debug["email_address_suppressed"] == [
        "ades:rule_regex:general-en:email_address:life-newsweek-com"
    ]


def test_surface_conflict_filter_suppresses_generic_lowercase_location_phrase() -> None:
    entities = [
        EntityMatch(
            text="high street",
            label="location",
            start=0,
            end=11,
            confidence=0.75,
            relevance=0.64,
            mention_count=1,
            link=EntityLink(
                entity_id="wikidata:Q105716706",
                canonical_text="High Street",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Boots",
            label="organization",
            start=15,
            end=20,
            confidence=0.72,
            relevance=0.72,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q10134",
                canonical_text="Boots",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Boots"]
    assert suppressed_ids == ["wikidata:Q105716706"]
    assert debug["generic_lowercase_location_suppressed"] == ["wikidata:Q105716706"]


def test_surface_conflict_filter_suppresses_numeric_fragment_location() -> None:
    entities = [
        EntityMatch(
            text="43rd",
            label="location",
            start=0,
            end=4,
            confidence=0.71,
            relevance=0.58,
            mention_count=1,
            link=EntityLink(
                entity_id="wikidata:Q2816829",
                canonical_text="43rd",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Alberta",
            label="location",
            start=10,
            end=17,
            confidence=0.8,
            relevance=0.8,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q1951",
                canonical_text="Alberta",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Alberta"]
    assert suppressed_ids == ["wikidata:Q2816829"]
    assert debug["numeric_fragment_suppressed"] == ["wikidata:Q2816829"]


def test_surface_conflict_filter_suppresses_titlecase_common_alias() -> None:
    entities = [
        EntityMatch(
            text="Good luck",
            label="location",
            start=0,
            end=9,
            confidence=0.75,
            relevance=0.54,
            mention_count=1,
            provenance={
                "match_kind": "alias",
                "match_path": "lookup.alias.exact",
                "match_source": "Good luck",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="wikidata:Q15221860",
                canonical_text="Good luck",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="AKC",
            label="organization",
            start=20,
            end=23,
            confidence=0.67,
            relevance=0.52,
            mention_count=1,
            link=EntityLink(
                entity_id="ades:heuristic_explicit_definition_acronym:general-en:organization:american-kennel-club",
                canonical_text="American Kennel Club",
                provider="heuristic.explicit_definition_acronym",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["AKC"]
    assert suppressed_ids == ["wikidata:Q15221860"]
    assert debug["titlecase_common_alias_suppressed"] == ["wikidata:Q15221860"]


def test_surface_conflict_filter_suppresses_weak_led_exact_alias() -> None:
    entities = [
        EntityMatch(
            text="For Britain",
            label="organization",
            start=0,
            end=11,
            confidence=0.48,
            relevance=0.48,
            mention_count=1,
            provenance={
                "match_kind": "alias",
                "match_path": "lookup.alias.exact",
                "match_source": "For Britain",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="wikidata:Q42530925",
                canonical_text="For Britain",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Britain",
            label="location",
            start=15,
            end=22,
            confidence=0.82,
            relevance=0.82,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q145",
                canonical_text="United Kingdom",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Britain"]
    assert suppressed_ids == ["wikidata:Q42530925"]
    assert debug["weak_led_exact_alias_suppressed"] == ["wikidata:Q42530925"]


def test_surface_conflict_filter_suppresses_late_tail_exact_alias() -> None:
    entities = [
        EntityMatch(
            text="China",
            label="location",
            start=80,
            end=85,
            confidence=0.82,
            relevance=0.82,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q148",
                canonical_text="China",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Evening Standard",
            label="organization",
            start=930,
            end=946,
            confidence=0.45,
            relevance=0.45,
            mention_count=1,
            provenance={
                "match_kind": "alias",
                "match_path": "lookup.alias.exact",
                "match_source": "Evening Standard",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="wikidata:Q5419995",
                canonical_text="Evening Standard",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
        document_extent=1000,
    )

    assert [entity.text for entity in filtered] == ["China"]
    assert suppressed_ids == ["wikidata:Q5419995"]
    assert debug["late_tail_exact_alias_suppressed"] == ["wikidata:Q5419995"]


def test_surface_conflict_filter_keeps_supported_downgraded_late_tail_org_alias() -> None:
    entity = EntityMatch(
        text="ABC News",
        label="organization",
        start=930,
        end=938,
        confidence=0.52,
        relevance=0.52,
        mention_count=1,
        provenance={
            "match_kind": "alias",
            "match_path": "lookup.alias.exact",
            "match_source": "ABC News",
            "source_pack": "general-en",
            "source_domain": "general",
        },
        link=EntityLink(
            entity_id="wikidata:Q287171",
            canonical_text="ABC News",
            provider="lookup.alias.exact",
        ),
    )
    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        [entity],
        decision_by_entity_id={
            "wikidata:Q287171": _SeedDecision(
                entity_index=0,
                entity_id="wikidata:Q287171",
                qid="Q287171",
                baseline_score=0.5204,
                local_score=0.4404,
                mean_peer_support=0.11,
                supporting_seed_count=3,
                genericity_penalty=0.0,
                cluster_fit=0.25868,
                action="downgrade",
                score_delta=-0.06,
                supporting_seed_ids=(
                    "wikidata:Q2074898",
                    "wikidata:Q500606",
                    "wikidata:Q5919677",
                ),
                shared_context_qids=("Q30",),
            )
        },
        document_extent=1000,
    )

    assert [item.text for item in filtered] == ["ABC News"]
    assert suppressed_ids == []
    assert debug["late_tail_exact_alias_suppressed"] == []


def test_surface_conflict_filter_keeps_named_downgraded_late_tail_org_alias_without_support() -> None:
    entity = EntityMatch(
        text="ABC News",
        label="organization",
        start=930,
        end=938,
        confidence=0.52,
        relevance=0.52,
        mention_count=1,
        provenance={
            "match_kind": "alias",
            "match_path": "lookup.alias.exact",
            "match_source": "ABC News",
            "source_pack": "general-en",
            "source_domain": "general",
        },
        link=EntityLink(
            entity_id="wikidata:Q287171",
            canonical_text="ABC News",
            provider="lookup.alias.exact",
        ),
    )
    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        [entity],
        decision_by_entity_id={
            "wikidata:Q287171": _SeedDecision(
                entity_index=0,
                entity_id="wikidata:Q287171",
                qid="Q287171",
                baseline_score=0.5201,
                local_score=0.4401,
                mean_peer_support=0.0066,
                supporting_seed_count=0,
                genericity_penalty=0.0,
                cluster_fit=0.201675,
                action="downgrade",
                score_delta=-0.04,
                supporting_seed_ids=(),
                shared_context_qids=("Q30",),
            )
        },
        document_extent=1000,
    )

    assert [item.text for item in filtered] == ["ABC News"]
    assert suppressed_ids == []
    assert debug["late_tail_exact_alias_suppressed"] == []


def test_surface_conflict_filter_suppresses_acronym_expansion_fragment_alias() -> None:
    entities = [
        EntityMatch(
            text="Task Force",
            label="organization",
            start=0,
            end=10,
            confidence=0.47,
            relevance=0.47,
            mention_count=1,
            provenance={
                "match_kind": "alias",
                "match_path": "lookup.alias.exact",
                "match_source": "Task Force",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="wikidata:Q91945108",
                canonical_text="Task Force",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="JMTF",
            label="organization",
            start=30,
            end=34,
            confidence=0.62,
            relevance=0.62,
            mention_count=1,
            provenance={
                "match_kind": "alias",
                "match_path": "heuristic.explicit_definition_acronym",
                "match_source": "joint military task force",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="ades:heuristic_explicit_definition_acronym:general-en:organization:joint-military-task-force",
                canonical_text="Joint Military Task Force",
                provider="heuristic.explicit_definition_acronym",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["JMTF"]
    assert suppressed_ids == [
        "wikidata:Q91945108",
    ]
    assert debug["acronym_fragment_suppressed"] == ["wikidata:Q91945108"]


def test_surface_conflict_filter_suppresses_article_led_canonical_mismatch_alias() -> None:
    entities = [
        EntityMatch(
            text="the Commonwealth",
            label="organization",
            start=0,
            end=16,
            confidence=0.45,
            relevance=0.45,
            mention_count=2,
            provenance={
                "match_kind": "alias",
                "match_path": "lookup.alias.exact",
                "match_source": "the Commonwealth",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="wikidata:Q7785",
                canonical_text="Commonwealth of Nations",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Australia",
            label="location",
            start=20,
            end=29,
            confidence=0.81,
            relevance=0.81,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q408",
                canonical_text="Australia",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Australia"]
    assert suppressed_ids == ["wikidata:Q7785"]
    assert debug["weak_led_exact_alias_suppressed"] == ["wikidata:Q7785"]


def test_apply_graph_context_suppresses_lowercase_geonames_noise(tmp_path: Path) -> None:
    artifact_path = _build_graph_artifact(tmp_path)
    response = _response_with_lowercase_geonames_noise()
    settings = Settings(
        graph_context_enabled=True,
        graph_context_artifact_path=artifact_path,
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
    )

    refined = apply_graph_context(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=True,
    )

    assert [entity.text for entity in refined.entities] == ["OpenAI", "Microsoft"]
    assert refined.graph_support is not None
    assert "geonames:4955258" in refined.graph_support.suppressed_entity_ids
    assert refined.refinement_debug is not None
    assert refined.refinement_debug["post_filter_debug"]["lowercase_geonames_suppressed"] == [
        "geonames:4955258"
    ]
