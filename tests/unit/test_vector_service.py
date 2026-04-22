import json
from pathlib import Path

from ades.config import Settings
from ades.service.models import EntityLink, EntityMatch, TagResponse
from ades.storage.backend import MetadataBackend, RuntimeTarget
from ades.vector import graph_builder as graph_builder_module
from ades.vector import service as vector_service
from ades.vector.qdrant import QdrantVectorSearchError


def _response_with_linked_entities() -> TagResponse:
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
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _graph_context_bundle_dir(root: Path, *, pack_id: str = "general-en") -> Path:
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
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q2",
                        "canonical_text": "Microsoft",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q3",
                        "canonical_text": "the Second",
                        "entity_type": "person",
                        "source_name": "wikidata-general-entities",
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q4",
                        "canonical_text": "AI Consortium",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q8",
                        "canonical_text": "Enterprise AI",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return bundle_dir


def _graph_context_artifact(tmp_path: Path) -> Path:
    bundle_dir = _graph_context_bundle_dir(tmp_path)
    truthy_lines = [
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q4> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q4> .",
        "<http://www.wikidata.org/entity/Q8> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
    ]
    truthy_lines.extend(
        f"<http://www.wikidata.org/entity/Q3> <http://www.wikidata.org/prop/direct/P17> <http://www.wikidata.org/entity/Q9{index:02d}> ."
        for index in range(1, 21)
    )
    truthy_path = tmp_path / "truthy.nt"
    truthy_path.write_text("\n".join(truthy_lines) + "\n", encoding="utf-8")
    response = graph_builder_module.build_qid_graph_store(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact",
        allowed_predicates=["P31", "P463", "P361", "P17"],
    )
    return Path(response.artifact_path)


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


def test_graph_context_vector_query_seed_entity_ids_prefers_non_person_seeds() -> None:
    response = _response_with_three_linked_entities().model_copy(
        update={
            "refinement_debug": {
                "seed_decisions": [
                    {
                        "entity_id": "wikidata:Q3",
                        "action": "keep",
                        "cluster_fit": 0.34,
                        "mean_peer_support": 0.08,
                        "supporting_seed_count": 4,
                        "genericity_penalty": 0.0,
                        "shared_context_qids": ["Q5"],
                    },
                    {
                        "entity_id": "wikidata:Q1",
                        "action": "keep",
                        "cluster_fit": 0.26,
                        "mean_peer_support": 0.05,
                        "supporting_seed_count": 2,
                        "genericity_penalty": 0.0,
                        "shared_context_qids": ["Q30"],
                    },
                    {
                        "entity_id": "wikidata:Q2",
                        "action": "boost",
                        "cluster_fit": 0.2,
                        "mean_peer_support": 0.03,
                        "supporting_seed_count": 1,
                        "genericity_penalty": 0.0,
                        "shared_context_qids": [],
                    },
                ]
            }
        }
    )

    query_seed_ids = vector_service._graph_context_vector_query_seed_entity_ids(
        response
    )

    assert query_seed_ids[:2] == ["wikidata:Q2", "wikidata:Q1"]
    assert query_seed_ids[-1] == "wikidata:Q3"


def test_enrich_tag_response_with_related_entities_aggregates_shared_neighbors(monkeypatch) -> None:
    response = _response_with_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
    )
    captured_filters: list[dict[str, object] | None] = []

    class _FakeClient:
        def __init__(self, base_url: str, *, api_key: str | None = None) -> None:
            assert base_url == "http://qdrant.local:6333"
            assert api_key is None

        def __enter__(self) -> "_FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def query_similar_by_id(
            self,
            collection_name: str,
            *,
            point_id: str,
            limit: int,
            filter_payload=None,
        ):
            assert collection_name == "ades-qids-current"
            assert limit >= 7
            captured_filters.append(filter_payload)
            if point_id == "wikidata:Q1":
                return [
                    vector_service.QdrantNearestPoint("wikidata:Q1", 1.0, {}),
                    vector_service.QdrantNearestPoint(
                        "wikidata:Q9",
                        0.82,
                        {
                            "canonical_text": "Sam Altman",
                            "entity_type": "person",
                            "packs": ["general-en"],
                            "source_name": "wikidata-general-entities",
                        },
                    ),
                    vector_service.QdrantNearestPoint(
                        "wikidata:Q10",
                        0.8,
                        {
                            "canonical_text": "Anthropic",
                            "entity_type": "organization",
                            "packs": ["general-en"],
                        },
                    ),
                ]
            return [
                vector_service.QdrantNearestPoint("wikidata:Q2", 1.0, {}),
                vector_service.QdrantNearestPoint(
                    "wikidata:Q9",
                    0.85,
                    {
                        "canonical_text": "Sam Altman",
                        "entity_type": "person",
                        "packs": ["general-en"],
                        "source_name": "wikidata-general-entities",
                    },
                ),
                vector_service.QdrantNearestPoint(
                    "wikidata:Q77",
                    0.95,
                    {
                        "canonical_text": "Finance Entity",
                        "entity_type": "organization",
                        "packs": ["finance-en"],
                    },
                ),
            ]

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert enriched.graph_support is not None
    assert enriched.graph_support.applied is True
    assert enriched.graph_support.requested_related_entities is True
    assert enriched.graph_support.seed_entity_ids == ["wikidata:Q1", "wikidata:Q2"]
    assert [item.entity_id for item in enriched.related_entities] == [
        "wikidata:Q9",
        "wikidata:Q10",
    ]
    assert captured_filters == [{"packs": ["general-en"]}, {"packs": ["general-en"]}]
    assert enriched.related_entities[0].shared_seed_count == 2
    assert enriched.related_entities[0].score > enriched.related_entities[1].score


def test_enrich_tag_response_with_related_entities_can_refine_supported_links(
    monkeypatch,
) -> None:
    response = _response_with_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
    )

    class _FakeClient:
        def __init__(self, base_url: str, *, api_key: str | None = None) -> None:
            assert base_url == "http://qdrant.local:6333"
            assert api_key is None

        def __enter__(self) -> "_FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def query_similar_by_id(
            self,
            collection_name: str,
            *,
            point_id: str,
            limit: int,
            filter_payload=None,
        ):
            assert collection_name == "ades-qids-current"
            assert filter_payload == {"packs": ["general-en"]}
            if point_id == "wikidata:Q1":
                return [
                    vector_service.QdrantNearestPoint("wikidata:Q1", 1.0, {}),
                    vector_service.QdrantNearestPoint("wikidata:Q2", 0.91, {}),
                ]
            return [
                vector_service.QdrantNearestPoint("wikidata:Q2", 1.0, {}),
                vector_service.QdrantNearestPoint("wikidata:Q1", 0.89, {}),
            ]

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_graph_support=True,
        refine_links=True,
        refinement_depth="deep",
    )

    assert enriched.refinement_applied is True
    assert enriched.refinement_strategy == "qid_graph_coherence_v1"
    assert enriched.refinement_score is not None
    assert enriched.graph_support is not None
    assert enriched.graph_support.requested_refinement is True
    assert enriched.graph_support.refined_entity_count == 2
    assert enriched.graph_support.boosted_entity_ids == ["wikidata:Q1", "wikidata:Q2"]
    assert [entity.relevance for entity in enriched.entities] == [0.585, 0.57]
    assert enriched.refinement_debug is not None
    assert "seed_support" in enriched.refinement_debug


def test_enrich_tag_response_with_related_entities_warns_when_disabled() -> None:
    response = _response_with_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=False,
        vector_search_url="http://qdrant.local:6333",
    )

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert enriched.related_entities == []
    assert enriched.graph_support is not None
    assert enriched.graph_support.warnings == ["vector_search_disabled"]


def test_enrich_tag_response_with_related_entities_skips_missing_seed_points(
    monkeypatch,
) -> None:
    response = _response_with_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
    )

    class _FakeClient:
        def __init__(self, base_url: str, *, api_key: str | None = None) -> None:
            assert base_url == "http://qdrant.local:6333"
            assert api_key is None

        def __enter__(self) -> "_FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def query_similar_by_id(
            self,
            collection_name: str,
            *,
            point_id: str,
            limit: int,
            filter_payload=None,
        ):
            assert collection_name == "ades-qids-current"
            assert filter_payload == {"packs": ["general-en"]}
            if point_id == "wikidata:Q1":
                return [
                    vector_service.QdrantNearestPoint("wikidata:Q1", 1.0, {}),
                    vector_service.QdrantNearestPoint(
                        "wikidata:Q9",
                        0.82,
                        {
                            "canonical_text": "Sam Altman",
                            "entity_type": "person",
                            "packs": ["general-en"],
                            "source_name": "wikidata-general-entities",
                        },
                    ),
                ]
            raise QdrantVectorSearchError(
                "Qdrant request failed (404): {'error': 'Not found: No point with id missing found'}",
                status_code=404,
            )

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
    )

    assert enriched.graph_support is not None
    assert enriched.graph_support.applied is True
    assert enriched.graph_support.seed_entity_ids == ["wikidata:Q1", "wikidata:Q2"]
    assert enriched.graph_support.warnings == ["vector_search_seed_missing:wikidata:Q2"]
    assert [item.entity_id for item in enriched.related_entities] == ["wikidata:Q9"]


def test_enrich_tag_response_with_related_entities_applies_graph_context_before_vector_related_entities(
    monkeypatch,
    tmp_path: Path,
) -> None:
    response = _response_with_three_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
        graph_context_enabled=True,
        graph_context_artifact_path=_graph_context_artifact(tmp_path),
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
        graph_context_min_supporting_seeds=2,
    )
    queried_point_ids: list[str] = []

    class _FakeClient:
        def __init__(self, base_url: str, *, api_key: str | None = None) -> None:
            assert base_url == "http://qdrant.local:6333"
            assert api_key is None

        def __enter__(self) -> "_FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def query_similar_by_id(
            self,
            collection_name: str,
            *,
            point_id: str,
            limit: int,
            filter_payload=None,
        ):
            assert collection_name == "ades-qids-current"
            queried_point_ids.append(point_id)
            if point_id == "wikidata:Q1":
                return [
                    vector_service.QdrantNearestPoint("wikidata:Q1", 1.0, {}),
                    vector_service.QdrantNearestPoint(
                        "wikidata:Q9",
                        0.82,
                        {
                            "canonical_text": "Sam Altman",
                            "entity_type": "person",
                            "packs": ["general-en"],
                            "source_name": "wikidata-general-entities",
                        },
                    ),
                ]
            return [
                vector_service.QdrantNearestPoint("wikidata:Q2", 1.0, {}),
                vector_service.QdrantNearestPoint(
                    "wikidata:Q9",
                    0.84,
                    {
                        "canonical_text": "Sam Altman",
                        "entity_type": "person",
                        "packs": ["general-en"],
                        "source_name": "wikidata-general-entities",
                    },
                ),
            ]

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert queried_point_ids == []
    assert [entity.link.entity_id for entity in enriched.entities if entity.link is not None] == [
        "wikidata:Q1",
        "wikidata:Q2",
        "wikidata:Q3",
    ]
    assert [item.entity_id for item in enriched.related_entities] == ["wikidata:Q4"]
    assert enriched.graph_support is not None
    assert enriched.graph_support.provider == "sqlite.qid_graph_context"
    assert enriched.graph_support.related_entity_count == 1
    assert enriched.graph_support.suppressed_entity_ids == []


def test_enrich_tag_response_with_related_entities_merges_graph_gated_vector_proposals(
    monkeypatch,
    tmp_path: Path,
) -> None:
    response = _response_with_three_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
        graph_context_enabled=True,
        graph_context_artifact_path=_graph_context_artifact(tmp_path),
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
        graph_context_min_supporting_seeds=2,
        graph_context_vector_proposals_enabled=True,
        graph_context_vector_proposal_limit=4,
    )
    queried_point_ids: list[str] = []

    class _FakeClient:
        def __init__(self, base_url: str, *, api_key: str | None = None) -> None:
            assert base_url == "http://qdrant.local:6333"
            assert api_key is None

        def __enter__(self) -> "_FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def query_similar_by_id(
            self,
            collection_name: str,
            *,
            point_id: str,
            limit: int,
            filter_payload=None,
        ):
            assert collection_name == "ades-qids-current"
            assert filter_payload == {"packs": ["general-en"]}
            queried_point_ids.append(point_id)
            return [
                vector_service.QdrantNearestPoint(point_id, 1.0, {}),
                vector_service.QdrantNearestPoint(
                    "wikidata:Q8",
                    0.83 if point_id == "wikidata:Q1" else 0.81,
                    {
                        "canonical_text": "Enterprise AI",
                        "entity_type": "organization",
                        "packs": ["general-en"],
                        "source_name": "wikidata-general-entities",
                    },
                ),
                vector_service.QdrantNearestPoint(
                    "wikidata:Q99",
                    0.9,
                    {
                        "canonical_text": "Noise Concept",
                        "entity_type": "organization",
                        "packs": ["general-en"],
                        "source_name": "wikidata-general-entities",
                    },
                ),
            ]

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert queried_point_ids == ["wikidata:Q1", "wikidata:Q2"]
    assert {item.entity_id for item in enriched.related_entities} == {
        "wikidata:Q4",
        "wikidata:Q8",
    }
    hybrid_item = next(
        item for item in enriched.related_entities if item.entity_id == "wikidata:Q8"
    )
    assert hybrid_item.provider == "qdrant.qid_graph"
    assert hybrid_item.seed_entity_ids == ["wikidata:Q1", "wikidata:Q2"]
    assert hybrid_item.shared_seed_count == 2
    assert "wikidata:Q99" not in {item.entity_id for item in enriched.related_entities}
    assert enriched.graph_support is not None
    assert enriched.graph_support.provider == "sqlite.qid_graph_context"
    assert enriched.graph_support.related_entity_count == 2


def test_enrich_tag_response_with_related_entities_uses_graph_vector_fallback_for_missing_seed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    response = _response_with_three_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
        graph_context_enabled=True,
        graph_context_artifact_path=_graph_context_artifact(tmp_path),
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
        graph_context_min_supporting_seeds=2,
        graph_context_vector_proposals_enabled=True,
        graph_context_vector_proposal_limit=4,
    )
    queried_by_id: list[str] = []
    queried_by_vector: list[list[float]] = []

    class _FakeClient:
        def __init__(self, base_url: str, *, api_key: str | None = None) -> None:
            assert base_url == "http://qdrant.local:6333"
            assert api_key is None

        def __enter__(self) -> "_FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def query_similar_by_id(
            self,
            collection_name: str,
            *,
            point_id: str,
            limit: int,
            filter_payload=None,
        ):
            assert collection_name == "ades-qids-current"
            assert filter_payload == {"packs": ["general-en"]}
            queried_by_id.append(point_id)
            if point_id == "wikidata:Q2":
                raise QdrantVectorSearchError(
                    "Qdrant request failed (404): {'error': 'Not found: No point with id missing found'}",
                    status_code=404,
                )
            return [
                vector_service.QdrantNearestPoint(point_id, 1.0, {}),
                vector_service.QdrantNearestPoint(
                    "wikidata:Q8",
                    0.84,
                    {
                        "canonical_text": "Enterprise AI",
                        "entity_type": "organization",
                        "packs": ["general-en"],
                        "source_name": "wikidata-general-entities",
                    },
                ),
            ]

        def query_similar_by_vector(
            self,
            collection_name: str,
            *,
            vector: list[float],
            limit: int,
            filter_payload=None,
        ):
            assert collection_name == "ades-qids-current"
            assert filter_payload == {"packs": ["general-en"]}
            queried_by_vector.append(vector)
            assert any(value != 0.0 for value in vector)
            return [
                vector_service.QdrantNearestPoint(
                    "wikidata:Q8",
                    0.82,
                    {
                        "canonical_text": "Enterprise AI",
                        "entity_type": "organization",
                        "packs": ["general-en"],
                        "source_name": "wikidata-general-entities",
                    },
                ),
            ]

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert queried_by_id == ["wikidata:Q1", "wikidata:Q2"]
    assert len(queried_by_vector) == 1
    assert {item.entity_id for item in enriched.related_entities} == {
        "wikidata:Q4",
        "wikidata:Q8",
    }
    assert enriched.graph_support is not None
    assert "vector_search_seed_graph_fallback:wikidata:Q2" not in (
        enriched.graph_support.warnings
    )


def test_enrich_tag_response_with_related_entities_allows_local_hybrid_vector_proposals(
    monkeypatch,
    tmp_path: Path,
) -> None:
    response = _response_with_three_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
        graph_context_enabled=True,
        graph_context_artifact_path=_graph_context_artifact(tmp_path),
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
        graph_context_min_supporting_seeds=2,
        graph_context_vector_proposals_enabled=True,
        graph_context_vector_proposal_limit=4,
    )
    queried_point_ids: list[str] = []

    class _FakeClient:
        def __init__(self, base_url: str, *, api_key: str | None = None) -> None:
            assert base_url == "http://qdrant.local:6333"
            assert api_key is None

        def __enter__(self) -> "_FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def query_similar_by_id(
            self,
            collection_name: str,
            *,
            point_id: str,
            limit: int,
            filter_payload=None,
        ):
            assert collection_name == "ades-qids-current"
            assert filter_payload == {"packs": ["general-en"]}
            queried_point_ids.append(point_id)
            return [
                vector_service.QdrantNearestPoint(point_id, 1.0, {}),
                vector_service.QdrantNearestPoint(
                    "wikidata:Q8",
                    0.84 if point_id == "wikidata:Q1" else 0.82,
                    {
                        "canonical_text": "Enterprise AI",
                        "entity_type": "organization",
                        "packs": ["general-en"],
                        "source_name": "wikidata-general-entities",
                    },
                ),
            ]

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert queried_point_ids == ["wikidata:Q1", "wikidata:Q2"]
    assert {item.entity_id for item in enriched.related_entities} == {
        "wikidata:Q4",
        "wikidata:Q8",
    }
    hybrid_item = next(
        item for item in enriched.related_entities if item.entity_id == "wikidata:Q8"
    )
    assert hybrid_item.seed_entity_ids == ["wikidata:Q1", "wikidata:Q2"]


def test_enrich_tag_response_with_related_entities_warns_when_local_hybrid_vector_transport_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    response = _response_with_three_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        vector_search_enabled=True,
        vector_search_url="http://127.0.0.1:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
        graph_context_enabled=True,
        graph_context_artifact_path=_graph_context_artifact(tmp_path),
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
        graph_context_min_supporting_seeds=2,
        graph_context_vector_proposals_enabled=True,
        graph_context_vector_proposal_limit=4,
    )

    class _FakeClient:
        def __init__(self, base_url: str, *, api_key: str | None = None) -> None:
            assert base_url == "http://127.0.0.1:6333"
            assert api_key is None

        def __enter__(self) -> "_FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def query_similar_by_id(
            self,
            collection_name: str,
            *,
            point_id: str,
            limit: int,
            filter_payload=None,
        ):
            raise QdrantVectorSearchError(
                "Qdrant transport request failed: [Errno 111] Connection refused"
            )

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert {item.entity_id for item in enriched.related_entities} == {"wikidata:Q4"}
    assert enriched.graph_support is not None
    assert (
        "vector_search_failed:Qdrant transport request failed: [Errno 111] Connection refused"
        in enriched.graph_support.warnings
    )
