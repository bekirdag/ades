from ades.config import Settings
from ades.service.models import EntityLink, EntityMatch, TagResponse
from ades.storage.backend import MetadataBackend, RuntimeTarget
from ades.vector import service as vector_service


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
