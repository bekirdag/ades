import json
from pathlib import Path

from ades.config import Settings
from ades.service.models import EntityLink, EntityMatch, GraphSupport, TagResponse
from ades.storage.backend import MetadataBackend, RuntimeTarget
from ades.vector import graph_builder as graph_builder_module
from ades.vector import service as vector_service
from ades.vector.qdrant import QdrantVectorSearchError


def _exact_alias_provenance(match_source: str) -> dict[str, str]:
    return {
        "match_kind": "alias",
        "match_path": "lookup.alias.exact",
        "match_source": match_source,
        "source_pack": "general-en",
        "source_domain": "general",
    }


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


def _response_with_low_fidelity_linked_entities() -> TagResponse:
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
                start=10,
                end=19,
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
                text="Friedrich Merz",
                label="person",
                start=20,
                end=34,
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
        "<http://www.wikidata.org/entity/Q8> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q4> .",
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


def _graph_context_artifact_with_party_affiliation(tmp_path: Path) -> Path:
    bundle_dir = _graph_context_bundle_dir(tmp_path)
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    with entities_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "entity_id": "wikidata:Q12",
                    "canonical_text": "Labour Party",
                    "entity_type": "organization",
                    "source_name": "wikidata-general-entities",
                }
            )
            + "\n"
        )
    truthy_path = tmp_path / "truthy-party-affiliation.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P102> <http://www.wikidata.org/entity/Q12> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    response = graph_builder_module.build_qid_graph_store(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact-party-affiliation",
        allowed_predicates=["P102"],
    )
    return Path(response.artifact_path)


def _graph_context_artifact_with_person_cluster(tmp_path: Path) -> Path:
    bundle_dir = _graph_context_bundle_dir(tmp_path)
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    with entities_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "entity_id": "wikidata:Q10",
                    "canonical_text": "Person Cluster Candidate",
                    "entity_type": "person",
                    "source_name": "wikidata-general-entities",
                }
            )
            + "\n"
        )
        handle.write(
            json.dumps(
                {
                    "entity_id": "wikidata:Q11",
                    "canonical_text": "Direct Person Candidate",
                    "entity_type": "person",
                    "source_name": "wikidata-general-entities",
                }
            )
            + "\n"
        )
    truthy_path = tmp_path / "truthy-person-cluster.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q3> <http://www.wikidata.org/prop/direct/P17> <http://www.wikidata.org/entity/Q901> .",
                "<http://www.wikidata.org/entity/Q3> <http://www.wikidata.org/prop/direct/P17> <http://www.wikidata.org/entity/Q902> .",
                "<http://www.wikidata.org/entity/Q10> <http://www.wikidata.org/prop/direct/P17> <http://www.wikidata.org/entity/Q901> .",
                "<http://www.wikidata.org/entity/Q10> <http://www.wikidata.org/prop/direct/P17> <http://www.wikidata.org/entity/Q902> .",
                "<http://www.wikidata.org/entity/Q3> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q11> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    response = graph_builder_module.build_qid_graph_store(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact-person-cluster",
        allowed_predicates=["P17", "P361"],
    )
    return Path(response.artifact_path)


def _graph_context_artifact_with_org_only_shared_context_cluster(tmp_path: Path) -> Path:
    bundle_dir = _graph_context_bundle_dir(tmp_path)
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    with entities_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "entity_id": "wikidata:Q10",
                    "canonical_text": "Historic Brewery",
                    "entity_type": "location",
                    "source_name": "wikidata-general-entities",
                }
            )
            + "\n"
        )
        handle.write(
            json.dumps(
                {
                    "entity_id": "wikidata:Q11",
                    "canonical_text": "Overshared Agency",
                    "entity_type": "organization",
                    "source_name": "wikidata-general-entities",
                }
            )
            + "\n"
        )
        handle.write(
            json.dumps(
                {
                    "entity_id": "wikidata:Q12",
                    "canonical_text": "Civic Research Institute",
                    "entity_type": "organization",
                    "source_name": "wikidata-general-entities",
                }
            )
            + "\n"
        )
    truthy_path = tmp_path / "truthy-org-only-shared-context.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q500> .",
                "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q500> .",
                "<http://www.wikidata.org/entity/Q12> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q500> .",
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q600> .",
                "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q600> .",
                "<http://www.wikidata.org/entity/Q12> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q600> .",
                "<http://www.wikidata.org/entity/Q10> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q500> .",
                "<http://www.wikidata.org/entity/Q11> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q500> .",
                "<http://www.wikidata.org/entity/Q11> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q600> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    response = graph_builder_module.build_qid_graph_store(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact-org-only-shared-context",
        allowed_predicates=["P361", "P463"],
    )
    return Path(response.artifact_path)


def _recent_news_support_artifact(tmp_path: Path) -> Path:
    path = tmp_path / "recent-news-support.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 18,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Enterprise Platform",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 4,
                    },
                    "wikidata:Q13": {
                        "canonical_text": "Political Insider",
                        "entity_type": "person",
                        "source_name": "saved-unseen-news",
                        "document_count": 3,
                    },
                },
                "cooccurrence": {
                    "wikidata:Q1": {
                        "wikidata:Q12": 3,
                        "wikidata:Q13": 1,
                    },
                    "wikidata:Q2": {
                        "wikidata:Q12": 2,
                        "wikidata:Q13": 1,
                    },
                    "wikidata:Q3": {
                        "wikidata:Q13": 2,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    return path


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


def _response_with_three_person_entities() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Alice",
                label="person",
                start=0,
                end=5,
                confidence=0.71,
                relevance=0.71,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Alice",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Bob",
                label="person",
                start=10,
                end=13,
                confidence=0.68,
                relevance=0.68,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Bob",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Carol",
                label="person",
                start=20,
                end=25,
                confidence=0.66,
                relevance=0.66,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Carol",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _response_with_person_and_location_entities() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Chancellor Reeves",
                label="person",
                start=0,
                end=18,
                confidence=0.66,
                relevance=0.66,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Rachel Reeves",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Macron",
                label="person",
                start=25,
                end=31,
                confidence=0.64,
                relevance=0.64,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Emmanuel Macron",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Iran",
                label="location",
                start=40,
                end=44,
                confidence=0.75,
                relevance=0.75,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Iran",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _response_with_org_and_location_entities() -> TagResponse:
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
                text="London",
                label="location",
                start=15,
                end=21,
                confidence=0.51,
                relevance=0.51,
                mention_count=1,
                link=EntityLink(
                    entity_id="wikidata:Q5",
                    canonical_text="London",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _response_with_location_only_seed() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="London",
                label="location",
                start=0,
                end=6,
                confidence=0.51,
                relevance=0.51,
                mention_count=1,
                link=EntityLink(
                    entity_id="wikidata:Q5",
                    canonical_text="London",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _response_with_multiple_location_only_seeds() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="United Kingdom",
                label="location",
                start=0,
                end=14,
                confidence=0.62,
                relevance=0.62,
                mention_count=3,
                link=EntityLink(
                    entity_id="wikidata:Q145",
                    canonical_text="United Kingdom",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Westminster",
                label="location",
                start=20,
                end=31,
                confidence=0.57,
                relevance=0.57,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q23436",
                    canonical_text="Westminster",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="The Middle East",
                label="location",
                start=40,
                end=55,
                confidence=0.71,
                relevance=0.71,
                mention_count=1,
                link=EntityLink(
                    entity_id="wikidata:Q7204",
                    canonical_text="The Middle East",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _response_with_fallback_and_location_seeds() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="UK government",
                label="government",
                start=0,
                end=13,
                confidence=0.66,
                relevance=0.66,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q31747",
                    canonical_text="Government of the United Kingdom",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="United Kingdom",
                label="location",
                start=20,
                end=34,
                confidence=0.62,
                relevance=0.62,
                mention_count=3,
                link=EntityLink(
                    entity_id="wikidata:Q145",
                    canonical_text="United Kingdom",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Westminster",
                label="location",
                start=40,
                end=51,
                confidence=0.57,
                relevance=0.57,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q23436",
                    canonical_text="Westminster",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def test_query_filter_payload_routes_domain_and_country_hints() -> None:
    response = _response_with_linked_entities()
    settings = Settings()

    assert vector_service._query_filter_payload(response, settings=settings) == {
        "packs": ["general-en"]
    }
    assert vector_service._query_filter_payload(
        response,
        settings=settings,
        domain_hint="business",
        country_hint="United Kingdom",
    ) == {"packs": ["general-en", "business-vector-en", "finance-uk-en"]}


def test_query_filter_payload_uses_configured_domain_and_country_routes() -> None:
    response = _response_with_linked_entities()
    settings = Settings(
        vector_search_domain_pack_routes={"business": "business-custom-en"},
        vector_search_country_aliases={"great-britain": "uk"},
    )

    assert vector_service._query_filter_payload(
        response,
        settings=settings,
        domain_hint="business",
        country_hint="Great Britain",
    ) == {"packs": ["general-en", "business-custom-en", "finance-uk-en"]}


def test_collection_alias_candidates_prefers_routed_alias_before_shared_alias_with_hints() -> None:
    response = _response_with_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_collection_alias="ades-qids-current",
    )

    assert vector_service._collection_alias_candidates(response, settings=settings) == [
        "ades-qids-current"
    ]
    assert vector_service._collection_alias_candidates(
        response,
        settings=settings,
        domain_hint="business",
        country_hint="uk",
    ) == [
        "ades-qids-business-current",
        "ades-qids-current",
    ]


def test_enrich_tag_response_with_related_entities_aggregates_routed_neighbors(monkeypatch) -> None:
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


def test_enrich_tag_response_with_related_entities_routes_hint_packs_to_qdrant_filters(
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
    observed_filters: list[dict[str, object] | None] = []
    observed_collections: list[str] = []

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
            observed_collections.append(collection_name)
            observed_filters.append(filter_payload)
            return [
                vector_service.QdrantNearestPoint(point_id, 1.0, {}),
                vector_service.QdrantNearestPoint(
                    "wikidata:Q8",
                    0.84 if point_id == "wikidata:Q1" else 0.82,
                    {
                        "canonical_text": "Enterprise AI",
                        "entity_type": "organization",
                        "packs": ["business-vector-en", "finance-uk-en"],
                        "source_name": "wikidata-general-entities",
                    },
                ),
            ]

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
        domain_hint="business",
        country_hint="uk",
    )

    assert observed_filters == [
        {"packs": ["general-en", "business-vector-en", "finance-uk-en"]},
        {"packs": ["general-en", "business-vector-en", "finance-uk-en"]},
        {"packs": ["general-en", "business-vector-en", "finance-uk-en"]},
        {"packs": ["general-en", "business-vector-en", "finance-uk-en"]},
    ]
    assert observed_collections == [
        "ades-qids-business-current",
        "ades-qids-current",
        "ades-qids-business-current",
        "ades-qids-current",
    ]
    assert {item.entity_id for item in enriched.related_entities} == {
        "wikidata:Q4",
        "wikidata:Q8",
    }


def test_enrich_tag_response_with_related_entities_falls_back_to_shared_alias_when_routed_alias_is_empty(
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
    observed_calls: list[tuple[str, str, dict[str, object] | None]] = []

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
            observed_calls.append((collection_name, point_id, filter_payload))
            assert filter_payload == {"packs": ["general-en", "business-vector-en"]}
            if collection_name == "ades-qids-business-current":
                return []
            assert collection_name == "ades-qids-current"
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
        domain_hint="business",
    )

    assert observed_calls == [
        (
            "ades-qids-business-current",
            "wikidata:Q1",
            {"packs": ["general-en", "business-vector-en"]},
        ),
        ("ades-qids-current", "wikidata:Q1", {"packs": ["general-en", "business-vector-en"]}),
        (
            "ades-qids-business-current",
            "wikidata:Q2",
            {"packs": ["general-en", "business-vector-en"]},
        ),
        ("ades-qids-current", "wikidata:Q2", {"packs": ["general-en", "business-vector-en"]}),
    ]
    assert [item.entity_id for item in enriched.related_entities] == ["wikidata:Q9"]


def test_enrich_tag_response_with_related_entities_merges_shared_alias_results_when_routed_alias_is_non_empty(
    monkeypatch,
) -> None:
    response = _response_with_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=4,
        vector_search_collection_alias="ades-qids-current",
    )
    observed_calls: list[tuple[str, str, dict[str, object] | None]] = []

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
            observed_calls.append((collection_name, point_id, filter_payload))
            assert filter_payload == {"packs": ["general-en", "business-vector-en"]}
            if collection_name == "ades-qids-business-current":
                return [
                    vector_service.QdrantNearestPoint(point_id, 1.0, {}),
                    vector_service.QdrantNearestPoint(
                        "wikidata:Q8",
                        0.78 if point_id == "wikidata:Q1" else 0.8,
                        {
                            "canonical_text": "Enterprise AI",
                            "entity_type": "organization",
                            "packs": ["business-vector-en"],
                        },
                    ),
                ]
            assert collection_name == "ades-qids-current"
            return [
                vector_service.QdrantNearestPoint(point_id, 1.0, {}),
                vector_service.QdrantNearestPoint(
                    "wikidata:Q9",
                    0.82 if point_id == "wikidata:Q1" else 0.84,
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
        domain_hint="business",
    )

    assert observed_calls == [
        (
            "ades-qids-business-current",
            "wikidata:Q1",
            {"packs": ["general-en", "business-vector-en"]},
        ),
        ("ades-qids-current", "wikidata:Q1", {"packs": ["general-en", "business-vector-en"]}),
        (
            "ades-qids-business-current",
            "wikidata:Q2",
            {"packs": ["general-en", "business-vector-en"]},
        ),
        ("ades-qids-current", "wikidata:Q2", {"packs": ["general-en", "business-vector-en"]}),
    ]
    assert [item.entity_id for item in enriched.related_entities] == [
        "wikidata:Q9",
        "wikidata:Q8",
    ]


def test_enrich_tag_response_with_related_entities_prefers_primary_seed_for_routed_alias_when_location_exists(
    monkeypatch,
) -> None:
    response = _response_with_org_and_location_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
    )
    observed_calls: list[tuple[str, str, dict[str, object] | None]] = []

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
            observed_calls.append((collection_name, point_id, filter_payload))
            return []

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        domain_hint="business",
    )

    assert observed_calls == [
        (
            "ades-qids-business-current",
            "wikidata:Q1",
            {"packs": ["general-en", "business-vector-en"]},
        ),
        (
            "ades-qids-current",
            "wikidata:Q1",
            {"packs": ["general-en", "business-vector-en"]},
        ),
    ]
    assert enriched.graph_support is not None
    assert enriched.graph_support.warnings == []


def test_enrich_tag_response_with_related_entities_prefers_primary_seed_for_shared_alias_without_domain_hint(
    monkeypatch,
) -> None:
    response = _response_with_org_and_location_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
    )
    observed_calls: list[tuple[str, str, dict[str, object] | None]] = []

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
            observed_calls.append((collection_name, point_id, filter_payload))
            return []

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
    )

    assert observed_calls == [
        (
            "ades-qids-current",
            "wikidata:Q1",
            {"packs": ["general-en"]},
        ),
    ]
    assert enriched.graph_support is not None
    assert enriched.graph_support.warnings == []


def test_enrich_tag_response_with_related_entities_keeps_routed_alias_for_location_only_seed(
    monkeypatch,
) -> None:
    response = _response_with_location_only_seed()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
    )
    observed_calls: list[tuple[str, str, dict[str, object] | None]] = []

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
            observed_calls.append((collection_name, point_id, filter_payload))
            return []

    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        domain_hint="business",
    )

    assert observed_calls == [
        (
            "ades-qids-business-current",
            "wikidata:Q5",
            {"packs": ["general-en", "business-vector-en"]},
        ),
        (
            "ades-qids-current",
            "wikidata:Q5",
            {"packs": ["general-en", "business-vector-en"]},
        ),
    ]
    assert enriched.graph_support is not None
    assert enriched.graph_support.warnings == []


def test_seed_entity_ids_for_collection_alias_caps_shared_location_only_fallback() -> None:
    response = _response_with_multiple_location_only_seeds()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
    )

    seed_ids = vector_service._seed_entity_ids_for_collection_alias(
        response,
        collection_alias="ades-qids-current",
        settings=settings,
    )

    assert seed_ids == ["wikidata:Q145", "wikidata:Q23436"]


def test_seed_entity_ids_for_collection_alias_keeps_routed_location_only_fallback_unbounded() -> None:
    response = _response_with_multiple_location_only_seeds()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
    )

    seed_ids = vector_service._seed_entity_ids_for_collection_alias(
        response,
        collection_alias="ades-qids-business-current",
        settings=settings,
    )

    assert seed_ids == ["wikidata:Q145", "wikidata:Q23436", "wikidata:Q7204"]


def test_seed_entity_ids_for_collection_alias_prefers_shared_non_location_fallback_seed() -> None:
    response = _response_with_fallback_and_location_seeds()
    settings = Settings(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_related_limit=3,
        vector_search_collection_alias="ades-qids-current",
    )

    seed_ids = vector_service._seed_entity_ids_for_collection_alias(
        response,
        collection_alias="ades-qids-current",
        settings=settings,
    )

    assert seed_ids == ["wikidata:Q31747"]


def test_linked_seed_entity_ids_skip_low_fidelity_exact_aliases() -> None:
    response = _response_with_low_fidelity_linked_entities()

    assert vector_service._linked_seed_entity_ids(response) == ["wikidata:Q1692388"]


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


def test_graph_gate_vector_related_entities_rejects_person_only_cluster_support(
    tmp_path: Path,
) -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Seed Person",
                label="person",
                start=0,
                end=11,
                confidence=0.41,
                relevance=0.41,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="the Second",
                    provider="lookup.alias.exact",
                ),
            )
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(applied=True, coherence_score=0.7),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        graph_context_artifact_path=_graph_context_artifact_with_person_cluster(tmp_path),
        graph_context_min_supporting_seeds=1,
    )

    admitted = vector_service._graph_gate_vector_related_entities(
        [
            vector_service.RelatedEntityMatch(
                entity_id="wikidata:Q10",
                canonical_text="Person Cluster Candidate",
                score=0.84,
                provider="qdrant.qid_graph",
                entity_type="person",
                packs=["general-en"],
                seed_entity_ids=["wikidata:Q3"],
                shared_seed_count=1,
            )
        ],
        response=response,
        strong_seed_entity_ids=["wikidata:Q3"],
        settings=settings,
    )

    assert admitted == []


def test_graph_gate_vector_related_entities_keeps_person_with_direct_support(
    tmp_path: Path,
) -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Seed Person",
                label="person",
                start=0,
                end=11,
                confidence=0.41,
                relevance=0.41,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="the Second",
                    provider="lookup.alias.exact",
                ),
            )
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(applied=True, coherence_score=0.7),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        graph_context_artifact_path=_graph_context_artifact_with_person_cluster(tmp_path),
        graph_context_min_supporting_seeds=1,
    )

    admitted = vector_service._graph_gate_vector_related_entities(
        [
            vector_service.RelatedEntityMatch(
                entity_id="wikidata:Q11",
                canonical_text="Direct Person Candidate",
                score=0.85,
                provider="qdrant.qid_graph",
                entity_type="person",
                packs=["general-en"],
                seed_entity_ids=["wikidata:Q3"],
                shared_seed_count=1,
            )
        ],
        response=response,
        strong_seed_entity_ids=["wikidata:Q3"],
        settings=settings,
    )

    assert [item.entity_id for item in admitted] == ["wikidata:Q11"]
    assert admitted[0].entity_type == "person"
    assert admitted[0].provider == "qdrant.qid_graph"


def test_graph_gate_vector_related_entities_rejects_org_only_no_direct_candidates_without_graph_related_overlap(
    tmp_path: Path,
) -> None:
    response = TagResponse(
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
                confidence=0.6,
                relevance=0.6,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="OpenAI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Microsoft",
                label="organization",
                start=8,
                end=17,
                confidence=0.6,
                relevance=0.6,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Microsoft",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Civic Research Institute",
                label="organization",
                start=19,
                end=43,
                confidence=0.6,
                relevance=0.6,
                link=EntityLink(
                    entity_id="wikidata:Q12",
                    canonical_text="Civic Research Institute",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(applied=True, coherence_score=0.7),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        graph_context_artifact_path=_graph_context_artifact_with_org_only_shared_context_cluster(
            tmp_path
        ),
        graph_context_min_supporting_seeds=2,
    )

    admitted = vector_service._graph_gate_vector_related_entities(
        [
            vector_service.RelatedEntityMatch(
                entity_id="wikidata:Q10",
                canonical_text="Historic Brewery",
                score=0.81,
                provider="qdrant.qid_graph",
                entity_type="location",
                packs=["general-en"],
                seed_entity_ids=["wikidata:Q1"],
                shared_seed_count=1,
            ),
            vector_service.RelatedEntityMatch(
                entity_id="wikidata:Q11",
                canonical_text="Overshared Agency",
                score=0.8,
                provider="qdrant.qid_graph",
                entity_type="organization",
                packs=["general-en"],
                seed_entity_ids=["wikidata:Q1"],
                shared_seed_count=1,
            ),
        ],
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q12"],
        settings=settings,
    )

    assert admitted == []


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
    assert "vector_search_seed_graph_fallback:wikidata:Q2" in enriched.graph_support.warnings


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


def test_enrich_tag_response_with_related_entities_allows_shared_fallback_for_hinted_local_hybrid_queries(
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
    observed_calls: list[tuple[str, str, dict[str, object] | None]] = []

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
            observed_calls.append((collection_name, point_id, filter_payload))
            assert filter_payload == {"packs": ["general-en", "business-vector-en", "finance-uk-en"]}
            if collection_name == "ades-qids-business-current":
                return []
            assert collection_name == "ades-qids-current"
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
        domain_hint="business",
        country_hint="uk",
    )

    assert observed_calls == [
        (
            "ades-qids-business-current",
            "wikidata:Q1",
            {"packs": ["general-en", "business-vector-en", "finance-uk-en"]},
        ),
        (
            "ades-qids-current",
            "wikidata:Q1",
            {"packs": ["general-en", "business-vector-en", "finance-uk-en"]},
        ),
        (
            "ades-qids-business-current",
            "wikidata:Q2",
            {"packs": ["general-en", "business-vector-en", "finance-uk-en"]},
        ),
        (
            "ades-qids-current",
            "wikidata:Q2",
            {"packs": ["general-en", "business-vector-en", "finance-uk-en"]},
        ),
    ]
    assert {item.entity_id for item in enriched.related_entities} == {
        "wikidata:Q4",
        "wikidata:Q8",
    }


def test_enrich_tag_response_with_related_entities_skips_vector_lane_when_graph_coherence_is_too_low(
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

    def _fake_apply_graph_context(*args, **kwargs):
        return response.model_copy(
            update={
                "graph_support": GraphSupport(
                    requested=True,
                    applied=True,
                    provider="sqlite.qid_graph_context",
                    requested_related_entities=True,
                    requested_refinement=False,
                    seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
                    coherence_score=0.02,
                )
            }
        )

    class _FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            raise AssertionError("vector client should not be constructed for low coherence")

    monkeypatch.setattr(vector_service, "apply_graph_context", _fake_apply_graph_context)
    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert enriched.related_entities == []
    assert enriched.graph_support is not None
    assert enriched.graph_support.coherence_score == 0.02


def test_enrich_tag_response_with_related_entities_uses_recent_news_probe_when_graph_coherence_is_too_low(
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
        news_context_artifact_path=_recent_news_support_artifact(tmp_path),
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
    )
    queried_point_ids: list[str] = []

    def _fake_apply_graph_context(*args, **kwargs):
        return response.model_copy(
            update={
                "graph_support": GraphSupport(
                    requested=True,
                    applied=True,
                    provider="sqlite.qid_graph_context",
                    requested_related_entities=True,
                    requested_refinement=False,
                    seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                    coherence_score=0.02,
                )
            }
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
            queried_point_ids.append(point_id)
            points = [vector_service.QdrantNearestPoint(point_id, 1.0, {})]
            if point_id in {"wikidata:Q1", "wikidata:Q2"}:
                points.append(
                    vector_service.QdrantNearestPoint(
                        "wikidata:Q12",
                        0.79 if point_id == "wikidata:Q1" else 0.77,
                        {
                            "canonical_text": "Enterprise Platform",
                            "entity_type": "organization",
                            "packs": ["general-en"],
                            "source_name": "wikidata-general-entities",
                        },
                    )
                )
            return points

    monkeypatch.setattr(vector_service, "apply_graph_context", _fake_apply_graph_context)
    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert queried_point_ids == ["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"]
    assert [item.entity_id for item in enriched.related_entities] == ["wikidata:Q12"]
    assert enriched.graph_support is not None
    assert enriched.graph_support.coherence_score == 0.02
    assert "vector_search_low_coherence_recent_news_probe" in enriched.graph_support.warnings


def test_graph_context_strong_seed_entity_ids_prefers_anchor_subset() -> None:
    response = _response_with_three_linked_entities().model_copy(
        update={
            "graph_support": GraphSupport(
                requested=True,
                applied=True,
                provider="sqlite.qid_graph_context",
                requested_related_entities=True,
                requested_refinement=False,
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                coherence_score=0.41,
            ),
            "refinement_debug": {
                "graph_anchor_seed_ids": [
                    "wikidata:Q2",
                    "wikidata:Q3",
                    "wikidata:Q1",
                ],
                "seed_decisions": [
                    {"entity_id": "wikidata:Q1", "action": "keep"},
                    {"entity_id": "wikidata:Q2", "action": "keep"},
                    {"entity_id": "wikidata:Q3", "action": "suppress"},
                ],
            },
        }
    )

    assert vector_service._graph_context_strong_seed_entity_ids(response) == [
        "wikidata:Q2",
        "wikidata:Q1",
    ]


def test_graph_context_strong_seed_entity_ids_skips_probable_source_outlets() -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="The Guardian",
                label="organization",
                start=0,
                end=12,
                confidence=0.74,
                relevance=0.74,
                provenance=_exact_alias_provenance("The Guardian"),
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="The Guardian",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Foreign Office",
                label="organization",
                start=13,
                end=27,
                confidence=0.78,
                relevance=0.78,
                provenance=_exact_alias_provenance("Foreign Office"),
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Foreign, Commonwealth and Development Office",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Donald Trump",
                label="person",
                start=28,
                end=40,
                confidence=0.81,
                relevance=0.81,
                provenance=_exact_alias_provenance("Donald Trump"),
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Donald Trump",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
            coherence_score=0.41,
        ),
        refinement_debug={
            "graph_anchor_seed_ids": [
                "wikidata:Q1",
                "wikidata:Q2",
                "wikidata:Q3",
            ],
            "seed_decisions": [
                {"entity_id": "wikidata:Q1", "action": "keep"},
                {"entity_id": "wikidata:Q2", "action": "keep"},
                {"entity_id": "wikidata:Q3", "action": "keep"},
            ],
        },
    )

    assert vector_service._graph_context_strong_seed_entity_ids(response) == [
        "wikidata:Q2",
        "wikidata:Q3",
    ]


def test_graph_context_strong_seed_entity_ids_supplements_filtered_anchor_subset() -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="ABC News",
                label="organization",
                start=0,
                end=8,
                confidence=0.74,
                relevance=0.74,
                provenance=_exact_alias_provenance("ABC News"),
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="ABC News",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Secret Service",
                label="organization",
                start=9,
                end=23,
                confidence=0.8,
                relevance=0.8,
                provenance=_exact_alias_provenance("Secret Service"),
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="United States Secret Service",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="law enforcement",
                label="organization",
                start=24,
                end=39,
                confidence=0.79,
                relevance=0.79,
                provenance=_exact_alias_provenance("law enforcement"),
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="law enforcement",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Donald Trump",
                label="person",
                start=40,
                end=52,
                confidence=0.83,
                relevance=0.83,
                provenance=_exact_alias_provenance("Donald Trump"),
                link=EntityLink(
                    entity_id="wikidata:Q4",
                    canonical_text="Donald Trump",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Washington",
                label="location",
                start=53,
                end=63,
                confidence=0.7,
                relevance=0.7,
                provenance=_exact_alias_provenance("Washington"),
                link=EntityLink(
                    entity_id="wikidata:Q5",
                    canonical_text="Washington, D.C.",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=[
                "wikidata:Q1",
                "wikidata:Q2",
                "wikidata:Q3",
                "wikidata:Q4",
                "wikidata:Q5",
            ],
            coherence_score=0.08,
        ),
        refinement_debug={
            "graph_anchor_seed_ids": [
                "wikidata:Q1",
                "wikidata:Q2",
                "wikidata:Q3",
            ],
            "seed_decisions": [
                {"entity_id": "wikidata:Q1", "action": "keep"},
                {"entity_id": "wikidata:Q2", "action": "keep"},
                {"entity_id": "wikidata:Q3", "action": "keep"},
                {"entity_id": "wikidata:Q4", "action": "keep"},
                {"entity_id": "wikidata:Q5", "action": "keep"},
            ],
        },
    )

    assert vector_service._graph_context_strong_seed_entity_ids(response) == [
        "wikidata:Q2",
        "wikidata:Q3",
        "wikidata:Q4",
    ]


def test_graph_context_strong_seed_entity_ids_supplements_low_coherence_anchor_subset_with_person_seed() -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Keir Starmer",
                label="person",
                start=0,
                end=12,
                confidence=0.82,
                relevance=0.82,
                provenance=_exact_alias_provenance("Keir Starmer"),
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Keir Starmer",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Invest NI",
                label="organization",
                start=13,
                end=22,
                confidence=0.76,
                relevance=0.76,
                provenance=_exact_alias_provenance("Invest NI"),
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Invest NI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Belfast",
                label="location",
                start=23,
                end=30,
                confidence=0.74,
                relevance=0.74,
                provenance=_exact_alias_provenance("Belfast"),
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Belfast",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
            coherence_score=0.06,
        ),
        refinement_debug={
            "graph_anchor_seed_ids": [
                "wikidata:Q2",
                "wikidata:Q3",
            ],
            "seed_decisions": [
                {"entity_id": "wikidata:Q1", "action": "keep"},
                {"entity_id": "wikidata:Q2", "action": "keep"},
                {"entity_id": "wikidata:Q3", "action": "keep"},
            ],
        },
    )

    assert vector_service._graph_context_strong_seed_entity_ids(response) == [
        "wikidata:Q2",
        "wikidata:Q3",
        "wikidata:Q1",
    ]


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


def test_recent_news_gate_vector_related_entities_admits_multi_seed_supported_candidate(
    tmp_path: Path,
) -> None:
    response = _response_with_three_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=_recent_news_support_artifact(tmp_path),
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
    )

    admitted = vector_service._recent_news_gate_vector_related_entities(
        [
            vector_service.RelatedEntityMatch(
                entity_id="wikidata:Q12",
                canonical_text="Enterprise Platform",
                score=0.79,
                provider="qdrant.qid_graph",
                entity_type="organization",
                packs=["general-en"],
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
                shared_seed_count=2,
            )
        ],
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
        settings=settings,
    )

    assert [item.entity_id for item in admitted] == ["wikidata:Q12"]
    assert admitted[0].canonical_text == "Enterprise Platform"
    assert admitted[0].seed_entity_ids == ["wikidata:Q1", "wikidata:Q2"]
    assert admitted[0].shared_seed_count == 2
    assert admitted[0].score > 0.79


def test_recent_news_gate_vector_related_entities_rejects_person_only_weak_support(
    tmp_path: Path,
) -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Seed Person One",
                label="person",
                start=0,
                end=15,
                confidence=0.41,
                relevance=0.41,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Seed Person One",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Seed Person Two",
                label="person",
                start=20,
                end=35,
                confidence=0.39,
                relevance=0.39,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Seed Person Two",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=_recent_news_support_artifact(tmp_path),
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=1,
    )

    admitted = vector_service._recent_news_gate_vector_related_entities(
        [
            vector_service.RelatedEntityMatch(
                entity_id="wikidata:Q13",
                canonical_text="Political Insider",
                score=0.74,
                provider="qdrant.qid_graph",
                entity_type="person",
                packs=["general-en"],
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
                shared_seed_count=2,
            )
        ],
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_emits_supported_candidates(
    tmp_path: Path,
) -> None:
    response = _response_with_three_linked_entities()
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=_recent_news_support_artifact(tmp_path),
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
        settings=settings,
    )

    assert [item.entity_id for item in admitted] == ["wikidata:Q12"]
    assert admitted[0].provider == "recent_news.support"
    assert admitted[0].canonical_text == "Enterprise Platform"
    assert admitted[0].seed_entity_ids == ["wikidata:Q1", "wikidata:Q2"]
    assert admitted[0].score >= 0.36


def test_recent_news_propose_related_entities_requires_three_shared_seeds_when_low_coherence(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-low-coherence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "High Support Candidate",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 6,
                    },
                    "wikidata:Q14": {
                        "canonical_text": "Low Support Candidate",
                        "entity_type": "person",
                        "source_name": "saved-unseen-news",
                        "document_count": 5,
                    },
                },
                "cooccurrence": {
                    "wikidata:Q1": {
                        "wikidata:Q12": 3,
                        "wikidata:Q14": 3,
                    },
                    "wikidata:Q2": {
                        "wikidata:Q12": 3,
                        "wikidata:Q14": 3,
                    },
                    "wikidata:Q3": {
                        "wikidata:Q12": 3,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    response = _response_with_three_linked_entities().model_copy(
        update={
            "graph_support": GraphSupport(
                requested=True,
                applied=True,
                provider="sqlite.qid_graph_context",
                requested_related_entities=True,
                requested_refinement=False,
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                coherence_score=0.02,
            )
        }
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        graph_context_artifact_path=_graph_context_artifact_with_party_affiliation(
            tmp_path
        ),
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
        settings=settings,
    )

    assert [item.entity_id for item in admitted] == ["wikidata:Q12"]


def test_recent_news_propose_related_entities_allows_person_backed_non_anchor_candidate_when_low_coherence(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-anchor-low-coherence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Weak Anchor Candidate",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 6,
                    },
                    "wikidata:Q13": {
                        "canonical_text": "Strong Anchor Candidate",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 6,
                    },
                },
                "cooccurrence": {
                    "wikidata:Q1": {
                        "wikidata:Q12": 5,
                        "wikidata:Q13": 5,
                    },
                    "wikidata:Q2": {
                        "wikidata:Q12": 1,
                        "wikidata:Q13": 3,
                    },
                    "wikidata:Q3": {
                        "wikidata:Q12": 3,
                        "wikidata:Q13": 3,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Donald Trump",
                label="person",
                start=0,
                end=12,
                confidence=0.7,
                relevance=0.7,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Donald Trump",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Secret Service",
                label="organization",
                start=13,
                end=27,
                confidence=0.72,
                relevance=0.72,
                mention_count=3,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="United States Secret Service",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="The White House",
                label="location",
                start=28,
                end=43,
                confidence=0.76,
                relevance=0.76,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="The White House",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
            coherence_score=0.02,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=1,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
        settings=settings,
    )

    assert [item.entity_id for item in admitted] == ["wikidata:Q13", "wikidata:Q12"]


def test_recent_news_propose_related_entities_rejects_person_location_only_non_anchor_candidate_when_low_coherence(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-person-location-only-low-coherence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Islamic Republican",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 13,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {
                        "wikidata:Q12": 10,
                    },
                    "wikidata:Q2": {
                        "wikidata:Q12": 2,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Donald Trump",
                label="person",
                start=0,
                end=12,
                confidence=0.7,
                relevance=0.7,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Donald Trump",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Oman",
                label="location",
                start=13,
                end=17,
                confidence=0.72,
                relevance=0.72,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Oman",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
            coherence_score=0.034,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        graph_context_artifact_path=_graph_context_artifact(tmp_path),
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_allows_person_only_org_fallback_when_low_coherence(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-person-only-org-low-coherence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Labour Party",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 10,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {
                        "wikidata:Q12": 6,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Keir Starmer",
                label="person",
                start=0,
                end=12,
                confidence=0.82,
                relevance=0.82,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Keir Starmer",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Invest NI",
                label="organization",
                start=13,
                end=22,
                confidence=0.76,
                relevance=0.76,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Invest NI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Belfast",
                label="location",
                start=23,
                end=30,
                confidence=0.74,
                relevance=0.74,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Belfast",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
            coherence_score=0.06,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q2", "wikidata:Q3", "wikidata:Q1"],
        settings=settings,
    )

    assert [item.entity_id for item in admitted] == ["wikidata:Q12"]
    assert admitted[0].seed_entity_ids == ["wikidata:Q1"]
    assert admitted[0].provider == "recent_news.support"


def test_recent_news_propose_related_entities_rejects_person_only_org_without_graph_affiliation(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-person-only-org-no-graph.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Foreign Commonwealth Office",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 10,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {
                        "wikidata:Q12": 6,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Keir Starmer",
                label="person",
                start=0,
                end=12,
                confidence=0.82,
                relevance=0.82,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Keir Starmer",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Invest NI",
                label="organization",
                start=13,
                end=22,
                confidence=0.76,
                relevance=0.76,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Invest NI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Belfast",
                label="location",
                start=23,
                end=30,
                confidence=0.74,
                relevance=0.74,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Belfast",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
            coherence_score=0.06,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        graph_context_artifact_path=_graph_context_artifact(tmp_path),
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q2", "wikidata:Q3", "wikidata:Q1"],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_prefers_graph_supported_direct_candidate_when_low_coherence(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-mixed-low-coherence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Labour Party",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 10,
                    },
                    "wikidata:Q13": {
                        "canonical_text": "Counter Terrorism Policing",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 10,
                    },
                },
                "cooccurrence": {
                    "wikidata:Q1": {
                        "wikidata:Q12": 6,
                        "wikidata:Q13": 6,
                    },
                    "wikidata:Q2": {
                        "wikidata:Q12": 3,
                        "wikidata:Q13": 3,
                    },
                    "wikidata:Q3": {
                        "wikidata:Q12": 3,
                        "wikidata:Q13": 3,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Keir Starmer",
                label="person",
                start=0,
                end=12,
                confidence=0.82,
                relevance=0.82,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Keir Starmer",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Metropolitan Police",
                label="organization",
                start=13,
                end=32,
                confidence=0.76,
                relevance=0.76,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Metropolitan Police",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Golders Green",
                label="location",
                start=33,
                end=46,
                confidence=0.74,
                relevance=0.74,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Golders Green",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
            coherence_score=0.06,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        graph_context_artifact_path=_graph_context_artifact_with_party_affiliation(
            tmp_path
        ),
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q2", "wikidata:Q3", "wikidata:Q1"],
        settings=settings,
    )

    assert [item.entity_id for item in admitted] == ["wikidata:Q12"]


def test_recent_news_propose_related_entities_aligns_support_candidate_to_graph_affiliation_qid(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-misaligned-party-qid.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q1142007": {
                        "canonical_text": "Labour Party",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 13,
                    },
                    "wikidata:Q13": {
                        "canonical_text": "Counter Terrorism Policing",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 10,
                    },
                },
                "cooccurrence": {
                    "wikidata:Q1": {
                        "wikidata:Q1142007": 10,
                        "wikidata:Q13": 5,
                    },
                    "wikidata:Q2": {
                        "wikidata:Q1142007": 2,
                        "wikidata:Q13": 8,
                    },
                    "wikidata:Q3": {
                        "wikidata:Q1142007": 2,
                        "wikidata:Q13": 2,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Keir Starmer",
                label="person",
                start=0,
                end=12,
                confidence=0.82,
                relevance=0.82,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Keir Starmer",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Metropolitan Police",
                label="organization",
                start=13,
                end=32,
                confidence=0.76,
                relevance=0.76,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Metropolitan Police",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Golders Green",
                label="location",
                start=33,
                end=46,
                confidence=0.74,
                relevance=0.74,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Golders Green",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
            coherence_score=0.06,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        graph_context_artifact_path=_graph_context_artifact_with_party_affiliation(
            tmp_path
        ),
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q2", "wikidata:Q3", "wikidata:Q1"],
        settings=settings,
    )

    assert [item.entity_id for item in admitted] == ["wikidata:Q12"]
    assert admitted[0].canonical_text == "Labour Party"
    assert admitted[0].provider == "recent_news.support"


def test_recent_news_propose_related_entities_uses_graph_affiliation_when_support_is_missing(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-person-only-org-empty.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {},
                "cooccurrence": {},
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Keir Starmer",
                label="person",
                start=0,
                end=12,
                confidence=0.82,
                relevance=0.82,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Keir Starmer",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Invest NI",
                label="organization",
                start=13,
                end=22,
                confidence=0.76,
                relevance=0.76,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Invest NI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Belfast",
                label="location",
                start=23,
                end=30,
                confidence=0.74,
                relevance=0.74,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Belfast",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
            coherence_score=0.07,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        graph_context_artifact_path=_graph_context_artifact_with_party_affiliation(
            tmp_path
        ),
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q2", "wikidata:Q3", "wikidata:Q1"],
        settings=settings,
    )

    assert [item.entity_id for item in admitted] == ["wikidata:Q12"]
    assert admitted[0].provider == "qid_graph.affiliation"
    assert admitted[0].seed_entity_ids == ["wikidata:Q1"]


def test_recent_news_propose_related_entities_rejects_single_person_graph_affiliation_in_broad_low_coherence_context(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-person-only-org-empty-broad-context.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {},
                "cooccurrence": {},
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Iran",
                label="location",
                start=0,
                end=4,
                confidence=0.82,
                relevance=0.82,
                mention_count=4,
                link=EntityLink(
                    entity_id="wikidata:Q20",
                    canonical_text="Iran",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="United States",
                label="location",
                start=5,
                end=18,
                confidence=0.8,
                relevance=0.8,
                mention_count=3,
                link=EntityLink(
                    entity_id="wikidata:Q21",
                    canonical_text="United States",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Donald Trump",
                label="person",
                start=19,
                end=31,
                confidence=0.78,
                relevance=0.78,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Donald Trump",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Russia",
                label="location",
                start=32,
                end=38,
                confidence=0.74,
                relevance=0.74,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q22",
                    canonical_text="Russia",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=[
                "wikidata:Q20",
                "wikidata:Q21",
                "wikidata:Q1",
                "wikidata:Q22",
            ],
            coherence_score=0.034,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        graph_context_artifact_path=_graph_context_artifact_with_party_affiliation(
            tmp_path
        ),
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=[
            "wikidata:Q20",
            "wikidata:Q21",
            "wikidata:Q1",
            "wikidata:Q22",
        ],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_rejects_two_seed_anchor_supported_org_without_person_seed_when_low_coherence(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-two-seed-anchor-low-coherence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Foreign Commonwealth Office",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 6,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {
                        "wikidata:Q12": 3,
                    },
                    "wikidata:Q2": {
                        "wikidata:Q12": 3,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    response = _response_with_three_linked_entities().model_copy(
        update={
            "graph_support": GraphSupport(
                requested=True,
                applied=True,
                provider="sqlite.qid_graph_context",
                requested_related_entities=True,
                requested_refinement=False,
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
                coherence_score=0.02,
            )
        }
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_rejects_two_seed_org_without_anchor_support_when_low_coherence(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-two-seed-non-anchor-low-coherence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Weak Office",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 6,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {
                        "wikidata:Q12": 3,
                    },
                    "wikidata:Q2": {
                        "wikidata:Q12": 1,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    response = _response_with_person_and_location_entities().model_copy(
        update={
            "graph_support": GraphSupport(
                requested=True,
                applied=True,
                provider="sqlite.qid_graph_context",
                requested_related_entities=True,
                requested_refinement=False,
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
                coherence_score=0.02,
            )
        }
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=1,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_rejects_person_only_support(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-person-only.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 12,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Celebrity Insider",
                        "entity_type": "person",
                        "source_name": "saved-unseen-news",
                        "document_count": 5,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 3},
                    "wikidata:Q2": {"wikidata:Q12": 3},
                    "wikidata:Q3": {"wikidata:Q12": 3},
                },
            }
        ),
        encoding="utf-8",
    )
    response = _response_with_three_person_entities().model_copy(
        update={
            "graph_support": GraphSupport(
                requested=True,
                applied=True,
                provider="sqlite.qid_graph_context",
                requested_related_entities=True,
                requested_refinement=False,
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                coherence_score=0.02,
            )
        }
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_rejects_probable_source_outlets(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-source-outlet.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 12,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Associated Press",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 5,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 3},
                    "wikidata:Q2": {"wikidata:Q12": 3},
                    "wikidata:Q3": {"wikidata:Q12": 3},
                },
            }
        ),
        encoding="utf-8",
    )
    response = _response_with_person_and_location_entities().model_copy(
        update={
            "graph_support": GraphSupport(
                requested=True,
                applied=True,
                provider="sqlite.qid_graph_context",
                requested_related_entities=True,
                requested_refinement=False,
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                coherence_score=0.02,
            )
        }
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q3"],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_rejects_strong_non_anchor_org_without_person_seed_when_low_coherence(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-strong-non-anchor-low-coherence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "United Nations",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 22,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 5},
                    "wikidata:Q2": {"wikidata:Q12": 4},
                },
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Russia",
                label="location",
                start=0,
                end=6,
                confidence=0.74,
                relevance=0.74,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Russia",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="North Korea",
                label="location",
                start=10,
                end=21,
                confidence=0.73,
                relevance=0.73,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="North Korea",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Vladimir Putin",
                label="person",
                start=25,
                end=39,
                confidence=0.72,
                relevance=0.72,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Vladimir Putin",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Kim Jong Un",
                label="person",
                start=45,
                end=56,
                confidence=0.71,
                relevance=0.71,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q4",
                    canonical_text="Kim Jong Un",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=[
                "wikidata:Q1",
                "wikidata:Q2",
                "wikidata:Q3",
                "wikidata:Q4",
            ],
            coherence_score=0.0,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=[
            "wikidata:Q1",
            "wikidata:Q2",
            "wikidata:Q3",
            "wikidata:Q4",
        ],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_rejects_low_document_non_anchor_org_when_low_coherence(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-weak-non-anchor-low-coherence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Law Enforcement",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 4,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 3},
                    "wikidata:Q2": {"wikidata:Q12": 3},
                    "wikidata:Q3": {"wikidata:Q12": 3},
                    "wikidata:Q4": {"wikidata:Q12": 3},
                },
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Russia",
                label="location",
                start=0,
                end=6,
                confidence=0.74,
                relevance=0.74,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Russia",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="North Korea",
                label="location",
                start=10,
                end=21,
                confidence=0.73,
                relevance=0.73,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="North Korea",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Vladimir Putin",
                label="person",
                start=25,
                end=39,
                confidence=0.72,
                relevance=0.72,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Vladimir Putin",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Kim Jong Un",
                label="person",
                start=45,
                end=56,
                confidence=0.71,
                relevance=0.71,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q4",
                    canonical_text="Kim Jong Un",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=[
                "wikidata:Q1",
                "wikidata:Q2",
                "wikidata:Q3",
                "wikidata:Q4",
            ],
            coherence_score=0.0,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=[
            "wikidata:Q1",
            "wikidata:Q2",
            "wikidata:Q3",
            "wikidata:Q4",
        ],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_rejects_strong_non_anchor_org_above_extreme_low_coherence_floor(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-strong-non-anchor-mid-low-coherence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "United Nations",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 22,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 5},
                    "wikidata:Q2": {"wikidata:Q12": 4},
                },
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Russia",
                label="location",
                start=0,
                end=6,
                confidence=0.74,
                relevance=0.74,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Russia",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="North Korea",
                label="location",
                start=10,
                end=21,
                confidence=0.73,
                relevance=0.73,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="North Korea",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
            coherence_score=0.073333,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_rejects_higher_coherence_non_anchor_fallback(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-higher-coherence-non-anchor.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "United Nations",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 22,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 5},
                    "wikidata:Q2": {"wikidata:Q12": 4},
                    "wikidata:Q3": {"wikidata:Q12": 4},
                },
            }
        ),
        encoding="utf-8",
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Abbas Araghchi",
                label="person",
                start=0,
                end=14,
                confidence=0.74,
                relevance=0.74,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Abbas Araghchi",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="the United States",
                label="location",
                start=20,
                end=37,
                confidence=0.73,
                relevance=0.73,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="United States",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Pakistan",
                label="location",
                start=42,
                end=50,
                confidence=0.72,
                relevance=0.72,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Pakistan",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
            coherence_score=0.133333,
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_propose_related_entities_rejects_article_led_source_outlets(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent-news-support-source-outlet-leading-article.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 12,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "The Guardian",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 5,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 3},
                    "wikidata:Q2": {"wikidata:Q12": 3},
                    "wikidata:Q3": {"wikidata:Q12": 3},
                },
            }
        ),
        encoding="utf-8",
    )
    response = _response_with_person_and_location_entities().model_copy(
        update={
            "graph_support": GraphSupport(
                requested=True,
                applied=True,
                provider="sqlite.qid_graph_context",
                requested_related_entities=True,
                requested_refinement=False,
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                coherence_score=0.02,
            )
        }
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
        graph_context_related_limit=5,
    )

    admitted = vector_service._recent_news_propose_related_entities(
        response=response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q3"],
        settings=settings,
    )

    assert admitted == []


def test_recent_news_direct_fallback_seed_entity_ids_adds_business_location_seed(
    tmp_path: Path,
) -> None:
    response = _response_with_person_and_location_entities().model_copy(
        update={
            "graph_support": GraphSupport(
                requested=True,
                applied=True,
                provider="sqlite.qid_graph_context",
                requested_related_entities=True,
                requested_refinement=False,
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                coherence_score=0.02,
            ),
            "refinement_debug": {
                "graph_anchor_seed_ids": ["wikidata:Q1", "wikidata:Q2"],
                "seed_decisions": [
                    {"entity_id": "wikidata:Q1", "action": "keep"},
                    {"entity_id": "wikidata:Q2", "action": "keep"},
                    {"entity_id": "wikidata:Q3", "action": "keep"},
                ],
            },
        }
    )

    fallback_seed_ids = vector_service._recent_news_direct_fallback_seed_entity_ids(
        response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
        domain_hint="business",
    )

    assert fallback_seed_ids == ["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"]


def test_recent_news_direct_fallback_seed_entity_ids_adds_general_location_seed(
    tmp_path: Path,
) -> None:
    response = _response_with_person_and_location_entities().model_copy(
        update={
            "graph_support": GraphSupport(
                requested=True,
                applied=True,
                provider="sqlite.qid_graph_context",
                requested_related_entities=True,
                requested_refinement=False,
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                coherence_score=0.02,
            ),
            "refinement_debug": {
                "graph_anchor_seed_ids": ["wikidata:Q1", "wikidata:Q2"],
                "seed_decisions": [
                    {"entity_id": "wikidata:Q1", "action": "keep"},
                    {"entity_id": "wikidata:Q2", "action": "keep"},
                    {"entity_id": "wikidata:Q3", "action": "keep"},
                ],
            },
        }
    )

    fallback_seed_ids = vector_service._recent_news_direct_fallback_seed_entity_ids(
        response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
        domain_hint=None,
    )

    assert fallback_seed_ids == ["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"]


def test_recent_news_direct_fallback_seed_entity_ids_adds_person_seed_when_anchor_set_has_none(
    tmp_path: Path,
) -> None:
    response = _response_with_person_and_location_entities().model_copy(
        update={
            "graph_support": GraphSupport(
                requested=True,
                applied=True,
                provider="sqlite.qid_graph_context",
                requested_related_entities=True,
                requested_refinement=False,
                seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                coherence_score=0.02,
            ),
            "refinement_debug": {
                "graph_anchor_seed_ids": ["wikidata:Q3"],
                "seed_decisions": [
                    {"entity_id": "wikidata:Q1", "action": "keep"},
                    {"entity_id": "wikidata:Q2", "action": "keep"},
                    {"entity_id": "wikidata:Q3", "action": "keep"},
                ],
            },
        }
    )

    fallback_seed_ids = vector_service._recent_news_direct_fallback_seed_entity_ids(
        response,
        strong_seed_entity_ids=["wikidata:Q3"],
        domain_hint=None,
    )

    assert fallback_seed_ids == ["wikidata:Q3", "wikidata:Q1"]


def test_recent_news_direct_fallback_seed_entity_ids_skips_generic_lowercase_org_seed(
    tmp_path: Path,
) -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Law enforcement",
                label="organization",
                start=0,
                end=15,
                confidence=0.8,
                relevance=0.8,
                mention_count=3,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="law enforcement",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Donald Trump",
                label="person",
                start=16,
                end=28,
                confidence=0.7,
                relevance=0.7,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Donald Trump",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Secret Service",
                label="organization",
                start=29,
                end=43,
                confidence=0.72,
                relevance=0.72,
                mention_count=3,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="United States Secret Service",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="The White House",
                label="location",
                start=44,
                end=59,
                confidence=0.76,
                relevance=0.76,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q4",
                    canonical_text="The White House",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3", "wikidata:Q4"],
            coherence_score=0.02,
        ),
    ).model_copy(
        update={
            "refinement_debug": {
                "graph_anchor_seed_ids": ["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                "seed_decisions": [
                    {"entity_id": "wikidata:Q1", "action": "keep"},
                    {"entity_id": "wikidata:Q2", "action": "keep"},
                    {"entity_id": "wikidata:Q3", "action": "keep"},
                    {"entity_id": "wikidata:Q4", "action": "keep"},
                ],
            }
        }
    )

    fallback_seed_ids = vector_service._recent_news_direct_fallback_seed_entity_ids(
        response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
        domain_hint=None,
    )

    assert fallback_seed_ids == ["wikidata:Q2", "wikidata:Q3", "wikidata:Q4"]


def test_recent_news_direct_fallback_seed_entity_ids_only_adds_org_extras(
    tmp_path: Path,
) -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Donald Trump",
                label="person",
                start=0,
                end=12,
                confidence=0.7,
                relevance=0.7,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Donald Trump",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Secret Service",
                label="organization",
                start=13,
                end=27,
                confidence=0.72,
                relevance=0.72,
                mention_count=3,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="United States Secret Service",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Todd Blanche",
                label="person",
                start=28,
                end=40,
                confidence=0.8,
                relevance=0.8,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="Todd Blanche",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Department of Justice",
                label="organization",
                start=41,
                end=62,
                confidence=0.74,
                relevance=0.74,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q4",
                    canonical_text="Department of Justice",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="The White House",
                label="location",
                start=63,
                end=78,
                confidence=0.76,
                relevance=0.76,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q5",
                    canonical_text="The White House",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            requested_related_entities=True,
            requested_refinement=False,
            seed_entity_ids=[
                "wikidata:Q1",
                "wikidata:Q2",
                "wikidata:Q3",
                "wikidata:Q4",
                "wikidata:Q5",
            ],
            coherence_score=0.02,
        ),
    ).model_copy(
        update={
            "refinement_debug": {
                "graph_anchor_seed_ids": ["wikidata:Q1", "wikidata:Q2"],
                "seed_decisions": [
                    {"entity_id": "wikidata:Q1", "action": "keep"},
                    {"entity_id": "wikidata:Q2", "action": "keep"},
                    {"entity_id": "wikidata:Q3", "action": "keep"},
                    {"entity_id": "wikidata:Q4", "action": "keep"},
                    {"entity_id": "wikidata:Q5", "action": "keep"},
                ],
            }
        }
    )

    fallback_seed_ids = vector_service._recent_news_direct_fallback_seed_entity_ids(
        response,
        strong_seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
        domain_hint=None,
    )

    assert fallback_seed_ids == ["wikidata:Q1", "wikidata:Q2", "wikidata:Q4", "wikidata:Q5"]

def test_enrich_tag_response_with_related_entities_uses_recent_news_direct_candidates_when_vectors_are_empty(
    monkeypatch,
    tmp_path: Path,
) -> None:
    response = _response_with_three_linked_entities()
    artifact_path = tmp_path / "recent-news-support-enrich-low-coherence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Enterprise Platform",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 6,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 3},
                    "wikidata:Q2": {"wikidata:Q12": 3},
                    "wikidata:Q3": {"wikidata:Q12": 3},
                },
            }
        ),
        encoding="utf-8",
    )
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
        graph_context_related_limit=3,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
    )

    def _fake_apply_graph_context(*args, **kwargs):
        return response.model_copy(
            update={
                "graph_support": GraphSupport(
                    requested=True,
                    applied=True,
                    provider="sqlite.qid_graph_context",
                    requested_related_entities=True,
                    requested_refinement=False,
                    seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                    coherence_score=0.02,
                )
            }
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
            assert collection_name in {
                "ades-qids-current",
                "ades-qids-business-current",
            }
            return [vector_service.QdrantNearestPoint(point_id, 1.0, {})]

    monkeypatch.setattr(vector_service, "apply_graph_context", _fake_apply_graph_context)
    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert [item.entity_id for item in enriched.related_entities] == ["wikidata:Q12"]
    assert enriched.related_entities[0].provider == "recent_news.support"
    assert enriched.graph_support is not None
    assert "vector_search_low_coherence_recent_news_probe" in enriched.graph_support.warnings


def test_enrich_tag_response_with_related_entities_skips_low_coherence_recent_news_direct_candidates_without_person_seed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    response = _response_with_linked_entities()
    artifact_path = tmp_path / "recent-news-support-enrich-low-coherence-no-person.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Enterprise Platform",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 6,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 3},
                    "wikidata:Q2": {"wikidata:Q12": 3},
                },
            }
        ),
        encoding="utf-8",
    )
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
        graph_context_related_limit=3,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
    )

    def _fake_apply_graph_context(*args, **kwargs):
        return response.model_copy(
            update={
                "graph_support": GraphSupport(
                    requested=True,
                    applied=True,
                    provider="sqlite.qid_graph_context",
                    requested_related_entities=True,
                    requested_refinement=False,
                    seed_entity_ids=["wikidata:Q1", "wikidata:Q2"],
                    coherence_score=0.02,
                )
            }
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
            assert collection_name in {
                "ades-qids-current",
                "ades-qids-business-current",
            }
            return [vector_service.QdrantNearestPoint(point_id, 1.0, {})]

    monkeypatch.setattr(vector_service, "apply_graph_context", _fake_apply_graph_context)
    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert enriched.related_entities == []
    assert enriched.graph_support is not None
    assert "vector_search_low_coherence_recent_news_probe" in enriched.graph_support.warnings


def test_enrich_tag_response_with_related_entities_rejects_person_location_only_recent_news_direct_candidate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    response = _response_with_three_person_entities()
    artifact_path = tmp_path / "recent-news-support-enrich-low-coherence-person-non-anchor.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "Republican Party",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 6,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 4},
                    "wikidata:Q2": {"wikidata:Q12": 2},
                },
            }
        ),
        encoding="utf-8",
    )
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
        graph_context_related_limit=3,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
    )

    def _fake_apply_graph_context(*args, **kwargs):
        return response.model_copy(
            update={
                "graph_support": GraphSupport(
                    requested=True,
                    applied=True,
                    provider="sqlite.qid_graph_context",
                    requested_related_entities=True,
                    requested_refinement=False,
                    seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                    coherence_score=0.0516,
                )
            }
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
            assert collection_name in {
                "ades-qids-current",
                "ades-qids-business-current",
            }
            return [vector_service.QdrantNearestPoint(point_id, 1.0, {})]

    monkeypatch.setattr(vector_service, "apply_graph_context", _fake_apply_graph_context)
    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert enriched.related_entities == []
    assert enriched.graph_support is not None
    assert "vector_search_low_coherence_recent_news_probe" in enriched.graph_support.warnings


def test_enrich_tag_response_with_related_entities_retries_recent_news_direct_with_business_location_seed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Donald Trump",
                label="person",
                start=0,
                end=12,
                confidence=0.7,
                relevance=0.7,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Donald Trump",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Secret Service",
                label="organization",
                start=13,
                end=27,
                confidence=0.72,
                relevance=0.72,
                mention_count=3,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="United States Secret Service",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="The White House",
                label="location",
                start=28,
                end=43,
                confidence=0.76,
                relevance=0.76,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="The White House",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )
    artifact_path = tmp_path / "recent-news-support-enrich-business-fallback.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "United Nations",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 6,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 3},
                    "wikidata:Q2": {"wikidata:Q12": 3},
                    "wikidata:Q3": {"wikidata:Q12": 3},
                },
            }
        ),
        encoding="utf-8",
    )
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
        graph_context_related_limit=3,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
    )

    def _fake_apply_graph_context(*args, **kwargs):
        return response.model_copy(
            update={
                "graph_support": GraphSupport(
                    requested=True,
                    applied=True,
                    provider="sqlite.qid_graph_context",
                    requested_related_entities=True,
                    requested_refinement=False,
                    seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                    coherence_score=0.02,
                ),
                "refinement_debug": {
                    "graph_anchor_seed_ids": ["wikidata:Q1", "wikidata:Q2"],
                    "seed_decisions": [
                        {"entity_id": "wikidata:Q1", "action": "keep"},
                        {"entity_id": "wikidata:Q2", "action": "keep"},
                        {"entity_id": "wikidata:Q3", "action": "keep"},
                    ],
                },
            }
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
            assert collection_name in {
                "ades-qids-current",
                "ades-qids-business-current",
            }
            return [vector_service.QdrantNearestPoint(point_id, 1.0, {})]

    monkeypatch.setattr(vector_service, "apply_graph_context", _fake_apply_graph_context)
    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
        domain_hint="business",
    )

    assert [item.entity_id for item in enriched.related_entities] == ["wikidata:Q12"]
    assert enriched.related_entities[0].provider == "recent_news.support"


def test_enrich_tag_response_with_related_entities_retries_recent_news_direct_with_general_location_seed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Donald Trump",
                label="person",
                start=0,
                end=12,
                confidence=0.7,
                relevance=0.7,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="Donald Trump",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Secret Service",
                label="organization",
                start=13,
                end=27,
                confidence=0.72,
                relevance=0.72,
                mention_count=3,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="United States Secret Service",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="The White House",
                label="location",
                start=28,
                end=43,
                confidence=0.76,
                relevance=0.76,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="The White House",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )
    artifact_path = tmp_path / "recent-news-support-enrich-general-fallback.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 24,
                "qid_metadata": {
                    "wikidata:Q12": {
                        "canonical_text": "United Nations",
                        "entity_type": "organization",
                        "source_name": "saved-unseen-news",
                        "document_count": 6,
                    }
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 3},
                    "wikidata:Q2": {"wikidata:Q12": 3},
                    "wikidata:Q3": {"wikidata:Q12": 3},
                },
            }
        ),
        encoding="utf-8",
    )
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
        graph_context_related_limit=3,
        news_context_artifact_path=artifact_path,
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
    )

    def _fake_apply_graph_context(*args, **kwargs):
        return response.model_copy(
            update={
                "graph_support": GraphSupport(
                    requested=True,
                    applied=True,
                    provider="sqlite.qid_graph_context",
                    requested_related_entities=True,
                    requested_refinement=False,
                    seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                    coherence_score=0.02,
                ),
                "refinement_debug": {
                    "graph_anchor_seed_ids": ["wikidata:Q1", "wikidata:Q2"],
                    "seed_decisions": [
                        {"entity_id": "wikidata:Q1", "action": "keep"},
                        {"entity_id": "wikidata:Q2", "action": "keep"},
                        {"entity_id": "wikidata:Q3", "action": "keep"},
                    ],
                },
            }
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
            return [vector_service.QdrantNearestPoint(point_id, 1.0, {})]

    monkeypatch.setattr(vector_service, "apply_graph_context", _fake_apply_graph_context)
    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert [item.entity_id for item in enriched.related_entities] == ["wikidata:Q12"]
    assert enriched.related_entities[0].provider == "recent_news.support"


def test_enrich_tag_response_with_related_entities_uses_recent_news_support_when_graph_gate_rejects_candidate(
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
        news_context_artifact_path=_recent_news_support_artifact(tmp_path),
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
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
            return [
                vector_service.QdrantNearestPoint(point_id, 1.0, {}),
                vector_service.QdrantNearestPoint(
                    "wikidata:Q12",
                    0.79 if point_id == "wikidata:Q1" else 0.77,
                    {
                        "canonical_text": "Enterprise Platform",
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

    assert {item.entity_id for item in enriched.related_entities} == {
        "wikidata:Q4",
        "wikidata:Q12",
    }
    rescued_item = next(
        item for item in enriched.related_entities if item.entity_id == "wikidata:Q12"
    )
    assert rescued_item.seed_entity_ids == ["wikidata:Q1", "wikidata:Q2"]
    assert rescued_item.shared_seed_count == 2


def test_enrich_tag_response_with_related_entities_uses_recent_news_direct_support_as_empty_higher_coherence_fallback(
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
        graph_context_related_limit=3,
        news_context_artifact_path=_recent_news_support_artifact(tmp_path),
        news_context_min_supporting_seeds=2,
        news_context_min_pair_count=2,
    )

    def _fake_apply_graph_context(*args, **kwargs):
        return response.model_copy(
            update={
                "graph_support": GraphSupport(
                    requested=True,
                    applied=True,
                    provider="sqlite.qid_graph_context",
                    requested_related_entities=True,
                    requested_refinement=False,
                    seed_entity_ids=["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"],
                    coherence_score=0.33,
                )
            }
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
            return [vector_service.QdrantNearestPoint(point_id, 1.0, {})]

    monkeypatch.setattr(vector_service, "apply_graph_context", _fake_apply_graph_context)
    monkeypatch.setattr(vector_service, "QdrantVectorSearchClient", _FakeClient)

    enriched = vector_service.enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=True,
    )

    assert [item.entity_id for item in enriched.related_entities] == ["wikidata:Q12"]
    assert enriched.related_entities[0].provider == "recent_news.support"
