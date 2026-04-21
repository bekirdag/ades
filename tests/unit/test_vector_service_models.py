from ades.service.models import (
    EntityLink,
    EntityMatch,
    EntityProvenance,
    GraphSupport,
    RelatedEntityMatch,
    TagResponse,
)


def _entity(
    *,
    text: str,
    start: int,
    end: int,
    entity_id: str,
    relevance: float,
) -> EntityMatch:
    return EntityMatch(
        text=text,
        label="organization",
        start=start,
        end=end,
        aliases=[],
        confidence=0.8,
        relevance=relevance,
        provenance=EntityProvenance(
            match_kind="alias",
            match_path="lookup.alias.exact",
            match_source=text,
            source_pack="general-en",
            source_domain="general",
            lane="deterministic_alias",
        ),
        link=EntityLink(
            entity_id=entity_id,
            canonical_text=text,
            provider="lookup.alias.exact",
        ),
    )


def test_tag_response_preserves_related_entity_enrichment_fields() -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            _entity(
                text="Org Beta",
                start=0,
                end=8,
                entity_id="wikidata:Q123",
                relevance=0.9,
            )
        ],
        related_entities=[
            RelatedEntityMatch(
                entity_id="wikidata:Q456",
                canonical_text="Org Gamma",
                score=0.82,
                provider="qdrant.qid_graph",
                packs=["general-en"],
                seed_entity_ids=["wikidata:Q123"],
                shared_seed_count=1,
            )
        ],
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="qdrant.qid_graph",
            collection_alias="ades-qids-current",
            seed_entity_ids=["wikidata:Q123"],
            related_entity_count=1,
            suppressed_entity_ids=["wikidata:Q789"],
            suppressed_entity_count=1,
        ),
        topics=[],
        warnings=[],
        timing_ms=9,
    )

    assert response.related_entities[0].entity_id == "wikidata:Q456"
    assert response.graph_support is not None
    assert response.graph_support.applied is True
    assert response.graph_support.related_entity_count == 1
    assert response.graph_support.suppressed_entity_ids == ["wikidata:Q789"]
