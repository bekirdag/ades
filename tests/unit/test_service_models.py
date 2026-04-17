from ades.service.models import (
    EntityLink,
    EntityMatch,
    EntityProvenance,
    TagMetrics,
    TagResponse,
    TopicMatch,
)


def _entity(
    *,
    text: str,
    start: int,
    end: int,
    entity_id: str,
    relevance: float,
    canonical_text: str | None = None,
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
            canonical_text=canonical_text or text,
            provider="lookup.alias.exact",
        ),
    )


def test_tag_response_dedupes_duplicate_linked_entities_at_final_output() -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            _entity(
                text="Org Beta",
                start=5,
                end=13,
                entity_id="wikidata:Q123",
                relevance=0.82,
            ),
            _entity(
                text="Beta Org",
                start=40,
                end=48,
                entity_id="wikidata:Q123",
                relevance=0.91,
            ),
            _entity(
                text="Reuters",
                start=60,
                end=67,
                entity_id="wikidata:Q130879",
                relevance=0.88,
            ),
        ],
        topics=[TopicMatch(label="general", score=0.7, evidence_count=3, entity_labels=["organization"])],
        metrics=TagMetrics(
            input_char_count=100,
            normalized_char_count=100,
            token_count=20,
            segment_count=1,
            chunk_count=1,
            entity_count=3,
            entities_per_100_tokens=15.0,
            overlap_drop_count=0,
            per_label_counts={"organization": 3},
            per_lane_counts={"deterministic_alias": 3},
        ),
        warnings=[],
        timing_ms=1,
    )

    assert len(response.entities) == 2
    assert [entity.text for entity in response.entities] == ["Org Beta", "Reuters"]
    assert response.entities[0].aliases == ["Org Beta", "Beta Org"]
    assert response.entities[0].link is not None
    assert response.entities[0].link.entity_id == "wikidata:Q123"
    assert response.entities[0].relevance == 0.91
    assert response.metrics is not None
    assert response.metrics.entity_count == 2
    assert response.metrics.per_label_counts == {"organization": 2}
    assert response.metrics.per_lane_counts == {"deterministic_alias": 2}
    assert response.metrics.entities_per_100_tokens == 10.0
    assert response.topics[0].evidence_count == 2


def test_tag_response_filters_general_en_executive_title_acronym_entities() -> None:
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            _entity(
                text="CEO",
                start=0,
                end=3,
                entity_id="wikidata:Q2997673",
                relevance=0.7,
            ),
            _entity(
                text="Fun",
                start=4,
                end=7,
                entity_id="wikidata:Q838984",
                relevance=0.7,
            ),
            _entity(
                text="Care",
                start=8,
                end=12,
                entity_id="wikidata:Q3658527",
                relevance=0.7,
            ),
            _entity(
                text="The Daily",
                start=13,
                end=22,
                entity_id="wikidata:Q41288",
                relevance=0.7,
            ),
            _entity(
                text="The Republican",
                start=23,
                end=37,
                entity_id="wikidata:Q2276678",
                relevance=0.7,
            ),
            _entity(
                text="World Bank",
                start=38,
                end=48,
                entity_id="wikidata:Q3210",
                relevance=0.9,
            ),
        ],
        topics=[TopicMatch(label="general", score=0.7, evidence_count=6, entity_labels=["organization"])],
        metrics=TagMetrics(
            input_char_count=60,
            normalized_char_count=60,
            token_count=12,
            segment_count=1,
            chunk_count=1,
            entity_count=6,
            entities_per_100_tokens=50.0,
            overlap_drop_count=0,
            per_label_counts={"organization": 6},
            per_lane_counts={"deterministic_alias": 6},
        ),
        warnings=[],
        timing_ms=1,
    )

    assert [entity.text for entity in response.entities] == ["World Bank"]
    assert response.metrics is not None
    assert response.metrics.entity_count == 1
    assert response.metrics.entities_per_100_tokens == 8.3333
    assert response.topics[0].evidence_count == 1
