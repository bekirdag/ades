from ades.pipeline.tagger import (
    ExtractedCandidate,
    _apply_repeated_surface_consistency,
    _build_entity_link,
    _build_provenance,
    _resolve_overlaps,
)
from ades.service.models import EntityMatch


def _candidate(
    *,
    text: str,
    label: str,
    start: int,
    end: int,
    relevance: float,
    lane: str,
    canonical_text: str,
    entity_id: str,
) -> ExtractedCandidate:
    return ExtractedCandidate(
        entity=EntityMatch(
            text=text,
            label=label,
            start=start,
            end=end,
            confidence=relevance,
            relevance=relevance,
            provenance=_build_provenance(
                match_kind="alias",
                match_path="lookup.alias.exact",
                match_source=text,
                source_pack="test-pack",
                source_domain="general",
                lane=lane,
            ),
            link=_build_entity_link(
                provider="lookup.alias.exact",
                pack_id="test-pack",
                label=label,
                canonical_text=canonical_text,
                entity_id=entity_id,
            ),
        ),
        domain="general",
        lane=lane,
        normalized_start=start,
        normalized_end=end,
    )


def test_overlap_resolution_prefers_longer_span_when_relevance_matches() -> None:
    shorter = _candidate(
        text="Entity Alpha",
        label="organization",
        start=0,
        end=12,
        relevance=0.8,
        lane="deterministic_alias",
        canonical_text="Entity Alpha",
        entity_id="entity-alpha",
    )
    longer = _candidate(
        text="Entity Alpha Holdings",
        label="organization",
        start=0,
        end=21,
        relevance=0.8,
        lane="deterministic_rule",
        canonical_text="Entity Alpha Holdings",
        entity_id="entity-alpha-holdings",
    )

    kept, debug, discarded_count = _resolve_overlaps([shorter, longer], debug_enabled=True)

    assert [item.entity.text for item in kept] == ["Entity Alpha Holdings"]
    assert discarded_count == 1
    assert debug is not None
    assert any(
        item.text == "Entity Alpha" and item.reason == "overlap_replaced"
        for item in debug.span_decisions
    )


def test_repeated_surface_consistency_prefers_anchor_entity() -> None:
    anchor = _candidate(
        text="Entity Alpha",
        label="organization",
        start=0,
        end=12,
        relevance=0.9,
        lane="deterministic_alias",
        canonical_text="Entity Alpha Holdings",
        entity_id="entity-alpha-holdings",
    )
    alternate = _candidate(
        text="Entity Alpha",
        label="organization",
        start=20,
        end=32,
        relevance=0.6,
        lane="deterministic_alias",
        canonical_text="Entity Alpha Group",
        entity_id="entity-alpha-group",
    )

    rewritten = _apply_repeated_surface_consistency([anchor, alternate])

    assert len(rewritten) == 2
    assert all(
        item.entity.link is not None and item.entity.link.entity_id == "entity-alpha-holdings"
        for item in rewritten
    )
