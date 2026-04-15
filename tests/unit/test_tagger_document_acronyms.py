from ades.pipeline.tagger import (
    ExtractedCandidate,
    _backfill_document_acronym_entities,
    _build_entity_link,
    _build_provenance,
)
from ades.service.models import EntityMatch
from ades.text_processing import normalize_text


def _candidate(*, text: str, label: str, full_text: str, entity_id: str) -> ExtractedCandidate:
    start = full_text.index(text)
    end = start + len(text)
    normalized = normalize_text(full_text, "text/plain")
    raw_start, raw_end = normalized.raw_span(start, end)
    return ExtractedCandidate(
        entity=EntityMatch(
            text=normalized.raw_text[raw_start:raw_end],
            label=label,
            start=raw_start,
            end=raw_end,
            confidence=0.78,
            link=_build_entity_link(
                provider="lookup.alias.exact",
                pack_id="general-en",
                label=label,
                canonical_text=text,
                entity_id=entity_id,
            ),
            provenance=_build_provenance(
                match_kind="alias",
                match_path="lookup.alias.exact",
                match_source=text,
                source_pack="general-en",
                source_domain="general",
                lane="deterministic_alias",
            ),
        ),
        domain="general",
        lane="deterministic_alias",
        normalized_start=start,
        normalized_end=end,
    )


def test_document_acronym_backfill_recovers_explicit_long_form_definitions() -> None:
    full_text = (
        "International Energy Agency (IEA) officials said the IEA would respond."
    )
    normalized = normalize_text(full_text, "text/plain")
    extracted = [
        _candidate(
            text="International Energy Agency",
            label="organization",
            full_text=normalized.text,
            entity_id="organization:international-energy-agency",
        )
    ]

    augmented = _backfill_document_acronym_entities(
        extracted,
        normalized_input=normalized,
    )

    acronym_entities = [entity.entity for entity in augmented if entity.entity.text == "IEA"]

    assert len(acronym_entities) == 2
    assert all(entity.label == "organization" for entity in acronym_entities)
    assert all(entity.link is not None for entity in acronym_entities)
    assert {
        entity.provenance.match_path for entity in acronym_entities if entity.provenance is not None
    } == {"lookup.alias.document_acronym"}


def test_document_acronym_backfill_requires_explicit_definition_anchor() -> None:
    full_text = "International Energy Agency officials said the IEA would respond."
    normalized = normalize_text(full_text, "text/plain")
    extracted = [
        _candidate(
            text="International Energy Agency",
            label="organization",
            full_text=normalized.text,
            entity_id="organization:international-energy-agency",
        )
    ]

    augmented = _backfill_document_acronym_entities(
        extracted,
        normalized_input=normalized,
    )

    assert [entity.entity.text for entity in augmented] == ["International Energy Agency"]
