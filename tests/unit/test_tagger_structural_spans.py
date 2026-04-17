from ades.pipeline.tagger import (
    ExtractedCandidate,
    _build_entity_link,
    _build_provenance,
    _extend_structural_entity_spans,
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


def test_structural_org_extension_absorbs_trailing_corporate_suffixes() -> None:
    full_text = "Signal Harbor Partners LLC announced new guidance."
    normalized = normalize_text(full_text, "text/plain")
    extracted = [
        _candidate(
            text="Signal Harbor Partners",
            label="organization",
            full_text=normalized.text,
            entity_id="organization:signal-harbor-partners",
        )
    ]

    augmented = _extend_structural_entity_spans(
        extracted,
        normalized_input=normalized,
    )

    assert [candidate.entity.text for candidate in augmented] == ["Signal Harbor Partners LLC"]


def test_structural_org_extension_absorbs_short_connector_tails() -> None:
    full_text = "Northwind Institute of Talora opened a new office."
    normalized = normalize_text(full_text, "text/plain")
    extracted = [
        _candidate(
            text="Northwind Institute",
            label="organization",
            full_text=normalized.text,
            entity_id="organization:northwind-institute",
        )
    ]

    augmented = _extend_structural_entity_spans(
        extracted,
        normalized_input=normalized,
    )

    assert [candidate.entity.text for candidate in augmented] == ["Northwind Institute of Talora"]


def test_structural_org_extension_absorbs_college_connector_tails() -> None:
    full_text = "The College of Physicians of Philadelphia hosted the exhibit."
    normalized = normalize_text(full_text, "text/plain")
    extracted = [
        _candidate(
            text="College of Physicians",
            label="organization",
            full_text=normalized.text,
            entity_id="organization:college-of-physicians",
        )
    ]

    augmented = _extend_structural_entity_spans(
        extracted,
        normalized_input=normalized,
    )

    assert [candidate.entity.text for candidate in augmented] == [
        "College of Physicians of Philadelphia"
    ]


def test_structural_org_extension_absorbs_generic_connector_tails() -> None:
    full_text = "Media Matters for America worked with Pulse of Europe organizers."
    normalized = normalize_text(full_text, "text/plain")
    extracted = [
        _candidate(
            text="Media Matters",
            label="organization",
            full_text=normalized.text,
            entity_id="organization:media-matters",
        ),
        _candidate(
            text="Pulse",
            label="organization",
            full_text=normalized.text,
            entity_id="organization:pulse",
        ),
    ]

    augmented = _extend_structural_entity_spans(
        extracted,
        normalized_input=normalized,
    )

    assert [candidate.entity.text for candidate in augmented] == [
        "Media Matters for America",
        "Pulse of Europe",
    ]


def test_structural_org_extension_absorbs_study_and_coordination_tails() -> None:
    full_text = (
        "The RE Council of England and Wales met after the Department of War Studies "
        "published a note."
    )
    normalized = normalize_text(full_text, "text/plain")
    extracted = [
        _candidate(
            text="RE Council of England",
            label="organization",
            full_text=normalized.text,
            entity_id="organization:re-council-of-england",
        ),
        _candidate(
            text="Department of War",
            label="organization",
            full_text=normalized.text,
            entity_id="organization:department-of-war",
        ),
    ]

    augmented = _extend_structural_entity_spans(
        extracted,
        normalized_input=normalized,
    )

    assert [candidate.entity.text for candidate in augmented] == [
        "RE Council of England and Wales",
        "Department of War Studies",
    ]
