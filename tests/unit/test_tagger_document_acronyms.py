from pathlib import Path
from types import SimpleNamespace

import pytest

import ades.pipeline.tagger as tagger_module
from ades.pipeline.tagger import (
    ExtractedCandidate,
    _apply_entity_relevance,
    _apply_repeated_surface_consistency,
    _dedupe_exact_candidates,
    _extract_lookup_alias_entities,
    _backfill_contextual_org_acronym_entities,
    _backfill_heuristic_structured_organization_entities,
    _backfill_heuristic_structured_location_entities,
    _backfill_heuristic_definition_acronym_entities,
    _backfill_document_acronym_entities,
    _build_entity_link,
    _build_provenance,
    _derive_initialism,
    _resolve_overlaps,
)
from ades.runtime_matcher import MatcherEntryPayload
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


def test_contextual_org_acronym_backfill_recovers_reporting_and_possessive_cases() -> None:
    full_text = "He told CNBC that WWF's office responded."
    normalized = normalize_text(full_text, "text/plain")

    augmented = _backfill_contextual_org_acronym_entities(
        [],
        normalized_input=normalized,
        runtime=SimpleNamespace(pack_id="general-en", domain="general"),
    )

    pairs = {(entity.entity.text, entity.entity.label, entity.lane) for entity in augmented}

    assert ("CNBC", "organization", "contextual_org_acronym_backfill") in pairs
    assert ("WWF", "organization", "contextual_org_acronym_backfill") in pairs


def test_contextual_org_acronym_backfill_skips_title_media_and_generic_policy_noise() -> None:
    full_text = (
        "Julian Knight MP praised the TV show and supported DACA, the act that helps students. "
        "LGBT (lesbian, gay, bisexual and transgender) refugees arrived. "
        "He told Walker that Bruyne's pass reached City."
    )
    normalized = normalize_text(full_text, "text/plain")

    augmented = _backfill_contextual_org_acronym_entities(
        [],
        normalized_input=normalized,
        runtime=SimpleNamespace(pack_id="general-en", domain="general"),
    )

    assert augmented == []


def test_heuristic_definition_acronym_backfill_recovers_parenthetical_and_comma_definitions() -> None:
    full_text = (
        "The Indian Youth Congress (IYC) met delegates. "
        "The Ijaw National Youth Council, IYC, welcomed members of the IYC. "
        "The Liberation Tigers of Tamil Eelam (LTTE) were also mentioned."
    )
    normalized = normalize_text(full_text, "text/plain")

    augmented = _backfill_heuristic_definition_acronym_entities(
        [],
        normalized_input=normalized,
        runtime=SimpleNamespace(
            pack_id="general-en",
            domain="general",
            labels=("organization", "location"),
        ),
    )

    pairs = {(entity.entity.text, entity.entity.label, entity.lane) for entity in augmented}

    assert ("IYC", "organization", "heuristic_definition_acronym_backfill") in pairs
    assert ("LTTE", "organization", "heuristic_definition_acronym_backfill") in pairs
    assert sum(1 for entity in augmented if entity.entity.text == "IYC") == 3


def test_heuristic_definition_acronym_backfill_supports_or_appositive_definitions() -> None:
    full_text = "Deferred Action for Childhood Arrivals, or DACA, program supporters gathered."
    normalized = normalize_text(full_text, "text/plain")

    augmented = _backfill_heuristic_definition_acronym_entities(
        [],
        normalized_input=normalized,
        runtime=SimpleNamespace(
            pack_id="general-en",
            domain="general",
            labels=("organization", "location"),
        ),
    )

    pairs = {(entity.entity.text, entity.entity.label, entity.lane) for entity in augmented}

    assert ("DACA", "organization", "heuristic_definition_acronym_backfill") in pairs


def test_contextual_org_acronym_backfill_supports_source_and_scholarship_cues() -> None:
    full_text = "Image Source: PTI/ Reuters. She received a scholarship from MIT."
    normalized = normalize_text(full_text, "text/plain")

    augmented = _backfill_contextual_org_acronym_entities(
        [],
        normalized_input=normalized,
        runtime=SimpleNamespace(
            pack_id="general-en",
            domain="general",
            labels=("organization", "location"),
        ),
    )

    pairs = {(entity.entity.text, entity.entity.label, entity.lane) for entity in augmented}

    assert ("PTI", "organization", "contextual_org_acronym_backfill") in pairs
    assert ("MIT", "organization", "contextual_org_acronym_backfill") in pairs


def test_derive_initialism_keeps_dotted_initial_prefixes() -> None:
    assert _derive_initialism("U.N. High Commissioner for Refugees") == "UNHCR"


def test_contextual_org_acronym_backfill_supports_article_led_reporting_cues() -> None:
    full_text = (
        "Roads to Goma were crowded with refugees, the UNHCR reported. "
        "The U.N. High Commissioner for Refugees warned of danger."
    )
    normalized = normalize_text(full_text, "text/plain")

    augmented = _backfill_contextual_org_acronym_entities(
        [],
        normalized_input=normalized,
        runtime=SimpleNamespace(
            pack_id="general-en",
            domain="general",
            labels=("organization", "location"),
        ),
    )

    pairs = {(entity.entity.text, entity.entity.label, entity.lane) for entity in augmented}

    assert ("UNHCR", "organization", "contextual_org_acronym_backfill") in pairs


def test_contextual_org_acronym_backfill_supports_clause_boundary_reporting_cues() -> None:
    full_text = (
        '"Our offices are basically destroyed now, nothing works," UNHCR spokesman '
        "Ron Redmond said from Geneva."
    )
    normalized = normalize_text(full_text, "text/plain")

    augmented = _backfill_contextual_org_acronym_entities(
        [],
        normalized_input=normalized,
        runtime=SimpleNamespace(
            pack_id="general-en",
            domain="general",
            labels=("organization", "location"),
        ),
    )

    pairs = {(entity.entity.text, entity.entity.label, entity.lane) for entity in augmented}

    assert ("UNHCR", "organization", "contextual_org_acronym_backfill") in pairs


def test_lookup_alias_entities_for_matcher_packs_supplement_short_exact_aliases_from_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = "The UK and US met UAE officials."
    normalized = normalize_text(text, "text/plain")

    class _StubRegistry:
        def lookup_candidates(
            self,
            candidate_text: str,
            *,
            pack_id: str | None = None,
            exact_alias: bool = False,
            active_only: bool = False,
            limit: int = 50,
        ) -> list[dict[str, str | float | bool | None]]:
            del active_only, limit
            assert exact_alias is True
            entries = {
                ("general-en", "UK"): [
                    {
                        "kind": "alias",
                        "value": "UK",
                        "label": "location",
                        "pack_id": "general-en",
                        "domain": "general",
                        "score": 0.8739,
                        "canonical_text": "United Kingdom",
                        "entity_id": "wikidata:Q145",
                    }
                ],
                ("general-en", "US"): [
                    {
                        "kind": "alias",
                        "value": "US",
                        "label": "location",
                        "pack_id": "general-en",
                        "domain": "general",
                        "score": 0.8754,
                        "canonical_text": "United States",
                        "entity_id": "geonames:6252001",
                    }
                ],
                ("general-en", "UAE"): [
                    {
                        "kind": "alias",
                        "value": "UAE",
                        "label": "location",
                        "pack_id": "general-en",
                        "domain": "general",
                        "score": 0.8845,
                        "canonical_text": "United Arab Emirates",
                        "entity_id": "geonames:290557",
                    }
                ],
            }
            if pack_id is not None:
                return entries.get((pack_id, candidate_text), [])
            return [
                item
                for (entry_pack_id, entry_text), values in entries.items()
                if entry_pack_id == "general-en" and entry_text == candidate_text
                for item in values
            ]

    compiled_matcher = SimpleNamespace(
        entry_payloads=[
            MatcherEntryPayload(
                text="UK",
                label="organization",
                canonical_text="UK",
                entity_id="wikidata:Q1330725",
                score=0.6906,
                alias_score=0.6906,
                generated=False,
                source_name="matcher",
                source_priority=1.0,
                popularity_weight=1.0,
                source_domain="general",
            )
        ]
    )
    matcher_runtime = SimpleNamespace(
        pack_chain=["general-en"],
        matchers=[
            SimpleNamespace(
                pack_id="general-en",
                artifact_path=Path("matcher.bin"),
                entries_path=Path("entries.bin"),
            )
        ],
        domain="general",
    )
    uk_start = normalized.text.index("UK")

    def _fake_find_exact_match_candidates(
        segment_text: str,
        _matcher: object,
    ) -> list[SimpleNamespace]:
        if segment_text == normalized.text:
            return [SimpleNamespace(start=uk_start, end=uk_start + 2, entry_indices=[0])]
        return []

    monkeypatch.setattr(tagger_module, "load_runtime_matcher", lambda *_args, **_kwargs: compiled_matcher)
    monkeypatch.setattr(
        tagger_module,
        "find_exact_match_candidates",
        _fake_find_exact_match_candidates,
    )

    extracted = _extract_lookup_alias_entities(
        normalized.text,
        segment_start=0,
        normalized_input=normalized,
        registry=_StubRegistry(),
        runtime=matcher_runtime,
    )
    pairs = {
        (candidate.entity.text, candidate.entity.label, candidate.entity.link.canonical_text)
        for candidate in extracted
        if candidate.entity.link is not None
    }

    assert ("UK", "location", "United Kingdom") in pairs
    assert ("US", "location", "United States") in pairs
    assert ("UAE", "location", "United Arab Emirates") in pairs


def test_relevance_prefers_registry_geopolitical_acronyms_over_stale_matcher_org_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = "UK officials said the UK and US met UAE officials while the UK responded."
    normalized = normalize_text(text, "text/plain")

    class _StubRegistry:
        def lookup_candidates(
            self,
            candidate_text: str,
            *,
            pack_id: str | None = None,
            exact_alias: bool = False,
            active_only: bool = False,
            limit: int = 50,
        ) -> list[dict[str, str | float | bool | None]]:
            del active_only, limit
            assert exact_alias is True
            entries = {
                ("general-en", "UK"): [
                    {
                        "kind": "alias",
                        "value": "UK",
                        "label": "location",
                        "pack_id": "general-en",
                        "domain": "general",
                        "score": 0.8739,
                        "canonical_text": "United Kingdom",
                        "entity_id": "wikidata:Q145",
                    }
                ],
                ("general-en", "US"): [
                    {
                        "kind": "alias",
                        "value": "US",
                        "label": "location",
                        "pack_id": "general-en",
                        "domain": "general",
                        "score": 0.8754,
                        "canonical_text": "United States",
                        "entity_id": "geonames:6252001",
                    }
                ],
                ("general-en", "UAE"): [
                    {
                        "kind": "alias",
                        "value": "UAE",
                        "label": "location",
                        "pack_id": "general-en",
                        "domain": "general",
                        "score": 0.8845,
                        "canonical_text": "United Arab Emirates",
                        "entity_id": "geonames:290557",
                    }
                ],
            }
            if pack_id is not None:
                return entries.get((pack_id, candidate_text), [])
            return [
                item
                for (entry_pack_id, entry_text), values in entries.items()
                if entry_pack_id == "general-en" and entry_text == candidate_text
                for item in values
            ]

    compiled_matcher = SimpleNamespace(
        entry_payloads=[
            MatcherEntryPayload(
                text="UK",
                label="organization",
                canonical_text="UK",
                entity_id="wikidata:Q1330725",
                score=0.6906,
                alias_score=0.6906,
                generated=False,
                source_name="matcher",
                source_priority=1.0,
                popularity_weight=1.0,
                source_domain="general",
            )
        ]
    )
    matcher_runtime = SimpleNamespace(
        pack_chain=["general-en"],
        matchers=[
            SimpleNamespace(
                pack_id="general-en",
                artifact_path=Path("matcher.bin"),
                entries_path=Path("entries.bin"),
            )
        ],
        domain="general",
    )
    uk_start = normalized.text.index("UK")

    def _fake_find_exact_match_candidates(
        segment_text: str,
        _matcher: object,
    ) -> list[SimpleNamespace]:
        if segment_text == normalized.text:
            return [
                SimpleNamespace(start=uk_start, end=uk_start + 2, entry_indices=[0]),
                SimpleNamespace(start=text.index("UK", uk_start + 1), end=text.index("UK", uk_start + 1) + 2, entry_indices=[0]),
                SimpleNamespace(start=text.rindex("UK"), end=text.rindex("UK") + 2, entry_indices=[0]),
            ]
        return []

    monkeypatch.setattr(tagger_module, "load_runtime_matcher", lambda *_args, **_kwargs: compiled_matcher)
    monkeypatch.setattr(
        tagger_module,
        "find_exact_match_candidates",
        _fake_find_exact_match_candidates,
    )

    extracted = _extract_lookup_alias_entities(
        normalized.text,
        segment_start=0,
        normalized_input=normalized,
        registry=_StubRegistry(),
        runtime=matcher_runtime,
    )
    deduped, _ = _dedupe_exact_candidates(extracted)
    coherent = _apply_repeated_surface_consistency(deduped)
    scored = _apply_entity_relevance(
        matcher_runtime,
        text=normalized.text,
        extracted=coherent,
    )
    resolved, _, _ = _resolve_overlaps(scored, debug_enabled=False)
    pairs = {(candidate.entity.text, candidate.entity.label) for candidate in resolved}

    assert ("UK", "location") in pairs
    assert ("UK", "organization") not in pairs
    assert ("US", "location") in pairs
    assert ("UAE", "location") in pairs


def test_heuristic_structured_org_backfill_recovers_ministry_university_and_corporation() -> None:
    full_text = (
        "Russian Foreign Ministry officials met Columbia University and "
        "Opinion Research Corporation advisers."
    )
    normalized = normalize_text(full_text, "text/plain")

    augmented = _backfill_heuristic_structured_organization_entities(
        [],
        normalized_input=normalized,
        runtime=SimpleNamespace(
            pack_id="general-en",
            domain="general",
            labels=("organization", "location"),
        ),
    )

    pairs = {(entity.entity.text, entity.entity.label, entity.lane) for entity in augmented}

    assert ("Russian Foreign Ministry", "organization", "heuristic_structured_org_backfill") in pairs
    assert ("Columbia University", "organization", "heuristic_structured_org_backfill") in pairs
    assert (
        "Opinion Research Corporation",
        "organization",
        "heuristic_structured_org_backfill",
    ) in pairs


def test_heuristic_structured_org_backfill_recovers_connector_and_hospital_spans() -> None:
    full_text = (
        "Mohini Giri led the National Commission for Women. "
        "WHO Collaborating Centre for pharmacovigilance launched at IPC Ghaziabad. "
        "Evans told the Society of Editors meeting. "
        "A patient was taken to Sunshine Coast University Hospital."
    )
    normalized = normalize_text(full_text, "text/plain")

    augmented = _backfill_heuristic_structured_organization_entities(
        [],
        normalized_input=normalized,
        runtime=SimpleNamespace(
            pack_id="general-en",
            domain="general",
            labels=("organization", "location"),
        ),
    )

    pairs = {(entity.entity.text, entity.entity.label, entity.lane) for entity in augmented}

    assert (
        "National Commission for Women",
        "organization",
        "heuristic_structured_org_backfill",
    ) in pairs
    assert (
        "WHO Collaborating Centre for pharmacovigilance",
        "organization",
        "heuristic_structured_org_backfill",
    ) in pairs
    assert ("Society of Editors", "organization", "heuristic_structured_org_backfill") in pairs
    assert (
        "Sunshine Coast University Hospital",
        "organization",
        "heuristic_structured_org_backfill",
    ) in pairs


def test_heuristic_structured_org_and_location_backfill_recover_solutions_and_location_heads() -> None:
    full_text = (
        "Dave Ramsey is CEO of Ramsey Solutions. "
        "The men were held on the island of Nauru."
    )
    normalized = normalize_text(full_text, "text/plain")

    org_augmented = _backfill_heuristic_structured_organization_entities(
        [],
        normalized_input=normalized,
        runtime=SimpleNamespace(
            pack_id="general-en",
            domain="general",
            labels=("organization", "location"),
        ),
    )
    location_augmented = _backfill_heuristic_structured_location_entities(
        org_augmented,
        normalized_input=normalized,
        runtime=SimpleNamespace(
            pack_id="general-en",
            domain="general",
            labels=("organization", "location"),
        ),
    )

    pairs = {(entity.entity.text, entity.entity.label, entity.lane) for entity in location_augmented}

    assert ("Ramsey Solutions", "organization", "heuristic_structured_org_backfill") in pairs
    assert ("island of Nauru", "location", "heuristic_structured_location_backfill") in pairs
