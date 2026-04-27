from ades.pipeline.tagger import (
    ExtractedCandidate,
    _apply_repeated_surface_consistency,
    _build_entity_link,
    _build_provenance,
    _is_runtime_contextual_org_acronym_noise,
    _is_runtime_date_like_alias,
    _is_runtime_article_led_generic_org_alias,
    _is_runtime_function_year_alias,
    _is_runtime_generic_phrase_fragment_alias,
    _is_runtime_generic_single_token_alias,
    _is_runtime_leading_apostrophe_fragment_alias,
    _is_runtime_office_title_person_alias_artifact,
    _is_runtime_partial_person_fragment_alias,
    _is_runtime_title_led_person_alias_artifact,
    _is_runtime_possessive_titleish_alias_artifact,
    _is_runtime_possessive_alias_artifact,
    _is_runtime_quoted_fragment_alias,
    _is_runtime_single_token_titleish_fragment_alias,
    _is_runtime_structural_geo_feature_label_mismatch,
    _is_runtime_truncated_connector_alias_artifact,
    _merge_linked_entity_candidates,
    _person_surname_key,
    _resolve_overlaps,
    _suppress_document_person_surname_false_positives,
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
    entity_id: str | None,
    match_source: str | None = None,
    match_path: str = "lookup.alias.exact",
    source_pack: str = "test-pack",
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
                match_path=match_path,
                match_source=match_source or text,
                source_pack=source_pack,
                source_domain="general",
                lane=lane,
            ),
            link=(
                _build_entity_link(
                    provider="lookup.alias.exact",
                    pack_id=source_pack,
                    label=label,
                    canonical_text=canonical_text,
                    entity_id=entity_id,
                )
                if entity_id is not None
                else None
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


def test_linked_entity_merge_returns_unique_entity_with_aliases() -> None:
    primary = _candidate(
        text="Org Beta",
        label="organization",
        start=0,
        end=8,
        relevance=0.9,
        lane="deterministic_alias",
        canonical_text="Organization Beta",
        entity_id="wikidata:Q123",
    )
    alternate = _candidate(
        text="Beta Org",
        label="organization",
        start=15,
        end=23,
        relevance=0.6,
        lane="deterministic_alias",
        canonical_text="Organization Beta",
        entity_id="wikidata:Q123",
    )
    unique, discarded = _merge_linked_entity_candidates([primary, alternate])

    assert len(unique) == 1
    assert [item.entity.text for item in unique] == ["Org Beta"]
    assert unique[0].entity.aliases == ["Org Beta", "Beta Org"]
    assert unique[0].entity.link is not None
    assert unique[0].entity.link.entity_id == "wikidata:Q123"
    assert len(discarded) == 1
    assert discarded[0].entity.text == "Beta Org"


def test_person_surname_key_ignores_titles_and_suffixes() -> None:
    assert _person_surname_key("Governor Lisa Cook") == "cook"
    assert _person_surname_key("Jerome H. Powell Jr.") == "powell"
    assert _person_surname_key("Powell") is None


def test_suppress_document_person_surname_false_positives() -> None:
    text = "Governor Lisa Cook said rates could stay high. Later, Cook said policy remained tight."
    cook_start = text.index("Cook said", text.index("Later"))
    cook_end = cook_start + len("Cook")
    person = _candidate(
        text="Governor Lisa Cook",
        label="person",
        start=0,
        end=18,
        relevance=0.88,
        lane="deterministic_alias",
        canonical_text="Lisa D. Cook",
        entity_id="wikidata:Q48698939",
    )
    location = _candidate(
        text="Cook",
        label="location",
        start=cook_start,
        end=cook_end,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Cook",
        entity_id="wikidata:Q2561369",
    )

    kept, discarded = _suppress_document_person_surname_false_positives(
        [person, location],
        text=text,
        pack_id="general-en",
    )

    assert [item.entity.text for item in kept] == ["Governor Lisa Cook"]
    assert [item.entity.text for item in discarded] == ["Cook"]


def test_suppress_document_person_surname_false_positive_after_about_context() -> None:
    text = (
        "Federal Reserve Chair Jerome Powell spoke earlier. "
        "Trump answered a question about Powell staying on at the Fed."
    )
    powell_start = text.index("Powell staying")
    powell_end = powell_start + len("Powell")
    person = _candidate(
        text="Jerome Powell",
        label="person",
        start=text.index("Jerome Powell"),
        end=text.index("Jerome Powell") + len("Jerome Powell"),
        relevance=0.88,
        lane="deterministic_alias",
        canonical_text="Jerome Powell",
        entity_id="wikidata:Q6182718",
    )
    location = _candidate(
        text="Powell",
        label="location",
        start=powell_start,
        end=powell_end,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Powell",
        entity_id="wikidata:Q66676",
    )

    kept, discarded = _suppress_document_person_surname_false_positives(
        [person, location],
        text=text,
        pack_id="general-en",
    )

    assert [item.entity.text for item in kept] == ["Jerome Powell"]
    assert [item.entity.text for item in discarded] == ["Powell"]


def test_suppress_document_person_surname_false_positive_for_confirmed_context() -> None:
    text = (
        "Federal Reserve Chair Jerome Powell spoke earlier. "
        "Later, Powell confirmed he would remain in place."
    )
    powell_start = text.index("Powell confirmed")
    powell_end = powell_start + len("Powell")
    person = _candidate(
        text="Jerome Powell",
        label="person",
        start=text.index("Jerome Powell"),
        end=text.index("Jerome Powell") + len("Jerome Powell"),
        relevance=0.88,
        lane="deterministic_alias",
        canonical_text="Jerome Powell",
        entity_id="wikidata:Q6182718",
    )
    location = _candidate(
        text="Powell",
        label="location",
        start=powell_start,
        end=powell_end,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Powell",
        entity_id="wikidata:Q66676",
    )

    kept, discarded = _suppress_document_person_surname_false_positives(
        [person, location],
        text=text,
        pack_id="general-en",
    )

    assert [item.entity.text for item in kept] == ["Jerome Powell"]
    assert [item.entity.text for item in discarded] == ["Powell"]


def test_suppress_document_person_surname_false_positive_for_led_context() -> None:
    text = (
        "Vice President JD Vance arrived in Rome. "
        "Later, Vance led a first round of unsuccessful talks."
    )
    vance_start = text.index("Vance led")
    vance_end = vance_start + len("Vance")
    person = _candidate(
        text="JD Vance",
        label="person",
        start=text.index("JD Vance"),
        end=text.index("JD Vance") + len("JD Vance"),
        relevance=0.88,
        lane="deterministic_alias",
        canonical_text="JD Vance",
        entity_id="wikidata:Q6279",
    )
    location = _candidate(
        text="Vance",
        label="location",
        start=vance_start,
        end=vance_end,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Vance",
        entity_id="wikidata:Q7947746",
    )

    kept, discarded = _suppress_document_person_surname_false_positives(
        [person, location],
        text=text,
        pack_id="general-en",
    )

    assert [item.entity.text for item in kept] == ["JD Vance"]
    assert [item.entity.text for item in discarded] == ["Vance"]


def test_suppress_document_person_surname_false_positive_for_reported_object_context() -> None:
    text = (
        "Morgan McSweeney spoke with Tim Barton earlier. "
        "Advisers told Barton to just approve the plan."
    )
    barton_start = text.index("Barton to")
    barton_end = barton_start + len("Barton")
    person = _candidate(
        text="Tim Barton",
        label="person",
        start=text.index("Tim Barton"),
        end=text.index("Tim Barton") + len("Tim Barton"),
        relevance=0.88,
        lane="deterministic_alias",
        canonical_text="Tim Barton",
        entity_id="wikidata:Q999001",
    )
    location = _candidate(
        text="Barton",
        label="location",
        start=barton_start,
        end=barton_end,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Barton",
        entity_id="wikidata:Q4866431",
    )

    kept, discarded = _suppress_document_person_surname_false_positives(
        [person, location],
        text=text,
        pack_id="general-en",
    )

    assert [item.entity.text for item in kept] == ["Tim Barton"]
    assert [item.entity.text for item in discarded] == ["Barton"]


def test_suppress_document_person_surname_false_positive_for_went_public_context() -> None:
    text = (
        "Premier Doug Ford criticized the proposal earlier. "
        "Ford went public in April with his concerns."
    )
    person = _candidate(
        text="Doug Ford",
        label="person",
        start=text.index("Doug Ford"),
        end=text.index("Doug Ford") + len("Doug Ford"),
        relevance=0.88,
        lane="deterministic_alias",
        canonical_text="Doug Ford",
        entity_id="wikidata:Q1625904",
    )
    ford_start = text.index("Ford went public")
    location = _candidate(
        text="Ford",
        label="location",
        start=ford_start,
        end=ford_start + len("Ford"),
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Ford",
        entity_id="wikidata:Q587761",
    )

    kept, discarded = _suppress_document_person_surname_false_positives(
        [person, location],
        text=text,
        pack_id="general-en",
    )

    assert [item.entity.text for item in kept] == ["Doug Ford"]
    assert [item.entity.text for item in discarded] == ["Ford"]


def test_suppress_document_person_surname_false_positive_for_denies_context() -> None:
    text = (
        "President Donald Trump criticized the Fed. "
        "Trump also tried to fire Fed Governor Lisa Cook for cause. "
        "Allegations Cook denies. Cook, who remains in her role, fought the Trump administration."
    )
    trump_person = _candidate(
        text="President Donald Trump",
        label="person",
        start=text.index("President Donald Trump"),
        end=text.index("President Donald Trump") + len("President Donald Trump"),
        relevance=0.88,
        lane="deterministic_alias",
        canonical_text="Donald Trump",
        entity_id="wikidata:Q22686",
    )
    person = _candidate(
        text="Governor Lisa Cook",
        label="person",
        start=text.index("Governor Lisa Cook"),
        end=text.index("Governor Lisa Cook") + len("Governor Lisa Cook"),
        relevance=0.88,
        lane="deterministic_alias",
        canonical_text="Lisa D. Cook",
        entity_id="wikidata:Q48698939",
    )
    first_cook_start = text.index("Cook denies")
    first_cook = _candidate(
        text="Cook",
        label="location",
        start=first_cook_start,
        end=first_cook_start + len("Cook"),
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Cook",
        entity_id="wikidata:Q2561369",
    )
    article_led = _candidate(
        text="the Trump",
        label="organization",
        start=text.index("the Trump"),
        end=text.index("the Trump") + len("the Trump"),
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="The Trump Group",
        entity_id="wikidata:Q43081125",
    )

    kept, discarded = _suppress_document_person_surname_false_positives(
        [trump_person, person, first_cook, article_led],
        text=text,
        pack_id="general-en",
    )

    assert [item.entity.text for item in kept] == [
        "President Donald Trump",
        "Governor Lisa Cook",
    ]
    assert [item.entity.text for item in discarded] == ["Cook", "the Trump"]


def test_suppress_document_person_surname_false_positive_for_preposition_led_person_fragment() -> None:
    text = (
        "Doug Ford defended the purchase. "
        "Travel conducted by Ford across Canada continued through the weekend."
    )
    person = _candidate(
        text="Doug Ford",
        label="person",
        start=text.index("Doug Ford"),
        end=text.index("Doug Ford") + len("Doug Ford"),
        relevance=0.88,
        lane="deterministic_alias",
        canonical_text="Doug Ford",
        entity_id="wikidata:Q1625904",
    )
    fragment_start = text.index("by Ford")
    wrong_person = _candidate(
        text="by Ford",
        label="person",
        start=fragment_start,
        end=fragment_start + len("by Ford"),
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Byington Ford",
        entity_id="wikidata:Q5004096",
    )

    kept, discarded = _suppress_document_person_surname_false_positives(
        [person, wrong_person],
        text=text,
        pack_id="general-en",
    )

    assert [item.entity.text for item in kept] == ["Doug Ford"]
    assert [item.entity.text for item in discarded] == ["by Ford"]


def test_suppress_document_person_surname_false_positive_for_governing_body_context() -> None:
    text = (
        "Premier Bill Davis came under fire for the purchase. "
        "The Davis government later sold the plane."
    )
    person = _candidate(
        text="Bill Davis",
        label="person",
        start=text.index("Bill Davis"),
        end=text.index("Bill Davis") + len("Bill Davis"),
        relevance=0.88,
        lane="deterministic_alias",
        canonical_text="Bill Davis",
        entity_id="wikidata:Q106371509",
    )
    davis_start = text.index("Davis government")
    location = _candidate(
        text="Davis",
        label="location",
        start=davis_start,
        end=davis_start + len("Davis"),
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Davis",
        entity_id="wikidata:Q1177926",
    )

    kept, discarded = _suppress_document_person_surname_false_positives(
        [person, location],
        text=text,
        pack_id="general-en",
    )

    assert [item.entity.text for item in kept] == ["Bill Davis"]
    assert [item.entity.text for item in discarded] == ["Davis"]


def test_suppress_document_person_surname_false_positive_for_office_title_context() -> None:
    text = "Doug Ford spoke earlier. Premier Ford defended the plan."
    person = _candidate(
        text="Doug Ford",
        label="person",
        start=text.index("Doug Ford"),
        end=text.index("Doug Ford") + len("Doug Ford"),
        relevance=0.88,
        lane="deterministic_alias",
        canonical_text="Doug Ford",
        entity_id="wikidata:Q1625904",
    )
    ford_start = text.index("Ford defended")
    location = _candidate(
        text="Ford",
        label="location",
        start=ford_start,
        end=ford_start + len("Ford"),
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Ford",
        entity_id="wikidata:Q587761",
    )

    kept, discarded = _suppress_document_person_surname_false_positives(
        [person, location],
        text=text,
        pack_id="general-en",
    )

    assert [item.entity.text for item in kept] == ["Doug Ford"]
    assert [item.entity.text for item in discarded] == ["Ford"]


def test_runtime_possessive_alias_artifact_is_dropped() -> None:
    candidate = _candidate(
        text="Department's",
        label="organization",
        start=0,
        end=12,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Department S",
        entity_id="wikidata:Q689360",
    )
    candidate.entity.provenance.match_source = "Department S"
    candidate.entity.provenance.source_pack = "general-en"

    assert _is_runtime_possessive_alias_artifact(candidate) is True


def test_runtime_article_led_generic_org_alias_is_dropped() -> None:
    candidate = _candidate(
        text="the Board",
        label="organization",
        start=0,
        end=9,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Associated Board of the Royal Schools of Music",
        entity_id="wikidata:Q579517",
    )
    candidate.entity.provenance.match_source = "The Board"
    candidate.entity.provenance.source_pack = "general-en"

    assert _is_runtime_article_led_generic_org_alias(candidate) is True


def test_runtime_article_led_generic_location_alias_is_dropped() -> None:
    candidate = _candidate(
        text="the Public",
        label="location",
        start=0,
        end=10,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Public",
        entity_id="wikidata:Q1",
    )
    candidate.entity.provenance.match_source = "The Public"
    candidate.entity.provenance.source_pack = "general-en"

    assert _is_runtime_article_led_generic_org_alias(candidate) is True


def test_runtime_article_led_generic_art_alias_is_dropped() -> None:
    candidate = _candidate(
        text="the Art",
        label="organization",
        start=0,
        end=7,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="VOF de Kunst",
        entity_id="wikidata:Q1666295",
    )
    candidate.entity.provenance.match_source = "The Art"
    candidate.entity.provenance.source_pack = "general-en"

    assert _is_runtime_article_led_generic_org_alias(candidate) is True


def test_runtime_generic_single_token_alias_is_dropped() -> None:
    candidate = _candidate(
        text="Brits",
        label="location",
        start=0,
        end=5,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Brits",
        entity_id="wikidata:Q1",
    )
    candidate.entity.provenance.source_pack = "general-en"

    assert _is_runtime_generic_single_token_alias(candidate) is True


def test_runtime_generic_many_single_token_alias_is_dropped() -> None:
    candidate = _candidate(
        text="Many",
        label="location",
        start=0,
        end=4,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Many",
        entity_id="wikidata:Q21827",
        source_pack="general-en",
    )

    assert _is_runtime_generic_single_token_alias(candidate) is True


def test_runtime_generic_org_acronym_alias_is_dropped() -> None:
    candidate = _candidate(
        text="CT",
        label="organization",
        start=0,
        end=2,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="CT",
        entity_id="wikidata:Q1",
        source_pack="general-en",
    )

    assert _is_runtime_generic_single_token_alias(candidate) is True


def test_runtime_generic_not_org_acronym_alias_is_dropped() -> None:
    candidate = _candidate(
        text="NOT",
        label="organization",
        start=0,
        end=3,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Nič od tega",
        entity_id="wikidata:Q125112455",
        source_pack="general-en",
    )
    candidate.entity.provenance.match_source = "NOT"

    assert _is_runtime_generic_single_token_alias(candidate) is True


def test_runtime_generic_uk_org_acronym_alias_is_dropped() -> None:
    candidate = _candidate(
        text="UK",
        label="organization",
        start=0,
        end=2,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="UK",
        entity_id="wikidata:Q1",
        source_pack="general-en",
    )

    assert _is_runtime_generic_single_token_alias(candidate) is True


def test_runtime_contextual_org_acronym_noise_is_dropped() -> None:
    candidate = _candidate(
        text="US",
        label="organization",
        start=4,
        end=6,
        relevance=0.61,
        lane="contextual_org_acronym_backfill",
        canonical_text="US",
        entity_id=None,
        match_path="heuristic.contextual_acronym",
        source_pack="general-en",
    )

    assert _is_runtime_contextual_org_acronym_noise(candidate) is True


def test_runtime_contextual_org_title_acronym_noise_is_dropped() -> None:
    candidate = _candidate(
        text="PM",
        label="organization",
        start=4,
        end=6,
        relevance=0.61,
        lane="contextual_org_acronym_backfill",
        canonical_text="PM",
        entity_id=None,
        match_path="heuristic.contextual_acronym",
        source_pack="general-en",
    )

    assert _is_runtime_contextual_org_acronym_noise(candidate) is True


def test_runtime_reporting_attribution_contextual_org_acronym_noise_is_dropped() -> None:
    text = "She told BBC Radio 4 that the programme was unfair."
    candidate = _candidate(
        text="BBC",
        label="organization",
        start=text.index("BBC"),
        end=text.index("BBC") + len("BBC"),
        relevance=0.61,
        lane="contextual_org_acronym_backfill",
        canonical_text="BBC",
        entity_id=None,
        match_path="heuristic.contextual_acronym",
        source_pack="general-en",
    )

    assert (
        _is_runtime_contextual_org_acronym_noise(
            candidate,
            text=text,
        )
        is True
    )


def test_runtime_reporting_attribution_contextual_org_acronym_noise_is_dropped_for_that_clause() -> None:
    text = "He told CNBC that the market reaction was immediate."
    candidate = _candidate(
        text="CNBC",
        label="organization",
        start=text.index("CNBC"),
        end=text.index("CNBC") + len("CNBC"),
        relevance=0.61,
        lane="contextual_org_acronym_backfill",
        canonical_text="CNBC",
        entity_id=None,
        match_path="heuristic.contextual_acronym",
        source_pack="general-en",
    )

    assert (
        _is_runtime_contextual_org_acronym_noise(
            candidate,
            text=text,
        )
        is True
    )


def test_runtime_generic_phrase_fragment_alias_is_dropped() -> None:
    candidate = _candidate(
        text="More than",
        label="organization",
        start=0,
        end=9,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="More Than",
        entity_id="wikidata:Q1",
    )
    candidate.entity.provenance.source_pack = "general-en"

    assert _is_runtime_generic_phrase_fragment_alias(candidate) is True


def test_runtime_as_ever_phrase_alias_is_dropped() -> None:
    candidate = _candidate(
        text="As ever",
        label="organization",
        start=0,
        end=7,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="As Ever",
        entity_id="wikidata:Q127104762",
        source_pack="general-en",
    )

    assert _is_runtime_generic_phrase_fragment_alias(candidate) is True


def test_runtime_structural_geo_feature_label_mismatch_is_dropped() -> None:
    candidate = _candidate(
        text="Atlantic Ocean",
        label="organization",
        start=0,
        end=14,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Atlantic Ocean",
        entity_id="wikidata:Q31956345",
        source_pack="general-en",
    )

    assert _is_runtime_structural_geo_feature_label_mismatch(candidate) is True


def test_runtime_structural_geo_feature_label_mismatch_keeps_explicit_org_lead() -> None:
    candidate = _candidate(
        text="Org Delta",
        label="organization",
        start=0,
        end=9,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Org Delta",
        entity_id="wikidata:Q5",
        source_pack="general-en",
    )

    assert _is_runtime_structural_geo_feature_label_mismatch(candidate) is False


def test_runtime_possessive_titleish_alias_artifact_is_dropped() -> None:
    candidate = _candidate(
        text="King's",
        label="organization",
        start=0,
        end=6,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="King's Company",
        entity_id="wikidata:Q2197437",
        source_pack="general-en",
    )

    assert _is_runtime_possessive_titleish_alias_artifact(candidate) is True


def test_runtime_office_title_person_alias_artifact_is_dropped() -> None:
    candidate = _candidate(
        text="Defense Minister",
        label="person",
        start=0,
        end=16,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Jerry Codiñera",
        entity_id="wikidata:Q8853429",
        source_pack="general-en",
    )

    assert _is_runtime_office_title_person_alias_artifact(candidate) is True


def test_runtime_single_title_real_person_alias_is_kept() -> None:
    candidate = _candidate(
        text="President Donald Trump",
        label="person",
        start=0,
        end=22,
        relevance=0.91,
        lane="deterministic_alias",
        canonical_text="Donald Trump",
        entity_id="wikidata:Q22686",
        source_pack="general-en",
    )

    assert _is_runtime_title_led_person_alias_artifact(candidate) is False


def test_runtime_central_command_phrase_alias_is_dropped() -> None:
    candidate = _candidate(
        text="Central Command",
        label="location",
        start=0,
        end=15,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Central Command",
        entity_id="wikidata:Q1",
        source_pack="general-en",
    )

    assert _is_runtime_generic_phrase_fragment_alias(candidate) is True


def test_runtime_new_deal_phrase_alias_is_dropped() -> None:
    candidate = _candidate(
        text="new deal",
        label="location",
        start=0,
        end=8,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="New Deal",
        entity_id="wikidata:Q1156399",
        source_pack="general-en",
    )

    assert _is_runtime_generic_phrase_fragment_alias(candidate) is True


def test_runtime_u_turn_phrase_alias_is_dropped() -> None:
    candidate = _candidate(
        text="U-turn",
        label="organization",
        start=0,
        end=6,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="U-Turn",
        entity_id="wikidata:Q1562323",
        source_pack="general-en",
    )

    assert _is_runtime_generic_phrase_fragment_alias(candidate) is True


def test_runtime_date_like_alias_is_dropped() -> None:
    candidate = _candidate(
        text="January 6, 2021",
        label="organization",
        start=0,
        end=15,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="January 6, 2021",
        entity_id="wikidata:Q1",
    )
    candidate.entity.provenance.source_pack = "general-en"

    assert _is_runtime_date_like_alias(candidate) is True


def test_runtime_function_year_alias_is_dropped() -> None:
    candidate = _candidate(
        text="and 2016",
        label="organization",
        start=0,
        end=8,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Androni Giocattoli-Sidermec 2016",
        entity_id="wikidata:Q21979627",
        source_pack="general-en",
    )
    candidate.entity.provenance.match_source = "And 2016"

    assert _is_runtime_function_year_alias(candidate) is True


def test_runtime_leading_apostrophe_fragment_alias_is_dropped() -> None:
    candidate = _candidate(
        text="s Jacobs",
        label="person",
        start=0,
        end=8,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="S Jacobs",
        entity_id="wikidata:Q1",
    )
    candidate.entity.provenance.source_pack = "general-en"

    assert _is_runtime_leading_apostrophe_fragment_alias(candidate) is True


def test_runtime_truncated_connector_alias_artifact_is_dropped() -> None:
    candidate = _candidate(
        text="O'Connor",
        label="organization",
        start=0,
        end=8,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="O'Connor & Associates",
        entity_id="wikidata:Q1",
        match_source="O'Connor &",
        source_pack="general-en",
    )

    assert _is_runtime_truncated_connector_alias_artifact(candidate) is True


def test_runtime_single_token_titleish_fragment_alias_is_dropped() -> None:
    text = "Don Cheadle returned to the stage."
    candidate = _candidate(
        text="Cheadle",
        label="location",
        start=text.index("Cheadle"),
        end=text.index("Cheadle") + len("Cheadle"),
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Cheadle",
        entity_id="wikidata:Q1",
    )
    candidate.entity.provenance.source_pack = "general-en"

    assert _is_runtime_single_token_titleish_fragment_alias(candidate, text=text) is True


def test_runtime_partial_person_fragment_alias_is_dropped() -> None:
    candidate = _candidate(
        text="Tamim bin Hamad Al",
        label="person",
        start=0,
        end=18,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text="Tamim bin Hamad Al Thani",
        entity_id="wikidata:Q1",
    )
    candidate.entity.provenance.source_pack = "general-en"

    assert _is_runtime_partial_person_fragment_alias(candidate) is True


def test_runtime_quoted_fragment_alias_is_dropped() -> None:
    candidate = _candidate(
        text='the "Cloud',
        label="location",
        start=0,
        end=10,
        relevance=0.61,
        lane="deterministic_alias",
        canonical_text='The "Cloud',
        entity_id="wikidata:Q1",
    )
    candidate.entity.provenance.source_pack = "general-en"

    assert _is_runtime_quoted_fragment_alias(candidate) is True
