from ades.pipeline.tagger import (
    _should_skip_multi_token_lookup_candidate,
    _should_skip_single_token_lookup_candidate,
)


def test_skip_single_token_generic_location_head() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="University",
            candidate_value="University",
            canonical_text="University",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_single_token_demonstrative_location() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="This",
            candidate_value="This",
            canonical_text="This",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_single_token_calendar_location() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="March",
            candidate_value="March",
            canonical_text="March",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_single_token_number_word_location() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="Four",
            candidate_value="Four",
            canonical_text="Four",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_single_token_generic_stock_head() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="Stock",
            candidate_value="Stock",
            canonical_text="Stock",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_single_token_generic_congress_location() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="Congress",
            candidate_value="Congress",
            canonical_text="Congress",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_single_token_generic_many_location() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="Many",
            candidate_value="Many",
            canonical_text="Many",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_single_token_generic_linda_location() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="Linda",
            candidate_value="Linda",
            canonical_text="Linda",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_single_token_generic_not_org_acronym() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="NOT",
            candidate_value="NOT",
            canonical_text="Nič od tega",
            candidate_label="organization",
            candidate_domain="general",
        )
        is True
    )


def test_skip_single_token_generic_u_turn_alias() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="U-turn",
            candidate_value="U-Turn",
            canonical_text="U-Turn",
            candidate_label="organization",
            candidate_domain="general",
        )
        is True
    )


def test_keep_finance_multi_token_person_in_single_token_filter() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="Jane Doe",
            candidate_value="Jane Doe",
            canonical_text="Jane Doe",
            candidate_label="person",
            candidate_domain="finance",
        )
        is False
    )


def test_skip_article_led_generic_person_phrase() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="The founder",
            candidate_value="The Founder",
            canonical_text="The Founder",
            candidate_label="person",
            candidate_domain="general",
        )
        is True
    )


def test_skip_article_led_generic_art_phrase() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="The Art",
            candidate_value="The Art",
            canonical_text="VOF de Kunst",
            candidate_label="organization",
            candidate_domain="general",
        )
        is True
    )


def test_skip_generic_new_deal_phrase() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="new deal",
            candidate_value="New Deal",
            canonical_text="New Deal",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_lowercase_article_led_generic_location_head() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="the Mall",
            candidate_value="The Mall",
            canonical_text="The Mall",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_lowercase_article_led_generic_house_location_head() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="the House",
            candidate_value="The House",
            canonical_text="The House",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_lowercase_generic_place_location_phrase() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="no place",
            candidate_value="No Place",
            canonical_text="No Place",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_keep_title_cased_article_led_non_generic_location() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="The Hague",
            candidate_value="The Hague",
            canonical_text="The Hague",
            candidate_label="location",
            candidate_domain="general",
        )
        is False
    )


def test_skip_lowercase_article_dropped_long_location_fragment() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="highest court in the land",
            candidate_value="The Highest Court in the Land",
            canonical_text="The Highest Court in the Land",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_keep_title_cased_long_location_name_with_function_word() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="Republic of Korea",
            candidate_value="The Republic of Korea",
            canonical_text="The Republic of Korea",
            candidate_label="location",
            candidate_domain="general",
        )
        is False
    )


def test_skip_structural_org_shaped_location_phrase() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="Supreme Court",
            candidate_value="Supreme Court",
            canonical_text="Supreme Court",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_keep_structural_geographic_location_phrase() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="Bay of Bengal",
            candidate_value="Bay of Bengal",
            canonical_text="Bay of Bengal",
            candidate_label="location",
            candidate_domain="general",
        )
        is False
    )


def test_skip_function_year_alias_phrase() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="and 2016",
            candidate_value="And 2016",
            canonical_text="Androni Giocattoli-Sidermec 2016",
            candidate_label="organization",
            candidate_domain="general",
        )
        is True
    )


def test_skip_malformed_person_appositive_phrase() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="Yang, an",
            candidate_value="Yang An",
            canonical_text="Yang An",
            candidate_label="person",
            candidate_domain="general",
        )
        is True
    )


def test_skip_malformed_location_appositive_phrase() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="Yang, an",
            candidate_value="Yang An",
            canonical_text="Yang'an",
            candidate_label="location",
            candidate_domain="general",
        )
        is True
    )


def test_skip_auxiliary_led_generic_org_phrase() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="We are",
            candidate_value="We Are",
            canonical_text="We Are",
            candidate_label="organization",
            candidate_domain="general",
        )
        is True
    )


def test_skip_dangling_org_fragment_with_trailing_function_word() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="University of",
            candidate_value="University Of",
            canonical_text="University Of",
            candidate_label="organization",
            candidate_domain="general",
        )
        is True
    )


def test_skip_sentence_initial_plural_person_noise() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="Banks",
            candidate_value="Banks",
            canonical_text="Banks",
            candidate_label="person",
            candidate_domain="general",
            segment_text="Banks have also hit back at the ruling.",
            start=0,
            end=5,
        )
        is True
    )


def test_skip_single_token_member_list_location_alias() -> None:
    text = (
        "The Finance and Leasing Association counts Santander and the financial "
        "arms of BMW and Volkswagen among its members."
    )
    start = text.index("Santander")
    end = start + len("Santander")
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="Santander",
            candidate_value="Santander",
            canonical_text="Santander",
            candidate_label="location",
            candidate_domain="general",
            segment_text=text,
            start=start,
            end=end,
        )
        is True
    )


def test_keep_single_token_location_in_plain_geographic_coordination() -> None:
    text = "Flights between Santander and Bilbao were canceled by the storm."
    start = text.index("Santander")
    end = start + len("Santander")
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="Santander",
            candidate_value="Santander",
            canonical_text="Santander",
            candidate_label="location",
            candidate_domain="general",
            segment_text=text,
            start=start,
            end=end,
        )
        is False
    )


def test_keep_sentence_initial_surname_with_non_plural_following_verb() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="Brooks",
            candidate_value="Brooks",
            canonical_text="Brooks",
            candidate_label="person",
            candidate_domain="general",
            segment_text="Brooks said the company would respond.",
            start=0,
            end=6,
        )
        is False
    )


def test_skip_numeric_general_single_token_candidate() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="100",
            candidate_value="100",
            canonical_text="100%",
            candidate_label="organization",
            candidate_domain="general",
        )
        is True
    )


def test_keep_exact_all_caps_expansion_acronym_candidate() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="IEA",
            candidate_value="IEA",
            canonical_text="International Energy Agency",
            candidate_label="organization",
            candidate_domain="general",
            segment_text="The IEA said oil markets would respond.",
            start=4,
            end=7,
        )
        is False
    )


def test_skip_all_caps_section_heading_singletons() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="NOTES",
            candidate_value="NOTES",
            canonical_text="NOTES",
            candidate_label="organization",
            candidate_domain="general",
            segment_text="NOTES: Pittsburgh closed the period well.",
            start=0,
            end=5,
        )
        is True
    )
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="UP",
            candidate_value="UP",
            canonical_text="UP",
            candidate_label="location",
            candidate_domain="general",
            segment_text="UP NEXT Lightning: At the Los Angeles Kings on Thursday night.",
            start=0,
            end=2,
        )
        is True
    )
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="NEXT",
            candidate_value="NEXT",
            canonical_text="NEXT",
            candidate_label="organization",
            candidate_domain="general",
            segment_text="UP NEXT Lightning: At the Los Angeles Kings on Thursday night.",
            start=3,
            end=7,
        )
        is True
    )
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="NHL",
            candidate_value="NHL",
            canonical_text="National Hockey League",
            candidate_label="organization",
            candidate_domain="general",
            segment_text="The NHL season resumed in October.",
            start=4,
            end=7,
        )
        is False
    )


def test_keep_valid_titleled_exact_phrases() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="President Donald Trump",
            candidate_value="President Donald Trump",
            canonical_text="Donald Trump",
            candidate_label="person",
            candidate_domain="general",
        )
        is False
    )


def test_skip_generated_article_led_alias_with_divergent_canonical() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="the United Kingdom",
            candidate_value="The United Kingdom",
            canonical_text="Kingdom of Israel",
            candidate_label="location",
            candidate_domain="general",
            candidate_generated=True,
        )
        is True
    )


def test_keep_generated_article_led_alias_when_canonical_preserves_core() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="The New York Times",
            candidate_value="The New York Times",
            canonical_text="The New York Times Company",
            candidate_label="organization",
            candidate_domain="general",
            candidate_generated=True,
        )
        is False
    )


def test_skip_lowercase_article_led_finance_org_fragment() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="the payments",
            candidate_value="The Payments",
            canonical_text="The Payments Group Holding GmbH & Co KGaA",
            candidate_label="organization",
            candidate_domain="finance",
        )
        is True
    )


def test_keep_title_cased_finance_org_phrase() -> None:
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="The Hartford",
            candidate_value="The Hartford",
            canonical_text="The Hartford Financial Services Group, Inc.",
            candidate_label="organization",
            candidate_domain="finance",
        )
        is False
    )


def test_skip_single_token_org_fragment_inside_person_name() -> None:
    assert (
        _should_skip_single_token_lookup_candidate(
            matched_text="James",
            candidate_value="James",
            canonical_text="James",
            candidate_label="organization",
            candidate_domain="general",
            segment_text="Lindsay James said markets were volatile.",
            start=8,
            end=13,
        )
        is True
    )
    assert (
        _should_skip_multi_token_lookup_candidate(
            matched_text="The New York Times",
            candidate_value="The New York Times",
            canonical_text="The New York Times Company",
            candidate_label="organization",
            candidate_domain="general",
        )
        is False
    )
