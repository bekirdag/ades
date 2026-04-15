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
