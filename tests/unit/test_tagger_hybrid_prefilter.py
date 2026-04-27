from types import SimpleNamespace

from ades.pipeline.hybrid import ProposalSpan
from ades.pipeline.tagger import (
    _can_emit_generated_hybrid_proposal,
    _generated_location_has_single_token_org_context,
    _is_in_pack_hybrid_link_candidate_compatible,
    _is_plausible_heuristic_structured_organization_candidate,
    _normalize_hybrid_proposal,
    _should_skip_multi_token_lookup_candidate,
    _should_skip_single_token_lookup_candidate,
    _should_skip_hybrid_lookup_proposal,
)


def _proposal(text: str, *, label: str) -> ProposalSpan:
    return ProposalSpan(
        start=0,
        end=len(text),
        text=text,
        label=label,
        confidence=0.82,
        model_name="stub",
        model_version="test",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_reporting_context() -> None:
    proposal = _proposal("WPI Strategy", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="WPI Strategy said inflation expectations rose sharply.",
    )


def test_finance_single_token_ticker_lookup_keeps_repeated_issuer_surface() -> None:
    assert not _should_skip_single_token_lookup_candidate(
        matched_text="Tesla",
        candidate_value="Tesla",
        canonical_text="TSLA",
        candidate_label="ticker",
        candidate_domain="finance",
        segment_text="Tesla reported earnings. Tesla shares rose after the call.",
        start=0,
        end=len("Tesla"),
    )


def test_finance_single_token_ticker_lookup_skips_common_word_fragment() -> None:
    assert _should_skip_single_token_lookup_candidate(
        matched_text="peers",
        candidate_value="peers",
        canonical_text="7066",
        candidate_label="ticker",
        candidate_domain="finance",
        segment_text="Tesla's stock has underperformed all of its megacap peers so far this year.",
        start=57,
        end=62,
    )


def test_finance_single_token_ticker_lookup_skips_org_prefix_surface() -> None:
    assert _should_skip_single_token_lookup_candidate(
        matched_text="Spirit",
        candidate_value="Spirit",
        canonical_text="FLYYQ",
        candidate_label="ticker",
        candidate_domain="finance",
        segment_text="Spirit Airlines warned the market after the carrier updated guidance.",
        start=0,
        end=len("Spirit"),
    )


def test_finance_multi_token_lookup_skips_person_alias_noise() -> None:
    assert _should_skip_multi_token_lookup_candidate(
        matched_text="Puerto Rico",
        candidate_value="Puerto Rico",
        canonical_text="puerto-rico",
        candidate_label="person",
        candidate_domain="finance",
    )


def test_finance_multi_token_lookup_keeps_person_name() -> None:
    assert not _should_skip_multi_token_lookup_candidate(
        matched_text="Jane Doe",
        candidate_value="Jane Doe",
        canonical_text="Jane Doe",
        candidate_label="person",
        candidate_domain="finance",
    )


def test_finance_multi_token_lookup_skips_ticker_alias_phrase() -> None:
    assert _should_skip_multi_token_lookup_candidate(
        matched_text="Bank of America",
        candidate_value="Bank of America",
        canonical_text="BAC-PB",
        candidate_label="ticker",
        candidate_domain="finance",
    )


def test_finance_multi_token_lookup_keeps_single_token_ticker() -> None:
    assert not _should_skip_multi_token_lookup_candidate(
        matched_text="TICKA",
        candidate_value="TICKA",
        canonical_text="TICKA",
        candidate_label="ticker",
        candidate_domain="finance",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_admin_tail() -> None:
    proposal = _proposal("Henrico Counties", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Henrico Counties voted heavily in the primary.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_political_group_tail() -> None:
    proposal = _proposal("Texas Republicans", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Trump last summer began urging Texas Republicans to redraw the districts.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_agency_tail() -> None:
    proposal = _proposal("Border Patrol", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Border Patrol said agents made several arrests at the crossing.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_participle_head() -> None:
    proposal = _proposal("Blockading Iranian", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Blockading Iranian ports is an act of war and a violation of the ceasefire.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_county_neighbor() -> None:
    proposal = _proposal("Prince William", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Prince William counties shifted sharply toward Democrats.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_trailing_admin_list() -> None:
    segment_text = (
        "Arlington, Fairfax, Loudoun, Prince William and Henrico Counties backed the referendum."
    )
    start = segment_text.index("Prince William")
    proposal = ProposalSpan(
        start=start,
        end=start + len("Prince William"),
        text="Prince William",
        label="person",
        confidence=0.82,
        model_name="stub",
        model_version="test",
    )

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value=segment_text,
    )


def test_hybrid_lookup_prefilter_skips_location_noise_with_office_title_prefix() -> None:
    proposal = _proposal("MP for Hackney North", label="location")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="MP for Hackney North criticised the spending plans.",
    )


def test_hybrid_lookup_prefilter_skips_location_noise_with_uppercase_teaser_phrase() -> None:
    proposal = _proposal("KEITH KELLOGG URGES US", label="location")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="KEITH KELLOGG URGES US TO FINISH THE JOB AGAINST IRAN BY SEIZING ISLANDS.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_ui_boilerplate_tokens() -> None:
    proposal = _proposal("CLICKING HERE Transportation", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="GET FOX BUSINESS ON THE GO BY CLICKING HERE Transportation Secretary Sean Duffy spoke after the meeting.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_admin_tail() -> None:
    proposal = _proposal("Trump Admin", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="The Trump Admin said new rules would take effect next month.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_fraud_tail() -> None:
    proposal = _proposal("Eliminate Fraud", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="The task force to Eliminate Fraud met with agency officials after the briefing.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_historical_tail() -> None:
    proposal = _proposal("Magna Carta", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Pupils will learn about Magna Carta and the Glorious Revolution in the new curriculum.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_location_area_context() -> None:
    proposal = _proposal("Los Angeles", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Several providers in the Los Angeles area were suspended after the audit.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_concessive_phrase_head() -> None:
    proposal = _proposal("Though FOMC", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Though FOMC members held rates steady, investors still expected cuts later this year.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_political_affiliation_tail() -> None:
    proposal = _proposal("Massachusetts Democrat", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Massachusetts Democrat Elizabeth Warren demanded answers from the administration.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_controlled_subtoken() -> None:
    proposal = _proposal("Democrat-controlled Virginia", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Democrat-controlled Virginia districts gave party leaders new leverage in the fight.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_ratings_tail() -> None:
    proposal = _proposal("Fitch Ratings", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Fitch Ratings warned that the downgrade could arrive later this year.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_securities_tail() -> None:
    proposal = _proposal("TD Securities", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="TD Securities analysts said the reserve release had clouded the quarter.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_minority_tail() -> None:
    proposal = _proposal("Virginia House Minority", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Virginia House Minority leaders accused the governor of stalling the plan.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_online_tail() -> None:
    proposal = _proposal("Mizan Online", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Mizan Online reported that the appeal had been rejected by the court.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_subscription_tail() -> None:
    proposal = _proposal("Digital Subscription", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Did you know with a Digital Subscription to Belfast News Letter, you can get unlimited access to the website.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_location_tail() -> None:
    proposal = _proposal("Pall Mall", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Pall Mall conversations shaped the strategy meetings around Westminster.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_geographic_feature_tail() -> None:
    proposal = _proposal("Silicon Valley", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Silicon Valley donors were again at the center of the White House event.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_prisoner_tail() -> None:
    proposal = _proposal("FREED IRANIAN PRISONER", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="FREED IRANIAN PRISONER returns home after months in detention, the statement said.",
    )


def test_hybrid_lookup_prefilter_skips_org_noise_with_app_head() -> None:
    proposal = _proposal("APP Fox News Digital", label="organization")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="APP Fox News Digital shared the clip inside the mobile preview feed.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_acronym_tail() -> None:
    proposal = _proposal("Sovereign AI", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Ministers said Sovereign AI would be a cornerstone of the new strategy.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_acronym_led_business_context() -> None:
    proposal = _proposal("SK Hynix", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="South Korean memory chip giant SK Hynix posted record profit and revenue as demand for AI memory stayed strong.",
    )


def test_hybrid_lookup_prefilter_skips_person_noise_with_common_modifier_title_stack() -> None:
    proposal = _proposal("Saudi Crown Prince Mohammed", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Saudi Crown Prince Mohammed joined the private dinner after the summit.",
    )


def test_hybrid_lookup_prefilter_keeps_valid_structural_location_span() -> None:
    proposal = _proposal("Strait of Hormuz", label="location")

    assert not _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="A Strait of Hormuz closure would hit tanker traffic and oil markets.",
    )


def test_normalize_hybrid_person_proposal_trims_trailing_calendar_context() -> None:
    proposal = _proposal("Ketanji Brown Jackson Wednesday", label="person")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="Justice Ketanji Brown Jackson Wednesday criticized the ruling.",
    )

    assert normalized is not None
    assert normalized.text == "Ketanji Brown Jackson"
    assert normalized.label == "person"


def test_normalize_hybrid_person_proposal_trims_pm_title_prefix() -> None:
    proposal = _proposal("PM Rishi Sunak", label="person")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="Former PM Rishi Sunak has told the BBC that taxes should fall.",
    )

    assert normalized is not None
    assert normalized.text == "Rishi Sunak"
    assert normalized.label == "person"


def test_normalize_hybrid_person_proposal_trims_cfo_title_prefix() -> None:
    proposal = _proposal("CFO Vaibhav Taneja", label="person")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="Tesla CFO Vaibhav Taneja said margins improved during the quarter.",
    )

    assert normalized is not None
    assert normalized.text == "Vaibhav Taneja"
    assert normalized.label == "person"


def test_normalize_hybrid_person_proposal_trims_branch_title_prefix_pair() -> None:
    proposal = _proposal("Navy Undersecretary Hung Cao", label="person")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="Navy Undersecretary Hung Cao was quoted after the Pentagon briefing.",
    )

    assert normalized is not None
    assert normalized.text == "Hung Cao"
    assert normalized.label == "person"


def test_normalize_hybrid_person_proposal_trims_leading_state_party_modifier_pair() -> None:
    proposal = _proposal("Minnesota Democrat Matt Klein", label="person")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value=(
            "The sanctioned candidates included Minnesota Democrat Matt Klein, "
            "who is running in the primary."
        ),
    )

    assert normalized is not None
    assert normalized.text == "Matt Klein"
    assert normalized.label == "person"


def test_normalize_hybrid_org_proposal_trims_role_name_tail() -> None:
    proposal = _proposal("Federal Reserve Governor Christopher Waller", label="organization")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="Federal Reserve Governor Christopher Waller said rates may stay on hold.",
    )

    assert normalized is not None
    assert normalized.text == "Federal Reserve"
    assert normalized.label == "organization"


def test_normalize_hybrid_person_proposal_relabels_org_surface() -> None:
    proposal = _proposal("World Liberty Financial", label="person")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="World Liberty Financial froze the tokens after the dispute.",
    )

    assert normalized is not None
    assert normalized.text == "World Liberty Financial"
    assert normalized.label == "organization"


def test_normalize_hybrid_person_proposal_relabels_club_surface() -> None:
    proposal = _proposal("Sierra Club", label="person")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="Sierra Club backed the lawsuit over the environmental rule.",
    )

    assert normalized is not None
    assert normalized.text == "Sierra Club"
    assert normalized.label == "organization"


def test_normalize_hybrid_person_proposal_relabels_broadcasting_corp_surface() -> None:
    proposal = _proposal("Canadian Broadcasting Corp", label="person")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="Canadian Broadcasting Corp reported the comments shortly after the meeting.",
    )

    assert normalized is not None
    assert normalized.text == "Canadian Broadcasting Corp"
    assert normalized.label == "organization"


def test_normalize_hybrid_person_proposal_relabels_parliament_surface() -> None:
    proposal = _proposal("Scottish Parliament", label="person")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="Scottish Parliament elections use a mixed voting system.",
    )

    assert normalized is not None
    assert normalized.text == "Scottish Parliament"
    assert normalized.label == "organization"


def test_normalize_hybrid_person_proposal_relabels_party_surface() -> None:
    proposal = _proposal("Lib Dems", label="person")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="Lib Dems targeted the ward after the by-election upset.",
    )

    assert normalized is not None
    assert normalized.text == "Lib Dems"
    assert normalized.label == "organization"


def test_generated_single_token_org_context_skips_country_name() -> None:
    assert not _generated_location_has_single_token_org_context(
        ["Britain"],
        ["britain"],
        segment_text_value="Britain technology firms are racing to hire more engineers.",
        start=0,
        end=len("Britain"),
    )


def test_in_pack_hybrid_link_candidate_rejects_misaligned_surface_expansion() -> None:
    proposal = _proposal("LAS VEGAS", label="organization")

    assert not _is_in_pack_hybrid_link_candidate_compatible(
        proposal,
        {
            "value": "LAS VEGAS FROM HOME COM ENTERTAINMENT INC",
            "canonical_text": "Jackpot Digital Inc.",
            "label": "organization",
            "pack_id": "business-vector-en",
            "domain": "business",
        },
    )


def test_generated_org_proposal_rejects_all_caps_location_surface_without_org_cue() -> None:
    proposal = _proposal("LAS VEGAS", label="organization")

    assert not _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="LAS VEGAS airport traffic rebounded after the airline schedule cuts.",
        runtime=SimpleNamespace(pack_id="general-en", domain="general"),
    )


def test_heuristic_structured_org_rejects_european_network_phrase() -> None:
    assert not _is_plausible_heuristic_structured_organization_candidate("European network")


def test_normalize_hybrid_person_proposal_relabels_acronym_led_business_surface() -> None:
    proposal = _proposal("SK Hynix", label="person")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="South Korean memory chip giant SK Hynix posted record profit and revenue as demand for AI memory stayed strong.",
    )

    assert normalized is not None
    assert normalized.text == "SK Hynix"
    assert normalized.label == "organization"


def test_hybrid_lookup_prefilter_skips_person_noise_with_legislative_tail() -> None:
    proposal = _proposal("Club Deputy Legislative", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Club Deputy Legislative staff said the group opposed the measure.",
    )


def test_hybrid_lookup_prefilter_skips_org_noise_with_embedded_role_and_name_tail() -> None:
    proposal = _proposal("American Airlines CEO Robert Isom", label="organization")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="American Airlines CEO Robert Isom has come under scrutiny as the head of the carrier.",
    )


def test_hybrid_lookup_prefilter_skips_org_noise_with_news_analysis_fragment() -> None:
    proposal = _proposal("NEWS & ANALYSIS Billionaire", label="organization")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="NEWS & ANALYSIS Billionaire investors are piling into the trade again.",
    )


def test_hybrid_lookup_prefilter_skips_org_noise_with_action_tail() -> None:
    proposal = _proposal("VANGUARD FUND STRIPS OUT", label="organization")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="VANGUARD FUND STRIPS OUT China holdings as the selloff intensifies.",
    )


def test_hybrid_lookup_prefilter_keeps_valid_person_and_organization_spans() -> None:
    assert not _should_skip_hybrid_lookup_proposal(
        _proposal("Donald Trump", label="person"),
        segment_text_value="President Donald Trump said the tariffs would stay in place.",
    )
    assert not _should_skip_hybrid_lookup_proposal(
        _proposal("Bank of America", label="organization"),
        segment_text_value="Bank of America raised its inflation forecast.",
    )


def test_hybrid_lookup_prefilter_skips_weak_generic_organization_surface() -> None:
    proposal = _proposal("Financial News", label="organization")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Financial News dashboards showed another market decline.",
    )


def test_hybrid_lookup_prefilter_skips_birthright_citizenship_person_noise() -> None:
    proposal = _proposal("Birthright Citizenship", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Trump's order to end birthright citizenship drew new scrutiny.",
    )


def test_hybrid_lookup_prefilter_skips_hardworking_americans_person_noise() -> None:
    proposal = _proposal("Hardworking Americans", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Hardworking Americans deserve lower taxes, the campaign said.",
    )


def test_hybrid_lookup_prefilter_skips_citizenship_verification_person_noise() -> None:
    proposal = _proposal("Ensuring Citizenship Verification", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Ensuring Citizenship Verification remains central to the proposed bill.",
    )


def test_hybrid_lookup_prefilter_skips_adare_manor_person_noise() -> None:
    proposal = _proposal("Adare Manor", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="Adare Manor will host the tournament and the ministers discussed the plan.",
    )


def test_hybrid_lookup_prefilter_skips_uppercase_generic_org_phrase() -> None:
    proposal = _proposal("PRO-LIFE ORGANIZATION", label="organization")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="The PRO-LIFE ORGANIZATION welcomed the vote in a statement.",
        runtime=SimpleNamespace(pack_id="politics-vector-en", domain="politics"),
    ) is False


def test_generated_hybrid_person_rejects_acronym_led_news_program() -> None:
    proposal = _proposal("BBC Newsnight", label="person")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value='He told BBC Newsnight that the jobs tax should fall.',
        runtime=SimpleNamespace(pack_id="general-en", domain="economics"),
    ) is False


def test_generated_hybrid_org_rejects_single_token_demonym_company_context() -> None:
    proposal = _proposal("British", label="organization")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="The Reform UK leader is a shareholder in British bitcoin company Stack and appeared in a promotional video.",
        runtime=SimpleNamespace(pack_id="general-en", domain="business"),
    ) is False


def test_generated_hybrid_person_rejects_prefix_of_following_org_continuation() -> None:
    proposal = _proposal("World Liberty", label="person")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="The Trump family's World Liberty Financial crypto venture is being sued by one of its billionaire backers.",
        runtime=SimpleNamespace(pack_id="general-en", domain="business"),
    ) is False


def test_generated_hybrid_person_rejects_org_prefix_when_longer_org_occurs_elsewhere() -> None:
    segment_text = (
        "The Trump family's World Liberty Financial crypto venture is being sued by one of its "
        "billionaire backers. Justin Sun has accused World Liberty of an illegal scheme."
    )
    start = segment_text.rindex("World Liberty")
    proposal = ProposalSpan(
        start=start,
        end=start + len("World Liberty"),
        text="World Liberty",
        label="person",
        confidence=0.82,
        model_name="stub",
        model_version="test",
    )

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value=segment_text,
        runtime=SimpleNamespace(pack_id="general-en", domain="business"),
    ) is False


def test_generated_hybrid_person_rejects_location_apposition_context() -> None:
    proposal = _proposal("Puerto Rico", label="person")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="Flights between Fort Meyers, Florida, and San Juan, Puerto Rico, jumped sharply after the carrier left.",
        runtime=SimpleNamespace(pack_id="general-en", domain="business"),
    ) is False


def test_generated_hybrid_person_rejects_org_tail_surface() -> None:
    proposal = _proposal("Samsung Electronics", label="person")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="Samsung Electronics said in February that it had started shipping its most advanced chips.",
        runtime=SimpleNamespace(pack_id="general-en", domain="economics"),
    ) is False


def test_generated_hybrid_person_rejects_following_product_cue() -> None:
    proposal = _proposal("Vera Rubin", label="person")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="It is expected to be the main AI memory chip used in Nvidia's next-generation Vera Rubin architecture, designed for powerful AI workloads.",
        runtime=SimpleNamespace(pack_id="general-en", domain="economics"),
    ) is False


def test_generated_hybrid_org_rejects_conditional_phrase_head() -> None:
    proposal = _proposal("If AI", label="organization")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="If AI governance fails, boards will face harder choices next year.",
        runtime=SimpleNamespace(pack_id="general-en", domain="business"),
    ) is False


def test_generated_hybrid_org_rejects_bare_ai_suffix_phrase() -> None:
    proposal = _proposal("White House AI", label="organization")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="White House AI advisers met with executives after the summit.",
        runtime=SimpleNamespace(pack_id="general-en", domain="business"),
    ) is False


def test_generated_hybrid_person_rejects_plural_role_tail() -> None:
    proposal = _proposal("Joint Chiefs", label="person")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="Joint Chiefs officials briefed reporters after the meeting.",
        runtime=SimpleNamespace(pack_id="politics-vector-en", domain="politics"),
    ) is False


def test_generated_hybrid_person_allows_context_backed_common_surname() -> None:
    proposal = _proposal("Justin Sun", label="person")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="Cryptocurrency billionaire Justin Sun is suing the company in federal court.",
        runtime=SimpleNamespace(pack_id="general-en", domain="business"),
    )


def test_generated_hybrid_person_allows_appositive_clause_context() -> None:
    proposal = ProposalSpan(
        start=22,
        end=32,
        text="Mark Moran",
        label="person",
        confidence=0.65,
        model_name="stub",
        model_version="test",
    )

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="The sanctioned candidates were Mark Moran, who had been a candidate in Virginia.",
        runtime=SimpleNamespace(pack_id="politics-vector-en", domain="politics"),
    )


def test_generated_hybrid_org_allows_single_token_company_context() -> None:
    proposal = _proposal("Kalshi", label="organization")

    assert _can_emit_generated_hybrid_proposal(
        proposal,
        segment_text_value="Kalshi exchange rules prohibited the candidates from trading on their own races.",
        runtime=SimpleNamespace(pack_id="politics-vector-en", domain="politics"),
    )


def test_normalize_hybrid_location_proposal_relabels_single_token_company_context() -> None:
    proposal = _proposal("Kalshi", label="location")

    normalized = _normalize_hybrid_proposal(
        proposal,
        segment_text_value="Kalshi exchange rules prohibited the candidates from trading on their own races.",
    )

    assert normalized is not None
    assert normalized.text == "Kalshi"
    assert normalized.label == "organization"


def test_hybrid_lookup_prefilter_skips_possessive_surface_before_lookup() -> None:
    proposal = _proposal("New York Fed's", label="person")

    assert _should_skip_hybrid_lookup_proposal(
        proposal,
        segment_text_value="New York Fed's latest survey showed expectations moving higher.",
    )
