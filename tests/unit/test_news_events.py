from ades.news_events import (
    extract_news_event_signals,
    gate_terminal_candidates_by_event_signals,
)
from ades.service.models import (
    ImpactCandidate,
    ImpactPathEdge,
    ImpactRelationshipPath,
)


def test_extract_news_event_signals_returns_evidence_spans_and_asset_families() -> None:
    text = (
        "The central bank cut interest rates after inflation came in hotter than forecast. "
        "Traders expect another rate cut later this year. "
        "Oil supply shipments through the strait were disrupted after attacks. "
        "The company beat earnings estimates and raised guidance."
    )

    signals = extract_news_event_signals(text)
    by_type = {signal.event_type: signal for signal in signals}

    assert "policy_rate_cut" in by_type
    assert "inflation_shock" in by_type
    assert "future_policy_expectation" in by_type
    assert "supply_disruption" in by_type
    assert "earnings_beat" in by_type
    assert "guidance_raise" in by_type
    assert "rates" in by_type["policy_rate_cut"].compatible_asset_families
    assert "rates" in by_type["future_policy_expectation"].compatible_asset_families
    assert "commodity" in by_type["supply_disruption"].compatible_asset_families
    assert by_type["policy_rate_cut"].evidence_start is not None
    assert by_type["policy_rate_cut"].evidence_end is not None
    assert (
        text[by_type["policy_rate_cut"].evidence_start : by_type["policy_rate_cut"].evidence_end]
        .lower()
        .find("cut interest rates")
        >= 0
    )


def test_extract_news_event_signals_covers_policy_rate_hold_without_unchanged_wording() -> None:
    text = (
        "Banxico held its benchmark policy rate and said inflation risks remain "
        "elevated, pressuring the Mexican peso and local bond yields."
    )

    by_type = {signal.event_type: signal for signal in extract_news_event_signals(text)}

    assert "policy_rate_hold" in by_type
    assert "rates" in by_type["policy_rate_hold"].compatible_asset_families
    assert "currency" in by_type["policy_rate_hold"].compatible_asset_families


def test_extract_news_event_signals_covers_trade_ma_default_and_conflict() -> None:
    text = (
        "Officials announced new export controls and tariffs. "
        "The group sold a stake in its unit after a debt default. "
        "War escalation continued, but ceasefire talks later resumed."
    )

    by_type = {signal.event_type for signal in extract_news_event_signals(text)}

    assert "export_control" in by_type
    assert "tariff" in by_type
    assert "divestiture" in by_type
    assert "default" in by_type
    assert "war_escalation" in by_type
    assert "ceasefire_risk_relief" in by_type


def test_extract_news_event_signals_covers_sector_policy_and_regulatory_events() -> None:
    text = (
        "UK parliament passed legislation raising mining royalties for gold and copper miners. "
        "The competition regulator opened an antitrust probe into telecom operators."
    )

    by_type = {signal.event_type: signal for signal in extract_news_event_signals(text)}

    assert "sector_policy_change" in by_type
    assert "sector" in by_type["sector_policy_change"].compatible_asset_families
    assert "ticker" in by_type["sector_policy_change"].compatible_asset_families
    assert "commodity" in by_type["sector_policy_change"].compatible_asset_families
    assert "regulatory_enforcement" in by_type
    assert "sector" in by_type["regulatory_enforcement"].compatible_asset_families
    assert "equity" in by_type["regulatory_enforcement"].compatible_asset_families


def test_extract_news_event_signals_covers_sector_class_action_without_governance() -> None:
    text = (
        "Housebuilders' shares tumbled after a GBP4bn class action lawsuit "
        "over price conduct claims emerged."
    )

    by_type = {signal.event_type: signal for signal in extract_news_event_signals(text)}

    assert "regulatory_enforcement" in by_type
    assert "sector" in by_type["regulatory_enforcement"].compatible_asset_families
    assert "equity_index" in by_type["regulatory_enforcement"].compatible_asset_families
    assert "key_person_ownership_governance" not in by_type


def test_extract_news_event_signals_covers_takeover_approach_language() -> None:
    text = (
        "Intertek backed a GBP10.6bn takeover approach from Swedish private "
        "equity firm EQT after rebuffing earlier offers."
    )

    by_type = {signal.event_type for signal in extract_news_event_signals(text)}

    assert "acquisition" in by_type


def test_extract_news_event_signals_covers_key_person_ownership_governance() -> None:
    text = (
        "Sam Altman testified that Elon Musk sought a 90% ownership stake in "
        "OpenAI during a corporate governance trial."
    )

    by_type = {signal.event_type: signal for signal in extract_news_event_signals(text)}

    assert "key_person_ownership_governance" in by_type
    assert "equity" in by_type["key_person_ownership_governance"].compatible_asset_families
    assert "ticker" in by_type["key_person_ownership_governance"].compatible_asset_families


def test_extract_news_event_signals_covers_key_person_leadership_events() -> None:
    text = (
        "Australia-listed BTG confirmed the resignation of chief executive "
        "Boqing Zhang after a board governance dispute."
    )

    by_type = {signal.event_type: signal for signal in extract_news_event_signals(text)}

    assert "key_person_ownership_governance" in by_type
    assert "ticker" in by_type["key_person_ownership_governance"].compatible_asset_families


def test_extract_news_event_signals_covers_precious_metal_market_moves() -> None:
    text = "Gold slips as Middle East tensions keep inflation risks elevated."

    by_type = {signal.event_type: signal for signal in extract_news_event_signals(text)}

    assert "safe_haven_commodity_move" in by_type
    assert "commodity" in by_type["safe_haven_commodity_move"].compatible_asset_families
    assert "safe_haven" in by_type["safe_haven_commodity_move"].compatible_asset_families


def test_extract_news_event_signals_covers_weekly_market_coverage_batch() -> None:
    text = (
        "Coffee futures climbed after Brazilian crop forecasts tightened. "
        "USD/BRL rose as traders repriced Brazil rate expectations. "
        "A silver miner listed shares in an IPO on the local exchange."
    )

    by_type = {signal.event_type: signal for signal in extract_news_event_signals(text)}

    assert "commodity_price_move" in by_type
    assert "agriculture" in by_type["commodity_price_move"].compatible_asset_families
    assert "currency_market_move" in by_type
    assert "currency" in by_type["currency_market_move"].compatible_asset_families
    assert "equity_listing" in by_type
    assert "ticker" in by_type["equity_listing"].compatible_asset_families


def test_extract_news_event_signals_covers_chokepoint_flow_decline() -> None:
    text = "Oil flows through Strait of Hormuz fall nearly 30% in first quarter."

    by_type = {signal.event_type: signal for signal in extract_news_event_signals(text)}

    assert "shipping_chokepoint_disruption" in by_type
    assert "supply_disruption" in by_type
    assert "commodity" in by_type["shipping_chokepoint_disruption"].compatible_asset_families
    assert "energy" in by_type["supply_disruption"].compatible_asset_families


def test_extract_news_event_signals_covers_central_bank_leadership_change() -> None:
    text = (
        "The Senate confirmed Kevin Warsh as Federal Reserve chair after "
        "President Trump backed the former Fed governor."
    )

    by_type = {signal.event_type: signal for signal in extract_news_event_signals(text)}

    assert "future_policy_expectation" in by_type
    assert "rates" in by_type["future_policy_expectation"].compatible_asset_families
    assert "currency" in by_type["future_policy_expectation"].compatible_asset_families


def test_gate_terminal_candidates_requires_event_signal_for_policy_rate_proxy() -> None:
    policy_proxy = ImpactCandidate(
        entity_ref="ades:impact:rate:tr-policy-rate",
        name="Turkey Policy Rate",
        entity_type="policy_rate",
        evidence_level="shallow",
        confidence=0.82,
        source_entity_refs=["country:tr"],
        relationship_paths=[
            ImpactRelationshipPath(
                path_depth=1,
                edges=[
                    ImpactPathEdge(
                        source_ref="country:tr",
                        target_ref="ades:impact:rate:tr-policy-rate",
                        relation="country_affects_policy_rate_proxy",
                        evidence_level="direct",
                        confidence=0.86,
                        direction_hint="contextual",
                        source_name="test",
                        source_url="https://example.com",
                        source_snapshot="test",
                    )
                ],
            )
        ],
    )

    without_signal, warnings = gate_terminal_candidates_by_event_signals([policy_proxy], [])

    assert without_signal == []
    assert warnings == ["event_signal_gate_filtered:missing_policy_event_signal:1"]

    signals = extract_news_event_signals(
        "The central bank cut interest rates and said more easing could follow."
    )
    with_signal, warnings = gate_terminal_candidates_by_event_signals(
        [policy_proxy],
        signals,
    )

    assert warnings == []
    assert with_signal[0].entity_ref == policy_proxy.entity_ref
    assert with_signal[0].compatible_event_types == ["policy_rate_cut"]


def test_gate_terminal_candidates_rejects_unsupported_broad_proxies() -> None:
    broad_candidates = [
        ImpactCandidate(
            entity_ref="ades:impact:currency:dxy",
            name="US Dollar Index",
            entity_type="currency",
            evidence_level="shallow",
            confidence=0.61,
            source_entity_refs=["country:us"],
            relationship_paths=[],
        ),
        ImpactCandidate(
            entity_ref="ades:impact:equity-index:global-equity",
            name="Global Equity Proxy",
            entity_type="market_index",
            evidence_level="inferred",
            confidence=0.49,
            source_entity_refs=["country:us"],
            relationship_paths=[],
        ),
        ImpactCandidate(
            entity_ref="ades:impact:risk:us-country-risk",
            name="US Country Risk",
            entity_type="risk",
            evidence_level="inferred",
            confidence=0.42,
            source_entity_refs=["country:us"],
            relationship_paths=[],
        ),
    ]

    gated, warnings = gate_terminal_candidates_by_event_signals(broad_candidates, [])

    assert gated == []
    assert warnings == ["event_signal_gate_filtered:missing_broad_proxy_macro_event_signal:3"]


def test_gate_terminal_candidates_keeps_directly_mentioned_broad_market_asset() -> None:
    direct_dxy = ImpactCandidate(
        entity_ref="ades:impact:currency:dxy",
        name="US Dollar Index",
        entity_type="currency",
        evidence_level="direct",
        confidence=0.9,
        source_entity_refs=["ades:impact:currency:dxy"],
        relationship_paths=[],
    )

    gated, warnings = gate_terminal_candidates_by_event_signals([direct_dxy], [])

    assert warnings == []
    assert gated[0].entity_ref == "ades:impact:currency:dxy"


def test_gate_terminal_candidates_honors_relationship_edge_event_metadata() -> None:
    country_risk_proxy = ImpactCandidate(
        entity_ref="ades:impact:risk:us-country-risk",
        name="US Country Risk",
        entity_type="risk",
        evidence_level="inferred",
        confidence=0.42,
        source_entity_refs=["country:us"],
        relationship_paths=[
            ImpactRelationshipPath(
                path_depth=1,
                edges=[
                    ImpactPathEdge(
                        source_ref="country:us",
                        target_ref="ades:impact:risk:us-country-risk",
                        relation="country_affects_country_risk_proxy",
                        evidence_level="inferred",
                        confidence=0.52,
                        direction_hint="country_risk_proxy",
                        source_name="test",
                        source_url="https://example.com",
                        source_snapshot="test",
                        compatible_event_types=[
                            "sanctions",
                            "war_escalation",
                            "fiscal_expansion",
                        ],
                        direction_preconditions=[
                            "political_risk",
                            "sanctions_risk",
                            "fiscal_risk",
                        ],
                    )
                ],
            )
        ],
        compatible_event_types=["sanctions", "war_escalation", "fiscal_expansion"],
    )

    without_signal, warnings = gate_terminal_candidates_by_event_signals(
        [country_risk_proxy],
        extract_news_event_signals("Officials met for routine talks on trade."),
    )

    assert without_signal == []
    assert warnings == ["event_signal_gate_filtered:missing_relationship_event_compatibility:1"]

    with_signal, warnings = gate_terminal_candidates_by_event_signals(
        [country_risk_proxy],
        extract_news_event_signals("Officials imposed new sanctions after the dispute."),
    )

    assert warnings == []
    assert with_signal[0].compatible_event_types == ["sanctions"]


def test_gate_terminal_candidates_keeps_direct_ticker_without_event_signal() -> None:
    direct_ticker = ImpactCandidate(
        entity_ref="finance-us-ticker:TSLA",
        name="TSLA",
        entity_type="ticker",
        evidence_level="direct",
        confidence=0.93,
        source_entity_refs=["finance-us-ticker:TSLA"],
        relationship_paths=[],
    )

    gated, warnings = gate_terminal_candidates_by_event_signals([direct_ticker], [])

    assert warnings == []
    assert gated[0].entity_ref == "finance-us-ticker:TSLA"


def test_gate_terminal_candidates_keeps_direct_listing_with_operating_signal() -> None:
    candidate = ImpactCandidate(
        entity_ref="finance-us-ticker:TSLA",
        name="TSLA",
        entity_type="ticker",
        evidence_level="shallow",
        confidence=0.89,
        source_entity_refs=["wikidata:Q478214"],
        relationship_paths=[
            ImpactRelationshipPath(
                path_depth=1,
                edges=[
                    ImpactPathEdge(
                        source_ref="finance-us-issuer:0001318605",
                        target_ref="finance-us-ticker:TSLA",
                        relation="issuer_has_listed_ticker",
                        evidence_level="direct",
                        confidence=0.90,
                        direction_hint="issuer_equity_security",
                        source_name="ades-finance-country-pack-metadata",
                        source_url="file:///packs/finance-us-en/sources.json",
                        source_snapshot="2026-04-21",
                        compatible_event_types=["earnings_beat", "acquisition"],
                        direction_preconditions=["direct_issuer_or_security_mention"],
                    )
                ],
            )
        ],
        compatible_event_types=["earnings_beat", "acquisition"],
    )

    signals = extract_news_event_signals("Tesla said it will expand production in Texas.")
    gated, warnings = gate_terminal_candidates_by_event_signals([candidate], signals)

    assert warnings == []
    assert gated[0].entity_ref == "finance-us-ticker:TSLA"
    assert gated[0].compatible_event_types == ["production_increase"]


def test_gate_terminal_candidates_keeps_direct_listing_with_sector_policy_signal() -> None:
    candidate = ImpactCandidate(
        entity_ref="finance-uk-ticker:bem",
        name="BEM",
        entity_type="ticker",
        evidence_level="shallow",
        confidence=0.86,
        source_entity_refs=["finance-uk-issuer:beowulf-mining"],
        relationship_paths=[
            ImpactRelationshipPath(
                path_depth=1,
                edges=[
                    ImpactPathEdge(
                        source_ref="finance-uk-issuer:beowulf-mining",
                        target_ref="finance-uk-ticker:bem",
                        relation="issuer_has_listed_ticker",
                        evidence_level="direct",
                        confidence=0.96,
                        direction_hint="listed_equity",
                        source_name="London Stock Exchange",
                        source_url="https://www.londonstockexchange.com/",
                        source_snapshot="2026-06-29",
                        compatible_event_types=["earnings_beat"],
                        direction_preconditions=["listed_issuer"],
                    )
                ],
            )
        ],
        compatible_event_types=["earnings_beat"],
    )

    signals = extract_news_event_signals(
        "UK parliament passed a mining royalty reform affecting listed mining companies."
    )
    gated, warnings = gate_terminal_candidates_by_event_signals([candidate], signals)

    assert warnings == []
    assert gated[0].entity_ref == "finance-uk-ticker:bem"
    assert gated[0].compatible_event_types == ["sector_policy_change"]


def test_gate_terminal_candidates_rejects_unexplained_indirect_ticker() -> None:
    indirect_ticker = ImpactCandidate(
        entity_ref="finance-us-ticker:OTC",
        name="OTC",
        entity_type="ticker",
        evidence_level="shallow",
        confidence=0.8,
        source_entity_refs=["country:us"],
        relationship_paths=[],
    )
    signals = extract_news_event_signals("The company beat earnings estimates.")

    gated, warnings = gate_terminal_candidates_by_event_signals([indirect_ticker], signals)

    assert gated == []
    assert warnings == ["event_signal_gate_filtered:missing_equity_direct_or_strong_event_path:1"]


def test_gate_terminal_candidates_keeps_key_person_ticker_with_governance_signal() -> None:
    candidate = ImpactCandidate(
        entity_ref="finance-us-ticker:TSLA",
        name="TSLA",
        entity_type="ticker",
        evidence_level="shallow",
        confidence=0.72,
        source_entity_refs=["wikidata:Q317521"],
        relationship_paths=[
            ImpactRelationshipPath(
                path_depth=1,
                edges=[
                    ImpactPathEdge(
                        source_ref="wikidata:Q317521",
                        target_ref="finance-us-ticker:TSLA",
                        relation="person_affects_employer_ticker",
                        evidence_level="direct",
                        confidence=0.86,
                        direction_hint="employer_equity_proxy",
                        source_name="Tesla Investor Relations",
                        source_url="https://ir.tesla.com/corporate/elon-musk",
                        source_snapshot="2026-05-13",
                        compatible_event_types=["key_person_ownership_governance"],
                        direction_preconditions=["strong_person_employer_event"],
                    )
                ],
            )
        ],
        compatible_event_types=["key_person_ownership_governance"],
    )
    signals = extract_news_event_signals(
        "Sam Altman testified that Elon Musk sought a 90% ownership stake in OpenAI."
    )

    gated, warnings = gate_terminal_candidates_by_event_signals([candidate], signals)

    assert warnings == []
    assert gated[0].entity_ref == "finance-us-ticker:TSLA"
    assert gated[0].compatible_event_types == ["key_person_ownership_governance"]


def test_gate_terminal_candidates_keeps_source_backed_key_person_path() -> None:
    candidate = ImpactCandidate(
        entity_ref="finance-au-ticker:nsx:BTG",
        name="BTG",
        entity_type="ticker",
        evidence_level="shallow",
        confidence=0.638222,
        source_entity_refs=["finance-au-person:nsx:BTG:boqing-zhang:director"],
        relationship_paths=[
            ImpactRelationshipPath(
                path_depth=2,
                edges=[
                    ImpactPathEdge(
                        source_ref="finance-au-person:nsx:BTG:boqing-zhang:director",
                        target_ref="finance-au-issuer:nsx:BTG",
                        relation="person_is_chair_of_issuer",
                        evidence_level="shallow",
                        confidence=0.86,
                        direction_hint="issuer_relationship",
                        source_name="ades-finance-country-pack-relationship-metadata",
                        source_url="file:///packs/finance-au-en/sources.json",
                        source_snapshot="2026-04-22",
                        compatible_event_types=["key_person_ownership_governance"],
                    ),
                    ImpactPathEdge(
                        source_ref="finance-au-issuer:nsx:BTG",
                        target_ref="finance-au-ticker:nsx:BTG",
                        relation="issuer_has_listed_ticker",
                        evidence_level="direct",
                        confidence=0.90,
                        direction_hint="issuer_equity_security",
                        source_name="ades-finance-country-pack-metadata",
                        source_url="file:///packs/finance-au-en/sources.json",
                        source_snapshot="2026-04-22",
                        compatible_event_types=["key_person_ownership_governance"],
                    ),
                ],
            )
        ],
    )

    signals = extract_news_event_signals(
        "Australia-listed BTG confirmed the resignation of chief executive "
        "Boqing Zhang after a board governance dispute."
    )
    gated, warnings = gate_terminal_candidates_by_event_signals([candidate], signals)

    assert warnings == []
    assert gated[0].entity_ref == "finance-au-ticker:nsx:BTG"
    assert gated[0].compatible_event_types == ["key_person_ownership_governance"]


def test_gate_terminal_candidates_keeps_source_backed_sector_policy_ticker_path() -> None:
    candidate = ImpactCandidate(
        entity_ref="finance-uk-ticker:anto",
        name="Antofagasta",
        entity_type="ticker",
        evidence_level="shallow",
        confidence=0.81,
        source_entity_refs=["finance-uk-sector:mining"],
        relationship_paths=[
            ImpactRelationshipPath(
                path_depth=1,
                edges=[
                    ImpactPathEdge(
                        source_ref="finance-uk-sector:mining",
                        target_ref="finance-uk-ticker:anto",
                        relation="sector_affects_issuer",
                        evidence_level="direct",
                        confidence=0.86,
                        direction_hint="sector_policy_exposure",
                        source_name="ades-finance-country-pack-metadata",
                        source_url="file:///packs/finance-uk-en/sources.json",
                        source_snapshot="2026-06-28",
                        compatible_event_types=["sector_policy_change"],
                        direction_preconditions=["sector_policy_event_signal"],
                    )
                ],
            )
        ],
        compatible_event_types=["sector_policy_change"],
    )

    signals = extract_news_event_signals(
        "UK parliament passed legislation raising mining royalties for gold and copper miners."
    )
    gated, warnings = gate_terminal_candidates_by_event_signals([candidate], signals)

    assert warnings == []
    assert gated[0].entity_ref == "finance-uk-ticker:anto"
    assert gated[0].compatible_event_types == ["sector_policy_change"]
