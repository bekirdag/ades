from ades.impact.relationship_schema import (
    normalized_relation_direction_preconditions,
    normalized_relation_event_types,
    relation_definition_for,
    relation_direction_preconditions,
    relation_event_types,
    relation_family_for_relation,
    validate_relation_metadata,
)
from ades.impact.source_catalog import (
    SOURCE_TIER_EXCHANGE,
    SOURCE_TIER_GOVERNMENT,
    SOURCE_TIER_INDUSTRY_ASSOCIATION,
    SOURCE_TIER_ISSUER_DISCLOSED,
    SOURCE_TIER_LICENSED,
    SOURCE_TIER_LOCAL_PACK_METADATA,
    SOURCE_TIER_REGULATOR,
    SOURCE_TIER_TEST_FIXTURE,
    SOURCE_TIER_UNKNOWN,
    SOURCE_TIER_WIKIDATA_BRIDGE,
    build_source_attribution,
    classify_source_tier,
    validate_source_attribution,
)


def test_relationship_schema_covers_policy_sector_and_regulatory_paths() -> None:
    definition = relation_definition_for("regulator_affects_sector")

    assert definition.family == "policy_sector_exposure"
    assert "sector_policy_change" in definition.compatible_event_types
    assert "regulatory_enforcement" in definition.compatible_event_types
    assert definition.direction_preconditions == (
        "sector_policy_event_signal",
        "jurisdiction_or_regulator_context",
    )
    assert relation_family_for_relation("sector_affects_issuer") == "policy_sector_exposure"
    assert "sector_policy_change" in relation_event_types("sector_affects_issuer")
    assert relation_direction_preconditions("law_affects_sector") == (
        "sector_policy_event_signal",
        "jurisdiction_or_regulator_context",
    )


def test_relationship_schema_preserves_existing_finance_country_defaults() -> None:
    assert relation_family_for_relation("issuer_has_listed_ticker") == "identity_listing"
    issuer_listing_preconditions = relation_direction_preconditions("issuer_has_listed_ticker")
    assert "direct_issuer_or_security_mention" in issuer_listing_preconditions
    assert "earnings_signal" in issuer_listing_preconditions
    assert "key_person_ownership_governance" in relation_event_types("person_is_ceo_of_issuer")
    assert relation_family_for_relation("issuer_supplier_to_issuer") == (
        "supply_chain_counterparty"
    )
    assert "supply_disruption" in relation_event_types("issuer_supplier_to_issuer")
    assert relation_family_for_relation("issuer_exposed_to_country") == "issuer_exposure"
    assert "war_escalation" in relation_event_types("issuer_exposed_to_country")
    assert "fiscal_expansion" in relation_event_types("issuer_exposed_to_region")
    assert "commodity_price_move" in relation_event_types("commodity_affects_issuer_revenue")
    assert relation_family_for_relation("country_affects_policy_rate_proxy") == (
        "country_macro_policy"
    )
    assert relation_direction_preconditions("country_affects_policy_rate_proxy") == (
        "rate_inflation_central_bank_or_fiscal_signal",
    )
    assert relation_family_for_relation("entity_affects_country_index_proxy") == (
        "global_equity_proxy"
    )
    assert "sector_policy_change" in relation_event_types("entity_affects_country_index_proxy")
    assert relation_direction_preconditions("entity_affects_country_index_proxy") == (
        "macro_policy_or_risk_signal",
        "equity_event_signal",
    )
    assert relation_family_for_relation("ticker_trades_on_exchange") == ("identity_listing")
    assert "policy_rate_cut" in relation_event_types("entity_affects_currency_proxy")
    assert relation_direction_preconditions("entity_affects_currency_proxy") == (
        "currency_or_rates_context",
        "macro_policy_or_risk_signal",
    )
    assert relation_direction_preconditions("index_affects_country_index_proxy") == (
        "sector_or_index_event_signal",
    )
    assert relation_direction_preconditions("ticker_listed_on_exchange") == (
        "direct_issuer_or_security_mention",
        "direct_listing_evidence",
    )
    assert relation_family_for_relation("shipping_chokepoint_affects_commodity") == (
        "geography_commodity"
    )
    assert relation_family_for_relation("industrial_metal_affects_sector") == (
        "geography_commodity"
    )
    assert "war_escalation" in relation_event_types("chokepoint_affects_commodity")
    assert "inflation_shock" in relation_event_types("food_commodity_affects_inflation")
    assert relation_family_for_relation("central_bank_affects_rates") == ("country_macro_policy")
    assert "policy_rate_hike" in relation_event_types("central_bank_affects_rates")
    assert relation_family_for_relation("central_bank_affects_currency") == ("country_macro_policy")
    assert relation_family_for_relation("central_bank_affects_credit_sector") == (
        "country_macro_policy"
    )
    assert relation_family_for_relation("statistics_body_reports_macro_indicator") == (
        "country_macro_policy"
    )
    assert "tariff" in relation_event_types("central_bank_affects_currency")
    assert "policy_rate_hike" in relation_event_types("central_bank_affects_credit_sector")
    assert "inflation_shock" in relation_event_types("statistics_body_reports_macro_indicator")
    assert relation_direction_preconditions("central_bank_affects_currency") == (
        "currency_or_rates_context",
        "macro_policy_or_risk_signal",
        "fx_fixing_or_capital_flow_signal",
    )
    assert relation_direction_preconditions("central_bank_affects_credit_sector") == (
        "credit_lending_or_liquidity_signal",
        "property_bank_or_financing_context",
    )
    assert relation_direction_preconditions("statistics_body_reports_macro_indicator") == (
        "macro_statistics_release_signal",
        "inflation_growth_or_property_indicator_context",
    )


def test_relationship_schema_covers_program_org_relationship_paths() -> None:
    definition = relation_definition_for("program_operated_by_org")

    assert definition.family == "program_org_relationship"
    assert "sector_policy_change" in definition.compatible_event_types
    assert "earnings_miss" in definition.compatible_event_types
    assert definition.direction_preconditions == (
        "direct_program_product_or_brand_mention",
        "source_backed_program_org_evidence",
        "compatible_event_signal",
    )
    assert relation_family_for_relation("org_part_of_holding") == "organization_control"
    assert relation_direction_preconditions("org_part_of_holding") == (
        "source_backed_ownership_or_affiliation_chain",
        "strong_ownership_or_control_event",
        "compatible_event_signal",
    )
    assert relation_family_for_relation("issuer_has_security") == "identity_listing"
    assert relation_direction_preconditions("issuer_has_security") == (
        "direct_issuer_or_security_mention",
        "direct_company_mention",
        "corporate_action",
    )
    assert relation_family_for_relation("program_loan_recipient_org") == (
        "program_org_relationship"
    )
    assert "source_backed_program_org_evidence" in relation_direction_preconditions(
        "program_loan_recipient_org"
    )
    assert relation_family_for_relation("program_financed_by_org") == ("program_org_relationship")
    assert "source_backed_program_org_evidence" in relation_direction_preconditions(
        "program_financed_by_org"
    )
    assert relation_family_for_relation("program_host_country") == ("program_country_relationship")
    assert relation_direction_preconditions("program_host_country") == (
        "direct_program_or_event_mention",
        "source_backed_host_country_evidence",
        "compatible_event_signal",
    )
    assert relation_family_for_relation("project_operated_by_org") == ("project_org_relationship")
    assert relation_direction_preconditions("project_operated_by_org") == (
        "direct_project_mention",
        "source_backed_project_org_evidence",
        "compatible_event_signal",
    )
    assert relation_family_for_relation("project_participant_org") == ("project_org_relationship")
    assert relation_direction_preconditions("project_participant_org") == (
        "direct_project_mention",
        "source_backed_project_org_evidence",
        "compatible_event_signal",
    )
    assert relation_family_for_relation("infrastructure_project_affects_sector") == (
        "policy_sector_exposure"
    )
    assert "supply_disruption" in relation_event_types("infrastructure_project_affects_sector")
    assert relation_direction_preconditions("infrastructure_project_affects_sector") == (
        "infrastructure_project_event_signal",
        "capacity_delay_or_logistics_context",
        "jurisdiction_or_project_context",
    )
    assert relation_family_for_relation("defense_project_affects_sector") == (
        "policy_sector_exposure"
    )
    assert "sector_policy_change" in relation_event_types("defense_project_affects_sector")
    assert "supply_disruption" in relation_event_types("defense_project_affects_sector")
    assert relation_direction_preconditions("defense_project_affects_sector") == (
        "defense_project_event_signal",
        "procurement_budget_or_collaboration_context",
        "jurisdiction_or_project_context",
    )
    assert relation_family_for_relation("org_in_sector") == "sector_exposure"
    assert relation_direction_preconditions("org_in_sector") == (
        "direct_org_or_sector_membership_evidence",
    )
    assert relation_family_for_relation("payment_network_operated_by_org") == (
        "program_org_relationship"
    )
    assert "source_backed_program_org_evidence" in relation_direction_preconditions(
        "payment_network_operated_by_org"
    )
    assert relation_family_for_relation("public_facility_available_to_sector") == (
        "policy_sector_exposure"
    )
    assert "public_facility_or_funding_context" in relation_direction_preconditions(
        "public_facility_available_to_sector"
    )
    assert relation_family_for_relation("trade_agreement_affects_sector") == (
        "policy_sector_exposure"
    )
    assert "trade_agreement_or_market_access_signal" in relation_direction_preconditions(
        "trade_agreement_affects_sector"
    )
    assert relation_family_for_relation("trade_agreement_counterparty_country") == (
        "policy_sector_exposure"
    )
    assert "bilateral_counterparty_context" in relation_direction_preconditions(
        "trade_agreement_counterparty_country"
    )
    assert relation_family_for_relation("prudential_authority_supervises_bank") == (
        "policy_sector_exposure"
    )
    assert "market_infrastructure_or_regulator_context" in relation_direction_preconditions(
        "prudential_authority_supervises_bank"
    )
    assert relation_family_for_relation("regulator_supervises_electricity") == (
        "policy_sector_exposure"
    )
    assert relation_family_for_relation("regulator_supervises_competition") == (
        "policy_sector_exposure"
    )
    assert "regulatory_enforcement" in relation_event_types("regulator_supervises_electricity")
    assert relation_family_for_relation("government_body_sets_mining_policy") == (
        "policy_sector_exposure"
    )
    assert "policy_rate_hike" in relation_event_types("government_body_sets_mining_policy")
    assert relation_family_for_relation("government_body_sets_technology_policy") == (
        "policy_sector_exposure"
    )
    assert relation_family_for_relation("government_body_sets_trade_policy") == (
        "policy_sector_exposure"
    )
    assert "sector_policy_change" in relation_event_types("industrial_policy_affects_sector")
    assert relation_direction_preconditions("industrial_policy_affects_sector") == (
        "sector_policy_event_signal",
        "industrial_policy_or_subsidy_context",
    )
    assert relation_family_for_relation("regulator_supervises_financial_group") == (
        "policy_sector_exposure"
    )
    assert relation_family_for_relation("issuer_exposed_to_semiconductor_cycle") == (
        "issuer_exposure"
    )
    assert "source_backed_issuer_exposure" in relation_direction_preconditions(
        "issuer_exposed_to_semiconductor_cycle"
    )
    assert relation_family_for_relation("state_owned_enterprise_operates_infrastructure") == (
        "infrastructure_asset"
    )
    assert relation_family_for_relation("org_operates_grid") == "infrastructure_asset"
    assert relation_family_for_relation("org_operates_rail") == "infrastructure_asset"
    assert relation_family_for_relation("org_operates_telecom_network") == ("infrastructure_asset")
    assert "direct_asset_or_operator_mention" in relation_direction_preconditions(
        "org_operates_grid"
    )
    assert relation_family_for_relation("rail_corridor_moves_commodity") == "commodity_flow"
    assert "source_backed_flow_evidence" in relation_direction_preconditions(
        "rail_corridor_moves_commodity"
    )


def test_relationship_schema_covers_russia_market_access_sanctions_and_flow_paths() -> None:
    assert relation_family_for_relation("security_has_market_access_status") == (
        "market_access_status"
    )
    assert "regulatory_enforcement" in relation_event_types("security_has_market_access_status")
    assert relation_direction_preconditions("security_has_market_access_status") == (
        "direct_issuer_or_security_mention",
        "trading_status_or_market_access_signal",
        "source_backed_security_status_evidence",
    )
    assert (
        validate_relation_metadata(
            relation="security_has_market_access_status",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )

    assert relation_family_for_relation("sanctions_restriction_affects_commodity") == "sanctions"
    assert relation_event_types("sanctions_restriction_affects_commodity") == (
        "sanctions",
        "export_control",
        "war_escalation",
        "ceasefire_risk_relief",
    )
    assert relation_direction_preconditions("sanctions_program_targets_sector") == (
        "direct_sanctions_authority_or_program_mention",
        "sanctions_target_or_restriction_context",
        "compatible_event_signal",
    )
    assert (
        validate_relation_metadata(
            relation="sanctions_restriction_affects_commodity",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )

    assert relation_family_for_relation("commodity_flow_affects_commodity") == "commodity_flow"
    assert "supply_disruption" in relation_event_types("commodity_flow_affects_commodity")
    assert relation_direction_preconditions("pipeline_transports_commodity") == (
        "commodity_flow_or_infrastructure_event_signal",
        "source_backed_flow_evidence",
        "compatible_event_signal",
    )
    assert (
        validate_relation_metadata(
            relation="org_operates_pipeline",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )

    assert relation_family_for_relation("regulator_supervises_exchange") == (
        "policy_sector_exposure"
    )
    assert relation_event_types("government_body_sets_energy_policy") == (
        "sector_policy_change",
        "regulatory_enforcement",
        "fiscal_expansion",
        "fiscal_austerity",
        "tariff",
        "export_control",
        "sanctions",
        "policy_rate_cut",
        "policy_rate_hike",
        "policy_rate_hold",
        "inflation_shock",
        "future_policy_expectation",
    )


def test_relationship_schema_covers_saudi_program_project_and_policy_paths() -> None:
    assert relation_family_for_relation("vision_program_affects_sector") == (
        "policy_sector_exposure"
    )
    assert relation_family_for_relation("vision_program_has_project") == (
        "program_project_relationship"
    )
    assert relation_direction_preconditions("vision_program_has_project") == (
        "direct_program_or_project_mention",
        "source_backed_program_project_evidence",
        "compatible_event_signal",
    )
    assert relation_family_for_relation("project_affects_sector") == ("policy_sector_exposure")
    assert "supply_disruption" in relation_event_types("project_affects_sector")
    assert relation_family_for_relation("sovereign_fund_sponsors_program") == (
        "program_org_relationship"
    )
    assert "source_backed_program_org_evidence" in relation_direction_preconditions(
        "sovereign_fund_sponsors_program"
    )
    assert relation_family_for_relation("central_bank_maintains_currency_peg") == (
        "country_macro_policy"
    )
    assert "fx_fixing_or_capital_flow_signal" in relation_direction_preconditions(
        "central_bank_maintains_currency_peg"
    )
    assert relation_family_for_relation("central_bank_sets_policy_rate") == ("country_macro_policy")
    assert "liquidity_or_policy_rate_signal" in relation_direction_preconditions(
        "central_bank_sets_policy_rate"
    )
    assert relation_family_for_relation("production_policy_affects_commodity") == (
        "geography_commodity"
    )
    assert "commodity_price_move" in relation_event_types("production_policy_affects_commodity")
    assert relation_family_for_relation("pilgrimage_flow_affects_sector") == (
        "policy_sector_exposure"
    )
    assert (
        validate_relation_metadata(
            relation="vision_program_has_project",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )


def test_relationship_schema_covers_turkiye_market_policy_and_exposure_paths() -> None:
    assert relation_family_for_relation("central_bank_sets_reserve_requirement") == (
        "country_macro_policy"
    )
    assert "policy_rate_hike" in relation_event_types("central_bank_sets_reserve_requirement")
    assert "liquidity_or_policy_rate_signal" in relation_direction_preconditions(
        "central_bank_sets_reserve_requirement"
    )
    assert relation_family_for_relation("regulator_supervises_capital_market") == (
        "policy_sector_exposure"
    )
    assert relation_family_for_relation("clearing_house_clears_security") == (
        "policy_sector_exposure"
    )
    assert relation_family_for_relation("issuer_exposed_to_air_travel_cycle") == ("issuer_exposure")
    assert "source_backed_issuer_exposure" in relation_direction_preconditions(
        "issuer_exposed_to_air_travel_cycle"
    )
    assert relation_family_for_relation("org_operates_airline") == "infrastructure_asset"
    assert relation_family_for_relation("org_operates_airport") == "infrastructure_asset"
    assert "energy_import_or_current_account_signal" in relation_direction_preconditions(
        "energy_import_affects_currency"
    )
    assert (
        validate_relation_metadata(
            relation="central_bank_sets_reserve_requirement",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )


def test_relationship_schema_covers_united_kingdom_macro_regulatory_and_exposure_paths() -> None:
    assert relation_family_for_relation("central_bank_sets_qt_policy") == "country_macro_policy"
    assert "policy_rate_hike" in relation_event_types("central_bank_sets_qt_policy")
    assert "gilt_or_bond_market_signal" in relation_direction_preconditions(
        "central_bank_sets_qt_policy"
    )

    assert relation_family_for_relation("sovereign_debt_office_issues_bond") == (
        "country_macro_policy"
    )
    assert "fiscal_austerity" in relation_event_types("sovereign_debt_office_issues_bond")
    assert "gilt_or_sovereign_debt_signal" in relation_direction_preconditions(
        "sovereign_debt_office_issues_bond"
    )

    assert relation_family_for_relation("regulator_supervises_water_utility") == (
        "policy_sector_exposure"
    )
    assert relation_family_for_relation("regulator_supervises_energy_utility") == (
        "policy_sector_exposure"
    )
    assert relation_family_for_relation("tax_authority_affects_sector") == (
        "policy_sector_exposure"
    )
    assert "tax_or_budget_context" in relation_direction_preconditions(
        "tax_authority_affects_sector"
    )
    assert relation_family_for_relation("company_registry_registers_legal_entity") == (
        "policy_sector_exposure"
    )
    assert relation_family_for_relation("issuer_exposed_to_gilt_yields") == "issuer_exposure"
    assert relation_family_for_relation("issuer_exposed_to_water_regulation") == ("issuer_exposure")
    assert relation_family_for_relation("org_operates_water_network") == "infrastructure_asset"
    assert relation_family_for_relation("org_operates_oil_gas_asset") == "infrastructure_asset"
    assert relation_family_for_relation("statistics_agency_publishes_indicator") == (
        "country_macro_policy"
    )
    assert relation_family_for_relation("product_authorized_by_regulator") == (
        "policy_sector_exposure"
    )
    assert "product_approval_or_regulatory_signal" in relation_direction_preconditions(
        "product_authorized_by_regulator"
    )
    assert (
        validate_relation_metadata(
            relation="central_bank_sets_qt_policy",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )


def test_relationship_schema_covers_united_states_macro_regulatory_and_identity_paths() -> None:
    assert relation_family_for_relation("central_bank_sets_balance_sheet_policy") == (
        "country_macro_policy"
    )
    assert relation_family_for_relation("central_bank_operates_liquidity_facility") == (
        "country_macro_policy"
    )
    assert "policy_rate_hike" in relation_event_types("central_bank_sets_balance_sheet_policy")
    assert "liquidity_or_policy_rate_signal" in relation_direction_preconditions(
        "central_bank_operates_liquidity_facility"
    )

    assert relation_family_for_relation("treasury_issues_security") == ("country_macro_policy")
    assert "gilt_or_sovereign_debt_signal" in relation_direction_preconditions(
        "treasury_issues_security"
    )

    for relation in [
        "issuer_has_cik",
        "security_has_ticker",
        "security_has_figi",
        "security_has_cusip",
        "legal_entity_has_lei",
    ]:
        assert relation_family_for_relation(relation) == "identity_listing"
        assert "source_backed_identifier_evidence" in relation_direction_preconditions(relation)

    for relation in [
        "regulator_supervises_broker_dealer",
        "regulator_supervises_consumer_finance",
        "regulator_supervises_derivatives_market",
        "regulator_supervises_health_product",
        "regulator_supervises_transport",
        "government_body_sets_transport_policy",
        "tariff_affects_sector",
        "product_reimbursed_by_program",
    ]:
        assert relation_family_for_relation(relation) == "policy_sector_exposure"
        assert "sector_policy_change" in relation_event_types(relation)

    for relation in [
        "issuer_exposed_to_treasury_yields",
        "issuer_exposed_to_energy_price",
        "issuer_exposed_to_fda_regulation",
        "issuer_exposed_to_transport_cycle",
    ]:
        assert relation_family_for_relation(relation) == "issuer_exposure"
        assert "source_backed_issuer_exposure" in relation_direction_preconditions(relation)

    assert (
        validate_relation_metadata(
            relation="central_bank_operates_liquidity_facility",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )


def test_relationship_schema_defaults_and_warnings_are_non_fatal() -> None:
    assert normalized_relation_event_types("regulator_affects_sector", ()) == (
        "sector_policy_change",
        "regulatory_enforcement",
        "fiscal_expansion",
        "fiscal_austerity",
        "tariff",
        "export_control",
        "sanctions",
    )
    assert normalized_relation_direction_preconditions(
        "regulator_affects_sector",
        None,
    ) == (
        "sector_policy_event_signal",
        "jurisdiction_or_regulator_context",
    )
    assert (
        validate_relation_metadata(
            relation="country_exports_commodity",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )
    assert (
        validate_relation_metadata(
            relation="country_member_of_bloc",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )
    assert (
        validate_relation_metadata(
            relation="entity_affects_country_index_proxy",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )
    assert (
        validate_relation_metadata(
            relation="country_affects_currency_proxy",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )
    assert (
        validate_relation_metadata(
            relation="ticker_trades_on_exchange",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )
    assert (
        validate_relation_metadata(
            relation="program_host_country",
            configured_event_types=(),
            configured_preconditions=(),
        )
        == []
    )
    assert (
        validate_relation_metadata(
            relation="index_affects_country_index_proxy",
            configured_event_types=(),
            configured_preconditions=["sector_or_index_event_signal"],
        )
        == []
    )
    assert validate_relation_metadata(
        relation="regulator_affects_sector",
        configured_event_types=["earnings_beat"],
        configured_preconditions=["sector_policy_event_signal"],
    ) == ["unsupported_compatible_event_type:earnings_beat"]


def test_source_catalog_classifies_core_source_tiers() -> None:
    assert (
        classify_source_tier("SEC filing", "https://www.sec.gov/Archives/edgar/data/1")
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "Federal Reserve Discount Window",
            "https://www.frbdiscountwindow.org/",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "Nasdaq Trader symbol directory",
            "https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier("Cboe U.S. equities", "https://www.cboe.com/us/equities/")
        == SOURCE_TIER_EXCHANGE
    )
    assert classify_source_tier("OCC About", "https://www.occ.gov/about/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("FDIC About", "https://www.fdic.gov/about/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("FDA What We Do", "https://www.fda.gov/about-fda") == (
        SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "Treasury Marketable Securities",
            "https://treasurydirect.gov/marketable-securities/",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier("UK legislation", "https://www.legislation.gov.uk/ukpga")
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Australian resources statistics",
            "https://www.industry.gov.au/publications/resources-and-energy-quarterly",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Company investor relations",
            "https://ir.example-issuer.com/governance",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "PNM company history",
            "https://www.pnm.co.id/tentang/sejarah",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Cabinet Secretariat Ultra Micro Holding release",
            "https://setkab.go.id/en/govt-establishes-ultra-micro-holding/",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "BRI investor relations corporate profile",
            "https://www.ir-bri.com/corporate_profile.html",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Intertek Group Overview",
            "https://www.intertek.com/investors/group-overview/",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "MSCI Global Equity Indexes",
            "https://www.msci.com/our-solutions/indexes/global-equity-indexes",
        )
        == SOURCE_TIER_LICENSED
    )
    assert (
        classify_source_tier(
            "SIA State of the U.S. Semiconductor Industry",
            "https://www.semiconductors.org/resources/",
        )
        == SOURCE_TIER_INDUSTRY_ASSOCIATION
    )
    assert (
        classify_source_tier(
            "ades-finance-country-pack-metadata",
            "file:///packs/finance-uk-en/sources.json",
        )
        == SOURCE_TIER_LOCAL_PACK_METADATA
    )
    assert (
        classify_source_tier("Wikidata", "https://www.wikidata.org/wiki/Q123")
        == SOURCE_TIER_WIKIDATA_BRIDGE
    )
    assert (
        classify_source_tier("GLEIF LEI records", "https://www.gleif.org/en/lei-data")
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "OpenFIGI mapping values",
            "https://www.openfigi.com/api/enumValues/v3",
        )
        == SOURCE_TIER_LICENSED
    )
    assert (
        classify_source_tier(
            "Indonesia Stock Exchange company profile",
            "https://www.idx.co.id/en/listed-companies/company-profiles/BBRI",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "BCRA Sistema Financiero",
            "https://www.bcra.gob.ar/sistema-financiero/",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "CNV AIF Banco Patagonia",
            "https://aif2.cnv.gov.ar/presentations/publicview/d90593c7",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "BYMA Empresas Listadas",
            "https://www.byma.com.ar/financiarse/para-emisoras/empresas-listadas",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "BYMADATA BPAT",
            "https://open.bymadata.com.ar/#/technical-detail-equity?symbol=BPAT",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "ASX CBA company information",
            "https://www.asx.com.au/markets/company/CBA",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert classify_source_tier("ASX Online", "https://www.asxonline.com/") == SOURCE_TIER_EXCHANGE
    assert (
        classify_source_tier("APRA registers", "https://www.apra.gov.au/registers")
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "ASIC registers",
            "https://connectonline.asic.gov.au/RegistrySearch/faces/landing",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "TGA medicinal cannabis hub",
            "https://www.tga.gov.au/products/medicinal-cannabis",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier("RBA statistics", "https://www.rba.gov.au/statistics/")
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Housing Australia",
            "https://www.housingaustralia.gov.au/",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Fuel tax credits",
            "https://business.gov.au/grants-and-programs/fuel-tax-credits",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "CEFC where we invest",
            "https://www.cefc.com.au/where-we-invest/",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier("ARENA projects", "https://arena.gov.au/projects/")
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "NRFC priority areas",
            "https://www.nrf.gov.au/priority-areas",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "NSW medicinal cannabis driving reform",
            "https://www.nsw.gov.au/media-releases/medicinal-cannabis-driving-reform",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Transport for NSW registration",
            "https://www.transport.nsw.gov.au/",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "B3 company detail API",
            "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetDetail/test",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier("CVM regulator page", "https://www.gov.br/cvm/en")
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "BNDES institutional page",
            "https://www.bndes.gov.br/SiteBNDES/bndes/bndes_en/Institucional/The_BNDES/",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "ANP regulator page",
            "https://www.gov.br/anp/en/access-information/what-is-anp/what-is-anp",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "ANEEL regulator page",
            "https://www.gov.br/aneel/en",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "Transport Ministry Ferrogrão page",
            "https://www.gov.br/transportes/pt-br/assuntos/sustentabilidade/gt-da-ef-170-ferrograo",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Petrobras profile",
            "https://petrobras.com.br/en/quem-somos/perfil",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Rumo investor relations",
            "https://ri.rumolog.com/en/",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Commonwealth Bank investor centre",
            "https://www.commbank.com.au/about-us/investors.html",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier("BHP investors", "https://www.bhp.com/investors")
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Banco Patagonia investor relations",
            "https://bp.bancopatagonia.com.ar/relacion-con-inversores/es/institucional",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Rio Negro program",
            "https://rionegro.gov.ar/articulo/59571/abren-las-inscripciones-al-programa-emprendedores-rio-negro-2026",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "TMX company directory",
            "https://www.tsx.com/json/company-directory/search/tsx/Royal%20Bank",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier("TMX quote", "https://money.tmx.com/en/quote/RY")
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "Bank of Canada policy rate",
            "https://www.bankofcanada.ca/core-functions/monetary-policy/key-interest-rate/",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Canada Infrastructure Bank investments",
            "https://www.cib-bic.ca/en/investments/",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Strategic Innovation Fund",
            "https://ised-isde.canada.ca/site/strategic-innovation-fund/en",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "CMHC mortgage loan insurance",
            "https://www.cmhc-schl.gc.ca/consumers/home-buying/mortgage-loan-insurance-for-consumers",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Farm Credit Canada",
            "https://www.fcc-fac.ca/en",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Agriculture and Agri-Food Canada AgriStability",
            "https://agriculture.canada.ca/en/programs/agristability",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Global Affairs Canada trade sectors",
            "https://www.international.gc.ca/world-monde/international_relations-relations_internationales/turkiye/relations.aspx",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "OSFI regulated institutions",
            "https://www.osfi-bsif.gc.ca/en/supervision/financial-institutions",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "Canada Energy Regulator facilities",
            "https://www.cer-rec.gc.ca/en/about/who-we-are-what-we-do/facilities-we-regulate/",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "CRTC telecommunications",
            "https://crtc.gc.ca/eng/home-accueil.htm",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "SEDAR+ public filings",
            "https://www.sedarplus.ca/landingpage/",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "Canadian Securities Administrators",
            "https://www.securities-administrators.ca/",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "Shopify investors",
            "https://www.shopify.com/investors",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Canadian Natural investor information",
            "https://www.cnrl.com/investor-information/",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "CPKC investor relations",
            "https://investor.cpkcr.com/",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Rogers investor relations",
            "https://about.rogers.com/investor-relations/",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "SSE company profile query",
            "https://query.sse.com.cn/commonQuery.do",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier("SZSE English site", "https://www.szse.cn/English/")
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "CNInfo stock list",
            "https://www.cninfo.com.cn/data/yellowpages/getYellowpageStockList",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "BSE security information",
            "https://www.bse.cn/nqxxController/nqxxCnzq.do",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "HKEX list of securities",
            "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier("CSI index page", "https://www.csindex.com.cn/mobile-web/")
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "PBOC monetary policy",
            "https://www.pbc.gov.cn/en/3688006/index.html",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier("NBS statistics", "https://www.stats.gov.cn/english/")
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier("SAMR homepage", "https://www.samr.gov.cn/") == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier("CSRC English site", "https://www.csrc.gov.cn/csrc_en/")
        == SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("ChinaClear", "https://www.chinaclear.cn/") == SOURCE_TIER_REGULATOR
    assert (
        classify_source_tier(
            "Sam's Club corporate page",
            "https://corporate.walmart.com/about/samsclub",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Tencent investors",
            "https://www.tencent.com/en-us/investors.html",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Boerse Frankfurt SAP equity page",
            "https://www.boerse-frankfurt.de/equity/sap-se",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert classify_source_tier("Xetra instruments", "https://www.xetra.com/xetra-en/") == (
        SOURCE_TIER_EXCHANGE
    )
    assert classify_source_tier("STOXX DAX page", "https://stoxx.com/index/dax/") == (
        SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "BaFin official homepage",
            "https://www.bafin.de/EN/Homepage/homepage_node.html",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "Bundesnetzagentur official homepage",
            "https://www.bundesnetzagentur.de/EN/Home/home_node.html",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "BMWK power plant strategy",
            "https://www.bmwk.de/Redaktion/EN/Pressemitteilungen/2024/02/"
            "20240205-germany-agrees-on-key-elements-of-a-power-plant-strategy.html",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier("Bundesbank official homepage", "https://www.bundesbank.de/en")
        == SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier("Destatis official homepage", "https://www.destatis.de/EN/")
        == SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("RBI official site", "https://www.rbi.org.in/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("MoSPI official site", "https://www.mospi.gov.in/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("SEBI official site", "https://www.sebi.gov.in/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("TRAI official site", "https://www.trai.gov.in/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("NSE HUDCO quote", "https://www.nseindia.com/") == (
        SOURCE_TIER_EXCHANGE
    )
    assert classify_source_tier("NPCI UPI product overview", "https://www.npci.org.in/") == (
        SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Euronext Milan BMED equity product page",
            "https://live.euronext.com/en/product/equities/IT0004776628-MTAA",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "Borsa Italiana official list",
            "https://www.borsaitaliana.it/listino-ufficiale.en.htm",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "Banca d'Italia tasks page",
            "https://www.bancaditalia.it/compiti/index.html",
        )
        == SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("ISTAT official site", "https://www.istat.it/en/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("CONSOB Italian markets", "https://www.consob.it/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("ARERA official site", "https://www.arera.it/en/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("PagoPA official site", "https://www.pagopa.it/en/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "Banca Mediolanum investor relations",
            "https://www.bancamediolanum.it/corporate-governance",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "Airbus FCAS official page",
            "https://www.airbus.com/en/products-services/defence/future-combat-air-system-fcas",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "BMV mobile quote directory",
            "https://www.bmv.com.mx/en/movil/JSONClaveCotizacion?idBusquedaCotizacion=2",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier(
            "BIVA issuer spreadsheet",
            "https://www.biva.mx/emisoras/empresas/xls",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert classify_source_tier("Banxico homepage", "https://www.banxico.org.mx/indexen.html") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("INEGI homepage", "https://www.inegi.org.mx/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("SHCP homepage", "https://www.gob.mx/shcp/en") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("CNBV homepage", "https://www.cnbv.gob.mx/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("CNE homepage", "https://www.gob.mx/cne") == (SOURCE_TIER_REGULATOR)
    assert classify_source_tier("CRT homepage", "https://www.gob.mx/crt") == (SOURCE_TIER_REGULATOR)
    assert classify_source_tier("CNA homepage", "https://www.gob.mx/antimonopolio") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("COFEPRIS homepage", "https://www.gob.mx/cofepris") == (
        SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "FIFA World Cup 2026 tournament page",
            "https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026",
        )
        == SOURCE_TIER_INDUSTRY_ASSOCIATION
    )
    assert (
        classify_source_tier(
            "America Movil investors",
            "https://www.americamovil.com/English/investors/default.aspx",
        )
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier(
            "MOEX ISS security description",
            "https://iss.moex.com/iss/securities/SBER.json",
        )
        == SOURCE_TIER_EXCHANGE
    )
    assert (
        classify_source_tier("SPIMEX official site", "https://spimex.global/about/")
        == SOURCE_TIER_EXCHANGE
    )
    assert classify_source_tier("Bank of Russia key rate", "https://www.cbr.ru/eng/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert (
        classify_source_tier(
            "EU Sanctions Map Russia regime",
            "https://www.sanctionsmap.eu/api/v1/regime?id%5B%5D=26&lang=en",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier(
            "National Settlement Depository official site",
            "https://www.nsd.ru/en/",
        )
        == SOURCE_TIER_REGULATOR
    )
    assert (
        classify_source_tier("Gazprom investors", "https://www.gazprom.com/investors/")
        == SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert classify_source_tier("OMZ official site", "https://omz.tech/en/") == (
        SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert (
        classify_source_tier("Saudi Exchange profile", "https://www.saudiexchange.sa/")
        == SOURCE_TIER_EXCHANGE
    )
    assert classify_source_tier("Saudi CMA", "https://cma.gov.sa/en/") == (SOURCE_TIER_REGULATOR)
    assert classify_source_tier("SAMA monetary policy", "https://www.sama.gov.sa/en-US/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("PIF official profile", "https://www.pif.gov.sa/en/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier(
        "Vision 2030 official site", "https://www.vision2030.gov.sa/en"
    ) == (SOURCE_TIER_GOVERNMENT)
    assert classify_source_tier("NEOM official site", "https://www.neom.com/en-us/about") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("SABIC investors", "https://www.sabic.com/en/investors") == (
        SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert classify_source_tier("JSE issuer directory", "https://clientportal.jse.co.za/") == (
        SOURCE_TIER_EXCHANGE
    )
    assert classify_source_tier("SARB Prudential Authority", "https://www.resbank.co.za/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("FSCA official site", "https://www.fsca.co.za/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("NERSA official site", "https://www.nersa.org.za/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("National Treasury", "https://www.treasury.gov.za/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("Eskom company information", "https://www.eskom.co.za/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("Sasol investors", "https://www.sasol.com/") == (
        SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert classify_source_tier("KRX listed companies", "https://global.krx.co.kr/") == (
        SOURCE_TIER_EXCHANGE
    )
    assert classify_source_tier("BOK monetary policy", "https://www.bok.or.kr/eng/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("FSC official site", "https://www.fsc.go.kr/eng/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("FSS DART", "https://dart.fss.or.kr/") == (SOURCE_TIER_REGULATOR)
    assert classify_source_tier("MOTIR policy page", "https://english.motir.go.kr/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("Samsung semiconductor", "https://semiconductor.samsung.com/") == (
        SOURCE_TIER_ISSUER_DISCLOSED
    )
    assert classify_source_tier("KAP profile", "https://www.kap.org.tr/en/bist-sirketler") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier(
        "TCMB monetary policy",
        "https://www.tcmb.gov.tr/wps/wcm/connect/EN/TCMB+EN/Main+Menu/Core+Functions/Monetary+Policy",
    ) == (SOURCE_TIER_GOVERNMENT)
    assert classify_source_tier(
        "Borsa Istanbul listed companies",
        "https://www.borsaistanbul.com/en/companies/listed-companies",
    ) == (SOURCE_TIER_EXCHANGE)
    assert classify_source_tier(
        "Turkish Airlines investor relations",
        "https://investor.turkishairlines.com/en",
    ) == (SOURCE_TIER_ISSUER_DISCLOSED)
    assert classify_source_tier(
        "Bank of England monetary policy",
        "https://www.bankofengland.co.uk/monetary-policy",
    ) == (SOURCE_TIER_REGULATOR)
    assert classify_source_tier(
        "HM Treasury official organisation page",
        "https://www.gov.uk/government/organisations/hm-treasury",
    ) == (SOURCE_TIER_GOVERNMENT)
    assert classify_source_tier("UK Debt Management Office", "https://www.dmo.gov.uk/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("Office for National Statistics", "https://www.ons.gov.uk/") == (
        SOURCE_TIER_GOVERNMENT
    )
    assert classify_source_tier("Ofwat official site", "https://www.ofwat.gov.uk/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier("Payment Systems Regulator", "https://www.psr.org.uk/") == (
        SOURCE_TIER_REGULATOR
    )
    assert classify_source_tier(
        "London Stock Exchange RR company page",
        "https://www.londonstockexchange.com/stock/RR./rolls-royce-holdings-plc/company-page",
    ) == (SOURCE_TIER_EXCHANGE)
    assert classify_source_tier(
        "LSEG Regulatory News Service",
        "https://www.lseg.com/en/capital-markets/regulatory-news-service",
    ) == (SOURCE_TIER_EXCHANGE)
    assert classify_source_tier(
        "Rolls-Royce investor relations",
        "https://www.rolls-royce.com/investors.aspx",
    ) == (SOURCE_TIER_ISSUER_DISCLOSED)
    assert classify_source_tier("unit test", "https://example.test/source") == (
        SOURCE_TIER_TEST_FIXTURE
    )
    assert classify_source_tier("blog", "https://unknown.example/source") == (SOURCE_TIER_UNKNOWN)


def test_source_catalog_validation_marks_non_promotable_sources() -> None:
    attribution = build_source_attribution(
        source_name="ades-finance-country-pack-metadata",
        source_url="file:///packs/finance-us-en/sources.json",
        source_snapshot="2026-06-28",
    )

    assert attribution.source_tier == SOURCE_TIER_LOCAL_PACK_METADATA
    assert attribution.promotion_eligible is True
    assert validate_source_attribution(
        source_name="Wikidata",
        source_url="https://www.wikidata.org/wiki/Q123",
        source_snapshot="2026-06-28",
    ) == ["bridge_source_requires_supporting_source_before_promotion"]
    assert validate_source_attribution(
        source_name="unit test",
        source_url="https://example.test/source",
        source_snapshot="2026-06-28",
    ) == ["test_fixture_source_not_promotable"]
    assert validate_source_attribution(
        source_name=None,
        source_url="",
        source_snapshot=None,
    ) == ["missing_source_name", "missing_source_url", "missing_source_snapshot"]
