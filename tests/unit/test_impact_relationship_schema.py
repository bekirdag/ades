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
    assert relation_family_for_relation("project_operated_by_org") == ("project_org_relationship")
    assert relation_direction_preconditions("project_operated_by_org") == (
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
    assert relation_family_for_relation("org_in_sector") == "sector_exposure"
    assert relation_direction_preconditions("org_in_sector") == (
        "direct_org_or_sector_membership_evidence",
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
