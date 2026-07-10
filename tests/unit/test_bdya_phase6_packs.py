import json
from pathlib import Path

from ades.packs.bdya_phase6 import (
    BDYA_DOMAIN_PACK_SPECS,
    BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES,
    BDYA_G20_COUNTRY_CODES,
    BDYA_G20_OFFICIAL_ROLE_NAMES,
    BDYA_G20_PUBLIC_OFFICE_ROLE_ENTITIES,
    BDYA_G20_REQUIRED_GENERAL_CATEGORIES,
    BDYA_G20_TRADE_OR_CUSTOMS_CATEGORIES,
    BDYA_GENERAL_OVERLAY_ENTITIES,
    build_bdya_domain_source_bundles,
)
from ades.packs.generation import generate_pack_source
from ades.runtime_matcher import find_exact_match_candidates, load_runtime_matcher


def _canonical_names(records: tuple[dict[str, object], ...]) -> set[str]:
    return {str(record["canonical_text"]) for record in records}


def _names_by_type(records: tuple[dict[str, object], ...], entity_type: str) -> set[str]:
    return {
        str(record["canonical_text"])
        for record in records
        if record.get("entity_type") == entity_type
    }


def _load_aliases(path: str) -> list[dict[str, object]]:
    return json.loads(Path(path).read_text(encoding="utf-8"))["aliases"]


def _load_rules(path: str) -> list[dict[str, object]]:
    return json.loads(Path(path).read_text(encoding="utf-8"))["patterns"]


def _categories_by_country(records: tuple[dict[str, object], ...]) -> dict[str, set[str]]:
    categories: dict[str, set[str]] = {}
    for record in records:
        metadata = record.get("metadata") or {}
        if not isinstance(metadata, dict):
            continue
        country_code = str(metadata.get("country_code") or "")
        category = str(metadata.get("category") or "")
        if country_code and category:
            categories.setdefault(country_code, set()).add(category)
    return categories


def test_phase6_overlays_cover_bdya_market_news_primitives() -> None:
    general_names = _canonical_names(BDYA_GENERAL_OVERLAY_ENTITIES)
    finance_names = _canonical_names(BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES)

    assert {"Strait of Hormuz", "Federal Reserve", "Reuters"} <= general_names
    assert {
        "United States Securities and Exchange Commission",
        "Financial Conduct Authority",
        "China Securities Regulatory Commission",
        "Securities and Exchange Board of India",
        "Capital Markets Board of Turkey",
        "Permian Basin",
        "North Sea",
        "Alberta oil sands",
        "Pilbara",
        "Atacama Desert",
        "Copperbelt",
    } <= general_names
    assert {
        "Diesel",
        "Copper",
        "Turkish lira",
        "Federal funds rate",
        "Semiconductors",
    } <= finance_names

    reuters = next(
        record
        for record in BDYA_GENERAL_OVERLAY_ENTITIES
        if record["canonical_text"] == "Reuters"
    )
    assert reuters["alias_quality"] == "source_outlet"
    assert reuters["runtime_tier"] == "search_only"
    assert reuters["weak_alias"] is True
    hormuz = next(
        record
        for record in BDYA_GENERAL_OVERLAY_ENTITIES
        if record["canonical_text"] == "Strait of Hormuz"
    )
    assert {"Hormuz", "Estreito de Ormuz", "Estreito de Hormuz"} <= set(
        hormuz["aliases"]
    )


def test_phase6_general_overlay_covers_g20_market_institutions_and_offices() -> None:
    categories_by_country = _categories_by_country(BDYA_GENERAL_OVERLAY_ENTITIES)

    for country_code in BDYA_G20_COUNTRY_CODES:
        assert set(BDYA_G20_REQUIRED_GENERAL_CATEGORIES) <= categories_by_country[
            country_code
        ]
        assert categories_by_country[country_code].intersection(
            BDYA_G20_TRADE_OR_CUSTOMS_CATEGORIES
        )

    office_roles_by_country: dict[str, set[str]] = {}
    for record in BDYA_G20_PUBLIC_OFFICE_ROLE_ENTITIES:
        metadata = record.get("metadata") or {}
        assert isinstance(metadata, dict)
        country_code = str(metadata["country_code"])
        office_roles_by_country.setdefault(country_code, set()).add(
            str(metadata["office_role"])
        )
        assert metadata["category"] == "public_office"
        assert metadata["current_official_refresh_required"] is True
        assert record["alias_quality"] == "strong"
        assert record["direct_mention_required"] is True
        assert "current_official_refresh_required" in record["quality_reasons"]

    assert set(office_roles_by_country) == set(BDYA_G20_COUNTRY_CODES)
    for country_code, office_roles in office_roles_by_country.items():
        assert set(BDYA_G20_OFFICIAL_ROLE_NAMES) == office_roles, country_code


def test_phase6_finance_overlay_has_direct_macro_commodity_coverage() -> None:
    commodities = _names_by_type(BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES, "commodity")
    currencies = _names_by_type(BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES, "currency")
    rates = _names_by_type(BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES, "rate")
    indexes = _names_by_type(BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES, "market_index")
    risks = _names_by_type(BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES, "risk")
    macros = _names_by_type(BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES, "macro_indicator")
    sectors = _names_by_type(BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES, "sector")

    assert {
        "Brent crude",
        "WTI crude",
        "Natural gas",
        "LNG",
        "Diesel",
        "Gasoline",
        "Heating oil",
        "Coal",
        "Uranium",
        "Copper",
        "Aluminum",
        "Iron ore",
        "Steel",
        "Nickel",
        "Lithium",
        "Cobalt",
        "Gold",
        "Silver",
        "Platinum",
        "Palladium",
        "Tungsten",
        "Rare earths",
        "Wheat",
        "Corn",
        "Soybeans",
        "Rice",
        "Coffee",
        "Cocoa",
        "Sugar",
        "Cotton",
        "Live cattle",
        "Lean hogs",
    } <= commodities
    assert {
        "US dollar",
        "Euro",
        "British pound",
        "Japanese yen",
        "Chinese yuan",
        "Turkish lira",
        "Brazilian real",
        "Indian rupee",
        "Canadian dollar",
        "Australian dollar",
        "Mexican peso",
        "South African rand",
        "Korean won",
        "Indonesian rupiah",
        "Saudi riyal",
        "Argentine peso",
        "Russian ruble",
    } <= currencies
    assert {
        "Federal funds rate",
        "PBOC loan prime rate",
        "BCRA policy rate",
        "US 10-year Treasury yield",
        "German 10-year bund yield",
    } <= rates
    assert {"S&P 500", "Nikkei 225", "BIST 100", "Jakarta Composite Index"} <= indexes
    assert {"Country risk", "Credit risk", "Sovereign spread risk"} <= risks
    assert {
        "Consumer Price Index",
        "Payrolls",
        "Debt ceiling",
        "DXY",
        "VIX",
        "Core inflation",
        "Current account",
    } <= macros
    assert {
        "Semiconductors",
        "Banks",
        "Utilities",
        "Real estate",
        "Technology",
        "Homebuilders",
    } <= sectors

    policies = _names_by_type(BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES, "policy")
    assert {
        "Energy reservation policy",
        "Production quota",
        "Price cap",
        "Windfall tax",
        "Mining royalties",
    } <= policies


def test_bdya_domain_source_bundles_generate_runtime_packs(tmp_path: Path) -> None:
    bundle_results = build_bdya_domain_source_bundles(output_dir=tmp_path / "bundles")
    assert {result.pack_id for result in bundle_results} == {
        "business-vector-en",
        "economics-vector-en",
        "politics-vector-en",
    }
    assert {spec.pack_id for spec in BDYA_DOMAIN_PACK_SPECS} == {
        result.pack_id for result in bundle_results
    }

    generated = {
        result.pack_id: generate_pack_source(
            result.bundle_dir,
            output_dir=tmp_path / "generated",
        )
        for result in bundle_results
    }

    business_aliases = _load_aliases(generated["business-vector-en"].aliases_path)
    economics_aliases = _load_aliases(generated["economics-vector-en"].aliases_path)
    politics_aliases = _load_aliases(generated["politics-vector-en"].aliases_path)

    spec_by_pack_id = {spec.pack_id: spec for spec in BDYA_DOMAIN_PACK_SPECS}
    assert len(spec_by_pack_id["business-vector-en"].entities) >= 25
    assert len(spec_by_pack_id["economics-vector-en"].entities) >= 25
    assert len(spec_by_pack_id["politics-vector-en"].entities) >= 25
    assert len(spec_by_pack_id["business-vector-en"].rules) >= 6
    assert len(spec_by_pack_id["economics-vector-en"].rules) >= 6
    assert len(spec_by_pack_id["politics-vector-en"].rules) >= 6

    assert any(
        alias["canonical_text"] == "Merger and acquisition"
        and alias["text"] == "takeover bid"
        for alias in business_aliases
    )
    assert any(alias["canonical_text"] == "Restructuring" for alias in business_aliases)
    assert any(alias["canonical_text"] == "Product recall" for alias in business_aliases)
    assert any(alias["canonical_text"] == "Initial public offering" for alias in business_aliases)
    assert any(alias["canonical_text"] == "Cybersecurity incident" for alias in business_aliases)
    assert any(alias["canonical_text"] == "Inflation" for alias in economics_aliases)
    assert any(alias["canonical_text"] == "Recession" for alias in economics_aliases)
    assert any(alias["canonical_text"] == "Housing market" for alias in economics_aliases)
    assert any(alias["canonical_text"] == "Currency intervention" for alias in economics_aliases)
    assert any(alias["canonical_text"] == "Sovereign debt" for alias in economics_aliases)
    assert any(
        alias["canonical_text"] == "Sanctions regime"
        for alias in politics_aliases
    )
    assert any(alias["canonical_text"] == "Treaty" for alias in politics_aliases)
    assert any(alias["canonical_text"] == "Election body" for alias in politics_aliases)
    assert any(alias["canonical_text"] == "Cabinet reshuffle" for alias in politics_aliases)
    assert any(alias["canonical_text"] == "Resource nationalism" for alias in politics_aliases)

    politics_rules = _load_rules(generated["politics-vector-en"].rules_path)
    assert any(rule["name"] == "sanctions_phrase" for rule in politics_rules)
    assert any(rule["name"] == "resource_policy_phrase" for rule in politics_rules)

    matcher = load_runtime_matcher(
        generated["business-vector-en"].matcher_artifact_path,
        generated["business-vector-en"].matcher_entries_path,
    )
    assert matcher.entry_count == generated["business-vector-en"].matcher_entry_count
    assert matcher.entry_payloads[0].runtime_tier.startswith("runtime_exact")

    non_market_text = (
        "The striker scored twice in the cup final while a reality TV actor "
        "shared wedding photos from a red-carpet magazine shoot."
    )
    for pack_result in generated.values():
        pack_matcher = load_runtime_matcher(
            pack_result.matcher_artifact_path,
            pack_result.matcher_entries_path,
        )
        assert find_exact_match_candidates(non_market_text, pack_matcher) == []
