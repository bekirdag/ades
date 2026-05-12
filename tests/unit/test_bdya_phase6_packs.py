import json
from pathlib import Path

from ades.packs.bdya_phase6 import (
    BDYA_DOMAIN_PACK_SPECS,
    BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES,
    BDYA_GENERAL_OVERLAY_ENTITIES,
    build_bdya_domain_source_bundles,
)
from ades.packs.generation import generate_pack_source
from ades.runtime_matcher import load_runtime_matcher


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
    assert {"Federal funds rate", "PBOC loan prime rate", "BCRA policy rate"} <= rates
    assert {"S&P 500", "Nikkei 225", "BIST 100", "Jakarta Composite Index"} <= indexes
    assert {"Country risk", "Credit risk", "Sovereign spread risk"} <= risks
    assert {"Consumer Price Index", "Payrolls", "Debt ceiling"} <= macros
    assert {"Semiconductors", "Banks", "Utilities", "Real estate"} <= sectors


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

    assert any(
        alias["canonical_text"] == "Merger and acquisition"
        and alias["text"] == "takeover bid"
        for alias in business_aliases
    )
    assert any(alias["canonical_text"] == "Restructuring" for alias in business_aliases)
    assert any(alias["canonical_text"] == "Product recall" for alias in business_aliases)
    assert any(alias["canonical_text"] == "Inflation" for alias in economics_aliases)
    assert any(alias["canonical_text"] == "Recession" for alias in economics_aliases)
    assert any(alias["canonical_text"] == "Housing market" for alias in economics_aliases)
    assert any(
        alias["canonical_text"] == "Sanctions regime"
        for alias in politics_aliases
    )
    assert any(alias["canonical_text"] == "Treaty" for alias in politics_aliases)
    assert any(alias["canonical_text"] == "Election body" for alias in politics_aliases)

    politics_rules = _load_rules(generated["politics-vector-en"].rules_path)
    assert any(rule["name"] == "sanctions_phrase" for rule in politics_rules)

    matcher = load_runtime_matcher(
        generated["business-vector-en"].matcher_artifact_path,
        generated["business-vector-en"].matcher_entries_path,
    )
    assert matcher.entry_count == generated["business-vector-en"].matcher_entry_count
    assert matcher.entry_payloads[0].runtime_tier.startswith("runtime_exact")
