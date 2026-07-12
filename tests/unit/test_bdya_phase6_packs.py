import json
from pathlib import Path
from urllib.parse import unquote, urlparse

from ades.impact.source_catalog import PROMOTION_ELIGIBLE_SOURCE_TIERS
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


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def _path_from_file_uri(value: str) -> Path:
    parsed = urlparse(value)
    assert parsed.scheme == "file"
    return Path(unquote(parsed.path))


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
        record for record in BDYA_GENERAL_OVERLAY_ENTITIES if record["canonical_text"] == "Reuters"
    )
    assert reuters["alias_quality"] == "source_outlet"
    assert reuters["runtime_tier"] == "search_only"
    assert reuters["weak_alias"] is True
    hormuz = next(
        record
        for record in BDYA_GENERAL_OVERLAY_ENTITIES
        if record["canonical_text"] == "Strait of Hormuz"
    )
    assert {"Hormuz", "Estreito de Ormuz", "Estreito de Hormuz"} <= set(hormuz["aliases"])


def test_phase6_general_overlay_covers_g20_market_institutions_and_offices() -> None:
    categories_by_country = _categories_by_country(BDYA_GENERAL_OVERLAY_ENTITIES)

    for country_code in BDYA_G20_COUNTRY_CODES:
        assert set(BDYA_G20_REQUIRED_GENERAL_CATEGORIES) <= categories_by_country[country_code]
        assert categories_by_country[country_code].intersection(
            BDYA_G20_TRADE_OR_CUSTOMS_CATEGORIES
        )

    office_roles_by_country: dict[str, set[str]] = {}
    for record in BDYA_G20_PUBLIC_OFFICE_ROLE_ENTITIES:
        metadata = record.get("metadata") or {}
        assert isinstance(metadata, dict)
        country_code = str(metadata["country_code"])
        office_roles_by_country.setdefault(country_code, set()).add(str(metadata["office_role"]))
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
    business_bundle = next(
        result for result in bundle_results if result.pack_id == "business-vector-en"
    )
    business_bundle_dir = Path(business_bundle.bundle_dir)
    business_bundle_manifest = json.loads(
        (business_bundle_dir / "bundle.json").read_text(encoding="utf-8")
    )
    business_sources_lock = json.loads(
        (business_bundle_dir / "sources.lock.json").read_text(encoding="utf-8")
    )
    business_entities = _load_jsonl(business_bundle_dir / "normalized" / "entities.jsonl")
    business_rules = _load_jsonl(business_bundle_dir / "normalized" / "rules.jsonl")
    economics_bundle = next(
        result for result in bundle_results if result.pack_id == "economics-vector-en"
    )
    economics_bundle_dir = Path(economics_bundle.bundle_dir)
    economics_bundle_manifest = json.loads(
        (economics_bundle_dir / "bundle.json").read_text(encoding="utf-8")
    )
    economics_sources_lock = json.loads(
        (economics_bundle_dir / "sources.lock.json").read_text(encoding="utf-8")
    )
    economics_entities = _load_jsonl(economics_bundle_dir / "normalized" / "entities.jsonl")
    economics_rules = _load_jsonl(economics_bundle_dir / "normalized" / "rules.jsonl")
    politics_bundle = next(
        result for result in bundle_results if result.pack_id == "politics-vector-en"
    )
    politics_bundle_dir = Path(politics_bundle.bundle_dir)
    politics_bundle_manifest = json.loads(
        (politics_bundle_dir / "bundle.json").read_text(encoding="utf-8")
    )
    politics_sources_lock = json.loads(
        (politics_bundle_dir / "sources.lock.json").read_text(encoding="utf-8")
    )
    politics_entities = _load_jsonl(politics_bundle_dir / "normalized" / "entities.jsonl")
    politics_rules = _load_jsonl(politics_bundle_dir / "normalized" / "rules.jsonl")

    spec_by_pack_id = {spec.pack_id: spec for spec in BDYA_DOMAIN_PACK_SPECS}
    assert len(spec_by_pack_id["business-vector-en"].entities) >= 25
    assert len(spec_by_pack_id["economics-vector-en"].entities) >= 25
    assert len(spec_by_pack_id["politics-vector-en"].entities) >= 25
    assert len(spec_by_pack_id["business-vector-en"].rules) >= 6
    assert len(spec_by_pack_id["economics-vector-en"].rules) >= 6
    assert len(spec_by_pack_id["politics-vector-en"].rules) >= 6

    assert any(
        alias["canonical_text"] == "Merger and acquisition" and alias["text"] == "takeover bid"
        for alias in business_aliases
    )
    assert any(alias["canonical_text"] == "Restructuring" for alias in business_aliases)
    assert any(alias["canonical_text"] == "Product recall" for alias in business_aliases)
    assert any(alias["canonical_text"] == "Initial public offering" for alias in business_aliases)
    assert any(alias["canonical_text"] == "Cybersecurity incident" for alias in business_aliases)
    assert generated["business-vector-en"].publishable_sources_only is True
    assert generated["business-vector-en"].source_count > 1
    assert business_bundle_manifest["tags"] == [
        "business",
        "bdya",
        "phase6",
        "source-backed",
        "english",
    ]
    assert business_sources_lock["source_count"] == len(business_bundle_manifest["sources"])
    lock_sources = {item["name"]: item for item in business_sources_lock["sources"]}
    assert "curated-business-bdya-phase6" not in lock_sources
    assert {
        "sec-form-8k",
        "sec-share-repurchase",
        "investor-gov-ipo",
        "dol-warn",
        "govinfo-reg-sk-103",
    } <= set(lock_sources)
    for source in lock_sources.values():
        assert source["source_tier"] in PROMOTION_ELIGIBLE_SOURCE_TIERS
        assert source["source_url"].startswith("https://")
        assert len(source["snapshot_sha256"]) == 64
        assert source["snapshot_size_bytes"] > 0
        assert source["adapter"] == "bdya_business_source_family_evidence"
        snapshot = json.loads(
            _path_from_file_uri(source["snapshot_uri"]).read_text(encoding="utf-8")
        )
        assert snapshot["source_tier"] == source["source_tier"]
        assert snapshot["source_url"] == source["source_url"]
        assert snapshot["record_count"] == source["record_count"]
        assert snapshot["records"]
        assert all("source_record_id" in item for item in snapshot["records"])
        assert all("source_span" in item for item in snapshot["records"])
        assert all("normalized_record_sha256" in item for item in snapshot["records"])

    source_backed_entities = [
        item
        for item in business_entities
        if item.get("promotion_status") == "source_backed_candidate"
    ]
    shadow_entities = [
        item for item in business_entities if item.get("promotion_status") == "shadow_only"
    ]
    assert len(source_backed_entities) == 19
    assert len(shadow_entities) == 8
    assert all(item.get("build_only") is True for item in shadow_entities)
    assert all(
        item.get("source_tier") in PROMOTION_ELIGIBLE_SOURCE_TIERS
        for item in source_backed_entities
    )
    assert all(item.get("source_record_id") for item in source_backed_entities)
    assert all(item.get("source_span") for item in source_backed_entities)
    assert len(business_rules) == 9
    assert all(
        item.get("source_tier") in PROMOTION_ELIGIBLE_SOURCE_TIERS for item in business_rules
    )
    assert all(item.get("source_record_id") for item in business_rules)
    assert all(item.get("source_span") for item in business_rules)

    business_alias_texts = {str(alias["text"]) for alias in business_aliases}
    assert {
        "M&A",
        "IPO",
        "recall",
        "walkout",
        "lawsuit",
        "court ruling",
        "strategic partnership",
    }.isdisjoint(business_alias_texts)
    shadow_canonical_texts = {str(item["canonical_text"]) for item in shadow_entities}
    assert all(
        str(alias["canonical_text"]) not in shadow_canonical_texts for alias in business_aliases
    )
    assert any(alias["canonical_text"] == "Inflation" for alias in economics_aliases)
    assert any(alias["canonical_text"] == "Economic growth" for alias in economics_aliases)
    assert any(alias["canonical_text"] == "Housing market" for alias in economics_aliases)
    assert any(alias["canonical_text"] == "Currency intervention" for alias in economics_aliases)
    assert any(alias["canonical_text"] == "Sovereign debt" for alias in economics_aliases)
    assert generated["economics-vector-en"].publishable_sources_only is True
    assert generated["economics-vector-en"].source_count == 9
    assert generated["economics-vector-en"].included_entity_count == 23
    assert generated["economics-vector-en"].included_rule_count == 13
    assert economics_bundle_manifest["tags"] == [
        "economics",
        "bdya",
        "phase6",
        "source-backed",
        "english",
    ]
    assert economics_sources_lock["source_count"] == len(economics_bundle_manifest["sources"])
    economics_lock_sources = {item["name"]: item for item in economics_sources_lock["sources"]}
    assert "curated-economics-bdya-phase6" not in economics_lock_sources
    assert set(economics_lock_sources) == {
        "bea-national-accounts-glossary",
        "bls-price-labor-glossaries",
        "census-economic-indicators",
        "federal-reserve-policy-statistics",
        "fred-yield-curve",
        "imf-bop-fiscal-glossary",
        "treasury-fiscaldata",
        "treasurydirect-auctions",
        "wto-tariff-trade-glossary",
    }
    for source in economics_lock_sources.values():
        assert source["source_tier"] in PROMOTION_ELIGIBLE_SOURCE_TIERS
        assert source["source_url"].startswith("https://")
        assert len(source["snapshot_sha256"]) == 64
        assert source["snapshot_size_bytes"] > 0
        assert source["adapter"] == "bdya_economics_source_family_evidence"
        snapshot = json.loads(
            _path_from_file_uri(source["snapshot_uri"]).read_text(encoding="utf-8")
        )
        assert snapshot["source_tier"] == source["source_tier"]
        assert snapshot["source_url"] == source["source_url"]
        assert snapshot["record_count"] == source["record_count"]
        assert snapshot["records"]
        assert all("source_record_id" in item for item in snapshot["records"])
        assert all("source_span" in item for item in snapshot["records"])
        assert all("normalized_record_sha256" in item for item in snapshot["records"])

    economics_source_backed_entities = [
        item
        for item in economics_entities
        if item.get("promotion_status") == "source_backed_candidate"
    ]
    economics_shadow_entities = [
        item for item in economics_entities if item.get("promotion_status") == "shadow_only"
    ]
    assert len(economics_source_backed_entities) == 23
    assert len(economics_shadow_entities) == 5
    assert all(item.get("build_only") is True for item in economics_shadow_entities)
    assert all(
        item.get("source_tier") in PROMOTION_ELIGIBLE_SOURCE_TIERS
        for item in economics_source_backed_entities
    )
    assert all(item.get("source_record_id") for item in economics_source_backed_entities)
    assert all(item.get("source_span") for item in economics_source_backed_entities)
    assert len(economics_rules) == 13
    assert all(
        item.get("source_tier") in PROMOTION_ELIGIBLE_SOURCE_TIERS for item in economics_rules
    )
    assert all(item.get("source_record_id") for item in economics_rules)
    assert all(item.get("source_span") for item in economics_rules)

    economics_alias_texts = {str(alias["text"]) for alias in economics_aliases}
    assert {
        "price pressures",
        "growth outlook",
        "downturn",
        "rate policy",
        "budget plan",
        "jobs report",
        "home prices",
        "mortgage rates",
        "supply-chain bottleneck",
        "logistics bottleneck",
        "confidence index",
        "currency depreciation",
        "spending cuts",
        "liquidity growth",
    }.isdisjoint(economics_alias_texts)
    economics_shadow_canonical_texts = {
        str(item["canonical_text"]) for item in economics_shadow_entities
    }
    assert all(
        str(alias["canonical_text"]) not in economics_shadow_canonical_texts
        for alias in economics_aliases
    )
    assert generated["politics-vector-en"].publishable_sources_only is True
    assert generated["politics-vector-en"].source_count == 20
    assert generated["politics-vector-en"].included_entity_count == 32
    assert generated["politics-vector-en"].included_rule_count == 12
    assert politics_bundle_manifest["tags"] == [
        "politics",
        "bdya",
        "phase6",
        "source-backed",
        "english",
    ]
    assert politics_sources_lock["source_count"] == len(politics_bundle_manifest["sources"])
    politics_lock_sources = {item["name"]: item for item in politics_sources_lock["sources"]}
    assert "curated-politics-bdya-phase6" not in politics_lock_sources
    assert set(politics_lock_sources) == {
        "bis-entity-list-ear",
        "cbp-border-restrictions",
        "cia-world-leaders",
        "eac-election-administration",
        "fema-disaster-declarations",
        "govuk-ministerial-appointments",
        "iea-energy-policies",
        "imo-maritime-security",
        "nato-defense-expenditure",
        "ofac-sanctions-list-service",
        "opm-shutdown-furlough",
        "state-department-diplomatic-history",
        "uk-electoral-commission-registers",
        "uk-parliament-confidence",
        "un-peacemaker-agreements",
        "un-security-council-consolidated-list",
        "un-treaty-collection",
        "usgs-mineral-commodity-summaries",
        "ustr-trade-agreements",
        "wto-regional-trade-agreements",
    }
    for source in politics_lock_sources.values():
        assert source["source_tier"] in PROMOTION_ELIGIBLE_SOURCE_TIERS
        assert source["source_url"].startswith("https://")
        assert len(source["snapshot_sha256"]) == 64
        assert source["snapshot_size_bytes"] > 0
        assert source["adapter"] == "bdya_politics_source_family_evidence"
        snapshot = json.loads(
            _path_from_file_uri(source["snapshot_uri"]).read_text(encoding="utf-8")
        )
        assert snapshot["source_tier"] == source["source_tier"]
        assert snapshot["source_url"] == source["source_url"]
        assert snapshot["record_count"] == source["record_count"]
        assert snapshot["records"]
        assert all("source_record_id" in item for item in snapshot["records"])
        assert all("source_span" in item for item in snapshot["records"])
        assert all("normalized_record_sha256" in item for item in snapshot["records"])

    politics_source_backed_entities = [
        item
        for item in politics_entities
        if item.get("promotion_status") == "source_backed_candidate"
    ]
    politics_shadow_entities = [
        item for item in politics_entities if item.get("promotion_status") == "shadow_only"
    ]
    assert len(politics_source_backed_entities) == 32
    assert len(politics_shadow_entities) == 5
    assert all(item.get("build_only") is True for item in politics_shadow_entities)
    assert all(
        item.get("source_tier") in PROMOTION_ELIGIBLE_SOURCE_TIERS
        for item in politics_source_backed_entities
    )
    assert all(item.get("source_record_id") for item in politics_source_backed_entities)
    assert all(item.get("source_span") for item in politics_source_backed_entities)
    assert len(politics_rules) == 12
    assert all(
        item.get("source_tier") in PROMOTION_ELIGIBLE_SOURCE_TIERS for item in politics_rules
    )
    assert all(item.get("source_record_id") for item in politics_rules)
    assert all(item.get("source_span") for item in politics_rules)

    assert any(alias["canonical_text"] == "Sanctions regime" for alias in politics_aliases)
    assert any(alias["canonical_text"] == "Treaty" for alias in politics_aliases)
    assert any(alias["canonical_text"] == "Election body" for alias in politics_aliases)
    assert any(alias["canonical_text"] == "Cabinet reshuffle" for alias in politics_aliases)
    assert any(alias["canonical_text"] == "Maritime security" for alias in politics_aliases)
    assert any(alias["canonical_text"] == "Mining policy" for alias in politics_aliases)
    assert {
        "wikidata:Q7184",
        "wikidata:Q1065",
        "wikidata:Q789915",
        "wikidata:Q455289",
        "wikidata:Q11010",
        "wikidata:Q20647202",
        "wikidata:Q826700",
        "wikidata:Q193755",
    } <= {
        str(item["entity_id"])
        for item in politics_source_backed_entities
        if item.get("entity_type") == "organization"
    }
    politics_alias_texts = {str(alias["text"]) for alias in politics_aliases}
    assert {
        "administration",
        "border",
        "cabinet",
        "chokepoint",
        "coalition talks",
        "diplomatic dispute",
        "government crackdown",
        "military escalation",
        "missile",
        "nationalization threat",
        "naval patrols",
        "opposition party",
        "regulatory probe",
        "resource tax",
        "risk relief",
        "ruling party",
        "sanction",
        "security crisis",
        "shipping security",
        "war escalation",
    }.isdisjoint(politics_alias_texts)
    politics_shadow_canonical_texts = {
        str(item["canonical_text"]) for item in politics_shadow_entities
    }
    assert all(
        str(alias["canonical_text"]) not in politics_shadow_canonical_texts
        for alias in politics_aliases
    )

    politics_generated_rules = _load_rules(generated["politics-vector-en"].rules_path)
    assert any(rule["name"] == "sanctions_phrase" for rule in politics_generated_rules)
    assert any(rule["name"] == "resource_policy_phrase" for rule in politics_generated_rules)

    matcher = load_runtime_matcher(
        generated["business-vector-en"].matcher_artifact_path,
        generated["business-vector-en"].matcher_entries_path,
    )
    assert matcher.entry_count == generated["business-vector-en"].matcher_entry_count
    assert matcher.entry_payloads[0].runtime_tier.startswith("runtime_exact")
    assert (
        find_exact_match_candidates(
            (
                "Please recall the IPO and M&A notes before the actor discusses "
                "a walkout, a lawsuit, a court ruling, and a strategic partnership."
            ),
            matcher,
        )
        == []
    )
    economics_matcher = load_runtime_matcher(
        generated["economics-vector-en"].matcher_artifact_path,
        generated["economics-vector-en"].matcher_entries_path,
    )
    assert (
        find_exact_match_candidates(
            (
                "The feature mentioned price pressures, a growth outlook, a downturn, "
                "rate policy, a budget plan, home prices, mortgage rates, private "
                "mortgage insurance PMI, currency depreciation, spending cuts, "
                "and liquidity growth."
            ),
            economics_matcher,
        )
        == []
    )
    politics_matcher = load_runtime_matcher(
        generated["politics-vector-en"].matcher_artifact_path,
        generated["politics-vector-en"].matcher_entries_path,
    )
    assert (
        find_exact_match_candidates(
            (
                "The article mentioned a kitchen cabinet, a network border, shipping "
                "security software, a survey poll, sanction approval, coalition talks, "
                "diplomatic dispute, and a resource tax debate."
            ),
            politics_matcher,
        )
        == []
    )

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
