"""Curated BDYA market-news pack overlays and domain bundle builders."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import shutil
from typing import Any

from .source_lock import build_bundle_source_entry, write_sources_lock


def _entity(
    entity_type: str,
    canonical_text: str,
    *,
    aliases: tuple[str, ...] = (),
    entity_id: str | None = None,
    source_name: str | None = None,
    metadata: dict[str, object] | None = None,
    alias_quality: str | None = None,
    runtime_tier: str | None = None,
    weak_alias: bool | None = None,
    direct_mention_required: bool | None = None,
    quality_reasons: tuple[str, ...] = (),
) -> dict[str, object]:
    record: dict[str, object] = {
        "entity_type": entity_type,
        "canonical_text": canonical_text,
        "aliases": list(aliases),
    }
    if entity_id:
        record["entity_id"] = entity_id
    if source_name:
        record["source_name"] = source_name
    if metadata:
        record["metadata"] = dict(metadata)
    if alias_quality:
        record["alias_quality"] = alias_quality
    if runtime_tier:
        record["runtime_tier"] = runtime_tier
    if weak_alias is not None:
        record["weak_alias"] = bool(weak_alias)
    if direct_mention_required is not None:
        record["direct_mention_required"] = bool(direct_mention_required)
    if quality_reasons:
        record["quality_reasons"] = list(quality_reasons)
    return record


BDYA_GENERAL_OVERLAY_ENTITIES: tuple[dict[str, object], ...] = (
    _entity(
        "location",
        "Strait of Hormuz",
        aliases=("Hormuz Strait", "Hormuz chokepoint"),
        entity_id="ades:market-location:strait-of-hormuz",
        metadata={"category": "strategic_chokepoint", "asset_family": "energy"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Suez Canal",
        aliases=("Suez chokepoint",),
        entity_id="ades:market-location:suez-canal",
        metadata={"category": "strategic_chokepoint", "asset_family": "shipping"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Panama Canal",
        aliases=("Panama chokepoint",),
        entity_id="ades:market-location:panama-canal",
        metadata={"category": "strategic_chokepoint", "asset_family": "shipping"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Bosphorus",
        aliases=("Bosporus", "Turkish Straits"),
        entity_id="ades:market-location:bosphorus",
        metadata={"category": "strategic_chokepoint", "asset_family": "energy_shipping"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Taiwan Strait",
        aliases=("Taiwan chokepoint",),
        entity_id="ades:market-location:taiwan-strait",
        metadata={"category": "strategic_chokepoint", "asset_family": "semiconductors"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Red Sea",
        aliases=("Red Sea shipping lane",),
        entity_id="ades:market-location:red-sea",
        metadata={"category": "strategic_chokepoint", "asset_family": "shipping"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Bab el-Mandeb Strait",
        aliases=("Bab al-Mandab", "Bab el Mandeb"),
        entity_id="ades:market-location:bab-el-mandeb",
        metadata={"category": "strategic_chokepoint", "asset_family": "shipping"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "South China Sea",
        aliases=("South China Sea shipping lane",),
        entity_id="ades:market-location:south-china-sea",
        metadata={"category": "strategic_chokepoint", "asset_family": "shipping"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Black Sea",
        aliases=("Black Sea grain corridor",),
        entity_id="ades:market-location:black-sea",
        metadata={"category": "strategic_chokepoint", "asset_family": "grain_energy"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Gulf of Mexico",
        aliases=("US Gulf", "Gulf Coast"),
        entity_id="ades:market-location:gulf-of-mexico",
        metadata={"category": "commodity_region", "asset_family": "energy"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Permian Basin",
        aliases=("Permian oil basin",),
        entity_id="ades:market-location:permian-basin",
        metadata={"category": "commodity_region", "asset_family": "energy"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "North Sea",
        aliases=("North Sea oil",),
        entity_id="ades:market-location:north-sea",
        metadata={"category": "commodity_region", "asset_family": "energy"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Alberta oil sands",
        aliases=("oil sands", "Canadian oil sands"),
        entity_id="ades:market-location:alberta-oil-sands",
        metadata={"category": "commodity_region", "asset_family": "energy"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Pilbara",
        aliases=("Pilbara iron ore region",),
        entity_id="ades:market-location:pilbara",
        metadata={"category": "commodity_region", "asset_family": "iron_ore"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Atacama Desert",
        aliases=("Atacama lithium region",),
        entity_id="ades:market-location:atacama",
        metadata={"category": "commodity_region", "asset_family": "lithium_copper"},
        alias_quality="strong",
    ),
    _entity(
        "location",
        "Copperbelt",
        aliases=("Central African Copperbelt",),
        entity_id="ades:market-location:copperbelt",
        metadata={"category": "commodity_region", "asset_family": "copper_cobalt"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Federal Reserve",
        aliases=("Fed", "Federal Reserve System", "FOMC"),
        entity_id="ades:policy-body:us:federal-reserve",
        metadata={"category": "central_bank", "country_code": "us"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "European Central Bank",
        aliases=("ECB",),
        entity_id="ades:policy-body:eu:european-central-bank",
        metadata={"category": "central_bank", "country_code": "eu"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Bank of England",
        aliases=("BoE",),
        entity_id="ades:policy-body:uk:bank-of-england",
        metadata={"category": "central_bank", "country_code": "uk"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Bank of Japan",
        aliases=("BOJ",),
        entity_id="ades:policy-body:jp:bank-of-japan",
        metadata={"category": "central_bank", "country_code": "jp"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "People's Bank of China",
        aliases=("PBOC", "People's Bank"),
        entity_id="ades:policy-body:cn:peoples-bank-of-china",
        metadata={"category": "central_bank", "country_code": "cn"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Central Bank of the Republic of Turkey",
        aliases=("CBRT", "Turkish central bank"),
        entity_id="ades:policy-body:tr:cbrt",
        metadata={"category": "central_bank", "country_code": "tr"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Central Bank of Brazil",
        aliases=("BCB", "Banco Central do Brasil"),
        entity_id="ades:policy-body:br:central-bank-of-brazil",
        metadata={"category": "central_bank", "country_code": "br"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Reserve Bank of India",
        aliases=("RBI",),
        entity_id="ades:policy-body:in:reserve-bank-of-india",
        metadata={"category": "central_bank", "country_code": "in"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Bank of Canada",
        aliases=("BoC",),
        entity_id="ades:policy-body:ca:bank-of-canada",
        metadata={"category": "central_bank", "country_code": "ca"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Bank of Korea",
        aliases=("BOK",),
        entity_id="ades:policy-body:kr:bank-of-korea",
        metadata={"category": "central_bank", "country_code": "kr"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "US Treasury",
        aliases=("United States Treasury", "Treasury Department"),
        entity_id="ades:policy-body:us:treasury",
        metadata={"category": "finance_ministry", "country_code": "us"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Office of Foreign Assets Control",
        aliases=("OFAC",),
        entity_id="ades:policy-body:us:ofac",
        metadata={"category": "sanctions_body", "country_code": "us"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "International Monetary Fund",
        aliases=("IMF",),
        entity_id="ades:policy-body:global:imf",
        metadata={"category": "multilateral_finance"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "World Bank",
        aliases=(),
        entity_id="ades:policy-body:global:world-bank",
        metadata={"category": "multilateral_finance"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "World Trade Organization",
        aliases=("WTO",),
        entity_id="ades:policy-body:global:wto",
        metadata={"category": "trade_body"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "European Commission",
        aliases=("EU Commission",),
        entity_id="ades:policy-body:eu:european-commission",
        metadata={"category": "trade_policy_body", "country_code": "eu"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "United States Securities and Exchange Commission",
        aliases=("SEC", "US SEC"),
        entity_id="ades:regulator:us:sec",
        metadata={"category": "securities_regulator", "country_code": "us"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Commodity Futures Trading Commission",
        aliases=("CFTC",),
        entity_id="ades:regulator:us:cftc",
        metadata={"category": "derivatives_regulator", "country_code": "us"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Office of the United States Trade Representative",
        aliases=("USTR", "US Trade Representative"),
        entity_id="ades:policy-body:us:ustr",
        metadata={"category": "trade_body", "country_code": "us"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "United States Customs and Border Protection",
        aliases=("CBP", "US Customs and Border Protection"),
        entity_id="ades:policy-body:us:customs-border-protection",
        metadata={"category": "customs_body", "country_code": "us"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "HM Treasury",
        aliases=("UK Treasury",),
        entity_id="ades:policy-body:uk:hm-treasury",
        metadata={"category": "finance_ministry", "country_code": "uk"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Financial Conduct Authority",
        aliases=("FCA", "UK FCA"),
        entity_id="ades:regulator:uk:fca",
        metadata={"category": "securities_regulator", "country_code": "uk"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "London Stock Exchange",
        aliases=("LSE",),
        entity_id="ades:exchange:uk:london-stock-exchange",
        metadata={"category": "exchange", "country_code": "uk"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Ministry of Finance Japan",
        aliases=("Japan Ministry of Finance", "MOF Japan"),
        entity_id="ades:policy-body:jp:ministry-of-finance",
        metadata={"category": "finance_ministry", "country_code": "jp"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Financial Services Agency Japan",
        aliases=("Japan FSA",),
        entity_id="ades:regulator:jp:fsa",
        metadata={"category": "securities_regulator", "country_code": "jp"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Tokyo Stock Exchange",
        aliases=("TSE",),
        entity_id="ades:exchange:jp:tokyo-stock-exchange",
        metadata={"category": "exchange", "country_code": "jp"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "China Securities Regulatory Commission",
        aliases=("CSRC",),
        entity_id="ades:regulator:cn:csrc",
        metadata={"category": "securities_regulator", "country_code": "cn"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "General Administration of Customs of China",
        aliases=("China Customs", "GACC"),
        entity_id="ades:policy-body:cn:gacc",
        metadata={"category": "customs_body", "country_code": "cn"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Shanghai Stock Exchange",
        aliases=("SSE",),
        entity_id="ades:exchange:cn:shanghai-stock-exchange",
        metadata={"category": "exchange", "country_code": "cn"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Shenzhen Stock Exchange",
        aliases=("SZSE",),
        entity_id="ades:exchange:cn:shenzhen-stock-exchange",
        metadata={"category": "exchange", "country_code": "cn"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Federal Ministry of Finance Germany",
        aliases=("German Finance Ministry", "BMF Germany"),
        entity_id="ades:policy-body:de:finance-ministry",
        metadata={"category": "finance_ministry", "country_code": "de"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Federal Financial Supervisory Authority",
        aliases=("BaFin",),
        entity_id="ades:regulator:de:bafin",
        metadata={"category": "securities_regulator", "country_code": "de"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Deutsche Boerse",
        aliases=("Deutsche Boerse", "Frankfurt Stock Exchange"),
        entity_id="ades:exchange:de:deutsche-boerse",
        metadata={"category": "exchange", "country_code": "de"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "French Ministry for the Economy and Finance",
        aliases=("French Finance Ministry", "Bercy"),
        entity_id="ades:policy-body:fr:economy-finance-ministry",
        metadata={"category": "finance_ministry", "country_code": "fr"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Autorite des marches financiers",
        aliases=("AMF France", "Autorite des marches financiers"),
        entity_id="ades:regulator:fr:amf",
        metadata={"category": "securities_regulator", "country_code": "fr"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Euronext Paris",
        aliases=("Paris stock exchange",),
        entity_id="ades:exchange:fr:euronext-paris",
        metadata={"category": "exchange", "country_code": "fr"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Ministry of Economy and Finance Italy",
        aliases=("Italian Treasury", "MEF Italy"),
        entity_id="ades:policy-body:it:mef",
        metadata={"category": "finance_ministry", "country_code": "it"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Commissione Nazionale per le Societa e la Borsa",
        aliases=("CONSOB",),
        entity_id="ades:regulator:it:consob",
        metadata={"category": "securities_regulator", "country_code": "it"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Borsa Italiana",
        aliases=("Italian stock exchange",),
        entity_id="ades:exchange:it:borsa-italiana",
        metadata={"category": "exchange", "country_code": "it"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Brazil Ministry of Finance",
        aliases=("Ministry of Finance Brazil",),
        entity_id="ades:policy-body:br:finance-ministry",
        metadata={"category": "finance_ministry", "country_code": "br"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Comissao de Valores Mobiliarios",
        aliases=("CVM", "CVM Brazil"),
        entity_id="ades:regulator:br:cvm",
        metadata={"category": "securities_regulator", "country_code": "br"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "B3",
        aliases=("Brasil Bolsa Balcao", "B3 exchange"),
        entity_id="ades:exchange:br:b3",
        metadata={"category": "exchange", "country_code": "br"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Ministry of Finance India",
        aliases=("Indian Finance Ministry",),
        entity_id="ades:policy-body:in:finance-ministry",
        metadata={"category": "finance_ministry", "country_code": "in"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Securities and Exchange Board of India",
        aliases=("SEBI",),
        entity_id="ades:regulator:in:sebi",
        metadata={"category": "securities_regulator", "country_code": "in"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "National Stock Exchange of India",
        aliases=("NSE India",),
        entity_id="ades:exchange:in:nse",
        metadata={"category": "exchange", "country_code": "in"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Finance Canada",
        aliases=("Department of Finance Canada",),
        entity_id="ades:policy-body:ca:finance-canada",
        metadata={"category": "finance_ministry", "country_code": "ca"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Canadian Securities Administrators",
        aliases=("CSA Canada",),
        entity_id="ades:regulator:ca:csa",
        metadata={"category": "securities_regulator", "country_code": "ca"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "TMX Group",
        aliases=("Toronto Stock Exchange", "TSX"),
        entity_id="ades:exchange:ca:tmx",
        metadata={"category": "exchange", "country_code": "ca"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Australian Treasury",
        aliases=("Treasury Australia",),
        entity_id="ades:policy-body:au:treasury",
        metadata={"category": "finance_ministry", "country_code": "au"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Reserve Bank of Australia",
        aliases=("RBA",),
        entity_id="ades:policy-body:au:reserve-bank-of-australia",
        metadata={"category": "central_bank", "country_code": "au"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Australian Securities and Investments Commission",
        aliases=("ASIC",),
        entity_id="ades:regulator:au:asic",
        metadata={"category": "securities_regulator", "country_code": "au"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Australian Securities Exchange",
        aliases=("ASX",),
        entity_id="ades:exchange:au:asx",
        metadata={"category": "exchange", "country_code": "au"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Bank of Mexico",
        aliases=("Banxico",),
        entity_id="ades:policy-body:mx:bank-of-mexico",
        metadata={"category": "central_bank", "country_code": "mx"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Secretariat of Finance and Public Credit",
        aliases=("SHCP", "Mexico Finance Ministry"),
        entity_id="ades:policy-body:mx:shcp",
        metadata={"category": "finance_ministry", "country_code": "mx"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Comision Nacional Bancaria y de Valores",
        aliases=("CNBV",),
        entity_id="ades:regulator:mx:cnbv",
        metadata={"category": "securities_regulator", "country_code": "mx"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Bolsa Mexicana de Valores",
        aliases=("BMV", "Mexican Stock Exchange"),
        entity_id="ades:exchange:mx:bmv",
        metadata={"category": "exchange", "country_code": "mx"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "South African Reserve Bank",
        aliases=("SARB",),
        entity_id="ades:policy-body:za:sarb",
        metadata={"category": "central_bank", "country_code": "za"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "National Treasury South Africa",
        aliases=("South African Treasury",),
        entity_id="ades:policy-body:za:national-treasury",
        metadata={"category": "finance_ministry", "country_code": "za"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Financial Sector Conduct Authority",
        aliases=("FSCA",),
        entity_id="ades:regulator:za:fsca",
        metadata={"category": "securities_regulator", "country_code": "za"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Johannesburg Stock Exchange",
        aliases=("JSE",),
        entity_id="ades:exchange:za:jse",
        metadata={"category": "exchange", "country_code": "za"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Financial Services Commission Korea",
        aliases=("FSC Korea",),
        entity_id="ades:regulator:kr:fsc",
        metadata={"category": "securities_regulator", "country_code": "kr"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Financial Supervisory Service",
        aliases=("FSS Korea",),
        entity_id="ades:regulator:kr:fss",
        metadata={"category": "securities_regulator", "country_code": "kr"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Korea Exchange",
        aliases=("KRX",),
        entity_id="ades:exchange:kr:krx",
        metadata={"category": "exchange", "country_code": "kr"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Bank Indonesia",
        aliases=("BI",),
        entity_id="ades:policy-body:id:bank-indonesia",
        metadata={"category": "central_bank", "country_code": "id"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Financial Services Authority Indonesia",
        aliases=("OJK",),
        entity_id="ades:regulator:id:ojk",
        metadata={"category": "securities_regulator", "country_code": "id"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Indonesia Stock Exchange",
        aliases=("IDX",),
        entity_id="ades:exchange:id:idx",
        metadata={"category": "exchange", "country_code": "id"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Saudi Central Bank",
        aliases=("SAMA",),
        entity_id="ades:policy-body:sa:central-bank",
        metadata={"category": "central_bank", "country_code": "sa"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Saudi Ministry of Finance",
        aliases=("Ministry of Finance Saudi Arabia",),
        entity_id="ades:policy-body:sa:finance-ministry",
        metadata={"category": "finance_ministry", "country_code": "sa"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Capital Market Authority Saudi Arabia",
        aliases=("CMA Saudi Arabia",),
        entity_id="ades:regulator:sa:cma",
        metadata={"category": "securities_regulator", "country_code": "sa"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Saudi Exchange",
        aliases=("Tadawul",),
        entity_id="ades:exchange:sa:tadawul",
        metadata={"category": "exchange", "country_code": "sa"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Central Bank of Argentina",
        aliases=("BCRA",),
        entity_id="ades:policy-body:ar:central-bank",
        metadata={"category": "central_bank", "country_code": "ar"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Ministry of Economy Argentina",
        aliases=("Argentina Economy Ministry",),
        entity_id="ades:policy-body:ar:economy-ministry",
        metadata={"category": "finance_ministry", "country_code": "ar"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Comision Nacional de Valores Argentina",
        aliases=("CNV Argentina",),
        entity_id="ades:regulator:ar:cnv",
        metadata={"category": "securities_regulator", "country_code": "ar"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Bolsas y Mercados Argentinos",
        aliases=("BYMA",),
        entity_id="ades:exchange:ar:byma",
        metadata={"category": "exchange", "country_code": "ar"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Bank of Russia",
        aliases=("Central Bank of Russia",),
        entity_id="ades:policy-body:ru:bank-of-russia",
        metadata={"category": "central_bank", "country_code": "ru"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Ministry of Finance Russia",
        aliases=("Russian Finance Ministry",),
        entity_id="ades:policy-body:ru:finance-ministry",
        metadata={"category": "finance_ministry", "country_code": "ru"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Moscow Exchange",
        aliases=("MOEX",),
        entity_id="ades:exchange:ru:moex",
        metadata={"category": "exchange", "country_code": "ru"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Turkish Ministry of Treasury and Finance",
        aliases=("Turkey Treasury and Finance Ministry",),
        entity_id="ades:policy-body:tr:treasury-finance-ministry",
        metadata={"category": "finance_ministry", "country_code": "tr"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Capital Markets Board of Turkey",
        aliases=("CMB Turkey", "SPK"),
        entity_id="ades:regulator:tr:cmb",
        metadata={"category": "securities_regulator", "country_code": "tr"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Borsa Istanbul",
        aliases=("BIST",),
        entity_id="ades:exchange:tr:borsa-istanbul",
        metadata={"category": "exchange", "country_code": "tr"},
        alias_quality="strong",
    ),
    _entity(
        "organization",
        "Reuters",
        aliases=("Reuters News",),
        entity_id="ades:source-outlet:reuters",
        metadata={"category": "source_outlet"},
        alias_quality="source_outlet",
        runtime_tier="search_only",
        weak_alias=True,
        quality_reasons=("source_outlet",),
    ),
    _entity(
        "organization",
        "Associated Press",
        aliases=("AP",),
        entity_id="ades:source-outlet:associated-press",
        metadata={"category": "source_outlet"},
        alias_quality="source_outlet",
        runtime_tier="search_only",
        weak_alias=True,
        quality_reasons=("source_outlet",),
    ),
    _entity(
        "organization",
        "France 24",
        aliases=("France24",),
        entity_id="ades:source-outlet:france-24",
        metadata={"category": "source_outlet"},
        alias_quality="source_outlet",
        runtime_tier="search_only",
        weak_alias=True,
        quality_reasons=("source_outlet",),
    ),
)


BDYA_FINANCE_MARKET_ONTOLOGY_ENTITIES: tuple[dict[str, object], ...] = (
    _entity("commodity", "Brent crude", aliases=("Brent oil", "Brent futures", "Brent crude futures"), entity_id="ades:impact:commodity:brent-crude", metadata={"category": "energy_benchmark"}),
    _entity("commodity", "WTI crude", aliases=("WTI oil", "West Texas Intermediate", "WTI crude futures"), entity_id="ades:impact:commodity:wti-crude", metadata={"category": "energy_benchmark"}),
    _entity("commodity", "Natural gas", aliases=("natural gas futures", "Henry Hub gas", "US natural gas"), entity_id="ades:impact:commodity:natural-gas", metadata={"category": "energy_benchmark"}),
    _entity("commodity", "LNG", aliases=("liquefied natural gas", "LNG cargoes"), entity_id="ades:impact:commodity:lng", metadata={"category": "energy_product"}),
    _entity("commodity", "Diesel", aliases=("diesel futures",), entity_id="ades:impact:commodity:diesel", metadata={"category": "energy_product"}),
    _entity("commodity", "Gasoline", aliases=("RBOB gasoline", "gasoline futures"), entity_id="ades:impact:commodity:gasoline", metadata={"category": "energy_product"}),
    _entity("commodity", "Heating oil", aliases=("heating oil futures",), entity_id="ades:impact:commodity:heating-oil", metadata={"category": "energy_product"}),
    _entity("commodity", "Coal", aliases=("thermal coal", "coal futures"), entity_id="ades:impact:commodity:coal", metadata={"category": "energy_product"}),
    _entity("commodity", "Uranium", aliases=("uranium spot", "uranium futures"), entity_id="ades:impact:commodity:uranium", metadata={"category": "energy_metal"}),
    _entity("commodity", "Copper", aliases=("LME copper", "copper futures"), entity_id="ades:impact:commodity:copper", metadata={"category": "industrial_metal"}),
    _entity("commodity", "Aluminum", aliases=("aluminium", "LME aluminum", "aluminum futures"), entity_id="ades:impact:commodity:aluminum", metadata={"category": "industrial_metal"}),
    _entity("commodity", "Iron ore", aliases=("iron ore futures", "iron ore benchmark"), entity_id="ades:impact:commodity:iron-ore", metadata={"category": "industrial_metal"}),
    _entity("commodity", "Steel", aliases=("steel rebar", "steel futures"), entity_id="ades:impact:commodity:steel", metadata={"category": "industrial_metal"}),
    _entity("commodity", "Nickel", aliases=("LME nickel", "nickel futures"), entity_id="ades:impact:commodity:nickel", metadata={"category": "battery_metal"}),
    _entity("commodity", "Lithium", aliases=("lithium carbonate", "lithium hydroxide"), entity_id="ades:impact:commodity:lithium", metadata={"category": "battery_metal"}),
    _entity("commodity", "Cobalt", aliases=("cobalt metal",), entity_id="ades:impact:commodity:cobalt", metadata={"category": "battery_metal"}),
    _entity("commodity", "Gold", aliases=("gold futures", "COMEX gold"), entity_id="ades:impact:commodity:gold", metadata={"category": "precious_metal"}),
    _entity("commodity", "Silver", aliases=("silver futures", "COMEX silver"), entity_id="ades:impact:commodity:silver", metadata={"category": "precious_metal"}),
    _entity("commodity", "Platinum", aliases=("platinum futures",), entity_id="ades:impact:commodity:platinum", metadata={"category": "precious_metal"}),
    _entity("commodity", "Palladium", aliases=("palladium futures",), entity_id="ades:impact:commodity:palladium", metadata={"category": "precious_metal"}),
    _entity("commodity", "Tungsten", aliases=("tungsten concentrate",), entity_id="ades:impact:commodity:tungsten", metadata={"category": "strategic_metal"}),
    _entity("commodity", "Rare earths", aliases=("rare earth metals", "rare earth elements"), entity_id="ades:impact:commodity:rare-earths", metadata={"category": "strategic_metal"}),
    _entity("commodity", "Wheat", aliases=("wheat futures", "CBOT wheat"), entity_id="ades:impact:commodity:wheat", metadata={"category": "agriculture_benchmark"}),
    _entity("commodity", "Corn", aliases=("corn futures", "CBOT corn"), entity_id="ades:impact:commodity:corn", metadata={"category": "agriculture_benchmark"}),
    _entity("commodity", "Soybeans", aliases=("soybean futures", "CBOT soybeans"), entity_id="ades:impact:commodity:soybeans", metadata={"category": "agriculture_benchmark"}),
    _entity("commodity", "Rice", aliases=("rice futures",), entity_id="ades:impact:commodity:rice", metadata={"category": "agriculture_benchmark"}),
    _entity("commodity", "Coffee", aliases=("coffee futures",), entity_id="ades:impact:commodity:coffee", metadata={"category": "agriculture_benchmark"}),
    _entity("commodity", "Cocoa", aliases=("cocoa futures",), entity_id="ades:impact:commodity:cocoa", metadata={"category": "agriculture_benchmark"}),
    _entity("commodity", "Sugar", aliases=("sugar futures",), entity_id="ades:impact:commodity:sugar", metadata={"category": "agriculture_benchmark"}),
    _entity("commodity", "Cotton", aliases=("cotton futures",), entity_id="ades:impact:commodity:cotton", metadata={"category": "agriculture_benchmark"}),
    _entity("commodity", "Live cattle", aliases=("cattle futures", "live cattle futures"), entity_id="ades:impact:commodity:live-cattle", metadata={"category": "livestock_benchmark"}),
    _entity("commodity", "Lean hogs", aliases=("hog futures", "lean hog futures"), entity_id="ades:impact:commodity:lean-hogs", metadata={"category": "livestock_benchmark"}),
    _entity("currency", "US dollar", aliases=("USD", "U.S. dollar", "dollar"), entity_id="ades:impact:currency:usd", metadata={"country_code": "us"}),
    _entity("currency", "Euro", aliases=("EUR",), entity_id="ades:impact:currency:eur", metadata={"country_code": "eu"}),
    _entity("currency", "British pound", aliases=("GBP", "sterling"), entity_id="ades:impact:currency:gbp", metadata={"country_code": "uk"}),
    _entity("currency", "Japanese yen", aliases=("JPY", "yen"), entity_id="ades:impact:currency:jpy", metadata={"country_code": "jp"}),
    _entity("currency", "Chinese yuan", aliases=("CNY", "CNH", "renminbi", "yuan"), entity_id="ades:impact:currency:cny", metadata={"country_code": "cn"}),
    _entity("currency", "Turkish lira", aliases=("TRY", "lira"), entity_id="ades:impact:currency:try", metadata={"country_code": "tr"}),
    _entity("currency", "Brazilian real", aliases=("BRL", "real"), entity_id="ades:impact:currency:brl", metadata={"country_code": "br"}),
    _entity("currency", "Indian rupee", aliases=("INR", "rupee"), entity_id="ades:impact:currency:inr", metadata={"country_code": "in"}),
    _entity("currency", "Canadian dollar", aliases=("CAD",), entity_id="ades:impact:currency:cad", metadata={"country_code": "ca"}),
    _entity("currency", "Australian dollar", aliases=("AUD",), entity_id="ades:impact:currency:aud", metadata={"country_code": "au"}),
    _entity("currency", "Mexican peso", aliases=("MXN", "peso"), entity_id="ades:impact:currency:mxn", metadata={"country_code": "mx"}),
    _entity("currency", "South African rand", aliases=("ZAR", "rand"), entity_id="ades:impact:currency:zar", metadata={"country_code": "za"}),
    _entity("currency", "Korean won", aliases=("KRW", "won"), entity_id="ades:impact:currency:krw", metadata={"country_code": "kr"}),
    _entity("currency", "Indonesian rupiah", aliases=("IDR", "rupiah"), entity_id="ades:impact:currency:idr", metadata={"country_code": "id"}),
    _entity("currency", "Saudi riyal", aliases=("SAR", "riyal"), entity_id="ades:impact:currency:sar", metadata={"country_code": "sa"}),
    _entity("currency", "Argentine peso", aliases=("ARS", "Argentina peso"), entity_id="ades:impact:currency:ars", metadata={"country_code": "ar"}),
    _entity("currency", "Russian ruble", aliases=("RUB", "rouble"), entity_id="ades:impact:currency:rub", metadata={"country_code": "ru"}),
    _entity("rate", "Federal funds rate", aliases=("fed funds rate", "US policy rate"), entity_id="ades:impact:rate:fed-funds", metadata={"country_code": "us"}),
    _entity("rate", "ECB deposit rate", aliases=("ECB policy rate", "deposit facility rate"), entity_id="ades:impact:rate:ecb-deposit", metadata={"country_code": "eu"}),
    _entity("rate", "Bank Rate", aliases=("BoE Bank Rate", "UK policy rate"), entity_id="ades:impact:rate:uk-bank-rate", metadata={"country_code": "uk"}),
    _entity("rate", "Bank of Japan policy rate", aliases=("BOJ policy rate", "Japan policy rate"), entity_id="ades:impact:rate:jp-policy-rate", metadata={"country_code": "jp"}),
    _entity("rate", "PBOC loan prime rate", aliases=("loan prime rate", "China LPR", "China policy rate"), entity_id="ades:impact:rate:cn-lpr", metadata={"country_code": "cn"}),
    _entity("rate", "CBRT policy rate", aliases=("Turkey policy rate", "Turkish policy rate", "one-week repo rate"), entity_id="ades:impact:rate:tr-policy-rate", metadata={"country_code": "tr"}),
    _entity("rate", "Selic rate", aliases=("Brazil Selic", "Brazil policy rate"), entity_id="ades:impact:rate:br-selic", metadata={"country_code": "br"}),
    _entity("rate", "RBI repo rate", aliases=("India repo rate", "India policy rate"), entity_id="ades:impact:rate:in-repo", metadata={"country_code": "in"}),
    _entity("rate", "Bank of Canada policy rate", aliases=("BoC policy rate", "Canada policy rate", "overnight rate"), entity_id="ades:impact:rate:ca-policy-rate", metadata={"country_code": "ca"}),
    _entity("rate", "RBA cash rate", aliases=("Australia cash rate", "RBA policy rate"), entity_id="ades:impact:rate:au-cash-rate", metadata={"country_code": "au"}),
    _entity("rate", "Banxico target rate", aliases=("Mexico policy rate", "Banxico policy rate"), entity_id="ades:impact:rate:mx-target-rate", metadata={"country_code": "mx"}),
    _entity("rate", "SARB repo rate", aliases=("South Africa repo rate", "SARB policy rate"), entity_id="ades:impact:rate:za-repo-rate", metadata={"country_code": "za"}),
    _entity("rate", "Bank of Korea base rate", aliases=("BOK base rate", "Korea policy rate"), entity_id="ades:impact:rate:kr-base-rate", metadata={"country_code": "kr"}),
    _entity("rate", "Bank Indonesia policy rate", aliases=("BI rate", "Indonesia policy rate"), entity_id="ades:impact:rate:id-policy-rate", metadata={"country_code": "id"}),
    _entity("rate", "Saudi repo rate", aliases=("SAMA repo rate", "Saudi policy rate"), entity_id="ades:impact:rate:sa-repo-rate", metadata={"country_code": "sa"}),
    _entity("rate", "BCRA policy rate", aliases=("Argentina policy rate", "BCRA rate"), entity_id="ades:impact:rate:ar-policy-rate", metadata={"country_code": "ar"}),
    _entity("rate", "Russian key rate", aliases=("Bank of Russia key rate", "Russia policy rate"), entity_id="ades:impact:rate:ru-key-rate", metadata={"country_code": "ru"}),
    _entity("market_index", "S&P 500", aliases=("SPX", "S&P 500 index"), entity_id="ades:impact:index:sp500", metadata={"country_code": "us"}),
    _entity("market_index", "Euro Stoxx 50", aliases=("SX5E", "Eurozone blue-chip index"), entity_id="ades:impact:index:euro-stoxx-50", metadata={"country_code": "eu"}),
    _entity("market_index", "FTSE 100", aliases=("UKX", "FTSE index"), entity_id="ades:impact:index:ftse-100", metadata={"country_code": "uk"}),
    _entity("market_index", "Nikkei 225", aliases=("Nikkei index",), entity_id="ades:impact:index:nikkei-225", metadata={"country_code": "jp"}),
    _entity("market_index", "CSI 300", aliases=("China CSI 300",), entity_id="ades:impact:index:csi-300", metadata={"country_code": "cn"}),
    _entity("market_index", "Ibovespa", aliases=("Bovespa index",), entity_id="ades:impact:index:ibovespa", metadata={"country_code": "br"}),
    _entity("market_index", "BIST 100", aliases=("Borsa Istanbul 100",), entity_id="ades:impact:index:bist-100", metadata={"country_code": "tr"}),
    _entity("market_index", "S&P/TSX Composite", aliases=("TSX Composite",), entity_id="ades:impact:index:tsx-composite", metadata={"country_code": "ca"}),
    _entity("market_index", "S&P/ASX 200", aliases=("ASX 200",), entity_id="ades:impact:index:asx-200", metadata={"country_code": "au"}),
    _entity("market_index", "IPC Mexico", aliases=("S&P/BMV IPC",), entity_id="ades:impact:index:mx-ipc", metadata={"country_code": "mx"}),
    _entity("market_index", "KOSPI", aliases=("Korea Composite Stock Price Index",), entity_id="ades:impact:index:kospi", metadata={"country_code": "kr"}),
    _entity("market_index", "Nifty 50", aliases=("India Nifty",), entity_id="ades:impact:index:nifty-50", metadata={"country_code": "in"}),
    _entity("market_index", "Jakarta Composite Index", aliases=("JCI", "IDX Composite"), entity_id="ades:impact:index:jakarta-composite", metadata={"country_code": "id"}),
    _entity("market_index", "DAX", aliases=("Germany DAX",), entity_id="ades:impact:index:dax", metadata={"country_code": "de"}),
    _entity("market_index", "CAC 40", aliases=("France CAC 40",), entity_id="ades:impact:index:cac-40", metadata={"country_code": "fr"}),
    _entity("market_index", "FTSE MIB", aliases=("Italy FTSE MIB",), entity_id="ades:impact:index:ftse-mib", metadata={"country_code": "it"}),
    _entity("market_index", "Tadawul All Share", aliases=("TASI", "Saudi Tadawul index"), entity_id="ades:impact:index:tasi", metadata={"country_code": "sa"}),
    _entity("market_index", "JSE Top 40", aliases=("South Africa Top 40",), entity_id="ades:impact:index:jse-top-40", metadata={"country_code": "za"}),
    _entity("market_index", "MOEX Russia Index", aliases=("MOEX index",), entity_id="ades:impact:index:moex-russia", metadata={"country_code": "ru"}),
    _entity("market_index", "S&P Merval", aliases=("MERVAL", "Argentina Merval"), entity_id="ades:impact:index:merval", metadata={"country_code": "ar"}),
    _entity("risk", "Country risk", aliases=("country-risk", "sovereign risk"), entity_id="ades:impact:risk:country-risk", metadata={"category": "risk_indicator"}),
    _entity("risk", "Geopolitical risk", aliases=("geopolitical tensions", "geopolitical risk premium"), entity_id="ades:impact:risk:geopolitical-risk", metadata={"category": "risk_indicator"}),
    _entity("risk", "Credit risk", aliases=("credit spread risk", "default risk"), entity_id="ades:impact:risk:credit-risk", metadata={"category": "risk_indicator"}),
    _entity("risk", "Sovereign spread risk", aliases=("sovereign spreads", "bond spread risk"), entity_id="ades:impact:risk:sovereign-spread-risk", metadata={"category": "risk_indicator"}),
    _entity("risk", "Sanctions risk", aliases=("sanction risk", "sanctions exposure"), entity_id="ades:impact:risk:sanctions-risk", metadata={"category": "risk_indicator"}),
    _entity("risk", "Banking stress", aliases=("bank stress", "banking crisis risk"), entity_id="ades:impact:risk:banking-stress", metadata={"category": "risk_indicator"}),
    _entity("macro_indicator", "Consumer Price Index", aliases=("CPI", "consumer inflation"), entity_id="ades:impact:macro:cpi", metadata={"category": "inflation"}),
    _entity("macro_indicator", "Producer Price Index", aliases=("PPI", "producer inflation"), entity_id="ades:impact:macro:ppi", metadata={"category": "inflation"}),
    _entity("macro_indicator", "Gross domestic product", aliases=("GDP", "economic growth"), entity_id="ades:impact:macro:gdp", metadata={"category": "growth"}),
    _entity("macro_indicator", "Purchasing Managers Index", aliases=("PMI", "manufacturing PMI", "services PMI"), entity_id="ades:impact:macro:pmi", metadata={"category": "activity"}),
    _entity("macro_indicator", "Trade balance", aliases=("trade deficit", "trade surplus"), entity_id="ades:impact:macro:trade-balance", metadata={"category": "external_balance"}),
    _entity("macro_indicator", "Fiscal deficit", aliases=("budget deficit", "fiscal shortfall"), entity_id="ades:impact:macro:fiscal-deficit", metadata={"category": "fiscal"}),
    _entity("macro_indicator", "Unemployment", aliases=("unemployment rate", "jobless rate"), entity_id="ades:impact:macro:unemployment", metadata={"category": "labor"}),
    _entity("macro_indicator", "Payrolls", aliases=("nonfarm payrolls", "jobs report"), entity_id="ades:impact:macro:payrolls", metadata={"category": "labor"}),
    _entity("macro_indicator", "Budget", aliases=("government budget", "federal budget"), entity_id="ades:impact:macro:budget", metadata={"category": "fiscal"}),
    _entity("macro_indicator", "Debt ceiling", aliases=("debt limit", "debt-ceiling standoff"), entity_id="ades:impact:macro:debt-ceiling", metadata={"category": "fiscal"}),
    _entity("policy", "Capital controls", aliases=("capital control", "foreign exchange controls"), entity_id="ades:impact:policy:capital-controls", metadata={"category": "financial_controls"}),
    _entity("policy", "Export controls", aliases=("export control", "export restrictions", "technology export controls"), entity_id="ades:impact:policy:export-controls", metadata={"category": "trade_restriction"}),
    _entity("sector", "Semiconductors", aliases=("chipmakers", "chip manufacturers", "semiconductor sector"), entity_id="ades:impact:sector:semiconductors", metadata={"category": "sector"}),
    _entity("sector", "Autos", aliases=("automakers", "auto sector", "automotive sector"), entity_id="ades:impact:sector:autos", metadata={"category": "sector"}),
    _entity("sector", "Banks", aliases=("bank stocks", "banking sector"), entity_id="ades:impact:sector:banks", metadata={"category": "sector"}),
    _entity("sector", "Defense", aliases=("defence sector", "defense contractors"), entity_id="ades:impact:sector:defense", metadata={"category": "sector"}),
    _entity("sector", "Energy", aliases=("energy sector", "oil and gas sector"), entity_id="ades:impact:sector:energy", metadata={"category": "sector"}),
    _entity("sector", "Airlines", aliases=("airline sector",), entity_id="ades:impact:sector:airlines", metadata={"category": "sector"}),
    _entity("sector", "Shipping", aliases=("shipping sector", "container shipping"), entity_id="ades:impact:sector:shipping", metadata={"category": "sector"}),
    _entity("sector", "Agriculture", aliases=("agriculture sector", "agribusiness"), entity_id="ades:impact:sector:agriculture", metadata={"category": "sector"}),
    _entity("sector", "Mining", aliases=("mining sector", "miners"), entity_id="ades:impact:sector:mining", metadata={"category": "sector"}),
    _entity("sector", "Utilities", aliases=("utility sector", "power utilities"), entity_id="ades:impact:sector:utilities", metadata={"category": "sector"}),
    _entity("sector", "Real estate", aliases=("real estate sector", "REITs"), entity_id="ades:impact:sector:real-estate", metadata={"category": "sector"}),
)


@dataclass(frozen=True)
class DomainPackSpec:
    pack_id: str
    domain: str
    description: str
    entities: tuple[dict[str, object], ...]
    rules: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class DomainSourceBundleResult:
    pack_id: str
    version: str
    bundle_dir: str
    bundle_manifest_path: str
    entities_path: str
    rules_path: str
    entity_record_count: int
    rule_record_count: int


def _rule(name: str, label: str, pattern: str) -> dict[str, str]:
    return {"name": name, "label": label, "kind": "regex", "pattern": pattern}


BDYA_DOMAIN_PACK_SPECS: tuple[DomainPackSpec, ...] = (
    DomainPackSpec(
        pack_id="business-vector-en",
        domain="business",
        description="Curated BDYA business concept/entity pack for market news routing.",
        entities=(
            _entity("business_concept", "Merger and acquisition", aliases=("M&A", "takeover bid", "acquisition offer"), entity_id="ades:business:concept:m-and-a"),
            _entity("business_concept", "Bankruptcy", aliases=("chapter 11", "insolvency filing", "debt restructuring"), entity_id="ades:business:concept:bankruptcy"),
            _entity("business_concept", "Restructuring", aliases=("corporate restructuring", "debt workout"), entity_id="ades:business:concept:restructuring"),
            _entity("business_concept", "Earnings guidance", aliases=("guidance cut", "guidance raise", "profit warning"), entity_id="ades:business:concept:earnings-guidance"),
            _entity("business_concept", "Product launch", aliases=("new product launch", "product rollout"), entity_id="ades:business:concept:product-launch"),
            _entity("business_concept", "Product recall", aliases=("recall", "product recall"), entity_id="ades:business:concept:product-recall"),
            _entity("business_concept", "Share buyback", aliases=("stock buyback", "repurchase program"), entity_id="ades:business:concept:buyback"),
            _entity("business_concept", "Business regulation", aliases=("regulatory approval", "regulatory restriction"), entity_id="ades:business:concept:regulation"),
            _entity("business_concept", "Antitrust investigation", aliases=("competition probe", "antitrust probe"), entity_id="ades:business:concept:antitrust"),
            _entity("business_concept", "Supply-chain disruption", aliases=("supply chain disruption", "supply disruption"), entity_id="ades:business:concept:supply-chain-disruption"),
            _entity("business_concept", "Labor strike", aliases=("strike action", "walkout", "labor disruption"), entity_id="ades:business:concept:labor-strike"),
            _entity("business_concept", "Capital expenditure", aliases=("capex", "investment spending"), entity_id="ades:business:concept:capex"),
        ),
        rules=(
            _rule("earnings_guidance_phrase", "business_concept", r"\b(?:guidance|profit outlook|earnings outlook)\b.{0,80}\b(?:cut|raise|raised|lowered|miss|beat|warning)\b"),
            _rule("mna_phrase", "business_concept", r"\b(?:merger|acquisition|takeover|divestiture|asset sale)\b"),
            _rule("bankruptcy_phrase", "business_concept", r"\b(?:bankruptcy|insolvency|chapter 11|debt restructuring)\b"),
        ),
    ),
    DomainPackSpec(
        pack_id="economics-vector-en",
        domain="economics",
        description="Curated BDYA economics concept pack for macro and policy news routing.",
        entities=(
            _entity("economics_concept", "Inflation", aliases=("consumer inflation", "price pressures", "inflation shock"), entity_id="ades:economics:concept:inflation"),
            _entity("economics_concept", "Economic growth", aliases=("growth outlook", "GDP growth"), entity_id="ades:economics:concept:growth"),
            _entity("economics_concept", "Recession", aliases=("economic contraction", "downturn"), entity_id="ades:economics:concept:recession"),
            _entity("economics_concept", "Monetary policy", aliases=("rate policy", "policy tightening", "policy easing"), entity_id="ades:economics:concept:monetary-policy"),
            _entity("economics_concept", "Fiscal policy", aliases=("fiscal stimulus", "fiscal tightening", "budget plan"), entity_id="ades:economics:concept:fiscal-policy"),
            _entity("economics_concept", "Tariffs", aliases=("import tariff", "trade tariff", "tariff hike"), entity_id="ades:economics:concept:tariffs"),
            _entity("economics_concept", "Trade deficit", aliases=("trade surplus", "current account gap"), entity_id="ades:economics:concept:trade-balance"),
            _entity("economics_concept", "Labor market", aliases=("jobs report", "wage growth", "unemployment rate"), entity_id="ades:economics:concept:labor-market"),
            _entity("economics_concept", "Wages", aliases=("wage inflation", "pay growth"), entity_id="ades:economics:concept:wages"),
            _entity("economics_concept", "Housing market", aliases=("home prices", "housing starts", "mortgage rates"), entity_id="ades:economics:concept:housing"),
            _entity("economics_concept", "Industrial output", aliases=("factory output", "industrial production"), entity_id="ades:economics:concept:industrial-output"),
            _entity("economics_concept", "Supply chain", aliases=("supply-chain bottleneck", "logistics bottleneck"), entity_id="ades:economics:concept:supply-chain"),
            _entity("economics_concept", "Imports and exports", aliases=("import growth", "export decline", "trade flows"), entity_id="ades:economics:concept:imports-exports"),
        ),
        rules=(
            _rule("central_bank_policy_phrase", "economics_concept", r"\b(?:central bank|monetary authority|policy makers?)\b.{0,100}\b(?:rate|inflation|tighten|ease|hawkish|dovish)\b"),
            _rule("tariff_trade_phrase", "economics_concept", r"\b(?:tariff|tariffs|import duties|trade restrictions?|export controls?)\b"),
            _rule("macro_indicator_phrase", "economics_concept", r"\b(?:CPI|PPI|GDP|PMI|payrolls?|unemployment|trade balance|budget deficit)\b"),
        ),
    ),
    DomainPackSpec(
        pack_id="politics-vector-en",
        domain="politics",
        description="Curated BDYA politics concept/entity pack for policy, sanctions, and geopolitical news routing.",
        entities=(
            _entity("politics_concept", "Sanctions regime", aliases=("economic sanctions", "sanctions package", "sanctions list"), entity_id="ades:politics:concept:sanctions-regime"),
            _entity("politics_concept", "Government", aliases=("cabinet", "administration"), entity_id="ades:politics:concept:government"),
            _entity("politics_concept", "Ministry", aliases=("government ministry", "finance ministry"), entity_id="ades:politics:concept:ministry"),
            _entity("politics_concept", "Political party", aliases=("ruling party", "opposition party"), entity_id="ades:politics:concept:political-party"),
            _entity("politics_concept", "Treaty", aliases=("bilateral treaty", "security treaty"), entity_id="ades:politics:concept:treaty"),
            _entity("politics_concept", "Export controls", aliases=("technology export controls", "export restrictions"), entity_id="ades:politics:concept:export-controls"),
            _entity("politics_concept", "Disputed region", aliases=("territorial dispute", "breakaway region"), entity_id="ades:politics:concept:disputed-region"),
            _entity("politics_concept", "Election body", aliases=("electoral commission", "election board"), entity_id="ades:politics:concept:election-body"),
            _entity("politics_concept", "Ceasefire", aliases=("cease-fire", "truce agreement", "risk relief"), entity_id="ades:politics:concept:ceasefire"),
            _entity("politics_concept", "Conflict escalation", aliases=("war escalation", "military escalation", "security crisis"), entity_id="ades:politics:concept:conflict-escalation"),
            _entity("politics_concept", "Election risk", aliases=("election uncertainty", "snap election", "vote recount"), entity_id="ades:politics:concept:election-risk"),
            _entity("politics_concept", "Trade bloc", aliases=("customs union", "trade agreement", "free trade agreement"), entity_id="ades:politics:concept:trade-bloc"),
            _entity("politics_concept", "Geopolitical alliance", aliases=("security alliance", "defense pact"), entity_id="ades:politics:concept:geopolitical-alliance"),
            _entity("politics_concept", "Regulatory crackdown", aliases=("regulatory probe", "government crackdown"), entity_id="ades:politics:concept:regulatory-crackdown"),
        ),
        rules=(
            _rule("sanctions_phrase", "politics_concept", r"\b(?:sanction|sanctions|blacklist|asset freeze|travel ban)\b"),
            _rule("export_control_phrase", "politics_concept", r"\b(?:export control|export controls|export ban|technology restrictions?)\b"),
            _rule("geopolitical_risk_phrase", "politics_concept", r"\b(?:war|conflict|ceasefire|missile|military|border tensions?|chokepoint)\b"),
        ),
    ),
)


def build_bdya_domain_source_bundles(
    *,
    output_dir: str | Path,
    version: str = "0.2.0",
) -> list[DomainSourceBundleResult]:
    """Build normalized source bundles for curated BDYA domain packs."""

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    results: list[DomainSourceBundleResult] = []
    for spec in BDYA_DOMAIN_PACK_SPECS:
        results.append(
            _build_domain_source_bundle(
                spec=spec,
                output_dir=resolved_output_dir,
                version=version,
            )
        )
    return results


def _build_domain_source_bundle(
    *,
    spec: DomainPackSpec,
    output_dir: Path,
    version: str,
) -> DomainSourceBundleResult:
    bundle_dir = output_dir / f"{spec.pack_id}-bundle"
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    normalized_dir = bundle_dir / "normalized"
    source_dir = bundle_dir / "source"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    source_dir.mkdir(parents=True, exist_ok=True)

    source_path = source_dir / "curated_entities.json"
    entities_path = normalized_dir / "entities.jsonl"
    rules_path = normalized_dir / "rules.jsonl"
    bundle_manifest_path = bundle_dir / "bundle.json"

    source_path.write_text(
        json.dumps({"entities": list(spec.entities)}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_jsonl(entities_path, list(spec.entities))
    _write_jsonl(rules_path, list(spec.rules))

    source_entry = build_bundle_source_entry(
        name=f"curated-{spec.domain}-bdya-phase6",
        snapshot_path=source_path,
        record_count=len(spec.entities),
        adapter="curated_bdya_domain_entities",
    )
    bundle_manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": spec.pack_id,
                "version": version,
                "language": "en",
                "domain": spec.domain,
                "tier": "domain",
                "description": spec.description,
                "tags": [spec.domain, "bdya", "phase6", "curated", "english"],
                "dependencies": [],
                "entities_path": "normalized/entities.jsonl",
                "rules_path": "normalized/rules.jsonl",
                "alias_resolution": "analyze",
                "label_mappings": {},
                "stoplisted_aliases": ["N/A"],
                "allowed_ambiguous_aliases": [],
                "sources": [source_entry],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    write_sources_lock(
        bundle_dir=bundle_dir,
        pack_id=spec.pack_id,
        version=version,
        entities_path="normalized/entities.jsonl",
        rules_path="normalized/rules.jsonl",
        sources=[source_entry],
    )
    return DomainSourceBundleResult(
        pack_id=spec.pack_id,
        version=version,
        bundle_dir=str(bundle_dir),
        bundle_manifest_path=str(bundle_manifest_path),
        entities_path=str(entities_path),
        rules_path=str(rules_path),
        entity_record_count=len(spec.entities),
        rule_record_count=len(spec.rules),
    )


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
