"""Country-scoped finance source fetchers and bundle builders."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
import httpx
import json
from pathlib import Path
import re
import shutil
from typing import Any
import urllib.request
from urllib.parse import urlparse

from .fetch import normalize_source_url
from .finance_bundle import _build_finance_rule_records
from .source_lock import build_bundle_source_entry, write_sources_lock


DEFAULT_FINANCE_COUNTRY_SOURCE_OUTPUT_ROOT = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en"
)
DEFAULT_FINANCE_COUNTRY_BUNDLE_OUTPUT_ROOT = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/bundles/finance-country-en"
)
DEFAULT_COUNTRY_SOURCE_FETCH_USER_AGENT = "ades/0.1.0 (ops@adestool.com)"
DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS = 20.0
_FINANCE_COUNTRY_STOPLISTED_ALIASES = ["N/A", "ON", "USD"]


def _source(
    name: str,
    source_url: str,
    *,
    category: str,
    notes: str | None = None,
) -> dict[str, str]:
    payload = {
        "name": name,
        "source_url": source_url,
        "category": category,
    }
    if notes:
        payload["notes"] = notes
    return payload


def _entity(
    entity_type: str,
    canonical_text: str,
    *,
    aliases: tuple[str, ...] = (),
    entity_id: str | None = None,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "entity_type": entity_type,
        "canonical_text": canonical_text,
        "aliases": list(aliases),
    }
    if entity_id:
        payload["entity_id"] = entity_id
    if metadata:
        payload["metadata"] = metadata
    return payload


def _profile(
    country_code: str,
    country_name: str,
    *,
    pack_id: str,
    description: str,
    sources: tuple[dict[str, str], ...],
    entities: tuple[dict[str, object], ...],
) -> dict[str, object]:
    return {
        "country_code": country_code,
        "country_name": country_name,
        "pack_id": pack_id,
        "description": description,
        "sources": list(sources),
        "entities": list(entities),
    }


FINANCE_COUNTRY_PROFILES: dict[str, dict[str, object]] = {
    "ar": _profile(
        "ar",
        "Argentina",
        pack_id="finance-ar-en",
        description="Argentina public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "cnv",
                "https://www.argentina.gob.ar/cnv",
                category="securities_regulator",
            ),
            _source(
                "cnv-aif",
                "https://www.cnv.gov.ar/",
                category="filing_system",
            ),
            _source(
                "byma",
                "https://www.byma.com.ar/en",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Comisión Nacional de Valores",
                aliases=("CNV", "National Securities Commission"),
                entity_id="finance-ar:cnv",
                metadata={"country_code": "ar", "category": "securities_regulator"},
            ),
            _entity(
                "organization",
                "Autopista de la Información Financiera",
                aliases=("AIF",),
                entity_id="finance-ar:aif",
                metadata={"country_code": "ar", "category": "filing_system"},
            ),
            _entity(
                "exchange",
                "Bolsas y Mercados Argentinos",
                aliases=("BYMA",),
                entity_id="finance-ar:byma",
                metadata={"country_code": "ar", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Banco Central de la República Argentina",
                aliases=("BCRA", "Central Bank of Argentina"),
                entity_id="finance-ar:bcra",
                metadata={"country_code": "ar", "category": "central_bank"},
            ),
            _entity(
                "market_index",
                "S&P Merval",
                aliases=("MERVAL",),
                entity_id="finance-ar:merval",
                metadata={"country_code": "ar", "category": "market_index"},
            ),
        ),
    ),
    "au": _profile(
        "au",
        "Australia",
        pack_id="finance-au-en",
        description="Australia public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source("asic", "https://www.asic.gov.au/", category="securities_regulator"),
            _source("asx", "https://www.asx.com.au/", category="exchange"),
        ),
        entities=(
            _entity(
                "organization",
                "Australian Securities and Investments Commission",
                aliases=("ASIC",),
                entity_id="finance-au:asic",
                metadata={"country_code": "au", "category": "securities_regulator"},
            ),
            _entity(
                "exchange",
                "Australian Securities Exchange",
                aliases=("ASX",),
                entity_id="finance-au:asx",
                metadata={"country_code": "au", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Australian Prudential Regulation Authority",
                aliases=("APRA",),
                entity_id="finance-au:apra",
                metadata={"country_code": "au", "category": "institution_regulator"},
            ),
            _entity(
                "organization",
                "Reserve Bank of Australia",
                aliases=("RBA",),
                entity_id="finance-au:rba",
                metadata={"country_code": "au", "category": "central_bank"},
            ),
            _entity(
                "market_index",
                "S&P/ASX 200",
                aliases=("ASX 200",),
                entity_id="finance-au:asx-200",
                metadata={"country_code": "au", "category": "market_index"},
            ),
        ),
    ),
    "br": _profile(
        "br",
        "Brazil",
        pack_id="finance-br-en",
        description="Brazil public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "cvm",
                "https://www.gov.br/cvm/pt-br/en",
                category="securities_regulator",
            ),
            _source(
                "b3",
                "https://www.b3.com.br/en_us/",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Comissão de Valores Mobiliários",
                aliases=("CVM", "Brazilian Securities and Exchange Commission"),
                entity_id="finance-br:cvm",
                metadata={"country_code": "br", "category": "securities_regulator"},
            ),
            _entity(
                "exchange",
                "B3",
                aliases=("Brasil Bolsa Balcão",),
                entity_id="finance-br:b3",
                metadata={"country_code": "br", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Banco Central do Brasil",
                aliases=("BCB", "Central Bank of Brazil"),
                entity_id="finance-br:bcb",
                metadata={"country_code": "br", "category": "central_bank"},
            ),
            _entity(
                "organization",
                "Novo Mercado",
                aliases=(),
                entity_id="finance-br:novo-mercado",
                metadata={"country_code": "br", "category": "market_segment"},
            ),
            _entity(
                "market_index",
                "Ibovespa",
                aliases=("Bovespa Index",),
                entity_id="finance-br:ibovespa",
                metadata={"country_code": "br", "category": "market_index"},
            ),
        ),
    ),
    "ca": _profile(
        "ca",
        "Canada",
        pack_id="finance-ca-en",
        description="Canada public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "sedar-plus",
                "https://www.sedarplus.ca/",
                category="filing_system",
            ),
            _source(
                "csa",
                "https://www.securities-administrators.ca/",
                category="securities_regulator",
            ),
            _source(
                "tsx",
                "https://www.tsx.com/en/",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Canadian Securities Administrators",
                aliases=("CSA",),
                entity_id="finance-ca:csa",
                metadata={"country_code": "ca", "category": "securities_regulator"},
            ),
            _entity(
                "organization",
                "SEDAR+",
                aliases=(),
                entity_id="finance-ca:sedar-plus",
                metadata={"country_code": "ca", "category": "filing_system"},
            ),
            _entity(
                "exchange",
                "Toronto Stock Exchange",
                aliases=("TSX",),
                entity_id="finance-ca:tsx",
                metadata={"country_code": "ca", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Office of the Superintendent of Financial Institutions",
                aliases=("OSFI",),
                entity_id="finance-ca:osfi",
                metadata={"country_code": "ca", "category": "institution_regulator"},
            ),
            _entity(
                "market_index",
                "S&P/TSX Composite Index",
                aliases=("S&P/TSX Composite",),
                entity_id="finance-ca:sp-tsx-composite",
                metadata={"country_code": "ca", "category": "market_index"},
            ),
        ),
    ),
    "cn": _profile(
        "cn",
        "China",
        pack_id="finance-cn-en",
        description="China public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "csrc",
                "https://www.csrc.gov.cn/csrc_en/",
                category="securities_regulator",
            ),
            _source(
                "sse",
                "https://english.sse.com.cn/",
                category="exchange",
            ),
            _source(
                "szse",
                "https://www.szse.cn/English/",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "China Securities Regulatory Commission",
                aliases=("CSRC",),
                entity_id="finance-cn:csrc",
                metadata={"country_code": "cn", "category": "securities_regulator"},
            ),
            _entity(
                "exchange",
                "Shanghai Stock Exchange",
                aliases=("SSE",),
                entity_id="finance-cn:sse",
                metadata={"country_code": "cn", "category": "exchange"},
            ),
            _entity(
                "exchange",
                "Shenzhen Stock Exchange",
                aliases=("SZSE",),
                entity_id="finance-cn:szse",
                metadata={"country_code": "cn", "category": "exchange"},
            ),
            _entity(
                "organization",
                "People's Bank of China",
                aliases=("PBOC",),
                entity_id="finance-cn:pboc",
                metadata={"country_code": "cn", "category": "central_bank"},
            ),
            _entity(
                "market_index",
                "CSI 300",
                aliases=("CSI 300 Index",),
                entity_id="finance-cn:csi-300",
                metadata={"country_code": "cn", "category": "market_index"},
            ),
        ),
    ),
    "fr": _profile(
        "fr",
        "France",
        pack_id="finance-fr-en",
        description="France public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "amf",
                "https://www.amf-france.org/en",
                category="securities_regulator",
            ),
            _source(
                "euronext-paris",
                "https://www.euronext.com/en/markets/paris",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Autorité des marchés financiers",
                aliases=("AMF", "French Financial Markets Authority"),
                entity_id="finance-fr:amf",
                metadata={"country_code": "fr", "category": "securities_regulator"},
            ),
            _entity(
                "exchange",
                "Euronext Paris",
                aliases=(),
                entity_id="finance-fr:euronext-paris",
                metadata={"country_code": "fr", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Banque de France",
                aliases=(),
                entity_id="finance-fr:banque-de-france",
                metadata={"country_code": "fr", "category": "central_bank"},
            ),
            _entity(
                "organization",
                "Autorité de contrôle prudentiel et de résolution",
                aliases=("ACPR",),
                entity_id="finance-fr:acpr",
                metadata={"country_code": "fr", "category": "institution_regulator"},
            ),
            _entity(
                "market_index",
                "CAC 40",
                aliases=(),
                entity_id="finance-fr:cac-40",
                metadata={"country_code": "fr", "category": "market_index"},
            ),
        ),
    ),
    "de": _profile(
        "de",
        "Germany",
        pack_id="finance-de-en",
        description="Germany public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "bafin",
                "https://www.bafin.de/EN/Homepage/homepage_node_en.html",
                category="securities_regulator",
            ),
            _source(
                "unternehmensregister",
                "https://www.unternehmensregister.de/ureg/?submitaction=language&language=en",
                category="company_registry",
            ),
            _source(
                "boerse-frankfurt",
                "https://www.boerse-frankfurt.de/",
                category="exchange",
            ),
            _source(
                "xetra",
                "https://www.xetra.com/xetra-en/",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Federal Financial Supervisory Authority",
                aliases=("BaFin",),
                entity_id="finance-de:bafin",
                metadata={"country_code": "de", "category": "securities_regulator"},
            ),
            _entity(
                "organization",
                "Unternehmensregister",
                aliases=("Company Register",),
                entity_id="finance-de:unternehmensregister",
                metadata={"country_code": "de", "category": "company_registry"},
            ),
            _entity(
                "exchange",
                "Börse Frankfurt",
                aliases=("Frankfurt Stock Exchange",),
                entity_id="finance-de:boerse-frankfurt",
                metadata={"country_code": "de", "category": "exchange"},
            ),
            _entity(
                "exchange",
                "Xetra",
                aliases=(),
                entity_id="finance-de:xetra",
                metadata={"country_code": "de", "category": "exchange"},
            ),
            _entity(
                "market_index",
                "DAX",
                aliases=("DAX Index",),
                entity_id="finance-de:dax",
                metadata={"country_code": "de", "category": "market_index"},
            ),
        ),
    ),
    "in": _profile(
        "in",
        "India",
        pack_id="finance-in-en",
        description="India public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source("sebi", "https://www.sebi.gov.in/", category="securities_regulator"),
            _source(
                "nse",
                "https://www.nseindia.com/",
                category="exchange",
            ),
            _source(
                "bse",
                "https://www.bseindia.com/",
                category="exchange",
            ),
            _source(
                "mca",
                "https://www.mca.gov.in/",
                category="company_registry",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Securities and Exchange Board of India",
                aliases=("SEBI",),
                entity_id="finance-in:sebi",
                metadata={"country_code": "in", "category": "securities_regulator"},
            ),
            _entity(
                "exchange",
                "National Stock Exchange of India",
                aliases=("NSE",),
                entity_id="finance-in:nse",
                metadata={"country_code": "in", "category": "exchange"},
            ),
            _entity(
                "exchange",
                "Bombay Stock Exchange",
                aliases=("BSE",),
                entity_id="finance-in:bse",
                metadata={"country_code": "in", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Ministry of Corporate Affairs",
                aliases=("MCA",),
                entity_id="finance-in:mca",
                metadata={"country_code": "in", "category": "company_registry"},
            ),
            _entity(
                "market_index",
                "NIFTY 50",
                aliases=(),
                entity_id="finance-in:nifty-50",
                metadata={"country_code": "in", "category": "market_index"},
            ),
        ),
    ),
    "id": _profile(
        "id",
        "Indonesia",
        pack_id="finance-id-en",
        description="Indonesia public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "ojk",
                "https://www.ojk.go.id/en/Default.aspx",
                category="securities_regulator",
            ),
            _source(
                "idx",
                "https://www.idx.id/en",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Otoritas Jasa Keuangan",
                aliases=("OJK", "Financial Services Authority of Indonesia"),
                entity_id="finance-id:ojk",
                metadata={"country_code": "id", "category": "securities_regulator"},
            ),
            _entity(
                "exchange",
                "Indonesia Stock Exchange",
                aliases=("IDX",),
                entity_id="finance-id:idx",
                metadata={"country_code": "id", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Bank Indonesia",
                aliases=(),
                entity_id="finance-id:bank-indonesia",
                metadata={"country_code": "id", "category": "central_bank"},
            ),
            _entity(
                "organization",
                "Kustodian Sentral Efek Indonesia",
                aliases=("KSEI", "Indonesia Central Securities Depository"),
                entity_id="finance-id:ksei",
                metadata={"country_code": "id", "category": "depository"},
            ),
            _entity(
                "market_index",
                "IDX Composite",
                aliases=("Jakarta Composite Index",),
                entity_id="finance-id:idx-composite",
                metadata={"country_code": "id", "category": "market_index"},
            ),
        ),
    ),
    "it": _profile(
        "it",
        "Italy",
        pack_id="finance-it-en",
        description="Italy public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "consob",
                "https://www.consob.it/web/consob/home",
                category="securities_regulator",
            ),
            _source(
                "consob-markets",
                "https://www.consob.it/web/consob-and-its-activities/italian-markets",
                category="market_authority",
            ),
            _source(
                "borsa-italiana",
                "https://www.borsaitaliana.it/listino-ufficiale.en.htm",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Commissione Nazionale per le Società e la Borsa",
                aliases=("CONSOB",),
                entity_id="finance-it:consob",
                metadata={"country_code": "it", "category": "securities_regulator"},
            ),
            _entity(
                "exchange",
                "Borsa Italiana",
                aliases=(),
                entity_id="finance-it:borsa-italiana",
                metadata={"country_code": "it", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Bank of Italy",
                aliases=("Banca d'Italia",),
                entity_id="finance-it:bank-of-italy",
                metadata={"country_code": "it", "category": "central_bank"},
            ),
            _entity(
                "organization",
                "Registro delle Imprese",
                aliases=("Italian Business Register",),
                entity_id="finance-it:registro-delle-imprese",
                metadata={"country_code": "it", "category": "company_registry"},
            ),
            _entity(
                "market_index",
                "FTSE MIB",
                aliases=("MIB Index",),
                entity_id="finance-it:ftse-mib",
                metadata={"country_code": "it", "category": "market_index"},
            ),
        ),
    ),
    "jp": _profile(
        "jp",
        "Japan",
        pack_id="finance-jp-en",
        description="Japan public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source("fsa", "https://www.fsa.go.jp/en/", category="securities_regulator"),
            _source(
                "edinet",
                "https://info.edinet-fsa.go.jp/",
                category="filing_system",
            ),
            _source(
                "jpx",
                "https://www.jpx.co.jp/english/",
                category="exchange",
            ),
            _source(
                "jpx-disclosure-gate",
                "https://www.jpx.co.jp/english/equities/listed-co/disclosure-gate/",
                category="disclosure_portal",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Financial Services Agency",
                aliases=("FSA",),
                entity_id="finance-jp:fsa",
                metadata={"country_code": "jp", "category": "securities_regulator"},
            ),
            _entity(
                "organization",
                "EDINET",
                aliases=("Electronic Disclosure for Investors' NETwork",),
                entity_id="finance-jp:edinet",
                metadata={"country_code": "jp", "category": "filing_system"},
            ),
            _entity(
                "exchange",
                "Japan Exchange Group",
                aliases=("JPX",),
                entity_id="finance-jp:jpx",
                metadata={"country_code": "jp", "category": "exchange_group"},
            ),
            _entity(
                "exchange",
                "Tokyo Stock Exchange",
                aliases=("TSE",),
                entity_id="finance-jp:tse",
                metadata={"country_code": "jp", "category": "exchange"},
            ),
            _entity(
                "market_index",
                "Nikkei 225",
                aliases=(),
                entity_id="finance-jp:nikkei-225",
                metadata={"country_code": "jp", "category": "market_index"},
            ),
        ),
    ),
    "mx": _profile(
        "mx",
        "Mexico",
        pack_id="finance-mx-en",
        description="Mexico public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "cnbv",
                "https://www.gob.mx/cnbv",
                category="securities_regulator",
            ),
            _source(
                "bmv",
                "https://www.bmv.com.mx/en/",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Comisión Nacional Bancaria y de Valores",
                aliases=("CNBV",),
                entity_id="finance-mx:cnbv",
                metadata={"country_code": "mx", "category": "securities_regulator"},
            ),
            _entity(
                "exchange",
                "Bolsa Mexicana de Valores",
                aliases=("BMV",),
                entity_id="finance-mx:bmv",
                metadata={"country_code": "mx", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Banco de México",
                aliases=("Banxico",),
                entity_id="finance-mx:banxico",
                metadata={"country_code": "mx", "category": "central_bank"},
            ),
            _entity(
                "exchange",
                "Bolsa Institucional de Valores",
                aliases=("BIVA",),
                entity_id="finance-mx:biva",
                metadata={"country_code": "mx", "category": "exchange"},
            ),
            _entity(
                "market_index",
                "S&P/BMV IPC",
                aliases=("IPC Index",),
                entity_id="finance-mx:ipc",
                metadata={"country_code": "mx", "category": "market_index"},
            ),
        ),
    ),
    "ru": _profile(
        "ru",
        "Russia",
        pack_id="finance-ru-en",
        description="Russia public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "bank-of-russia",
                "https://www.cbr.ru/eng/",
                category="securities_regulator",
                notes="Sanctions and publication policy require review before promotion.",
            ),
            _source(
                "moex",
                "https://www.moex.com/en",
                category="exchange",
                notes="Sanctions and publication policy require review before promotion.",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Bank of Russia",
                aliases=("Central Bank of the Russian Federation",),
                entity_id="finance-ru:cbr",
                metadata={"country_code": "ru", "category": "securities_regulator"},
            ),
            _entity(
                "exchange",
                "Moscow Exchange",
                aliases=("MOEX",),
                entity_id="finance-ru:moex",
                metadata={"country_code": "ru", "category": "exchange"},
            ),
            _entity(
                "organization",
                "National Settlement Depository",
                aliases=("NSD",),
                entity_id="finance-ru:nsd",
                metadata={"country_code": "ru", "category": "depository"},
            ),
            _entity(
                "exchange",
                "SPB Exchange",
                aliases=("SPBE",),
                entity_id="finance-ru:spbe",
                metadata={"country_code": "ru", "category": "exchange"},
            ),
            _entity(
                "market_index",
                "MOEX Russia Index",
                aliases=("Russia Index",),
                entity_id="finance-ru:moex-russia-index",
                metadata={"country_code": "ru", "category": "market_index"},
            ),
        ),
    ),
    "sa": _profile(
        "sa",
        "Saudi Arabia",
        pack_id="finance-sa-en",
        description="Saudi Arabia public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "cma",
                "https://cma.gov.sa/en/Pages/default.aspx",
                category="securities_regulator",
            ),
            _source(
                "saudi-exchange",
                "https://www.saudiexchange.sa/",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Capital Market Authority",
                aliases=("CMA",),
                entity_id="finance-sa:cma",
                metadata={"country_code": "sa", "category": "securities_regulator"},
            ),
            _entity(
                "exchange",
                "Saudi Exchange",
                aliases=("Tadawul",),
                entity_id="finance-sa:tadawul",
                metadata={"country_code": "sa", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Saudi Central Bank",
                aliases=("SAMA",),
                entity_id="finance-sa:sama",
                metadata={"country_code": "sa", "category": "central_bank"},
            ),
            _entity(
                "organization",
                "Securities Depository Center Company",
                aliases=("Edaa",),
                entity_id="finance-sa:edaa",
                metadata={"country_code": "sa", "category": "depository"},
            ),
            _entity(
                "market_index",
                "Tadawul All Share Index",
                aliases=("TASI",),
                entity_id="finance-sa:tasi",
                metadata={"country_code": "sa", "category": "market_index"},
            ),
        ),
    ),
    "za": _profile(
        "za",
        "South Africa",
        pack_id="finance-za-en",
        description="South Africa public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "fsca",
                "https://www.fsca.co.za/",
                category="securities_regulator",
            ),
            _source(
                "jse",
                "https://www.jse.co.za/",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Financial Sector Conduct Authority",
                aliases=("FSCA",),
                entity_id="finance-za:fsca",
                metadata={"country_code": "za", "category": "securities_regulator"},
            ),
            _entity(
                "exchange",
                "Johannesburg Stock Exchange",
                aliases=("JSE",),
                entity_id="finance-za:jse",
                metadata={"country_code": "za", "category": "exchange"},
            ),
            _entity(
                "organization",
                "South African Reserve Bank",
                aliases=("SARB",),
                entity_id="finance-za:sarb",
                metadata={"country_code": "za", "category": "central_bank"},
            ),
            _entity(
                "organization",
                "Companies and Intellectual Property Commission",
                aliases=("CIPC",),
                entity_id="finance-za:cipc",
                metadata={"country_code": "za", "category": "company_registry"},
            ),
            _entity(
                "market_index",
                "FTSE/JSE All Share Index",
                aliases=("JSE All Share",),
                entity_id="finance-za:ftse-jse-all-share",
                metadata={"country_code": "za", "category": "market_index"},
            ),
        ),
    ),
    "kr": _profile(
        "kr",
        "South Korea",
        pack_id="finance-kr-en",
        description="South Korea public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source(
                "fss",
                "https://english.fss.or.kr/",
                category="securities_regulator",
            ),
            _source(
                "dart",
                "https://englishdart.fss.or.kr/",
                category="filing_system",
            ),
            _source(
                "krx",
                "https://global.krx.co.kr/",
                category="exchange",
            ),
            _source(
                "kind",
                "https://engkind.krx.co.kr/",
                category="disclosure_portal",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Financial Supervisory Service",
                aliases=("FSS",),
                entity_id="finance-kr:fss",
                metadata={"country_code": "kr", "category": "securities_regulator"},
            ),
            _entity(
                "organization",
                "DART",
                aliases=("Data Analysis, Retrieval and Transfer System",),
                entity_id="finance-kr:dart",
                metadata={"country_code": "kr", "category": "filing_system"},
            ),
            _entity(
                "exchange",
                "Korea Exchange",
                aliases=("KRX",),
                entity_id="finance-kr:krx",
                metadata={"country_code": "kr", "category": "exchange"},
            ),
            _entity(
                "organization",
                "KIND",
                aliases=("Korea Investor's Network for Disclosure",),
                entity_id="finance-kr:kind",
                metadata={"country_code": "kr", "category": "disclosure_portal"},
            ),
            _entity(
                "market_index",
                "KOSPI",
                aliases=("Korea Composite Stock Price Index",),
                entity_id="finance-kr:kospi",
                metadata={"country_code": "kr", "category": "market_index"},
            ),
        ),
    ),
    "tr": _profile(
        "tr",
        "Türkiye",
        pack_id="finance-tr-en",
        description="Türkiye public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source("spk", "https://spk.gov.tr/", category="securities_regulator"),
            _source("kap", "https://www.kap.org.tr/en/", category="filing_system"),
            _source(
                "borsa-istanbul",
                "https://www.borsaistanbul.com/en",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Capital Markets Board of Türkiye",
                aliases=("SPK", "Capital Markets Board of Turkey"),
                entity_id="finance-tr:spk",
                metadata={"country_code": "tr", "category": "securities_regulator"},
            ),
            _entity(
                "organization",
                "Public Disclosure Platform",
                aliases=("KAP",),
                entity_id="finance-tr:kap",
                metadata={"country_code": "tr", "category": "filing_system"},
            ),
            _entity(
                "exchange",
                "Borsa Istanbul",
                aliases=("BIST", "Borsa İstanbul"),
                entity_id="finance-tr:bist",
                metadata={"country_code": "tr", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Central Securities Depository of Türkiye",
                aliases=("MKK", "Merkezi Kayıt Kuruluşu"),
                entity_id="finance-tr:mkk",
                metadata={"country_code": "tr", "category": "depository"},
            ),
            _entity(
                "market_index",
                "BIST 100",
                aliases=("BIST 100 Index",),
                entity_id="finance-tr:bist-100",
                metadata={"country_code": "tr", "category": "market_index"},
            ),
        ),
    ),
    "uk": _profile(
        "uk",
        "United Kingdom",
        pack_id="finance-uk-en",
        description="United Kingdom public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source("fca", "https://www.fca.org.uk/", category="securities_regulator"),
            _source(
                "official-list",
                "https://marketsecurities.fca.org.uk/officiallist",
                category="official_list",
            ),
            _source(
                "rns",
                "https://www.lseg.com/en/capital-markets/regulatory-news-service",
                category="disclosure_portal",
            ),
            _source(
                "lse",
                "https://www.londonstockexchange.com/",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Financial Conduct Authority",
                aliases=("FCA",),
                entity_id="finance-uk:fca",
                metadata={"country_code": "uk", "category": "securities_regulator"},
            ),
            _entity(
                "organization",
                "Official List",
                aliases=("FCA Official List",),
                entity_id="finance-uk:official-list",
                metadata={"country_code": "uk", "category": "official_list"},
            ),
            _entity(
                "organization",
                "Regulatory News Service",
                aliases=("RNS",),
                entity_id="finance-uk:rns",
                metadata={"country_code": "uk", "category": "disclosure_portal"},
            ),
            _entity(
                "exchange",
                "London Stock Exchange",
                aliases=("LSE",),
                entity_id="finance-uk:lse",
                metadata={"country_code": "uk", "category": "exchange"},
            ),
            _entity(
                "market_index",
                "FTSE 100",
                aliases=("FTSE 100 Index",),
                entity_id="finance-uk:ftse-100",
                metadata={"country_code": "uk", "category": "market_index"},
            ),
        ),
    ),
    "us": _profile(
        "us",
        "United States",
        pack_id="finance-us-en",
        description="United States public-market finance entities built from official regulator and exchange sources.",
        sources=(
            _source("sec", "https://www.sec.gov/", category="securities_regulator"),
            _source(
                "edgar",
                "https://www.sec.gov/search-filings",
                category="filing_system",
            ),
            _source(
                "nasdaq",
                "https://www.nasdaq.com/",
                category="exchange",
            ),
            _source(
                "nyse",
                "https://www.nyse.com/",
                category="exchange",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Securities and Exchange Commission",
                aliases=("SEC",),
                entity_id="finance-us:sec",
                metadata={"country_code": "us", "category": "securities_regulator"},
            ),
            _entity(
                "organization",
                "EDGAR",
                aliases=("Electronic Data Gathering, Analysis, and Retrieval",),
                entity_id="finance-us:edgar",
                metadata={"country_code": "us", "category": "filing_system"},
            ),
            _entity(
                "exchange",
                "Nasdaq Stock Market",
                aliases=("NASDAQ", "Nasdaq"),
                entity_id="finance-us:nasdaq",
                metadata={"country_code": "us", "category": "exchange"},
            ),
            _entity(
                "exchange",
                "New York Stock Exchange",
                aliases=("NYSE",),
                entity_id="finance-us:nyse",
                metadata={"country_code": "us", "category": "exchange"},
            ),
            _entity(
                "organization",
                "Federal Deposit Insurance Corporation",
                aliases=("FDIC",),
                entity_id="finance-us:fdic",
                metadata={"country_code": "us", "category": "institution_regulator"},
            ),
            _entity(
                "market_index",
                "S&P 500",
                aliases=("S&P 500 Index",),
                entity_id="finance-us:sp-500",
                metadata={"country_code": "us", "category": "market_index"},
            ),
        ),
    ),
}


@dataclass(frozen=True)
class FinanceCountrySourceFetchItem:
    """One downloaded country finance source snapshot set."""

    country_code: str
    country_name: str
    pack_id: str
    snapshot_dir: str
    profile_path: str
    source_manifest_path: str
    curated_entities_path: str
    source_count: int
    curated_entity_count: int
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class FinanceCountrySourceFetchResult:
    """Aggregate downloaded country finance source snapshot set."""

    output_dir: str
    snapshot: str
    snapshot_dir: str
    generated_at: str
    country_count: int
    countries: list[FinanceCountrySourceFetchItem] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class FinanceCountryBundleItem:
    """One built country finance bundle."""

    country_code: str
    country_name: str
    pack_id: str
    version: str
    bundle_dir: str
    bundle_manifest_path: str
    sources_lock_path: str
    entities_path: str
    rules_path: str
    generated_at: str
    source_count: int
    entity_record_count: int
    rule_record_count: int
    organization_count: int
    exchange_count: int
    market_index_count: int
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class FinanceCountryBundleBuildResult:
    """Aggregate built country finance bundles."""

    output_dir: str
    generated_at: str
    country_count: int
    bundles: list[FinanceCountryBundleItem] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def available_finance_country_codes() -> tuple[str, ...]:
    """Return the supported country codes for country finance packs."""

    return tuple(sorted(FINANCE_COUNTRY_PROFILES))


def fetch_finance_country_source_snapshots(
    *,
    output_dir: str | Path = DEFAULT_FINANCE_COUNTRY_SOURCE_OUTPUT_ROOT,
    snapshot: str | None = None,
    country_codes: list[str] | tuple[str, ...] | None = None,
    user_agent: str = DEFAULT_COUNTRY_SOURCE_FETCH_USER_AGENT,
) -> FinanceCountrySourceFetchResult:
    """Download official source landing pages for the requested finance country packs."""

    resolved_snapshot = _resolve_snapshot(snapshot)
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir = resolved_output_dir / resolved_snapshot
    if snapshot_dir.exists():
        raise FileExistsError(f"Snapshot directory already exists: {snapshot_dir}")
    generated_at = _utc_timestamp()
    selected_profiles = _select_profiles(country_codes)
    warnings: list[str] = []
    items: list[FinanceCountrySourceFetchItem] = []
    try:
        for profile in selected_profiles:
            item, item_warnings = _fetch_country_snapshot(
                snapshot_dir=snapshot_dir,
                generated_at=generated_at,
                profile=profile,
                user_agent=user_agent,
            )
            items.append(item)
            warnings.extend(item_warnings)
    except Exception:
        shutil.rmtree(snapshot_dir, ignore_errors=True)
        raise
    return FinanceCountrySourceFetchResult(
        output_dir=str(resolved_output_dir),
        snapshot=resolved_snapshot,
        snapshot_dir=str(snapshot_dir),
        generated_at=generated_at,
        country_count=len(items),
        countries=items,
        warnings=warnings,
    )


def build_finance_country_source_bundles(
    *,
    snapshot_dir: str | Path,
    output_dir: str | Path = DEFAULT_FINANCE_COUNTRY_BUNDLE_OUTPUT_ROOT,
    country_codes: list[str] | tuple[str, ...] | None = None,
    version: str = "0.2.0",
) -> FinanceCountryBundleBuildResult:
    """Build country-scoped finance bundles from downloaded country source snapshots."""

    resolved_snapshot_dir = Path(snapshot_dir).expanduser().resolve()
    if not resolved_snapshot_dir.exists():
        raise FileNotFoundError(f"Snapshot directory not found: {resolved_snapshot_dir}")
    if not resolved_snapshot_dir.is_dir():
        raise NotADirectoryError(
            f"Snapshot directory is not a directory: {resolved_snapshot_dir}"
        )
    selected_profiles = _select_profiles_for_snapshot(
        snapshot_dir=resolved_snapshot_dir,
        country_codes=country_codes,
    )
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []
    items: list[FinanceCountryBundleItem] = []
    for profile in selected_profiles:
        item = _build_country_bundle(
            snapshot_dir=resolved_snapshot_dir,
            output_dir=resolved_output_dir,
            profile=profile,
            version=version,
        )
        items.append(item)
        warnings.extend(item.warnings)
    return FinanceCountryBundleBuildResult(
        output_dir=str(resolved_output_dir),
        generated_at=_utc_timestamp(),
        country_count=len(items),
        bundles=items,
        warnings=warnings,
    )


def _fetch_country_snapshot(
    *,
    snapshot_dir: Path,
    generated_at: str,
    profile: dict[str, object],
    user_agent: str,
) -> tuple[FinanceCountrySourceFetchItem, list[str]]:
    country_code = str(profile["country_code"])
    country_name = str(profile["country_name"])
    pack_id = str(profile["pack_id"])
    country_dir = snapshot_dir / country_code
    country_dir.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []
    downloaded_sources: list[dict[str, object]] = []
    for source in profile["sources"]:
        source_name = str(source["name"])
        source_url = str(source["source_url"])
        destination = country_dir / _source_destination_filename(
            source_name=source_name,
            source_url=source_url,
        )
        try:
            resolved_url = _download_source(
                source_url,
                destination,
                user_agent=user_agent,
            )
        except Exception as exc:
            warnings.append(
                f"{pack_id}:{source_name} source download failed for {source_url}: {exc}"
            )
            continue
        downloaded_sources.append(
            {
                "name": source_name,
                "source_url": resolved_url,
                "path": destination.name,
                "category": str(source["category"]),
                "notes": str(source.get("notes", "")),
            }
        )
    curated_entities_path = country_dir / "curated_finance_entities.json"
    _write_curated_entities(curated_entities_path, entities=profile["entities"])
    profile_path = country_dir / "country_profile.json"
    _write_country_profile(profile_path, profile=profile)
    source_manifest_path = country_dir / "sources.fetch.json"
    _write_country_source_manifest(
        path=source_manifest_path,
        generated_at=generated_at,
        user_agent=user_agent,
        profile=profile,
        downloaded_sources=downloaded_sources,
        curated_entities_path=curated_entities_path,
    )
    item = FinanceCountrySourceFetchItem(
        country_code=country_code,
        country_name=country_name,
        pack_id=pack_id,
        snapshot_dir=str(country_dir),
        profile_path=str(profile_path),
        source_manifest_path=str(source_manifest_path),
        curated_entities_path=str(curated_entities_path),
        source_count=len(downloaded_sources) + 1,
        curated_entity_count=len(profile["entities"]),
        warnings=warnings,
    )
    return item, warnings


def _build_country_bundle(
    *,
    snapshot_dir: Path,
    output_dir: Path,
    profile: dict[str, object],
    version: str,
) -> FinanceCountryBundleItem:
    country_code = str(profile["country_code"])
    country_name = str(profile["country_name"])
    pack_id = str(profile["pack_id"])
    country_source_dir = snapshot_dir / country_code
    if not country_source_dir.exists():
        raise FileNotFoundError(f"Country snapshot directory not found: {country_source_dir}")
    profile_path = country_source_dir / "country_profile.json"
    curated_entities_path = country_source_dir / "curated_finance_entities.json"
    manifest_path = country_source_dir / "sources.fetch.json"
    if not profile_path.exists():
        raise FileNotFoundError(f"Country profile not found: {profile_path}")
    if not curated_entities_path.exists():
        raise FileNotFoundError(f"Curated entities not found: {curated_entities_path}")
    if not manifest_path.exists():
        raise FileNotFoundError(f"Source manifest not found: {manifest_path}")
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    pack_id = str(profile["pack_id"])
    country_name = str(profile["country_name"])
    bundle_dir = output_dir / f"{pack_id}-bundle"
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    entities = _load_curated_entities(curated_entities_path)
    rules = _build_finance_rule_records()
    entities_path = normalized_dir / "entities.jsonl"
    rules_path = normalized_dir / "rules.jsonl"
    _write_jsonl(entities_path, entities)
    _write_jsonl(rules_path, rules)
    source_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    bundle_sources = [
        build_bundle_source_entry(
            name=str(source["name"]),
            snapshot_path=country_source_dir / str(source["path"]),
            record_count=1,
            adapter="country_finance_source_snapshot",
            extra_notes=f"category={source['category']}",
        )
        for source in source_manifest["sources"]
    ]
    bundle_sources.append(
        build_bundle_source_entry(
            name="curated-finance-country-entities",
            snapshot_path=curated_entities_path,
            record_count=len(entities),
            adapter="curated_finance_country_entities",
            extra_notes=f"country_code={country_code}",
        )
    )
    bundle_manifest_path = bundle_dir / "bundle.json"
    bundle_manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": pack_id,
                "version": version,
                "language": "en",
                "domain": "finance",
                "tier": "domain",
                "description": str(profile["description"]),
                "tags": [
                    "finance",
                    "generated",
                    "english",
                    country_code,
                    "country",
                ],
                "dependencies": ["general-en", "finance-en"],
                "entities_path": "normalized/entities.jsonl",
                "rules_path": "normalized/rules.jsonl",
                "label_mappings": {
                    "issuer": "organization",
                    "organization": "organization",
                    "ticker": "ticker",
                    "person": "person",
                    "exchange": "exchange",
                    "market_index": "market_index",
                    "commodity": "commodity",
                },
                "stoplisted_aliases": _FINANCE_COUNTRY_STOPLISTED_ALIASES,
                "allowed_ambiguous_aliases": [],
                "coverage": {
                    "country_code": country_code,
                    "country_name": country_name,
                    "organization_count": sum(
                        1 for entity in entities if entity.get("entity_type") == "organization"
                    ),
                    "exchange_count": sum(
                        1 for entity in entities if entity.get("entity_type") == "exchange"
                    ),
                    "market_index_count": sum(
                        1 for entity in entities if entity.get("entity_type") == "market_index"
                    ),
                },
                "sources": bundle_sources,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    sources_lock_path = write_sources_lock(
        bundle_dir=bundle_dir,
        pack_id=pack_id,
        version=version,
        entities_path="normalized/entities.jsonl",
        rules_path="normalized/rules.jsonl",
        sources=bundle_sources,
    )
    organization_count = sum(
        1 for entity in entities if entity.get("entity_type") == "organization"
    )
    exchange_count = sum(
        1 for entity in entities if entity.get("entity_type") == "exchange"
    )
    market_index_count = sum(
        1 for entity in entities if entity.get("entity_type") == "market_index"
    )
    warnings: list[str] = []
    if not entities:
        warnings.append(f"{pack_id}: no curated entities loaded")
    return FinanceCountryBundleItem(
        country_code=country_code,
        country_name=country_name,
        pack_id=pack_id,
        version=version,
        bundle_dir=str(bundle_dir),
        bundle_manifest_path=str(bundle_manifest_path),
        sources_lock_path=str(sources_lock_path),
        entities_path=str(entities_path),
        rules_path=str(rules_path),
        generated_at=_utc_timestamp(),
        source_count=len(bundle_sources),
        entity_record_count=len(entities),
        rule_record_count=len(rules),
        organization_count=organization_count,
        exchange_count=exchange_count,
        market_index_count=market_index_count,
        warnings=warnings,
    )


def _select_profiles(
    country_codes: list[str] | tuple[str, ...] | None,
) -> list[dict[str, object]]:
    if not country_codes:
        selected_codes = available_finance_country_codes()
    else:
        selected_codes = tuple(_normalize_country_code(code) for code in country_codes)
    missing_codes = [
        code for code in selected_codes if code not in FINANCE_COUNTRY_PROFILES
    ]
    if missing_codes:
        missing_text = ", ".join(sorted(missing_codes))
        raise ValueError(f"Unsupported finance country codes: {missing_text}")
    return [FINANCE_COUNTRY_PROFILES[code] for code in selected_codes]


def _select_profiles_for_snapshot(
    *,
    snapshot_dir: Path,
    country_codes: list[str] | tuple[str, ...] | None,
) -> list[dict[str, object]]:
    if not country_codes:
        selected_codes = tuple(
            sorted(path.name for path in snapshot_dir.iterdir() if path.is_dir())
        )
    else:
        selected_codes = tuple(_normalize_country_code(code) for code in country_codes)
    if not selected_codes:
        raise ValueError(f"No country snapshots found in: {snapshot_dir}")
    profiles: list[dict[str, object]] = []
    for code in selected_codes:
        profile_path = snapshot_dir / code / "country_profile.json"
        if not profile_path.exists():
            raise FileNotFoundError(f"Country profile not found: {profile_path}")
        payload = json.loads(profile_path.read_text(encoding="utf-8"))
        if _normalize_country_code(str(payload.get("country_code", ""))) != code:
            raise ValueError(f"Country profile code mismatch: {profile_path}")
        profiles.append(payload)
    return profiles


def _normalize_country_code(value: str) -> str:
    normalized = value.strip().casefold()
    if not re.fullmatch(r"[a-z]{2}", normalized):
        raise ValueError(f"Country code must be a two-letter code: {value!r}")
    return normalized


def _resolve_snapshot(snapshot: str | None) -> str:
    if snapshot is None:
        return date.today().isoformat()
    return date.fromisoformat(snapshot).isoformat()


def _source_destination_filename(*, source_name: str, source_url: str) -> str:
    parsed = urlparse(normalize_source_url(source_url))
    suffix = Path(parsed.path).suffix.lower()
    if suffix not in {".html", ".htm", ".xml", ".json", ".csv", ".txt", ".pdf"}:
        suffix = ".html"
    safe_name = re.sub(r"[^a-z0-9]+", "-", source_name.casefold()).strip("-")
    return f"{safe_name}{suffix}"


def _download_source(source_url: str | Path, destination: Path, *, user_agent: str) -> str:
    normalized_source_url = normalize_source_url(source_url)
    parsed = urlparse(normalized_source_url)
    if parsed.scheme in {"http", "https"}:
        response = httpx.get(
            normalized_source_url,
            headers={
                "User-Agent": user_agent,
                "Accept": "text/html,application/json,text/plain;q=0.9,*/*;q=0.1",
            },
            follow_redirects=True,
            timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        destination.write_bytes(response.content)
        return normalized_source_url
    request: str | urllib.request.Request = normalized_source_url
    with urllib.request.urlopen(
        request,
        timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
    ) as response:
        destination.write_bytes(response.read())
    return normalized_source_url


def _write_curated_entities(path: Path, *, entities: list[dict[str, object]]) -> None:
    path.write_text(
        json.dumps({"entities": entities}, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_country_profile(path: Path, *, profile: dict[str, object]) -> None:
    path.write_text(
        json.dumps(profile, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_country_source_manifest(
    *,
    path: Path,
    generated_at: str,
    user_agent: str,
    profile: dict[str, object],
    downloaded_sources: list[dict[str, object]],
    curated_entities_path: Path,
) -> None:
    sources = [
        {
            "name": str(source["name"]),
            "source_url": str(source["source_url"]),
            "path": str(source["path"]),
            "category": str(source["category"]),
            "notes": str(source.get("notes", "")),
            "sha256": _sha256_file(path.parent / str(source["path"])),
            "size_bytes": (path.parent / str(source["path"])).stat().st_size,
        }
        for source in downloaded_sources
    ]
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": str(profile["pack_id"]),
                "country_code": str(profile["country_code"]),
                "country_name": str(profile["country_name"]),
                "generated_at": generated_at,
                "user_agent": user_agent,
                "sources": sources,
                "curated_entities": {
                    "path": curated_entities_path.name,
                    "sha256": _sha256_file(curated_entities_path),
                    "size_bytes": curated_entities_path.stat().st_size,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _load_curated_entities(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    entities = payload.get("entities", [])
    if not isinstance(entities, list):
        raise ValueError(f"Curated finance country entities must be a list: {path}")
    return [entity for entity in entities if isinstance(entity, dict)]


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=True) + "\n" for record in records),
        encoding="utf-8",
    )


def _sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
