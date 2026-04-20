"""Country-scoped finance source fetchers and bundle builders."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
import codecs
import csv
import html
import httpx
import json
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any
import unicodedata
import urllib.request
from urllib.parse import quote, urlencode, urljoin, urlparse
import xml.etree.ElementTree as ET
import zipfile

from .fetch import normalize_source_url
from .finance_bundle import (
    _build_finance_rule_records,
    _build_sec_company_issuers,
    _clean_text,
    _collect_sec_source_ids,
    _load_finance_people_entities,
    _load_sec_company_items,
    _load_sec_submission_profiles,
)
from .finance_sources import (
    DEFAULT_SEC_ARCHIVES_BASE_URL,
    DEFAULT_SEC_COMPANIES_URL,
    DEFAULT_SEC_SUBMISSIONS_URL,
    _write_sec_proxy_people_entities,
)
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
_KAP_LISTING_PATH = "https://www.kap.org.tr/en/bist-sirketler"
_KAP_COMPANY_DETAIL_URL_TEMPLATE = (
    "https://www.kap.org.tr/en/sirket-bilgileri/genel/{permalink}"
)
_KAP_COMPANY_LISTING_PATTERN = re.compile(
    r'\\"permaLink\\":\\"(?P<permalink>[^\\]+)\\",'
    r'\\"title\\":\\"(?P<title>[^\\]+)\\",'
    r'\\"fundCode\\":\\"(?P<fund_code>[^\\]+)\\"'
)
_KAP_STOCK_CODE_PATTERN = re.compile(r'\\"stockCode\\":\\"(?P<stock_code>[^\\]+)\\"')
_KAP_ITEM_OBJECT_NEEDLE = '\\"itemObject\\":{'
_KAP_ROLE_CLASS_BY_ITEM_KEY = {
    "kpy41_acc1_yatirimci_iliskileri": "investor_relations",
    "kpy41_acc6_yonetim_kurulu_uyeleri": "director",
    "kpy41_acc6_yonetimde_soz_sahibi": "executive_officer",
}
_ASX_LISTED_COMPANIES_CSV_URL = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
_ASX_BOARD_URL = "https://www.asx.com.au/about/our-board-and-management"
_ASX_EXECUTIVE_TEAM_URL = (
    "https://www.asx.com.au/about/our-board-and-management/our-executive-team"
)
_ASX_BIO_PATTERN = re.compile(
    r'<div class="bio__heading"><h3>(?P<name>.*?)</h3>\s*<p>(?P<title>.*?)</p>',
    re.IGNORECASE | re.DOTALL,
)
_DEUTSCHE_BOERSE_LISTED_COMPANIES_URL = (
    "https://www.cashmarket.deutsche-boerse.com/cash-en/Data-Tech/statistics/listed-companies"
)
_BOERSE_MUENCHEN_MACCESS_LISTED_COMPANIES_URL = (
    "https://www.boerse-muenchen.de/maccess/gelistete-unternehmen"
)
_BOERSE_DUESSELDORF_PRIMARY_MARKET_URL = (
    "https://www.boerse-duesseldorf.de/aktien-primaermarkt/"
)
_TRADEGATE_ORDER_BOOK_URL_TEMPLATE = (
    "https://www.tradegate.de/orderbuch.php?isin={isin}&lang=en"
)
_BAFIN_COMPANY_DATABASE_PORTAL_URL = "https://portal.mvp.bafin.de/database/InstInfo/"
_BAFIN_COMPANY_DATABASE_SEARCH_URL = (
    "https://portal.mvp.bafin.de/database/InstInfo/sucheForm.do"
)
_BAFIN_COMPANY_DATABASE_EXPORT_CSV_URL = (
    "https://portal.mvp.bafin.de/database/InstInfo/sucheForm.do"
    "?6578706f7274=1&sucheButtonInstitut=Suche&institutName=&d-4012550-e=1"
)
_WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
_WIKIDATA_ENTITY_URL_TEMPLATE = "https://www.wikidata.org/wiki/{qid}"
_WIKIDATA_SEARCH_LIMIT = 5
_WIKIDATA_GETENTITIES_BATCH_SIZE = 50
_DEUTSCHE_BOERSE_COMPANY_DETAILS_URL_TEMPLATE = (
    "https://live.deutsche-boerse.com/equity/{identifier}/company-details"
)
_DEUTSCHE_BOERSE_COMPANY_DETAILS_RENDER_BUDGET_MS = 20_000
_DEUTSCHE_BOERSE_HEADLESS_CHROME_CANDIDATES = (
    "google-chrome",
    "chromium",
    "chromium-browser",
)
_UNTERNEHMENSREGISTER_REGISTER_INFORMATION_URL = (
    "https://www.unternehmensregister.de/en/search/register-information"
)
_UNTERNEHMENSREGISTER_REGISTER_PORTAL_ADVICE_URL = (
    "https://www.unternehmensregister.de/en/registerPortalAdvice"
)
_DEUTSCHE_BOERSE_LISTED_COMPANIES_LINK_PATTERN = re.compile(
    r'(?P<url>(?:https?://|/)[^"\']*Listed-companies\.xlsx)',
    re.IGNORECASE,
)
_DEUTSCHE_BOERSE_COMPANY_DETAILS_ROW_ROLE_CLASS = {
    "executive board": "executive_officer",
    "supervisory board": "director",
}
_DEUTSCHE_BOERSE_COMPANY_DETAILS_DEFAULT_ROLE_TITLE = {
    "executive board": "Executive Board Member",
    "supervisory board": "Supervisory Board Member",
}
_GERMANY_MARKET_TABLE_CELL_PATTERN = re.compile(
    r"<td[^>]*>(?P<cell>.*?)</td>",
    re.IGNORECASE | re.DOTALL,
)
_GERMANY_MARKET_LINK_ISIN_PATTERN = re.compile(
    r'href="(?P<path>/akti(?:e|en)/(?P<isin>[A-Z0-9]{12})/[^"]*)"',
    re.IGNORECASE,
)
_GERMANY_MARKET_SUB_INFO_PATTERN = re.compile(
    r"<span[^>]*class=\"[^\"]*sub-info[^\"]*\"[^>]*>.*?</span>",
    re.IGNORECASE | re.DOTALL,
)
_TRADEGATE_ORDER_BOOK_IDENTIFIER_PATTERN = re.compile(
    r"<th>\s*WKN\s*</th>\s*"
    r"<th>\s*Code\s*</th>\s*"
    r"<th>\s*ISIN\s*</th>\s*"
    r"<th>\s*Trading Currency\s*</th>.*?"
    r"<tr>\s*"
    r"<td[^>]*>(?P<wkn>.*?)</td>\s*"
    r"<td[^>]*>(?P<code>.*?)</td>\s*"
    r"<td[^>]*>(?P<isin>.*?)</td>",
    re.IGNORECASE | re.DOTALL,
)
_GERMAN_GOVERNANCE_TABLE_ROW_PATTERN = re.compile(
    r"<tr[^>]*>(?P<row>.*?)</tr>",
    re.IGNORECASE | re.DOTALL,
)
_GERMAN_GOVERNANCE_CARD_PATTERN = re.compile(
    r"<h[2-6][^>]*>(?P<name>.*?)</h[2-6]>\s*<p[^>]*>(?P<role>.*?)</p>",
    re.IGNORECASE | re.DOTALL,
)
_GERMAN_GOVERNANCE_ROLE_HEADINGS = {
    "director": ("supervisory board", "aufsichtsrat"),
    "executive_officer": (
        "board of management",
        "management board",
        "executive board",
        "managing directors",
        "managing director",
        "geschäftsführer",
        "vorstand",
    ),
    "investor_relations": ("investor relations",),
}
_FCA_OFFICIAL_LIST_LIVE_URL = "https://marketsecurities.fca.org.uk/downloadOfficiallist/live"
_COMPANIES_HOUSE_SEARCH_URL_TEMPLATE = (
    "https://find-and-update.company-information.service.gov.uk/search/companies?q={query}"
)
_COMPANIES_HOUSE_OFFICERS_URL_TEMPLATE = (
    "https://find-and-update.company-information.service.gov.uk/company/{company_number}/officers"
)
_UK_OFFICIAL_LIST_COMPANY_CATEGORIES = frozenset({"equity shares (commercial companies)"})
_COMPANIES_HOUSE_COMPANY_SUFFIX_TOKENS = frozenset(
    {
        "limited",
        "ltd",
        "llc",
        "llp",
        "plc",
        "group",
        "company",
        "co",
        "corp",
        "corporation",
        "inc",
        "partners",
        "trust",
        "fund",
        "secretaries",
        "services",
    }
)
_GERMANY_COMPANY_LEGAL_SUFFIX_PATTERNS = (
    re.compile(r",?\s+gmbh\s*&\s*co\.?\s*kgaa$", re.IGNORECASE),
    re.compile(r",?\s+gmbh\s*&\s*co\.?\s*kg$", re.IGNORECASE),
    re.compile(r",?\s+ag\s*&\s*co\.?\s*kgaa$", re.IGNORECASE),
    re.compile(r",?\s+kgaa$", re.IGNORECASE),
    re.compile(r",?\s+gmbh$", re.IGNORECASE),
    re.compile(r",?\s+ag$", re.IGNORECASE),
    re.compile(r",?\s+se$", re.IGNORECASE),
    re.compile(r",?\s+n\.?\s*v\.?$", re.IGNORECASE),
    re.compile(r",?\s+sa$", re.IGNORECASE),
    re.compile(r",?\s+plc$", re.IGNORECASE),
)
_COMPANIES_HOUSE_HONORIFIC_SUFFIXES = frozenset(
    {"mr", "mrs", "ms", "miss", "sir", "dr", "prof", "professor", "lord", "lady"}
)


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
    extra_fields: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "country_code": country_code,
        "country_name": country_name,
        "pack_id": pack_id,
        "description": description,
        "sources": list(sources),
        "entities": list(entities),
    }
    if extra_fields:
        payload.update(extra_fields)
    return payload


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
            _source(
                "asx-listed-companies",
                _ASX_LISTED_COMPANIES_CSV_URL,
                category="issuer_directory",
            ),
            _source("asx-board", _ASX_BOARD_URL, category="issuer_profile"),
            _source(
                "asx-executive-team",
                _ASX_EXECUTIVE_TEAM_URL,
                category="issuer_profile",
            ),
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
            _source(
                "boerse-muenchen",
                "https://www.boerse-muenchen.de/",
                category="exchange",
            ),
            _source(
                "boerse-duesseldorf",
                "https://www.boerse-duesseldorf.de/",
                category="exchange",
            ),
            _source(
                "tradegate-exchange",
                "https://www.tradegate.de/",
                category="exchange",
            ),
            _source(
                "bafin-company-database-export",
                _BAFIN_COMPANY_DATABASE_EXPORT_CSV_URL,
                category="institution_register",
                notes="Use the official BaFin company-database CSV export to add Germany-domiciled regulated institutions and Germany branches with BaFin metadata.",
            ),
            _source(
                "deutsche-boerse-listed-companies",
                _DEUTSCHE_BOERSE_LISTED_COMPANIES_URL,
                category="issuer_directory",
                notes="Use the Deutsche Borse listed-companies workbook as the issuer and ticker seed set.",
            ),
            _source(
                "boerse-muenchen-maccess-listed-companies",
                _BOERSE_MUENCHEN_MACCESS_LISTED_COMPANIES_URL,
                category="issuer_directory",
                notes="Use the official m:access issuer table to extend Germany small-cap issuer coverage beyond the Deutsche Borse workbook.",
            ),
            _source(
                "boerse-duesseldorf-primary-market",
                _BOERSE_DUESSELDORF_PRIMARY_MARKET_URL,
                category="issuer_directory",
                notes="Use the official Primarmarkt table to extend Germany issuer coverage for companies listed in Dusseldorf's quality segment.",
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
                "exchange",
                "Börse München",
                aliases=("Munich Stock Exchange", "Borse Munchen"),
                entity_id="finance-de:boerse-muenchen",
                metadata={"country_code": "de", "category": "exchange"},
            ),
            _entity(
                "exchange",
                "Börse Düsseldorf",
                aliases=("Dusseldorf Stock Exchange", "Borse Dusseldorf"),
                entity_id="finance-de:boerse-duesseldorf",
                metadata={"country_code": "de", "category": "exchange"},
            ),
            _entity(
                "exchange",
                "Tradegate Exchange",
                aliases=("Tradegate",),
                entity_id="finance-de:tradegate-exchange",
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
            _source("kap", _KAP_LISTING_PATH, category="filing_system"),
            _source(
                "borsa-istanbul",
                "https://www.borsaistanbul.com/en/companies/listed-companies",
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
                "fca-official-list-live",
                _FCA_OFFICIAL_LIST_LIVE_URL,
                category="issuer_directory",
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
            _source(
                "companies-house-register",
                "https://www.gov.uk/guidance/search-the-companies-house-register",
                category="company_registry",
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
                "sec-company-tickers",
                DEFAULT_SEC_COMPANIES_URL,
                category="issuer_directory",
                notes="Official SEC company-ticker universe used to bind issuers, CIKs, and tickers.",
            ),
            _source(
                "sec-submissions",
                DEFAULT_SEC_SUBMISSIONS_URL,
                category="filing_system",
                notes="Official SEC bulk submissions archive used for listed-company people derivation.",
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
        extra_fields={
            "people_derivation": {
                "kind": "sec_proxy",
                "companies_source": "sec-company-tickers",
                "submissions_source": "sec-submissions",
                "archive_base_url": DEFAULT_SEC_ARCHIVES_BASE_URL,
                "output_path": "derived_sec_proxy_people_entities.json",
                "name": "derived-finance-us-sec-proxy-people",
                "adapter": "sec_proxy_people_derivation",
            }
        },
    ),
}


FINANCE_COUNTRY_PEOPLE_SOURCE_PLANS: dict[str, dict[str, object]] = {
    "ar": {
        "country_code": "ar",
        "country_name": "Argentina",
        "automation_status": "planned",
        "scope": "listed-company directors, executive officers, investor-relations contacts, and regulated financial-institution principals",
        "update_strategy": [
            "Refresh CNV/AIF issuer disclosures and BYMA issuer pages on each country snapshot.",
            "Extract board and management names from annual-report, governance, and issuer-profile disclosures.",
            "Add institution-register lanes only after validating official publication terms.",
        ],
        "sources": [
            _source("cnv-aif-issuers", "https://www.cnv.gov.ar/", category="filing_system", notes="Use issuer disclosure records and annual filings for board and officer names."),
            _source("byma-issuer-pages", "https://www.byma.com.ar/en/listed-companies/", category="issuer_directory", notes="Use listed-company pages and linked reports for issuer-to-person joins."),
        ],
    },
    "au": {
        "country_code": "au",
        "country_name": "Australia",
        "automation_status": "partial",
        "scope": "listed-company directors, named key management personnel, investor-relations contacts, and licensed financial-institution principals",
        "update_strategy": [
            "Refresh the official ASX listed-companies CSV plus ASX governance pages on each country snapshot.",
            "Current automation derives issuer/ticker coverage from the ASX listed-companies CSV and named board/executive people from ASX's official board and executive-team pages.",
            "Expand next to per-issuer annual reports, governance statements, and investor contacts linked from ASX issuer pages.",
            "Augment with ASIC/APRA registers only where official access terms allow repeatable ingestion.",
        ],
        "sources": [
            _source("asx-listed-companies", _ASX_LISTED_COMPANIES_CSV_URL, category="issuer_directory", notes="Official listed-company CSV for issuer and ticker coverage."),
            _source("asx-board", _ASX_BOARD_URL, category="issuer_profile", notes="Official ASX governance page with named ASX Limited board members; current automated people lane uses this directly."),
            _source("asx-executive-team", _ASX_EXECUTIVE_TEAM_URL, category="issuer_profile", notes="Official ASX management page with named ASX Limited executives; current automated people lane uses this directly."),
            _source("asic-company-search", "https://asic.gov.au/for-business/search-registers/", category="company_registry", notes="Use official officer/director registry data where access and rate limits permit."),
            _source("apra-regulated-entities", "https://www.apra.gov.au/registers-of-authorised-institutions", category="institution_register", notes="Use for bank and insurer principals when publication format is stable."),
        ],
    },
    "br": {
        "country_code": "br",
        "country_name": "Brazil",
        "automation_status": "planned",
        "scope": "listed-company board members, executive officers, investor-relations officers, and regulated institution principals",
        "update_strategy": [
            "Refresh CVM issuer disclosure pages and B3 listed-company pages on each snapshot.",
            "Extract people from issuer reference forms, annual reports, and governance materials.",
            "Add central-bank institution registers only after schema and access rules are stable.",
        ],
        "sources": [
            _source("cvm-companhia-aberta", "https://www.gov.br/cvm/pt-br", category="filing_system", notes="Use issuer filings and reference forms for management and board names."),
            _source("b3-listed-companies", "https://www.b3.com.br/en_us/products-and-services/trading/equities/companies-listed.htm", category="issuer_directory", notes="Use issuer pages and linked reference materials."),
        ],
    },
    "ca": {
        "country_code": "ca",
        "country_name": "Canada",
        "automation_status": "planned",
        "scope": "listed-company directors, named executive officers, investor-relations contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh SEDAR+ issuer filings and TSX issuer profiles on each country snapshot.",
            "Extract people from management information circulars, annual information forms, and management discussion documents.",
            "Add OSFI and provincial institution registers for non-issuer financial institutions.",
        ],
        "sources": [
            _source("sedar-plus-filings", "https://www.sedarplus.ca/", category="filing_system", notes="Primary filing source for directors and officers via circulars and annual filings."),
            _source("tsx-listed-companies", "https://www.tsx.com/en/listings/listing-with-us/listed-company-directory", category="issuer_directory", notes="Primary issuer directory and ticker join source."),
            _source("osfi-regulated-entities", "https://www.osfi-bsif.gc.ca/en/about-osfi/osfi-knowledge-centre/who-does-osfi-regulate", category="institution_register", notes="Use for banks and insurers outside listed-issuer coverage."),
        ],
    },
    "cn": {
        "country_code": "cn",
        "country_name": "China",
        "automation_status": "planned",
        "scope": "listed-company directors, supervisors, senior management, and state financial-institution principals",
        "update_strategy": [
            "Refresh SSE and SZSE issuer profile pages plus linked annual reports and governance reports.",
            "Extract board and management names from official issuer reports rather than third-party portals.",
            "Treat non-English publication changes as a schema-risk lane and verify sample output each refresh.",
        ],
        "sources": [
            _source("sse-listed-companies", "https://english.sse.com.cn/markets/equities/listedcompanies/", category="issuer_directory", notes="Use issuer profile pages and annual reports."),
            _source("szse-listed-companies", "https://www.szse.cn/English/services/companyInfo/index.html", category="issuer_directory", notes="Use issuer information pages and linked disclosures."),
            _source("csrc-disclosures", "https://www.csrc.gov.cn/csrc_en/", category="securities_regulator", notes="Use official regulator publications for governance and disclosure metadata."),
        ],
    },
    "de": {
        "country_code": "de",
        "country_name": "Germany",
        "automation_status": "partial",
        "scope": "listed-company management board members, supervisory board members, issuer contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh the Deutsche Borse listed-companies workbook to seed issuer and ticker coverage.",
            "Refresh the official Borse Munchen m:access table and Borse Dusseldorf Primarmarkt table to extend issuer coverage beyond Frankfurt/Xetra.",
            "Refresh Unternehmensregister issuer filings, Deutsche Borse company-details pages, and official German issuer governance pages.",
            "Refresh the BaFin company-database CSV export and merge Germany-domiciled institutions plus Germany branches into the pack.",
            "Extract people from annual reports, corporate-governance statements, and issuer profile disclosures.",
        ],
        "sources": [
            _source("deutsche-boerse-listed-companies", _DEUTSCHE_BOERSE_LISTED_COMPANIES_URL, category="issuer_directory", notes="Primary issuer and ticker seed source for listed German companies."),
            _source("boerse-muenchen-maccess-listed-companies", _BOERSE_MUENCHEN_MACCESS_LISTED_COMPANIES_URL, category="issuer_directory", notes="Official m:access issuer table for additional listed German SMEs."),
            _source("boerse-duesseldorf-primary-market", _BOERSE_DUESSELDORF_PRIMARY_MARKET_URL, category="issuer_directory", notes="Official Primarmarkt issuer table for Dusseldorf-listed companies."),
            _source("unternehmensregister-filings", "https://www.unternehmensregister.de/ureg/?submitaction=language&language=en", category="filing_system", notes="Primary official filing source for governance and board disclosures."),
            _source("bafin-company-database-export", _BAFIN_COMPANY_DATABASE_EXPORT_CSV_URL, category="institution_register", notes="Official BaFin company-database CSV export for Germany-domiciled institutions and Germany branches."),
        ],
    },
    "fr": {
        "country_code": "fr",
        "country_name": "France",
        "automation_status": "planned",
        "scope": "listed-company directors, executive officers, investor-relations contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh Euronext Paris issuer pages and AMF filing/disclosure pages.",
            "Extract people from universal registration documents and annual reports.",
            "Use REGAFI separately for financial-institution principals.",
        ],
        "sources": [
            _source("euronext-paris-issuers", "https://live.euronext.com/en/markets/paris/equities/list", category="issuer_directory", notes="Primary issuer directory and ticker join source."),
            _source("amf-filings", "https://www.amf-france.org/en", category="filing_system", notes="Use official filing/disclosure materials for board and executive names."),
            _source("regafi", "https://www.regafi.fr/", category="institution_register", notes="Use for licensed institution principals."),
        ],
    },
    "id": {
        "country_code": "id",
        "country_name": "Indonesia",
        "automation_status": "planned",
        "scope": "listed-company commissioners, directors, corporate-secretary contacts, and licensed institution principals",
        "update_strategy": [
            "Refresh IDX issuer profiles and annual report links on each snapshot.",
            "Extract directors, commissioners, and corporate-secretary contacts from issuer annual reports.",
            "Use OJK registries for regulated-institution principals where official publication is stable.",
        ],
        "sources": [
            _source("idx-listed-companies", "https://www.idx.co.id/en/listed-companies/company-profiles/", category="issuer_directory", notes="Primary issuer profile and annual-report link source."),
            _source("ojk-public-disclosures", "https://www.ojk.go.id/en/Default.aspx", category="securities_regulator", notes="Use public-company and institution disclosures where they publish named principals."),
        ],
    },
    "in": {
        "country_code": "in",
        "country_name": "India",
        "automation_status": "planned",
        "scope": "listed-company directors, key managerial personnel, company secretaries, and regulated institution principals",
        "update_strategy": [
            "Refresh NSE/BSE issuer pages and corporate filing feeds on each snapshot.",
            "Extract people from annual reports, shareholding patterns, and corporate-governance filings.",
            "Use MCA director/company data as the canonical private-company and officer lane where access terms permit.",
        ],
        "sources": [
            _source("nse-corporate-filings", "https://www.nseindia.com/companies-listing/corporate-filings-financial-results", category="filing_system", notes="Use issuer filing feeds and annual reports for directors and KMP."),
            _source("bse-corporate-filings", "https://www.bseindia.com/corporates/ann.html", category="filing_system", notes="Use issuer filings and corporate announcements for people extraction."),
            _source("mca-company-registry", "https://www.mca.gov.in/", category="company_registry", notes="Use for canonical director/officer names and DIN-linked identities."),
        ],
    },
    "it": {
        "country_code": "it",
        "country_name": "Italy",
        "automation_status": "planned",
        "scope": "listed-company board members, executive officers, issuer contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh Borsa Italiana issuer pages and CONSOB market disclosures on each snapshot.",
            "Extract people from annual reports, governance reports, and issuer profile pages.",
            "Add business-register or prudential-register lanes only after official-access review.",
        ],
        "sources": [
            _source("borsa-italiana-listed-companies", "https://www.borsaitaliana.it/borsa/azioni/listino-a-z.html?lang=en", category="issuer_directory", notes="Primary issuer directory and listed-company join source."),
            _source("consob-market-disclosures", "https://www.consob.it/web/consob-and-its-activities/italian-markets", category="filing_system", notes="Use official market disclosures and linked filings."),
        ],
    },
    "jp": {
        "country_code": "jp",
        "country_name": "Japan",
        "automation_status": "planned",
        "scope": "listed-company directors, officers, representative executives, investor-relations contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh EDINET issuer filings and JPX issuer/disclosure pages on each snapshot.",
            "Extract people from annual securities reports, corporate-governance reports, and timely disclosures.",
            "Use the corporate-governance report lane as a first-class people source because it exposes named directors and executives.",
        ],
        "sources": [
            _source("edinet-filings", "https://info.edinet-fsa.go.jp/", category="filing_system", notes="Primary official filing source for directors, officers, and governance disclosures."),
            _source("jpx-disclosure-gate", "https://www.jpx.co.jp/english/equities/listed-co/disclosure-gate/", category="issuer_directory", notes="Primary listed-company and disclosure join source."),
        ],
    },
    "kr": {
        "country_code": "kr",
        "country_name": "South Korea",
        "automation_status": "planned",
        "scope": "listed-company directors, executive officers, responsible management personnel, and regulated institution principals",
        "update_strategy": [
            "Refresh DART filings and KIND/KRX issuer profile pages on each snapshot.",
            "Extract people from business reports, annual reports, and corporate-governance filings.",
            "Treat Korean-language schema changes as a high-risk lane and keep sample validation in every refresh.",
        ],
        "sources": [
            _source("dart-filings", "https://englishdart.fss.or.kr/", category="filing_system", notes="Primary official filing source for directors and officers."),
            _source("kind-company-search", "https://engkind.krx.co.kr/disclosureinfo/companyinfo.do", category="issuer_directory", notes="Use issuer profile pages and disclosure joins."),
            _source("krx-listed-companies", "https://global.krx.co.kr/", category="exchange", notes="Use market metadata and listed-company identifiers."),
        ],
    },
    "mx": {
        "country_code": "mx",
        "country_name": "Mexico",
        "automation_status": "planned",
        "scope": "listed-company directors, executive officers, issuer contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh BMV issuer pages and CNBV filing/disclosure pages on each snapshot.",
            "Extract people from annual reports, governance sections, and investor-relation materials.",
            "Extend with institution-register lanes if CNBV publishes stable registries with named principals.",
        ],
        "sources": [
            _source("bmv-listed-companies", "https://www.bmv.com.mx/en/issuers", category="issuer_directory", notes="Primary issuer directory and ticker join source."),
            _source("cnbv-disclosures", "https://www.gob.mx/cnbv", category="filing_system", notes="Use official disclosure publications and issuer materials."),
        ],
    },
    "ru": {
        "country_code": "ru",
        "country_name": "Russia",
        "automation_status": "planned",
        "scope": "listed-company board members, executive officers, issuer contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh MOEX issuer pages and Bank of Russia disclosure publications on each snapshot.",
            "Extract people from annual reports and issuer profile pages when accessible.",
            "Run sanctions and publication-policy review before promoting any regenerated Russia pack.",
        ],
        "sources": [
            _source("moex-listed-companies", "https://www.moex.com/en/listing/securities-list.aspx", category="issuer_directory", notes="Primary issuer directory and disclosure join source."),
            _source("bank-of-russia-disclosures", "https://www.cbr.ru/eng/", category="filing_system", notes="Use official disclosure and institution-register publications after compliance review."),
        ],
    },
    "sa": {
        "country_code": "sa",
        "country_name": "Saudi Arabia",
        "automation_status": "planned",
        "scope": "listed-company board members, senior executives, investor-relations contacts, and licensed institution principals",
        "update_strategy": [
            "Refresh Saudi Exchange issuer profiles and announcements on each snapshot.",
            "Extract people from annual reports, governance sections, and board-management pages.",
            "Use CMA disclosures and regulator publications as secondary confirmation for named principals.",
        ],
        "sources": [
            _source("saudi-exchange-issuers", "https://www.saudiexchange.sa/wps/portal/tadawul/markets/equities/issuers-directory", category="issuer_directory", notes="Primary issuer profile and disclosure join source."),
            _source("cma-disclosures", "https://cma.gov.sa/en/Pages/default.aspx", category="filing_system", notes="Use official disclosure and governance publications."),
        ],
    },
    "tr": {
        "country_code": "tr",
        "country_name": "Türkiye",
        "automation_status": "implemented",
        "scope": "listed-company board members, investor-relations contacts, named management personnel, and disclosed executive officers from KAP company detail pages",
        "update_strategy": [
            "Refresh KAP listed-company directory and company detail pages on each country snapshot.",
            "Derive issuer, ticker, board-member, investor-relations, and management-person entities from KAP detail sections.",
            "Expand later with SPK or MKK lanes only if they add people not already disclosed in KAP.",
        ],
        "sources": [
            _source("kap-listed-companies", "https://www.kap.org.tr/en/bist-sirketler", category="issuer_directory", notes="Primary automated source for listed-company people extraction."),
            _source("kap-company-details", "https://www.kap.org.tr/en/", category="filing_system", notes="Use company detail pages and disclosure sections for board and management names."),
            _source("mkk-e-company", "https://e-sirket.mkk.com.tr/", category="company_registry", notes="Secondary official source for issuer and governance confirmation where accessible."),
        ],
    },
    "uk": {
        "country_code": "uk",
        "country_name": "United Kingdom",
        "automation_status": "partial",
        "scope": "listed-company directors, current company officers, persons discharging managerial responsibilities, issuer contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh the FCA Official List live CSV and derive UK commercial-company issuers on each snapshot.",
            "Resolve listed issuers to Companies House company numbers, then extract current officers from official officer pages.",
            "Expand later with RNS / annual-report / governance lanes for issuer personnel not represented in Companies House officer data.",
        ],
        "sources": [
            _source("fca-official-list-live", _FCA_OFFICIAL_LIST_LIVE_URL, category="issuer_directory", notes="Primary official listed-company source for UK commercial companies."),
            _source("rns-announcements", "https://www.lseg.com/en/capital-markets/regulatory-news-service", category="filing_system", notes="Use official announcements and annual reports for board and management names."),
            _source("companies-house-officers", "https://www.gov.uk/guidance/search-the-companies-house-register", category="company_registry", notes="Use company-officer registry for canonical director names."),
            _source("fca-financial-services-register", "https://www.fca.org.uk/register", category="institution_register", notes="Use for financial-institution principals."),
        ],
    },
    "us": {
        "country_code": "us",
        "country_name": "United States",
        "automation_status": "partial",
        "scope": "public-company directors, executive officers, insiders, investor-relations contacts, and regulated financial-personnel classes",
        "update_strategy": [
            "Refresh SEC issuer snapshots and proxy-filing archives on each finance source snapshot.",
            "Derive people from DEF 14A, Forms 3/4/5, and 8-K item 5.02 disclosures; reuse the existing SEC proxy-people extractor where possible.",
            "Extend country-pack coverage with exchange joins and institution/adviser registers for non-issuer finance personnel.",
        ],
        "sources": [
            _source("sec-proxy-filings", "https://www.sec.gov/search-filings", category="filing_system", notes="Primary official source for directors and executive officers via DEF 14A and related filings."),
            _source("sec-insider-filings", "https://www.sec.gov/edgar/search-and-access", category="filing_system", notes="Use Forms 3/4/5 and 8-K item 5.02 for named management changes."),
            _source("nasdaq-listed-companies", "https://www.nasdaq.com/market-activity/stocks/screener", category="issuer_directory", notes="Use listed-company joins and issuer profile links."),
            _source("nyse-listed-companies", "https://www.nyse.com/listings_directory/stock", category="issuer_directory", notes="Use listed-company joins and issuer profile links."),
            _source("iapd", "https://adviserinfo.sec.gov/", category="institution_register", notes="Use for investment-adviser principals and representatives."),
            _source("fdic-bankfind", "https://api.fdic.gov/banks/docs/", category="institution_register", notes="Use for bank institutions and named executive/board metadata where published."),
        ],
    },
    "za": {
        "country_code": "za",
        "country_name": "South Africa",
        "automation_status": "planned",
        "scope": "listed-company directors, executive officers, issuer contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh JSE issuer profiles and SENS announcements on each snapshot.",
            "Extract people from annual reports, governance statements, and issuer profile materials.",
            "Use FSCA and prudential-regulator registers for institution principals where official publication is stable.",
        ],
        "sources": [
            _source("jse-listed-companies", "https://www.jse.co.za/listings", category="issuer_directory", notes="Primary issuer directory and disclosure join source."),
            _source("jse-sens", "https://www.jse.co.za/services/market-data/market-sens", category="filing_system", notes="Use announcements and annual reports for board/management names."),
            _source("fsca-public-registers", "https://www.fsca.co.za/", category="institution_register", notes="Use for regulated institution principals."),
        ],
    },
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


def finance_country_people_source_plan(
    country_code: str | None = None,
) -> dict[str, object] | dict[str, dict[str, object]]:
    """Return the official people-source/update plan for one or all country finance packs."""

    if country_code is None:
        return FINANCE_COUNTRY_PEOPLE_SOURCE_PLANS
    normalized_country_code = _normalize_country_code(country_code)
    try:
        return FINANCE_COUNTRY_PEOPLE_SOURCE_PLANS[normalized_country_code]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported finance country code for people-source plan: {country_code}"
        ) from exc


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
    profile_path = country_dir / "country_profile.json"
    _write_country_profile(profile_path, profile=profile)
    curated_entities_path = country_dir / "curated_finance_entities.json"
    _write_curated_entities(curated_entities_path, entities=profile["entities"])
    people_source_plan_path = country_dir / "country_people_source_plan.json"
    _write_country_people_source_plan(
        people_source_plan_path,
        country_code=country_code,
    )
    source_manifest_path = country_dir / "sources.fetch.json"
    derived_entities = _derive_country_entities(
        profile=profile,
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent=user_agent,
    )
    warnings.extend(
        str(warning)
        for entity_file in derived_entities
        for warning in entity_file.get("warnings", [])
    )
    _write_country_source_manifest(
        path=source_manifest_path,
        generated_at=generated_at,
        user_agent=user_agent,
        profile=profile,
        downloaded_sources=downloaded_sources,
        curated_entities_path=curated_entities_path,
        people_source_plan_path=people_source_plan_path,
        derived_entities=derived_entities,
    )
    item = FinanceCountrySourceFetchItem(
        country_code=country_code,
        country_name=country_name,
        pack_id=pack_id,
        snapshot_dir=str(country_dir),
        profile_path=str(profile_path),
        source_manifest_path=str(source_manifest_path),
        curated_entities_path=str(curated_entities_path),
        source_count=len(downloaded_sources) + 1 + len(derived_entities),
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
    source_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    derived_entities = source_manifest.get("derived_entities", [])
    for derived_entity in derived_entities:
        entities.extend(
            _load_curated_entities(country_source_dir / str(derived_entity["path"]))
        )
    rules = _build_finance_rule_records()
    entities_path = normalized_dir / "entities.jsonl"
    rules_path = normalized_dir / "rules.jsonl"
    _write_jsonl(entities_path, entities)
    _write_jsonl(rules_path, rules)
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
    for derived_entity in derived_entities:
        bundle_sources.append(
            build_bundle_source_entry(
                name=str(derived_entity["name"]),
                snapshot_path=country_source_dir / str(derived_entity["path"]),
                record_count=int(derived_entity["record_count"]),
                adapter=str(derived_entity["adapter"]),
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
                    "person_count": sum(
                        1 for entity in entities if entity.get("entity_type") == "person"
                    ),
                    "ticker_count": sum(
                        1 for entity in entities if entity.get("entity_type") == "ticker"
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
        warnings.append(f"{pack_id}: no country entities loaded")
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
    if suffix not in {".html", ".htm", ".xml", ".json", ".csv", ".txt", ".pdf", ".xlsx"}:
        suffix = ".html"
    safe_name = re.sub(r"[^a-z0-9]+", "-", source_name.casefold()).strip("-")
    return f"{safe_name}{suffix}"


def _download_source(source_url: str | Path, destination: Path, *, user_agent: str) -> str:
    normalized_source_url = normalize_source_url(source_url)
    parsed = urlparse(normalized_source_url)
    if parsed.scheme in {"http", "https"}:
        headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/json,text/plain;q=0.9,*/*;q=0.1",
        }
        response = httpx.get(
            normalized_source_url,
            headers=headers,
            follow_redirects=True,
            timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        resolved_url = normalized_source_url
        content = response.content
        redirect_target = _extract_html_redirect_target(
            response.text,
            base_url=normalized_source_url,
            content_type=response.headers.get("content-type"),
        )
        if redirect_target is not None:
            redirect_response = httpx.get(
                redirect_target,
                headers=headers,
                follow_redirects=True,
                timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
            )
            redirect_response.raise_for_status()
            resolved_url = redirect_target
            content = redirect_response.content
        destination.write_bytes(content)
        return resolved_url
    request: str | urllib.request.Request = normalized_source_url
    with urllib.request.urlopen(
        request,
        timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
    ) as response:
        destination.write_bytes(response.read())
    return normalized_source_url


def _extract_html_redirect_target(
    html_text: str,
    *,
    base_url: str,
    content_type: str | None,
) -> str | None:
    if content_type is not None and "html" not in content_type.casefold():
        return None
    for pattern in (
        r"window\.location\.replace\('(?P<url>[^']+)'\)",
        r"window\.location\.href\s*=\s*'(?P<url>[^']+)'",
        r'window\.location\.href\s*=\s*"(?P<url>[^"]+)"',
    ):
        match = re.search(pattern, html_text, re.IGNORECASE)
        if match is not None:
            return urljoin(base_url, match.group("url"))
    meta_refresh_match = re.search(
        r'<meta[^>]+http-equiv=["\']refresh["\'][^>]+content=["\'][^"\']*url=(?P<url>[^"\']+)["\']',
        html_text,
        re.IGNORECASE,
    )
    if meta_refresh_match is not None:
        return urljoin(base_url, meta_refresh_match.group("url"))
    return None


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


def _write_country_people_source_plan(path: Path, *, country_code: str) -> None:
    path.write_text(
        json.dumps(finance_country_people_source_plan(country_code), indent=2) + "\n",
        encoding="utf-8",
    )


def _derive_country_entities(
    *,
    profile: dict[str, object],
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[dict[str, object]]:
    country_code = str(profile["country_code"])
    derived_entities = _derive_profile_people_entities(
        profile=profile,
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent=user_agent,
    )
    deriver = _COUNTRY_ENTITY_DERIVERS.get(country_code)
    if deriver is not None:
        derived_entities.extend(
            deriver(
                country_dir=country_dir,
                downloaded_sources=downloaded_sources,
                user_agent=user_agent,
            )
        )
    return derived_entities


def _derive_profile_people_entities(
    *,
    profile: dict[str, object],
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[dict[str, object]]:
    people_derivation = profile.get("people_derivation")
    if not isinstance(people_derivation, dict):
        return []
    if str(people_derivation.get("kind", "")).casefold() != "sec_proxy":
        return []
    source_paths = {
        str(source.get("name")): country_dir / str(source.get("path"))
        for source in downloaded_sources
    }
    companies_source_name = str(people_derivation.get("companies_source", ""))
    submissions_source_name = str(people_derivation.get("submissions_source", ""))
    companies_path = source_paths.get(companies_source_name)
    submissions_path = source_paths.get(submissions_source_name)
    if companies_path is None or submissions_path is None:
        return []
    if not companies_path.exists() or not submissions_path.exists():
        return []
    output_path = country_dir / str(
        people_derivation.get(
            "output_path",
            "derived_country_people_entities.json",
        )
    )
    raw_output_path = country_dir / (
        f"{output_path.stem}.raw{output_path.suffix or '.json'}"
    )
    _write_sec_proxy_people_entities(
        path=raw_output_path,
        sec_companies_path=companies_path,
        sec_submissions_path=submissions_path,
        archive_base_url=str(
            people_derivation.get("archive_base_url", DEFAULT_SEC_ARCHIVES_BASE_URL)
        ),
        user_agent=user_agent,
        max_companies=None,
    )
    normalized_people_entities = _load_finance_people_entities(raw_output_path)
    output_path.write_text(
        json.dumps({"entities": normalized_people_entities}, indent=2) + "\n",
        encoding="utf-8",
    )
    record_count = len(normalized_people_entities)
    return [
        {
            "name": str(
                people_derivation.get(
                    "name",
                    "derived-country-people-entities",
                )
            ),
            "path": output_path.name,
            "record_count": record_count,
            "adapter": str(
                people_derivation.get("adapter", "country_people_derivation")
            ),
            "sha256": _sha256_file(output_path),
            "size_bytes": output_path.stat().st_size,
        }
    ]


def _derive_turkiye_kap_entities(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[dict[str, object]]:
    kap_source = next(
        (source for source in downloaded_sources if str(source.get("name")) == "kap"),
        None,
    )
    if kap_source is None:
        return []
    directory_path = country_dir / str(kap_source["path"])
    if not directory_path.exists():
        return []
    listing_html = directory_path.read_text(encoding="utf-8", errors="ignore")
    company_records = _extract_kap_company_directory_records(listing_html)
    if not company_records:
        return []

    issuer_entities: list[dict[str, object]] = []
    people_entities: list[dict[str, object]] = []
    ticker_entities: list[dict[str, object]] = []
    warnings: list[str] = []
    for company_record in company_records:
        permalink = str(company_record["permalink"])
        company_title = str(company_record["title"])
        detail_url = _KAP_COMPANY_DETAIL_URL_TEMPLATE.format(permalink=permalink)
        detail_path = country_dir / f"kap-company-{permalink}.html"
        try:
            _download_source(detail_url, detail_path, user_agent=user_agent)
        except Exception as exc:
            warnings.append(f"kap company detail fetch failed for {detail_url}: {exc}")
            continue
        detail_html = detail_path.read_text(encoding="utf-8", errors="ignore")
        stock_code = _extract_kap_stock_code(detail_html)
        issuer_entities.append(
            _build_kap_company_entity(
                company_title=company_title,
                stock_code=stock_code,
            )
        )
        if stock_code:
            ticker_entities.append(
                _build_kap_ticker_entity(
                    company_title=company_title,
                    stock_code=stock_code,
                )
            )
        people_entities.extend(
            _extract_kap_people_entities(
                detail_html=detail_html,
                company_title=company_title,
                stock_code=stock_code,
            )
        )

    derived_files: list[dict[str, object]] = []
    if issuer_entities:
        issuer_path = country_dir / "derived_issuer_entities.json"
        _write_curated_entities(issuer_path, entities=_dedupe_entities(issuer_entities))
        derived_files.append(
            {
                "name": "derived-finance-tr-issuers",
                "path": issuer_path.name,
                "record_count": len(_load_curated_entities(issuer_path)),
                "adapter": "kap_company_directory",
                "warnings": warnings,
            }
        )
    if ticker_entities:
        ticker_path = country_dir / "derived_ticker_entities.json"
        _write_curated_entities(ticker_path, entities=_dedupe_entities(ticker_entities))
        derived_files.append(
            {
                "name": "derived-finance-tr-tickers",
                "path": ticker_path.name,
                "record_count": len(_load_curated_entities(ticker_path)),
                "adapter": "kap_company_directory",
                "warnings": [],
            }
        )
    if people_entities:
        people_path = country_dir / "derived_people_entities.json"
        _write_curated_entities(people_path, entities=_dedupe_entities(people_entities))
        derived_files.append(
            {
                "name": "derived-finance-tr-people",
                "path": people_path.name,
                "record_count": len(_load_curated_entities(people_path)),
                "adapter": "kap_company_detail_people",
                "warnings": [],
            }
        )
    return derived_files


def _derive_australia_asx_entities(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[dict[str, object]]:
    del user_agent
    source_paths = {
        str(source.get("name")): country_dir / str(source.get("path"))
        for source in downloaded_sources
    }
    listed_companies_path = source_paths.get("asx-listed-companies")
    board_path = source_paths.get("asx-board")
    executive_path = source_paths.get("asx-executive-team")

    issuer_entities: list[dict[str, object]] = []
    ticker_entities: list[dict[str, object]] = []
    people_entities: list[dict[str, object]] = []

    if listed_companies_path is not None and listed_companies_path.exists():
        issuer_entities, ticker_entities = _extract_asx_listed_company_entities(
            listed_companies_path
        )
    if board_path is not None and board_path.exists():
        people_entities.extend(
            _extract_asx_people_entities(
                board_path.read_text(encoding="utf-8", errors="ignore"),
                employer_name="ASX Limited",
                employer_ticker="ASX",
                role_class="director",
            )
        )
    if executive_path is not None and executive_path.exists():
        people_entities.extend(
            _extract_asx_people_entities(
                executive_path.read_text(encoding="utf-8", errors="ignore"),
                employer_name="ASX Limited",
                employer_ticker="ASX",
                role_class="executive_officer",
            )
        )

    derived_files: list[dict[str, object]] = []
    if issuer_entities:
        issuer_path = country_dir / "derived_issuer_entities.json"
        _write_curated_entities(issuer_path, entities=_dedupe_entities(issuer_entities))
        derived_files.append(
            {
                "name": "derived-finance-au-issuers",
                "path": issuer_path.name,
                "record_count": len(_load_curated_entities(issuer_path)),
                "adapter": "asx_listed_companies_csv",
                "warnings": [],
            }
        )
    if ticker_entities:
        ticker_path = country_dir / "derived_ticker_entities.json"
        _write_curated_entities(ticker_path, entities=_dedupe_entities(ticker_entities))
        derived_files.append(
            {
                "name": "derived-finance-au-tickers",
                "path": ticker_path.name,
                "record_count": len(_load_curated_entities(ticker_path)),
                "adapter": "asx_listed_companies_csv",
                "warnings": [],
            }
        )
    if people_entities:
        people_path = country_dir / "derived_people_entities.json"
        _write_curated_entities(people_path, entities=_dedupe_entities(people_entities))
        derived_files.append(
            {
                "name": "derived-finance-au-people",
                "path": people_path.name,
                "record_count": len(_load_curated_entities(people_path)),
                "adapter": "asx_official_people_pages",
                "warnings": [],
            }
        )
    return derived_files


def _derive_germany_deutsche_boerse_entities(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[dict[str, object]]:
    source_paths = {
        str(source.get("name")): country_dir / str(source.get("path"))
        for source in downloaded_sources
    }
    issuer_warnings: list[str] = []
    people_warnings: list[str] = []
    issuer_records: list[dict[str, str | list[str]]] = []
    listed_companies_path = source_paths.get("deutsche-boerse-listed-companies")
    if listed_companies_path is not None and listed_companies_path.exists():
        workbook_path = _resolve_deutsche_boerse_listed_companies_workbook(
            source_path=listed_companies_path,
            source_url=next(
                (
                    str(source.get("source_url"))
                    for source in downloaded_sources
                    if str(source.get("name")) == "deutsche-boerse-listed-companies"
                ),
                _DEUTSCHE_BOERSE_LISTED_COMPANIES_URL,
            ),
            country_dir=country_dir,
            user_agent=user_agent,
        )
        if workbook_path is not None:
            issuer_records.extend(
                _extract_deutsche_boerse_listed_company_records(workbook_path)
            )
    maccess_path = source_paths.get("boerse-muenchen-maccess-listed-companies")
    if maccess_path is not None and maccess_path.exists():
        issuer_records.extend(
            _extract_boerse_muenchen_maccess_issuer_records(
                maccess_path.read_text(encoding="utf-8", errors="ignore")
            )
        )
    duesseldorf_path = source_paths.get("boerse-duesseldorf-primary-market")
    if duesseldorf_path is not None and duesseldorf_path.exists():
        issuer_records.extend(
            _extract_boerse_duesseldorf_primary_market_issuer_records(
                duesseldorf_path.read_text(encoding="utf-8", errors="ignore")
            )
        )
    issuer_records = _merge_germany_issuer_records(issuer_records)
    bafin_company_database_path = source_paths.get("bafin-company-database-export")
    if issuer_records:
        _backfill_germany_tradegate_identifiers(
            issuer_records=issuer_records,
            country_dir=country_dir,
            downloaded_sources=downloaded_sources,
            user_agent=user_agent,
            warnings=issuer_warnings,
        )

    issuer_entities = [
        _build_germany_deutsche_boerse_issuer_entity(record)
        for record in issuer_records
    ]
    ticker_entities = [
        _build_germany_deutsche_boerse_ticker_entity(record)
        for record in issuer_records
        if record["ticker"]
    ]
    issuer_entities_by_isin = {
        str(entity.get("metadata", {}).get("isin", "")): entity for entity in issuer_entities
    }
    ticker_entities_by_ticker = {
        str(entity["canonical_text"]): entity for entity in ticker_entities
    }

    issuer_lookup = _build_germany_issuer_lookup(issuer_records)
    people_entities: list[dict[str, object]] = []
    unternehmensregister_search_dir = (
        country_dir / "unternehmensregister-register-information-search"
    )
    unternehmensregister_detail_dir = country_dir / "unternehmensregister-register-information"
    unternehmensregister_search_dir.mkdir(parents=True, exist_ok=True)
    unternehmensregister_detail_dir.mkdir(parents=True, exist_ok=True)
    for source in downloaded_sources:
        if str(source.get("category")) != "issuer_profile":
            continue
        source_path = country_dir / str(source.get("path"))
        if not source_path.exists():
            continue
        people_entities.extend(
            _extract_german_governance_people_entities(
                source_path.read_text(encoding="utf-8", errors="ignore"),
                issuer_lookup=issuer_lookup,
                source_name=str(source.get("name", "issuer-profile")),
            )
        )
    for record in issuer_records:
        search_url = _build_unternehmensregister_register_information_search_url(
            company_name=str(record["company_name"])
        )
        search_path = unternehmensregister_search_dir / f"{str(record['isin']).casefold()}.html"
        try:
            _download_unternehmensregister_register_information_search_page(
                search_url,
                search_path,
                user_agent=user_agent,
            )
        except Exception as exc:
            warning_text = str(exc)
            if warning_text and warning_text not in people_warnings and len(people_warnings) < 5:
                people_warnings.append(warning_text)
            continue
        search_source_name = (
            f"unternehmensregister-register-information-search-{str(record['isin']).casefold()}"
        )
        downloaded_sources.append(
            {
                "name": search_source_name,
                "source_url": search_url,
                "path": str(search_path.relative_to(country_dir)),
                "category": "filing_system",
                "notes": f"isin={record['isin']}",
            }
        )
        search_html = search_path.read_text(encoding="utf-8", errors="ignore")
        search_result = _extract_unternehmensregister_register_information_search_result(
            search_html,
            company_name=str(record["company_name"]),
            base_url=search_url,
        )
        _merge_unternehmensregister_search_result_metadata(
            issuer_entity=issuer_entities_by_isin.get(str(record["isin"])),
            ticker_entity=ticker_entities_by_ticker.get(str(record["ticker"])),
            search_result=search_result,
        )
        search_page_people = _extract_german_governance_people_entities(
            search_html,
            issuer_lookup=issuer_lookup,
            source_name=search_source_name,
        )
        if search_page_people:
            people_entities.extend(search_page_people)
            continue
        detail_url = (
            str(search_result.get("detail_url", "")).strip()
            if isinstance(search_result, dict)
            else ""
        )
        if not detail_url:
            continue
        detail_path = unternehmensregister_detail_dir / f"{str(record['isin']).casefold()}.html"
        try:
            _download_unternehmensregister_register_information_detail_page(
                detail_url,
                detail_path,
                user_agent=user_agent,
            )
        except Exception as exc:
            warning_text = str(exc)
            if warning_text and warning_text not in people_warnings and len(people_warnings) < 5:
                people_warnings.append(warning_text)
            continue
        detail_source_name = (
            f"unternehmensregister-register-information-{str(record['isin']).casefold()}"
        )
        downloaded_sources.append(
            {
                "name": detail_source_name,
                "source_url": detail_url,
                "path": str(detail_path.relative_to(country_dir)),
                "category": "issuer_profile",
                "notes": f"isin={record['isin']}",
            }
        )
        people_entities.extend(
            _extract_german_governance_people_entities(
                detail_path.read_text(encoding="utf-8", errors="ignore"),
                issuer_lookup=issuer_lookup,
                source_name=detail_source_name,
            )
        )
    company_details_dir = country_dir / "deutsche-boerse-company-details"
    company_details_dir.mkdir(parents=True, exist_ok=True)
    for record in issuer_records:
        company_details_url = _DEUTSCHE_BOERSE_COMPANY_DETAILS_URL_TEMPLATE.format(
            identifier=str(record["isin"])
        )
        company_details_path = company_details_dir / f"{str(record['isin']).casefold()}.html"
        try:
            _download_deutsche_boerse_company_details_page(
                company_details_url,
                company_details_path,
                user_agent=user_agent,
            )
        except RuntimeError as exc:
            warning_text = str(exc)
            if warning_text not in people_warnings and len(people_warnings) < 5:
                people_warnings.append(warning_text)
            if "headless chrome not available" in warning_text.casefold():
                break
            continue
        except Exception:
            continue
        downloaded_sources.append(
            {
                "name": f"deutsche-boerse-company-details-{str(record['isin']).casefold()}",
                "source_url": company_details_url,
                "path": str(company_details_path.relative_to(country_dir)),
                "category": "issuer_profile",
                "notes": f"isin={record['isin']}",
            }
        )
        company_details_html = company_details_path.read_text(
            encoding="utf-8",
            errors="ignore",
        )
        alias_candidates = _extract_deutsche_boerse_company_details_aliases(
            company_details_html,
            company_name=str(record["company_name"]),
        )
        issuer_entity = issuer_entities_by_isin.get(str(record["isin"]))
        if issuer_entity is not None and alias_candidates:
            issuer_entity["aliases"] = _unique_aliases(
                [
                    *[
                        alias
                        for alias in issuer_entity.get("aliases", [])
                        if isinstance(alias, str)
                    ],
                    *alias_candidates,
                ]
            )
        ticker_entity = ticker_entities_by_ticker.get(str(record["ticker"]))
        if ticker_entity is not None and alias_candidates:
            ticker_entity["aliases"] = _unique_aliases(
                [
                    *[
                        alias
                        for alias in ticker_entity.get("aliases", [])
                        if isinstance(alias, str)
                    ],
                    *alias_candidates,
                ]
            )
        people_entities.extend(
            _extract_deutsche_boerse_company_details_people_entities(
                company_details_html,
                issuer_record=record,
            )
        )

    _merge_germany_bafin_institutions(
        bafin_csv_path=bafin_company_database_path,
        issuer_entities=issuer_entities,
        ticker_entities=ticker_entities,
        warnings=issuer_warnings,
    )

    issuer_entities = _dedupe_entities(issuer_entities)
    ticker_entities = _dedupe_entities(ticker_entities)
    people_entities = _dedupe_entities(people_entities)
    wikidata_warnings = _enrich_germany_entities_with_wikidata(
        country_dir=country_dir,
        issuer_entities=issuer_entities,
        ticker_entities=ticker_entities,
        people_entities=people_entities,
        downloaded_sources=downloaded_sources,
        user_agent=user_agent,
    )
    for warning in wikidata_warnings:
        if warning not in issuer_warnings and len(issuer_warnings) < 5:
            issuer_warnings.append(warning)
        if warning not in people_warnings and len(people_warnings) < 5:
            people_warnings.append(warning)

    derived_files: list[dict[str, object]] = []
    if issuer_entities:
        issuer_path = country_dir / "derived_issuer_entities.json"
        _write_curated_entities(issuer_path, entities=issuer_entities)
        derived_files.append(
            {
                "name": "derived-finance-de-issuers",
                "path": issuer_path.name,
                "record_count": len(issuer_entities),
                "adapter": "germany_public_market_and_bafin_sources",
                "warnings": issuer_warnings,
            }
        )
    if ticker_entities:
        ticker_path = country_dir / "derived_ticker_entities.json"
        _write_curated_entities(ticker_path, entities=ticker_entities)
        derived_files.append(
            {
                "name": "derived-finance-de-tickers",
                "path": ticker_path.name,
                "record_count": len(ticker_entities),
                "adapter": "deutsche_boerse_listed_companies_xlsx",
                "warnings": [],
            }
        )
    if people_entities:
        people_path = country_dir / "derived_people_entities.json"
        _write_curated_entities(people_path, entities=people_entities)
        derived_files.append(
            {
                "name": "derived-finance-de-people",
                "path": people_path.name,
                "record_count": len(people_entities),
                "adapter": "official_governance_pages",
                "warnings": people_warnings,
            }
        )
    return derived_files


def _merge_germany_bafin_institutions(
    *,
    bafin_csv_path: Path | None,
    issuer_entities: list[dict[str, object]],
    ticker_entities: list[dict[str, object]],
    warnings: list[str],
) -> None:
    if bafin_csv_path is None or not bafin_csv_path.exists():
        return
    try:
        bafin_records = _extract_bafin_company_database_records(bafin_csv_path)
    except Exception as exc:
        warning_text = f"Failed to parse BaFin company database export: {exc}"
        if warning_text not in warnings and len(warnings) < 5:
            warnings.append(warning_text)
        return
    if not bafin_records:
        return
    issuer_lookup = _build_germany_company_entity_lookup(issuer_entities)
    ticker_lookup = {
        _normalize_whitespace(str(entity.get("canonical_text", ""))).upper(): entity
        for entity in ticker_entities
    }
    for record in bafin_records:
        matched_entity = _find_matching_germany_entity_for_bafin_record(
            record=record,
            issuer_lookup=issuer_lookup,
        )
        if matched_entity is not None:
            _merge_bafin_record_into_germany_entity(
                entity=matched_entity,
                record=record,
            )
            matched_ticker = ticker_lookup.get(
                _normalize_whitespace(str(matched_entity.get("metadata", {}).get("ticker", ""))).upper()
            )
            if matched_ticker is not None:
                _merge_bafin_record_into_germany_entity(
                    entity=matched_ticker,
                    record=record,
                )
            _register_germany_company_entity_aliases(issuer_lookup, matched_entity)
            continue
        institution_entity = _build_germany_bafin_institution_entity(record)
        issuer_entities.append(institution_entity)
        _register_germany_company_entity_aliases(issuer_lookup, institution_entity)


def _extract_bafin_company_database_records(
    csv_path: Path,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    current_record: dict[str, object] | None = None
    with csv_path.open("r", encoding="utf-8-sig", errors="ignore", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = next(reader, None)
        if not header:
            return []
        expected_columns = len(header)
        for row in reader:
            if len(row) < expected_columns:
                row = [*row, *([""] * (expected_columns - len(row)))]
            row_map = {
                header[index]: _normalize_whitespace(row[index])
                for index in range(expected_columns)
            }
            if not any(value for value in row_map.values()):
                continue
            if row_map.get("NAME"):
                if current_record is not None and _is_relevant_bafin_company_database_record(
                    current_record
                ):
                    records.append(current_record)
                raw_name = str(row_map.get("NAME", ""))
                company_name = _normalize_whitespace(
                    _smart_titlecase_company_name(raw_name)
                )
                current_record = {
                    "company_name": company_name or raw_name,
                    "raw_company_name": raw_name,
                    "bak_nr": str(row_map.get("BAK NR", "")),
                    "reg_nr": str(row_map.get("REG NR", "")),
                    "bafin_id": str(row_map.get("BAFIN-ID", "")),
                    "lei": str(row_map.get("LEI", "")),
                    "home_state_authority_id": str(
                        row_map.get(
                            "NATIONALE IDENTIFIKATIONSNUMMER DER BEHÖRDE DES HERKUNFTSMITGLIEDSTAATES",
                            "",
                        )
                    ),
                    "postal_code": str(row_map.get("PLZ", "")),
                    "city": str(row_map.get("ORT", "")),
                    "street": str(row_map.get("STRASSE", "")),
                    "country": str(row_map.get("LAND", "")),
                    "institution_type": str(row_map.get("GATTUNG", "")),
                    "institution_types": _split_bafin_company_database_values(
                        str(row_map.get("GATTUNG", "")),
                        split_commas=True,
                    ),
                    "ombudsman": str(row_map.get("SCHLICHTUNGSSTELLE", "")),
                    "trade_names": _split_bafin_company_database_values(
                        str(row_map.get("HANDELSNAMEN", "")),
                        split_commas=False,
                    ),
                    "germany_branch": str(
                        row_map.get("ZWEIGNIEDERLASSUNG IN DEUTSCHLAND", "")
                    ),
                    "consumer_complaint_contacts": str(
                        row_map.get(
                            "KONTAKTDATEN FÜR VERBRAUCHERBESCHWERDEN",
                            "",
                        )
                    ),
                    "cross_border_credit_service_countries": (
                        _split_bafin_company_database_values(
                            str(
                                row_map.get(
                                    "GRENZÜBERSCHREITENDE ERBRINGUNG VON KREDITDIENSTLEISTUNGEN IN",
                                    "",
                                )
                            ),
                            split_commas=True,
                        )
                    ),
                    "activities": [],
                    "detail_url": "",
                }
                detail_id = _normalize_whitespace(str(current_record["bak_nr"]))
                if detail_id and detail_id != "---":
                    current_record["detail_url"] = (
                        f"{_BAFIN_COMPANY_DATABASE_PORTAL_URL}"
                        f"institutDetails.do?cmd=loadInstitutAction&institutId={quote(detail_id)}"
                    )
            if current_record is None:
                continue
            activity = _extract_bafin_company_database_activity(row_map)
            if activity is not None:
                activities = current_record.get("activities")
                if isinstance(activities, list):
                    activities.append(activity)
    if current_record is not None and _is_relevant_bafin_company_database_record(
        current_record
    ):
        records.append(current_record)
    return records


def _extract_bafin_company_database_activity(
    row_map: dict[str, str],
) -> dict[str, str] | None:
    activity_name = _normalize_whitespace(
        row_map.get("ERLAUBNISSE/ZULASSUNG/TÄTIGKEITEN", "")
    )
    if not activity_name:
        return None
    activity: dict[str, str] = {"activity": activity_name}
    granted_on = _normalize_whitespace(row_map.get("ERTEILUNGSDATUM", ""))
    ended_on = _normalize_whitespace(row_map.get("ENDE AM", ""))
    end_reason = _normalize_whitespace(row_map.get("ENDEGRUND", ""))
    if granted_on:
        activity["granted_on"] = granted_on
    if ended_on and ended_on != "---":
        activity["ended_on"] = ended_on
    if end_reason and end_reason != "---":
        activity["end_reason"] = end_reason
    return activity


def _split_bafin_company_database_values(
    value: str,
    *,
    split_commas: bool,
) -> list[str]:
    normalized = _normalize_whitespace(value)
    if not normalized or normalized == "---":
        return []
    separators = ["\n", "|", ";", "\t"]
    if split_commas:
        separators.append(",")
    pattern = "|".join(re.escape(separator) for separator in separators)
    return _unique_aliases(
        [
            candidate
            for candidate in (
                _normalize_whitespace(part)
                for part in re.split(pattern, normalized)
            )
            if candidate and candidate != "---"
        ]
    )


def _is_relevant_bafin_company_database_record(record: dict[str, object]) -> bool:
    country = _normalize_whitespace(str(record.get("country", ""))).casefold()
    germany_branch = _normalize_whitespace(str(record.get("germany_branch", "")))
    institution_type = _normalize_whitespace(
        str(record.get("institution_type", ""))
    ).casefold()
    return (
        country == "deutschland"
        or bool(germany_branch)
        or "in deutschland" in institution_type
        or "zweigniederlassung" in institution_type
        or "repräsentanz" in institution_type
    )


def _build_germany_company_entity_lookup(
    entities: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    lookup: dict[str, dict[str, object]] = {}
    for entity in entities:
        _register_germany_company_entity_aliases(lookup, entity)
    return lookup


def _register_germany_company_entity_aliases(
    lookup: dict[str, dict[str, object]],
    entity: dict[str, object],
) -> None:
    alias_candidates = [
        str(entity.get("canonical_text", "")),
        *[
            alias
            for alias in entity.get("aliases", [])
            if isinstance(alias, str)
        ],
    ]
    for candidate in alias_candidates:
        for key in (
            _normalize_company_name_for_match(candidate),
            _normalize_company_name_for_match(candidate, drop_suffixes=True),
        ):
            if key and key not in lookup:
                lookup[key] = entity


def _find_matching_germany_entity_for_bafin_record(
    *,
    record: dict[str, object],
    issuer_lookup: dict[str, dict[str, object]],
) -> dict[str, object] | None:
    for candidate in _bafin_company_database_alias_candidates(record):
        for key in (
            _normalize_company_name_for_match(candidate),
            _normalize_company_name_for_match(candidate, drop_suffixes=True),
        ):
            if key and key in issuer_lookup:
                return issuer_lookup[key]
    return None


def _bafin_company_database_alias_candidates(
    record: dict[str, object],
) -> list[str]:
    candidates = [
        _normalize_whitespace(str(record.get("company_name", ""))),
        _normalize_whitespace(str(record.get("raw_company_name", ""))),
        *[
            trade_name
            for trade_name in record.get("trade_names", [])
            if isinstance(trade_name, str)
        ],
    ]
    aliases: list[str] = []
    for candidate in candidates:
        if not candidate:
            continue
        aliases.extend(_build_germany_company_aliases(candidate))
    return _unique_aliases(aliases)


def _merge_bafin_record_into_germany_entity(
    *,
    entity: dict[str, object],
    record: dict[str, object],
) -> None:
    aliases = [
        alias
        for alias in entity.get("aliases", [])
        if isinstance(alias, str)
    ]
    entity["aliases"] = _unique_aliases(
        [*aliases, *_bafin_company_database_alias_candidates(record)]
    )
    metadata = entity.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        entity["metadata"] = metadata
    metadata["regulated_institution"] = True
    if not metadata.get("lei") and _normalize_whitespace(str(record.get("lei", ""))):
        metadata["lei"] = _normalize_whitespace(str(record.get("lei", "")))
    metadata["bafin_categories"] = _unique_aliases(
        [
            *[
                value
                for value in metadata.get("bafin_categories", [])
                if isinstance(value, str)
            ],
            str(record.get("institution_type", "")),
            *[
                value
                for value in record.get("institution_types", [])
                if isinstance(value, str)
            ],
        ]
    )
    metadata["bafin_bak_nrs"] = _unique_aliases(
        [
            *[
                value
                for value in metadata.get("bafin_bak_nrs", [])
                if isinstance(value, str)
            ],
            str(record.get("bak_nr", "")),
        ]
    )
    metadata["bafin_ids"] = _unique_aliases(
        [
            *[
                value
                for value in metadata.get("bafin_ids", [])
                if isinstance(value, str)
            ],
            str(record.get("bafin_id", "")),
        ]
    )
    metadata["bafin_reg_nrs"] = _unique_aliases(
        [
            *[
                value
                for value in metadata.get("bafin_reg_nrs", [])
                if isinstance(value, str)
            ],
            str(record.get("reg_nr", "")),
        ]
    )
    metadata["bafin_detail_urls"] = _unique_aliases(
        [
            *[
                value
                for value in metadata.get("bafin_detail_urls", [])
                if isinstance(value, str)
            ],
            str(record.get("detail_url", "")),
        ]
    )
    metadata["bafin_trade_names"] = _unique_aliases(
        [
            *[
                value
                for value in metadata.get("bafin_trade_names", [])
                if isinstance(value, str)
            ],
            *[
                value
                for value in record.get("trade_names", [])
                if isinstance(value, str)
            ],
        ]
    )
    metadata["bafin_activities"] = _unique_aliases(
        [
            *[
                value
                for value in metadata.get("bafin_activities", [])
                if isinstance(value, str)
            ],
            *[
                str(activity.get("activity", ""))
                for activity in record.get("activities", [])
                if isinstance(activity, dict)
            ],
        ]
    )
    existing_records = [
        item
        for item in metadata.get("bafin_records", [])
        if isinstance(item, dict)
    ]
    record_key = (
        _normalize_whitespace(str(record.get("bak_nr", "")))
        or _normalize_whitespace(str(record.get("bafin_id", "")))
        or _normalize_whitespace(str(record.get("company_name", "")))
    )
    if record_key and not any(
        (
            _normalize_whitespace(str(item.get("bak_nr", "")))
            or _normalize_whitespace(str(item.get("bafin_id", "")))
            or _normalize_whitespace(str(item.get("company_name", "")))
        )
        == record_key
        for item in existing_records
    ):
        existing_records.append(_build_bafin_company_database_record_summary(record))
    metadata["bafin_records"] = existing_records
    metadata["bafin_record_count"] = len(existing_records)


def _build_bafin_company_database_record_summary(
    record: dict[str, object],
) -> dict[str, object]:
    return {
        "company_name": str(record.get("company_name", "")),
        "bak_nr": str(record.get("bak_nr", "")),
        "reg_nr": str(record.get("reg_nr", "")),
        "bafin_id": str(record.get("bafin_id", "")),
        "lei": str(record.get("lei", "")),
        "country": str(record.get("country", "")),
        "postal_code": str(record.get("postal_code", "")),
        "city": str(record.get("city", "")),
        "street": str(record.get("street", "")),
        "institution_type": str(record.get("institution_type", "")),
        "institution_types": [
            value
            for value in record.get("institution_types", [])
            if isinstance(value, str)
        ],
        "ombudsman": str(record.get("ombudsman", "")),
        "trade_names": [
            value
            for value in record.get("trade_names", [])
            if isinstance(value, str)
        ],
        "germany_branch": str(record.get("germany_branch", "")),
        "consumer_complaint_contacts": str(
            record.get("consumer_complaint_contacts", "")
        ),
        "cross_border_credit_service_countries": [
            value
            for value in record.get("cross_border_credit_service_countries", [])
            if isinstance(value, str)
        ],
        "activities": [
            activity
            for activity in record.get("activities", [])
            if isinstance(activity, dict)
        ],
        "detail_url": str(record.get("detail_url", "")),
    }


def _build_germany_bafin_institution_entity(
    record: dict[str, object],
) -> dict[str, object]:
    canonical_text = _normalize_whitespace(str(record.get("company_name", "")))
    identifier = (
        _normalize_whitespace(str(record.get("bak_nr", "")))
        or _normalize_whitespace(str(record.get("bafin_id", "")))
        or _slug(canonical_text)
    )
    metadata = {
        "country_code": "de",
        "category": "regulated_institution",
        "regulated_institution": True,
        "lei": _normalize_whitespace(str(record.get("lei", ""))),
        "institution_type": _normalize_whitespace(
            str(record.get("institution_type", ""))
        ),
        "institution_types": [
            value
            for value in record.get("institution_types", [])
            if isinstance(value, str)
        ],
        "postal_code": _normalize_whitespace(str(record.get("postal_code", ""))),
        "city": _normalize_whitespace(str(record.get("city", ""))),
        "street": _normalize_whitespace(str(record.get("street", ""))),
        "country": _normalize_whitespace(str(record.get("country", ""))),
        "ombudsman": _normalize_whitespace(str(record.get("ombudsman", ""))),
        "detail_url": _normalize_whitespace(str(record.get("detail_url", ""))),
        "bafin_bak_nrs": _unique_aliases([str(record.get("bak_nr", ""))]),
        "bafin_ids": _unique_aliases([str(record.get("bafin_id", ""))]),
        "bafin_reg_nrs": _unique_aliases([str(record.get("reg_nr", ""))]),
        "bafin_categories": _unique_aliases(
            [
                str(record.get("institution_type", "")),
                *[
                    value
                    for value in record.get("institution_types", [])
                    if isinstance(value, str)
                ],
            ]
        ),
        "bafin_detail_urls": _unique_aliases([str(record.get("detail_url", ""))]),
        "bafin_trade_names": [
            value
            for value in record.get("trade_names", [])
            if isinstance(value, str)
        ],
        "bafin_activities": _unique_aliases(
            [
                str(activity.get("activity", ""))
                for activity in record.get("activities", [])
                if isinstance(activity, dict)
            ]
        ),
        "bafin_records": [_build_bafin_company_database_record_summary(record)],
        "bafin_record_count": 1,
    }
    germany_branch = _normalize_whitespace(str(record.get("germany_branch", "")))
    if germany_branch:
        metadata["germany_branch"] = germany_branch
    consumer_contacts = _normalize_whitespace(
        str(record.get("consumer_complaint_contacts", ""))
    )
    if consumer_contacts:
        metadata["consumer_complaint_contacts"] = consumer_contacts
    cross_border_countries = [
        value
        for value in record.get("cross_border_credit_service_countries", [])
        if isinstance(value, str)
    ]
    if cross_border_countries:
        metadata["cross_border_credit_service_countries"] = cross_border_countries
    if not metadata["lei"]:
        metadata.pop("lei")
    return {
        "entity_type": "organization",
        "canonical_text": canonical_text,
        "aliases": _bafin_company_database_alias_candidates(record),
        "entity_id": f"finance-de-institution:bafin:{identifier}",
        "metadata": metadata,
    }


def _derive_united_kingdom_fca_companies_house_entities(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[dict[str, object]]:
    source_paths = {
        str(source.get("name")): country_dir / str(source.get("path"))
        for source in downloaded_sources
    }
    official_list_path = source_paths.get("fca-official-list-live")
    if official_list_path is None or not official_list_path.exists():
        return []

    issuer_records = _extract_uk_official_list_commercial_company_records(official_list_path)
    if not issuer_records:
        return []

    search_dir = country_dir / "companies-house-search"
    officers_dir = country_dir / "companies-house-officers"
    search_dir.mkdir(parents=True, exist_ok=True)
    officers_dir.mkdir(parents=True, exist_ok=True)

    issuer_entities: list[dict[str, object]] = []
    people_entities: list[dict[str, object]] = []
    for issuer_record in issuer_records:
        company_name = str(issuer_record["company_name"])
        raw_company_name = str(issuer_record["raw_company_name"])
        company_number = _find_companies_house_company_number(
            company_name=company_name,
            raw_company_name=raw_company_name,
            search_dir=search_dir,
            user_agent=user_agent,
        )
        issuer_entities.append(
            _build_uk_official_list_company_entity(
                issuer_record=issuer_record,
                company_number=company_number,
            )
        )
        if company_number is None:
            continue
        officers_path = officers_dir / f"{company_number}.html"
        officers_url = _COMPANIES_HOUSE_OFFICERS_URL_TEMPLATE.format(
            company_number=company_number
        )
        try:
            _download_source(officers_url, officers_path, user_agent=user_agent)
        except Exception:
            continue
        people_entities.extend(
            _extract_companies_house_officers_entities(
                officers_path.read_text(encoding="utf-8", errors="ignore"),
                employer_name=company_name,
                employer_company_number=company_number,
            )
        )

    derived_files: list[dict[str, object]] = []
    if issuer_entities:
        issuer_path = country_dir / "derived_issuer_entities.json"
        _write_curated_entities(issuer_path, entities=_dedupe_entities(issuer_entities))
        derived_files.append(
            {
                "name": "derived-finance-uk-issuers",
                "path": issuer_path.name,
                "record_count": len(_load_curated_entities(issuer_path)),
                "adapter": "fca_official_list_live",
            }
        )
    if people_entities:
        people_path = country_dir / "derived_people_entities.json"
        _write_curated_entities(people_path, entities=_dedupe_entities(people_entities))
        derived_files.append(
            {
                "name": "derived-finance-uk-people",
                "path": people_path.name,
                "record_count": len(_load_curated_entities(people_path)),
                "adapter": "companies_house_officers",
            }
        )
    return derived_files


def _derive_united_states_sec_entities(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[dict[str, object]]:
    del user_agent
    source_paths = {
        str(source.get("name")): country_dir / str(source.get("path"))
        for source in downloaded_sources
    }
    companies_path = source_paths.get("sec-company-tickers")
    if companies_path is None or not companies_path.exists():
        return []
    submissions_path = source_paths.get("sec-submissions")
    sec_company_items = _load_sec_company_items(companies_path)
    submission_profiles: dict[str, dict[str, Any]] = {}
    if submissions_path is not None and submissions_path.exists():
        submission_profiles = _load_sec_submission_profiles(
            submissions_path,
            target_source_ids=_collect_sec_source_ids(sec_company_items),
        )
    issuer_records, _ = _build_sec_company_issuers(
        sec_company_items,
        submission_profiles=submission_profiles,
        companyfacts_profiles={},
        symbol_profiles={},
    )
    derived_entities: list[dict[str, object]] = []
    for record in issuer_records:
        canonical_text = _clean_text(record.get("canonical_text"))
        if not canonical_text:
            continue
        metadata = dict(record.get("metadata") or {})
        issuer_source_id = _clean_text(record.get("source_id")) or _clean_text(
            record.get("entity_id")
        )
        if not issuer_source_id:
            continue
        ticker = _clean_text(metadata.get("ticker"))
        aliases = _unique_aliases(
            [
                canonical_text,
                *[
                    alias
                    for alias in record.get("aliases", [])
                    if isinstance(alias, str)
                ],
                *([ticker] if ticker else []),
            ]
        )
        derived_entities.append(
            {
                "entity_type": "organization",
                "canonical_text": canonical_text,
                "aliases": aliases,
                "entity_id": f"finance-us-issuer:{issuer_source_id}",
                "metadata": {
                    "country_code": "us",
                    "category": "issuer",
                    **metadata,
                },
            }
        )
        if ticker:
            derived_entities.append(
                {
                    "entity_type": "ticker",
                    "canonical_text": ticker,
                    "aliases": [ticker],
                    "entity_id": f"finance-us-ticker:{ticker}",
                    "metadata": {
                        "country_code": "us",
                        "category": "ticker",
                        "issuer_name": canonical_text,
                        **metadata,
                    },
                }
            )
    if not derived_entities:
        return []
    output_path = country_dir / "derived_sec_issuer_entities.json"
    _write_curated_entities(output_path, entities=_dedupe_entities(derived_entities))
    return [
        {
            "name": "derived-finance-us-sec-issuers",
            "path": output_path.name,
            "record_count": len(_load_curated_entities(output_path)),
            "adapter": "sec_company_tickers_country_derivation",
        }
    ]


def _extract_asx_listed_company_entities(
    csv_path: Path,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    issuer_entities: list[dict[str, object]] = []
    ticker_entities: list[dict[str, object]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        lines = handle.read().splitlines()
    header_index = next(
        (
            index
            for index, line in enumerate(lines)
            if line.strip().casefold() == "company name,asx code,gics industry group"
        ),
        None,
    )
    if header_index is None:
        return issuer_entities, ticker_entities
    reader = csv.DictReader(lines[header_index:])
    for row in reader:
        company_name = str(row.get("Company name", "")).strip()
        ticker = str(row.get("ASX code", "")).strip().upper()
        industry_group = str(row.get("GICS industry group", "")).strip()
        if not company_name or not ticker:
            continue
        issuer_aliases = [company_name, ticker]
        ascii_alias = _ascii_fold_alias(company_name)
        if ascii_alias and ascii_alias != company_name:
            issuer_aliases.append(ascii_alias)
        issuer_entities.append(
            {
                "entity_type": "organization",
                "canonical_text": company_name,
                "aliases": _unique_aliases(issuer_aliases),
                "entity_id": f"finance-au-issuer:{ticker}",
                "metadata": {
                    "country_code": "au",
                    "category": "issuer",
                    "ticker": ticker,
                    "industry_group": industry_group,
                },
            }
        )
        ticker_aliases = [ticker, company_name]
        if ascii_alias and ascii_alias != company_name:
            ticker_aliases.append(ascii_alias)
        ticker_entities.append(
            {
                "entity_type": "ticker",
                "canonical_text": ticker,
                "aliases": _unique_aliases(ticker_aliases),
                "entity_id": f"finance-au-ticker:{ticker}",
                "metadata": {
                    "country_code": "au",
                    "category": "ticker",
                    "issuer_name": company_name,
                    "industry_group": industry_group,
                },
            }
        )
    return issuer_entities, ticker_entities


def _resolve_deutsche_boerse_listed_companies_workbook(
    *,
    source_path: Path,
    source_url: str,
    country_dir: Path,
    user_agent: str,
) -> Path | None:
    if zipfile.is_zipfile(source_path):
        return source_path
    html_text = source_path.read_text(encoding="utf-8", errors="ignore")
    match = _DEUTSCHE_BOERSE_LISTED_COMPANIES_LINK_PATTERN.search(html_text)
    if match is None:
        return None
    workbook_url = urljoin(source_url, match.group("url"))
    workbook_path = country_dir / "deutsche-boerse-listed-companies.xlsx"
    try:
        _download_source(workbook_url, workbook_path, user_agent=user_agent)
    except Exception:
        return None
    if not zipfile.is_zipfile(workbook_path):
        return None
    return workbook_path


def _extract_deutsche_boerse_listed_company_records(
    workbook_path: Path,
) -> list[dict[str, str | list[str]]]:
    if not zipfile.is_zipfile(workbook_path):
        return []
    records: dict[str, dict[str, str | list[str]]] = {}
    with zipfile.ZipFile(workbook_path) as archive:
        shared_strings = _load_xlsx_shared_strings(archive)
        for sheet_name, worksheet_path in _load_xlsx_sheet_targets(archive):
            if sheet_name.casefold() == "cover":
                continue
            header: list[str] | None = None
            exchange_index: int | None = None
            for row in _iter_xlsx_sheet_rows(
                archive,
                worksheet_path=worksheet_path,
                shared_strings=shared_strings,
            ):
                if header is None:
                    if _is_deutsche_boerse_header_row(row):
                        header = row
                        exchange_index = next(
                            (
                                index
                                for index, value in enumerate(header)
                                if value.casefold() == "instrument exchange"
                            ),
                            None,
                        )
                    continue
                row_map = {
                    header[index].casefold(): row[index].strip()
                    for index in range(min(len(header), len(row)))
                    if header[index].strip()
                }
                if str(row_map.get("country", "")).strip().casefold() != "germany":
                    continue
                raw_company_name = str(row_map.get("company", "")).strip()
                ticker = str(row_map.get("trading symbol", "")).strip().upper()
                isin = str(row_map.get("isin", "")).strip().upper()
                if not raw_company_name or not ticker or not isin:
                    continue
                company_name = _normalize_whitespace(
                    _smart_titlecase_company_name(raw_company_name)
                )
                indices: list[str] = []
                if exchange_index is not None:
                    indices = [
                        value.strip()
                        for value in row[exchange_index + 1 :]
                        if value.strip() and value.strip() != "-"
                    ]
                records.setdefault(
                    isin,
                    {
                        "isin": isin,
                        "ticker": ticker,
                        "company_name": company_name,
                        "listing_segment": sheet_name,
                        "sector": str(row_map.get("sector", "")).strip(),
                        "subsector": str(row_map.get("subsector", "")).strip(),
                        "instrument_exchange": str(
                            row_map.get("instrument exchange", "")
                        ).strip(),
                        "indices": indices,
                    },
                )
    return sorted(
        records.values(),
        key=lambda item: (
            str(item["company_name"]).casefold(),
            str(item["ticker"]).casefold(),
        ),
    )


def _extract_germany_market_table_cells(row_html: str) -> list[str]:
    return [
        match.group("cell")
        for match in _GERMANY_MARKET_TABLE_CELL_PATTERN.finditer(row_html)
    ]


def _merge_germany_issuer_records(
    records: list[dict[str, str | list[str]]],
) -> list[dict[str, str | list[str]]]:
    merged: dict[str, dict[str, str | list[str]]] = {}
    for record in records:
        isin = _normalize_whitespace(str(record.get("isin", ""))).upper()
        if not isin:
            continue
        listing_segment = _normalize_whitespace(str(record.get("listing_segment", "")))
        instrument_exchange = _normalize_whitespace(
            str(record.get("instrument_exchange", ""))
        )
        source_directory = _normalize_whitespace(str(record.get("source_directory", "")))
        indices = [
            _normalize_whitespace(value)
            for value in record.get("indices", [])
            if isinstance(value, str) and _normalize_whitespace(value)
        ]
        current = merged.get(isin)
        if current is None:
            current = {}
            for key, value in record.items():
                if isinstance(value, list):
                    current[key] = [
                        item
                        for item in (_normalize_whitespace(entry) for entry in value if isinstance(entry, str))
                        if item
                    ]
                    continue
                current[key] = _normalize_whitespace(str(value))
            current["isin"] = isin
            current["indices"] = _unique_aliases(indices)
            if listing_segment:
                current["listing_segments"] = [listing_segment]
            if instrument_exchange:
                current["instrument_exchanges"] = [instrument_exchange]
            if source_directory:
                current["source_directories"] = [source_directory]
            merged[isin] = current
            continue

        for key in (
            "ticker",
            "company_name",
            "sector",
            "subsector",
            "wkn",
            "listed_since",
            "issuer_website",
        ):
            incoming_value = _normalize_whitespace(str(record.get(key, "")))
            current_value = _normalize_whitespace(str(current.get(key, "")))
            if incoming_value and not current_value:
                current[key] = incoming_value

        current_indices = [
            value
            for value in current.get("indices", [])
            if isinstance(value, str) and _normalize_whitespace(value)
        ]
        current["indices"] = _unique_aliases([*current_indices, *indices])

        if listing_segment:
            current_segments = [
                value
                for value in current.get("listing_segments", [])
                if isinstance(value, str) and _normalize_whitespace(value)
            ]
            current["listing_segments"] = _unique_aliases(
                [*current_segments, listing_segment]
            )
            if not _normalize_whitespace(str(current.get("listing_segment", ""))):
                current["listing_segment"] = listing_segment
        if instrument_exchange:
            current_exchanges = [
                value
                for value in current.get("instrument_exchanges", [])
                if isinstance(value, str) and _normalize_whitespace(value)
            ]
            current["instrument_exchanges"] = _unique_aliases(
                [*current_exchanges, instrument_exchange]
            )
            if not _normalize_whitespace(str(current.get("instrument_exchange", ""))):
                current["instrument_exchange"] = instrument_exchange
        if source_directory:
            current_directories = [
                value
                for value in current.get("source_directories", [])
                if isinstance(value, str) and _normalize_whitespace(value)
            ]
            current["source_directories"] = _unique_aliases(
                [*current_directories, source_directory]
            )
            if not _normalize_whitespace(str(current.get("source_directory", ""))):
                current["source_directory"] = source_directory

    return sorted(
        merged.values(),
        key=lambda item: (
            str(item.get("company_name", "")).casefold(),
            str(item.get("ticker", "")).casefold(),
            str(item.get("isin", "")).casefold(),
        ),
    )


def _extract_boerse_muenchen_maccess_issuer_records(
    html_text: str,
) -> list[dict[str, str | list[str]]]:
    records: list[dict[str, str | list[str]]] = []
    for match in _GERMAN_GOVERNANCE_TABLE_ROW_PATTERN.finditer(html_text):
        cells = _extract_germany_market_table_cells(match.group("row"))
        if len(cells) < 4:
            continue
        security_match = _GERMANY_MARKET_LINK_ISIN_PATTERN.search(cells[1])
        if security_match is None:
            continue
        company_name = _normalize_whitespace(
            html.unescape(_smart_titlecase_company_name(_strip_html(cells[0])))
        )
        if not company_name:
            continue
        website_match = re.search(r'href="(?P<url>https?://[^"]+)"', cells[0], re.IGNORECASE)
        wkn = _normalize_whitespace(_strip_html(cells[1])).upper()
        sector = _normalize_whitespace(_strip_html(cells[2]))
        listed_since = _normalize_whitespace(_strip_html(cells[3]))
        records.append(
            {
                "isin": _normalize_whitespace(security_match.group("isin")).upper(),
                "ticker": "",
                "company_name": company_name,
                "listing_segment": "m:access",
                "sector": sector,
                "subsector": "",
                "instrument_exchange": "Börse München",
                "indices": [],
                "wkn": wkn,
                "listed_since": listed_since,
                "issuer_website": (
                    _normalize_whitespace(website_match.group("url"))
                    if website_match is not None
                    else ""
                ),
                "source_directory": "boerse-muenchen-maccess-listed-companies",
            }
        )
    return _merge_germany_issuer_records(records)


def _extract_boerse_duesseldorf_primary_market_issuer_records(
    html_text: str,
) -> list[dict[str, str | list[str]]]:
    records: list[dict[str, str | list[str]]] = []
    for match in _GERMAN_GOVERNANCE_TABLE_ROW_PATTERN.finditer(html_text):
        cells = _extract_germany_market_table_cells(match.group("row"))
        if not cells:
            continue
        security_match = _GERMANY_MARKET_LINK_ISIN_PATTERN.search(cells[0])
        if security_match is None:
            continue
        company_html = _GERMANY_MARKET_SUB_INFO_PATTERN.sub(" ", cells[0])
        company_name = _normalize_whitespace(
            html.unescape(_smart_titlecase_company_name(_strip_html(company_html)))
        )
        if not company_name:
            continue
        records.append(
            {
                "isin": _normalize_whitespace(security_match.group("isin")).upper(),
                "ticker": "",
                "company_name": company_name,
                "listing_segment": "Primärmarkt",
                "sector": "",
                "subsector": "",
                "instrument_exchange": "Börse Düsseldorf",
                "indices": [],
                "source_directory": "boerse-duesseldorf-primary-market",
            }
        )
    return _merge_germany_issuer_records(records)


def _extract_tradegate_order_book_identifiers(
    html_text: str,
) -> dict[str, str] | None:
    match = _TRADEGATE_ORDER_BOOK_IDENTIFIER_PATTERN.search(html_text)
    if match is None:
        return None
    identifiers = {
        "wkn": _normalize_whitespace(_strip_html(match.group("wkn"))).upper(),
        "code": _normalize_whitespace(_strip_html(match.group("code"))).upper(),
        "isin": _normalize_whitespace(_strip_html(match.group("isin"))).upper(),
    }
    if not identifiers["isin"]:
        return None
    return identifiers


def _backfill_germany_tradegate_identifiers(
    *,
    issuer_records: list[dict[str, str | list[str]]],
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
    warnings: list[str],
) -> None:
    tradegate_dir = country_dir / "tradegate-order-book"
    tradegate_dir.mkdir(parents=True, exist_ok=True)
    for record in issuer_records:
        isin = _normalize_whitespace(str(record.get("isin", ""))).upper()
        ticker = _normalize_whitespace(str(record.get("ticker", ""))).upper()
        if not isin or ticker:
            continue
        tradegate_url = _TRADEGATE_ORDER_BOOK_URL_TEMPLATE.format(isin=isin)
        tradegate_path = tradegate_dir / f"{isin.casefold()}.html"
        try:
            resolved_url = _download_source(
                tradegate_url,
                tradegate_path,
                user_agent=user_agent,
            )
        except Exception as exc:
            warning_text = str(exc)
            if warning_text and warning_text not in warnings and len(warnings) < 5:
                warnings.append(warning_text)
            continue
        downloaded_sources.append(
            {
                "name": f"tradegate-order-book-{isin.casefold()}",
                "source_url": resolved_url,
                "path": str(tradegate_path.relative_to(country_dir)),
                "category": "issuer_directory",
                "notes": f"isin={isin}",
            }
        )
        identifiers = _extract_tradegate_order_book_identifiers(
            tradegate_path.read_text(encoding="utf-8", errors="ignore")
        )
        if identifiers is None or identifiers["isin"] != isin:
            continue
        code = identifiers.get("code", "")
        wkn = identifiers.get("wkn", "")
        if code:
            record["ticker"] = code
        if wkn and not _normalize_whitespace(str(record.get("wkn", ""))):
            record["wkn"] = wkn
        exchanges = [
            value
            for value in record.get("instrument_exchanges", [])
            if isinstance(value, str) and _normalize_whitespace(value)
        ]
        exchanges = _unique_aliases([*exchanges, "Tradegate Exchange"])
        record["instrument_exchanges"] = exchanges
        if not _normalize_whitespace(str(record.get("instrument_exchange", ""))):
            record["instrument_exchange"] = "Tradegate Exchange"


def _enrich_germany_entities_with_wikidata(
    *,
    country_dir: Path,
    issuer_entities: list[dict[str, object]],
    ticker_entities: list[dict[str, object]],
    people_entities: list[dict[str, object]],
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[str]:
    resolution_payload, resolution_path, warnings = _build_germany_wikidata_resolution_snapshot(
        issuer_entities=issuer_entities,
        people_entities=people_entities,
        country_dir=country_dir,
        user_agent=user_agent,
    )
    if resolution_path is None:
        return warnings
    downloaded_sources.append(
        {
            "name": "wikidata-germany-entity-resolution",
            "source_url": _WIKIDATA_API_URL,
            "path": str(resolution_path.relative_to(country_dir)),
            "category": "knowledge_graph",
            "notes": "Wikidata QID resolution snapshot for Germany issuers and related people.",
        }
    )
    issuer_matches = resolution_payload.get("issuers", {})
    if isinstance(issuer_matches, dict):
        for entity in issuer_entities:
            metadata = entity.get("metadata")
            if not isinstance(metadata, dict):
                continue
            isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
            match = issuer_matches.get(isin)
            if isinstance(match, dict):
                _merge_wikidata_metadata(entity=entity, match=match)
        for entity in ticker_entities:
            metadata = entity.get("metadata")
            if not isinstance(metadata, dict):
                continue
            isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
            match = issuer_matches.get(isin)
            if isinstance(match, dict):
                _merge_wikidata_metadata(entity=entity, match=match)
    people_matches = resolution_payload.get("people", {})
    if isinstance(people_matches, dict):
        for entity in people_entities:
            match = people_matches.get(str(entity.get("entity_id", "")))
            if isinstance(match, dict):
                _merge_wikidata_metadata(entity=entity, match=match)
    return warnings


def _build_germany_wikidata_resolution_snapshot(
    *,
    issuer_entities: list[dict[str, object]],
    people_entities: list[dict[str, object]],
    country_dir: Path,
    user_agent: str,
) -> tuple[dict[str, object], Path | None, list[str]]:
    warnings: list[str] = []
    issuer_matches, issuer_warnings = _resolve_germany_issuer_wikidata_matches(
        issuer_entities=issuer_entities,
        user_agent=user_agent,
    )
    warnings.extend(issuer_warnings)
    people_matches, people_warnings = _resolve_germany_person_wikidata_matches(
        people_entities=people_entities,
        issuer_matches=issuer_matches,
        user_agent=user_agent,
    )
    warnings.extend(people_warnings)
    payload: dict[str, object] = {
        "schema_version": 1,
        "country_code": "de",
        "generated_at": _utc_timestamp(),
        "issuers": issuer_matches,
        "people": people_matches,
    }
    if not issuer_matches and not people_matches:
        return payload, None, warnings
    resolution_path = country_dir / "wikidata-germany-entity-resolution.json"
    resolution_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload, resolution_path, warnings


def _resolve_germany_issuer_wikidata_matches(
    *,
    issuer_entities: list[dict[str, object]],
    user_agent: str,
) -> tuple[dict[str, dict[str, object]], list[str]]:
    search_cache: dict[str, list[dict[str, Any]]] = {}
    target_candidates: dict[str, list[str]] = {}
    warnings: list[str] = []
    for entity in issuer_entities:
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
        if not isin:
            continue
        candidates: list[str] = []
        for query in _build_germany_wikidata_search_queries(entity, entity_type="issuer"):
            if query not in search_cache:
                try:
                    search_cache[query] = _search_wikidata_entities(
                        query,
                        user_agent=user_agent,
                    )
                except Exception as exc:
                    warning_text = f"Wikidata issuer search failed for {query}: {exc}"
                    if warning_text not in warnings and len(warnings) < 5:
                        warnings.append(warning_text)
                    search_cache[query] = []
            candidates.extend(
                str(item.get("id", "")).strip()
                for item in search_cache[query]
                if isinstance(item, dict)
            )
            if candidates:
                break
        if candidates:
            target_candidates[isin] = _unique_aliases(candidates)
    entity_payloads = _fetch_wikidata_entity_payloads(
        entity_ids=[
            candidate_id
            for candidate_ids in target_candidates.values()
            for candidate_id in candidate_ids
        ],
        user_agent=user_agent,
        warnings=warnings,
    )
    matches: dict[str, dict[str, object]] = {}
    for entity in issuer_entities:
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
        candidate_ids = target_candidates.get(isin, [])
        if not candidate_ids:
            continue
        match = _select_best_germany_issuer_wikidata_match(
            entity=entity,
            candidate_ids=candidate_ids,
            entity_payloads=entity_payloads,
        )
        if match is not None:
            matches[isin] = match
    return matches, warnings


def _resolve_germany_person_wikidata_matches(
    *,
    people_entities: list[dict[str, object]],
    issuer_matches: dict[str, dict[str, object]],
    user_agent: str,
) -> tuple[dict[str, dict[str, object]], list[str]]:
    search_cache: dict[str, list[dict[str, Any]]] = {}
    target_candidates: dict[str, list[str]] = {}
    warnings: list[str] = []
    for entity in people_entities:
        entity_id = str(entity.get("entity_id", "")).strip()
        if not entity_id:
            continue
        candidates: list[str] = []
        for query in _build_germany_wikidata_search_queries(entity, entity_type="person"):
            if query not in search_cache:
                try:
                    search_cache[query] = _search_wikidata_entities(
                        query,
                        user_agent=user_agent,
                    )
                except Exception as exc:
                    warning_text = f"Wikidata people search failed for {query}: {exc}"
                    if warning_text not in warnings and len(warnings) < 5:
                        warnings.append(warning_text)
                    search_cache[query] = []
            candidates.extend(
                str(item.get("id", "")).strip()
                for item in search_cache[query]
                if isinstance(item, dict)
            )
            if _has_exact_wikidata_person_search_hit(entity, search_cache[query]):
                break
        if candidates:
            target_candidates[entity_id] = _unique_aliases(candidates)
    entity_payloads = _fetch_wikidata_entity_payloads(
        entity_ids=[
            candidate_id
            for candidate_ids in target_candidates.values()
            for candidate_id in candidate_ids
        ],
        user_agent=user_agent,
        warnings=warnings,
    )
    matches: dict[str, dict[str, object]] = {}
    for entity in people_entities:
        entity_id = str(entity.get("entity_id", "")).strip()
        candidate_ids = target_candidates.get(entity_id, [])
        if not candidate_ids:
            continue
        match = _select_best_germany_person_wikidata_match(
            entity=entity,
            candidate_ids=candidate_ids,
            entity_payloads=entity_payloads,
            issuer_matches=issuer_matches,
        )
        if match is not None:
            matches[entity_id] = match
    return matches, warnings


def _build_germany_wikidata_search_queries(
    entity: dict[str, object],
    *,
    entity_type: str,
) -> list[str]:
    queries: list[str] = []
    canonical_text = _normalize_whitespace(str(entity.get("canonical_text", "")))
    if canonical_text:
        queries.append(canonical_text)
    for alias in entity.get("aliases", []):
        if not isinstance(alias, str):
            continue
        cleaned_alias = _normalize_whitespace(alias)
        if not cleaned_alias or cleaned_alias in queries:
            continue
        if entity_type == "issuer":
            if cleaned_alias.isupper() or len(cleaned_alias) <= 4:
                continue
        queries.append(cleaned_alias)
        if len(queries) >= 4:
            break
    return queries


def _has_exact_wikidata_person_search_hit(
    entity: dict[str, object],
    search_results: list[dict[str, Any]],
) -> bool:
    target_keys = _build_germany_person_match_keys(entity)
    return any(
        _normalize_person_name_for_match(str(item.get("label", ""))) in target_keys
        for item in search_results
        if isinstance(item, dict)
    )


def _select_best_germany_issuer_wikidata_match(
    *,
    entity: dict[str, object],
    candidate_ids: list[str],
    entity_payloads: dict[str, dict[str, Any]],
) -> dict[str, object] | None:
    scored_candidates: list[tuple[int, str]] = []
    for candidate_id in candidate_ids:
        payload = entity_payloads.get(candidate_id)
        if payload is None:
            continue
        score = _score_germany_issuer_wikidata_candidate(
            entity=entity,
            candidate_payload=payload,
        )
        if score > 0:
            scored_candidates.append((score, candidate_id))
    if not scored_candidates:
        return None
    scored_candidates.sort(key=lambda item: (-item[0], item[1]))
    best_score, best_qid = scored_candidates[0]
    second_score = scored_candidates[1][0] if len(scored_candidates) > 1 else 0
    if best_score < 100 and (best_score < 70 or best_score - second_score < 20):
        return None
    return _build_wikidata_resolution_match(
        best_qid,
        entity_payloads.get(best_qid),
        entity_type="organization",
    )


def _select_best_germany_person_wikidata_match(
    *,
    entity: dict[str, object],
    candidate_ids: list[str],
    entity_payloads: dict[str, dict[str, Any]],
    issuer_matches: dict[str, dict[str, object]],
) -> dict[str, object] | None:
    scored_candidates: list[tuple[int, str]] = []
    for candidate_id in candidate_ids:
        payload = entity_payloads.get(candidate_id)
        if payload is None:
            continue
        score = _score_germany_person_wikidata_candidate(
            entity=entity,
            candidate_payload=payload,
            issuer_matches=issuer_matches,
        )
        if score > 0:
            scored_candidates.append((score, candidate_id))
    if not scored_candidates:
        return None
    scored_candidates.sort(key=lambda item: (-item[0], item[1]))
    best_score, best_qid = scored_candidates[0]
    second_score = scored_candidates[1][0] if len(scored_candidates) > 1 else 0
    if best_score < 100:
        return None
    if len(scored_candidates) > 1 and best_score - second_score < 20:
        return None
    return _build_wikidata_resolution_match(
        best_qid,
        entity_payloads.get(best_qid),
        entity_type="person",
    )


def _score_germany_issuer_wikidata_candidate(
    *,
    entity: dict[str, object],
    candidate_payload: dict[str, Any],
) -> int:
    metadata = entity.get("metadata")
    if not isinstance(metadata, dict):
        return 0
    score = 0
    target_full = _normalize_company_name_for_match(str(entity.get("canonical_text", "")))
    target_core = _normalize_company_name_for_match(
        str(entity.get("canonical_text", "")),
        drop_suffixes=True,
    )
    target_keys = {
        key
        for key in {
            target_full,
            target_core,
            *(
                _normalize_company_name_for_match(alias)
                for alias in entity.get("aliases", [])
                if isinstance(alias, str)
            ),
            *(
                _normalize_company_name_for_match(alias, drop_suffixes=True)
                for alias in entity.get("aliases", [])
                if isinstance(alias, str)
            ),
        }
        if key
    }
    candidate_keys = _extract_wikidata_organization_match_keys(candidate_payload)
    if target_full and target_full in candidate_keys:
        score += 60
    elif target_core and target_core in candidate_keys:
        score += 40
    string_values = _extract_wikidata_string_claim_values(candidate_payload)
    isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
    if isin and isin in string_values:
        score += 100
    wkn = _normalize_whitespace(str(metadata.get("wkn", ""))).upper()
    if wkn and wkn in string_values:
        score += 30
    ticker = _normalize_whitespace(str(metadata.get("ticker", ""))).upper()
    if ticker and ticker in string_values:
        score += 15
    candidate_description = _extract_wikidata_description(candidate_payload).casefold()
    if any(keyword in candidate_description for keyword in ("company", "manufacturer", "exchange", "services")):
        score += 5
    return score


def _score_germany_person_wikidata_candidate(
    *,
    entity: dict[str, object],
    candidate_payload: dict[str, Any],
    issuer_matches: dict[str, dict[str, object]],
) -> int:
    metadata = entity.get("metadata")
    if not isinstance(metadata, dict):
        return 0
    score = 0
    target_keys = _build_germany_person_match_keys(entity)
    candidate_keys = _extract_wikidata_person_match_keys(candidate_payload)
    canonical_key = _normalize_person_name_for_match(str(entity.get("canonical_text", "")))
    if canonical_key and canonical_key in candidate_keys:
        score += 100
    elif target_keys.intersection(candidate_keys):
        score += 90
    employer_isin = _normalize_whitespace(str(metadata.get("employer_isin", ""))).upper()
    employer_qid = (
        str(issuer_matches.get(employer_isin, {}).get("qid", "")).strip()
        if employer_isin
        else ""
    )
    if employer_qid:
        entity_claim_ids = _extract_wikidata_entity_claim_ids(candidate_payload)
        if employer_qid in entity_claim_ids:
            score += 40
    candidate_description = _extract_wikidata_description(candidate_payload).casefold()
    if any(
        keyword in candidate_description
        for keyword in ("manager", "executive", "business", "chief", "board", "banker")
    ):
        score += 10
    return score


def _build_germany_person_match_keys(entity: dict[str, object]) -> set[str]:
    keys = {
        _normalize_person_name_for_match(str(entity.get("canonical_text", ""))),
    }
    keys.update(
        _normalize_person_name_for_match(alias)
        for alias in entity.get("aliases", [])
        if isinstance(alias, str)
    )
    return {key for key in keys if key}


def _extract_wikidata_organization_match_keys(
    payload: dict[str, Any],
) -> set[str]:
    texts = _extract_wikidata_entity_text_values(payload)
    keys = {
        _normalize_company_name_for_match(text)
        for text in texts
    }
    keys.update(
        _normalize_company_name_for_match(text, drop_suffixes=True)
        for text in texts
    )
    return {key for key in keys if key}


def _extract_wikidata_person_match_keys(
    payload: dict[str, Any],
) -> set[str]:
    return {
        key
        for key in (
            _normalize_person_name_for_match(text)
            for text in _extract_wikidata_entity_text_values(payload)
        )
        if key
    }


def _extract_wikidata_entity_text_values(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for collection_name in ("labels", "aliases"):
        collection = payload.get(collection_name)
        if not isinstance(collection, dict):
            continue
        for language_value in collection.values():
            if isinstance(language_value, dict):
                text = _normalize_whitespace(str(language_value.get("value", "")))
                if text:
                    values.append(text)
            elif isinstance(language_value, list):
                for item in language_value:
                    if not isinstance(item, dict):
                        continue
                    text = _normalize_whitespace(str(item.get("value", "")))
                    if text:
                        values.append(text)
    return _unique_aliases(values)


def _extract_wikidata_description(payload: dict[str, Any]) -> str:
    descriptions = payload.get("descriptions")
    if not isinstance(descriptions, dict):
        return ""
    for language in ("en", "de"):
        description = descriptions.get(language)
        if isinstance(description, dict):
            text = _normalize_whitespace(str(description.get("value", "")))
            if text:
                return text
    for description in descriptions.values():
        if isinstance(description, dict):
            text = _normalize_whitespace(str(description.get("value", "")))
            if text:
                return text
    return ""


def _extract_wikidata_string_claim_values(payload: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    claims = payload.get("claims")
    if not isinstance(claims, dict):
        return values
    for claim_list in claims.values():
        if not isinstance(claim_list, list):
            continue
        for claim in claim_list:
            if not isinstance(claim, dict):
                continue
            mainsnak = claim.get("mainsnak")
            if not isinstance(mainsnak, dict):
                continue
            datavalue = mainsnak.get("datavalue")
            if not isinstance(datavalue, dict):
                continue
            value = datavalue.get("value")
            if isinstance(value, str):
                normalized = _normalize_whitespace(value).upper()
                if normalized:
                    values.add(normalized)
    return values


def _extract_wikidata_entity_claim_ids(payload: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    claims = payload.get("claims")
    if not isinstance(claims, dict):
        return values
    for claim_list in claims.values():
        if not isinstance(claim_list, list):
            continue
        for claim in claim_list:
            if not isinstance(claim, dict):
                continue
            mainsnak = claim.get("mainsnak")
            if not isinstance(mainsnak, dict):
                continue
            datavalue = mainsnak.get("datavalue")
            if not isinstance(datavalue, dict):
                continue
            value = datavalue.get("value")
            if not isinstance(value, dict):
                continue
            entity_id = str(value.get("id", "")).strip()
            if entity_id:
                values.add(entity_id)
    return values


def _normalize_person_name_for_match(value: str) -> str:
    normalized = _normalize_whitespace(_ascii_fold_alias(value) or value).casefold()
    return " ".join(re.findall(r"[a-z]+", normalized))


def _build_germany_wikidata_entity_aliases(
    payload: dict[str, Any],
    *,
    entity_type: str,
) -> list[str]:
    aliases: list[str] = []
    for text in _extract_wikidata_entity_text_values(payload):
        normalized_text = _normalize_whitespace(text)
        if not normalized_text or normalized_text.isdigit():
            continue
        if entity_type == "person":
            _, person_aliases = _normalize_person_name_aliases(normalized_text)
            aliases.extend(person_aliases)
            continue
        if normalized_text.isupper() and len(normalized_text) <= 5:
            continue
        aliases.extend(_build_germany_company_aliases(normalized_text))
    return _unique_aliases(aliases)


def _build_wikidata_resolution_match(
    qid: str,
    payload: dict[str, Any] | None,
    *,
    entity_type: str,
) -> dict[str, object]:
    label = ""
    aliases: list[str] = []
    if isinstance(payload, dict):
        for language in ("en", "de"):
            labels = payload.get("labels")
            if isinstance(labels, dict):
                language_value = labels.get(language)
                if isinstance(language_value, dict):
                    label = _normalize_whitespace(str(language_value.get("value", "")))
                    if label:
                        break
        if not label:
            texts = _extract_wikidata_entity_text_values(payload)
            label = texts[0] if texts else ""
        aliases = _build_germany_wikidata_entity_aliases(
            payload,
            entity_type=entity_type,
        )
    return {
        "qid": qid,
        "entity_id": f"wikidata:{qid}",
        "url": _WIKIDATA_ENTITY_URL_TEMPLATE.format(qid=qid),
        "label": label,
        "aliases": aliases,
    }


def _merge_wikidata_metadata(
    *,
    entity: dict[str, object],
    match: dict[str, object],
) -> None:
    metadata = entity.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        entity["metadata"] = metadata
    qid = _normalize_whitespace(str(match.get("qid", "")))
    url = _normalize_whitespace(str(match.get("url", "")))
    if qid:
        metadata["wikidata_qid"] = qid
        metadata["wikidata_entity_id"] = f"wikidata:{qid}"
    if url:
        metadata["wikidata_url"] = url
    label = _normalize_whitespace(str(match.get("label", "")))
    if label:
        metadata["wikidata_label"] = label
    wikidata_aliases = [
        alias
        for alias in match.get("aliases", [])
        if isinstance(alias, str) and _normalize_whitespace(alias)
    ]
    if wikidata_aliases:
        existing_aliases = [
            alias
            for alias in entity.get("aliases", [])
            if isinstance(alias, str) and _normalize_whitespace(alias)
        ]
        entity_type = str(entity.get("entity_type", "")).strip().casefold()
        if entity_type in {"organization", "ticker"}:
            entity["aliases"] = _unique_aliases(
                [
                    *existing_aliases,
                    *[
                        alias
                        for wikidata_alias in wikidata_aliases
                        for alias in _build_germany_company_aliases(wikidata_alias)
                    ],
                ]
            )
        else:
            entity["aliases"] = _unique_aliases([*existing_aliases, *wikidata_aliases])


def _search_wikidata_entities(
    query: str,
    *,
    user_agent: str,
) -> list[dict[str, Any]]:
    payload = _fetch_wikidata_api_json(
        params={
            "action": "wbsearchentities",
            "format": "json",
            "language": "en",
            "type": "item",
            "limit": str(_WIKIDATA_SEARCH_LIMIT),
            "search": query,
        },
        user_agent=user_agent,
    )
    search_results = payload.get("search")
    if not isinstance(search_results, list):
        return []
    return [item for item in search_results if isinstance(item, dict)]


def _fetch_wikidata_entity_payloads(
    *,
    entity_ids: list[str],
    user_agent: str,
    warnings: list[str],
) -> dict[str, dict[str, Any]]:
    resolved_ids = _unique_aliases(
        [
            entity_id
            for entity_id in entity_ids
            if entity_id.startswith("Q")
        ]
    )
    payloads: dict[str, dict[str, Any]] = {}
    for index in range(0, len(resolved_ids), _WIKIDATA_GETENTITIES_BATCH_SIZE):
        batch = resolved_ids[index : index + _WIKIDATA_GETENTITIES_BATCH_SIZE]
        try:
            payload = _fetch_wikidata_api_json(
                params={
                    "action": "wbgetentities",
                    "format": "json",
                    "ids": "|".join(batch),
                    "languages": "en|de",
                    "props": "labels|aliases|descriptions|claims",
                },
                user_agent=user_agent,
            )
        except Exception as exc:
            warning_text = f"Wikidata entity batch fetch failed: {exc}"
            if warning_text not in warnings and len(warnings) < 5:
                warnings.append(warning_text)
            continue
        entities = payload.get("entities")
        if not isinstance(entities, dict):
            continue
        for entity_id, entity_payload in entities.items():
            if isinstance(entity_payload, dict):
                payloads[str(entity_id)] = entity_payload
    return payloads


def _fetch_wikidata_api_json(
    *,
    params: dict[str, str],
    user_agent: str,
) -> dict[str, Any]:
    with httpx.Client(
        headers={
            "Accept": "application/json",
            "User-Agent": user_agent,
        },
        follow_redirects=True,
        timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
    ) as client:
        response = client.get(_WIKIDATA_API_URL, params=params)
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Wikidata API returned a non-object payload")
    return payload


def _load_xlsx_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    values: list[str] = []
    for item in root.findall("x:si", namespace):
        values.append("".join(text.text or "" for text in item.iterfind(".//x:t", namespace)))
    return values


def _load_xlsx_sheet_targets(archive: zipfile.ZipFile) -> list[tuple[str, str]]:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    workbook_ns = {
        "x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    rel_ns = {"p": "http://schemas.openxmlformats.org/package/2006/relationships"}
    relationship_targets = {
        relationship.attrib["Id"]: relationship.attrib["Target"]
        for relationship in rels.findall("p:Relationship", rel_ns)
    }
    sheets: list[tuple[str, str]] = []
    for sheet in workbook.findall("x:sheets/x:sheet", workbook_ns):
        relationship_id = sheet.attrib.get(
            "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
        )
        if not relationship_id:
            continue
        target = relationship_targets.get(relationship_id)
        if not target:
            continue
        sheets.append((sheet.attrib.get("name", ""), f"xl/{target}"))
    return sheets


def _iter_xlsx_sheet_rows(
    archive: zipfile.ZipFile,
    *,
    worksheet_path: str,
    shared_strings: list[str],
) -> list[list[str]]:
    namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    worksheet = ET.fromstring(archive.read(worksheet_path))
    rows: list[list[str]] = []
    for row in worksheet.findall("x:sheetData/x:row", namespace):
        values: list[str] = []
        expected_index = 0
        for cell in row.findall("x:c", namespace):
            reference = str(cell.attrib.get("r", ""))
            column_letters = "".join(character for character in reference if character.isalpha())
            column_index = _xlsx_column_index(column_letters) if column_letters else expected_index
            while len(values) < column_index:
                values.append("")
            values.append(_decode_xlsx_cell_value(cell, namespace, shared_strings))
            expected_index = column_index + 1
        rows.append(values)
    return rows


def _decode_xlsx_cell_value(
    cell: ET.Element,
    namespace: dict[str, str],
    shared_strings: list[str],
) -> str:
    cell_type = str(cell.attrib.get("t", ""))
    value_node = cell.find("x:v", namespace)
    if cell_type == "s" and value_node is not None and value_node.text is not None:
        try:
            return shared_strings[int(value_node.text)]
        except (IndexError, ValueError):
            return ""
    if cell_type == "inlineStr":
        inline_node = cell.find("x:is", namespace)
        if inline_node is None:
            return ""
        return "".join(text.text or "" for text in inline_node.iterfind(".//x:t", namespace))
    if value_node is None or value_node.text is None:
        return ""
    return value_node.text


def _xlsx_column_index(column_letters: str) -> int:
    index = 0
    for character in column_letters.upper():
        index = (index * 26) + (ord(character) - ord("A") + 1)
    return max(index - 1, 0)


def _is_deutsche_boerse_header_row(row: list[str]) -> bool:
    normalized = [_normalize_whitespace(value).casefold() for value in row if value.strip()]
    required = {"isin", "trading symbol", "company", "country", "instrument exchange"}
    return required.issubset(set(normalized))


def _build_germany_deutsche_boerse_issuer_entity(
    record: dict[str, str | list[str]],
) -> dict[str, object]:
    company_name = str(record["company_name"])
    ticker = _normalize_whitespace(str(record.get("ticker", ""))).upper()
    isin = str(record["isin"])
    aliases = [*_build_germany_company_aliases(company_name), isin]
    if ticker:
        aliases.append(ticker)
    wkn = _normalize_whitespace(str(record.get("wkn", ""))).upper()
    if wkn:
        aliases.append(wkn)
    metadata: dict[str, object] = {
        "country_code": "de",
        "category": "issuer",
        "isin": isin,
        "listing_segment": str(record["listing_segment"]),
        "sector": str(record["sector"]),
        "subsector": str(record["subsector"]),
        "instrument_exchange": str(record["instrument_exchange"]),
    }
    if ticker:
        metadata["ticker"] = ticker
    if wkn:
        metadata["wkn"] = wkn
    indices = [value for value in record.get("indices", []) if isinstance(value, str)]
    if indices:
        metadata["indices"] = indices
    listing_segments = [
        value for value in record.get("listing_segments", []) if isinstance(value, str)
    ]
    if listing_segments:
        metadata["listing_segments"] = listing_segments
    instrument_exchanges = [
        value
        for value in record.get("instrument_exchanges", [])
        if isinstance(value, str)
    ]
    if instrument_exchanges:
        metadata["instrument_exchanges"] = instrument_exchanges
    issuer_website = _normalize_whitespace(str(record.get("issuer_website", "")))
    if issuer_website:
        metadata["issuer_website"] = issuer_website
    listed_since = _normalize_whitespace(str(record.get("listed_since", "")))
    if listed_since:
        metadata["listed_since"] = listed_since
    source_directories = [
        value
        for value in record.get("source_directories", [])
        if isinstance(value, str)
    ]
    if source_directories:
        metadata["source_directories"] = source_directories
    return {
        "entity_type": "organization",
        "canonical_text": company_name,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-de-issuer:{isin}",
        "metadata": metadata,
    }


def _build_germany_deutsche_boerse_ticker_entity(
    record: dict[str, str | list[str]],
) -> dict[str, object]:
    company_name = str(record["company_name"])
    ticker = _normalize_whitespace(str(record.get("ticker", ""))).upper()
    isin = str(record["isin"])
    aliases = [ticker, *_build_germany_company_aliases(company_name), isin]
    wkn = _normalize_whitespace(str(record.get("wkn", ""))).upper()
    if wkn:
        aliases.append(wkn)
    metadata: dict[str, object] = {
        "country_code": "de",
        "category": "ticker",
        "issuer_name": company_name,
        "isin": isin,
        "listing_segment": str(record["listing_segment"]),
        "sector": str(record["sector"]),
        "subsector": str(record["subsector"]),
        "instrument_exchange": str(record["instrument_exchange"]),
    }
    if wkn:
        metadata["wkn"] = wkn
    indices = [value for value in record.get("indices", []) if isinstance(value, str)]
    if indices:
        metadata["indices"] = indices
    listing_segments = [
        value for value in record.get("listing_segments", []) if isinstance(value, str)
    ]
    if listing_segments:
        metadata["listing_segments"] = listing_segments
    instrument_exchanges = [
        value
        for value in record.get("instrument_exchanges", [])
        if isinstance(value, str)
    ]
    if instrument_exchanges:
        metadata["instrument_exchanges"] = instrument_exchanges
    listed_since = _normalize_whitespace(str(record.get("listed_since", "")))
    if listed_since:
        metadata["listed_since"] = listed_since
    source_directories = [
        value
        for value in record.get("source_directories", [])
        if isinstance(value, str)
    ]
    if source_directories:
        metadata["source_directories"] = source_directories
    return {
        "entity_type": "ticker",
        "canonical_text": ticker,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-de-ticker:{ticker}",
        "metadata": metadata,
    }


def _build_germany_issuer_lookup(
    issuer_records: list[dict[str, str | list[str]]],
) -> dict[str, dict[str, str | list[str]]]:
    lookup: dict[str, dict[str, str | list[str]]] = {}
    for record in issuer_records:
        company_name = str(record["company_name"])
        for alias in _build_germany_company_aliases(company_name):
            for key in {
                _normalize_company_name_for_match(alias),
                _normalize_company_name_for_match(alias, drop_suffixes=True),
            }:
                if key:
                    lookup.setdefault(key, record)
    return lookup


def _download_deutsche_boerse_company_details_page(
    source_url: str | Path,
    destination: Path,
    *,
    user_agent: str,
) -> str:
    normalized_source_url = normalize_source_url(source_url)
    parsed = urlparse(normalized_source_url)
    if (
        parsed.scheme in {"http", "https"}
        and parsed.netloc.casefold() == "live.deutsche-boerse.com"
    ):
        chrome_binary = _find_headless_chrome_binary()
        if chrome_binary is None:
            raise RuntimeError(
                "Headless Chrome not available; skipped live.deutsche-boerse company-details enrichment"
            )
        rendered_html = _render_dynamic_page_with_headless_chrome(
            chrome_binary=chrome_binary,
            source_url=normalized_source_url,
            user_agent=user_agent,
        )
        destination.write_text(rendered_html, encoding="utf-8")
        return normalized_source_url
    return _download_source(normalized_source_url, destination, user_agent=user_agent)


def _build_unternehmensregister_register_information_search_url(
    *,
    company_name: str,
) -> str:
    query_params = {
        "companyName": company_name,
        "companyNameExactMatch": "true",
        "formType": "REGISTER_INFORMATION",
    }
    return (
        f"{_UNTERNEHMENSREGISTER_REGISTER_PORTAL_ADVICE_URL}?"
        f"{urlencode(query_params)}"
    )


def _download_unternehmensregister_register_information_search_page(
    source_url: str | Path,
    destination: Path,
    *,
    user_agent: str,
) -> str:
    return _download_source(source_url, destination, user_agent=user_agent)


def _download_unternehmensregister_register_information_detail_page(
    source_url: str | Path,
    destination: Path,
    *,
    user_agent: str,
) -> str:
    return _download_source(source_url, destination, user_agent=user_agent)


def _extract_unternehmensregister_register_information_search_result(
    search_html: str,
    *,
    company_name: str,
    base_url: str = _UNTERNEHMENSREGISTER_REGISTER_INFORMATION_URL,
) -> dict[str, str] | None:
    target_full = _normalize_company_name_for_match(company_name)
    target_core = _normalize_company_name_for_match(
        company_name,
        drop_suffixes=True,
    )

    def _matches_company_name(value: str) -> bool:
        result_full = _normalize_company_name_for_match(value)
        result_core = _normalize_company_name_for_match(
            value,
            drop_suffixes=True,
        )
        return result_full == target_full or (target_core and result_core == target_core)

    try:
        payload = json.loads(search_html)
    except Exception:
        payload = None
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                item_name = _normalize_whitespace(
                    str(item.get("company_name") or item.get("title") or "")
                )
                if not item_name or not _matches_company_name(item_name):
                    continue
                detail_url = _normalize_whitespace(
                    str(
                        item.get("detail_url")
                        or item.get("href")
                        or item.get("url")
                        or ""
                    )
                )
                return {
                    "company_name": item_name,
                    "detail_url": urljoin(base_url, detail_url) if detail_url else "",
                    "register_court": _normalize_whitespace(
                        str(item.get("register_court") or item.get("court") or "")
                    ),
                    "register_number": _normalize_whitespace(
                        str(item.get("register_number") or item.get("number") or "")
                    ),
                    "register_type": _normalize_whitespace(
                        str(item.get("register_type") or item.get("type") or "")
                    ),
                }
    for row_match in _GERMAN_GOVERNANCE_TABLE_ROW_PATTERN.finditer(search_html):
        row_html = row_match.group("row")
        cells = re.findall(
            r"<t[hd][^>]*>(.*?)</t[hd]>",
            row_html,
            re.IGNORECASE | re.DOTALL,
        )
        if len(cells) < 2:
            continue
        cell_texts = [_normalize_whitespace(_strip_html(cell)) for cell in cells]
        matched_name = next(
            (value for value in cell_texts if value and _matches_company_name(value)),
            None,
        )
        if matched_name is None:
            continue
        href_match = re.search(
            r'href=["\'](?P<href>[^"\']+)["\']',
            row_html,
            re.IGNORECASE,
        )
        register_court = cell_texts[1] if len(cell_texts) > 1 else ""
        register_number = cell_texts[2] if len(cell_texts) > 2 else ""
        register_type = cell_texts[3] if len(cell_texts) > 3 else ""
        if not register_type and register_number:
            register_match = re.match(
                r"(?P<type>[A-Z]{2,4})\s+(?P<number>.+)",
                register_number,
            )
            if register_match is not None:
                register_type = _normalize_whitespace(register_match.group("type"))
                register_number = _normalize_whitespace(register_match.group("number"))
        return {
            "company_name": matched_name,
            "detail_url": (
                urljoin(base_url, href_match.group("href"))
                if href_match is not None
                else ""
            ),
            "register_court": register_court,
            "register_number": register_number,
            "register_type": register_type,
        }
    for anchor_match in re.finditer(
        r'<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<name>.*?)</a>',
        search_html,
        re.IGNORECASE | re.DOTALL,
    ):
        matched_name = _normalize_whitespace(_strip_html(anchor_match.group("name")))
        if matched_name and _matches_company_name(matched_name):
            return {
                "company_name": matched_name,
                "detail_url": urljoin(base_url, anchor_match.group("href")),
                "register_court": "",
                "register_number": "",
                "register_type": "",
            }
    return None


def _merge_unternehmensregister_search_result_metadata(
    *,
    issuer_entity: dict[str, object] | None,
    ticker_entity: dict[str, object] | None,
    search_result: dict[str, str] | None,
) -> None:
    if not search_result:
        return
    register_metadata = {
        key: value
        for key, value in {
            "register_court": _normalize_whitespace(search_result.get("register_court", "")),
            "register_number": _normalize_whitespace(search_result.get("register_number", "")),
            "register_type": _normalize_whitespace(search_result.get("register_type", "")),
        }.items()
        if value
    }
    if not register_metadata:
        return
    for entity in (issuer_entity, ticker_entity):
        if entity is None:
            continue
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        metadata.update(register_metadata)


def _find_headless_chrome_binary() -> str | None:
    for candidate in _DEUTSCHE_BOERSE_HEADLESS_CHROME_CANDIDATES:
        binary = shutil.which(candidate)
        if binary:
            return binary
    return None


def _render_dynamic_page_with_headless_chrome(
    *,
    chrome_binary: str,
    source_url: str,
    user_agent: str,
) -> str:
    command = [
        chrome_binary,
        "--headless=new",
        "--disable-gpu",
        f"--virtual-time-budget={_DEUTSCHE_BOERSE_COMPANY_DETAILS_RENDER_BUDGET_MS}",
        f"--user-agent={user_agent}",
        "--dump-dom",
        source_url,
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=max(
                DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS * 2,
                _DEUTSCHE_BOERSE_COMPANY_DETAILS_RENDER_BUDGET_MS / 1000 + 10,
            ),
        )
    except (OSError, subprocess.SubprocessError, TimeoutError) as exc:
        raise RuntimeError(
            f"Failed to render live.deutsche-boerse company-details page: {exc}"
        ) from exc
    if completed.returncode != 0:
        stderr = _normalize_whitespace(completed.stderr)
        raise RuntimeError(
            "Failed to render live.deutsche-boerse company-details page"
            + (f": {stderr}" if stderr else "")
        )
    rendered_html = completed.stdout.strip()
    if not rendered_html:
        raise RuntimeError(
            "Failed to render live.deutsche-boerse company-details page: empty DOM dump"
        )
    return f"{rendered_html}\n"


def _extract_deutsche_boerse_company_details_people_entities(
    html_text: str,
    *,
    issuer_record: dict[str, str | list[str]],
    source_name: str = "deutsche-boerse-company-details",
) -> list[dict[str, object]]:
    company_name = str(issuer_record["company_name"])
    ticker = str(issuer_record["ticker"])
    isin = str(issuer_record["isin"])
    entities: list[dict[str, object]] = []
    for row_label, row_html in _iter_deutsche_boerse_company_details_rows(html_text):
        role_class = _DEUTSCHE_BOERSE_COMPANY_DETAILS_ROW_ROLE_CLASS.get(
            row_label.casefold()
        )
        if role_class is None:
            continue
        default_role_title = _DEUTSCHE_BOERSE_COMPANY_DETAILS_DEFAULT_ROLE_TITLE.get(
            row_label.casefold(),
            row_label,
        )
        for person_entry in _split_deutsche_boerse_company_details_people(row_html):
            raw_name = person_entry["name"]
            canonical_name, aliases = _normalize_person_name_aliases(raw_name)
            if not canonical_name:
                continue
            role_title = _normalize_whitespace(
                str(person_entry.get("role_title") or default_role_title)
            )
            metadata: dict[str, object] = {
                "country_code": "de",
                "category": role_class,
                "role_title": role_title,
                "employer_name": company_name,
                "employer_ticker": ticker,
                "employer_isin": isin,
                "source_name": source_name,
                "section_name": row_label,
            }
            if person_entry.get("employee_representative"):
                metadata["employee_representative"] = True
            entities.append(
                {
                    "entity_type": "person",
                    "canonical_text": canonical_name,
                    "aliases": _unique_aliases(aliases),
                    "entity_id": (
                        f"finance-de-person:{ticker}:{_slug(canonical_name)}:{_slug(role_class)}"
                    ),
                    "metadata": metadata,
                }
            )
    return entities


def _extract_deutsche_boerse_company_details_aliases(
    html_text: str,
    *,
    company_name: str,
) -> list[str]:
    aliases = _build_germany_company_aliases(company_name)
    heading_alias = _extract_deutsche_boerse_company_details_heading_alias(html_text)
    if heading_alias:
        aliases.extend(_build_germany_company_aliases(heading_alias))
    for former_name in _extract_deutsche_boerse_company_details_former_names(html_text):
        aliases.extend(_build_germany_company_aliases(former_name))
    return _unique_aliases(aliases)


def _extract_deutsche_boerse_company_details_heading_alias(html_text: str) -> str | None:
    match = re.search(
        r'<h1[^>]*class="[^"]*instrument-name[^"]*"[^>]*>(?P<value>.*?)</h1>',
        html_text,
        re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        return None
    heading_text = _normalize_whitespace(_strip_html(match.group("value")))
    former_match = re.search(
        r"\((?:former|formerly)\s+(?P<value>[^)]+)\)",
        heading_text,
        re.IGNORECASE,
    )
    if former_match is None:
        return None
    return _clean_german_governance_employer_candidate(former_match.group("value"))


def _extract_deutsche_boerse_company_details_former_names(
    html_text: str,
) -> list[str]:
    names: list[str] = []
    for row_label, row_html in _iter_deutsche_boerse_company_details_rows(html_text):
        if row_label.casefold() != "further information":
            continue
        row_text = _normalize_whitespace(_strip_html(row_html))
        for match in re.finditer(
            r"company name until\s+\d{2}/\d{2}/\d{4}\s*:\s*(?P<value>[^.;]+)",
            row_text,
            re.IGNORECASE,
        ):
            cleaned = _clean_german_governance_employer_candidate(match.group("value"))
            if cleaned:
                names.append(cleaned)
    return _unique_aliases(names)


def _iter_deutsche_boerse_company_details_rows(
    html_text: str,
) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for match in _GERMAN_GOVERNANCE_TABLE_ROW_PATTERN.finditer(html_text):
        cells = re.findall(
            r"<t[hd][^>]*>(.*?)</t[hd]>",
            match.group("row"),
            re.IGNORECASE | re.DOTALL,
        )
        if len(cells) < 2:
            continue
        row_label = _normalize_whitespace(_strip_html(cells[0]))
        if not row_label:
            continue
        rows.append((row_label, cells[1]))
    return rows


def _split_deutsche_boerse_company_details_people(
    value_html: str,
) -> list[dict[str, object]]:
    text = re.sub(r"<br\s*/?>", "\n", value_html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = "\n".join(
        _normalize_whitespace(line) for line in text.splitlines() if line.strip()
    )
    lines = [
        line
        for line in text.splitlines()
        if line and not line.lstrip().startswith("*")
    ]
    entries: list[dict[str, object]] = []
    for item in _split_on_commas_outside_parentheses(" ".join(lines)):
        candidate = _normalize_whitespace(item)
        if not candidate:
            continue
        employee_representative = candidate.endswith("*")
        if employee_representative:
            candidate = candidate[:-1].rstrip()
        match = re.fullmatch(r"(?P<name>.+?)\s*\((?P<role>.+)\)", candidate)
        role_title = _normalize_whitespace(match.group("role")) if match else None
        raw_name = _normalize_whitespace(match.group("name") if match else candidate)
        if not raw_name:
            continue
        entries.append(
            {
                "name": raw_name,
                "role_title": role_title,
                "employee_representative": employee_representative,
            }
        )
    return entries


def _split_on_commas_outside_parentheses(value: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    for character in value:
        if character == "(":
            depth += 1
        elif character == ")" and depth > 0:
            depth -= 1
        if character == "," and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(character)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _extract_german_governance_people_entities(
    html_text: str,
    *,
    issuer_lookup: dict[str, dict[str, str | list[str]]] | None = None,
    source_name: str = "official-governance-page",
) -> list[dict[str, object]]:
    employer_name = _extract_german_governance_employer_name(html_text)
    employer_record = (
        _lookup_germany_issuer_record(employer_name, issuer_lookup)
        if employer_name and issuer_lookup
        else None
    )
    resolved_employer_name = (
        str(employer_record["company_name"])
        if employer_record is not None
        else employer_name
    )
    employer_ticker = str(employer_record["ticker"]) if employer_record is not None else None
    employer_isin = str(employer_record["isin"]) if employer_record is not None else None
    if not resolved_employer_name:
        return []

    entities: list[dict[str, object]] = []
    for section_title, role_class, section_body in _iter_german_governance_sections(
        html_text
    ):
        for raw_name, raw_role in _extract_german_governance_pairs(section_body):
            canonical_name, aliases = _normalize_person_name_aliases(raw_name)
            if not canonical_name:
                continue
            role_title = _normalize_whitespace(raw_role or section_title) or section_title
            metadata: dict[str, object] = {
                "country_code": "de",
                "category": role_class,
                "role_title": role_title,
                "employer_name": resolved_employer_name,
                "source_name": source_name,
                "section_name": section_title,
            }
            if employer_ticker:
                metadata["employer_ticker"] = employer_ticker
            if employer_isin:
                metadata["employer_isin"] = employer_isin
            entities.append(
                {
                    "entity_type": "person",
                    "canonical_text": canonical_name,
                    "aliases": _unique_aliases(aliases),
                    "entity_id": (
                        f"finance-de-person:{(employer_ticker or employer_isin or _slug(resolved_employer_name))}:"
                        f"{_slug(canonical_name)}:{_slug(role_class)}"
                    ),
                    "metadata": metadata,
                }
            )
    return entities


def _iter_german_governance_sections(
    html_text: str,
) -> list[tuple[str, str, str]]:
    heading_pattern = re.compile(
        r"<h[1-6][^>]*>(?P<heading>.*?)</h[1-6]>",
        re.IGNORECASE | re.DOTALL,
    )
    governance_headings: list[tuple[int, int, str, str]] = []
    for match in heading_pattern.finditer(html_text):
        section_title = _normalize_whitespace(_strip_html(match.group("heading")))
        role_class = _role_class_for_german_governance_heading(section_title)
        if role_class is None:
            continue
        governance_headings.append(
            (match.start(), match.end(), section_title, role_class)
        )
    sections: list[tuple[str, str, str]] = []
    for index, (_, body_start, section_title, role_class) in enumerate(governance_headings):
        body_end = (
            governance_headings[index + 1][0]
            if index + 1 < len(governance_headings)
            else len(html_text)
        )
        sections.append((section_title, role_class, html_text[body_start:body_end]))
    return sections


def _extract_german_governance_employer_name(html_text: str) -> str | None:
    for pattern in (
        r"<title[^>]*>(?P<value>.*?)</title>",
        r"<h1[^>]*>(?P<value>.*?)</h1>",
    ):
        match = re.search(pattern, html_text, re.IGNORECASE | re.DOTALL)
        if match is None:
            continue
        value = _normalize_whitespace(_strip_html(match.group("value")))
        for candidate in [segment.strip() for segment in value.split("|")] + [value]:
            cleaned = _clean_german_governance_employer_candidate(candidate)
            if cleaned:
                return cleaned
    return None


def _clean_german_governance_employer_candidate(value: str) -> str | None:
    candidate = _normalize_whitespace(value)
    if not candidate:
        return None
    cleaned = re.sub(
        (
            r"^(?:register information|register details)\s*(?:of|for|at|der|des|:|-)?\s+"
        ),
        "",
        candidate,
        flags=re.IGNORECASE,
    ).strip()
    if cleaned != candidate:
        return cleaned or None
    cleaned = re.sub(
        (
            r"^(?:supervisory board|board of management|management board|executive board|"
            r"managing directors?|geschäftsführer|"
            r"investor relations|aufsichtsrat|vorstand)\s+(?:of|for|at|der|des)\s+"
        ),
        "",
        candidate,
        flags=re.IGNORECASE,
    ).strip()
    if cleaned != candidate:
        return cleaned or None
    normalized = candidate.casefold()
    if normalized in {"register information", "register details"}:
        return None
    if any(
        heading in normalized
        for headings in _GERMAN_GOVERNANCE_ROLE_HEADINGS.values()
        for heading in headings
    ):
        return None
    return candidate


def _lookup_germany_issuer_record(
    employer_name: str,
    issuer_lookup: dict[str, dict[str, str | list[str]]] | None,
) -> dict[str, str | list[str]] | None:
    if not issuer_lookup:
        return None
    for key in (
        _normalize_company_name_for_match(employer_name),
        _normalize_company_name_for_match(employer_name, drop_suffixes=True),
    ):
        if key and key in issuer_lookup:
            return issuer_lookup[key]
    return None


def _role_class_for_german_governance_heading(section_title: str) -> str | None:
    normalized = section_title.casefold()
    for role_class, headings in _GERMAN_GOVERNANCE_ROLE_HEADINGS.items():
        if any(heading in normalized for heading in headings):
            return role_class
    return None


def _extract_german_governance_pairs(section_body: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def _append_pair(raw_name: str, raw_role: str) -> None:
        name = _normalize_whitespace(raw_name)
        role = _normalize_whitespace(raw_role)
        if not name or not _looks_like_person_name(name):
            return
        pair = (name, role)
        if pair in seen:
            return
        seen.add(pair)
        pairs.append(pair)

    for row_match in _GERMAN_GOVERNANCE_TABLE_ROW_PATTERN.finditer(section_body):
        cells = [
            _normalize_whitespace(_strip_html(cell))
            for cell in re.findall(
                r"<t[hd][^>]*>(.*?)</t[hd]>",
                row_match.group("row"),
                re.IGNORECASE | re.DOTALL,
            )
        ]
        if len(cells) < 2:
            continue
        _append_pair(cells[0], cells[1])
    for card_match in _GERMAN_GOVERNANCE_CARD_PATTERN.finditer(section_body):
        raw_name = _normalize_whitespace(_strip_html(card_match.group("name")))
        raw_role = _normalize_whitespace(_strip_html(card_match.group("role")))
        if not raw_name or not raw_role:
            continue
        _append_pair(raw_name, raw_role)
    for definition_match in re.finditer(
        r"<dt[^>]*>(?P<name>.*?)</dt>\s*<dd[^>]*>(?P<role>.*?)</dd>",
        section_body,
        re.IGNORECASE | re.DOTALL,
    ):
        _append_pair(
            _strip_html(definition_match.group("name")),
            _strip_html(definition_match.group("role")),
        )
    for item_match in re.finditer(
        r"<li[^>]*>(?P<item>.*?)</li>",
        section_body,
        re.IGNORECASE | re.DOTALL,
    ):
        parsed_pair = _split_german_governance_list_item(
            _normalize_whitespace(_strip_html(item_match.group("item")))
        )
        if parsed_pair is None:
            continue
        _append_pair(*parsed_pair)
    if not pairs:
        for person_entry in _split_deutsche_boerse_company_details_people(section_body):
            _append_pair(
                str(person_entry.get("name", "")),
                str(person_entry.get("role_title", "")),
            )
    return pairs


def _split_german_governance_list_item(item_text: str) -> tuple[str, str] | None:
    if not item_text:
        return None
    parentheses_match = re.fullmatch(r"(?P<name>.+?)\s*\((?P<role>.+)\)", item_text)
    if parentheses_match is not None:
        name = _normalize_whitespace(parentheses_match.group("name"))
        role = _normalize_whitespace(parentheses_match.group("role"))
        if name and role and _looks_like_person_name(name):
            return name, role
    for separator in (",", " - ", " – ", ": "):
        if separator not in item_text:
            continue
        name, role = item_text.split(separator, 1)
        name = _normalize_whitespace(name)
        role = _normalize_whitespace(role)
        if name and role and _looks_like_person_name(name):
            return name, role
    return None


def _extract_asx_people_entities(
    html_text: str,
    *,
    employer_name: str,
    employer_ticker: str,
    role_class: str,
) -> list[dict[str, object]]:
    entities: list[dict[str, object]] = []
    for match in _ASX_BIO_PATTERN.finditer(html_text):
        canonical_name = _strip_html(match.group("name"))
        role_title = _normalize_whitespace(_strip_html(match.group("title")))
        if not canonical_name or not role_title:
            continue
        if not _looks_like_person_name(canonical_name):
            continue
        aliases = [canonical_name]
        ascii_alias = _ascii_fold_alias(canonical_name)
        if ascii_alias and ascii_alias != canonical_name:
            aliases.append(ascii_alias)
        entities.append(
            {
                "entity_type": "person",
                "canonical_text": canonical_name,
                "aliases": _unique_aliases(aliases),
                "entity_id": (
                    f"finance-au-person:{employer_ticker}:"
                    f"{_slug(canonical_name)}:{_slug(role_class)}"
                ),
                "metadata": {
                    "country_code": "au",
                    "category": role_class,
                    "role_title": role_title,
                    "employer_name": employer_name,
                    "employer_ticker": employer_ticker,
                },
            }
        )
    return entities


def _extract_uk_official_list_commercial_company_records(
    csv_path: Path,
) -> list[dict[str, str]]:
    records: dict[str, dict[str, str]] = {}
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw_company_name = str(row.get("Company Name", "")).strip()
            if not raw_company_name:
                continue
            country_of_incorporation = str(row.get("Country of Inc.", "")).strip().casefold()
            if country_of_incorporation != "united kingdom":
                continue
            listing_category = str(row.get("Listing Category", "")).strip()
            if listing_category.casefold() not in _UK_OFFICIAL_LIST_COMPANY_CATEGORIES:
                continue
            normalized_key = _normalize_company_name_for_match(raw_company_name)
            if not normalized_key:
                continue
            records.setdefault(
                normalized_key,
                {
                    "raw_company_name": raw_company_name,
                    "company_name": _smart_titlecase_company_name(raw_company_name),
                    "listing_category": listing_category,
                    "market_status": str(row.get("Market Status", "")).strip(),
                    "trading_venue": str(row.get("Trading Venue", "")).strip(),
                    "isin": str(row.get("ISIN(S)", "")).strip().rstrip(","),
                },
            )
    return sorted(records.values(), key=lambda item: item["company_name"].casefold())


def _build_uk_official_list_company_entity(
    *,
    issuer_record: dict[str, str],
    company_number: str | None,
) -> dict[str, object]:
    company_name = str(issuer_record["company_name"])
    raw_company_name = str(issuer_record["raw_company_name"])
    aliases = [company_name]
    if raw_company_name.casefold() != company_name.casefold():
        aliases.append(raw_company_name)
    ascii_alias = _ascii_fold_alias(company_name)
    if ascii_alias and ascii_alias != company_name:
        aliases.append(ascii_alias)
    metadata: dict[str, object] = {
        "country_code": "uk",
        "category": "issuer",
        "listing_category": str(issuer_record["listing_category"]),
        "market_status": str(issuer_record["market_status"]),
        "trading_venue": str(issuer_record["trading_venue"]),
    }
    isin = str(issuer_record.get("isin", "")).strip()
    if isin:
        metadata["isin"] = isin
    if company_number:
        metadata["company_number"] = company_number
    entity_id_suffix = company_number or _slug(company_name)
    return {
        "entity_type": "organization",
        "canonical_text": company_name,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-uk-issuer:{entity_id_suffix}",
        "metadata": metadata,
    }


def _find_companies_house_company_number(
    *,
    company_name: str,
    raw_company_name: str,
    search_dir: Path,
    user_agent: str,
) -> str | None:
    query_text = raw_company_name or company_name
    search_url = _COMPANIES_HOUSE_SEARCH_URL_TEMPLATE.format(query=quote(query_text))
    search_path = search_dir / f"{_slug(company_name)}.html"
    try:
        _download_source(search_url, search_path, user_agent=user_agent)
    except Exception:
        return None
    return _extract_companies_house_search_result_company_number(
        search_path.read_text(encoding="utf-8", errors="ignore"),
        company_name=company_name,
        raw_company_name=raw_company_name,
    )


def _extract_companies_house_search_result_company_number(
    search_html: str,
    *,
    company_name: str,
    raw_company_name: str | None = None,
) -> str | None:
    target_full = _normalize_company_name_for_match(raw_company_name or company_name)
    target_core = _normalize_company_name_for_match(
        raw_company_name or company_name,
        drop_suffixes=True,
    )
    try:
        payload = json.loads(search_html)
    except Exception:
        payload = None
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                result_name = _normalize_whitespace(str(item.get("title", "")))
                result_full = _normalize_company_name_for_match(result_name)
                result_core = _normalize_company_name_for_match(
                    result_name,
                    drop_suffixes=True,
                )
                if result_full == target_full or (target_core and result_core == target_core):
                    company_number = str(item.get("company_number", "")).strip()
                    if company_number:
                        return company_number
    for match in re.finditer(
        r'<li class="type-company">(?P<block>.*?)(?=<li class="type-company">|</ul>)',
        search_html,
        re.IGNORECASE | re.DOTALL,
    ):
        block = match.group("block")
        href_match = re.search(
            r'href="(?P<href>/company/(?P<number>[A-Z0-9]+))"',
            block,
            re.IGNORECASE,
        )
        name_match = re.search(
            r"<a[^>]+href=\"/company/[A-Z0-9]+\"[^>]*>(?P<name>.*?)</a>",
            block,
            re.IGNORECASE | re.DOTALL,
        )
        if href_match is None or name_match is None:
            continue
        result_name = _normalize_whitespace(_strip_html(name_match.group("name")))
        result_full = _normalize_company_name_for_match(result_name)
        result_core = _normalize_company_name_for_match(result_name, drop_suffixes=True)
        if result_full == target_full or (target_core and result_core == target_core):
            return href_match.group("number")
    return None


def _extract_companies_house_officers_entities(
    html_text: str,
    *,
    employer_name: str,
    employer_company_number: str,
) -> list[dict[str, object]]:
    entities: list[dict[str, object]] = []
    for match in re.finditer(
        r'<div class="appointment-(?P<index>\d+)">(?P<block>.*?)(?=<div class="appointment-\d+">|$)',
        html_text,
        re.IGNORECASE | re.DOTALL,
    ):
        block = match.group("block")
        name_match = re.search(
            r'<span id="officer-name-[^"]+">\s*(?:<a[^>]*>)?(?P<name>.*?)(?:</a>)?\s*</span>',
            block,
            re.IGNORECASE | re.DOTALL,
        )
        role_match = re.search(
            r'<dd id="officer-role-[^"]+"[^>]*>(?P<role>.*?)</dd>',
            block,
            re.IGNORECASE | re.DOTALL,
        )
        status_match = re.search(
            r'<span id="officer-status-tag-[^"]+"[^>]*>(?P<status>.*?)</span>',
            block,
            re.IGNORECASE | re.DOTALL,
        )
        if name_match is None or role_match is None:
            continue
        status_text = (
            _normalize_whitespace(_strip_html(status_match.group("status")))
            if status_match
            else ""
        )
        if status_text and status_text.casefold() != "active":
            continue
        raw_name = _normalize_whitespace(_strip_html(name_match.group("name")))
        canonical_name = _normalize_companies_house_officer_name(raw_name)
        if canonical_name is None:
            continue
        role_title = _normalize_whitespace(_strip_html(role_match.group("role")))
        role_class = _role_class_for_companies_house_role(role_title)
        aliases = [canonical_name]
        if raw_name.casefold() != canonical_name.casefold():
            aliases.append(raw_name)
        ascii_alias = _ascii_fold_alias(canonical_name)
        if ascii_alias and ascii_alias != canonical_name:
            aliases.append(ascii_alias)
        entities.append(
            {
                "entity_type": "person",
                "canonical_text": canonical_name,
                "aliases": _unique_aliases(aliases),
                "entity_id": (
                    f"finance-uk-person:{employer_company_number}:"
                    f"{_slug(canonical_name)}:{_slug(role_class)}"
                ),
                "metadata": {
                    "country_code": "uk",
                    "category": role_class,
                    "role_title": role_title,
                    "employer_name": employer_name,
                    "employer_company_number": employer_company_number,
                    "source_name": "companies-house-officers",
                },
            }
        )
    return entities


def _normalize_companies_house_officer_name(raw_name: str) -> str | None:
    candidate = _normalize_whitespace(raw_name)
    if not candidate:
        return None
    lowered_tokens = [
        token.casefold().strip(".")
        for token in re.split(r"[\s,]+", candidate)
        if token.strip(" .")
    ]
    if any(token in _COMPANIES_HOUSE_COMPANY_SUFFIX_TOKENS for token in lowered_tokens):
        return None
    segments = [segment.strip() for segment in candidate.split(",") if segment.strip()]
    if len(segments) >= 2:
        surname = _smart_titlecase_name(segments[0])
        given_name = _smart_titlecase_name(segments[1])
        extras = [
            _smart_titlecase_name(segment)
            for segment in segments[2:]
            if segment.casefold().strip(".") not in _COMPANIES_HOUSE_HONORIFIC_SUFFIXES
        ]
        canonical_name = _normalize_whitespace(
            " ".join([given_name, *extras, surname]).strip()
        )
    else:
        canonical_name = _smart_titlecase_name(candidate)
    if not _looks_like_person_name(canonical_name):
        return None
    return canonical_name


def _role_class_for_companies_house_role(role_title: str) -> str:
    normalized_role = role_title.casefold()
    if "director" in normalized_role:
        return "director"
    if "secretary" in normalized_role:
        return "secretary"
    return "officer"


def _extract_kap_company_directory_records(html_text: str) -> list[dict[str, str]]:
    records: dict[str, dict[str, str]] = {}
    for match in _KAP_COMPANY_LISTING_PATTERN.finditer(html_text):
        permalink = match.group("permalink").strip()
        if permalink in records:
            continue
        records[permalink] = {
            "permalink": permalink,
            "title": _repair_kap_embedded_text(match.group("title").strip()),
            "fund_code": match.group("fund_code").strip(),
        }
    return list(records.values())


def _extract_kap_stock_code(detail_html: str) -> str | None:
    match = _KAP_STOCK_CODE_PATTERN.search(detail_html)
    if match is None:
        return None
    return _repair_kap_embedded_text(match.group("stock_code").strip())


def _extract_kap_people_entities(
    *,
    detail_html: str,
    company_title: str,
    stock_code: str | None,
) -> list[dict[str, object]]:
    entities: list[dict[str, object]] = []
    for item_object in _iter_kap_item_objects(detail_html):
        item_key = str(item_object.get("itemKey", "")).strip()
        item_name = str(item_object.get("itemName", "")).strip()
        values = item_object.get("value")
        if not isinstance(values, list):
            continue
        for value in values:
            if not isinstance(value, dict):
                continue
            raw_name = str(value.get("nameSurname", "")).strip()
            if not raw_name:
                continue
            canonical_name = _smart_titlecase_name(_repair_kap_embedded_text(raw_name))
            aliases = [canonical_name]
            ascii_alias = _ascii_fold_alias(canonical_name)
            if ascii_alias and ascii_alias != canonical_name:
                aliases.append(ascii_alias)
            role_title = _extract_kap_role_title(value, item_name=item_name)
            metadata: dict[str, object] = {
                "country_code": "tr",
                "category": _KAP_ROLE_CLASS_BY_ITEM_KEY.get(item_key, "personnel"),
                "role_title": role_title,
                "employer_name": company_title,
                "section_key": item_key,
                "section_name": item_name,
            }
            if stock_code:
                metadata["employer_ticker"] = stock_code
            entities.append(
                {
                    "entity_type": "person",
                    "canonical_text": canonical_name,
                    "aliases": _unique_aliases(aliases),
                    "entity_id": (
                        "finance-tr-person:"
                        f"{(stock_code or _slug(company_title))}:"
                        f"{_slug(canonical_name)}:{_slug(item_key or item_name)}"
                    ),
                    "metadata": metadata,
                }
            )
    return entities


def _extract_kap_role_title(value: dict[str, object], *, item_name: str) -> str:
    position = value.get("position")
    if isinstance(position, str) and position.strip():
        return _repair_kap_embedded_text(position.strip())
    title = value.get("title")
    if isinstance(title, dict):
        title_text = title.get("text")
        if isinstance(title_text, str) and title_text.strip():
            return _repair_kap_embedded_text(title_text.strip())
    return item_name


def _build_kap_company_entity(
    *,
    company_title: str,
    stock_code: str | None,
) -> dict[str, object]:
    aliases = [company_title]
    ascii_alias = _ascii_fold_alias(company_title)
    if ascii_alias and ascii_alias != company_title:
        aliases.append(ascii_alias)
    if stock_code:
        aliases.append(stock_code)
    metadata: dict[str, object] = {
        "country_code": "tr",
        "category": "issuer",
    }
    if stock_code:
        metadata["ticker"] = stock_code
    entity_id_suffix = stock_code or _slug(company_title)
    return {
        "entity_type": "organization",
        "canonical_text": company_title,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-tr-issuer:{entity_id_suffix}",
        "metadata": metadata,
    }


def _build_kap_ticker_entity(
    *,
    company_title: str,
    stock_code: str,
) -> dict[str, object]:
    aliases = [stock_code]
    ascii_alias = _ascii_fold_alias(company_title)
    if ascii_alias and ascii_alias != company_title:
        aliases.append(ascii_alias)
    aliases.append(company_title)
    return {
        "entity_type": "ticker",
        "canonical_text": stock_code,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-tr-ticker:{stock_code}",
        "metadata": {
            "country_code": "tr",
            "category": "ticker",
            "issuer_name": company_title,
        },
    }


def _iter_kap_item_objects(detail_html: str) -> list[dict[str, object]]:
    objects: list[dict[str, object]] = []
    position = 0
    while True:
        index = detail_html.find(_KAP_ITEM_OBJECT_NEEDLE, position)
        if index == -1:
            break
        start = detail_html.find("{", index)
        if start == -1:
            break
        raw_object = _extract_balanced_brace_segment(detail_html, start)
        if raw_object is None:
            break
        try:
            decoded = _decode_kap_embedded_object(raw_object)
            payload = json.loads(decoded)
        except Exception:
            position = start + 1
            continue
        objects.append(payload)
        position = start + len(raw_object)
    return objects


def _extract_balanced_brace_segment(text: str, start: int) -> str | None:
    level = 0
    for index, char in enumerate(text[start:], start=start):
        if char == "{":
            level += 1
        elif char == "}":
            level -= 1
            if level == 0:
                return text[start : index + 1]
    return None


def _decode_kap_embedded_object(raw_object: str) -> str:
    decoded = codecs.decode(raw_object, "unicode_escape")
    return decoded.encode("latin1").decode("utf-8")


def _repair_kap_embedded_text(value: str) -> str:
    repaired = (
        value.replace("\\/", "/")
        .replace("\\n", "\n")
        .replace("\\t", "\t")
        .strip()
    )
    if "\\" in repaired:
        try:
            repaired = _decode_kap_embedded_object(f'"{repaired}"')[1:-1]
        except Exception:
            repaired = repaired.replace('\\"', '"')
    return repaired.strip()


def _smart_titlecase_name(value: str) -> str:
    if not value:
        return value
    if value != value.upper():
        return value
    lowered = unicodedata.normalize("NFC", value.casefold()).replace("i̇", "i")
    pieces = re.split(r"(\s+)", lowered)
    return "".join(piece.title() if not piece.isspace() else piece for piece in pieces)


_PERSON_HONORIFIC_PREFIXES = {
    "dame",
    "dipling",
    "dipljur",
    "diplkfm",
    "diploec",
    "diplvolksw",
    "doktor",
    "dr",
    "dring",
    "frau",
    "herr",
    "miss",
    "mr",
    "mrs",
    "ms",
    "prof",
    "professor",
    "sir",
}


def _normalize_person_name_aliases(raw_name: str) -> tuple[str | None, list[str]]:
    normalized_raw_name = _normalize_whitespace(raw_name)
    titled_name = _normalize_whitespace(_smart_titlecase_name(normalized_raw_name))
    canonical_name = _strip_person_honorific_prefixes(titled_name) or titled_name
    if not canonical_name or not _looks_like_person_name(canonical_name):
        return None, []
    aliases = [canonical_name]
    for candidate in (titled_name, normalized_raw_name):
        if candidate and candidate.casefold() != canonical_name.casefold():
            aliases.append(candidate)
    for alias in tuple(aliases):
        ascii_alias = _ascii_fold_alias(alias)
        if ascii_alias and ascii_alias.casefold() != alias.casefold():
            aliases.append(ascii_alias)
    return canonical_name, _unique_aliases(aliases)


def _strip_person_honorific_prefixes(value: str) -> str | None:
    parts = [part for part in re.split(r"\s+", value) if part]
    index = 0
    while index < len(parts) - 1:
        normalized = re.sub(r"[^a-z]", "", parts[index].casefold())
        if normalized not in _PERSON_HONORIFIC_PREFIXES:
            break
        index += 1
    if index == 0:
        return None
    stripped = " ".join(parts[index:])
    if not stripped or stripped.casefold() == value.casefold():
        return None
    if not _looks_like_person_name(stripped):
        return None
    return stripped


def _smart_titlecase_company_name(value: str) -> str:
    if not value:
        return value
    if value != value.upper():
        return value
    lowered = unicodedata.normalize("NFC", value.casefold()).replace("i̇", "i")
    pieces = re.split(r"(\s+)", lowered)
    uppercase_tokens = {
        "plc",
        "ltd",
        "llp",
        "lp",
        "sa",
        "ag",
        "nv",
        "se",
        "uk",
        "gmbh",
        "kgaa",
        "kg",
        "na",
        "eo",
        "o.n",
    }
    transformed: list[str] = []
    for piece in pieces:
        if piece.isspace():
            transformed.append(piece)
            continue
        stripped = piece.strip(".,")
        if not stripped:
            transformed.append(piece)
            continue
        if stripped in uppercase_tokens:
            transformed.append(piece.upper())
            continue
        if any(character.isdigit() for character in stripped):
            transformed.append(piece.upper())
            continue
        transformed.append(piece.title())
    return "".join(transformed)


def _build_germany_company_aliases(*values: str) -> list[str]:
    aliases: list[str] = []
    pending = [
        _normalize_whitespace(value)
        for value in values
        if _normalize_whitespace(value)
    ]
    seen: set[str] = set()
    while pending:
        candidate = pending.pop(0)
        key = candidate.casefold()
        if key in seen:
            continue
        seen.add(key)
        aliases.append(candidate)
        variants: list[str] = []
        ascii_alias = _ascii_fold_alias(candidate)
        if ascii_alias and ascii_alias.casefold() != key:
            variants.append(_normalize_whitespace(ascii_alias))
        if "-" in candidate:
            variants.append(_normalize_whitespace(candidate.replace("-", " ")))
        if "." in candidate:
            variants.append(_normalize_whitespace(candidate.replace(".", "")))
        stripped = _strip_germany_company_legal_suffix(candidate)
        if stripped and stripped.casefold() != key:
            variants.append(stripped)
        for variant in variants:
            normalized_variant = _normalize_whitespace(variant)
            if normalized_variant and normalized_variant.casefold() not in seen:
                pending.append(normalized_variant)
    return aliases


def _strip_germany_company_legal_suffix(value: str) -> str | None:
    candidate = _normalize_whitespace(value).rstrip(",")
    previous = ""
    while candidate and candidate.casefold() != previous.casefold():
        previous = candidate
        for pattern in _GERMANY_COMPANY_LEGAL_SUFFIX_PATTERNS:
            updated = _normalize_whitespace(pattern.sub("", candidate)).rstrip(",")
            if updated != candidate:
                candidate = updated
                break
        else:
            break
    return candidate or None


def _normalize_company_name_for_match(
    value: str,
    *,
    drop_suffixes: bool = False,
) -> str:
    normalized = _normalize_whitespace(_ascii_fold_alias(value) or value).casefold()
    normalized = normalized.replace("&", " and ")
    tokens = re.findall(r"[a-z0-9]+", normalized)
    if drop_suffixes:
        tokens = [
            token for token in tokens if token not in _COMPANIES_HOUSE_COMPANY_SUFFIX_TOKENS
        ]
    return " ".join(tokens)


def _ascii_fold_alias(value: str) -> str | None:
    folded = "".join(
        character
        for character in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(character)
    )
    folded = folded.replace("ı", "i").replace("İ", "I")
    return folded if folded != value else None


def _unique_aliases(values: list[str]) -> list[str]:
    seen: set[str] = set()
    aliases: list[str] = []
    for value in values:
        normalized = value.strip()
        if not normalized:
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        aliases.append(normalized)
    return aliases


def _dedupe_entities(entities: list[dict[str, object]]) -> list[dict[str, object]]:
    deduped: dict[str, dict[str, object]] = {}
    for entity in entities:
        entity_id = str(entity["entity_id"])
        current = deduped.get(entity_id)
        if current is None:
            deduped[entity_id] = entity
            continue
        aliases = _unique_aliases(
            [*current.get("aliases", []), *entity.get("aliases", [])]  # type: ignore[list-item]
        )
        current["aliases"] = aliases
    return sorted(
        deduped.values(),
        key=lambda item: (
            str(item["entity_type"]).casefold(),
            str(item["canonical_text"]).casefold(),
            str(item["entity_id"]).casefold(),
        ),
    )


def _slug(value: str) -> str:
    ascii_value = _ascii_fold_alias(value) or value
    return re.sub(r"[^a-z0-9]+", "-", ascii_value.casefold()).strip("-")


def _strip_html(value: str) -> str:
    text = re.sub(r"<br\s*/?>", ", ", value, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\xa0", " ")
    text = html.unescape(text)
    return _normalize_whitespace(text)


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip(" ,")


def _looks_like_person_name(value: str) -> bool:
    parts = [part for part in re.split(r"\s+", value) if part]
    if len(parts) < 2:
        return False
    if any(len(part) == 1 for part in parts):
        return False
    for part in parts:
        normalized = part.replace(".", "").replace("'", "").replace("-", "")
        if not normalized or not normalized[0].isalpha():
            return False
        if not all(character.isalpha() for character in normalized):
            return False
    return True


_COUNTRY_ENTITY_DERIVERS: dict[str, Any] = {
    "au": _derive_australia_asx_entities,
    "de": _derive_germany_deutsche_boerse_entities,
    "tr": _derive_turkiye_kap_entities,
    "uk": _derive_united_kingdom_fca_companies_house_entities,
    "us": _derive_united_states_sec_entities,
}


def _write_country_source_manifest(
    *,
    path: Path,
    generated_at: str,
    user_agent: str,
    profile: dict[str, object],
    downloaded_sources: list[dict[str, object]],
    curated_entities_path: Path,
    people_source_plan_path: Path,
    derived_entities: list[dict[str, object]],
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
                "people_source_plan": {
                    "path": people_source_plan_path.name,
                    "sha256": _sha256_file(people_source_plan_path),
                    "size_bytes": people_source_plan_path.stat().st_size,
                },
                "derived_entities": [
                    {
                        "name": str(entity["name"]),
                        "path": str(entity["path"]),
                        "record_count": int(entity["record_count"]),
                        "adapter": str(entity["adapter"]),
                        "sha256": _sha256_file(path.parent / str(entity["path"])),
                        "size_bytes": (path.parent / str(entity["path"])).stat().st_size,
                    }
                    for entity in derived_entities
                ],
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
