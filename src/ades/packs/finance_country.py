"""Country-scoped finance source fetchers and bundle builders."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
import codecs
import csv
import httpx
import json
from pathlib import Path
import re
import shutil
from typing import Any
import unicodedata
import urllib.request
from urllib.parse import quote, urljoin, urlparse

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
        "automation_status": "planned",
        "scope": "listed-company management board members, supervisory board members, issuer contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh Unternehmensregister issuer filings and Börse Frankfurt/Xetra issuer pages.",
            "Extract people from annual reports, corporate-governance statements, and issuer profile disclosures.",
            "Add BaFin institution and company registers for bank/insurer principals after access review.",
        ],
        "sources": [
            _source("unternehmensregister-filings", "https://www.unternehmensregister.de/ureg/?submitaction=language&language=en", category="filing_system", notes="Primary official filing source for governance and board disclosures."),
            _source("boerse-frankfurt-listed-companies", "https://www.boerse-frankfurt.de/equity", category="issuer_directory", notes="Use issuer pages for listed-company joins and linked reports."),
            _source("bafin-company-database", "https://www.bafin.de/EN/Homepage/homepage_node_en.html", category="institution_register", notes="Use for institution principals outside listed issuers."),
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
    if suffix not in {".html", ".htm", ".xml", ".json", ".csv", ".txt", ".pdf"}:
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


def _smart_titlecase_company_name(value: str) -> str:
    if not value:
        return value
    if value != value.upper():
        return value
    lowered = unicodedata.normalize("NFC", value.casefold()).replace("i̇", "i")
    pieces = re.split(r"(\s+)", lowered)
    uppercase_tokens = {"plc", "ltd", "llp", "lp", "sa", "ag", "nv", "se", "uk"}
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
    return _normalize_whitespace(text)


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip(" ,")


def _looks_like_person_name(value: str) -> bool:
    parts = [part for part in re.split(r"\s+", value) if part]
    if len(parts) < 2:
        return False
    if any(len(part) == 1 for part in parts):
        return False
    return all(re.fullmatch(r"[A-Za-z][A-Za-z.'-]*", part) for part in parts)


_COUNTRY_ENTITY_DERIVERS: dict[str, Any] = {
    "au": _derive_australia_asx_entities,
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
