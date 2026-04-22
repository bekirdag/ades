"""Country-scoped finance source fetchers and bundle builders."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
import codecs
import csv
from hashlib import sha256
import html
import httpx
import json
from pathlib import Path
import re
import shutil
import subprocess
import time
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
    _build_sec_filing_document_url,
    _classify_finance_person_role,
    _dedupe_preserving_order,
    _download_text,
    _format_sec_source_id,
    _looks_like_finance_person_name,
    _normalize_person_display_name,
    _normalize_sec_proxy_document_lines,
    _read_targeted_zip_json_records,
    _slugify,
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
_UNITED_STATES_SEC_INSIDER_FORMS = {"3", "4", "5"}
_UNITED_STATES_SEC_INSIDER_MAX_FILINGS_PER_ISSUER = 3
_UNITED_STATES_SEC_INSIDER_DOWNLOAD_WORKERS = 4
_UNITED_STATES_SEC_REPORTING_PERSON_HEADER_PATTERN = re.compile(
    r"^1\.?\s+Name(?:s)? and Address of Reporting Person",
    re.IGNORECASE,
)
_UNITED_STATES_SEC_RELATIONSHIP_HEADER_PATTERN = re.compile(
    r"^5\.?\s+Relationship of Reporting Person",
    re.IGNORECASE,
)
_UNITED_STATES_SEC_SECTION_BREAK_PATTERN = re.compile(
    r"^(?:Table I|[2-6](?:[A-Za-z])?\.?\s)",
    re.IGNORECASE,
)
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
_JPX_ENGLISH_DISCLOSURE_GATE_URL = (
    "https://www.jpx.co.jp/english/equities/listed-co/disclosure-gate/"
)
_JPX_ENGLISH_DISCLOSURE_AVAILABILITY_URL = (
    f"{_JPX_ENGLISH_DISCLOSURE_GATE_URL}availability/"
)
_JPX_LISTED_COMPANY_SEARCH_URL = "https://www.jpx.co.jp/english/listing/co-search/"
_JPX_LISTED_COMPANY_SEARCH_APP_URL = (
    "https://www2.jpx.co.jp/tseHpFront/JJK020010Action.do?Show=Show"
)
_JPX_LISTED_COMPANY_RESULTS_BASE_URL = "https://www2.jpx.co.jp"
_JPX_CORPORATE_GOVERNANCE_SEARCH_URL = (
    "https://www.jpx.co.jp/english/listing/cg-search/"
)
_JPX_CORPORATE_GOVERNANCE_SEARCH_APP_URL = (
    "https://www2.jpx.co.jp/tseHpFront/CGK020010Action.do?Show=Show"
)
_JPX_CORPORATE_GOVERNANCE_RESULTS_BASE_URL = "https://www2.jpx.co.jp"
_JPX_DISCLOSURE_WORKBOOK_URL_PATTERN = re.compile(
    r'href="(?P<url>[^"]+?\.xlsx)"',
    re.IGNORECASE,
)
_JPX_LISTED_COMPANY_SEARCH_FORM_PATTERN = re.compile(
    r'<form[^>]+name="JJK020010Form"[^>]+action="(?P<action>[^"]+)"',
    re.IGNORECASE,
)
_JPX_LISTED_COMPANY_RESULTS_FORM_PATTERN = re.compile(
    r'<form[^>]+name="JJK020030Form"[^>]+action="(?P<action>[^"]+)"(?P<body>.*?)</form>',
    re.IGNORECASE | re.DOTALL,
)
_JPX_LISTED_COMPANY_RESULT_ROW_PATTERN = re.compile(
    r'<tr height="50">(?P<body>.*?)</tr>',
    re.IGNORECASE | re.DOTALL,
)
_JPX_LISTED_COMPANY_RESULT_INPUT_PATTERN = re.compile(
    r'name="ccJjCrpSelKekkLst_st\[\d+\]\.(?P<field>[^"]+)" value="(?P<value>[^"]*)"',
    re.IGNORECASE,
)
_JPX_LISTED_COMPANY_DETAIL_BUTTON_PATTERN = re.compile(
    r'onclick="gotoBaseJh\(\'(?P<code>[^\']+)\',\s*\'(?P<delisted_flag>[^\']+)\'\);"',
    re.IGNORECASE,
)
_JPX_LISTED_COMPANY_STOCK_PRICE_URL_PATTERN = re.compile(
    r'href="(?P<url>https://quote\.jpx\.co\.jp[^"]+)"[^>]*>\s*Stock prices',
    re.IGNORECASE | re.DOTALL,
)
_JPX_LISTED_COMPANY_TOTAL_COUNT_PATTERN = re.compile(
    r'name="souKnsu" value="(?P<count>\d+)"',
    re.IGNORECASE,
)
_JPX_LISTED_COMPANY_PAGE_SIZE_PATTERN = re.compile(
    r'name="dspGs" value="(?P<size>\d+)"',
    re.IGNORECASE,
)
_JPX_LISTED_COMPANY_HIDDEN_INPUT_PATTERN = re.compile(
    r'<input[^>]+type="hidden"[^>]+name="(?P<name>[^"]+)"[^>]+value="(?P<value>[^"]*)"',
    re.IGNORECASE,
)
_JPX_LISTED_COMPANY_SEARCH_FETCH_MAX_PAGES = 30
_JPX_LISTED_COMPANY_SEARCH_PAGE_SIZE = "200"
_JPX_LISTED_COMPANY_SEARCH_SEGMENT_VALUES = ("011", "012", "013")
_JPX_LISTED_COMPANY_SEARCH_SEGMENT_MAP_OUT = "011>Prime<012>Standard<013>Growth<"
_JPX_LISTED_COMPANY_DETAIL_FORM_MARKER = 'name="JJK020040Form"'
_JPX_LISTED_COMPANY_DETAIL_COMPANY_NAME_PATTERN = re.compile(
    r'<h3 class="fontsizeM">(?P<company_name>.*?)</h3>',
    re.IGNORECASE | re.DOTALL,
)
_JPX_LISTED_COMPANY_DETAIL_TABLE_PATTERN = re.compile(
    r"<table[^>]*>(?P<body>.*?)</table>",
    re.IGNORECASE | re.DOTALL,
)
_JPX_LISTED_COMPANY_DETAIL_ROW_PATTERN = re.compile(
    r"<tr[^>]*>(?P<body>.*?)</tr>",
    re.IGNORECASE | re.DOTALL,
)
_JPX_LISTED_COMPANY_DETAIL_HEADER_CELL_PATTERN = re.compile(
    r"<th[^>]*>(?P<cell>.*?)</th>",
    re.IGNORECASE | re.DOTALL,
)
_JPX_LISTED_COMPANY_DETAIL_DATA_CELL_PATTERN = re.compile(
    r"<td[^>]*>(?P<cell>.*?)</td>",
    re.IGNORECASE | re.DOTALL,
)
_JPX_LISTED_COMPANY_DETAIL_LABEL_TO_KEY = {
    "isin code": "isin_code",
    "trading unit": "trading_unit",
    "date of incorporation": "date_of_incorporation",
    "address of main office": "address_of_main_office",
    "listed exchange": "listed_exchange",
    "investment unit as of the end of last month": "investment_unit_as_of_end_of_last_month",
    "earnings results announcement (scheduled)": "earnings_results_announcement_scheduled",
    "first quarter (scheduled)": "first_quarter_scheduled",
    "second quarter (scheduled)": "second_quarter_scheduled",
    "third quarter (scheduled)": "third_quarter_scheduled",
    "date of general shareholders meeting (scheduled)": "date_of_general_shareholders_meeting_scheduled",
    "title of representative": "representative_title",
    "name of representative": "representative_name",
    "date of listing": "date_of_listing",
    "no. of listed shares": "listed_shares_count",
    "no. of issued shares": "issued_shares_count",
    "registration with j-iriss": "j_iriss_registration_status",
    "loan issue": "loan_issue_status",
    "margin issue": "margin_issue_status",
    "membership of financial accounting standards foundation": "financial_accounting_standards_foundation_membership",
    "notes on going concern assumption": "going_concern_assumption_notes",
    "information on controlling shareholder(s), etc.": "controlling_shareholders_information",
}
_JPX_CORPORATE_GOVERNANCE_SEARCH_FORM_PATTERN = re.compile(
    r'<form[^>]+name="CGK020010Form"[^>]+action="(?P<action>[^"]+)"',
    re.IGNORECASE,
)
_JPX_CORPORATE_GOVERNANCE_RESULTS_FORM_PATTERN = re.compile(
    r'<form[^>]+name="CGK020030Form"[^>]+action="(?P<action>[^"]+)"(?P<body>.*?)</form>',
    re.IGNORECASE | re.DOTALL,
)
_JPX_CORPORATE_GOVERNANCE_RESULT_ROW_PATTERN = re.compile(
    r"<tr>\s*"
    r'.*?codeLink\([\'"](?P<raw_code>[^\'"]+)[\'"]\)'
    r'.*?name="ccEibnCGSelKekkLst_st\[\d+\]\.eqMgrNm" value="(?P<company_name>[^"]+)"'
    r'.*?name="ccEibnCGSelKekkLst_st\[\d+\]\.jhUpdDay" value="(?P<report_date>[^"]+)"'
    r'.*?<a href="(?P<report_url>/disc/[^"]+?\.pdf)"[^>]*>.*?<span[^>]*>(?P<heading>.*?)</span>',
    re.IGNORECASE | re.DOTALL,
)
_JPX_CORPORATE_GOVERNANCE_TOTAL_COUNT_PATTERN = re.compile(
    r'name="souKnsu" value="(?P<count>\d+)"',
    re.IGNORECASE,
)
_JPX_CORPORATE_GOVERNANCE_PAGE_SIZE_PATTERN = re.compile(
    r'name="dspGs" value="(?P<size>\d+)"',
    re.IGNORECASE,
)
_JPX_CORPORATE_GOVERNANCE_HIDDEN_INPUT_PATTERN = re.compile(
    r'<input[^>]+type="hidden"[^>]+name="(?P<name>[^"]+)"[^>]+value="(?P<value>[^"]*)"',
    re.IGNORECASE,
)
_JPX_CORPORATE_GOVERNANCE_FETCH_MAX_PAGES = 30
_JPX_CORPORATE_GOVERNANCE_TOP_LINE_LIMIT = 80
_JPX_CORPORATE_GOVERNANCE_CONTACT_PATTERN = re.compile(
    r"Contact:\s*(?P<name>[^,\n]+),\s*(?P<details>.*?)(?:Phone:|Securities Code:|https?://|$)",
    re.IGNORECASE | re.DOTALL,
)
_AMF_LISTED_COMPANIES_ISSUERS_URL = (
    "https://www.amf-france.org/en/professionals/listed-companies-issuers/my-listed-companies-issuers-space"
)
_EURONEXT_PARIS_REGULATED_LIST_URL = (
    "https://live.euronext.com/en/markets/paris/equities/euronext/list"
)
_EURONEXT_PARIS_GROWTH_LIST_URL = (
    "https://live.euronext.com/en/markets/paris/equities/growth/list"
)
_EURONEXT_PARIS_ACCESS_LIST_URL = (
    "https://live.euronext.com/en/markets/paris/equities/access/list"
)
_EURONEXT_PARIS_COMPANY_NEWS_URL = (
    "https://live.euronext.com/en/markets/paris/company-news"
)
_EURONEXT_PARIS_COMPANY_NEWS_ARCHIVE_URL = (
    "https://live.euronext.com/en/markets/paris/company-news-archive"
)
_FRANCE_COMPANY_PRESS_RELEASE_MODAL_URL_TEMPLATE = (
    "https://live.euronext.com/ajax/node/company-press-release/{node_nid}"
)
_EURONEXT_DIRECTORY_RENDER_BUDGET_MS = 20_000
_FRANCE_COMPANY_NEWS_FETCH_MAX_PAGES = 250
_FRANCE_MAX_GOVERNANCE_URLS_PER_ISSUER = 4
_EURONEXT_PARIS_RENDERED_DIRECTORY_URLS = frozenset(
    {
        _EURONEXT_PARIS_REGULATED_LIST_URL,
        _EURONEXT_PARIS_GROWTH_LIST_URL,
        _EURONEXT_PARIS_ACCESS_LIST_URL,
    }
)
_FRANCE_EURONEXT_SOURCE_SEGMENTS = {
    "euronext-paris-regulated": "Euronext Paris",
    "euronext-paris-growth": "Euronext Growth Paris",
    "euronext-paris-access": "Euronext Access Paris",
}
_FRANCE_COMPANY_NEWS_TITLE_SCORES = (
    ("document d'enregistrement universel", 100),
    ("document denregistrement universel", 100),
    ("universal registration document", 100),
    ("annual financial report", 95),
    ("rapport financier annuel", 95),
    ("annual report", 90),
    ("gouvernement d'entreprise", 90),
    ("gouvernement dentreprise", 90),
    ("corporate governance", 90),
    ("résultat annuel", 80),
    ("resultat annuel", 80),
    ("résultats annuels", 80),
    ("resultats annuels", 80),
    ("annual results", 80),
    ("nomination", 75),
    ("appointment", 75),
    ("conseil d'administration", 75),
    ("conseil dadministration", 75),
    ("conseil de surveillance", 75),
    ("directoire", 75),
)
_FRANCE_GOVERNANCE_URL_KEYWORDS = (
    "gouvernance",
    "governance",
    "gouvernement-dentreprise",
    "gouvernement-d-entreprise",
    "corporate-governance",
    "conseil-administration",
    "conseil-d-administration",
    "conseil-de-surveillance",
    "directoire",
    "management",
    "leadership",
    "dirigeants",
    "investor-relations",
    "relations-investisseurs",
)
_FRANCE_GOVERNANCE_LINK_TEXT_KEYWORDS = (
    "gouvernance",
    "governance",
    "conseil",
    "directoire",
    "management",
    "dirigeants",
)
_FRANCE_FILING_URL_KEYWORDS = (
    "document-denregistrement-universel",
    "document-d-enregistrement-universel",
    "document-enregistrement-universel",
    "universal-registration-document",
    "annual-financial-report",
    "annual-report",
    "rapport-financier-annuel",
    "rapport-annuel",
    "corporate-governance",
    "gouvernance",
    "gouvernement-dentreprise",
    "gouvernement-d-entreprise",
    "info-financiere",
)
_FRANCE_FILING_LINK_TEXT_KEYWORDS = (
    "document d'enregistrement universel",
    "document denregistrement universel",
    "universal registration document",
    "annual financial report",
    "rapport financier annuel",
    "annual report",
    "rapport annuel",
    "corporate governance",
    "gouvernance",
    "gouvernement d'entreprise",
    "gouvernement dentreprise",
)
_FRANCE_MAX_FILING_URLS_PER_ISSUER = 4
_FRANCE_BLOCKED_BINARY_SUFFIXES = {
    ".pdf",
    ".xls",
    ".xlsx",
    ".doc",
    ".docx",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
}
_BYMA_DATA_API_BASE_URL = "https://open.bymadata.com.ar/vanoms-be-core/rest/api"
_BYMA_LISTED_COMPANIES_URL = (
    "https://www.byma.com.ar/financiarse/para-emisoras/empresas-listadas"
)
_CNV_EMITTER_REGIMES_URL = (
    "https://www.cnv.gov.ar/SitioWeb/Reportes/ListadoSociedadesEmisorasPorRegimen"
)
_BYMA_DATA_ALL_INSTRUMENT_SELECT_URL = (
    f"{_BYMA_DATA_API_BASE_URL}/bymadata/free/instrumentproduct/instrument/all-instrument-select"
)
_BYMA_DATA_SPECIES_GENERAL_URL = (
    f"{_BYMA_DATA_API_BASE_URL}/bymadata/free/bnown/fichatecnica/especies/general"
)
_BYMA_DATA_SOCIETY_GENERAL_URL = (
    f"{_BYMA_DATA_API_BASE_URL}/bymadata/free/bnown/fichatecnica/sociedades/general"
)
_BYMA_DATA_SOCIETY_ADMINISTRATION_URL = (
    f"{_BYMA_DATA_API_BASE_URL}/bymadata/free/bnown/fichatecnica/sociedades/administracion"
)
_BYMA_DATA_SOCIETY_RESPONSIBLES_URL = (
    f"{_BYMA_DATA_API_BASE_URL}/bymadata/free/bnown/fichatecnica/sociedades/responsables"
)
_RELAXED_TLS_SOURCE_HOSTS = {
    "open.bymadata.com.ar",
}
_BCRA_FINANCIAL_INSTITUTIONS_PUBLICATION_URL = (
    "https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/"
)
_BCRA_FINANCIAL_INSTITUTIONS_ARCHIVE_URL_PATTERN = re.compile(
    r'(?P<url>(?:https?://|file://|/)[^"\'\s>]*?(?P<period>\d{6})d\.7z)',
    re.IGNORECASE,
)
_BCRA_FINANCIAL_INSTITUTIONS_PERIOD_PAGE_URL_PATTERN = re.compile(
    r'<option[^>]+value="(?P<url>[^"]*?\?fecha=(?P<period>\d{6})[^"]*)"',
    re.IGNORECASE,
)
_BCRA_FINANCIAL_INSTITUTIONS_ARCHIVE_MEMBERS = (
    "Entfin/Tec_Cont/entidad/COMPLETO.TXT",
    "Entfin/Direct/COMPLETO.TXT",
    "Entfin/Gerent/COMPLETO.TXT",
    "Entfin/Respclie/completo.txt",
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
_BOERSE_STUTTGART_URL = "https://www.boerse-stuttgart.de/"
_BOERSE_BERLIN_URL = "https://www.boerse-berlin.com/"
_BOERSE_HAMBURG_URL = "https://www.boerse-hamburg.de/"
_BOERSE_HANNOVER_URL = "https://www.boerse-hannover.de/"
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
_WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
_WIKIDATA_ENTITY_URL_TEMPLATE = "https://www.wikidata.org/wiki/{qid}"
_WIKIDATA_SEARCH_LIMIT = 5
_WIKIDATA_GETENTITIES_BATCH_SIZE = 50
_WIKIDATA_STRING_PROPERTY_BATCH_SIZE = 200
_WIKIDATA_CIK_PROPERTY_ID = "P5531"
_WIKIDATA_ISIN_PROPERTY_ID = "P946"
_WIKIDATA_LEI_PROPERTY_ID = "P1278"
_WIKIDATA_LONDON_STOCK_EXCHANGE_COMPANY_ID_PROPERTY_ID = "P8676"
_WIKIDATA_INSTANCE_OF_PROPERTY_ID = "P31"
_WIKIDATA_HUMAN_QID = "Q5"
_WIKIDATA_API_MAX_ATTEMPTS = 4
_WIKIDATA_API_RETRY_BASE_SECONDS = 1.0
_WIKIDATA_SPARQL_MAX_ATTEMPTS = 4
_WIKIDATA_SPARQL_RETRY_BASE_SECONDS = 2.0
_GERMANY_WIKIDATA_PUBLIC_PEOPLE_PROPERTIES = (
    ("P169", "executive_officer", "Chief Executive Officer"),
    ("P488", "director", "Chairperson"),
    ("P3320", "director", "Board Member"),
    ("P127", "shareholder", "Owner"),
    ("P112", "founder", "Founder"),
)
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
_EURONEXT_DIRECTORY_ROW_PATTERN = re.compile(
    r"<tr[^>]*>(?P<row>.*?)</tr>",
    re.IGNORECASE | re.DOTALL,
)
_EURONEXT_DIRECTORY_LINK_PATTERN = re.compile(
    r'<a[^>]+href=["\'](?P<path>/en/product/equities/(?P<isin>[A-Z0-9]{12})-(?P<market_code>[A-Z0-9]+))["\'][^>]*>(?P<company_name>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
_EURONEXT_DIRECTORY_SYMBOL_PATTERN = re.compile(
    r'<td[^>]*class="[^"]*stocks-symbol[^"]*"[^>]*>(?P<value>.*?)</td>',
    re.IGNORECASE | re.DOTALL,
)
_EURONEXT_DIRECTORY_MARKET_PATTERN = re.compile(
    r'<td[^>]*class="[^"]*stocks-market[^"]*"[^>]*>(?P<value>.*?)</td>',
    re.IGNORECASE | re.DOTALL,
)
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
_FRANCE_COMPANY_NEWS_TABLE_ROW_PATTERN = re.compile(
    r"<tr[^>]*>(?P<row>.*?)</tr>",
    re.IGNORECASE | re.DOTALL,
)
_FRANCE_GOVERNANCE_CARD_PATTERN = re.compile(
    r"<p[^>]*class=\"[^\"]*uppercase[^\"]*\"[^>]*>(?P<name>.*?)</p>\s*"
    r"<p[^>]*>(?P<role>.*?)</p>",
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
_FRANCE_GOVERNANCE_ROLE_HEADINGS = {
    "director": (
        "conseil d'administration",
        "conseil dadministration",
        "board of directors",
        "conseil de surveillance",
        "supervisory board",
        "administrateurs",
    ),
    "executive_officer": (
        "directoire",
        "comité exécutif",
        "comite executif",
        "executive committee",
        "management committee",
        "direction générale",
        "direction generale",
        "management team",
        "leadership team",
        "dirigeants",
        "executive board",
    ),
    "investor_relations": (
        "relations investisseurs",
        "investor relations",
        "communication financière",
        "communication financiere",
    ),
    "press_contact": (
        "relations presse",
        "media relations",
        "press contact",
        "communication",
    ),
}
_FCA_OFFICIAL_LIST_LIVE_URL = "https://marketsecurities.fca.org.uk/downloadOfficiallist/live"
_UK_LSE_AIM_ALL_SHARE_CONSTITUENTS_URL = (
    "https://www.londonstockexchange.com/indices/ftse-aim-all-share/constituents/table"
)
_COMPANIES_HOUSE_SEARCH_URL_TEMPLATE = (
    "https://find-and-update.company-information.service.gov.uk/search/companies?q={query}"
)
_COMPANIES_HOUSE_OFFICERS_URL_TEMPLATE = (
    "https://find-and-update.company-information.service.gov.uk/company/{company_number}/officers"
)
_COMPANIES_HOUSE_PSC_URL_TEMPLATE = (
    "https://find-and-update.company-information.service.gov.uk/company/{company_number}/persons-with-significant-control"
)
_UK_OFFICIAL_LIST_COMPANY_CATEGORIES = frozenset({"equity shares (commercial companies)"})
_UK_LSE_AIM_RENDER_BUDGET_MS = 20_000
_UK_LSE_RENDERED_DIRECTORY_URLS = frozenset({_UK_LSE_AIM_ALL_SHARE_CONSTITUENTS_URL})
_HEADLESS_CHROME_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0 Safari/537.36"
)
_UK_AIM_SECURITY_NAME_TRAILING_PATTERNS = (
    re.compile(
        (
            r"\s+(?:ordinary\s+shares?|ords?|ord|common\s+shares?|com\s+shares?|"
            r"depository\s+receipts?|shares?|shs?|units?|rights?)\b.*$"
        ),
        re.IGNORECASE,
    ),
    re.compile(r"\s+npv\b.*$", re.IGNORECASE),
)
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
_FRANCE_COMPANY_LEGAL_SUFFIX_PATTERNS = (
    re.compile(r",?\s+s\.?\s*a\.?\s*s\.?\s*u\.?$", re.IGNORECASE),
    re.compile(r",?\s+s\.?\s*a\.?\s*s\.?$", re.IGNORECASE),
    re.compile(r",?\s+s\.?\s*c\.?\s*a\.?$", re.IGNORECASE),
    re.compile(r",?\s+s\.?\s*e\.?$", re.IGNORECASE),
    re.compile(r",?\s+s\.?\s*a\.?$", re.IGNORECASE),
    re.compile(r",?\s+soci[eé]t[eé]\s+anonyme$", re.IGNORECASE),
)
_JAPAN_COMPANY_LEGAL_SUFFIX_PATTERNS = (
    re.compile(r",?\s+co\.?,?\s*ltd\.?$", re.IGNORECASE),
    re.compile(r",?\s+corporation$", re.IGNORECASE),
    re.compile(r",?\s+holdings?\s+inc\.?$", re.IGNORECASE),
    re.compile(r",?\s+holdings?$", re.IGNORECASE),
    re.compile(r",?\s+inc\.?$", re.IGNORECASE),
    re.compile(r",?\s+ltd\.?$", re.IGNORECASE),
)
_COMPANIES_HOUSE_HONORIFIC_SUFFIXES = frozenset(
    {"mr", "mrs", "ms", "miss", "sir", "dr", "prof", "professor", "lord", "lady"}
)
_ARGENTINA_COMPANY_LEGAL_SUFFIX_PATTERNS = (
    re.compile(r",?\s+s\.?\s*a\.?\s*u\.?$", re.IGNORECASE),
    re.compile(r",?\s+s\.?\s*a\.?\s*i\.?\s*c\.?$", re.IGNORECASE),
    re.compile(r",?\s+s\.?\s*r\.?\s*l\.?$", re.IGNORECASE),
    re.compile(r",?\s+s\.?\s*a\.?$", re.IGNORECASE),
    re.compile(r",?\s+sociedad an[oó]nima$", re.IGNORECASE),
)
_ARGENTINA_BYMA_PERSON_ROLE_CLASS_KEYWORDS = {
    "director": (
        "presidente",
        "vicepresidente",
        "director",
        "sindico",
        "síndico",
        "comite de auditoria",
        "comite de auditoría",
        "miemb.tit.comite",
        "miemb.sup.comite",
    ),
    "executive_officer": (
        "chief",
        "ceo",
        "cfo",
        "coo",
        "cto",
        "gerente",
        "director ejecutivo",
    ),
}
_ARGENTINA_BYMA_LISTED_COMPANY_CARD_PATTERN = re.compile(
    r'technical-detail-equity\?symbol=(?P<symbol>[^"&]+)"[^>]*>'
    r"\s*<p[^>]*>.*?</p>\s*<h3[^>]*>(?P<name>.*?)</h3>\s*</a>"
    r"(?P<body>.*?<ul role=\"list\" class=\"card_v1_call-to-actions\">(?P<actions>.*?)</ul>)",
    re.IGNORECASE | re.DOTALL,
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


def _wikidata_metadata(
    qid: str,
    *,
    label: str = "",
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    metadata = dict(extra or {})
    resolved_qid = str(qid).strip()
    if not resolved_qid:
        return metadata
    metadata["wikidata_qid"] = resolved_qid
    metadata["wikidata_slug"] = resolved_qid
    metadata["wikidata_entity_id"] = f"wikidata:{resolved_qid}"
    metadata["wikidata_url"] = _WIKIDATA_ENTITY_URL_TEMPLATE.format(qid=resolved_qid)
    normalized_label = " ".join(str(label).split())
    if normalized_label:
        metadata["wikidata_label"] = normalized_label
    return metadata


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
                "https://www.byma.com.ar/",
                category="exchange",
            ),
            _source(
                "byma-listed-companies",
                _BYMA_LISTED_COMPANIES_URL,
                category="issuer_directory",
            ),
            _source(
                "cnv-emitter-regimes",
                _CNV_EMITTER_REGIMES_URL,
                category="issuer_directory",
                notes="Use the official CNV issuer-regime workbook to enrich Argentina public issuers with CUIT and current public-offer regime metadata.",
            ),
            _source(
                "bymadata-all-instrument-select",
                _BYMA_DATA_ALL_INSTRUMENT_SELECT_URL,
                category="issuer_directory",
            ),
            _source(
                "bcra-financial-institutions",
                _BCRA_FINANCIAL_INSTITUTIONS_PUBLICATION_URL,
                category="institution_register",
                notes="Use the official BCRA financial-institutions publication page and linked monthly .7z archive to derive regulated institutions plus public directors, managers, and customer-service officers.",
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
                aliases=("BYMA", "Bolsas y Mercados Argentinos S.A."),
                entity_id="finance-ar:byma",
                metadata={"country_code": "ar", "category": "exchange"},
            ),
            _entity(
                "exchange",
                "A3 Mercados",
                aliases=("A3", "A3 Mercados S.A."),
                entity_id="finance-ar:a3",
                metadata={"country_code": "ar", "category": "exchange"},
            ),
            _entity(
                "exchange",
                "Mercado Argentino de Valores",
                aliases=("MAV", "Mercado Argentino de Valores S.A."),
                entity_id="finance-ar:mav",
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
                "amf-listed-companies-issuers",
                _AMF_LISTED_COMPANIES_ISSUERS_URL,
                category="securities_regulator",
            ),
            _source(
                "euronext-paris-regulated",
                _EURONEXT_PARIS_REGULATED_LIST_URL,
                category="issuer_directory",
            ),
            _source(
                "euronext-paris-growth",
                _EURONEXT_PARIS_GROWTH_LIST_URL,
                category="issuer_directory",
            ),
            _source(
                "euronext-paris-access",
                _EURONEXT_PARIS_ACCESS_LIST_URL,
                category="issuer_directory",
            ),
            _source(
                "euronext-paris-company-news",
                _EURONEXT_PARIS_COMPANY_NEWS_URL,
                category="filing_system",
            ),
            _source(
                "euronext-paris-company-news-archive",
                _EURONEXT_PARIS_COMPANY_NEWS_ARCHIVE_URL,
                category="filing_system",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Autorité des marchés financiers",
                aliases=("AMF", "French Financial Markets Authority"),
                entity_id="finance-fr:amf",
                metadata=_wikidata_metadata(
                    "Q788606",
                    label="Autorité des marchés financiers",
                    extra={
                        "country_code": "fr",
                        "category": "securities_regulator",
                    },
                ),
            ),
            _entity(
                "exchange",
                "Euronext Paris",
                aliases=(),
                entity_id="finance-fr:euronext-paris",
                metadata=_wikidata_metadata(
                    "Q2385849",
                    label="Euronext Paris",
                    extra={"country_code": "fr", "category": "exchange"},
                ),
            ),
            _entity(
                "exchange",
                "Euronext Growth Paris",
                aliases=("Paris Growth Market",),
                entity_id="finance-fr:euronext-growth-paris",
                metadata=_wikidata_metadata(
                    "Q107188657",
                    label="Euronext Growth Paris",
                    extra={"country_code": "fr", "category": "exchange"},
                ),
            ),
            _entity(
                "exchange",
                "Euronext Access Paris",
                aliases=("Paris Access Market",),
                entity_id="finance-fr:euronext-access-paris",
                metadata=_wikidata_metadata(
                    "Q107190270",
                    label="Euronext Access Paris",
                    extra={"country_code": "fr", "category": "exchange"},
                ),
            ),
            _entity(
                "organization",
                "Banque de France",
                aliases=(),
                entity_id="finance-fr:banque-de-france",
                metadata=_wikidata_metadata(
                    "Q806950",
                    label="Bank of France",
                    extra={"country_code": "fr", "category": "central_bank"},
                ),
            ),
            _entity(
                "organization",
                "Autorité de contrôle prudentiel et de résolution",
                aliases=("ACPR",),
                entity_id="finance-fr:acpr",
                metadata=_wikidata_metadata(
                    "Q2872781",
                    label="French Prudential Supervisory Authority",
                    extra={
                        "country_code": "fr",
                        "category": "institution_regulator",
                    },
                ),
            ),
            _entity(
                "market_index",
                "CAC 40",
                aliases=(),
                entity_id="finance-fr:cac-40",
                metadata=_wikidata_metadata(
                    "Q648828",
                    label="CAC 40",
                    extra={"country_code": "fr", "category": "market_index"},
                ),
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
            _source("boerse-stuttgart", _BOERSE_STUTTGART_URL, category="exchange"),
            _source("boerse-berlin", _BOERSE_BERLIN_URL, category="exchange"),
            _source("boerse-hamburg", _BOERSE_HAMBURG_URL, category="exchange"),
            _source("boerse-hannover", _BOERSE_HANNOVER_URL, category="exchange"),
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
                "Börse Stuttgart",
                aliases=("Stuttgart Stock Exchange", "Borse Stuttgart"),
                entity_id="finance-de:boerse-stuttgart",
                metadata={"country_code": "de", "category": "exchange"},
            ),
            _entity(
                "exchange",
                "Börse Berlin",
                aliases=("Berlin Stock Exchange", "Borse Berlin"),
                entity_id="finance-de:boerse-berlin",
                metadata={"country_code": "de", "category": "exchange"},
            ),
            _entity(
                "exchange",
                "Börse Hamburg",
                aliases=("Hamburg Stock Exchange", "Borse Hamburg"),
                entity_id="finance-de:boerse-hamburg",
                metadata={"country_code": "de", "category": "exchange"},
            ),
            _entity(
                "exchange",
                "Börse Hannover",
                aliases=("Hannover Stock Exchange", "Hanover Stock Exchange", "Borse Hannover"),
                entity_id="finance-de:boerse-hannover",
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
                _JPX_ENGLISH_DISCLOSURE_GATE_URL,
                category="disclosure_portal",
            ),
            _source(
                "jpx-english-disclosure-availability",
                _JPX_ENGLISH_DISCLOSURE_AVAILABILITY_URL,
                category="issuer_directory",
                notes="Use the official JPX monthly workbook on English disclosure availability as the current issuer and ticker seed set for TSE Prime, Standard, and Growth companies.",
            ),
            _source(
                "jpx-listed-company-search",
                _JPX_LISTED_COMPANY_SEARCH_URL,
                category="issuer_directory",
                notes="Official JPX listed-company search landing page for expanding issuer-profile and filing joins.",
            ),
            _source(
                "jpx-corporate-governance-search",
                _JPX_CORPORATE_GOVERNANCE_SEARCH_URL,
                category="issuer_profile",
                notes="Official JPX corporate-governance search landing page and governance-report discovery lane.",
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
                "lse-aim-all-share-constituents",
                _UK_LSE_AIM_ALL_SHARE_CONSTITUENTS_URL,
                category="issuer_directory",
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
                metadata=_wikidata_metadata(
                    "Q5449586",
                    label="Financial Conduct Authority",
                    extra={"country_code": "uk", "category": "securities_regulator"},
                ),
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
                metadata=_wikidata_metadata(
                    "Q7309686",
                    label="Regulatory News Service",
                    extra={"country_code": "uk", "category": "disclosure_portal"},
                ),
            ),
            _entity(
                "exchange",
                "London Stock Exchange",
                aliases=("LSE",),
                entity_id="finance-uk:lse",
                metadata=_wikidata_metadata(
                    "Q171240",
                    label="London Stock Exchange",
                    extra={"country_code": "uk", "category": "exchange"},
                ),
            ),
            _entity(
                "organization",
                "Alternative Investment Market",
                aliases=("AIM",),
                entity_id="finance-uk:aim",
                metadata=_wikidata_metadata(
                    "Q438511",
                    label="Alternative Investment Market",
                    extra={"country_code": "uk", "category": "market_segment"},
                ),
            ),
            _entity(
                "market_index",
                "FTSE 100",
                aliases=("FTSE 100 Index",),
                entity_id="finance-uk:ftse-100",
                metadata=_wikidata_metadata(
                    "Q466496",
                    label="FTSE 100 Index",
                    extra={"country_code": "uk", "category": "market_index"},
                ),
            ),
            _entity(
                "market_index",
                "FTSE AIM All-Share",
                aliases=("AIM All-Share", "FTSE AIM All-Share Index"),
                entity_id="finance-uk:ftse-aim-all-share",
                metadata=_wikidata_metadata(
                    "Q5427224",
                    label="FTSE AIM All-Share Index",
                    extra={"country_code": "uk", "category": "market_index"},
                ),
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
            _source(
                "wikidata-entity-resolution",
                _WIKIDATA_API_URL,
                category="knowledge_graph",
                notes="Use for SEC CIK-backed issuer and ticker QID resolution plus alias enrichment after deterministic SEC derivation.",
            ),
        ),
        entities=(
            _entity(
                "organization",
                "Securities and Exchange Commission",
                aliases=("SEC",),
                entity_id="finance-us:sec",
                metadata=_wikidata_metadata(
                    "Q953944",
                    label="United States Securities and Exchange Commission",
                    extra={"country_code": "us", "category": "securities_regulator"},
                ),
            ),
            _entity(
                "organization",
                "EDGAR",
                aliases=("Electronic Data Gathering, Analysis, and Retrieval",),
                entity_id="finance-us:edgar",
                metadata=_wikidata_metadata(
                    "Q3050604",
                    label="EDGAR",
                    extra={"country_code": "us", "category": "filing_system"},
                ),
            ),
            _entity(
                "exchange",
                "Nasdaq Stock Market",
                aliases=("NASDAQ", "Nasdaq"),
                entity_id="finance-us:nasdaq",
                metadata=_wikidata_metadata(
                    "Q82059",
                    label="Nasdaq",
                    extra={"country_code": "us", "category": "exchange"},
                ),
            ),
            _entity(
                "exchange",
                "New York Stock Exchange",
                aliases=("NYSE",),
                entity_id="finance-us:nyse",
                metadata=_wikidata_metadata(
                    "Q13677",
                    label="New York Stock Exchange",
                    extra={"country_code": "us", "category": "exchange"},
                ),
            ),
            _entity(
                "organization",
                "Federal Deposit Insurance Corporation",
                aliases=("FDIC",),
                entity_id="finance-us:fdic",
                metadata=_wikidata_metadata(
                    "Q1345065",
                    label="Federal Deposit Insurance Corporation",
                    extra={"country_code": "us", "category": "institution_regulator"},
                ),
            ),
            _entity(
                "market_index",
                "S&P 500",
                aliases=("S&P 500 Index",),
                entity_id="finance-us:sp-500",
                metadata=_wikidata_metadata(
                    "Q242345",
                    label="S&P 500",
                    extra={"country_code": "us", "category": "market_index"},
                ),
            ),
        ),
        extra_fields={
            "people_derivation": {
                "kind": "sec_proxy",
                "companies_source": "sec-company-tickers",
                "submissions_source": "sec-submissions",
                "archive_base_url": DEFAULT_SEC_ARCHIVES_BASE_URL,
                "output_path": "derived_sec_proxy_people_entities.json",
                "name": "derived-finance-us-sec-people",
                "adapter": "sec_proxy_plus_insider_people_derivation",
            }
        },
    ),
}


FINANCE_COUNTRY_PEOPLE_SOURCE_PLANS: dict[str, dict[str, object]] = {
    "ar": {
        "country_code": "ar",
        "country_name": "Argentina",
        "automation_status": "partial",
        "scope": "listed-company directors, executive officers, market representatives, investor-relations contacts, and regulated financial-institution principals",
        "update_strategy": [
            "Refresh the BYMADATA instrument universe on each country snapshot and rebuild the Argentina issuer and ticker seed set from official BYMA symbols.",
            "Refresh the BYMA listed-companies page and use its live listed-company cards as the primary Argentina equity issuer roster plus fallback company metadata when some BYMADATA profile endpoints return empty payloads.",
            "Refresh the official CNV issuer-regime workbook so matched Argentina issuers, tickers, and issuer-linked people keep current CUIT and public-offer regime metadata.",
            "Current automation derives public issuers, tickers, and named board and market-representative people from BYMADATA species and sociedades endpoints keyed by official symbols.",
            "Refresh the BCRA financial-institutions publication and its current monthly .7z archive so regulated institutions, public directors, managers, and customer-service officers stay current.",
            "Enrich the deterministic BYMADATA issuer and person set with Wikidata QIDs and aliases so issuer short forms and public executive names stay easier to match.",
            "Expand next to CNV/AIF issuer disclosures and annual filings so listed-company board and executive coverage does not depend only on BYMADATA societary pages.",
        ],
        "sources": [
            _source("cnv-aif-issuers", "https://www.cnv.gov.ar/", category="filing_system", notes="Use issuer disclosure records and annual filings for board and officer names."),
            _source("byma-listed-companies", _BYMA_LISTED_COMPANIES_URL, category="issuer_directory", notes="Use the live listed-company page as the primary BYMA equity issuer roster and as fallback company metadata when some BYMADATA profile endpoints return empty payloads."),
            _source("cnv-emitter-regimes", _CNV_EMITTER_REGIMES_URL, category="issuer_directory", notes="Official CNV workbook with issuer CUIT, current category, and current category status for public-offer companies."),
            _source("bymadata-all-instrument-select", _BYMA_DATA_ALL_INSTRUMENT_SELECT_URL, category="issuer_directory", notes="Official BYMADATA instrument universe used to discover BYMA equity symbols."),
            _source("bymadata-sociedades-general", _BYMA_DATA_SOCIETY_GENERAL_URL, category="issuer_profile", notes="Official BYMADATA societary profile endpoint with issuer website, address, and activity metadata."),
            _source("bymadata-sociedades-administracion", _BYMA_DATA_SOCIETY_ADMINISTRATION_URL, category="issuer_profile", notes="Official BYMADATA societary administration endpoint with board and named administration people."),
            _source("bymadata-sociedades-responsables", _BYMA_DATA_SOCIETY_RESPONSIBLES_URL, category="issuer_profile", notes="Official BYMADATA societary responsables endpoint with named market representatives."),
            _source("bcra-financial-institutions", _BCRA_FINANCIAL_INSTITUTIONS_PUBLICATION_URL, category="institution_register", notes="Use the official BCRA monthly publication archive for regulated institutions plus public directors, managers, and customer-service officers."),
            _source("wikidata-entity-resolution", _WIKIDATA_API_URL, category="knowledge_graph", notes="Use for issuer and public-person QID resolution plus alias enrichment after deterministic BYMADATA extraction."),
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
        "scope": "listed-company management board members, supervisory board members, issuer contacts, bounded major-shareholder entities, and regulated institution principals",
        "update_strategy": [
            "Refresh the Deutsche Borse listed-companies workbook to seed issuer and ticker coverage.",
            "Refresh the official Borse Munchen m:access table and Borse Dusseldorf Primarmarkt table to extend issuer coverage beyond Frankfurt/Xetra.",
            "Refresh Unternehmensregister issuer filings, Deutsche Borse company-details pages, and official German issuer governance pages.",
            "Refresh the BaFin company-database CSV export and merge Germany-domiciled institutions plus Germany branches into the pack.",
            "Extract people from annual reports, corporate-governance statements, and issuer profile disclosures.",
            "Promote clearly disclosed major shareholders into organization or person entities instead of leaving them as metadata-only strings.",
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
        "automation_status": "partial",
        "scope": "listed-company directors, executive officers, investor-relations contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh the rendered Euronext Paris regulated, Growth, and Access market lists to keep official issuer and ticker coverage current.",
            "Refresh Euronext Paris company-news latest and archive pages, then derive issuer-linked contacts and governance-page entry points from official regulated news.",
            "Enrich the deterministic France issuer and public-person set with Wikidata QIDs and aliases so issuer legal-name variants and named executives remain easier to match.",
            "Refresh AMF issuer/disclosure surfaces plus linked DILA or issuer-hosted annual-report and universal-registration-document pages.",
            "Extract people from issuer governance pages, investor-relations contacts, and HTML or XHTML annual-report and universal-registration-document disclosures.",
            "Use REGAFI separately for financial-institution principals.",
        ],
        "sources": [
            _source("euronext-paris-regulated", _EURONEXT_PARIS_REGULATED_LIST_URL, category="issuer_directory", notes="Primary issuer and ticker seed source for the regulated Euronext Paris roster; render the live table DOM before parsing."),
            _source("euronext-paris-growth", _EURONEXT_PARIS_GROWTH_LIST_URL, category="issuer_directory", notes="Official Euronext Growth Paris issuer directory; render the live table DOM before parsing."),
            _source("euronext-paris-access", _EURONEXT_PARIS_ACCESS_LIST_URL, category="issuer_directory", notes="Official Euronext Access Paris issuer directory; render the live table DOM before parsing."),
            _source("euronext-paris-company-news", _EURONEXT_PARIS_COMPANY_NEWS_URL, category="filing_system", notes="Official Euronext Paris regulated-news latest page; use article modals to collect issuer-linked contacts and finance-site links."),
            _source("euronext-paris-company-news-archive", _EURONEXT_PARIS_COMPANY_NEWS_ARCHIVE_URL, category="filing_system", notes="Official Euronext Paris regulated-news archive; paginate this to find annual-report and universal-registration-document announcements by issuer."),
            _source("amf-listed-companies-issuers", _AMF_LISTED_COMPANIES_ISSUERS_URL, category="filing_system", notes="Official AMF issuer/disclosure entry point for future board and executive extraction."),
            _source("dila-regulated-information", "https://www.info-financiere.fr/", category="filing_system", notes="Official DILA archive for annual reports and universal registration documents discovered from France issuer disclosure flows."),
            _source("wikidata-entity-resolution", _WIKIDATA_API_URL, category="knowledge_graph", notes="Use for issuer and public-person QID resolution plus alias enrichment after deterministic Euronext extraction."),
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
        "automation_status": "partial",
        "scope": "listed-company directors, officers, representative executives, investor-relations contacts, and regulated institution principals",
        "update_strategy": [
            "Refresh the JPX English Disclosure GATE availability page and resolve its current monthly workbook on each country snapshot.",
            "Current automation derives broader TSE Prime, Standard, and Growth issuer and ticker coverage from JPX Listed Company Search result pages, enriches the workbook/governance subset from JPX listed-company detail pages, then overlays the JPX monthly workbook metadata for companies with English disclosure availability.",
            "Refresh EDINET issuer filings plus JPX Listed Company Search and Corporate Governance Information Search pages on each snapshot.",
            "Current automation extracts representative executives and investor-relations contacts from official English corporate-governance reports discovered through the JPX Corporate Governance Information Search.",
            "Expand people coverage next from broader listed-company detail-page coverage, annual securities reports, broader corporate-governance reports, and timely disclosures.",
        ],
        "sources": [
            _source("edinet-filings", "https://info.edinet-fsa.go.jp/", category="filing_system", notes="Primary official filing source for directors, officers, and governance disclosures."),
            _source("jpx-disclosure-gate", _JPX_ENGLISH_DISCLOSURE_GATE_URL, category="issuer_directory", notes="Primary official disclosure portal for JPX English listed-company materials."),
            _source("jpx-english-disclosure-availability", _JPX_ENGLISH_DISCLOSURE_AVAILABILITY_URL, category="issuer_directory", notes="Resolve and parse the current versioned monthly workbook linked from the public JPX page as the English-disclosure metadata overlay lane."),
            _source("jpx-listed-company-search", _JPX_LISTED_COMPANY_SEARCH_URL, category="issuer_directory", notes="Primary official issuer and ticker coverage lane for live search results plus linked listed-company detail-page enrichment."),
            _source("jpx-corporate-governance-search", _JPX_CORPORATE_GOVERNANCE_SEARCH_URL, category="issuer_profile", notes="Primary official governance-report discovery lane for current representative-executive and investor-relations people extraction."),
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
            "Refresh the rendered FTSE AIM All-Share constituents table and merge AIM issuers plus tickers into the UK company roster.",
            "Resolve listed issuers to Companies House company numbers, then extract current officers from official officer pages.",
            "Promote active Companies House persons with significant control into shareholder people or organization entities when the official PSC page exposes them.",
            "Expand later with RNS / annual-report / governance lanes for issuer personnel not represented in Companies House officer data.",
        ],
        "sources": [
            _source("fca-official-list-live", _FCA_OFFICIAL_LIST_LIVE_URL, category="issuer_directory", notes="Primary official listed-company source for UK commercial companies."),
            _source("lse-aim-all-share-constituents", _UK_LSE_AIM_ALL_SHARE_CONSTITUENTS_URL, category="issuer_directory", notes="Rendered London Stock Exchange AIM roster for additional UK listed companies and tickers."),
            _source("rns-announcements", "https://www.lseg.com/en/capital-markets/regulatory-news-service", category="filing_system", notes="Use official announcements and annual reports for board and management names."),
            _source("companies-house-officers", "https://www.gov.uk/guidance/search-the-companies-house-register", category="company_registry", notes="Use company-officer registry for canonical director names."),
            _source("companies-house-psc", "https://www.gov.uk/guidance/search-the-companies-house-register", category="company_registry", notes="Use persons-with-significant-control pages for active shareholder people and controlling organizations."),
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
            "Derive people from DEF 14A plus Forms 3/4/5 insider filings; widen later with 8-K item 5.02 disclosures.",
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
            resolved_url = _download_country_source(
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
        verify = _httpx_verify_for_source_url(normalized_source_url)
        response = httpx.get(
            normalized_source_url,
            headers=headers,
            follow_redirects=True,
            timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
            verify=verify,
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
                verify=_httpx_verify_for_source_url(redirect_target),
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


def _download_country_source(
    source_url: str | Path,
    destination: Path,
    *,
    user_agent: str,
) -> str:
    normalized_source_url = normalize_source_url(source_url)
    if normalized_source_url in _EURONEXT_PARIS_RENDERED_DIRECTORY_URLS:
        return _download_euronext_directory_page(
            normalized_source_url,
            destination,
            user_agent=user_agent,
        )
    if normalized_source_url in _UK_LSE_RENDERED_DIRECTORY_URLS:
        return _download_london_stock_exchange_directory_page(
            normalized_source_url,
            destination,
            user_agent=user_agent,
        )
    return _download_source(normalized_source_url, destination, user_agent=user_agent)


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


def _fetch_json_post_source(
    source_url: str,
    destination: Path,
    *,
    payload: dict[str, object],
    user_agent: str,
) -> object:
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json,text/plain;q=0.9,*/*;q=0.1",
    }
    response = httpx.post(
        source_url,
        json=payload,
        headers=headers,
        follow_redirects=True,
        timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
        verify=_httpx_verify_for_source_url(source_url),
    )
    response.raise_for_status()
    response_payload = response.json()
    _write_json_snapshot(destination, payload=response_payload)
    return response_payload


def _httpx_verify_for_source_url(source_url: str | Path) -> bool:
    hostname = urlparse(normalize_source_url(source_url)).hostname or ""
    return hostname.casefold() not in _RELAXED_TLS_SOURCE_HOSTS


def _write_curated_entities(path: Path, *, entities: list[dict[str, object]]) -> None:
    path.write_text(
        json.dumps({"entities": entities}, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_json_snapshot(path: Path, *, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
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
    if str(profile.get("country_code", "")).casefold() == "us":
        sec_company_items = _load_sec_company_items(companies_path)
        sec_company_profiles = {
            source_id: {
                "employer_cik": source_id,
                "employer_name": _clean_text(
                    item.get("title") or item.get("name") or item.get("issuer")
                ),
                "employer_ticker": _clean_text(item.get("ticker")),
            }
            for item in sec_company_items
            if (source_id := _format_sec_source_id(item.get("cik_str") or item.get("cik")))
        }
        insider_people_entities = _extract_united_states_sec_insider_people_entities(
            sec_submissions_path=submissions_path,
            sec_company_profiles=sec_company_profiles,
            archive_base_url=str(
                people_derivation.get(
                    "archive_base_url",
                    DEFAULT_SEC_ARCHIVES_BASE_URL,
                )
            ),
            user_agent=user_agent,
        )
        normalized_people_entities = _merge_country_people_entities(
            normalized_people_entities,
            insider_people_entities,
        )
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


def _extract_united_states_sec_insider_people_entities(
    *,
    sec_submissions_path: Path,
    sec_company_profiles: dict[str, dict[str, str]],
    archive_base_url: str,
    user_agent: str,
) -> list[dict[str, object]]:
    filings_to_download: list[dict[str, str]] = []
    submission_records = _read_targeted_zip_json_records(
        sec_submissions_path,
        target_source_ids=set(sec_company_profiles),
    )
    for submission in submission_records:
        source_id = _format_sec_source_id(submission.get("cik"))
        if not source_id:
            continue
        employer_profile = sec_company_profiles.get(source_id, {})
        employer_name = _clean_text(submission.get("name")) or employer_profile.get(
            "employer_name", ""
        )
        employer_ticker = employer_profile.get("employer_ticker", "")
        insider_filing_count = 0
        for filing in _iter_country_sec_recent_filings(submission):
            form = _clean_text(filing.get("form")).upper()
            if form not in _UNITED_STATES_SEC_INSIDER_FORMS:
                continue
            primary_document = _clean_text(filing.get("primaryDocument"))
            accession_number = _clean_text(filing.get("accessionNumber"))
            filing_date = _clean_text(filing.get("filingDate"))
            if not primary_document or not accession_number:
                continue
            filings_to_download.append(
                {
                    "employer_cik": source_id,
                    "employer_name": employer_name,
                    "employer_ticker": employer_ticker,
                    "filing_date": filing_date,
                    "filing_url": _build_sec_filing_document_url(
                        archive_base_url=archive_base_url,
                        source_id=source_id,
                        accession_number=accession_number,
                        primary_document=primary_document,
                    ),
                    "source_form": form,
                }
            )
            insider_filing_count += 1
            if (
                insider_filing_count
                >= _UNITED_STATES_SEC_INSIDER_MAX_FILINGS_PER_ISSUER
            ):
                break
    if not filings_to_download:
        return []
    extracted_entities: list[dict[str, object]] = []
    with ThreadPoolExecutor(
        max_workers=_UNITED_STATES_SEC_INSIDER_DOWNLOAD_WORKERS
    ) as executor:
        futures = {
            executor.submit(
                _extract_united_states_sec_insider_people_entity_from_filing,
                filing=filing,
                user_agent=user_agent,
            ): filing
            for filing in filings_to_download
        }
        for future in as_completed(futures):
            try:
                entity = future.result()
            except Exception:
                continue
            if entity is not None:
                extracted_entities.append(entity)
    return _merge_country_people_entities([], extracted_entities)


def _extract_united_states_sec_insider_people_entity_from_filing(
    *,
    filing: dict[str, str],
    user_agent: str,
) -> dict[str, object] | None:
    document_text = _download_text(
        str(filing.get("filing_url", "")),
        user_agent=user_agent,
    )
    return _extract_united_states_sec_insider_people_entity(
        text=document_text,
        employer_cik=str(filing.get("employer_cik", "")),
        employer_name=str(filing.get("employer_name", "")),
        employer_ticker=str(filing.get("employer_ticker", "")),
        filing_date=str(filing.get("filing_date", "")),
        filing_url=str(filing.get("filing_url", "")),
        source_form=str(filing.get("source_form", "")),
    )


def _extract_united_states_sec_insider_people_entity(
    *,
    text: str,
    employer_cik: str,
    employer_name: str,
    employer_ticker: str,
    filing_date: str,
    filing_url: str,
    source_form: str,
) -> dict[str, object] | None:
    lines = _normalize_sec_proxy_document_lines(text)
    raw_name, canonical_name, aliases = _extract_united_states_sec_reporting_owner_name(
        lines
    )
    if not canonical_name:
        return None
    relationship = _extract_united_states_sec_reporting_relationship(lines)
    role_title = relationship["role_title"]
    role_class = relationship["role_class"]
    slug = _slugify(canonical_name)
    if not slug:
        return None
    return {
        "entity_id": f"finance-person:{employer_cik}:{slug}",
        "entity_type": "person",
        "canonical_text": canonical_name,
        "aliases": _unique_aliases(
            [
                *aliases,
                *([raw_name] if raw_name else []),
            ]
        ),
        "source_name": "sec-insider-people",
        "source_id": f"{employer_cik}:{slug}:{source_form.casefold()}",
        "metadata": {
            "country_code": "us",
            "category": role_class,
            "employer_name": employer_name,
            "employer_ticker": employer_ticker,
            "employer_cik": employer_cik,
            "role_title": role_title,
            "role_class": role_class,
            "source_form": source_form,
            "source_forms": [source_form],
            "effective_date": filing_date,
            "source_url": filing_url,
            "source_urls": [filing_url],
        },
    }


def _extract_united_states_sec_reporting_owner_name(
    lines: list[str],
) -> tuple[str, str, list[str]]:
    for index, line in enumerate(lines):
        if _UNITED_STATES_SEC_REPORTING_PERSON_HEADER_PATTERN.search(line) is None:
            continue
        for offset in range(1, 6):
            candidate_index = index + offset
            if candidate_index >= len(lines):
                break
            raw_name = _clean_text(lines[candidate_index]).strip("*")
            if (
                not raw_name
                or raw_name.startswith("(")
                or "Issuer Name" in raw_name
                or "Street" in raw_name
            ):
                continue
            lookahead = lines[candidate_index + 1] if candidate_index + 1 < len(lines) else ""
            reordered_name = raw_name
            if "," in raw_name:
                last_name, remainder = [part.strip() for part in raw_name.split(",", 1)]
                reordered_name = f"{remainder} {last_name}".strip()
            elif "(Last)" in lookahead and "(First)" in lookahead:
                parts = raw_name.split()
                if len(parts) >= 2:
                    reordered_name = " ".join([*parts[1:], parts[0]])
            canonical_name = _normalize_person_display_name(reordered_name)
            if not _looks_like_finance_person_name(
                canonical_name,
                raw_value=raw_name,
            ):
                continue
            aliases = (
                [raw_name]
                if raw_name and raw_name.casefold() != canonical_name.casefold()
                else []
            )
            return raw_name, canonical_name, aliases
        break
    return "", "", []


def _extract_united_states_sec_reporting_relationship(
    lines: list[str],
) -> dict[str, str]:
    start_index = next(
        (
            index
            for index, line in enumerate(lines)
            if _UNITED_STATES_SEC_RELATIONSHIP_HEADER_PATTERN.search(line) is not None
        ),
        None,
    )
    if start_index is None:
        return {"role_class": "executive_officer", "role_title": "Insider"}
    relationship_lines: list[str] = []
    for line in lines[start_index + 1 : start_index + 8]:
        if _UNITED_STATES_SEC_SECTION_BREAK_PATTERN.match(line):
            break
        if "Check all applicable" in line:
            continue
        relationship_lines.append(line)
    relationship_text = " | ".join(relationship_lines)
    officer_title = _extract_united_states_sec_officer_title(relationship_lines)
    director_checked = _line_starts_checked_relationship(relationship_text, "Director")
    officer_checked = _line_starts_checked_relationship(
        relationship_text,
        "Officer (give title below)",
    )
    owner_checked = _line_starts_checked_relationship(relationship_text, "10% Owner")
    if not owner_checked and re.search(
        r"Director\s*\|\s*X\s*\|\s*10%\s+Owner",
        relationship_text,
        re.IGNORECASE,
    ):
        owner_checked = True
    if officer_title:
        return {
            "role_class": _classify_finance_person_role(officer_title),
            "role_title": officer_title,
        }
    if director_checked:
        return {"role_class": "director", "role_title": "Director"}
    if officer_checked:
        return {"role_class": "executive_officer", "role_title": "Officer"}
    if owner_checked:
        return {"role_class": "beneficial_owner", "role_title": "10% Owner"}
    return {"role_class": "executive_officer", "role_title": "Insider"}


def _extract_united_states_sec_officer_title(lines: list[str]) -> str:
    for index, line in enumerate(lines):
        if "Officer (give title below)" not in line:
            continue
        for candidate in lines[index + 1 : index + 4]:
            normalized = _clean_text(candidate)
            if (
                not normalized
                or normalized.startswith("(")
                or "Form filed" in normalized
                or "Director" in normalized
                or "Owner" in normalized
                or "Officer (give title below)" in normalized
            ):
                continue
            return normalized
    return ""


def _line_starts_checked_relationship(value: str, label: str) -> bool:
    return (
        re.search(
            rf"(?:^|\|)\s*X\s*\|\s*{re.escape(label)}(?:\s|\||$)",
            value,
            re.IGNORECASE,
        )
        is not None
    )


def _iter_country_sec_recent_filings(submission: dict[str, Any]) -> list[dict[str, str]]:
    recent = submission.get("filings", {}).get("recent", {})
    if not isinstance(recent, dict):
        return []
    keys = (
        "accessionNumber",
        "filingDate",
        "form",
        "primaryDocument",
        "primaryDocDescription",
    )
    columns = {
        key: value if isinstance(value, list) else []
        for key, value in ((key, recent.get(key)) for key in keys)
    }
    row_count = max((len(values) for values in columns.values()), default=0)
    filings: list[dict[str, str]] = []
    for index in range(row_count):
        filings.append(
            {
                key: _clean_text(values[index]) if index < len(values) else ""
                for key, values in columns.items()
            }
        )
    return filings


def _merge_country_people_entities(
    entities: list[dict[str, object]],
    additional_entities: list[dict[str, object]],
) -> list[dict[str, object]]:
    merged: dict[str, dict[str, object]] = {}
    for source in [*entities, *additional_entities]:
        entity_id = _clean_text(source.get("entity_id"))
        canonical_text = _clean_text(source.get("canonical_text"))
        if not entity_id or not canonical_text:
            continue
        metadata = dict(source.get("metadata") or {})
        current = merged.get(entity_id)
        if current is None:
            merged[entity_id] = {
                **source,
                "aliases": list(source.get("aliases", [])),
                "metadata": metadata,
            }
            continue
        current["aliases"] = _unique_aliases(
            [
                *[str(alias) for alias in current.get("aliases", [])],
                *[str(alias) for alias in source.get("aliases", [])],
            ]
        )
        current_metadata = dict(current.get("metadata") or {})
        candidate_role = _clean_text(metadata.get("role_class") or metadata.get("category"))
        existing_role = _clean_text(
            current_metadata.get("role_class") or current_metadata.get("category")
        )
        if _country_people_role_rank(candidate_role) > _country_people_role_rank(
            existing_role
        ):
            for key in ("role_title", "role_class", "category", "effective_date"):
                value = _clean_text(metadata.get(key))
                if value:
                    current_metadata[key] = value
        for key in ("country_code", "employer_name", "employer_ticker", "employer_cik"):
            value = _clean_text(metadata.get(key))
            if value and not _clean_text(current_metadata.get(key)):
                current_metadata[key] = value
        current_metadata["source_forms"] = _dedupe_preserving_order(
            [
                *_coerce_country_metadata_list(current_metadata.get("source_forms")),
                *_coerce_country_metadata_list(metadata.get("source_forms")),
                *(
                    [_clean_text(current_metadata.get("source_form"))]
                    if _clean_text(current_metadata.get("source_form"))
                    else []
                ),
                *(
                    [_clean_text(metadata.get("source_form"))]
                    if _clean_text(metadata.get("source_form"))
                    else []
                ),
            ]
        )
        current_metadata["source_urls"] = _dedupe_preserving_order(
            [
                *_coerce_country_metadata_list(current_metadata.get("source_urls")),
                *_coerce_country_metadata_list(metadata.get("source_urls")),
                *(
                    [_clean_text(current_metadata.get("source_url"))]
                    if _clean_text(current_metadata.get("source_url"))
                    else []
                ),
                *(
                    [_clean_text(metadata.get("source_url"))]
                    if _clean_text(metadata.get("source_url"))
                    else []
                ),
            ]
        )
        if current_metadata.get("source_forms"):
            current_metadata["source_form"] = current_metadata["source_forms"][0]
        if current_metadata.get("source_urls"):
            current_metadata["source_url"] = current_metadata["source_urls"][0]
        current["metadata"] = current_metadata
    return sorted(
        merged.values(),
        key=lambda item: (
            str(item.get("metadata", {}).get("employer_cik", "")),
            str(item.get("canonical_text", "")).casefold(),
        ),
    )


def _coerce_country_metadata_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [_clean_text(item) for item in value if _clean_text(item)]
    cleaned = _clean_text(value)
    return [cleaned] if cleaned else []


def _country_people_role_rank(value: str) -> int:
    normalized = _clean_text(value).casefold()
    if normalized == "director":
        return 4
    if normalized == "executive_officer":
        return 3
    if normalized == "investment_professional":
        return 2
    if normalized == "beneficial_owner":
        return 1
    return 0


def _derive_argentina_byma_entities(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[dict[str, object]]:
    source_paths = {
        str(source.get("name")): country_dir / str(source.get("path"))
        for source in downloaded_sources
    }
    listed_companies_path = source_paths.get("byma-listed-companies")
    cnv_emitter_regimes_path = source_paths.get("cnv-emitter-regimes")
    instrument_select_path = source_paths.get("bymadata-all-instrument-select")
    listed_company_records_by_symbol: dict[str, dict[str, str]] = {}
    cnv_emitter_regime_records_by_key: dict[str, dict[str, str]] = {}
    if listed_companies_path is not None and listed_companies_path.exists():
        listed_company_records = _extract_argentina_byma_listed_company_records(
            listed_companies_path.read_text(encoding="utf-8", errors="ignore")
        )
        listed_company_records_by_symbol = {
            str(record["symbol"]): record for record in listed_company_records
        }
    if cnv_emitter_regimes_path is not None and cnv_emitter_regimes_path.exists():
        cnv_emitter_regime_records = _extract_argentina_cnv_emitter_regime_records(
            cnv_emitter_regimes_path
        )
        cnv_emitter_regime_records_by_key = {
            key: record
            for record in cnv_emitter_regime_records
            for key in _argentina_cnv_emitter_regime_record_keys(record)
        }
    if (
        (instrument_select_path is None or not instrument_select_path.exists())
        and not listed_company_records_by_symbol
    ):
        return []

    instrument_equity_symbols = (
        _extract_argentina_byma_equity_symbols(instrument_select_path)
        if instrument_select_path is not None and instrument_select_path.exists()
        else []
    )
    equity_symbols = list(listed_company_records_by_symbol) or instrument_equity_symbols
    if not equity_symbols:
        return []

    issuer_records_by_key: dict[str, dict[str, object]] = {}
    issuer_entities: list[dict[str, object]] = []
    institution_entities: list[dict[str, object]] = []
    ticker_entities: list[dict[str, object]] = []
    people_entities: list[dict[str, object]] = []
    issuer_warnings: list[str] = []
    institution_warnings: list[str] = []
    people_warnings: list[str] = []

    species_general_dir = country_dir / "bymadata-especies-general"
    society_general_dir = country_dir / "bymadata-sociedades-general"
    society_administration_dir = country_dir / "bymadata-sociedades-administracion"
    society_responsables_dir = country_dir / "bymadata-sociedades-responsables"
    for directory in (
        species_general_dir,
        society_general_dir,
        society_administration_dir,
        society_responsables_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    for symbol in equity_symbols:
        listed_company_record = listed_company_records_by_symbol.get(symbol)
        society_general_path = society_general_dir / f"{symbol.casefold()}.json"
        society_general_payload: object | None = None
        try:
            society_general_payload = _fetch_json_post_source(
                _BYMA_DATA_SOCIETY_GENERAL_URL,
                society_general_path,
                payload={"symbol": symbol},
                user_agent=user_agent,
            )
        except httpx.HTTPStatusError as exc:
            response = exc.response
            if response is not None and response.status_code in {400, 404}:
                society_general_payload = None
            else:
                warning_text = str(exc)
                if warning_text and warning_text not in issuer_warnings and len(issuer_warnings) < 5:
                    issuer_warnings.append(warning_text)
                society_general_payload = None
        except Exception as exc:
            warning_text = str(exc)
            if warning_text and warning_text not in issuer_warnings and len(issuer_warnings) < 5:
                issuer_warnings.append(warning_text)
            society_general_payload = None
        else:
            downloaded_sources.append(
                {
                    "name": f"bymadata-sociedades-general-{symbol.casefold()}",
                    "source_url": _BYMA_DATA_SOCIETY_GENERAL_URL,
                    "path": str(society_general_path.relative_to(country_dir)),
                    "category": "issuer_profile",
                    "notes": f"symbol={symbol}",
                }
            )
        society_general_snapshot = (
            _extract_argentina_byma_society_general_snapshot(society_general_payload)
            if society_general_payload is not None
            else None
        )

        species_general_path = species_general_dir / f"{symbol.casefold()}.json"
        species_general_payload: object | None = None
        try:
            species_general_payload = _fetch_json_post_source(
                _BYMA_DATA_SPECIES_GENERAL_URL,
                species_general_path,
                payload={"symbol": symbol},
                user_agent=user_agent,
            )
        except httpx.HTTPStatusError as exc:
            response = exc.response
            if response is not None and response.status_code in {400, 404}:
                species_general_payload = None
            else:
                warning_text = str(exc)
                if warning_text and warning_text not in issuer_warnings and len(issuer_warnings) < 5:
                    issuer_warnings.append(warning_text)
                species_general_payload = None
        except Exception as exc:
            warning_text = str(exc)
            if warning_text and warning_text not in issuer_warnings and len(issuer_warnings) < 5:
                issuer_warnings.append(warning_text)
            species_general_payload = None
        else:
            downloaded_sources.append(
                {
                    "name": f"bymadata-especies-general-{symbol.casefold()}",
                    "source_url": _BYMA_DATA_SPECIES_GENERAL_URL,
                    "path": str(species_general_path.relative_to(country_dir)),
                    "category": "issuer_profile",
                    "notes": f"symbol={symbol}",
                }
            )
        species_snapshot = (
            _extract_argentina_byma_species_snapshot(
                species_general_payload,
                symbol=symbol,
            )
            if species_general_payload is not None
            else None
        )
        if (
            species_snapshot is None
            and society_general_snapshot is None
            and listed_company_record is None
        ):
            continue

        ticker_entity = _build_argentina_byma_ticker_entity(
            symbol=symbol,
            species_snapshot=species_snapshot,
            society_general_snapshot=society_general_snapshot,
            listed_company_record=listed_company_record,
        )
        if ticker_entity is not None:
            ticker_entities.append(ticker_entity)
        issuer_record = _build_argentina_byma_issuer_record(
            symbol=symbol,
            species_snapshot=species_snapshot,
            society_general_snapshot=society_general_snapshot,
            listed_company_record=listed_company_record,
        )
        if issuer_record is None:
            continue
        company_name = str(issuer_record["company_name"])
        issuer_key = (
            _normalize_company_name_for_match(company_name, drop_suffixes=True)
            or _slug(company_name)
        )
        existing_issuer_record = issuer_records_by_key.get(issuer_key)
        if existing_issuer_record is None:
            issuer_records_by_key[issuer_key] = issuer_record

            administration_path = society_administration_dir / f"{symbol.casefold()}.json"
            try:
                administration_payload = _fetch_json_post_source(
                    _BYMA_DATA_SOCIETY_ADMINISTRATION_URL,
                    administration_path,
                    payload={"symbol": symbol},
                    user_agent=user_agent,
                )
            except httpx.HTTPStatusError as exc:
                response = exc.response
                if response is not None and response.status_code in {400, 404}:
                    administration_payload = {"data": []}
                else:
                    warning_text = str(exc)
                    if warning_text and warning_text not in people_warnings and len(people_warnings) < 5:
                        people_warnings.append(warning_text)
                    administration_payload = {"data": []}
            except Exception as exc:
                warning_text = str(exc)
                if warning_text and warning_text not in people_warnings and len(people_warnings) < 5:
                    people_warnings.append(warning_text)
                administration_payload = {"data": []}
            else:
                downloaded_sources.append(
                    {
                        "name": f"bymadata-sociedades-administracion-{symbol.casefold()}",
                        "source_url": _BYMA_DATA_SOCIETY_ADMINISTRATION_URL,
                        "path": str(administration_path.relative_to(country_dir)),
                        "category": "issuer_profile",
                        "notes": f"symbol={symbol}",
                    }
                )

            responsables_path = society_responsables_dir / f"{symbol.casefold()}.json"
            try:
                responsables_payload = _fetch_json_post_source(
                    _BYMA_DATA_SOCIETY_RESPONSIBLES_URL,
                    responsables_path,
                    payload={"symbol": symbol},
                    user_agent=user_agent,
                )
            except httpx.HTTPStatusError as exc:
                response = exc.response
                if response is not None and response.status_code in {400, 404}:
                    responsables_payload = {"data": []}
                else:
                    warning_text = str(exc)
                    if warning_text and warning_text not in people_warnings and len(people_warnings) < 5:
                        people_warnings.append(warning_text)
                    responsables_payload = {"data": []}
            except Exception as exc:
                warning_text = str(exc)
                if warning_text and warning_text not in people_warnings and len(people_warnings) < 5:
                    people_warnings.append(warning_text)
                responsables_payload = {"data": []}
            else:
                downloaded_sources.append(
                    {
                        "name": f"bymadata-sociedades-responsables-{symbol.casefold()}",
                        "source_url": _BYMA_DATA_SOCIETY_RESPONSIBLES_URL,
                        "path": str(responsables_path.relative_to(country_dir)),
                        "category": "issuer_profile",
                        "notes": f"symbol={symbol}",
                    }
                )

            people_entities.extend(
                _extract_argentina_byma_people_entities(
                    administration_payload=administration_payload,
                    responsables_payload=responsables_payload,
                    issuer_record=issuer_record,
                )
            )
            continue

        _merge_argentina_byma_issuer_record(
            issuer_record=existing_issuer_record,
            species_snapshot=species_snapshot,
            society_general_snapshot=society_general_snapshot,
            listed_company_record=listed_company_record,
        )

    for issuer_record in issuer_records_by_key.values():
        cnv_emitter_regime_record = _match_argentina_cnv_emitter_regime_record(
            issuer_record=issuer_record,
            records_by_key=cnv_emitter_regime_records_by_key,
        )
        if cnv_emitter_regime_record is not None:
            _merge_argentina_cnv_emitter_regime_record(
                issuer_record=issuer_record,
                regime_record=cnv_emitter_regime_record,
            )

    issuer_entities = [
        _build_argentina_byma_issuer_entity(record)
        for record in issuer_records_by_key.values()
    ]
    issuer_entities = _dedupe_entities(issuer_entities)
    bcra_institution_entities, bcra_people_entities, bcra_warnings = (
        _derive_argentina_bcra_institutions_and_people(
            country_dir=country_dir,
            downloaded_sources=downloaded_sources,
            issuer_entities=issuer_entities,
            user_agent=user_agent,
        )
    )
    institution_entities = _dedupe_entities(bcra_institution_entities)
    _enrich_argentina_ticker_entities_with_issuer_metadata(
        ticker_entities=ticker_entities,
        issuer_records=list(issuer_records_by_key.values()),
    )
    ticker_entities = _dedupe_entities(ticker_entities)
    _enrich_argentina_byma_people_with_issuer_metadata(
        people_entities=people_entities,
        issuer_records=list(issuer_records_by_key.values()),
    )
    people_entities = _dedupe_entities([*people_entities, *bcra_people_entities])
    for warning in bcra_warnings:
        if warning not in institution_warnings and len(institution_warnings) < 5:
            institution_warnings.append(warning)
        if warning not in people_warnings and len(people_warnings) < 5:
            people_warnings.append(warning)
    wikidata_warnings = _enrich_argentina_entities_with_wikidata(
        country_dir=country_dir,
        issuer_entities=[*issuer_entities, *institution_entities],
        ticker_entities=ticker_entities,
        people_entities=people_entities,
        downloaded_sources=downloaded_sources,
        user_agent=user_agent,
    )
    for warning in wikidata_warnings:
        if warning not in issuer_warnings and len(issuer_warnings) < 5:
            issuer_warnings.append(warning)
        if warning not in institution_warnings and len(institution_warnings) < 5:
            institution_warnings.append(warning)
        if warning not in people_warnings and len(people_warnings) < 5:
            people_warnings.append(warning)

    derived_files: list[dict[str, object]] = []
    if issuer_entities:
        issuer_path = country_dir / "derived_issuer_entities.json"
        _write_curated_entities(issuer_path, entities=issuer_entities)
        derived_files.append(
            {
                "name": "derived-finance-ar-issuers",
                "path": issuer_path.name,
                "record_count": len(issuer_entities),
                "adapter": "bymadata_public_equity_symbols",
                "warnings": issuer_warnings,
            }
        )
    if ticker_entities:
        ticker_path = country_dir / "derived_ticker_entities.json"
        _write_curated_entities(ticker_path, entities=ticker_entities)
        derived_files.append(
            {
                "name": "derived-finance-ar-tickers",
                "path": ticker_path.name,
                "record_count": len(ticker_entities),
                "adapter": "bymadata_public_equity_symbols",
                "warnings": [],
            }
        )
    if institution_entities:
        institution_path = country_dir / "derived_institution_entities.json"
        _write_curated_entities(institution_path, entities=institution_entities)
        derived_files.append(
            {
                "name": "derived-finance-ar-institutions",
                "path": institution_path.name,
                "record_count": len(institution_entities),
                "adapter": "bcra_financial_institutions",
                "warnings": institution_warnings,
            }
        )
    if people_entities:
        people_path = country_dir / "derived_people_entities.json"
        _write_curated_entities(people_path, entities=people_entities)
        derived_files.append(
            {
                "name": "derived-finance-ar-people",
                "path": people_path.name,
                "record_count": len(people_entities),
                "adapter": "bymadata_sociedades_people",
                "warnings": people_warnings,
            }
        )
    return derived_files


def _derive_argentina_bcra_institutions_and_people(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    issuer_entities: list[dict[str, object]],
    user_agent: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[str]]:
    source_entries_by_name = {
        str(source.get("name")): source for source in downloaded_sources
    }
    publication_source = source_entries_by_name.get("bcra-financial-institutions")
    if not isinstance(publication_source, dict):
        return [], [], []
    publication_path = country_dir / str(publication_source.get("path", ""))
    if not publication_path.exists():
        return [], [], []
    publication_html = publication_path.read_text(encoding="utf-8", errors="ignore")
    publication_url = str(
        publication_source.get(
            "source_url",
            _BCRA_FINANCIAL_INSTITUTIONS_PUBLICATION_URL,
        )
    )
    archive_url = _extract_bcra_financial_institutions_archive_url(
        publication_html,
        base_url=publication_url,
    )
    if not archive_url:
        latest_period_page_url = _extract_bcra_financial_institutions_period_page_url(
            publication_html,
            base_url=publication_url,
        )
        if latest_period_page_url:
            period_page_path = (
                country_dir / "bcra-financial-institutions" / "latest-period.html"
            )
            period_page_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                resolved_period_page_url = _download_source(
                    latest_period_page_url,
                    period_page_path,
                    user_agent=user_agent,
                )
            except Exception as exc:
                return [], [], [
                    f"BCRA financial-institutions latest-period page fetch failed: {exc}"
                ]
            downloaded_sources.append(
                {
                    "name": "bcra-financial-institutions-latest-period",
                    "source_url": resolved_period_page_url,
                    "path": str(period_page_path.relative_to(country_dir)),
                    "category": "institution_register",
                }
            )
            period_html = period_page_path.read_text(encoding="utf-8", errors="ignore")
            archive_url = _extract_bcra_financial_institutions_archive_url(
                period_html,
                base_url=resolved_period_page_url,
            )
    if not archive_url:
        return [], [], ["BCRA financial-institutions archive link not found in publication pages"]

    archive_name = Path(urlparse(archive_url).path).name or "bcra-financial-institutions.7z"
    archive_path = country_dir / "bcra-financial-institutions" / archive_name
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []
    try:
        resolved_archive_url = _download_source(
            archive_url,
            archive_path,
            user_agent=user_agent,
        )
    except Exception as exc:
        return [], [], [f"BCRA financial-institutions archive fetch failed: {exc}"]
    downloaded_sources.append(
        {
            "name": "bcra-financial-institutions-archive",
            "source_url": resolved_archive_url,
            "path": str(archive_path.relative_to(country_dir)),
            "category": "institution_register",
        }
    )

    extracted_dir = country_dir / "bcra-financial-institutions-extracted"
    try:
        extracted_members = _extract_bcra_7z_members(
            archive_path,
            output_dir=extracted_dir,
            members=_BCRA_FINANCIAL_INSTITUTIONS_ARCHIVE_MEMBERS,
        )
    except Exception as exc:
        return [], [], [f"BCRA financial-institutions archive extraction failed: {exc}"]
    for member_name, member_path in extracted_members.items():
        member_label = f"{Path(member_name).parent.name}-{Path(member_name).stem}"
        downloaded_sources.append(
            {
                "name": f"bcra-financial-institutions-{_slug(member_label)}",
                "source_url": resolved_archive_url,
                "path": str(member_path.relative_to(country_dir)),
                "category": "institution_register",
                "notes": f"archive_member={member_name}",
            }
        )

    institution_member = extracted_members.get("Entfin/Tec_Cont/entidad/COMPLETO.TXT")
    if institution_member is None or not institution_member.exists():
        return [], [], ["BCRA financial-institutions archive did not contain Entfin/Tec_Cont/entidad/COMPLETO.TXT"]
    institution_records = _extract_argentina_bcra_institution_records(institution_member)
    if not institution_records:
        return [], [], []

    institution_entities: list[dict[str, object]] = []
    employer_entities_by_code: dict[str, dict[str, object]] = {}
    for record in institution_records:
        matched_entity = _find_argentina_bcra_matching_organization(
            record=record,
            entities=issuer_entities,
        )
        if matched_entity is not None:
            _merge_argentina_bcra_institution_metadata(matched_entity, record)
            employer_entities_by_code[str(record.get("entity_code", ""))] = matched_entity
            continue
        entity = _build_argentina_bcra_institution_entity(record)
        institution_entities.append(entity)
        employer_entities_by_code[str(record.get("entity_code", ""))] = entity

    institution_records_by_code = {
        str(record.get("entity_code", "")): record for record in institution_records
    }
    people_entities: list[dict[str, object]] = []
    for member_name, source_name in (
        ("Entfin/Direct/COMPLETO.TXT", "bcra-financial-institutions-directors"),
        ("Entfin/Gerent/COMPLETO.TXT", "bcra-financial-institutions-managers"),
        ("Entfin/Respclie/completo.txt", "bcra-financial-institutions-customer-service"),
    ):
        member_path = extracted_members.get(member_name)
        if member_path is None or not member_path.exists():
            continue
        people_entities.extend(
            _extract_argentina_bcra_people_entities(
                rows_path=member_path,
                source_name=source_name,
                institution_records_by_code=institution_records_by_code,
                employer_entities_by_code=employer_entities_by_code,
            )
    )
    return institution_entities, _dedupe_entities(people_entities), warnings


def _extract_bcra_financial_institutions_archive_url(
    html_text: str,
    *,
    base_url: str,
) -> str | None:
    matches: list[tuple[str, str]] = []
    for match in _BCRA_FINANCIAL_INSTITUTIONS_ARCHIVE_URL_PATTERN.finditer(html_text):
        period = str(match.group("period"))
        archive_url = urljoin(base_url, match.group("url"))
        matches.append((period, archive_url))
    if not matches:
        return None
    matches.sort(key=lambda item: item[0])
    return matches[-1][1]


def _extract_bcra_financial_institutions_period_page_url(
    html_text: str,
    *,
    base_url: str,
) -> str | None:
    matches: list[tuple[str, str]] = []
    for match in _BCRA_FINANCIAL_INSTITUTIONS_PERIOD_PAGE_URL_PATTERN.finditer(html_text):
        period = str(match.group("period"))
        period_url = urljoin(base_url, match.group("url"))
        matches.append((period, period_url))
    if not matches:
        return None
    matches.sort(key=lambda item: item[0])
    return matches[-1][1]


def _extract_bcra_7z_members(
    archive_path: Path,
    *,
    output_dir: Path,
    members: tuple[str, ...],
) -> dict[str, Path]:
    bsdtar_path = shutil.which("bsdtar")
    if not bsdtar_path:
        raise RuntimeError("bsdtar is required for BCRA .7z extraction")
    output_dir.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [bsdtar_path, "-xf", str(archive_path), "-C", str(output_dir), *members],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = _normalize_whitespace(result.stderr)
        raise RuntimeError(stderr or f"bsdtar failed for {archive_path.name}")
    extracted: dict[str, Path] = {}
    for member in members:
        member_path = output_dir / Path(member)
        if member_path.exists():
            extracted[member] = member_path
    return extracted


def _load_bcra_tab_delimited_rows(path: Path) -> list[list[str]]:
    rows: list[list[str]] = []
    with path.open("r", encoding="latin-1", errors="ignore", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t", quotechar='"')
        for row in reader:
            normalized_row = [_normalize_whitespace(column) for column in row]
            if any(normalized_row):
                rows.append(normalized_row)
    return rows


def _extract_argentina_bcra_institution_records(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in _load_bcra_tab_delimited_rows(path):
        if len(row) < 45:
            continue
        entity_code = row[0]
        company_name = row[1]
        if not entity_code or not company_name:
            continue
        associations = [value for value in row[36:39] if value]
        record: dict[str, object] = {
            "entity_code": entity_code,
            "company_name": company_name,
            "reduced_name": row[2],
            "abbreviated_name": row[3],
            "cuit": row[4],
            "month_close": row[5],
            "liquidity_position_date": row[6],
            "president_name": row[27] if len(row) > 27 else "",
            "management_role_title": row[28] if len(row) > 28 else "",
            "general_manager_name": row[29] if len(row) > 29 else "",
            "street": row[30] if len(row) > 30 else "",
            "operates_in_aladi": row[31] if len(row) > 31 else "",
            "locality": row[32] if len(row) > 32 else "",
            "province": row[33] if len(row) > 33 else "",
            "phone": row[34] if len(row) > 34 else "",
            "homogeneous_group": row[35] if len(row) > 35 else "",
            "associations": associations,
            "institution_group": row[39] if len(row) > 39 else "",
            "head_office_type": row[40] if len(row) > 40 else "",
            "institutional_group_code": row[41] if len(row) > 41 else "",
            "website": row[42] if len(row) > 42 else "",
            "social_media": row[43] if len(row) > 43 else "",
            "email": row[44] if len(row) > 44 else "",
        }
        records.append(record)
    return records


def _find_argentina_bcra_matching_organization(
    *,
    record: dict[str, object],
    entities: list[dict[str, object]],
) -> dict[str, object] | None:
    record_cuit = _normalize_argentina_cuit_key(str(record.get("cuit", "")))
    if record_cuit:
        for entity in entities:
            metadata = entity.get("metadata")
            if not isinstance(metadata, dict):
                continue
            entity_cuit = _normalize_argentina_cuit_key(
                str(metadata.get("cuit") or metadata.get("issuer_cuit") or "")
            )
            if entity_cuit and entity_cuit == record_cuit:
                return entity
    candidate_keys = {
        normalized
        for normalized in (
            _normalize_company_name_for_match(
                str(record.get("company_name", "")),
                drop_suffixes=True,
            ),
            _normalize_company_name_for_match(
                str(record.get("reduced_name", "")),
                drop_suffixes=True,
            ),
            _normalize_company_name_for_match(
                str(record.get("abbreviated_name", "")),
                drop_suffixes=True,
            ),
        )
        if normalized
    }
    if not candidate_keys:
        return None
    for entity in entities:
        candidate_texts = [
            str(entity.get("canonical_text", "")),
            *[
                alias
                for alias in entity.get("aliases", [])
                if isinstance(alias, str)
            ],
        ]
        for candidate_text in candidate_texts:
            normalized = _normalize_company_name_for_match(
                candidate_text,
                drop_suffixes=True,
            )
            if normalized and normalized in candidate_keys:
                return entity
    return None


def _build_argentina_bcra_institution_entity(
    record: dict[str, object],
) -> dict[str, object]:
    canonical_text = _normalize_whitespace(str(record.get("company_name", "")))
    entity_code = _normalize_whitespace(str(record.get("entity_code", "")))
    metadata: dict[str, object] = {
        "country_code": "ar",
        "category": "regulated_institution",
        "regulated_institution": True,
        "bcra_entity_codes": _unique_aliases([entity_code]),
        "institution_group": _normalize_whitespace(
            str(record.get("institution_group", ""))
        ),
        "head_office_type": _normalize_whitespace(
            str(record.get("head_office_type", ""))
        ),
        "institutional_group_code": _normalize_whitespace(
            str(record.get("institutional_group_code", ""))
        ),
        "associations": [
            value
            for value in record.get("associations", [])
            if isinstance(value, str)
        ],
        "bcra_records": [_build_argentina_bcra_record_summary(record)],
        "bcra_record_count": 1,
    }
    for key, record_key in (
        ("cuit", "cuit"),
        ("month_close", "month_close"),
        ("street", "street"),
        ("locality", "locality"),
        ("province", "province"),
        ("phone", "phone"),
        ("website", "website"),
        ("email", "email"),
        ("social_media", "social_media"),
        ("homogeneous_group", "homogeneous_group"),
        ("operates_in_aladi", "operates_in_aladi"),
    ):
        value = _normalize_whitespace(str(record.get(record_key, "")))
        if value:
            metadata[key] = value
    aliases = _build_argentina_company_aliases(
        canonical_text,
        str(record.get("reduced_name", "")),
        str(record.get("abbreviated_name", "")),
    )
    return {
        "entity_type": "organization",
        "canonical_text": canonical_text,
        "aliases": aliases,
        "entity_id": f"finance-ar-institution:bcra:{entity_code or _slug(canonical_text)}",
        "metadata": metadata,
    }


def _merge_argentina_bcra_institution_metadata(
    entity: dict[str, object],
    record: dict[str, object],
) -> None:
    aliases = [
        alias for alias in entity.get("aliases", []) if isinstance(alias, str)
    ]
    entity["aliases"] = _unique_aliases(
        [
            *aliases,
            *_build_argentina_company_aliases(
                str(record.get("company_name", "")),
                str(record.get("reduced_name", "")),
                str(record.get("abbreviated_name", "")),
            ),
        ]
    )
    metadata = entity.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        entity["metadata"] = metadata
    metadata["regulated_institution"] = True
    metadata["category"] = "regulated_institution"
    metadata["bcra_entity_codes"] = _unique_aliases(
        [
            *[
                value
                for value in metadata.get("bcra_entity_codes", [])
                if isinstance(value, str)
            ],
            str(record.get("entity_code", "")),
        ]
    )
    metadata["bcra_records"] = [
        *[
            item
            for item in metadata.get("bcra_records", [])
            if isinstance(item, dict)
        ],
        _build_argentina_bcra_record_summary(record),
    ]
    metadata["bcra_record_count"] = len(metadata["bcra_records"])
    metadata["associations"] = _unique_aliases(
        [
            *[
                value
                for value in metadata.get("associations", [])
                if isinstance(value, str)
            ],
            *[
                value
                for value in record.get("associations", [])
                if isinstance(value, str)
            ],
        ]
    )
    for key, record_key in (
        ("cuit", "cuit"),
        ("institution_group", "institution_group"),
        ("head_office_type", "head_office_type"),
        ("institutional_group_code", "institutional_group_code"),
        ("month_close", "month_close"),
        ("street", "street"),
        ("locality", "locality"),
        ("province", "province"),
        ("phone", "phone"),
        ("website", "website"),
        ("email", "email"),
        ("social_media", "social_media"),
        ("homogeneous_group", "homogeneous_group"),
        ("operates_in_aladi", "operates_in_aladi"),
    ):
        value = _normalize_whitespace(str(record.get(record_key, "")))
        if value and not metadata.get(key):
            metadata[key] = value


def _build_argentina_bcra_record_summary(
    record: dict[str, object],
) -> dict[str, object]:
    return {
        "entity_code": str(record.get("entity_code", "")),
        "company_name": str(record.get("company_name", "")),
        "reduced_name": str(record.get("reduced_name", "")),
        "abbreviated_name": str(record.get("abbreviated_name", "")),
        "cuit": str(record.get("cuit", "")),
        "institution_group": str(record.get("institution_group", "")),
        "head_office_type": str(record.get("head_office_type", "")),
        "institutional_group_code": str(record.get("institutional_group_code", "")),
        "website": str(record.get("website", "")),
        "email": str(record.get("email", "")),
    }


def _extract_argentina_bcra_people_entities(
    *,
    rows_path: Path,
    source_name: str,
    institution_records_by_code: dict[str, dict[str, object]],
    employer_entities_by_code: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    entities: list[dict[str, object]] = []
    for row in _load_bcra_tab_delimited_rows(rows_path):
        if source_name == "bcra-financial-institutions-customer-service":
            if len(row) < 8:
                continue
            entity_code = row[0]
            raw_name = row[3]
            role_title = row[4]
            email = row[5]
            address = row[6]
            phone = row[7]
        else:
            if len(row) < 6:
                continue
            entity_code = row[0]
            raw_name = row[3]
            role_title = row[5]
            email = ""
            address = ""
            phone = ""
        institution_record = institution_records_by_code.get(entity_code)
        if institution_record is None:
            continue
        canonical_name, aliases = _normalize_argentina_byma_person_name_aliases(raw_name)
        if not canonical_name:
            continue
        employer_entity = employer_entities_by_code.get(entity_code)
        employer_name = _normalize_whitespace(
            str(institution_record.get("company_name", ""))
        )
        employer_ticker = ""
        if isinstance(employer_entity, dict):
            metadata = employer_entity.get("metadata")
            if isinstance(metadata, dict):
                employer_ticker = _normalize_whitespace(str(metadata.get("ticker", "")))
        role_class = _role_class_for_argentina_bcra_role(
            role_title,
            source_name=source_name,
        )
        metadata = {
            "country_code": "ar",
            "category": role_class,
            "role_title": role_title,
            "employer_name": employer_name,
            "employer_ticker": employer_ticker,
            "bcra_entity_code": entity_code,
            "source_name": source_name,
        }
        cuit = _normalize_whitespace(str(institution_record.get("cuit", "")))
        if cuit:
            metadata["employer_cuit"] = cuit
        institution_group = _normalize_whitespace(
            str(institution_record.get("institution_group", ""))
        )
        if institution_group:
            metadata["institution_group"] = institution_group
        if email:
            metadata["contact_email"] = email
        if address:
            metadata["contact_address"] = address
        if phone:
            metadata["contact_phone"] = phone
        entities.append(
            {
                "entity_type": "person",
                "canonical_text": canonical_name,
                "aliases": _unique_aliases(aliases),
                "entity_id": (
                    f"finance-ar-person:bcra:{entity_code}:"
                    f"{_slug(canonical_name)}:{_slug(role_class)}"
                ),
                "metadata": metadata,
            }
        )
    return entities


def _role_class_for_argentina_bcra_role(
    role_title: str,
    *,
    source_name: str,
) -> str:
    if source_name == "bcra-financial-institutions-customer-service":
        return "customer_service_officer"
    normalized = _normalize_whitespace(_ascii_fold_alias(role_title) or role_title).casefold()
    if source_name == "bcra-financial-institutions-directors":
        return "director"
    if any(keyword in normalized for keyword in ("gerente", "subgerente", "respons", "jefe")):
        return "executive_officer"
    return "personnel"


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


def _derive_japan_jpx_entities(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[dict[str, object]]:
    issuer_warnings: list[str] = []
    people_warnings: list[str] = []
    source_info = {
        str(source.get("name")): source for source in downloaded_sources if source.get("name")
    }
    source_paths = {
        str(source.get("name")): country_dir / str(source.get("path"))
        for source in downloaded_sources
    }
    availability_source = source_info.get("jpx-english-disclosure-availability")
    availability_path = source_paths.get("jpx-english-disclosure-availability")
    listed_company_records = _collect_japan_listed_company_search_records(
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent=user_agent,
        warnings=issuer_warnings,
        allow_live_fetch=bool(
            isinstance(source_info.get("jpx-listed-company-search"), dict)
            and str(source_info["jpx-listed-company-search"].get("source_url", "")).startswith(
                ("http://", "https://")
            )
        ),
    )
    workbook_path: Path | None = None
    if availability_path is not None and availability_path.exists():
        if zipfile.is_zipfile(availability_path):
            workbook_path = availability_path
        else:
            availability_url = str(
                availability_source.get("source_url", _JPX_ENGLISH_DISCLOSURE_AVAILABILITY_URL)
            ) if isinstance(availability_source, dict) else _JPX_ENGLISH_DISCLOSURE_AVAILABILITY_URL
            workbook_url = _extract_jpx_english_disclosure_workbook_url(
                availability_path.read_text(encoding="utf-8", errors="ignore"),
                base_url=availability_url,
            )
            if workbook_url:
                workbook_path = country_dir / "jpx-english-disclosure-availability.xlsx"
                try:
                    resolved_workbook_url = _download_source(
                        workbook_url,
                        workbook_path,
                        user_agent=user_agent,
                    )
                except Exception as exc:
                    issuer_warnings.append(f"JPX workbook download failed: {exc}")
                    workbook_path = None
                else:
                    downloaded_sources.append(
                        {
                            "name": "jpx-english-disclosure-availability-workbook",
                            "source_url": resolved_workbook_url,
                            "path": workbook_path.name,
                            "category": "issuer_directory",
                            "notes": "Resolved current monthly workbook downloaded from the JPX English disclosure availability page.",
                        }
                    )
            else:
                issuer_warnings.append(
                    "JPX availability page did not expose a downloadable workbook URL"
                )
    workbook_records: list[dict[str, str]] = []
    if workbook_path is not None and workbook_path.exists():
        workbook_records = _extract_japan_jpx_english_disclosure_records(workbook_path)
    governance_records = _collect_japan_corporate_governance_report_records(
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent=user_agent,
        warnings=people_warnings,
        allow_live_fetch=bool(
            isinstance(source_info.get("jpx-corporate-governance-search"), dict)
            and str(
                source_info["jpx-corporate-governance-search"].get("source_url", "")
            ).startswith(("http://", "https://"))
        ),
    )
    listed_company_detail_records = _collect_japan_listed_company_detail_records(
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent=user_agent,
        warnings=issuer_warnings,
        allow_live_fetch=bool(
            isinstance(source_info.get("jpx-listed-company-search"), dict)
            and str(source_info["jpx-listed-company-search"].get("source_url", "")).startswith(
                ("http://", "https://")
            )
        ),
        candidate_records=_build_japan_listed_company_detail_fetch_candidates(
            listed_company_records=listed_company_records,
            workbook_records=workbook_records,
            governance_records=governance_records,
        ),
    )
    issuer_records = _merge_japan_issuer_seed_records(
        listed_company_records=listed_company_records,
        listed_company_detail_records=listed_company_detail_records,
        workbook_records=workbook_records,
    )
    if not issuer_records:
        if workbook_path is not None and workbook_path.exists():
            issuer_warnings.append("JPX workbook yielded no issuer records")
        return []

    issuer_entities = _dedupe_entities(
        [
            entity
            for record in issuer_records
            if (entity := _build_japan_jpx_issuer_entity(record)) is not None
        ]
    )
    ticker_entities = _dedupe_entities(
        [
            entity
            for record in issuer_records
            if (entity := _build_japan_jpx_ticker_entity(record)) is not None
        ]
    )
    issuer_record_by_code = {
        str(record["code"]).upper(): record
        for record in issuer_records
        if record.get("code")
    }
    detail_page_people_entities = _extract_japan_listed_company_detail_people_entities(
        detail_records=listed_company_detail_records,
        issuer_records_by_code=issuer_record_by_code,
    )
    issuer_entity_by_code = {
        _normalize_whitespace(str(entity.get("metadata", {}).get("ticker", ""))).upper(): entity
        for entity in issuer_entities
        if isinstance(entity.get("metadata"), dict)
    }
    ticker_entity_by_code = {
        _normalize_whitespace(str(entity.get("canonical_text", ""))).upper(): entity
        for entity in ticker_entities
    }
    if governance_records:
        people_entities = _extract_japan_corporate_governance_people_entities(
            governance_records=governance_records,
            issuer_records_by_code=issuer_record_by_code,
            country_dir=country_dir,
            downloaded_sources=downloaded_sources,
            user_agent=user_agent,
            warnings=people_warnings,
        )
        latest_governance_records = _select_latest_japan_governance_report_records(
            governance_records
        )
        for code, governance_record in latest_governance_records.items():
            issuer_entity = issuer_entity_by_code.get(code)
            if issuer_entity is not None:
                _merge_japan_governance_report_metadata(
                    entity=issuer_entity,
                    governance_record=governance_record,
                )
            ticker_entity = ticker_entity_by_code.get(code)
            if ticker_entity is not None:
                _merge_japan_governance_report_metadata(
                    entity=ticker_entity,
                    governance_record=governance_record,
                )
    else:
        people_entities = []
    people_entities.extend(detail_page_people_entities)

    derived_files: list[dict[str, object]] = []
    if issuer_entities:
        issuer_path = country_dir / "derived_issuer_entities.json"
        _write_curated_entities(issuer_path, entities=issuer_entities)
        derived_files.append(
            {
                "name": "derived-finance-jp-issuers",
                "path": issuer_path.name,
                "record_count": len(_load_curated_entities(issuer_path)),
                "adapter": "jpx_listed_company_search_plus_workbook",
                "warnings": issuer_warnings,
            }
        )
    if ticker_entities:
        ticker_path = country_dir / "derived_ticker_entities.json"
        _write_curated_entities(ticker_path, entities=ticker_entities)
        derived_files.append(
            {
                "name": "derived-finance-jp-tickers",
                "path": ticker_path.name,
                "record_count": len(_load_curated_entities(ticker_path)),
                "adapter": "jpx_listed_company_search_plus_workbook",
                "warnings": issuer_warnings,
            }
        )
    if people_entities:
        people_path = country_dir / "derived_people_entities.json"
        _write_curated_entities(people_path, entities=_dedupe_entities(people_entities))
        derived_files.append(
            {
                "name": "derived-finance-jp-people",
                "path": people_path.name,
                "record_count": len(_load_curated_entities(people_path)),
                "adapter": "jpx_corporate_governance_reports_plus_detail_pages",
                "warnings": people_warnings,
            }
        )
    return derived_files


def _derive_france_euronext_entities(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[dict[str, object]]:
    issuer_warnings: list[str] = []
    source_info = {
        str(source.get("name")): source for source in downloaded_sources if source.get("name")
    }
    source_paths = {
        str(source.get("name")): country_dir / str(source.get("path"))
        for source in downloaded_sources
    }
    issuer_records: list[dict[str, str]] = []
    for source_name, listing_segment in _FRANCE_EURONEXT_SOURCE_SEGMENTS.items():
        source_path = source_paths.get(source_name)
        if source_path is None or not source_path.exists():
            continue
        issuer_records.extend(
            _extract_france_euronext_directory_records(
                source_path.read_text(encoding="utf-8", errors="ignore"),
                listing_segment=listing_segment,
            )
        )
    merged_records = _merge_france_euronext_records(issuer_records)
    issuer_lookup = _build_france_issuer_lookup(merged_records)
    issuer_records_by_isin = {
        str(record["isin"]).upper(): record for record in merged_records if record.get("isin")
    }
    issuer_entities = [
        entity
        for entity in (
            _build_france_euronext_issuer_entity(record) for record in merged_records
        )
        if entity is not None
    ]
    ticker_entities = [
        entity
        for entity in (
            _build_france_euronext_ticker_entity(record) for record in merged_records
        )
        if entity is not None
    ]
    people_entities: list[dict[str, object]] = []
    people_warnings: list[str] = []

    # Parse any already-downloaded issuer profile pages first so local fixtures can
    # exercise the people lane without triggering live network fetches.
    for source in downloaded_sources:
        if str(source.get("category")) != "issuer_profile":
            continue
        source_path = country_dir / str(source.get("path"))
        if not source_path.exists():
            continue
        source_name = str(source.get("name", "issuer-profile"))
        people_entities.extend(
            _extract_france_governance_people_entities(
                source_path.read_text(encoding="utf-8", errors="ignore"),
                issuer_lookup=issuer_lookup,
                source_name=source_name,
            )
        )

    company_news_records: list[dict[str, str]] = []
    company_news_latest_path = source_paths.get("euronext-paris-company-news")
    if company_news_latest_path is not None and company_news_latest_path.exists():
        company_news_records.extend(
            _extract_france_company_news_records(
                company_news_latest_path.read_text(encoding="utf-8", errors="ignore")
            )
        )
    company_news_archive_path = source_paths.get("euronext-paris-company-news-archive")
    if company_news_archive_path is not None and company_news_archive_path.exists():
        archive_html = company_news_archive_path.read_text(encoding="utf-8", errors="ignore")
        company_news_records.extend(_extract_france_company_news_records(archive_html))
        archive_source_url = str(
            source_info.get("euronext-paris-company-news-archive", {}).get("source_url", "")
        )
        if archive_source_url.startswith(("http://", "https://")):
            archive_page_count = min(
                _extract_france_company_news_last_page(archive_html),
                _FRANCE_COMPANY_NEWS_FETCH_MAX_PAGES,
            )
            if archive_page_count > 1:
                archive_pages_dir = country_dir / "euronext-paris-company-news-archive"
                archive_pages_dir.mkdir(parents=True, exist_ok=True)
                target_issuer_count = len(issuer_records_by_isin)
                for page_number in range(1, archive_page_count):
                    page_url = f"{archive_source_url}?page={page_number}"
                    page_path = archive_pages_dir / f"page-{page_number}.html"
                    try:
                        _download_france_company_news_page(
                            page_url,
                            page_path,
                            user_agent=user_agent,
                        )
                    except Exception as exc:
                        warning_text = str(exc)
                        if (
                            warning_text
                            and warning_text not in people_warnings
                            and len(people_warnings) < 5
                        ):
                            people_warnings.append(warning_text)
                        continue
                    downloaded_sources.append(
                        {
                            "name": f"euronext-paris-company-news-archive-page-{page_number}",
                            "source_url": page_url,
                            "path": str(page_path.relative_to(country_dir)),
                            "category": "filing_system",
                            "notes": f"page={page_number}",
                        }
                    )
                    company_news_records.extend(
                        _extract_france_company_news_records(
                            page_path.read_text(encoding="utf-8", errors="ignore")
                        )
                    )
                    if target_issuer_count and (
                        len(
                            _select_france_company_news_candidate_releases(
                                company_news_records,
                                issuer_lookup=issuer_lookup,
                            )
                        )
                        >= target_issuer_count
                    ):
                        break

    company_news_latest_source_url = str(
        source_info.get("euronext-paris-company-news", {}).get("source_url", "")
    )
    company_news_archive_source_url = str(
        source_info.get("euronext-paris-company-news-archive", {}).get("source_url", "")
    )
    remote_company_news_available = any(
        source_url.startswith(("http://", "https://"))
        for source_url in (
            company_news_latest_source_url,
            company_news_archive_source_url,
        )
    )
    if remote_company_news_available and company_news_records:
        modal_dir = country_dir / "euronext-company-press-release-modals"
        filing_document_dir = country_dir / "france-filing-documents"
        governance_seed_dir = country_dir / "france-governance-seeds"
        governance_page_dir = country_dir / "france-governance-pages"
        modal_dir.mkdir(parents=True, exist_ok=True)
        filing_document_dir.mkdir(parents=True, exist_ok=True)
        governance_seed_dir.mkdir(parents=True, exist_ok=True)
        governance_page_dir.mkdir(parents=True, exist_ok=True)
        seen_filing_urls: set[str] = set()
        seen_governance_urls: set[str] = set()
        for record in _select_france_company_news_candidate_releases(
            company_news_records,
            issuer_lookup=issuer_lookup,
        ):
            node_nid = str(record.get("node_nid", "")).strip()
            if not node_nid:
                continue
            modal_url = _FRANCE_COMPANY_PRESS_RELEASE_MODAL_URL_TEMPLATE.format(
                node_nid=node_nid
            )
            modal_path = modal_dir / f"{node_nid}.html"
            try:
                _download_france_company_press_release_modal(
                    node_nid,
                    modal_path,
                    user_agent=user_agent,
                )
            except Exception as exc:
                warning_text = str(exc)
                if (
                    warning_text
                    and warning_text not in people_warnings
                    and len(people_warnings) < 5
                ):
                    people_warnings.append(warning_text)
                continue
            modal_source_name = f"euronext-company-press-release-{node_nid}"
            downloaded_sources.append(
                {
                    "name": modal_source_name,
                    "source_url": modal_url,
                    "path": str(modal_path.relative_to(country_dir)),
                    "category": "issuer_profile",
                    "notes": f"node_nid={node_nid}",
                }
            )
            modal_html = modal_path.read_text(encoding="utf-8", errors="ignore")
            issuer_record = _resolve_france_press_release_issuer_record(
                modal_html,
                record=record,
                issuer_lookup=issuer_lookup,
                issuer_records_by_isin=issuer_records_by_isin,
            )
            people_entities.extend(
                _extract_france_company_press_release_people_entities(
                    modal_html,
                    issuer_record=issuer_record,
                    source_name=modal_source_name,
                )
            )
            filing_urls = _extract_france_filing_candidate_urls(
                modal_html,
                base_url=modal_url,
            )
            seed_urls = _extract_france_company_press_release_candidate_urls(
                modal_html,
                base_url=modal_url,
            )
            governance_urls: list[str] = []
            for seed_url in seed_urls:
                if any(keyword in seed_url.casefold() for keyword in _FRANCE_GOVERNANCE_URL_KEYWORDS):
                    governance_urls.append(seed_url)
                    continue
                seed_path = governance_seed_dir / (
                    f"{_stable_url_file_stem(seed_url)}.html"
                )
                try:
                    _download_france_governance_page(
                        seed_url,
                        seed_path,
                        user_agent=user_agent,
                    )
                except Exception:
                    continue
                downloaded_sources.append(
                    {
                        "name": f"france-governance-seed-{_slug(seed_url)}",
                        "source_url": seed_url,
                        "path": str(seed_path.relative_to(country_dir)),
                        "category": "issuer_profile",
                        "notes": f"issuer={str(record.get('issuer_name', ''))}",
                    }
                )
                seed_html = seed_path.read_text(encoding="utf-8", errors="ignore")
                filing_urls.extend(
                    _extract_france_filing_candidate_urls(
                        seed_html,
                        base_url=seed_url,
                    )
                )
                governance_urls.extend(
                    _extract_france_governance_candidate_urls(
                        seed_html,
                        base_url=seed_url,
                    )
                )
            selected_filing_urls: list[str] = []
            for filing_url in filing_urls:
                normalized_filing_url = filing_url.strip()
                if (
                    not normalized_filing_url
                    or normalized_filing_url in seen_filing_urls
                ):
                    continue
                seen_filing_urls.add(normalized_filing_url)
                selected_filing_urls.append(normalized_filing_url)
                if len(selected_filing_urls) >= _FRANCE_MAX_FILING_URLS_PER_ISSUER:
                    break
            for filing_url in selected_filing_urls:
                filing_suffix = Path(urlparse(filing_url).path).suffix.lower()
                if filing_suffix not in {".html", ".htm", ".xhtml", ".xml"}:
                    filing_suffix = ".html"
                filing_path = filing_document_dir / (
                    f"{_stable_url_file_stem(filing_url)}{filing_suffix}"
                )
                try:
                    _download_france_filing_document(
                        filing_url,
                        filing_path,
                        user_agent=user_agent,
                    )
                except Exception as exc:
                    warning_text = str(exc)
                    if (
                        warning_text
                        and warning_text not in people_warnings
                        and len(people_warnings) < 5
                    ):
                        people_warnings.append(warning_text)
                    continue
                filing_source_name = (
                    f"france-filing-document-{_slug(_stable_url_file_stem(filing_url))}"
                )
                downloaded_sources.append(
                    {
                        "name": filing_source_name,
                        "source_url": filing_url,
                        "path": str(filing_path.relative_to(country_dir)),
                        "category": "filing_system",
                        "notes": (
                            f"issuer={str(record.get('issuer_name', ''))}; "
                            "official filing document"
                        ),
                    }
                )
                filing_html = filing_path.read_text(encoding="utf-8", errors="ignore")
                people_entities.extend(
                    _extract_france_filing_people_entities(
                        filing_html,
                        issuer_lookup=issuer_lookup,
                        issuer_record=issuer_record,
                        source_name=filing_source_name,
                    )
                )
                governance_urls.extend(
                    url
                    for url in _extract_france_governance_candidate_urls(
                        filing_html,
                        base_url=filing_url,
                    )
                    if url.rstrip("/") != filing_url.rstrip("/")
                )
            selected_governance_urls: list[str] = []
            for governance_url in governance_urls:
                normalized_governance_url = governance_url.strip()
                if (
                    not normalized_governance_url
                    or normalized_governance_url in seen_governance_urls
                ):
                    continue
                seen_governance_urls.add(normalized_governance_url)
                selected_governance_urls.append(normalized_governance_url)
                if len(selected_governance_urls) >= _FRANCE_MAX_GOVERNANCE_URLS_PER_ISSUER:
                    break
            for governance_url in selected_governance_urls:
                governance_path = governance_page_dir / (
                    f"{_stable_url_file_stem(governance_url)}.html"
                )
                try:
                    _download_france_governance_page(
                        governance_url,
                        governance_path,
                        user_agent=user_agent,
                    )
                except Exception as exc:
                    warning_text = str(exc)
                    if (
                        warning_text
                        and warning_text not in people_warnings
                        and len(people_warnings) < 5
                    ):
                        people_warnings.append(warning_text)
                    continue
                governance_source_name = (
                    f"france-governance-page-{_slug(governance_url)}"
                )
                downloaded_sources.append(
                    {
                        "name": governance_source_name,
                        "source_url": governance_url,
                        "path": str(governance_path.relative_to(country_dir)),
                        "category": "issuer_profile",
                        "notes": f"issuer={str(record.get('issuer_name', ''))}",
                    }
                )
                people_entities.extend(
                    _extract_france_governance_people_entities(
                        governance_path.read_text(encoding="utf-8", errors="ignore"),
                        issuer_lookup=issuer_lookup,
                        issuer_record=issuer_record,
                        source_name=governance_source_name,
                    )
                )

    issuer_entities = _dedupe_entities(issuer_entities)
    ticker_entities = _dedupe_entities(ticker_entities)
    people_entities = _dedupe_entities(people_entities)
    wikidata_warnings = _enrich_france_entities_with_wikidata(
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
                "name": "derived-finance-fr-issuers",
                "path": issuer_path.name,
                "record_count": len(issuer_entities),
                "adapter": "euronext_paris_directory",
                "warnings": issuer_warnings,
            }
        )
    if ticker_entities:
        ticker_path = country_dir / "derived_ticker_entities.json"
        _write_curated_entities(ticker_path, entities=ticker_entities)
        derived_files.append(
            {
                "name": "derived-finance-fr-tickers",
                "path": ticker_path.name,
                "record_count": len(ticker_entities),
                "adapter": "euronext_paris_directory",
                "warnings": [],
            }
        )
    if people_entities:
        people_path = country_dir / "derived_people_entities.json"
        _write_curated_entities(people_path, entities=people_entities)
        derived_files.append(
            {
                "name": "derived-finance-fr-people",
                "path": people_path.name,
                "record_count": len(people_entities),
                "adapter": "euronext_company_news_filing_documents_and_issuer_governance_pages",
                "warnings": people_warnings,
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
    shareholder_organization_records: list[dict[str, str]] = []
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
        contact_details = _extract_deutsche_boerse_company_details_contact_details(
            company_details_html
        )
        shareholder_records = _extract_deutsche_boerse_company_details_shareholder_records(
            company_details_html
        )
        if issuer_entity is not None:
            _merge_deutsche_boerse_company_details_metadata(
                entity=issuer_entity,
                contact_details=contact_details,
                shareholder_records=shareholder_records,
            )
        if ticker_entity is not None:
            _merge_deutsche_boerse_company_details_metadata(
                entity=ticker_entity,
                contact_details=contact_details,
                shareholder_records=shareholder_records,
            )
        people_entities.extend(
            _extract_deutsche_boerse_company_details_people_entities(
                company_details_html,
                issuer_record=record,
            )
        )
        people_entities.extend(
            _extract_deutsche_boerse_company_details_contact_people_entities(
                company_details_html,
                issuer_record=record,
            )
        )
        people_entities.extend(
            _extract_deutsche_boerse_company_details_shareholder_people_entities(
                company_details_html,
                issuer_record=record,
            )
        )
        shareholder_organization_records.extend(
            _extract_deutsche_boerse_company_details_shareholder_organization_records(
                company_details_html,
                issuer_record=record,
            )
        )

    issuer_entities.extend(
        _merge_germany_bafin_institutions(
        bafin_csv_path=bafin_company_database_path,
        issuer_entities=issuer_entities,
        ticker_entities=ticker_entities,
        warnings=issuer_warnings,
        )
    )
    issuer_entities.extend(
        _build_germany_major_shareholder_organization_entities(
            shareholder_records=shareholder_organization_records,
            organization_lookup=_build_germany_organization_entity_lookup(issuer_entities),
        )
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
) -> list[dict[str, object]]:
    branch_entities: list[dict[str, object]] = []
    if bafin_csv_path is None or not bafin_csv_path.exists():
        return branch_entities
    try:
        bafin_records = _extract_bafin_company_database_records(bafin_csv_path)
    except Exception as exc:
        warning_text = f"Failed to parse BaFin company database export: {exc}"
        if warning_text not in warnings and len(warnings) < 5:
            warnings.append(warning_text)
        return branch_entities
    if not bafin_records:
        return branch_entities
    issuer_lookup = _build_germany_company_entity_lookup(issuer_entities)
    ticker_lookup = {
        _normalize_whitespace(str(entity.get("canonical_text", ""))).upper(): entity
        for entity in ticker_entities
    }
    for record in bafin_records:
        parent_entity: dict[str, object] | None = None
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
            parent_entity = matched_entity
        else:
            institution_entity = _build_germany_bafin_institution_entity(record)
            issuer_entities.append(institution_entity)
            _register_germany_company_entity_aliases(issuer_lookup, institution_entity)
            parent_entity = institution_entity
        branch_entity = _build_germany_bafin_branch_entity(
            record=record,
            parent_entity=parent_entity,
        )
        if branch_entity is not None:
            branch_entities.append(branch_entity)
    return branch_entities


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
    country = _normalize_whitespace(str(record.get("country", "")))
    if country and not metadata.get("country"):
        metadata["country"] = country
    ombudsman = _normalize_whitespace(str(record.get("ombudsman", "")))
    if ombudsman and not metadata.get("ombudsman"):
        metadata["ombudsman"] = ombudsman
    germany_branch = _normalize_whitespace(str(record.get("germany_branch", "")))
    if germany_branch:
        metadata["germany_branch"] = germany_branch
    consumer_contacts = _normalize_whitespace(
        str(record.get("consumer_complaint_contacts", ""))
    )
    if consumer_contacts:
        metadata["consumer_complaint_contacts"] = consumer_contacts
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
    metadata["cross_border_credit_service_countries"] = _unique_aliases(
        [
            *[
                value
                for value in metadata.get("cross_border_credit_service_countries", [])
                if isinstance(value, str)
            ],
            *[
                value
                for value in record.get("cross_border_credit_service_countries", [])
                if isinstance(value, str)
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


def _build_germany_bafin_branch_entity(
    *,
    record: dict[str, object],
    parent_entity: dict[str, object] | None,
) -> dict[str, object] | None:
    branch_name = _normalize_whitespace(str(record.get("germany_branch", "")))
    if not branch_name:
        return None
    parent_name = _normalize_whitespace(
        str((parent_entity or {}).get("canonical_text", ""))
    ) or _normalize_whitespace(str(record.get("company_name", "")))
    parent_entity_id = _normalize_whitespace(str((parent_entity or {}).get("entity_id", "")))
    identifier = (
        _normalize_whitespace(str(record.get("bak_nr", "")))
        or _normalize_whitespace(str(record.get("bafin_id", "")))
        or _slug(parent_name or branch_name)
    )
    metadata: dict[str, object] = {
        "country_code": "de",
        "category": "regulated_institution_branch",
        "regulated_institution": True,
        "regulated_institution_branch": True,
        "parent_institution_name": parent_name,
        "branch_name": branch_name,
        "postal_code": _normalize_whitespace(str(record.get("postal_code", ""))),
        "city": _normalize_whitespace(str(record.get("city", ""))),
        "street": _normalize_whitespace(str(record.get("street", ""))),
        "country": "Deutschland",
        "institution_type": _normalize_whitespace(
            str(record.get("institution_type", ""))
        ),
        "institution_types": [
            value
            for value in record.get("institution_types", [])
            if isinstance(value, str)
        ],
        "ombudsman": _normalize_whitespace(str(record.get("ombudsman", ""))),
        "consumer_complaint_contacts": _normalize_whitespace(
            str(record.get("consumer_complaint_contacts", ""))
        ),
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
        "cross_border_credit_service_countries": [
            value
            for value in record.get("cross_border_credit_service_countries", [])
            if isinstance(value, str)
        ],
        "bafin_records": [_build_bafin_company_database_record_summary(record)],
        "bafin_record_count": 1,
    }
    home_country = _normalize_whitespace(str(record.get("country", "")))
    if home_country and home_country.casefold() != "deutschland":
        metadata["home_country"] = home_country
    if parent_entity_id:
        metadata["parent_institution_entity_id"] = parent_entity_id
    aliases = _build_germany_company_aliases(branch_name)
    return {
        "entity_type": "organization",
        "canonical_text": branch_name,
        "aliases": aliases,
        "entity_id": f"finance-de-institution-branch:bafin:{identifier}:{_slug(branch_name)}",
        "metadata": metadata,
    }


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
    source_info = {
        str(source.get("name")): source for source in downloaded_sources if source.get("name")
    }
    source_paths = {
        str(source.get("name")): country_dir / str(source.get("path"))
        for source in downloaded_sources
    }
    downloaded_source_names = {
        str(source.get("name")) for source in downloaded_sources if source.get("name")
    }

    def _append_downloaded_source(
        *,
        name: str,
        source_url: str,
        path: Path,
        category: str,
        notes: str = "",
    ) -> None:
        if name in downloaded_source_names:
            return
        downloaded_sources.append(
            {
                "name": name,
                "source_url": source_url,
                "path": str(path.relative_to(country_dir)),
                "category": category,
                "notes": notes,
            }
        )
        downloaded_source_names.add(name)

    issuer_records: list[dict[str, str]] = []
    official_list_path = source_paths.get("fca-official-list-live")
    if official_list_path is not None and official_list_path.exists():
        issuer_records.extend(
            _extract_uk_official_list_commercial_company_records(official_list_path)
        )

    aim_source_name = "lse-aim-all-share-constituents"
    aim_path = source_paths.get(aim_source_name)
    if aim_path is not None and aim_path.exists():
        aim_html = aim_path.read_text(encoding="utf-8", errors="ignore")
        issuer_records.extend(_extract_uk_aim_constituent_records(aim_html))
        aim_source_url = str(source_info.get(aim_source_name, {}).get("source_url", ""))
        if aim_source_url.startswith(("http://", "https://")):
            aim_dir = country_dir / "lse-aim-all-share-constituents"
            aim_dir.mkdir(parents=True, exist_ok=True)
            last_page = _extract_uk_lse_directory_last_page(aim_html)
            for page_number in range(2, last_page + 1):
                page_url = f"{aim_source_url}?page={page_number}"
                page_path = aim_dir / f"page-{page_number}.html"
                if not page_path.exists():
                    try:
                        _download_london_stock_exchange_directory_page(
                            page_url,
                            page_path,
                            user_agent=user_agent,
                        )
                    except RuntimeError as exc:
                        if "headless chrome not available" in str(exc).casefold():
                            break
                        continue
                    except Exception:
                        continue
                _append_downloaded_source(
                    name=f"lse-aim-all-share-constituents-page-{page_number}",
                    source_url=page_url,
                    path=page_path,
                    category="issuer_directory",
                    notes=f"page={page_number}",
                )
                issuer_records.extend(
                    _extract_uk_aim_constituent_records(
                        page_path.read_text(encoding="utf-8", errors="ignore")
                    )
                )

    issuer_records = _merge_uk_issuer_records(issuer_records)
    if not issuer_records:
        return []

    search_dir = country_dir / "companies-house-search"
    officers_dir = country_dir / "companies-house-officers"
    psc_dir = country_dir / "companies-house-psc"
    search_dir.mkdir(parents=True, exist_ok=True)
    officers_dir.mkdir(parents=True, exist_ok=True)
    psc_dir.mkdir(parents=True, exist_ok=True)

    issuer_entities: list[dict[str, object]] = []
    ticker_entities: list[dict[str, object]] = []
    people_entities: list[dict[str, object]] = []
    shareholder_organization_records: list[dict[str, str]] = []
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
        ticker_entity = _build_uk_aim_ticker_entity(
            issuer_record=issuer_record,
            company_number=company_number,
        )
        if ticker_entity is not None:
            ticker_entities.append(ticker_entity)
        if company_number is None:
            continue
        officers_path = officers_dir / f"{company_number}.html"
        officers_url = _COMPANIES_HOUSE_OFFICERS_URL_TEMPLATE.format(
            company_number=company_number
        )
        if not officers_path.exists():
            try:
                _download_source(officers_url, officers_path, user_agent=user_agent)
            except Exception:
                pass
        if officers_path.exists():
            _append_downloaded_source(
                name=f"companies-house-officers-{company_number}",
                source_url=officers_url,
                path=officers_path,
                category="company_registry",
                notes=f"company_number={company_number}",
            )
            people_entities.extend(
                _extract_companies_house_officers_entities(
                    officers_path.read_text(encoding="utf-8", errors="ignore"),
                    employer_name=company_name,
                    employer_company_number=company_number,
                )
            )
        psc_path = psc_dir / f"{company_number}.html"
        psc_url = _COMPANIES_HOUSE_PSC_URL_TEMPLATE.format(company_number=company_number)
        if not psc_path.exists():
            try:
                _download_source(psc_url, psc_path, user_agent=user_agent)
            except Exception:
                pass
        if psc_path.exists():
            _append_downloaded_source(
                name=f"companies-house-psc-{company_number}",
                source_url=psc_url,
                path=psc_path,
                category="company_registry",
                notes=f"company_number={company_number}",
            )
            psc_html = psc_path.read_text(encoding="utf-8", errors="ignore")
            people_entities.extend(
                _extract_companies_house_psc_people_entities(
                    psc_html,
                    employer_name=company_name,
                    employer_company_number=company_number,
                    employer_ticker=str(issuer_record.get("ticker", "")).strip(),
                )
            )
            shareholder_organization_records.extend(
                _extract_companies_house_psc_organization_records(
                    psc_html,
                    employer_name=company_name,
                    employer_company_number=company_number,
                    employer_ticker=str(issuer_record.get("ticker", "")).strip(),
                )
            )

    shareholder_organization_entities = _build_uk_major_shareholder_organization_entities(
        shareholder_records=shareholder_organization_records,
        organization_lookup=_build_uk_organization_entity_lookup(issuer_entities),
    )
    issuer_warnings: list[str] = []
    people_warnings: list[str] = []
    issuer_entities = _dedupe_entities(issuer_entities)
    _enrich_united_kingdom_ticker_entities_with_issuer_metadata(
        ticker_entities=ticker_entities,
        issuer_entities=issuer_entities,
    )
    ticker_entities = _dedupe_entities(ticker_entities)
    _enrich_united_kingdom_people_with_issuer_metadata(
        people_entities=people_entities,
        issuer_entities=issuer_entities,
    )
    people_entities = _dedupe_entities(people_entities)
    wikidata_warnings = _enrich_united_kingdom_entities_with_wikidata(
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
                "name": "derived-finance-uk-issuers",
                "path": issuer_path.name,
                "record_count": len(issuer_entities),
                "adapter": "fca_official_list_live_plus_lse_aim_constituents",
                "warnings": issuer_warnings,
            }
        )
    if ticker_entities:
        ticker_path = country_dir / "derived_ticker_entities.json"
        _write_curated_entities(ticker_path, entities=ticker_entities)
        derived_files.append(
            {
                "name": "derived-finance-uk-tickers",
                "path": ticker_path.name,
                "record_count": len(ticker_entities),
                "adapter": "lse_aim_constituents",
            }
        )
    if people_entities:
        people_path = country_dir / "derived_people_entities.json"
        _write_curated_entities(people_path, entities=people_entities)
        derived_files.append(
            {
                "name": "derived-finance-uk-people",
                "path": people_path.name,
                "record_count": len(people_entities),
                "adapter": "companies_house_officers_plus_psc",
                "warnings": people_warnings,
            }
        )
    if shareholder_organization_entities:
        shareholder_org_path = country_dir / "derived_shareholder_organization_entities.json"
        _write_curated_entities(
            shareholder_org_path,
            entities=_dedupe_entities(shareholder_organization_entities),
        )
        derived_files.append(
            {
                "name": "derived-finance-uk-shareholder-organizations",
                "path": shareholder_org_path.name,
                "record_count": len(_load_curated_entities(shareholder_org_path)),
                "adapter": "companies_house_psc_organizations",
            }
        )
    return derived_files


def _derive_united_states_sec_entities(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[dict[str, object]]:
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
    issuer_entities: list[dict[str, object]] = []
    ticker_entities: list[dict[str, object]] = []
    ticker_source_ids: dict[str, str] = {}
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
        issuer_entities.append(
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
            ticker_entities.append(
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
            ticker_source_ids[ticker] = issuer_source_id
    if not issuer_entities and not ticker_entities:
        return []
    wikidata_warnings = _enrich_united_states_entities_with_wikidata(
        country_dir=country_dir,
        issuer_entities=issuer_entities,
        ticker_entities=ticker_entities,
        ticker_source_ids=ticker_source_ids,
        downloaded_sources=downloaded_sources,
        user_agent=user_agent,
    )
    if wikidata_warnings:
        print("\n".join(wikidata_warnings))
    derived_entities = [*issuer_entities, *ticker_entities]
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


def _enrich_united_states_entities_with_wikidata(
    *,
    country_dir: Path,
    issuer_entities: list[dict[str, object]],
    ticker_entities: list[dict[str, object]],
    ticker_source_ids: dict[str, str],
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[str]:
    resolution_payload, resolution_path, warnings = (
        _build_united_states_wikidata_resolution_snapshot(
            issuer_entities=issuer_entities,
            country_dir=country_dir,
            user_agent=user_agent,
        )
    )
    if resolution_path is None:
        return warnings
    downloaded_sources.append(
        {
            "name": "wikidata-united-states-entity-resolution",
            "source_url": _WIKIDATA_API_URL,
            "path": str(resolution_path.relative_to(country_dir)),
            "category": "knowledge_graph",
            "notes": "Wikidata QID resolution snapshot for United States issuers and aligned tickers using SEC CIK claims.",
        }
    )
    issuer_matches = resolution_payload.get("issuers", {})
    if not isinstance(issuer_matches, dict):
        return warnings
    for entity in issuer_entities:
        cik = _united_states_wikidata_target_cik(entity)
        match = issuer_matches.get(cik)
        if isinstance(match, dict):
            _merge_wikidata_metadata(entity=entity, match=match)
    for entity in ticker_entities:
        ticker = _clean_text(entity.get("canonical_text")).upper()
        match = issuer_matches.get(ticker_source_ids.get(ticker, ""))
        if isinstance(match, dict):
            _merge_wikidata_metadata(entity=entity, match=match)
    return warnings


def _build_united_states_wikidata_resolution_snapshot(
    *,
    issuer_entities: list[dict[str, object]],
    country_dir: Path,
    user_agent: str,
) -> tuple[dict[str, object], Path | None, list[str]]:
    issuer_matches, warnings = _resolve_united_states_issuer_wikidata_matches(
        issuer_entities=issuer_entities,
        user_agent=user_agent,
    )
    payload: dict[str, object] = {
        "schema_version": 1,
        "country_code": "us",
        "generated_at": _utc_timestamp(),
        "issuers": issuer_matches,
        "people": {},
    }
    if not issuer_matches:
        return payload, None, warnings
    resolution_path = country_dir / "wikidata-united-states-entity-resolution.json"
    resolution_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload, resolution_path, warnings


def _resolve_united_states_issuer_wikidata_matches(
    *,
    issuer_entities: list[dict[str, object]],
    user_agent: str,
) -> tuple[dict[str, dict[str, object]], list[str]]:
    warnings: list[str] = []
    candidate_ids_by_cik = _query_wikidata_entity_ids_by_string_property(
        property_id=_WIKIDATA_CIK_PROPERTY_ID,
        values=[
            cik
            for entity in issuer_entities
            if (cik := _united_states_wikidata_target_cik(entity))
        ],
        user_agent=user_agent,
        warnings=warnings,
    )
    entity_payloads = _fetch_wikidata_entity_payloads(
        entity_ids=[
            candidate_id
            for candidate_ids in candidate_ids_by_cik.values()
            for candidate_id in candidate_ids
        ],
        user_agent=user_agent,
        warnings=warnings,
    )
    matches: dict[str, dict[str, object]] = {}
    for entity in issuer_entities:
        cik = _united_states_wikidata_target_cik(entity)
        candidate_ids = candidate_ids_by_cik.get(cik, [])
        if not candidate_ids:
            continue
        match = _select_best_united_states_issuer_wikidata_match(
            entity=entity,
            candidate_ids=candidate_ids,
            entity_payloads=entity_payloads,
        )
        if match is not None:
            matches[cik] = match
    return matches, warnings


def _query_wikidata_entity_ids_by_string_property(
    *,
    property_id: str,
    values: list[str],
    user_agent: str,
    warnings: list[str],
) -> dict[str, list[str]]:
    resolved_values = _unique_aliases(
        [
            _normalize_whitespace(value).upper()
            for value in values
            if _normalize_whitespace(value)
        ]
    )
    matches: dict[str, list[str]] = {}
    for index in range(0, len(resolved_values), _WIKIDATA_STRING_PROPERTY_BATCH_SIZE):
        batch = resolved_values[index : index + _WIKIDATA_STRING_PROPERTY_BATCH_SIZE]
        query = (
            "SELECT ?item ?property_value WHERE { "
            f"VALUES ?property_value {{ {' '.join(json.dumps(value) for value in batch)} }} "
            f"?item wdt:{property_id} ?property_value . "
            "}"
        )
        try:
            payload = _fetch_wikidata_sparql_json(query=query, user_agent=user_agent)
        except Exception as exc:
            warning_text = f"Wikidata property query failed for {property_id}: {exc}"
            if warning_text not in warnings and len(warnings) < 5:
                warnings.append(warning_text)
            continue
        results = payload.get("results")
        if not isinstance(results, dict):
            continue
        bindings = results.get("bindings")
        if not isinstance(bindings, list):
            continue
        for binding in bindings:
            if not isinstance(binding, dict):
                continue
            property_binding = binding.get("property_value")
            item_binding = binding.get("item")
            if not isinstance(property_binding, dict) or not isinstance(item_binding, dict):
                continue
            property_value = _normalize_whitespace(str(property_binding.get("value", ""))).upper()
            qid = _extract_wikidata_qid_from_uri(str(item_binding.get("value", "")))
            if not property_value or not qid:
                continue
            matches.setdefault(property_value, [])
            if qid not in matches[property_value]:
                matches[property_value].append(qid)
    return matches


def _fetch_wikidata_sparql_json(
    *,
    query: str,
    user_agent: str,
) -> dict[str, Any]:
    with httpx.Client(
        headers={
            "Accept": "application/sparql-results+json",
            "User-Agent": user_agent,
        },
        follow_redirects=True,
        timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
    ) as client:
        last_error: Exception | None = None
        for attempt in range(1, _WIKIDATA_SPARQL_MAX_ATTEMPTS + 1):
            try:
                response = client.get(
                    _WIKIDATA_SPARQL_URL,
                    params={"query": query, "format": "json"},
                )
                response.raise_for_status()
                payload = response.json()
                break
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status_code = exc.response.status_code
                if status_code != 429 or attempt >= _WIKIDATA_SPARQL_MAX_ATTEMPTS:
                    raise
                retry_after = _normalize_whitespace(
                    str(exc.response.headers.get("Retry-After", ""))
                )
                delay_seconds = (
                    float(retry_after)
                    if retry_after.isdigit()
                    else _WIKIDATA_SPARQL_RETRY_BASE_SECONDS * attempt
                )
                time.sleep(max(delay_seconds, _WIKIDATA_SPARQL_RETRY_BASE_SECONDS))
        else:
            if last_error is not None:
                raise last_error
            raise ValueError("Wikidata SPARQL query failed without a concrete error")
    if not isinstance(payload, dict):
        raise ValueError("Wikidata SPARQL endpoint returned a non-object payload")
    return payload


def _extract_wikidata_qid_from_uri(value: str) -> str:
    match = re.search(r"/(Q\d+)$", _normalize_whitespace(value))
    return match.group(1) if match is not None else ""


def _extract_wikidata_property_id_from_uri(value: str) -> str:
    match = re.search(r"/(P\d+)$", _normalize_whitespace(value))
    return match.group(1) if match is not None else ""


def _united_states_wikidata_target_cik(entity: dict[str, object]) -> str:
    metadata = entity.get("metadata")
    if isinstance(metadata, dict):
        for key in ("cik", "issuer_cik", "employer_cik"):
            if (value := _format_sec_source_id(metadata.get(key))):
                return value
    entity_id = _normalize_whitespace(str(entity.get("entity_id", "")))
    if entity_id.startswith("finance-us-issuer:"):
        return _format_sec_source_id(entity_id.split(":", 1)[1]) or ""
    return ""


def _select_best_united_states_issuer_wikidata_match(
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
        score = _score_united_states_issuer_wikidata_candidate(
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
    if best_score < 100:
        return None
    if len(scored_candidates) > 1 and best_score - second_score < 10 and best_score < 160:
        return None
    return _build_wikidata_resolution_match(
        best_qid,
        entity_payloads.get(best_qid),
        entity_type="organization",
        alias_builder=_build_united_states_wikidata_entity_aliases,
        label_languages=("en",),
    )


def _score_united_states_issuer_wikidata_candidate(
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
    elif target_keys.intersection(candidate_keys):
        score += 30
    string_values = _extract_wikidata_string_claim_values(candidate_payload)
    cik = _united_states_wikidata_target_cik(entity)
    if cik and cik in string_values:
        score += 100
    ticker = _normalize_whitespace(str(metadata.get("ticker", ""))).upper()
    if ticker and ticker in string_values:
        score += 15
    candidate_description = _extract_wikidata_description(candidate_payload).casefold()
    if any(
        keyword in candidate_description
        for keyword in (
            "american",
            "company",
            "corporation",
            "technology",
            "bank",
            "financial",
            "insurance",
            "holding",
            "biotechnology",
        )
    ):
        score += 5
    return score


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


def _extract_jpx_english_disclosure_workbook_url(
    html_text: str,
    *,
    base_url: str,
) -> str | None:
    match = _JPX_DISCLOSURE_WORKBOOK_URL_PATTERN.search(html_text)
    if match is None:
        return None
    workbook_url = html.unescape(match.group("url")).strip()
    if not workbook_url:
        return None
    return normalize_source_url(urljoin(base_url, workbook_url))


def _collect_japan_listed_company_search_records(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
    warnings: list[str],
    allow_live_fetch: bool,
) -> list[dict[str, str]]:
    page_paths = [
        country_dir / str(source.get("path"))
        for source in downloaded_sources
        if str(source.get("name", "")).startswith("jpx-listed-company-search-results-page-")
    ]
    existing_pages = [path for path in page_paths if path.exists()]
    if not existing_pages and allow_live_fetch:
        existing_pages.extend(
            _download_japan_listed_company_search_pages(
                country_dir=country_dir,
                downloaded_sources=downloaded_sources,
                user_agent=user_agent,
                warnings=warnings,
            )
        )
    records: list[dict[str, str]] = []
    for page_path in sorted(existing_pages):
        records.extend(
            _extract_japan_listed_company_search_records(
                page_path.read_text(encoding="utf-8", errors="ignore")
            )
        )
    return records


def _download_japan_listed_company_search_pages(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
    warnings: list[str],
) -> list[Path]:
    pages_dir = country_dir / "jpx-listed-company-search"
    pages_dir.mkdir(parents=True, exist_ok=True)
    page_paths: list[Path] = []
    headers = {
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        with httpx.Client(
            headers=headers,
            follow_redirects=True,
            timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
        ) as client:
            search_response = client.get(_JPX_LISTED_COMPANY_SEARCH_APP_URL)
            search_response.raise_for_status()
            search_action = _extract_japan_listed_company_search_action(
                search_response.text
            ) or _JPX_LISTED_COMPANY_SEARCH_APP_URL
            first_page_response = client.post(
                search_action,
                data={
                    "ListShow": "ListShow",
                    "sniMtGmnId": "",
                    "dspSsuPd": _JPX_LISTED_COMPANY_SEARCH_PAGE_SIZE,
                    "dspSsuPdMapOut": "10>10<50>50<100>100<200>200<",
                    "mgrMiTxtBx": "",
                    "eqMgrCd": "",
                    "szkbuChkbx": list(_JPX_LISTED_COMPANY_SEARCH_SEGMENT_VALUES),
                    "szkbuChkbxMapOut": _JPX_LISTED_COMPANY_SEARCH_SEGMENT_MAP_OUT,
                },
            )
            first_page_response.raise_for_status()
            first_page_html = first_page_response.text
            first_page_path = pages_dir / "page-1.html"
            first_page_path.write_text(first_page_html, encoding="utf-8")
            downloaded_sources.append(
                {
                    "name": "jpx-listed-company-search-results-page-1",
                    "source_url": str(first_page_response.request.url),
                    "path": str(first_page_path.relative_to(country_dir)),
                    "category": "issuer_directory",
                    "notes": (
                        "page=1; market_segments="
                        f"{','.join(_JPX_LISTED_COMPANY_SEARCH_SEGMENT_VALUES)}"
                    ),
                }
            )
            page_paths.append(first_page_path)
            page_count = _extract_japan_listed_company_search_page_count(first_page_html)
            page_count = min(page_count, _JPX_LISTED_COMPANY_SEARCH_FETCH_MAX_PAGES)
            result_action = _extract_japan_listed_company_results_action(
                first_page_html
            ) or urljoin(
                _JPX_LISTED_COMPANY_RESULTS_BASE_URL,
                "/tseHpFront/JJK020030Action.do",
            )
            hidden_fields = _extract_japan_listed_company_result_form_fields(
                first_page_html
            )
            for page_number in range(2, page_count + 1):
                page_fields = dict(hidden_fields)
                page_fields["lstDspPg"] = str(page_number)
                page_fields["Transition"] = "Transition"
                page_response = client.post(result_action, data=page_fields)
                page_response.raise_for_status()
                page_path = pages_dir / f"page-{page_number}.html"
                page_path.write_text(page_response.text, encoding="utf-8")
                downloaded_sources.append(
                    {
                        "name": f"jpx-listed-company-search-results-page-{page_number}",
                        "source_url": str(page_response.request.url),
                        "path": str(page_path.relative_to(country_dir)),
                        "category": "issuer_directory",
                        "notes": (
                            f"page={page_number}; market_segments="
                            f"{','.join(_JPX_LISTED_COMPANY_SEARCH_SEGMENT_VALUES)}"
                        ),
                    }
                )
                page_paths.append(page_path)
    except Exception as exc:
        warning_text = f"JPX listed company search fetch failed: {exc}"
        if warning_text not in warnings:
            warnings.append(warning_text)
    return page_paths


def _extract_japan_listed_company_search_action(html_text: str) -> str | None:
    match = _JPX_LISTED_COMPANY_SEARCH_FORM_PATTERN.search(html_text)
    if match is None:
        return None
    action = _normalize_whitespace(html.unescape(match.group("action")))
    if not action:
        return None
    return normalize_source_url(urljoin(_JPX_LISTED_COMPANY_RESULTS_BASE_URL, action))


def _extract_japan_listed_company_results_action(html_text: str) -> str | None:
    match = _JPX_LISTED_COMPANY_RESULTS_FORM_PATTERN.search(html_text)
    if match is None:
        return None
    action = _normalize_whitespace(html.unescape(match.group("action")))
    if not action:
        return None
    return normalize_source_url(urljoin(_JPX_LISTED_COMPANY_RESULTS_BASE_URL, action))


def _extract_japan_listed_company_result_form_fields(
    html_text: str,
) -> dict[str, str]:
    match = _JPX_LISTED_COMPANY_RESULTS_FORM_PATTERN.search(html_text)
    if match is None:
        return {}
    body = match.group("body")
    fields: dict[str, str] = {}
    for input_match in _JPX_LISTED_COMPANY_HIDDEN_INPUT_PATTERN.finditer(body):
        name = _normalize_whitespace(input_match.group("name"))
        if not name:
            continue
        fields[name] = _normalize_whitespace(html.unescape(input_match.group("value")))
    return fields


def _extract_japan_listed_company_search_page_count(html_text: str) -> int:
    total_match = _JPX_LISTED_COMPANY_TOTAL_COUNT_PATTERN.search(html_text)
    page_size_match = _JPX_LISTED_COMPANY_PAGE_SIZE_PATTERN.search(html_text)
    try:
        total_count = int(total_match.group("count")) if total_match is not None else 0
        page_size = int(page_size_match.group("size")) if page_size_match is not None else 0
    except ValueError:
        return 1
    if total_count <= 0 or page_size <= 0:
        return 1
    return max(1, (total_count + page_size - 1) // page_size)


def _extract_japan_listed_company_search_records(
    html_text: str,
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    seen_codes: set[str] = set()
    for match in _JPX_LISTED_COMPANY_RESULT_ROW_PATTERN.finditer(html_text):
        row_html = match.group("body")
        row_fields = {
            _normalize_whitespace(field_match.group("field")): _normalize_whitespace(
                html.unescape(field_match.group("value"))
            )
            for field_match in _JPX_LISTED_COMPANY_RESULT_INPUT_PATTERN.finditer(row_html)
        }
        raw_code = row_fields.get("eqMgrCd", "").upper()
        code = _normalize_japan_jpx_company_code(raw_code)
        company_name = _normalize_japan_company_display_name(row_fields.get("eqMgrNm", ""))
        if not code or not company_name or code in seen_codes:
            continue
        seen_codes.add(code)
        detail_match = _JPX_LISTED_COMPANY_DETAIL_BUTTON_PATTERN.search(row_html)
        stock_price_match = _JPX_LISTED_COMPANY_STOCK_PRICE_URL_PATTERN.search(row_html)
        stock_price_url = ""
        if stock_price_match is not None:
            stock_price_url = normalize_source_url(
                html.unescape(stock_price_match.group("url"))
            )
        records.append(
            {
                "code": code,
                "raw_code": raw_code,
                "company_name": company_name,
                "raw_company_name": row_fields.get("eqMgrNm", ""),
                "market_segment": row_fields.get("szkbuNm", ""),
                "industry": row_fields.get("gyshDspNm", ""),
                "fiscal_year_end": row_fields.get("dspYuKssnKi", ""),
                "listed_company_search_code": _normalize_whitespace(
                    detail_match.group("code") if detail_match is not None else raw_code
                ),
                "listed_company_search_delisted_flag": _normalize_whitespace(
                    detail_match.group("delisted_flag") if detail_match is not None else "1"
                ),
                "stock_price_url": stock_price_url,
            }
        )
    return records


def _build_japan_listed_company_detail_fetch_candidates(
    *,
    listed_company_records: list[dict[str, str]],
    workbook_records: list[dict[str, str]],
    governance_records: list[dict[str, str]],
) -> list[dict[str, str]]:
    listed_company_record_by_code = {
        _normalize_whitespace(str(record.get("code", ""))).upper(): record
        for record in listed_company_records
        if _normalize_whitespace(str(record.get("code", "")))
    }
    candidate_codes: set[str] = {
        _normalize_whitespace(str(record.get("code", ""))).upper()
        for record in workbook_records
        if _normalize_whitespace(str(record.get("code", "")))
    }
    candidate_codes.update(
        _normalize_whitespace(str(record.get("code", ""))).upper()
        for record in governance_records
        if _normalize_whitespace(str(record.get("code", "")))
    )
    return [
        listed_company_record_by_code[code]
        for code in sorted(candidate_codes)
        if code in listed_company_record_by_code
        and _normalize_whitespace(
            str(listed_company_record_by_code[code].get("listed_company_search_code", ""))
        )
    ]


def _collect_japan_listed_company_detail_records(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
    warnings: list[str],
    allow_live_fetch: bool,
    candidate_records: list[dict[str, str]],
) -> list[dict[str, str]]:
    detail_sources = [
        source
        for source in downloaded_sources
        if str(source.get("name", "")).startswith("jpx-listed-company-detail-")
    ]
    detail_records_by_code: dict[str, dict[str, str]] = {}
    for source in sorted(detail_sources, key=lambda item: str(item.get("path", ""))):
        page_path = country_dir / str(source.get("path", ""))
        if not page_path.exists():
            continue
        record = _extract_japan_listed_company_detail_record(
            page_path.read_text(encoding="utf-8", errors="ignore")
        )
        code = _normalize_whitespace(str(record.get("code", ""))).upper()
        if code:
            _merge_japan_listed_company_detail_source_metadata(record=record, source=source)
            detail_records_by_code[code] = record
    if allow_live_fetch and candidate_records:
        missing_records = [
            record
            for record in candidate_records
            if _normalize_whitespace(str(record.get("code", ""))).upper()
            not in detail_records_by_code
        ]
        if missing_records:
            downloaded_paths = _download_japan_listed_company_detail_pages(
                country_dir=country_dir,
                downloaded_sources=downloaded_sources,
                user_agent=user_agent,
                warnings=warnings,
                candidate_records=missing_records,
            )
            for page_path in downloaded_paths:
                record = _extract_japan_listed_company_detail_record(
                    page_path.read_text(encoding="utf-8", errors="ignore")
                )
                code = _normalize_whitespace(str(record.get("code", ""))).upper()
                if code:
                    source = next(
                        (
                            item
                            for item in downloaded_sources
                            if str(item.get("name", "")).startswith("jpx-listed-company-detail-")
                            and country_dir / str(item.get("path", "")) == page_path
                        ),
                        None,
                    )
                    if isinstance(source, dict):
                        _merge_japan_listed_company_detail_source_metadata(
                            record=record,
                            source=source,
                        )
                    detail_records_by_code[code] = record
    ordered_codes: list[str] = []
    for record in candidate_records:
        code = _normalize_whitespace(str(record.get("code", ""))).upper()
        if code and code in detail_records_by_code and code not in ordered_codes:
            ordered_codes.append(code)
    for code in sorted(detail_records_by_code):
        if code not in ordered_codes:
            ordered_codes.append(code)
    return [detail_records_by_code[code] for code in ordered_codes]


def _merge_japan_listed_company_detail_source_metadata(
    *,
    record: dict[str, str],
    source: dict[str, object],
) -> None:
    source_name = _normalize_whitespace(str(source.get("name", "")))
    if source_name:
        record["source_name"] = source_name
    source_url = _normalize_whitespace(str(source.get("source_url", "")))
    if source_url:
        record["detail_page_url"] = source_url


def _download_japan_listed_company_detail_pages(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
    warnings: list[str],
    candidate_records: list[dict[str, str]],
) -> list[Path]:
    pages_dir = country_dir / "jpx-listed-company-details"
    pages_dir.mkdir(parents=True, exist_ok=True)
    page_paths: list[Path] = []
    headers = {
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        with httpx.Client(
            headers=headers,
            follow_redirects=True,
            timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
        ) as client:
            landing_response = client.get(_JPX_LISTED_COMPANY_SEARCH_APP_URL)
            landing_response.raise_for_status()
            detail_action = urljoin(
                _JPX_LISTED_COMPANY_RESULTS_BASE_URL,
                "/tseHpFront/JJK020030Action.do",
            )
            for record in candidate_records:
                code = _normalize_whitespace(str(record.get("code", ""))).upper()
                search_code = _normalize_whitespace(
                    str(record.get("listed_company_search_code", ""))
                )
                delisted_flag = _normalize_whitespace(
                    str(record.get("listed_company_search_delisted_flag", ""))
                ) or "1"
                if not code or not search_code:
                    continue
                response = client.post(
                    detail_action,
                    data={
                        "BaseJh": "BaseJh",
                        "mgrCd": search_code,
                        "jjHisiFlg": delisted_flag,
                    },
                )
                response.raise_for_status()
                if _JPX_LISTED_COMPANY_DETAIL_FORM_MARKER not in response.text:
                    warning_text = (
                        "JPX listed company detail page did not resolve for "
                        f"{code} (search code {search_code})"
                    )
                    if warning_text not in warnings:
                        warnings.append(warning_text)
                    continue
                page_path = pages_dir / f"{_slug(code) or code.casefold()}.html"
                page_path.write_text(response.text, encoding="utf-8")
                downloaded_sources.append(
                    {
                        "name": f"jpx-listed-company-detail-{code.casefold()}",
                        "source_url": str(response.request.url),
                        "path": str(page_path.relative_to(country_dir)),
                        "category": "issuer_profile",
                        "notes": (
                            f"jpx_company_code={code}; "
                            f"listed_company_search_code={search_code}"
                        ),
                    }
                )
                page_paths.append(page_path)
    except Exception as exc:
        warning_text = f"JPX listed company detail fetch failed: {exc}"
        if warning_text not in warnings:
            warnings.append(warning_text)
    return page_paths


def _extract_japan_listed_company_detail_record(
    html_text: str,
) -> dict[str, str]:
    if _JPX_LISTED_COMPANY_DETAIL_FORM_MARKER not in html_text:
        return {}
    field_values = _extract_japan_listed_company_detail_field_values(html_text)
    detail_record: dict[str, str] = {}
    company_name_match = _JPX_LISTED_COMPANY_DETAIL_COMPANY_NAME_PATTERN.search(html_text)
    if company_name_match is not None:
        company_name = _normalize_japan_company_display_name(
            _strip_html(company_name_match.group("company_name"))
        )
        if company_name:
            detail_record["company_name"] = company_name
            detail_record["raw_company_name"] = company_name
    raw_code = _normalize_whitespace(field_values.get("code", "")).upper()
    if raw_code:
        detail_record["raw_code"] = raw_code
        detail_record["code"] = _normalize_japan_jpx_company_code(raw_code)
        detail_record["listed_company_search_code"] = raw_code
    for label, key in _JPX_LISTED_COMPANY_DETAIL_LABEL_TO_KEY.items():
        value = _normalize_japan_listed_company_detail_value(field_values.get(label, ""))
        if not value:
            continue
        if key in {
            "date_of_incorporation",
            "earnings_results_announcement_scheduled",
            "first_quarter_scheduled",
            "second_quarter_scheduled",
            "third_quarter_scheduled",
            "date_of_general_shareholders_meeting_scheduled",
            "date_of_listing",
        }:
            value = _normalize_japan_listed_company_detail_date(value)
        detail_record[key] = value
    return detail_record


def _extract_japan_listed_company_detail_field_values(
    html_text: str,
) -> dict[str, str]:
    field_values: dict[str, str] = {}
    for table_match in _JPX_LISTED_COMPANY_DETAIL_TABLE_PATTERN.finditer(html_text):
        rows: list[tuple[list[str], list[str]]] = []
        for row_match in _JPX_LISTED_COMPANY_DETAIL_ROW_PATTERN.finditer(
            table_match.group("body")
        ):
            row_html = row_match.group("body")
            headers = [
                _normalize_japan_listed_company_detail_label(_strip_html(cell_match.group("cell")))
                for cell_match in _JPX_LISTED_COMPANY_DETAIL_HEADER_CELL_PATTERN.finditer(row_html)
            ]
            data_cells = [
                _normalize_japan_listed_company_detail_value(_strip_html(cell_match.group("cell")))
                for cell_match in _JPX_LISTED_COMPANY_DETAIL_DATA_CELL_PATTERN.finditer(row_html)
            ]
            rows.append((headers, data_cells))
        for index in range(len(rows) - 1):
            headers, header_data = rows[index]
            values_headers, values = rows[index + 1]
            if not headers or header_data or values_headers or len(headers) != len(values):
                continue
            for label, value in zip(headers, values):
                if label and value:
                    field_values[label] = value
    return field_values


def _normalize_japan_listed_company_detail_label(value: str) -> str:
    return _normalize_whitespace(value).casefold()


def _normalize_japan_listed_company_detail_value(value: str) -> str:
    normalized = _normalize_whitespace(value)
    if normalized in {"-", "N/A", "n/a"}:
        return ""
    return normalized


def _normalize_japan_listed_company_detail_date(value: str) -> str:
    normalized = _normalize_japan_listed_company_detail_value(value)
    if not normalized:
        return ""
    return _normalize_japan_governance_report_date(normalized)


def _extract_japan_jpx_english_disclosure_records(
    source_path: Path,
) -> list[dict[str, str]]:
    if not zipfile.is_zipfile(source_path):
        return []
    with zipfile.ZipFile(source_path) as archive:
        shared_strings = _load_xlsx_shared_strings(archive)
        sheet_targets = _load_xlsx_sheet_targets(archive)
        worksheet_path = next(
            (
                target
                for name, target in sheet_targets
                if _normalize_whitespace(name).casefold() == "english disclosure"
            ),
            sheet_targets[0][1] if sheet_targets else None,
        )
        if worksheet_path is None:
            return []
        rows = _iter_xlsx_sheet_rows(
            archive,
            worksheet_path=worksheet_path,
            shared_strings=shared_strings,
        )
    header_map: dict[str, int] | None = None
    as_of = ""
    records: list[dict[str, str]] = []
    for row in rows:
        if not any(value.strip() for value in row):
            continue
        if header_map is None:
            if _is_jpx_english_disclosure_header_row(row):
                header_map = {
                    _normalize_whitespace(value).casefold(): index
                    for index, value in enumerate(row)
                    if _normalize_whitespace(value)
                }
            else:
                row_text = " ".join(_normalize_whitespace(value) for value in row if value.strip())
                if "as of" in row_text.casefold():
                    as_of = row_text
            continue
        code = _jpx_row_value(row, header_map, "code").upper()
        raw_company_name = _jpx_row_value(row, header_map, "company name")
        if not code or not raw_company_name:
            continue
        market_segment = _jpx_row_value(row, header_map, "market segment")
        company_name = _normalize_japan_company_display_name(raw_company_name)
        if not company_name:
            continue
        records.append(
            {
                "code": code,
                "company_name": company_name,
                "raw_company_name": raw_company_name,
                "market_segment": market_segment,
                "sector": _jpx_row_value(row, header_map, "sector"),
                "market_capitalization_jpy_millions": _jpx_row_value(
                    row,
                    header_map,
                    "market capitalization (jpy mil.)",
                ),
                "corporate_governance_reports_status": _jpx_row_value(
                    row,
                    header_map,
                    "corporate governance reports disclosure status",
                ),
                "annual_securities_reports_status": _jpx_row_value(
                    row,
                    header_map,
                    "annual securities reports disclosure status",
                ),
                "ir_presentations_status": _jpx_row_value(
                    row,
                    header_map,
                    "ir presentations disclosure status",
                ),
                "other_english_documents": _jpx_row_value(
                    row,
                    header_map,
                    "other english documents disclosed on ir website",
                ),
                "ir_website_url": _jpx_row_value(
                    row,
                    header_map,
                    "ir website english links",
                ),
                "as_of": as_of,
            }
        )
    return records


def _jpx_row_value(
    row: list[str],
    header_map: dict[str, int],
    header_name: str,
) -> str:
    normalized_header = _normalize_whitespace(header_name).casefold()
    for key, index in header_map.items():
        if key == normalized_header or key.startswith(f"{normalized_header} "):
            if index < len(row):
                return _normalize_whitespace(row[index])
            return ""
    return ""


def _is_jpx_english_disclosure_header_row(row: list[str]) -> bool:
    normalized = [_normalize_whitespace(value).casefold() for value in row if value.strip()]
    required = {
        "code",
        "company name",
        "market segment",
        "sector",
        "ir website english links",
    }
    return required.issubset(set(normalized))


def _build_japan_jpx_issuer_entity(record: dict[str, str]) -> dict[str, object] | None:
    code = _normalize_whitespace(str(record.get("code", ""))).upper()
    company_name = _normalize_whitespace(str(record.get("company_name", "")))
    raw_company_name = _normalize_whitespace(str(record.get("raw_company_name", "")))
    if not code or not company_name:
        return None
    metadata: dict[str, object] = {
        "country_code": "jp",
        "category": "issuer",
        "ticker": code,
        "jpx_company_code": code,
    }
    _merge_japan_jpx_listed_company_search_metadata(metadata=metadata, record=record)
    _merge_japan_jpx_listed_company_detail_metadata(metadata=metadata, record=record)
    _merge_japan_jpx_workbook_metadata(metadata=metadata, record=record)
    aliases = [*_build_japan_company_aliases(raw_company_name, company_name), code]
    return {
        "entity_type": "organization",
        "canonical_text": company_name,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-jp-issuer:{code}",
        "metadata": metadata,
    }


def _build_japan_jpx_ticker_entity(record: dict[str, str]) -> dict[str, object] | None:
    code = _normalize_whitespace(str(record.get("code", ""))).upper()
    company_name = _normalize_whitespace(str(record.get("company_name", "")))
    raw_company_name = _normalize_whitespace(str(record.get("raw_company_name", "")))
    if not code or not company_name:
        return None
    metadata: dict[str, object] = {
        "country_code": "jp",
        "category": "ticker",
        "issuer_name": company_name,
        "jpx_company_code": code,
    }
    _merge_japan_jpx_listed_company_search_metadata(metadata=metadata, record=record)
    _merge_japan_jpx_listed_company_detail_metadata(metadata=metadata, record=record)
    _merge_japan_jpx_workbook_metadata(metadata=metadata, record=record)
    aliases = [code, *_build_japan_company_aliases(raw_company_name, company_name)]
    return {
        "entity_type": "ticker",
        "canonical_text": code,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-jp-ticker:{code}",
        "metadata": metadata,
    }


def _merge_japan_issuer_seed_records(
    *,
    listed_company_records: list[dict[str, str]],
    listed_company_detail_records: list[dict[str, str]],
    workbook_records: list[dict[str, str]],
) -> list[dict[str, str]]:
    merged_by_code: dict[str, dict[str, str]] = {}
    ordered_codes: list[str] = []
    for record_group in (
        listed_company_records,
        listed_company_detail_records,
        workbook_records,
    ):
        for record in record_group:
            code = _normalize_whitespace(str(record.get("code", ""))).upper()
            if not code:
                continue
            merged = merged_by_code.setdefault(code, {})
            for key, value in record.items():
                normalized_value = _normalize_whitespace(str(value))
                if normalized_value:
                    merged[key] = normalized_value
            merged.setdefault("code", code)
            if "company_name" in merged and "raw_company_name" not in merged:
                merged["raw_company_name"] = merged["company_name"]
            if code not in ordered_codes:
                ordered_codes.append(code)
    return [merged_by_code[code] for code in ordered_codes]


def _merge_japan_jpx_listed_company_search_metadata(
    *,
    metadata: dict[str, object],
    record: dict[str, str],
) -> None:
    market_segment = _normalize_whitespace(str(record.get("market_segment", "")))
    if market_segment:
        metadata["market_segment"] = market_segment
    industry = _normalize_whitespace(str(record.get("industry", "")))
    if industry:
        metadata["industry"] = industry
    fiscal_year_end = _normalize_whitespace(str(record.get("fiscal_year_end", "")))
    if fiscal_year_end:
        metadata["fiscal_year_end"] = fiscal_year_end
    listed_company_search_code = _normalize_whitespace(
        str(record.get("listed_company_search_code", ""))
    )
    if listed_company_search_code:
        metadata["jpx_listed_company_search_code"] = listed_company_search_code
    stock_price_url = _normalize_whitespace(str(record.get("stock_price_url", "")))
    if stock_price_url:
        metadata["stock_price_url"] = stock_price_url


def _merge_japan_jpx_listed_company_detail_metadata(
    *,
    metadata: dict[str, object],
    record: dict[str, str],
) -> None:
    for record_key in (
        "isin_code",
        "trading_unit",
        "date_of_incorporation",
        "address_of_main_office",
        "listed_exchange",
        "investment_unit_as_of_end_of_last_month",
        "earnings_results_announcement_scheduled",
        "first_quarter_scheduled",
        "second_quarter_scheduled",
        "third_quarter_scheduled",
        "date_of_general_shareholders_meeting_scheduled",
        "representative_title",
        "representative_name",
        "date_of_listing",
        "listed_shares_count",
        "issued_shares_count",
        "j_iriss_registration_status",
        "loan_issue_status",
        "margin_issue_status",
        "financial_accounting_standards_foundation_membership",
        "going_concern_assumption_notes",
        "controlling_shareholders_information",
    ):
        value = _normalize_whitespace(str(record.get(record_key, "")))
        if value:
            metadata[record_key] = value


def _merge_japan_jpx_workbook_metadata(
    *,
    metadata: dict[str, object],
    record: dict[str, str],
) -> None:
    market_segment = _normalize_whitespace(str(record.get("market_segment", "")))
    if market_segment:
        metadata["market_segment"] = market_segment
    sector = _normalize_whitespace(str(record.get("sector", "")))
    if sector:
        metadata["sector"] = sector
    market_cap = _normalize_whitespace(
        str(record.get("market_capitalization_jpy_millions", ""))
    )
    if market_cap:
        metadata["market_capitalization_jpy_millions"] = market_cap
    ir_website_url = _normalize_whitespace(str(record.get("ir_website_url", "")))
    if ir_website_url:
        metadata["ir_website_url"] = ir_website_url
    as_of = _normalize_whitespace(str(record.get("as_of", "")))
    if as_of:
        metadata["source_as_of"] = as_of
    disclosure_status = _normalize_whitespace(
        str(record.get("corporate_governance_reports_status", ""))
    )
    if disclosure_status:
        metadata["corporate_governance_reports_status"] = disclosure_status
    annual_report_status = _normalize_whitespace(
        str(record.get("annual_securities_reports_status", ""))
    )
    if annual_report_status:
        metadata["annual_securities_reports_status"] = annual_report_status
    ir_presentations_status = _normalize_whitespace(
        str(record.get("ir_presentations_status", ""))
    )
    if ir_presentations_status:
        metadata["ir_presentations_status"] = ir_presentations_status
    other_english_documents = _normalize_whitespace(
        str(record.get("other_english_documents", ""))
    )
    if other_english_documents:
        metadata["other_english_documents"] = other_english_documents


def _extract_japan_listed_company_detail_people_entities(
    *,
    detail_records: list[dict[str, str]],
    issuer_records_by_code: dict[str, dict[str, str]],
) -> list[dict[str, object]]:
    people_entities: list[dict[str, object]] = []
    for detail_record in detail_records:
        employer_ticker = _normalize_whitespace(str(detail_record.get("code", ""))).upper()
        issuer_record = issuer_records_by_code.get(employer_ticker)
        if issuer_record is None:
            continue
        employer_name = _normalize_whitespace(str(issuer_record.get("company_name", "")))
        representative_title = _normalize_whitespace(
            str(detail_record.get("representative_title", ""))
        )
        representative_name = _clean_japan_governance_person_name(
            str(detail_record.get("representative_name", ""))
        )
        if (
            not employer_name
            or not representative_title
            or representative_name is None
            or not _looks_like_person_name(representative_name)
        ):
            continue
        role_class = _role_class_for_japan_governance_title(representative_title)
        if role_class is None:
            continue
        people_entities.append(
            _build_japan_listed_company_detail_person_entity(
                raw_name=representative_name,
                role_title=representative_title,
                role_class=role_class,
                employer_name=employer_name,
                employer_ticker=employer_ticker,
                source_name=_normalize_whitespace(str(detail_record.get("source_name", "")))
                or f"jpx-listed-company-detail-{employer_ticker.casefold()}",
                detail_page_url=_normalize_whitespace(
                    str(detail_record.get("detail_page_url", ""))
                ),
            )
        )
    return _dedupe_entities(people_entities)


def _build_japan_listed_company_detail_person_entity(
    *,
    raw_name: str,
    role_title: str,
    role_class: str,
    employer_name: str,
    employer_ticker: str,
    source_name: str,
    detail_page_url: str,
) -> dict[str, object]:
    canonical_name, aliases = _normalize_person_name_aliases(raw_name)
    metadata: dict[str, object] = {
        "country_code": "jp",
        "category": role_class,
        "role_title": role_title,
        "employer_name": employer_name,
        "employer_ticker": employer_ticker,
        "source_name": source_name,
        "section_name": "listed company detail page",
    }
    if detail_page_url:
        metadata["listed_company_detail_url"] = detail_page_url
    return {
        "entity_type": "person",
        "canonical_text": canonical_name,
        "aliases": _unique_aliases(aliases),
        "entity_id": (
            f"finance-jp-person:{(employer_ticker or _slug(employer_name))}:"
            f"{_slug(canonical_name)}:{_slug(role_class)}"
        ),
        "metadata": metadata,
    }


def _collect_japan_corporate_governance_report_records(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
    warnings: list[str],
    allow_live_fetch: bool,
) -> list[dict[str, str]]:
    page_paths = [
        country_dir / str(source.get("path"))
        for source in downloaded_sources
        if str(source.get("name", "")).startswith(
            "jpx-corporate-governance-search-results-page-"
        )
    ]
    existing_pages = [path for path in page_paths if path.exists()]
    if not existing_pages and allow_live_fetch:
        existing_pages.extend(
            _download_japan_corporate_governance_search_pages(
                country_dir=country_dir,
                downloaded_sources=downloaded_sources,
                user_agent=user_agent,
                warnings=warnings,
            )
        )
    records: list[dict[str, str]] = []
    for page_path in sorted(existing_pages):
        records.extend(
            _extract_japan_corporate_governance_report_records(
                page_path.read_text(encoding="utf-8", errors="ignore")
            )
        )
    return records


def _download_japan_corporate_governance_search_pages(
    *,
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
    warnings: list[str],
) -> list[Path]:
    pages_dir = country_dir / "jpx-corporate-governance-search"
    pages_dir.mkdir(parents=True, exist_ok=True)
    page_paths: list[Path] = []
    headers = {
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        with httpx.Client(
            headers=headers,
            follow_redirects=True,
            timeout=DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS,
        ) as client:
            search_response = client.get(_JPX_CORPORATE_GOVERNANCE_SEARCH_APP_URL)
            search_response.raise_for_status()
            search_action = _extract_japan_corporate_governance_search_action(
                search_response.text
            ) or _JPX_CORPORATE_GOVERNANCE_SEARCH_APP_URL
            first_page_response = client.post(
                search_action,
                data={
                    "ListShow": "ListShow",
                    "eibnCGJhSelChkbx": "on",
                    "souKnsu": "0",
                    "sbrkmFlg": "1",
                    "dspSsuPd": "100",
                },
            )
            first_page_response.raise_for_status()
            first_page_html = first_page_response.text
            first_page_path = pages_dir / "page-1.html"
            first_page_path.write_text(first_page_html, encoding="utf-8")
            downloaded_sources.append(
                {
                    "name": "jpx-corporate-governance-search-results-page-1",
                    "source_url": str(first_page_response.request.url),
                    "path": str(first_page_path.relative_to(country_dir)),
                    "category": "issuer_profile",
                    "notes": "page=1",
                }
            )
            page_paths.append(first_page_path)
            page_count = _extract_japan_corporate_governance_page_count(first_page_html)
            page_count = min(page_count, _JPX_CORPORATE_GOVERNANCE_FETCH_MAX_PAGES)
            result_action = (
                _extract_japan_corporate_governance_results_action(first_page_html)
                or urljoin(
                    _JPX_CORPORATE_GOVERNANCE_RESULTS_BASE_URL,
                    "/tseHpFront/CGK020030Action.do",
                )
            )
            hidden_fields = _extract_japan_corporate_governance_result_form_fields(
                first_page_html
            )
            for page_number in range(2, page_count + 1):
                page_fields = dict(hidden_fields)
                page_fields["lstDspPg"] = str(page_number)
                page_fields["Transition"] = "Transition"
                page_response = client.post(result_action, data=page_fields)
                page_response.raise_for_status()
                page_path = pages_dir / f"page-{page_number}.html"
                page_path.write_text(page_response.text, encoding="utf-8")
                downloaded_sources.append(
                    {
                        "name": f"jpx-corporate-governance-search-results-page-{page_number}",
                        "source_url": str(page_response.request.url),
                        "path": str(page_path.relative_to(country_dir)),
                        "category": "issuer_profile",
                        "notes": f"page={page_number}",
                    }
                )
                page_paths.append(page_path)
    except Exception as exc:
        warning_text = f"JPX corporate governance search fetch failed: {exc}"
        if warning_text not in warnings:
            warnings.append(warning_text)
    return page_paths


def _extract_japan_corporate_governance_search_action(html_text: str) -> str | None:
    match = _JPX_CORPORATE_GOVERNANCE_SEARCH_FORM_PATTERN.search(html_text)
    if match is None:
        return None
    action = _normalize_whitespace(html.unescape(match.group("action")))
    if not action:
        return None
    return normalize_source_url(
        urljoin(_JPX_CORPORATE_GOVERNANCE_RESULTS_BASE_URL, action)
    )


def _extract_japan_corporate_governance_results_action(html_text: str) -> str | None:
    match = _JPX_CORPORATE_GOVERNANCE_RESULTS_FORM_PATTERN.search(html_text)
    if match is None:
        return None
    action = _normalize_whitespace(html.unescape(match.group("action")))
    if not action:
        return None
    return normalize_source_url(
        urljoin(_JPX_CORPORATE_GOVERNANCE_RESULTS_BASE_URL, action)
    )


def _extract_japan_corporate_governance_result_form_fields(
    html_text: str,
) -> dict[str, str]:
    match = _JPX_CORPORATE_GOVERNANCE_RESULTS_FORM_PATTERN.search(html_text)
    if match is None:
        return {}
    body = match.group("body")
    fields: dict[str, str] = {}
    for input_match in _JPX_CORPORATE_GOVERNANCE_HIDDEN_INPUT_PATTERN.finditer(body):
        name = _normalize_whitespace(input_match.group("name"))
        if not name:
            continue
        fields[name] = _normalize_whitespace(html.unescape(input_match.group("value")))
    return fields


def _extract_japan_corporate_governance_page_count(html_text: str) -> int:
    total_match = _JPX_CORPORATE_GOVERNANCE_TOTAL_COUNT_PATTERN.search(html_text)
    page_size_match = _JPX_CORPORATE_GOVERNANCE_PAGE_SIZE_PATTERN.search(html_text)
    try:
        total_count = int(total_match.group("count")) if total_match is not None else 0
        page_size = int(page_size_match.group("size")) if page_size_match is not None else 0
    except ValueError:
        return 1
    if total_count <= 0 or page_size <= 0:
        return 1
    return max(1, (total_count + page_size - 1) // page_size)


def _extract_japan_corporate_governance_report_records(
    html_text: str,
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for match in _JPX_CORPORATE_GOVERNANCE_RESULT_ROW_PATTERN.finditer(html_text):
        raw_code = _normalize_whitespace(match.group("raw_code")).upper()
        code = _normalize_japan_jpx_company_code(raw_code)
        company_name = _normalize_japan_company_display_name(
            html.unescape(match.group("company_name"))
        )
        report_date = _normalize_japan_governance_report_date(match.group("report_date"))
        heading = _normalize_whitespace(
            _strip_html(html.unescape(match.group("heading")))
        )
        report_url = normalize_source_url(
            urljoin(
                _JPX_CORPORATE_GOVERNANCE_RESULTS_BASE_URL,
                html.unescape(match.group("report_url")),
            )
        )
        if not code or not report_url:
            continue
        record_key = (code, report_url)
        if record_key in seen:
            continue
        seen.add(record_key)
        records.append(
            {
                "code": code,
                "raw_code": raw_code,
                "company_name": company_name,
                "report_date": report_date,
                "heading": heading,
                "report_url": report_url,
            }
        )
    return records


def _normalize_japan_jpx_company_code(value: str) -> str:
    code = _normalize_whitespace(value).upper()
    if len(code) == 5 and code.endswith("0"):
        return code[:-1]
    return code


def _normalize_japan_jpx_governance_code(value: str) -> str:
    return _normalize_japan_jpx_company_code(value)


def _normalize_japan_governance_report_date(value: str) -> str:
    normalized = _normalize_whitespace(value)
    match = re.fullmatch(r"(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})", normalized)
    if match is None:
        return normalized
    return f"{match.group('year')}-{match.group('month')}-{match.group('day')}"


def _select_latest_japan_governance_report_records(
    records: list[dict[str, str]],
) -> dict[str, dict[str, str]]:
    latest: dict[str, dict[str, str]] = {}
    for record in records:
        code = _normalize_whitespace(str(record.get("code", ""))).upper()
        if not code:
            continue
        existing = latest.get(code)
        if existing is None or _japan_governance_report_sort_key(
            record.get("report_date", "")
        ) > _japan_governance_report_sort_key(existing.get("report_date", "")):
            latest[code] = record
    return latest


def _japan_governance_report_sort_key(value: str) -> tuple[int, int, int]:
    normalized = _normalize_japan_governance_report_date(value)
    match = re.fullmatch(r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})", normalized)
    if match is None:
        return (0, 0, 0)
    return (
        int(match.group("year")),
        int(match.group("month")),
        int(match.group("day")),
    )


def _merge_japan_governance_report_metadata(
    *,
    entity: dict[str, object],
    governance_record: dict[str, str],
) -> None:
    metadata = entity.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        entity["metadata"] = metadata
    report_url = _normalize_whitespace(str(governance_record.get("report_url", "")))
    report_date = _normalize_whitespace(str(governance_record.get("report_date", "")))
    heading = _normalize_whitespace(str(governance_record.get("heading", "")))
    if report_url:
        metadata["corporate_governance_report_url"] = report_url
    if report_date:
        metadata["corporate_governance_report_date"] = report_date
    if heading:
        metadata["corporate_governance_report_heading"] = heading


def _extract_japan_corporate_governance_people_entities(
    *,
    governance_records: list[dict[str, str]],
    issuer_records_by_code: dict[str, dict[str, str]],
    country_dir: Path,
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
    warnings: list[str],
) -> list[dict[str, object]]:
    people_entities: list[dict[str, object]] = []
    report_dir = country_dir / "jpx-corporate-governance-reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    existing_report_sources = {
        str(source.get("name")): source
        for source in downloaded_sources
        if str(source.get("name", "")).startswith("jpx-corporate-governance-report-")
    }
    for code, governance_record in _select_latest_japan_governance_report_records(
        governance_records
    ).items():
        issuer_record = issuer_records_by_code.get(code)
        if issuer_record is None:
            continue
        source_name = f"jpx-corporate-governance-report-{code.casefold()}"
        existing_source = existing_report_sources.get(source_name)
        if isinstance(existing_source, dict):
            report_path = country_dir / str(existing_source.get("path", ""))
        else:
            report_path = report_dir / f"{code.casefold()}.pdf"
            report_url = _normalize_whitespace(str(governance_record.get("report_url", "")))
            if not report_url:
                continue
            try:
                resolved_report_url = _download_source(
                    report_url,
                    report_path,
                    user_agent=user_agent,
                )
            except Exception as exc:
                warning_text = f"JPX governance report download failed for {code}: {exc}"
                if warning_text not in warnings:
                    warnings.append(warning_text)
                continue
            downloaded_sources.append(
                {
                    "name": source_name,
                    "source_url": resolved_report_url,
                    "path": str(report_path.relative_to(country_dir)),
                    "category": "issuer_profile",
                    "notes": (
                        f"jpx_company_code={code}; report_date="
                        f"{_normalize_whitespace(str(governance_record.get('report_date', '')))}"
                    ),
                }
            )
        if not report_path.exists():
            continue
        report_text = _extract_japan_corporate_governance_report_text(
            report_path,
            warnings=warnings,
        )
        if not report_text:
            continue
        people_entities.extend(
            _extract_people_from_japan_corporate_governance_report(
                text=report_text,
                issuer_record=issuer_record,
                source_name=source_name,
                report_url=_normalize_whitespace(
                    str(governance_record.get("report_url", ""))
                ),
                report_date=_normalize_whitespace(
                    str(governance_record.get("report_date", ""))
                ),
                report_heading=_normalize_whitespace(
                    str(governance_record.get("heading", ""))
                ),
            )
        )
    return _dedupe_entities(people_entities)


def _extract_japan_corporate_governance_report_text(
    report_path: Path,
    *,
    warnings: list[str],
) -> str:
    try:
        raw_bytes = report_path.read_bytes()
    except OSError as exc:
        warning_text = f"JPX governance report read failed for {report_path.name}: {exc}"
        if warning_text not in warnings:
            warnings.append(warning_text)
        return ""
    if not raw_bytes.lstrip().startswith(b"%PDF"):
        return raw_bytes.decode("utf-8", errors="ignore")
    pdftotext_path = shutil.which("pdftotext")
    if not pdftotext_path:
        warning_text = "pdftotext is required to extract Japan governance report text"
        if warning_text not in warnings:
            warnings.append(warning_text)
        return ""
    text_path = report_path.with_suffix(report_path.suffix + ".txt")
    result = subprocess.run(
        [pdftotext_path, str(report_path), str(text_path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if result.returncode != 0 or not text_path.exists():
        warning_text = _normalize_whitespace(result.stderr) or (
            f"pdftotext failed for {report_path.name}"
        )
        if warning_text not in warnings:
            warnings.append(warning_text)
        return ""
    return text_path.read_text(encoding="utf-8", errors="ignore")


def _extract_people_from_japan_corporate_governance_report(
    *,
    text: str,
    issuer_record: dict[str, str],
    source_name: str,
    report_url: str,
    report_date: str,
    report_heading: str,
) -> list[dict[str, object]]:
    employer_name = _normalize_whitespace(str(issuer_record.get("company_name", "")))
    employer_ticker = _normalize_whitespace(str(issuer_record.get("code", ""))).upper()
    if not employer_name or not employer_ticker:
        return []
    lines = [
        _normalize_whitespace(line)
        for line in text.replace("\f", "\n").splitlines()
        if _normalize_whitespace(line)
    ]
    entities: list[dict[str, object]] = []
    seen_entity_ids: set[str] = set()
    top_lines = lines[:_JPX_CORPORATE_GOVERNANCE_TOP_LINE_LIMIT]
    for index, line in enumerate(top_lines[:-1]):
        candidate_name = _clean_japan_governance_person_name(line)
        if candidate_name is None or not _looks_like_person_name(candidate_name):
            continue
        role_title = ""
        role_class = None
        for lookahead in (1, 2):
            role_candidate = _normalize_whitespace(
                " ".join(top_lines[index + 1 : index + 1 + lookahead])
            )
            role_class = _role_class_for_japan_governance_title(role_candidate)
            if role_class is not None:
                role_title = role_candidate
                break
        if role_class is None or not role_title:
            continue
        entity = _build_japan_governance_person_entity(
            raw_name=candidate_name,
            role_title=role_title,
            role_class=role_class,
            employer_name=employer_name,
            employer_ticker=employer_ticker,
            source_name=source_name,
            report_url=report_url,
            report_date=report_date,
            report_heading=report_heading,
        )
        entity_id = str(entity.get("entity_id", ""))
        if entity_id and entity_id not in seen_entity_ids:
            seen_entity_ids.add(entity_id)
            entities.append(entity)
    contact_match = _JPX_CORPORATE_GOVERNANCE_CONTACT_PATTERN.search(text)
    if contact_match is not None:
        contact_name = _clean_japan_governance_person_name(contact_match.group("name"))
        if contact_name is not None and _looks_like_person_name(contact_name):
            contact_details = _normalize_whitespace(contact_match.group("details"))
            if _role_class_for_japan_governance_title(contact_details) == "investor_relations":
                contact_entity = _build_japan_governance_person_entity(
                    raw_name=contact_name,
                    role_title="Investor Relations Contact",
                    role_class="investor_relations",
                    employer_name=employer_name,
                    employer_ticker=employer_ticker,
                    source_name=source_name,
                    report_url=report_url,
                    report_date=report_date,
                    report_heading=report_heading,
                )
                if contact_details:
                    contact_entity.setdefault("metadata", {})["contact_details"] = (
                        contact_details
                    )
                entity_id = str(contact_entity.get("entity_id", ""))
                if entity_id and entity_id not in seen_entity_ids:
                    seen_entity_ids.add(entity_id)
                    entities.append(contact_entity)
    return entities


def _clean_japan_governance_person_name(value: str) -> str | None:
    candidate = _normalize_whitespace(re.sub(r"^Contact:\s*", "", value, flags=re.IGNORECASE))
    if not candidate:
        return None
    lowered = candidate.casefold()
    if any(
        token in lowered
        for token in (
            "corporate governance report",
            "last update",
            "securities code",
            "phone:",
            "department",
            "basic views",
            "corporate governance of",
            "representative director",
            "representative executive officer",
            "executive officer",
            "investor relations",
            "president",
            "chairman",
            "chairperson",
            "director",
        )
    ):
        return None
    if any(
        keyword in lowered
        for keyword in (
            " corporation",
            " co.,",
            " co.",
            " company",
            " holdings",
            " group",
            " ltd",
            " inc.",
        )
    ):
        return None
    if any(character.isdigit() for character in candidate):
        return None
    if not 2 <= len(candidate.split()) <= 5:
        return None
    return _smart_titlecase_person_name(candidate)


def _role_class_for_japan_governance_title(title: str) -> str | None:
    normalized = _normalize_whitespace(_ascii_fold_alias(title) or title).casefold()
    if not normalized:
        return None
    if "investor relations" in normalized or " ir " in f" {normalized} ":
        return "investor_relations"
    if any(
        keyword in normalized
        for keyword in (
            "representative director",
            "representative executive officer",
            "executive officer",
            "president",
            "chief executive officer",
            "ceo",
            "cfo",
            "coo",
        )
    ):
        return "executive_officer"
    if any(
        keyword in normalized
        for keyword in (
            "director",
            "chairman",
            "chairperson",
            "board member",
            "audit & supervisory board",
            "audit and supervisory board",
            "outside director",
        )
    ):
        return "director"
    return None


def _build_japan_governance_person_entity(
    *,
    raw_name: str,
    role_title: str,
    role_class: str,
    employer_name: str,
    employer_ticker: str,
    source_name: str,
    report_url: str,
    report_date: str,
    report_heading: str,
) -> dict[str, object]:
    canonical_name, aliases = _normalize_person_name_aliases(raw_name)
    metadata: dict[str, object] = {
        "country_code": "jp",
        "category": role_class,
        "role_title": role_title,
        "employer_name": employer_name,
        "employer_ticker": employer_ticker,
        "source_name": source_name,
        "report_url": report_url,
        "section_name": "corporate governance report",
    }
    if report_date:
        metadata["report_date"] = report_date
    if report_heading:
        metadata["report_heading"] = report_heading
    return {
        "entity_type": "person",
        "canonical_text": canonical_name,
        "aliases": _unique_aliases(aliases),
        "entity_id": (
            f"finance-jp-person:{(employer_ticker or _slug(employer_name))}:"
            f"{_slug(canonical_name)}:{_slug(role_class)}"
        ),
        "metadata": metadata,
    }


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


def _extract_france_euronext_directory_records(
    html_text: str,
    *,
    listing_segment: str,
    base_url: str = "https://live.euronext.com",
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row_match in _EURONEXT_DIRECTORY_ROW_PATTERN.finditer(html_text):
        row_html = row_match.group("row")
        link_match = _EURONEXT_DIRECTORY_LINK_PATTERN.search(row_html)
        if link_match is None:
            continue
        symbol_match = _EURONEXT_DIRECTORY_SYMBOL_PATTERN.search(row_html)
        if symbol_match is None:
            continue
        raw_company_name = _normalize_whitespace(_strip_html(link_match.group("company_name")))
        isin = _normalize_whitespace(link_match.group("isin")).upper()
        symbol = _normalize_whitespace(_strip_html(symbol_match.group("value"))).upper()
        market_match = _EURONEXT_DIRECTORY_MARKET_PATTERN.search(row_html)
        market_code = _normalize_whitespace(
            _strip_html(market_match.group("value")) if market_match is not None else ""
        ).upper() or _normalize_whitespace(link_match.group("market_code")).upper()
        if not raw_company_name or not isin or not symbol or not market_code:
            continue
        record_key = (isin, symbol)
        if record_key in seen:
            continue
        seen.add(record_key)
        company_name = _normalize_whitespace(
            _smart_titlecase_company_name(raw_company_name)
        )
        records.append(
            {
                "company_name": company_name,
                "raw_company_name": raw_company_name,
                "isin": isin,
                "symbol": symbol,
                "market_code": market_code,
                "listing_segment": listing_segment,
                "product_url": urljoin(base_url, link_match.group("path")),
            }
        )
    return sorted(
        records,
        key=lambda item: (
            str(item["company_name"]).casefold(),
            str(item["symbol"]).casefold(),
        ),
    )


def _merge_france_euronext_records(
    records: list[dict[str, str]],
) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    for record in records:
        isin = _normalize_whitespace(str(record.get("isin", ""))).upper()
        if not isin:
            continue
        existing = merged.get(isin)
        if existing is None:
            merged[isin] = dict(record)
            continue
        for key in (
            "company_name",
            "raw_company_name",
            "symbol",
            "market_code",
            "listing_segment",
            "product_url",
        ):
            value = _normalize_whitespace(str(record.get(key, "")))
            if value and not _normalize_whitespace(str(existing.get(key, ""))):
                existing[key] = value
    return sorted(
        merged.values(),
        key=lambda item: (
            str(item["company_name"]).casefold(),
            str(item["symbol"]).casefold(),
        ),
    )


def _build_france_issuer_lookup(
    issuer_records: list[dict[str, str]],
) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    for record in issuer_records:
        company_name = _normalize_whitespace(str(record.get("company_name", "")))
        raw_company_name = _normalize_whitespace(str(record.get("raw_company_name", "")))
        symbol = _normalize_whitespace(str(record.get("symbol", ""))).upper()
        isin = _normalize_whitespace(str(record.get("isin", ""))).upper()
        alias_candidates = [
            *_build_france_company_aliases(raw_company_name, company_name),
            symbol,
            isin,
        ]
        for alias in alias_candidates:
            for key in {
                _normalize_company_name_for_match(alias),
                _normalize_company_name_for_match(alias, drop_suffixes=True),
            }:
                if key:
                    lookup.setdefault(key, record)
    return lookup


def _lookup_france_issuer_record(
    employer_name: str,
    issuer_lookup: dict[str, dict[str, str]] | None,
) -> dict[str, str] | None:
    if not issuer_lookup:
        return None
    for key in (
        _normalize_company_name_for_match(employer_name),
        _normalize_company_name_for_match(employer_name, drop_suffixes=True),
    ):
        if key and key in issuer_lookup:
            return issuer_lookup[key]
    return None


def _extract_france_company_news_records(
    html_text: str,
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    seen_node_ids: set[str] = set()
    for row_match in _FRANCE_COMPANY_NEWS_TABLE_ROW_PATTERN.finditer(html_text):
        row_html = row_match.group("row")
        node_match = re.search(
            (
                r'class=["\'][^"\']*standardRightCompanyPressRelease[^"\']*["\'][^>]*'
                r'data-node-nid=["\'](?P<node_nid>\d+)["\'][^>]*>(?P<title>.*?)</a>'
            ),
            row_html,
            re.IGNORECASE | re.DOTALL,
        )
        if node_match is None:
            continue
        node_nid = str(node_match.group("node_nid")).strip()
        if not node_nid or node_nid in seen_node_ids:
            continue
        company_match = re.search(
            (
                r'<td[^>]*class=["\'][^"\']*views-field-field-company-name[^"\']*["\'][^>]*>'
                r'(?P<value>.*?)</td>'
            ),
            row_html,
            re.IGNORECASE | re.DOTALL,
        )
        company_name = _normalize_whitespace(
            _smart_titlecase_company_name(
                _strip_html(company_match.group("value")) if company_match is not None else ""
            )
        )
        if not company_name:
            continue
        category_match = re.search(
            (
                r'<td[^>]*class=["\'][^"\']*views-field-field-company-press-releases[^"\']*["\'][^>]*>'
                r'(?P<value>.*?)</td>'
            ),
            row_html,
            re.IGNORECASE | re.DOTALL,
        )
        published_match = re.search(
            (
                r'<td[^>]*class=["\'][^"\']*views-field-field-company-pr-pub-datetime[^"\']*["\'][^>]*>'
                r'(?P<value>.*?)</td>'
            ),
            row_html,
            re.IGNORECASE | re.DOTALL,
        )
        seen_node_ids.add(node_nid)
        records.append(
            {
                "node_nid": node_nid,
                "issuer_name": company_name,
                "title": _normalize_whitespace(_strip_html(node_match.group("title"))),
                "category": _normalize_whitespace(
                    _strip_html(category_match.group("value"))
                    if category_match is not None
                    else ""
                ),
                "published_at": _normalize_whitespace(
                    (
                        _strip_html(published_match.group("value"))
                        if published_match is not None
                        else ""
                    ).replace(" ,", ",")
                ),
                "sequence": str(len(records)),
            }
        )
    return records


def _extract_france_company_news_last_page(html_text: str) -> int:
    page_numbers = [
        int(match.group("page"))
        for match in re.finditer(r"[?&]page=(?P<page>\d+)", html_text, re.IGNORECASE)
    ]
    if not page_numbers:
        return 1
    return max(page_numbers) + 1


def _download_france_company_news_page(
    source_url: str | Path,
    destination: Path,
    *,
    user_agent: str,
) -> str:
    return _download_source(source_url, destination, user_agent=user_agent)


def _score_france_company_news_record(record: dict[str, str]) -> int:
    title = _normalize_company_name_for_match(record.get("title", ""))
    category = _normalize_company_name_for_match(record.get("category", ""))
    score = 0
    for needle, weight in _FRANCE_COMPANY_NEWS_TITLE_SCORES:
        if needle in title:
            score = max(score, weight)
    if "legal" in category:
        score += 5
    if "financial" in category:
        score += 5
    if "share history" in category:
        score -= 5
    if "holding" in category:
        score -= 5
    return score


def _select_france_company_news_candidate_releases(
    records: list[dict[str, str]],
    *,
    issuer_lookup: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    best_by_issuer: dict[str, tuple[int, int, dict[str, str]]] = {}
    for record in records:
        issuer_name = _normalize_whitespace(str(record.get("issuer_name", "")))
        node_nid = _normalize_whitespace(str(record.get("node_nid", "")))
        if not issuer_name or not node_nid:
            continue
        issuer_record = _lookup_france_issuer_record(issuer_name, issuer_lookup)
        if issuer_record is None:
            continue
        issuer_key = _normalize_whitespace(str(issuer_record.get("isin", ""))).upper()
        if not issuer_key:
            issuer_key = _normalize_company_name_for_match(
                str(issuer_record.get("company_name", ""))
            )
        if not issuer_key:
            continue
        score = _score_france_company_news_record(record)
        sequence = int(str(record.get("sequence", "0")) or "0")
        current = best_by_issuer.get(issuer_key)
        if current is None or score > current[0] or (
            score == current[0] and sequence < current[1]
        ):
            best_by_issuer[issuer_key] = (score, sequence, record)
    return [
        item[2]
        for item in sorted(
            best_by_issuer.values(),
            key=lambda value: (
                _normalize_whitespace(str(value[2].get("issuer_name", ""))).casefold(),
                value[1],
            ),
        )
    ]


def _download_france_company_press_release_modal(
    node_nid: str,
    destination: Path,
    *,
    user_agent: str,
) -> str:
    modal_url = _FRANCE_COMPANY_PRESS_RELEASE_MODAL_URL_TEMPLATE.format(node_nid=node_nid)
    return _download_source(modal_url, destination, user_agent=user_agent)


def _resolve_france_press_release_issuer_record(
    html_text: str,
    *,
    record: dict[str, str],
    issuer_lookup: dict[str, dict[str, str]] | None = None,
    issuer_records_by_isin: dict[str, dict[str, str]] | None = None,
) -> dict[str, str] | None:
    isin_match = re.search(
        r'data-isin=["\'](?P<isin>[A-Z0-9]{12})["\']',
        html_text,
        re.IGNORECASE,
    )
    if isin_match is not None and issuer_records_by_isin is not None:
        matched = issuer_records_by_isin.get(
            _normalize_whitespace(isin_match.group("isin")).upper()
        )
        if matched is not None:
            return matched
    issuer_match = re.search(
        (
            r"<h3[^>]*>\s*Issuer\s*</h3>.*?"
            r'<span[^>]*class=["\'][^"\']*text-uppercase[^"\']*["\'][^>]*>(?P<name>.*?)</span>'
        ),
        html_text,
        re.IGNORECASE | re.DOTALL,
    )
    issuer_name = _normalize_whitespace(
        _smart_titlecase_company_name(
            _strip_html(issuer_match.group("name")) if issuer_match is not None else ""
        )
    )
    if issuer_name:
        matched = _lookup_france_issuer_record(issuer_name, issuer_lookup)
        if matched is not None:
            return matched
    fallback_name = _normalize_whitespace(str(record.get("issuer_name", "")))
    if fallback_name:
        return _lookup_france_issuer_record(fallback_name, issuer_lookup)
    return None


def _extract_france_press_release_employer_name(html_text: str) -> str | None:
    issuer_match = re.search(
        (
            r"<h3[^>]*>\s*Issuer\s*</h3>.*?"
            r'<span[^>]*class=["\'][^"\']*text-uppercase[^"\']*["\'][^>]*>(?P<name>.*?)</span>'
        ),
        html_text,
        re.IGNORECASE | re.DOTALL,
    )
    if issuer_match is not None:
        issuer_name = _normalize_whitespace(
            _smart_titlecase_company_name(_strip_html(issuer_match.group("name")))
        )
        if issuer_name:
            return issuer_name
    return None


def _role_class_for_france_contact_title(title: str) -> str | None:
    normalized = _normalize_company_name_for_match(title)
    if "investor" in normalized or "investisseurs" in normalized:
        return "investor_relations"
    if "presse" in normalized or "press" in normalized or "media" in normalized:
        return "press_contact"
    return None


def _clean_france_contact_name(value: str) -> str | None:
    candidate = _normalize_whitespace(value)
    if not candidate:
        return None
    if candidate.casefold() in {"contact", "contacts"}:
        return None
    candidate = re.sub(
        r"^(?:contact|contacts)\s*[:\-–—]?\s*",
        "",
        candidate,
        flags=re.IGNORECASE,
    ).strip()
    for delimiter in (" – ", " — ", " - ", "–", "—"):
        if delimiter not in candidate:
            continue
        suffix = _normalize_whitespace(candidate.split(delimiter)[-1])
        if suffix and _looks_like_person_name(suffix):
            return _smart_titlecase_person_name(suffix)
    return _smart_titlecase_person_name(candidate) if candidate else None


def _extract_france_company_press_release_people_entities(
    html_text: str,
    *,
    issuer_record: dict[str, str] | None = None,
    source_name: str = "euronext-company-press-release",
) -> list[dict[str, object]]:
    employer_name = (
        _normalize_whitespace(str(issuer_record.get("company_name", "")))
        if issuer_record is not None
        else _extract_france_press_release_employer_name(html_text) or ""
    )
    employer_ticker = (
        _normalize_whitespace(str(issuer_record.get("symbol", ""))).upper()
        if issuer_record is not None
        else ""
    )
    employer_isin = (
        _normalize_whitespace(str(issuer_record.get("isin", ""))).upper()
        if issuer_record is not None
        else ""
    )
    if not employer_isin:
        isin_match = re.search(
            r'data-isin=["\'](?P<isin>[A-Z0-9]{12})["\']',
            html_text,
            re.IGNORECASE,
        )
        if isin_match is not None:
            employer_isin = _normalize_whitespace(isin_match.group("isin")).upper()
    if not employer_name:
        return []

    paragraph_matches = list(
        re.finditer(r"<p[^>]*>(?P<value>.*?)</p>", html_text, re.IGNORECASE | re.DOTALL)
    )
    entities: list[dict[str, object]] = []
    seen_entity_ids: set[str] = set()
    for index, match in enumerate(paragraph_matches[:-1]):
        name_html = match.group("value")
        next_match = paragraph_matches[index + 1]
        role_title = _normalize_whitespace(_strip_html(next_match.group("value")))
        role_class = _role_class_for_france_contact_title(role_title)
        if role_class is None:
            continue
        raw_name = _clean_france_contact_name(_strip_html(name_html))
        if raw_name is None or not _looks_like_person_name(raw_name):
            continue
        canonical_name, aliases = _normalize_person_name_aliases(raw_name)
        if not canonical_name:
            continue
        contact_details_html = (
            paragraph_matches[index + 2].group("value")
            if index + 2 < len(paragraph_matches)
            else ""
        )
        contact_details_text = _normalize_whitespace(_strip_html(contact_details_html))
        email_match = re.search(
            r'mailto:(?P<email>[^"\']+)',
            contact_details_html,
            re.IGNORECASE,
        )
        phone_match = re.search(
            r"(?:T[eé]l\.?|Phone)\s*[:.]?\s*(?P<phone>[^/|<]+)",
            contact_details_text,
            re.IGNORECASE,
        )
        entity_id = (
            f"finance-fr-person:{(employer_ticker or employer_isin or _slug(employer_name))}:"
            f"{_slug(canonical_name)}:{_slug(role_class)}"
        )
        if entity_id in seen_entity_ids:
            continue
        seen_entity_ids.add(entity_id)
        metadata: dict[str, object] = {
            "country_code": "fr",
            "category": role_class,
            "role_title": role_title,
            "employer_name": employer_name,
            "source_name": source_name,
            "section_name": "company press release contacts",
        }
        if employer_ticker:
            metadata["employer_ticker"] = employer_ticker
        if employer_isin:
            metadata["employer_isin"] = employer_isin
        if email_match is not None:
            metadata["contact_email"] = _normalize_whitespace(email_match.group("email"))
        if phone_match is not None:
            metadata["contact_phone"] = _normalize_whitespace(phone_match.group("phone"))
        entities.append(
            {
                "entity_type": "person",
                "canonical_text": canonical_name,
                "aliases": _unique_aliases(aliases),
                "entity_id": entity_id,
                "metadata": metadata,
            }
        )
    return entities


def _extract_france_company_press_release_candidate_urls(
    html_text: str,
    *,
    base_url: str,
) -> list[str]:
    urls: list[str] = []
    blocked_hosts = {"www.security-master-key.com", "security-master-key.com"}
    for match in re.finditer(
        r'<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<label>.*?)</a>',
        html_text,
        re.IGNORECASE | re.DOTALL,
    ):
        href = _normalize_whitespace(match.group("href"))
        if not href or href.startswith(("mailto:", "tel:", "#")):
            continue
        resolved_url = urljoin(base_url, href)
        parsed = urlparse(resolved_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if (parsed.hostname or "").casefold() in blocked_hosts:
            continue
        if Path(parsed.path).suffix.lower() in _FRANCE_BLOCKED_BINARY_SUFFIXES:
            continue
        link_text = _normalize_whitespace(_strip_html(match.group("label")))
        normalized_link_text = _normalize_company_name_for_match(link_text)
        normalized_url = resolved_url.casefold()
        if parsed.netloc.casefold() == "live.euronext.com":
            continue
        if (
            any(keyword in normalized_url for keyword in _FRANCE_GOVERNANCE_URL_KEYWORDS)
            or any(
                keyword in normalized_link_text
                for keyword in _FRANCE_GOVERNANCE_LINK_TEXT_KEYWORDS
            )
            or "finance" in normalized_url
            or "invest" in normalized_url
        ):
            urls.append(resolved_url)
            continue
        if link_text.lower().startswith(("www.", "http")):
            urls.append(resolved_url)
    return _unique_aliases(urls)


def _extract_france_filing_candidate_urls(
    html_text: str,
    *,
    base_url: str,
) -> list[str]:
    urls: list[str] = []
    for match in re.finditer(
        r'<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<label>.*?)</a>',
        html_text,
        re.IGNORECASE | re.DOTALL,
    ):
        href = _normalize_whitespace(match.group("href"))
        if not href or href.startswith(("mailto:", "tel:", "#")):
            continue
        resolved_url = urljoin(base_url, href)
        parsed = urlparse(resolved_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        suffix = Path(parsed.path).suffix.lower()
        if suffix in _FRANCE_BLOCKED_BINARY_SUFFIXES:
            continue
        if suffix and suffix not in {".html", ".htm", ".xhtml", ".xml"}:
            continue
        if (parsed.hostname or "").casefold() == "live.euronext.com":
            continue
        link_text = _normalize_whitespace(_strip_html(match.group("label")))
        normalized_link_text = _normalize_company_name_for_match(link_text)
        normalized_url = resolved_url.casefold()
        if (
            "info-financiere.fr" in normalized_url
            or any(keyword in normalized_url for keyword in _FRANCE_FILING_URL_KEYWORDS)
            or any(
                keyword in normalized_link_text
                for keyword in _FRANCE_FILING_LINK_TEXT_KEYWORDS
            )
        ):
            urls.append(resolved_url)
    return _unique_aliases(urls)


def _download_france_filing_document(
    source_url: str | Path,
    destination: Path,
    *,
    user_agent: str,
) -> str:
    return _download_source(source_url, destination, user_agent=user_agent)


def _extract_france_filing_people_entities(
    html_text: str,
    *,
    issuer_lookup: dict[str, dict[str, str]] | None = None,
    issuer_record: dict[str, str] | None = None,
    source_name: str = "official-filing-document",
) -> list[dict[str, object]]:
    return _extract_france_governance_people_entities(
        html_text,
        issuer_lookup=issuer_lookup,
        issuer_record=issuer_record,
        source_name=source_name,
    )


def _download_france_governance_page(
    source_url: str | Path,
    destination: Path,
    *,
    user_agent: str,
) -> str:
    return _download_source(source_url, destination, user_agent=user_agent)


def _stable_url_file_stem(source_url: str) -> str:
    parsed = urlparse(source_url)
    stem_parts = [
        _slug(parsed.hostname or "url"),
        _slug(Path(parsed.path).stem or "page"),
    ]
    digest = sha256(source_url.encode("utf-8")).hexdigest()[:12]
    return "-".join(part for part in stem_parts if part) + f"-{digest}"


def _extract_france_governance_candidate_urls(
    html_text: str,
    *,
    base_url: str,
) -> list[str]:
    urls: list[str] = []
    if _iter_france_governance_sections(html_text):
        urls.append(base_url)
    base_host = (urlparse(base_url).hostname or "").casefold()
    for match in re.finditer(
        r'<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<label>.*?)</a>',
        html_text,
        re.IGNORECASE | re.DOTALL,
    ):
        href = _normalize_whitespace(match.group("href"))
        if not href or href.startswith(("mailto:", "tel:", "#")):
            continue
        resolved_url = urljoin(base_url, href)
        parsed = urlparse(resolved_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if Path(parsed.path).suffix.lower() in _FRANCE_BLOCKED_BINARY_SUFFIXES:
            continue
        link_text = _normalize_company_name_for_match(_strip_html(match.group("label")))
        normalized_url = resolved_url.casefold()
        if (
            any(keyword in normalized_url for keyword in _FRANCE_GOVERNANCE_URL_KEYWORDS)
            or any(
                keyword in link_text for keyword in _FRANCE_GOVERNANCE_LINK_TEXT_KEYWORDS
            )
        ) and (not base_host or (parsed.hostname or "").casefold() == base_host):
            urls.append(resolved_url)
    return _unique_aliases(urls)


def _extract_france_governance_people_entities(
    html_text: str,
    *,
    issuer_lookup: dict[str, dict[str, str]] | None = None,
    issuer_record: dict[str, str] | None = None,
    source_name: str = "official-governance-page",
) -> list[dict[str, object]]:
    employer_record = issuer_record
    if employer_record is None:
        employer_name = _extract_france_governance_employer_name(html_text)
        if employer_name:
            employer_record = _lookup_france_issuer_record(employer_name, issuer_lookup)
    resolved_employer_name = (
        _normalize_whitespace(str(employer_record.get("company_name", "")))
        if employer_record is not None
        else _extract_france_governance_employer_name(html_text) or ""
    )
    employer_ticker = (
        _normalize_whitespace(str(employer_record.get("symbol", ""))).upper()
        if employer_record is not None
        else ""
    )
    employer_isin = (
        _normalize_whitespace(str(employer_record.get("isin", ""))).upper()
        if employer_record is not None
        else ""
    )
    if not resolved_employer_name:
        return []

    entities: list[dict[str, object]] = []
    for section_title, role_class, section_body in _iter_france_governance_sections(html_text):
        for raw_name, raw_role in _extract_france_governance_pairs(section_body):
            canonical_name, aliases = _normalize_person_name_aliases(raw_name)
            if not canonical_name:
                continue
            role_title = _normalize_whitespace(raw_role or section_title) or section_title
            metadata: dict[str, object] = {
                "country_code": "fr",
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
                        f"finance-fr-person:{(employer_ticker or employer_isin or _slug(resolved_employer_name))}:"
                        f"{_slug(canonical_name)}:{_slug(role_class)}"
                    ),
                    "metadata": metadata,
                }
            )
    return entities


def _iter_france_governance_sections(
    html_text: str,
) -> list[tuple[str, str, str]]:
    heading_pattern = re.compile(
        (
            r"<h[1-6][^>]*>(?P<heading>.*?)</h[1-6]>"
            r"|<p[^>]*>\s*<strong[^>]*>(?P<strong_heading>.*?)</strong>\s*</p>"
        ),
        re.IGNORECASE | re.DOTALL,
    )
    governance_headings: list[tuple[int, int, str, str]] = []
    for match in heading_pattern.finditer(html_text):
        raw_heading = match.group("heading") or match.group("strong_heading") or ""
        section_title = _normalize_whitespace(_strip_html(raw_heading))
        role_class = _role_class_for_france_governance_heading(section_title)
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


def _extract_france_governance_employer_name(html_text: str) -> str | None:
    for pattern in (
        r"<title[^>]*>(?P<value>.*?)</title>",
        r"<h1[^>]*>(?P<value>.*?)</h1>",
    ):
        match = re.search(pattern, html_text, re.IGNORECASE | re.DOTALL)
        if match is None:
            continue
        value = _normalize_whitespace(_strip_html(match.group("value")))
        if not value:
            continue
        candidates = [value]
        if " - " in value:
            candidates.extend(segment.strip() for segment in value.split(" - "))
        if "|" in value:
            candidates.extend(segment.strip() for segment in value.split("|"))
        for candidate in candidates:
            cleaned = _clean_france_governance_employer_candidate(candidate)
            if cleaned:
                return cleaned
    return None


def _clean_france_governance_employer_candidate(value: str) -> str | None:
    candidate = _normalize_whitespace(value)
    if not candidate:
        return None
    cleaned = re.sub(
        (
            r"^(?:gouvernance|governance|conseil d'administration|conseil de surveillance|"
            r"directoire|comité exécutif|comite executif|executive committee)\s*(?:-|:)?\s*"
        ),
        "",
        candidate,
        flags=re.IGNORECASE,
    ).strip()
    if cleaned != candidate:
        return cleaned or None
    cleaned = re.sub(
        (
            r"^(?:gouvernance|governance)\s*(?:de|du|des|for|of)\s+"
        ),
        "",
        candidate,
        flags=re.IGNORECASE,
    ).strip()
    if cleaned != candidate:
        return cleaned or None
    normalized = candidate.casefold()
    if normalized in {"gouvernance", "governance"}:
        return None
    if any(
        heading in normalized
        for headings in _FRANCE_GOVERNANCE_ROLE_HEADINGS.values()
        for heading in headings
    ):
        return None
    return candidate


def _role_class_for_france_governance_heading(section_title: str) -> str | None:
    normalized = section_title.casefold()
    for role_class, headings in _FRANCE_GOVERNANCE_ROLE_HEADINGS.items():
        if any(heading in normalized for heading in headings):
            return role_class
    return None


def _extract_france_governance_pairs(section_body: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for match in _FRANCE_GOVERNANCE_CARD_PATTERN.finditer(section_body):
        raw_name = _smart_titlecase_person_name(
            _normalize_whitespace(_strip_html(match.group("name")))
        )
        raw_role = _normalize_whitespace(_strip_html(match.group("role")))
        if not raw_name or not raw_role:
            continue
        pair = (raw_name, raw_role)
        if pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    paragraph_values = [
        _normalize_whitespace(_strip_html(match.group("value")))
        for match in re.finditer(
            r"<p[^>]*>(?P<value>.*?)</p>",
            section_body,
            re.IGNORECASE | re.DOTALL,
        )
    ]
    for index, paragraph in enumerate(paragraph_values[:-1]):
        cleaned_name = _clean_france_contact_name(paragraph)
        if cleaned_name is None or not _looks_like_person_name(cleaned_name):
            continue
        raw_role = paragraph_values[index + 1]
        if (
            not raw_role
            or raw_role.casefold() in {"contact", "contacts"}
            or _looks_like_person_name(raw_role)
        ):
            continue
        pair = (cleaned_name, raw_role)
        if pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    for match in re.finditer(
        r"<li[^>]*>(?P<value>.*?)</li>",
        section_body,
        re.IGNORECASE | re.DOTALL,
    ):
        pair = _split_france_governance_list_item(_strip_html(match.group("value")))
        if pair is None or pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    return pairs


def _split_france_governance_list_item(value: str) -> tuple[str, str] | None:
    candidate = _normalize_whitespace(value)
    if not candidate:
        return None
    for delimiter in (" - ", " – ", " — ", ", "):
        if delimiter not in candidate:
            continue
        name_part, role_part = candidate.split(delimiter, 1)
        cleaned_name = _clean_france_contact_name(name_part)
        role_title = _normalize_whitespace(role_part)
        if cleaned_name and role_title and _looks_like_person_name(cleaned_name):
            return cleaned_name, role_title
    return None


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


def _enrich_argentina_entities_with_wikidata(
    *,
    country_dir: Path,
    issuer_entities: list[dict[str, object]],
    ticker_entities: list[dict[str, object]],
    people_entities: list[dict[str, object]],
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[str]:
    resolution_payload, resolution_path, warnings = (
        _build_argentina_wikidata_resolution_snapshot(
            issuer_entities=issuer_entities,
            people_entities=people_entities,
            country_dir=country_dir,
            user_agent=user_agent,
        )
    )
    if resolution_path is None:
        return warnings
    downloaded_sources.append(
        {
            "name": "wikidata-argentina-entity-resolution",
            "source_url": _WIKIDATA_API_URL,
            "path": str(resolution_path.relative_to(country_dir)),
            "category": "knowledge_graph",
            "notes": "Wikidata QID resolution snapshot for Argentina issuers and related people.",
        }
    )
    issuer_matches = resolution_payload.get("issuers", {})
    if isinstance(issuer_matches, dict):
        for entity in issuer_entities:
            match = issuer_matches.get(_argentina_wikidata_target_key(entity))
            if isinstance(match, dict):
                _merge_wikidata_metadata(entity=entity, match=match)
        for entity in ticker_entities:
            match = issuer_matches.get(_argentina_wikidata_target_key(entity))
            if isinstance(match, dict):
                _merge_wikidata_metadata(entity=entity, match=match)
    people_matches = resolution_payload.get("people", {})
    if isinstance(people_matches, dict):
        for entity in people_entities:
            match = people_matches.get(str(entity.get("entity_id", "")))
            if isinstance(match, dict):
                _merge_wikidata_metadata(entity=entity, match=match)
    return warnings


def _build_argentina_wikidata_resolution_snapshot(
    *,
    issuer_entities: list[dict[str, object]],
    people_entities: list[dict[str, object]],
    country_dir: Path,
    user_agent: str,
) -> tuple[dict[str, object], Path | None, list[str]]:
    warnings: list[str] = []
    issuer_matches, issuer_warnings = _resolve_argentina_issuer_wikidata_matches(
        issuer_entities=issuer_entities,
        user_agent=user_agent,
    )
    warnings.extend(issuer_warnings)
    people_matches, people_warnings = _resolve_argentina_person_wikidata_matches(
        people_entities=people_entities,
        issuer_matches=issuer_matches,
        user_agent=user_agent,
    )
    warnings.extend(people_warnings)
    payload: dict[str, object] = {
        "schema_version": 1,
        "country_code": "ar",
        "generated_at": _utc_timestamp(),
        "issuers": issuer_matches,
        "people": people_matches,
    }
    if not issuer_matches and not people_matches:
        return payload, None, warnings
    resolution_path = country_dir / "wikidata-argentina-entity-resolution.json"
    resolution_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload, resolution_path, warnings


def _resolve_argentina_issuer_wikidata_matches(
    *,
    issuer_entities: list[dict[str, object]],
    user_agent: str,
) -> tuple[dict[str, dict[str, object]], list[str]]:
    search_cache: dict[str, list[dict[str, Any]]] = {}
    target_candidates: dict[str, list[str]] = {}
    warnings: list[str] = []
    for entity in issuer_entities:
        resolution_key = _argentina_wikidata_target_key(entity)
        if not resolution_key:
            continue
        candidates: list[str] = []
        for query in _build_argentina_wikidata_search_queries(entity, entity_type="issuer"):
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
            target_candidates[resolution_key] = _unique_aliases(candidates)
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
        resolution_key = _argentina_wikidata_target_key(entity)
        if not resolution_key:
            continue
        candidate_ids = target_candidates.get(resolution_key, [])
        if not candidate_ids:
            continue
        match = _select_best_argentina_issuer_wikidata_match(
            entity=entity,
            candidate_ids=candidate_ids,
            entity_payloads=entity_payloads,
        )
        if match is not None:
            matches[resolution_key] = match
    return matches, warnings


def _argentina_wikidata_target_key(entity: dict[str, object]) -> str:
    metadata = entity.get("metadata")
    if isinstance(metadata, dict):
        isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
        if isin:
            return isin
    entity_id = _normalize_whitespace(str(entity.get("entity_id", "")))
    if entity_id:
        return entity_id
    return _normalize_whitespace(str(entity.get("canonical_text", "")))


def _resolve_argentina_person_wikidata_matches(
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
        for query in _build_argentina_wikidata_search_queries(entity, entity_type="person"):
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
            if _has_exact_argentina_person_search_hit(entity, search_cache[query]):
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
        match = _select_best_argentina_person_wikidata_match(
            entity=entity,
            candidate_ids=candidate_ids,
            entity_payloads=entity_payloads,
            issuer_matches=issuer_matches,
        )
        if match is not None:
            matches[entity_id] = match
    return matches, warnings


def _build_argentina_wikidata_search_queries(
    entity: dict[str, object],
    *,
    entity_type: str,
) -> list[str]:
    queries: list[str] = []
    query_candidates = [str(entity.get("canonical_text", ""))]
    for alias in entity.get("aliases", []):
        if isinstance(alias, str):
            query_candidates.append(alias)
    if entity_type == "person":
        query_candidates = [
            variant
            for candidate in query_candidates
            for variant in _build_argentina_person_name_query_variants(candidate)
        ]
    for candidate in query_candidates:
        cleaned_candidate = _normalize_whitespace(candidate)
        if not cleaned_candidate or cleaned_candidate in queries:
            continue
        if entity_type == "issuer":
            if cleaned_candidate.isupper() and len(cleaned_candidate) <= 4:
                continue
            if re.fullmatch(r"[A-Z]{2}\d{9,}[A-Z0-9]", cleaned_candidate):
                continue
        queries.append(cleaned_candidate)
        if len(queries) >= 4:
            break
    return queries


def _has_exact_argentina_person_search_hit(
    entity: dict[str, object],
    search_results: list[dict[str, Any]],
) -> bool:
    target_keys = _build_argentina_person_match_keys(entity)
    return any(
        _normalize_person_name_for_match(str(item.get("label", ""))) in target_keys
        for item in search_results
        if isinstance(item, dict)
    )


def _select_best_argentina_issuer_wikidata_match(
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
        score = _score_argentina_issuer_wikidata_candidate(
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
        alias_builder=_build_argentina_wikidata_entity_aliases,
        label_languages=("en", "es"),
    )


def _select_best_argentina_person_wikidata_match(
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
        score = _score_argentina_person_wikidata_candidate(
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
        alias_builder=_build_argentina_wikidata_entity_aliases,
        label_languages=("en", "es"),
    )


def _score_argentina_issuer_wikidata_candidate(
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
    elif target_keys.intersection(candidate_keys):
        score += 30
    string_values = _extract_wikidata_string_claim_values(candidate_payload)
    isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
    if isin and isin in string_values:
        score += 100
    ticker = _normalize_whitespace(str(metadata.get("ticker", ""))).upper()
    if ticker and ticker in string_values:
        score += 15
    candidate_description = _extract_wikidata_description(candidate_payload).casefold()
    if any(
        keyword in candidate_description
        for keyword in ("argentine", "argentinian", "company", "group", "bank", "energy")
    ):
        score += 5
    if (
        str(metadata.get("category", "")).casefold() == "regulated_institution"
        and target_full
        and target_full in candidate_keys
        and any(
            keyword in candidate_description
            for keyword in ("bank", "banco", "financial", "credit", "argentine", "argentinian")
        )
    ):
        score += 20
    return score


def _score_argentina_person_wikidata_candidate(
    *,
    entity: dict[str, object],
    candidate_payload: dict[str, Any],
    issuer_matches: dict[str, dict[str, object]],
) -> int:
    metadata = entity.get("metadata")
    if not isinstance(metadata, dict):
        return 0
    score = 0
    target_keys = _build_argentina_person_match_keys(entity)
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
        for keyword in (
            "argentine",
            "argentinian",
            "manager",
            "executive",
            "business",
            "banker",
            "chair",
            "president",
            "director",
        )
    ):
        score += 10
    return score


def _build_argentina_person_match_keys(entity: dict[str, object]) -> set[str]:
    keys = {
        _normalize_person_name_for_match(str(entity.get("canonical_text", ""))),
    }
    keys.update(
        _normalize_person_name_for_match(variant)
        for alias in entity.get("aliases", [])
        if isinstance(alias, str)
        for variant in _build_argentina_person_name_query_variants(alias)
    )
    return {key for key in keys if key}


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
    organization_matches = resolution_payload.get("organizations", {})
    if isinstance(organization_matches, dict):
        for entity in issuer_entities:
            entity_id = str(entity.get("entity_id", "")).strip()
            match = organization_matches.get(entity_id)
            if not isinstance(match, dict):
                continue
            metadata = entity.get("metadata")
            if (
                isinstance(metadata, dict)
                and metadata.get("wikidata_qid")
                and str(metadata.get("wikidata_qid", "")).strip()
                != str(match.get("qid", "")).strip()
            ):
                continue
            _merge_wikidata_metadata(entity=entity, match=match)
    people_matches = resolution_payload.get("people", {})
    if isinstance(people_matches, dict):
        for entity in people_entities:
            match = people_matches.get(str(entity.get("entity_id", "")))
            if isinstance(match, dict):
                _merge_wikidata_metadata(entity=entity, match=match)
    organization_people = resolution_payload.get("organization_people", {})
    if isinstance(organization_people, dict):
        _merge_germany_wikidata_public_people_entities(
            people_entities=people_entities,
            issuer_entities=issuer_entities,
            organization_people=organization_people,
        )
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
    organization_matches, organization_warnings = (
        _resolve_germany_organization_wikidata_matches(
            organization_entities=issuer_entities,
            user_agent=user_agent,
        )
    )
    warnings.extend(organization_warnings)
    organization_people, organization_people_warnings = (
        _build_germany_wikidata_public_people_snapshot(
            issuer_entities=issuer_entities,
            issuer_matches=issuer_matches,
            organization_matches=organization_matches,
            user_agent=user_agent,
        )
    )
    warnings.extend(organization_people_warnings)
    people_matches = _build_germany_person_wikidata_matches_from_organization_people(
        people_entities=people_entities,
        issuer_entities=issuer_entities,
        organization_people=organization_people,
    )
    payload: dict[str, object] = {
        "schema_version": 1,
        "country_code": "de",
        "generated_at": _utc_timestamp(),
        "issuers": issuer_matches,
        "organizations": organization_matches,
        "people": people_matches,
        "organization_people": organization_people,
    }
    if (
        not issuer_matches
        and not organization_matches
        and not people_matches
        and not organization_people
    ):
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
    target_candidates: dict[str, list[str]] = {}
    direct_matches: dict[str, dict[str, object]] = {}
    warnings: list[str] = []
    candidate_ids_by_isin = _query_wikidata_entity_ids_by_string_property(
        property_id=_WIKIDATA_ISIN_PROPERTY_ID,
        values=[
            _normalize_whitespace(str(entity.get("metadata", {}).get("isin", "")))
            for entity in issuer_entities
            if isinstance(entity.get("metadata"), dict)
            and _normalize_whitespace(str(entity.get("metadata", {}).get("isin", "")))
        ],
        user_agent=user_agent,
        warnings=warnings,
    )
    for entity in issuer_entities:
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
        if not isin:
            continue
        candidates = [
            candidate_id
            for candidate_id in candidate_ids_by_isin.get(isin, [])
            if candidate_id
        ]
        if not candidates:
            continue
        resolved_candidates = _unique_aliases(candidates)
        if len(resolved_candidates) == 1:
            direct_matches[isin] = _build_minimal_germany_wikidata_match(
                entity=entity,
                qid=resolved_candidates[0],
            )
            continue
        target_candidates[isin] = resolved_candidates
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
    matches.update(direct_matches)
    for entity in issuer_entities:
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
        if isin in direct_matches:
            continue
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


def _resolve_germany_organization_wikidata_matches(
    *,
    organization_entities: list[dict[str, object]],
    user_agent: str,
) -> tuple[dict[str, dict[str, object]], list[str]]:
    target_candidates: dict[str, list[str]] = {}
    direct_matches: dict[str, dict[str, object]] = {}
    warnings: list[str] = []
    candidate_ids_by_lei = _query_wikidata_entity_ids_by_string_property(
        property_id=_WIKIDATA_LEI_PROPERTY_ID,
        values=[
            _normalize_whitespace(str(entity.get("metadata", {}).get("lei", "")))
            for entity in organization_entities
            if isinstance(entity.get("metadata"), dict)
            and str(entity.get("metadata", {}).get("category", "")).casefold()
            == "regulated_institution"
        ],
        user_agent=user_agent,
        warnings=warnings,
    )
    for entity in organization_entities:
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        category = str(metadata.get("category", "")).casefold()
        if category not in {"regulated_institution", "major_shareholder_organization"}:
            continue
        entity_id = str(entity.get("entity_id", "")).strip()
        if not entity_id:
            continue
        candidates = [
            candidate_id
            for candidate_id in candidate_ids_by_lei.get(
                _normalize_whitespace(str(metadata.get("lei", ""))).upper(),
                [],
            )
            if candidate_id
        ]
        if not candidates:
            continue
        resolved_candidates = _unique_aliases(candidates)
        if len(resolved_candidates) == 1:
            direct_matches[entity_id] = _build_minimal_germany_wikidata_match(
                entity=entity,
                qid=resolved_candidates[0],
            )
            continue
        target_candidates[entity_id] = resolved_candidates
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
    matches.update(direct_matches)
    for entity in organization_entities:
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        category = str(metadata.get("category", "")).casefold()
        if category not in {"regulated_institution", "major_shareholder_organization"}:
            continue
        entity_id = str(entity.get("entity_id", "")).strip()
        if entity_id in direct_matches:
            continue
        candidate_ids = target_candidates.get(entity_id, [])
        if not candidate_ids:
            continue
        match = _select_best_germany_organization_wikidata_match(
            entity=entity,
            candidate_ids=candidate_ids,
            entity_payloads=entity_payloads,
        )
        if match is not None:
            matches[entity_id] = match
    return matches, warnings


def _build_germany_wikidata_public_people_snapshot(
    *,
    issuer_entities: list[dict[str, object]],
    issuer_matches: dict[str, dict[str, object]],
    organization_matches: dict[str, dict[str, object]],
    user_agent: str,
) -> tuple[dict[str, list[dict[str, object]]], list[str]]:
    warnings: list[str] = []
    organization_qids: dict[str, str] = {}
    for entity in issuer_entities:
        match = _match_germany_wikidata_entity(
            entity=entity,
            issuer_matches=issuer_matches,
            organization_matches=organization_matches,
        )
        if not isinstance(match, dict):
            continue
        entity_id = str(entity.get("entity_id", "")).strip()
        qid = _normalize_whitespace(str(match.get("qid", "")))
        if entity_id and qid:
            organization_qids[entity_id] = qid
    if not organization_qids:
        return {}, warnings
    organization_entity_ids_by_qid: dict[str, list[str]] = {}
    for entity_id, qid in organization_qids.items():
        organization_entity_ids_by_qid.setdefault(qid, []).append(entity_id)
    public_people_rows = _query_germany_wikidata_public_people_rows(
        organization_qids=list(organization_entity_ids_by_qid),
        user_agent=user_agent,
        warnings=warnings,
    )
    if not public_people_rows:
        return {}, warnings
    organization_people: dict[str, list[dict[str, object]]] = {}
    seen_roles: set[tuple[str, str, str, str]] = set()
    role_metadata_by_property = {
        property_id: (role_class, role_title)
        for property_id, role_class, role_title in _GERMANY_WIKIDATA_PUBLIC_PEOPLE_PROPERTIES
    }
    for row in public_people_rows:
        organization_qid = str(row.get("organization_qid", "")).strip()
        property_id = str(row.get("property_id", "")).strip()
        person_qid = str(row.get("person_qid", "")).strip()
        person_label = _normalize_whitespace(str(row.get("person_label", "")))
        if (
            not organization_qid
            or property_id not in role_metadata_by_property
            or not person_qid
            or not person_label
        ):
            continue
        role_class, role_title = role_metadata_by_property[property_id]
        canonical_name, aliases = _normalize_person_name_aliases(person_label)
        if not canonical_name:
            continue
        match = {
            "qid": person_qid,
            "slug": person_qid,
            "entity_id": f"wikidata:{person_qid}",
            "url": _WIKIDATA_ENTITY_URL_TEMPLATE.format(qid=person_qid),
            "label": person_label,
            "aliases": aliases,
        }
        for employer_entity_id in organization_entity_ids_by_qid.get(organization_qid, []):
            dedupe_key = (employer_entity_id, person_qid, role_class, role_title)
            if dedupe_key in seen_roles:
                continue
            seen_roles.add(dedupe_key)
            organization_people.setdefault(employer_entity_id, []).append(
                {
                    "canonical_text": canonical_name,
                    "aliases": aliases,
                    "role_class": role_class,
                    "role_title": role_title,
                    "source_property_id": property_id,
                    "organization_qid": organization_qid,
                    **match,
                }
            )
    for people_records in organization_people.values():
        people_records.sort(
            key=lambda item: (
                str(item.get("role_class", "")).casefold(),
                str(item.get("canonical_text", "")).casefold(),
                str(item.get("qid", "")).casefold(),
            )
        )
    return organization_people, warnings


def _build_minimal_germany_wikidata_match(
    *,
    entity: dict[str, object],
    qid: str,
) -> dict[str, object]:
    canonical_text = _normalize_whitespace(str(entity.get("canonical_text", "")))
    aliases = _unique_aliases(
        [
            canonical_text,
            *_build_germany_company_aliases(canonical_text),
            *[
                alias
                for alias in entity.get("aliases", [])
                if isinstance(alias, str)
            ],
        ]
    )
    return {
        "qid": qid,
        "slug": qid,
        "entity_id": f"wikidata:{qid}",
        "url": _WIKIDATA_ENTITY_URL_TEMPLATE.format(qid=qid),
        "label": canonical_text,
        "aliases": aliases,
    }


def _query_germany_wikidata_public_people_rows(
    *,
    organization_qids: list[str],
    user_agent: str,
    warnings: list[str],
) -> list[dict[str, str]]:
    resolved_qids = _unique_aliases(
        [
            qid
            for qid in organization_qids
            if qid.startswith("Q")
        ]
    )
    if not resolved_qids:
        return []
    property_ids = [property_id for property_id, _, _ in _GERMANY_WIKIDATA_PUBLIC_PEOPLE_PROPERTIES]
    rows: list[dict[str, str]] = []
    batch_size = 100
    for index in range(0, len(resolved_qids), batch_size):
        batch = resolved_qids[index : index + batch_size]
        query = (
            "SELECT ?organization ?property ?person ?personLabel WHERE { "
            f"VALUES ?organization {{ {' '.join(f'wd:{qid}' for qid in batch)} }} "
            f"VALUES ?property {{ {' '.join(f'wdt:{property_id}' for property_id in property_ids)} }} "
            "?organization ?property ?person . "
            f"?person wdt:{_WIKIDATA_INSTANCE_OF_PROPERTY_ID} wd:{_WIKIDATA_HUMAN_QID} . "
            'SERVICE wikibase:label { bd:serviceParam wikibase:language "en,de". } '
            "}"
        )
        try:
            payload = _fetch_wikidata_sparql_json(query=query, user_agent=user_agent)
        except Exception as exc:
            warning_text = f"Wikidata Germany public-people query failed: {exc}"
            if warning_text not in warnings and len(warnings) < 5:
                warnings.append(warning_text)
            continue
        results = payload.get("results")
        if not isinstance(results, dict):
            continue
        bindings = results.get("bindings")
        if not isinstance(bindings, list):
            continue
        for binding in bindings:
            if not isinstance(binding, dict):
                continue
            organization_binding = binding.get("organization")
            property_binding = binding.get("property")
            person_binding = binding.get("person")
            person_label_binding = binding.get("personLabel")
            if not all(
                isinstance(item, dict)
                for item in (
                    organization_binding,
                    property_binding,
                    person_binding,
                    person_label_binding,
                )
            ):
                continue
            organization_qid = _extract_wikidata_qid_from_uri(
                str(organization_binding.get("value", ""))
            )
            property_id = _extract_wikidata_property_id_from_uri(
                str(property_binding.get("value", ""))
            )
            person_qid = _extract_wikidata_qid_from_uri(
                str(person_binding.get("value", ""))
            )
            person_label = _normalize_whitespace(str(person_label_binding.get("value", "")))
            if not organization_qid or not property_id or not person_qid or not person_label:
                continue
            rows.append(
                {
                    "organization_qid": organization_qid,
                    "property_id": property_id,
                    "person_qid": person_qid,
                    "person_label": person_label,
                }
            )
    return rows


def _merge_germany_wikidata_public_people_entities(
    *,
    people_entities: list[dict[str, object]],
    issuer_entities: list[dict[str, object]],
    organization_people: dict[str, list[dict[str, object]]],
) -> None:
    issuer_lookup = {
        str(entity.get("entity_id", "")).strip(): entity
        for entity in issuer_entities
        if str(entity.get("entity_id", "")).strip()
    }
    existing_people_by_id = {
        str(entity.get("entity_id", "")).strip(): entity
        for entity in people_entities
        if str(entity.get("entity_id", "")).strip()
    }
    for employer_entity_id, people_records in organization_people.items():
        employer_entity = issuer_lookup.get(str(employer_entity_id))
        if employer_entity is None:
            continue
        for person_record in people_records:
            public_person_entity = _build_germany_wikidata_public_person_entity(
                employer_entity=employer_entity,
                person_record=person_record,
            )
            entity_id = str(public_person_entity.get("entity_id", "")).strip()
            if not entity_id:
                continue
            existing_entity = existing_people_by_id.get(entity_id)
            if existing_entity is None:
                people_entities.append(public_person_entity)
                existing_people_by_id[entity_id] = public_person_entity
                continue
            existing_entity["aliases"] = _unique_aliases(
                [
                    *[
                        alias
                        for alias in existing_entity.get("aliases", [])
                        if isinstance(alias, str)
                    ],
                    *[
                        alias
                        for alias in public_person_entity.get("aliases", [])
                        if isinstance(alias, str)
                    ],
                ]
            )
            existing_metadata = existing_entity.get("metadata")
            incoming_metadata = public_person_entity.get("metadata")
            if not isinstance(existing_metadata, dict) or not isinstance(incoming_metadata, dict):
                continue
            if not _normalize_whitespace(str(existing_metadata.get("role_title", ""))):
                existing_metadata["role_title"] = incoming_metadata.get("role_title", "")
            existing_metadata.setdefault(
                "wikidata_source_property_id",
                incoming_metadata.get("wikidata_source_property_id", ""),
            )
            existing_metadata.setdefault(
                "employer_wikidata_qid",
                incoming_metadata.get("employer_wikidata_qid", ""),
            )
            existing_metadata.setdefault(
                "source_name",
                incoming_metadata.get("source_name", ""),
            )
            _merge_wikidata_metadata(entity=existing_entity, match=person_record)


def _build_germany_person_wikidata_matches_from_organization_people(
    *,
    people_entities: list[dict[str, object]],
    issuer_entities: list[dict[str, object]],
    organization_people: dict[str, list[dict[str, object]]],
) -> dict[str, dict[str, object]]:
    issuer_lookup = {
        str(entity.get("entity_id", "")).strip(): entity
        for entity in issuer_entities
        if str(entity.get("entity_id", "")).strip()
    }
    existing_people_ids = {
        str(entity.get("entity_id", "")).strip()
        for entity in people_entities
        if str(entity.get("entity_id", "")).strip()
    }
    matches: dict[str, dict[str, object]] = {}
    for employer_entity_id, people_records in organization_people.items():
        employer_entity = issuer_lookup.get(str(employer_entity_id))
        if employer_entity is None:
            continue
        for person_record in people_records:
            derived_entity = _build_germany_wikidata_public_person_entity(
                employer_entity=employer_entity,
                person_record=person_record,
            )
            entity_id = str(derived_entity.get("entity_id", "")).strip()
            if entity_id and entity_id in existing_people_ids:
                matches[entity_id] = {
                    key: value
                    for key, value in person_record.items()
                    if key in {"qid", "slug", "entity_id", "url", "label", "aliases"}
                }
    return matches


def _build_germany_wikidata_public_person_entity(
    *,
    employer_entity: dict[str, object],
    person_record: dict[str, object],
) -> dict[str, object]:
    employer_metadata = employer_entity.get("metadata")
    if not isinstance(employer_metadata, dict):
        employer_metadata = {}
    employer_name = _normalize_whitespace(str(employer_entity.get("canonical_text", "")))
    employer_ticker = _normalize_whitespace(str(employer_metadata.get("ticker", ""))).upper()
    employer_isin = _normalize_whitespace(str(employer_metadata.get("isin", ""))).upper()
    employer_key = employer_ticker or employer_isin or _slug(str(employer_entity.get("entity_id", "")))
    role_class = _normalize_whitespace(str(person_record.get("role_class", "")))
    canonical_name = _normalize_whitespace(str(person_record.get("canonical_text", "")))
    aliases = [
        alias
        for alias in person_record.get("aliases", [])
        if isinstance(alias, str)
    ]
    metadata: dict[str, object] = {
        "country_code": "de",
        "category": role_class,
        "role_title": _normalize_whitespace(str(person_record.get("role_title", ""))),
        "employer_name": employer_name,
        "source_name": "wikidata-germany-organization-people",
        "section_name": _normalize_whitespace(str(person_record.get("role_title", ""))),
        "wikidata_source_property_id": _normalize_whitespace(
            str(person_record.get("source_property_id", ""))
        ),
        "employer_wikidata_qid": _normalize_whitespace(
            str(person_record.get("organization_qid", ""))
        ),
    }
    if employer_ticker:
        metadata["employer_ticker"] = employer_ticker
    if employer_isin:
        metadata["employer_isin"] = employer_isin
    entity = {
        "entity_type": "person",
        "canonical_text": canonical_name,
        "aliases": _unique_aliases(aliases),
        "entity_id": (
            f"finance-de-person:{employer_key}:{_slug(canonical_name)}:{_slug(role_class)}"
        ),
        "metadata": metadata,
    }
    _merge_wikidata_metadata(entity=entity, match=person_record)
    return entity


def _match_germany_wikidata_entity(
    *,
    entity: dict[str, object],
    issuer_matches: dict[str, dict[str, object]],
    organization_matches: dict[str, dict[str, object]],
) -> dict[str, object] | None:
    metadata = entity.get("metadata")
    if isinstance(metadata, dict):
        isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
        if isin:
            issuer_match = issuer_matches.get(isin)
            if isinstance(issuer_match, dict):
                return issuer_match
    entity_id = str(entity.get("entity_id", "")).strip()
    match = organization_matches.get(entity_id)
    return match if isinstance(match, dict) else None


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
        if entity_type != "person":
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
        alias_builder=_build_germany_wikidata_entity_aliases,
    )


def _select_best_germany_organization_wikidata_match(
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
        score = _score_germany_organization_wikidata_candidate(
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
        alias_builder=_build_germany_wikidata_entity_aliases,
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
        alias_builder=_build_germany_wikidata_entity_aliases,
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


def _score_germany_organization_wikidata_candidate(
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
    elif target_keys.intersection(candidate_keys):
        score += 30
    string_values = _extract_wikidata_string_claim_values(candidate_payload)
    lei = _normalize_whitespace(str(metadata.get("lei", ""))).upper()
    if lei and lei in string_values:
        score += 100
    candidate_description = _extract_wikidata_description(candidate_payload).casefold()
    if any(
        keyword in candidate_description
        for keyword in (
            "company",
            "bank",
            "financial",
            "investment",
            "holding",
            "authority",
            "fund",
            "services",
        )
    ):
        score += 5
    category = str(metadata.get("category", "")).casefold()
    if (
        category == "regulated_institution"
        and (target_full and target_full in candidate_keys or target_core and target_core in candidate_keys)
        and any(
            keyword in candidate_description
            for keyword in ("bank", "financial", "credit", "institution")
        )
    ):
        score += 20
    if (
        category == "major_shareholder_organization"
        and (target_full and target_full in candidate_keys or target_core and target_core in candidate_keys)
        and any(
            keyword in candidate_description
            for keyword in ("investment", "holding", "authority", "fund", "company", "group")
        )
    ):
        score += 10
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


def _enrich_france_entities_with_wikidata(
    *,
    country_dir: Path,
    issuer_entities: list[dict[str, object]],
    ticker_entities: list[dict[str, object]],
    people_entities: list[dict[str, object]],
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[str]:
    resolution_payload, resolution_path, warnings = _build_france_wikidata_resolution_snapshot(
        issuer_entities=issuer_entities,
        people_entities=people_entities,
        country_dir=country_dir,
        user_agent=user_agent,
    )
    if resolution_path is None:
        return warnings
    downloaded_sources.append(
        {
            "name": "wikidata-france-entity-resolution",
            "source_url": _WIKIDATA_API_URL,
            "path": str(resolution_path.relative_to(country_dir)),
            "category": "knowledge_graph",
            "notes": "Wikidata QID resolution snapshot for France issuers and related people.",
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


def _build_france_wikidata_resolution_snapshot(
    *,
    issuer_entities: list[dict[str, object]],
    people_entities: list[dict[str, object]],
    country_dir: Path,
    user_agent: str,
) -> tuple[dict[str, object], Path | None, list[str]]:
    warnings: list[str] = []
    issuer_matches, issuer_warnings = _resolve_france_issuer_wikidata_matches(
        issuer_entities=issuer_entities,
        user_agent=user_agent,
    )
    warnings.extend(issuer_warnings)
    people_matches, people_warnings = _resolve_france_person_wikidata_matches(
        people_entities=people_entities,
        issuer_matches=issuer_matches,
        user_agent=user_agent,
    )
    warnings.extend(people_warnings)
    payload: dict[str, object] = {
        "schema_version": 1,
        "country_code": "fr",
        "generated_at": _utc_timestamp(),
        "issuers": issuer_matches,
        "people": people_matches,
    }
    if not issuer_matches and not people_matches:
        return payload, None, warnings
    resolution_path = country_dir / "wikidata-france-entity-resolution.json"
    resolution_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload, resolution_path, warnings


def _resolve_france_issuer_wikidata_matches(
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
        for query in _build_france_wikidata_search_queries(entity, entity_type="issuer"):
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
        match = _select_best_france_issuer_wikidata_match(
            entity=entity,
            candidate_ids=candidate_ids,
            entity_payloads=entity_payloads,
        )
        if match is not None:
            matches[isin] = match
    return matches, warnings


def _resolve_france_person_wikidata_matches(
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
        for query in _build_france_wikidata_search_queries(entity, entity_type="person"):
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
            if _has_exact_france_person_search_hit(entity, search_cache[query]):
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
        match = _select_best_france_person_wikidata_match(
            entity=entity,
            candidate_ids=candidate_ids,
            entity_payloads=entity_payloads,
            issuer_matches=issuer_matches,
        )
        if match is not None:
            matches[entity_id] = match
    return matches, warnings


def _build_france_wikidata_search_queries(
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


def _has_exact_france_person_search_hit(
    entity: dict[str, object],
    search_results: list[dict[str, Any]],
) -> bool:
    target_keys = _build_france_person_match_keys(entity)
    return any(
        _normalize_person_name_for_match(str(item.get("label", ""))) in target_keys
        for item in search_results
        if isinstance(item, dict)
    )


def _select_best_france_issuer_wikidata_match(
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
        score = _score_france_issuer_wikidata_candidate(
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
        alias_builder=_build_france_wikidata_entity_aliases,
        label_languages=("en", "fr"),
    )


def _select_best_france_person_wikidata_match(
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
        score = _score_france_person_wikidata_candidate(
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
        alias_builder=_build_france_wikidata_entity_aliases,
        label_languages=("en", "fr"),
    )


def _score_france_issuer_wikidata_candidate(
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
    elif target_keys.intersection(candidate_keys):
        score += 30
    string_values = _extract_wikidata_string_claim_values(candidate_payload)
    isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
    if isin and isin in string_values:
        score += 100
    ticker = _normalize_whitespace(str(metadata.get("ticker", ""))).upper()
    if ticker and ticker in string_values:
        score += 15
    candidate_description = _extract_wikidata_description(candidate_payload).casefold()
    if any(
        keyword in candidate_description
        for keyword in (
            "france",
            "french",
            "company",
            "software",
            "furniture",
            "retailer",
            "industrial",
            "bank",
        )
    ):
        score += 5
    return score


def _score_france_person_wikidata_candidate(
    *,
    entity: dict[str, object],
    candidate_payload: dict[str, Any],
    issuer_matches: dict[str, dict[str, object]],
) -> int:
    metadata = entity.get("metadata")
    if not isinstance(metadata, dict):
        return 0
    score = 0
    target_keys = _build_france_person_match_keys(entity)
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
        for keyword in (
            "manager",
            "executive",
            "business",
            "chief",
            "board",
            "chairman",
            "director",
            "french",
            "france",
        )
    ):
        score += 10
    return score


def _build_france_person_match_keys(entity: dict[str, object]) -> set[str]:
    keys = {
        _normalize_person_name_for_match(str(entity.get("canonical_text", ""))),
    }
    keys.update(
        _normalize_person_name_for_match(alias)
        for alias in entity.get("aliases", [])
        if isinstance(alias, str)
    )
    return {key for key in keys if key}


def _enrich_united_kingdom_entities_with_wikidata(
    *,
    country_dir: Path,
    issuer_entities: list[dict[str, object]],
    ticker_entities: list[dict[str, object]],
    people_entities: list[dict[str, object]],
    downloaded_sources: list[dict[str, object]],
    user_agent: str,
) -> list[str]:
    resolution_payload, resolution_path, warnings = (
        _build_united_kingdom_wikidata_resolution_snapshot(
            issuer_entities=issuer_entities,
            people_entities=people_entities,
            country_dir=country_dir,
            user_agent=user_agent,
        )
    )
    if resolution_path is None:
        return warnings
    downloaded_sources.append(
        {
            "name": "wikidata-united-kingdom-entity-resolution",
            "source_url": _WIKIDATA_API_URL,
            "path": str(resolution_path.relative_to(country_dir)),
            "category": "knowledge_graph",
            "notes": "Wikidata QID resolution snapshot for United Kingdom issuers and related people.",
        }
    )
    issuer_matches = resolution_payload.get("issuers", {})
    if isinstance(issuer_matches, dict):
        for entity in [*issuer_entities, *ticker_entities]:
            match = issuer_matches.get(_united_kingdom_wikidata_target_key(entity))
            if isinstance(match, dict):
                _merge_wikidata_metadata(entity=entity, match=match)
    people_matches = resolution_payload.get("people", {})
    if isinstance(people_matches, dict):
        for entity in people_entities:
            match = people_matches.get(str(entity.get("entity_id", "")))
            if isinstance(match, dict):
                _merge_wikidata_metadata(entity=entity, match=match)
    return warnings


def _build_united_kingdom_wikidata_resolution_snapshot(
    *,
    issuer_entities: list[dict[str, object]],
    people_entities: list[dict[str, object]],
    country_dir: Path,
    user_agent: str,
) -> tuple[dict[str, object], Path | None, list[str]]:
    del people_entities
    issuer_matches, warnings = _resolve_united_kingdom_issuer_wikidata_matches(
        issuer_entities=issuer_entities,
        user_agent=user_agent,
    )
    payload: dict[str, object] = {
        "schema_version": 1,
        "country_code": "uk",
        "generated_at": _utc_timestamp(),
        "issuers": issuer_matches,
        "people": {},
    }
    if not issuer_matches:
        return payload, None, warnings
    resolution_path = country_dir / "wikidata-united-kingdom-entity-resolution.json"
    resolution_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload, resolution_path, warnings


def _resolve_united_kingdom_issuer_wikidata_matches(
    *,
    issuer_entities: list[dict[str, object]],
    user_agent: str,
) -> tuple[dict[str, dict[str, object]], list[str]]:
    warnings: list[str] = []
    candidate_ids_by_isin = _query_wikidata_entity_ids_by_string_property(
        property_id=_WIKIDATA_ISIN_PROPERTY_ID,
        values=[
            _normalize_whitespace(str(entity.get("metadata", {}).get("isin", "")))
            for entity in issuer_entities
            if isinstance(entity.get("metadata"), dict)
            and _normalize_whitespace(str(entity.get("metadata", {}).get("isin", "")))
        ],
        user_agent=user_agent,
        warnings=warnings,
    )
    candidate_ids_by_ticker = _query_wikidata_entity_ids_by_string_property(
        property_id=_WIKIDATA_LONDON_STOCK_EXCHANGE_COMPANY_ID_PROPERTY_ID,
        values=[
            _normalize_whitespace(str(entity.get("metadata", {}).get("ticker", ""))).upper()
            for entity in issuer_entities
            if isinstance(entity.get("metadata"), dict)
            and _normalize_whitespace(str(entity.get("metadata", {}).get("ticker", "")))
        ],
        user_agent=user_agent,
        warnings=warnings,
    )
    target_candidates: dict[str, list[str]] = {}
    for entity in issuer_entities:
        target_key = _united_kingdom_wikidata_target_key(entity)
        if not target_key:
            continue
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        candidate_ids: list[str] = []
        isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
        if isin:
            candidate_ids.extend(candidate_ids_by_isin.get(isin, []))
        ticker = _normalize_whitespace(str(metadata.get("ticker", ""))).upper()
        if ticker:
            candidate_ids.extend(candidate_ids_by_ticker.get(ticker, []))
        resolved_candidate_ids = _unique_aliases(
            [candidate_id for candidate_id in candidate_ids if candidate_id]
        )
        if resolved_candidate_ids:
            target_candidates[target_key] = resolved_candidate_ids
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
        target_key = _united_kingdom_wikidata_target_key(entity)
        if not target_key:
            continue
        candidate_ids = target_candidates.get(target_key, [])
        if not candidate_ids:
            continue
        match = _select_best_united_kingdom_issuer_wikidata_match(
            entity=entity,
            candidate_ids=candidate_ids,
            entity_payloads=entity_payloads,
        )
        if match is not None:
            matches[target_key] = match
    return matches, warnings


def _resolve_united_kingdom_person_wikidata_matches(
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
        candidate_ids: list[str] = []
        for query in _build_united_kingdom_wikidata_search_queries(
            entity, entity_type="person"
        ):
            if query not in search_cache:
                try:
                    search_cache[query] = _search_wikidata_entities(
                        query,
                        user_agent=user_agent,
                    )
                except Exception as exc:
                    warning_text = f"Wikidata UK people search failed for {query}: {exc}"
                    if warning_text not in warnings and len(warnings) < 5:
                        warnings.append(warning_text)
                    search_cache[query] = []
            candidate_ids.extend(
                str(item.get("id", "")).strip()
                for item in search_cache[query]
                if isinstance(item, dict)
            )
            if _has_exact_united_kingdom_person_search_hit(entity, search_cache[query]):
                break
        resolved_candidate_ids = _unique_aliases(
            [candidate_id for candidate_id in candidate_ids if candidate_id]
        )
        if resolved_candidate_ids:
            target_candidates[entity_id] = resolved_candidate_ids
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
        match = _select_best_united_kingdom_person_wikidata_match(
            entity=entity,
            candidate_ids=candidate_ids,
            entity_payloads=entity_payloads,
            issuer_matches=issuer_matches,
        )
        if match is not None:
            matches[entity_id] = match
    return matches, warnings


def _united_kingdom_wikidata_target_key(entity: dict[str, object]) -> str:
    metadata = entity.get("metadata")
    if isinstance(metadata, dict):
        company_number = _normalize_whitespace(str(metadata.get("company_number", ""))).upper()
        if company_number:
            return f"company-number:{company_number}"
        isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
        if isin:
            return f"isin:{isin}"
        ticker = _normalize_whitespace(str(metadata.get("ticker", ""))).upper()
        if ticker:
            return f"ticker:{ticker}"
    if str(entity.get("entity_type", "")) == "ticker":
        ticker_text = _normalize_whitespace(str(entity.get("canonical_text", ""))).upper()
        if ticker_text:
            return f"ticker:{ticker_text}"
    entity_id = _normalize_whitespace(str(entity.get("entity_id", "")))
    if entity_id:
        return entity_id
    return _normalize_whitespace(str(entity.get("canonical_text", "")))


def _build_united_kingdom_wikidata_search_queries(
    entity: dict[str, object],
    *,
    entity_type: str,
) -> list[str]:
    queries: list[str] = []
    candidate_values = [str(entity.get("canonical_text", ""))]
    candidate_values.extend(
        alias
        for alias in entity.get("aliases", [])
        if isinstance(alias, str)
    )
    for candidate in candidate_values:
        normalized_candidate = _normalize_whitespace(candidate)
        if not normalized_candidate or normalized_candidate in queries:
            continue
        if entity_type == "person":
            queries.append(normalized_candidate)
        else:
            if normalized_candidate.isupper() and len(normalized_candidate) <= 5:
                continue
            queries.append(normalized_candidate)
        if len(queries) >= 4:
            break
    return queries


def _has_exact_united_kingdom_person_search_hit(
    entity: dict[str, object],
    search_results: list[dict[str, Any]],
) -> bool:
    target_keys = _build_united_kingdom_person_match_keys(entity)
    return any(
        _normalize_person_name_for_match(str(item.get("label", ""))) in target_keys
        for item in search_results
        if isinstance(item, dict)
    )


def _select_best_united_kingdom_issuer_wikidata_match(
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
        score = _score_united_kingdom_issuer_wikidata_candidate(
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
        alias_builder=_build_united_kingdom_wikidata_entity_aliases,
        label_languages=("en",),
    )


def _select_best_united_kingdom_person_wikidata_match(
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
        score = _score_united_kingdom_person_wikidata_candidate(
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
        alias_builder=_build_united_kingdom_wikidata_entity_aliases,
        label_languages=("en",),
    )


def _score_united_kingdom_issuer_wikidata_candidate(
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
    elif target_keys.intersection(candidate_keys):
        score += 30
    string_values = _extract_wikidata_string_claim_values(candidate_payload)
    isin = _normalize_whitespace(str(metadata.get("isin", ""))).upper()
    if isin and isin in string_values:
        score += 100
    ticker = _normalize_whitespace(str(metadata.get("ticker", ""))).upper()
    if ticker and ticker in string_values:
        score += 25
    candidate_description = _extract_wikidata_description(candidate_payload).casefold()
    if any(
        keyword in candidate_description
        for keyword in (
            "british",
            "uk",
            "united kingdom",
            "company",
            "bank",
            "investment",
            "services",
            "software",
            "retailer",
        )
    ):
        score += 5
    return score


def _score_united_kingdom_person_wikidata_candidate(
    *,
    entity: dict[str, object],
    candidate_payload: dict[str, Any],
    issuer_matches: dict[str, dict[str, object]],
) -> int:
    metadata = entity.get("metadata")
    if not isinstance(metadata, dict):
        return 0
    score = 0
    target_keys = _build_united_kingdom_person_match_keys(entity)
    candidate_keys = _extract_wikidata_person_match_keys(candidate_payload)
    canonical_key = _normalize_person_name_for_match(str(entity.get("canonical_text", "")))
    if canonical_key and canonical_key in candidate_keys:
        score += 100
    elif target_keys.intersection(candidate_keys):
        score += 90
    employer_match_key = _build_united_kingdom_employer_match_key(metadata)
    employer_qid = (
        str(issuer_matches.get(employer_match_key, {}).get("qid", "")).strip()
        if employer_match_key
        else ""
    )
    if employer_qid:
        entity_claim_ids = _extract_wikidata_entity_claim_ids(candidate_payload)
        if employer_qid in entity_claim_ids:
            score += 40
    candidate_description = _extract_wikidata_description(candidate_payload).casefold()
    if any(
        keyword in candidate_description
        for keyword in (
            "british",
            "business",
            "executive",
            "director",
            "chair",
            "investor",
            "banker",
            "manager",
        )
    ):
        score += 10
    return score


def _build_united_kingdom_employer_match_key(metadata: dict[str, object]) -> str:
    company_number = _normalize_whitespace(str(metadata.get("employer_company_number", ""))).upper()
    if company_number:
        return f"company-number:{company_number}"
    employer_isin = _normalize_whitespace(str(metadata.get("employer_isin", ""))).upper()
    if employer_isin:
        return f"isin:{employer_isin}"
    employer_ticker = _normalize_whitespace(str(metadata.get("employer_ticker", ""))).upper()
    if employer_ticker:
        return f"ticker:{employer_ticker}"
    return ""


def _build_united_kingdom_person_match_keys(entity: dict[str, object]) -> set[str]:
    keys = {
        _normalize_person_name_for_match(str(entity.get("canonical_text", ""))),
    }
    keys.update(
        _normalize_person_name_for_match(alias)
        for alias in entity.get("aliases", [])
        if isinstance(alias, str)
    )
    return {key for key in keys if key}


def _build_united_kingdom_wikidata_entity_aliases(
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
        aliases.extend(_build_united_kingdom_company_aliases(normalized_text))
    return _unique_aliases(aliases)


def _build_france_wikidata_entity_aliases(
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
        aliases.extend(_build_france_company_aliases(normalized_text))
    return _unique_aliases(aliases)


def _build_argentina_wikidata_entity_aliases(
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
            canonical_name, person_aliases = _normalize_argentina_byma_person_name_aliases(
                normalized_text
            )
            if canonical_name:
                aliases.extend(person_aliases)
            continue
        if normalized_text.isupper() and len(normalized_text) <= 4:
            continue
        aliases.extend(_build_argentina_company_aliases(normalized_text))
    return _unique_aliases(aliases)


def _build_united_states_wikidata_entity_aliases(
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
        aliases.extend(_build_united_states_company_aliases(normalized_text))
    return _unique_aliases(aliases)


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
    for language in ("en", "de", "es"):
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


def _extract_wikidata_entity_claim_ids_for_property(
    payload: dict[str, Any],
    *,
    property_id: str,
) -> list[str]:
    claims = payload.get("claims")
    if not isinstance(claims, dict):
        return []
    property_claims = claims.get(property_id)
    if not isinstance(property_claims, list):
        return []
    values: list[str] = []
    for claim in property_claims:
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
            values.append(entity_id)
    return _unique_aliases(values)


def _wikidata_payload_has_instance_of(
    payload: dict[str, Any],
    target_qid: str,
) -> bool:
    return target_qid in _extract_wikidata_entity_claim_ids_for_property(
        payload,
        property_id=_WIKIDATA_INSTANCE_OF_PROPERTY_ID,
    )


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
    alias_builder: Any,
    label_languages: tuple[str, ...] = ("en", "de"),
) -> dict[str, object]:
    label = ""
    aliases: list[str] = []
    if isinstance(payload, dict):
        for language in label_languages:
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
        aliases = alias_builder(payload, entity_type=entity_type)
    return {
        "qid": qid,
        "slug": qid,
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
    slug = _normalize_whitespace(str(match.get("slug", ""))) or qid
    url = _normalize_whitespace(str(match.get("url", "")))
    if qid:
        metadata["wikidata_qid"] = qid
    if slug:
        metadata["wikidata_slug"] = slug
    if qid:
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
    batches = [
        resolved_ids[index : index + _WIKIDATA_GETENTITIES_BATCH_SIZE]
        for index in range(0, len(resolved_ids), _WIKIDATA_GETENTITIES_BATCH_SIZE)
    ]

    def _fetch_batch(batch: list[str]) -> dict[str, Any]:
        return _fetch_wikidata_api_json(
            params={
                "action": "wbgetentities",
                "format": "json",
                "ids": "|".join(batch),
                "languages": "en|de",
                "props": "labels|aliases|descriptions|claims",
            },
            user_agent=user_agent,
        )

    max_workers = min(4, len(batches)) or 1
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(_fetch_batch, batch): batch
            for batch in batches
        }
        for future in as_completed(future_to_batch):
            try:
                payload = future.result()
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
        payload: dict[str, Any] | None = None
        for attempt in range(1, _WIKIDATA_API_MAX_ATTEMPTS + 1):
            response = client.get(_WIKIDATA_API_URL, params=params)
            if response.status_code == 429 and attempt < _WIKIDATA_API_MAX_ATTEMPTS:
                retry_after = response.headers.get("Retry-After", "").strip()
                try:
                    delay_seconds = float(retry_after) if retry_after else 0.0
                except ValueError:
                    delay_seconds = 0.0
                time.sleep(max(delay_seconds, _WIKIDATA_API_RETRY_BASE_SECONDS))
                continue
            response.raise_for_status()
            candidate_payload = response.json()
            if not isinstance(candidate_payload, dict):
                raise ValueError("Wikidata API returned a non-object payload")
            payload = candidate_payload
            break
    if payload is None:
        raise RuntimeError("Wikidata API request did not return a payload")
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


def _build_france_euronext_issuer_entity(
    record: dict[str, str],
) -> dict[str, object] | None:
    company_name = _normalize_whitespace(str(record.get("company_name", "")))
    raw_company_name = _normalize_whitespace(str(record.get("raw_company_name", "")))
    isin = _normalize_whitespace(str(record.get("isin", ""))).upper()
    symbol = _normalize_whitespace(str(record.get("symbol", ""))).upper()
    if not company_name or not isin:
        return None
    aliases = _build_france_company_aliases(raw_company_name, company_name)
    metadata: dict[str, object] = {
        "country_code": "fr",
        "category": "issuer",
        "issuer_name": company_name,
        "isin": isin,
    }
    if symbol:
        metadata["ticker"] = symbol
    listing_segment = _normalize_whitespace(str(record.get("listing_segment", "")))
    if listing_segment:
        metadata["listing_segment"] = listing_segment
    market_code = _normalize_whitespace(str(record.get("market_code", ""))).upper()
    if market_code:
        metadata["instrument_exchange"] = market_code
    product_url = _normalize_whitespace(str(record.get("product_url", "")))
    if product_url:
        metadata["product_url"] = product_url
    return {
        "entity_type": "organization",
        "canonical_text": company_name,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-fr-issuer:{isin}",
        "metadata": metadata,
    }


def _build_france_euronext_ticker_entity(
    record: dict[str, str],
) -> dict[str, object] | None:
    symbol = _normalize_whitespace(str(record.get("symbol", ""))).upper()
    company_name = _normalize_whitespace(str(record.get("company_name", "")))
    raw_company_name = _normalize_whitespace(str(record.get("raw_company_name", "")))
    isin = _normalize_whitespace(str(record.get("isin", ""))).upper()
    if not symbol or not company_name:
        return None
    aliases = [symbol, *_build_france_company_aliases(raw_company_name, company_name)]
    if isin:
        aliases.append(isin)
    metadata: dict[str, object] = {
        "country_code": "fr",
        "category": "ticker",
        "ticker": symbol,
        "issuer_name": company_name,
    }
    if isin:
        metadata["isin"] = isin
    listing_segment = _normalize_whitespace(str(record.get("listing_segment", "")))
    if listing_segment:
        metadata["listing_segment"] = listing_segment
    market_code = _normalize_whitespace(str(record.get("market_code", ""))).upper()
    if market_code:
        metadata["instrument_exchange"] = market_code
    product_url = _normalize_whitespace(str(record.get("product_url", "")))
    if product_url:
        metadata["product_url"] = product_url
    return {
        "entity_type": "ticker",
        "canonical_text": symbol,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-fr-ticker:{symbol}",
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
            page_label="live.deutsche-boerse company-details page",
            render_budget_ms=_DEUTSCHE_BOERSE_COMPANY_DETAILS_RENDER_BUDGET_MS,
        )
        destination.write_text(rendered_html, encoding="utf-8")
        return normalized_source_url
    return _download_source(normalized_source_url, destination, user_agent=user_agent)


def _download_euronext_directory_page(
    source_url: str | Path,
    destination: Path,
    *,
    user_agent: str,
) -> str:
    normalized_source_url = normalize_source_url(source_url)
    parsed = urlparse(normalized_source_url)
    if parsed.scheme in {"http", "https"} and parsed.netloc.casefold() == "live.euronext.com":
        chrome_binary = _find_headless_chrome_binary()
        if chrome_binary is None:
            raise RuntimeError(
                "Headless Chrome not available; skipped live.euronext directory rendering"
            )
        rendered_html = _render_dynamic_page_with_headless_chrome(
            chrome_binary=chrome_binary,
            source_url=normalized_source_url,
            user_agent=user_agent,
            page_label="live.euronext directory page",
            render_budget_ms=_EURONEXT_DIRECTORY_RENDER_BUDGET_MS,
        )
        destination.write_text(rendered_html, encoding="utf-8")
        return normalized_source_url
    return _download_source(normalized_source_url, destination, user_agent=user_agent)


def _download_london_stock_exchange_directory_page(
    source_url: str | Path,
    destination: Path,
    *,
    user_agent: str,
) -> str:
    normalized_source_url = normalize_source_url(source_url)
    parsed = urlparse(normalized_source_url)
    if (
        parsed.scheme in {"http", "https"}
        and parsed.netloc.casefold() == "www.londonstockexchange.com"
        and parsed.path.startswith("/indices/ftse-aim-all-share/constituents/table")
    ):
        chrome_binary = _find_headless_chrome_binary()
        if chrome_binary is None:
            raise RuntimeError(
                "Headless Chrome not available; skipped London Stock Exchange directory rendering"
            )
        rendered_html = _render_dynamic_page_with_headless_chrome(
            chrome_binary=chrome_binary,
            source_url=normalized_source_url,
            user_agent=_HEADLESS_CHROME_BROWSER_USER_AGENT,
            page_label="London Stock Exchange directory page",
            render_budget_ms=_UK_LSE_AIM_RENDER_BUDGET_MS,
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
    page_label: str,
    render_budget_ms: int,
) -> str:
    command = [
        chrome_binary,
        "--headless=new",
        "--disable-gpu",
        f"--virtual-time-budget={render_budget_ms}",
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
                render_budget_ms / 1000 + 10,
            ),
        )
    except (OSError, subprocess.SubprocessError, TimeoutError) as exc:
        raise RuntimeError(f"Failed to render {page_label}: {exc}") from exc
    if completed.returncode != 0:
        stderr = _normalize_whitespace(completed.stderr)
        raise RuntimeError(f"Failed to render {page_label}" + (f": {stderr}" if stderr else ""))
    rendered_html = completed.stdout.strip()
    if not rendered_html:
        raise RuntimeError(f"Failed to render {page_label}: empty DOM dump")
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


def _extract_deutsche_boerse_company_details_contact_people_entities(
    html_text: str,
    *,
    issuer_record: dict[str, str | list[str]],
    source_name: str = "deutsche-boerse-company-details",
) -> list[dict[str, object]]:
    contact_value = _normalize_whitespace(
        str(_extract_deutsche_boerse_company_details_contact_details(html_text).get("contact", ""))
    )
    if not contact_value or "@" in contact_value:
        return []
    raw_person_name = _extract_germany_named_contact_or_holder_person_name(contact_value)
    if raw_person_name is None:
        return []
    canonical_name, aliases = _normalize_person_name_aliases(raw_person_name)
    if not canonical_name:
        return []
    aliases = _unique_aliases([*aliases, contact_value])
    company_name = str(issuer_record["company_name"])
    ticker = str(issuer_record["ticker"])
    isin = str(issuer_record["isin"])
    return [
        {
            "entity_type": "person",
            "canonical_text": canonical_name,
            "aliases": _unique_aliases(aliases),
            "entity_id": f"finance-de-person:{ticker}:{_slug(canonical_name)}:issuer-contact",
            "metadata": {
                "country_code": "de",
                "category": "issuer_contact",
                "role_title": "Issuer Contact",
                "employer_name": company_name,
                "employer_ticker": ticker,
                "employer_isin": isin,
                "source_name": source_name,
                "section_name": "Contact",
            },
        }
    ]


def _extract_deutsche_boerse_company_details_shareholder_people_entities(
    html_text: str,
    *,
    issuer_record: dict[str, str | list[str]],
    source_name: str = "deutsche-boerse-company-details",
) -> list[dict[str, object]]:
    company_name = str(issuer_record["company_name"])
    ticker = str(issuer_record["ticker"])
    isin = str(issuer_record["isin"])
    entities: list[dict[str, object]] = []
    for shareholder_record in _extract_deutsche_boerse_company_details_shareholder_records(
        html_text
    ):
        holder_name = _normalize_whitespace(str(shareholder_record.get("holder_name", "")))
        raw_person_name = _extract_germany_named_contact_or_holder_person_name(holder_name)
        if raw_person_name is None:
            continue
        canonical_name, aliases = _normalize_person_name_aliases(raw_person_name)
        if not canonical_name:
            continue
        aliases = _unique_aliases([*aliases, holder_name])
        metadata: dict[str, object] = {
            "country_code": "de",
            "category": "shareholder",
            "role_title": "Major Shareholder",
            "employer_name": company_name,
            "employer_ticker": ticker,
            "employer_isin": isin,
            "source_name": source_name,
            "section_name": "Shareholder structure",
        }
        ownership_percent = _normalize_whitespace(
            str(shareholder_record.get("ownership_percent", ""))
        )
        if ownership_percent:
            metadata["ownership_percent"] = ownership_percent
        entities.append(
            {
                "entity_type": "person",
                "canonical_text": canonical_name,
                "aliases": _unique_aliases(aliases),
                "entity_id": f"finance-de-person:{ticker}:{_slug(canonical_name)}:shareholder",
                "metadata": metadata,
            }
        )
    return entities


def _extract_deutsche_boerse_company_details_shareholder_organization_records(
    html_text: str,
    *,
    issuer_record: dict[str, str | list[str]],
    source_name: str = "deutsche-boerse-company-details",
) -> list[dict[str, str]]:
    company_name = str(issuer_record["company_name"])
    ticker = str(issuer_record["ticker"])
    isin = str(issuer_record["isin"])
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for shareholder_record in _extract_deutsche_boerse_company_details_shareholder_records(
        html_text
    ):
        holder_name = _clean_germany_major_shareholder_name(
            str(shareholder_record.get("holder_name", ""))
        )
        ownership_percent = _normalize_whitespace(
            str(shareholder_record.get("ownership_percent", ""))
        )
        if (
            not holder_name
            or not ownership_percent
            or _extract_germany_named_contact_or_holder_person_name(holder_name) is not None
        ):
            continue
        key = (holder_name.casefold(), ticker, ownership_percent)
        if key in seen:
            continue
        seen.add(key)
        records.append(
            {
                "holder_name": holder_name,
                "ownership_percent": ownership_percent,
                "employer_name": company_name,
                "employer_ticker": ticker,
                "employer_isin": isin,
                "source_name": source_name,
            }
        )
    return records


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


def _extract_deutsche_boerse_company_details_contact_details(
    html_text: str,
) -> dict[str, str]:
    body = _extract_deutsche_boerse_company_details_widget_tbody(
        html_text,
        headline_prefix="Contact ",
    )
    if not body:
        return {}
    details: dict[str, str] = {}
    for row in re.findall(r"<tr[^>]*>(?P<body>.*?)</tr>", body, re.IGNORECASE | re.DOTALL):
        cells = [
            _normalize_whitespace(_strip_html(cell))
            for cell in re.findall(
                r"<t[dh][^>]*>(.*?)</t[dh]>",
                row,
                re.IGNORECASE | re.DOTALL,
            )
        ]
        if len(cells) < 2:
            continue
        label = cells[0].casefold()
        value = _normalize_whitespace(cells[1])
        if not value:
            continue
        if label == "phone":
            details["phone"] = value
        elif label == "web":
            details["website"] = normalize_source_url(value)
        elif label == "contact":
            details["contact"] = value
    return details


def _extract_deutsche_boerse_company_details_shareholder_records(
    html_text: str,
) -> list[dict[str, str]]:
    body = _extract_deutsche_boerse_company_details_widget_tbody(
        html_text,
        headline_prefix="Shareholder structure ",
    )
    if not body:
        return []
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in re.findall(r"<tr[^>]*>(?P<body>.*?)</tr>", body, re.IGNORECASE | re.DOTALL):
        cells = [
            _normalize_whitespace(_strip_html(cell))
            for cell in re.findall(
                r"<t[dh][^>]*>(.*?)</t[dh]>",
                row,
                re.IGNORECASE | re.DOTALL,
            )
        ]
        if len(cells) < 2:
            continue
        holder_name = _normalize_whitespace(cells[0])
        ownership_percent = _normalize_whitespace(cells[1])
        if (
            not holder_name
            or not ownership_percent
            or _is_non_entity_germany_shareholder(holder_name)
        ):
            continue
        key = (holder_name.casefold(), ownership_percent)
        if key in seen:
            continue
        seen.add(key)
        records.append(
            {
                "holder_name": holder_name,
                "ownership_percent": ownership_percent,
            }
        )
    return records


def _extract_deutsche_boerse_company_details_widget_tbody(
    html_text: str,
    *,
    headline_prefix: str,
) -> str | None:
    match = re.search(
        (
            r"<h2[^>]*class=\"[^\"]*widget-table-headline[^\"]*\"[^>]*>"
            r"\s*"
            + re.escape(headline_prefix)
            + r".*?</h2>.*?<tbody>(?P<body>.*?)</tbody>"
        ),
        html_text,
        re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        return None
    return match.group("body")


def _is_non_entity_germany_shareholder(value: str) -> bool:
    normalized = _normalize_whitespace(value).casefold()
    if not normalized:
        return True
    return any(
        token in normalized
        for token in (
            "treasury shares",
            "eigene aktien",
            "own shares",
            "free float",
            "freefloat",
            "streubesitz",
            "public float",
            "others",
            "andere",
            "organbesitz",
            "altgesellschafter",
            "supervisory board members",
            "aufsichtsrat",
            "vorstand",
            "management board",
            "vorstand und aufsichtsrat",
            "management / aufsichtsrat",
            "management & aufsichtsrat",
            "grunder und management",
            "gründer und management",
        )
    )


def _looks_like_germany_named_contact_or_holder(value: str) -> bool:
    return _extract_germany_named_contact_or_holder_person_name(value) is not None


def _extract_germany_named_contact_or_holder_person_name(value: str) -> str | None:
    normalized = _clean_germany_major_shareholder_name(value)
    if not normalized:
        return None
    candidates: list[str] = [normalized]
    stripped_parenthetical = _normalize_whitespace(
        re.sub(r"\s*\([^)]*\)\s*$", "", normalized)
    )
    if stripped_parenthetical and stripped_parenthetical != normalized:
        candidates.append(stripped_parenthetical)
    if "," in normalized:
        leading_segment = _normalize_whitespace(normalized.split(",", 1)[0])
        if leading_segment and leading_segment not in candidates:
            candidates.append(leading_segment)
    org_tokens = {
        "ab",
        "ag",
        "asset",
        "assets",
        "authority",
        "bank",
        "banco",
        "capital",
        "co",
        "company",
        "companies",
        "family",
        "familie",
        "familien",
        "familiengesellschaft",
        "financial",
        "fonds",
        "fondation",
        "foundation",
        "fund",
        "funds",
        "gmbh",
        "group",
        "gesellschaft",
        "holding",
        "holdings",
        "inc",
        "insurance",
        "investment",
        "investments",
        "investor",
        "investoren",
        "investors",
        "kg",
        "kgaa",
        "limited",
        "llc",
        "ltd",
        "management",
        "mbh",
        "medicine",
        "n.v",
        "nv",
        "office",
        "partners",
        "plc",
        "pooling",
        "sarl",
        "sicav",
        "stiftung",
        "resources",
        "s.a",
        "sa",
        "se",
        "services",
        "studios",
        "trust",
        "versicherungen",
    }
    for candidate in candidates:
        lowered = candidate.casefold()
        if any(separator in lowered for separator in (" / ", " und ", " and ", " & ")):
            continue
        normalized_company_name = _normalize_company_name_for_match(candidate)
        lowered_tokens = set(normalized_company_name.split())
        if lowered_tokens.intersection(org_tokens):
            continue
        normalized_marker_text = f" {normalized_company_name} "
        if any(
            marker in normalized_marker_text
            for marker in (
                " s a r l ",
                " sicav ",
                " stiftung ",
                " fondation ",
                " beteiligungsgesellschaft ",
                " familiengesellschaft ",
            )
        ):
            continue
        canonical_name, _ = _normalize_person_name_aliases(candidate)
        if canonical_name is not None:
            return candidate
    return None


def _clean_germany_major_shareholder_name(value: str) -> str:
    candidate = _normalize_whitespace(value)
    candidate = re.sub(
        r"(?:\s+(?:ca\.?|circa\s*/\s*approximately|approximately|mehr\s+als|more\s+than))+$",
        "",
        candidate,
        flags=re.IGNORECASE,
    )
    return _normalize_whitespace(candidate)


def _build_germany_major_shareholder_organization_entities(
    *,
    shareholder_records: list[dict[str, str]],
    organization_lookup: dict[str, dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    grouped_records: dict[str, list[dict[str, str]]] = {}
    for shareholder_record in shareholder_records:
        holder_name = _clean_germany_major_shareholder_name(
            str(shareholder_record.get("holder_name", ""))
        )
        if not holder_name:
            continue
        group_key = _normalize_company_name_for_match(holder_name)
        if not group_key:
            continue
        normalized_record = dict(shareholder_record)
        normalized_record["holder_name"] = holder_name
        grouped_records.setdefault(group_key, []).append(normalized_record)
    entities: list[dict[str, object]] = []
    for records in grouped_records.values():
        holder_names = [
            _normalize_whitespace(str(record.get("holder_name", "")))
            for record in records
            if _normalize_whitespace(str(record.get("holder_name", "")))
        ]
        if not holder_names:
            continue
        aliases = _unique_aliases(
            [
                alias
                for holder_name in holder_names
                for alias in _build_germany_company_aliases(holder_name)
            ]
        )
        preferred_name = max(
            set(holder_names),
            key=lambda item: (holder_names.count(item), len(item), item.casefold()),
        )
        matched_entity = _find_matching_germany_organization_entity(
            holder_names=holder_names,
            organization_lookup=organization_lookup,
        )
        if matched_entity is not None:
            _merge_germany_major_shareholding_metadata(
                entity=matched_entity,
                holder_aliases=aliases,
                shareholding_records=records,
            )
            continue
        entity: dict[str, object] = {
            "entity_type": "organization",
            "canonical_text": preferred_name,
            "aliases": aliases,
            "entity_id": f"finance-de-major-shareholder-org:{_slug(preferred_name)}",
            "metadata": {
                "country_code": "de",
                "category": "major_shareholder_organization",
            },
        }
        _merge_germany_major_shareholding_metadata(
            entity=entity,
            holder_aliases=aliases,
            shareholding_records=records,
        )
        entities.append(entity)
    return entities


def _build_germany_organization_entity_lookup(
    entities: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    lookup: dict[str, dict[str, object]] = {}
    for entity in entities:
        if str(entity.get("entity_type", "")) != "organization":
            continue
        for alias in (
            str(entity.get("canonical_text", "")),
            *[
                alias
                for alias in entity.get("aliases", [])
                if isinstance(alias, str)
            ],
        ):
            for key in {
                _normalize_company_name_for_match(alias),
                _normalize_company_name_for_match(alias, drop_suffixes=True),
            }:
                if key:
                    lookup.setdefault(key, entity)
    return lookup


def _find_matching_germany_organization_entity(
    *,
    holder_names: list[str],
    organization_lookup: dict[str, dict[str, object]] | None,
) -> dict[str, object] | None:
    if not organization_lookup:
        return None
    for holder_name in holder_names:
        for key in {
            _normalize_company_name_for_match(holder_name),
            _normalize_company_name_for_match(holder_name, drop_suffixes=True),
        }:
            if key and key in organization_lookup:
                return organization_lookup[key]
    return None


def _merge_germany_major_shareholding_metadata(
    *,
    entity: dict[str, object],
    holder_aliases: list[str],
    shareholding_records: list[dict[str, str]],
) -> None:
    entity["aliases"] = _unique_aliases(
        [
            *[
                alias
                for alias in entity.get("aliases", [])
                if isinstance(alias, str)
            ],
            *holder_aliases,
        ]
    )
    metadata = entity.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        entity["metadata"] = metadata
    merged_records: list[dict[str, str]] = [
        {
            "employer_name": _normalize_whitespace(str(item.get("employer_name", ""))),
            "employer_ticker": _normalize_whitespace(str(item.get("employer_ticker", ""))),
            "employer_isin": _normalize_whitespace(str(item.get("employer_isin", ""))),
            "ownership_percent": _normalize_whitespace(
                str(item.get("ownership_percent", ""))
            ),
            "source_name": _normalize_whitespace(str(item.get("source_name", ""))),
        }
        for item in metadata.get("major_shareholding_records", [])
        if isinstance(item, dict)
    ]
    seen = {
        (
            item["employer_ticker"],
            item["employer_isin"],
            item["ownership_percent"],
            item["source_name"],
        )
        for item in merged_records
    }
    for record in shareholding_records:
        normalized_record = {
            "employer_name": _normalize_whitespace(str(record.get("employer_name", ""))),
            "employer_ticker": _normalize_whitespace(str(record.get("employer_ticker", ""))),
            "employer_isin": _normalize_whitespace(str(record.get("employer_isin", ""))),
            "ownership_percent": _normalize_whitespace(
                str(record.get("ownership_percent", ""))
            ),
            "source_name": _normalize_whitespace(str(record.get("source_name", ""))),
        }
        key = (
            normalized_record["employer_ticker"],
            normalized_record["employer_isin"],
            normalized_record["ownership_percent"],
            normalized_record["source_name"],
        )
        if key in seen:
            continue
        seen.add(key)
        merged_records.append(normalized_record)
    if not merged_records:
        return
    metadata["major_shareholding_records"] = merged_records
    metadata["major_shareholding_record_count"] = len(merged_records)
    metadata["major_shareholder_of_issuers"] = _unique_aliases(
        [item["employer_name"] for item in merged_records if item["employer_name"]]
    )
    metadata["major_shareholder_of_tickers"] = _unique_aliases(
        [item["employer_ticker"] for item in merged_records if item["employer_ticker"]]
    )


def _merge_deutsche_boerse_company_details_metadata(
    *,
    entity: dict[str, object],
    contact_details: dict[str, str],
    shareholder_records: list[dict[str, str]],
) -> None:
    metadata = entity.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        entity["metadata"] = metadata
    phone = _normalize_whitespace(str(contact_details.get("phone", "")))
    if phone:
        metadata["contact_phone"] = phone
    website = _normalize_whitespace(str(contact_details.get("website", "")))
    if website:
        metadata["contact_website"] = website
        if not _normalize_whitespace(str(metadata.get("issuer_website", ""))):
            metadata["issuer_website"] = website
    contact_value = _normalize_whitespace(str(contact_details.get("contact", "")))
    if contact_value:
        if "@" in contact_value:
            metadata["contact_email"] = contact_value
        else:
            metadata["contact_value"] = contact_value
    if not shareholder_records:
        return
    merged_shareholders: list[dict[str, str]] = [
        {
            "holder_name": _normalize_whitespace(str(item.get("holder_name", ""))),
            "ownership_percent": _normalize_whitespace(
                str(item.get("ownership_percent", ""))
            ),
        }
        for item in metadata.get("major_shareholders", [])
        if isinstance(item, dict)
        and _normalize_whitespace(str(item.get("holder_name", "")))
        and _normalize_whitespace(str(item.get("ownership_percent", "")))
    ]
    seen = {
        (
            item["holder_name"].casefold(),
            item["ownership_percent"],
        )
        for item in merged_shareholders
    }
    for shareholder_record in shareholder_records:
        holder_name = _normalize_whitespace(str(shareholder_record.get("holder_name", "")))
        ownership_percent = _normalize_whitespace(
            str(shareholder_record.get("ownership_percent", ""))
        )
        if not holder_name or not ownership_percent:
            continue
        key = (holder_name.casefold(), ownership_percent)
        if key in seen:
            continue
        seen.add(key)
        merged_shareholders.append(
            {
                "holder_name": holder_name,
                "ownership_percent": ownership_percent,
            }
        )
    if merged_shareholders:
        metadata["major_shareholders"] = merged_shareholders
        metadata["major_shareholder_names"] = _unique_aliases(
            [item["holder_name"] for item in merged_shareholders]
        )


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


def _extract_argentina_byma_equity_symbols(source_path: Path) -> list[str]:
    try:
        payload = json.loads(source_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    symbols: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        if item.get("deleted") is True:
            continue
        if str(item.get("description", "")).strip().casefold() != "equityinstrument":
            continue
        symbol = _normalize_whitespace(str(item.get("code", ""))).upper()
        if not symbol or " " in symbol:
            continue
        symbols.append(symbol)
    return _unique_aliases(symbols)


def _extract_argentina_byma_listed_company_records(
    html_text: str,
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    seen_symbols: set[str] = set()
    for match in _ARGENTINA_BYMA_LISTED_COMPANY_CARD_PATTERN.finditer(html_text):
        symbol = _normalize_whitespace(match.group("symbol")).upper()
        if not symbol or symbol in seen_symbols:
            continue
        company_name = _normalize_whitespace(_strip_html(html.unescape(match.group("name"))))
        if not company_name:
            continue
        body = match.group("body")
        actions = match.group("actions")
        isin_match = re.search(
            r">ISIN</div><div class=\"w-condition-invisible\">-</div><div[^>]*>(?P<isin>[^<]+)</div>",
            body,
            re.IGNORECASE,
        )
        sector_match = re.search(
            r">Sector</div><div[^>]*>(?P<sector>[^<]+)</div>",
            body,
            re.IGNORECASE,
        )
        address_match = re.search(
            r">Domicilio</div><div[^>]*>(?P<address>[^<]+)</div>",
            body,
            re.IGNORECASE,
        )
        panel_match = re.search(
            r'fs-cmsfilter-field="agent-type" class="text-size-tiny">(?P<label>[^<]+)</div>',
            body,
            re.IGNORECASE,
        )
        action_urls = [
            url
            for url in re.findall(r'href="(?P<url>[^"]+)"', actions, re.IGNORECASE)
            if url and not url.startswith("#")
        ]
        website = next(
            (
                url
                for url in action_urls
                if url.startswith("http") and "open.bymadata.com.ar" not in url
            ),
            "",
        )
        records.append(
            {
                "symbol": symbol,
                "company_name": company_name,
                "isin": _normalize_whitespace(
                    html.unescape(isin_match.group("isin")) if isin_match else ""
                ).upper(),
                "sector": _normalize_whitespace(
                    html.unescape(sector_match.group("sector")) if sector_match else ""
                ),
                "website": _normalize_whitespace(website),
                "street_address": _normalize_whitespace(
                    html.unescape(address_match.group("address")) if address_match else ""
                ),
                "panel_label": _normalize_whitespace(
                    html.unescape(panel_match.group("label")) if panel_match else ""
                ),
                "country": "Argentina",
            }
        )
        seen_symbols.add(symbol)
    return records


def _extract_argentina_cnv_emitter_regime_records(
    workbook_path: Path,
) -> list[dict[str, str]]:
    try:
        with zipfile.ZipFile(workbook_path) as archive:
            shared_strings = _load_xlsx_shared_strings(archive)
            sheet_targets = _load_xlsx_sheet_targets(archive)
            rows_by_sheet = [
                _iter_xlsx_sheet_rows(
                    archive,
                    worksheet_path=target,
                    shared_strings=shared_strings,
                )
                for _, target in sheet_targets
            ]
    except Exception:
        return []

    header_aliases = {
        "cuit": "cuit",
        "sociedad": "company_name",
        "categoria": "cnv_category",
        "estado categoria": "cnv_category_status",
    }
    records: list[dict[str, str]] = []
    for rows in rows_by_sheet:
        header_map: dict[str, int] | None = None
        for row in rows:
            normalized_row = [
                _normalize_argentina_cnv_emitter_regime_header(value)
                for value in row
            ]
            if header_map is None:
                candidate_map = {
                    header_aliases[value]: index
                    for index, value in enumerate(normalized_row)
                    if value in header_aliases
                }
                if {
                    "cuit",
                    "company_name",
                    "cnv_category",
                    "cnv_category_status",
                } <= set(candidate_map):
                    header_map = candidate_map
                continue
            if not any(_normalize_whitespace(value) for value in row):
                continue
            cuit = _format_argentina_cuit(
                row[header_map["cuit"]] if header_map["cuit"] < len(row) else ""
            )
            company_name = _normalize_whitespace(
                row[header_map["company_name"]]
                if header_map["company_name"] < len(row)
                else ""
            )
            if not cuit or not company_name:
                continue
            records.append(
                {
                    "cuit": cuit,
                    "company_name": company_name,
                    "cnv_category": _normalize_whitespace(
                        row[header_map["cnv_category"]]
                        if header_map["cnv_category"] < len(row)
                        else ""
                    ),
                    "cnv_category_status": _normalize_whitespace(
                        row[header_map["cnv_category_status"]]
                        if header_map["cnv_category_status"] < len(row)
                        else ""
                    ),
                }
            )
    return records


def _normalize_argentina_cnv_emitter_regime_header(value: str) -> str:
    normalized = _normalize_whitespace(_ascii_fold_alias(value) or value).casefold()
    return re.sub(r"\s+", " ", normalized)


def _argentina_cnv_emitter_regime_record_keys(
    record: dict[str, str],
) -> list[str]:
    keys: list[str] = []
    for candidate in _build_argentina_company_aliases(str(record.get("company_name", ""))):
        for key in (
            _normalize_company_name_for_match(candidate),
            _normalize_company_name_for_match(candidate, drop_suffixes=True),
        ):
            if key:
                keys.append(key)
    return _unique_aliases(keys)


def _match_argentina_cnv_emitter_regime_record(
    *,
    issuer_record: dict[str, object],
    records_by_key: dict[str, dict[str, str]],
) -> dict[str, str] | None:
    company_name = _normalize_whitespace(str(issuer_record.get("company_name", "")))
    if not company_name or not records_by_key:
        return None
    for candidate in _build_argentina_company_aliases(company_name):
        for key in (
            _normalize_company_name_for_match(candidate),
            _normalize_company_name_for_match(candidate, drop_suffixes=True),
        ):
            if key and key in records_by_key:
                return records_by_key[key]
    return None


def _merge_argentina_cnv_emitter_regime_record(
    *,
    issuer_record: dict[str, object],
    regime_record: dict[str, str],
) -> None:
    cuit = _format_argentina_cuit(str(regime_record.get("cuit", "")))
    if cuit and not str(issuer_record.get("cuit", "")):
        issuer_record["cuit"] = cuit
    category = _normalize_whitespace(str(regime_record.get("cnv_category", "")))
    if category and not str(issuer_record.get("cnv_category", "")):
        issuer_record["cnv_category"] = category
    category_status = _normalize_whitespace(
        str(regime_record.get("cnv_category_status", ""))
    )
    if category_status and not str(issuer_record.get("cnv_category_status", "")):
        issuer_record["cnv_category_status"] = category_status
    cnv_company_name = _normalize_whitespace(str(regime_record.get("company_name", "")))
    issuer_company_name = _normalize_whitespace(str(issuer_record.get("company_name", "")))
    if (
        cnv_company_name
        and issuer_company_name
        and cnv_company_name.casefold() != issuer_company_name.casefold()
        and not str(issuer_record.get("cnv_company_name", ""))
    ):
        issuer_record["cnv_company_name"] = cnv_company_name


def _extract_argentina_byma_society_general_snapshot(
    payload: object,
) -> dict[str, str] | None:
    row = _first_argentina_byma_payload_row(payload)
    if row is None:
        return None
    country = _normalize_whitespace(str(row.get("pais", "")))
    if country and country.casefold() != "argentina":
        return None
    return {
        "country": country,
        "activity": _normalize_whitespace(str(row.get("actividad", ""))),
        "website": _normalize_whitespace(str(row.get("website", ""))),
        "email": _normalize_whitespace(str(row.get("email", ""))),
        "phone": _normalize_whitespace(str(row.get("telefono", ""))),
        "constituted_on": _normalize_whitespace(str(row.get("fechaConstitucion", ""))),
        "fiscal_year_end": _normalize_whitespace(str(row.get("cierreEjercicio", ""))),
        "province_state": _normalize_whitespace(str(row.get("provinciaEstado", ""))),
        "city": _normalize_whitespace(str(row.get("localidad", ""))),
        "postal_code": _normalize_whitespace(str(row.get("codPostal", ""))),
        "street_address": _normalize_whitespace(
            ", ".join(
                value
                for value in (
                    _normalize_whitespace(str(row.get("calle", ""))),
                    _normalize_whitespace(str(row.get("piso", ""))),
                    _normalize_whitespace(str(row.get("dpto", ""))),
                )
                if value
            )
        ),
    }


def _extract_argentina_byma_species_snapshot(
    payload: object,
    *,
    symbol: str,
) -> dict[str, object] | None:
    row = _first_argentina_byma_payload_row(payload)
    if row is None:
        return None
    company_name = _normalize_whitespace(str(row.get("emisor", "")))
    if not company_name:
        return None
    isin = _normalize_whitespace(str(row.get("codigoIsin", ""))).upper()
    if isin and not re.fullmatch(r"[A-Z0-9]{12}", isin):
        isin = ""
    return {
        "symbol": symbol,
        "company_name": company_name,
        "isin": isin,
        "instrument_type": _normalize_whitespace(str(row.get("tipoEspecie", ""))),
        "share_class": _normalize_whitespace(str(row.get("denominacion", ""))),
        "settlement_currency": _normalize_whitespace(str(row.get("descripcion", ""))),
        "leader_panel": _normalize_whitespace(str(row.get("lider", ""))).casefold()
        == "si",
        "security_type": _normalize_whitespace(str(row.get("insType", ""))),
    }


def _first_argentina_byma_payload_row(payload: object) -> dict[str, object] | None:
    if not isinstance(payload, dict):
        return None
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        return None
    row = rows[0]
    if not isinstance(row, dict):
        return None
    return row


def _build_argentina_byma_issuer_record(
    *,
    symbol: str,
    species_snapshot: dict[str, object] | None,
    society_general_snapshot: dict[str, str] | None,
    listed_company_record: dict[str, str] | None,
) -> dict[str, object] | None:
    normalized_symbol = _normalize_whitespace(symbol).upper()
    company_name = _normalize_whitespace(
        str(
            (species_snapshot or {}).get("company_name")
            or (listed_company_record or {}).get("company_name")
            or ""
        )
    )
    if not company_name:
        return None
    isin = _normalize_whitespace(
        str(
            (species_snapshot or {}).get("isin")
            or (listed_company_record or {}).get("isin")
            or ""
        )
    ).upper()
    merged_society_snapshot = {
        **(listed_company_record or {}),
        **(society_general_snapshot or {}),
    }
    return {
        "company_name": company_name,
        "primary_symbol": normalized_symbol,
        "primary_isin": isin,
        "symbols": [normalized_symbol] if normalized_symbol else [],
        "isins": [isin] if isin else [],
        "instrument_type": str((species_snapshot or {}).get("instrument_type", "")),
        "security_type": str((species_snapshot or {}).get("security_type", "")),
        "share_classes": (
            [str((species_snapshot or {})["share_class"])]
            if str((species_snapshot or {}).get("share_class", ""))
            else []
        ),
        "settlement_currencies": (
            [str((species_snapshot or {})["settlement_currency"])]
            if str((species_snapshot or {}).get("settlement_currency", ""))
            else []
        ),
        "leader_panel": bool((species_snapshot or {}).get("leader_panel")),
        **merged_society_snapshot,
    }


def _merge_argentina_byma_issuer_record(
    *,
    issuer_record: dict[str, object],
    species_snapshot: dict[str, object] | None,
    society_general_snapshot: dict[str, str] | None,
    listed_company_record: dict[str, str] | None,
) -> None:
    if listed_company_record:
        company_name = _normalize_whitespace(str(listed_company_record.get("company_name", "")))
        if company_name and not str(issuer_record.get("company_name", "")):
            issuer_record["company_name"] = company_name
    symbol = _normalize_whitespace(
        str(
            (species_snapshot or {}).get("symbol")
            or (listed_company_record or {}).get("symbol")
            or ""
        )
    ).upper()
    if symbol and symbol not in issuer_record["symbols"]:
        issuer_record["symbols"].append(symbol)  # type: ignore[index]
    isin = _normalize_whitespace(
        str(
            (species_snapshot or {}).get("isin")
            or (listed_company_record or {}).get("isin")
            or ""
        )
    ).upper()
    if isin and isin not in issuer_record["isins"]:
        issuer_record["isins"].append(isin)  # type: ignore[index]
    share_class = str((species_snapshot or {}).get("share_class", ""))
    if share_class and share_class not in issuer_record["share_classes"]:
        issuer_record["share_classes"].append(share_class)  # type: ignore[index]
    settlement_currency = str((species_snapshot or {}).get("settlement_currency", ""))
    if (
        settlement_currency
        and settlement_currency not in issuer_record["settlement_currencies"]
    ):
        issuer_record["settlement_currencies"].append(settlement_currency)  # type: ignore[index]
    if not str(issuer_record.get("primary_isin", "")) and isin:
        issuer_record["primary_isin"] = isin
    if (species_snapshot or {}).get("leader_panel"):
        issuer_record["leader_panel"] = True
    for key, value in {**(listed_company_record or {}), **(society_general_snapshot or {})}.items():
        if value and not str(issuer_record.get(key, "")):
            issuer_record[key] = value


def _build_argentina_byma_issuer_entity(record: dict[str, object]) -> dict[str, object]:
    company_name = str(record["company_name"])
    symbols = [value for value in record.get("symbols", []) if isinstance(value, str)]
    isins = [value for value in record.get("isins", []) if isinstance(value, str)]
    aliases = [
        *_build_argentina_company_aliases(
            company_name,
            str(record.get("cnv_company_name", "")),
        ),
        *symbols,
        *isins,
    ]
    metadata: dict[str, object] = {
        "country_code": "ar",
        "category": "issuer",
        "instrument_exchange": "BYMA",
    }
    primary_symbol = _normalize_whitespace(str(record.get("primary_symbol", ""))).upper()
    primary_isin = _normalize_whitespace(str(record.get("primary_isin", ""))).upper()
    if primary_symbol:
        metadata["ticker"] = primary_symbol
    if primary_isin:
        metadata["isin"] = primary_isin
    if len(symbols) > 1:
        metadata["tickers"] = symbols
    if len(isins) > 1:
        metadata["isins"] = isins
    for input_key, metadata_key in (
        ("instrument_type", "instrument_type"),
        ("security_type", "security_type"),
        ("activity", "activity"),
        ("sector", "sector"),
        ("website", "issuer_website"),
        ("email", "issuer_email"),
        ("phone", "issuer_phone"),
        ("constituted_on", "constituted_on"),
        ("fiscal_year_end", "fiscal_year_end"),
        ("country", "country"),
        ("province_state", "province_state"),
        ("city", "city"),
        ("postal_code", "postal_code"),
        ("street_address", "street_address"),
        ("cuit", "cuit"),
        ("cnv_category", "cnv_category"),
        ("cnv_category_status", "cnv_category_status"),
    ):
        value = _normalize_whitespace(str(record.get(input_key, "")))
        if value:
            metadata[metadata_key] = value
    share_classes = [
        value for value in record.get("share_classes", []) if isinstance(value, str)
    ]
    if share_classes:
        metadata["share_classes"] = share_classes
    settlement_currencies = [
        value
        for value in record.get("settlement_currencies", [])
        if isinstance(value, str)
    ]
    if settlement_currencies:
        metadata["settlement_currencies"] = settlement_currencies
    panel_label = _normalize_whitespace(str(record.get("panel_label", "")))
    if panel_label:
        metadata["byma_panel"] = panel_label
    if bool(record.get("leader_panel")):
        metadata["leader_panel"] = True
    entity_id_suffix = primary_isin or _slug(company_name)
    return {
        "entity_type": "organization",
        "canonical_text": company_name,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-ar-issuer:{entity_id_suffix}",
        "metadata": metadata,
    }


def _build_argentina_byma_ticker_entity(
    *,
    symbol: str,
    species_snapshot: dict[str, object] | None,
    society_general_snapshot: dict[str, str] | None,
    listed_company_record: dict[str, str] | None,
) -> dict[str, object] | None:
    normalized_symbol = _normalize_whitespace(symbol).upper()
    company_name = _normalize_whitespace(
        str(
            (species_snapshot or {}).get("company_name")
            or (listed_company_record or {}).get("company_name")
            or ""
        )
    )
    if not normalized_symbol or not company_name:
        return None
    isin = _normalize_whitespace(
        str(
            (species_snapshot or {}).get("isin")
            or (listed_company_record or {}).get("isin")
            or ""
        )
    ).upper()
    aliases = [symbol, *_build_argentina_company_aliases(company_name)]
    if normalized_symbol != symbol:
        aliases.append(normalized_symbol)
    if isin:
        aliases.append(isin)
    metadata: dict[str, object] = {
        "country_code": "ar",
        "category": "ticker",
        "ticker": normalized_symbol,
        "issuer_name": company_name,
        "instrument_exchange": "BYMA",
    }
    if isin:
        metadata["isin"] = isin
    issuer_cuit = _format_argentina_cuit(
        str(
            (society_general_snapshot or {}).get("cuit")
            or (listed_company_record or {}).get("cuit")
            or ""
        )
    )
    if issuer_cuit:
        metadata["issuer_cuit"] = issuer_cuit
    cnv_category = _normalize_whitespace(
        str(
            (society_general_snapshot or {}).get("cnv_category")
            or (listed_company_record or {}).get("cnv_category")
            or ""
        )
    )
    if cnv_category:
        metadata["cnv_category"] = cnv_category
    cnv_category_status = _normalize_whitespace(
        str(
            (society_general_snapshot or {}).get("cnv_category_status")
            or (listed_company_record or {}).get("cnv_category_status")
            or ""
        )
    )
    if cnv_category_status:
        metadata["cnv_category_status"] = cnv_category_status
    for input_key, metadata_key in (
        ("instrument_type", "instrument_type"),
        ("security_type", "security_type"),
        ("share_class", "share_class"),
        ("settlement_currency", "settlement_currency"),
    ):
        value = _normalize_whitespace(str((species_snapshot or {}).get(input_key, "")))
        if value:
            metadata[metadata_key] = value
    website = _normalize_whitespace(
        str(
            (society_general_snapshot or {}).get("website")
            or (listed_company_record or {}).get("website")
            or ""
        )
    )
    if website:
        metadata["issuer_website"] = website
    country = _normalize_whitespace(
        str(
            (society_general_snapshot or {}).get("country")
            or (listed_company_record or {}).get("country")
            or ""
        )
    )
    if country:
        metadata["country"] = country
    sector = _normalize_whitespace(str((listed_company_record or {}).get("sector", "")))
    if sector:
        metadata["sector"] = sector
    panel_label = _normalize_whitespace(
        str((listed_company_record or {}).get("panel_label", ""))
    )
    if panel_label:
        metadata["byma_panel"] = panel_label
    if (species_snapshot or {}).get("leader_panel"):
        metadata["leader_panel"] = True
    return {
        "entity_type": "ticker",
        "canonical_text": normalized_symbol,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-ar-ticker:{normalized_symbol}",
        "metadata": metadata,
    }


def _extract_argentina_byma_people_entities(
    *,
    administration_payload: object,
    responsables_payload: object,
    issuer_record: dict[str, object],
) -> list[dict[str, object]]:
    company_name = str(issuer_record["company_name"])
    employer_ticker = str(issuer_record.get("primary_symbol", ""))
    employer_isin = str(issuer_record.get("primary_isin", ""))
    entities: list[dict[str, object]] = []
    administration_rows = (
        administration_payload.get("data", [])
        if isinstance(administration_payload, dict)
        else []
    )
    for row in administration_rows:
        if not isinstance(row, dict):
            continue
        canonical_name, aliases = _normalize_argentina_byma_person_name_aliases(
            str(row.get("persona", ""))
        )
        if not canonical_name:
            continue
        role_title = _normalize_whitespace(str(row.get("cargo", "")))
        role_class = _role_class_for_argentina_byma_role(
            role_title,
            source_name="bymadata-sociedades-administracion",
        )
        entities.append(
            {
                "entity_type": "person",
                "canonical_text": canonical_name,
                "aliases": _unique_aliases(aliases),
                "entity_id": (
                    f"finance-ar-person:{(employer_ticker or employer_isin or _slug(company_name))}:"
                    f"{_slug(canonical_name)}:{_slug(role_class)}"
                ),
                "metadata": {
                    "country_code": "ar",
                    "category": role_class,
                    "role_title": role_title,
                    "employer_name": company_name,
                    "employer_ticker": employer_ticker,
                    "employer_isin": employer_isin,
                    "source_name": "bymadata-sociedades-administracion",
                },
            }
        )
    responsables_rows = (
        responsables_payload.get("data", [])
        if isinstance(responsables_payload, dict)
        else []
    )
    for row in responsables_rows:
        if not isinstance(row, dict):
            continue
        canonical_name, aliases = _normalize_argentina_byma_person_name_aliases(
            str(row.get("responsableMercado", ""))
        )
        if not canonical_name:
            continue
        market_role = _normalize_whitespace(str(row.get("cargo", "")))
        company_role = _normalize_whitespace(str(row.get("cargoEmpresa", "")))
        role_title = company_role or "Responsable de Mercado"
        metadata: dict[str, object] = {
            "country_code": "ar",
            "category": "market_representative",
            "role_title": role_title,
            "employer_name": company_name,
            "employer_ticker": employer_ticker,
            "employer_isin": employer_isin,
            "source_name": "bymadata-sociedades-responsables",
        }
        if market_role:
            metadata["market_role"] = market_role
        designated_on = _normalize_whitespace(str(row.get("fechaDesignacion", "")))
        if designated_on:
            metadata["designated_on"] = designated_on
        entities.append(
            {
                "entity_type": "person",
                "canonical_text": canonical_name,
                "aliases": _unique_aliases(aliases),
                "entity_id": (
                    f"finance-ar-person:{(employer_ticker or employer_isin or _slug(company_name))}:"
                    f"{_slug(canonical_name)}:market-representative"
                ),
                "metadata": metadata,
            }
        )
    return entities


def _enrich_argentina_byma_people_with_issuer_metadata(
    *,
    people_entities: list[dict[str, object]],
    issuer_records: list[dict[str, object]],
) -> None:
    issuer_records_by_symbol: dict[str, dict[str, object]] = {}
    issuer_records_by_name: dict[str, dict[str, object]] = {}
    for issuer_record in issuer_records:
        for symbol in issuer_record.get("symbols", []):
            if isinstance(symbol, str):
                normalized_symbol = _normalize_whitespace(symbol).upper()
                if normalized_symbol:
                    issuer_records_by_symbol[normalized_symbol] = issuer_record
        company_name = _normalize_whitespace(str(issuer_record.get("company_name", "")))
        for key in (
            _normalize_company_name_for_match(company_name),
            _normalize_company_name_for_match(company_name, drop_suffixes=True),
        ):
            if key:
                issuer_records_by_name[key] = issuer_record

    for entity in people_entities:
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        issuer_record: dict[str, object] | None = None
        employer_ticker = _normalize_whitespace(
            str(metadata.get("employer_ticker", ""))
        ).upper()
        if employer_ticker:
            issuer_record = issuer_records_by_symbol.get(employer_ticker)
        if issuer_record is None:
            employer_name = _normalize_whitespace(str(metadata.get("employer_name", "")))
            for key in (
                _normalize_company_name_for_match(employer_name),
                _normalize_company_name_for_match(employer_name, drop_suffixes=True),
            ):
                if key and key in issuer_records_by_name:
                    issuer_record = issuer_records_by_name[key]
                    break
        if issuer_record is None:
            continue
        cuit = _format_argentina_cuit(str(issuer_record.get("cuit", "")))
        if cuit and not _normalize_whitespace(str(metadata.get("employer_cuit", ""))):
            metadata["employer_cuit"] = cuit
        cnv_category = _normalize_whitespace(str(issuer_record.get("cnv_category", "")))
        if cnv_category and not _normalize_whitespace(
            str(metadata.get("employer_cnv_category", ""))
        ):
            metadata["employer_cnv_category"] = cnv_category
        cnv_category_status = _normalize_whitespace(
            str(issuer_record.get("cnv_category_status", ""))
        )
        if cnv_category_status and not _normalize_whitespace(
            str(metadata.get("employer_cnv_category_status", ""))
        ):
            metadata["employer_cnv_category_status"] = cnv_category_status


def _enrich_argentina_ticker_entities_with_issuer_metadata(
    *,
    ticker_entities: list[dict[str, object]],
    issuer_records: list[dict[str, object]],
) -> None:
    issuer_records_by_symbol: dict[str, dict[str, object]] = {}
    for issuer_record in issuer_records:
        for symbol in issuer_record.get("symbols", []):
            if isinstance(symbol, str):
                normalized_symbol = _normalize_whitespace(symbol).upper()
                if normalized_symbol:
                    issuer_records_by_symbol[normalized_symbol] = issuer_record

    for entity in ticker_entities:
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        ticker = _normalize_whitespace(str(metadata.get("ticker", ""))).upper()
        issuer_record = issuer_records_by_symbol.get(ticker)
        if issuer_record is None:
            continue
        issuer_cuit = _format_argentina_cuit(str(issuer_record.get("cuit", "")))
        if issuer_cuit and not _normalize_whitespace(str(metadata.get("issuer_cuit", ""))):
            metadata["issuer_cuit"] = issuer_cuit
        cnv_category = _normalize_whitespace(str(issuer_record.get("cnv_category", "")))
        if cnv_category and not _normalize_whitespace(str(metadata.get("cnv_category", ""))):
            metadata["cnv_category"] = cnv_category
        cnv_category_status = _normalize_whitespace(
            str(issuer_record.get("cnv_category_status", ""))
        )
        if cnv_category_status and not _normalize_whitespace(
            str(metadata.get("cnv_category_status", ""))
        ):
            metadata["cnv_category_status"] = cnv_category_status


def _role_class_for_argentina_byma_role(
    role_title: str,
    *,
    source_name: str,
) -> str:
    if source_name == "bymadata-sociedades-responsables":
        return "market_representative"
    normalized = _normalize_whitespace(_ascii_fold_alias(role_title) or role_title).casefold()
    for role_class, keywords in _ARGENTINA_BYMA_PERSON_ROLE_CLASS_KEYWORDS.items():
        if any(keyword in normalized for keyword in keywords):
            return role_class
    return "personnel"


def _normalize_argentina_byma_person_name_aliases(
    raw_name: str,
) -> tuple[str | None, list[str]]:
    normalized_raw_name = _normalize_whitespace(raw_name)
    if not normalized_raw_name:
        return None, []
    candidate_names = [normalized_raw_name]
    if "," in normalized_raw_name:
        surname, given_names = [
            _normalize_whitespace(part)
            for part in normalized_raw_name.split(",", maxsplit=1)
        ]
        if surname and given_names:
            candidate_names.insert(0, f"{given_names} {surname}")
    else:
        reordered_name = _reorder_latin_name_tokens(normalized_raw_name)
        if reordered_name:
            candidate_names.append(reordered_name)
    aliases: list[str] = []
    canonical_name: str | None = None
    for candidate in candidate_names:
        titled_candidate = _normalize_whitespace(_smart_titlecase_name(candidate))
        if not titled_candidate:
            continue
        if not _looks_like_argentina_person_name(titled_candidate):
            continue
        if canonical_name is None:
            canonical_name = titled_candidate
        aliases.append(titled_candidate)
        if candidate.casefold() != titled_candidate.casefold():
            aliases.append(candidate)
        ascii_alias = _ascii_fold_alias(titled_candidate)
        if ascii_alias and ascii_alias.casefold() != titled_candidate.casefold():
            aliases.append(ascii_alias)
    if canonical_name is None:
        return None, []
    if normalized_raw_name.casefold() != canonical_name.casefold():
        aliases.append(normalized_raw_name)
        raw_ascii_alias = _ascii_fold_alias(normalized_raw_name)
        if raw_ascii_alias and raw_ascii_alias.casefold() != normalized_raw_name.casefold():
            aliases.append(raw_ascii_alias)
    return canonical_name, _unique_aliases(aliases)


def _build_argentina_person_name_query_variants(value: str) -> list[str]:
    normalized_value = _normalize_whitespace(value)
    if not normalized_value:
        return []
    variants = [normalized_value]
    reordered_value = _reorder_latin_name_tokens(normalized_value)
    if reordered_value:
        variants.append(reordered_value)
    return _unique_aliases(variants)


def _looks_like_argentina_person_name(value: str) -> bool:
    parts = [part for part in re.split(r"\s+", value) if part]
    if len(parts) < 2:
        return False
    substantive_parts = 0
    for part in parts:
        normalized = (
            part.replace(".", "")
            .replace("'", "")
            .replace("´", "")
            .replace("-", "")
        )
        if not normalized or not normalized[0].isalpha():
            return False
        if not all(character.isalpha() for character in normalized):
            return False
        if len(normalized) > 1:
            substantive_parts += 1
    return substantive_parts >= 2


def _build_argentina_company_aliases(*values: str) -> list[str]:
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
        ascii_alias = _ascii_fold_alias(candidate)
        if ascii_alias and ascii_alias.casefold() not in seen:
            pending.append(_normalize_whitespace(ascii_alias))
        dotless_alias = _normalize_whitespace(candidate.replace(".", ""))
        if dotless_alias and dotless_alias.casefold() not in seen:
            pending.append(dotless_alias)
        stripped = _strip_argentina_company_legal_suffix(candidate)
        if stripped and stripped.casefold() not in seen:
            pending.append(stripped)
    return aliases


def _normalize_argentina_cuit_key(value: str) -> str:
    digits = re.sub(r"\D+", "", value)
    return digits if len(digits) == 11 else ""


def _format_argentina_cuit(value: str) -> str:
    digits = _normalize_argentina_cuit_key(value)
    if not digits:
        return ""
    return f"{digits[:2]}-{digits[2:10]}-{digits[10:]}"


def _strip_argentina_company_legal_suffix(value: str) -> str | None:
    candidate = _normalize_whitespace(value).rstrip(",")
    previous = ""
    while candidate and candidate.casefold() != previous.casefold():
        previous = candidate
        for pattern in _ARGENTINA_COMPANY_LEGAL_SUFFIX_PATTERNS:
            updated = _normalize_whitespace(pattern.sub("", candidate)).rstrip(",")
            if updated != candidate:
                candidate = updated
                break
        else:
            break
    return candidate or None


def _reorder_latin_name_tokens(value: str) -> str | None:
    normalized_value = _normalize_whitespace(value)
    if not normalized_value or "," in normalized_value:
        return None
    parts = [part for part in re.split(r"\s+", normalized_value) if part]
    if len(parts) < 2 or len(parts) > 4:
        return None
    reordered_value = " ".join([*parts[1:], parts[0]])
    if reordered_value.casefold() == normalized_value.casefold():
        return None
    return reordered_value


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
    ticker = str(issuer_record.get("ticker", "")).strip()
    if ticker:
        metadata["ticker"] = ticker
    market_segment = str(issuer_record.get("market_segment", "")).strip()
    if market_segment:
        metadata["market_segment"] = market_segment
    currency = str(issuer_record.get("currency", "")).strip()
    if currency:
        metadata["currency"] = currency
    market_cap_millions = str(issuer_record.get("market_cap_millions", "")).strip()
    if market_cap_millions:
        metadata["market_cap_millions"] = market_cap_millions
    share_price = str(issuer_record.get("share_price", "")).strip()
    if share_price:
        metadata["share_price"] = share_price
    price_change = str(issuer_record.get("price_change", "")).strip()
    if price_change:
        metadata["price_change"] = price_change
    price_change_percent = str(issuer_record.get("price_change_percent", "")).strip()
    if price_change_percent:
        metadata["price_change_percent"] = price_change_percent
    source_names = issuer_record.get("source_names")
    if isinstance(source_names, list):
        normalized_source_names = [
            _normalize_whitespace(str(value))
            for value in source_names
            if _normalize_whitespace(str(value))
        ]
        if normalized_source_names:
            metadata["source_names"] = _unique_aliases(normalized_source_names)
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


def _build_uk_aim_ticker_entity(
    *,
    issuer_record: dict[str, str],
    company_number: str | None,
) -> dict[str, object] | None:
    ticker = str(issuer_record.get("ticker", "")).strip().upper()
    if not ticker:
        return None
    company_name = str(issuer_record["company_name"])
    aliases = [ticker]
    metadata: dict[str, object] = {
        "country_code": "uk",
        "category": "ticker",
        "ticker": ticker,
        "issuer_name": company_name,
        "market_segment": str(issuer_record.get("market_segment", "")).strip() or "AIM",
    }
    if company_number:
        metadata["company_number"] = company_number
    isin = str(issuer_record.get("isin", "")).strip()
    if isin:
        metadata["isin"] = isin
    currency = str(issuer_record.get("currency", "")).strip()
    if currency:
        metadata["currency"] = currency
    market_cap_millions = str(issuer_record.get("market_cap_millions", "")).strip()
    if market_cap_millions:
        metadata["market_cap_millions"] = market_cap_millions
    share_price = str(issuer_record.get("share_price", "")).strip()
    if share_price:
        metadata["share_price"] = share_price
    return {
        "entity_type": "ticker",
        "canonical_text": ticker,
        "aliases": _unique_aliases(aliases),
        "entity_id": f"finance-uk-ticker:{ticker.casefold()}",
        "metadata": metadata,
    }


def _extract_uk_aim_constituent_records(html_text: str) -> list[dict[str, str]]:
    records: dict[str, dict[str, str]] = {}
    for row_html in re.findall(
        r'<tr[^>]*class="[^"]*slide-panel[^"]*"[^>]*>(?P<row>.*?)</tr>',
        html_text,
        re.IGNORECASE | re.DOTALL,
    ):
        cells = [
            _normalize_whitespace(_strip_html(cell))
            for cell in re.findall(
                r"<td[^>]*>(.*?)</td>",
                row_html,
                re.IGNORECASE | re.DOTALL,
            )
        ]
        if len(cells) < 2:
            continue
        ticker = cells[0].upper()
        raw_security_name = cells[1]
        company_name = _normalize_uk_aim_security_name_to_company_name(raw_security_name)
        if not ticker or not company_name:
            continue
        normalized_key = _normalize_company_name_for_match(company_name, drop_suffixes=True)
        if not normalized_key:
            continue
        record = {
            "raw_company_name": raw_security_name,
            "company_name": company_name,
            "listing_category": "Equity shares (AIM companies)",
            "market_status": "AIM",
            "trading_venue": "LSE AIM",
            "isin": "",
            "ticker": ticker,
            "market_segment": "AIM",
            "currency": cells[2] if len(cells) > 2 else "",
            "market_cap_millions": cells[3] if len(cells) > 3 else "",
            "share_price": cells[4] if len(cells) > 4 else "",
            "price_change": cells[5] if len(cells) > 5 else "",
            "price_change_percent": cells[6] if len(cells) > 6 else "",
            "source_name": "lse-aim-all-share-constituents",
        }
        existing = records.get(normalized_key)
        if existing is None:
            records[normalized_key] = record
            continue
        for field, value in record.items():
            if value and not existing.get(field):
                existing[field] = value
    return sorted(records.values(), key=lambda item: item["company_name"].casefold())


def _normalize_uk_aim_security_name_to_company_name(value: str) -> str | None:
    candidate = _normalize_whitespace(value)
    if not candidate:
        return None
    for legal_suffix in (" PLC", " LIMITED", " LTD", " LLP", " INC", " CORPORATION"):
        upper_candidate = candidate.upper()
        index = upper_candidate.find(legal_suffix)
        if index != -1:
            candidate = candidate[: index + len(legal_suffix)].strip(" ,")
            break
    for pattern in _UK_AIM_SECURITY_NAME_TRAILING_PATTERNS:
        candidate = _normalize_whitespace(pattern.sub("", candidate))
    if not candidate:
        return None
    return _smart_titlecase_company_name(candidate)


def _extract_uk_lse_directory_last_page(html_text: str) -> int:
    page_numbers = [1]
    for page_text in re.findall(
        (
            r'/indices/ftse-aim-all-share/constituents/table'
            r'(?:\?page=(?P<page>\d+))?'
        ),
        html_text,
        re.IGNORECASE,
    ):
        if not page_text:
            page_numbers.append(1)
            continue
        try:
            page_numbers.append(int(page_text))
        except ValueError:
            continue
    return max(page_numbers) if page_numbers else 1


def _merge_uk_issuer_records(
    records: list[dict[str, str]],
) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    for record in records:
        company_name = _normalize_whitespace(str(record.get("company_name", "")))
        if not company_name:
            continue
        key = _normalize_company_name_for_match(company_name, drop_suffixes=True)
        if not key:
            continue
        normalized_record = {
            key_name: _normalize_whitespace(str(value))
            for key_name, value in record.items()
        }
        normalized_record["company_name"] = company_name
        normalized_record["raw_company_name"] = _normalize_whitespace(
            str(record.get("raw_company_name", "")) or company_name
        )
        existing = merged.get(key)
        if existing is None:
            source_name = _normalize_whitespace(
                str(record.get("source_name", "")) or "uk-issuer-directory"
            )
            if source_name:
                normalized_record["source_names"] = [source_name]
            merged[key] = normalized_record
            continue
        if (
            normalized_record.get("listing_category")
            == "Equity shares (commercial companies)"
            and existing.get("listing_category") != "Equity shares (commercial companies)"
        ):
            existing["listing_category"] = normalized_record["listing_category"]
        for field, value in normalized_record.items():
            if value and not existing.get(field):
                existing[field] = value
        preferred_company_name = max(
            [existing["company_name"], normalized_record["company_name"]],
            key=lambda item: (
                item.endswith("PLC"),
                len(item),
                item.casefold(),
            ),
        )
        existing["company_name"] = preferred_company_name
        source_names = _unique_aliases(
            [
                *(
                    existing.get("source_names", [])
                    if isinstance(existing.get("source_names"), list)
                    else []
                ),
                _normalize_whitespace(str(existing.get("source_name", ""))),
                _normalize_whitespace(str(normalized_record.get("source_name", ""))),
            ]
        )
        if source_names:
            existing["source_names"] = source_names
    return sorted(merged.values(), key=lambda item: item["company_name"].casefold())


def _extract_companies_house_psc_people_entities(
    html_text: str,
    *,
    employer_name: str,
    employer_company_number: str,
    employer_ticker: str = "",
) -> list[dict[str, object]]:
    entities: list[dict[str, object]] = []
    for record in _extract_companies_house_psc_records(html_text):
        raw_name = str(record.get("holder_name", ""))
        canonical_name, aliases = _normalize_person_name_aliases(raw_name)
        if not canonical_name:
            continue
        metadata: dict[str, object] = {
            "country_code": "uk",
            "category": "shareholder",
            "role_title": "Person with Significant Control",
            "employer_name": employer_name,
            "employer_company_number": employer_company_number,
            "source_name": "companies-house-psc",
        }
        if employer_ticker:
            metadata["employer_ticker"] = employer_ticker
        notified_on = _normalize_whitespace(str(record.get("notified_on", "")))
        if notified_on:
            metadata["notified_on"] = notified_on
        nationality = _normalize_whitespace(str(record.get("nationality", "")))
        if nationality:
            metadata["nationality"] = nationality
        country_of_residence = _normalize_whitespace(
            str(record.get("country_of_residence", ""))
        )
        if country_of_residence:
            metadata["country_of_residence"] = country_of_residence
        nature_of_control = [
            _normalize_whitespace(str(value))
            for value in record.get("nature_of_control", [])
            if _normalize_whitespace(str(value))
        ]
        if nature_of_control:
            metadata["nature_of_control"] = nature_of_control
        entities.append(
            {
                "entity_type": "person",
                "canonical_text": canonical_name,
                "aliases": _unique_aliases(aliases),
                "entity_id": (
                    f"finance-uk-person:{employer_company_number}:"
                    f"{_slug(canonical_name)}:shareholder"
                ),
                "metadata": metadata,
            }
        )
    return entities


def _extract_companies_house_psc_organization_records(
    html_text: str,
    *,
    employer_name: str,
    employer_company_number: str,
    employer_ticker: str = "",
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for record in _extract_companies_house_psc_records(html_text):
        holder_name = _normalize_whitespace(str(record.get("holder_name", "")))
        if not holder_name:
            continue
        canonical_name, _ = _normalize_person_name_aliases(holder_name)
        if canonical_name is not None:
            continue
        key = (holder_name.casefold(), employer_company_number)
        if key in seen:
            continue
        seen.add(key)
        nature_of_control = " | ".join(
            _normalize_whitespace(str(value))
            for value in record.get("nature_of_control", [])
            if _normalize_whitespace(str(value))
        )
        records.append(
            {
                "holder_name": holder_name,
                "employer_name": employer_name,
                "employer_company_number": employer_company_number,
                "employer_ticker": employer_ticker,
                "notified_on": _normalize_whitespace(str(record.get("notified_on", ""))),
                "nature_of_control": nature_of_control,
                "registration_number": _normalize_whitespace(
                    str(record.get("registration_number", ""))
                ),
                "legal_form": _normalize_whitespace(str(record.get("legal_form", ""))),
                "governing_law": _normalize_whitespace(
                    str(record.get("governing_law", ""))
                ),
                "source_name": "companies-house-psc",
            }
        )
    return records


def _extract_companies_house_psc_records(html_text: str) -> list[dict[str, object]]:
    if (
        "The PSC information is not available because:" in html_text
        or "There are no persons with significant control or statements available for this company."
        in html_text
    ):
        return []
    records: list[dict[str, object]] = []
    for match in re.finditer(
        r'<div class="appointment-(?P<index>\d+)">(?P<block>.*?)(?=<div class="appointment-\d+">|$)',
        html_text,
        re.IGNORECASE | re.DOTALL,
    ):
        block = match.group("block")
        name_match = re.search(
            r'<span id="psc-name-[^"]+">\s*(?:<span[^>]*>)?(?:<b>)?(?P<name>.*?)(?:</b>)?(?:</span>)?\s*</span>',
            block,
            re.IGNORECASE | re.DOTALL,
        )
        status_match = re.search(
            r'<span id="psc-status-tag-[^"]+"[^>]*>(?P<status>.*?)</span>',
            block,
            re.IGNORECASE | re.DOTALL,
        )
        if name_match is None:
            continue
        status_text = (
            _normalize_whitespace(_strip_html(status_match.group("status")))
            if status_match is not None
            else ""
        )
        if status_text and status_text.casefold() != "active":
            continue
        holder_name = _normalize_whitespace(_strip_html(name_match.group("name")))
        if not holder_name:
            continue
        records.append(
            {
                "holder_name": holder_name,
                "notified_on": _extract_companies_house_psc_field_value(
                    block, "notified-on"
                ),
                "nationality": _extract_companies_house_psc_field_value(
                    block, "nationality"
                ),
                "country_of_residence": _extract_companies_house_psc_field_value(
                    block, "country-of-residence"
                ),
                "registration_number": _extract_companies_house_psc_field_value(
                    block, "registration-number"
                ),
                "legal_form": _extract_companies_house_psc_field_value(
                    block, "legal-form"
                ),
                "governing_law": _extract_companies_house_psc_field_value(
                    block, "legal-authority"
                ),
                "nature_of_control": re.findall(
                    r'<dd id="psc-noc-[^"]+" class="data">\s*(?P<value>.*?)\s*</dd>',
                    block,
                    re.IGNORECASE | re.DOTALL,
                ),
            }
        )
    for record in records:
        record["nature_of_control"] = [
            _normalize_whitespace(_strip_html(str(value)))
            for value in record.get("nature_of_control", [])
            if _normalize_whitespace(_strip_html(str(value)))
        ]
    return records


def _extract_companies_house_psc_field_value(block: str, field_key: str) -> str:
    match = re.search(
        rf'<dd id="psc-{re.escape(field_key)}-[^"]+" class="data">\s*(?P<value>.*?)\s*</dd>',
        block,
        re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        return ""
    return _normalize_whitespace(_strip_html(match.group("value")))


def _build_uk_organization_entity_lookup(
    entities: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    lookup: dict[str, dict[str, object]] = {}
    for entity in entities:
        if str(entity.get("entity_type")) != "organization":
            continue
        values = [str(entity.get("canonical_text", ""))]
        values.extend(
            str(value)
            for value in entity.get("aliases", [])
            if _normalize_whitespace(str(value))
        )
        for value in values:
            for key in (
                _normalize_company_name_for_match(value),
                _normalize_company_name_for_match(value, drop_suffixes=True),
            ):
                if key and key not in lookup:
                    lookup[key] = entity
    return lookup


def _build_uk_major_shareholder_organization_entities(
    *,
    shareholder_records: list[dict[str, str]],
    organization_lookup: dict[str, dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    grouped_records: dict[str, list[dict[str, str]]] = {}
    for shareholder_record in shareholder_records:
        holder_name = _normalize_whitespace(str(shareholder_record.get("holder_name", "")))
        if not holder_name:
            continue
        group_key = _normalize_company_name_for_match(holder_name, drop_suffixes=True)
        if not group_key:
            continue
        grouped_records.setdefault(group_key, []).append(dict(shareholder_record))
    entities: list[dict[str, object]] = []
    for records in grouped_records.values():
        holder_names = [
            _normalize_whitespace(str(record.get("holder_name", "")))
            for record in records
            if _normalize_whitespace(str(record.get("holder_name", "")))
        ]
        if not holder_names:
            continue
        preferred_name = max(
            set(holder_names),
            key=lambda item: (holder_names.count(item), len(item), item.casefold()),
        )
        aliases = _unique_aliases(holder_names)
        matched_entity = _find_matching_uk_organization_entity(
            holder_names=holder_names,
            organization_lookup=organization_lookup,
        )
        if matched_entity is not None:
            _merge_uk_major_shareholding_metadata(
                entity=matched_entity,
                holder_aliases=aliases,
                shareholding_records=records,
            )
            continue
        entity: dict[str, object] = {
            "entity_type": "organization",
            "canonical_text": preferred_name,
            "aliases": aliases,
            "entity_id": f"finance-uk-major-shareholder-org:{_slug(preferred_name)}",
            "metadata": {
                "country_code": "uk",
                "category": "major_shareholder_organization",
            },
        }
        _merge_uk_major_shareholding_metadata(
            entity=entity,
            holder_aliases=aliases,
            shareholding_records=records,
        )
        entities.append(entity)
    return entities


def _find_matching_uk_organization_entity(
    *,
    holder_names: list[str],
    organization_lookup: dict[str, dict[str, object]] | None,
) -> dict[str, object] | None:
    if not organization_lookup:
        return None
    for holder_name in holder_names:
        for key in (
            _normalize_company_name_for_match(holder_name),
            _normalize_company_name_for_match(holder_name, drop_suffixes=True),
        ):
            if key and key in organization_lookup:
                return organization_lookup[key]
    return None


def _merge_uk_major_shareholding_metadata(
    *,
    entity: dict[str, object],
    holder_aliases: list[str],
    shareholding_records: list[dict[str, str]],
) -> None:
    metadata = entity.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        return
    aliases = _unique_aliases(
        [
            *(entity.get("aliases", []) if isinstance(entity.get("aliases"), list) else []),
            *holder_aliases,
        ]
    )
    entity["aliases"] = aliases
    merged_records: list[dict[str, str]] = [
        {
            "holder_name": _normalize_whitespace(str(item.get("holder_name", ""))),
            "employer_name": _normalize_whitespace(str(item.get("employer_name", ""))),
            "employer_company_number": _normalize_whitespace(
                str(item.get("employer_company_number", ""))
            ),
            "employer_ticker": _normalize_whitespace(str(item.get("employer_ticker", ""))),
            "notified_on": _normalize_whitespace(str(item.get("notified_on", ""))),
            "nature_of_control": _normalize_whitespace(
                str(item.get("nature_of_control", ""))
            ),
            "registration_number": _normalize_whitespace(
                str(item.get("registration_number", ""))
            ),
            "legal_form": _normalize_whitespace(str(item.get("legal_form", ""))),
            "governing_law": _normalize_whitespace(str(item.get("governing_law", ""))),
            "source_name": _normalize_whitespace(str(item.get("source_name", ""))),
        }
        for item in metadata.get("major_shareholding_records", [])
        if isinstance(item, dict)
    ]
    seen = {
        (
            item.get("holder_name", "").casefold(),
            item.get("employer_company_number", "").casefold(),
            item.get("registration_number", "").casefold(),
        )
        for item in merged_records
    }
    for shareholder_record in shareholding_records:
        normalized_record = {
            "holder_name": _normalize_whitespace(str(shareholder_record.get("holder_name", ""))),
            "employer_name": _normalize_whitespace(
                str(shareholder_record.get("employer_name", ""))
            ),
            "employer_company_number": _normalize_whitespace(
                str(shareholder_record.get("employer_company_number", ""))
            ),
            "employer_ticker": _normalize_whitespace(
                str(shareholder_record.get("employer_ticker", ""))
            ),
            "notified_on": _normalize_whitespace(str(shareholder_record.get("notified_on", ""))),
            "nature_of_control": _normalize_whitespace(
                str(shareholder_record.get("nature_of_control", ""))
            ),
            "registration_number": _normalize_whitespace(
                str(shareholder_record.get("registration_number", ""))
            ),
            "legal_form": _normalize_whitespace(str(shareholder_record.get("legal_form", ""))),
            "governing_law": _normalize_whitespace(
                str(shareholder_record.get("governing_law", ""))
            ),
            "source_name": _normalize_whitespace(str(shareholder_record.get("source_name", ""))),
        }
        key = (
            normalized_record["holder_name"].casefold(),
            normalized_record["employer_company_number"].casefold(),
            normalized_record["registration_number"].casefold(),
        )
        if key in seen:
            continue
        seen.add(key)
        merged_records.append(normalized_record)
    if merged_records:
        metadata["major_shareholding_records"] = merged_records
        metadata["major_shareholder_of_companies"] = _unique_aliases(
            [
                str(item["employer_name"])
                for item in merged_records
                if _normalize_whitespace(str(item.get("employer_name", "")))
            ]
        )
        metadata["major_shareholder_of_tickers"] = _unique_aliases(
            [
                str(item["employer_ticker"])
                for item in merged_records
                if _normalize_whitespace(str(item.get("employer_ticker", "")))
            ]
        )


def _build_united_kingdom_issuer_lookup(
    issuer_entities: list[dict[str, object]],
) -> dict[str, dict[str, dict[str, object]]]:
    by_company_number: dict[str, dict[str, object]] = {}
    by_ticker: dict[str, dict[str, object]] = {}
    by_name: dict[str, dict[str, object]] = {}
    for entity in issuer_entities:
        if str(entity.get("entity_type", "")) != "organization":
            continue
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        company_number = _normalize_whitespace(str(metadata.get("company_number", ""))).upper()
        if company_number and company_number not in by_company_number:
            by_company_number[company_number] = entity
        ticker = _normalize_whitespace(str(metadata.get("ticker", ""))).upper()
        if ticker and ticker not in by_ticker:
            by_ticker[ticker] = entity
        candidate_names = [str(entity.get("canonical_text", ""))]
        candidate_names.extend(
            str(alias)
            for alias in entity.get("aliases", [])
            if isinstance(alias, str) and _normalize_whitespace(alias)
        )
        for candidate_name in candidate_names:
            for key in (
                _normalize_company_name_for_match(candidate_name),
                _normalize_company_name_for_match(candidate_name, drop_suffixes=True),
            ):
                if key and key not in by_name:
                    by_name[key] = entity
    return {
        "company_number": by_company_number,
        "ticker": by_ticker,
        "name": by_name,
    }


def _find_united_kingdom_issuer_entity(
    *,
    issuer_lookup: dict[str, dict[str, dict[str, object]]],
    company_number: str = "",
    ticker: str = "",
    issuer_name: str = "",
) -> dict[str, object] | None:
    normalized_company_number = _normalize_whitespace(company_number).upper()
    if normalized_company_number:
        entity = issuer_lookup["company_number"].get(normalized_company_number)
        if entity is not None:
            return entity
    normalized_ticker = _normalize_whitespace(ticker).upper()
    if normalized_ticker:
        entity = issuer_lookup["ticker"].get(normalized_ticker)
        if entity is not None:
            return entity
    for key in (
        _normalize_company_name_for_match(issuer_name),
        _normalize_company_name_for_match(issuer_name, drop_suffixes=True),
    ):
        if not key:
            continue
        entity = issuer_lookup["name"].get(key)
        if entity is not None:
            return entity
    return None


def _enrich_united_kingdom_ticker_entities_with_issuer_metadata(
    *,
    ticker_entities: list[dict[str, object]],
    issuer_entities: list[dict[str, object]],
) -> None:
    issuer_lookup = _build_united_kingdom_issuer_lookup(issuer_entities)
    for entity in ticker_entities:
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        issuer_entity = _find_united_kingdom_issuer_entity(
            issuer_lookup=issuer_lookup,
            company_number=str(metadata.get("company_number", "")),
            ticker=str(metadata.get("ticker", "")) or str(entity.get("canonical_text", "")),
            issuer_name=str(metadata.get("issuer_name", "")),
        )
        if issuer_entity is None:
            continue
        issuer_metadata = issuer_entity.get("metadata")
        if not isinstance(issuer_metadata, dict):
            continue
        for source_key, target_key in (
            ("isin", "isin"),
            ("listing_category", "listing_category"),
            ("market_status", "market_status"),
            ("trading_venue", "trading_venue"),
        ):
            source_value = _normalize_whitespace(str(issuer_metadata.get(source_key, "")))
            if source_value and not _normalize_whitespace(str(metadata.get(target_key, ""))):
                metadata[target_key] = source_value


def _enrich_united_kingdom_people_with_issuer_metadata(
    *,
    people_entities: list[dict[str, object]],
    issuer_entities: list[dict[str, object]],
) -> None:
    issuer_lookup = _build_united_kingdom_issuer_lookup(issuer_entities)
    for entity in people_entities:
        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            continue
        issuer_entity = _find_united_kingdom_issuer_entity(
            issuer_lookup=issuer_lookup,
            company_number=str(metadata.get("employer_company_number", "")),
            ticker=str(metadata.get("employer_ticker", "")),
            issuer_name=str(metadata.get("employer_name", "")),
        )
        if issuer_entity is None:
            continue
        issuer_metadata = issuer_entity.get("metadata")
        if not isinstance(issuer_metadata, dict):
            continue
        employer_ticker = _normalize_whitespace(str(issuer_metadata.get("ticker", ""))).upper()
        if employer_ticker and not _normalize_whitespace(str(metadata.get("employer_ticker", ""))):
            metadata["employer_ticker"] = employer_ticker
        employer_isin = _normalize_whitespace(str(issuer_metadata.get("isin", ""))).upper()
        if employer_isin and not _normalize_whitespace(str(metadata.get("employer_isin", ""))):
            metadata["employer_isin"] = employer_isin


def _find_companies_house_company_number(
    *,
    company_name: str,
    raw_company_name: str,
    search_dir: Path,
    user_agent: str,
) -> str | None:
    query_candidates = _unique_aliases(
        [
            _normalize_whitespace(company_name),
            _normalize_whitespace(raw_company_name),
        ]
    )
    for index, query_text in enumerate(query_candidates):
        search_url = _COMPANIES_HOUSE_SEARCH_URL_TEMPLATE.format(query=quote(query_text))
        if index == 0:
            search_path = search_dir / f"{_slug(company_name)}.html"
        else:
            search_path = search_dir / f"{_slug(company_name)}-query-{index + 1}.html"
        if not search_path.exists():
            try:
                _download_source(search_url, search_path, user_agent=user_agent)
            except Exception:
                continue
        company_number = _extract_companies_house_search_result_company_number(
            search_path.read_text(encoding="utf-8", errors="ignore"),
            company_name=company_name,
            raw_company_name=raw_company_name,
        )
        if company_number:
            return company_number
    return None


def _extract_companies_house_search_result_company_number(
    search_html: str,
    *,
    company_name: str,
    raw_company_name: str | None = None,
) -> str | None:
    target_full_values = {
        key
        for key in (
            _normalize_company_name_for_match(company_name),
            _normalize_company_name_for_match(raw_company_name or ""),
        )
        if key
    }
    target_core_values = {
        key
        for key in (
            _normalize_company_name_for_match(company_name, drop_suffixes=True),
            _normalize_company_name_for_match(
                raw_company_name or "",
                drop_suffixes=True,
            ),
        )
        if key
    }
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
                if result_full in target_full_values or result_core in target_core_values:
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
        if result_full in target_full_values or result_core in target_core_values:
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


def _build_france_company_aliases(*values: str) -> list[str]:
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
        stripped = _strip_france_company_legal_suffix(candidate)
        if stripped and stripped.casefold() != key:
            variants.append(stripped)
        for variant in variants:
            normalized_variant = _normalize_whitespace(variant)
            if normalized_variant and normalized_variant.casefold() not in seen:
                pending.append(normalized_variant)
    return aliases


def _build_united_kingdom_company_aliases(*values: str) -> list[str]:
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
        if "," in candidate:
            variants.append(_normalize_whitespace(candidate.replace(",", "")))
        stripped = _strip_united_kingdom_company_legal_suffix(candidate)
        if stripped and stripped.casefold() != key:
            variants.append(stripped)
        for variant in variants:
            normalized_variant = _normalize_whitespace(variant)
            if normalized_variant and normalized_variant.casefold() not in seen:
                pending.append(normalized_variant)
    return aliases


def _build_united_states_company_aliases(*values: str) -> list[str]:
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
        if "." in candidate:
            variants.append(_normalize_whitespace(candidate.replace(".", "")))
        if "," in candidate:
            variants.append(_normalize_whitespace(candidate.replace(",", "")))
        stripped = _strip_united_states_company_legal_suffix(candidate)
        if stripped and stripped.casefold() != key:
            variants.append(stripped)
        for variant in variants:
            normalized_variant = _normalize_whitespace(variant)
            if normalized_variant and normalized_variant.casefold() not in seen:
                pending.append(normalized_variant)
    return aliases


def _build_japan_company_aliases(*values: str) -> list[str]:
    aliases: list[str] = []
    pending: list[str] = []
    for value in values:
        raw_candidate = _normalize_whitespace(value)
        normalized_candidate = _normalize_japan_company_display_name(value)
        for candidate in (normalized_candidate, raw_candidate):
            if candidate and candidate not in pending:
                pending.append(candidate)
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
        if "." in candidate:
            variants.append(_normalize_whitespace(candidate.replace(".", "")))
        if "," in candidate:
            variants.append(_normalize_whitespace(candidate.replace(",", "")))
        stripped = _strip_japan_company_legal_suffix(candidate)
        if stripped and stripped.casefold() != key:
            variants.append(stripped)
        for variant in variants:
            normalized_variant = _normalize_whitespace(variant)
            if normalized_variant and normalized_variant.casefold() not in seen:
                pending.append(normalized_variant)
    return aliases


def _normalize_japan_company_display_name(value: str) -> str:
    normalized = _smart_titlecase_company_name(_normalize_whitespace(value))
    normalized = re.sub(
        r"\bCo\.?\s*,?\s*Ltd\.?\b",
        "Co., Ltd.",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = re.sub(r"\bInc\.?\b", "Inc.", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bLtd\.?\b", "Ltd.", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\.{2,}", ".", normalized)
    return _normalize_whitespace(normalized)


def _strip_france_company_legal_suffix(value: str) -> str | None:
    candidate = _normalize_whitespace(value).rstrip(",")
    previous = ""
    while candidate and candidate.casefold() != previous.casefold():
        previous = candidate
        for pattern in _FRANCE_COMPANY_LEGAL_SUFFIX_PATTERNS:
            updated = _normalize_whitespace(pattern.sub("", candidate)).rstrip(",")
            if updated != candidate:
                candidate = updated
                break
        else:
            break
    return candidate or None


def _strip_united_kingdom_company_legal_suffix(value: str) -> str | None:
    candidate = _normalize_whitespace(value).rstrip(",")
    previous = ""
    while candidate and candidate.casefold() != previous.casefold():
        previous = candidate
        updated = re.sub(
            r"(?:,\s*)?\b(?:limited|ltd|plc|llp|llc)\.?$",
            "",
            candidate,
            flags=re.IGNORECASE,
        )
        updated = _normalize_whitespace(updated).rstrip(",")
        if updated == candidate:
            break
        candidate = updated
    return candidate or None


def _strip_united_states_company_legal_suffix(value: str) -> str | None:
    candidate = _normalize_whitespace(value).rstrip(",")
    previous = ""
    while candidate and candidate.casefold() != previous.casefold():
        previous = candidate
        updated = re.sub(
            r"(?:,\s*)?\b(?:incorporated|inc|corp(?:oration)?|company|co|holdings?|group|limited|ltd|plc)\.?$",
            "",
            candidate,
            flags=re.IGNORECASE,
        )
        updated = _normalize_whitespace(updated).rstrip(",")
        if updated == candidate:
            break
        candidate = updated
    return candidate or None


def _strip_japan_company_legal_suffix(value: str) -> str | None:
    candidate = _normalize_japan_company_display_name(value).rstrip(",")
    previous = ""
    while candidate and candidate.casefold() != previous.casefold():
        previous = candidate
        for pattern in _JAPAN_COMPANY_LEGAL_SUFFIX_PATTERNS:
            updated = _normalize_whitespace(pattern.sub("", candidate)).rstrip(",")
            if updated != candidate:
                candidate = updated
                break
        else:
            break
    return candidate or None


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


def _smart_titlecase_person_name(value: str) -> str:
    candidate = _normalize_whitespace(value)
    if not candidate:
        return ""
    parts: list[str] = []
    for part in candidate.split():
        segments = re.split(r"([\-'])", part)
        normalized_segments: list[str] = []
        for segment in segments:
            if segment in {"-", "'"}:
                normalized_segments.append(segment)
                continue
            if not segment:
                continue
            if segment.isupper() or segment.islower():
                normalized_segments.append(segment[:1].upper() + segment[1:].lower())
                continue
            normalized_segments.append(segment)
        parts.append("".join(normalized_segments))
    return " ".join(parts)


_COUNTRY_ENTITY_DERIVERS: dict[str, Any] = {
    "ar": _derive_argentina_byma_entities,
    "au": _derive_australia_asx_entities,
    "de": _derive_germany_deutsche_boerse_entities,
    "fr": _derive_france_euronext_entities,
    "jp": _derive_japan_jpx_entities,
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
