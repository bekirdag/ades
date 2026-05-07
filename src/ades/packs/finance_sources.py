"""Finance-source snapshot fetchers for real-content pack generation."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from hashlib import sha256
import html
import json
from pathlib import Path
import re
import shutil
import time
from typing import Any
import urllib.error
import urllib.request
from urllib.parse import urlparse
import zipfile

from .fetch import normalize_source_url


DEFAULT_FINANCE_SOURCE_OUTPUT_ROOT = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/raw/finance-en"
)
DEFAULT_SEC_COMPANIES_URL = "https://www.sec.gov/files/company_tickers.json"
DEFAULT_SEC_SUBMISSIONS_URL = (
    "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
)
DEFAULT_SEC_COMPANYFACTS_URL = (
    "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
)
DEFAULT_SYMBOL_DIRECTORY_URL = (
    "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
)
DEFAULT_OTHER_LISTED_URL = (
    "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
)
DEFAULT_FINANCE_PEOPLE_OPERATOR_URL = "operator://ades/finance-people-entities"
DEFAULT_DERIVED_FINANCE_PEOPLE_OPERATOR_URL = "operator://ades/sec-proxy-people-entities"
DEFAULT_SEC_ARCHIVES_BASE_URL = "https://www.sec.gov/Archives/edgar/data"
DEFAULT_SOURCE_FETCH_USER_AGENT = "ades/0.1.0 (ops@adestool.com)"
_SEC_RETRYABLE_DOWNLOAD_STATUS_CODES = {429, 503}
_SEC_DOWNLOAD_MAX_ATTEMPTS = 5
_SEC_DOWNLOAD_RETRY_BASE_SECONDS = 1.0
_DEFAULT_FINANCE_PEOPLE_FORMS = ("DEF 14A", "DEFA14A")
_DEFAULT_FINANCE_PEOPLE_FILINGS_PER_COMPANY = 2
_SEC_PROXY_DOWNLOAD_WORKERS = 4
_PROXY_ROLE_MAX_WORDS = 16
_PROXY_ROLE_MAX_CHARS = 120
_FINANCE_PERSON_TITLE_BY_CLASS: tuple[tuple[str, str], ...] = (
    ("president and chief executive officer", "executive_officer"),
    ("chief executive officer", "executive_officer"),
    ("chief financial officer", "executive_officer"),
    ("chief operating officer", "executive_officer"),
    ("chief accounting officer", "executive_officer"),
    ("principal accounting officer", "executive_officer"),
    ("principal executive officer", "executive_officer"),
    ("principal financial officer", "executive_officer"),
    ("chief technology officer", "executive_officer"),
    ("chief people officer", "executive_officer"),
    ("chief investment officer", "investment_professional"),
    ("chief risk officer", "investment_professional"),
    ("chief compliance officer", "investment_professional"),
    ("chief legal officer", "executive_officer"),
    ("general counsel", "executive_officer"),
    ("executive chairman", "director"),
    ("executive chair", "director"),
    ("independent chair", "director"),
    ("executive vice president", "executive_officer"),
    ("senior vice president", "executive_officer"),
    ("vice president", "executive_officer"),
    ("managing director", "investment_professional"),
    ("portfolio manager", "investment_professional"),
    ("independent director", "director"),
    ("lead director", "director"),
    ("board chair", "director"),
    ("chair of the board", "director"),
    ("chairman", "director"),
    ("chairwoman", "director"),
    ("chairperson", "director"),
    ("chair", "director"),
    ("director", "director"),
    ("president", "executive_officer"),
    ("treasurer", "executive_officer"),
    ("secretary", "executive_officer"),
)
_FINANCE_PERSON_TITLE_PATTERN = "|".join(
    re.escape(title)
    for title, _role_class in sorted(
        _FINANCE_PERSON_TITLE_BY_CLASS,
        key=lambda item: len(item[0]),
        reverse=True,
    )
)
_FINANCE_PERSON_NAME_TOKEN_PATTERN = (
    r"(?:[A-Z][A-Za-z'`.-]+|[A-Z]{2,}|[A-Z]\.)"
)
_FINANCE_PERSON_NAME_PATTERN = (
    rf"{_FINANCE_PERSON_NAME_TOKEN_PATTERN}"
    rf"(?:\s+{_FINANCE_PERSON_NAME_TOKEN_PATTERN}){{1,4}}"
)
_FINANCE_PERSON_LINE_PATTERNS = (
    re.compile(
        rf"^\s*(?P<name>{_FINANCE_PERSON_NAME_PATTERN})\s*(?:,|\||-)\s*"
        rf"(?P<title>{_FINANCE_PERSON_TITLE_PATTERN})\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        rf"^\s*(?P<name>{_FINANCE_PERSON_NAME_PATTERN})\s{{2,}}"
        rf"(?P<title>{_FINANCE_PERSON_TITLE_PATTERN})\s*$",
        re.IGNORECASE,
    ),
)
_FINANCE_PERSON_INLINE_LINE_PATTERN = re.compile(
    rf"^\s*(?P<name>{_FINANCE_PERSON_NAME_PATTERN})\s*(?:,|\||-)\s*"
    rf"(?P<title>.+?)\s*$",
    re.IGNORECASE,
)
_FINANCE_PERSON_SECTION_HEADING_PATTERN = re.compile(
    r"(?:members of the .* committee|committee members)",
    re.IGNORECASE,
)
_FINANCE_PERSON_CORPORATE_TOKENS = {
    "co",
    "co.",
    "corp",
    "corp.",
    "corporation",
    "holdings",
    "inc",
    "inc.",
    "llp",
    "ltd",
    "ltd.",
    "llc",
    "lp",
    "partners",
    "plc",
}
_FINANCE_PERSON_STOP_TOKENS = {
    "at",
    "and",
    "annual",
    "against",
    "awards",
    "board",
    "benefits",
    "business",
    "career",
    "cash",
    "certain",
    "chain",
    "check",
    "common",
    "committee",
    "company",
    "compensation",
    "consequences",
    "continued",
    "corporate",
    "dear",
    "determination",
    "equity",
    "exhibit",
    "fees",
    "fellow",
    "global",
    "governance",
    "goals",
    "guidelines",
    "hedging",
    "highlights",
    "income",
    "information",
    "internal",
    "leadership",
    "management",
    "meeting",
    "no",
    "non",
    "objectives",
    "officer",
    "other",
    "overboarding",
    "our",
    "owners",
    "page",
    "parity",
    "pay",
    "performance",
    "plan",
    "plans",
    "policy",
    "primary",
    "protections",
    "proposal",
    "proposals",
    "proxy",
    "psu",
    "reference",
    "retainers",
    "required",
    "reserve",
    "relationships",
    "relationship",
    "responsibilities",
    "shares",
    "shareholder",
    "shareholders",
    "statement",
    "stock",
    "structure",
    "summary",
    "supply",
    "table",
    "tax",
    "target",
    "value",
    "vote",
    "future",
}
_FINANCE_PERSON_NAME_PARTICLES = {
    "al",
    "bin",
    "da",
    "das",
    "de",
    "del",
    "der",
    "di",
    "dos",
    "du",
    "la",
    "le",
    "st",
    "st.",
    "van",
    "von",
}
_FINANCE_PERSON_TITLE_WORDS = {
    token
    for title, _role_class in _FINANCE_PERSON_TITLE_BY_CLASS
    for token in title.split()
}
_FINANCE_PERSON_ROLE_WORDS = {
    token.casefold()
    for title, _role_class in _FINANCE_PERSON_TITLE_BY_CLASS
    for token in title.split()
}
_FINANCE_PERSON_ROLE_WORDS.update(
    {
        "&",
        "ceo",
        "cfo",
        "coo",
        "cpo",
        "cto",
        "evp",
        "former",
        "interim",
        "svp",
        "vp",
    }
)


def _curated_finance_entity(
    entity_type: str,
    canonical_text: str,
    *,
    aliases: tuple[str, ...] = (),
    entity_id: str | None = None,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    record: dict[str, object] = {
        "entity_type": entity_type,
        "canonical_text": canonical_text,
        "aliases": list(aliases),
    }
    if entity_id:
        record["entity_id"] = entity_id
    if metadata:
        record["metadata"] = metadata
    return record


DEFAULT_CURATED_FINANCE_ENTITIES: tuple[dict[str, object], ...] = (
    _curated_finance_entity(
        "exchange",
        "NASDAQ",
        aliases=("Nasdaq", "Nasdaq Stock Market"),
        metadata={"region": "us", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "NYSE",
        aliases=("New York Stock Exchange",),
        metadata={"region": "us", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "NYSE American",
        aliases=("AMEX", "American Stock Exchange"),
        metadata={"region": "us", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "CBOE",
        aliases=("Chicago Board Options Exchange",),
        metadata={"region": "us", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "CME",
        aliases=("Chicago Mercantile Exchange",),
        metadata={"region": "us", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "CBOT",
        aliases=("Chicago Board of Trade",),
        metadata={"region": "us", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "COMEX",
        aliases=("Commodity Exchange",),
        metadata={"region": "us", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "NYMEX",
        aliases=("New York Mercantile Exchange",),
        metadata={"region": "us", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "London Stock Exchange",
        aliases=("LSE",),
        metadata={"region": "uk", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "Euronext",
        aliases=("Euronext Paris", "Euronext Amsterdam"),
        metadata={"region": "eu", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "Deutsche Borse",
        aliases=("Xetra", "Frankfurt Stock Exchange"),
        metadata={"region": "de", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "SIX Swiss Exchange",
        aliases=("SIX",),
        metadata={"region": "ch", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "Japan Exchange Group",
        aliases=("JPX",),
        metadata={"region": "jp", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "Tokyo Stock Exchange",
        aliases=(),
        metadata={"region": "jp", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "Hong Kong Stock Exchange",
        aliases=("HKEX", "Hong Kong Exchanges and Clearing"),
        metadata={"region": "hk", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "Toronto Stock Exchange",
        aliases=("TSX",),
        metadata={"region": "ca", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "Shanghai Stock Exchange",
        aliases=(),
        metadata={"region": "cn", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "Shenzhen Stock Exchange",
        aliases=("SZSE",),
        metadata={"region": "cn", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "National Stock Exchange of India",
        aliases=(),
        metadata={"region": "in", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "Singapore Exchange",
        aliases=("SGX",),
        metadata={"region": "sg", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "Korea Exchange",
        aliases=("KRX",),
        metadata={"region": "kr", "category": "exchange"},
    ),
    _curated_finance_entity(
        "exchange",
        "Saudi Exchange",
        aliases=("Tadawul",),
        metadata={"region": "sa", "category": "exchange"},
    ),
    _curated_finance_entity(
        "market_index",
        "S&P 500",
        aliases=("S&P 500 Index",),
        metadata={"region": "us", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "Dow Jones Industrial Average",
        aliases=(),
        metadata={"region": "us", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "Nasdaq Composite",
        aliases=(),
        metadata={"region": "us", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "Nasdaq-100",
        aliases=(),
        metadata={"region": "us", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "Russell 2000",
        aliases=(),
        metadata={"region": "us", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "Cboe Volatility Index",
        aliases=("VIX",),
        metadata={"region": "us", "category": "volatility_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "FTSE 100",
        aliases=(),
        metadata={"region": "uk", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "DAX",
        aliases=(),
        metadata={"region": "de", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "CAC 40",
        aliases=(),
        metadata={"region": "fr", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "Nikkei 225",
        aliases=(),
        metadata={"region": "jp", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "Hang Seng Index",
        aliases=(),
        metadata={"region": "hk", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "Euro Stoxx 50",
        aliases=(),
        metadata={"region": "eu", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "MSCI World Index",
        aliases=("MSCI World",),
        metadata={"region": "global", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "MSCI Emerging Markets Index",
        aliases=("MSCI Emerging Markets",),
        metadata={"region": "global", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "S&P/TSX Composite Index",
        aliases=(),
        metadata={"region": "ca", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "S&P/ASX 200",
        aliases=(),
        metadata={"region": "au", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "Nifty 50",
        aliases=(),
        metadata={"region": "in", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "BSE Sensex",
        aliases=("Sensex",),
        metadata={"region": "in", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "market_index",
        "KOSPI",
        aliases=(),
        metadata={"region": "kr", "category": "equity_index"},
    ),
    _curated_finance_entity(
        "commodity",
        "Brent crude",
        aliases=("Brent crude oil",),
        metadata={"category": "energy_benchmark"},
    ),
    _curated_finance_entity(
        "commodity",
        "West Texas Intermediate",
        aliases=("WTI crude",),
        metadata={"category": "energy_benchmark"},
    ),
    _curated_finance_entity(
        "commodity",
        "Natural gas",
        aliases=(
            "Henry Hub natural gas",
            "natural gas futures",
            "gas futures",
            "liquefied natural gas",
        ),
        entity_id="ades:impact:commodity:natural-gas",
        metadata={"category": "energy_benchmark"},
    ),
    _curated_finance_entity(
        "policy",
        "Gas reservation policy",
        aliases=(
            "domestic gas reservation",
            "gas reservation requirement",
            "gas supply reservation",
            "natural gas reservation policy",
        ),
        entity_id="ades:impact:energy-policy:gas-reservation-policy",
        metadata={"category": "energy_regulation"},
    ),
    _curated_finance_entity(
        "sector",
        "Gas producers",
        aliases=(
            "gas producer",
            "natural gas producer",
            "natural gas producers",
        ),
        entity_id="ades:impact:energy-sector:gas-producers",
        metadata={"category": "energy_supply"},
    ),
    _curated_finance_entity(
        "commodity",
        "COMEX gold",
        aliases=(),
        metadata={"category": "metals_benchmark"},
    ),
    _curated_finance_entity(
        "commodity",
        "COMEX silver",
        aliases=(),
        metadata={"category": "metals_benchmark"},
    ),
    _curated_finance_entity(
        "commodity",
        "LME copper",
        aliases=(),
        metadata={"category": "metals_benchmark"},
    ),
    _curated_finance_entity(
        "commodity",
        "iron ore",
        aliases=(),
        metadata={"category": "bulk_commodity"},
    ),
)


@dataclass(frozen=True)
class FinanceSourceFetchResult:
    """Summary of one downloaded finance source snapshot set."""

    pack_id: str
    output_dir: str
    snapshot: str
    snapshot_dir: str
    sec_companies_url: str
    sec_submissions_url: str
    sec_companyfacts_url: str
    symbol_directory_url: str
    other_listed_url: str
    finance_people_url: str
    source_manifest_path: str
    sec_companies_path: str
    sec_submissions_path: str
    sec_companyfacts_path: str
    symbol_directory_path: str
    other_listed_path: str
    finance_people_path: str
    curated_entities_path: str
    generated_at: str
    source_count: int
    curated_entity_count: int
    sec_companies_sha256: str
    sec_submissions_sha256: str
    sec_companyfacts_sha256: str
    symbol_directory_sha256: str
    other_listed_sha256: str
    finance_people_sha256: str
    curated_entities_sha256: str
    warnings: list[str] = field(default_factory=list)


def fetch_finance_source_snapshot(
    *,
    output_dir: str | Path = DEFAULT_FINANCE_SOURCE_OUTPUT_ROOT,
    snapshot: str | None = None,
    sec_companies_url: str = DEFAULT_SEC_COMPANIES_URL,
    sec_submissions_url: str = DEFAULT_SEC_SUBMISSIONS_URL,
    sec_companyfacts_url: str = DEFAULT_SEC_COMPANYFACTS_URL,
    symbol_directory_url: str = DEFAULT_SYMBOL_DIRECTORY_URL,
    other_listed_url: str = DEFAULT_OTHER_LISTED_URL,
    finance_people_url: str | None = None,
    derive_finance_people_from_sec: bool = False,
    finance_people_archive_base_url: str = DEFAULT_SEC_ARCHIVES_BASE_URL,
    finance_people_max_companies: int | None = None,
    user_agent: str = DEFAULT_SOURCE_FETCH_USER_AGENT,
) -> FinanceSourceFetchResult:
    """Download one immutable finance source snapshot set into the big-data root."""

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_snapshot = _resolve_snapshot(snapshot)
    snapshot_dir = resolved_output_dir / resolved_snapshot
    if snapshot_dir.exists():
        raise FileExistsError(
            f"Finance source snapshot directory already exists: {snapshot_dir}"
        )
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    generated_at = _utc_timestamp()
    warnings: list[str] = []
    if finance_people_max_companies is not None and finance_people_max_companies <= 0:
        raise ValueError("finance_people_max_companies must be positive when provided.")
    try:
        sec_url = _download_source(
            sec_companies_url,
            snapshot_dir / "sec_company_tickers.json",
            user_agent=user_agent,
        )
        sec_submissions_resolved_url = _download_source(
            sec_submissions_url,
            snapshot_dir / "sec_submissions.zip",
            user_agent=user_agent,
        )
        sec_companyfacts_resolved_url = _download_source(
            sec_companyfacts_url,
            snapshot_dir / "sec_companyfacts.zip",
            user_agent=user_agent,
        )
        symbol_url = _download_source(
            symbol_directory_url,
            snapshot_dir / "nasdaqtraded.txt",
            user_agent=user_agent,
        )
        other_listed_resolved_url = _download_source(
            other_listed_url,
            snapshot_dir / "otherlisted.txt",
            user_agent=user_agent,
        )
        finance_people_resolved_url = DEFAULT_FINANCE_PEOPLE_OPERATOR_URL
        finance_people_path = snapshot_dir / "finance_people_entities.json"
        if finance_people_url is not None:
            finance_people_resolved_url = _download_source(
                finance_people_url,
                finance_people_path,
                user_agent=user_agent,
            )
        elif derive_finance_people_from_sec:
            extracted_people_count = _write_sec_proxy_people_entities(
                path=finance_people_path,
                sec_companies_path=snapshot_dir / "sec_company_tickers.json",
                sec_submissions_path=snapshot_dir / "sec_submissions.zip",
                archive_base_url=finance_people_archive_base_url,
                user_agent=user_agent,
                max_companies=finance_people_max_companies,
            )
            finance_people_resolved_url = DEFAULT_DERIVED_FINANCE_PEOPLE_OPERATOR_URL
            if finance_people_max_companies is not None:
                warnings.append(
                    "Finance people derivation was bounded to "
                    f"{finance_people_max_companies} companies."
                )
            if extracted_people_count == 0:
                warnings.append(
                    "SEC proxy derivation produced no finance people entities."
                )
        else:
            _write_default_finance_people_entities(finance_people_path)
        curated_entities_path = snapshot_dir / "curated_finance_entities.json"
        _write_curated_entities(curated_entities_path)
        _validate_sec_snapshot(snapshot_dir / "sec_company_tickers.json")
        _validate_nonempty_snapshot(snapshot_dir / "sec_submissions.zip")
        _validate_nonempty_snapshot(snapshot_dir / "sec_companyfacts.zip")
        _validate_symbol_directory(snapshot_dir / "nasdaqtraded.txt")
        _validate_symbol_directory(snapshot_dir / "otherlisted.txt")
        _validate_finance_people_snapshot(finance_people_path)
    except Exception:
        shutil.rmtree(snapshot_dir, ignore_errors=True)
        raise

    sec_companies_path = snapshot_dir / "sec_company_tickers.json"
    sec_submissions_path = snapshot_dir / "sec_submissions.zip"
    sec_companyfacts_path = snapshot_dir / "sec_companyfacts.zip"
    symbol_directory_path = snapshot_dir / "nasdaqtraded.txt"
    other_listed_path = snapshot_dir / "otherlisted.txt"
    finance_people_path = snapshot_dir / "finance_people_entities.json"
    curated_entities_path = snapshot_dir / "curated_finance_entities.json"
    manifest_path = snapshot_dir / "sources.fetch.json"
    _write_source_manifest(
        path=manifest_path,
        snapshot=resolved_snapshot,
        generated_at=generated_at,
        user_agent=user_agent,
        sec_companies_url=sec_url,
        sec_submissions_url=sec_submissions_resolved_url,
        sec_companyfacts_url=sec_companyfacts_resolved_url,
        symbol_directory_url=symbol_url,
        other_listed_url=other_listed_resolved_url,
        finance_people_url=finance_people_resolved_url,
        sec_companies_path=sec_companies_path,
        sec_submissions_path=sec_submissions_path,
        sec_companyfacts_path=sec_companyfacts_path,
        symbol_directory_path=symbol_directory_path,
        other_listed_path=other_listed_path,
        finance_people_path=finance_people_path,
        curated_entities_path=curated_entities_path,
    )

    return FinanceSourceFetchResult(
        pack_id="finance-en",
        output_dir=str(resolved_output_dir),
        snapshot=resolved_snapshot,
        snapshot_dir=str(snapshot_dir),
        sec_companies_url=sec_url,
        sec_submissions_url=sec_submissions_resolved_url,
        sec_companyfacts_url=sec_companyfacts_resolved_url,
        symbol_directory_url=symbol_url,
        other_listed_url=other_listed_resolved_url,
        finance_people_url=finance_people_resolved_url,
        source_manifest_path=str(manifest_path),
        sec_companies_path=str(sec_companies_path),
        sec_submissions_path=str(sec_submissions_path),
        sec_companyfacts_path=str(sec_companyfacts_path),
        symbol_directory_path=str(symbol_directory_path),
        other_listed_path=str(other_listed_path),
        finance_people_path=str(finance_people_path),
        curated_entities_path=str(curated_entities_path),
        generated_at=generated_at,
        source_count=7,
        curated_entity_count=len(DEFAULT_CURATED_FINANCE_ENTITIES),
        sec_companies_sha256=_sha256_file(sec_companies_path),
        sec_submissions_sha256=_sha256_file(sec_submissions_path),
        sec_companyfacts_sha256=_sha256_file(sec_companyfacts_path),
        symbol_directory_sha256=_sha256_file(symbol_directory_path),
        other_listed_sha256=_sha256_file(other_listed_path),
        finance_people_sha256=_sha256_file(finance_people_path),
        curated_entities_sha256=_sha256_file(curated_entities_path),
        warnings=warnings,
    )


def _resolve_snapshot(snapshot: str | None) -> str:
    if snapshot is None:
        return date.today().isoformat()
    return date.fromisoformat(snapshot).isoformat()


def _download_source(source_url: str | Path, destination: Path, *, user_agent: str) -> str:
    normalized_source_url = normalize_source_url(source_url)
    parsed = urlparse(normalized_source_url)
    request: str | urllib.request.Request
    if parsed.scheme in {"http", "https"}:
        request = urllib.request.Request(
            normalized_source_url,
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json,text/plain;q=0.9,*/*;q=0.1",
            },
        )
    else:
        request = normalized_source_url
    with urllib.request.urlopen(request, timeout=60.0) as response:
        destination.write_bytes(response.read())
    return normalized_source_url


def _write_curated_entities(path: Path) -> None:
    payload = {"entities": list(DEFAULT_CURATED_FINANCE_ENTITIES)}
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_default_finance_people_entities(path: Path) -> None:
    path.write_text(
        json.dumps({"entities": []}, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_sec_proxy_people_entities(
    *,
    path: Path,
    sec_companies_path: Path,
    sec_submissions_path: Path,
    archive_base_url: str,
    user_agent: str,
    max_companies: int | None,
) -> int:
    sec_company_items = _load_sec_company_items(sec_companies_path)
    if max_companies is not None:
        sec_company_items = sec_company_items[:max_companies]
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
    extracted_entities = _extract_sec_proxy_people_entities(
        sec_submissions_path=sec_submissions_path,
        sec_company_profiles=sec_company_profiles,
        archive_base_url=archive_base_url,
        user_agent=user_agent,
    )
    path.write_text(
        json.dumps({"entities": extracted_entities}, indent=2) + "\n",
        encoding="utf-8",
    )
    return len(extracted_entities)


def _write_source_manifest(
    *,
    path: Path,
    snapshot: str,
    generated_at: str,
    user_agent: str,
    sec_companies_url: str,
    sec_submissions_url: str,
    sec_companyfacts_url: str,
    symbol_directory_url: str,
    other_listed_url: str,
    finance_people_url: str,
    sec_companies_path: Path,
    sec_submissions_path: Path,
    sec_companyfacts_path: Path,
    symbol_directory_path: Path,
    other_listed_path: Path,
    finance_people_path: Path,
    curated_entities_path: Path,
) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": "finance-en",
                "snapshot": snapshot,
                "generated_at": generated_at,
                "user_agent": user_agent,
                "sources": [
                    _source_manifest_entry(
                        name="sec-company-tickers",
                        source_url=sec_companies_url,
                        path=sec_companies_path,
                    ),
                    _source_manifest_entry(
                        name="sec-submissions",
                        source_url=sec_submissions_url,
                        path=sec_submissions_path,
                    ),
                    _source_manifest_entry(
                        name="sec-companyfacts",
                        source_url=sec_companyfacts_url,
                        path=sec_companyfacts_path,
                    ),
                    _source_manifest_entry(
                        name="symbol-directory",
                        source_url=symbol_directory_url,
                        path=symbol_directory_path,
                    ),
                    _source_manifest_entry(
                        name="other-listed",
                        source_url=other_listed_url,
                        path=other_listed_path,
                    ),
                    _source_manifest_entry(
                        name="finance-people",
                        source_url=finance_people_url,
                        path=finance_people_path,
                    ),
                    _source_manifest_entry(
                        name="curated-finance-entities",
                        source_url="operator://ades/curated-finance-entities",
                        path=curated_entities_path,
                    ),
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _source_manifest_entry(*, name: str, source_url: str, path: Path) -> dict[str, object]:
    return {
        "name": name,
        "source_url": source_url,
        "path": path.name,
        "sha256": _sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _validate_sec_snapshot(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, (dict, list)):
        raise ValueError(f"Unsupported SEC snapshot structure: {path}")


def _validate_nonempty_snapshot(path: Path) -> None:
    if path.stat().st_size <= 0:
        raise ValueError(f"Snapshot is empty: {path}")


def _validate_symbol_directory(path: Path) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise ValueError(f"Symbol directory snapshot is empty: {path}")
    header = lines[0]
    if "Symbol" not in header:
        raise ValueError(f"Symbol directory snapshot header is invalid: {path}")


def _validate_finance_people_snapshot(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return
    if not isinstance(payload, dict):
        raise ValueError(f"Unsupported finance people snapshot structure: {path}")
    entities = payload.get("entities") or payload.get("people") or []
    if not isinstance(entities, list):
        raise ValueError(f"Finance people snapshot entities must be a list: {path}")


def _load_sec_company_items(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        raw_records = list(payload.values())
    elif isinstance(payload, list):
        raw_records = payload
    else:
        raise ValueError(f"Unsupported SEC snapshot structure: {path}")
    return [item for item in raw_records if isinstance(item, dict)]


def _extract_sec_proxy_people_entities(
    *,
    sec_submissions_path: Path,
    sec_company_profiles: dict[str, dict[str, str]],
    archive_base_url: str,
    user_agent: str,
) -> list[dict[str, object]]:
    entities_by_key: dict[tuple[str, str], dict[str, object]] = {}
    filings_by_source_id: dict[str, list[dict[str, str]]] = {}
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
        proxy_filing_count = 0
        for filing in _iter_recent_sec_filings(submission):
            form = _clean_text(filing.get("form")).upper()
            if form not in _DEFAULT_FINANCE_PEOPLE_FORMS:
                continue
            primary_document = _clean_text(filing.get("primaryDocument"))
            accession_number = _clean_text(filing.get("accessionNumber"))
            filing_date = _clean_text(filing.get("filingDate"))
            if not primary_document or not accession_number:
                continue
            filings_by_source_id.setdefault(source_id, []).append(
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
            proxy_filing_count += 1
            if proxy_filing_count >= _DEFAULT_FINANCE_PEOPLE_FILINGS_PER_COMPANY:
                break
    if not filings_by_source_id:
        return []
    with ThreadPoolExecutor(max_workers=_SEC_PROXY_DOWNLOAD_WORKERS) as executor:
        futures = {
            executor.submit(
                _extract_sec_proxy_people_entities_for_company,
                filings=filings,
                user_agent=user_agent,
            ): source_id
            for source_id, filings in filings_by_source_id.items()
        }
        for future in as_completed(futures):
            try:
                extracted = future.result()
            except Exception:
                continue
            for person_record in extracted:
                key = (
                    str(person_record.get("employer_cik", "")),
                    _clean_text(person_record.get("canonical_text")).casefold(),
                )
                if key not in entities_by_key:
                    entities_by_key[key] = person_record
    return sorted(
        entities_by_key.values(),
        key=lambda item: (
            str(item.get("employer_cik", "")),
            str(item.get("canonical_text", "")).casefold(),
        ),
    )


def _extract_sec_proxy_people_entities_for_company(
    *,
    filings: list[dict[str, str]],
    user_agent: str,
) -> list[dict[str, object]]:
    best_extracted: list[dict[str, object]] = []
    for filing in filings:
        try:
            document_text = _download_text(
                str(filing.get("filing_url", "")),
                user_agent=user_agent,
            )
        except Exception:
            continue
        extracted = _extract_people_from_sec_proxy_document(
            text=document_text,
            employer_cik=str(filing.get("employer_cik", "")),
            employer_name=str(filing.get("employer_name", "")),
            employer_ticker=str(filing.get("employer_ticker", "")),
            filing_date=str(filing.get("filing_date", "")),
            filing_url=str(filing.get("filing_url", "")),
            source_form=str(filing.get("source_form", "")),
        )
        if len(extracted) > len(best_extracted):
            best_extracted = extracted
    return best_extracted


def _iter_recent_sec_filings(submission: dict[str, Any]) -> list[dict[str, str]]:
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


def _build_sec_filing_document_url(
    *,
    archive_base_url: str,
    source_id: str,
    accession_number: str,
    primary_document: str,
) -> str:
    normalized_archive_base_url = normalize_source_url(archive_base_url).rstrip("/")
    accession_no_dashes = accession_number.replace("-", "")
    cik_component = str(int(source_id))
    return (
        f"{normalized_archive_base_url}/{cik_component}/{accession_no_dashes}/"
        f"{primary_document}"
    )


def _download_text(source_url: str, *, user_agent: str) -> str:
    normalized_source_url = normalize_source_url(source_url)
    parsed = urlparse(normalized_source_url)
    payload: bytes
    charset = "utf-8"
    for attempt in range(1, _SEC_DOWNLOAD_MAX_ATTEMPTS + 1):
        request: str | urllib.request.Request
        if parsed.scheme in {"http", "https"}:
            request = urllib.request.Request(
                normalized_source_url,
                headers={
                    "User-Agent": user_agent,
                    "Accept": "text/html,text/plain;q=0.9,*/*;q=0.1",
                },
            )
        else:
            request = normalized_source_url
        try:
            with urllib.request.urlopen(request, timeout=60.0) as response:
                payload = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
            break
        except urllib.error.HTTPError as exc:
            if not _should_retry_sec_download(
                source_url=normalized_source_url,
                error=exc,
                attempt=attempt,
            ):
                raise
            time.sleep(_sec_download_retry_delay_seconds(exc, attempt=attempt))
    try:
        return payload.decode(charset, errors="replace")
    except LookupError:
        return payload.decode("utf-8", errors="replace")


def _should_retry_sec_download(
    *,
    source_url: str,
    error: urllib.error.HTTPError,
    attempt: int,
) -> bool:
    hostname = (urlparse(source_url).hostname or "").casefold()
    return (
        hostname.endswith("sec.gov")
        and error.code in _SEC_RETRYABLE_DOWNLOAD_STATUS_CODES
        and attempt < _SEC_DOWNLOAD_MAX_ATTEMPTS
    )


def _sec_download_retry_delay_seconds(
    error: urllib.error.HTTPError,
    *,
    attempt: int,
) -> float:
    retry_after = error.headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), _SEC_DOWNLOAD_RETRY_BASE_SECONDS)
        except ValueError:
            pass
    return _SEC_DOWNLOAD_RETRY_BASE_SECONDS * (2 ** (attempt - 1))


def _extract_people_from_sec_proxy_document(
    *,
    text: str,
    employer_cik: str,
    employer_name: str,
    employer_ticker: str,
    filing_date: str,
    filing_url: str,
    source_form: str,
) -> list[dict[str, object]]:
    entities: dict[str, dict[str, object]] = {}
    lines = _normalize_sec_proxy_document_lines(text)
    extracted_records: list[tuple[str, str]] = [
        *_extract_inline_sec_proxy_people_records(lines),
        *_extract_adjacent_sec_proxy_people_records(lines),
        *_extract_committee_member_sec_proxy_people_records(lines),
    ]
    for raw_name, raw_role_title in extracted_records:
        canonical_name = _normalize_person_display_name(raw_name)
        if not _looks_like_finance_person_name(canonical_name, raw_value=raw_name):
            continue
        role_title = _normalize_proxy_role_title(raw_role_title)
        if not role_title:
            continue
        slug = _slugify(canonical_name)
        if not slug:
            continue
        entity_id = f"finance-person:{employer_cik}:{slug}"
        record: dict[str, object] = {
            "entity_id": entity_id,
            "canonical_text": canonical_name,
            "aliases": (
                [raw_name]
                if raw_name and raw_name.casefold() != canonical_name.casefold()
                else []
            ),
            "employer_name": employer_name,
            "employer_ticker": employer_ticker,
            "employer_cik": employer_cik,
            "role_title": role_title,
            "role_class": _classify_finance_person_role(role_title),
            "source_form": source_form,
            "source_name": "sec-proxy-people",
            "source_id": f"{employer_cik}:{slug}:def14a",
            "effective_date": filing_date,
            "source_url": filing_url,
        }
        existing = entities.get(entity_id)
        if existing is None:
            entities[entity_id] = record
            continue
        existing_aliases = [
            _clean_text(alias)
            for alias in existing.get("aliases", [])
            if _clean_text(alias)
        ]
        if raw_name and raw_name.casefold() != canonical_name.casefold():
            existing_aliases.append(raw_name)
        existing["aliases"] = _dedupe_preserving_order(existing_aliases)
        existing_role_title = _clean_text(existing.get("role_title"))
        if len(role_title) > len(existing_role_title):
            existing["role_title"] = role_title
            existing["role_class"] = _classify_finance_person_role(role_title)
    return list(entities.values())


def _extract_inline_sec_proxy_people_records(
    lines: list[str],
) -> list[tuple[str, str]]:
    records: list[tuple[str, str]] = []
    for line in lines:
        for segment in _iter_proxy_line_segments(line):
            for pattern in _FINANCE_PERSON_LINE_PATTERNS:
                match = pattern.fullmatch(segment)
                if match is None:
                    continue
                raw_name = _clean_text(match.group("name"))
                role_title = _normalize_proxy_role_title(match.group("title"))
                if raw_name and role_title:
                    records.append((raw_name, role_title))
                break
            else:
                match = _FINANCE_PERSON_INLINE_LINE_PATTERN.fullmatch(segment)
                if match is None:
                    continue
                raw_name = _clean_text(match.group("name"))
                role_title = _normalize_proxy_role_title(match.group("title"))
                if raw_name and _looks_like_proxy_role_title(role_title):
                    records.append((raw_name, role_title))
    return records


def _extract_adjacent_sec_proxy_people_records(
    lines: list[str],
) -> list[tuple[str, str]]:
    records: list[tuple[str, str]] = []
    for index, line in enumerate(lines):
        raw_name = _clean_text(line)
        canonical_name = _normalize_person_display_name(raw_name)
        if not _looks_like_finance_person_name(canonical_name, raw_value=raw_name):
            continue
        title_candidates: list[str] = []
        for offset in range(1, 3):
            candidate_index = index + offset
            if candidate_index >= len(lines):
                break
            candidate = _normalize_proxy_role_title(lines[candidate_index])
            if not candidate:
                break
            title_candidates.append(candidate)
            joined = " ".join(title_candidates)
            if _looks_like_proxy_role_title(joined):
                records.append((raw_name, joined))
                break
    return records


def _extract_committee_member_sec_proxy_people_records(
    lines: list[str],
) -> list[tuple[str, str]]:
    records: list[tuple[str, str]] = []
    for index, line in enumerate(lines):
        if _FINANCE_PERSON_SECTION_HEADING_PATTERN.search(line) is None:
            continue
        consecutive_non_names = 0
        saw_name = False
        for candidate in lines[index + 1 : index + 26]:
            normalized_candidate = _clean_text(candidate)
            normalized_candidate = re.sub(r"\s*\([^)]*\)\s*$", "", normalized_candidate)
            if not normalized_candidate or normalized_candidate in {"•", "*", "-"}:
                continue
            if _looks_like_finance_person_name(
                _normalize_person_display_name(normalized_candidate),
                raw_value=normalized_candidate,
            ):
                records.append((normalized_candidate, "Director"))
                saw_name = True
                consecutive_non_names = 0
                continue
            consecutive_non_names += 1
            if saw_name and consecutive_non_names >= 3:
                break
    return records


def _iter_proxy_line_segments(line: str) -> list[str]:
    normalized_line = _clean_text(line)
    if not normalized_line:
        return []
    segments = [normalized_line]
    if ";" in normalized_line:
        segments.extend(
            _clean_text(segment)
            for segment in normalized_line.split(";")
            if _clean_text(segment)
        )
    return _dedupe_preserving_order(segments)


def _looks_like_proxy_role_title(value: str) -> bool:
    normalized = _normalize_proxy_role_title(value).casefold()
    if not normalized:
        return False
    words = [
        token.strip(" ,.;:()[]{}\"'").casefold()
        for token in normalized.split()
        if token.strip(" ,.;:()[]{}\"'")
    ]
    if not words:
        return False
    if len(normalized) > _PROXY_ROLE_MAX_CHARS or len(words) > _PROXY_ROLE_MAX_WORDS:
        return False
    if not any(title in normalized for title, _role_class in _FINANCE_PERSON_TITLE_BY_CLASS):
        return False
    allowed_word_count = sum(
        1 for token in words if token in _FINANCE_PERSON_ROLE_WORDS
    )
    return allowed_word_count >= max(1, len(words) // 2)


def _normalize_proxy_role_title(value: str) -> str:
    normalized = _clean_text(value)
    normalized = re.sub(r"\(\d+\)", " ", normalized)
    normalized = re.sub(r"\[[0-9]+\]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip(" ,;:-")
    if not normalized:
        return ""
    return _normalize_person_role_title(normalized)


def _normalize_sec_proxy_document_lines(text: str) -> list[str]:
    normalized = re.sub(r"(?is)<!--.*?-->", " ", text)
    normalized = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", normalized)
    normalized = re.sub(r"(?i)<br\s*/?>", "\n", normalized)
    normalized = re.sub(r"(?i)<tr[^>]*>", "\n", normalized)
    normalized = re.sub(r"(?i)</tr\s*>", "\n", normalized)
    normalized = re.sub(r"(?i)<(?:td|th)[^>]*>", " | ", normalized)
    normalized = re.sub(r"(?i)</(?:td|th)\s*>", " | ", normalized)
    normalized = re.sub(r"(?i)</(?:p|div|li|h[1-6])\s*>", "\n", normalized)
    normalized = re.sub(r"(?s)<[^>]+>", " ", normalized)
    normalized = html.unescape(normalized).replace("\xa0", " ")
    lines: list[str] = []
    for line in normalized.splitlines():
        line = re.sub(r"(?:\s*\|\s*)+", " | ", line)
        cleaned_line = re.sub(r"\s+", " ", line).strip(" |")
        if cleaned_line:
            lines.append(cleaned_line)
    return lines


def _normalize_person_role_title(value: str) -> str:
    normalized = _clean_text(value)
    return " ".join(word.capitalize() for word in normalized.split())


def _normalize_person_display_name(value: str) -> str:
    tokens = _clean_text(value).split()
    normalized_tokens: list[str] = []
    for token in tokens:
        suffix = ""
        if token.endswith((".", ",")):
            suffix = token[-1]
            token = token[:-1]
        if not token:
            continue
        if len(token) == 1:
            normalized_tokens.append(token.upper() + suffix)
            continue
        if token.isupper():
            token = token.capitalize()
        elif "'" in token:
            token = "'".join(part.capitalize() for part in token.split("'"))
        elif "-" in token:
            token = "-".join(part.capitalize() for part in token.split("-"))
        else:
            token = token[0].upper() + token[1:].lower()
        normalized_tokens.append(token + suffix)
    return " ".join(normalized_tokens)


def _looks_like_finance_person_name(value: str, *, raw_value: str | None = None) -> bool:
    normalized = _clean_text(value)
    if not normalized:
        return False
    normalized = re.sub(r"\s*\(\d+\)\s*$", "", normalized)
    normalized = re.sub(
        r"^(?:Dr|Mr|Mrs|Ms|Messrs)\.?\s+",
        "",
        normalized,
        flags=re.IGNORECASE,
    )
    if "|" in normalized:
        return False
    if any(character.isdigit() for character in normalized):
        return False
    tokens = [
        token.strip(" ,.;:()[]{}\"'")
        for token in normalized.split()
        if token.strip(" ,.;:()[]{}\"'")
    ]
    if len(tokens) < 2:
        return False
    alpha_tokens = [
        token for token in tokens if any(character.isalpha() for character in token)
    ]
    if len(alpha_tokens) < 2:
        return False
    lowered_tokens = {token.casefold() for token in alpha_tokens}
    if lowered_tokens & _FINANCE_PERSON_STOP_TOKENS:
        return False
    if lowered_tokens & _FINANCE_PERSON_CORPORATE_TOKENS:
        return False
    if lowered_tokens & _FINANCE_PERSON_TITLE_WORDS:
        return False
    if raw_value is not None:
        cleaned_raw_value = re.sub(r"\s*\(\d+\)\s*$", "", _clean_text(raw_value))
        cleaned_raw_value = re.sub(
            r"^(?:Dr|Mr|Mrs|Ms|Messrs)\.?\s+",
            "",
            cleaned_raw_value,
            flags=re.IGNORECASE,
        )
        if "|" in cleaned_raw_value:
            return False
        if any(character.isdigit() for character in cleaned_raw_value):
            return False
        raw_tokens = [
            token.strip(" ,.;:()[]{}\"'")
            for token in cleaned_raw_value.split()
            if token.strip(" ,.;:()[]{}\"'")
        ]
        saw_presented_name_token = False
        for index, token in enumerate(raw_tokens):
            if not any(character.isalpha() for character in token):
                continue
            lowered_token = token.casefold()
            if len(token) == 1 and token.isalpha():
                saw_presented_name_token = True
                continue
            if token.isupper():
                saw_presented_name_token = True
                continue
            if token[0].isupper():
                saw_presented_name_token = True
                continue
            if index > 0 and lowered_token in _FINANCE_PERSON_NAME_PARTICLES:
                continue
            return False
        if not saw_presented_name_token:
            return False
    return True


def _classify_finance_person_role(role_title: str) -> str:
    normalized_role_title = _clean_text(role_title).casefold()
    for known_title, role_class in _FINANCE_PERSON_TITLE_BY_CLASS:
        if known_title in normalized_role_title:
            return role_class
    return "executive_officer"


def _read_targeted_zip_json_records(
    path: Path,
    *,
    target_source_ids: set[str],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not target_source_ids:
        return records
    with zipfile.ZipFile(path) as archive:
        for source_id in sorted(target_source_ids):
            member_name = f"CIK{source_id}.json"
            try:
                with archive.open(member_name) as handle:
                    payload = json.loads(handle.read().decode("utf-8"))
            except KeyError:
                continue
            if isinstance(payload, dict):
                records.append(payload)
    return records


def _format_sec_source_id(value: Any) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""
    digits = "".join(character for character in cleaned if character.isdigit())
    if not digits:
        return ""
    return digits.zfill(10)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        normalized = _clean_text(value)
        if not normalized:
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def _slugify(value: str) -> str:
    return "".join(
        character.lower() if character.isalnum() else "-"
        for character in _clean_text(value)
    ).strip("-")


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )
