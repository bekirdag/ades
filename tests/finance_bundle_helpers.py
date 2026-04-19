import json
from pathlib import Path
import zipfile


def create_finance_raw_snapshots(root: Path) -> dict[str, Path]:
    raw_dir = root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    archives_root = root / "sec-archives"
    archives_root.mkdir(parents=True, exist_ok=True)

    sec_path = raw_dir / "sec-company_tickers.json"
    sec_path.write_text(
        json.dumps(
            {
                "0": {"cik_str": 320193, "ticker": "TICKA", "title": "Issuer Alpha Holdings"},
                "1": {"cik_str": 789019, "ticker": "TICKB", "title": "Issuer Beta Group"},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    sec_submissions_path = raw_dir / "sec_submissions.zip"
    with zipfile.ZipFile(
        sec_submissions_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        alpha_accession = "0000320193-24-000001"
        alpha_document = "proxy2024.htm"
        alpha_accession_dir = alpha_accession.replace("-", "")
        alpha_archive_dir = archives_root / "320193" / alpha_accession_dir
        alpha_archive_dir.mkdir(parents=True, exist_ok=True)
        (alpha_archive_dir / alpha_document).write_text(
            """
            <html>
              <body>
                <h1>Executive Officers</h1>
                <table>
                  <tr><td>Jane Doe</td><td>Chief Executive Officer</td></tr>
                  <tr><td>Mark Smith</td><td>Chief Financial Officer</td></tr>
                </table>
                <h2>Board of Directors</h2>
                <p>Mary Major, Independent Director</p>
              </body>
            </html>
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        archive.writestr(
            "CIK0000320193.json",
            json.dumps(
                {
                    "cik": "0000320193",
                    "name": "Issuer Alpha Holdings",
                    "tickers": ["TICKA"],
                    "exchanges": ["Exchange Alpha"],
                    "formerNames": [{"name": "Issuer Alpha Legacy"}],
                    "filings": {
                        "recent": {
                            "accessionNumber": [alpha_accession],
                            "filingDate": ["2024-03-01"],
                            "form": ["DEF 14A"],
                            "primaryDocument": [alpha_document],
                            "primaryDocDescription": ["Definitive Proxy Statement"],
                        }
                    },
                },
                indent=2,
            )
            + "\n",
        )
        beta_accession = "0000789019-24-000007"
        beta_document = "def14a.htm"
        beta_accession_dir = beta_accession.replace("-", "")
        beta_archive_dir = archives_root / "789019" / beta_accession_dir
        beta_archive_dir.mkdir(parents=True, exist_ok=True)
        (beta_archive_dir / beta_document).write_text(
            """
            <html>
              <body>
                <p>John Roe | Chief Financial Officer</p>
                <p>Lucy Stone | Portfolio Manager</p>
              </body>
            </html>
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        archive.writestr(
            "CIK0000789019.json",
            json.dumps(
                {
                    "cik": "0000789019",
                    "name": "Issuer Beta Group",
                    "tickers": ["TICKB"],
                    "exchanges": ["EXCHX"],
                    "formerNames": [{"name": "Issuer Beta Group"}],
                    "filings": {
                        "recent": {
                            "accessionNumber": [beta_accession],
                            "filingDate": ["2024-04-15"],
                            "form": ["DEF 14A"],
                            "primaryDocument": [beta_document],
                            "primaryDocDescription": ["Definitive Proxy Statement"],
                        }
                    },
                },
                indent=2,
            )
            + "\n",
        )

    sec_companyfacts_path = raw_dir / "sec_companyfacts.zip"
    with zipfile.ZipFile(
        sec_companyfacts_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        archive.writestr(
            "CIK0000320193.json",
            json.dumps(
                {
                    "cik": 320193,
                    "entityName": "Issuer Alpha Holdings",
                },
                indent=2,
            )
            + "\n",
        )
        archive.writestr(
            "CIK0000789019.json",
            json.dumps(
                {
                    "cik": 789019,
                    "entityName": "Issuer Beta Group",
                },
                indent=2,
            )
            + "\n",
        )

    symbol_path = raw_dir / "nasdaq_symbols.txt"
    symbol_path.write_text(
        "\n".join(
            [
                "Symbol|Security Name|Test Issue",
                "TICKA|Issuer Alpha Holdings Common Stock|N",
                "TICKB|Issuer Beta Group Common Stock|N",
                "ZZZZ|Ignore Test Security|Y",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    other_listed_path = raw_dir / "otherlisted.txt"
    other_listed_path.write_text(
        "\n".join(
            [
                "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|EXCHX Symbol",
                "TICKC|Issuer Gamma Group Common Stock|N|TICKC|N|100|N|N",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    finance_people_path = raw_dir / "finance_people_entities.json"
    finance_people_path.write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "canonical_text": "Jane Doe",
                        "aliases": ["Jane A. Doe"],
                        "employer_name": "Issuer Alpha Holdings",
                        "employer_ticker": "TICKA",
                        "employer_cik": "0000320193",
                        "role_title": "Chief Executive Officer",
                        "role_class": "executive_officer",
                        "source_form": "DEF 14A",
                        "source_name": "sec-proxy-people",
                        "source_id": "0000320193:jane-doe:def14a",
                    },
                    {
                        "canonical_text": "John Roe",
                        "aliases": ["John Q. Roe"],
                        "employer_name": "Issuer Beta Group",
                        "employer_ticker": "TICKB",
                        "employer_cik": "0000789019",
                        "role_title": "Chief Financial Officer",
                        "role_class": "executive_officer",
                        "source_form": "DEF 14A",
                        "source_name": "sec-proxy-people",
                        "source_id": "0000789019:john-roe:def14a",
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    curated_path = raw_dir / "curated_finance_entities.json"
    curated_path.write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "entity_type": "exchange",
                        "canonical_text": "EXCHX",
                        "aliases": ["Exchange Alpha"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "EXCHY",
                        "aliases": ["Exchange Beta"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "EXCHZ",
                        "aliases": ["EXCHZ", "Exchange Gamma"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "EXCHQ",
                        "aliases": ["Exchange Delta"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "EXCHR",
                        "aliases": ["Exchange Epsilon"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "London Stock Exchange",
                        "aliases": ["LSE"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "CME",
                        "aliases": ["Chicago Mercantile Exchange"],
                    },
                    {
                        "entity_type": "market_index",
                        "canonical_text": "S&P 500",
                        "aliases": ["S&P 500 Index"],
                        "metadata": {"region": "us"},
                    },
                    {
                        "entity_type": "market_index",
                        "canonical_text": "Cboe Volatility Index",
                        "aliases": ["VIX"],
                    },
                    {
                        "entity_type": "commodity",
                        "canonical_text": "Brent crude",
                        "aliases": ["Brent crude oil"],
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "sec_companies": sec_path,
        "sec_submissions": sec_submissions_path,
        "sec_companyfacts": sec_companyfacts_path,
        "symbol_directory": symbol_path,
        "other_listed": other_listed_path,
        "finance_people": finance_people_path,
        "archives_root": archives_root,
        "curated_entities": curated_path,
    }


def create_finance_remote_sources(root: Path) -> dict[str, str]:
    snapshots = create_finance_raw_snapshots(root)
    return {
        "sec_companies_url": snapshots["sec_companies"].resolve().as_uri(),
        "sec_submissions_url": snapshots["sec_submissions"].resolve().as_uri(),
        "sec_companyfacts_url": snapshots["sec_companyfacts"].resolve().as_uri(),
        "symbol_directory_url": snapshots["symbol_directory"].resolve().as_uri(),
        "other_listed_url": snapshots["other_listed"].resolve().as_uri(),
        "finance_people_url": snapshots["finance_people"].resolve().as_uri(),
        "finance_people_archive_base_url": snapshots["archives_root"].resolve().as_uri(),
    }


def create_finance_country_profiles(root: Path) -> dict[str, dict[str, object]]:
    root.mkdir(parents=True, exist_ok=True)

    us_regulator = root / "us-sec.html"
    us_regulator.write_text("<html><body>US SEC landing page</body></html>\n", encoding="utf-8")
    us_exchange = root / "us-nyse.html"
    us_exchange.write_text("<html><body>NYSE listing hub</body></html>\n", encoding="utf-8")

    uk_regulator = root / "uk-fca.html"
    uk_regulator.write_text("<html><body>UK FCA register landing page</body></html>\n", encoding="utf-8")
    uk_exchange = root / "uk-lse.html"
    uk_exchange.write_text("<html><body>London Stock Exchange news</body></html>\n", encoding="utf-8")

    return {
        "us": {
            "country_code": "us",
            "country_name": "United States",
            "pack_id": "finance-us-en",
            "description": "US finance entities built from official regulator and exchange sources.",
            "sources": [
                {
                    "name": "sec",
                    "source_url": us_regulator.resolve().as_uri(),
                    "category": "securities_regulator",
                },
                {
                    "name": "nyse",
                    "source_url": us_exchange.resolve().as_uri(),
                    "category": "exchange",
                },
            ],
            "entities": [
                {
                    "entity_type": "organization",
                    "canonical_text": "Securities and Exchange Commission",
                    "aliases": ["SEC"],
                    "entity_id": "finance-us:sec",
                    "metadata": {"country_code": "us", "category": "securities_regulator"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "Federal Reserve",
                    "aliases": ["Fed"],
                    "entity_id": "finance-us:fed",
                    "metadata": {"country_code": "us", "category": "central_bank"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "New York Stock Exchange",
                    "aliases": ["NYSE"],
                    "entity_id": "finance-us:nyse",
                    "metadata": {"country_code": "us", "category": "exchange"},
                },
                {
                    "entity_type": "market_index",
                    "canonical_text": "S&P 500",
                    "aliases": ["S&P 500 Index"],
                    "entity_id": "finance-us:sp500",
                    "metadata": {"country_code": "us", "category": "market_index"},
                },
                {
                    "entity_type": "person",
                    "canonical_text": "Jane Doe",
                    "aliases": ["Jane A. Doe"],
                    "entity_id": "finance-us:jane-doe",
                    "metadata": {
                        "country_code": "us",
                        "category": "executive_officer",
                        "role_title": "Chief Executive Officer",
                    },
                },
            ],
        },
        "uk": {
            "country_code": "uk",
            "country_name": "United Kingdom",
            "pack_id": "finance-uk-en",
            "description": "UK finance entities built from official regulator and exchange sources.",
            "sources": [
                {
                    "name": "fca",
                    "source_url": uk_regulator.resolve().as_uri(),
                    "category": "securities_regulator",
                },
                {
                    "name": "lse",
                    "source_url": uk_exchange.resolve().as_uri(),
                    "category": "exchange",
                },
            ],
            "entities": [
                {
                    "entity_type": "organization",
                    "canonical_text": "Financial Conduct Authority",
                    "aliases": ["FCA"],
                    "entity_id": "finance-uk:fca",
                    "metadata": {"country_code": "uk", "category": "securities_regulator"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "Bank of England",
                    "aliases": ["BoE"],
                    "entity_id": "finance-uk:boe",
                    "metadata": {"country_code": "uk", "category": "central_bank"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "London Stock Exchange",
                    "aliases": ["LSE"],
                    "entity_id": "finance-uk:lse",
                    "metadata": {"country_code": "uk", "category": "exchange"},
                },
                {
                    "entity_type": "market_index",
                    "canonical_text": "FTSE 100",
                    "aliases": ["FTSE 100 Index"],
                    "entity_id": "finance-uk:ftse-100",
                    "metadata": {"country_code": "uk", "category": "market_index"},
                },
                {
                    "entity_type": "person",
                    "canonical_text": "John Roe",
                    "aliases": ["John Q. Roe"],
                    "entity_id": "finance-uk:john-roe",
                    "metadata": {
                        "country_code": "uk",
                        "category": "executive_officer",
                        "role_title": "Chief Financial Officer",
                    },
                },
            ],
        },
    }
