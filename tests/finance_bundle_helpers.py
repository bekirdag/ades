import json
from pathlib import Path
import zipfile


def create_finance_raw_snapshots(root: Path) -> dict[str, Path]:
    raw_dir = root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

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
        archive.writestr(
            "CIK0000320193.json",
            json.dumps(
                {
                    "cik": "0000320193",
                    "name": "Issuer Alpha Holdings",
                    "tickers": ["TICKA"],
                    "exchanges": ["Exchange Alpha"],
                    "formerNames": [{"name": "Issuer Alpha Legacy"}],
                },
                indent=2,
            )
            + "\n",
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
                "TICKA|Issuer Alpha Holdings Common Units|N",
                "TICKB|Issuer Beta Group Common Units|N",
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
                "TICKC|Issuer Gamma Group Common Units|N|TICKC|N|100|N|N",
            ]
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
    }
