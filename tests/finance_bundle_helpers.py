import json
from pathlib import Path


def create_finance_raw_snapshots(root: Path) -> dict[str, Path]:
    raw_dir = root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    sec_path = raw_dir / "sec-company_tickers.json"
    sec_path.write_text(
        json.dumps(
            {
                "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
                "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corp."},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    symbol_path = raw_dir / "nasdaq_symbols.txt"
    symbol_path.write_text(
        "\n".join(
            [
                "Symbol|Security Name|Test Issue",
                "AAPL|Apple Inc. Common Stock|N",
                "MSFT|Microsoft Corporation Common Stock|N",
                "ZZZZ|Ignore Test Security|Y",
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
                        "canonical_text": "NASDAQ",
                        "aliases": ["Nasdaq"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "NYSE",
                        "aliases": ["New York Stock Exchange"],
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
        "symbol_directory": symbol_path,
        "curated_entities": curated_path,
    }


def create_finance_remote_sources(root: Path) -> dict[str, str]:
    snapshots = create_finance_raw_snapshots(root)
    return {
        "sec_companies_url": snapshots["sec_companies"].resolve().as_uri(),
        "symbol_directory_url": snapshots["symbol_directory"].resolve().as_uri(),
    }
