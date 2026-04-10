import json
from pathlib import Path


def create_finance_generation_bundle(
    root: Path,
    *,
    include_ambiguous_alias: bool = False,
) -> Path:
    bundle_dir = root / "finance-bundle"
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    bundle_manifest = {
        "schema_version": 1,
        "pack_id": "finance-en",
        "version": "0.2.0",
        "language": "en",
        "domain": "finance",
        "tier": "domain",
        "description": "Generated English finance entities and deterministic rules.",
        "tags": ["finance", "generated", "english"],
        "dependencies": [],
        "label_mappings": {
            "issuer": "organization",
            "ticker": "ticker",
            "exchange": "exchange",
        },
        "stoplisted_aliases": ["N/A"],
        "allowed_ambiguous_aliases": [],
        "sources": [
            {
                "name": "sec-companyfacts",
                "snapshot_uri": "s3://ades-bundles/finance/sec/companyfacts-2026-04-10.jsonl",
                "license_class": "build-only",
                "license": "build-only",
                "retrieved_at": "2026-04-10T09:00:00Z",
                "record_count": 4,
            },
            {
                "name": "nasdaq-symbols",
                "snapshot_uri": "s3://ades-bundles/finance/nasdaq/symbols-2026-04-10.csv",
                "license_class": "ship-now",
                "license": "ship-now",
                "retrieved_at": "2026-04-10T09:00:00Z",
                "record_count": 2,
            },
        ],
    }
    (bundle_dir / "bundle.json").write_text(
        json.dumps(bundle_manifest, indent=2) + "\n",
        encoding="utf-8",
    )

    entities = [
        {
            "entity_id": "issuer:apple",
            "entity_type": "issuer",
            "canonical_text": "Apple Inc.",
            "aliases": ["N/A"],
            "source_name": "sec-companyfacts",
        },
        {
            "entity_id": "ticker:aapl",
            "entity_type": "ticker",
            "canonical_text": "AAPL",
            "aliases": [],
            "source_name": "nasdaq-symbols",
        },
        {
            "entity_id": "exchange:nasdaq",
            "entity_type": "exchange",
            "canonical_text": "NASDAQ",
            "aliases": ["Nasdaq"],
            "source_name": "mic-codes",
        },
        {
            "entity_id": "issuer:private",
            "entity_type": "issuer",
            "canonical_text": "Internal Holdings LLC",
            "aliases": ["PRIVATE-42"],
            "build_only": True,
            "source_name": "internal-only",
        },
    ]
    if include_ambiguous_alias:
        entities.append(
            {
                "entity_id": "issuer:nasdaq-inc",
                "entity_type": "issuer",
                "canonical_text": "Nasdaq Inc.",
                "aliases": [],
                "source_name": "sec-companyfacts",
            }
        )

    entities_path = normalized_dir / "entities.jsonl"
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in entities),
        encoding="utf-8",
    )

    rules = [
        {
            "name": "currency_amount",
            "label": "currency_amount",
            "kind": "regex",
            "pattern": r"(USD|EUR|GBP|TRY)\s?[0-9]+(?:\.[0-9]+)?",
        },
        {
            "name": "internal_reference",
            "label": "organization",
            "kind": "regex",
            "pattern": r"PRIVATE-[0-9]+",
            "build_only": True,
        },
    ]
    rules_path = normalized_dir / "rules.jsonl"
    rules_path.write_text(
        "".join(json.dumps(item) + "\n" for item in rules),
        encoding="utf-8",
    )
    return bundle_dir
