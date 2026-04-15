import json
from pathlib import Path


def create_general_generation_bundle(root: Path) -> Path:
    bundle_dir = root / "general-bundle"
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    bundle_manifest = {
        "schema_version": 1,
        "pack_id": "general-en",
        "version": "0.2.0",
        "language": "en",
        "domain": "general",
        "tier": "base",
        "description": "Generated English baseline entities and deterministic rules.",
        "tags": ["general", "generated", "english"],
        "dependencies": [],
        "label_mappings": {},
        "stoplisted_aliases": [],
        "allowed_ambiguous_aliases": [],
        "sources": [
            {
                "name": "curated-general",
                "snapshot_uri": "s3://ades-bundles/general/curated-2026-04-11.jsonl",
                "license_class": "ship-now",
                "license": "ship-now",
                "retrieved_at": "2026-04-11T09:00:00Z",
                "record_count": 5,
            }
        ],
    }
    (bundle_dir / "bundle.json").write_text(
        json.dumps(bundle_manifest, indent=2) + "\n",
        encoding="utf-8",
    )

    entities = [
        {
            "entity_id": "person:jordan-elliott-vale",
            "entity_type": "person",
            "canonical_text": "Jordan Elliott Vale",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.7,
        },
        {
            "entity_id": "person:riley-morgan-stone",
            "entity_type": "person",
            "canonical_text": "Riley Morgan Stone",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.65,
        },
        {
            "entity_id": "person:avery-lee-grant",
            "entity_type": "person",
            "canonical_text": "Avery Lee Grant",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.6,
        },
        {
            "entity_id": "organization:north-harbor-management",
            "entity_type": "organization",
            "canonical_text": "North Harbor Management",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.45,
        },
        {
            "entity_id": "location:north-harbor",
            "entity_type": "location",
            "canonical_text": "North Harbor",
            "aliases": [],
            "source_name": "curated-general",
            "population": 800000,
        },
        {
            "entity_id": "organization:beacon-group",
            "entity_type": "organization",
            "canonical_text": "Beacon Group",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.95,
        },
        {
            "entity_id": "organization:signal-harbor-partners",
            "entity_type": "organization",
            "canonical_text": "Signal Harbor Partners",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.72,
        },
        {
            "entity_id": "location:beacon",
            "entity_type": "location",
            "canonical_text": "Beacon",
            "aliases": [],
            "source_name": "curated-general",
            "population": 50,
        },
    ]
    (normalized_dir / "entities.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in entities),
        encoding="utf-8",
    )

    rules = [
        {
            "name": "email_address",
            "label": "email_address",
            "kind": "regex",
            "pattern": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        }
    ]
    (normalized_dir / "rules.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in rules),
        encoding="utf-8",
    )
    return bundle_dir


def create_acronym_general_generation_bundle(root: Path) -> Path:
    bundle_dir = create_general_generation_bundle(root)
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    records = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    records.extend(
        [
            {
                "entity_id": "organization:international-energy-agency",
                "entity_type": "organization",
                "canonical_text": "International Energy Agency",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.94,
            },
            {
                "entity_id": "location:united-states",
                "entity_type": "location",
                "canonical_text": "United States",
                "aliases": [],
                "source_name": "curated-general",
                "population": 331_000_000,
            },
        ]
    )
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    return bundle_dir


def create_pre_resolved_general_generation_bundle(root: Path) -> Path:
    bundle_dir = root / "general-bundle-pre-resolved"
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    bundle_manifest = {
        "schema_version": 1,
        "pack_id": "general-en",
        "version": "0.2.0",
        "language": "en",
        "domain": "general",
        "tier": "base",
        "description": "Generated English baseline entities and deterministic rules.",
        "tags": ["general", "generated", "english"],
        "dependencies": [],
        "label_mappings": {},
        "stoplisted_aliases": [],
        "allowed_ambiguous_aliases": [],
        "alias_resolution": "pre_resolved_explicit",
        "sources": [
            {
                "name": "curated-general",
                "snapshot_uri": "s3://ades-bundles/general/curated-2026-04-12.jsonl",
                "license_class": "ship-now",
                "license": "ship-now",
                "retrieved_at": "2026-04-12T09:00:00Z",
                "record_count": 4,
            }
        ],
    }
    (bundle_dir / "bundle.json").write_text(
        json.dumps(bundle_manifest, indent=2) + "\n",
        encoding="utf-8",
    )

    entities = [
        {
            "entity_id": "person:jordan-elliott-vale",
            "entity_type": "person",
            "canonical_text": "Jordan Elliott Vale",
            "aliases": ["Jordan Vale"],
            "source_name": "curated-general",
            "popularity": 0.7,
        },
        {
            "entity_id": "person:avery-morgan-cole",
            "entity_type": "person",
            "canonical_text": "Avery Morgan Cole",
            "aliases": ["Avery Cole"],
            "source_name": "curated-general",
            "popularity": 0.66,
        },
        {
            "entity_id": "person:riley-sutton-hale",
            "entity_type": "person",
            "canonical_text": "Riley Sutton Hale",
            "aliases": ["Riley Hale"],
            "source_name": "curated-general",
            "popularity": 0.64,
        },
        {
            "entity_id": "organization:beacon-group",
            "entity_type": "organization",
            "canonical_text": "Beacon Group",
            "aliases": ["Beacon"],
            "source_name": "curated-general",
            "popularity": 0.95,
        },
        {
            "entity_id": "location:north-harbor",
            "entity_type": "location",
            "canonical_text": "North Harbor",
            "aliases": [],
            "source_name": "curated-general",
            "population": 800000,
        },
        {
            "entity_id": "organization:signal-harbor-partners",
            "entity_type": "organization",
            "canonical_text": "Signal Harbor Partners",
            "aliases": ["Signal Harbor"],
            "source_name": "curated-general",
            "popularity": 0.72,
        },
        {
            "entity_id": "organization:atlas-collective",
            "entity_type": "organization",
            "canonical_text": "Atlas Collective",
            "aliases": ["Atlas"],
            "source_name": "curated-general",
            "popularity": 0.68,
        },
    ]
    (normalized_dir / "entities.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in entities),
        encoding="utf-8",
    )

    rules = [
        {
            "name": "email_address",
            "label": "email_address",
            "kind": "regex",
            "pattern": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        }
    ]
    (normalized_dir / "rules.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in rules),
        encoding="utf-8",
    )
    return bundle_dir


def create_noisy_general_generation_bundle(root: Path) -> Path:
    bundle_dir = root / "general-bundle-noisy"
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    bundle_manifest = {
        "schema_version": 1,
        "pack_id": "general-en",
        "version": "0.2.0",
        "language": "en",
        "domain": "general",
        "tier": "base",
        "description": "Generated English baseline entities and deterministic rules.",
        "tags": ["general", "generated", "english"],
        "dependencies": [],
        "label_mappings": {},
        "stoplisted_aliases": [],
        "allowed_ambiguous_aliases": [],
        "sources": [
            {
                "name": "curated-general",
                "snapshot_uri": "s3://ades-bundles/general/curated-2026-04-13.jsonl",
                "license_class": "ship-now",
                "license": "ship-now",
                "retrieved_at": "2026-04-13T09:00:00Z",
                "record_count": 20,
            }
        ],
    }
    (bundle_dir / "bundle.json").write_text(
        json.dumps(bundle_manifest, indent=2) + "\n",
        encoding="utf-8",
    )

    entities = [
        {
            "entity_id": "person:alden-voss",
            "entity_type": "person",
            "canonical_text": "Alden Voss",
            "aliases": ["Alden"],
            "source_name": "curated-general",
            "popularity": 0.9,
        },
        {
            "entity_id": "person:ava-hart",
            "entity_type": "person",
            "canonical_text": "Ava Hart",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.72,
        },
        {
            "entity_id": "person:mira-stone",
            "entity_type": "person",
            "canonical_text": "Mira Stone",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.68,
        },
        {
            "entity_id": "person:april-may",
            "entity_type": "person",
            "canonical_text": "April May",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.82,
        },
        {
            "entity_id": "person:yang-an",
            "entity_type": "person",
            "canonical_text": "Yang An",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.76,
        },
        {
            "entity_id": "person:the-founder",
            "entity_type": "person",
            "canonical_text": "The Founder",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.9,
        },
        {
            "entity_id": "person:the-report",
            "entity_type": "person",
            "canonical_text": "The Report",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.88,
        },
        {
            "entity_id": "organization:beacon-group",
            "entity_type": "organization",
            "canonical_text": "Beacon Group",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.95,
        },
        {
            "entity_id": "organization:james-cook-university",
            "entity_type": "organization",
            "canonical_text": "James Cook University",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.91,
        },
        {
            "entity_id": "organization:james",
            "entity_type": "organization",
            "canonical_text": "James",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.89,
        },
        {
            "entity_id": "organization:third-capital",
            "entity_type": "organization",
            "canonical_text": "Third Capital",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.61,
        },
        {
            "entity_id": "organization:we-are",
            "entity_type": "organization",
            "canonical_text": "We Are",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.84,
        },
        {
            "entity_id": "organization:real-estate-band",
            "entity_type": "organization",
            "canonical_text": "Real Estate",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.82,
        },
        {
            "entity_id": "organization:meridia",
            "entity_type": "organization",
            "canonical_text": "Meridia",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.88,
        },
        {
            "entity_id": "organization:europe-collective",
            "entity_type": "organization",
            "canonical_text": "Europe",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.84,
        },
        {
            "entity_id": "organization:harborview-capital",
            "entity_type": "organization",
            "canonical_text": "Harborview Capital",
            "aliases": ["Harborview"],
            "source_name": "curated-general",
            "popularity": 0.9,
        },
        {
            "entity_id": "organization:harborview-records",
            "entity_type": "organization",
            "canonical_text": "Harborview Records",
            "aliases": ["Harborview"],
            "source_name": "curated-general",
            "popularity": 0.86,
        },
        {
            "entity_id": "location:letter",
            "entity_type": "location",
            "canonical_text": "Letter",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.15,
        },
        {
            "entity_id": "location:university",
            "entity_type": "location",
            "canonical_text": "University",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.95,
        },
        {
            "entity_id": "location:this",
            "entity_type": "location",
            "canonical_text": "This",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.95,
        },
        {
            "entity_id": "location:march",
            "entity_type": "location",
            "canonical_text": "March",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.95,
        },
        {
            "entity_id": "location:stock",
            "entity_type": "location",
            "canonical_text": "Stock",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.95,
        },
        {
            "entity_id": "location:europe",
            "entity_type": "location",
            "canonical_text": "Europe",
            "aliases": [],
            "source_name": "curated-general",
            "population": 740_000_000,
        },
        {
            "entity_id": "location:talora",
            "entity_type": "location",
            "canonical_text": "Talora",
            "aliases": [],
            "source_name": "curated-general",
            "population": 9_100_000,
        },
        {
            "entity_id": "location:strait-of-talora",
            "entity_type": "location",
            "canonical_text": "Strait of Talora",
            "aliases": [],
            "source_name": "curated-general",
            "population": 9_200_000,
        },
        {
            "entity_id": "location:north-vale",
            "entity_type": "location",
            "canonical_text": "North Vale",
            "aliases": ["Vale"],
            "source_name": "curated-general",
            "popularity": 0.55,
        },
        {
            "entity_id": "location:north-harbor",
            "entity_type": "location",
            "canonical_text": "North Harbor",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.5,
        },
        {
            "entity_id": "location:harborview-city",
            "entity_type": "location",
            "canonical_text": "Harborview",
            "aliases": [],
            "source_name": "curated-general",
            "population": 8_800_000,
        },
        {
            "entity_id": "location:harborview-metro",
            "entity_type": "location",
            "canonical_text": "Harborview",
            "aliases": [],
            "source_name": "curated-general",
            "population": 8_900_000,
        },
        {
            "entity_id": "organization:bank-of-talora",
            "entity_type": "organization",
            "canonical_text": "Bank of Talora",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.83,
        },
        {
            "entity_id": "organization:national-bank-of-talora",
            "entity_type": "organization",
            "canonical_text": "National Bank of Talora",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.9,
        },
    ]
    (normalized_dir / "entities.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in entities),
        encoding="utf-8",
    )

    rules = [
        {
            "name": "email_address",
            "label": "email_address",
            "kind": "regex",
            "pattern": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        }
    ]
    (normalized_dir / "rules.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in rules),
        encoding="utf-8",
    )
    return bundle_dir


def create_bundle_backed_general_pack_source(root: Path) -> Path:
    pack_dir = root / "general-en"
    normalized_dir = pack_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "schema_version": 1,
        "pack_id": "general-en",
        "version": "0.2.1",
        "language": "en",
        "domain": "general",
        "tier": "base",
        "description": "Generated English baseline entities and deterministic rules.",
        "tags": ["general", "generated", "english"],
        "dependencies": [],
        "artifacts": [],
        "models": [],
        "rules": ["email_address"],
        "labels": ["person", "organization", "location"],
        "min_ades_version": "0.1.0",
        "min_entities_per_100_tokens_warning": 0.25,
    }
    (pack_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    (pack_dir / "labels.json").write_text(
        json.dumps(["person", "organization", "location"], indent=2) + "\n",
        encoding="utf-8",
    )
    (pack_dir / "rules.json").write_text(
        json.dumps(
            {
                "patterns": [
                    {
                        "name": "email_address",
                        "label": "email_address",
                        "pattern": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (pack_dir / "build.json").write_text(
        json.dumps(
            {
                "included_entity_count": 7,
                "label_distribution": {
                    "person": 3,
                    "organization": 3,
                    "location": 1,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    entities = [
        {
            "entity_id": "person:jordan-elliott-vale",
            "entity_type": "person",
            "canonical_text": "Jordan Elliott Vale",
            "aliases": ["Jordan Vale"],
            "source_name": "curated-general",
            "popularity": 0.7,
        },
        {
            "entity_id": "person:avery-morgan-cole",
            "entity_type": "person",
            "canonical_text": "Avery Morgan Cole",
            "aliases": ["Avery Cole"],
            "source_name": "curated-general",
            "popularity": 0.66,
        },
        {
            "entity_id": "person:riley-sutton-hale",
            "entity_type": "person",
            "canonical_text": "Riley Sutton Hale",
            "aliases": ["Riley Hale"],
            "source_name": "curated-general",
            "popularity": 0.64,
        },
        {
            "entity_id": "organization:beacon-group",
            "entity_type": "organization",
            "canonical_text": "Beacon Group",
            "aliases": ["Beacon"],
            "source_name": "curated-general",
            "popularity": 0.95,
        },
        {
            "entity_id": "organization:signal-harbor-partners",
            "entity_type": "organization",
            "canonical_text": "Signal Harbor Partners",
            "aliases": ["Signal Harbor"],
            "source_name": "curated-general",
            "popularity": 0.72,
        },
        {
            "entity_id": "organization:atlas-collective",
            "entity_type": "organization",
            "canonical_text": "Atlas Collective",
            "aliases": ["Atlas"],
            "source_name": "curated-general",
            "popularity": 0.68,
        },
        {
            "entity_id": "location:north-harbor",
            "entity_type": "location",
            "canonical_text": "North Harbor",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.5,
        },
    ]
    (normalized_dir / "entities.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in entities),
        encoding="utf-8",
    )
    return pack_dir


def create_bundle_backed_general_pack_source_with_alias_collision(root: Path) -> Path:
    pack_dir = create_bundle_backed_general_pack_source(root)
    entities_path = pack_dir / "normalized" / "entities.jsonl"
    records = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    records.extend(
        [
            {
                "entity_id": "person:jordan-elliott-vale-primary",
                "entity_type": "person",
                "canonical_text": "Jordan Elliott Vale Prime",
                "aliases": ["Jordan Vale"],
                "source_name": "curated-general",
                "popularity": 0.92,
            },
            {
                "entity_id": "person:jordan-avery-vale-secondary",
                "entity_type": "person",
                "canonical_text": "Jordan Avery Vale Secondary",
                "aliases": ["Jordan Vale"],
                "source_name": "curated-general",
                "popularity": 0.35,
            },
        ]
    )
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    return pack_dir


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
            "entity_id": "issuer:alpha-holdings",
            "entity_type": "issuer",
            "canonical_text": "Issuer Alpha Holdings",
            "aliases": ["N/A"],
            "source_name": "sec-companyfacts",
        },
        {
            "entity_id": "ticker:ticka",
            "entity_type": "ticker",
            "canonical_text": "TICKA",
            "aliases": [],
            "source_name": "nasdaq-symbols",
        },
        {
            "entity_id": "exchange:exchx",
            "entity_type": "exchange",
            "canonical_text": "EXCHX",
            "aliases": ["Exchange Alpha"],
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
                "entity_id": "issuer:exchange-alpha-holdings",
                "entity_type": "issuer",
                "canonical_text": "Exchange Alpha Holdings",
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
