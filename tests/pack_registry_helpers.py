import json
from pathlib import Path
import sqlite3


def create_pack_source(
    root: Path,
    *,
    pack_id: str,
    domain: str,
    version: str = "0.1.0",
    language: str = "en",
    tier: str = "base",
    dependencies: tuple[str, ...] = (),
    description: str | None = None,
    tags: tuple[str, ...] = (),
    labels: tuple[str, ...] = ("entity",),
    aliases: tuple[tuple[str, str], ...] = (),
    rules: tuple[tuple[str, str] | tuple[str, str, str], ...] = (),
) -> Path:
    pack_dir = root / pack_id
    pack_dir.mkdir(parents=True, exist_ok=True)
    (pack_dir / "labels.json").write_text(
        json.dumps(list(labels), indent=2) + "\n",
        encoding="utf-8",
    )
    (pack_dir / "aliases.json").write_text(
        json.dumps(
            {
                "aliases": [
                    {"text": text, "label": label}
                    for text, label in aliases
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (pack_dir / "rules.json").write_text(
        json.dumps(
            {
                "patterns": [
                    _rule_record(item)
                    for item in rules
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (pack_dir / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": pack_id,
                "version": version,
                "language": language,
                "domain": domain,
                "tier": tier,
                "description": description or f"{pack_id} pack for {domain} tagging.",
                "tags": list(tags),
                "dependencies": list(dependencies),
                "artifacts": [
                    {
                        "name": "pack",
                        "url": f"../../artifacts/{pack_id}-{version}.tar.zst",
                        "sha256": "source-placeholder",
                    }
                ],
                "models": [],
                "rules": ["rules.json"],
                "labels": ["labels.json"],
                "min_ades_version": "0.1.0",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return pack_dir


def create_finance_registry_sources(
    root: Path,
    *,
    general_version: str = "0.1.0",
    finance_version: str = "0.1.0",
) -> tuple[Path, Path]:
    general_dir = create_pack_source(
        root,
        pack_id="general-en",
        domain="general",
        version=general_version,
        labels=("person", "organization", "location", "product"),
        description="Shared English baseline entities and generic utility patterns.",
        tags=("baseline", "general", "english"),
        rules=(
            ("email_address", r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
        ),
    )
    finance_dir = create_pack_source(
        root,
        pack_id="finance-en",
        domain="finance",
        version=finance_version,
        dependencies=("general-en",),
        description="English financial entities, exchanges, tickers, and money patterns.",
        tags=("finance", "markets", "english"),
        labels=("ticker", "exchange", "currency_amount"),
        aliases=(
            ("TICKA", "ticker"),
            ("EXCHX", "exchange"),
        ),
        rules=(
            ("currency_amount", r"\b(?:USD|EUR)\s?\d+(?:\.\d+)?\b"),
        ),
    )
    return general_dir, finance_dir


def append_pack_alias(pack_dir: Path, *, text: str, label: str) -> None:
    aliases_path = pack_dir / "aliases.json"
    payload = json.loads(aliases_path.read_text())
    payload.setdefault("aliases", []).append({"text": text, "label": label})
    aliases_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_general_pack_build(
    pack_dir: Path,
    *,
    person_count: int,
    organization_count: int,
    location_count: int,
) -> None:
    included_entity_count = person_count + organization_count + location_count
    (pack_dir / "build.json").write_text(
        json.dumps(
            {
                "included_entity_count": included_entity_count,
                "entity_label_distribution": {
                    "person": person_count,
                    "organization": organization_count,
                    "location": location_count,
                },
                "label_distribution": {
                    "person": person_count,
                    "organization": organization_count,
                    "location": location_count,
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def delete_installed_pack_metadata(storage_root: Path, pack_id: str) -> None:
    db_path = storage_root / "registry" / "ades.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("DELETE FROM installed_packs WHERE pack_id = ?", (pack_id,))


def _rule_record(item: tuple[str, str] | tuple[str, str, str]) -> dict[str, str]:
    if len(item) == 2:
        name, pattern = item
        label = name
    else:
        name, label, pattern = item
    return {
        "name": name,
        "label": label,
        "kind": "regex",
        "pattern": pattern,
    }
