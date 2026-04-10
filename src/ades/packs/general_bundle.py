"""General-domain source-bundle builders for real-content pack generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import csv
import json
from pathlib import Path
import shutil
from typing import Any


@dataclass(frozen=True)
class GeneralSourceBundleResult:
    """Summary of one generated `general-en` normalized source bundle."""

    pack_id: str
    version: str
    output_dir: str
    bundle_dir: str
    bundle_manifest_path: str
    entities_path: str
    rules_path: str
    generated_at: str
    source_count: int
    entity_record_count: int
    rule_record_count: int
    wikidata_entity_count: int
    geonames_location_count: int
    curated_entity_count: int
    warnings: list[str] = field(default_factory=list)


def build_general_source_bundle(
    *,
    wikidata_entities_path: str | Path,
    geonames_places_path: str | Path,
    curated_entities_path: str | Path,
    output_dir: str | Path,
    version: str = "0.2.0",
) -> GeneralSourceBundleResult:
    """Build a normalized `general-en` source bundle from raw snapshot files."""

    wikidata_path = _resolve_input_file(
        wikidata_entities_path,
        label="Wikidata general entity snapshot",
    )
    geonames_path = _resolve_input_file(
        geonames_places_path,
        label="GeoNames place snapshot",
    )
    curated_path = _resolve_input_file(
        curated_entities_path,
        label="curated general entities snapshot",
    )
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    bundle_dir = resolved_output_dir / "general-en-bundle"
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    wikidata_entities = _load_wikidata_general_entities(wikidata_path)
    geonames_locations = _load_geonames_locations(geonames_path)
    curated_entities = _load_curated_general_entities(curated_path)
    rules = _build_general_rule_records()

    entities = sorted(
        [*wikidata_entities, *geonames_locations, *curated_entities],
        key=lambda item: (
            str(item["entity_type"]).casefold(),
            str(item["canonical_text"]).casefold(),
            str(item["entity_id"]).casefold(),
        ),
    )

    entities_path = normalized_dir / "entities.jsonl"
    rules_path = normalized_dir / "rules.jsonl"
    bundle_manifest_path = bundle_dir / "bundle.json"
    generated_at = _utc_timestamp()

    _write_jsonl(entities_path, entities)
    _write_jsonl(rules_path, rules)

    sources = [
        {
            "name": "wikidata-general-entities",
            "snapshot_uri": wikidata_path.as_uri(),
            "license_class": "ship-now",
            "license": "operator-supplied",
            "retrieved_at": _path_timestamp(wikidata_path),
            "record_count": len(wikidata_entities),
            "notes": "adapter=wikidata_general_entities",
        },
        {
            "name": "geonames-places",
            "snapshot_uri": geonames_path.as_uri(),
            "license_class": "ship-now",
            "license": "operator-supplied",
            "retrieved_at": _path_timestamp(geonames_path),
            "record_count": len(geonames_locations),
            "notes": "adapter=geonames_places_delimited",
        },
        {
            "name": "curated-general-entities",
            "snapshot_uri": curated_path.as_uri(),
            "license_class": "ship-now",
            "license": "operator-supplied",
            "retrieved_at": _path_timestamp(curated_path),
            "record_count": len(curated_entities),
            "notes": "adapter=curated_general_entities",
        },
    ]
    bundle_manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": "general-en",
                "version": version,
                "language": "en",
                "domain": "general",
                "tier": "base",
                "description": "Generated English baseline entities and generic utility rules.",
                "tags": ["baseline", "general", "generated", "english"],
                "dependencies": [],
                "entities_path": "normalized/entities.jsonl",
                "rules_path": "normalized/rules.jsonl",
                "label_mappings": {
                    "organization": "organization",
                    "person": "person",
                    "location": "location",
                },
                "stoplisted_aliases": ["N/A"],
                "allowed_ambiguous_aliases": [],
                "sources": sources,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    warnings: list[str] = []
    if not wikidata_entities:
        warnings.append("Wikidata snapshot produced no general entities.")
    if not geonames_locations:
        warnings.append("GeoNames snapshot produced no locations.")
    if not curated_entities:
        warnings.append("Curated entity snapshot produced no general entities.")

    return GeneralSourceBundleResult(
        pack_id="general-en",
        version=version,
        output_dir=str(resolved_output_dir),
        bundle_dir=str(bundle_dir),
        bundle_manifest_path=str(bundle_manifest_path),
        entities_path=str(entities_path),
        rules_path=str(rules_path),
        generated_at=generated_at,
        source_count=len(sources),
        entity_record_count=len(entities),
        rule_record_count=len(rules),
        wikidata_entity_count=len(wikidata_entities),
        geonames_location_count=len(geonames_locations),
        curated_entity_count=len(curated_entities),
        warnings=warnings,
    )


def _load_wikidata_general_entities(path: Path) -> list[dict[str, Any]]:
    if path.suffix.casefold() == ".jsonl":
        records = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            records = payload.get("entities", list(payload.values()))
        elif isinstance(payload, list):
            records = payload
        else:
            raise ValueError(f"Unsupported Wikidata snapshot structure: {path}")

    normalized: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        canonical_text = _clean_text(
            item.get("canonical_text") or item.get("label") or item.get("name")
        )
        entity_type = _clean_text(item.get("entity_type") or item.get("type"))
        if not canonical_text or not entity_type:
            continue
        aliases = item.get("aliases") or item.get("alt_labels") or []
        if not isinstance(aliases, list):
            aliases = [aliases]
        source_id = _clean_text(item.get("entity_id") or item.get("id")) or canonical_text.casefold()
        normalized.append(
            {
                "entity_id": f"wikidata:{source_id}",
                "entity_type": entity_type,
                "canonical_text": canonical_text,
                "aliases": [_clean_text(alias) for alias in aliases if _clean_text(alias)],
                "source_name": "wikidata-general-entities",
                "source_id": source_id,
            }
        )
    return normalized


def _load_geonames_locations(path: Path) -> list[dict[str, Any]]:
    first_line = path.read_text(encoding="utf-8").splitlines()[0]
    delimiter = "\t" if "\t" in first_line else ("|" if "|" in first_line else ",")
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        records: list[dict[str, Any]] = []
        for row in reader:
            name = _clean_text(row.get("name") or row.get("Name"))
            if not name:
                continue
            source_id = _clean_text(
                row.get("geonameid") or row.get("geoname_id") or row.get("id")
            ) or name.casefold()
            alternates = _clean_text(
                row.get("alternatenames") or row.get("alternate_names")
            )
            aliases = [item for item in (alternates or "").split(",") if item.strip()]
            records.append(
                {
                    "entity_id": f"geonames:{source_id}",
                    "entity_type": "location",
                    "canonical_text": name,
                    "aliases": [_clean_text(alias) for alias in aliases if _clean_text(alias)],
                    "source_name": "geonames-places",
                    "source_id": source_id,
                }
            )
    return records


def _load_curated_general_entities(path: Path) -> list[dict[str, Any]]:
    if path.suffix.casefold() == ".jsonl":
        records = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            records = payload.get("entities", [])
        elif isinstance(payload, list):
            records = payload
        else:
            raise ValueError(f"Unsupported curated entity snapshot structure: {path}")

    normalized: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        canonical_text = _clean_text(item.get("canonical_text") or item.get("name"))
        entity_type = _clean_text(item.get("entity_type") or item.get("type"))
        if not canonical_text or not entity_type:
            continue
        aliases = item.get("aliases") or []
        if not isinstance(aliases, list):
            aliases = [aliases]
        source_id = _clean_text(item.get("entity_id") or item.get("id")) or canonical_text.casefold()
        normalized.append(
            {
                "entity_id": f"curated-general:{source_id}",
                "entity_type": entity_type,
                "canonical_text": canonical_text,
                "aliases": [_clean_text(alias) for alias in aliases if _clean_text(alias)],
                "source_name": "curated-general-entities",
                "source_id": source_id,
            }
        )
    return normalized


def _build_general_rule_records() -> list[dict[str, str]]:
    return [
        {
            "name": "email_address",
            "label": "email_address",
            "kind": "regex",
            "pattern": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        },
        {
            "name": "url",
            "label": "url",
            "kind": "regex",
            "pattern": r"https?://[^\s]+",
        },
    ]


def _resolve_input_file(path: str | Path, *, label: str) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"{label} not found: {resolved}")
    if not resolved.is_file():
        raise IsADirectoryError(f"{label} is not a file: {resolved}")
    return resolved


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(record, sort_keys=True) + "\n" for record in records), encoding="utf-8")


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _path_timestamp(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
