"""General-domain source-bundle builders for real-content pack generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import csv
import io
import json
from pathlib import Path
import re
import shutil
from typing import Any, Iterator
import zipfile

from .source_lock import build_bundle_source_entry, write_sources_lock

_PLAIN_SINGLE_TOKEN_PERSON_ALIAS_RE = re.compile(r"^[A-Za-z][A-Za-z'-]*$")
_GENERAL_SINGLE_TOKEN_ALPHA_RE = re.compile(r"^[A-Za-z][A-Za-z'-]*$")
_GENERAL_LOCATION_ASCII_TOKEN_RE = re.compile(r"^[A-Za-z][A-Za-z'-]*$")
_GENERAL_LOCATION_LOWERCASE_TOKEN_RE = re.compile(r"^[a-z][a-z'-]*$")
_GENERAL_DOMAIN_RE = re.compile(r"(?i)^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$")
_GENERAL_ORG_SUFFIX_TOKENS = {
    "ag",
    "analytics",
    "bank",
    "capital",
    "co",
    "company",
    "consultancy",
    "consulting",
    "corp",
    "corporation",
    "exchange",
    "fund",
    "group",
    "holding",
    "holdings",
    "inc",
    "information",
    "intelligence",
    "issuer",
    "limited",
    "llc",
    "ltd",
    "management",
    "media",
    "nv",
    "organization",
    "partner",
    "partners",
    "plc",
    "research",
    "sa",
    "solutions",
    "systems",
    "technologies",
    "technology",
    "venture",
    "ventures",
}
_GENERAL_LOCATION_MIN_POPULATION_FOR_SHORT_ASCII = 1_000_000
_GENERAL_REAL_SUPPORT_MIN_SITELINKS_BY_TYPE = {
    "person": 1,
    "organization": 1,
    "location": 1,
}
_GENERAL_REAL_SUPPORT_MIN_SINGLE_TOKEN_LOCATION_SITELINKS = 10
_GENERAL_REAL_SUPPORT_MIN_ALIAS_COUNT_BY_TYPE = {
    "person": 1,
    "organization": 1,
    "location": 1,
}
_GENERAL_REAL_SUPPORT_MIN_SHORT_WIKIDATA_LOCATION_SITELINKS = 10
_GENERAL_PERSON_SINGLE_TOKEN_ALIAS_MIN_SUPPORT = 250


@dataclass(frozen=True)
class GeneralSourceBundleResult:
    """Summary of one generated `general-en` normalized source bundle."""

    pack_id: str
    version: str
    output_dir: str
    bundle_dir: str
    bundle_manifest_path: str
    sources_lock_path: str
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

    rules = _build_general_rule_records()

    entities_path = normalized_dir / "entities.jsonl"
    rules_path = normalized_dir / "rules.jsonl"
    bundle_manifest_path = bundle_dir / "bundle.json"
    sources_lock_path = bundle_dir / "sources.lock.json"
    generated_at = _utc_timestamp()

    wikidata_entity_count = 0
    geonames_location_count = 0
    curated_entity_count = 0
    entity_record_count = 0

    with entities_path.open("w", encoding="utf-8") as handle:
        for record in _iter_wikidata_general_entities(wikidata_path):
            handle.write(json.dumps(record, sort_keys=True) + "\n")
            wikidata_entity_count += 1
            entity_record_count += 1
        for record in _iter_geonames_locations(geonames_path):
            handle.write(json.dumps(record, sort_keys=True) + "\n")
            geonames_location_count += 1
            entity_record_count += 1
        for record in _iter_curated_general_entities(curated_path):
            handle.write(json.dumps(record, sort_keys=True) + "\n")
            curated_entity_count += 1
            entity_record_count += 1
    _write_jsonl(rules_path, rules)

    sources = [
        build_bundle_source_entry(
            name="wikidata-general-entities",
            snapshot_path=wikidata_path,
            record_count=wikidata_entity_count,
            adapter="wikidata_general_entities",
        ),
        build_bundle_source_entry(
            name="geonames-places",
            snapshot_path=geonames_path,
            record_count=geonames_location_count,
            adapter="geonames_places_delimited",
        ),
        build_bundle_source_entry(
            name="curated-general-entities",
            snapshot_path=curated_path,
            record_count=curated_entity_count,
            adapter="curated_general_entities",
        ),
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
                "alias_resolution": "analyze",
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
    write_sources_lock(
        bundle_dir=bundle_dir,
        pack_id="general-en",
        version=version,
        entities_path="normalized/entities.jsonl",
        rules_path="normalized/rules.jsonl",
        sources=sources,
    )

    warnings: list[str] = []
    if wikidata_entity_count == 0:
        warnings.append("Wikidata snapshot produced no general entities.")
    if geonames_location_count == 0:
        warnings.append("GeoNames snapshot produced no locations.")
    if curated_entity_count == 0:
        warnings.append("Curated entity snapshot produced no general entities.")

    return GeneralSourceBundleResult(
        pack_id="general-en",
        version=version,
        output_dir=str(resolved_output_dir),
        bundle_dir=str(bundle_dir),
        bundle_manifest_path=str(bundle_manifest_path),
        sources_lock_path=str(sources_lock_path),
        entities_path=str(entities_path),
        rules_path=str(rules_path),
        generated_at=generated_at,
        source_count=len(sources),
        entity_record_count=entity_record_count,
        rule_record_count=len(rules),
        wikidata_entity_count=wikidata_entity_count,
        geonames_location_count=geonames_location_count,
        curated_entity_count=curated_entity_count,
        warnings=warnings,
    )


def _iter_wikidata_general_entities(path: Path) -> Iterator[dict[str, Any]]:
    if path.suffix.casefold() == ".jsonl":
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                cleaned_line = line.strip()
                if not cleaned_line:
                    continue
                item = json.loads(cleaned_line)
                if not isinstance(item, dict):
                    continue
                normalized = _normalize_wikidata_general_entity(item)
                if normalized is None:
                    continue
                yield normalized
        return
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            records = payload.get("entities", list(payload.values()))
        elif isinstance(payload, list):
            records = payload
        else:
            raise ValueError(f"Unsupported Wikidata snapshot structure: {path}")

    for item in records:
        if not isinstance(item, dict):
            continue
        normalized = _normalize_wikidata_general_entity(item)
        if normalized is None:
            continue
        yield normalized


def _iter_geonames_locations(path: Path) -> Iterator[dict[str, Any]]:
    with _open_geonames_text_stream(path) as handle:
        first_line = handle.readline()
        if not first_line:
            return
        delimiter = "\t" if "\t" in first_line else ("|" if "|" in first_line else ",")
        handle.seek(0)
        header_fields = [
            field.strip().casefold() for field in first_line.rstrip("\n\r").split(delimiter)
        ]
        if _looks_like_geonames_header(header_fields):
            reader = csv.DictReader(handle, delimiter=delimiter)
        else:
            reader = csv.DictReader(
                handle,
                delimiter=delimiter,
                fieldnames=_GEONAMES_OFFICIAL_FIELDNAMES,
            )
        for row in reader:
            name = _clean_text(
                row.get("name") or row.get("Name") or row.get("asciiname") or row.get("ascii_name")
            )
            if not name:
                continue
            population = _coerce_int(row.get("population"))
            feature_class = _clean_text(
                row.get("feature class") or row.get("feature_class") or row.get("featureClass")
            )
            feature_code = _clean_text(
                row.get("feature code") or row.get("feature_code") or row.get("featureCode")
            )
            if not _should_keep_geonames_location_row(
                feature_class=feature_class,
                feature_code=feature_code,
                population=population,
            ):
                continue
            if not _should_keep_general_location_text(name, population=population):
                continue
            source_id = (
                _clean_text(row.get("geonameid") or row.get("geoname_id") or row.get("id"))
                or name.casefold()
            )
            alternates = _clean_text(row.get("alternatenames") or row.get("alternate_names"))
            aliases = _prune_general_aliases(
                entity_type="location",
                canonical_text=name,
                aliases=[item for item in (alternates or "").split(",") if item.strip()],
                popularity=population,
            )
            yield {
                "entity_id": f"geonames:{source_id}",
                "entity_type": "location",
                "canonical_text": name,
                "aliases": aliases,
                "source_name": "geonames-places",
                "source_id": source_id,
                "population": population,
            }


_GEONAMES_OFFICIAL_FIELDNAMES = [
    "geonameid",
    "name",
    "asciiname",
    "alternatenames",
    "latitude",
    "longitude",
    "feature class",
    "feature code",
    "country code",
    "cc2",
    "admin1 code",
    "admin2 code",
    "admin3 code",
    "admin4 code",
    "population",
    "elevation",
    "dem",
    "timezone",
    "modification date",
]

_GENERAL_GEONAMES_ALLOWED_FEATURE_CLASSES = {"A", "P"}
_GENERAL_GEONAMES_ALLOWED_POPULATED_PLACE_CODES = {
    "PPLC",
    "PPLA",
    "PPLA2",
    "PPLA3",
    "PPLA4",
    "PPLG",
    "PPLCH",
}
_GENERAL_GEONAMES_MIN_POPULATION = 1000


def _open_geonames_text_stream(path: Path) -> io.TextIOBase:
    if path.suffix.casefold() != ".zip":
        return path.open("r", encoding="utf-8", newline="")
    archive = zipfile.ZipFile(path)
    member_name = _select_geonames_zip_member(archive)
    raw_handle = archive.open(member_name, "r")
    text_handle = io.TextIOWrapper(raw_handle, encoding="utf-8", newline="")

    class _GeonamesZipTextStream(io.TextIOBase):
        def readable(self) -> bool:  # pragma: no cover - delegation wrapper
            return True

        def seekable(self) -> bool:  # pragma: no cover - delegation wrapper
            return text_handle.seekable()

        def read(self, size: int = -1) -> str:
            return text_handle.read(size)

        def readline(self, size: int = -1) -> str:
            return text_handle.readline(size)

        def seek(self, offset: int, whence: int = 0) -> int:
            return text_handle.seek(offset, whence)

        def tell(self) -> int:  # pragma: no cover - delegation wrapper
            return text_handle.tell()

        def close(self) -> None:
            try:
                text_handle.close()
            finally:
                archive.close()

        def __enter__(self) -> "_GeonamesZipTextStream":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            self.close()

    return _GeonamesZipTextStream()


def _select_geonames_zip_member(archive: zipfile.ZipFile) -> str:
    member_names = [info.filename for info in archive.infolist() if not info.is_dir()]
    if not member_names:
        raise ValueError("GeoNames zip archive is empty.")
    preferred_names = [
        name for name in member_names if Path(name).name.casefold() == "allcountries.txt"
    ]
    if preferred_names:
        return preferred_names[0]
    text_members = [name for name in member_names if name.casefold().endswith(".txt")]
    if len(text_members) == 1:
        return text_members[0]
    if len(member_names) == 1:
        return member_names[0]
    raise ValueError(f"GeoNames zip archive contains multiple candidate members: {member_names}")


def _looks_like_geonames_header(fields: list[str]) -> bool:
    return "name" in fields and ("geonameid" in fields or "geoname_id" in fields or "id" in fields)


def _should_keep_geonames_location_row(
    *,
    feature_class: str | None,
    feature_code: str | None,
    population: int | None,
) -> bool:
    normalized_feature_class = (feature_class or "").strip().upper()
    normalized_feature_code = (feature_code or "").strip().upper()
    population_value = population or 0

    if normalized_feature_class not in _GENERAL_GEONAMES_ALLOWED_FEATURE_CLASSES:
        return False
    if normalized_feature_class == "A":
        return normalized_feature_code.startswith("ADM") or normalized_feature_code.startswith(
            "PCL"
        )
    if normalized_feature_class == "P":
        return (
            population_value >= _GENERAL_GEONAMES_MIN_POPULATION
            or normalized_feature_code in _GENERAL_GEONAMES_ALLOWED_POPULATED_PLACE_CODES
        )
    return False


def _iter_curated_general_entities(path: Path) -> Iterator[dict[str, Any]]:
    if path.suffix.casefold() == ".jsonl":
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                cleaned_line = line.strip()
                if not cleaned_line:
                    continue
                item = json.loads(cleaned_line)
                if not isinstance(item, dict):
                    continue
                normalized = _normalize_curated_general_entity(item)
                if normalized is not None:
                    yield normalized
        return
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            records = payload.get("entities", [])
        elif isinstance(payload, list):
            records = payload
        else:
            raise ValueError(f"Unsupported curated entity snapshot structure: {path}")

    for item in records:
        if not isinstance(item, dict):
            continue
        normalized = _normalize_curated_general_entity(item)
        if normalized is None:
            continue
        yield normalized


def _coerce_int(raw_value: object) -> int | None:
    if raw_value is None:
        return None
    text = _clean_text(raw_value)
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _normalize_wikidata_general_entity(item: dict[str, Any]) -> dict[str, Any] | None:
    canonical_text = _clean_text(
        item.get("canonical_text") or item.get("label") or item.get("name")
    )
    entity_type = _clean_text(item.get("entity_type") or item.get("type"))
    if not canonical_text or not entity_type:
        return None
    if entity_type not in {"person", "organization", "location"}:
        return None
    aliases = item.get("aliases") or item.get("alt_labels") or []
    if not isinstance(aliases, list):
        aliases = [aliases]
    source_id = _clean_text(item.get("entity_id") or item.get("id")) or canonical_text.casefold()
    popularity = _coerce_int(
        item.get("popularity") or item.get("sitelinks") or item.get("sitelink_count")
    )
    source_features = item.get("source_features")
    if not isinstance(source_features, dict):
        source_features = None
    sitelink_support = (
        _coerce_int(source_features.get("sitelink_count"))
        if isinstance(source_features, dict)
        else None
    ) or popularity
    pruned_aliases = _prune_general_aliases(
        entity_type=entity_type,
        canonical_text=canonical_text,
        aliases=aliases,
        popularity=popularity if source_features is None else None,
        support_count=sitelink_support if source_features is not None else None,
    )
    if not _should_keep_general_wikidata_entity(
        entity_type=entity_type,
        canonical_text=canonical_text,
        aliases=pruned_aliases,
        popularity=popularity,
        source_features=source_features,
    ):
        return None
    record: dict[str, Any] = {
        "entity_id": f"wikidata:{source_id}",
        "entity_type": entity_type,
        "canonical_text": canonical_text,
        "aliases": pruned_aliases,
        "source_name": "wikidata-general-entities",
        "source_id": source_id,
        "popularity": popularity,
    }
    description = _clean_text(item.get("description"))
    if description:
        record["description"] = description
    raw_type_ids = item.get("type_ids")
    if isinstance(raw_type_ids, list):
        record["type_ids"] = [type_id for type_id in raw_type_ids if isinstance(type_id, str)]
    if isinstance(source_features, dict):
        record["source_features"] = source_features
    family_confidence = item.get("family_confidence")
    if isinstance(family_confidence, (float, int)):
        record["family_confidence"] = float(family_confidence)
    family_source = _clean_text(item.get("family_source"))
    if family_source:
        record["family_source"] = family_source
    return record


def _normalize_curated_general_entity(item: dict[str, Any]) -> dict[str, Any] | None:
    canonical_text = _clean_text(item.get("canonical_text") or item.get("name"))
    entity_type = _clean_text(item.get("entity_type") or item.get("type"))
    if not canonical_text or not entity_type:
        return None
    if entity_type not in {"person", "organization", "location"}:
        return None
    aliases = item.get("aliases") or []
    if not isinstance(aliases, list):
        aliases = [aliases]
    raw_entity_id = _clean_text(item.get("entity_id"))
    if raw_entity_id and ":" in raw_entity_id:
        namespace, local_id = raw_entity_id.split(":", 1)
        if namespace and local_id:
            entity_id = raw_entity_id
            source_id = local_id
        else:
            source_id = raw_entity_id or canonical_text.casefold()
            entity_id = f"curated-general:{source_id}"
    else:
        source_id = raw_entity_id or _clean_text(item.get("id")) or canonical_text.casefold()
        entity_id = f"curated-general:{source_id}"
    record: dict[str, Any] = {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "canonical_text": canonical_text,
        "aliases": _prune_general_aliases(
            entity_type=entity_type,
            canonical_text=canonical_text,
            aliases=aliases,
        ),
        "source_name": "curated-general-entities",
        "source_id": source_id,
    }
    description = _clean_text(item.get("description"))
    if description:
        record["description"] = description
    popularity = _coerce_int(
        item.get("popularity") or item.get("sitelinks") or item.get("sitelink_count")
    )
    if popularity is not None:
        record["popularity"] = popularity
    source_features = item.get("source_features")
    if isinstance(source_features, dict):
        record["source_features"] = source_features
    return record


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
    path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records), encoding="utf-8"
    )


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _prune_general_aliases(
    *,
    entity_type: str,
    canonical_text: str,
    aliases: list[object],
    popularity: int | None = None,
    support_count: int | None = None,
) -> list[str]:
    normalized_aliases: list[str] = []
    canonical_token_count = len(canonical_text.split())
    for alias in aliases:
        cleaned = _clean_text(alias)
        if not cleaned:
            continue
        if cleaned.casefold() == canonical_text.casefold():
            continue
        if entity_type == "location" and not _should_keep_general_location_text(
            cleaned,
            population=popularity,
            support_count=support_count,
        ):
            continue
        if (
            entity_type == "person"
            and canonical_token_count > 1
            and _PLAIN_SINGLE_TOKEN_PERSON_ALIAS_RE.fullmatch(cleaned) is not None
            and not _should_keep_supported_person_single_token_alias(
                cleaned,
                canonical_text=canonical_text,
                popularity=popularity,
                support_count=support_count,
            )
        ):
            continue
        if (
            entity_type == "location"
            and cleaned.casefold() != canonical_text.casefold()
            and _is_single_token_alpha_alias(cleaned)
        ):
            continue
        if (
            entity_type == "organization"
            and cleaned.casefold() != canonical_text.casefold()
            and _should_drop_general_organization_alias(
                cleaned,
                canonical_text=canonical_text,
            )
        ):
            continue
        if entity_type == "organization" and _looks_like_domain_alias(cleaned):
            continue
        if cleaned not in normalized_aliases:
            normalized_aliases.append(cleaned)
    return normalized_aliases


def _looks_like_domain_alias(value: str) -> bool:
    return _GENERAL_DOMAIN_RE.fullmatch(value) is not None


def _has_structural_org_suffix(value: str) -> bool:
    tokens = [token.rstrip(".,").casefold() for token in value.split() if token.strip()]
    if len(tokens) < 2:
        return False
    return tokens[-1] in _GENERAL_ORG_SUFFIX_TOKENS


def _should_keep_supported_person_single_token_alias(
    value: str,
    *,
    canonical_text: str,
    popularity: int | None,
    support_count: int | None,
) -> bool:
    canonical_tokens = [
        token for token in re.findall(r"[A-Za-z][A-Za-z'-]*", canonical_text) if token.strip()
    ]
    if len(canonical_tokens) < 2:
        return True
    normalized_value = value.casefold()
    if normalized_value != canonical_tokens[-1].casefold():
        return False
    if len(normalized_value) < 5:
        return False
    support = support_count if support_count is not None else popularity
    return (support or 0) >= _GENERAL_PERSON_SINGLE_TOKEN_ALIAS_MIN_SUPPORT


def _should_keep_general_wikidata_entity(
    *,
    entity_type: str,
    canonical_text: str,
    aliases: list[str],
    popularity: int | None,
    source_features: dict[str, Any] | None,
) -> bool:
    if source_features is None:
        return True
    sitelink_count = _coerce_int(source_features.get("sitelink_count")) or popularity or 0
    alias_count = max(
        len(aliases),
        _coerce_int(source_features.get("alias_count")) or 0,
    )
    identifier_count = _coerce_int(source_features.get("identifier_count")) or 0
    statement_count = _coerce_int(source_features.get("statement_count")) or 0
    has_enwiki = bool(source_features.get("has_enwiki"))
    popularity_signal = _clean_text(source_features.get("popularity_signal")) or ""
    if entity_type == "location" and not _should_keep_general_location_text(
        canonical_text,
        support_count=sitelink_count,
    ):
        return False
    if (
        entity_type == "location"
        and _is_single_token_alpha_alias(canonical_text)
        and sitelink_count < _GENERAL_REAL_SUPPORT_MIN_SINGLE_TOKEN_LOCATION_SITELINKS
    ):
        return False
    if has_enwiki:
        return True
    minimum_sitelinks = _GENERAL_REAL_SUPPORT_MIN_SITELINKS_BY_TYPE.get(entity_type, 0)
    if sitelink_count >= minimum_sitelinks:
        return True
    if entity_type == "location":
        return False
    if entity_type == "person":
        minimum_aliases = max(
            _GENERAL_REAL_SUPPORT_MIN_ALIAS_COUNT_BY_TYPE.get(entity_type, 0),
            2 if popularity_signal == "fallback" else 1,
        )
        return alias_count >= minimum_aliases
    if (
        entity_type == "organization"
        and alias_count >= _GENERAL_REAL_SUPPORT_MIN_ALIAS_COUNT_BY_TYPE.get(entity_type, 0)
    ):
        return True
    if entity_type == "organization" and _has_structural_org_suffix(canonical_text):
        return True
    if entity_type == "organization" and identifier_count > 0:
        return True
    if entity_type == "organization" and statement_count >= 8:
        return True
    return False


def _should_keep_general_location_text(
    value: str,
    *,
    population: int | None = None,
    support_count: int | None = None,
) -> bool:
    cleaned = _clean_text(value)
    if not cleaned:
        return False
    if _GENERAL_LOCATION_LOWERCASE_TOKEN_RE.fullmatch(cleaned) is not None:
        return False
    if cleaned.isupper() and cleaned.isalpha() and len(cleaned) <= 3:
        return False
    if _GENERAL_LOCATION_ASCII_TOKEN_RE.fullmatch(cleaned) is None:
        return True
    if " " in cleaned:
        return True
    population_value = population or 0
    if len(cleaned) <= 3:
        return False
    if (
        len(cleaned) <= 5
        and support_count is not None
        and support_count >= _GENERAL_REAL_SUPPORT_MIN_SHORT_WIKIDATA_LOCATION_SITELINKS
    ):
        return True
    if len(cleaned) <= 5 and population_value < _GENERAL_LOCATION_MIN_POPULATION_FOR_SHORT_ASCII:
        return False
    return True


def _is_single_token_alpha_alias(value: str) -> bool:
    return _GENERAL_SINGLE_TOKEN_ALPHA_RE.fullmatch(value) is not None


def _should_drop_general_organization_alias(
    value: str,
    *,
    canonical_text: str,
) -> bool:
    if not _is_single_token_alpha_alias(value):
        return False
    if value.isupper():
        return False
    if _is_structural_general_organization_fragment_alias(
        value,
        canonical_text=canonical_text,
    ):
        return True
    compact = "".join(character for character in value if character.isalnum())
    return len(compact) < 6


def _is_structural_general_organization_fragment_alias(
    value: str,
    *,
    canonical_text: str,
) -> bool:
    canonical_tokens = [
        token.rstrip(".,").casefold() for token in canonical_text.split() if token.strip()
    ]
    if len(canonical_tokens) < 3:
        return False
    if canonical_tokens[-1] not in _GENERAL_ORG_SUFFIX_TOKENS:
        return False
    normalized_value = value.strip().rstrip(".,").casefold()
    if not normalized_value:
        return False
    return normalized_value in {canonical_tokens[0], canonical_tokens[-1]}


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
