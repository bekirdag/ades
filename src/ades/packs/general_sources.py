"""General-source snapshot fetchers for real-content pack generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from hashlib import sha256
import gzip
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import tempfile
from typing import Any, Iterator
import urllib.error
import urllib.request
from urllib.parse import urlparse
import zipfile

from .fetch import normalize_source_url


DEFAULT_GENERAL_SOURCE_OUTPUT_ROOT = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/raw/general-en"
)
DEFAULT_WIKIDATA_TRUTHY_URL = (
    "https://dumps.wikimedia.org/wikidatawiki/entities/latest-truthy.nt.gz"
)
DEFAULT_WIKIDATA_ENTITIES_URL = (
    "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz"
)
DEFAULT_GEONAMES_PLACES_URL = "https://download.geonames.org/export/dump/allCountries.zip"
DEFAULT_GEONAMES_ALTERNATE_NAMES_URL = (
    "https://download.geonames.org/export/dump/alternateNamesV2.zip"
)
DEFAULT_SOURCE_FETCH_USER_AGENT = "ades/0.1.0 (ops@adestool.com)"
DEFAULT_WIKIDATA_ALLOWED_TYPE_ROOTS: dict[str, str] = {
    "Q5": "person",
    "Q6256": "location",
    "Q35657": "location",
    "Q3624078": "location",
    "Q515": "location",
    "Q56061": "location",
    "Q486972": "location",
    "Q618123": "location",
    "Q163740": "organization",
    "Q31855": "organization",
    "Q43229": "organization",
    "Q4830453": "organization",
    "Q783794": "organization",
    "Q2385804": "organization",
    "Q2659904": "organization",
    "Q4438121": "organization",
    "Q7210356": "organization",
    "Q7278": "organization",
    "Q1126006": "organization",
    "Q1331793": "organization",
}
_GENERAL_ALLOWED_ENTITY_TYPES = frozenset({"person", "organization", "location"})
DEFAULT_WIKIDATA_SEED_SNAPSHOT_URL: str | None = None
_GEONAMES_ALLOWED_FEATURE_CODES = {
    "ADM1",
    "ADM2",
    "ADM3",
    "ADM4",
    "PCL",
    "PCLD",
    "PCLF",
    "PCLI",
    "PCLIX",
    "PCLS",
    "PPLA",
    "PPLA2",
    "PPLA3",
    "PPLA4",
    "PPLC",
    "PPL",
}
_GEONAMES_ALIAS_LANGUAGES = {"", "en"}
DEFAULT_GENERAL_GEONAMES_MIN_POPULATION = 100_000
DEFAULT_GENERAL_GEONAMES_MIN_ADMIN_POPULATION = 25_000
DEFAULT_WIKIDATA_MIN_SITELINKS_BY_ENTITY_TYPE: dict[str, int] = {
    "person": 5,
    "organization": 3,
    "location": 3,
}
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
}
_GENERAL_SINGLE_TOKEN_ALPHA_RE = re.compile(r"^[A-Za-z][A-Za-z'-]*$")
_GENERAL_MULTI_TOKEN_ALPHA_RE = re.compile(r"^[A-Za-z][A-Za-z' -]*[A-Za-z]$")
_GENERAL_WIKIDATA_MIN_SINGLE_TOKEN_PERSON_SITELINKS = 20
_GENERAL_WIKIDATA_MIN_SINGLE_TOKEN_LOCATION_SITELINKS = 15
_GENERAL_WIKIDATA_MIN_PLAIN_MULTI_TOKEN_ORGANIZATION_SITELINKS = 20
_WIKIDATA_IGNORED_SITELINK_TITLE_PREFIXES = {
    "appendix:",
    "book:",
    "category:",
    "draft:",
    "file:",
    "gadget definition:",
    "gadget:",
    "help:",
    "mediawiki:",
    "module:",
    "portal:",
    "template:",
    "timedtext:",
    "topic:",
    "user:",
    "wikipedia:",
}
_WIKIDATA_SOURCE_QUALITY_GATE_MIN_RECORDS = 100_000
_WIKIDATA_SOURCE_QUALITY_MIN_POSITIVE_POPULARITY_RATIO = 0.001
_WIKIDATA_SOURCE_QUALITY_MIN_ALIAS_RATIO = 0.05
_TRUTHY_STAGE_FAST_SIGNATURE_MIN_BYTES = 10 * 1024 * 1024 * 1024
_WIKIDATA_MIN_CANDIDATE_SITELINKS = min(
    DEFAULT_WIKIDATA_MIN_SITELINKS_BY_ENTITY_TYPE.values(),
    default=0,
)
DEFAULT_CURATED_GENERAL_ENTITIES: tuple[dict[str, object], ...] = ()
_WIKIDATA_TRUTHY_STAGE_SCHEMA_VERSION = 3
_WIKIDATA_TRUTHY_STAGE_LOGIC_VERSION = 3
_GEONAMES_OUTPUT_HEADER = (
    "geonameid|name|alternatenames|feature_class|feature_code|country_code|population"
)
_WIKIDATA_ENTITY_PREFIX = "http://www.wikidata.org/entity/"
_WIKIDATA_ENTITY_DATA_PREFIXES = (
    "http://www.wikidata.org/wiki/Special:EntityData/",
    "https://www.wikidata.org/wiki/Special:EntityData/",
)
_WIKIDATA_DIRECT_PROPERTY_PREFIX = "http://www.wikidata.org/prop/direct/"
_WIKIDATA_ALLOWED_PROPERTIES = {"P31", "P279"}
_WIKIDATA_RDFS_LABEL_PREDICATE = "http://www.w3.org/2000/01/rdf-schema#label"
_WIKIDATA_SKOS_PREF_LABEL_PREDICATE = "http://www.w3.org/2004/02/skos/core#prefLabel"
_WIKIDATA_SKOS_ALT_LABEL_PREDICATE = "http://www.w3.org/2004/02/skos/core#altLabel"
_WIKIDATA_SCHEMA_ABOUT_PREDICATE = "http://schema.org/about"
_WIKIDATA_SCHEMA_NAME_PREDICATE = "http://schema.org/name"
_WIKIDATA_SITELINKS_PREDICATE = "http://wikiba.se/ontology#sitelinks"
_WIKIDATA_DIRECT_P31_TERM = f"<{_WIKIDATA_DIRECT_PROPERTY_PREFIX}P31>"
_WIKIDATA_DIRECT_P279_TERM = f"<{_WIKIDATA_DIRECT_PROPERTY_PREFIX}P279>"
_WIKIDATA_SCHEMA_ABOUT_PREDICATE_TERM = f"<{_WIKIDATA_SCHEMA_ABOUT_PREDICATE}>"
_WIKIDATA_LABEL_PREDICATES = {
    _WIKIDATA_RDFS_LABEL_PREDICATE,
    _WIKIDATA_SKOS_PREF_LABEL_PREDICATE,
    _WIKIDATA_SCHEMA_NAME_PREDICATE,
}
_WIKIDATA_LABEL_PREDICATE_TERMS = {
    f"<{predicate}>" for predicate in _WIKIDATA_LABEL_PREDICATES
}
_WIKIDATA_ALT_LABEL_PREDICATE_TERM = f"<{_WIKIDATA_SKOS_ALT_LABEL_PREDICATE}>"
_WIKIDATA_SITELINKS_PREDICATE_TERM = f"<{_WIKIDATA_SITELINKS_PREDICATE}>"
_WIKIDATA_RELEVANT_PREDICATE_TERMS = {
    _WIKIDATA_DIRECT_P31_TERM,
    _WIKIDATA_DIRECT_P279_TERM,
    _WIKIDATA_SCHEMA_ABOUT_PREDICATE_TERM,
    *_WIKIDATA_LABEL_PREDICATE_TERMS,
    _WIKIDATA_ALT_LABEL_PREDICATE_TERM,
    _WIKIDATA_SITELINKS_PREDICATE_TERM,
}
_WIKIDATA_RELEVANT_LINE_PATTERNS = (
    re.escape(_WIKIDATA_DIRECT_P31_TERM),
    re.escape(_WIKIDATA_DIRECT_P279_TERM),
    re.escape(_WIKIDATA_SCHEMA_ABOUT_PREDICATE_TERM),
    re.escape(_WIKIDATA_SITELINKS_PREDICATE_TERM),
    *tuple(
        rf"{re.escape(predicate_term)} .*@en \.$"
        for predicate_term in sorted(
            (
                *_WIKIDATA_LABEL_PREDICATE_TERMS,
                _WIKIDATA_ALT_LABEL_PREDICATE_TERM,
            )
        )
    ),
)
_WIKIDATA_GREP_PREDICATE_TERMS = tuple(sorted(_WIKIDATA_RELEVANT_PREDICATE_TERMS))


@dataclass
class _WikidataTruthEntityState:
    entity_id: str
    type_ids: set[str] = field(default_factory=set)
    label: str | None = None
    aliases: list[str] = field(default_factory=list)
    sitelinks: int | None = None


@dataclass
class _WikidataTruthArticleState:
    subject_term: str
    entity_id: str | None = None
    aliases: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class GeneralSourceFetchResult:
    """Summary of one downloaded general source snapshot set."""

    pack_id: str
    output_dir: str
    snapshot: str
    snapshot_dir: str
    wikidata_url: str
    geonames_places_url: str
    source_manifest_path: str
    wikidata_entities_path: str
    geonames_places_path: str
    curated_entities_path: str
    generated_at: str
    source_count: int
    wikidata_entity_count: int
    geonames_location_count: int
    curated_entity_count: int
    wikidata_entities_sha256: str
    geonames_places_sha256: str
    curated_entities_sha256: str
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _WikidataSourceQualitySummary:
    record_count: int
    alias_backed_count: int
    positive_popularity_count: int

    @property
    def alias_ratio(self) -> float:
        if self.record_count <= 0:
            return 0.0
        return self.alias_backed_count / self.record_count

    @property
    def positive_popularity_ratio(self) -> float:
        if self.record_count <= 0:
            return 0.0
        return self.positive_popularity_count / self.record_count


@dataclass(frozen=True)
class _WikidataTruthyStageInfo:
    schema_version: int | None
    source_sha256: str | None
    root_fingerprint: str | None
    has_required_tables: bool
    is_complete: bool


def fetch_general_source_snapshot(
    *,
    output_dir: str | Path = DEFAULT_GENERAL_SOURCE_OUTPUT_ROOT,
    snapshot: str | None = None,
    wikidata_url: str | None = None,
    wikidata_truthy_url: str | None = DEFAULT_WIKIDATA_TRUTHY_URL,
    wikidata_entities_url: str | None = DEFAULT_WIKIDATA_ENTITIES_URL,
    wikidata_seed_url: str | None = DEFAULT_WIKIDATA_SEED_SNAPSHOT_URL,
    geonames_places_url: str = DEFAULT_GEONAMES_PLACES_URL,
    geonames_alternate_names_url: str | None = DEFAULT_GEONAMES_ALTERNATE_NAMES_URL,
    geonames_modifications_url: str | None = None,
    geonames_deletes_url: str | None = None,
    geonames_alternate_modifications_url: str | None = None,
    geonames_alternate_deletes_url: str | None = None,
    user_agent: str = DEFAULT_SOURCE_FETCH_USER_AGENT,
) -> GeneralSourceFetchResult:
    """Download one immutable general source snapshot set into the big-data root."""

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_snapshot = _resolve_snapshot(snapshot)
    snapshot_dir = resolved_output_dir / resolved_snapshot
    created_snapshot_dir = False
    if snapshot_dir.exists():
        if (snapshot_dir / "sources.fetch.json").exists():
            raise FileExistsError(
                f"General source snapshot directory already exists: {snapshot_dir}"
            )
    else:
        snapshot_dir.mkdir(parents=True, exist_ok=False)
        created_snapshot_dir = True

    generated_at = _utc_timestamp()
    warnings: list[str] = []
    wikidata_quality = _WikidataSourceQualitySummary(
        record_count=0,
        alias_backed_count=0,
        positive_popularity_count=0,
    )
    try:
        (
            resolved_wikidata_url,
            wikidata_entities_path,
            wikidata_entity_count,
            wikidata_source_entries,
        ) = _download_wikidata_snapshot(
            wikidata_url=wikidata_url,
            wikidata_truthy_url=wikidata_truthy_url,
            wikidata_entities_url=wikidata_entities_url,
            wikidata_seed_url=wikidata_seed_url,
            snapshot_dir=snapshot_dir,
            user_agent=user_agent,
        )
        wikidata_quality = _summarize_wikidata_source_quality(wikidata_entities_path)
        _validate_wikidata_source_quality(
            wikidata_quality,
            source_url=resolved_wikidata_url,
        )
        (
            resolved_geonames_url,
            geonames_places_path,
            geonames_location_count,
            geonames_source_entries,
        ) = _download_geonames_snapshot(
            geonames_places_url=geonames_places_url,
            geonames_alternate_names_url=geonames_alternate_names_url,
            geonames_modifications_url=geonames_modifications_url,
            geonames_deletes_url=geonames_deletes_url,
            geonames_alternate_modifications_url=geonames_alternate_modifications_url,
            geonames_alternate_deletes_url=geonames_alternate_deletes_url,
            snapshot_dir=snapshot_dir,
            user_agent=user_agent,
        )
        curated_entities_path = snapshot_dir / "curated_general_entities.json"
        _write_curated_entities(curated_entities_path)
        source_entries = [
            *wikidata_source_entries,
            *geonames_source_entries,
            {
                "name": "curated-general-entities",
                "source_url": "operator://ades/curated-general-entities",
                "path": curated_entities_path.name,
                "sha256": _sha256_file(curated_entities_path),
                "size_bytes": curated_entities_path.stat().st_size,
                "record_count": len(DEFAULT_CURATED_GENERAL_ENTITIES),
            },
        ]
        _write_source_manifest(
            path=snapshot_dir / "sources.fetch.json",
            snapshot=resolved_snapshot,
            generated_at=generated_at,
            user_agent=user_agent,
            source_entries=source_entries,
        )
    except Exception:
        if created_snapshot_dir:
            shutil.rmtree(snapshot_dir, ignore_errors=True)
        raise

    manifest_path = snapshot_dir / "sources.fetch.json"
    if wikidata_entity_count == 0:
        warnings.append("Wikidata snapshot produced no general entities.")
    else:
        if wikidata_quality.alias_ratio < _WIKIDATA_SOURCE_QUALITY_MIN_ALIAS_RATIO:
            warnings.append(
                "wikidata_source_low_alias_coverage:"
                + f"{wikidata_quality.alias_ratio:.4f}"
            )
        if (
            wikidata_quality.record_count
            >= _WIKIDATA_SOURCE_QUALITY_GATE_MIN_RECORDS
            and wikidata_quality.positive_popularity_ratio
            < _WIKIDATA_SOURCE_QUALITY_MIN_POSITIVE_POPULARITY_RATIO
        ):
            warnings.append(
                "wikidata_source_low_positive_popularity_coverage:"
                + f"{wikidata_quality.positive_popularity_ratio:.4f}"
            )
    if geonames_location_count == 0:
        warnings.append("GeoNames snapshot produced no locations.")
    logical_source_count = 3
    if wikidata_url is None:
        if (
            wikidata_entities_url
            and normalize_source_url(wikidata_entities_url)
            != normalize_source_url(wikidata_truthy_url or "")
        ):
            logical_source_count += 1
        if wikidata_seed_url:
            logical_source_count += 1

    return GeneralSourceFetchResult(
        pack_id="general-en",
        output_dir=str(resolved_output_dir),
        snapshot=resolved_snapshot,
        snapshot_dir=str(snapshot_dir),
        wikidata_url=resolved_wikidata_url,
        geonames_places_url=resolved_geonames_url,
        source_manifest_path=str(manifest_path),
        wikidata_entities_path=str(wikidata_entities_path),
        geonames_places_path=str(geonames_places_path),
        curated_entities_path=str(curated_entities_path),
        generated_at=generated_at,
        source_count=logical_source_count,
        wikidata_entity_count=wikidata_entity_count,
        geonames_location_count=geonames_location_count,
        curated_entity_count=len(DEFAULT_CURATED_GENERAL_ENTITIES),
        wikidata_entities_sha256=_sha256_file(wikidata_entities_path),
        geonames_places_sha256=_sha256_file(geonames_places_path),
        curated_entities_sha256=_sha256_file(curated_entities_path),
        warnings=warnings,
    )


def _resolve_snapshot(snapshot: str | None) -> str:
    if snapshot is None:
        return date.today().isoformat()
    return date.fromisoformat(snapshot).isoformat()


def _summarize_wikidata_source_quality(path: Path) -> _WikidataSourceQualitySummary:
    record_count = 0
    alias_backed_count = 0
    positive_popularity_count = 0
    if path.suffix.casefold() == ".jsonl":
        entities: Iterator[dict[str, object]] = _iter_wikidata_jsonl_entities(path)
    else:
        payload = _load_wikidata_payload(path)
        entities = iter(_normalize_wikidata_seed_entities(payload))
    for entity in entities:
        record_count += 1
        aliases = entity.get("aliases")
        if isinstance(aliases, list) and any(
            isinstance(alias, str) and _clean_text(alias) for alias in aliases
        ):
            alias_backed_count += 1
        popularity = _coerce_int(entity.get("popularity"))
        if popularity is None:
            source_features = entity.get("source_features")
            if isinstance(source_features, dict):
                popularity = _coerce_int(source_features.get("sitelink_count"))
        if popularity is not None and popularity > 0:
            positive_popularity_count += 1
    return _WikidataSourceQualitySummary(
        record_count=record_count,
        alias_backed_count=alias_backed_count,
        positive_popularity_count=positive_popularity_count,
    )


def _validate_wikidata_source_quality(
    summary: _WikidataSourceQualitySummary,
    *,
    source_url: str,
) -> None:
    if summary.record_count < _WIKIDATA_SOURCE_QUALITY_GATE_MIN_RECORDS:
        return
    if (
        summary.positive_popularity_ratio
        < _WIKIDATA_SOURCE_QUALITY_MIN_POSITIVE_POPULARITY_RATIO
    ):
        raise ValueError(
            "Wikidata general snapshot quality is too weak for production: "
            + f"only {summary.positive_popularity_count}/{summary.record_count} "
            + "entities carry positive popularity or sitelink support. "
            + "This usually means the full Wikidata entity JSON dump was not used "
            + f"or the staged source is degraded: {source_url}"
        )


def _download_wikidata_snapshot(
    *,
    wikidata_url: str | None,
    wikidata_truthy_url: str | None,
    wikidata_entities_url: str | None,
    wikidata_seed_url: str | None,
    snapshot_dir: Path,
    user_agent: str,
) -> tuple[str, Path, int, list[dict[str, object]]]:
    if wikidata_url:
        return _download_wikidata_seed_snapshot(
            wikidata_url,
            snapshot_dir=snapshot_dir,
            user_agent=user_agent,
        )
    if not wikidata_truthy_url:
        raise ValueError("Provide either wikidata_url or wikidata_truthy_url.")
    return _download_wikidata_dump_snapshot(
        truthy_url=wikidata_truthy_url,
        entities_url=wikidata_entities_url,
        seed_url=wikidata_seed_url,
        snapshot_dir=snapshot_dir,
        user_agent=user_agent,
    )


def _download_wikidata_seed_snapshot(
    source_url: str | Path,
    *,
    snapshot_dir: Path,
    user_agent: str,
) -> tuple[str, Path, int, list[dict[str, object]]]:
    normalized_source_url = normalize_source_url(source_url)
    raw_path = snapshot_dir / "wikidata_general_entities.source.json"
    _download_source_file(normalized_source_url, raw_path, user_agent=user_agent)
    payload = _load_wikidata_payload(raw_path)
    entities = _normalize_wikidata_seed_entities(payload)
    formatted_path = snapshot_dir / "wikidata_general_entities.json"
    formatted_path.write_text(
        json.dumps({"entities": entities}, indent=2) + "\n",
        encoding="utf-8",
    )
    return normalized_source_url, formatted_path, len(entities), [
        _normalized_source_manifest_entry(
            name="wikidata-general-entities",
            source_url=normalized_source_url,
            raw_path=raw_path,
            formatted_path=formatted_path,
            record_count=len(entities),
        )
    ]


def _download_wikidata_dump_snapshot(
    *,
    truthy_url: str | Path,
    entities_url: str | Path,
    seed_url: str | Path | None,
    snapshot_dir: Path,
    user_agent: str,
) -> tuple[str, Path, int, list[dict[str, object]]]:
    normalized_truthy_url = normalize_source_url(truthy_url)
    normalized_entities_url = (
        normalize_source_url(entities_url)
        if entities_url
        else None
    )
    truthy_raw_path = snapshot_dir / _download_target_name(
        "wikidata_truthy.source",
        normalized_truthy_url,
    )
    _download_source_file(normalized_truthy_url, truthy_raw_path, user_agent=user_agent)
    stage_db_path = snapshot_dir / ".wikidata_truthy.stage.sqlite"
    using_entity_dump = (
        normalized_entities_url is not None and normalized_entities_url != normalized_truthy_url
    )
    _collect_wikidata_truthy_work_data(
        truthy_raw_path,
        work_db_path=stage_db_path,
        allow_legacy_root_mismatch=using_entity_dump,
    )
    type_matches = _load_wikidata_truthy_type_matches(stage_db_path)
    formatted_path = snapshot_dir / "wikidata_general_entities.jsonl"
    source_entries: list[dict[str, object]] = [
        _raw_source_manifest_entry(
            name="wikidata-truthy-dump",
            source_url=normalized_truthy_url,
            path=truthy_raw_path,
        )
    ]

    if normalized_entities_url and normalized_entities_url != normalized_truthy_url:
        entities_raw_path = snapshot_dir / _download_target_name(
            "wikidata_entities.source",
            normalized_entities_url,
        )
        _download_source_file(
            normalized_entities_url,
            entities_raw_path,
            user_agent=user_agent,
        )
        _sha256_file(entities_raw_path)
        entity_count = _write_selected_wikidata_entity_dump_entities(
            entities_raw_path,
            output_path=formatted_path,
            type_matches=type_matches,
        )
        entity_count = _append_missing_wikidata_truthy_candidates(
            formatted_path,
            work_db_path=stage_db_path,
        )
        source_entries.append(
            _raw_source_manifest_entry(
                name="wikidata-entity-dump",
                source_url=normalized_entities_url,
                path=entities_raw_path,
            )
        )
        normalized_source_url = normalized_entities_url
        normalized_raw_path = entities_raw_path
    else:
        entity_count = _write_selected_wikidata_truthy_entities(
            truthy_raw_path,
            output_path=formatted_path,
            work_db_path=stage_db_path,
        )
        normalized_source_url = normalized_truthy_url
        normalized_raw_path = truthy_raw_path

    seed_entities: list[dict[str, object]] = []
    if seed_url:
        normalized_seed_url = normalize_source_url(seed_url)
        seed_raw_path = snapshot_dir / _download_target_name(
            "wikidata_seed_entities.source",
            normalized_seed_url,
        )
        _download_source_file(normalized_seed_url, seed_raw_path, user_agent=user_agent)
        seed_payload = _load_wikidata_payload(seed_raw_path)
        seed_entities = _normalize_wikidata_seed_entities(seed_payload)
        source_entries.append(
            _raw_source_manifest_entry(
                name="wikidata-seed-entities",
                source_url=normalized_seed_url,
                path=seed_raw_path,
            )
        )
    entity_count = _merge_wikidata_truthy_jsonl_with_seed_entities(
        formatted_path,
        seed_entities=seed_entities,
    )
    source_entries.append(
        _normalized_source_manifest_entry(
            name="wikidata-general-entities",
            source_url=normalized_source_url,
            raw_path=normalized_raw_path,
            formatted_path=formatted_path,
            record_count=entity_count,
        )
    )
    return normalized_source_url, formatted_path, entity_count, source_entries


def _download_geonames_snapshot(
    *,
    geonames_places_url: str | Path,
    geonames_alternate_names_url: str | Path | None,
    geonames_modifications_url: str | Path | None,
    geonames_deletes_url: str | Path | None,
    geonames_alternate_modifications_url: str | Path | None,
    geonames_alternate_deletes_url: str | Path | None,
    snapshot_dir: Path,
    user_agent: str,
) -> tuple[str, Path, int, list[dict[str, object]]]:
    normalized_places_url = normalize_source_url(geonames_places_url)
    places_raw_path = snapshot_dir / _download_target_name(
        "geonames_places.source",
        normalized_places_url,
    )
    _download_source_file(normalized_places_url, places_raw_path, user_agent=user_agent)
    place_rows = {row["geonameid"]: row for row in _load_geonames_rows(places_raw_path)}
    source_entries = [
        _raw_source_manifest_entry(
            name="geonames-places-raw",
            source_url=normalized_places_url,
            path=places_raw_path,
        )
    ]

    alternate_records_by_id: dict[str, dict[str, str]] = {}
    if geonames_alternate_names_url:
        normalized_alternate_url = normalize_source_url(geonames_alternate_names_url)
        alternate_raw_path = snapshot_dir / _download_target_name(
            "geonames_alternate_names.source",
            normalized_alternate_url,
        )
        _download_source_file(
            normalized_alternate_url,
            alternate_raw_path,
            user_agent=user_agent,
        )
        alternate_records_by_id = _load_geonames_alternate_name_records(alternate_raw_path)
        source_entries.append(
            _raw_source_manifest_entry(
                name="geonames-alternate-names-raw",
                source_url=normalized_alternate_url,
                path=alternate_raw_path,
            )
        )

    _apply_geonames_place_delta(
        geonames_modifications_url,
        snapshot_dir=snapshot_dir,
        user_agent=user_agent,
        records_by_id=place_rows,
        source_entries=source_entries,
        source_name="geonames-modifications-raw",
        destination_stub="geonames_modifications.source",
    )
    _apply_geonames_place_delete_delta(
        geonames_deletes_url,
        snapshot_dir=snapshot_dir,
        user_agent=user_agent,
        records_by_id=place_rows,
        source_entries=source_entries,
        source_name="geonames-deletes-raw",
        destination_stub="geonames_deletes.source",
    )
    _apply_geonames_alternate_delta(
        geonames_alternate_modifications_url,
        snapshot_dir=snapshot_dir,
        user_agent=user_agent,
        records_by_id=alternate_records_by_id,
        source_entries=source_entries,
        source_name="geonames-alternate-modifications-raw",
        destination_stub="geonames_alternate_modifications.source",
    )
    _apply_geonames_alternate_delete_delta(
        geonames_alternate_deletes_url,
        snapshot_dir=snapshot_dir,
        user_agent=user_agent,
        records_by_id=alternate_records_by_id,
        source_entries=source_entries,
        source_name="geonames-alternate-deletes-raw",
        destination_stub="geonames_alternate_deletes.source",
    )

    names_by_geonameid = _build_geonames_alternate_name_index(alternate_records_by_id)
    filtered_rows = []
    for row in place_rows.values():
        merged_row = dict(row)
        merged_row["alternatenames"] = _merge_geonames_aliases(
            row["alternatenames"],
            names_by_geonameid.get(row["geonameid"], []),
        )
        if _should_keep_geonames_row(merged_row):
            filtered_rows.append(merged_row)
    filtered_rows.sort(key=lambda item: int(item["geonameid"]))

    formatted_path = snapshot_dir / "geonames_places.txt"
    formatted_path.write_text(
        _GEONAMES_OUTPUT_HEADER
        + "\n"
        + "".join(
            "|".join(
                [
                    row["geonameid"],
                    row["name"],
                    row["alternatenames"],
                    row["feature_class"],
                    row["feature_code"],
                    row["country_code"],
                    row["population"],
                ]
            )
            + "\n"
            for row in filtered_rows
        ),
        encoding="utf-8",
    )
    source_entries.append(
        _normalized_source_manifest_entry(
            name="geonames-places",
            source_url=normalized_places_url,
            raw_path=places_raw_path,
            formatted_path=formatted_path,
            record_count=len(filtered_rows),
        )
    )
    return normalized_places_url, formatted_path, len(filtered_rows), source_entries


def _apply_geonames_place_delta(
    source_url: str | Path | None,
    *,
    snapshot_dir: Path,
    user_agent: str,
    records_by_id: dict[str, dict[str, str]],
    source_entries: list[dict[str, object]],
    source_name: str,
    destination_stub: str,
) -> None:
    if not source_url:
        return
    normalized_source_url = normalize_source_url(source_url)
    raw_path = snapshot_dir / _download_target_name(destination_stub, normalized_source_url)
    _download_source_file(normalized_source_url, raw_path, user_agent=user_agent)
    for row in _load_geonames_rows(raw_path):
        records_by_id[row["geonameid"]] = row
    source_entries.append(
        _raw_source_manifest_entry(
            name=source_name,
            source_url=normalized_source_url,
            path=raw_path,
        )
    )


def _apply_geonames_place_delete_delta(
    source_url: str | Path | None,
    *,
    snapshot_dir: Path,
    user_agent: str,
    records_by_id: dict[str, dict[str, str]],
    source_entries: list[dict[str, object]],
    source_name: str,
    destination_stub: str,
) -> None:
    if not source_url:
        return
    normalized_source_url = normalize_source_url(source_url)
    raw_path = snapshot_dir / _download_target_name(destination_stub, normalized_source_url)
    _download_source_file(normalized_source_url, raw_path, user_agent=user_agent)
    for identifier in _load_single_column_ids(raw_path):
        records_by_id.pop(identifier, None)
    source_entries.append(
        _raw_source_manifest_entry(
            name=source_name,
            source_url=normalized_source_url,
            path=raw_path,
        )
    )


def _apply_geonames_alternate_delta(
    source_url: str | Path | None,
    *,
    snapshot_dir: Path,
    user_agent: str,
    records_by_id: dict[str, dict[str, str]],
    source_entries: list[dict[str, object]],
    source_name: str,
    destination_stub: str,
) -> None:
    if not source_url:
        return
    normalized_source_url = normalize_source_url(source_url)
    raw_path = snapshot_dir / _download_target_name(destination_stub, normalized_source_url)
    _download_source_file(normalized_source_url, raw_path, user_agent=user_agent)
    records_by_id.update(_load_geonames_alternate_name_records(raw_path))
    source_entries.append(
        _raw_source_manifest_entry(
            name=source_name,
            source_url=normalized_source_url,
            path=raw_path,
        )
    )


def _apply_geonames_alternate_delete_delta(
    source_url: str | Path | None,
    *,
    snapshot_dir: Path,
    user_agent: str,
    records_by_id: dict[str, dict[str, str]],
    source_entries: list[dict[str, object]],
    source_name: str,
    destination_stub: str,
) -> None:
    if not source_url:
        return
    normalized_source_url = normalize_source_url(source_url)
    raw_path = snapshot_dir / _download_target_name(destination_stub, normalized_source_url)
    _download_source_file(normalized_source_url, raw_path, user_agent=user_agent)
    for identifier in _load_single_column_ids(raw_path):
        records_by_id.pop(identifier, None)
    source_entries.append(
        _raw_source_manifest_entry(
            name=source_name,
            source_url=normalized_source_url,
            path=raw_path,
        )
    )


def _write_curated_entities(path: Path) -> None:
    payload = {"entities": list(DEFAULT_CURATED_GENERAL_ENTITIES)}
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_source_manifest(
    *,
    path: Path,
    snapshot: str,
    generated_at: str,
    user_agent: str,
    source_entries: list[dict[str, object]],
) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": "general-en",
                "snapshot": snapshot,
                "generated_at": generated_at,
                "user_agent": user_agent,
                "sources": source_entries,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _raw_source_manifest_entry(*, name: str, source_url: str, path: Path) -> dict[str, object]:
    fingerprint = _source_manifest_fingerprint(path)
    payload: dict[str, object] = {
        "name": name,
        "source_url": source_url,
        "path": path.name,
        "size_bytes": path.stat().st_size,
        "content_signature": fingerprint["content_signature"],
    }
    sha256_value = fingerprint.get("sha256")
    if isinstance(sha256_value, str) and sha256_value:
        payload["sha256"] = sha256_value
    return payload


def _normalized_source_manifest_entry(
    *,
    name: str,
    source_url: str,
    raw_path: Path,
    formatted_path: Path,
    record_count: int,
) -> dict[str, object]:
    raw_fingerprint = _source_manifest_fingerprint(raw_path)
    payload: dict[str, object] = {
        "name": name,
        "source_url": source_url,
        "path": formatted_path.name,
        "sha256": _sha256_file(formatted_path),
        "size_bytes": formatted_path.stat().st_size,
        "record_count": record_count,
        "raw_path": raw_path.name,
        "raw_size_bytes": raw_path.stat().st_size,
        "raw_content_signature": raw_fingerprint["content_signature"],
    }
    raw_sha256_value = raw_fingerprint.get("sha256")
    if isinstance(raw_sha256_value, str) and raw_sha256_value:
        payload["raw_sha256"] = raw_sha256_value
    return payload


def _parse_wikidata_nt_terms(line: str) -> tuple[str, str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    parts = stripped.split(" ", 2)
    if len(parts) < 3:
        return None
    subject_term, predicate_term, object_fragment = parts
    if not object_fragment.endswith(" ."):
        return None
    return subject_term, predicate_term, object_fragment[:-2]


def _iter_selected_wikidata_truthy_entities(path: Path) -> Iterator[dict[str, object]]:
    with tempfile.TemporaryDirectory(
        prefix="ades-wikidata-truthy-",
        dir=str(path.parent),
    ) as temp_dir:
        temp_output_path = Path(temp_dir) / "wikidata_general_entities.jsonl"
        _write_selected_wikidata_truthy_entities(path, output_path=temp_output_path)
        with temp_output_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                cleaned = line.strip()
                if not cleaned:
                    continue
                payload = json.loads(cleaned)
                if isinstance(payload, dict):
                    yield payload


def _load_selected_wikidata_truthy_entities(path: Path) -> list[dict[str, object]]:
    return list(_iter_selected_wikidata_truthy_entities(path))


def _write_selected_wikidata_truthy_entities(
    path: Path,
    *,
    output_path: Path,
    work_db_path: Path | None = None,
) -> int:
    temporary_work_db_path = (
        output_path.with_name(f".{output_path.name}.work.tmp.sqlite")
        if work_db_path is None
        else None
    )
    resolved_work_db_path = work_db_path or temporary_work_db_path
    if resolved_work_db_path is None:
        raise ValueError("Resolved Wikidata truthy work DB path is required.")
    record_count = 0
    try:
        _collect_wikidata_truthy_work_data(
            path,
            work_db_path=resolved_work_db_path,
        )
        with output_path.open("w", encoding="utf-8") as handle:
            for entity in _iter_selected_wikidata_truthy_candidates(
                resolved_work_db_path
            ):
                handle.write(json.dumps(entity, sort_keys=True) + "\n")
                record_count += 1
    finally:
        if temporary_work_db_path is not None:
            temporary_work_db_path.unlink(missing_ok=True)
            temporary_work_db_path.with_suffix(
                f"{temporary_work_db_path.suffix}-wal"
            ).unlink(missing_ok=True)
            temporary_work_db_path.with_suffix(
                f"{temporary_work_db_path.suffix}-shm"
            ).unlink(missing_ok=True)
    return record_count


def _write_selected_wikidata_entity_dump_entities(
    path: Path,
    *,
    output_path: Path,
    type_matches: dict[str, tuple[int, str]],
) -> int:
    record_count = 0
    with output_path.open("w", encoding="utf-8") as handle:
        for entity in _iter_wikidata_dump_objects(path):
            normalized = _normalize_wikidata_dump_entity(
                entity,
                type_matches=type_matches,
            )
            if normalized is None:
                continue
            handle.write(json.dumps(normalized, sort_keys=True) + "\n")
            record_count += 1
    return record_count


def _collect_wikidata_truthy_work_data(
    path: Path,
    *,
    work_db_path: Path,
    allow_legacy_root_mismatch: bool = False,
) -> None:
    source_signature = _wikidata_truthy_stage_source_signature(path)
    root_fingerprint = _wikidata_allowed_type_root_fingerprint()
    if work_db_path.exists() and _wikidata_truthy_stage_matches(
        work_db_path,
        source_sha256=source_signature,
        root_fingerprint=root_fingerprint,
    ):
        return
    reusable_stage_path = _find_reusable_wikidata_truthy_stage(
        work_db_path,
        source_sha256=source_signature,
        root_fingerprint=root_fingerprint,
        allow_legacy_root_mismatch=allow_legacy_root_mismatch,
    )
    if reusable_stage_path is not None:
        _promote_wikidata_truthy_stage(
            reusable_stage_path,
            work_db_path=work_db_path,
            source_sha256=source_signature,
            root_fingerprint=root_fingerprint,
        )
        if _wikidata_truthy_stage_matches(
            work_db_path,
            source_sha256=source_signature,
            root_fingerprint=root_fingerprint,
        ):
            return
    if work_db_path.exists():
        work_db_path.unlink()
    work_db_path.with_suffix(f"{work_db_path.suffix}-wal").unlink(missing_ok=True)
    work_db_path.with_suffix(f"{work_db_path.suffix}-shm").unlink(missing_ok=True)
    connection = sqlite3.connect(str(work_db_path))
    try:
        _initialize_wikidata_truthy_work_store(
            connection,
            source_sha256=source_signature,
            root_fingerprint=root_fingerprint,
        )
        current_state: _WikidataTruthEntityState | None = None
        current_article_state: _WikidataTruthArticleState | None = None
        edge_batch: list[tuple[str, str]] = []
        segment_batch: list[tuple[str, str | None, str, int | None, str]] = []

        def flush_edges() -> None:
            nonlocal edge_batch
            if not edge_batch:
                return
            connection.executemany(
                """
                INSERT OR IGNORE INTO subclass_edges(subject_id, parent_id)
                VALUES (?, ?)
                """,
                edge_batch,
            )
            edge_batch = []

        def flush_segments() -> None:
            nonlocal segment_batch
            if not segment_batch:
                return
            connection.executemany(
                """
                INSERT INTO entity_segments(
                    entity_id,
                    label,
                    aliases_json,
                    sitelinks,
                    type_ids_json
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                segment_batch,
            )
            segment_batch = []

        def flush_current() -> None:
            nonlocal current_state
            if current_state is None:
                return
            segment_row = _build_wikidata_truthy_segment_row(current_state)
            current_state = None
            if segment_row is None:
                return
            segment_batch.append(segment_row)
            if len(segment_batch) >= 50_000:
                flush_segments()

        def flush_article() -> None:
            nonlocal current_article_state
            if current_article_state is None:
                return
            article_entity_id = current_article_state.entity_id
            article_aliases = _dedupe_preserving_order(current_article_state.aliases)
            current_article_state = None
            if not article_entity_id or not article_aliases:
                return
            segment_batch.append(
                (
                    article_entity_id,
                    None,
                    json.dumps(article_aliases, sort_keys=True),
                    None,
                    json.dumps([], sort_keys=True),
                )
            )
            if len(segment_batch) >= 50_000:
                flush_segments()

        for line in _iter_wikidata_truthy_relevant_lines(path):
            triple = _parse_wikidata_nt_terms(line)
            if triple is None:
                continue
            subject_term, predicate_term, object_term = triple
            if predicate_term not in _WIKIDATA_RELEVANT_PREDICATE_TERMS:
                continue
            subject_id = _extract_wikidata_entity_id(subject_term)
            if subject_id:
                flush_article()
                if current_state is None or current_state.entity_id != subject_id:
                    flush_current()
                    current_state = _WikidataTruthEntityState(entity_id=subject_id)

                if predicate_term == _WIKIDATA_DIRECT_P279_TERM:
                    object_id = _extract_wikidata_entity_id(object_term)
                    if object_id:
                        edge_batch.append((subject_id, object_id))
                        if len(edge_batch) >= 100_000:
                            flush_edges()
                    continue
                if predicate_term == _WIKIDATA_DIRECT_P31_TERM:
                    object_id = _extract_wikidata_entity_id(object_term)
                    if object_id:
                        current_state.type_ids.add(object_id)
                    continue
                if predicate_term in _WIKIDATA_LABEL_PREDICATE_TERMS:
                    label = _extract_wikidata_language_literal(object_term, language="en")
                    if label and not current_state.label:
                        current_state.label = label
                    continue
                if predicate_term == _WIKIDATA_ALT_LABEL_PREDICATE_TERM:
                    alias = _extract_wikidata_language_literal(object_term, language="en")
                    if alias:
                        current_state.aliases.append(alias)
                    continue
                if predicate_term == _WIKIDATA_SITELINKS_PREDICATE_TERM:
                    sitelinks = _extract_wikidata_integer_literal(object_term)
                    if sitelinks is not None:
                        current_state.sitelinks = sitelinks
                continue

            flush_current()
            if current_article_state is None or current_article_state.subject_term != subject_term:
                flush_article()
                current_article_state = _WikidataTruthArticleState(
                    subject_term=subject_term
                )
            if predicate_term == _WIKIDATA_SCHEMA_ABOUT_PREDICATE_TERM:
                current_article_state.entity_id = _extract_wikidata_entity_id(object_term)
                continue
            if predicate_term == f"<{_WIKIDATA_SCHEMA_NAME_PREDICATE}>":
                alias = _extract_wikidata_language_literal(object_term, language="en")
                if alias:
                    current_article_state.aliases.append(alias)

        flush_current()
        flush_article()
        flush_segments()
        flush_edges()
        connection.commit()
        _finalize_wikidata_truthy_work_store(connection)
        connection.commit()
    finally:
        connection.close()


def _wikidata_allowed_type_root_fingerprint() -> str:
    return sha256(
        json.dumps(
            {
                "allowed_type_roots": DEFAULT_WIKIDATA_ALLOWED_TYPE_ROOTS,
                "stage_logic_version": _WIKIDATA_TRUTHY_STAGE_LOGIC_VERSION,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _wikidata_truthy_stage_matches(
    work_db_path: Path,
    *,
    source_sha256: str,
    root_fingerprint: str,
) -> bool:
    info = _read_wikidata_truthy_stage_info(work_db_path)
    if info is None:
        return False
    return (
        info.schema_version == _WIKIDATA_TRUTHY_STAGE_SCHEMA_VERSION
        and info.source_sha256 == source_sha256
        and info.root_fingerprint == root_fingerprint
        and info.has_required_tables
        and info.is_complete
    )


def _read_wikidata_truthy_stage_info(
    work_db_path: Path,
) -> _WikidataTruthyStageInfo | None:
    try:
        connection = sqlite3.connect(f"file:{work_db_path}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if "stage_metadata" not in tables:
            return None
        row = connection.execute(
            """
            SELECT schema_version, source_sha256, root_fingerprint
            FROM stage_metadata
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        schema_version, stored_source_sha256, stored_root_fingerprint = row
        has_required_tables = {
            "subclass_edges",
            "entity_segments",
        }.issubset(tables)
        if has_required_tables:
            # Only reuse a stage DB if the materialized content tables are readable.
            connection.execute("SELECT 1 FROM subclass_edges LIMIT 1").fetchone()
            connection.execute("SELECT 1 FROM entity_segments LIMIT 1").fetchone()
        completion_row = None
        if "stage_completion" in tables:
            completion_row = connection.execute(
                """
                SELECT is_complete
                FROM stage_completion
                LIMIT 1
                """
            ).fetchone()
        is_complete = completion_row is not None and completion_row[0] == 1
    except sqlite3.Error:
        return None
    finally:
        connection.close()
    return _WikidataTruthyStageInfo(
        schema_version=_coerce_int(schema_version),
        source_sha256=stored_source_sha256,
        root_fingerprint=stored_root_fingerprint,
        has_required_tables=has_required_tables,
        is_complete=is_complete,
    )


def _find_reusable_wikidata_truthy_stage(
    work_db_path: Path,
    *,
    source_sha256: str,
    root_fingerprint: str,
    allow_legacy_root_mismatch: bool = False,
) -> Path | None:
    raw_root = work_db_path.parents[2] if len(work_db_path.parents) >= 3 else None
    if raw_root is None or not raw_root.exists():
        return None
    best_candidate: Path | None = None
    best_sort_key: tuple[int, int, float] | None = None
    for candidate in raw_root.rglob(".wikidata_truthy.stage.sqlite"):
        if candidate == work_db_path:
            continue
        info = _read_wikidata_truthy_stage_info(candidate)
        if info is None:
            continue
        if (
            info.source_sha256 != source_sha256
            or not info.has_required_tables
        ):
            continue
        if info.schema_version == _WIKIDATA_TRUTHY_STAGE_SCHEMA_VERSION and info.is_complete:
            if info.root_fingerprint != root_fingerprint:
                continue
            candidate_rank = 2
        elif info.schema_version in {1, 2}:
            if (
                info.root_fingerprint != root_fingerprint
                and not allow_legacy_root_mismatch
            ):
                continue
            candidate_rank = 1
        else:
            continue
        stat = candidate.stat()
        sort_key = (candidate_rank, int(stat.st_size), stat.st_mtime)
        if best_sort_key is None or sort_key > best_sort_key:
            best_candidate = candidate
            best_sort_key = sort_key
    return best_candidate


def _promote_wikidata_truthy_stage(
    source_stage_path: Path,
    *,
    work_db_path: Path,
    source_sha256: str,
    root_fingerprint: str,
) -> None:
    if work_db_path.exists():
        work_db_path.unlink()
    work_db_path.with_suffix(f"{work_db_path.suffix}-wal").unlink(missing_ok=True)
    work_db_path.with_suffix(f"{work_db_path.suffix}-shm").unlink(missing_ok=True)
    shutil.copy2(source_stage_path, work_db_path)
    connection = sqlite3.connect(str(work_db_path))
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if "stage_metadata" not in tables:
            raise sqlite3.OperationalError("stage_metadata table is missing")
        if "stage_completion" not in tables:
            connection.execute(
                """
                CREATE TABLE stage_completion (
                    is_complete INTEGER NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO stage_completion(is_complete)
                VALUES (0)
                """
            )
        else:
            connection.execute("DELETE FROM stage_completion")
            connection.execute(
                """
                INSERT INTO stage_completion(is_complete)
                VALUES (0)
                """
            )
        connection.execute(
            """
            UPDATE stage_metadata
            SET schema_version = ?, source_sha256 = ?, root_fingerprint = ?
            """,
            (
                _WIKIDATA_TRUTHY_STAGE_SCHEMA_VERSION,
                source_sha256,
                root_fingerprint,
            ),
        )
        _finalize_wikidata_truthy_work_store(connection)
        connection.commit()
    finally:
        connection.close()


def _initialize_wikidata_truthy_work_store(
    connection: sqlite3.Connection,
    *,
    source_sha256: str,
    root_fingerprint: str,
) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        PRAGMA temp_store=MEMORY;
        PRAGMA cache_size=-200000;
        CREATE TABLE IF NOT EXISTS stage_metadata (
            schema_version INTEGER NOT NULL,
            source_sha256 TEXT NOT NULL,
            root_fingerprint TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stage_completion (
            is_complete INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS subclass_edges (
            subject_id TEXT NOT NULL,
            parent_id TEXT NOT NULL,
            PRIMARY KEY(subject_id, parent_id)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS allowed_roots (
            root_id TEXT PRIMARY KEY,
            priority INTEGER NOT NULL,
            entity_type TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS entity_segments (
            entity_id TEXT NOT NULL,
            label TEXT,
            aliases_json TEXT NOT NULL,
            sitelinks INTEGER,
            type_ids_json TEXT NOT NULL
        );
        DELETE FROM stage_metadata;
        DELETE FROM stage_completion;
        DELETE FROM subclass_edges;
        DELETE FROM allowed_roots;
        DELETE FROM entity_segments;
        """
    )
    connection.execute(
        """
        INSERT INTO stage_metadata(schema_version, source_sha256, root_fingerprint)
        VALUES (?, ?, ?)
        """,
        (
            _WIKIDATA_TRUTHY_STAGE_SCHEMA_VERSION,
            source_sha256,
            root_fingerprint,
        ),
    )
    connection.execute(
        """
        INSERT INTO stage_completion(is_complete)
        VALUES (0)
        """
    )
    connection.executemany(
        """
        INSERT INTO allowed_roots(root_id, priority, entity_type)
        VALUES (?, ?, ?)
        """,
        [
            (root_id, index, entity_type)
            for index, (root_id, entity_type) in enumerate(
                DEFAULT_WIKIDATA_ALLOWED_TYPE_ROOTS.items()
            )
        ],
    )
    connection.commit()


def _finalize_wikidata_truthy_work_store(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_subclass_edges_parent_id
        ON subclass_edges(parent_id);
        CREATE INDEX IF NOT EXISTS idx_entity_segments_entity_id
        ON entity_segments(entity_id);
        """
    )
    connection.execute("UPDATE stage_completion SET is_complete = 1")


def _build_wikidata_truthy_segment_row(
    current_state: _WikidataTruthEntityState,
) -> tuple[str, str | None, str, int | None, str] | None:
    if (
        not current_state.label
        and not current_state.type_ids
        and not current_state.aliases
        and current_state.sitelinks is None
    ):
        return None
    return (
        current_state.entity_id,
        current_state.label,
        json.dumps(_dedupe_preserving_order(current_state.aliases), sort_keys=True),
        current_state.sitelinks,
        json.dumps(sorted(current_state.type_ids), sort_keys=True),
    )


def _load_wikidata_truthy_type_matches(
    work_db_path: Path,
) -> dict[str, tuple[int, str]]:
    connection = sqlite3.connect(str(work_db_path))
    try:
        return {
            type_id: (priority, entity_type)
            for type_id, priority, entity_type in _iter_wikidata_truthy_type_match_rows(
                connection
            )
        }
    finally:
        connection.close()


def _iter_selected_wikidata_truthy_candidates(
    work_db_path: Path,
) -> Iterator[dict[str, object]]:
    connection = sqlite3.connect(str(work_db_path))
    try:
        _prepare_wikidata_truthy_type_matches_temp_table(connection)
        _prepare_wikidata_truthy_resolved_entity_types_temp_table(connection)
        current_entity_id: str | None = None
        label: str | None = None
        aliases: list[str] = []
        sitelinks: int | None = None
        resolved_entity_type: str | None = None
        type_ids: set[str] = set()

        def flush_current() -> dict[str, object] | None:
            nonlocal current_entity_id, label, aliases, sitelinks, resolved_entity_type, type_ids
            if current_entity_id is None:
                return None
            if label is None or resolved_entity_type is None:
                current_entity_id = None
                label = None
                aliases = []
                sitelinks = None
                resolved_entity_type = None
                type_ids = set()
                return None
            minimum_sitelinks = DEFAULT_WIKIDATA_MIN_SITELINKS_BY_ENTITY_TYPE.get(
                resolved_entity_type,
                0,
            )
            retained_aliases = _dedupe_preserving_order(
                [
                    alias
                    for alias in aliases
                    if alias.casefold() != label.casefold()
                ]
            )
            candidate_sitelinks = sitelinks or 0
            payload = None
            if _should_keep_wikidata_truthy_candidate(
                entity_type=resolved_entity_type,
                label=label,
                aliases=retained_aliases,
                sitelinks=sitelinks,
                minimum_sitelinks=minimum_sitelinks,
            ):
                payload = {
                    "id": current_entity_id,
                    "entity_type": resolved_entity_type,
                    "label": label,
                    "aliases": retained_aliases,
                    "popularity": candidate_sitelinks,
                    "type_ids": sorted(type_ids),
                    "source_features": {
                        "sitelink_count": candidate_sitelinks,
                        "alias_count": len(retained_aliases),
                        "popularity_signal": (
                            "sitelinks" if sitelinks is not None else "fallback"
                        ),
                    },
                }
            current_entity_id = None
            label = None
            aliases = []
            sitelinks = None
            resolved_entity_type = None
            type_ids = set()
            return payload

        for (
            entity_id,
            raw_label,
            aliases_json,
            raw_sitelinks,
            type_ids_json,
            entity_type,
        ) in connection.execute(
            """
            SELECT
                entity_segments.entity_id,
                entity_segments.label,
                entity_segments.aliases_json,
                entity_segments.sitelinks,
                entity_segments.type_ids_json,
                resolved_entity_types.entity_type
            FROM entity_segments
            JOIN resolved_entity_types
              ON resolved_entity_types.entity_id = entity_segments.entity_id
            ORDER BY entity_segments.entity_id ASC
            """
        ):
            if current_entity_id is None:
                current_entity_id = entity_id
            elif entity_id != current_entity_id:
                entity = flush_current()
                if entity is not None:
                    yield entity
                current_entity_id = entity_id
            cleaned_label = _clean_text(raw_label)
            if cleaned_label and label is None:
                label = cleaned_label
            parsed_aliases = json.loads(aliases_json)
            if isinstance(parsed_aliases, list):
                aliases.extend(
                    alias for alias in parsed_aliases if isinstance(alias, str) and alias
                )
            parsed_type_ids = json.loads(type_ids_json)
            if isinstance(parsed_type_ids, list):
                type_ids.update(
                    type_id for type_id in parsed_type_ids if isinstance(type_id, str)
                )
            coerced_sitelinks = _coerce_int(raw_sitelinks)
            if coerced_sitelinks is not None:
                sitelinks = (
                    coerced_sitelinks
                    if sitelinks is None
                    else max(sitelinks, coerced_sitelinks)
                )
            if resolved_entity_type is None:
                resolved_entity_type = entity_type
        entity = flush_current()
        if entity is not None:
            yield entity
    finally:
        connection.close()


def _iter_wikidata_truthy_type_match_rows(
    connection: sqlite3.Connection,
) -> Iterator[tuple[str, int, str]]:
    seen_type_ids: set[str] = set()
    for priority, (root_id, entity_type) in enumerate(
        DEFAULT_WIKIDATA_ALLOWED_TYPE_ROOTS.items()
    ):
        frontier = [root_id]
        if root_id not in seen_type_ids:
            seen_type_ids.add(root_id)
            yield root_id, priority, entity_type
        while frontier:
            batch = frontier[:500]
            frontier = frontier[500:]
            placeholders = ",".join("?" for _ in batch)
            rows = connection.execute(
                f"""
                SELECT subject_id
                FROM subclass_edges
                WHERE parent_id IN ({placeholders})
                """,
                batch,
            ).fetchall()
            next_frontier: list[str] = []
            for (subject_id,) in rows:
                if not isinstance(subject_id, str) or subject_id in seen_type_ids:
                    continue
                seen_type_ids.add(subject_id)
                yield subject_id, priority, entity_type
                next_frontier.append(subject_id)
            frontier.extend(next_frontier)


def _prepare_wikidata_truthy_type_matches_temp_table(
    connection: sqlite3.Connection,
) -> None:
    connection.execute("DROP TABLE IF EXISTS type_matches")
    connection.execute(
        """
        CREATE TEMP TABLE type_matches (
            type_id TEXT PRIMARY KEY,
            priority INTEGER NOT NULL,
            entity_type TEXT NOT NULL
        ) WITHOUT ROWID
        """
    )
    batch: list[tuple[str, int, str]] = []
    for row in _iter_wikidata_truthy_type_match_rows(connection):
        batch.append(row)
        if len(batch) >= 10_000:
            connection.executemany(
                """
                INSERT INTO type_matches(type_id, priority, entity_type)
                VALUES (?, ?, ?)
                """,
                batch,
            )
            batch = []
    if batch:
        connection.executemany(
            """
            INSERT INTO type_matches(type_id, priority, entity_type)
            VALUES (?, ?, ?)
            """,
            batch,
        )


def _prepare_wikidata_truthy_resolved_entity_types_temp_table(
    connection: sqlite3.Connection,
) -> None:
    connection.execute("DROP TABLE IF EXISTS resolved_entity_types")
    connection.execute(
        """
        CREATE TEMP TABLE resolved_entity_types (
            entity_id TEXT PRIMARY KEY,
            priority INTEGER NOT NULL,
            entity_type TEXT NOT NULL
        ) WITHOUT ROWID
        """
    )
    connection.execute(
        """
        INSERT INTO resolved_entity_types(entity_id, priority, entity_type)
        SELECT entity_id, priority, entity_type
        FROM (
            SELECT
                entity_segments.entity_id AS entity_id,
                type_matches.priority AS priority,
                type_matches.entity_type AS entity_type,
                ROW_NUMBER() OVER (
                    PARTITION BY entity_segments.entity_id
                    ORDER BY type_matches.priority ASC
                ) AS candidate_rank
            FROM entity_segments
            JOIN json_each(entity_segments.type_ids_json)
              ON 1 = 1
            JOIN type_matches
              ON type_matches.type_id = json_each.value
        )
        WHERE candidate_rank = 1
        """
    )


def _resolve_wikidata_truthy_candidate(
    payload: dict[str, object],
    *,
    type_matches: dict[str, tuple[int, str]],
) -> dict[str, object] | None:
    entity_id = _clean_text(payload.get("entity_id"))
    label = _clean_text(payload.get("label"))
    if not entity_id or not label:
        return None
    raw_type_ids = payload.get("type_ids")
    type_ids = (
        {type_id for type_id in raw_type_ids if isinstance(type_id, str)}
        if isinstance(raw_type_ids, list)
        else set()
    )
    entity_type = _resolve_wikidata_entity_type(type_ids, type_matches=type_matches)
    if entity_type is None:
        return None
    sitelinks = _coerce_int(payload.get("sitelinks")) or 0
    if sitelinks < _WIKIDATA_MIN_CANDIDATE_SITELINKS:
        return None
    minimum_sitelinks = DEFAULT_WIKIDATA_MIN_SITELINKS_BY_ENTITY_TYPE.get(
        entity_type,
        0,
    )
    if sitelinks < minimum_sitelinks:
        return None
    raw_aliases = payload.get("aliases")
    aliases = raw_aliases if isinstance(raw_aliases, list) else []
    return {
        "id": entity_id,
        "entity_type": entity_type,
        "label": label,
        "aliases": _dedupe_preserving_order(
            [
                alias
                for alias in aliases
                if isinstance(alias, str) and alias and alias.casefold() != label.casefold()
            ]
        ),
        "popularity": sitelinks,
        "type_ids": sorted(type_ids),
        "source_features": {
            "sitelink_count": sitelinks,
        },
    }


def _extract_wikidata_entity_id(term: str) -> str | None:
    cleaned = term.strip().rstrip(".")
    if not cleaned.startswith("<") or not cleaned.endswith(">"):
        return None
    value = cleaned[1:-1]
    if value.startswith(_WIKIDATA_ENTITY_PREFIX):
        entity_id = value.removeprefix(_WIKIDATA_ENTITY_PREFIX)
    else:
        entity_id = None
        for prefix in _WIKIDATA_ENTITY_DATA_PREFIXES:
            if value.startswith(prefix):
                entity_id = value.removeprefix(prefix)
                break
        if entity_id is None:
            return None
    entity_id = entity_id.split("?", 1)[0].split("#", 1)[0]
    if not entity_id.startswith("Q"):
        return None
    return entity_id


def _extract_wikidata_property_id(term: str) -> str | None:
    value = _extract_wikidata_uri(term)
    if value is None:
        return None
    if not value.startswith(_WIKIDATA_DIRECT_PROPERTY_PREFIX):
        return None
    property_id = value.removeprefix(_WIKIDATA_DIRECT_PROPERTY_PREFIX)
    return property_id if property_id in _WIKIDATA_ALLOWED_PROPERTIES else None


def _extract_wikidata_uri(term: str) -> str | None:
    cleaned = term.strip().rstrip(".")
    if not cleaned.startswith("<") or not cleaned.endswith(">"):
        return None
    return cleaned[1:-1]


def _extract_wikidata_language_literal(term: str, *, language: str) -> str | None:
    cleaned = term.strip()
    suffix = f"@{language}"
    if not cleaned.endswith(suffix):
        return None
    literal = cleaned[: -len(suffix)]
    if not literal.startswith('"') or len(literal) < 2:
        return None
    try:
        return _clean_text(json.loads(literal))
    except json.JSONDecodeError:
        return None


def _extract_wikidata_integer_literal(term: str) -> int | None:
    cleaned = term.strip()
    literal, _, _datatype = cleaned.partition("^^")
    if not literal.startswith('"') or not literal.endswith('"'):
        return None
    try:
        return int(json.loads(literal))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _extract_wikidata_monolingual_value(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    return _clean_text(value.get("value"))


def _normalize_wikidata_seed_entities(payload: object) -> list[dict[str, object]]:
    if isinstance(payload, dict) and "entities" in payload:
        entity_payload = payload["entities"]
        if isinstance(entity_payload, dict):
            normalized: list[dict[str, object]] = []
            for entity_id, entity in entity_payload.items():
                if not isinstance(entity, dict):
                    continue
                normalized_entity = _normalize_wikidata_seed_entity(
                    entity,
                    fallback_entity_id=entity_id,
                )
                if normalized_entity is not None:
                    normalized.append(normalized_entity)
            return sorted(normalized, key=lambda item: str(item["id"]))
        if isinstance(entity_payload, list):
            normalized = [
                normalized_entity
                for normalized_entity in (
                    _normalize_wikidata_seed_entity(item)
                    for item in entity_payload
                    if isinstance(item, dict)
                )
                if normalized_entity is not None
            ]
            return sorted(normalized, key=lambda item: str(item["id"]))
    raise ValueError("Unsupported Wikidata general entity payload structure.")


def _normalize_wikidata_seed_entity(
    entity: dict[str, object],
    *,
    fallback_entity_id: str | None = None,
) -> dict[str, object] | None:
    entity_id = _clean_text(entity.get("id")) or _clean_text(fallback_entity_id)
    entity_type = _clean_text(entity.get("entity_type") or entity.get("type"))
    label = _clean_text(entity.get("label") or entity.get("canonical_text"))
    if not label and isinstance(entity.get("labels"), dict):
        label = _extract_wikidata_monolingual_value(entity["labels"].get("en")) or ""
    aliases: list[str] = []
    raw_aliases = entity.get("aliases")
    if isinstance(raw_aliases, list):
        aliases = [
            alias
            for alias in (_clean_text(item) for item in raw_aliases)
            if alias
        ]
    elif isinstance(raw_aliases, dict):
        aliases_payload = raw_aliases.get("en", [])
        if isinstance(aliases_payload, list):
            aliases = [
                alias
                for alias in (
                    _extract_wikidata_monolingual_value(item)
                    for item in aliases_payload
                )
                if alias
            ]
    if not entity_id or not label or not entity_type:
        return None
    if entity_type not in _GENERAL_ALLOWED_ENTITY_TYPES:
        return None
    return {
        "id": entity_id,
        "entity_type": entity_type,
        "label": label,
        "aliases": _dedupe_preserving_order(
            alias
            for alias in aliases
            if alias and alias.casefold() != label.casefold()
        ),
        "popularity": _coerce_int(entity.get("popularity")),
    }


def _normalize_wikidata_dump_entity(
    entity: dict[str, object],
    *,
    type_matches: dict[str, tuple[int, str]],
) -> dict[str, object] | None:
    entity_id = _clean_text(entity.get("id"))
    if not entity_id:
        return None
    labels = entity.get("labels")
    label = (
        _extract_wikidata_monolingual_value(labels.get("en"))
        if isinstance(labels, dict)
        else None
    )
    if not label:
        return None
    descriptions = entity.get("descriptions")
    description = (
        _extract_wikidata_monolingual_value(descriptions.get("en"))
        if isinstance(descriptions, dict)
        else None
    )
    type_ids = _extract_wikidata_claim_entity_ids(entity, "P31")
    entity_type = _resolve_wikidata_entity_type(type_ids, type_matches=type_matches)
    if entity_type is None:
        return None
    aliases = _extract_wikidata_entity_dump_aliases(entity, label=label)
    sitelinks = entity.get("sitelinks")
    sitelink_count = len(sitelinks) if isinstance(sitelinks, dict) else 0
    claims = entity.get("claims")
    statement_count = (
        sum(len(value) for value in claims.values() if isinstance(value, list))
        if isinstance(claims, dict)
        else 0
    )
    identifier_count = (
        sum(
            1
            for property_id, value in claims.items()
            if property_id not in {"P31", "P279"} and isinstance(value, list) and value
        )
        if isinstance(claims, dict)
        else 0
    )
    has_enwiki = bool(
        isinstance(sitelinks, dict) and sitelinks.get("enwiki") is not None
    )
    if not _should_keep_wikidata_dump_entity(
        entity_type=entity_type,
        label=label,
        aliases=aliases,
        sitelink_count=sitelink_count,
        has_enwiki=has_enwiki,
        identifier_count=identifier_count,
        statement_count=statement_count,
    ):
        return None
    return {
        "id": entity_id,
        "entity_type": entity_type,
        "label": label,
        "aliases": aliases,
        "popularity": sitelink_count,
        "description": description,
        "type_ids": sorted(type_ids),
        "source_features": {
            "alias_count": len(aliases),
            "identifier_count": identifier_count,
            "statement_count": statement_count,
            "sitelink_count": sitelink_count,
            "has_enwiki": has_enwiki,
            "popularity_signal": "sitelinks" if sitelink_count > 0 else "fallback",
        },
    }


def _should_keep_wikidata_dump_entity(
    *,
    entity_type: str,
    label: str,
    aliases: list[str],
    sitelink_count: int,
    has_enwiki: bool,
    identifier_count: int,
    statement_count: int,
) -> bool:
    minimum_sitelinks = DEFAULT_WIKIDATA_MIN_SITELINKS_BY_ENTITY_TYPE.get(
        entity_type,
        0,
    )
    token_count = len([token for token in label.split() if token.strip()])
    alias_count = len(aliases)
    if entity_type == "person":
        if token_count <= 1:
            return (
                has_enwiki
                and sitelink_count >= _GENERAL_WIKIDATA_MIN_SINGLE_TOKEN_PERSON_SITELINKS
                and (alias_count >= 1 or statement_count >= 20)
            )
        if has_enwiki or sitelink_count >= minimum_sitelinks:
            return True
        return alias_count >= 1 and statement_count >= 10
    if entity_type == "location":
        if not _should_keep_wikidata_dump_location_label(
            label,
            sitelink_count=sitelink_count,
        ):
            return False
        return has_enwiki or sitelink_count >= minimum_sitelinks
    if entity_type == "organization":
        if token_count <= 1:
            if _looks_like_structured_single_token_organization(label):
                return has_enwiki or sitelink_count >= minimum_sitelinks
            return (
                sitelink_count >= max(minimum_sitelinks, 12)
                and (
                    has_enwiki
                    or alias_count >= 2
                    or identifier_count > 0
                    or statement_count >= 12
                )
            )
        if _looks_like_plain_multi_token_organization_label(label):
            if sitelink_count < _GENERAL_WIKIDATA_MIN_PLAIN_MULTI_TOKEN_ORGANIZATION_SITELINKS:
                return False
            return has_enwiki or identifier_count > 0 or statement_count >= 12
        if has_enwiki or sitelink_count >= minimum_sitelinks:
            return True
        return alias_count >= 1 or (
            sitelink_count > 0
            and (
                _has_structural_org_suffix(label)
                or identifier_count > 0
                or statement_count >= 8
            )
        )
    if entity_type == "location":
        return False
    return False


def _has_structural_org_suffix(value: str) -> bool:
    tokens = [token.rstrip(".,").casefold() for token in value.split() if token.strip()]
    if len(tokens) < 2:
        return False
    return tokens[-1] in _GENERAL_ORG_SUFFIX_TOKENS


def _looks_like_structured_single_token_organization(value: str) -> bool:
    normalized = value.strip().strip(".,")
    if not normalized:
        return False
    alpha_count = sum(1 for character in normalized if character.isalpha())
    if alpha_count < 2:
        return False
    collapsed = normalized.replace("&", "").replace("-", "")
    return collapsed.isupper()


def _looks_like_plain_alpha_label(value: str) -> bool:
    normalized = value.strip()
    if not normalized:
        return False
    return _GENERAL_SINGLE_TOKEN_ALPHA_RE.fullmatch(normalized) is not None


def _looks_like_plain_multi_token_organization_label(value: str) -> bool:
    normalized = value.strip()
    if not normalized or _has_structural_org_suffix(normalized):
        return False
    if _GENERAL_MULTI_TOKEN_ALPHA_RE.fullmatch(normalized) is None:
        return False
    tokens = [token for token in normalized.split() if token.strip()]
    if len(tokens) < 2:
        return False
    return all(token[:1].isupper() and token[1:].islower() for token in tokens)


def _should_keep_wikidata_dump_location_label(
    value: str,
    *,
    sitelink_count: int,
) -> bool:
    normalized = value.strip()
    if not normalized:
        return False
    if " " in normalized:
        return True
    if not _looks_like_plain_alpha_label(normalized):
        return True
    if len(normalized) <= 3:
        return False
    return sitelink_count >= _GENERAL_WIKIDATA_MIN_SINGLE_TOKEN_LOCATION_SITELINKS


def _should_ignore_wikidata_sitelink_title(value: str) -> bool:
    normalized = value.strip()
    if not normalized:
        return True
    normalized_casefold = normalized.casefold()
    return any(
        normalized_casefold.startswith(prefix)
        for prefix in _WIKIDATA_IGNORED_SITELINK_TITLE_PREFIXES
    )


def _extract_wikidata_entity_dump_aliases(
    entity: dict[str, object],
    *,
    label: str,
) -> list[str]:
    aliases: list[str] = []
    raw_aliases = entity.get("aliases")
    if isinstance(raw_aliases, dict):
        english_aliases = raw_aliases.get("en", [])
        if isinstance(english_aliases, list):
            aliases.extend(
                alias
                for alias in (
                    _extract_wikidata_monolingual_value(item) for item in english_aliases
                )
                if alias
            )
    sitelinks = entity.get("sitelinks")
    if isinstance(sitelinks, dict):
        for site_id, sitelink in sitelinks.items():
            if not str(site_id).casefold().startswith("en"):
                continue
            if not isinstance(sitelink, dict):
                continue
            title = _clean_text(sitelink.get("title"))
            if title and not _should_ignore_wikidata_sitelink_title(title):
                aliases.append(title.replace("_", " "))
    return _dedupe_preserving_order(
        [
            alias
            for alias in aliases
            if alias and alias.casefold() != label.casefold()
        ]
    )


def _extract_wikidata_claim_entity_ids(
    entity: dict[str, object],
    property_id: str,
) -> set[str]:
    claims = entity.get("claims")
    if not isinstance(claims, dict):
        return set()
    values = claims.get(property_id, [])
    if not isinstance(values, list):
        return set()
    entity_ids: set[str] = set()
    for claim in values:
        if not isinstance(claim, dict):
            continue
        mainsnak = claim.get("mainsnak")
        if not isinstance(mainsnak, dict):
            continue
        if str(mainsnak.get("snaktype", "")).casefold() != "value":
            continue
        datavalue = mainsnak.get("datavalue")
        if not isinstance(datavalue, dict):
            continue
        value = datavalue.get("value")
        if not isinstance(value, dict):
            continue
        entity_id = _clean_text(value.get("id"))
        if entity_id:
            entity_ids.add(entity_id)
    return entity_ids


def _resolve_wikidata_entity_type(
    type_ids: set[str],
    *,
    type_matches: dict[str, tuple[int, str]],
) -> str | None:
    candidate_matches = [
        type_matches[type_id]
        for type_id in sorted(type_ids)
        if type_id in type_matches
    ]
    if not candidate_matches:
        return None
    entity_type = sorted(candidate_matches)[0][1]
    if entity_type not in _GENERAL_ALLOWED_ENTITY_TYPES:
        return None
    return entity_type


def _should_keep_wikidata_truthy_candidate(
    *,
    entity_type: str,
    label: str,
    aliases: list[str],
    sitelinks: int | None,
    minimum_sitelinks: int,
) -> bool:
    if sitelinks is not None:
        return sitelinks >= minimum_sitelinks
    token_count = len([token for token in label.split() if token])
    alias_count = len(aliases)
    if entity_type == "person":
        return token_count >= 2 or alias_count > 0
    if entity_type == "organization":
        return token_count >= 2 or alias_count > 0
    if entity_type == "location":
        normalized = label.strip()
        if not normalized:
            return False
        if normalized.islower():
            return False
        if normalized.isupper() and normalized.isalpha() and len(normalized) <= 3:
            return False
        if len(normalized) <= 3:
            return False
        return token_count >= 2 or alias_count > 0 or len(normalized) > 5
    return False


def _iter_wikidata_jsonl_entities(path: Path) -> Iterator[dict[str, object]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            cleaned = line.strip()
            if not cleaned:
                continue
            payload = json.loads(cleaned)
            if isinstance(payload, dict):
                yield payload


def _load_wikidata_jsonl_entities(path: Path) -> list[dict[str, object]]:
    return list(_iter_wikidata_jsonl_entities(path))


def _write_wikidata_jsonl_entities(
    path: Path,
    entities: list[dict[str, object]],
) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for entity in entities:
            handle.write(json.dumps(entity, sort_keys=True) + "\n")


def _merge_wikidata_truthy_jsonl_with_seed_entities(
    path: Path,
    *,
    seed_entities: list[dict[str, object]],
) -> int:
    if not seed_entities:
        return sum(1 for _ in _iter_wikidata_jsonl_entities(path))
    seed_by_id: dict[str, dict[str, object]] = {}
    for entity in seed_entities:
        entity_id = _clean_text(entity.get("id"))
        label = _clean_text(entity.get("label"))
        entity_type = _clean_text(entity.get("entity_type"))
        if not entity_id or not label or not entity_type:
            continue
        normalized_entity = dict(entity)
        normalized_entity["id"] = entity_id
        normalized_entity["label"] = label
        normalized_entity["entity_type"] = entity_type
        normalized_entity["aliases"] = _dedupe_preserving_order(
            [
                alias
                for alias in entity.get("aliases", [])
                if isinstance(alias, str) and alias and alias.casefold() != label.casefold()
            ]
        )
        normalized_entity["popularity"] = _coerce_int(entity.get("popularity"))
        seed_by_id[entity_id] = normalized_entity
    merged_count = 0
    temp_path = path.with_name(f".{path.name}.merge.tmp")
    try:
        with path.open("r", encoding="utf-8") as source_handle, temp_path.open(
            "w",
            encoding="utf-8",
        ) as output_handle:
            for line in source_handle:
                cleaned = line.strip()
                if not cleaned:
                    continue
                payload = json.loads(cleaned)
                if not isinstance(payload, dict):
                    continue
                entity_id = _clean_text(payload.get("id"))
                if not entity_id:
                    continue
                seed_entity = seed_by_id.pop(entity_id, None)
                merged_entity = (
                    _merge_wikidata_truthy_and_seed_entities(
                        truthy_entities=[payload],
                        seed_entities=[seed_entity],
                    )[0]
                    if seed_entity is not None
                    else payload
                )
                output_handle.write(json.dumps(merged_entity, sort_keys=True) + "\n")
                merged_count += 1
            for entity_id in sorted(seed_by_id):
                output_handle.write(json.dumps(seed_by_id[entity_id], sort_keys=True) + "\n")
                merged_count += 1
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)
    return merged_count


def _append_missing_wikidata_truthy_candidates(
    path: Path,
    *,
    work_db_path: Path,
) -> int:
    existing_ids: set[str] = set()
    entity_count = 0
    if path.exists():
        for payload in _iter_wikidata_jsonl_entities(path):
            entity_id = _clean_text(payload.get("id"))
            if not entity_id or entity_id in existing_ids:
                continue
            existing_ids.add(entity_id)
            entity_count += 1
    with path.open("a", encoding="utf-8") as handle:
        for payload in _iter_selected_wikidata_truthy_candidates(work_db_path):
            entity_id = _clean_text(payload.get("id"))
            if not entity_id or entity_id in existing_ids:
                continue
            entity_type = _clean_text(payload.get("entity_type")) or ""
            label = _clean_text(payload.get("label")) or ""
            aliases = payload.get("aliases", [])
            aliases = [alias for alias in aliases if isinstance(alias, str)]
            popularity = _coerce_int(payload.get("popularity")) or 0
            if not _should_append_wikidata_truthy_fallback_candidate(
                entity_type=entity_type,
                label=label,
                aliases=aliases,
                popularity=popularity,
            ):
                continue
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
            existing_ids.add(entity_id)
            entity_count += 1
    return entity_count


def _should_append_wikidata_truthy_fallback_candidate(
    *,
    entity_type: str,
    label: str,
    aliases: list[str],
    popularity: int,
) -> bool:
    if not entity_type or not label:
        return False
    alias_count = len(aliases)
    token_count = len([token for token in label.split() if token.strip()])
    if entity_type == "person":
        if token_count <= 1:
            return (
                popularity >= _GENERAL_WIKIDATA_MIN_SINGLE_TOKEN_PERSON_SITELINKS
                and alias_count >= 1
            )
        return popularity > 0 or alias_count >= 1
    if entity_type == "organization":
        if token_count <= 1 and not _looks_like_structured_single_token_organization(label):
            return popularity >= max(DEFAULT_WIKIDATA_MIN_SITELINKS_BY_ENTITY_TYPE.get("organization", 0), 12) and (
                alias_count >= 2 or _has_structural_org_suffix(label)
            )
        return (
            popularity > 0
            or alias_count >= 1
            or _has_structural_org_suffix(label)
            or token_count >= 2
        )
    if entity_type == "location":
        if token_count <= 1 and _looks_like_plain_alpha_label(label):
            return (
                popularity >= _GENERAL_WIKIDATA_MIN_SINGLE_TOKEN_LOCATION_SITELINKS
                or alias_count >= 2
            )
        return alias_count >= 1 or token_count >= 2 or popularity > 0
    return False


def _merge_wikidata_truthy_and_seed_entities(
    *,
    truthy_entities: list[dict[str, object]],
    seed_entities: list[dict[str, object]],
) -> list[dict[str, object]]:
    merged_by_id: dict[str, dict[str, object]] = {}
    for entity in truthy_entities:
        entity_id = _clean_text(entity.get("id"))
        label = _clean_text(entity.get("label"))
        entity_type = _clean_text(entity.get("entity_type"))
        if not entity_id or not label or not entity_type:
            continue
        merged_by_id[entity_id] = dict(entity)
        merged_by_id[entity_id]["id"] = entity_id
        merged_by_id[entity_id]["entity_type"] = entity_type
        merged_by_id[entity_id]["label"] = label
        merged_by_id[entity_id]["aliases"] = _dedupe_preserving_order(
            [
                alias
                for alias in entity.get("aliases", [])
                if isinstance(alias, str) and alias and alias.casefold() != label.casefold()
            ]
        )
        merged_by_id[entity_id]["popularity"] = _coerce_int(entity.get("popularity"))
    for entity in seed_entities:
        entity_id = _clean_text(entity.get("id"))
        label = _clean_text(entity.get("label"))
        entity_type = _clean_text(entity.get("entity_type"))
        if not entity_id or not label or not entity_type:
            continue
        seed_aliases = _dedupe_preserving_order(
            [
                alias
                for alias in entity.get("aliases", [])
                if isinstance(alias, str) and alias and alias.casefold() != label.casefold()
            ]
        )
        existing = merged_by_id.get(entity_id)
        if existing is None:
            merged_by_id[entity_id] = dict(entity)
            merged_by_id[entity_id]["id"] = entity_id
            merged_by_id[entity_id]["entity_type"] = entity_type
            merged_by_id[entity_id]["label"] = label
            merged_by_id[entity_id]["aliases"] = seed_aliases
            merged_by_id[entity_id]["popularity"] = _coerce_int(entity.get("popularity"))
            continue
        merged_label = _clean_text(existing.get("label")) or label
        merged_entity_type = _clean_text(existing.get("entity_type")) or entity_type
        existing_aliases = existing.get("aliases", [])
        merged_aliases = _dedupe_preserving_order(
            [
                alias
                for alias in (
                    *(existing_aliases if isinstance(existing_aliases, list) else []),
                    *seed_aliases,
                )
                if isinstance(alias, str)
                and alias
                and alias.casefold() != merged_label.casefold()
            ]
        )
        existing["entity_type"] = merged_entity_type
        existing["label"] = merged_label
        existing["aliases"] = merged_aliases
        existing["popularity"] = _coerce_int(existing.get("popularity")) or _coerce_int(
            entity.get("popularity")
        )
    return [merged_by_id[entity_id] for entity_id in sorted(merged_by_id)]


def _load_wikidata_payload(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        raw_text = handle.read()
    stripped = raw_text.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        return json.loads(raw_text)
    entities: list[dict[str, object]] = []
    for line in raw_text.splitlines():
        cleaned = line.strip()
        if not cleaned:
            continue
        payload = json.loads(cleaned)
        if isinstance(payload, dict):
            entities.append(payload)
    return {"entities": entities}


def _iter_wikidata_dump_objects(path: Path) -> Iterator[dict[str, object]]:
    for line in _iter_text_lines(path):
        cleaned = line.strip().rstrip(",")
        if not cleaned or cleaned in {"[", "]"}:
            continue
        payload = json.loads(cleaned)
        if isinstance(payload, dict):
            yield payload


def _load_geonames_rows(path: Path) -> list[dict[str, str]]:
    text = _read_archive_or_text_file(path)
    rows: list[dict[str, str]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        columns = line.split("\t")
        if len(columns) < 9:
            raise ValueError("GeoNames row does not have the expected column count.")
        rows.append(
            {
                "geonameid": columns[0].strip(),
                "name": columns[1].strip(),
                "alternatenames": columns[3].strip(),
                "feature_class": columns[6].strip(),
                "feature_code": columns[7].strip(),
                "country_code": columns[8].strip(),
                "population": columns[14].strip() if len(columns) > 14 else "0",
            }
        )
    return rows


def _load_geonames_alternate_name_records(path: Path) -> dict[str, dict[str, str]]:
    text = _read_archive_or_text_file(path)
    records: dict[str, dict[str, str]] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        columns = line.split("\t")
        if len(columns) < 4:
            continue
        alternate_id = columns[0].strip()
        geonameid = columns[1].strip()
        iso_language = columns[2].strip()
        name = columns[3].strip()
        is_preferred_name = columns[4].strip() if len(columns) > 4 else ""
        is_short_name = columns[5].strip() if len(columns) > 5 else ""
        if not alternate_id or not geonameid or not name:
            continue
        records[alternate_id] = {
            "alternate_id": alternate_id,
            "geonameid": geonameid,
            "iso_language": iso_language,
            "name": name,
            "is_preferred_name": is_preferred_name,
            "is_short_name": is_short_name,
        }
    return records


def _build_geonames_alternate_name_index(
    records_by_id: dict[str, dict[str, str]],
) -> dict[str, list[str]]:
    names_by_geonameid: dict[str, list[str]] = {}
    for record in records_by_id.values():
        if not _should_keep_geonames_alternate_name(record):
            continue
        geonameid = record["geonameid"]
        names_by_geonameid.setdefault(geonameid, []).append(record["name"])
    return {
        geonameid: _dedupe_preserving_order(names)
        for geonameid, names in names_by_geonameid.items()
    }


def _should_keep_geonames_alternate_name(record: dict[str, str]) -> bool:
    iso_language = record["iso_language"].strip().casefold()
    if iso_language in _GEONAMES_ALIAS_LANGUAGES:
        return True
    return _truthy_like(record["is_preferred_name"]) or _truthy_like(
        record["is_short_name"]
    )


def _merge_geonames_aliases(existing_aliases: str, additional_aliases: list[str]) -> str:
    merged = _dedupe_preserving_order(
        [
            alias
            for alias in (
                *_split_comma_aliases(existing_aliases),
                *additional_aliases,
            )
            if alias
        ]
    )
    return ",".join(merged)


def _split_comma_aliases(value: str) -> list[str]:
    return [alias.strip() for alias in value.split(",") if alias.strip()]


def _load_single_column_ids(path: Path) -> list[str]:
    text = _read_archive_or_text_file(path)
    identifiers: list[str] = []
    for line in text.splitlines():
        cleaned = line.strip()
        if not cleaned:
            continue
        identifiers.append(cleaned.split("\t", 1)[0].strip())
    return identifiers


def _read_archive_or_text_file(path: Path) -> str:
    if path.suffix.casefold() == ".zip":
        with zipfile.ZipFile(path) as archive:
            members = [name for name in archive.namelist() if not name.endswith("/")]
            if not members:
                raise ValueError(f"Archive does not contain any files: {path}")
            target_name = members[0]
            return archive.read(target_name).decode("utf-8")
    return "".join(_iter_text_lines(path))


def _should_keep_geonames_row(row: dict[str, str]) -> bool:
    feature_code = row["feature_code"].strip()
    if feature_code not in _GEONAMES_ALLOWED_FEATURE_CODES:
        return False
    population = row["population"].strip()
    try:
        population_value = int(population) if population else 0
    except ValueError:
        population_value = 0
    if feature_code == "PPL":
        return population_value >= DEFAULT_GENERAL_GEONAMES_MIN_POPULATION
    if feature_code.startswith("PPLA") or feature_code == "PPLC":
        return population_value >= DEFAULT_GENERAL_GEONAMES_MIN_ADMIN_POPULATION
    return True


def _download_source_file(source_url: str, destination: Path, *, user_agent: str) -> None:
    parsed = urlparse(source_url)
    if parsed.scheme == "file":
        source_path = Path(urllib.request.url2pathname(parsed.path)).resolve()
        try:
            destination_path = destination.resolve()
        except FileNotFoundError:
            destination_path = destination.absolute()
        if source_path == destination_path:
            return
        if destination.exists():
            try:
                if os.path.samefile(source_path, destination):
                    return
            except OSError:
                pass
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(source_path, destination)
        except FileExistsError:
            if os.path.samefile(source_path, destination):
                return
            raise
        except OSError:
            shutil.copyfile(source_path, destination)
        return
    request: str | urllib.request.Request
    if parsed.scheme in {"http", "https"}:
        headers = {
            "User-Agent": user_agent,
            "Accept": "application/json,text/plain;q=0.9,*/*;q=0.1",
        }
        existing_size = destination.stat().st_size if destination.exists() else 0
        if existing_size > 0:
            headers["Range"] = f"bytes={existing_size}-"
        request = urllib.request.Request(source_url, headers=headers)
    else:
        request = source_url
        existing_size = 0
    try:
        with urllib.request.urlopen(request, timeout=120.0) as response:
            response_code = response.getcode() or 200
            append = (
                parsed.scheme in {"http", "https"}
                and existing_size > 0
                and response_code == 206
            )
            mode = "ab" if append else "wb"
            with destination.open(mode) as handle:
                shutil.copyfileobj(response, handle, length=65536)
    except urllib.error.HTTPError as exc:
        if parsed.scheme in {"http", "https"} and exc.code == 416 and destination.exists():
            total_size = _extract_total_size_from_content_range(
                exc.headers.get("Content-Range")
            )
            if total_size is not None:
                current_size = destination.stat().st_size
                if current_size > total_size:
                    with destination.open("r+b") as handle:
                        handle.truncate(total_size)
            return
        raise


def _extract_total_size_from_content_range(header_value: str | None) -> int | None:
    if not header_value:
        return None
    marker = "*/"
    if marker not in header_value:
        return None
    _, _, size_fragment = header_value.partition(marker)
    try:
        return int(size_fragment.strip())
    except ValueError:
        return None


def _download_target_name(prefix: str, source_url: str) -> str:
    parsed = urlparse(source_url)
    suffixes = Path(parsed.path).suffixes
    if suffixes:
        if prefix.endswith(suffixes[0]):
            suffix = "".join(suffixes[1:])
        else:
            suffix = "".join(suffixes)
        if suffix:
            return f"{prefix}{suffix}"
    return prefix


def _iter_text_lines(path: Path) -> Iterator[str]:
    if path.suffix.casefold() == ".gz":
        pigz_path = shutil.which("pigz")
        if pigz_path is not None:
            process = subprocess.Popen(
                [
                    pigz_path,
                    "-dc",
                    str(path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            if process.stdout is None or process.stderr is None:
                process.kill()
                raise RuntimeError(f"Unable to stream gzip file with pigz: {path}")
            try:
                yield from process.stdout
            finally:
                process.stdout.close()
                stderr_output = process.stderr.read()
                process.stderr.close()
                return_code = process.wait()
                if return_code != 0:
                    raise RuntimeError(
                        f"pigz failed while streaming {path}: {stderr_output.strip() or return_code}"
                    )
            return
        rapidgzip_path = shutil.which("rapidgzip")
        if rapidgzip_path is not None:
            process = subprocess.Popen(
                [
                    rapidgzip_path,
                    "-d",
                    "-c",
                    "-P",
                    "0",
                    "--no-verify",
                    str(path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            if process.stdout is None or process.stderr is None:
                process.kill()
                raise RuntimeError(f"Unable to stream gzip file with rapidgzip: {path}")
            try:
                yield from process.stdout
            finally:
                process.stdout.close()
                stderr_output = process.stderr.read()
                process.stderr.close()
                return_code = process.wait()
                if return_code != 0:
                    raise RuntimeError(
                        f"rapidgzip failed while streaming {path}: {stderr_output.strip() or return_code}"
                    )
            return
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            yield from handle
    else:
        with path.open("r", encoding="utf-8") as handle:
            yield from handle


def _iter_wikidata_truthy_relevant_lines(path: Path) -> Iterator[str]:
    if path.suffix.casefold() == ".nt":
        grep_executable = shutil.which("grep")
        if grep_executable:
            command = [grep_executable, "-a", "-F"]
            for predicate_term in _WIKIDATA_GREP_PREDICATE_TERMS:
                command.extend(("-e", predicate_term))
            command.append(str(path))
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            try:
                assert process.stdout is not None
                for line in process.stdout:
                    if _is_relevant_wikidata_truthy_line(line):
                        yield line
                stderr_text = (
                    process.stderr.read() if process.stderr is not None else ""
                )
            finally:
                if process.stdout is not None:
                    process.stdout.close()
                if process.stderr is not None:
                    process.stderr.close()
            return_code = process.wait()
            if return_code not in (0, 1):
                raise RuntimeError(
                    "grep predicate prefilter failed for Wikidata truthy dump"
                    + (f": {stderr_text.strip()}" if stderr_text.strip() else "")
                )
            return
    for line in _iter_text_lines(path):
        if _is_relevant_wikidata_truthy_line(line):
            yield line


def _is_relevant_wikidata_truthy_line(line: str) -> bool:
    if (
        _WIKIDATA_DIRECT_P31_TERM in line
        or _WIKIDATA_DIRECT_P279_TERM in line
        or _WIKIDATA_SCHEMA_ABOUT_PREDICATE_TERM in line
        or _WIKIDATA_SITELINKS_PREDICATE_TERM in line
    ):
        return True
    if "@en ." not in line:
        return False
    return any(
        predicate_term in line
        for predicate_term in (
            *_WIKIDATA_LABEL_PREDICATE_TERMS,
            _WIKIDATA_ALT_LABEL_PREDICATE_TERM,
        )
    )


def _truthy_like(value: str) -> bool:
    return value.strip().casefold() in {"1", "true", "t", "y", "yes"}


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).strip().split())
    return text or None


def _coerce_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _sha256_file(path: Path) -> str:
    cache_path = path.with_name(f"{path.name}.sha256.json")
    cached_digest = _read_sha256_cache(
        cache_path,
        expected_size=path.stat().st_size,
        expected_mtime_ns=path.stat().st_mtime_ns,
    )
    if cached_digest is not None:
        return cached_digest
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    digest_hex = digest.hexdigest()
    cache_payload = {
        "sha256": digest_hex,
        "size_bytes": path.stat().st_size,
        "mtime_ns": path.stat().st_mtime_ns,
    }
    cache_path.write_text(
        json.dumps(cache_payload, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return digest_hex


def _fast_local_file_signature(path: Path) -> str:
    stat_result = path.stat()
    return (
        f"stat:{stat_result.st_dev}:{stat_result.st_ino}:"
        f"{stat_result.st_size}:{stat_result.st_mtime_ns}"
    )


def _source_manifest_fingerprint(path: Path) -> dict[str, str]:
    stat_result = path.stat()
    cache_path = path.with_name(f"{path.name}.sha256.json")
    cached_digest = _read_sha256_cache(
        cache_path,
        expected_size=stat_result.st_size,
        expected_mtime_ns=stat_result.st_mtime_ns,
    )
    if cached_digest is not None:
        return {
            "sha256": cached_digest,
            "content_signature": f"sha256:{cached_digest}",
        }
    if (
        path.is_file()
        and stat_result.st_size >= _TRUTHY_STAGE_FAST_SIGNATURE_MIN_BYTES
    ):
        return {
            "content_signature": _fast_local_file_signature(path),
        }
    digest = _sha256_file(path)
    return {
        "sha256": digest,
        "content_signature": f"sha256:{digest}",
    }


def _wikidata_truthy_stage_source_signature(path: Path) -> str:
    stat_result = path.stat()
    cache_path = path.with_name(f"{path.name}.sha256.json")
    cached_digest = _read_sha256_cache(
        cache_path,
        expected_size=stat_result.st_size,
        expected_mtime_ns=stat_result.st_mtime_ns,
    )
    if cached_digest is not None:
        return f"sha256:{cached_digest}"
    if (
        path.is_file()
        and stat_result.st_size >= _TRUTHY_STAGE_FAST_SIGNATURE_MIN_BYTES
    ):
        return _fast_local_file_signature(path)
    return f"sha256:{_sha256_file(path)}"


def _read_sha256_cache(
    cache_path: Path,
    *,
    expected_size: int,
    expected_mtime_ns: int,
) -> str | None:
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    digest = payload.get("sha256")
    cached_size = payload.get("size_bytes")
    cached_mtime_ns = payload.get("mtime_ns")
    if (
        isinstance(digest, str)
        and digest
        and cached_size == expected_size
        and cached_mtime_ns == expected_mtime_ns
    ):
        return digest
    return None


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
