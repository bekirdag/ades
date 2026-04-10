"""General-source snapshot fetchers for real-content pack generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from hashlib import sha256
import io
import json
from pathlib import Path
import shutil
import urllib.request
from urllib.parse import urlparse
import zipfile

from .fetch import normalize_source_url


DEFAULT_GENERAL_SOURCE_OUTPUT_ROOT = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/raw/general-en"
)
DEFAULT_WIKIDATA_URL = (
    "https://www.wikidata.org/w/api.php?action=wbgetentities&"
    "ids=Q265852|Q2283|Q95|Q3884|Q380|Q7426870|Q305177|Q182477|Q116758847|Q7407093|Q15733006|Q3503829|Q3022141&languages=en&props=labels|aliases&format=json"
)
DEFAULT_GEONAMES_PLACES_URL = "https://download.geonames.org/export/dump/cities15000.zip"
DEFAULT_SOURCE_FETCH_USER_AGENT = "ades/0.1.0 (ops@adestool.com)"
DEFAULT_GENERAL_GEONAMES_IDS: tuple[str, ...] = ("745044", "2643743")
DEFAULT_WIKIDATA_ENTITY_TYPES: dict[str, str] = {
    "Q265852": "person",
    "Q2283": "organization",
    "Q95": "organization",
    "Q3884": "organization",
    "Q380": "organization",
    "Q7426870": "person",
    "Q305177": "person",
    "Q182477": "organization",
    "Q116758847": "organization",
    "Q7407093": "person",
    "Q15733006": "organization",
    "Q3503829": "person",
    "Q3022141": "person",
}
DEFAULT_CURATED_GENERAL_ENTITIES: tuple[dict[str, object], ...] = (
    {
        "entity_type": "organization",
        "canonical_text": "OpenAI",
        "aliases": ["Open AI"],
    },
    {
        "entity_type": "organization",
        "canonical_text": "Apple",
        "aliases": [],
    },
)
_GEONAMES_OUTPUT_HEADER = (
    "geonameid|name|alternatenames|feature_class|feature_code|country_code"
)


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


def fetch_general_source_snapshot(
    *,
    output_dir: str | Path = DEFAULT_GENERAL_SOURCE_OUTPUT_ROOT,
    snapshot: str | None = None,
    wikidata_url: str = DEFAULT_WIKIDATA_URL,
    geonames_places_url: str = DEFAULT_GEONAMES_PLACES_URL,
    user_agent: str = DEFAULT_SOURCE_FETCH_USER_AGENT,
) -> GeneralSourceFetchResult:
    """Download one immutable general source snapshot set into the big-data root."""

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_snapshot = _resolve_snapshot(snapshot)
    snapshot_dir = resolved_output_dir / resolved_snapshot
    if snapshot_dir.exists():
        raise FileExistsError(
            f"General source snapshot directory already exists: {snapshot_dir}"
        )
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    generated_at = _utc_timestamp()
    warnings: list[str] = []
    try:
        (
            resolved_wikidata_url,
            wikidata_entities_path,
            wikidata_entity_count,
        ) = _download_wikidata_snapshot(
            wikidata_url,
            snapshot_dir=snapshot_dir,
            user_agent=user_agent,
        )
        (
            resolved_geonames_url,
            geonames_places_path,
            geonames_location_count,
        ) = _download_geonames_snapshot(
            geonames_places_url,
            snapshot_dir=snapshot_dir,
            user_agent=user_agent,
        )
        curated_entities_path = snapshot_dir / "curated_general_entities.json"
        _write_curated_entities(curated_entities_path)
        _write_source_manifest(
            path=snapshot_dir / "sources.fetch.json",
            snapshot=resolved_snapshot,
            generated_at=generated_at,
            user_agent=user_agent,
            wikidata_url=resolved_wikidata_url,
            geonames_places_url=resolved_geonames_url,
            wikidata_entities_path=wikidata_entities_path,
            geonames_places_path=geonames_places_path,
            curated_entities_path=curated_entities_path,
        )
    except Exception:
        shutil.rmtree(snapshot_dir, ignore_errors=True)
        raise

    manifest_path = snapshot_dir / "sources.fetch.json"
    if wikidata_entity_count == 0:
        warnings.append("Wikidata snapshot produced no general entities.")
    if geonames_location_count == 0:
        warnings.append("GeoNames snapshot produced no locations.")

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
        source_count=3,
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


def _download_wikidata_snapshot(
    source_url: str | Path,
    *,
    snapshot_dir: Path,
    user_agent: str,
) -> tuple[str, Path, int]:
    normalized_source_url = normalize_source_url(source_url)
    raw_bytes = _read_bytes(normalized_source_url, user_agent=user_agent)
    raw_path = snapshot_dir / "wikidata_general_entities.source.json"
    raw_path.write_bytes(raw_bytes)

    payload = json.loads(raw_bytes.decode("utf-8"))
    entities = _normalize_wikidata_entities(payload)
    formatted_path = snapshot_dir / "wikidata_general_entities.json"
    formatted_path.write_text(
        json.dumps({"entities": entities}, indent=2) + "\n",
        encoding="utf-8",
    )
    return normalized_source_url, formatted_path, len(entities)


def _download_geonames_snapshot(
    source_url: str | Path,
    *,
    snapshot_dir: Path,
    user_agent: str,
) -> tuple[str, Path, int]:
    normalized_source_url = normalize_source_url(source_url)
    raw_bytes = _read_bytes(normalized_source_url, user_agent=user_agent)
    raw_suffix = ".zip" if _looks_like_zip(raw_bytes) else ".txt"
    raw_path = snapshot_dir / f"geonames_places.source{raw_suffix}"
    raw_path.write_bytes(raw_bytes)

    rows = _load_geonames_rows(raw_bytes)
    filtered_rows = [
        row for row in rows if row["geonameid"] in DEFAULT_GENERAL_GEONAMES_IDS
    ]
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
                ]
            )
            + "\n"
            for row in filtered_rows
        ),
        encoding="utf-8",
    )
    return normalized_source_url, formatted_path, len(filtered_rows)


def _write_curated_entities(path: Path) -> None:
    payload = {"entities": list(DEFAULT_CURATED_GENERAL_ENTITIES)}
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_source_manifest(
    *,
    path: Path,
    snapshot: str,
    generated_at: str,
    user_agent: str,
    wikidata_url: str,
    geonames_places_url: str,
    wikidata_entities_path: Path,
    geonames_places_path: Path,
    curated_entities_path: Path,
) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": "general-en",
                "snapshot": snapshot,
                "generated_at": generated_at,
                "user_agent": user_agent,
                "sources": [
                    _source_manifest_entry(
                        name="wikidata-general-entities",
                        source_url=wikidata_url,
                        path=wikidata_entities_path,
                    ),
                    _source_manifest_entry(
                        name="geonames-places",
                        source_url=geonames_places_url,
                        path=geonames_places_path,
                    ),
                    _source_manifest_entry(
                        name="curated-general-entities",
                        source_url="operator://ades/curated-general-entities",
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


def _normalize_wikidata_entities(payload: object) -> list[dict[str, object]]:
    if isinstance(payload, dict) and "entities" in payload:
        entity_payload = payload["entities"]
        if isinstance(entity_payload, dict):
            normalized: list[dict[str, object]] = []
            for entity_id, entity in entity_payload.items():
                if not isinstance(entity, dict):
                    continue
                label = _clean_text(entity.get("labels", {}).get("en", {}).get("value"))
                entity_type = DEFAULT_WIKIDATA_ENTITY_TYPES.get(entity_id)
                if not label or not entity_type:
                    continue
                aliases_payload = entity.get("aliases", {}).get("en", [])
                aliases = [
                    _clean_text(item.get("value"))
                    for item in aliases_payload
                    if isinstance(item, dict)
                ]
                normalized.append(
                    {
                        "id": entity_id,
                        "entity_type": entity_type,
                        "label": label,
                        "aliases": [alias for alias in aliases if alias],
                    }
                )
            return sorted(normalized, key=lambda item: str(item["id"]))
        if isinstance(entity_payload, list):
            return [
                item
                for item in entity_payload
                if isinstance(item, dict)
                and _clean_text(item.get("entity_type") or item.get("type"))
                and _clean_text(item.get("label") or item.get("canonical_text"))
            ]
    raise ValueError("Unsupported Wikidata general entity payload structure.")


def _load_geonames_rows(raw_bytes: bytes) -> list[dict[str, str]]:
    if _looks_like_zip(raw_bytes):
        archive = zipfile.ZipFile(io.BytesIO(raw_bytes))
        members = [name for name in archive.namelist() if name.endswith(".txt")]
        if not members:
            raise ValueError("GeoNames snapshot zip does not contain a .txt member.")
        text = archive.read(members[0]).decode("utf-8")
    else:
        text = raw_bytes.decode("utf-8")

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
            }
        )
    return rows


def _looks_like_zip(raw_bytes: bytes) -> bool:
    return raw_bytes.startswith(b"PK\x03\x04")


def _read_bytes(source_url: str, *, user_agent: str) -> bytes:
    parsed = urlparse(source_url)
    request: str | urllib.request.Request
    if parsed.scheme in {"http", "https"}:
        request = urllib.request.Request(
            source_url,
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json,text/plain;q=0.9,*/*;q=0.1",
            },
        )
    else:
        request = source_url
    with urllib.request.urlopen(request, timeout=60.0) as response:
        return response.read()


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
