"""Derive reusable market-graph source-lane TSV inputs from installed packs."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Iterable, Iterator

from .finance_country_proxy import (
    COUNTRY_DISPLAY_NAME,
    DEFAULT_BIG_DATA_ROOT,
    DEFAULT_PACKS_ROOT,
    FinanceCountryEntity,
    country_code_from_pack_id,
    discover_finance_country_pack_dirs,
    load_finance_country_pack_entities,
)

DEFAULT_OUTPUT_ROOT = (
    DEFAULT_BIG_DATA_ROOT
    / "pack_sources"
    / "impact_relationships"
    / "finance_country_source_lane_inputs"
)

ISSUER_SECTOR_COLUMNS = [
    "issuer_ref",
    "issuer_name",
    "sector_ref",
    "sector_name",
    "ticker_ref",
    "ticker_symbol",
    "jurisdiction",
    "relation",
    "evidence_level",
    "confidence",
    "direction_hint",
    "source_name",
    "source_url",
    "source_snapshot",
    "source_year",
    "refresh_policy",
    "pack_ids",
    "notes",
]

ISSUER_GEOGRAPHY_COLUMNS = [
    "issuer_ref",
    "issuer_name",
    "country_ref",
    "country_name",
    "geography_type",
    "ticker_ref",
    "ticker_symbol",
    "jurisdiction",
    "relation",
    "evidence_level",
    "confidence",
    "direction_hint",
    "source_name",
    "source_url",
    "source_snapshot",
    "source_year",
    "refresh_policy",
    "pack_ids",
    "notes",
]

_ISSUER_SECTOR_KEYS = (
    "sector",
    "sectors",
    "industry",
    "industries",
    "gics_sector",
    "gics_industry",
    "icb_sector",
    "icb_industry",
    "business_sector",
    "business_segments",
)
_TICKER_KEYS = ("ticker", "tickers", "symbol", "symbols")
_EXCHANGE_KEYS = (
    "exchange",
    "exchange_code",
    "exchanges",
    "instrument_exchange",
    "primary_exchange",
    "trading_venue",
    "venue",
    "market",
)
_LOOKUP_NON_WORD_RE = re.compile(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿ]+")
_LOOKUP_NON_ALNUM_RE = re.compile(r"[^0-9A-Za-z]+")


@dataclass(frozen=True)
class FinanceCountrySourceLaneInputDerivationResult:
    run_id: str
    output_dir: Path
    issuer_sector_tsv_path: Path
    issuer_geography_tsv_path: Path
    manifest_path: Path
    pack_count: int
    issuer_count: int
    ticker_resolved_count: int
    issuer_sector_row_count: int
    issuer_geography_row_count: int

    def to_json_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "output_dir": str(self.output_dir),
            "issuer_sector_tsv_path": str(self.issuer_sector_tsv_path),
            "issuer_geography_tsv_path": str(self.issuer_geography_tsv_path),
            "manifest_path": str(self.manifest_path),
            "pack_count": self.pack_count,
            "issuer_count": self.issuer_count,
            "ticker_resolved_count": self.ticker_resolved_count,
            "issuer_sector_row_count": self.issuer_sector_row_count,
            "issuer_geography_row_count": self.issuer_geography_row_count,
        }


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _clean_metadata_string(value: object | None) -> str:
    raw = _normalize_text(value)
    if raw.casefold() in {"", "none", "null", "nan", "n/a", "na", "-"}:
        return ""
    return raw


def _metadata_values(metadata: dict[str, object], *keys: str) -> Iterator[str]:
    for key in keys:
        raw = metadata.get(key)
        if isinstance(raw, (list, tuple, set)):
            for item in raw:
                cleaned = _clean_metadata_string(item)
                if cleaned:
                    yield cleaned
            continue
        cleaned = _clean_metadata_string(raw)
        if not cleaned:
            continue
        if any(separator in cleaned for separator in ("|", ";")):
            for item in re.split(r"[|;]", cleaned):
                part = _clean_metadata_string(item)
                if part:
                    yield part
            continue
        yield cleaned


def _lookup_name_key(value: object | None) -> str:
    cleaned = _clean_metadata_string(value)
    parts = [part for part in _LOOKUP_NON_WORD_RE.split(cleaned.casefold()) if part]
    return " ".join(parts)


def _lookup_identifier_key(value: object | None) -> str:
    return _LOOKUP_NON_ALNUM_RE.sub("", _clean_metadata_string(value).casefold())


def _ref_segment(value: object | None, *, max_length: int = 80) -> str:
    key = _lookup_name_key(value)
    segment = re.sub(r"[^a-z0-9]+", "-", key).strip("-")
    return segment[:max_length].strip("-") or "unknown"


def _is_issuer(entity: FinanceCountryEntity) -> bool:
    return entity.category == "issuer" or entity.entity_type.casefold() == "issuer"


def _is_ticker(entity: FinanceCountryEntity) -> bool:
    return entity.category == "ticker" or entity.entity_type.casefold() == "ticker"


def _source_snapshot_for_pack(pack_dir: Path) -> str:
    build_path = pack_dir / "build.json"
    if build_path.exists():
        payload = json.loads(build_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and payload.get("generated_at"):
            return str(payload["generated_at"])[:10]
    sources_path = pack_dir / "sources.json"
    if sources_path.exists():
        payload = json.loads(sources_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            for source in payload.get("sources") or []:
                if isinstance(source, dict) and source.get("retrieved_at"):
                    return str(source["retrieved_at"])[:10]
    return datetime.now(timezone.utc).date().isoformat()


def _source_url_for_pack(pack_dir: Path) -> str:
    sources_path = pack_dir / "sources.json"
    if sources_path.exists():
        return sources_path.resolve().as_uri()
    return pack_dir.resolve().as_uri()


def _source_year(source_snapshot: str) -> str:
    match = re.match(r"^(\d{4})", source_snapshot)
    if match:
        return match.group(1)
    return str(datetime.now(timezone.utc).year)


def _ticker_ref(pack_id: str, ticker: object | None) -> str:
    raw = _clean_metadata_string(ticker).upper()
    if not raw:
        return ""
    return f"{pack_id.removesuffix('-en')}-ticker:{raw}"


def _ticker_lookup_keys(ticker: object | None, exchanges: Iterable[object] = ()) -> list[str]:
    symbol_key = _lookup_identifier_key(ticker)
    if not symbol_key:
        return []
    keys: list[str] = []
    for exchange in exchanges:
        exchange_key = _lookup_identifier_key(exchange)
        if exchange_key:
            keys.append(f"{exchange_key}:{symbol_key}")
    keys.append(symbol_key)
    return list(dict.fromkeys(keys))


def _ticker_symbol(entity: FinanceCountryEntity) -> str:
    return next(
        _metadata_values(entity.metadata, "ticker", "symbol"),
        entity.canonical_name,
    ).upper()


def _build_ticker_indexes(
    entities: Iterable[FinanceCountryEntity],
) -> tuple[dict[str, FinanceCountryEntity], dict[str, FinanceCountryEntity]]:
    by_symbol: dict[str, FinanceCountryEntity] = {}
    by_issuer_name: dict[str, FinanceCountryEntity] = {}
    for entity in entities:
        if not _is_ticker(entity):
            continue
        exchanges = tuple(_metadata_values(entity.metadata, *_EXCHANGE_KEYS))
        for ticker in (
            *tuple(_metadata_values(entity.metadata, *_TICKER_KEYS)),
            entity.canonical_name,
        ):
            for key in _ticker_lookup_keys(ticker, exchanges):
                by_symbol.setdefault(key, entity)
        for issuer_name in _metadata_values(
            entity.metadata,
            "issuer_name",
            "company_name",
            "legal_name",
        ):
            name_key = _lookup_name_key(issuer_name)
            if name_key:
                by_issuer_name.setdefault(name_key, entity)
    return by_symbol, by_issuer_name


def _resolve_issuer_ticker(
    issuer: FinanceCountryEntity,
    *,
    tickers_by_symbol: dict[str, FinanceCountryEntity],
    tickers_by_issuer_name: dict[str, FinanceCountryEntity],
) -> tuple[str, str]:
    exchanges = tuple(_metadata_values(issuer.metadata, *_EXCHANGE_KEYS))
    first_symbol = ""
    for ticker in _metadata_values(issuer.metadata, *_TICKER_KEYS):
        if not first_symbol:
            first_symbol = _clean_metadata_string(ticker).upper()
        for key in _ticker_lookup_keys(ticker, exchanges):
            ticker_entity = tickers_by_symbol.get(key)
            if ticker_entity is not None:
                return ticker_entity.entity_ref, _ticker_symbol(ticker_entity)
    name_key = _lookup_name_key(issuer.canonical_name)
    if name_key:
        ticker_entity = tickers_by_issuer_name.get(name_key)
        if ticker_entity is not None:
            return ticker_entity.entity_ref, _ticker_symbol(ticker_entity)
    if first_symbol:
        return _ticker_ref(issuer.pack_id, first_symbol), first_symbol
    return "", ""


def _write_tsv(
    path: Path,
    *,
    columns: list[str],
    rows: Iterable[dict[str, str]],
) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})
            count += 1
    return count


def derive_finance_country_source_lane_inputs(
    *,
    packs_root: str | Path = DEFAULT_PACKS_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> FinanceCountrySourceLaneInputDerivationResult:
    """Derive issuer-sector and issuer-geography input TSVs from finance-country packs."""

    resolved_run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(output_root).expanduser().resolve() / resolved_run_id
    issuer_sector_tsv_path = output_dir / "issuer_sector.tsv"
    issuer_geography_tsv_path = output_dir / "issuer_geography.tsv"
    manifest_path = output_dir / "manifest.json"

    issuer_sector_rows: dict[tuple[str, str], dict[str, str]] = {}
    issuer_geography_rows: dict[str, dict[str, str]] = {}
    pack_summaries: list[dict[str, object]] = []
    issuer_count = 0
    ticker_resolved_count = 0

    for pack_dir in discover_finance_country_pack_dirs(packs_root):
        pack_id = pack_dir.name
        country_code = country_code_from_pack_id(pack_id)
        if country_code is None:
            continue
        country_name = COUNTRY_DISPLAY_NAME.get(country_code, country_code.upper())
        source_snapshot = _source_snapshot_for_pack(pack_dir)
        source_url = _source_url_for_pack(pack_dir)
        entities = load_finance_country_pack_entities(pack_dir)
        tickers_by_symbol, tickers_by_issuer_name = _build_ticker_indexes(entities)
        pack_issuer_count = 0
        pack_sector_rows_start = len(issuer_sector_rows)
        pack_geography_rows_start = len(issuer_geography_rows)

        for issuer in entities:
            if not _is_issuer(issuer):
                continue
            issuer_count += 1
            pack_issuer_count += 1
            ticker_ref, ticker_symbol = _resolve_issuer_ticker(
                issuer,
                tickers_by_symbol=tickers_by_symbol,
                tickers_by_issuer_name=tickers_by_issuer_name,
            )
            if ticker_ref:
                ticker_resolved_count += 1
            base_row = {
                "issuer_ref": issuer.entity_ref,
                "issuer_name": issuer.canonical_name,
                "ticker_ref": ticker_ref,
                "ticker_symbol": ticker_symbol,
                "jurisdiction": country_code,
                "source_url": str(issuer.metadata.get("source_url") or source_url),
                "source_snapshot": source_snapshot,
                "source_year": _source_year(source_snapshot),
                "refresh_policy": "monthly",
                "pack_ids": pack_id,
            }
            issuer_geography_rows.setdefault(
                issuer.entity_ref,
                {
                    **base_row,
                    "country_ref": f"country:{country_code}",
                    "country_name": country_name,
                    "geography_type": "country",
                    "relation": "issuer_exposed_to_country",
                    "evidence_level": "shallow",
                    "confidence": "0.66",
                    "direction_hint": "issuer_country_exposure",
                    "source_name": "ades-finance-country-pack-country-metadata",
                    "notes": ("Primary issuer jurisdiction from finance-country pack metadata."),
                },
            )
            for sector_name in _metadata_values(issuer.metadata, *_ISSUER_SECTOR_KEYS):
                sector_ref = f"{pack_id.removesuffix('-en')}-sector:{_ref_segment(sector_name)}"
                issuer_sector_rows.setdefault(
                    (issuer.entity_ref, sector_ref),
                    {
                        **base_row,
                        "sector_ref": sector_ref,
                        "sector_name": sector_name,
                        "relation": "sector_affects_issuer",
                        "evidence_level": "direct",
                        "confidence": "0.78",
                        "direction_hint": "sector_exposure",
                        "source_name": "ades-finance-country-pack-sector-metadata",
                        "notes": ("Issuer sector membership from finance-country pack metadata."),
                    },
                )

        pack_summaries.append(
            {
                "pack_id": pack_id,
                "issuer_count": pack_issuer_count,
                "issuer_sector_row_count": len(issuer_sector_rows) - pack_sector_rows_start,
                "issuer_geography_row_count": (
                    len(issuer_geography_rows) - pack_geography_rows_start
                ),
                "source_snapshot": source_snapshot,
            }
        )

    issuer_sector_row_count = _write_tsv(
        issuer_sector_tsv_path,
        columns=ISSUER_SECTOR_COLUMNS,
        rows=(issuer_sector_rows[key] for key in sorted(issuer_sector_rows)),
    )
    issuer_geography_row_count = _write_tsv(
        issuer_geography_tsv_path,
        columns=ISSUER_GEOGRAPHY_COLUMNS,
        rows=(issuer_geography_rows[key] for key in sorted(issuer_geography_rows)),
    )

    manifest = {
        "schema_version": 1,
        "run_id": resolved_run_id,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "packs_root": str(Path(packs_root).expanduser().resolve()),
        "output_dir": str(output_dir),
        "issuer_sector_tsv_path": str(issuer_sector_tsv_path),
        "issuer_geography_tsv_path": str(issuer_geography_tsv_path),
        "pack_count": len(pack_summaries),
        "issuer_count": issuer_count,
        "ticker_resolved_count": ticker_resolved_count,
        "issuer_sector_row_count": issuer_sector_row_count,
        "issuer_geography_row_count": issuer_geography_row_count,
        "pack_summaries": pack_summaries,
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return FinanceCountrySourceLaneInputDerivationResult(
        run_id=resolved_run_id,
        output_dir=output_dir,
        issuer_sector_tsv_path=issuer_sector_tsv_path,
        issuer_geography_tsv_path=issuer_geography_tsv_path,
        manifest_path=manifest_path,
        pack_count=len(pack_summaries),
        issuer_count=issuer_count,
        ticker_resolved_count=ticker_resolved_count,
        issuer_sector_row_count=issuer_sector_row_count,
        issuer_geography_row_count=issuer_geography_row_count,
    )
