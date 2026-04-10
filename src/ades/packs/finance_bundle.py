"""Finance-specific source-bundle builders for real-content pack generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import csv
import json
from pathlib import Path
import shutil
from typing import Any

from .source_lock import build_bundle_source_entry, write_sources_lock

_FINANCE_STOPLISTED_ALIASES = ["N/A", "ON", "USD"]
_ISSUER_BLOCKED_ALIAS_MAP = {
    "nasdaq, inc.": ["NASDAQ"],
}


@dataclass(frozen=True)
class FinanceSourceBundleResult:
    """Summary of one generated `finance-en` normalized source bundle."""

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
    sec_issuer_count: int
    symbol_count: int
    curated_entity_count: int
    warnings: list[str] = field(default_factory=list)


def build_finance_source_bundle(
    *,
    sec_companies_path: str | Path,
    symbol_directory_path: str | Path,
    curated_entities_path: str | Path,
    output_dir: str | Path,
    version: str = "0.2.0",
) -> FinanceSourceBundleResult:
    """Build a normalized `finance-en` source bundle from raw snapshot files."""

    sec_path = _resolve_input_file(
        sec_companies_path,
        label="SEC company tickers snapshot",
    )
    symbol_path = _resolve_input_file(
        symbol_directory_path,
        label="symbol directory snapshot",
    )
    curated_path = _resolve_input_file(
        curated_entities_path,
        label="curated finance entities snapshot",
    )
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    bundle_dir = resolved_output_dir / "finance-en-bundle"
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    sec_issuers = _load_sec_company_issuers(sec_path)
    symbol_entities = _load_symbol_directory_tickers(symbol_path)
    curated_entities = _load_curated_finance_entities(curated_path)
    rules = _build_finance_rule_records()

    entities = sorted(
        [*sec_issuers, *symbol_entities, *curated_entities],
        key=lambda item: (
            str(item["entity_type"]).casefold(),
            str(item["canonical_text"]).casefold(),
            str(item["entity_id"]).casefold(),
        ),
    )

    entities_path = normalized_dir / "entities.jsonl"
    rules_path = normalized_dir / "rules.jsonl"
    bundle_manifest_path = bundle_dir / "bundle.json"
    sources_lock_path = bundle_dir / "sources.lock.json"
    generated_at = _utc_timestamp()

    _write_jsonl(entities_path, entities)
    _write_jsonl(rules_path, rules)

    sources = [
        build_bundle_source_entry(
            name="sec-company-tickers",
            snapshot_path=sec_path,
            record_count=len(sec_issuers),
            adapter="sec_company_tickers_json",
        ),
        build_bundle_source_entry(
            name="symbol-directory",
            snapshot_path=symbol_path,
            record_count=len(symbol_entities),
            adapter="delimited_symbol_directory",
        ),
        build_bundle_source_entry(
            name="curated-finance-entities",
            snapshot_path=curated_path,
            record_count=len(curated_entities),
            adapter="curated_finance_entities",
        ),
    ]
    bundle_manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": "finance-en",
                "version": version,
                "language": "en",
                "domain": "finance",
                "tier": "domain",
                "description": "Generated English finance entities and deterministic rules.",
                "tags": ["finance", "generated", "english"],
                "dependencies": [],
                "entities_path": "normalized/entities.jsonl",
                "rules_path": "normalized/rules.jsonl",
                "label_mappings": {
                    "issuer": "organization",
                    "ticker": "ticker",
                    "exchange": "exchange",
                },
                "stoplisted_aliases": _FINANCE_STOPLISTED_ALIASES,
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
        pack_id="finance-en",
        version=version,
        entities_path="normalized/entities.jsonl",
        rules_path="normalized/rules.jsonl",
        sources=sources,
    )

    warnings: list[str] = []
    if not sec_issuers:
        warnings.append("SEC snapshot produced no issuer entities.")
    if not symbol_entities:
        warnings.append("Symbol directory snapshot produced no ticker entities.")
    if not curated_entities:
        warnings.append("Curated entity snapshot produced no finance entities.")

    return FinanceSourceBundleResult(
        pack_id="finance-en",
        version=version,
        output_dir=str(resolved_output_dir),
        bundle_dir=str(bundle_dir),
        bundle_manifest_path=str(bundle_manifest_path),
        sources_lock_path=str(sources_lock_path),
        entities_path=str(entities_path),
        rules_path=str(rules_path),
        generated_at=generated_at,
        source_count=len(sources),
        entity_record_count=len(entities),
        rule_record_count=len(rules),
        sec_issuer_count=len(sec_issuers),
        symbol_count=len(symbol_entities),
        curated_entity_count=len(curated_entities),
        warnings=warnings,
    )


def _load_sec_company_issuers(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        raw_records = list(payload.values())
    elif isinstance(payload, list):
        raw_records = payload
    else:
        raise ValueError(f"Unsupported SEC snapshot structure: {path}")

    records: list[dict[str, Any]] = []
    for item in raw_records:
        if not isinstance(item, dict):
            continue
        canonical_text = _clean_text(
            item.get("title") or item.get("name") or item.get("issuer")
        )
        if not canonical_text:
            continue
        cik = item.get("cik_str") or item.get("cik")
        source_id = str(int(cik)).zfill(10) if cik is not None else canonical_text.casefold()
        metadata: dict[str, Any] = {}
        ticker = _clean_text(item.get("ticker"))
        if ticker:
            metadata["ticker"] = ticker
        record = {
            "entity_id": f"sec-cik:{source_id}",
            "entity_type": "issuer",
            "canonical_text": canonical_text,
            "aliases": [],
            "source_name": "sec-company-tickers",
            "source_id": source_id,
            "metadata": metadata,
        }
        blocked_aliases = _ISSUER_BLOCKED_ALIAS_MAP.get(canonical_text.casefold(), [])
        if blocked_aliases:
            record["blocked_aliases"] = list(blocked_aliases)
        records.append(record)
    return records


def _load_symbol_directory_tickers(path: Path) -> list[dict[str, Any]]:
    first_line = path.read_text(encoding="utf-8").splitlines()[0]
    delimiter = "|" if "|" in first_line else ","
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        records: list[dict[str, Any]] = []
        for row in reader:
            symbol = _clean_text(
                row.get("Symbol")
                or row.get("symbol")
                or row.get("Ticker")
            )
            if not symbol:
                continue
            if _truthy_like(row.get("Test Issue") or row.get("TestIssue")):
                continue
            security_name = _clean_text(
                row.get("Security Name")
                or row.get("security_name")
                or row.get("Name")
            )
            aliases = [security_name] if security_name else []
            records.append(
                {
                    "entity_id": f"ticker:{symbol.upper()}",
                    "entity_type": "ticker",
                    "canonical_text": symbol.upper(),
                    "aliases": aliases,
                    "source_name": "symbol-directory",
                    "source_id": symbol.upper(),
                }
            )
    return records


def _load_curated_finance_entities(path: Path) -> list[dict[str, Any]]:
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
        entity_type = _clean_text(item.get("entity_type") or item.get("label"))
        if not canonical_text or not entity_type:
            continue
        aliases = item.get("aliases", [])
        if aliases is None:
            aliases = []
        if not isinstance(aliases, list):
            raise ValueError("Curated finance entity aliases must be a list.")
        normalized.append(
            {
                "entity_id": str(
                    item.get("entity_id")
                    or f"curated:{entity_type.casefold()}:{_slugify(canonical_text)}"
                ),
                "entity_type": entity_type,
                "canonical_text": canonical_text,
                "aliases": [_clean_text(alias) for alias in aliases if _clean_text(alias)],
                "source_name": str(item.get("source_name") or "curated-finance-entities"),
                "source_id": str(item.get("source_id") or canonical_text),
            }
        )
    return normalized


def _build_finance_rule_records() -> list[dict[str, str]]:
    return [
        {
            "name": "currency_amount",
            "label": "currency_amount",
            "kind": "regex",
            "pattern": r"(USD|EUR|GBP|TRY)\s?[0-9]+(?:\.[0-9]+)?",
        },
        {
            "name": "ticker_symbol",
            "label": "ticker",
            "kind": "regex",
            "pattern": r"\$[A-Z]{1,5}",
        },
    ]


def _resolve_input_file(path: str | Path, *, label: str) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"{label} not found: {resolved}")
    if not resolved.is_file():
        raise FileExistsError(f"{label} is not a file: {resolved}")
    return resolved


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


def _truthy_like(value: Any) -> bool:
    if value is None:
        return False
    return _clean_text(value).lower() in {"1", "true", "y", "yes"}


def _slugify(value: str) -> str:
    return "".join(character.lower() if character.isalnum() else "-" for character in value).strip("-")


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
