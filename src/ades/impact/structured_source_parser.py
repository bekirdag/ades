"""Deterministic parsers for structured official relationship sources."""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
from typing import Iterable, Mapping

from .source_catalog import (
    OFFICIAL_HIGH_TRUST_SOURCE_TIERS,
    SOURCE_TIER_POLICY_OFFICIAL_HIGH_TRUST,
    source_tier_policy_label,
)
from .source_row_schema import (
    CANONICAL_SOURCE_ROW_COLUMNS,
    validate_canonical_source_row,
)

STRUCTURED_SOURCE_FORMATS = ("csv", "tsv", "json", "jsonl")
OFFICIAL_STRUCTURED_SOURCE_TIERS = tuple(sorted(OFFICIAL_HIGH_TRUST_SOURCE_TIERS))


@dataclass(frozen=True)
class StructuredSourceSpec:
    """Explicit mapping contract for one structured official source snapshot."""

    parser_id: str
    source_format: str
    source_path: str | Path
    column_map: Mapping[str, str]
    defaults: Mapping[str, object] = field(default_factory=dict)
    passthrough_columns: tuple[str, ...] = ()
    json_records_path: str | None = None


@dataclass(frozen=True)
class StructuredSourceRejectedRow:
    source_path: Path
    row_number: int
    reason: str
    raw_row: Mapping[str, object]

    def to_json_dict(self) -> dict[str, object]:
        return {
            "source_path": str(self.source_path),
            "row_number": self.row_number,
            "reason": self.reason,
            "raw_row": dict(self.raw_row),
        }


@dataclass(frozen=True)
class StructuredSourceParseResult:
    parser_id: str
    source_path: Path
    rows: tuple[dict[str, str], ...]
    rejected_rows: tuple[StructuredSourceRejectedRow, ...]

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def rejected_row_count(self) -> int:
        return len(self.rejected_rows)

    def to_json_dict(self) -> dict[str, object]:
        return {
            "parser_id": self.parser_id,
            "source_path": str(self.source_path),
            "row_count": self.row_count,
            "rejected_row_count": self.rejected_row_count,
            "rejected_rows": [row.to_json_dict() for row in self.rejected_rows],
        }


def _normalize_text(value: object | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).strip()


def _read_nested_value(record: Mapping[str, object], key_path: str) -> object | None:
    current: object | None = record
    for segment in key_path.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(segment)
    return current


def _canonical_hash(payload: object) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"sha256:{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"


def _source_format(spec: StructuredSourceSpec) -> str:
    source_format = spec.source_format.strip().casefold()
    if source_format not in STRUCTURED_SOURCE_FORMATS:
        raise ValueError(f"Unsupported structured source format: {spec.source_format!r}")
    return source_format


def _load_tabular_rows(path: Path, *, delimiter: str) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle, delimiter=delimiter)]


def _load_jsonl_rows(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            parsed = json.loads(stripped)
            if not isinstance(parsed, dict):
                raise ValueError(f"JSONL row {index} is not an object: {path}")
            rows.append(parsed)
    return rows


def _json_records(payload: object, records_path: str | None) -> object:
    if not records_path:
        return payload
    current = payload
    for segment in records_path.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(segment)
    return current


def _load_json_rows(path: Path, *, records_path: str | None) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = _json_records(payload, records_path)
    if not isinstance(records, list):
        raise ValueError(f"JSON source does not contain a record list: {path}")
    rows: list[dict[str, object]] = []
    for index, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(f"JSON record {index} is not an object: {path}")
        rows.append(record)
    return rows


def _load_structured_rows(spec: StructuredSourceSpec) -> list[dict[str, object]]:
    path = Path(spec.source_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(path)
    source_format = _source_format(spec)
    if source_format == "csv":
        return _load_tabular_rows(path, delimiter=",")
    if source_format == "tsv":
        return _load_tabular_rows(path, delimiter="\t")
    if source_format == "jsonl":
        return _load_jsonl_rows(path)
    return _load_json_rows(path, records_path=spec.json_records_path)


def _mapped_value(
    record: Mapping[str, object],
    *,
    output_column: str,
    spec: StructuredSourceSpec,
) -> str:
    if output_column in spec.column_map:
        return _normalize_text(_read_nested_value(record, spec.column_map[output_column]))
    if output_column in record:
        return _normalize_text(record.get(output_column))
    return _normalize_text(spec.defaults.get(output_column))


def _normalize_confidence(row: dict[str, str]) -> None:
    raw = row.get("confidence", "")
    if not raw:
        return
    parsed = float(raw)
    if parsed < 0 or parsed > 1:
        raise ValueError("invalid_confidence")
    row["confidence"] = f"{parsed:.4g}"


def _normalize_review_status(row: dict[str, str]) -> None:
    if row.get("review_status"):
        row["review_status"] = row["review_status"].casefold()


def _normalize_source_tier(row: dict[str, str]) -> None:
    source_tier = _normalize_text(row.get("source_tier")).casefold()
    if source_tier not in OFFICIAL_HIGH_TRUST_SOURCE_TIERS:
        policy = source_tier_policy_label(source_tier)
        raise ValueError(
            "structured official parsers require high-trust source_tier; "
            f"got {source_tier or '<empty>'} ({policy})"
        )
    if source_tier_policy_label(source_tier) != SOURCE_TIER_POLICY_OFFICIAL_HIGH_TRUST:
        raise ValueError(f"source_tier is not official high-trust: {source_tier}")
    row["source_tier"] = source_tier


def _build_source_row(
    record: Mapping[str, object],
    *,
    spec: StructuredSourceSpec,
) -> dict[str, str]:
    row = {
        column: _mapped_value(record, output_column=column, spec=spec)
        for column in CANONICAL_SOURCE_ROW_COLUMNS
    }
    for column in spec.passthrough_columns:
        if column in row:
            continue
        row[column] = _mapped_value(record, output_column=column, spec=spec)
    if not row.get("content_hash"):
        row["content_hash"] = _canonical_hash(
            {
                "parser_id": spec.parser_id,
                "source_path": str(Path(spec.source_path).expanduser()),
                "record": record,
            }
        )
    _normalize_confidence(row)
    _normalize_review_status(row)
    _normalize_source_tier(row)
    return row


def parse_structured_official_source(
    spec: StructuredSourceSpec,
) -> StructuredSourceParseResult:
    """Parse one official structured source into canonical source rows.

    The parser only reads CSV, TSV, JSON, or JSONL from disk and only uses the
    explicit field mappings/defaults supplied in ``spec``.
    """

    path = Path(spec.source_path).expanduser().resolve()
    rows: list[dict[str, str]] = []
    rejected_rows: list[StructuredSourceRejectedRow] = []
    for row_number, record in enumerate(_load_structured_rows(spec), start=2):
        try:
            row = _build_source_row(record, spec=spec)
            warnings = validate_canonical_source_row(row)
            if warnings:
                rejected_rows.append(
                    StructuredSourceRejectedRow(
                        source_path=path,
                        row_number=row_number,
                        reason=";".join(warnings),
                        raw_row=record,
                    )
                )
                continue
        except (TypeError, ValueError) as exc:
            rejected_rows.append(
                StructuredSourceRejectedRow(
                    source_path=path,
                    row_number=row_number,
                    reason=str(exc),
                    raw_row=record,
                )
            )
            continue
        rows.append(row)

    return StructuredSourceParseResult(
        parser_id=spec.parser_id,
        source_path=path,
        rows=tuple(rows),
        rejected_rows=tuple(rejected_rows),
    )


def write_canonical_source_rows_tsv(
    path: str | Path,
    rows: Iterable[Mapping[str, object]],
    *,
    extra_columns: Iterable[str] = (),
) -> int:
    """Write canonical source rows with stable column ordering."""

    resolved_path = Path(path).expanduser().resolve()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    extras = tuple(
        column
        for column in dict.fromkeys(extra_columns)
        if column not in CANONICAL_SOURCE_ROW_COLUMNS
    )
    columns = [*CANONICAL_SOURCE_ROW_COLUMNS, *extras]
    count = 0
    with resolved_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            delimiter="\t",
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _normalize_text(row.get(column)) for column in columns})
            count += 1
    return count
