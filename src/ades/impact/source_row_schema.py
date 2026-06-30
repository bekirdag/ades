"""Canonical source-row schema for source-backed relationship lanes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

CANONICAL_SOURCE_ROW_SCHEMA_ID = "ades.source_row.v1"
CANONICAL_SOURCE_ROW_FORMATS = ("tsv", "jsonl")

REVIEW_STATUS_ACCEPTED = "accepted"
REVIEW_STATUS_NEEDS_REVIEW = "needs_review"
REVIEW_STATUS_REJECTED = "rejected"
REVIEW_STATUS_OBSERVED = "observed"
REVIEW_STATUS_VALUES = (
    REVIEW_STATUS_ACCEPTED,
    REVIEW_STATUS_NEEDS_REVIEW,
    REVIEW_STATUS_REJECTED,
    REVIEW_STATUS_OBSERVED,
)


@dataclass(frozen=True)
class SourceRowField:
    name: str
    required_value: bool
    description: str

    def to_json_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "required_value": self.required_value,
            "description": self.description,
        }


CANONICAL_SOURCE_ROW_FIELDS = (
    SourceRowField("source_ref", True, "Stable source entity reference."),
    SourceRowField("source_type", True, "Canonical type for source_ref."),
    SourceRowField("target_ref", True, "Stable target entity reference."),
    SourceRowField("target_type", True, "Canonical type for target_ref."),
    SourceRowField("relation", True, "Relationship predicate between source and target."),
    SourceRowField("jurisdiction", True, "ISO country/region code or global scope."),
    SourceRowField("source_url", True, "Primary URL supporting the relationship row."),
    SourceRowField("source_title", True, "Human-readable source document or page title."),
    SourceRowField("source_tier", True, "Classified provenance tier for the source URL."),
    SourceRowField("evidence", True, "Brief evidence text or evidence pointer."),
    SourceRowField("effective_start_date", True, "ISO-8601 date the relation became effective."),
    SourceRowField("effective_end_date", False, "ISO-8601 date the relation ended, if known."),
    SourceRowField("confidence", True, "Decimal confidence score from 0.0 through 1.0."),
    SourceRowField("notes", False, "Operator notes that do not belong in evidence."),
    SourceRowField("fetched_at", True, "UTC fetch timestamp for the source material."),
    SourceRowField("content_hash", True, "Stable hash of the fetched or normalized source content."),
    SourceRowField("review_status", True, "Review state for promotion gates."),
    SourceRowField("license_notes", False, "License, reuse, or redistribution notes."),
    SourceRowField("confidence_basis", True, "Short reason supporting the confidence score."),
)

CANONICAL_SOURCE_ROW_COLUMNS = tuple(
    field.name for field in CANONICAL_SOURCE_ROW_FIELDS
)
CANONICAL_SOURCE_ROW_REQUIRED_VALUE_COLUMNS = tuple(
    field.name for field in CANONICAL_SOURCE_ROW_FIELDS if field.required_value
)
LEGACY_EDGE_SHARED_COLUMNS = {
    "source_ref",
    "target_ref",
    "relation",
    "confidence",
    "notes",
    "source_url",
}
CANONICAL_SOURCE_ROW_DETECTOR_COLUMNS = tuple(
    column
    for column in CANONICAL_SOURCE_ROW_COLUMNS
    if column not in LEGACY_EDGE_SHARED_COLUMNS
)


def canonical_source_row_schema() -> dict[str, object]:
    """Return the machine-readable schema contract shared by TSV and JSONL rows."""
    return {
        "schema_id": CANONICAL_SOURCE_ROW_SCHEMA_ID,
        "formats": list(CANONICAL_SOURCE_ROW_FORMATS),
        "columns": list(CANONICAL_SOURCE_ROW_COLUMNS),
        "required_value_columns": list(CANONICAL_SOURCE_ROW_REQUIRED_VALUE_COLUMNS),
        "fields": [field.to_json_dict() for field in CANONICAL_SOURCE_ROW_FIELDS],
        "review_status_values": list(REVIEW_STATUS_VALUES),
    }


def row_uses_canonical_source_schema(
    row_or_columns: Mapping[str, object] | tuple[str, ...] | list[str] | None,
) -> bool:
    if row_or_columns is None:
        return False
    columns = set(row_or_columns)
    return bool(columns.intersection(CANONICAL_SOURCE_ROW_DETECTOR_COLUMNS))


def validate_canonical_source_row(row: Mapping[str, object]) -> tuple[str, ...]:
    """Return deterministic schema warnings for one canonical source row."""
    warnings: list[str] = []
    for column in CANONICAL_SOURCE_ROW_COLUMNS:
        if column not in row:
            warnings.append(f"missing_column:{column}")

    for column in CANONICAL_SOURCE_ROW_REQUIRED_VALUE_COLUMNS:
        value = str(row.get(column) or "").strip()
        if not value:
            warnings.append(f"missing_value:{column}")

    confidence = str(row.get("confidence") or "").strip()
    if confidence:
        try:
            parsed_confidence = float(confidence)
        except ValueError:
            warnings.append("invalid_confidence")
        else:
            if parsed_confidence < 0 or parsed_confidence > 1:
                warnings.append("invalid_confidence")

    review_status = str(row.get("review_status") or "").strip().casefold()
    if review_status and review_status not in REVIEW_STATUS_VALUES:
        warnings.append("invalid_review_status")

    return tuple(warnings)
