import csv
from pathlib import Path

from ades.impact.source_lane_validation import validate_market_graph_source_lanes
from ades.impact.source_row_schema import (
    CANONICAL_SOURCE_ROW_COLUMNS,
    CANONICAL_SOURCE_ROW_FORMATS,
    CANONICAL_SOURCE_ROW_REQUIRED_VALUE_COLUMNS,
    canonical_source_row_schema,
    row_uses_canonical_source_schema,
    validate_canonical_source_row,
)


def _canonical_row(**overrides: str) -> dict[str, str]:
    row = {
        "source_ref": "ades:program:de:fcas",
        "source_type": "program",
        "target_ref": "ades:issuer:airbus",
        "target_type": "issuer",
        "relation": "project_participant_org",
        "jurisdiction": "DE",
        "source_url": "https://www.bmwk.de/example",
        "source_title": "FCAS program source",
        "source_tier": "government",
        "evidence": "BMWK describes Airbus as a program participant.",
        "effective_start_date": "2026-06-30",
        "effective_end_date": "",
        "confidence": "0.92",
        "notes": "reviewed lane seed",
        "fetched_at": "2026-06-30T09:00:00Z",
        "content_hash": "sha256:abc123",
        "review_status": "accepted",
        "license_notes": "public sector source",
        "confidence_basis": "official government program page",
    }
    row.update(overrides)
    return row


def _write_tsv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            delimiter="\t",
            fieldnames=CANONICAL_SOURCE_ROW_COLUMNS,
        )
        writer.writeheader()
        writer.writerows(rows)


def test_canonical_source_row_schema_defines_shared_tsv_jsonl_columns() -> None:
    schema = canonical_source_row_schema()

    assert schema["schema_id"] == "ades.source_row.v1"
    assert tuple(schema["formats"]) == CANONICAL_SOURCE_ROW_FORMATS
    assert tuple(schema["columns"]) == CANONICAL_SOURCE_ROW_COLUMNS
    assert CANONICAL_SOURCE_ROW_COLUMNS == (
        "source_ref",
        "source_type",
        "target_ref",
        "target_type",
        "relation",
        "jurisdiction",
        "source_url",
        "source_title",
        "source_tier",
        "evidence",
        "effective_start_date",
        "effective_end_date",
        "confidence",
        "notes",
        "fetched_at",
        "content_hash",
        "review_status",
        "license_notes",
        "confidence_basis",
    )
    assert "effective_end_date" not in CANONICAL_SOURCE_ROW_REQUIRED_VALUE_COLUMNS
    assert "license_notes" not in CANONICAL_SOURCE_ROW_REQUIRED_VALUE_COLUMNS


def test_validate_canonical_source_row_accepts_complete_row() -> None:
    row = _canonical_row()

    assert row_uses_canonical_source_schema(row)
    assert validate_canonical_source_row(row) == ()


def test_validate_canonical_source_row_reports_missing_and_invalid_values() -> None:
    row = _canonical_row(
        source_type="",
        confidence="1.5",
        review_status="pending",
    )
    del row["content_hash"]

    assert validate_canonical_source_row(row) == (
        "missing_column:content_hash",
        "missing_value:source_type",
        "missing_value:content_hash",
        "invalid_confidence",
        "invalid_review_status",
    )


def test_source_lane_validation_counts_canonical_source_row_warnings(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    _write_tsv(
        edge_path,
        [
            _canonical_row(confidence="not-a-number"),
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.row_count == 1
    assert result.invalid_row_count == 0
    assert result.source_tier_counts == {"government": 1}
    assert result.canonical_schema_warning_counts == {"invalid_confidence": 1}
    assert result.warning_samples[0]["scope"] == "canonical_schema"
