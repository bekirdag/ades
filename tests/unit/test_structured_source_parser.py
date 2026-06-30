import csv
import json
from pathlib import Path

from ades.impact.program_org_relationship import build_program_org_relationship_source_lane
from ades.impact.source_lane_validation import validate_market_graph_source_lanes
from ades.impact.structured_source_parser import (
    StructuredSourceSpec,
    parse_structured_official_source,
    write_canonical_source_rows_tsv,
)


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def test_parse_structured_official_csv_into_canonical_source_rows(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "official_relationships.csv"
    source_path.write_text(
        "\n".join(
            [
                ("body_ref,body_name,sector_ref,sector_name,evidence,start_date,review_priority"),
                (
                    "gov:gb:department-for-energy,Department for Energy,"
                    "sector:gb:utilities,Utilities,"
                    "The department administers regulated utility policy,2026-01-01,P1"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = parse_structured_official_source(
        StructuredSourceSpec(
            parser_id="gb-government-sector-v1",
            source_format="csv",
            source_path=source_path,
            column_map={
                "source_ref": "body_ref",
                "source_name": "body_name",
                "source_entity_name": "body_name",
                "target_ref": "sector_ref",
                "target_name": "sector_name",
                "target_entity_name": "sector_name",
                "evidence": "evidence",
                "effective_start_date": "start_date",
            },
            defaults={
                "source_type": "government_body",
                "target_type": "sector",
                "relation": "government_body_affects_sector",
                "jurisdiction": "GB",
                "source_url": "https://www.gov.uk/government/organisations/department-for-energy",
                "source_title": "UK government department register",
                "source_tier": "Government",
                "confidence": "0.93",
                "notes": "official structured register",
                "fetched_at": "2026-06-30T00:00:00Z",
                "review_status": "accepted",
                "license_status": "allowed",
                "license_notes": "Open Government Licence",
                "confidence_basis": "official structured source row",
                "pack_ids": "finance-gb-en",
                "source_snapshot": "2026-06-30",
                "source_year": "2026",
            },
            passthrough_columns=(
                "source_name",
                "source_entity_name",
                "target_name",
                "target_entity_name",
                "pack_ids",
                "source_snapshot",
                "source_year",
                "review_priority",
            ),
        )
    )

    assert result.row_count == 1
    assert result.rejected_row_count == 0
    row = result.rows[0]
    assert row["source_ref"] == "gov:gb:department-for-energy"
    assert row["source_type"] == "government_body"
    assert row["target_ref"] == "sector:gb:utilities"
    assert row["relation"] == "government_body_affects_sector"
    assert row["source_tier"] == "government"
    assert row["review_priority"] == "P1"
    assert row["confidence"] == "0.93"
    assert row["content_hash"].startswith("sha256:")
    assert len(row["content_hash"]) == 71


def test_json_parser_output_feeds_program_org_lane_and_validation(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "official_programs.json"
    source_path.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "program": {
                            "ref": "program:gb:green-homes-grant",
                            "name": "Green Homes Grant",
                        },
                        "sector": {
                            "ref": "sector:gb:homebuilders",
                            "name": "Homebuilders",
                        },
                        "evidence": "Official scheme page lists eligible home improvement sectors.",
                    }
                ]
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    spec = StructuredSourceSpec(
        parser_id="gb-program-sector-v1",
        source_format="json",
        source_path=source_path,
        json_records_path="records",
        column_map={
            "source_ref": "program.ref",
            "source_name": "program.name",
            "source_entity_name": "program.name",
            "target_ref": "sector.ref",
            "target_name": "sector.name",
            "target_entity_name": "sector.name",
            "evidence": "evidence",
        },
        defaults={
            "source_type": "program",
            "target_type": "sector",
            "relation": "policy_program_affects_sector",
            "jurisdiction": "GB",
            "source_url": "https://www.gov.uk/green-homes-grant",
            "source_title": "Green Homes Grant official scheme page",
            "source_tier": "government",
            "effective_start_date": "2026-01-01",
            "effective_from": "2026-01-01",
            "confidence": "0.91",
            "notes": "golden=gb-program-sector",
            "fetched_at": "2026-06-30T00:00:00Z",
            "review_status": "accepted",
            "license_status": "allowed",
            "license_notes": "Open Government Licence",
            "confidence_basis": "official structured source row",
            "pack_ids": "finance-gb-en",
            "source_snapshot": "2026-06-30",
            "source_year": "2026",
        },
        passthrough_columns=(
            "source_name",
            "source_entity_name",
            "target_name",
            "target_entity_name",
            "pack_ids",
            "source_snapshot",
            "source_year",
            "effective_from",
        ),
    )

    result = parse_structured_official_source(spec)
    canonical_path = tmp_path / "program_sector.tsv"
    write_canonical_source_rows_tsv(
        canonical_path,
        result.rows,
        extra_columns=spec.passthrough_columns,
    )

    lane = build_program_org_relationship_source_lane(
        relationship_tsv_paths=[canonical_path],
        output_root=tmp_path / "program_lane",
        run_id="program-lane",
    )
    validation = validate_market_graph_source_lanes(
        edge_tsv_paths=[canonical_path, lane.edge_tsv_path],
        node_tsv_paths=[lane.node_tsv_path],
    )

    assert lane.relation_counts == {"policy_program_affects_sector": 1}
    assert lane.node_type_counts == {"program": 1, "sector": 1}
    assert validation.canonical_schema_warning_counts == {}
    assert validation.production_promotion_warning_counts == {}

    canonical_rows = _read_tsv(canonical_path)
    assert canonical_rows[0]["source_entity_name"] == "Green Homes Grant"
    assert canonical_rows[0]["target_entity_name"] == "Homebuilders"


def test_parser_rejects_low_trust_or_incomplete_rows(tmp_path: Path) -> None:
    source_path = tmp_path / "low_trust.tsv"
    source_path.write_text(
        "source_ref\ttarget_ref\tevidence\n"
        "news:claim-1\tsector:gb:banks\tA search result claimed the relation.\n",
        encoding="utf-8",
    )

    result = parse_structured_official_source(
        StructuredSourceSpec(
            parser_id="low-trust-v1",
            source_format="tsv",
            source_path=source_path,
            column_map={
                "source_ref": "source_ref",
                "target_ref": "target_ref",
                "evidence": "evidence",
            },
            defaults={
                "source_type": "program",
                "target_type": "sector",
                "relation": "policy_program_affects_sector",
                "jurisdiction": "GB",
                "source_url": "https://example.test/search-result",
                "source_title": "Search result",
                "source_tier": "reviewed_proposal",
                "effective_start_date": "2026-01-01",
                "confidence": "0.5",
                "fetched_at": "2026-06-30T00:00:00Z",
                "review_status": "needs_review",
                "confidence_basis": "search result only",
            },
        )
    )

    assert result.row_count == 0
    assert result.rejected_row_count == 1
    assert "structured official parsers require high-trust source_tier" in (
        result.rejected_rows[0].reason
    )
