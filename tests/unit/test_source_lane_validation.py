import csv
from pathlib import Path

from ades.impact.source_lane_validation import validate_market_graph_source_lanes


EDGE_COLUMNS = [
    "source_ref",
    "target_ref",
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
    "compatible_event_types",
    "direction_preconditions",
]
NODE_COLUMNS = [
    "entity_ref",
    "canonical_name",
    "entity_type",
    "library_id",
    "is_tradable",
    "is_seed_eligible",
    "identifiers_json",
    "packs",
]


def _write_edges(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=EDGE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _write_nodes(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=NODE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def test_validate_market_graph_source_lanes_accepts_source_backed_row(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "edges.tsv"
    _write_edges(
        edge_path,
        [
            {
                "source_ref": "ades:uk-law:mining-royalties",
                "target_ref": "ades:sector:mining",
                "relation": "law_affects_sector",
                "evidence_level": "official",
                "confidence": "0.9",
                "direction_hint": "policy_sector",
                "source_name": "UK Parliament",
                "source_url": "https://bills.parliament.uk/bills/example",
                "source_snapshot": "2026-06-28",
                "source_year": "2026",
                "refresh_policy": "monthly",
                "pack_ids": "finance-uk-en",
                "notes": "",
                "compatible_event_types": "sector_policy_change",
                "direction_preconditions": "sector_policy_event_signal",
            }
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.row_count == 1
    assert result.invalid_row_count == 0
    assert result.relation_counts == {"law_affects_sector": 1}
    assert result.source_warning_counts == {}
    assert result.relation_warning_counts == {}
    assert result.source_tier_counts == {"government": 1}


def test_validate_market_graph_source_lanes_counts_source_and_schema_warnings(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "edges.tsv"
    _write_edges(
        edge_path,
        [
            {
                "source_ref": "ades:unknown",
                "target_ref": "LSE:AAL",
                "relation": "unknown_relation",
                "evidence_level": "",
                "confidence": "0.1",
                "direction_hint": "",
                "source_name": "",
                "source_url": "",
                "source_snapshot": "",
                "source_year": "",
                "refresh_policy": "",
                "pack_ids": "",
                "notes": "",
                "compatible_event_types": "",
                "direction_preconditions": "",
            }
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.row_count == 1
    assert result.invalid_row_count == 0
    assert result.source_warning_counts == {
        "missing_source_name": 1,
        "missing_source_snapshot": 1,
        "missing_source_url": 1,
    }
    assert result.relation_warning_counts == {"unknown_relation_schema": 1}
    assert len(result.warning_samples) == 4


def test_validate_market_graph_source_lanes_counts_node_type_warnings(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "edges.tsv"
    node_path = tmp_path / "nodes.tsv"
    _write_edges(
        edge_path,
        [
            {
                "source_ref": "ades:uk-policy:clean-energy",
                "target_ref": "ades:sector:utilities",
                "relation": "policy_body_affects_sector",
                "evidence_level": "official",
                "confidence": "0.9",
                "direction_hint": "policy_sector",
                "source_name": "UK Government",
                "source_url": "https://www.gov.uk/example",
                "source_snapshot": "2026-06-28",
                "source_year": "2026",
                "refresh_policy": "monthly",
                "pack_ids": "finance-uk-en",
                "notes": "",
                "compatible_event_types": "",
                "direction_preconditions": "",
            }
        ],
    )
    _write_nodes(
        node_path,
        [
            {
                "entity_ref": "ades:uk-policy:clean-energy",
                "canonical_name": "Clean Energy Policy",
                "entity_type": "policy",
                "library_id": "finance-uk-en",
                "is_tradable": "false",
                "is_seed_eligible": "true",
                "identifiers_json": "{}",
                "packs": "finance-uk-en",
            },
            {
                "entity_ref": "ades:impact:index:ftse-350",
                "canonical_name": "FTSE 350",
                "entity_type": "market_index",
                "library_id": "finance-uk-en",
                "is_tradable": "true",
                "is_seed_eligible": "false",
                "identifiers_json": "{}",
                "packs": "finance-uk-en",
            },
            {
                "entity_ref": "ades:person:test",
                "canonical_name": "Unsupported Person",
                "entity_type": "person",
                "library_id": "finance-uk-en",
                "is_tradable": "false",
                "is_seed_eligible": "true",
                "identifiers_json": "{}",
                "packs": "finance-uk-en",
            },
        ],
    )

    result = validate_market_graph_source_lanes(
        edge_tsv_paths=[edge_path],
        node_tsv_paths=[node_path],
    )

    assert result.row_count == 4
    assert result.invalid_row_count == 0
    assert result.node_type_counts == {"market_index": 1, "person": 1, "policy": 1}
    assert result.node_warning_counts == {
        "noncanonical_node_type:market_index:index": 1,
        "unknown_node_type_schema:person": 1,
    }
    assert len(result.warning_samples) == 2
