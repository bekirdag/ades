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
CANONICAL_EDGE_COLUMNS = [
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
    "license_status",
    "license_notes",
    "confidence_basis",
    "exchange_ref",
    "exchange_country",
    "priority",
    "fixture_coverage",
    "golden_coverage",
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


def _write_canonical_edges(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=CANONICAL_EDGE_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(_canonical_edge(row))


def _canonical_edge(overrides: dict[str, str]) -> dict[str, str]:
    row = {
        "source_ref": "ades:uk-law:mining-royalties",
        "source_type": "law",
        "target_ref": "ades:sector:mining",
        "target_type": "sector",
        "relation": "law_affects_sector",
        "jurisdiction": "GB",
        "source_url": "https://bills.parliament.uk/bills/example",
        "source_title": "UK Parliament bill",
        "source_tier": "government",
        "evidence": "Official source backs the relationship.",
        "effective_start_date": "2026-01-01",
        "effective_end_date": "",
        "confidence": "0.9",
        "notes": "",
        "fetched_at": "2026-06-28T00:00:00Z",
        "content_hash": "sha256:abc123",
        "review_status": "accepted",
        "license_status": "allowed",
        "license_notes": "public sector source",
        "confidence_basis": "official source page",
        "exchange_ref": "",
        "exchange_country": "",
        "priority": "",
        "fixture_coverage": "",
        "golden_coverage": "",
    }
    row.update(overrides)
    return row


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
                "notes": (
                    "evidence=UK Parliament source backs the relationship. | "
                    "effective_from=2026-01-01 | golden=starter-policy-sector"
                ),
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
    assert result.license_status_counts == {}
    assert result.source_policy_counts == {"official_high_trust": 1}
    assert result.production_eligible_row_count == 1
    assert result.proposal_only_row_count == 0
    assert result.promotion_ready_row_count == 1
    assert result.promotion_blocked_row_count == 0
    assert result.production_promotion_warning_counts == {}


def test_validate_market_graph_source_lanes_distinguishes_source_tier_policy(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "edges.tsv"
    _write_edges(
        edge_path,
        [
            {
                "source_ref": "ades:issuer:tesla",
                "target_ref": "NASDAQ:TSLA",
                "relation": "issuer_has_security",
                "evidence_level": "official",
                "confidence": "0.95",
                "direction_hint": "issuer_security",
                "source_name": "SEC EDGAR Tesla filing",
                "source_url": "https://www.sec.gov/Archives/edgar/data/example",
                "source_snapshot": "2026-06-28",
                "source_year": "2026",
                "refresh_policy": "monthly",
                "pack_ids": "finance-us-en",
                "notes": "",
                "compatible_event_types": "company_specific",
                "direction_preconditions": "direct_issuer_or_security_match",
            },
            {
                "source_ref": "ades:issuer:sample",
                "target_ref": "FIGI:BBG000000001",
                "relation": "issuer_has_security",
                "evidence_level": "licensed",
                "confidence": "0.85",
                "direction_hint": "issuer_security",
                "source_name": "OpenFIGI licensed mapping",
                "source_url": "https://api.openfigi.com/v3/mapping",
                "source_snapshot": "2026-06-28",
                "source_year": "2026",
                "refresh_policy": "monthly",
                "pack_ids": "finance-us-en",
                "notes": "",
                "compatible_event_types": "company_specific",
                "direction_preconditions": "direct_issuer_or_security_match",
            },
            {
                "source_ref": "ades:proposal:program",
                "target_ref": "ades:issuer:sample",
                "relation": "program_operated_by_org",
                "evidence_level": "reviewed_proposal",
                "confidence": "0.65",
                "direction_hint": "program_org",
                "source_name": "Reviewed Proposal Source",
                "source_url": "https://review-queue.internal/proposals/program.jsonl",
                "source_snapshot": "2026-06-28",
                "source_year": "2026",
                "refresh_policy": "manual_review",
                "pack_ids": "reviewed-proposals-en",
                "notes": "",
                "compatible_event_types": "program_funding",
                "direction_preconditions": "program_entity_match",
            },
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.source_tier_counts == {
        "licensed": 1,
        "regulator": 1,
        "reviewed_proposal": 1,
    }
    assert result.source_policy_counts == {
        "official_high_trust": 1,
        "eligible_medium_trust": 1,
        "low_trust_proposal_only": 1,
    }
    assert result.production_eligible_row_count == 2
    assert result.proposal_only_row_count == 1
    assert result.source_warning_counts == {"low_trust_source_proposal_only": 1}


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
    assert result.source_policy_counts == {"low_trust_proposal_only": 1}
    assert result.production_eligible_row_count == 0
    assert result.proposal_only_row_count == 1
    assert result.relation_warning_counts == {"unknown_relation_schema": 1}
    assert result.production_promotion_warning_counts == {
        "missing_effective_date_policy": 1,
        "missing_evidence_text": 1,
        "relation_or_conflict_warnings_present": 1,
        "source_tier_not_production_eligible": 1,
        "source_warnings_present": 1,
    }
    assert len(result.warning_samples) == 9


def test_validate_market_graph_source_lanes_keeps_missing_canonical_tier_proposal_only(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    columns = [
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
        "license_status",
        "license_notes",
        "confidence_basis",
    ]
    with edge_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=columns)
        writer.writeheader()
        writer.writerow(
            {
                "source_ref": "ades:uk-law:mining-royalties",
                "source_type": "law",
                "target_ref": "ades:sector:mining",
                "target_type": "sector",
                "relation": "law_affects_sector",
                "jurisdiction": "GB",
                "source_url": "https://bills.parliament.uk/bills/example",
                "source_title": "UK Parliament bill",
                "source_tier": "",
                "evidence": "Bill text affects mining royalties.",
                "effective_start_date": "2026-06-28",
                "effective_end_date": "",
                "confidence": "0.9",
                "notes": "",
                "fetched_at": "2026-06-28T00:00:00Z",
                "content_hash": "sha256:abc123",
                "review_status": "accepted",
                "license_status": "allowed",
                "license_notes": "public sector source",
                "confidence_basis": "official legislation page",
            }
        )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.canonical_schema_warning_counts == {"missing_value:source_tier": 1}
    assert result.source_tier_counts == {"unknown": 1}
    assert result.source_policy_counts == {"low_trust_proposal_only": 1}
    assert result.production_eligible_row_count == 0
    assert result.proposal_only_row_count == 1
    assert result.source_warning_counts == {}
    assert result.promotion_blocked_row_count == 1
    assert result.production_promotion_warning_counts == {
        "canonical_schema_warnings_present": 1,
        "production_required_fields_missing": 1,
        "source_tier_not_production_eligible": 1,
    }


def test_validate_market_graph_source_lanes_blocks_incomplete_canonical_production_row(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    columns = [
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
        "license_status",
        "license_notes",
        "confidence_basis",
    ]
    with edge_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=columns)
        writer.writeheader()
        writer.writerow(
            {
                "source_ref": "ades:uk-law:mining-royalties",
                "source_type": "law",
                "target_ref": "ades:sector:mining",
                "target_type": "sector",
                "relation": "law_affects_sector",
                "jurisdiction": "GB",
                "source_url": "https://bills.parliament.uk/bills/example",
                "source_title": "UK Parliament bill",
                "source_tier": "government",
                "evidence": "",
                "effective_start_date": "2026-06-28",
                "effective_end_date": "",
                "confidence": "0.9",
                "notes": "",
                "fetched_at": "2026-06-28T00:00:00Z",
                "content_hash": "sha256:abc123",
                "review_status": "accepted",
                "license_status": "allowed",
                "license_notes": "public sector source",
                "confidence_basis": "official legislation page",
            }
        )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.canonical_schema_warning_counts == {"missing_value:evidence": 1}
    assert result.source_tier_counts == {"government": 1}
    assert result.source_policy_counts == {"official_high_trust": 1}
    assert result.production_eligible_row_count == 0
    assert result.proposal_only_row_count == 1
    assert result.promotion_blocked_row_count == 1
    assert result.production_promotion_warning_counts == {
        "canonical_schema_warnings_present": 1,
        "missing_evidence_text": 1,
        "production_required_fields_missing": 1,
    }


def test_validate_market_graph_source_lanes_blocks_unclear_license_status(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    _write_canonical_edges(
        edge_path,
        [
            _canonical_edge(
                {
                    "license_status": "unclear",
                    "license_notes": "Terms require legal review before redistribution.",
                }
            ),
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.source_tier_counts == {"government": 1}
    assert result.license_status_counts == {"unclear": 1}
    assert result.production_eligible_row_count == 0
    assert result.proposal_only_row_count == 1
    assert result.promotion_ready_row_count == 0
    assert result.promotion_blocked_row_count == 1
    assert result.production_promotion_warning_counts == {"source_license_unclear": 1}


def test_validate_market_graph_source_lanes_enforces_production_promotion_rules(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    _write_canonical_edges(
        edge_path,
        [
            _canonical_edge(
                {
                    "priority": "P0",
                    "golden_coverage": "tests/golden/policy_sector.jsonl",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "missing scheme",
                    "source_tier": "reviewed_proposal",
                    "evidence": "",
                    "effective_start_date": "",
                    "priority": "P0",
                }
            ),
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.promotion_ready_row_count == 1
    assert result.promotion_blocked_row_count == 1
    assert result.production_promotion_warning_counts == {
        "canonical_schema_warnings_present": 1,
        "missing_effective_date_policy": 1,
        "missing_evidence_text": 1,
        "missing_fixture_or_golden_coverage": 1,
        "production_required_fields_missing": 1,
        "source_tier_not_production_eligible": 1,
        "unnormalized_source_ref": 1,
    }


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
                "notes": (
                    "evidence=UK Government source backs the relationship. | "
                    "effective_from=2026-06-28"
                ),
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


def test_validate_market_graph_source_lanes_detects_relationship_conflicts(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    _write_canonical_edges(
        edge_path,
        [
            _canonical_edge(
                {
                    "source_ref": "ades:org:operating-company",
                    "source_type": "organization",
                    "target_ref": "ades:holding:alpha",
                    "target_type": "holding_company",
                    "relation": "org_part_of_holding",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:org:operating-company",
                    "source_type": "organization",
                    "target_ref": "ades:holding:beta",
                    "target_type": "holding_company",
                    "relation": "org_part_of_holding",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:alpha",
                    "source_type": "issuer",
                    "target_ref": "NYSE:ABC",
                    "target_type": "ticker",
                    "relation": "issuer_has_listed_ticker",
                    "jurisdiction": "US",
                    "exchange_ref": "NYSE",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:beta",
                    "source_type": "issuer",
                    "target_ref": "NYSE:ABC",
                    "target_type": "ticker",
                    "relation": "issuer_has_listed_ticker",
                    "jurisdiction": "US",
                    "exchange_ref": "NYSE",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:security:abc",
                    "source_type": "security",
                    "target_ref": "NYSE",
                    "target_type": "exchange",
                    "relation": "security_listed_on_exchange",
                    "jurisdiction": "GB",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:program:subsidy",
                    "source_type": "program",
                    "target_ref": "NYSE:XYZ",
                    "target_type": "ticker",
                    "relation": "program_operated_by_org",
                    "jurisdiction": "US",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:sector:builders",
                    "source_type": "sector",
                    "target_ref": "GBP",
                    "target_type": "currency",
                    "relation": "sector_affects_issuer",
                    "jurisdiction": "GB",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:law:expired",
                    "source_type": "law",
                    "target_ref": "ades:sector:energy",
                    "target_type": "sector",
                    "relation": "law_affects_sector",
                    "effective_end_date": "2020-01-01",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:law:blog",
                    "source_type": "law",
                    "target_ref": "ades:sector:retail",
                    "target_type": "sector",
                    "relation": "law_affects_sector",
                    "source_url": "https://randomblog.invalid/example",
                    "source_tier": "government",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:wrong-source-type",
                    "source_type": "currency",
                    "target_ref": "ades:sector:mining",
                    "target_type": "sector",
                    "relation": "law_affects_sector",
                }
            ),
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.source_warning_counts == {
        "low_trust_source_proposal_only": 1,
        "unknown_source_tier": 1,
        "unrecognized_source_host": 1,
    }
    assert result.relation_warning_counts == {
        "active_parent_conflict:ades:org:operating-company": 1,
        "direct_program_to_ticker_shortcut": 1,
        "expired_edge": 1,
        "sector_to_currency_shortcut": 1,
        "ticker_security_conflict:ticker_parent:NYSE:ABC": 1,
        "unsupported_source_node_type:currency:policy|law|regulation|government_body|regulator|"
        "ministry": 1,
        "unsupported_target_node_type:currency:issuer|ticker|security": 1,
        "unsupported_target_node_type:ticker:organization|issuer|legal_entity|holding_company": 1,
        "wrong_exchange_country:NYSE:US:GB": 1,
    }


def test_validate_market_graph_source_lanes_ignores_expired_parent_conflicts(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    _write_canonical_edges(
        edge_path,
        [
            _canonical_edge(
                {
                    "source_ref": "ades:holding:old",
                    "source_type": "holding_company",
                    "target_ref": "ades:org:operating-company",
                    "target_type": "organization",
                    "relation": "org_part_of_holding",
                    "effective_end_date": "2020-01-01",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:holding:current",
                    "source_type": "holding_company",
                    "target_ref": "ades:org:operating-company",
                    "target_type": "organization",
                    "relation": "org_part_of_holding",
                }
            ),
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.relation_warning_counts == {"expired_edge": 1}


def test_validate_market_graph_source_lanes_reports_stale_relationship_audit(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    _write_canonical_edges(
        edge_path,
        [
            _canonical_edge(
                {
                    "source_ref": "ades:org:operating-alpha",
                    "source_type": "organization",
                    "target_ref": "ades:holding:alpha",
                    "target_type": "holding_company",
                    "relation": "org_part_of_holding",
                    "effective_start_date": "2000-01-01",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:org:operating-beta",
                    "source_type": "organization",
                    "target_ref": "ades:holding:beta",
                    "target_type": "holding_company",
                    "relation": "org_part_of_holding",
                    "effective_end_date": "2020-01-01",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:org:operating-gamma",
                    "source_type": "organization",
                    "target_ref": "ades:holding:gamma",
                    "target_type": "holding_company",
                    "relation": "org_part_of_holding",
                    "effective_start_date": "",
                    "effective_end_date": "",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:alpha",
                    "source_type": "issuer",
                    "target_ref": "ades:security:alpha-common",
                    "target_type": "security",
                    "relation": "issuer_has_security",
                    "effective_start_date": "2000-01-01",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:bravo",
                    "source_type": "issuer",
                    "target_ref": "ades:security:bravo-common",
                    "target_type": "security",
                    "relation": "issuer_has_security",
                    "effective_end_date": "2020-01-01",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:beta",
                    "source_type": "issuer",
                    "target_ref": "ades:sector:mining",
                    "target_type": "sector",
                    "relation": "issuer_in_sector",
                    "effective_start_date": "2001-01-01",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:delta",
                    "source_type": "issuer",
                    "target_ref": "ades:sector:banking",
                    "target_type": "sector",
                    "relation": "issuer_in_sector",
                    "effective_end_date": "2020-01-01",
                }
            ),
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])
    payload = result.to_json_dict()

    assert result.stale_relationship_warning_counts == {
        "expired_issuer_relationship": 1,
        "expired_ownership_relationship": 1,
        "expired_security_relationship": 1,
        "missing_effective_date_policy_ownership_relationship": 1,
        "suspiciously_old_issuer_relationship": 1,
        "suspiciously_old_ownership_relationship": 1,
        "suspiciously_old_security_relationship": 1,
    }
    assert result.production_promotion_warning_counts == {
        "canonical_schema_warnings_present": 1,
        "missing_effective_date_policy": 1,
        "production_required_fields_missing": 1,
        "relation_or_conflict_warnings_present": 3,
        "stale_relationship_warnings_present": 7,
    }
    assert payload["stale_relationship_warning_counts"] == (
        result.stale_relationship_warning_counts
    )
    stale_samples = {sample["warning"]: sample for sample in payload["stale_relationship_samples"]}
    assert sorted(stale_samples) == sorted(result.stale_relationship_warning_counts)
    assert stale_samples["suspiciously_old_ownership_relationship"] == {
        "path": str(edge_path.resolve()),
        "line": 2,
        "scope": "stale_relationship",
        "warning": "suspiciously_old_ownership_relationship",
        "source_ref": "ades:org:operating-alpha",
        "target_ref": "ades:holding:alpha",
        "entity_ref": None,
        "entity_type": None,
        "relation": "org_part_of_holding",
        "effective_start": "2000-01-01",
        "effective_end": None,
    }
    assert stale_samples["expired_security_relationship"]["effective_end"] == "2020-01-01"
    assert stale_samples["suspiciously_old_security_relationship"]["effective_start"] == (
        "2000-01-01"
    )
    assert stale_samples["expired_issuer_relationship"]["relation"] == "issuer_in_sector"
    assert (
        stale_samples["missing_effective_date_policy_ownership_relationship"]["effective_start"]
        is None
    )


def test_validate_market_graph_source_lanes_allows_resolved_active_parent_conflict(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    _write_canonical_edges(
        edge_path,
        [
            _canonical_edge(
                {
                    "source_ref": "ades:org:operating-company",
                    "source_type": "organization",
                    "target_ref": "ades:holding:alpha",
                    "target_type": "holding_company",
                    "relation": "org_part_of_holding",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:org:operating-company",
                    "source_type": "organization",
                    "target_ref": "ades:issuer:alpha",
                    "target_type": "issuer",
                    "relation": "org_subsidiary_of_org",
                    "notes": "Conflict resolved: direct subsidiary edge retained alongside holding edge.",
                }
            ),
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.relation_warning_counts == {}


def test_validate_market_graph_source_lanes_detects_security_ticker_exchange_conflicts(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    _write_canonical_edges(
        edge_path,
        [
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:alpha",
                    "source_type": "issuer",
                    "target_ref": "ades:security:alpha-common",
                    "target_type": "security",
                    "relation": "issuer_has_security",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:security:alpha-common",
                    "source_type": "security",
                    "target_ref": "NYSE:AAA",
                    "target_type": "ticker",
                    "relation": "security_has_ticker",
                    "jurisdiction": "US",
                    "exchange_ref": "NYSE",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:security:alpha-common",
                    "source_type": "security",
                    "target_ref": "NYSE:AAB",
                    "target_type": "ticker",
                    "relation": "security_has_ticker",
                    "jurisdiction": "US",
                    "exchange_ref": "NYSE",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:beta",
                    "source_type": "issuer",
                    "target_ref": "NYSE:AAA",
                    "target_type": "ticker",
                    "relation": "issuer_has_listed_ticker",
                    "jurisdiction": "US",
                    "exchange_ref": "NYSE",
                }
            ),
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.relation_warning_counts == {
        "ticker_security_conflict:issuer_mismatch:ades:security:alpha-common:NYSE:AAA": 1,
        "ticker_security_conflict:security_ticker:ades:security:alpha-common": 1,
    }


def test_validate_market_graph_source_lanes_allows_security_tickers_on_different_exchanges(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    _write_canonical_edges(
        edge_path,
        [
            _canonical_edge(
                {
                    "source_ref": "ades:security:alpha-common",
                    "source_type": "security",
                    "target_ref": "NYSE:AAA",
                    "target_type": "ticker",
                    "relation": "security_has_ticker",
                    "jurisdiction": "US",
                    "exchange_ref": "NYSE",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:security:alpha-common",
                    "source_type": "security",
                    "target_ref": "LSE:AAA",
                    "target_type": "ticker",
                    "relation": "security_has_ticker",
                    "jurisdiction": "GB",
                    "exchange_ref": "LSE",
                }
            ),
        ],
    )

    result = validate_market_graph_source_lanes(edge_tsv_paths=[edge_path])

    assert result.relation_warning_counts == {}
