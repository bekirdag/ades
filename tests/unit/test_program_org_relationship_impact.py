import csv
from pathlib import Path

import pytest

from ades.config import Settings
from ades.impact.expansion import expand_impact_paths
from ades.impact.graph_store import MarketGraphStore
from ades.impact.program_org_relationship import (
    build_program_org_relationship_source_lane,
)
from ades.impact.source_lane_validation import validate_market_graph_source_lanes


REVIEWED_INDONESIA_PNM_BRI_FIXTURE = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/"
    "program_org_relationship/reviewed/2026-06-29/indonesia_pnm_bri_relationships.tsv"
)
REVIEWED_ARGENTINA_BPAT_RIO_NEGRO_FIXTURE = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/"
    "program_org_relationship/reviewed/2026-06-29/"
    "argentina_banco_patagonia_rio_negro_relationships.tsv"
)


def _write_tsv(path: Path, columns: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(columns)
        writer.writerows(rows)


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def test_program_org_relationship_builds_program_holding_issuer_ticker_path(
    tmp_path: Path,
) -> None:
    relationship_path = tmp_path / "program_org_relationships.tsv"
    columns = [
        "source_entity_ref",
        "source_entity_name",
        "source_entity_type",
        "relation",
        "target_entity_ref",
        "target_entity_name",
        "target_entity_type",
        "jurisdiction",
        "source_title",
        "source_url",
        "source_snapshot",
        "source_tier",
        "evidence_text",
        "effective_from",
        "confidence",
        "pack_ids",
        "notes",
    ]
    _write_tsv(
        relationship_path,
        columns,
        [
            [
                "ades:program:id:pnm-mekaar",
                "PNM Mekaar",
                "program",
                "program_operated_by_org",
                "ades:org:id:permodalan-nasional-madani",
                "Permodalan Nasional Madani",
                "organization",
                "ID",
                "PNM annual report",
                "https://www.pnm.co.id/laporan-tahunan",
                "2026-06-29",
                "issuer_disclosed",
                "PNM Mekaar is operated by PNM.",
                "2024-01-01",
                "0.95",
                "finance-id-en",
                "source-backed program operator",
            ],
            [
                "ades:org:id:permodalan-nasional-madani",
                "Permodalan Nasional Madani",
                "organization",
                "org_part_of_holding",
                "ades:holding:id:ultra-micro-holding",
                "Ultra Micro Holding",
                "holding_company",
                "ID",
                "Bank Rakyat Indonesia annual report",
                "https://bri.co.id/en/report",
                "2026-06-29",
                "issuer_disclosed",
                "PNM is part of Ultra Micro Holding.",
                "2024-01-01",
                "0.92",
                "finance-id-en",
                "source-backed holding membership",
            ],
            [
                "ades:holding:id:ultra-micro-holding",
                "Ultra Micro Holding",
                "holding_company",
                "holding_parent_is_issuer",
                "finance-id-issuer:bank-rakyat-indonesia",
                "Bank Rakyat Indonesia",
                "issuer",
                "ID",
                "Bank Rakyat Indonesia annual report",
                "https://bri.co.id/en/report",
                "2026-06-29",
                "issuer_disclosed",
                "BRI is the listed parent in the ultra micro holding structure.",
                "2024-01-01",
                "0.91",
                "finance-id-en",
                "source-backed listed parent",
            ],
            [
                "finance-id-issuer:bank-rakyat-indonesia",
                "Bank Rakyat Indonesia",
                "issuer",
                "issuer_has_listed_ticker",
                "IDX:BBRI",
                "BBRI",
                "ticker",
                "ID",
                "Indonesia Stock Exchange company profile",
                "https://www.idx.co.id/en/listed-companies/company-profiles/BBRI",
                "2026-06-29",
                "exchange",
                "IDX lists Bank Rakyat Indonesia with ticker BBRI.",
                "2024-01-01",
                "0.98",
                "finance-id-en",
                "source-backed listing",
            ],
        ],
    )

    result = build_program_org_relationship_source_lane(
        relationship_tsv_paths=[relationship_path],
        output_root=tmp_path / "impact_relationships",
        run_id="program-org",
        build_artifact=True,
        include_starter_graph=False,
        artifact_output_root=tmp_path / "artifacts",
    )

    assert result.relationship_row_count == 4
    assert result.node_count == 5
    assert result.edge_count == 4
    assert result.artifact_node_count == 5
    assert result.artifact_edge_count == 4
    assert result.relation_counts == {
        "holding_parent_is_issuer": 1,
        "issuer_has_listed_ticker": 1,
        "org_part_of_holding": 1,
        "program_operated_by_org": 1,
    }
    assert result.node_type_counts == {
        "holding_company": 1,
        "issuer": 1,
        "organization": 1,
        "program": 1,
        "ticker": 1,
    }

    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    assert nodes["IDX:BBRI"]["is_tradable"] == "true"
    assert nodes["ades:program:id:pnm-mekaar"]["is_seed_eligible"] == "true"

    edges = _read_tsv(result.edge_tsv_path)
    edge_keys = {(row["source_ref"], row["relation"], row["target_ref"]) for row in edges}
    assert (
        "ades:program:id:pnm-mekaar",
        "program_operated_by_org",
        "ades:org:id:permodalan-nasional-madani",
    ) in edge_keys
    assert (
        "finance-id-issuer:bank-rakyat-indonesia",
        "issuer_has_listed_ticker",
        "IDX:BBRI",
    ) in edge_keys
    program_edge = next(row for row in edges if row["relation"] == "program_operated_by_org")
    assert "PNM Mekaar is operated by PNM" in program_edge["notes"]
    assert "direct_program_product_or_brand_mention" in (
        program_edge["direction_preconditions"].split(",")
    )

    validation = validate_market_graph_source_lanes(edge_tsv_paths=[result.edge_tsv_path])
    assert validation.invalid_row_count == 0
    assert validation.relation_warning_counts == {}
    assert validation.row_count == 4

    assert result.artifact_path is not None
    with MarketGraphStore(result.artifact_path) as store:
        program_edges = store.outbound_edges_batch(["ades:program:id:pnm-mekaar"])[
            "ades:program:id:pnm-mekaar"
        ]
        issuer_edges = store.outbound_edges_batch(["finance-id-issuer:bank-rakyat-indonesia"])[
            "finance-id-issuer:bank-rakyat-indonesia"
        ]

    assert program_edges[0].target_ref == "ades:org:id:permodalan-nasional-madani"
    assert issuer_edges[0].target_ref == "IDX:BBRI"


def test_program_org_relationship_generates_stable_refs_when_missing(
    tmp_path: Path,
) -> None:
    relationship_path = tmp_path / "program_org_relationships.tsv"
    _write_tsv(
        relationship_path,
        [
            "source_entity_name",
            "source_entity_type",
            "relation",
            "target_entity_name",
            "target_entity_type",
            "jurisdiction",
            "source_title",
            "source_url",
            "source_snapshot",
            "evidence_text",
            "pack_ids",
        ],
        [
            [
                "Simpati",
                "product",
                "product_owned_by_org",
                "Telkom Indonesia",
                "issuer",
                "ID",
                "Telkom annual report",
                "https://www.telkom.co.id/sites/about-telkom/en_US/page/ir",
                "2026-06-29",
                "Telkom annual report names Simpati as a Telkom product.",
                "finance-id-en",
            ],
        ],
    )

    result = build_program_org_relationship_source_lane(
        relationship_tsv_paths=[relationship_path],
        output_root=tmp_path / "impact_relationships",
        run_id="stable-refs",
    )

    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    assert "ades:program-org:id:product:simpati" in nodes
    assert "ades:program-org:id:issuer:telkom-indonesia" in nodes
    edges = _read_tsv(result.edge_tsv_path)
    assert edges[0]["relation"] == "product_owned_by_org"


def test_program_org_relationship_requires_evidence_text(
    tmp_path: Path,
) -> None:
    relationship_path = tmp_path / "missing_evidence.tsv"
    _write_tsv(
        relationship_path,
        [
            "source_entity_ref",
            "source_entity_name",
            "source_entity_type",
            "relation",
            "target_entity_ref",
            "target_entity_name",
            "target_entity_type",
            "jurisdiction",
            "source_title",
            "source_url",
            "source_snapshot",
        ],
        [
            [
                "ades:program:ar:test-program",
                "Test Program",
                "program",
                "program_operated_by_org",
                "ades:org:ar:test-agency",
                "Test Agency",
                "organization",
                "AR",
                "Test source",
                "https://rionegro.gov.ar/test",
                "2026-06-29",
            ],
        ],
    )

    with pytest.raises(ValueError, match="evidence_text is required"):
        build_program_org_relationship_source_lane(
            relationship_tsv_paths=[relationship_path],
            output_root=tmp_path / "impact_relationships",
            run_id="missing-evidence",
        )


def test_reviewed_indonesia_pnm_mekaar_fixture_expands_to_bbri_only(
    tmp_path: Path,
) -> None:
    if not REVIEWED_INDONESIA_PNM_BRI_FIXTURE.exists():
        pytest.skip("reviewed Indonesia PNM/BRI source-lane fixture is not mounted")

    result = build_program_org_relationship_source_lane(
        relationship_tsv_paths=[REVIEWED_INDONESIA_PNM_BRI_FIXTURE],
        output_root=tmp_path / "impact_relationships",
        run_id="reviewed-indonesia-pnm-bri",
        build_artifact=True,
        include_starter_graph=False,
        artifact_output_root=tmp_path / "artifacts",
    )

    validation = validate_market_graph_source_lanes(edge_tsv_paths=[result.edge_tsv_path])
    assert validation.invalid_row_count == 0
    assert validation.source_warning_counts == {}
    assert validation.relation_warning_counts == {}

    assert result.artifact_path is not None
    expansion = expand_impact_paths(
        ["ades:product:id:pnm-mekaar"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=4,
        max_candidates=10,
        include_passive_paths=True,
    )

    candidate_refs = {candidate.entity_ref for candidate in expansion.candidates}
    assert "IDX:BBRI" in candidate_refs
    assert "ades:impact:currency:idr" not in candidate_refs

    bbri_candidate = next(
        candidate for candidate in expansion.candidates if candidate.entity_ref == "IDX:BBRI"
    )
    assert bbri_candidate.relationship_paths
    bbri_relations = {
        edge.relation for path in bbri_candidate.relationship_paths for edge in path.edges
    }
    assert "product_owned_by_org" in bbri_relations
    assert "issuer_has_listed_ticker" in bbri_relations


def test_reviewed_argentina_fixture_expands_bpat_and_keeps_program_guardrails(
    tmp_path: Path,
) -> None:
    if not REVIEWED_ARGENTINA_BPAT_RIO_NEGRO_FIXTURE.exists():
        pytest.skip("reviewed Argentina BPAT/Rio Negro fixture is not mounted")

    result = build_program_org_relationship_source_lane(
        relationship_tsv_paths=[REVIEWED_ARGENTINA_BPAT_RIO_NEGRO_FIXTURE],
        output_root=tmp_path / "impact_relationships",
        run_id="reviewed-argentina-bpat-rio-negro",
        build_artifact=True,
        include_starter_graph=False,
        artifact_output_root=tmp_path / "artifacts",
    )

    assert result.relationship_row_count == 10
    assert result.relation_counts == {
        "government_body_affects_sector": 2,
        "issuer_has_listed_ticker": 1,
        "issuer_has_security": 1,
        "issuer_in_sector": 1,
        "product_owned_by_org": 1,
        "program_operated_by_org": 1,
        "regulator_affects_sector": 1,
        "security_listed_on_exchange": 1,
        "ticker_listed_on_exchange": 1,
    }
    assert result.node_type_counts["sector"] == 4
    assert result.node_type_counts["ticker"] == 1

    validation = validate_market_graph_source_lanes(edge_tsv_paths=[result.edge_tsv_path])
    assert validation.invalid_row_count == 0
    assert validation.source_warning_counts == {}
    assert validation.relation_warning_counts == {}
    assert validation.source_tier_counts == {
        "exchange": 5,
        "government": 3,
        "issuer_disclosed": 1,
        "regulator": 1,
    }

    assert result.artifact_path is not None
    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    assert nodes["finance-ar-ticker:BPAT"]["is_tradable"] == "true"
    assert nodes["ades:sector:ar:sme-credit"]["is_tradable"] == "false"

    with MarketGraphStore(result.artifact_path) as store:
        issuer_edges = store.outbound_edges_batch(["ades:issuer:ar:banco-patagonia"])[
            "ades:issuer:ar:banco-patagonia"
        ]
        subsidy_edges = store.outbound_edges_batch(
            ["ades:program:ar:subsidio-tasas-mujeres-emprendedoras-rio-negro"]
        )["ades:program:ar:subsidio-tasas-mujeres-emprendedoras-rio-negro"]
        emprendedores_edges = store.outbound_edges_batch(
            ["ades:program:ar:emprendedores-rio-negro-2026"]
        )["ades:program:ar:emprendedores-rio-negro-2026"]

    issuer_targets = {edge.target_ref for edge in issuer_edges}
    assert "finance-ar-ticker:BPAT" in issuer_targets
    assert "ades:sector:ar:financial-services" in issuer_targets
    assert subsidy_edges[0].target_ref == "ades:org:ar:agencia-crear-rio-negro"
    assert emprendedores_edges == []

    product_expansion = expand_impact_paths(
        ["ades:product:ar:patagonia-emprendimiento"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=3,
        max_candidates=10,
        include_passive_paths=True,
    )
    product_candidate_refs = {candidate.entity_ref for candidate in product_expansion.candidates}
    assert "finance-ar-ticker:BPAT" in product_candidate_refs

    subsidy_expansion = expand_impact_paths(
        ["ades:program:ar:subsidio-tasas-mujeres-emprendedoras-rio-negro"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=3,
        max_candidates=10,
        include_passive_paths=True,
    )
    subsidy_passive_refs = {path.entity_ref for path in subsidy_expansion.passive_paths}
    subsidy_candidate_refs = {candidate.entity_ref for candidate in subsidy_expansion.candidates}
    assert "ades:org:ar:agencia-crear-rio-negro" in subsidy_passive_refs
    assert "ades:sector:ar:sme-credit" in subsidy_passive_refs
    assert "ades:currency:ARS" not in subsidy_candidate_refs
    assert "finance-ar-ticker:BPAT" not in subsidy_candidate_refs
