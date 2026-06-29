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
REVIEWED_AUSTRALIA_ASX_REGULATOR_PROGRAM_FIXTURE = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/"
    "program_org_relationship/reviewed/2026-06-29/"
    "australia_asx_regulator_program_relationships.tsv"
)
REVIEWED_BRAZIL_B3_BNDES_TRANSPORT_FIXTURE = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/"
    "program_org_relationship/reviewed/2026-06-29/"
    "brazil_b3_bndes_transport_relationships.tsv"
)
REVIEWED_CANADA_TMX_BOC_INFRASTRUCTURE_FIXTURE = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/"
    "program_org_relationship/reviewed/2026-06-29/"
    "canada_tmx_boc_infrastructure_relationships.tsv"
)
REVIEWED_CHINA_SSE_SZSE_PBOC_POLICY_FIXTURE = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/"
    "program_org_relationship/reviewed/2026-06-29/"
    "china_sse_szse_pboc_policy_relationships.tsv"
)
REVIEWED_GERMANY_BOERSE_BAFIN_BMWK_FCAS_FIXTURE = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/"
    "program_org_relationship/reviewed/2026-06-29/"
    "germany_boerse_bafin_bmwk_fcas_relationships.tsv"
)
REVIEWED_INDIA_NSE_BSE_RBI_POLICY_FIXTURE = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/"
    "program_org_relationship/reviewed/2026-06-29/"
    "india_nse_bse_rbi_policy_relationships.tsv"
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
    assert program_edge["confidence"] == "0.95"
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


def test_reviewed_australia_fixture_expands_asx_and_keeps_program_guardrails(
    tmp_path: Path,
) -> None:
    if not REVIEWED_AUSTRALIA_ASX_REGULATOR_PROGRAM_FIXTURE.exists():
        pytest.skip("reviewed Australia ASX/regulator/program fixture is not mounted")

    result = build_program_org_relationship_source_lane(
        relationship_tsv_paths=[REVIEWED_AUSTRALIA_ASX_REGULATOR_PROGRAM_FIXTURE],
        output_root=tmp_path / "impact_relationships",
        run_id="reviewed-australia-asx-regulator-programs",
        build_artifact=True,
        include_starter_graph=False,
        artifact_output_root=tmp_path / "artifacts",
    )

    assert result.relationship_row_count == 30
    assert result.relation_counts == {
        "government_body_affects_sector": 12,
        "issuer_has_listed_ticker": 2,
        "issuer_in_sector": 2,
        "program_operated_by_org": 3,
        "regulator_affects_sector": 6,
        "sector_affects_index": 1,
        "sector_affects_issuer": 2,
        "ticker_listed_on_exchange": 2,
    }
    assert result.node_type_counts["market_index"] == 1
    assert result.node_type_counts["ticker"] == 2

    validation = validate_market_graph_source_lanes(edge_tsv_paths=[result.edge_tsv_path])
    assert validation.invalid_row_count == 0
    assert validation.source_warning_counts == {}
    assert validation.relation_warning_counts == {}
    assert validation.source_tier_counts == {
        "exchange": 9,
        "government": 15,
        "regulator": 6,
    }

    assert result.artifact_path is not None
    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    assert nodes["finance-au-ticker:CBA"]["is_tradable"] == "true"
    assert nodes["finance-au-ticker:BHP"]["is_tradable"] == "true"
    assert nodes["finance-au:asx-200"]["is_tradable"] == "true"
    assert nodes["ades:program:au:fuel-tax-credits"]["is_seed_eligible"] == "true"
    assert nodes["ades:sector:au:fuel-retail"]["is_tradable"] == "false"

    with MarketGraphStore(result.artifact_path) as store:
        cba_edges = store.outbound_edges_batch(["ades:issuer:au:commonwealth-bank-of-australia"])[
            "ades:issuer:au:commonwealth-bank-of-australia"
        ]
        bhp_edges = store.outbound_edges_batch(["ades:issuer:au:bhp-group"])[
            "ades:issuer:au:bhp-group"
        ]
        fuel_edges = store.outbound_edges_batch(["ades:program:au:fuel-tax-credits"])[
            "ades:program:au:fuel-tax-credits"
        ]

    assert {edge.target_ref for edge in cba_edges} == {
        "ades:sector:au:banking",
        "finance-au-ticker:CBA",
    }
    assert {edge.target_ref for edge in bhp_edges} == {
        "ades:sector:au:materials",
        "finance-au-ticker:BHP",
    }
    assert [edge.target_ref for edge in fuel_edges] == ["ades:org:au:ato"]

    cba_expansion = expand_impact_paths(
        ["ades:issuer:au:commonwealth-bank-of-australia"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=2,
        max_candidates=10,
        include_passive_paths=True,
    )
    cba_candidate_refs = {candidate.entity_ref for candidate in cba_expansion.candidates}
    assert "finance-au-ticker:CBA" in cba_candidate_refs
    assert "ades:currency:AUD" not in cba_candidate_refs

    apra_expansion = expand_impact_paths(
        ["ades:org:au:apra"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=4,
        max_candidates=10,
        include_passive_paths=True,
    )
    apra_candidate_refs = {candidate.entity_ref for candidate in apra_expansion.candidates}
    assert "finance-au-ticker:CBA" in apra_candidate_refs
    assert "ades:currency:AUD" not in apra_candidate_refs

    fuel_expansion = expand_impact_paths(
        ["ades:program:au:fuel-tax-credits"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=3,
        max_candidates=10,
        include_passive_paths=True,
    )
    fuel_passive_refs = {path.entity_ref for path in fuel_expansion.passive_paths}
    fuel_candidate_refs = {candidate.entity_ref for candidate in fuel_expansion.candidates}
    assert "ades:org:au:ato" in fuel_passive_refs
    assert "ades:sector:au:fuel-retail" in fuel_passive_refs
    assert "ades:sector:au:transport-logistics" in fuel_passive_refs
    assert "finance-au-ticker:BHP" not in fuel_candidate_refs
    assert "finance-au:asx-200" not in fuel_candidate_refs
    assert "ades:currency:AUD" not in fuel_candidate_refs

    housing_expansion = expand_impact_paths(
        ["ades:program:au:five-percent-deposit-scheme"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=3,
        max_candidates=10,
        include_passive_paths=True,
    )
    housing_passive_refs = {path.entity_ref for path in housing_expansion.passive_paths}
    housing_candidate_refs = {candidate.entity_ref for candidate in housing_expansion.candidates}
    assert "ades:org:au:housing-australia" in housing_passive_refs
    assert "ades:sector:au:housing-credit" in housing_passive_refs
    assert "finance-au-ticker:CBA" not in housing_candidate_refs
    assert "ades:currency:AUD" not in housing_candidate_refs

    equity_market_expansion = expand_impact_paths(
        ["ades:sector:au:australian-equity-market"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=2,
        max_candidates=10,
        include_passive_paths=True,
    )
    equity_candidate_refs = {
        candidate.entity_ref for candidate in equity_market_expansion.candidates
    }
    assert "finance-au:asx-200" in equity_candidate_refs
    assert "ades:currency:AUD" not in equity_candidate_refs


def test_reviewed_brazil_fixture_expands_b3_and_keeps_project_guardrails(
    tmp_path: Path,
) -> None:
    if not REVIEWED_BRAZIL_B3_BNDES_TRANSPORT_FIXTURE.exists():
        pytest.skip("reviewed Brazil B3/BNDES/transport fixture is not mounted")

    result = build_program_org_relationship_source_lane(
        relationship_tsv_paths=[REVIEWED_BRAZIL_B3_BNDES_TRANSPORT_FIXTURE],
        output_root=tmp_path / "impact_relationships",
        run_id="reviewed-brazil-b3-bndes-transport",
        build_artifact=True,
        include_starter_graph=False,
        artifact_output_root=tmp_path / "artifacts",
    )

    assert result.relationship_row_count == 47
    assert result.relation_counts == {
        "government_body_affects_sector": 8,
        "infrastructure_project_affects_sector": 6,
        "issuer_has_listed_ticker": 6,
        "issuer_in_sector": 5,
        "org_in_sector": 1,
        "program_loan_recipient_org": 1,
        "program_operated_by_org": 2,
        "regulator_affects_sector": 7,
        "sector_affects_index": 1,
        "sector_affects_issuer": 4,
        "ticker_listed_on_exchange": 6,
    }
    assert result.node_type_counts == {
        "exchange": 1,
        "government_body": 4,
        "issuer": 5,
        "market_index": 1,
        "organization": 1,
        "program": 2,
        "project": 2,
        "regulator": 5,
        "sector": 16,
        "ticker": 6,
    }

    validation = validate_market_graph_source_lanes(edge_tsv_paths=[result.edge_tsv_path])
    assert validation.invalid_row_count == 0
    assert validation.source_warning_counts == {}
    assert validation.relation_warning_counts == {}
    assert validation.source_tier_counts == {
        "exchange": 22,
        "government": 18,
        "regulator": 7,
    }

    assert result.artifact_path is not None
    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    assert nodes["finance-br-ticker:PETR3"]["is_tradable"] == "true"
    assert nodes["finance-br-ticker:PETR4"]["is_tradable"] == "true"
    assert nodes["finance-br-ticker:RAIL3"]["is_tradable"] == "true"
    assert nodes["finance-br:ibovespa"]["is_tradable"] == "true"
    assert nodes["ades:project:br:ferrograo"]["is_seed_eligible"] == "true"
    assert nodes["ades:sector:br:rail-infrastructure"]["is_tradable"] == "false"

    with MarketGraphStore(result.artifact_path) as store:
        petrobras_edges = store.outbound_edges_batch(["ades:issuer:br:petrobras"])[
            "ades:issuer:br:petrobras"
        ]
        ferrograo_edges = store.outbound_edges_batch(["ades:project:br:ferrograo"])[
            "ades:project:br:ferrograo"
        ]
        bndes_program_edges = store.outbound_edges_batch(["ades:program:br:bndes-mais-inovacao"])[
            "ades:program:br:bndes-mais-inovacao"
        ]

    assert {edge.target_ref for edge in petrobras_edges} == {
        "ades:sector:br:oil-gas",
        "finance-br-ticker:PETR3",
        "finance-br-ticker:PETR4",
    }
    assert {edge.target_ref for edge in ferrograo_edges} == {
        "ades:sector:br:agriculture-export-logistics",
        "ades:sector:br:rail-infrastructure",
        "ades:sector:br:transport-logistics",
    }
    assert {edge.target_ref for edge in bndes_program_edges} == {
        "ades:org:br:bndes",
        "ades:org:br:zilia-technologies",
    }

    petrobras_expansion = expand_impact_paths(
        ["ades:issuer:br:petrobras"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=2,
        max_candidates=10,
        include_passive_paths=True,
    )
    petrobras_candidate_refs = {
        candidate.entity_ref for candidate in petrobras_expansion.candidates
    }
    assert {"finance-br-ticker:PETR3", "finance-br-ticker:PETR4"}.issubset(petrobras_candidate_refs)
    assert "ades:currency:BRL" not in petrobras_candidate_refs

    for issuer_ref, ticker_ref in [
        ("ades:issuer:br:vale", "finance-br-ticker:VALE3"),
        ("ades:issuer:br:banco-do-brasil", "finance-br-ticker:BBAS3"),
        ("ades:issuer:br:rumo", "finance-br-ticker:RAIL3"),
    ]:
        expansion = expand_impact_paths(
            [issuer_ref],
            artifact_path=result.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=2,
            max_candidates=10,
            include_passive_paths=True,
        )
        candidate_refs = {candidate.entity_ref for candidate in expansion.candidates}
        assert ticker_ref in candidate_refs
        assert "ades:currency:BRL" not in candidate_refs

    bndes_expansion = expand_impact_paths(
        ["ades:program:br:bndes-mais-inovacao"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=4,
        max_candidates=10,
        include_passive_paths=True,
    )
    bndes_passive_refs = {path.entity_ref for path in bndes_expansion.passive_paths}
    bndes_candidate_refs = {candidate.entity_ref for candidate in bndes_expansion.candidates}
    assert "ades:org:br:bndes" in bndes_passive_refs
    assert "ades:org:br:zilia-technologies" in bndes_passive_refs
    assert "ades:sector:br:semiconductors" in bndes_passive_refs
    assert "finance-br-ticker:RAIL3" not in bndes_candidate_refs
    assert "ades:currency:BRL" not in bndes_candidate_refs

    for project_ref in ["ades:project:br:ferrograo", "ades:project:br:fico-railway"]:
        project_expansion = expand_impact_paths(
            [project_ref],
            artifact_path=result.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=3,
            max_candidates=10,
            include_passive_paths=True,
        )
        project_passive_refs = {path.entity_ref for path in project_expansion.passive_paths}
        project_candidate_refs = {
            candidate.entity_ref for candidate in project_expansion.candidates
        }
        assert "ades:sector:br:rail-infrastructure" in project_passive_refs
        assert "ades:sector:br:transport-logistics" in project_passive_refs
        assert "ades:sector:br:agriculture-export-logistics" in project_passive_refs
        assert "finance-br-ticker:RAIL3" not in project_candidate_refs
        assert "finance-br:ibovespa" not in project_candidate_refs
        assert "ades:currency:BRL" not in project_candidate_refs

    equity_market_expansion = expand_impact_paths(
        ["ades:sector:br:brazilian-equity-market"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=2,
        max_candidates=10,
        include_passive_paths=True,
    )
    equity_candidate_refs = {
        candidate.entity_ref for candidate in equity_market_expansion.candidates
    }
    assert "finance-br:ibovespa" in equity_candidate_refs
    assert "ades:currency:BRL" not in equity_candidate_refs

    anp_expansion = expand_impact_paths(
        ["ades:org:br:anp"],
        artifact_path=result.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=4,
        max_candidates=10,
        include_passive_paths=True,
    )
    anp_candidate_refs = {candidate.entity_ref for candidate in anp_expansion.candidates}
    assert {"finance-br-ticker:PETR3", "finance-br-ticker:PETR4"}.issubset(anp_candidate_refs)
    assert "ades:currency:BRL" not in anp_candidate_refs


def test_reviewed_canada_fixture_expands_tmx_and_keeps_policy_guardrails(
    tmp_path: Path,
) -> None:
    if not REVIEWED_CANADA_TMX_BOC_INFRASTRUCTURE_FIXTURE.exists():
        pytest.skip("reviewed Canada TMX/BoC/infrastructure fixture is not mounted")

    result = build_program_org_relationship_source_lane(
        relationship_tsv_paths=[REVIEWED_CANADA_TMX_BOC_INFRASTRUCTURE_FIXTURE],
        output_root=tmp_path / "impact_relationships",
        run_id="reviewed-canada-tmx-boc-infrastructure",
        build_artifact=True,
        include_starter_graph=False,
        artifact_output_root=tmp_path / "artifacts",
    )

    assert result.relationship_row_count == 79
    assert result.relation_counts == {
        "government_body_affects_sector": 28,
        "infrastructure_project_affects_sector": 6,
        "issuer_has_listed_ticker": 10,
        "issuer_in_sector": 7,
        "program_operated_by_org": 5,
        "regulator_affects_sector": 12,
        "sector_affects_index": 1,
        "ticker_listed_on_exchange": 10,
    }
    assert result.node_type_counts == {
        "exchange": 1,
        "government_body": 7,
        "issuer": 10,
        "market_index": 1,
        "program": 5,
        "project": 2,
        "regulator": 5,
        "sector": 44,
        "ticker": 10,
    }

    validation = validate_market_graph_source_lanes(edge_tsv_paths=[result.edge_tsv_path])
    assert validation.invalid_row_count == 0
    assert validation.source_warning_counts == {}
    assert validation.relation_warning_counts == {}
    assert validation.source_tier_counts == {
        "exchange": 22,
        "government": 38,
        "issuer_disclosed": 7,
        "regulator": 12,
    }

    assert result.artifact_path is not None
    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    for ticker_ref in [
        "finance-ca-ticker:tsx:ry",
        "finance-ca-ticker:tsx:td",
        "finance-ca-ticker:tsx:shop",
        "finance-ca-ticker:tsx:enb",
        "finance-ca-ticker:tsx:cnq",
        "finance-ca-ticker:tsx:cnr",
        "finance-ca-ticker:tsx:cp",
        "finance-ca-ticker:tsx:bce",
        "finance-ca-ticker:tsx:t",
        "finance-ca-ticker:tsx:rci.b",
    ]:
        assert nodes[ticker_ref]["is_tradable"] == "true"
    assert nodes["finance-ca:sp-tsx-composite"]["is_tradable"] == "true"
    assert nodes["ades:sector:ca:banking"]["is_tradable"] == "false"
    assert nodes["ades:project:ca:contrecoeur-port-terminal"]["is_seed_eligible"] == "true"

    with MarketGraphStore(result.artifact_path) as store:
        rbc_edges = store.outbound_edges_batch(["ades:issuer:ca:royal-bank-of-canada"])[
            "ades:issuer:ca:royal-bank-of-canada"
        ]
        osfi_edges = store.outbound_edges_batch(["ades:org:ca:osfi"])["ades:org:ca:osfi"]
        boc_edges = store.outbound_edges_batch(["ades:org:ca:bank-of-canada"])[
            "ades:org:ca:bank-of-canada"
        ]
        contrecoeur_edges = store.outbound_edges_batch(
            ["ades:project:ca:contrecoeur-port-terminal"]
        )["ades:project:ca:contrecoeur-port-terminal"]

    assert {edge.target_ref for edge in rbc_edges} == {"finance-ca-ticker:tsx:ry"}
    assert {edge.target_ref for edge in osfi_edges} == {
        "ades:sector:ca:banking",
        "ades:sector:ca:insurance",
        "ades:sector:ca:pension-plans",
    }
    assert {edge.target_ref for edge in boc_edges} == {
        "ades:sector:ca:foreign-exchange",
        "ades:sector:ca:interest-rates",
        "ades:sector:ca:monetary-policy",
    }
    assert {edge.target_ref for edge in contrecoeur_edges} == {
        "ades:sector:ca:port-infrastructure",
        "ades:sector:ca:public-infrastructure",
        "ades:sector:ca:trade-logistics",
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=result.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=20,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    issuer_candidate_refs, _ = expanded_refs("ades:issuer:ca:royal-bank-of-canada")
    assert "finance-ca-ticker:tsx:ry" in issuer_candidate_refs
    assert "ades:currency:CAD" not in issuer_candidate_refs

    shopify_candidate_refs, shopify_passive_refs = expanded_refs("ades:issuer:ca:shopify")
    assert "finance-ca-ticker:tsx:shop" in shopify_candidate_refs
    assert "ades:sector:ca:commerce-software" in shopify_passive_refs
    assert "ades:currency:CAD" not in shopify_candidate_refs

    equity_candidate_refs, _ = expanded_refs("ades:sector:ca:canadian-equity-market")
    assert "finance-ca:sp-tsx-composite" in equity_candidate_refs
    assert "ades:currency:CAD" not in equity_candidate_refs

    no_policy_ticker_refs = {
        "finance-ca-ticker:tsx:ry",
        "finance-ca-ticker:tsx:td",
        "finance-ca-ticker:tsx:shop",
        "finance-ca-ticker:tsx:enb",
        "finance-ca-ticker:tsx:cnq",
        "finance-ca-ticker:tsx:cnr",
        "finance-ca-ticker:tsx:cp",
        "finance-ca-ticker:tsx:bce",
        "finance-ca-ticker:tsx:t",
        "finance-ca-ticker:tsx:rci.b",
        "finance-ca:sp-tsx-composite",
        "ades:currency:CAD",
    }
    expansion_expectations = [
        (
            "ades:org:ca:osfi",
            {
                "ades:sector:ca:banking",
                "ades:sector:ca:insurance",
                "ades:sector:ca:pension-plans",
            },
        ),
        (
            "ades:org:ca:bank-of-canada",
            {
                "ades:sector:ca:foreign-exchange",
                "ades:sector:ca:interest-rates",
                "ades:sector:ca:monetary-policy",
            },
        ),
        (
            "ades:project:ca:contrecoeur-port-terminal",
            {
                "ades:sector:ca:port-infrastructure",
                "ades:sector:ca:public-infrastructure",
                "ades:sector:ca:trade-logistics",
            },
        ),
        (
            "ades:org:ca:crtc",
            {
                "ades:sector:ca:broadband",
                "ades:sector:ca:broadcasting-media",
                "ades:sector:ca:telecommunications",
            },
        ),
        (
            "ades:org:ca:global-affairs-canada",
            {
                "ades:sector:ca:agriculture-export",
                "ades:sector:ca:trade-logistics",
                "ades:sector:ca:mining",
            },
        ),
        (
            "ades:program:ca:cmhc-mortgage-loan-insurance",
            {
                "ades:org:ca:cmhc",
                "ades:sector:ca:housing-finance",
                "ades:sector:ca:housing-supply",
                "ades:sector:ca:mortgage-lending",
            },
        ),
    ]
    for source_ref, expected_passive_refs in expansion_expectations:
        candidate_refs, passive_refs = expanded_refs(source_ref)
        assert expected_passive_refs.issubset(passive_refs)
        assert candidate_refs.isdisjoint(no_policy_ticker_refs)


def test_reviewed_china_fixture_expands_exchange_paths_and_keeps_policy_guardrails(
    tmp_path: Path,
) -> None:
    if not REVIEWED_CHINA_SSE_SZSE_PBOC_POLICY_FIXTURE.exists():
        pytest.skip("reviewed China SSE/SZSE/PBOC policy fixture is not mounted")

    result = build_program_org_relationship_source_lane(
        relationship_tsv_paths=[REVIEWED_CHINA_SSE_SZSE_PBOC_POLICY_FIXTURE],
        output_root=tmp_path / "impact_relationships",
        run_id="reviewed-china-sse-szse-pboc-policy",
        build_artifact=True,
        include_starter_graph=False,
        artifact_output_root=tmp_path / "artifacts",
    )

    assert result.relationship_row_count == 81
    assert result.relation_counts == {
        "brand_owned_by_org": 1,
        "central_bank_affects_credit_sector": 3,
        "central_bank_affects_currency": 1,
        "central_bank_affects_rates": 1,
        "government_body_affects_sector": 12,
        "issuer_has_listed_ticker": 14,
        "issuer_has_security": 4,
        "issuer_in_sector": 9,
        "policy_body_affects_sector": 1,
        "regulator_affects_sector": 9,
        "sector_affects_index": 1,
        "security_has_identifier": 4,
        "security_listed_on_exchange": 4,
        "statistics_body_reports_macro_indicator": 3,
        "ticker_listed_on_exchange": 14,
    }
    assert result.node_type_counts == {
        "brand": 1,
        "currency": 1,
        "exchange": 5,
        "government_body": 7,
        "issuer": 12,
        "macro_indicator": 3,
        "market_index": 1,
        "rate": 1,
        "regulator": 4,
        "sector": 28,
        "security": 4,
        "ticker": 14,
    }

    validation = validate_market_graph_source_lanes(edge_tsv_paths=[result.edge_tsv_path])
    assert validation.invalid_row_count == 0
    assert validation.source_warning_counts == {}
    assert validation.relation_warning_counts == {}
    assert validation.source_tier_counts == {
        "exchange": 48,
        "government": 28,
        "regulator": 5,
    }

    assert result.artifact_path is not None
    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    for tradable_ref in [
        "finance-cn-ticker:600519",
        "finance-cn-ticker:300750",
        "finance-hk-ticker:hkex:00700",
        "finance-hk-ticker:hkex:09988",
        "finance-cn:csi-300",
        "ades:impact:currency:cny",
        "ades:impact:rate:cn-lpr",
        "finance-us-ticker:WMT",
    ]:
        assert nodes[tradable_ref]["is_tradable"] == "true"
    for passive_ref in [
        "ades:sector:cn:banking",
        "ades:sector:cn:consumer-retail",
        "ades:macro:cn:gdp",
    ]:
        assert nodes[passive_ref]["is_tradable"] == "false"

    edges = _read_tsv(result.edge_tsv_path)
    assert all(row["relation"] != "same_as" for row in edges)
    pboc_edges = [row for row in edges if row["source_ref"] == "finance-cn:pboc"]
    assert {row["target_ref"] for row in pboc_edges} == {
        "ades:impact:currency:cny",
        "ades:impact:rate:cn-lpr",
        "ades:sector:cn:banking",
        "ades:sector:cn:local-government-finance",
        "ades:sector:cn:real-estate",
    }
    cny_edge = next(row for row in pboc_edges if row["relation"] == "central_bank_affects_currency")
    assert "fx_fixing_or_capital_flow_signal" in cny_edge["direction_preconditions"]
    lpr_edge = next(row for row in pboc_edges if row["relation"] == "central_bank_affects_rates")
    assert "liquidity_or_policy_rate_signal" in lpr_edge["direction_preconditions"]
    assert not any("North Korea" in row["notes"] for row in edges)
    edge_refs = {ref for row in edges for ref in (row["source_ref"], row["target_ref"])}
    assert "finance-us-ticker:BABA" not in edge_refs
    assert "finance-us-ticker:PDD" not in edge_refs

    with MarketGraphStore(result.artifact_path) as store:
        bank_of_china_edges = store.outbound_edges_batch(["finance-cn-issuer:601988"])[
            "finance-cn-issuer:601988"
        ]
        samr_edges = store.outbound_edges_batch(
            ["ades:org:cn:state-administration-for-market-regulation"]
        )["ades:org:cn:state-administration-for-market-regulation"]

    assert {edge.target_ref for edge in bank_of_china_edges} == {
        "ades:security:cn:sse:601988-ashare",
        "ades:security:hk:hkex:03988-hshare",
        "ades:sector:cn:banking",
        "finance-cn-ticker:601988",
        "finance-hk-ticker:hkex:03988",
    }
    assert {edge.target_ref for edge in samr_edges} == {
        "ades:sector:cn:consumer-retail",
        "ades:sector:cn:food-safety",
        "ades:sector:cn:platform-internet",
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=result.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=40,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    maotai_candidate_refs, maotai_passive_refs = expanded_refs("finance-cn-issuer:600519")
    assert maotai_candidate_refs == {"finance-cn-ticker:600519"}
    assert "ades:sector:cn:consumer-staples" in maotai_passive_refs
    assert "ades:impact:currency:cny" not in maotai_candidate_refs

    boc_candidate_refs, boc_passive_refs = expanded_refs("finance-cn-issuer:601988")
    assert {
        "ades:security:cn:sse:601988-ashare",
        "ades:security:hk:hkex:03988-hshare",
        "finance-cn-ticker:601988",
        "finance-hk-ticker:hkex:03988",
    }.issubset(boc_candidate_refs)
    assert "ades:sector:cn:banking" in boc_passive_refs
    assert "ades:impact:currency:cny" not in boc_candidate_refs

    sams_candidate_refs, sams_passive_refs = expanded_refs("ades:brand:global:sams-club")
    assert sams_candidate_refs == {"finance-us-ticker:WMT"}
    assert "finance-us-issuer:0000104169" in sams_passive_refs
    assert "ades:impact:currency:cny" not in sams_candidate_refs
    assert "finance-cn:csi-300" not in sams_candidate_refs

    equity_candidate_refs, _ = expanded_refs("ades:sector:cn:mainland-equity-market")
    assert equity_candidate_refs == {"finance-cn:csi-300"}

    pboc_candidate_refs, pboc_passive_refs = expanded_refs("finance-cn:pboc")
    assert pboc_candidate_refs == {"ades:impact:currency:cny", "ades:impact:rate:cn-lpr"}
    assert {
        "ades:sector:cn:banking",
        "ades:sector:cn:local-government-finance",
        "ades:sector:cn:real-estate",
    }.issubset(pboc_passive_refs)

    no_policy_candidate_refs = {
        "ades:impact:currency:cny",
        "ades:impact:rate:cn-lpr",
        "finance-cn:csi-300",
        "finance-cn-ticker:002594",
        "finance-cn-ticker:300750",
        "finance-cn-ticker:600028",
        "finance-cn-ticker:600519",
        "finance-cn-ticker:600941",
        "finance-cn-ticker:601398",
        "finance-cn-ticker:601628",
        "finance-cn-ticker:601988",
        "finance-cn-ticker:920185",
        "finance-hk-ticker:hkex:00700",
        "finance-hk-ticker:hkex:00941",
        "finance-hk-ticker:hkex:03988",
        "finance-hk-ticker:hkex:09988",
        "finance-us-ticker:WMT",
    }
    policy_expectations = [
        (
            "ades:org:cn:state-administration-for-market-regulation",
            {
                "ades:sector:cn:consumer-retail",
                "ades:sector:cn:food-safety",
                "ades:sector:cn:platform-internet",
            },
        ),
        (
            "ades:org:cn:ministry-of-industry-and-information-technology",
            {
                "ades:sector:cn:batteries",
                "ades:sector:cn:ev-and-nev",
                "ades:sector:cn:semiconductors",
                "ades:sector:cn:telecom",
            },
        ),
    ]
    for source_ref, expected_passive_refs in policy_expectations:
        candidate_refs, passive_refs = expanded_refs(source_ref)
        assert expected_passive_refs.issubset(passive_refs)
        assert candidate_refs.isdisjoint(no_policy_candidate_refs)


def test_reviewed_germany_fixture_expands_boerse_and_keeps_policy_guardrails(
    tmp_path: Path,
) -> None:
    if not REVIEWED_GERMANY_BOERSE_BAFIN_BMWK_FCAS_FIXTURE.exists():
        pytest.skip("reviewed Germany Boerse/BaFin/BMWK/FCAS fixture is not mounted")

    result = build_program_org_relationship_source_lane(
        relationship_tsv_paths=[REVIEWED_GERMANY_BOERSE_BAFIN_BMWK_FCAS_FIXTURE],
        output_root=tmp_path / "impact_relationships",
        run_id="reviewed-germany-boerse-bafin-bmwk-fcas",
        build_artifact=True,
        include_starter_graph=False,
        artifact_output_root=tmp_path / "artifacts",
    )

    assert result.relationship_row_count == 89
    assert result.relation_counts == {
        "central_bank_affects_credit_sector": 2,
        "central_bank_affects_currency": 1,
        "central_bank_affects_rates": 1,
        "defense_project_affects_sector": 2,
        "government_body_affects_sector": 10,
        "issuer_has_listed_ticker": 15,
        "issuer_in_sector": 15,
        "program_operated_by_org": 1,
        "project_participant_org": 1,
        "regulator_affects_sector": 22,
        "sector_affects_index": 1,
        "statistics_body_reports_macro_indicator": 3,
        "ticker_listed_on_exchange": 15,
    }
    assert result.node_type_counts == {
        "currency": 1,
        "exchange": 1,
        "government_body": 5,
        "issuer": 15,
        "macro_indicator": 3,
        "market_index": 1,
        "organization": 1,
        "program": 1,
        "project": 1,
        "rate": 1,
        "regulator": 6,
        "sector": 38,
        "ticker": 15,
    }

    validation = validate_market_graph_source_lanes(edge_tsv_paths=[result.edge_tsv_path])
    assert validation.invalid_row_count == 0
    assert validation.source_warning_counts == {}
    assert validation.relation_warning_counts == {}
    assert validation.source_tier_counts == {
        "exchange": 46,
        "government": 20,
        "issuer_disclosed": 1,
        "regulator": 22,
    }

    assert result.artifact_path is not None
    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    for tradable_ref in [
        "finance-de-ticker:SAP",
        "finance-de-ticker:RHM",
        "finance-de-ticker:ENR",
        "finance-de:dax",
        "ades:impact:currency:eur",
        "ades:impact:rate:ecb-deposit-rate",
    ]:
        assert nodes[tradable_ref]["is_tradable"] == "true"
    for passive_ref in [
        "ades:sector:de:gas-fired-generation",
        "ades:sector:de:defense",
        "ades:project:eu:fcas-scaf",
    ]:
        assert nodes[passive_ref]["is_tradable"] == "false"

    with MarketGraphStore(result.artifact_path) as store:
        sap_edges = store.outbound_edges_batch(["finance-de-issuer:DE0007164600"])[
            "finance-de-issuer:DE0007164600"
        ]
        bmwk_edges = store.outbound_edges_batch(["ades:org:de:bmwk"])["ades:org:de:bmwk"]
        fcas_edges = store.outbound_edges_batch(["ades:project:eu:fcas-scaf"])[
            "ades:project:eu:fcas-scaf"
        ]

    assert {edge.target_ref for edge in sap_edges} == {
        "ades:sector:de:industrial-software",
        "finance-de-ticker:SAP",
    }
    assert {edge.target_ref for edge in bmwk_edges} == {
        "ades:sector:de:electricity",
        "ades:sector:de:gas-fired-generation",
        "ades:sector:de:hydrogen",
        "ades:sector:de:industrial-policy",
        "ades:sector:de:natural-gas",
        "ades:sector:de:power-grid",
    }
    assert {edge.target_ref for edge in fcas_edges} == {
        "ades:org:eu:airbus",
        "ades:sector:de:aerospace",
        "ades:sector:de:defense",
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=result.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=40,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    sap_candidate_refs, sap_passive_refs = expanded_refs("finance-de-issuer:DE0007164600")
    assert sap_candidate_refs == {"finance-de-ticker:SAP"}
    assert "ades:sector:de:industrial-software" in sap_passive_refs
    assert "ades:impact:currency:eur" not in sap_candidate_refs

    equity_candidate_refs, _ = expanded_refs("ades:sector:de:german-equity-market")
    assert equity_candidate_refs == {"finance-de:dax"}

    ecb_candidate_refs, _ = expanded_refs("ades:org:eu:ecb")
    assert ecb_candidate_refs == {
        "ades:impact:currency:eur",
        "ades:impact:rate:ecb-deposit-rate",
    }

    no_policy_candidate_refs = {
        "ades:impact:currency:eur",
        "ades:impact:rate:ecb-deposit-rate",
        "finance-de:dax",
        "finance-de-ticker:ALV",
        "finance-de-ticker:BAS",
        "finance-de-ticker:BMW",
        "finance-de-ticker:DB1",
        "finance-de-ticker:DBK",
        "finance-de-ticker:DTE",
        "finance-de-ticker:ENR",
        "finance-de-ticker:EOAN",
        "finance-de-ticker:IFX",
        "finance-de-ticker:MBG",
        "finance-de-ticker:MTX",
        "finance-de-ticker:RHM",
        "finance-de-ticker:RWE",
        "finance-de-ticker:SAP",
        "finance-de-ticker:SIE",
    }
    for source_ref, expected_passive_refs in [
        (
            "ades:org:de:bmwk",
            {
                "ades:sector:de:electricity",
                "ades:sector:de:gas-fired-generation",
                "ades:sector:de:natural-gas",
                "ades:sector:de:power-grid",
            },
        ),
        (
            "ades:program:de:kraftwerksstrategie",
            {
                "ades:org:de:bmwk",
                "ades:sector:de:electricity",
                "ades:sector:de:gas-fired-generation",
                "ades:sector:de:natural-gas",
                "ades:sector:de:power-grid",
            },
        ),
        (
            "ades:project:eu:fcas-scaf",
            {
                "ades:org:eu:airbus",
                "ades:sector:de:aerospace",
                "ades:sector:de:defense",
            },
        ),
        (
            "ades:org:de:bundesnetzagentur",
            {
                "ades:sector:de:electricity",
                "ades:sector:de:gas-fired-generation",
                "ades:sector:de:natural-gas",
                "ades:sector:de:power-grid",
                "ades:sector:de:telecom",
            },
        ),
    ]:
        candidate_refs, passive_refs = expanded_refs(source_ref)
        assert expected_passive_refs.issubset(passive_refs)
        assert candidate_refs.isdisjoint(no_policy_candidate_refs)


def test_reviewed_india_fixture_expands_nse_macro_trade_and_policy_guardrails(
    tmp_path: Path,
) -> None:
    if not REVIEWED_INDIA_NSE_BSE_RBI_POLICY_FIXTURE.exists():
        pytest.skip("reviewed India NSE/RBI/policy fixture is not mounted")

    result = build_program_org_relationship_source_lane(
        relationship_tsv_paths=[REVIEWED_INDIA_NSE_BSE_RBI_POLICY_FIXTURE],
        output_root=tmp_path / "impact_relationships",
        run_id="reviewed-india-nse-bse-rbi-policy",
        build_artifact=True,
        include_starter_graph=False,
        artifact_output_root=tmp_path / "artifacts",
    )

    assert result.relationship_row_count == 136
    assert result.relation_counts == {
        "central_bank_affects_credit_sector": 4,
        "central_bank_affects_currency": 1,
        "central_bank_affects_rates": 4,
        "export_program_affects_sector": 6,
        "government_body_affects_sector": 35,
        "issuer_has_listed_ticker": 16,
        "issuer_in_sector": 16,
        "org_in_sector": 3,
        "payment_network_operated_by_org": 1,
        "policy_program_affects_sector": 1,
        "program_operated_by_org": 1,
        "public_facility_available_to_sector": 1,
        "regulator_affects_sector": 20,
        "sector_affects_index": 2,
        "statistics_body_reports_macro_indicator": 3,
        "ticker_listed_on_exchange": 16,
        "trade_agreement_affects_sector": 4,
        "trade_agreement_counterparty_country": 2,
    }
    assert result.node_type_counts == {
        "country": 2,
        "currency": 1,
        "exchange": 1,
        "government_body": 17,
        "issuer": 16,
        "macro_indicator": 3,
        "market_index": 2,
        "organization": 1,
        "payment_network": 1,
        "program": 2,
        "rate": 4,
        "regulator": 9,
        "sector": 61,
        "ticker": 16,
        "trade_agreement": 1,
    }

    validation = validate_market_graph_source_lanes(edge_tsv_paths=[result.edge_tsv_path])
    assert validation.invalid_row_count == 0
    assert validation.source_warning_counts == {}
    assert validation.relation_warning_counts == {}
    assert validation.source_tier_counts == {
        "exchange": 50,
        "government": 62,
        "issuer_disclosed": 4,
        "regulator": 20,
    }

    assert result.artifact_path is not None
    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    for tradable_ref in [
        "finance-in-ticker:HUDCO",
        "finance-in-ticker:RELIANCE",
        "finance-in:nifty-50",
        "ades:impact:currency:inr",
        "ades:impact:rate:in-repo-rate",
    ]:
        assert nodes[tradable_ref]["is_tradable"] == "true"
    for passive_ref in [
        "ades:trade-agreement:in-om:cepa",
        "ades:sector:in:seafood-exports",
        "ades:payment-network:in:upi",
    ]:
        assert nodes[passive_ref]["is_tradable"] == "false"

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=result.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=60,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    hudco_candidate_refs, hudco_passive_refs = expanded_refs("finance-in-issuer:HUDCO")
    assert hudco_candidate_refs == {"finance-in-ticker:HUDCO"}
    assert {"ades:sector:in:public-sector-borrowers", "finance-in:nse"}.issubset(hudco_passive_refs)
    assert "ades:impact:currency:inr" not in hudco_candidate_refs

    rbi_candidate_refs, rbi_passive_refs = expanded_refs("ades:org:in:rbi")
    assert rbi_candidate_refs == {
        "ades:impact:currency:inr",
        "ades:impact:rate:in-crr",
        "ades:impact:rate:in-gsec-10y",
        "ades:impact:rate:in-repo-rate",
        "ades:impact:rate:in-slr",
    }
    assert {
        "ades:sector:in:banking",
        "ades:sector:in:fx-funding",
        "ades:sector:in:nbfc",
        "ades:sector:in:public-sector-borrowers",
    }.issubset(rbi_passive_refs)
    assert "finance-in-ticker:HDFCBANK" not in rbi_candidate_refs

    cepa_candidate_refs, cepa_passive_refs = expanded_refs("ades:trade-agreement:in-om:cepa")
    assert cepa_candidate_refs == set()
    assert {
        "ades:country:IN",
        "ades:country:OM",
        "ades:sector:in:seafood-exports",
        "ades:sector:in:export-logistics",
        "ades:sector:in:ports-logistics",
    }.issubset(cepa_passive_refs)
    assert "ades:impact:currency:inr" not in cepa_candidate_refs

    upi_candidate_refs, upi_passive_refs = expanded_refs("ades:payment-network:in:upi")
    assert upi_candidate_refs == set()
    assert {
        "ades:org:in:npci",
        "ades:sector:in:digital-payments",
        "ades:sector:in:fintech",
        "ades:sector:in:payments",
    }.issubset(upi_passive_refs)

    trai_candidate_refs, trai_passive_refs = expanded_refs("ades:org:in:trai")
    assert trai_candidate_refs == set()
    assert {
        "ades:sector:in:broadband",
        "ades:sector:in:spectrum",
        "ades:sector:in:telecom",
    }.issubset(trai_passive_refs)
    assert "finance-in-ticker:BHARTIARTL" not in trai_candidate_refs

    pli_candidate_refs, pli_passive_refs = expanded_refs(
        "ades:program:in:production-linked-incentive"
    )
    assert pli_candidate_refs == set()
    assert {
        "ades:org:in:dpiit",
        "ades:sector:in:industrial-policy",
        "ades:sector:in:logistics",
        "ades:sector:in:manufacturing",
        "ades:sector:in:startups",
    }.issubset(pli_passive_refs)
