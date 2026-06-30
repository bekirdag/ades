from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ades.config import Settings
from ades.impact.evaluation import (
    evaluate_impact_article_golden_set,
    evaluate_impact_golden_set,
)
from ades.impact.expansion import expand_impact_paths
from ades.impact.starter import (
    build_starter_market_graph_store,
    starter_golden_set_path,
    starter_source_manifest_path,
    starter_source_paths,
)
from ades.service.models import EntityLink, EntityMatch, TagResponse


def test_starter_market_graph_covers_inner_ring_families(tmp_path):
    with starter_source_paths() as (node_path, edge_path):
        assert node_path.exists()
        assert edge_path.exists()

    response = build_starter_market_graph_store(output_dir=tmp_path)

    assert response.node_count >= 57
    assert response.edge_count >= 52
    assert response.warnings == []

    with sqlite3.connect(response.artifact_path) as connection:
        families = {
            str(row[0]) for row in connection.execute("SELECT DISTINCT relation FROM impact_edges")
        }
    assert {
        "country_exports_commodity",
        "country_produces_commodity",
        "chokepoint_affects_commodity",
        "chokepoint_affects_sector",
        "commodity_traded_in_currency",
        "commodity_region_affects_commodity",
        "central_bank_affects_currency",
        "central_bank_affects_rates",
        "country_affects_country_risk_proxy",
        "country_affects_currency_index",
        "country_affects_global_equity_proxy",
        "energy_policy_affects_commodity",
        "energy_input_affects_sector",
        "digital_asset_policy_affects_crypto",
        "food_commodity_affects_inflation",
        "industrial_metal_affects_sector",
        "issuer_has_listed_ticker",
        "pollution_affects_agriculture_commodity",
        "pollution_affects_livestock_commodity",
        "producer_group_affects_commodity",
        "sanction_affects_country",
        "sanction_affects_commodity",
        "sanctions_body_affects_risk_proxy",
        "strategic_metal_affects_sector",
        "tanker_sanctions_affects_commodity",
    }.issubset(families)

    with sqlite3.connect(response.artifact_path) as connection:
        row = connection.execute(
            """
            SELECT compatible_event_types_json, direction_preconditions_json
            FROM impact_edges
            WHERE relation = 'chokepoint_affects_sector'
              AND source_ref = 'ades:market-location:red-sea'
            """
        ).fetchone()
    assert row is not None
    assert "shipping_chokepoint_disruption" in json.loads(row[0])
    assert "transit_disruption" in json.loads(row[1])


def test_starter_golden_set_evaluates_without_warnings(tmp_path):
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with starter_golden_set_path() as golden_set_path:
        golden_payload = json.loads(golden_set_path.read_text(encoding="utf-8"))
        report = evaluate_impact_golden_set(
            golden_set_path=golden_set_path,
            artifact_path=response.artifact_path,
        )

    case_names = {str(case["name"]) for case in golden_payload["cases"]}
    assert "italy_banca_mediolanum_borsa_bridge" in case_names
    assert "mexico_gfnorte_bmv_banxico_bridge" in case_names
    assert report.warnings == []
    assert report.case_count == 28
    assert report.empty_path_rate == 0.0357
    assert report.unrelated_asset_rate == 0.0
    assert report.passed
    assert report.per_relation_family_recall["chokepoint_affects_commodity"] == 1.0
    assert report.per_relation_family_recall["energy_policy_affects_commodity"] == 1.0
    assert report.per_relation_family_recall["digital_asset_policy_affects_crypto"] == 1.0
    assert report.per_relation_family_recall["pollution_affects_agriculture_commodity"] == 1.0
    assert report.per_relation_family_recall["pollution_affects_livestock_commodity"] == 1.0
    assert report.per_relation_family_recall["sanction_affects_commodity"] == 1.0
    assert report.per_relation_family_recall["tanker_sanctions_affects_commodity"] == 1.0
    assert report.per_relation_family_recall["chokepoint_affects_sector"] == 1.0
    assert report.per_relation_family_recall["holding_company_controls_public_issuer"] == 1.0
    assert report.per_relation_family_recall["industrial_metal_affects_sector"] == 1.0
    assert report.per_relation_family_recall["issuer_has_listed_ticker"] == 1.0
    assert report.per_relation_family_recall["person_affects_employer_ticker"] == 1.0
    assert report.per_relation_family_recall["state_body_holds_ownership_stake"] == 1.0


def test_starter_source_manifest_has_required_fields():
    with starter_source_manifest_path() as manifest_path:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["generated_at"] == "2026-06-29T00:00:00Z"
    assert payload["storage_policy"].endswith(
        "/mnt/githubActions/ades_big_data, not in the package."
    )
    assert payload["normalized_files"] == ["impact_nodes.tsv", "impact_edges.tsv"]
    assert len(payload["sources"]) >= 8
    for source in payload["sources"]:
        assert source["source_name"]
        assert str(source["source_url"]).startswith("https://")
        assert source["source_snapshot"]
        assert source["source_year"]
        assert source["refresh_policy"]
        assert source["normalized_output_allowed"] is True
        assert source["license_notes"]
        assert source["notes"]


def test_starter_graph_includes_promoted_australia_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        australia_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_australia/%'
                """
            ).fetchone()[0]
        )
        asx_200_row = connection.execute(
            """
            SELECT is_tradable, is_seed_eligible
            FROM impact_nodes
            WHERE entity_ref = 'finance-au:asx-200'
            """
        ).fetchone()

    assert australia_edge_count == 38
    assert asx_200_row == (1, 0)

    expansion = expand_impact_paths(
        ["ades:sector:au:australian-equity-market"],
        artifact_path=response.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=2,
        max_candidates=5,
    )
    candidate_refs = {candidate.entity_ref for candidate in expansion.candidates}
    assert "finance-au:asx-200" in candidate_refs

    systemic_bank_expansion = expand_impact_paths(
        ["ades:sector:au:banking"],
        artifact_path=response.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=2,
        max_candidates=10,
    )
    systemic_bank_refs = {candidate.entity_ref for candidate in systemic_bank_expansion.candidates}
    assert {"finance-au-ticker:CBA", "finance-au-ticker:NAB"}.issubset(systemic_bank_refs)

    telecom_expansion = expand_impact_paths(
        ["ades:sector:au:telecommunications"],
        artifact_path=response.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=2,
        max_candidates=10,
    )
    telecom_refs = {candidate.entity_ref for candidate in telecom_expansion.candidates}
    assert "finance-au-ticker:TLS" in telecom_refs


def test_starter_graph_includes_promoted_brazil_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        brazil_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_brazil/%'
                """
            ).fetchone()[0]
        )
        ibovespa_row = connection.execute(
            """
            SELECT is_tradable, is_seed_eligible
            FROM impact_nodes
            WHERE entity_ref = 'finance-br:ibovespa'
            """
        ).fetchone()

    assert brazil_edge_count == 49
    assert ibovespa_row == (1, 0)

    bcb_expansion = expand_impact_paths(
        ["ades:org:br:bcb"],
        artifact_path=response.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=1,
        max_candidates=5,
    )
    bcb_candidate_refs = {candidate.entity_ref for candidate in bcb_expansion.candidates}
    assert {"ades:impact:currency:brl", "ades:impact:rate:br-selic"}.issubset(bcb_candidate_refs)

    equity_expansion = expand_impact_paths(
        ["ades:sector:br:brazilian-equity-market"],
        artifact_path=response.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=2,
        max_candidates=5,
    )
    equity_candidate_refs = {candidate.entity_ref for candidate in equity_expansion.candidates}
    assert "finance-br:ibovespa" in equity_candidate_refs

    ferrograo_expansion = expand_impact_paths(
        ["ades:project:br:ferrograo"],
        artifact_path=response.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=3,
        max_candidates=10,
        include_passive_paths=True,
    )
    ferrograo_candidate_refs = {
        candidate.entity_ref for candidate in ferrograo_expansion.candidates
    }
    ferrograo_passive_refs = {path.entity_ref for path in ferrograo_expansion.passive_paths}
    assert "ades:sector:br:rail-infrastructure" in ferrograo_passive_refs
    assert "ades:sector:br:transport-logistics" in ferrograo_passive_refs
    assert "finance-br-ticker:RAIL3" not in ferrograo_candidate_refs
    assert "finance-br:ibovespa" not in ferrograo_candidate_refs


def test_starter_graph_includes_promoted_canada_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        canada_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_canada/%'
                """
            ).fetchone()[0]
        )
        sp_tsx_row = connection.execute(
            """
            SELECT is_tradable, is_seed_eligible
            FROM impact_nodes
            WHERE entity_ref = 'finance-ca:sp-tsx-composite'
            """
        ).fetchone()

    assert canada_edge_count == 79
    assert sp_tsx_row == (1, 0)

    equity_expansion = expand_impact_paths(
        ["ades:sector:ca:canadian-equity-market"],
        artifact_path=response.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=2,
        max_candidates=5,
    )
    equity_candidate_refs = {candidate.entity_ref for candidate in equity_expansion.candidates}
    assert "finance-ca:sp-tsx-composite" in equity_candidate_refs
    assert "ades:currency:CAD" not in equity_candidate_refs

    contrecoeur_expansion = expand_impact_paths(
        ["ades:project:ca:contrecoeur-port-terminal"],
        artifact_path=response.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=3,
        max_candidates=10,
        include_passive_paths=True,
    )
    contrecoeur_candidate_refs = {
        candidate.entity_ref for candidate in contrecoeur_expansion.candidates
    }
    contrecoeur_passive_refs = {path.entity_ref for path in contrecoeur_expansion.passive_paths}
    assert "ades:sector:ca:port-infrastructure" in contrecoeur_passive_refs
    assert "ades:sector:ca:trade-logistics" in contrecoeur_passive_refs
    assert "finance-ca-ticker:tsx:cnr" not in contrecoeur_candidate_refs
    assert "finance-ca-ticker:tsx:cp" not in contrecoeur_candidate_refs
    assert "finance-ca:sp-tsx-composite" not in contrecoeur_candidate_refs

    osfi_expansion = expand_impact_paths(
        ["ades:org:ca:osfi"],
        artifact_path=response.artifact_path,
        settings=Settings(impact_expansion_enabled=True),
        max_depth=4,
        max_candidates=10,
        include_passive_paths=True,
    )
    osfi_candidate_refs = {candidate.entity_ref for candidate in osfi_expansion.candidates}
    osfi_passive_refs = {path.entity_ref for path in osfi_expansion.passive_paths}
    assert "ades:sector:ca:banking" in osfi_passive_refs
    assert "ades:sector:ca:insurance" in osfi_passive_refs
    assert "finance-ca-ticker:tsx:ry" not in osfi_candidate_refs
    assert "finance-ca-ticker:tsx:td" not in osfi_candidate_refs
    assert "ades:currency:CAD" not in osfi_candidate_refs


def test_starter_graph_includes_promoted_china_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        china_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_china/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-cn:csi-300',
                    'ades:impact:currency:cny',
                    'ades:impact:rate:cn-lpr',
                    'finance-us-ticker:WMT'
                )
                """
            )
        }
        bank_of_china_security_targets = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT target_ref
                FROM impact_edges
                WHERE source_ref = 'finance-cn-issuer:601988'
                  AND relation = 'issuer_has_security'
                """
            )
        }
        same_as_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE relation = 'same_as'
                  AND source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_china/%'
                """
            ).fetchone()[0]
        )
        pboc_preconditions = {
            str(row[0]): set(json.loads(row[1]))
            for row in connection.execute(
                """
                SELECT relation, direction_preconditions_json
                FROM impact_edges
                WHERE source_ref = 'finance-cn:pboc'
                  AND relation IN (
                      'central_bank_affects_currency',
                      'central_bank_affects_rates'
                  )
                """
            )
        }

    assert china_edge_count == 83
    assert tradable_rows == {
        "finance-cn:csi-300": (1, 0),
        "ades:impact:currency:cny": (1, 0),
        "ades:impact:rate:cn-lpr": (1, 0),
        "finance-us-ticker:WMT": (1, 0),
    }
    assert bank_of_china_security_targets == {
        "ades:security:cn:sse:601988-ashare",
        "ades:security:hk:hkex:03988-hshare",
    }
    assert same_as_count == 0
    assert "fx_fixing_or_capital_flow_signal" in pboc_preconditions["central_bank_affects_currency"]
    assert "liquidity_or_policy_rate_signal" in pboc_preconditions["central_bank_affects_rates"]

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=40,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    sams_candidate_refs, sams_passive_refs = expanded_refs("ades:brand:global:sams-club")
    assert sams_candidate_refs == {"finance-us-ticker:WMT"}
    assert "finance-us-issuer:0000104169" in sams_passive_refs
    assert "ades:impact:currency:cny" not in sams_candidate_refs
    assert "finance-cn:csi-300" not in sams_candidate_refs

    equity_candidate_refs, _ = expanded_refs("ades:sector:cn:mainland-equity-market")
    assert equity_candidate_refs == {"finance-cn:csi-300"}

    sasac_candidate_refs, sasac_passive_refs = expanded_refs(
        "ades:org:cn:state-owned-assets-supervision-and-administration-commission"
    )
    assert {
        "ades:security:cn:sse:600941-ashare",
        "ades:security:hk:hkex:00941-hshare",
        "finance-cn-ticker:600941",
        "finance-hk-ticker:hkex:00941",
    }.issubset(sasac_candidate_refs)
    assert "ades:holding:cn:china-mobile-communications-group" in sasac_passive_refs
    assert "finance-cn-issuer:600941" in sasac_passive_refs
    assert "finance-cn-ticker:601988" not in sasac_candidate_refs

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
    for source_ref, expected_passive_refs in [
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
    ]:
        candidate_refs, passive_refs = expanded_refs(source_ref)
        assert expected_passive_refs.issubset(passive_refs)
        assert candidate_refs.isdisjoint(no_policy_candidate_refs)


def test_starter_graph_includes_promoted_france_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        france_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_france/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-fr:cac-40',
                    'ades:impact:currency:eur',
                    'ades:impact:rate:ecb-deposit-rate',
                    'finance-fr-ticker:MC'
                )
                """
            )
        }
        lvmh_targets = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT target_ref
                FROM impact_edges
                WHERE source_ref = 'finance-fr-issuer:FR0000121014'
                """
            )
        }

    assert france_edge_count == 49
    assert tradable_rows == {
        "ades:impact:currency:eur": (1, 1),
        "ades:impact:rate:ecb-deposit-rate": (1, 0),
        "finance-fr:cac-40": (1, 0),
        "finance-fr-ticker:MC": (1, 0),
    }
    assert lvmh_targets == {
        "ades:sector:fr:luxury-consumer",
        "ades:security:fr:euronext:fr0000121014-ordinary",
        "finance-fr-ticker:MC",
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=40,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    lvmh_candidate_refs, lvmh_passive_refs = expanded_refs("finance-fr-issuer:FR0000121014")
    assert lvmh_candidate_refs == {"finance-fr-ticker:MC"}
    assert {
        "ades:sector:fr:luxury-consumer",
        "ades:security:fr:euronext:fr0000121014-ordinary",
        "finance-fr:euronext-paris",
    }.issubset(lvmh_passive_refs)
    assert "ades:impact:currency:eur" not in lvmh_candidate_refs

    banque_candidate_refs, _ = expanded_refs("finance-fr:banque-de-france")
    assert banque_candidate_refs == {
        "ades:impact:currency:eur",
        "ades:impact:rate:ecb-deposit-rate",
    }

    no_policy_candidate_refs = {
        "finance-fr:cac-40",
        "finance-fr-ticker:AI",
        "finance-fr-ticker:BNP",
        "finance-fr-ticker:CS",
        "finance-fr-ticker:MC",
        "finance-fr-ticker:ORA",
        "finance-fr-ticker:SAN",
        "finance-fr-ticker:SU",
        "finance-fr-ticker:TTE",
    }
    for source_ref, expected_passive_refs in [
        ("finance-fr:amf", {"ades:sector:fr:securities-market"}),
        ("finance-fr:acpr", {"ades:sector:fr:banking", "ades:sector:fr:insurance"}),
        (
            "ades:org:fr:ministere-economie-finances",
            {"ades:sector:fr:industrial-policy"},
        ),
        (
            "ades:org:eu:european-commission",
            {"ades:sector:fr:energy-transition", "ades:sector:fr:industrial-policy"},
        ),
    ]:
        candidate_refs, passive_refs = expanded_refs(source_ref)
        assert expected_passive_refs.issubset(passive_refs)
        assert candidate_refs.isdisjoint(no_policy_candidate_refs)


def test_starter_graph_includes_promoted_germany_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        germany_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_germany/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-de:dax',
                    'ades:impact:currency:eur',
                    'ades:impact:rate:ecb-deposit-rate',
                    'finance-de-ticker:SAP'
                )
                """
            )
        }
        fcas_targets = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT target_ref
                FROM impact_edges
                WHERE source_ref = 'ades:project:eu:fcas-scaf'
                """
            )
        }

    assert germany_edge_count == 91
    assert tradable_rows == {
        "ades:impact:currency:eur": (1, 1),
        "ades:impact:rate:ecb-deposit-rate": (1, 0),
        "finance-de-ticker:SAP": (1, 0),
        "finance-de:dax": (1, 0),
    }
    assert fcas_targets == {
        "ades:org:eu:airbus",
        "ades:sector:de:aerospace",
        "ades:sector:de:defense",
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
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
        "finance-de-ticker:SAP",
        "finance-de-ticker:RWE",
        "finance-de-ticker:ENR",
        "finance-de-ticker:RHM",
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
            "ades:org:eu:european-commission",
            {
                "ades:sector:de:climate-policy",
                "ades:sector:de:industrial-policy",
            },
        ),
    ]:
        candidate_refs, passive_refs = expanded_refs(source_ref)
        assert expected_passive_refs.issubset(passive_refs)
        assert candidate_refs.isdisjoint(no_policy_candidate_refs)


def test_starter_graph_includes_promoted_india_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        india_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_india/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-in-ticker:HUDCO',
                    'finance-in:nifty-50',
                    'ades:impact:currency:inr',
                    'ades:impact:rate:in-repo-rate'
                )
                """
            )
        }
        india_source_nodes = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, entity_type, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-in:bse',
                    'finance-in:nse',
                    'finance-in:sebi',
                    'ades:org:in:rbi',
                    'ades:org:in:commerce'
                )
                """
            )
        }
        cepa_targets = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT target_ref
                FROM impact_edges
                WHERE source_ref = 'ades:trade-agreement:in-om:cepa'
                """
            )
        }

    assert india_edge_count == 137
    assert tradable_rows == {
        "ades:impact:currency:inr": (1, 0),
        "ades:impact:rate:in-repo-rate": (1, 0),
        "finance-in-ticker:HUDCO": (1, 0),
        "finance-in:nifty-50": (1, 0),
    }
    assert india_source_nodes == {
        "ades:org:in:commerce": ("government_body", 0, 1),
        "ades:org:in:rbi": ("government_body", 0, 1),
        "finance-in:bse": ("exchange", 0, 1),
        "finance-in:nse": ("exchange", 0, 1),
        "finance-in:sebi": ("regulator", 0, 1),
    }
    assert {
        "ades:country:IN",
        "ades:country:OM",
        "ades:sector:in:export-logistics",
        "ades:sector:in:marine-products",
        "ades:sector:in:ports-logistics",
        "ades:sector:in:seafood-exports",
    } == cepa_targets

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
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
    assert "ades:sector:in:public-sector-borrowers" in hudco_passive_refs
    assert "ades:impact:currency:inr" not in hudco_candidate_refs

    rbi_candidate_refs, rbi_passive_refs = expanded_refs("ades:org:in:rbi")
    assert rbi_candidate_refs == {
        "ades:impact:currency:inr",
        "ades:impact:rate:in-crr",
        "ades:impact:rate:in-gsec-10y",
        "ades:impact:rate:in-repo-rate",
        "ades:impact:rate:in-slr",
    }
    assert "ades:sector:in:fx-funding" in rbi_passive_refs

    sebi_candidate_refs, sebi_passive_refs = expanded_refs("finance-in:sebi")
    assert sebi_candidate_refs == set()
    assert {
        "ades:sector:in:securities-markets",
        "finance-in:bse",
    }.issubset(sebi_passive_refs)

    cepa_candidate_refs, cepa_passive_refs = expanded_refs("ades:trade-agreement:in-om:cepa")
    assert cepa_candidate_refs == set()
    assert {"ades:country:OM", "ades:sector:in:seafood-exports"}.issubset(cepa_passive_refs)
    assert "ades:impact:currency:inr" not in cepa_candidate_refs

    upi_candidate_refs, upi_passive_refs = expanded_refs("ades:payment-network:in:upi")
    assert upi_candidate_refs == set()
    assert {"ades:org:in:npci", "ades:sector:in:digital-payments"}.issubset(upi_passive_refs)


def test_starter_graph_includes_promoted_japan_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        japan_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_japan/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-jp-ticker:7203',
                    'ades:security:jp:tse:jp3633400001-ordinary',
                    'ades:impact:currency:jpy',
                    'ades:impact:rate:jp-policy-rate'
                )
                """
            )
        }
        japan_source_nodes = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, entity_type, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-jp:jpx',
                    'finance-jp:tse',
                    'finance-jp:fsa',
                    'finance-jp:boj',
                    'ades:org:jp:meti',
                    'ades:org:jp:mlit',
                    'ades:org:jp:mic'
                )
                """
            )
        }
        boj_targets = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT target_ref
                FROM impact_edges
                WHERE source_ref = 'finance-jp:boj'
                """
            )
        }
        fsa_targets = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT target_ref
                FROM impact_edges
                WHERE source_ref = 'finance-jp:fsa'
                """
            )
        }

    assert japan_edge_count == 42
    assert tradable_rows == {
        "ades:impact:currency:jpy": (1, 0),
        "ades:impact:rate:jp-policy-rate": (1, 0),
        "ades:security:jp:tse:jp3633400001-ordinary": (1, 0),
        "finance-jp-ticker:7203": (1, 0),
    }
    assert japan_source_nodes == {
        "ades:org:jp:meti": ("government_body", 0, 1),
        "ades:org:jp:mic": ("government_body", 0, 1),
        "ades:org:jp:mlit": ("government_body", 0, 1),
        "finance-jp:boj": ("central_bank", 0, 1),
        "finance-jp:fsa": ("regulator", 0, 1),
        "finance-jp:jpx": ("exchange", 0, 1),
        "finance-jp:tse": ("exchange", 0, 1),
    }
    assert {
        "ades:impact:currency:jpy",
        "ades:impact:rate:jp-jgb-10y",
        "ades:impact:rate:jp-policy-rate",
        "ades:sector:jp:banking",
    } == boj_targets
    assert {
        "ades:sector:jp:asset-management",
        "ades:sector:jp:banking",
        "ades:sector:jp:securities-markets",
    } == fsa_targets

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=80,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    toyota_candidate_refs, toyota_passive_refs = expanded_refs("finance-jp-issuer:7203")
    assert toyota_candidate_refs == {
        "ades:security:jp:tse:jp3633400001-ordinary",
        "finance-jp-ticker:7203",
    }
    assert "ades:sector:jp:automotive" in toyota_passive_refs
    assert "ades:impact:currency:jpy" not in toyota_candidate_refs

    boj_candidate_refs, boj_passive_refs = expanded_refs("finance-jp:boj")
    assert boj_candidate_refs == {
        "ades:impact:currency:jpy",
        "ades:impact:rate:jp-jgb-10y",
        "ades:impact:rate:jp-policy-rate",
    }
    assert "ades:sector:jp:banking" in boj_passive_refs
    assert "finance-jp-ticker:8306" not in boj_candidate_refs

    prius_candidate_refs, prius_passive_refs = expanded_refs("ades:product:jp:prius")
    assert {
        "ades:security:jp:tse:jp3633400001-ordinary",
        "finance-jp-ticker:7203",
    }.issubset(prius_candidate_refs)
    assert "finance-jp-issuer:7203" in prius_passive_refs

    meti_candidate_refs, meti_passive_refs = expanded_refs("ades:org:jp:meti")
    assert meti_candidate_refs == set()
    assert {
        "ades:sector:jp:automotive",
        "ades:sector:jp:semiconductor-equipment",
    }.issubset(meti_passive_refs)


def test_starter_graph_includes_promoted_argentina_relationships(
    tmp_path: Path,
) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        argentina_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_argentina/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-ar-ticker:BPAT',
                    'ades:security:ar:banco-patagonia-common-share',
                    'ades:exchange:ar:byma',
                    'ades:product:ar:patagonia-emprendimiento',
                    'ades:program:ar:subsidio-tasas-mujeres-emprendedoras-rio-negro',
                    'ades:org:ar:bcra',
                    'ades:org:ar:agencia-crear-rio-negro'
                )
                """
            )
        }
        issuer_targets = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT target_ref
                FROM impact_edges
                WHERE source_ref = 'ades:issuer:ar:banco-patagonia'
                """
            )
        }

    assert argentina_edge_count == 10
    assert tradable_rows == {
        "finance-ar-ticker:BPAT": (1, 0),
        "ades:security:ar:banco-patagonia-common-share": (0, 1),
        "ades:exchange:ar:byma": (0, 1),
        "ades:product:ar:patagonia-emprendimiento": (0, 1),
        "ades:program:ar:subsidio-tasas-mujeres-emprendedoras-rio-negro": (0, 1),
        "ades:org:ar:bcra": (0, 1),
        "ades:org:ar:agencia-crear-rio-negro": (0, 1),
    }
    assert issuer_targets == {
        "ades:sector:ar:financial-services",
        "ades:security:ar:banco-patagonia-common-share",
        "finance-ar-ticker:BPAT",
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=20,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    product_candidate_refs, product_passive_refs = expanded_refs(
        "ades:product:ar:patagonia-emprendimiento"
    )
    assert product_candidate_refs == {"finance-ar-ticker:BPAT"}
    assert "ades:issuer:ar:banco-patagonia" in product_passive_refs
    assert "ades:currency:ARS" not in product_candidate_refs

    issuer_candidate_refs, issuer_passive_refs = expanded_refs("ades:issuer:ar:banco-patagonia")
    assert issuer_candidate_refs == {"finance-ar-ticker:BPAT"}
    assert "ades:security:ar:banco-patagonia-common-share" in issuer_passive_refs
    assert "ades:sector:ar:financial-services" in issuer_passive_refs

    bcra_candidate_refs, bcra_passive_refs = expanded_refs("ades:org:ar:bcra")
    assert bcra_candidate_refs == set()
    assert "ades:sector:ar:banking" in bcra_passive_refs
    assert "finance-ar-ticker:BPAT" not in bcra_candidate_refs

    subsidy_candidate_refs, subsidy_passive_refs = expanded_refs(
        "ades:program:ar:subsidio-tasas-mujeres-emprendedoras-rio-negro"
    )
    assert subsidy_candidate_refs == set()
    assert "ades:org:ar:agencia-crear-rio-negro" in subsidy_passive_refs
    assert "ades:sector:ar:sme-credit" in subsidy_passive_refs
    assert "finance-ar-ticker:BPAT" not in subsidy_candidate_refs
    assert "ades:currency:ARS" not in subsidy_candidate_refs


def test_starter_graph_includes_promoted_italy_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        italy_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_italy/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-it-ticker:BMED',
                    'ades:security:it:borsa:it0004776628-ordinary',
                    'finance-it:ftse-mib',
                    'ades:impact:currency:eur',
                    'ades:impact:rate:it-btp-10y'
                )
                """
            )
        }
        bank_targets = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT target_ref
                FROM impact_edges
                WHERE source_ref = 'finance-it:bank-of-italy'
                """
            )
        }

    assert italy_edge_count == 187
    assert tradable_rows == {
        "ades:impact:currency:eur": (1, 1),
        "ades:impact:rate:it-btp-10y": (1, 0),
        "ades:security:it:borsa:it0004776628-ordinary": (1, 0),
        "finance-it-ticker:BMED": (1, 0),
        "finance-it:ftse-mib": (1, 0),
    }
    assert {
        "ades:impact:currency:eur",
        "ades:impact:rate:ecb-deposit-rate",
        "ades:impact:rate:it-btp-10y",
        "ades:impact:rate:it-btp-bund-spread",
        "ades:sector:it:banking",
        "ades:sector:it:credit",
        "ades:sector:it:financial-stability",
        "ades:sector:it:payments",
    } == bank_targets

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=80,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    bmed_candidate_refs, bmed_passive_refs = expanded_refs("finance-it-issuer:IT0004776628")
    assert bmed_candidate_refs == {
        "ades:security:it:borsa:it0004776628-ordinary",
        "finance-it-ticker:BMED",
    }
    assert "ades:sector:it:wealth-management" in bmed_passive_refs
    assert "ades:impact:currency:eur" not in bmed_candidate_refs

    bank_candidate_refs, bank_passive_refs = expanded_refs("finance-it:bank-of-italy")
    assert bank_candidate_refs == {
        "ades:impact:currency:eur",
        "ades:impact:rate:ecb-deposit-rate",
        "ades:impact:rate:it-btp-10y",
        "ades:impact:rate:it-btp-bund-spread",
    }
    assert "ades:sector:it:banking" in bank_passive_refs
    assert "finance-it-ticker:BMED" not in bank_candidate_refs

    consob_candidate_refs, consob_passive_refs = expanded_refs("finance-it:consob")
    assert consob_candidate_refs == set()
    assert {"ades:sector:it:listed-issuers", "ades:sector:it:securities-markets"}.issubset(
        consob_passive_refs
    )

    pnrr_candidate_refs, pnrr_passive_refs = expanded_refs("ades:program:it:pnrr")
    assert pnrr_candidate_refs == set()
    assert {"ades:org:it:mef", "ades:sector:it:infrastructure"}.issubset(pnrr_passive_refs)
    assert "ades:impact:currency:eur" not in pnrr_candidate_refs

    equity_candidate_refs, _ = expanded_refs("ades:sector:it:italian-equity-market")
    assert equity_candidate_refs == {"finance-it:ftse-mib"}


def test_starter_graph_includes_promoted_mexico_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        mexico_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_mexico/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-mx-ticker:GFNORTE:o',
                    'finance-mx:ipc',
                    'ades:currency:MXN',
                    'ades:rates:mx:banxico-policy-rate',
                    'finance-us-ticker:AMX'
                )
                """
            )
        }
        world_cup_targets = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT target_ref
                FROM impact_edges
                WHERE source_ref = 'ades:program:global:fifa-world-cup-2026'
                """
            )
        }
        cne_targets = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT target_ref
                FROM impact_edges
                WHERE source_ref = 'ades:org:mx:cne'
                """
            )
        }

    assert mexico_edge_count == 199
    assert tradable_rows == {
        "ades:currency:MXN": (1, 0),
        "ades:rates:mx:banxico-policy-rate": (1, 0),
        "finance-mx-ticker:GFNORTE:o": (1, 0),
        "finance-mx:ipc": (1, 0),
        "finance-us-ticker:AMX": (1, 0),
    }
    assert {
        "ades:country:CA",
        "ades:country:MX",
        "ades:country:US",
        "ades:org:global:fifa",
        "ades:sector:mx:airports-and-tourism",
        "ades:sector:mx:beer-and-beverages",
        "ades:sector:mx:retail-and-consumer",
    }.issubset(world_cup_targets)
    assert cne_targets == {
        "ades:sector:mx:electricity-and-gas",
        "ades:sector:mx:energy-permits",
        "ades:sector:mx:fuel-and-refining",
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=120,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    gfnorte_candidate_refs, gfnorte_passive_refs = expanded_refs("finance-mx-issuer:GFNORTE")
    assert gfnorte_candidate_refs == {"finance-mx-ticker:GFNORTE:o"}
    assert "ades:sector:mx:banking" in gfnorte_passive_refs
    assert "ades:currency:MXN" not in gfnorte_candidate_refs

    banxico_candidate_refs, banxico_passive_refs = expanded_refs("ades:org:mx:banxico")
    assert banxico_candidate_refs == {
        "ades:currency:MXN",
        "ades:rates:mx:banxico-policy-rate",
        "ades:rates:mx:mbono-10y",
    }
    assert "ades:sector:mx:banking" in banxico_passive_refs
    assert "finance-mx-ticker:GFNORTE:o" not in banxico_candidate_refs

    usmca_candidate_refs, usmca_passive_refs = expanded_refs("ades:trade-agreement:usmca")
    assert usmca_candidate_refs == set()
    assert {
        "ades:country:CA",
        "ades:country:MX",
        "ades:country:US",
        "ades:sector:mx:logistics-and-border-trade",
        "ades:sector:mx:manufacturing",
    }.issubset(usmca_passive_refs)

    world_cup_candidate_refs, world_cup_passive_refs = expanded_refs(
        "ades:program:global:fifa-world-cup-2026"
    )
    assert world_cup_candidate_refs == set()
    assert {
        "ades:country:CA",
        "ades:country:MX",
        "ades:country:US",
        "ades:org:global:fifa",
        "ades:sector:mx:beer-and-beverages",
        "ades:sector:mx:retail-and-consumer",
    }.issubset(world_cup_passive_refs)
    assert "finance-mx-ticker:FEMSA:ubd" not in world_cup_candidate_refs

    oxxo_candidate_refs, oxxo_passive_refs = expanded_refs("ades:brand:mx:oxxo")
    assert {
        "finance-mx-ticker:FEMSA:ub",
        "finance-us-ticker:FMX",
        "ades:security:mx:bmv:femsa-ubd",
        "ades:security:us:nyse:fmx-adr",
    }.issubset(oxxo_candidate_refs)
    assert "finance-mx-issuer:FEMSA" in oxxo_passive_refs
    assert "ades:currency:MXN" not in oxxo_candidate_refs

    cne_candidate_refs, cne_passive_refs = expanded_refs("ades:org:mx:cne")
    assert cne_candidate_refs == set()
    assert "ades:sector:mx:fuel-and-refining" in cne_passive_refs

    cre_candidate_refs, cre_passive_refs = expanded_refs("ades:org:mx:cre")
    assert cre_candidate_refs == set()
    assert "ades:sector:mx:energy-permits" in cre_passive_refs

    equity_candidate_refs, _ = expanded_refs("ades:sector:mx:mexican-equity-market")
    assert equity_candidate_refs == {"finance-mx:ipc"}


def test_starter_graph_includes_promoted_russia_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        russia_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_russia/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-ru-ticker:SBER',
                    'ades:security:ru:moex:sber-share',
                    'finance-ru:moex-russia-index',
                    'ades:impact:currency:rub',
                    'ades:impact:rate:ru-cbr-key-rate',
                    'ades:impact:commodity:crude-oil',
                    'ades:impact:commodity:natural-gas',
                    'ades:impact:commodity:fertilizer'
                )
                """
            )
        }

    assert russia_edge_count == 165
    assert tradable_rows == {
        "ades:impact:commodity:crude-oil": (1, 1),
        "ades:impact:commodity:fertilizer": (1, 0),
        "ades:impact:commodity:natural-gas": (1, 1),
        "ades:impact:currency:rub": (1, 0),
        "ades:impact:rate:ru-cbr-key-rate": (1, 0),
        "ades:security:ru:moex:sber-share": (1, 0),
        "finance-ru-ticker:SBER": (1, 0),
        "finance-ru:moex-russia-index": (1, 0),
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=120,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    sber_candidate_refs, sber_passive_refs = expanded_refs("finance-ru-issuer:484")
    assert sber_candidate_refs == {"ades:security:ru:moex:sber-share", "finance-ru-ticker:SBER"}
    assert "ades:sector:ru:banking" in sber_passive_refs
    assert "ades:impact:currency:rub" not in sber_candidate_refs
    assert "ades:impact:commodity:crude-oil" not in sber_candidate_refs

    cbr_candidate_refs, cbr_passive_refs = expanded_refs("finance-ru:cbr")
    assert cbr_candidate_refs == {"ades:impact:currency:rub", "ades:impact:rate:ru-cbr-key-rate"}
    assert {"finance-ru:moex", "finance-ru:nsd"}.issubset(cbr_passive_refs)
    assert "finance-ru-ticker:SBER" not in cbr_candidate_refs

    wikidata_cbr_candidate_refs, wikidata_cbr_passive_refs = expanded_refs("wikidata:Q827011")
    assert wikidata_cbr_candidate_refs == cbr_candidate_refs
    assert {"finance-ru:moex", "finance-ru:nsd"}.issubset(wikidata_cbr_passive_refs)

    wikidata_sber_candidate_refs, wikidata_sber_passive_refs = expanded_refs("wikidata:Q205012")
    assert wikidata_sber_candidate_refs == {
        "ades:security:ru:moex:sber-share",
        "finance-ru-ticker:SBER",
    }
    assert "ades:sector:ru:banking" in wikidata_sber_passive_refs

    wikidata_nsd_candidate_refs, wikidata_nsd_passive_refs = expanded_refs("wikidata:Q4315073")
    assert wikidata_nsd_candidate_refs == set()
    assert "ades:sector:ru:settlement-and-depository" in wikidata_nsd_passive_refs

    ofac_candidate_refs, ofac_passive_refs = expanded_refs(
        "ades:sanctions-program:us-ofac:russia-related"
    )
    assert ofac_candidate_refs == set()
    assert {"ades:sector:ru:banking", "finance-ru:nsd"}.issubset(ofac_passive_refs)

    eu_candidate_refs, _ = expanded_refs("ades:sanctions-program:eu:russia-restrictive-measures")
    assert {
        "ades:impact:commodity:crude-oil",
        "ades:impact:commodity:fertilizer",
        "ades:impact:commodity:natural-gas",
    }.issubset(eu_candidate_refs)
    assert "finance-ru-ticker:GAZP" not in eu_candidate_refs
    assert "ades:impact:currency:rub" not in eu_candidate_refs

    crude_flow_candidate_refs, _ = expanded_refs("ades:commodity-flow:ru:crude-oil-exports")
    assert {
        "ades:impact:commodity:crude-oil",
        "ades:impact:currency:usd",
        "ades:impact:sector:airlines",
    }.issubset(crude_flow_candidate_refs)
    assert "finance-ru-ticker:SBER" not in crude_flow_candidate_refs
    assert "ades:impact:currency:rub" not in crude_flow_candidate_refs

    equity_candidate_refs, _ = expanded_refs("ades:sector:ru:russian-equity-market")
    assert equity_candidate_refs == {"finance-ru:moex-russia-index"}


def test_article_golden_set_evaluates_extraction_then_impact(tmp_path: Path):
    response = build_starter_market_graph_store(output_dir=tmp_path / "artifact")
    golden_set_path = tmp_path / "article_golden_set.json"
    golden_set_path.write_text(
        json.dumps(
            {
                "name": "article-impact-smoke",
                "default_pack": "general-en",
                "default_enabled_packs": ["general-en", "finance-en"],
                "cases": [
                    {
                        "name": "hormuz-iran",
                        "text": "Iran tensions around the Strait of Hormuz lifted oil risk.",
                        "expected_entity_refs": [
                            "ades:heuristic_structural_location:general-en:location:strait-of-hormuz",
                            "wikidata:Q794",
                        ],
                        "expected_candidate_refs": [
                            "ades:impact:commodity:crude-oil",
                            "ades:impact:currency:usd",
                            "ades:impact:sector:airlines",
                        ],
                        "expected_passive_refs": ["wikidata:Q121826282"],
                        "expected_relation_families": [
                            "chokepoint_affects_commodity",
                            "commodity_traded_in_currency",
                            "country_member_of_bloc",
                            "energy_input_affects_sector",
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    def fake_tagger(text: str, **kwargs) -> TagResponse:
        assert "Hormuz" in text
        return TagResponse(
            version="0.3.1",
            pack=str(kwargs["pack"]),
            language="en",
            content_type="text/plain",
            entities=[
                EntityMatch(
                    text="Strait of Hormuz",
                    label="location",
                    start=31,
                    end=48,
                    confidence=0.99,
                    link=EntityLink(
                        entity_id="ades:heuristic_structural_location:general-en:location:strait-of-hormuz",
                        canonical_text="Strait of Hormuz",
                        provider="ades",
                    ),
                ),
                EntityMatch(
                    text="Iran",
                    label="country",
                    start=0,
                    end=4,
                    confidence=0.99,
                    link=EntityLink(
                        entity_id="wikidata:Q794",
                        canonical_text="Iran",
                        provider="wikidata",
                    ),
                ),
            ],
            timing_ms=1,
        )

    report = evaluate_impact_article_golden_set(
        golden_set_path=golden_set_path,
        artifact_path=response.artifact_path,
        tagger=fake_tagger,
    )

    assert report.warnings == []
    assert report.case_count == 1
    assert report.extraction_recall == 1.0
    assert report.unrelated_asset_rate == 0.0
    assert report.passed
    assert report.cases[0].extracted_entity_refs == [
        "ades:heuristic_structural_location:general-en:location:strait-of-hormuz",
        "wikidata:Q794",
    ]


def test_starter_graph_includes_promoted_saudi_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        saudi_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_saudi_arabia/%'
                   OR source_snapshot LIKE '/mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en-sa-stage/%'
                   OR source_snapshot LIKE '/mnt/githubActions/ades_big_data/prod-deploy/registry-2026-05-07-general-location-repair/packs/finance-sa-en/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-sa-ticker:2222',
                    'ades:security:sa:tadawul:2222-ordinary-share',
                    'finance-sa:tasi',
                    'ades:impact:currency:sar',
                    'ades:impact:rate:sa-repo-rate',
                    'ades:impact:commodity:crude-oil'
                )
                """
            )
        }

    assert saudi_edge_count == 146
    assert tradable_rows == {
        "ades:impact:commodity:crude-oil": (1, 1),
        "ades:impact:currency:sar": (1, 0),
        "ades:impact:rate:sa-repo-rate": (1, 0),
        "ades:security:sa:tadawul:2222-ordinary-share": (1, 0),
        "finance-sa-ticker:2222": (1, 0),
        "finance-sa:tasi": (1, 0),
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=80,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    aramco_candidate_refs, aramco_passive_refs = expanded_refs("finance-sa-issuer:2222")
    assert aramco_candidate_refs == {
        "ades:security:sa:tadawul:2222-ordinary-share",
        "finance-sa-ticker:2222",
    }
    assert "finance-sa:tadawul" in aramco_passive_refs
    assert "ades:impact:currency:sar" not in aramco_candidate_refs
    assert "finance-sa:tasi" not in aramco_candidate_refs

    sama_candidate_refs, sama_passive_refs = expanded_refs("ades:org:sa:sama")
    assert sama_candidate_refs == {"ades:impact:currency:sar", "ades:impact:rate:sa-repo-rate"}
    assert {"ades:sector:sa:banking", "ades:sector:sa:payments-fintech"}.issubset(sama_passive_refs)
    assert "finance-sa-ticker:1120" not in sama_candidate_refs
    assert "finance-sa:tasi" not in sama_candidate_refs

    neom_candidate_refs, neom_passive_refs = expanded_refs("ades:project:sa:neom")
    assert neom_candidate_refs == set()
    assert {
        "ades:sector:sa:construction",
        "ades:sector:sa:tourism",
        "ades:sector:sa:transport-logistics",
    }.issubset(neom_passive_refs)
    assert "finance-sa-ticker:2222" not in neom_candidate_refs
    assert "ades:impact:currency:sar" not in neom_candidate_refs

    equity_candidate_refs, _ = expanded_refs("ades:sector:sa:saudi-equity-market")
    assert equity_candidate_refs == {"finance-sa:tasi"}


def test_starter_graph_includes_promoted_south_africa_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        south_africa_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_south_africa/%'
                   OR source_snapshot LIKE '/mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en-za-stage-r2/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-za-ticker:NPN',
                    'ades:security:za:jse:npn-ordinary-share',
                    'finance-za:ftse-jse-all-share',
                    'ades:impact:currency:zar',
                    'ades:impact:rate:za-sarb-policy-rate',
                    'ades:impact:commodity:gold',
                    'ades:impact:commodity:platinum-group-metals'
                )
                """
            )
        }
        alias_rows = {
            str(row[0]): json.loads(str(row[1]))
            for row in connection.execute(
                """
                SELECT entity_ref, identifiers_json
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-za:sarb',
                    'ades:org:za:eskom',
                    'finance-za-issuer:1925-001431-06'
                )
                """
            )
        }

    assert south_africa_edge_count == 212
    assert tradable_rows == {
        "ades:impact:commodity:gold": (1, 1),
        "ades:impact:commodity:platinum-group-metals": (1, 0),
        "ades:impact:currency:zar": (1, 0),
        "ades:impact:rate:za-sarb-policy-rate": (1, 0),
        "ades:security:za:jse:npn-ordinary-share": (1, 0),
        "finance-za-ticker:NPN": (1, 0),
        "finance-za:ftse-jse-all-share": (1, 0),
    }
    assert alias_rows["finance-za:sarb"]["same_as_refs"] == ["wikidata:Q912920"]
    assert alias_rows["ades:org:za:eskom"]["same_as_refs"] == ["wikidata:Q1367885"]
    assert "wikidata:Q1965898" in alias_rows["finance-za-issuer:1925-001431-06"]["same_as_refs"]

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=80,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    sarb_candidate_refs, sarb_passive_refs = expanded_refs("finance-za:sarb")
    assert sarb_candidate_refs == {
        "ades:impact:currency:zar",
        "ades:impact:rate:za-sarb-policy-rate",
    }
    assert {
        "ades:sector:za:banking",
        "ades:sector:za:credit",
        "ades:sector:za:financial-stability",
    }.issubset(sarb_passive_refs)
    assert "finance-za-ticker:SBK" not in sarb_candidate_refs
    assert "finance-za:ftse-jse-all-share" not in sarb_candidate_refs
    wikidata_sarb_candidate_refs, wikidata_sarb_passive_refs = expanded_refs("wikidata:Q912920")
    assert wikidata_sarb_candidate_refs == sarb_candidate_refs
    assert {
        "ades:sector:za:banking",
        "ades:sector:za:credit",
        "ades:sector:za:financial-stability",
    }.issubset(wikidata_sarb_passive_refs)

    eskom_candidate_refs, eskom_passive_refs = expanded_refs("ades:org:za:eskom")
    assert eskom_candidate_refs == set()
    assert {
        "ades:grid:za:electricity-grid",
        "ades:sector:za:electric-utilities",
        "ades:sector:za:electricity-generation-transmission-distribution",
    }.issubset(eskom_passive_refs)
    assert "ades:impact:currency:zar" not in eskom_candidate_refs
    assert "finance-za:ftse-jse-all-share" not in eskom_candidate_refs
    wikidata_eskom_candidate_refs, wikidata_eskom_passive_refs = expanded_refs("wikidata:Q1367885")
    assert wikidata_eskom_candidate_refs == set()
    assert {
        "ades:grid:za:electricity-grid",
        "ades:sector:za:electric-utilities",
        "ades:sector:za:electricity-generation-transmission-distribution",
    }.issubset(wikidata_eskom_passive_refs)

    naspers_candidate_refs, naspers_passive_refs = expanded_refs("finance-za-issuer:1925-001431-06")
    assert naspers_candidate_refs == {
        "ades:security:za:jse:npn-ordinary-share",
        "finance-za-ticker:NPN",
    }
    assert "ades:sector:za:digital-consumer-internet" in naspers_passive_refs
    assert "ades:impact:currency:zar" not in naspers_candidate_refs
    assert "finance-za:ftse-jse-all-share" not in naspers_candidate_refs
    wikidata_naspers_candidate_refs, wikidata_naspers_passive_refs = expanded_refs(
        "wikidata:Q1965898"
    )
    assert wikidata_naspers_candidate_refs == naspers_candidate_refs
    assert "ades:sector:za:digital-consumer-internet" in wikidata_naspers_passive_refs

    gold_candidate_refs, gold_passive_refs = expanded_refs("finance-za-issuer:1968-004880-06")
    assert {
        "ades:impact:commodity:gold",
        "ades:security:za:jse:gfie-ordinary-share",
        "finance-za-ticker:GFIE",
    }.issubset(gold_candidate_refs)
    assert "ades:sector:za:gold-mining" in gold_passive_refs
    assert "ades:impact:currency:zar" not in gold_candidate_refs
    assert "finance-za:ftse-jse-all-share" not in gold_candidate_refs

    equity_candidate_refs, _ = expanded_refs("ades:sector:za:south-african-equity-market")
    assert equity_candidate_refs == {"finance-za:ftse-jse-all-share"}


def test_starter_graph_includes_promoted_south_korea_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        south_korea_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_south_korea/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-kr-ticker:1:000660',
                    'ades:security:kr:krx:000660-common-share',
                    'ades:impact:index:kospi',
                    'ades:impact:currency:krw',
                    'ades:impact:rate:kr-base-rate',
                    'ades:sector:kr:semiconductors'
                )
                """
            )
        }

    assert south_korea_edge_count == 172
    assert tradable_rows == {
        "ades:impact:currency:krw": (1, 0),
        "ades:impact:index:kospi": (1, 0),
        "ades:impact:rate:kr-base-rate": (1, 0),
        "ades:sector:kr:semiconductors": (0, 1),
        "ades:security:kr:krx:000660-common-share": (1, 0),
        "finance-kr-ticker:1:000660": (1, 0),
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=80,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    bok_candidate_refs, bok_passive_refs = expanded_refs("ades:policy-body:kr:bank-of-korea")
    assert bok_candidate_refs == {
        "ades:impact:currency:krw",
        "ades:impact:rate:kr-base-rate",
    }
    assert "ades:sector:kr:banking" in bok_passive_refs
    assert "ades:impact:index:kospi" not in bok_candidate_refs

    sk_hynix_candidate_refs, sk_hynix_passive_refs = expanded_refs("finance-kr-issuer:1:000660")
    assert {
        "ades:security:kr:krx:000660-common-share",
        "finance-kr-ticker:1:000660",
    }.issubset(sk_hynix_candidate_refs)
    assert {
        "ades:sector:kr:semiconductors",
        "ades:supply-chain-stage:kr:memory-semiconductors",
    }.issubset(sk_hynix_passive_refs)
    assert "ades:impact:currency:krw" not in sk_hynix_candidate_refs

    motir_candidate_refs, motir_passive_refs = expanded_refs("ades:government-body:kr:motir")
    assert motir_candidate_refs == set()
    assert {"ades:sector:kr:batteries", "ades:sector:kr:semiconductors"}.issubset(
        motir_passive_refs
    )

    krx_candidate_refs, krx_passive_refs = expanded_refs("ades:exchange:kr:krx")
    assert krx_candidate_refs == {"ades:impact:index:kospi"}
    assert "ades:sector:kr:exchange-market-structure" in krx_passive_refs

    sector_candidate_refs, sector_passive_refs = expanded_refs("ades:sector:kr:semiconductors")
    assert sector_candidate_refs == set()
    assert sector_passive_refs == set()


def test_starter_graph_includes_promoted_turkiye_relationships(tmp_path: Path) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        turkiye_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_turkiye/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-tr-ticker:THYAO',
                    'ades:security:tr:bist:thyao-ordinary-share',
                    'ades:impact:index:bist-100',
                    'ades:impact:currency:try',
                    'ades:impact:rate:tr-policy-rate',
                    'ades:sector:tr:aviation'
                )
                """
            )
        }

    assert turkiye_edge_count == 244
    assert tradable_rows == {
        "ades:impact:currency:try": (1, 0),
        "ades:impact:index:bist-100": (1, 0),
        "ades:impact:rate:tr-policy-rate": (1, 0),
        "ades:sector:tr:aviation": (0, 1),
        "ades:security:tr:bist:thyao-ordinary-share": (1, 0),
        "finance-tr-ticker:THYAO": (1, 0),
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=80,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    tcmb_candidate_refs, tcmb_passive_refs = expanded_refs("ades:policy-body:tr:tcmb")
    assert tcmb_candidate_refs == {
        "ades:impact:currency:try",
        "ades:impact:rate:tr-policy-rate",
    }
    assert {
        "ades:policy:tr:reserve-requirements",
        "ades:sector:tr:banking-credit",
    }.issubset(tcmb_passive_refs)
    assert "ades:impact:index:bist-100" not in tcmb_candidate_refs
    assert "finance-tr-ticker:THYAO" not in tcmb_candidate_refs

    bddk_candidate_refs, bddk_passive_refs = expanded_refs("ades:regulator:tr:bddk")
    assert bddk_candidate_refs == set()
    assert "ades:sector:tr:banking" in bddk_passive_refs
    assert "finance-tr-ticker:AKBNK" not in bddk_candidate_refs
    assert "ades:impact:currency:try" not in bddk_candidate_refs

    thyao_candidate_refs, thyao_passive_refs = expanded_refs("finance-tr-issuer:1107")
    assert thyao_candidate_refs == {
        "ades:security:tr:bist:thyao-ordinary-share",
        "finance-tr-ticker:THYAO",
    }
    assert {
        "ades:airline:tr:turkish-airlines",
        "ades:sector:tr:air-travel",
        "ades:sector:tr:aviation",
    }.issubset(thyao_passive_refs)
    assert "ades:impact:currency:try" not in thyao_candidate_refs
    assert "ades:impact:index:bist-100" not in thyao_candidate_refs

    energy_candidate_refs, energy_passive_refs = expanded_refs(
        "ades:government-body:tr:energy-ministry"
    )
    assert energy_candidate_refs == set()
    assert "ades:sector:tr:energy" in energy_passive_refs
    assert "ades:impact:currency:try" not in energy_candidate_refs

    equity_candidate_refs, equity_passive_refs = expanded_refs("ades:sector:tr:bist-equity-market")
    assert equity_candidate_refs == {"ades:impact:index:bist-100"}
    assert equity_passive_refs == set()


def test_starter_graph_includes_promoted_united_kingdom_relationships(
    tmp_path: Path,
) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        united_kingdom_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_united_kingdom/%'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'finance-uk-ticker:rr',
                    'ades:security:gb:lse:rr-ordinary-share',
                    'ades:impact:currency:gbp',
                    'ades:impact:rate:gb-bank-rate',
                    'ades:sovereign-bond:gb:gilt',
                    'ades:yield-curve:gb:gilt-yield-curve',
                    'finance-uk:ftse-100',
                    'ades:sector:gb:water-utilities'
                )
                """
            )
        }

    assert united_kingdom_edge_count == 264
    assert tradable_rows == {
        "ades:impact:currency:gbp": (1, 0),
        "ades:impact:rate:gb-bank-rate": (1, 0),
        "ades:sector:gb:water-utilities": (0, 1),
        "ades:security:gb:lse:rr-ordinary-share": (1, 0),
        "ades:sovereign-bond:gb:gilt": (1, 0),
        "ades:yield-curve:gb:gilt-yield-curve": (1, 0),
        "finance-uk:ftse-100": (1, 0),
        "finance-uk-ticker:rr": (1, 0),
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=120,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    boe_candidate_refs, boe_passive_refs = expanded_refs("ades:policy-body:gb:bank-of-england")
    assert boe_candidate_refs == {
        "ades:impact:currency:gbp",
        "ades:impact:rate:gb-bank-rate",
        "ades:sovereign-bond:gb:gilt",
        "ades:yield-curve:gb:gilt-yield-curve",
    }
    assert {
        "ades:policy:gb:asset-purchase-facility",
        "ades:sector:gb:banking-credit",
    }.issubset(boe_passive_refs)
    assert "finance-uk:ftse-100" not in boe_candidate_refs
    assert "finance-uk-ticker:rr" not in boe_candidate_refs

    fca_candidate_refs, fca_passive_refs = expanded_refs("finance-uk:fca")
    assert fca_candidate_refs == set()
    assert {
        "ades:sector:gb:asset-management",
        "ades:sector:gb:listed-issuers",
        "finance-uk:lse",
    }.issubset(fca_passive_refs)

    rr_candidate_refs, rr_passive_refs = expanded_refs("finance-uk-issuer:07524813")
    assert rr_candidate_refs == {
        "ades:security:gb:lse:rr-ordinary-share",
        "finance-uk-ticker:rr",
    }
    assert {
        "ades:sector:gb:aerospace-defense",
        "ades:sector:gb:defense-procurement",
        "finance-uk:lse",
    }.issubset(rr_passive_refs)
    assert "ades:impact:currency:gbp" not in rr_candidate_refs
    assert "finance-uk:ftse-100" not in rr_candidate_refs

    ofwat_candidate_refs, ofwat_passive_refs = expanded_refs("ades:regulator:gb:ofwat")
    assert ofwat_candidate_refs == set()
    assert "ades:sector:gb:water-utilities" in ofwat_passive_refs
    assert "finance-uk-ticker:svt" not in ofwat_candidate_refs

    equity_candidate_refs, equity_passive_refs = expanded_refs("ades:sector:gb:uk-equity-market")
    assert equity_candidate_refs == {"finance-uk:ftse-100"}
    assert equity_passive_refs == set()


def test_starter_graph_includes_promoted_united_states_relationships(
    tmp_path: Path,
) -> None:
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with sqlite3.connect(response.artifact_path) as connection:
        united_states_edge_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_united_states/%'
                """
            ).fetchone()[0]
        )
        exchange_fanout_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM impact_edges
                WHERE source_snapshot LIKE '/mnt/githubActions/ades_big_data/source_lane_united_states/%'
                  AND relation = 'exchange_lists_security'
                """
            ).fetchone()[0]
        )
        tradable_rows = {
            str(row[0]): tuple(row[1:])
            for row in connection.execute(
                """
                SELECT entity_ref, is_tradable, is_seed_eligible
                FROM impact_nodes
                WHERE entity_ref IN (
                    'ades:impact:currency:usd',
                    'ades:interest-rate:us:fed-funds-target-range',
                    'ades:interest-rate:us:sofr',
                    'ades:treasury-security:us:treasury-bill',
                    'ades:security:us:nvda:common-stock',
                    'finance-us-ticker:NVDA',
                    'ades:security:us:jpm:common-stock',
                    'finance-us-ticker:JPM'
                )
                """
            )
        }

    assert united_states_edge_count == 200
    assert exchange_fanout_count == 0
    assert tradable_rows == {
        "ades:impact:currency:usd": (1, 1),
        "ades:interest-rate:us:fed-funds-target-range": (1, 0),
        "ades:interest-rate:us:sofr": (1, 0),
        "ades:security:us:jpm:common-stock": (1, 0),
        "ades:security:us:nvda:common-stock": (1, 0),
        "ades:treasury-security:us:treasury-bill": (1, 0),
        "finance-us-ticker:JPM": (1, 0),
        "finance-us-ticker:NVDA": (1, 0),
    }

    def expanded_refs(source_ref: str) -> tuple[set[str], set[str]]:
        expansion = expand_impact_paths(
            [source_ref],
            artifact_path=response.artifact_path,
            settings=Settings(impact_expansion_enabled=True),
            max_depth=4,
            max_candidates=120,
            include_passive_paths=True,
        )
        return (
            {candidate.entity_ref for candidate in expansion.candidates},
            {path.entity_ref for path in expansion.passive_paths},
        )

    fed_candidate_refs, fed_passive_refs = expanded_refs("ades:central-bank:us:federal-reserve")
    assert fed_candidate_refs == {
        "ades:impact:currency:usd",
        "ades:interest-rate:us:fed-funds-target-range",
        "ades:interest-rate:us:sofr",
    }
    assert {
        "ades:policy:us:fed-balance-sheet-policy",
        "ades:program:us:discount-window",
        "ades:sector:us:banking-credit",
    }.issubset(fed_passive_refs)
    assert "finance-us-ticker:JPM" not in fed_candidate_refs
    assert "finance-us:sp-500" not in fed_candidate_refs

    treasury_candidate_refs, treasury_passive_refs = expanded_refs(
        "ades:government-body:us:treasury"
    )
    assert treasury_candidate_refs == {
        "ades:treasury-security:us:treasury-bill",
        "ades:treasury-security:us:treasury-bond",
        "ades:treasury-security:us:treasury-note",
    }
    assert treasury_passive_refs == {"ades:sector:us:fiscal-policy-sensitive"}
    assert "ades:impact:currency:usd" not in treasury_candidate_refs
    assert "finance-us:sp-500" not in treasury_candidate_refs

    sec_candidate_refs, sec_passive_refs = expanded_refs("ades:regulator:us:sec")
    assert sec_candidate_refs == set()
    assert {
        "ades:exchange:us:cboe",
        "ades:sector:us:broker-dealers",
        "ades:sector:us:listed-issuers",
        "finance-us:nasdaq",
        "finance-us:nyse",
    }.issubset(sec_passive_refs)

    nvda_candidate_refs, nvda_passive_refs = expanded_refs("finance-us-issuer:0001045810")
    assert nvda_candidate_refs == {
        "ades:security:us:nvda:common-stock",
        "finance-us-ticker:NVDA",
    }
    assert {
        "ades:sector:us:semiconductors",
        "finance-us:nasdaq",
        "sec:cik:0001045810",
    }.issubset(nvda_passive_refs)
    assert "finance-us-ticker:AAPL" not in nvda_candidate_refs
    assert "finance-us-ticker:JPM" not in nvda_candidate_refs
    assert "ades:impact:currency:usd" not in nvda_candidate_refs

    jpm_candidate_refs, jpm_passive_refs = expanded_refs("finance-us-issuer:0000019617")
    assert jpm_candidate_refs == {
        "ades:security:us:jpm:common-stock",
        "finance-us-ticker:JPM",
    }
    assert {
        "ades:sector:us:major-banks",
        "finance-us:nyse",
        "sec:cik:0000019617",
    }.issubset(jpm_passive_refs)
    assert "finance-us-ticker:BAC" not in jpm_candidate_refs
    assert "ades:impact:currency:usd" not in jpm_candidate_refs
