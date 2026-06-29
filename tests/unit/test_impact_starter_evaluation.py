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
        report = evaluate_impact_golden_set(
            golden_set_path=golden_set_path,
            artifact_path=response.artifact_path,
        )

    assert report.warnings == []
    assert report.case_count == 17
    assert report.empty_path_rate == 0.0588
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
    assert report.per_relation_family_recall["industrial_metal_affects_sector"] == 1.0
    assert report.per_relation_family_recall["issuer_has_listed_ticker"] == 1.0
    assert report.per_relation_family_recall["person_affects_employer_ticker"] == 1.0


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

    assert australia_edge_count == 30
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

    assert brazil_edge_count == 47
    assert ibovespa_row == (1, 0)

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

    assert china_edge_count == 81
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

    assert germany_edge_count == 89
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
    ]:
        candidate_refs, passive_refs = expanded_refs(source_ref)
        assert expected_passive_refs.issubset(passive_refs)
        assert candidate_refs.isdisjoint(no_policy_candidate_refs)


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
