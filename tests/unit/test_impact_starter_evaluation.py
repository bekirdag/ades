from __future__ import annotations

import sqlite3

from ades.impact.evaluation import evaluate_impact_golden_set
from ades.impact.starter import (
    build_starter_market_graph_store,
    starter_golden_set_path,
    starter_source_paths,
)


def test_starter_market_graph_covers_inner_ring_families(tmp_path):
    with starter_source_paths() as (node_path, edge_path):
        assert node_path.exists()
        assert edge_path.exists()

    response = build_starter_market_graph_store(output_dir=tmp_path)

    assert response.node_count >= 18
    assert response.edge_count >= 15
    assert response.warnings == []

    with sqlite3.connect(response.artifact_path) as connection:
        families = {
            str(row[0])
            for row in connection.execute("SELECT DISTINCT relation FROM impact_edges")
        }
    assert {
        "country_exports_commodity",
        "country_produces_commodity",
        "chokepoint_affects_commodity",
        "commodity_traded_in_currency",
        "central_bank_affects_currency",
        "central_bank_affects_rates",
        "sanction_affects_country",
        "sanction_affects_commodity",
    }.issubset(families)


def test_starter_golden_set_evaluates_without_warnings(tmp_path):
    response = build_starter_market_graph_store(output_dir=tmp_path)

    with starter_golden_set_path() as golden_set_path:
        report = evaluate_impact_golden_set(
            golden_set_path=golden_set_path,
            artifact_path=response.artifact_path,
        )

    assert report.warnings == []
    assert report.case_count == 8
    assert report.empty_path_rate == 0.125
    assert report.unrelated_asset_rate == 0.0
    assert report.passed
    assert report.per_relation_family_recall["chokepoint_affects_commodity"] == 1.0
    assert report.per_relation_family_recall["sanction_affects_commodity"] == 1.0
