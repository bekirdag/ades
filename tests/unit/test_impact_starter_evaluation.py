from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ades.impact.evaluation import (
    evaluate_impact_article_golden_set,
    evaluate_impact_golden_set,
)
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

    assert response.node_count >= 20
    assert response.edge_count >= 18
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
        "energy_policy_affects_commodity",
        "producer_group_affects_commodity",
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
    assert report.case_count == 9
    assert report.empty_path_rate == 0.1111
    assert report.unrelated_asset_rate == 0.0
    assert report.passed
    assert report.per_relation_family_recall["chokepoint_affects_commodity"] == 1.0
    assert report.per_relation_family_recall["energy_policy_affects_commodity"] == 1.0
    assert report.per_relation_family_recall["sanction_affects_commodity"] == 1.0


def test_starter_source_manifest_has_required_fields():
    with starter_source_manifest_path() as manifest_path:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["storage_policy"].endswith("/mnt/githubActions/ades_big_data, not in the package.")
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
                        ],
                        "expected_passive_refs": ["wikidata:Q121826282"],
                        "expected_relation_families": [
                            "chokepoint_affects_commodity",
                            "commodity_traded_in_currency",
                            "country_member_of_bloc",
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
