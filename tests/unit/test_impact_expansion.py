import sys
from pathlib import Path

import pytest

from ades.config import Settings
from ades.impact.expansion import enrich_tag_response_with_impact_paths, expand_impact_paths
from ades.impact.graph_builder import build_market_graph_store
from ades.impact.graph_store import MarketGraphStore
from ades.service.models import EntityLink, EntityMatch, TagResponse


def _write_market_graph_tsvs(root: Path) -> tuple[Path, Path]:
    node_path = root / "impact_nodes.tsv"
    edge_path = root / "impact_edges.tsv"
    node_path.write_text(
        "\t".join(
            [
                "entity_ref",
                "canonical_name",
                "entity_type",
                "library_id",
                "is_tradable",
                "is_seed_eligible",
                "identifiers_json",
                "packs",
            ]
        )
        + "\n"
        + "\n".join(
            [
                (
                    "entity_hormuz\tStrait of Hormuz\tchokepoint\tpolitics-vector-en\t0\t1\t"
                    '{"same_as_refs":["ades:heuristic_structural_location:general-en:location:strait-of-hormuz","wikidata:Q63132"]}'
                    "\tfinance-en,politics-vector-en"
                ),
                "entity_iran\tIran\tcountry\tpolitics-vector-en\t0\t1\t{}\tpolitics-vector-en",
                "entity_crude_oil\tCrude oil\tcommodity\tfinance-en\t1\t0\t{}\tfinance-en",
                "entity_gold\tGold\tcommodity\tfinance-en\t1\t0\t{}\tfinance-en",
                "entity_opec\tOPEC\torganization\tfinance-en\t0\t1\t{}\tfinance-en",
                "entity_zero\tZero Degree Country\tcountry\tpolitics-vector-en\t0\t1\t{}\tpolitics-vector-en",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    edge_path.write_text(
        "\t".join(
            [
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
        )
        + "\n"
        + "\n".join(
            [
                "entity_hormuz\tentity_crude_oil\tchokepoint_affects_commodity\tdirect\t0.92\tsupply_risk\tEIA\thttps://example.test/eia\t2026-05-05\t2024\tannual\tfinance-en\tprimary\tshipping_chokepoint_disruption,supply_disruption\ttransit_disruption,war_risk",
                "entity_hormuz\tentity_crude_oil\tchokepoint_affects_commodity\tdirect\t0.80\tsupply_risk\tReviewed\thttps://example.test/reviewed\t2026-05-05\t2024\tannual\tpolitics-vector-en\tduplicate\twar_escalation\twar_risk",
                "entity_iran\tentity_hormuz\tcountry_exports_commodity\tshallow\t0.70\tcontext\tReviewed\thttps://example.test/iran\t2026-05-05\t2024\tannual\tpolitics-vector-en\tintermediate\t\t",
                "entity_iran\tentity_opec\tcountry_member_of_bloc\tdirect\t0.95\tcontext\tOPEC\thttps://example.test/opec\t2026-05-05\t2026\tannual\tfinance-en\tpassive\t\t",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return node_path, edge_path


def _settings(artifact_path: Path) -> Settings:
    return Settings(
        impact_expansion_enabled=True,
        impact_expansion_artifact_path=artifact_path,
    )


def test_market_graph_builder_merges_edges_and_computes_seed_degree(tmp_path: Path) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    response = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact",
        artifact_version="2026-05-05T00:00:00Z",
    )

    assert response.node_count == 6
    assert response.edge_count == 3
    assert response.pack_ids == ["finance-en", "politics-vector-en"]
    assert response.relation_counts == {
        "chokepoint_affects_commodity": 1,
        "country_exports_commodity": 1,
        "country_member_of_bloc": 1,
    }
    assert response.edge_family_counts == {
        "geography_commodity": 2,
        "other": 1,
    }
    assert any(warning.startswith("duplicate_edge_merged") for warning in response.warnings)

    with MarketGraphStore(response.artifact_path) as store:
        nodes = store.node_batch(["entity_hormuz", "entity_zero"])
        assert nodes["entity_hormuz"].seed_degree == 1
        assert nodes["entity_zero"].seed_degree == 0
        assert store.bridge_refs_batch(["wikidata:Q63132"])["wikidata:Q63132"] == ("entity_hormuz",)
        assert (
            "wikidata:Q63132"
            in store.identity_refs_for_entity_batch(["entity_hormuz"])["entity_hormuz"]
        )
        edges = store.outbound_edges_batch(["entity_hormuz"])["entity_hormuz"]
        assert len(edges) == 1
        assert edges[0].confidence == 0.92
        assert edges[0].packs == ("finance-en", "politics-vector-en")
        assert edges[0].compatible_event_types == (
            "shipping_chokepoint_disruption",
            "supply_disruption",
            "war_escalation",
        )
        assert edges[0].direction_preconditions == ("transit_disruption", "war_risk")


def test_market_graph_builder_records_release_gate_failure(tmp_path: Path) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    response = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact",
        artifact_version="2026-05-05T00:00:00Z",
        release_gate_commands=[f'{sys.executable} -c "import sys; sys.exit(7)"'],
        release_gate_working_dir=tmp_path,
    )

    assert response.release_gate_passed is False
    assert response.release_gate_working_dir == str(tmp_path.resolve())
    assert any(warning.startswith("release_gate_failed:") for warning in response.warnings)
    manifest = Path(response.manifest_path).read_text(encoding="utf-8")
    assert '"release_gate_passed": false' in manifest


def test_market_graph_builder_requires_plan_edge_columns(tmp_path: Path) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    edge_path.write_text(
        edge_path.read_text(encoding="utf-8")
        .replace("\tsource_year", "", 1)
        .replace("\tnotes", "", 1),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing columns"):
        build_market_graph_store(
            node_tsv_paths=[node_path],
            edge_tsv_paths=[edge_path],
            output_dir=tmp_path / "artifact",
            artifact_version="2026-05-05T00:00:00Z",
        )


def test_market_graph_builder_hash_includes_edge_notes(tmp_path: Path) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    first = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact-one",
        artifact_version="2026-05-05T00:00:00Z",
    )
    edge_path.write_text(
        edge_path.read_text(encoding="utf-8").replace("primary", "primary reviewed", 1),
        encoding="utf-8",
    )
    second = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact-two",
        artifact_version="2026-05-05T00:00:00Z",
    )

    assert first.source_manifest_hash != second.source_manifest_hash


def test_expand_impact_paths_returns_direct_and_derived_candidates(tmp_path: Path) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    build_response = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact",
        artifact_version="2026-05-05T00:00:00Z",
    )

    result = expand_impact_paths(
        ["entity_gold", "entity_hormuz", "entity_iran", "entity_zero"],
        enabled_packs=["finance-en", "politics-vector-en"],
        settings=_settings(Path(build_response.artifact_path)),
        max_depth=2,
        max_candidates=10,
    )

    assert result.graph_version == "market-graph-v1"
    assert not result.warnings
    by_ref = {candidate.entity_ref: candidate for candidate in result.candidates}
    assert by_ref["entity_gold"].evidence_level == "direct"
    assert by_ref["entity_gold"].relationship_paths == []
    assert by_ref["entity_crude_oil"].evidence_level == "shallow"
    assert by_ref["entity_crude_oil"].relationship_paths[0].path_depth == 1
    assert by_ref["entity_crude_oil"].source_entity_refs == [
        "entity_hormuz",
        "entity_iran",
    ]
    assert by_ref["entity_crude_oil"].compatible_event_types == [
        "shipping_chokepoint_disruption",
        "supply_disruption",
        "war_escalation",
    ]
    assert by_ref["entity_crude_oil"].relationship_paths[0].edges[0].direction_preconditions == [
        "transit_disruption",
        "war_risk",
    ]
    source_entities = {source.entity_ref: source for source in result.source_entities}
    assert source_entities["entity_zero"].is_graph_seed is False
    assert source_entities["entity_zero"].seed_degree == 0
    assert result.passive_paths[0].entity_ref in {"entity_hormuz", "entity_opec"}


def test_expand_impact_paths_returns_direct_market_terminal_when_graph_lags(
    tmp_path: Path,
) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    build_response = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact",
        artifact_version="2026-05-05T00:00:00Z",
    )

    result = expand_impact_paths(
        ["ades:impact:commodity:coffee"],
        enabled_packs=["finance-en"],
        settings=_settings(Path(build_response.artifact_path)),
        max_depth=2,
        max_candidates=10,
    )

    by_ref = {candidate.entity_ref: candidate for candidate in result.candidates}
    assert by_ref["ades:impact:commodity:coffee"].name == "Coffee"
    assert by_ref["ades:impact:commodity:coffee"].entity_type == "commodity"
    assert by_ref["ades:impact:commodity:coffee"].evidence_level == "direct"
    assert by_ref["ades:impact:commodity:coffee"].source_entity_refs == [
        "ades:impact:commodity:coffee"
    ]
    assert by_ref["ades:impact:commodity:coffee"].relationship_paths == []
    source_entities = {source.entity_ref: source for source in result.source_entities}
    assert source_entities["ades:impact:commodity:coffee"].name == "Coffee"
    assert source_entities["ades:impact:commodity:coffee"].is_tradable is True
    assert source_entities["ades:impact:commodity:coffee"].is_graph_seed is False
    assert source_entities["ades:impact:commodity:coffee"].seed_degree == 0


def test_expand_impact_paths_reports_only_selected_graph_seeds(tmp_path: Path) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    build_response = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact",
        artifact_version="2026-05-05T00:00:00Z",
    )

    result = expand_impact_paths(
        ["entity_hormuz", "entity_iran"],
        enabled_packs=["finance-en", "politics-vector-en"],
        settings=_settings(Path(build_response.artifact_path)),
        max_depth=2,
        impact_expansion_seed_limit=1,
        max_candidates=10,
    )

    source_entities = {source.entity_ref: source for source in result.source_entities}
    assert source_entities["entity_hormuz"].is_graph_seed is True
    assert source_entities["entity_iran"].is_graph_seed is False
    assert source_entities["entity_iran"].seed_degree > 0
    assert result.warnings == ["impact_seed_limit_truncated"]


def test_expand_impact_paths_applies_max_candidates_after_dedup(tmp_path: Path) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    build_response = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact",
        artifact_version="2026-05-05T00:00:00Z",
    )

    result = expand_impact_paths(
        ["entity_gold", "entity_hormuz", "entity_iran"],
        enabled_packs=["finance-en", "politics-vector-en"],
        settings=_settings(Path(build_response.artifact_path)),
        max_depth=2,
        max_candidates=1,
    )

    assert [candidate.entity_ref for candidate in result.candidates] == ["entity_crude_oil"]


def test_expand_impact_paths_bridges_identity_refs_before_traversal(tmp_path: Path) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    node_path.write_text(
        node_path.read_text(encoding="utf-8")
        + "\n".join(
            [
                (
                    "finance-us-issuer:0001318605\tTesla Inc.\tissuer\tfinance-us-en\t0\t1\t"
                    '{"same_as_refs":["wikidata:Q478214"],"ticker_ref":"finance-us-ticker:TSLA"}'
                    "\tfinance-us-en"
                ),
                (
                    "finance-us-ticker:TSLA\tTSLA\tticker\tfinance-us-en\t1\t0\t"
                    '{"same_as_refs":["wikidata:Q478214","finance-us-issuer:0001318605"]}'
                    "\tfinance-us-en"
                ),
                (
                    "country:us\tUnited States\tcountry\tgeneral-en\t0\t1\t"
                    '{"same_as_refs":["wikidata:Q30","geonames:6252001"],"country_code":"US"}'
                    "\tgeneral-en,finance-us-en"
                ),
                "ades:impact:index:us-market\tUS Market\tindex\tfinance-us-en\t1\t0\t{}\tfinance-us-en",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    edge_path.write_text(
        edge_path.read_text(encoding="utf-8")
        + "\n".join(
            [
                (
                    "finance-us-issuer:0001318605\tfinance-us-ticker:TSLA\tissuer_has_listed_ticker"
                    "\tdirect\t0.99\tissuer_exposure\tSEC\thttps://example.test/sec"
                    "\t2026-05-05\t2026\tannual\tfinance-us-en\tbridge"
                ),
                (
                    "country:us\tades:impact:index:us-market\tcountry_affects_market"
                    "\tshallow\t0.88\tmarket_proxy\tReviewed\thttps://example.test/us"
                    "\t2026-05-05\t2026\tannual\tfinance-us-en\tbridge"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    build_response = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact",
        artifact_version="2026-05-05T00:00:00Z",
    )

    result = expand_impact_paths(
        [
            "wikidata:Q478214",
            "wikidata:Q30",
            "ades:heuristic_structural_location:finance-en:location:strait-of-hormuz",
        ],
        enabled_packs=["finance-en", "finance-us-en", "politics-vector-en"],
        settings=_settings(Path(build_response.artifact_path)),
        max_depth=2,
        max_candidates=10,
    )

    candidate_refs = {candidate.entity_ref for candidate in result.candidates}
    assert {
        "finance-us-ticker:TSLA",
        "ades:impact:index:us-market",
        "entity_crude_oil",
    }.issubset(candidate_refs)
    source_entities = {source.entity_ref: source for source in result.source_entities}
    assert "finance-us-ticker:TSLA" in source_entities["wikidata:Q478214"].same_as_refs
    assert "country:us" in source_entities["wikidata:Q30"].same_as_refs
    assert source_entities["wikidata:Q30"].is_graph_seed is True
    assert (
        "entity_hormuz"
        in source_entities[
            "ades:heuristic_structural_location:finance-en:location:strait-of-hormuz"
        ].same_as_refs
    )


def test_expand_impact_paths_bridges_public_person_to_employer_ticker(
    tmp_path: Path,
) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    node_path.write_text(
        node_path.read_text(encoding="utf-8")
        + "\n".join(
            [
                (
                    "wikidata:Q317521\tElon Musk\tperson\tgeneral-en\t0\t1\t"
                    '{"same_as_refs":["person:elon-musk"]}'
                    "\tgeneral-en,finance-us-en"
                ),
                (
                    "finance-us-ticker:TSLA\tTSLA\tticker\tfinance-us-en\t1\t0\t"
                    '{"same_as_refs":["wikidata:Q478214","finance-us-issuer:0001318605"]}'
                    "\tfinance-us-en"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    edge_path.write_text(
        edge_path.read_text(encoding="utf-8")
        + (
            "wikidata:Q317521\tfinance-us-ticker:TSLA\tperson_affects_employer_ticker"
            "\tdirect\t0.86\temployer_equity_proxy\tTesla Investor Relations"
            "\thttps://ir.tesla.com/corporate/elon-musk"
            "\t2026-05-13\t2026\tmonthly\tgeneral-en,finance-us-en\tbridge"
            "\tearnings_beat,earnings_miss,guidance_raise,guidance_cut"
            "\tstrong_person_employer_event"
        )
        + "\n",
        encoding="utf-8",
    )
    build_response = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact",
        artifact_version="2026-05-13T00:00:00Z",
    )

    result = expand_impact_paths(
        ["wikidata:Q317521"],
        enabled_packs=["general-en", "finance-us-en"],
        settings=_settings(Path(build_response.artifact_path)),
        max_depth=2,
        max_candidates=10,
    )

    candidate_refs = {candidate.entity_ref for candidate in result.candidates}
    assert "finance-us-ticker:TSLA" in candidate_refs
    tsla_candidate = next(
        candidate
        for candidate in result.candidates
        if candidate.entity_ref == "finance-us-ticker:TSLA"
    )
    assert (
        tsla_candidate.relationship_paths[0].edges[0].relation == "person_affects_employer_ticker"
    )
    assert "wikidata:Q317521" in tsla_candidate.source_entity_refs


def test_tag_impact_enrichment_does_not_filter_to_response_pack_by_default(
    tmp_path: Path,
) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    build_response = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact",
        artifact_version="2026-05-05T00:00:00Z",
    )
    response = TagResponse(
        version="0.3.1",
        pack="politics-vector-en",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="Hormuz",
                label="location",
                start=0,
                end=6,
                confidence=0.99,
                link=EntityLink(
                    entity_id="entity_hormuz",
                    canonical_text="Strait of Hormuz",
                    provider="ades",
                ),
            )
        ],
        timing_ms=1,
    )

    enriched = enrich_tag_response_with_impact_paths(
        response,
        settings=_settings(Path(build_response.artifact_path)),
        include_impact_paths=True,
        max_depth=1,
        max_candidates=5,
    )

    assert enriched.impact_paths is not None
    assert {candidate.entity_ref for candidate in enriched.impact_paths.candidates} == {
        "entity_crude_oil"
    }


def test_expand_impact_paths_resolves_cross_pack_structural_refs(tmp_path: Path) -> None:
    node_path, edge_path = _write_market_graph_tsvs(tmp_path)
    node_path.write_text(
        node_path.read_text(encoding="utf-8")
        + (
            "ades:heuristic_structural_location:general-en:location:strait-of-hormuz"
            "\tStrait of Hormuz\tchokepoint\tgeneral-en\t0\t1\t{}\tgeneral-en\n"
        ),
        encoding="utf-8",
    )
    edge_path.write_text(
        edge_path.read_text(encoding="utf-8")
        + (
            "ades:heuristic_structural_location:general-en:location:strait-of-hormuz"
            "\tentity_crude_oil\tchokepoint_affects_commodity\tdirect\t0.92"
            "\tsupply_risk\tEIA\thttps://example.test/eia\t2026-05-05\t2024"
            "\tannual\tfinance-en\tprimary\n"
        ),
        encoding="utf-8",
    )
    build_response = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "artifact",
        artifact_version="2026-05-05T00:00:00Z",
    )

    result = expand_impact_paths(
        ["ades:heuristic_structural_location:economics-vector-en:location:strait-of-hormuz"],
        settings=_settings(Path(build_response.artifact_path)),
        max_depth=1,
        max_candidates=5,
    )

    assert {candidate.entity_ref for candidate in result.candidates} == {"entity_crude_oil"}
    assert result.source_entities[0].entity_ref == (
        "ades:heuristic_structural_location:economics-vector-en:location:strait-of-hormuz"
    )
    assert result.source_entities[0].is_graph_seed is True


def test_expand_impact_paths_degrades_when_artifact_missing(tmp_path: Path) -> None:
    result = expand_impact_paths(
        ["entity_iran"],
        settings=_settings(tmp_path / "missing.sqlite"),
    )

    assert result.candidates == []
    assert result.passive_paths == []
    assert result.warnings == ["market_graph_artifact_missing"]
