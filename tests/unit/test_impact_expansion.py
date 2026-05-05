from pathlib import Path

from ades.config import Settings
from ades.impact.expansion import expand_impact_paths
from ades.impact.graph_builder import build_market_graph_store
from ades.impact.graph_store import MarketGraphStore


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
                "entity_hormuz\tStrait of Hormuz\tchokepoint\tpolitics-vector-en\t0\t1\t{}\tfinance-en,politics-vector-en",
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
            ]
        )
        + "\n"
        + "\n".join(
            [
                "entity_hormuz\tentity_crude_oil\tchokepoint_affects_commodity\tdirect\t0.92\tsupply_risk\tEIA\thttps://example.test/eia\t2026-05-05\t2024\tannual\tfinance-en\tprimary",
                "entity_hormuz\tentity_crude_oil\tchokepoint_affects_commodity\tdirect\t0.80\tsupply_risk\tReviewed\thttps://example.test/reviewed\t2026-05-05\t2024\tannual\tpolitics-vector-en\tduplicate",
                "entity_iran\tentity_hormuz\tcountry_exports_commodity\tshallow\t0.70\tcontext\tReviewed\thttps://example.test/iran\t2026-05-05\t2024\tannual\tpolitics-vector-en\tintermediate",
                "entity_iran\tentity_opec\tcountry_member_of_bloc\tdirect\t0.95\tcontext\tOPEC\thttps://example.test/opec\t2026-05-05\t2026\tannual\tfinance-en\tpassive",
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
    assert any(warning.startswith("duplicate_edge_merged") for warning in response.warnings)

    with MarketGraphStore(response.artifact_path) as store:
        nodes = store.node_batch(["entity_hormuz", "entity_zero"])
        assert nodes["entity_hormuz"].seed_degree == 1
        assert nodes["entity_zero"].seed_degree == 0
        edges = store.outbound_edges_batch(["entity_hormuz"])["entity_hormuz"]
        assert len(edges) == 1
        assert edges[0].confidence == 0.92
        assert edges[0].packs == ("finance-en", "politics-vector-en")


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
    source_entities = {source.entity_ref: source for source in result.source_entities}
    assert source_entities["entity_zero"].is_graph_seed is False
    assert source_entities["entity_zero"].seed_degree == 0
    assert result.passive_paths[0].entity_ref in {"entity_hormuz", "entity_opec"}


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


def test_expand_impact_paths_degrades_when_artifact_missing(tmp_path: Path) -> None:
    result = expand_impact_paths(
        ["entity_iran"],
        settings=_settings(tmp_path / "missing.sqlite"),
    )

    assert result.candidates == []
    assert result.passive_paths == []
    assert result.warnings == ["market_graph_artifact_missing"]
