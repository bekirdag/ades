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
    source_entities = {source.entity_ref: source for source in result.source_entities}
    assert source_entities["entity_zero"].is_graph_seed is False
    assert source_entities["entity_zero"].seed_degree == 0
    assert result.passive_paths[0].entity_ref in {"entity_hormuz", "entity_opec"}


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
    assert {
        candidate.entity_ref for candidate in enriched.impact_paths.candidates
    } == {"entity_crude_oil"}


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

    assert {
        candidate.entity_ref for candidate in result.candidates
    } == {"entity_crude_oil"}
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
