from pathlib import Path

from fastapi.testclient import TestClient

from ades.impact.graph_builder import build_market_graph_store
from ades.service.app import create_app


def _write_tsv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    path.write_text(
        "\t".join(headers)
        + "\n"
        + "\n".join("\t".join(row) for row in rows)
        + "\n",
        encoding="utf-8",
    )


def _build_artifact(root: Path) -> Path:
    node_path = root / "nodes.tsv"
    edge_path = root / "edges.tsv"
    _write_tsv(
        node_path,
        [
            "entity_ref",
            "canonical_name",
            "entity_type",
            "library_id",
            "is_tradable",
            "is_seed_eligible",
            "identifiers_json",
            "packs",
        ],
        [
            [
                "entity_hormuz",
                "Strait of Hormuz",
                "chokepoint",
                "politics-vector-en",
                "0",
                "1",
                "{}",
                "finance-en",
            ],
            [
                "entity_crude_oil",
                "Crude oil",
                "commodity",
                "finance-en",
                "1",
                "0",
                "{}",
                "finance-en",
            ],
        ],
    )
    _write_tsv(
        edge_path,
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
        ],
        [
            [
                "entity_hormuz",
                "entity_crude_oil",
                "chokepoint_affects_commodity",
                "direct",
                "0.92",
                "supply_risk",
                "EIA",
                "https://example.test/eia",
                "2026-05-05",
                "2024",
                "annual",
                "finance-en",
                "primary",
            ],
        ],
    )
    response = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=root / "artifact",
        artifact_version="2026-05-05T00:00:00Z",
    )
    return Path(response.artifact_path)


def test_impact_expand_endpoint_returns_refs_only_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifact_path = _build_artifact(tmp_path)
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", str(artifact_path))
    client = TestClient(create_app(storage_root=tmp_path / "storage"))

    response = client.post(
        "/v0/impact/expand",
        json={
            "entity_refs": ["entity_hormuz"],
            "enabled_packs": ["finance-en"],
            "max_depth": 2,
            "max_candidates": 25,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["artifact_hash"].startswith("sha256:")
    assert [candidate["entity_ref"] for candidate in payload["candidates"]] == [
        "entity_crude_oil"
    ]
    assert "direction" not in payload
    assert "bullish" not in payload
    assert "bearish" not in payload


def test_market_graph_store_build_endpoint_creates_artifact(tmp_path: Path) -> None:
    node_path = tmp_path / "nodes.tsv"
    edge_path = tmp_path / "edges.tsv"
    _write_tsv(
        node_path,
        [
            "entity_ref",
            "canonical_name",
            "entity_type",
            "library_id",
            "is_tradable",
            "is_seed_eligible",
            "identifiers_json",
            "packs",
        ],
        [
            [
                "entity_hormuz",
                "Strait of Hormuz",
                "chokepoint",
                "politics-vector-en",
                "0",
                "1",
                "{}",
                "finance-en",
            ],
            [
                "entity_crude_oil",
                "Crude oil",
                "commodity",
                "finance-en",
                "1",
                "0",
                "{}",
                "finance-en",
            ],
        ],
    )
    _write_tsv(
        edge_path,
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
        ],
        [
            [
                "entity_hormuz",
                "entity_crude_oil",
                "chokepoint_affects_commodity",
                "direct",
                "0.92",
                "supply_risk",
                "EIA",
                "https://example.test/eia",
                "2026-05-05",
                "2024",
                "annual",
                "finance-en",
                "primary",
            ],
        ],
    )
    client = TestClient(create_app(storage_root=tmp_path / "storage"))

    response = client.post(
        "/v0/registry/build-market-graph-store",
        json={
            "node_tsv_paths": [str(node_path)],
            "edge_tsv_paths": [str(edge_path)],
            "output_dir": str(tmp_path / "artifact"),
            "artifact_version": "2026-05-05T00:00:00Z",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert Path(payload["artifact_path"]).exists()
    assert payload["node_count"] == 2
    assert payload["edge_count"] == 1
