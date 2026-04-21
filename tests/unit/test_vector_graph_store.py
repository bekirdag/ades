import json
from pathlib import Path
import sqlite3

from ades.vector import graph_builder as graph_builder_module
from ades.vector.graph_store import QidGraphStore


def _bundle_dir(root: Path, *, pack_id: str = "general-en") -> Path:
    bundle_dir = root / pack_id
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True)
    (bundle_dir / "bundle.json").write_text(
        json.dumps(
            {
                "pack_id": pack_id,
                "entities_path": "normalized/entities.jsonl",
            }
        ),
        encoding="utf-8",
    )
    (normalized_dir / "entities.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "entity_id": "wikidata:Q1",
                        "canonical_text": "OpenAI",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 91,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q2",
                        "canonical_text": "Microsoft",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 96,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "curated:company:123",
                        "canonical_text": "Local Only",
                        "entity_type": "organization",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return bundle_dir


def test_build_qid_graph_store_writes_sqlite_artifact(tmp_path: Path) -> None:
    bundle_dir = _bundle_dir(tmp_path)
    truthy_path = tmp_path / "truthy.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
                "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
                "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
                "<http://www.wikidata.org/entity/Q900> <http://www.wikidata.org/prop/direct/P17> <http://www.wikidata.org/entity/Q901> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    response = graph_builder_module.build_qid_graph_store(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact",
        allowed_predicates=["P31", "P463"],
    )

    assert response.target_node_count == 2
    assert response.stored_node_count == 4
    assert response.stored_edge_count == 4
    assert response.bundle_count == 1
    assert response.allowed_predicates == ["P31", "P463"]
    assert response.adjacent_node_count == 2
    assert response.predicate_histogram == {"P31": 2, "P463": 2}
    assert response.max_degree_total >= 2
    assert response.p95_degree_total >= 1
    assert "skipped_non_wikidata:general-en:1" in response.warnings
    assert Path(response.artifact_path).exists()
    assert Path(response.manifest_path).exists()


def test_qid_graph_store_supports_neighbors_paths_and_shared_ancestors(tmp_path: Path) -> None:
    bundle_dir = _bundle_dir(tmp_path)
    truthy_path = tmp_path / "truthy.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
                "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
                "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    response = graph_builder_module.build_qid_graph_store(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact",
        allowed_predicates=["P31", "P463"],
    )

    with QidGraphStore(response.artifact_path) as store:
        metadata_batch = store.node_metadata_batch(["Q1", "Q100", "Q999"])
        assert metadata_batch["Q1"].canonical_text == "OpenAI"
        assert metadata_batch["Q1"].popularity == 91.0
        assert metadata_batch["Q100"].entity_id == "wikidata:Q100"
        assert metadata_batch["Q999"].canonical_text == "Q999"

        stats_batch = store.node_stats_batch(["Q1", "Q100"])
        assert stats_batch["Q1"].degree_total == 2
        assert stats_batch["Q100"].in_degree == 2

        batched_neighbors = store.neighbors_batch(
            ["Q1", "Q2"],
            direction="out",
            predicates=["P31", "P463"],
            limit_per_qid=4,
        )
        assert [neighbor.qid for neighbor in batched_neighbors["Q1"]] == ["Q100", "Q300"]
        assert [neighbor.qid for neighbor in batched_neighbors["Q2"]] == ["Q100", "Q300"]

        neighbors = store.neighbors("Q1", direction="out", predicates=["P31"])
        assert [(neighbor.qid, neighbor.predicate) for neighbor in neighbors] == [("Q100", "P31")]

        path = store.shortest_path("Q1", "Q2", max_depth=2)
        assert path is not None
        assert path.node_qids[0] == "Q1"
        assert path.node_qids[-1] == "Q2"
        assert len(path.steps) == 2

        shared_neighbors = store.shared_neighbors(["Q1", "Q2"], predicates=["P31", "P463"])
        assert [item.qid for item in shared_neighbors] == ["Q100", "Q300"]

        shared_ancestors = store.shared_ancestors(["Q1", "Q2"], predicates=["P31"], max_depth=1)
        assert [item.qid for item in shared_ancestors] == ["Q100"]


def test_qid_graph_store_chunks_large_in_queries(tmp_path: Path, monkeypatch) -> None:
    bundle_dir = _bundle_dir(tmp_path)
    truthy_path = tmp_path / "truthy.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
                "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
                "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
            ]
            + [
                (
                    f"<http://www.wikidata.org/entity/Q1> "
                    f"<http://www.wikidata.org/prop/direct/P463> "
                    f"<http://www.wikidata.org/entity/Q{1000 + index}> ."
                )
                for index in range(6)
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    response = graph_builder_module.build_qid_graph_store(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact",
        allowed_predicates=["P31", "P463"],
    )

    monkeypatch.setattr("ades.vector.graph_store._SQLITE_IN_CLAUSE_BATCH_SIZE", 2)

    with QidGraphStore(response.artifact_path) as store:
        metadata_batch = store.node_metadata_batch(["Q1", "Q2", "Q100", "Q300", "Q1001", "Q1002"])
        assert metadata_batch["Q1"].canonical_text == "OpenAI"
        assert metadata_batch["Q1001"].entity_id == "wikidata:Q1001"

        stats_batch = store.node_stats_batch(["Q1", "Q2", "Q100", "Q300", "Q1001", "Q1002"])
        assert stats_batch["Q100"].in_degree == 2
        assert stats_batch["Q1001"].degree_total == 1

        neighbors_batch = store.neighbors_batch(
            ["Q1", "Q2", "Q100", "Q300", "Q1001", "Q1002"],
            direction="both",
            predicates=["P31", "P463"],
        )
        assert any(neighbor.qid == "Q1001" for neighbor in neighbors_batch["Q1"])
        assert any(neighbor.qid == "Q1" for neighbor in neighbors_batch["Q1001"])

        limited_neighbors = store.neighbors_batch(
            ["Q1"],
            direction="out",
            predicates=["P31", "P463"],
            limit_per_qid=2,
        )
        assert [neighbor.qid for neighbor in limited_neighbors["Q1"]] == ["Q100", "Q1000"]


def test_qid_graph_store_supports_legacy_nodes_without_popularity(tmp_path: Path) -> None:
    artifact_path = tmp_path / "legacy.sqlite"
    connection = sqlite3.connect(artifact_path)
    try:
        connection.executescript(
            """
            CREATE TABLE nodes (
                qid TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL,
                canonical_text TEXT NOT NULL,
                entity_type TEXT,
                source_name TEXT
            );
            CREATE TABLE node_packs (
                qid TEXT NOT NULL,
                pack_id TEXT NOT NULL
            );
            CREATE TABLE node_stats (
                qid TEXT PRIMARY KEY,
                out_degree INTEGER NOT NULL DEFAULT 0,
                in_degree INTEGER NOT NULL DEFAULT 0,
                degree_total INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE edges (
                src_qid TEXT NOT NULL,
                predicate TEXT NOT NULL,
                dst_qid TEXT NOT NULL
            );
            """
        )
        connection.executemany(
            "INSERT INTO nodes (qid, entity_id, canonical_text, entity_type, source_name) VALUES (?, ?, ?, ?, ?)",
            [
                ("Q1", "wikidata:Q1", "OpenAI", "organization", "wikidata-general-entities"),
                ("Q100", "wikidata:Q100", "AI lab", "organization", "wikidata-general-entities"),
            ],
        )
        connection.executemany(
            "INSERT INTO node_packs (qid, pack_id) VALUES (?, ?)",
            [("Q1", "general-en"), ("Q100", "general-en")],
        )
        connection.executemany(
            "INSERT INTO node_stats (qid, out_degree, in_degree, degree_total) VALUES (?, ?, ?, ?)",
            [("Q1", 1, 0, 1), ("Q100", 0, 1, 1)],
        )
        connection.execute(
            "INSERT INTO edges (src_qid, predicate, dst_qid) VALUES (?, ?, ?)",
            ("Q1", "P31", "Q100"),
        )
        connection.commit()
    finally:
        connection.close()

    with QidGraphStore(artifact_path) as store:
        metadata = store.node_metadata_batch(["Q1", "Q100"])
        assert metadata["Q1"].canonical_text == "OpenAI"
        assert metadata["Q1"].popularity is None

        neighbors = store.neighbors_batch(["Q1"], direction="out", predicates=["P31"])
        assert [neighbor.qid for neighbor in neighbors["Q1"]] == ["Q100"]
        assert neighbors["Q1"][0].popularity is None
