import json
from pathlib import Path

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
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q2",
                        "canonical_text": "Microsoft",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
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
