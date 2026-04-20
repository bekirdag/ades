import json
from pathlib import Path

from ades import build_qid_graph_store
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
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return bundle_dir


def test_public_api_can_build_and_query_qid_graph_store(tmp_path: Path) -> None:
    bundle_dir = _bundle_dir(tmp_path / "sources")
    truthy_path = tmp_path / "truthy.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
                "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    response = build_qid_graph_store(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact",
        allowed_predicates=["P31"],
    )

    assert response.target_node_count == 2
    assert response.stored_edge_count == 2

    with QidGraphStore(response.artifact_path) as store:
        shared_ancestors = store.shared_ancestors(["Q1", "Q2"], predicates=["P31"], max_depth=1)
        assert [item.qid for item in shared_ancestors] == ["Q100"]
