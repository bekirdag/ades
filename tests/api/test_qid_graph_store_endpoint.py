import json
from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app


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


def test_qid_graph_store_endpoint_builds_explicit_graph_artifact(tmp_path: Path) -> None:
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

    client = TestClient(create_app(storage_root=tmp_path / "service-storage"))
    response = client.post(
        "/v0/registry/build-qid-graph-store",
        json={
            "bundle_dirs": [str(bundle_dir)],
            "truthy_path": str(truthy_path),
            "output_dir": str(tmp_path / "artifact"),
            "predicate": ["P31"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["target_node_count"] == 2
    assert payload["stored_node_count"] == 3
    assert payload["stored_edge_count"] == 2
