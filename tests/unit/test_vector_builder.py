import gzip
import json
from pathlib import Path

from ades.vector import builder as builder_module


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


def test_build_qid_graph_index_writes_sparse_artifact(tmp_path: Path) -> None:
    bundle_dir = _bundle_dir(tmp_path)
    truthy_path = tmp_path / "truthy.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q43229> .",
                "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q43229> .",
                "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q3> .",
                "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q3> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    response = builder_module.build_qid_graph_index(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact",
        dimensions=16,
        allowed_predicates=["P31", "P463"],
    )

    assert response.point_count == 2
    assert response.bundle_count == 1
    assert response.allowed_predicates == ["P31", "P463"]
    assert "skipped_non_wikidata:general-en:1" in response.warnings
    assert Path(response.artifact_path).exists()
    assert Path(response.manifest_path).exists()

    with gzip.open(response.artifact_path, "rt", encoding="utf-8") as handle:
        records = [json.loads(line) for line in handle]

    assert [record["id"] for record in records] == ["wikidata:Q1", "wikidata:Q2"]
    assert records[0]["payload"]["packs"] == ["general-en"]
    assert records[0]["vector"]


def test_build_qid_graph_index_can_publish_to_qdrant(tmp_path: Path, monkeypatch) -> None:
    bundle_dir = _bundle_dir(tmp_path)
    truthy_path = tmp_path / "truthy.nt"
    truthy_path.write_text(
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q43229> .\n",
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    class _FakeClient:
        def __init__(self, base_url: str, *, api_key: str | None = None) -> None:
            captured["base_url"] = base_url
            captured["api_key"] = api_key

        def __enter__(self) -> "_FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def ensure_collection(self, collection_name: str, *, dimensions: int) -> None:
            captured["collection_name"] = collection_name
            captured["dimensions"] = dimensions

        def upsert_points(self, collection_name: str, points, *, batch_size: int = 256) -> None:
            captured["upsert_collection_name"] = collection_name
            captured["points"] = list(points)
            captured["batch_size"] = batch_size

        def set_alias(self, alias_name: str, collection_name: str) -> None:
            captured["alias_name"] = alias_name
            captured["alias_collection_name"] = collection_name

    monkeypatch.setattr(builder_module, "QdrantVectorSearchClient", _FakeClient)

    response = builder_module.build_qid_graph_index(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact",
        dimensions=8,
        allowed_predicates=["P31"],
        qdrant_url="http://qdrant.local:6333",
        qdrant_api_key="secret",
        collection_name="ades-qids-20260419",
        publish_alias="ades-qids-current",
    )

    assert response.published is True
    assert response.collection_name == "ades-qids-20260419"
    assert response.alias_name == "ades-qids-current"
    assert captured["base_url"] == "http://qdrant.local:6333"
    assert captured["api_key"] == "secret"
    assert captured["collection_name"] == "ades-qids-20260419"
    assert captured["alias_name"] == "ades-qids-current"
    assert len(captured["points"]) == 2
