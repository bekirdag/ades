import gzip
import json
import subprocess
import sys
from pathlib import Path


SCRIPT_PATH = Path("scripts/build_filtered_qid_collection_from_artifact.py")


def _bundle_dir(root: Path, *, pack_id: str) -> Path:
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
    return bundle_dir


def _write_entities(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row))
            handle.write("\n")


def _write_sparse_artifact(path: Path) -> None:
    points = [
        {
            "id": "wikidata:Q1",
            "payload": {
                "entity_id": "wikidata:Q1",
                "canonical_text": "Alpha",
                "source_name": "wikidata-general-entities",
            },
            "vector": [[0, 1.0], [2, 0.5]],
        },
        {
            "id": "wikidata:Q2",
            "payload": {
                "entity_id": "wikidata:Q2",
                "canonical_text": "Beta",
                "source_name": "synthetic-politics-rss",
            },
            "vector": [[1, 0.75]],
        },
        {
            "id": "wikidata:Q3",
            "payload": {
                "entity_id": "wikidata:Q3",
                "canonical_text": "Gamma",
                "source_name": "wikidata-general-entities",
            },
            "vector": [[3, 0.25]],
        },
    ]
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for point in points:
            handle.write(json.dumps(point))
            handle.write("\n")


def test_filtered_artifact_script_keeps_only_bundle_qids(tmp_path: Path) -> None:
    bundle_a = _bundle_dir(tmp_path / "bundles", pack_id="business-vector-en")
    bundle_b = _bundle_dir(tmp_path / "bundles", pack_id="economics-vector-en")
    _write_entities(
        bundle_a / "normalized" / "entities.jsonl",
        [
            {"entity_id": "wikidata:Q1"},
            {"entity_id": "curated:skip-me"},
        ],
    )
    _write_entities(
        bundle_b / "normalized" / "entities.jsonl",
        [
            {"link": {"entity_id": "wikidata:Q3"}},
            {"link": {"entity_id": "curated:skip-me-too"}},
        ],
    )

    source_artifact = tmp_path / "source" / "qid_graph_points.jsonl.gz"
    source_artifact.parent.mkdir(parents=True)
    _write_sparse_artifact(source_artifact)

    output_dir = tmp_path / "output"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--source-artifact",
            str(source_artifact),
            "--bundle-dir",
            str(bundle_a),
            "--bundle-dir",
            str(bundle_b),
            "--output-dir",
            str(output_dir),
            "--collection-name",
            "ades-qids-candidate-test",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    manifest = json.loads((output_dir / "qid_graph_index_manifest.json").read_text())
    assert manifest["build_strategy"] == "filtered_artifact"
    assert manifest["bundle_qid_count"] == 2
    assert manifest["point_count"] == 2
    assert manifest["published"] is False
    assert json.loads(result.stdout)["collection_name"] == "ades-qids-candidate-test"

    with gzip.open(output_dir / "qid_graph_points.jsonl.gz", "rt", encoding="utf-8") as handle:
        kept_ids = [json.loads(line)["id"] for line in handle]
    assert kept_ids == ["wikidata:Q1", "wikidata:Q3"]


def test_filtered_artifact_script_keeps_explicit_extra_qids(tmp_path: Path) -> None:
    bundle_dir = _bundle_dir(tmp_path / "bundles", pack_id="politics-vector-en")
    _write_entities(
        bundle_dir / "normalized" / "entities.jsonl",
        [
            {"entity_id": "wikidata:Q1"},
        ],
    )
    extra_qids_path = tmp_path / "extra-qids.json"
    extra_qids_path.write_text(
        json.dumps(
            {
                "extra_entity_ids": [
                    "wikidata:Q2",
                    "Q3",
                    "curated:skip-me",
                ]
            }
        ),
        encoding="utf-8",
    )

    source_artifact = tmp_path / "source" / "qid_graph_points.jsonl.gz"
    source_artifact.parent.mkdir(parents=True)
    _write_sparse_artifact(source_artifact)

    output_dir = tmp_path / "output"
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--source-artifact",
            str(source_artifact),
            "--bundle-dir",
            str(bundle_dir),
            "--extra-entity-id",
            "wikidata:Q2",
            "--extra-qid-file",
            str(extra_qids_path),
            "--output-dir",
            str(output_dir),
            "--collection-name",
            "ades-qids-politics-candidate-test",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    manifest = json.loads((output_dir / "qid_graph_index_manifest.json").read_text())
    assert manifest["bundle_qid_count"] == 1
    assert manifest["extra_qid_count"] == 2
    assert manifest["allowed_qid_count"] == 3
    with gzip.open(output_dir / "qid_graph_points.jsonl.gz", "rt", encoding="utf-8") as handle:
        kept_ids = [json.loads(line)["id"] for line in handle]
    assert kept_ids == ["wikidata:Q1", "wikidata:Q2", "wikidata:Q3"]


def test_filtered_artifact_script_can_exclude_payload_source_names(tmp_path: Path) -> None:
    bundle_dir = _bundle_dir(tmp_path / "bundles", pack_id="politics-vector-en")
    _write_entities(
        bundle_dir / "normalized" / "entities.jsonl",
        [
            {"entity_id": "wikidata:Q1"},
            {"entity_id": "wikidata:Q2"},
            {"entity_id": "wikidata:Q3"},
        ],
    )

    source_artifact = tmp_path / "source" / "qid_graph_points.jsonl.gz"
    source_artifact.parent.mkdir(parents=True)
    _write_sparse_artifact(source_artifact)

    output_dir = tmp_path / "output"
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--source-artifact",
            str(source_artifact),
            "--bundle-dir",
            str(bundle_dir),
            "--exclude-source-name",
            "synthetic-politics-rss",
            "--output-dir",
            str(output_dir),
            "--collection-name",
            "ades-qids-politics-clean-test",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    manifest = json.loads((output_dir / "qid_graph_index_manifest.json").read_text())
    assert manifest["excluded_source_names"] == ["synthetic-politics-rss"]
    with gzip.open(output_dir / "qid_graph_points.jsonl.gz", "rt", encoding="utf-8") as handle:
        kept_ids = [json.loads(line)["id"] for line in handle]
    assert kept_ids == ["wikidata:Q1", "wikidata:Q3"]
