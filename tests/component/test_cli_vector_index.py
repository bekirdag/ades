import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.service.models import VectorIndexBuildResponse


def test_cli_registry_build_qid_graph_index_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    truthy_path = tmp_path / "truthy.nt.gz"
    truthy_path.write_text("", encoding="utf-8")
    output_dir = tmp_path / "artifact"
    captured: dict[str, object] = {}

    def _fake_build_qid_graph_index(
        bundle_dirs,
        *,
        truthy_path,
        output_dir,
        dimensions,
        allowed_predicates,
        qdrant_url,
        qdrant_api_key,
        collection_name,
        publish_alias,
    ) -> VectorIndexBuildResponse:
        captured["bundle_dirs"] = bundle_dirs
        captured["truthy_path"] = truthy_path
        captured["output_dir"] = output_dir
        captured["dimensions"] = dimensions
        captured["allowed_predicates"] = allowed_predicates
        captured["qdrant_url"] = qdrant_url
        captured["qdrant_api_key"] = qdrant_api_key
        captured["collection_name"] = collection_name
        captured["publish_alias"] = publish_alias
        return VectorIndexBuildResponse(
            output_dir=str(output_dir),
            manifest_path=str(output_dir / "manifest.json"),
            artifact_path=str(output_dir / "points.jsonl.gz"),
            dimensions=dimensions,
            point_count=2,
            target_entity_count=2,
            bundle_count=1,
            bundle_dirs=[str(bundle_dir)],
            pack_ids=["general-en"],
            truthy_path=str(truthy_path),
            processed_line_count=10,
            matched_statement_count=4,
            allowed_predicates=allowed_predicates or ["P31"],
            warnings=[],
        )

    monkeypatch.setattr("ades.cli.api_build_qid_graph_index", _fake_build_qid_graph_index)

    result = runner.invoke(
        app,
        [
            "registry",
            "build-qid-graph-index",
            "--bundle-dir",
            str(bundle_dir),
            "--truthy-path",
            str(truthy_path),
            "--output-dir",
            str(output_dir),
            "--predicate",
            "P31",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["point_count"] == 2
    assert captured["bundle_dirs"] == [bundle_dir]
    assert captured["allowed_predicates"] == ["P31"]


def test_cli_registry_build_qid_graph_index_from_store_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    graph_store_path = tmp_path / "qid_graph_store.sqlite"
    graph_store_path.write_text("", encoding="utf-8")
    output_dir = tmp_path / "artifact"
    captured: dict[str, object] = {}

    def _fake_build_qid_graph_index_from_store(
        bundle_dirs,
        *,
        graph_store_path,
        output_dir,
        dimensions,
        allowed_predicates,
        neighbor_limit_per_qid,
        qdrant_url,
        qdrant_api_key,
        collection_name,
        publish_alias,
    ) -> VectorIndexBuildResponse:
        captured["bundle_dirs"] = bundle_dirs
        captured["graph_store_path"] = graph_store_path
        captured["output_dir"] = output_dir
        captured["dimensions"] = dimensions
        captured["allowed_predicates"] = allowed_predicates
        captured["neighbor_limit_per_qid"] = neighbor_limit_per_qid
        captured["qdrant_url"] = qdrant_url
        captured["qdrant_api_key"] = qdrant_api_key
        captured["collection_name"] = collection_name
        captured["publish_alias"] = publish_alias
        return VectorIndexBuildResponse(
            output_dir=str(output_dir),
            manifest_path=str(output_dir / "manifest.json"),
            artifact_path=str(output_dir / "points.jsonl.gz"),
            build_strategy="graph_store",
            graph_store_path=str(graph_store_path),
            dimensions=dimensions,
            point_count=2,
            target_entity_count=2,
            bundle_count=1,
            bundle_dirs=[str(bundle_dir)],
            pack_ids=["finance-en"],
            truthy_path=None,
            processed_line_count=2,
            matched_statement_count=2,
            allowed_predicates=allowed_predicates or ["P31"],
            warnings=[],
        )

    monkeypatch.setattr(
        "ades.cli.api_build_qid_graph_index_from_store",
        _fake_build_qid_graph_index_from_store,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "build-qid-graph-index-from-store",
            "--bundle-dir",
            str(bundle_dir),
            "--graph-store-path",
            str(graph_store_path),
            "--output-dir",
            str(output_dir),
            "--predicate",
            "P31",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["build_strategy"] == "graph_store"
    assert captured["bundle_dirs"] == [bundle_dir]
    assert captured["allowed_predicates"] == ["P31"]
    assert captured["neighbor_limit_per_qid"] == 128
