import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.service.models import QidGraphStoreBuildResponse


def test_cli_registry_build_qid_graph_store_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    truthy_path = tmp_path / "truthy.nt.gz"
    truthy_path.write_text("", encoding="utf-8")
    output_dir = tmp_path / "artifact"
    captured: dict[str, object] = {}

    def _fake_build_qid_graph_store(
        bundle_dirs,
        *,
        truthy_path,
        output_dir,
        allowed_predicates,
    ) -> QidGraphStoreBuildResponse:
        captured["bundle_dirs"] = bundle_dirs
        captured["truthy_path"] = truthy_path
        captured["output_dir"] = output_dir
        captured["allowed_predicates"] = allowed_predicates
        return QidGraphStoreBuildResponse(
            output_dir=str(output_dir),
            manifest_path=str(output_dir / "manifest.json"),
            artifact_path=str(output_dir / "qid_graph_store.sqlite"),
            target_node_count=2,
            stored_node_count=4,
            stored_edge_count=4,
            bundle_count=1,
            bundle_dirs=[str(bundle_dir)],
            pack_ids=["general-en"],
            truthy_path=str(truthy_path),
            processed_line_count=10,
            matched_statement_count=4,
            allowed_predicates=allowed_predicates or ["P31"],
            warnings=[],
        )

    monkeypatch.setattr("ades.cli.api_build_qid_graph_store", _fake_build_qid_graph_store)

    result = runner.invoke(
        app,
        [
            "registry",
            "build-qid-graph-store",
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
    assert payload["stored_edge_count"] == 4
    assert captured["bundle_dirs"] == [bundle_dir]
    assert captured["allowed_predicates"] == ["P31"]
