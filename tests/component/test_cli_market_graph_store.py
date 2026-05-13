import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.service.models import MarketGraphStoreBuildResponse


def _market_response(
    output_dir: Path,
    *,
    release_gate_commands: list[str] | None = None,
    release_gate_working_dir: Path | None = None,
    release_gate_passed: bool = True,
) -> MarketGraphStoreBuildResponse:
    return MarketGraphStoreBuildResponse(
        output_dir=str(output_dir),
        manifest_path=str(output_dir / "market_graph_store_manifest.json"),
        artifact_path=str(output_dir / "market_graph_store.sqlite"),
        graph_version="market-graph-v1",
        artifact_version="2026-05-13T00:00:00Z",
        artifact_hash="sha256:test",
        builder_version="market-graph-builder-v3",
        node_count=2,
        edge_count=1,
        source_manifest_hash="sha256:test",
        node_tsv_paths=[],
        edge_tsv_paths=[],
        pack_ids=["finance-en"],
        processed_edge_row_count=1,
        release_gate_commands=release_gate_commands or [],
        release_gate_working_dir=(
            str(release_gate_working_dir) if release_gate_working_dir else None
        ),
        release_gate_passed=release_gate_passed,
        warnings=[] if release_gate_passed else ["release_gate_failed:false:exit_code=1"],
    )


def test_cli_registry_build_market_graph_store_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    edge_path = tmp_path / "edges.tsv"
    edge_path.write_text("", encoding="utf-8")
    node_path = tmp_path / "nodes.tsv"
    node_path.write_text("", encoding="utf-8")
    output_dir = tmp_path / "artifact"
    captured: dict[str, object] = {}

    def _fake_build_market_graph_store(
        *,
        edge_tsv_paths,
        output_dir,
        node_tsv_paths,
        graph_version,
        artifact_version,
        release_gate_commands,
        release_gate_working_dir,
    ) -> MarketGraphStoreBuildResponse:
        captured["edge_tsv_paths"] = edge_tsv_paths
        captured["output_dir"] = output_dir
        captured["node_tsv_paths"] = node_tsv_paths
        captured["graph_version"] = graph_version
        captured["artifact_version"] = artifact_version
        captured["release_gate_commands"] = release_gate_commands
        captured["release_gate_working_dir"] = release_gate_working_dir
        return _market_response(
            output_dir,
            release_gate_commands=release_gate_commands,
            release_gate_working_dir=release_gate_working_dir,
        )

    monkeypatch.setattr(
        "ades.cli.api_build_market_graph_store",
        _fake_build_market_graph_store,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "build-market-graph-store",
            "--edge-tsv-path",
            str(edge_path),
            "--node-tsv-path",
            str(node_path),
            "--output-dir",
            str(output_dir),
            "--artifact-version",
            "2026-05-13T00:00:00Z",
            "--release-gate-command",
            "echo market-gate",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["edge_count"] == 1
    assert captured["edge_tsv_paths"] == [edge_path]
    assert captured["node_tsv_paths"] == [node_path]
    assert captured["release_gate_commands"] == ["echo market-gate"]


def test_cli_registry_build_market_graph_store_exits_on_release_gate_failure(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    edge_path = tmp_path / "edges.tsv"
    edge_path.write_text("", encoding="utf-8")
    output_dir = tmp_path / "artifact"

    def _fake_build_market_graph_store(
        *,
        edge_tsv_paths,
        output_dir,
        node_tsv_paths,
        graph_version,
        artifact_version,
        release_gate_commands,
        release_gate_working_dir,
    ) -> MarketGraphStoreBuildResponse:
        return _market_response(
            output_dir,
            release_gate_commands=release_gate_commands,
            release_gate_working_dir=release_gate_working_dir,
            release_gate_passed=False,
        )

    monkeypatch.setattr(
        "ades.cli.api_build_market_graph_store",
        _fake_build_market_graph_store,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "build-market-graph-store",
            "--edge-tsv-path",
            str(edge_path),
            "--output-dir",
            str(output_dir),
            "--release-gate-command",
            "false",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["release_gate_passed"] is False


def test_cli_registry_build_starter_market_graph_store_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    output_dir = tmp_path / "artifact"
    captured: dict[str, object] = {}

    def _fake_build_starter_market_graph_store(
        *,
        output_dir,
        artifact_version,
        release_gate_commands,
        release_gate_working_dir,
    ) -> MarketGraphStoreBuildResponse:
        captured["output_dir"] = output_dir
        captured["artifact_version"] = artifact_version
        captured["release_gate_commands"] = release_gate_commands
        captured["release_gate_working_dir"] = release_gate_working_dir
        return _market_response(
            output_dir,
            release_gate_commands=release_gate_commands,
            release_gate_working_dir=release_gate_working_dir,
        )

    monkeypatch.setattr(
        "ades.cli.api_build_starter_market_graph_store",
        _fake_build_starter_market_graph_store,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "build-starter-market-graph-store",
            "--output-dir",
            str(output_dir),
            "--release-gate-command",
            "echo starter-gate",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["builder_version"] == "market-graph-builder-v3"
    assert captured["output_dir"] == output_dir
    assert captured["release_gate_commands"] == ["echo starter-gate"]
