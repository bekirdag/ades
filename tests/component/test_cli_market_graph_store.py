import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.impact.bdya_gap_backlog import BdyaGapBacklogImportResult
from ades.impact.finance_country_proxy import FinanceCountryProxyBuildResult
from ades.impact.issuer_exposure import IssuerExposureBuildResult
from ades.impact.news_analysis_evaluation import (
    ImpactNewsAnalysisGoldenCaseResult,
    ImpactNewsAnalysisGoldenSetReport,
)
from ades.impact.policy_sector_proxy import PolicySectorProxyBuildResult
from ades.impact.program_org_relationship import ProgramOrgRelationshipBuildResult
from ades.impact.proposal_promoter import ProposalPromotionResult
from ades.impact.source_lane_inputs import FinanceCountrySourceLaneInputDerivationResult
from ades.impact.source_lane_validation import ImpactSourceLaneValidationResult
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


def _impact_news_golden_report(*, passed: bool) -> ImpactNewsAnalysisGoldenSetReport:
    case_result = ImpactNewsAnalysisGoldenCaseResult(
        case_id="case-1",
        expected_terminal_refs=["finance-us:equity:EXM"],
        actual_terminal_refs=["finance-us:equity:EXM"] if passed else [],
        missing_terminal_refs=[] if passed else ["finance-us:equity:EXM"],
        terminal_candidate_recall=1.0 if passed else 0.0,
        terminal_candidate_precision=1.0,
        source_entity_recall=1.0,
        event_type_recall=1.0,
        passed=passed,
    )
    return ImpactNewsAnalysisGoldenSetReport(
        schema="ades.impact_news_analysis_golden_report.v1",
        golden_set_path="/tmp/ades_impact_golden.jsonl",
        case_count=1,
        passed_case_count=1 if passed else 0,
        failed_case_count=0 if passed else 1,
        terminal_candidate_recall=1.0 if passed else 0.0,
        terminal_candidate_precision=1.0,
        source_entity_recall=1.0,
        event_type_recall=1.0,
        forbidden_candidate_hit_count=0,
        unexpected_candidate_count=0,
        warning_count=0,
        p95_latency_ms=4,
        cases=[case_result],
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


def test_cli_registry_build_finance_country_proxy_source_lane_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    packs_root = tmp_path / "packs"
    packs_root.mkdir()
    output_root = tmp_path / "source_lanes"
    artifact_root = tmp_path / "artifacts"
    captured: dict[str, object] = {}

    def _fake_build_finance_country_proxy_source_lane(
        *,
        packs_root,
        output_root,
        run_id,
        artifact_output_root,
        build_artifact,
        include_starter_graph,
        extra_proxy_pack_ids,
        minimum_edge_count,
    ) -> FinanceCountryProxyBuildResult:
        captured["packs_root"] = packs_root
        captured["output_root"] = output_root
        captured["run_id"] = run_id
        captured["artifact_output_root"] = artifact_output_root
        captured["build_artifact"] = build_artifact
        captured["include_starter_graph"] = include_starter_graph
        captured["extra_proxy_pack_ids"] = extra_proxy_pack_ids
        captured["minimum_edge_count"] = minimum_edge_count
        output_dir = output_root / run_id
        return FinanceCountryProxyBuildResult(
            run_id=run_id,
            output_dir=output_dir,
            node_tsv_path=output_dir / "impact_nodes.tsv",
            edge_tsv_path=output_dir / "impact_edges.tsv",
            manifest_path=output_dir / "manifest.json",
            artifact_path=artifact_output_root / run_id / "market_graph_store.sqlite",
            artifact_node_count=11,
            artifact_edge_count=10,
            pack_count=2,
            extra_proxy_pack_count=1,
            entity_count=8,
            extra_proxy_entity_count=3,
            node_count=7,
            edge_count=6,
            terminal_node_count=4,
            uncovered_entity_count=1,
            relation_counts={"issuer_has_listed_ticker": 2},
            edge_family_counts={"identity_listing": 2},
        )

    monkeypatch.setattr(
        "ades.cli.api_build_finance_country_proxy_source_lane",
        _fake_build_finance_country_proxy_source_lane,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "build-finance-country-proxy-source-lane",
            "--packs-root",
            str(packs_root),
            "--output-root",
            str(output_root),
            "--run-id",
            "proxy-run",
            "--build-artifact",
            "--artifact-output-root",
            str(artifact_root),
            "--no-starter-graph",
            "--extra-proxy-pack-id",
            "business-vector-en",
            "--minimum-edge-count",
            "10",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["run_id"] == "proxy-run"
    assert payload["artifact_edge_count"] == 10
    assert payload["artifact_path"] == str(
        artifact_root / "proxy-run" / "market_graph_store.sqlite"
    )
    assert captured["packs_root"] == packs_root
    assert captured["output_root"] == output_root
    assert captured["artifact_output_root"] == artifact_root
    assert captured["build_artifact"] is True
    assert captured["include_starter_graph"] is False
    assert captured["extra_proxy_pack_ids"] == ["business-vector-en"]
    assert captured["minimum_edge_count"] == 10


def test_cli_registry_derive_finance_country_source_lane_inputs_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    packs_root = tmp_path / "packs"
    packs_root.mkdir()
    output_root = tmp_path / "source_lane_inputs"
    captured: dict[str, object] = {}

    def _fake_derive_finance_country_source_lane_inputs(
        *,
        packs_root,
        output_root,
        run_id,
    ) -> FinanceCountrySourceLaneInputDerivationResult:
        captured["packs_root"] = packs_root
        captured["output_root"] = output_root
        captured["run_id"] = run_id
        output_dir = output_root / run_id
        return FinanceCountrySourceLaneInputDerivationResult(
            run_id=run_id,
            output_dir=output_dir,
            issuer_sector_tsv_path=output_dir / "issuer_sector.tsv",
            issuer_geography_tsv_path=output_dir / "issuer_geography.tsv",
            manifest_path=output_dir / "manifest.json",
            pack_count=2,
            issuer_count=7,
            ticker_resolved_count=6,
            issuer_sector_row_count=5,
            issuer_geography_row_count=7,
        )

    monkeypatch.setattr(
        "ades.cli.api_derive_finance_country_source_lane_inputs",
        _fake_derive_finance_country_source_lane_inputs,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "derive-finance-country-source-lane-inputs",
            "--packs-root",
            str(packs_root),
            "--output-root",
            str(output_root),
            "--run-id",
            "source-input-run",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["run_id"] == "source-input-run"
    assert payload["issuer_sector_row_count"] == 5
    assert payload["issuer_geography_tsv_path"] == str(
        output_root / "source-input-run" / "issuer_geography.tsv"
    )
    assert captured["packs_root"] == packs_root
    assert captured["output_root"] == output_root
    assert captured["run_id"] == "source-input-run"


def test_cli_registry_evaluate_impact_news_analysis_golden_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    golden_path = tmp_path / "ades_impact_golden.jsonl"
    golden_path.write_text("", encoding="utf-8")
    artifact_path = tmp_path / "market_graph_store.sqlite"
    artifact_path.write_text("", encoding="utf-8")
    storage_root = tmp_path / "storage"
    captured: dict[str, object] = {}

    def _fake_evaluate_impact_news_analysis_golden_set(
        **kwargs,
    ) -> ImpactNewsAnalysisGoldenSetReport:
        captured.update(kwargs)
        return _impact_news_golden_report(passed=True)

    monkeypatch.setattr(
        "ades.cli.api_evaluate_impact_news_analysis_golden_set",
        _fake_evaluate_impact_news_analysis_golden_set,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "evaluate-impact-news-analysis-golden",
            "--golden-set-path",
            str(golden_path),
            "--artifact-path",
            str(artifact_path),
            "--storage-root",
            str(storage_root),
            "--pack",
            "general-en",
            "--pack",
            "finance-en",
            "--reviewed-only",
            "--max-cases",
            "5",
            "--max-depth",
            "3",
            "--impact-seed-limit",
            "4",
            "--max-terminal-candidates",
            "12",
            "--min-terminal-candidate-recall",
            "0.9",
            "--min-terminal-candidate-precision",
            "0.85",
            "--min-source-entity-recall",
            "0.75",
            "--min-event-type-recall",
            "0.7",
            "--allow-warnings",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["passed"] is True
    assert payload["case_count"] == 1
    assert captured["golden_set_path"] == golden_path
    assert captured["artifact_path"] == artifact_path
    assert captured["storage_root"] == storage_root
    assert captured["packs"] == ["general-en", "finance-en"]
    assert captured["reviewed_only"] is True
    assert captured["max_cases"] == 5
    assert captured["max_depth"] == 3
    assert captured["impact_seed_limit"] == 4
    assert captured["max_terminal_candidates"] == 12
    assert captured["min_terminal_candidate_recall"] == 0.9
    assert captured["min_terminal_candidate_precision"] == 0.85
    assert captured["min_source_entity_recall"] == 0.75
    assert captured["min_event_type_recall"] == 0.7
    assert captured["fail_on_warnings"] is False


def test_cli_registry_evaluate_impact_news_analysis_golden_exits_on_failure(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    golden_path = tmp_path / "ades_impact_golden.jsonl"
    golden_path.write_text("", encoding="utf-8")

    def _fake_evaluate_impact_news_analysis_golden_set(
        **kwargs,
    ) -> ImpactNewsAnalysisGoldenSetReport:
        return _impact_news_golden_report(passed=False)

    monkeypatch.setattr(
        "ades.cli.api_evaluate_impact_news_analysis_golden_set",
        _fake_evaluate_impact_news_analysis_golden_set,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "evaluate-impact-news-analysis-golden",
            "--golden-set-path",
            str(golden_path),
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["passed"] is False
    assert payload["failed_case_count"] == 1


def test_cli_registry_build_policy_sector_source_lane_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    policy_path = tmp_path / "policy_sector.tsv"
    policy_path.write_text("", encoding="utf-8")
    issuer_path = tmp_path / "issuer_sector.tsv"
    issuer_path.write_text("", encoding="utf-8")
    output_root = tmp_path / "source_lanes"
    artifact_root = tmp_path / "artifacts"
    extra_node_path = tmp_path / "extra_nodes.tsv"
    extra_node_path.write_text("", encoding="utf-8")
    extra_edge_path = tmp_path / "extra_edges.tsv"
    extra_edge_path.write_text("", encoding="utf-8")
    captured: dict[str, object] = {}

    def _fake_build_policy_sector_source_lane(
        *,
        policy_sector_tsv_paths,
        issuer_sector_tsv_paths,
        output_root,
        run_id,
        artifact_output_root,
        build_artifact,
        include_starter_graph,
        extra_node_tsv_paths,
        extra_edge_tsv_paths,
        namespace,
    ) -> PolicySectorProxyBuildResult:
        captured["policy_sector_tsv_paths"] = policy_sector_tsv_paths
        captured["issuer_sector_tsv_paths"] = issuer_sector_tsv_paths
        captured["output_root"] = output_root
        captured["run_id"] = run_id
        captured["artifact_output_root"] = artifact_output_root
        captured["build_artifact"] = build_artifact
        captured["include_starter_graph"] = include_starter_graph
        captured["extra_node_tsv_paths"] = extra_node_tsv_paths
        captured["extra_edge_tsv_paths"] = extra_edge_tsv_paths
        captured["namespace"] = namespace
        output_dir = output_root / run_id
        return PolicySectorProxyBuildResult(
            run_id=run_id,
            output_dir=output_dir,
            node_tsv_path=output_dir / "impact_nodes.tsv",
            edge_tsv_path=output_dir / "impact_edges.tsv",
            manifest_path=output_dir / "manifest.json",
            artifact_path=artifact_output_root / run_id / "market_graph_store.sqlite",
            artifact_node_count=7,
            artifact_edge_count=6,
            policy_sector_row_count=2,
            issuer_sector_row_count=3,
            node_count=5,
            edge_count=4,
            relation_counts={"law_affects_sector": 1, "sector_affects_issuer": 1},
        )

    monkeypatch.setattr(
        "ades.cli.api_build_policy_sector_source_lane",
        _fake_build_policy_sector_source_lane,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "build-policy-sector-source-lane",
            "--policy-sector-tsv-path",
            str(policy_path),
            "--issuer-sector-tsv-path",
            str(issuer_path),
            "--output-root",
            str(output_root),
            "--run-id",
            "policy-run",
            "--build-artifact",
            "--artifact-output-root",
            str(artifact_root),
            "--no-starter-graph",
            "--extra-node-tsv-path",
            str(extra_node_path),
            "--extra-edge-tsv-path",
            str(extra_edge_path),
            "--namespace",
            "custom-policy",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["run_id"] == "policy-run"
    assert payload["artifact_edge_count"] == 6
    assert payload["artifact_path"] == str(
        artifact_root / "policy-run" / "market_graph_store.sqlite"
    )
    assert captured["policy_sector_tsv_paths"] == [policy_path]
    assert captured["issuer_sector_tsv_paths"] == [issuer_path]
    assert captured["output_root"] == output_root
    assert captured["artifact_output_root"] == artifact_root
    assert captured["build_artifact"] is True
    assert captured["include_starter_graph"] is False
    assert captured["extra_node_tsv_paths"] == [extra_node_path]
    assert captured["extra_edge_tsv_paths"] == [extra_edge_path]
    assert captured["namespace"] == "custom-policy"


def test_cli_registry_build_program_org_relationship_source_lane_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    relationship_path = tmp_path / "program_org_relationships.tsv"
    relationship_path.write_text("", encoding="utf-8")
    output_root = tmp_path / "source_lanes"
    artifact_root = tmp_path / "artifacts"
    extra_node_path = tmp_path / "extra_nodes.tsv"
    extra_node_path.write_text("", encoding="utf-8")
    extra_edge_path = tmp_path / "extra_edges.tsv"
    extra_edge_path.write_text("", encoding="utf-8")
    captured: dict[str, object] = {}

    def _fake_build_program_org_relationship_source_lane(
        *,
        relationship_tsv_paths,
        output_root,
        run_id,
        artifact_output_root,
        build_artifact,
        include_starter_graph,
        extra_node_tsv_paths,
        extra_edge_tsv_paths,
        namespace,
    ) -> ProgramOrgRelationshipBuildResult:
        captured["relationship_tsv_paths"] = relationship_tsv_paths
        captured["output_root"] = output_root
        captured["run_id"] = run_id
        captured["artifact_output_root"] = artifact_output_root
        captured["build_artifact"] = build_artifact
        captured["include_starter_graph"] = include_starter_graph
        captured["extra_node_tsv_paths"] = extra_node_tsv_paths
        captured["extra_edge_tsv_paths"] = extra_edge_tsv_paths
        captured["namespace"] = namespace
        output_dir = output_root / run_id
        return ProgramOrgRelationshipBuildResult(
            run_id=run_id,
            output_dir=output_dir,
            node_tsv_path=output_dir / "impact_nodes.tsv",
            edge_tsv_path=output_dir / "impact_edges.tsv",
            manifest_path=output_dir / "manifest.json",
            artifact_path=artifact_output_root / run_id / "market_graph_store.sqlite",
            artifact_node_count=7,
            artifact_edge_count=6,
            relationship_row_count=4,
            node_count=5,
            edge_count=4,
            relation_counts={"program_operated_by_org": 1},
            node_type_counts={"program": 1, "issuer": 1},
        )

    monkeypatch.setattr(
        "ades.cli.api_build_program_org_relationship_source_lane",
        _fake_build_program_org_relationship_source_lane,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "build-program-org-relationship-source-lane",
            "--relationship-tsv-path",
            str(relationship_path),
            "--output-root",
            str(output_root),
            "--run-id",
            "program-org-run",
            "--build-artifact",
            "--artifact-output-root",
            str(artifact_root),
            "--no-starter-graph",
            "--extra-node-tsv-path",
            str(extra_node_path),
            "--extra-edge-tsv-path",
            str(extra_edge_path),
            "--namespace",
            "custom-program-org",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["run_id"] == "program-org-run"
    assert payload["artifact_edge_count"] == 6
    assert payload["artifact_path"] == str(
        artifact_root / "program-org-run" / "market_graph_store.sqlite"
    )
    assert captured["relationship_tsv_paths"] == [relationship_path]
    assert captured["output_root"] == output_root
    assert captured["artifact_output_root"] == artifact_root
    assert captured["build_artifact"] is True
    assert captured["include_starter_graph"] is False
    assert captured["extra_node_tsv_paths"] == [extra_node_path]
    assert captured["extra_edge_tsv_paths"] == [extra_edge_path]
    assert captured["namespace"] == "custom-program-org"


def test_cli_registry_build_issuer_exposure_source_lane_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    commodity_path = tmp_path / "issuer_commodity.tsv"
    commodity_path.write_text("", encoding="utf-8")
    geography_path = tmp_path / "issuer_geography.tsv"
    geography_path.write_text("", encoding="utf-8")
    supply_chain_path = tmp_path / "issuer_supply_chain.tsv"
    supply_chain_path.write_text("", encoding="utf-8")
    output_root = tmp_path / "source_lanes"
    artifact_root = tmp_path / "artifacts"
    extra_node_path = tmp_path / "extra_nodes.tsv"
    extra_node_path.write_text("", encoding="utf-8")
    extra_edge_path = tmp_path / "extra_edges.tsv"
    extra_edge_path.write_text("", encoding="utf-8")
    captured: dict[str, object] = {}

    def _fake_build_issuer_exposure_source_lane(
        *,
        issuer_commodity_tsv_paths,
        issuer_geography_tsv_paths,
        issuer_supply_chain_tsv_paths,
        output_root,
        run_id,
        artifact_output_root,
        build_artifact,
        include_starter_graph,
        extra_node_tsv_paths,
        extra_edge_tsv_paths,
        namespace,
    ) -> IssuerExposureBuildResult:
        captured["issuer_commodity_tsv_paths"] = issuer_commodity_tsv_paths
        captured["issuer_geography_tsv_paths"] = issuer_geography_tsv_paths
        captured["issuer_supply_chain_tsv_paths"] = issuer_supply_chain_tsv_paths
        captured["output_root"] = output_root
        captured["run_id"] = run_id
        captured["artifact_output_root"] = artifact_output_root
        captured["build_artifact"] = build_artifact
        captured["include_starter_graph"] = include_starter_graph
        captured["extra_node_tsv_paths"] = extra_node_tsv_paths
        captured["extra_edge_tsv_paths"] = extra_edge_tsv_paths
        captured["namespace"] = namespace
        output_dir = output_root / run_id
        return IssuerExposureBuildResult(
            run_id=run_id,
            output_dir=output_dir,
            node_tsv_path=output_dir / "impact_nodes.tsv",
            edge_tsv_path=output_dir / "impact_edges.tsv",
            manifest_path=output_dir / "manifest.json",
            artifact_path=artifact_output_root / run_id / "market_graph_store.sqlite",
            artifact_node_count=9,
            artifact_edge_count=8,
            issuer_commodity_row_count=2,
            issuer_geography_row_count=3,
            issuer_supply_chain_row_count=4,
            node_count=7,
            edge_count=6,
            relation_counts={"commodity_affects_issuer_revenue": 1},
        )

    monkeypatch.setattr(
        "ades.cli.api_build_issuer_exposure_source_lane",
        _fake_build_issuer_exposure_source_lane,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "build-issuer-exposure-source-lane",
            "--issuer-commodity-tsv-path",
            str(commodity_path),
            "--issuer-geography-tsv-path",
            str(geography_path),
            "--issuer-supply-chain-tsv-path",
            str(supply_chain_path),
            "--output-root",
            str(output_root),
            "--run-id",
            "issuer-run",
            "--build-artifact",
            "--artifact-output-root",
            str(artifact_root),
            "--no-starter-graph",
            "--extra-node-tsv-path",
            str(extra_node_path),
            "--extra-edge-tsv-path",
            str(extra_edge_path),
            "--namespace",
            "custom-issuer-exposure",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["run_id"] == "issuer-run"
    assert payload["artifact_edge_count"] == 8
    assert payload["artifact_path"] == str(
        artifact_root / "issuer-run" / "market_graph_store.sqlite"
    )
    assert captured["issuer_commodity_tsv_paths"] == [commodity_path]
    assert captured["issuer_geography_tsv_paths"] == [geography_path]
    assert captured["issuer_supply_chain_tsv_paths"] == [supply_chain_path]
    assert captured["output_root"] == output_root
    assert captured["artifact_output_root"] == artifact_root
    assert captured["build_artifact"] is True
    assert captured["include_starter_graph"] is False
    assert captured["extra_node_tsv_paths"] == [extra_node_path]
    assert captured["extra_edge_tsv_paths"] == [extra_edge_path]
    assert captured["namespace"] == "custom-issuer-exposure"


def test_cli_registry_import_bdya_impact_gap_backlog_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    input_path = tmp_path / "bdya_gap.jsonl"
    input_path.write_text("", encoding="utf-8")
    output_root = tmp_path / "imports"
    captured: dict[str, object] = {}

    def _fake_import_bdya_gap_backlog(
        *,
        input_paths,
        output_root,
        run_id,
    ) -> BdyaGapBacklogImportResult:
        captured["input_paths"] = input_paths
        captured["output_root"] = output_root
        captured["run_id"] = run_id
        output_dir = output_root / run_id
        return BdyaGapBacklogImportResult(
            run_id=run_id,
            output_dir=output_dir,
            story_misses_path=output_dir / "story_misses.jsonl",
            unresolved_entities_path=output_dir / "unresolved_entities.jsonl",
            relationship_proposals_path=output_dir / "relationship_proposals.jsonl",
            summary_path=output_dir / "summary.json",
            input_paths=(input_path,),
            story_count=4,
            market_relevant_story_count=3,
            unresolved_entity_count=2,
            relationship_proposal_count=1,
            invalid_line_count=0,
            issue_counts={"relationship_proposals_present": 1},
        )

    monkeypatch.setattr(
        "ades.cli.api_import_bdya_gap_backlog",
        _fake_import_bdya_gap_backlog,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "import-bdya-impact-gap-backlog",
            "--input",
            str(input_path),
            "--output-root",
            str(output_root),
            "--run-id",
            "bdya-import",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["story_count"] == 4
    assert payload["relationship_proposals_path"] == str(
        output_root / "bdya-import" / "relationship_proposals.jsonl"
    )
    assert captured["input_paths"] == [input_path]
    assert captured["output_root"] == output_root
    assert captured["run_id"] == "bdya-import"


def test_cli_registry_validate_market_graph_source_lanes_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    edge_path = tmp_path / "edges.tsv"
    edge_path.write_text("", encoding="utf-8")
    captured: dict[str, object] = {}

    def _fake_validate_market_graph_source_lanes(
        *,
        edge_tsv_paths,
        node_tsv_paths,
        sample_limit,
    ) -> ImpactSourceLaneValidationResult:
        captured["edge_tsv_paths"] = edge_tsv_paths
        captured["node_tsv_paths"] = node_tsv_paths
        captured["sample_limit"] = sample_limit
        return ImpactSourceLaneValidationResult(
            edge_tsv_paths=(edge_path,),
            node_tsv_paths=(),
            row_count=2,
            invalid_row_count=1,
            relation_counts={"law_affects_sector": 1},
            node_type_counts={},
            source_tier_counts={"government": 1},
            source_warning_counts={"missing_source_url": 1},
            relation_warning_counts={"unknown_relation_schema": 1},
            node_warning_counts={},
            warning_samples=(
                {
                    "line": 2,
                    "path": str(edge_path),
                    "relation": "unknown_relation",
                    "scope": "relation",
                    "source_ref": "ades:test",
                    "target_ref": "LSE:AAL",
                    "warning": "unknown_relation_schema",
                },
            ),
        )

    monkeypatch.setattr(
        "ades.cli.api_validate_market_graph_source_lanes",
        _fake_validate_market_graph_source_lanes,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "validate-market-graph-source-lanes",
            "--edge-tsv-path",
            str(edge_path),
            "--sample-limit",
            "7",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["row_count"] == 2
    assert payload["relation_warning_counts"] == {"unknown_relation_schema": 1}
    assert captured["edge_tsv_paths"] == [edge_path]
    assert captured["node_tsv_paths"] == ()
    assert captured["sample_limit"] == 7


def test_cli_registry_promote_reviewed_relationship_proposals_calls_public_api(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    input_path = tmp_path / "relationship_proposals.jsonl"
    input_path.write_text("", encoding="utf-8")
    output_root = tmp_path / "promoted"
    captured: dict[str, object] = {}

    def _fake_promote_reviewed_relationship_proposals(
        *,
        input_paths,
        output_root,
        run_id,
    ) -> ProposalPromotionResult:
        captured["input_paths"] = input_paths
        captured["output_root"] = output_root
        captured["run_id"] = run_id
        output_dir = output_root / run_id
        return ProposalPromotionResult(
            run_id=run_id,
            output_dir=output_dir,
            accepted_edges_path=output_dir / "accepted_edges.tsv",
            rejected_proposals_path=output_dir / "rejected_proposals.jsonl",
            needs_review_path=output_dir / "needs_review.jsonl",
            summary_path=output_dir / "summary.json",
            input_paths=(input_path,),
            accepted_edge_count=3,
            rejected_proposal_count=1,
            needs_review_count=2,
            invalid_line_count=0,
            relation_counts={"law_affects_sector": 2},
            rejection_counts={"missing_source_url": 1},
            needs_review_counts={"review_state:observed": 2},
        )

    monkeypatch.setattr(
        "ades.cli.api_promote_reviewed_relationship_proposals",
        _fake_promote_reviewed_relationship_proposals,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "promote-reviewed-relationship-proposals",
            "--input",
            str(input_path),
            "--output-root",
            str(output_root),
            "--run-id",
            "promote-run",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["accepted_edge_count"] == 3
    assert payload["accepted_edges_path"] == str(output_root / "promote-run" / "accepted_edges.tsv")
    assert captured["input_paths"] == [input_path]
    assert captured["output_root"] == output_root
    assert captured["run_id"] == "promote-run"


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
