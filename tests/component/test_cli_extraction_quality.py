import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.extraction_quality import GoldenDocument, GoldenEntity, GoldenSet, write_golden_set
from ades.packs.installer import PackInstaller
from tests.pack_registry_helpers import create_pack_source


def test_cli_can_evaluate_extraction_quality_and_diff_packs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    storage_root = tmp_path / "storage"
    monkeypatch.setenv("ADES_STORAGE_ROOT", str(storage_root))
    PackInstaller(storage_root).install("finance-en")
    golden_set_path = write_golden_set(
        tmp_path / "golden" / "finance-en.json",
        GoldenSet(
            pack_id="finance-en",
            documents=(
                GoldenDocument(
                    name="finance-smoke",
                    text="TICKA traded on EXCHX after USD 12.5 guidance.",
                    expected_entities=(
                        GoldenEntity(text="TICKA", label="ticker"),
                        GoldenEntity(text="EXCHX", label="exchange"),
                        GoldenEntity(text="USD 12.5", label="currency_amount"),
                    ),
                ),
            ),
        ),
    )
    report_path = tmp_path / "reports" / "finance-report.json"
    runner = CliRunner()

    evaluate = runner.invoke(
        app,
        [
            "registry",
            "evaluate-extraction-quality",
            "finance-en",
            "--golden-set-path",
            str(golden_set_path),
            "--report-path",
            str(report_path),
        ],
    )

    assert evaluate.exit_code == 0
    evaluate_payload = json.loads(evaluate.stdout)
    assert evaluate_payload["pack_id"] == "finance-en"
    assert evaluate_payload["report_path"] == str(report_path.resolve())
    assert report_path.exists()

    old_pack_dir = create_pack_source(
        tmp_path / "old-packs",
        pack_id="finance-en",
        domain="finance",
        labels=("ticker",),
        aliases=(("TICKA", "ticker"),),
    )
    new_pack_dir = create_pack_source(
        tmp_path / "new-packs",
        pack_id="finance-en",
        domain="finance",
        labels=("ticker", "exchange"),
        aliases=(("TICKA", "ticker"), ("EXCHX", "exchange")),
    )

    diff = runner.invoke(
        app,
        [
            "registry",
            "diff-pack",
            str(old_pack_dir),
            str(new_pack_dir),
        ],
    )

    assert diff.exit_code == 0
    diff_payload = json.loads(diff.stdout)
    assert diff_payload["severity"] == "minor"
    assert diff_payload["requires_retag"] is True
    assert "exchange" in diff_payload["added_labels"]


def test_cli_release_thresholds_exit_nonzero_when_thresholds_fail(
    tmp_path: Path,
    monkeypatch,
) -> None:
    storage_root = tmp_path / "storage"
    monkeypatch.setenv("ADES_STORAGE_ROOT", str(storage_root))
    PackInstaller(storage_root).install("finance-en")
    golden_set_path = write_golden_set(
        tmp_path / "golden" / "finance-en.json",
        GoldenSet(
            pack_id="finance-en",
            documents=(
                GoldenDocument(
                    name="finance-smoke",
                    text="TICKA traded on EXCHX after USD 12.5 guidance.",
                    expected_entities=(
                        GoldenEntity(text="TICKA", label="ticker"),
                        GoldenEntity(text="EXCHX", label="exchange"),
                        GoldenEntity(text="USD 12.5", label="currency_amount"),
                    ),
                ),
            ),
        ),
    )
    report_path = tmp_path / "reports" / "finance-report.json"
    runner = CliRunner()

    runner.invoke(
        app,
        [
            "registry",
            "evaluate-extraction-quality",
            "finance-en",
            "--golden-set-path",
            str(golden_set_path),
            "--report-path",
            str(report_path),
        ],
        catch_exceptions=False,
    )

    result = runner.invoke(
        app,
        [
            "registry",
            "evaluate-release-thresholds",
            "--report-path",
            str(report_path),
            "--mode",
            "deterministic",
            "--max-p95-latency-ms",
            "0",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["passed"] is False
    assert any(reason.startswith("p95_latency_above_threshold:") for reason in payload["reasons"])


def test_cli_can_render_pack_health_as_table_and_json(
    tmp_path: Path,
    monkeypatch,
) -> None:
    storage_root = tmp_path / "storage"
    monkeypatch.setenv("ADES_STORAGE_ROOT", str(storage_root))
    PackInstaller(storage_root).install("finance-en")
    runner = CliRunner()

    tag = runner.invoke(
        app,
        [
            "tag",
            "--pack",
            "finance-en",
            "TICKA traded on EXCHX after USD 12.5 guidance.",
        ],
        catch_exceptions=False,
    )

    assert tag.exit_code == 0

    health = runner.invoke(
        app,
        ["packs", "health", "finance-en"],
        catch_exceptions=False,
    )

    assert health.exit_code == 0
    assert "PACK" in health.stdout
    assert "finance-en" in health.stdout
    assert "OBSERVATIONS" in health.stdout
    assert "Lane" in health.stdout
    assert "deterministic_alias" in health.stdout

    health_json = runner.invoke(
        app,
        ["packs", "health", "finance-en", "--json"],
        catch_exceptions=False,
    )

    assert health_json.exit_code == 0
    payload = json.loads(health_json.stdout)
    assert payload["pack_id"] == "finance-en"
    assert payload["observation_count"] == 1
    assert payload["per_lane_counts"]["deterministic_alias"] >= 2


def test_cli_can_benchmark_runtime_and_write_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    storage_root = tmp_path / "storage"
    monkeypatch.setenv("ADES_STORAGE_ROOT", str(storage_root))
    PackInstaller(storage_root).install("finance-en")
    golden_set_path = write_golden_set(
        tmp_path / "golden" / "finance-en.json",
        GoldenSet(
            pack_id="finance-en",
            documents=(
                GoldenDocument(
                    name="finance-smoke",
                    text="TICKA traded on EXCHX after USD 12.5 guidance.",
                    expected_entities=(
                        GoldenEntity(text="TICKA", label="ticker"),
                        GoldenEntity(text="EXCHX", label="exchange"),
                        GoldenEntity(text="USD 12.5", label="currency_amount"),
                    ),
                ),
            ),
        ),
    )
    report_path = tmp_path / "reports" / "finance-runtime-benchmark.json"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "registry",
            "benchmark-runtime",
            "finance-en",
            "--golden-set-path",
            str(golden_set_path),
            "--warm-runs",
            "2",
            "--report-path",
            str(report_path),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["pack_id"] == "finance-en"
    assert payload["report_path"] == str(report_path.resolve())
    assert payload["warm_tagging"]["sample_count"] == 2
    assert payload["operator_lookup"]["sample_count"] == 3
    assert payload["disk_usage"]["matcher_total_bytes"] >= 0
    assert payload["quality_report"]["recall"] == 1.0
    if payload["disk_usage"]["matcher_total_bytes"] == 0:
        assert "matcher_not_available:finance-en" in payload["warnings"]
    assert report_path.exists()


def test_cli_can_benchmark_matcher_backends_and_write_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    storage_root = tmp_path / "storage"
    monkeypatch.setenv("ADES_STORAGE_ROOT", str(storage_root))
    PackInstaller(storage_root).install("finance-en")
    golden_set_path = write_golden_set(
        tmp_path / "golden" / "finance-en.json",
        GoldenSet(
            pack_id="finance-en",
            documents=(
                GoldenDocument(
                    name="finance-smoke",
                    text="TICKA traded on EXCHX after USD 12.5 guidance.",
                    expected_entities=(
                        GoldenEntity(text="TICKA", label="ticker"),
                        GoldenEntity(text="EXCHX", label="exchange"),
                        GoldenEntity(text="USD 12.5", label="currency_amount"),
                    ),
                ),
            ),
        ),
    )
    report_path = tmp_path / "reports" / "finance-matcher-backends.json"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "registry",
            "benchmark-matcher-backends",
            "finance-en",
            "--golden-set-path",
            str(golden_set_path),
            "--alias-limit",
            "25",
            "--scan-limit",
            "100",
            "--min-alias-score",
            "0.0",
            "--exact-tier-min-token-count",
            "1",
            "--query-runs",
            "2",
            "--report-path",
            str(report_path),
            "--output-root",
            str(tmp_path / "matcher-spike"),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["pack_id"] == "finance-en"
    assert payload["report_path"] == str(report_path.resolve())
    assert payload["query_runs"] == 2
    assert {item["backend"] for item in payload["candidates"]} == {
        "current_json_aho",
        "token_trie_v1",
    }
    assert all(item["artifact_bytes"] > 0 for item in payload["candidates"])
    assert payload["recommended_backend"] in {"current_json_aho", "token_trie_v1"}
    assert report_path.exists()
