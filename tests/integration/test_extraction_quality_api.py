from pathlib import Path

from ades import (
    benchmark_matcher_backends,
    benchmark_runtime,
    compare_extraction_quality_reports,
    diff_pack_versions,
    evaluate_extraction_quality,
    evaluate_extraction_release_thresholds,
    get_pack_health,
    tag,
)
from ades.extraction_quality import (
    GoldenDocument,
    GoldenEntity,
    GoldenSet,
    load_golden_set,
    load_quality_report,
    write_golden_set,
    write_quality_report,
)
from ades.packs.installer import PackInstaller
from tests.pack_registry_helpers import create_pack_source


def test_public_api_can_evaluate_and_compare_extraction_quality(tmp_path: Path) -> None:
    storage_root = tmp_path / "storage"
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
    baseline_report_path = tmp_path / "reports" / "finance-baseline.json"

    baseline = evaluate_extraction_quality(
        "finance-en",
        storage_root=storage_root,
        golden_set_path=golden_set_path,
        report_path=baseline_report_path,
    )

    assert baseline.pack_id == "finance-en"
    assert baseline.recall == 1.0
    assert baseline.precision == 1.0
    assert baseline.report_path == str(baseline_report_path.resolve())

    cloned_report = load_quality_report(baseline_report_path)
    candidate_report_path = tmp_path / "reports" / "finance-candidate.json"
    write_quality_report(candidate_report_path, cloned_report)

    delta = compare_extraction_quality_reports(
        baseline_report_path=baseline_report_path,
        candidate_report_path=candidate_report_path,
    )

    assert delta.pack_id == "finance-en"
    assert delta.recall_delta == 0.0
    assert delta.precision_delta == 0.0
    assert delta.baseline_report_path == str(baseline_report_path.resolve())
    assert delta.candidate_report_path == str(candidate_report_path.resolve())


def test_public_api_can_diff_packs_and_fail_release_thresholds(tmp_path: Path) -> None:
    storage_root = tmp_path / "storage"
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
    evaluate_extraction_quality(
        "finance-en",
        storage_root=storage_root,
        golden_set_path=golden_set_path,
        report_path=report_path,
    )

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

    diff = diff_pack_versions(old_pack_dir, new_pack_dir)

    assert diff.pack_id == "finance-en"
    assert diff.severity == "minor"
    assert diff.requires_retag is True
    assert "exchange" in diff.added_labels

    decision = evaluate_extraction_release_thresholds(
        report_path=report_path,
        mode="deterministic",
        max_p95_latency_ms=0,
    )

    assert decision.passed is False
    assert any(reason.startswith("p95_latency_above_threshold:") for reason in decision.reasons)


def test_public_api_exposes_pack_health_and_locked_release_defaults(tmp_path: Path) -> None:
    storage_root = tmp_path / "storage"
    PackInstaller(storage_root).install("finance-en")

    first = tag(
        "TICKA traded on EXCHX after USD 12.5 guidance.",
        pack="finance-en",
        storage_root=storage_root,
    )
    second = tag(
        "TICKA moved again.",
        pack="finance-en",
        storage_root=storage_root,
    )
    health = get_pack_health("finance-en", storage_root=storage_root, limit=10)

    assert first.pack_version is not None
    assert second.pack_version is not None
    assert health.pack_id == "finance-en"
    assert health.observation_count == 2
    assert health.latest_pack_version == first.pack_version
    assert health.average_entity_count >= 1.0
    assert health.per_lane_counts["deterministic_alias"] >= 2

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
    evaluate_extraction_quality(
        "finance-en",
        storage_root=storage_root,
        golden_set_path=golden_set_path,
        report_path=report_path,
    )

    decision = evaluate_extraction_release_thresholds(
        report_path=report_path,
        mode="deterministic",
    )

    assert decision.passed is True
    assert decision.thresholds.min_recall == 0.85
    assert decision.thresholds.min_precision == 0.85
    assert decision.thresholds.max_label_recall_drop == 0.0
    assert decision.thresholds.max_p95_latency_ms == 100


def test_public_api_can_benchmark_runtime(tmp_path: Path) -> None:
    storage_root = tmp_path / "storage"
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

    benchmark = benchmark_runtime(
        "finance-en",
        storage_root=storage_root,
        golden_set_path=golden_set_path,
        warm_runs=2,
        report_path=report_path,
    )

    assert benchmark.pack_id == "finance-en"
    assert benchmark.report_path == str(report_path.resolve())
    assert benchmark.warm_tagging.sample_count == 2
    assert benchmark.operator_lookup.sample_count == 3
    assert benchmark.disk_usage.matcher_total_bytes >= 0
    assert benchmark.quality_report.recall == 1.0
    if benchmark.disk_usage.matcher_total_bytes == 0:
        assert "matcher_not_available:finance-en" in benchmark.warnings


def test_public_api_can_benchmark_matcher_backends(tmp_path: Path) -> None:
    storage_root = tmp_path / "storage"
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

    benchmark = benchmark_matcher_backends(
        "finance-en",
        storage_root=storage_root,
        golden_set_path=golden_set_path,
        alias_limit=25,
        scan_limit=100,
        min_alias_score=0.0,
        exact_tier_min_token_count=1,
        query_runs=2,
        report_path=report_path,
        output_root=tmp_path / "matcher-spike",
    )

    assert benchmark.pack_id == "finance-en"
    assert benchmark.report_path == str(report_path.resolve())
    assert benchmark.query_runs == 2
    assert {candidate.backend for candidate in benchmark.candidates} == {
        "current_json_aho",
        "token_trie_v1",
    }
    assert all(candidate.artifact_bytes > 0 for candidate in benchmark.candidates)
    assert benchmark.recommended_backend in {"current_json_aho", "token_trie_v1"}
