import json
from pathlib import Path

from ades.extraction_quality import (
    ExtractionQualityReport,
    ExtractionCaseResult,
    GoldenDocument,
    GoldenEntity,
    GoldenSet,
    benchmark_runtime_pack,
    compare_quality_reports,
    evaluate_golden_set,
    get_pack_health_summary,
    load_golden_set,
    load_quality_report,
    pack_health_db_path,
    record_tag_observation,
    write_runtime_benchmark_report,
    write_quality_report,
    write_golden_set,
)
from ades.packs.installer import PackInstaller
from ades.service.models import TagMetrics, TagResponse


def test_write_and_load_golden_set_round_trip(tmp_path: Path) -> None:
    golden_set = GoldenSet(
        pack_id="finance-en",
        profile="smoke",
        description="synthetic finance golden set",
        documents=(
            GoldenDocument(
                name="alpha",
                text="TICKA traded on EXCHX.",
                expected_entities=(GoldenEntity(text="TICKA", label="ticker"),),
            ),
        ),
    )

    path = write_golden_set(tmp_path / "finance-en.json", golden_set)
    loaded = load_golden_set(path)

    assert loaded == golden_set


def test_evaluate_golden_set_collects_metrics(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    golden_set = GoldenSet(
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
    )

    report = evaluate_golden_set(golden_set, storage_root=tmp_path)

    assert report.pack_id == "finance-en"
    assert report.document_count == 1
    assert report.recall == 1.0
    assert report.precision == 1.0
    assert report.per_label_recall == {
        "currency_amount": 1.0,
        "exchange": 1.0,
        "ticker": 1.0,
    }
    assert report.per_lane_counts["deterministic_alias"] >= 2
    assert report.per_lane_counts["deterministic_rule"] >= 1


def test_load_golden_set_accepts_document_id_alias(tmp_path: Path) -> None:
    path = tmp_path / "general-en.json"
    path.write_text(
        json.dumps(
            {
                "pack_id": "general-en",
                "documents": [
                    {
                        "document_id": "general-smoke",
                        "text": "Org Alpha met Person Alpha.",
                        "expected_entities": [
                            {"text": "Org Alpha", "label": "organization"},
                            {"text": "Person Alpha", "label": "person"},
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    loaded = load_golden_set(path)

    assert loaded.documents[0].name == "general-smoke"


def test_compare_quality_reports_returns_label_deltas() -> None:
    baseline = ExtractionQualityReport(
        pack_id="finance-en",
        profile="default",
        hybrid=False,
        document_count=1,
        expected_entity_count=4,
        actual_entity_count=4,
        matched_entity_count=4,
        missing_entity_count=0,
        unexpected_entity_count=0,
        recall=1.0,
        precision=1.0,
        entities_per_100_tokens=8.0,
        overlap_drop_count=0,
        chunk_count=1,
        low_density_warning_count=0,
        p95_latency_ms=10,
        per_label_expected={"ticker": 2},
        per_label_actual={"ticker": 2},
        per_label_matched={"ticker": 2},
        per_label_recall={"ticker": 1.0},
    )
    candidate = ExtractionQualityReport(
        pack_id="finance-en",
        profile="default",
        hybrid=True,
        document_count=1,
        expected_entity_count=4,
        actual_entity_count=5,
        matched_entity_count=4,
        missing_entity_count=0,
        unexpected_entity_count=1,
        recall=1.0,
        precision=0.8,
        entities_per_100_tokens=9.0,
        overlap_drop_count=1,
        chunk_count=1,
        low_density_warning_count=0,
        p95_latency_ms=18,
        per_label_expected={"ticker": 2},
        per_label_actual={"ticker": 3},
        per_label_matched={"ticker": 2},
        per_label_recall={"ticker": 1.0},
    )

    delta = compare_quality_reports(baseline, candidate)

    assert delta.recall_delta == 0.0
    assert delta.precision_delta == -0.2
    assert delta.p95_latency_delta_ms == 8
    assert delta.per_label_deltas[0].label == "ticker"


def test_write_and_load_quality_report_round_trip(tmp_path: Path) -> None:
    report = ExtractionQualityReport(
        pack_id="finance-en",
        profile="default",
        hybrid=False,
        document_count=1,
        expected_entity_count=2,
        actual_entity_count=2,
        matched_entity_count=2,
        missing_entity_count=0,
        unexpected_entity_count=0,
        recall=1.0,
        precision=1.0,
        entities_per_100_tokens=12.5,
        overlap_drop_count=1,
        chunk_count=1,
        low_density_warning_count=0,
        p95_latency_ms=14,
        per_label_expected={"ticker": 1, "exchange": 1},
        per_label_actual={"ticker": 1, "exchange": 1},
        per_label_matched={"ticker": 1, "exchange": 1},
        per_label_recall={"ticker": 1.0, "exchange": 1.0},
        per_lane_counts={"deterministic_alias": 2},
        warnings=["low_entity_density:finance-en:1.0000<2.0000"],
        cases=[
            ExtractionCaseResult(
                name="alpha",
                expected_entities=[GoldenEntity(text="ticka", label="ticker")],
                actual_entities=[GoldenEntity(text="ticka", label="ticker")],
                matched_entities=[GoldenEntity(text="ticka", label="ticker")],
                missing_entities=[],
                unexpected_entities=[],
                warnings=[],
                token_count=8,
                entity_count=1,
                entities_per_100_tokens=12.5,
                overlap_drop_count=1,
                chunk_count=1,
                timing_ms=14,
                passed=True,
            )
        ],
    )

    path = write_quality_report(tmp_path / "finance-en.deterministic.json", report)
    loaded = load_quality_report(path)

    assert loaded == report


def test_record_tag_observation_aggregates_pack_health(tmp_path: Path) -> None:
    response_one = TagResponse(
        version="0.1.0",
        pack="finance-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[],
        topics=[],
        metrics=TagMetrics(
            input_char_count=24,
            normalized_char_count=24,
            token_count=4,
            segment_count=1,
            chunk_count=1,
            entity_count=2,
            entities_per_100_tokens=50.0,
            overlap_drop_count=0,
            per_label_counts={"exchange": 1, "ticker": 1},
            per_lane_counts={"deterministic_alias": 2},
        ),
        warnings=[],
        timing_ms=18,
    )
    response_two = TagResponse(
        version="0.1.0",
        pack="finance-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[],
        topics=[],
        metrics=TagMetrics(
            input_char_count=40,
            normalized_char_count=40,
            token_count=10,
            segment_count=1,
            chunk_count=1,
            entity_count=0,
            entities_per_100_tokens=0.0,
            overlap_drop_count=1,
            per_label_counts={},
            per_lane_counts={"proposal_linked": 1},
        ),
        warnings=["low_entity_density:finance-en:0.0000<1.0000"],
        timing_ms=42,
    )
    skipped = response_two.model_copy(
        update={
            "pack_version": None,
            "warnings": ["pack_not_installed:finance-en"],
        }
    )

    record_tag_observation(tmp_path, response_one)
    record_tag_observation(tmp_path, response_two)
    record_tag_observation(tmp_path, skipped)

    health_path = pack_health_db_path(tmp_path)
    assert health_path.exists()

    summary = get_pack_health_summary("finance-en", storage_root=tmp_path, limit=10)

    assert summary["pack_id"] == "finance-en"
    assert summary["observation_count"] == 2
    assert summary["latest_pack_version"] == "0.2.0"
    assert summary["average_entity_count"] == 1.0
    assert summary["average_entities_per_100_tokens"] == 25.0
    assert summary["zero_entity_rate"] == 0.5
    assert summary["warning_rate"] == 0.5
    assert summary["low_density_warning_rate"] == 0.5
    assert summary["p95_timing_ms"] == 41
    assert summary["per_label_counts"] == {"exchange": 1, "ticker": 1}
    assert summary["per_lane_counts"] == {
        "deterministic_alias": 2,
        "proposal_linked": 1,
    }
    assert summary["warning_counts"] == {
        "low_entity_density:finance-en:0.0000<1.0000": 1
    }


def test_benchmark_runtime_pack_writes_consolidated_report(tmp_path: Path) -> None:
    storage_root = tmp_path / "storage"
    PackInstaller(storage_root).install("finance-en")
    golden_set = GoldenSet(
        pack_id="finance-en",
        profile="benchmark",
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
    )

    report = benchmark_runtime_pack(
        golden_set,
        storage_root=storage_root,
        warm_runs=2,
        lookup_limit=10,
    )

    assert report.pack_id == "finance-en"
    assert report.profile == "benchmark"
    assert report.matcher_entry_count >= 0
    assert report.matcher_state_count >= 0
    assert report.matcher_cold_load_ms >= 0
    assert report.warm_tagging.sample_count == 2
    assert report.operator_lookup.sample_count == 3
    assert report.disk_usage.runtime_pack_bytes > 0
    assert report.disk_usage.matcher_total_bytes >= 0
    assert report.quality_report.recall == 1.0
    assert report.quality_report.p50_latency_ms >= 0
    if report.disk_usage.matcher_total_bytes == 0:
        assert "matcher_not_available:finance-en" in report.warnings

    report_path = write_runtime_benchmark_report(
        tmp_path / "reports" / "finance-runtime-benchmark.json",
        report,
    )
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["pack_id"] == "finance-en"
    assert payload["warm_tagging"]["sample_count"] == 2
    assert payload["operator_lookup"]["sample_count"] == 3
