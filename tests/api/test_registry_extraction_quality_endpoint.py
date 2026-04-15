from pathlib import Path

from fastapi.testclient import TestClient

from ades.extraction_quality import GoldenDocument, GoldenEntity, GoldenSet, write_golden_set
from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_registry_helpers import create_pack_source


def test_registry_evaluate_extraction_quality_endpoint(tmp_path: Path) -> None:
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
    client = TestClient(create_app(storage_root=storage_root))

    response = client.post(
        "/v0/registry/evaluate-extraction-quality",
        json={
            "pack_id": "finance-en",
            "golden_set_path": str(golden_set_path),
            "report_path": str(report_path),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "finance-en"
    assert payload["recall"] == 1.0
    assert payload["report_path"] == str(report_path.resolve())


def test_registry_compare_and_release_threshold_endpoints(tmp_path: Path) -> None:
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
    candidate_report_path = tmp_path / "reports" / "finance-candidate.json"
    client = TestClient(create_app(storage_root=storage_root))

    baseline = client.post(
        "/v0/registry/evaluate-extraction-quality",
        json={
            "pack_id": "finance-en",
            "golden_set_path": str(golden_set_path),
            "report_path": str(baseline_report_path),
        },
    )
    assert baseline.status_code == 200

    candidate = client.post(
        "/v0/registry/evaluate-extraction-quality",
        json={
            "pack_id": "finance-en",
            "golden_set_path": str(golden_set_path),
            "report_path": str(candidate_report_path),
        },
    )
    assert candidate.status_code == 200

    comparison = client.post(
        "/v0/registry/compare-extraction-quality",
        json={
            "baseline_report_path": str(baseline_report_path),
            "candidate_report_path": str(candidate_report_path),
        },
    )

    assert comparison.status_code == 200
    comparison_payload = comparison.json()
    assert comparison_payload["pack_id"] == "finance-en"
    assert comparison_payload["recall_delta"] == 0.0

    threshold = client.post(
        "/v0/registry/evaluate-release-thresholds",
        json={
            "report_path": str(candidate_report_path),
            "mode": "deterministic",
            "max_p95_latency_ms": 0,
        },
    )

    assert threshold.status_code == 200
    threshold_payload = threshold.json()
    assert threshold_payload["passed"] is False
    assert any(
        reason.startswith("p95_latency_above_threshold:")
        for reason in threshold_payload["reasons"]
    )


def test_registry_diff_pack_endpoint(tmp_path: Path) -> None:
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
    client = TestClient(create_app(storage_root=tmp_path / "storage"))

    response = client.post(
        "/v0/registry/diff-pack",
        json={
            "old_pack_dir": str(old_pack_dir),
            "new_pack_dir": str(new_pack_dir),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["severity"] == "minor"
    assert payload["requires_retag"] is True
    assert "exchange" in payload["added_labels"]


def test_pack_health_endpoint_and_default_release_thresholds(tmp_path: Path) -> None:
    storage_root = tmp_path / "storage"
    PackInstaller(storage_root).install("finance-en")
    client = TestClient(create_app(storage_root=storage_root))

    tag_response = client.post(
        "/v0/tag",
        json={
            "text": "TICKA traded on EXCHX after USD 12.5 guidance.",
            "pack": "finance-en",
            "content_type": "text/plain",
        },
    )

    assert tag_response.status_code == 200

    health = client.get("/v0/packs/finance-en/health")

    assert health.status_code == 200
    health_payload = health.json()
    assert health_payload["pack_id"] == "finance-en"
    assert health_payload["observation_count"] == 1
    assert health_payload["per_lane_counts"]["deterministic_alias"] >= 2

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

    evaluate = client.post(
        "/v0/registry/evaluate-extraction-quality",
        json={
            "pack_id": "finance-en",
            "golden_set_path": str(golden_set_path),
            "report_path": str(report_path),
        },
    )

    assert evaluate.status_code == 200

    thresholds = client.post(
        "/v0/registry/evaluate-release-thresholds",
        json={
            "report_path": str(report_path),
            "mode": "deterministic",
        },
    )

    assert thresholds.status_code == 200
    threshold_payload = thresholds.json()
    assert threshold_payload["passed"] is True
    assert threshold_payload["thresholds"]["min_recall"] == 0.85
    assert threshold_payload["thresholds"]["min_precision"] == 0.85
    assert threshold_payload["thresholds"]["max_label_recall_drop"] == 0.0
    assert threshold_payload["thresholds"]["max_p95_latency_ms"] == 100


def test_registry_benchmark_runtime_endpoint(tmp_path: Path) -> None:
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
    client = TestClient(create_app(storage_root=storage_root))

    response = client.post(
        "/v0/registry/benchmark-runtime",
        json={
            "pack_id": "finance-en",
            "golden_set_path": str(golden_set_path),
            "warm_runs": 2,
            "report_path": str(report_path),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "finance-en"
    assert payload["report_path"] == str(report_path.resolve())
    assert payload["warm_tagging"]["sample_count"] == 2
    assert payload["operator_lookup"]["sample_count"] == 3
    assert payload["disk_usage"]["matcher_total_bytes"] >= 0
    assert payload["quality_report"]["recall"] == 1.0
    if payload["disk_usage"]["matcher_total_bytes"] == 0:
        assert "matcher_not_available:finance-en" in payload["warnings"]


def test_registry_benchmark_matcher_backends_endpoint(tmp_path: Path) -> None:
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
    client = TestClient(create_app(storage_root=storage_root))

    response = client.post(
        "/v0/registry/benchmark-matcher-backends",
        json={
            "pack_id": "finance-en",
            "golden_set_path": str(golden_set_path),
            "alias_limit": 25,
            "scan_limit": 100,
            "min_alias_score": 0.0,
            "exact_tier_min_token_count": 1,
            "query_runs": 2,
            "report_path": str(report_path),
            "output_root": str(tmp_path / "matcher-spike"),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack_id"] == "finance-en"
    assert payload["report_path"] == str(report_path.resolve())
    assert payload["query_runs"] == 2
    assert {item["backend"] for item in payload["candidates"]} == {
        "current_json_aho",
        "token_trie_v1",
    }
    assert all(item["artifact_bytes"] > 0 for item in payload["candidates"])
    assert payload["recommended_backend"] in {"current_json_aho", "token_trie_v1"}
