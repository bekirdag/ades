from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import FastAPI

from ades.api import (
    evaluate_impact_news_analysis_golden_set as api_evaluate_impact_news_analysis_golden_set,
)
from ades.impact.news_analysis_evaluation import (
    ImpactNewsAnalysisGoldenCase,
    evaluate_impact_news_analysis_golden_set,
    load_impact_news_analysis_golden_cases,
)


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> Path:
    path.write_text(
        "".join(f"{json.dumps(record)}\n" for record in records),
        encoding="utf-8",
    )
    return path


def test_impact_news_analysis_golden_evaluator_passes_with_allowed_proxy(
    tmp_path: Path,
) -> None:
    golden_path = _write_jsonl(
        tmp_path / "ades_impact_golden.jsonl",
        [
            {
                "schema": "ades.impact_path_golden.v1",
                "case_id": "person-company-link",
                "input": {
                    "title": "Executive change hits Example Corp",
                    "summary": "Jane Doe left Example Corp.",
                },
                "expected": {
                    "terminal_candidates": [{"entity_ref": "finance-us:equity:EXM"}],
                    "accepted_broad_proxies": [{"assetRef": "finance-us:index:SPY"}],
                    "source_entities": [{"entity_ref": "wikidata:Q123"}],
                    "event_types": ["executive_transition"],
                },
                "review": {"needs_review": False, "status": "reviewed"},
            }
        ],
    )

    cases = load_impact_news_analysis_golden_cases(golden_path)
    assert cases[0].text == "Executive change hits Example Corp\n\nJane Doe left Example Corp."

    def analyzer(case: ImpactNewsAnalysisGoldenCase) -> dict[str, object]:
        assert case.case_id == "person-company-link"
        return {
            "terminal_impact_candidates": [
                {"entity_ref": "finance-us:equity:EXM"},
                {"entity_ref": "finance-us:index:SPY"},
            ],
            "source_entities": [{"entity_ref": "wikidata:Q123"}],
            "event_signals": [{"event_type": "executive_transition"}],
            "warnings": [],
            "timing_ms": 3,
        }

    report = evaluate_impact_news_analysis_golden_set(
        golden_set_path=golden_path,
        analyzer=analyzer,
    )

    assert report.passed
    assert report.case_count == 1
    assert report.terminal_candidate_recall == 1.0
    assert report.terminal_candidate_precision == 1.0
    assert report.source_entity_recall == 1.0
    assert report.event_type_recall == 1.0
    assert report.p95_latency_ms == 3
    assert report.cases[0].accepted_broad_proxy_refs == ["finance-us:index:SPY"]


def test_impact_news_analysis_golden_evaluator_fails_missing_forbidden_and_warnings(
    tmp_path: Path,
) -> None:
    golden_path = _write_jsonl(
        tmp_path / "ades_impact_golden.jsonl",
        [
            {
                "schema": "ades.impact_path_golden.v1",
                "case_id": "uk-mining-law",
                "input": {"text": "A UK mining law affects listed miners."},
                "expected": {
                    "terminal_candidates": [{"asset_ref": "finance-uk:equity:RIO"}],
                    "forbidden_terminal_candidates": [{"entity_ref": "finance-us:equity:TSLA"}],
                    "source_entities": [{"entity_ref": "wikidata:Q145"}],
                    "event_types": ["sector_policy_change"],
                },
                "review": {"needs_review": False, "status": "reviewed"},
            }
        ],
    )

    def analyzer(_: ImpactNewsAnalysisGoldenCase) -> dict[str, object]:
        return {
            "terminal_impact_candidates": [
                {"entity_ref": "finance-us:equity:TSLA"},
                {"entity_ref": "finance-us:equity:MSFT"},
            ],
            "source_entities": [],
            "event_signals": [],
            "warnings": ["market_graph_artifact_missing"],
        }

    report = evaluate_impact_news_analysis_golden_set(
        golden_set_path=golden_path,
        analyzer=analyzer,
    )

    assert not report.passed
    assert report.failed_case_count == 1
    assert report.terminal_candidate_recall == 0.0
    assert report.terminal_candidate_precision == 0.0
    assert report.source_entity_recall == 0.0
    assert report.event_type_recall == 0.0
    assert report.forbidden_candidate_hit_count == 1
    assert report.unexpected_candidate_count == 2
    assert report.warning_count == 1
    case = report.cases[0]
    assert case.missing_terminal_refs == ["finance-uk:equity:RIO"]
    assert case.forbidden_terminal_hit_refs == ["finance-us:equity:TSLA"]
    assert case.missing_source_refs == ["wikidata:Q145"]
    assert case.missing_event_types == ["sector_policy_change"]


def test_api_impact_news_analysis_golden_evaluator_uses_embedded_news_endpoint(
    tmp_path: Path,
    monkeypatch,
) -> None:
    golden_path = _write_jsonl(
        tmp_path / "ades_impact_golden.jsonl",
        [
            {
                "schema": "ades.impact_path_golden.v1",
                "case_id": "endpoint-shaped",
                "input": {"text": "Example Corp earnings affected its shares."},
                "expected": {
                    "terminal_candidates": [{"entity_ref": "finance-us:equity:EXM"}],
                    "source_entities": [{"entity_ref": "wikidata:Q123"}],
                    "event_types": ["earnings"],
                },
                "review": {"needs_review": False, "status": "reviewed"},
            }
        ],
    )
    artifact_path = tmp_path / "market_graph_store.sqlite"
    storage_root = tmp_path / "storage"
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "0")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", "before.sqlite")
    captured: dict[str, object] = {}

    def _fake_create_service_app(*, storage_root=None):
        captured["storage_root"] = storage_root
        captured["news_enabled"] = os.environ.get("ADES_NEWS_ANALYZE_ENABLED")
        captured["artifact_path"] = os.environ.get("ADES_IMPACT_EXPANSION_ARTIFACT_PATH")
        app = FastAPI()

        @app.post("/v0/news/analyze")
        def _news_analyze(payload: dict[str, object]) -> dict[str, object]:
            captured["request"] = payload
            return {
                "terminal_impact_candidates": [{"entity_ref": "finance-us:equity:EXM"}],
                "source_entities": [{"entity_ref": "wikidata:Q123"}],
                "event_signals": [{"event_type": "earnings"}],
                "warnings": [],
                "timing_ms": 2,
            }

        return app

    monkeypatch.setattr("ades.api.create_service_app", _fake_create_service_app)

    report = api_evaluate_impact_news_analysis_golden_set(
        golden_set_path=golden_path,
        artifact_path=artifact_path,
        storage_root=storage_root,
        packs=["general-en"],
        max_depth=3,
        max_terminal_candidates=9,
    )

    assert report.passed
    assert captured["storage_root"] == storage_root
    assert captured["news_enabled"] == "1"
    assert captured["artifact_path"] == str(artifact_path)
    request = captured["request"]
    assert isinstance(request, dict)
    assert request["packs"] == ["general-en"]
    assert request["options"]["impact_max_depth"] == 3
    assert request["options"]["max_terminal_candidates"] == 9
    assert os.environ["ADES_NEWS_ANALYZE_ENABLED"] == "0"
    assert os.environ["ADES_IMPACT_EXPANSION_ARTIFACT_PATH"] == "before.sqlite"
