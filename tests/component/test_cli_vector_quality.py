import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.vector.evaluation import (
    VectorGoldenCase,
    VectorGoldenSeedEntity,
    VectorGoldenSet,
    write_vector_golden_set,
)


def test_cli_can_evaluate_vector_quality_and_release_thresholds(
    tmp_path: Path, monkeypatch
) -> None:
    golden_set_path = write_vector_golden_set(
        tmp_path / "golden" / "general-en.json",
        VectorGoldenSet(
            pack_id="general-en",
            cases=(
                VectorGoldenCase(
                    name="related-support",
                    seed_entities=(
                        VectorGoldenSeedEntity(
                            entity_id="wikidata:Q1",
                            canonical_text="OpenAI",
                            label="organization",
                            score=0.45,
                        ),
                    ),
                    expected_related_entity_ids=("wikidata:Q9",),
                ),
            ),
        ),
    )
    report_path = tmp_path / "reports" / "vector-report.json"
    runner = CliRunner()

    def _fake_evaluate_vector_quality(
        pack_id: str,
        *,
        golden_set_path: Path | None = None,
        profile: str = "default",
        refinement_depth: str = "light",
        top_k: int | None = None,
        write_report: bool = True,
        report_path: Path | None = None,
        storage_root: Path | None = None,
    ):
        return {
            "report_path": str(report_path.resolve()) if report_path is not None else None,
            "pack_id": pack_id,
            "profile": profile,
            "refinement_depth": refinement_depth,
            "collection_alias": "ades-qids-current",
            "provider": "qdrant.qid_graph",
            "case_count": 1,
            "top_k": top_k or 3,
            "related_precision_at_k": 1.0,
            "related_recall_at_k": 1.0,
            "related_mrr": 1.0,
            "refinement_alignment_rate": 1.0,
            "easy_case_pass_rate": 1.0,
            "graph_support_rate": 1.0,
            "refinement_trigger_rate": 0.0,
            "fallback_rate": 0.0,
            "p50_latency_ms": 4,
            "p95_latency_ms": 5,
            "warnings": [],
            "cases": [],
        }

    def _fake_release_thresholds(
        *,
        report_path: Path,
        min_related_precision_at_k: float | None = None,
        min_related_recall_at_k: float | None = None,
        min_related_mrr: float | None = None,
        min_suppression_alignment_rate: float | None = None,
        min_refinement_alignment_rate: float | None = None,
        min_easy_case_pass_rate: float | None = None,
        max_fallback_rate: float | None = None,
        max_p95_latency_ms: int | None = None,
    ):
        return {
            "report_path": str(report_path.resolve()),
            "thresholds": {
                "min_related_precision_at_k": min_related_precision_at_k or 0.75,
                "min_related_recall_at_k": min_related_recall_at_k or 0.6,
                "min_related_mrr": min_related_mrr or 0.7,
                "min_suppression_alignment_rate": min_suppression_alignment_rate or 0.75,
                "min_refinement_alignment_rate": min_refinement_alignment_rate or 0.75,
                "min_easy_case_pass_rate": min_easy_case_pass_rate or 1.0,
                "max_fallback_rate": max_fallback_rate or 0.1,
                "max_p95_latency_ms": max_p95_latency_ms or 150,
            },
            "passed": True,
            "reasons": [],
        }

    class _Response:
        def __init__(self, payload: dict[str, object]) -> None:
            self.payload = payload
            self.passed = bool(payload.get("passed", True))

        def model_dump(self, *, mode: str = "json") -> dict[str, object]:
            return dict(self.payload)

    monkeypatch.setattr(
        "ades.cli.api_evaluate_vector_quality",
        lambda *args, **kwargs: _Response(_fake_evaluate_vector_quality(*args, **kwargs)),
    )
    monkeypatch.setattr(
        "ades.cli.api_evaluate_vector_release_thresholds",
        lambda **kwargs: _Response(_fake_release_thresholds(**kwargs)),
    )

    evaluate = runner.invoke(
        app,
        [
            "registry",
            "evaluate-vector-quality",
            "general-en",
            "--golden-set-path",
            str(golden_set_path),
            "--report-path",
            str(report_path),
            "--refinement-depth",
            "deep",
        ],
    )

    assert evaluate.exit_code == 0
    evaluate_payload = json.loads(evaluate.stdout)
    assert evaluate_payload["pack_id"] == "general-en"
    assert evaluate_payload["refinement_depth"] == "deep"

    thresholds = runner.invoke(
        app,
        [
            "registry",
            "evaluate-vector-release-thresholds",
            "--report-path",
            str(report_path),
            "--min-suppression-alignment-rate",
            "0.9",
        ],
    )

    assert thresholds.exit_code == 0
    threshold_payload = json.loads(thresholds.stdout)
    assert threshold_payload["passed"] is True
    assert threshold_payload["thresholds"]["min_related_precision_at_k"] == 0.75
    assert threshold_payload["thresholds"]["min_suppression_alignment_rate"] == 0.9
