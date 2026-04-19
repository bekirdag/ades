from pathlib import Path

from ades.config import Settings
from ades.service.models import GraphSupport, RelatedEntityMatch
from ades.storage.backend import MetadataBackend, RuntimeTarget
from ades.vector.evaluation import (
    VectorGoldenCase,
    VectorGoldenSeedEntity,
    VectorGoldenSet,
    VectorQualityReport,
    VectorReleaseThresholds,
    evaluate_vector_golden_set,
    evaluate_vector_release_thresholds,
    load_vector_quality_report,
    write_vector_quality_report,
)


def test_evaluate_vector_golden_set_computes_quality_metrics(monkeypatch) -> None:
    golden_set = VectorGoldenSet(
        pack_id="general-en",
        cases=(
            VectorGoldenCase(
                name="related-support",
                case_kind="related",
                seed_entities=(
                    VectorGoldenSeedEntity(
                        entity_id="wikidata:Q1",
                        canonical_text="OpenAI",
                        label="organization",
                        score=0.45,
                    ),
                    VectorGoldenSeedEntity(
                        entity_id="wikidata:Q2",
                        canonical_text="Microsoft",
                        label="organization",
                        score=0.47,
                    ),
                ),
                expected_related_entity_ids=("wikidata:Q9",),
                expected_boosted_entity_ids=("wikidata:Q1",),
                expected_refinement_applied=True,
            ),
            VectorGoldenCase(
                name="easy-exact",
                case_kind="easy_exact",
                seed_entities=(
                    VectorGoldenSeedEntity(
                        entity_id="wikidata:Q3",
                        canonical_text="Anthropic",
                        label="organization",
                        score=0.95,
                    ),
                ),
                expected_refinement_applied=False,
            ),
        ),
    )
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_collection_alias="ades-qids-current",
        vector_search_related_limit=3,
    )

    def _fake_enrichment(
        response,
        *,
        settings,
        include_related_entities: bool,
        include_graph_support: bool,
        refine_links: bool,
        refinement_depth: str,
    ):
        seed_ids = [entity.link.entity_id for entity in response.entities if entity.link is not None]
        if seed_ids == ["wikidata:Q1", "wikidata:Q2"]:
            return response.model_copy(
                update={
                    "related_entities": [
                        RelatedEntityMatch(
                            entity_id="wikidata:Q9",
                            canonical_text="Sam Altman",
                            score=0.92,
                            provider="qdrant.qid_graph",
                            packs=["general-en"],
                            seed_entity_ids=seed_ids,
                            shared_seed_count=2,
                        )
                    ],
                    "graph_support": GraphSupport(
                        requested=True,
                        applied=True,
                        provider="qdrant.qid_graph",
                        collection_alias="ades-qids-current",
                        requested_related_entities=include_related_entities,
                        requested_refinement=refine_links,
                        refinement_depth=refinement_depth,
                        seed_entity_ids=seed_ids,
                        coherence_score=0.84,
                        related_entity_count=1,
                        refined_entity_count=1,
                        boosted_entity_ids=["wikidata:Q1"],
                    ),
                    "refinement_applied": True,
                    "refinement_strategy": "qid_graph_coherence_v1",
                    "refinement_score": 0.84,
                }
            )
        return response.model_copy(
            update={
                "graph_support": GraphSupport(
                    requested=True,
                    applied=True,
                    provider="qdrant.qid_graph",
                    collection_alias="ades-qids-current",
                    requested_related_entities=include_related_entities,
                    requested_refinement=refine_links,
                    refinement_depth=refinement_depth,
                    seed_entity_ids=seed_ids,
                    coherence_score=0.91,
                ),
                "refinement_applied": False,
                "refinement_score": 0.91,
            }
        )

    monkeypatch.setattr(
        "ades.vector.evaluation.enrich_tag_response_with_related_entities",
        _fake_enrichment,
    )

    report = evaluate_vector_golden_set(
        golden_set,
        settings=settings,
        refinement_depth="deep",
        top_k=3,
    )

    assert report.pack_id == "general-en"
    assert report.collection_alias == "ades-qids-current"
    assert report.refinement_depth == "deep"
    assert report.case_count == 2
    assert report.related_precision_at_k == 1.0
    assert report.related_recall_at_k == 1.0
    assert report.related_mrr == 1.0
    assert report.refinement_alignment_rate == 1.0
    assert report.easy_case_pass_rate == 1.0
    assert report.graph_support_rate == 1.0
    assert report.refinement_trigger_rate == 0.5
    assert report.fallback_rate == 0.0
    assert [case.name for case in report.cases] == ["related-support", "easy-exact"]
    assert report.cases[0].matched_related_entity_ids == ["wikidata:Q9"]
    assert report.cases[0].actual_boosted_entity_ids == ["wikidata:Q1"]


def test_vector_quality_report_round_trip_and_thresholds(tmp_path: Path) -> None:
    report = VectorQualityReport(
        pack_id="general-en",
        profile="default",
        refinement_depth="light",
        collection_alias="ades-qids-current",
        provider="qdrant.qid_graph",
        case_count=2,
        top_k=3,
        related_precision_at_k=0.8,
        related_recall_at_k=0.75,
        related_mrr=0.82,
        refinement_alignment_rate=0.9,
        easy_case_pass_rate=1.0,
        graph_support_rate=1.0,
        refinement_trigger_rate=0.5,
        fallback_rate=0.05,
        p50_latency_ms=5,
        p95_latency_ms=22,
    )

    report_path = write_vector_quality_report(tmp_path / "report.json", report)
    loaded = load_vector_quality_report(report_path)
    assert loaded == report

    passed = evaluate_vector_release_thresholds(
        loaded,
        thresholds=VectorReleaseThresholds(
            min_related_precision_at_k=0.75,
            min_related_recall_at_k=0.7,
            min_related_mrr=0.8,
            min_refinement_alignment_rate=0.85,
            min_easy_case_pass_rate=1.0,
            max_fallback_rate=0.1,
            max_p95_latency_ms=50,
        ),
    )
    failed = evaluate_vector_release_thresholds(
        loaded,
        thresholds=VectorReleaseThresholds(
            min_related_precision_at_k=0.9,
            min_related_recall_at_k=0.8,
            min_related_mrr=0.9,
            min_refinement_alignment_rate=0.95,
            min_easy_case_pass_rate=1.0,
            max_fallback_rate=0.01,
            max_p95_latency_ms=10,
        ),
    )

    assert passed.passed is True
    assert failed.passed is False
    assert any(reason.startswith("related_precision_at_k_below_threshold:") for reason in failed.reasons)
    assert any(reason.startswith("fallback_rate_above_threshold:") for reason in failed.reasons)
