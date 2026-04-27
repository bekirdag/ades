from pathlib import Path

from ades.config import Settings
from ades.service.models import GraphSupport, RelatedEntityMatch
from ades.storage.backend import MetadataBackend, RuntimeTarget
from ades.vector.evaluation import (
    SavedVectorAuditReleaseThresholds,
    VectorGoldenCase,
    VectorGoldenSeedEntity,
    VectorGoldenSet,
    VectorQualityReport,
    VectorReleaseThresholds,
    VectorSeedAuditCase,
    audit_vector_seed_neighbors,
    evaluate_saved_vector_seed_audit_release,
    evaluate_vector_golden_set,
    evaluate_vector_release_thresholds,
    load_vector_quality_report,
    write_vector_quality_report,
)
from ades.vector.qdrant import QdrantNearestPoint, QdrantVectorSearchError


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
                    VectorGoldenSeedEntity(
                        entity_id="wikidata:Q7",
                        canonical_text="the Second",
                        label="person",
                        score=0.24,
                    ),
                ),
                expected_related_entity_ids=("wikidata:Q9",),
                expected_boosted_entity_ids=("wikidata:Q1",),
                expected_suppressed_entity_ids=("wikidata:Q7",),
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
        if seed_ids == ["wikidata:Q1", "wikidata:Q2", "wikidata:Q7"]:
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
                        refined_entity_count=2,
                        boosted_entity_ids=["wikidata:Q1"],
                        suppressed_entity_ids=["wikidata:Q7"],
                        suppressed_entity_count=1,
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
    assert report.suppression_alignment_rate == 1.0
    assert report.refinement_alignment_rate == 1.0
    assert report.easy_case_pass_rate == 1.0
    assert report.graph_support_rate == 1.0
    assert report.refinement_trigger_rate == 0.5
    assert report.fallback_rate == 0.0
    assert [case.name for case in report.cases] == ["related-support", "easy-exact"]
    assert report.cases[0].matched_related_entity_ids == ["wikidata:Q9"]
    assert report.cases[0].actual_boosted_entity_ids == ["wikidata:Q1"]
    assert report.cases[0].actual_suppressed_entity_ids == ["wikidata:Q7"]


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
        suppression_alignment_rate=0.9,
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
            min_suppression_alignment_rate=0.85,
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
            min_suppression_alignment_rate=0.95,
            min_refinement_alignment_rate=0.95,
            min_easy_case_pass_rate=1.0,
            max_fallback_rate=0.01,
            max_p95_latency_ms=10,
        ),
    )

    assert passed.passed is True
    assert failed.passed is False
    assert any(reason.startswith("related_precision_at_k_below_threshold:") for reason in failed.reasons)
    assert any(
        reason.startswith("suppression_alignment_rate_below_threshold:")
        for reason in failed.reasons
    )
    assert any(reason.startswith("fallback_rate_above_threshold:") for reason in failed.reasons)


def test_audit_vector_seed_neighbors_routes_aliases_and_records_failures(
    monkeypatch,
) -> None:
    settings = Settings(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
        vector_search_enabled=True,
        vector_search_url="http://qdrant.local:6333",
        vector_search_collection_alias="ades-qids-current",
        vector_search_related_limit=2,
    )
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    class _FakeClient:
        def __init__(self, base_url: str, *, api_key=None, timeout_seconds: float = 30.0) -> None:
            assert base_url == "http://qdrant.local:6333"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def query_similar_by_id(
            self,
            collection_name: str,
            *,
            point_id: str,
            limit: int,
            filter_payload=None,
        ):
            calls.append((collection_name, point_id, filter_payload))
            assert limit == 3
            if collection_name == "ades-qids-finance-current":
                raise QdrantVectorSearchError("missing", status_code=404)
            if collection_name == "ades-qids-business-current":
                return [
                    QdrantNearestPoint(
                        point_id=point_id,
                        score=0.99,
                        payload={"entity_id": point_id, "canonical_text": "OpenAI"},
                    ),
                    QdrantNearestPoint(
                        point_id="wikidata:Q9",
                        score=0.91,
                        payload={
                            "entity_id": "wikidata:Q9",
                            "canonical_text": "Sam Altman",
                            "entity_type": "person",
                            "packs": ["general-en", "business-vector-en"],
                        },
                    ),
                ]
            return [
                QdrantNearestPoint(
                    point_id="wikidata:Q2283",
                    score=0.73,
                    payload={
                        "entity_id": "wikidata:Q2283",
                        "canonical_text": "Microsoft",
                        "entity_type": "organization",
                        "packs": ["general-en"],
                    },
                )
            ]

    monkeypatch.setattr("ades.vector.evaluation.QdrantVectorSearchClient", _FakeClient)

    report = audit_vector_seed_neighbors(
        (
            VectorSeedAuditCase(
                name="uk-business-hint",
                pack_id="general-en",
                domain_hint="business",
                country_hint="uk",
                seed_entities=(
                    VectorGoldenSeedEntity(
                        entity_id="wikidata:Q1",
                        canonical_text="OpenAI",
                        label="organization",
                    ),
                ),
            ),
        ),
        settings=settings,
        collection_alias_overrides={"ades-qids-current": "ades-qids-shared-general-plus-domain-v1"},
    )

    assert report.provider == "qdrant.qid_graph"
    assert report.case_count == 1
    assert report.top_k == 2
    assert report.collection_aliases_seen == [
        "ades-qids-business-current",
        "ades-qids-shared-general-plus-domain-v1",
    ]
    assert calls == [
        (
            "ades-qids-business-current",
            "wikidata:Q1",
            {"packs": ["general-en", "business-vector-en", "finance-uk-en"]},
        ),
        (
            "ades-qids-shared-general-plus-domain-v1",
            "wikidata:Q1",
            {"packs": ["general-en", "business-vector-en", "finance-uk-en"]},
        ),
    ]

    case = report.cases[0]
    assert case.name == "uk-business-hint"
    assert len(case.alias_results) == 2
    assert case.warnings == []

    business_alias = case.alias_results[0]
    assert business_alias.collection_alias == "ades-qids-business-current"
    assert business_alias.filter_payload == {
        "packs": ["general-en", "business-vector-en", "finance-uk-en"]
    }
    assert business_alias.seed_results[0].neighbors == [
        report.cases[0].alias_results[0].seed_results[0].neighbors[0].__class__(
            entity_id="wikidata:Q9",
            canonical_text="Sam Altman",
            entity_type="person",
            source_name=None,
            score=0.91,
            packs=["general-en", "business-vector-en"],
        )
    ]

    general_alias = case.alias_results[1]
    assert general_alias.collection_alias == "ades-qids-shared-general-plus-domain-v1"
    assert general_alias.filter_payload == {
        "packs": ["general-en", "business-vector-en", "finance-uk-en"]
    }
    assert general_alias.seed_results[0].neighbors == [
        report.cases[0].alias_results[0].seed_results[0].neighbors[0].__class__(
            entity_id="wikidata:Q2283",
            canonical_text="Microsoft",
            entity_type="organization",
            source_name=None,
            score=0.73,
            packs=["general-en"],
        )
    ]


def test_evaluate_saved_vector_seed_audit_release_passes_on_healthy_summary() -> None:
    decision = evaluate_saved_vector_seed_audit_release(
        {
            "baseline_metrics": {
                "case_count": 12,
                "neighbor_seed_hit_rate": 0.82,
                "hinted_related_entity_hit_rate": 0.25,
                "hinted_related_case_count": 5,
                "hinted_warning_case_rate": 0.0,
                "p95_hinted_timing_ms": 96,
            },
            "seed_shape_analysis": {
                "seed_query_label_counts": [
                    {"value": "organization", "count": 30},
                    {"value": "person", "count": 20},
                    {"value": "location", "count": 10},
                ]
            },
        },
        thresholds=SavedVectorAuditReleaseThresholds(),
    )

    assert decision.passed is True
    assert decision.reasons == []


def test_evaluate_saved_vector_seed_audit_release_reports_threshold_failures() -> None:
    decision = evaluate_saved_vector_seed_audit_release(
        {
            "baseline_metrics": {
                "case_count": 5,
                "neighbor_seed_hit_rate": 0.42,
                "hinted_related_entity_hit_rate": 0.0,
                "hinted_related_case_count": 0,
                "hinted_warning_case_rate": 0.25,
                "p95_hinted_timing_ms": 205,
            },
            "seed_shape_analysis": {
                "seed_query_label_counts": [
                    {"value": "location", "count": 12},
                    {"value": "organization", "count": 6},
                ]
            },
        },
        thresholds=SavedVectorAuditReleaseThresholds(),
    )

    assert decision.passed is False
    assert any(reason.startswith("case_count_below_threshold:") for reason in decision.reasons)
    assert any(
        reason.startswith("neighbor_seed_hit_rate_below_threshold:")
        for reason in decision.reasons
    )
    assert any(
        reason.startswith("related_entity_hit_rate_below_threshold:")
        for reason in decision.reasons
    )
    assert any(
        reason.startswith("related_case_count_below_threshold:")
        for reason in decision.reasons
    )
    assert any(
        reason.startswith("warning_case_rate_above_threshold:")
        for reason in decision.reasons
    )
    assert any(
        reason.startswith("p95_hinted_timing_ms_above_threshold:")
        for reason in decision.reasons
    )
    assert any(
        reason.startswith("location_seed_query_share_above_threshold:")
        for reason in decision.reasons
    )


def test_evaluate_saved_vector_seed_audit_release_reports_missing_timing() -> None:
    decision = evaluate_saved_vector_seed_audit_release(
        {
            "baseline_metrics": {
                "case_count": 12,
                "neighbor_seed_hit_rate": 0.82,
                "hinted_related_entity_hit_rate": 0.25,
                "hinted_related_case_count": 5,
                "hinted_warning_case_rate": 0.0,
                "p95_hinted_timing_ms": None,
            },
            "seed_shape_analysis": {
                "seed_query_label_counts": [
                    {"value": "organization", "count": 30},
                    {"value": "location", "count": 10},
                ]
            },
        },
        thresholds=SavedVectorAuditReleaseThresholds(),
    )

    assert decision.passed is False
    assert "p95_hinted_timing_ms_missing" in decision.reasons
