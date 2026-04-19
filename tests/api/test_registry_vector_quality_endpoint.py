from pathlib import Path
import time

from fastapi.testclient import TestClient

from ades.service.app import create_app
from ades.service.models import GraphSupport, RelatedEntityMatch
from ades.vector.evaluation import (
    VectorGoldenCase,
    VectorGoldenSeedEntity,
    VectorGoldenSet,
    write_vector_golden_set,
)


def test_registry_vector_quality_and_release_threshold_endpoints(
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
                    expected_refinement_applied=False,
                ),
            ),
        ),
    )
    report_path = tmp_path / "reports" / "vector-report.json"

    def _fake_enrichment(
        response,
        *,
        settings,
        include_related_entities: bool,
        include_graph_support: bool,
        refine_links: bool,
        refinement_depth: str,
    ):
        time.sleep(0.002)
        seed_ids = [entity.link.entity_id for entity in response.entities if entity.link is not None]
        return response.model_copy(
            update={
                "related_entities": [
                    RelatedEntityMatch(
                        entity_id="wikidata:Q9",
                        canonical_text="Sam Altman",
                        score=0.91,
                        provider="qdrant.qid_graph",
                        packs=["general-en"],
                        seed_entity_ids=seed_ids,
                        shared_seed_count=1,
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
                    coherence_score=0.88,
                    related_entity_count=1,
                ),
                "refinement_applied": False,
                "refinement_score": 0.88,
            }
        )

    monkeypatch.setattr(
        "ades.vector.evaluation.enrich_tag_response_with_related_entities",
        _fake_enrichment,
    )

    client = TestClient(create_app(storage_root=tmp_path / "storage"))

    evaluate = client.post(
        "/v0/registry/evaluate-vector-quality",
        json={
            "pack_id": "general-en",
            "golden_set_path": str(golden_set_path),
            "report_path": str(report_path),
            "refinement_depth": "deep",
        },
    )

    assert evaluate.status_code == 200
    payload = evaluate.json()
    assert payload["pack_id"] == "general-en"
    assert payload["refinement_depth"] == "deep"
    assert payload["related_precision_at_k"] == 1.0
    assert payload["report_path"] == str(report_path.resolve())

    thresholds = client.post(
        "/v0/registry/evaluate-vector-release-thresholds",
        json={
            "report_path": str(report_path),
            "max_p95_latency_ms": 0,
        },
    )

    assert thresholds.status_code == 200
    threshold_payload = thresholds.json()
    assert threshold_payload["passed"] is False
    assert any(
        reason.startswith("p95_latency_above_threshold:")
        for reason in threshold_payload["reasons"]
    )
