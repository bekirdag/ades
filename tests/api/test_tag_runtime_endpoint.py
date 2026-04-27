from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.models import GraphSupport, RelatedEntityMatch, TagResponse
from ades.packs.registry import PackRegistry
from ades.pipeline.hybrid import ProposalSpan
from ades.service.app import create_app
from ades.storage.paths import build_storage_layout, ensure_storage_layout
from tests.pack_registry_helpers import create_pack_source


def _install_endpoint_pack(storage_root: Path) -> str:
    layout = ensure_storage_layout(build_storage_layout(storage_root))
    pack_dir = create_pack_source(
        layout.packs_dir,
        pack_id="hybrid-en",
        domain="general",
        labels=("organization",),
        aliases=(("Entity Alpha Holdings", "organization"),),
        rules=(("entity_alpha_holdings", "organization", r"Entity Alpha Holdings"),),
    )
    PackRegistry(storage_root).sync_pack_from_disk(pack_dir.name)
    return pack_dir.name


def test_tag_endpoint_exposes_metrics_and_debug_options(tmp_path: Path) -> None:
    pack_id = _install_endpoint_pack(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "Entity Alpha Holdings moved.",
            "pack": pack_id,
            "content_type": "text/plain",
            "options": {"debug": True},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == 2
    assert payload["pack_version"] == "0.1.0"
    assert payload["metrics"]["entity_count"] == 1
    assert payload["debug"]["discarded_span_count"] >= 1
    assert payload["total_time_ms"] == payload["timing_ms"]
    assert set(payload["timing_breakdown_ms"]) == {
        "normalization_ms",
        "segmentation_ms",
        "lookup_rule_extraction_ms",
        "overlap_resolution_ms",
        "final_metric_assembly_ms",
    }


def test_tag_endpoint_can_enable_hybrid_linking(tmp_path: Path, monkeypatch) -> None:
    pack_id = _install_endpoint_pack(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path))

    def _proposal_spans(text: str, **_: object) -> list[ProposalSpan]:
        start = text.index("Entity Alpha")
        end = start + len("Entity Alpha")
        return [
            ProposalSpan(
                start=start,
                end=end,
                text=text[start:end],
                label="organization",
                confidence=0.84,
                model_name="patched-proposer",
                model_version="test",
            )
        ]

    monkeypatch.setattr("ades.pipeline.tagger.get_proposal_spans", _proposal_spans)

    response = client.post(
        "/v0/tag",
        json={
            "text": "Entity Alpha moved.",
            "pack": pack_id,
            "content_type": "text/plain",
            "options": {"hybrid": True},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["entities"][0]["text"] == "Entity Alpha"
    assert payload["entities"][0]["provenance"]["match_kind"] == "proposal"


def test_tag_endpoint_rejects_invalid_boolean_options(tmp_path: Path) -> None:
    pack_id = _install_endpoint_pack(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "Entity Alpha Holdings moved.",
            "pack": pack_id,
            "content_type": "text/plain",
            "options": {"debug": "sometimes"},
        },
    )

    assert response.status_code == 400
    assert "Invalid boolean option for debug." in response.json()["detail"]


def test_tag_endpoint_can_request_related_entity_enrichment(
    tmp_path: Path, monkeypatch
) -> None:
    pack_id = _install_endpoint_pack(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_enrichment(
        response,
        *,
        settings,
        include_related_entities: bool,
        include_graph_support: bool = False,
        refine_links: bool = False,
        refinement_depth: str = "light",
        domain_hint: str | None = None,
        country_hint: str | None = None,
    ):
        assert include_related_entities is True
        assert include_graph_support is True
        assert refine_links is True
        assert refinement_depth == "deep"
        assert domain_hint is None
        assert country_hint is None
        return response.model_copy(
            update={
                "related_entities": [
                    RelatedEntityMatch(
                        entity_id="wikidata:Q999",
                        canonical_text="Entity Zeta",
                        score=0.88,
                        provider="qdrant.qid_graph",
                        packs=[pack_id],
                        seed_entity_ids=["wikidata:Q123"],
                        shared_seed_count=1,
                    )
                ],
                "graph_support": GraphSupport(
                    requested=True,
                    applied=True,
                    provider="qdrant.qid_graph",
                    collection_alias="ades-qids-current",
                    requested_related_entities=True,
                    requested_refinement=True,
                    refinement_depth="deep",
                    seed_entity_ids=["wikidata:Q123"],
                    coherence_score=0.81,
                    related_entity_count=1,
                    refined_entity_count=1,
                    boosted_entity_ids=["wikidata:Q123"],
                ),
                "refinement_applied": True,
                "refinement_strategy": "qid_graph_coherence_v1",
                "refinement_score": 0.81,
            }
        )

    monkeypatch.setattr("ades.api.enrich_tag_response_with_related_entities", _fake_enrichment)

    response = client.post(
        "/v0/tag",
        json={
            "text": "Entity Alpha Holdings moved.",
            "pack": pack_id,
            "content_type": "text/plain",
            "options": {
                "include_related_entities": True,
                "include_graph_support": True,
                "refine_links": True,
                "refinement_depth": "deep",
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["graph_support"]["applied"] is True
    assert payload["refinement_applied"] is True
    assert payload["refinement_strategy"] == "qid_graph_coherence_v1"
    assert payload["related_entities"][0]["entity_id"] == "wikidata:Q999"


def test_tag_endpoint_rejects_invalid_related_entity_boolean_option(tmp_path: Path) -> None:
    pack_id = _install_endpoint_pack(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "Entity Alpha Holdings moved.",
            "pack": pack_id,
            "content_type": "text/plain",
            "options": {"include_related_entities": "sometimes"},
        },
    )

    assert response.status_code == 400
    assert "Invalid boolean option for include_related_entities." in response.json()["detail"]


def test_tag_endpoint_rejects_invalid_refinement_depth_option(tmp_path: Path) -> None:
    pack_id = _install_endpoint_pack(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "Entity Alpha Holdings moved.",
            "pack": pack_id,
            "content_type": "text/plain",
            "options": {"refinement_depth": "aggressive"},
        },
    )

    assert response.status_code == 400
    assert "Invalid refinement_depth option." in response.json()["detail"]


def test_tag_endpoint_passes_domain_routing_options(tmp_path: Path, monkeypatch) -> None:
    pack_id = _install_endpoint_pack(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path))
    captured: dict[str, object] = {}

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        domain_hint: str | None = None,
        retrieval_profile: str | None = None,
        **_: object,
    ):
        captured["text"] = text
        captured["pack"] = pack
        captured["content_type"] = content_type
        captured["domain_hint"] = domain_hint
        captured["retrieval_profile"] = retrieval_profile
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[],
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    response = client.post(
        "/v0/tag",
        json={
            "text": "Entity Alpha Holdings moved.",
            "pack": pack_id,
            "content_type": "text/plain",
            "domain_hint": "finance",
            "retrieval_profile": "finance_politics",
            "options": {
                "domain_hint": "politics",
                "retrieval_profile": "politics",
            },
        },
    )

    assert response.status_code == 200
    assert captured["domain_hint"] == "finance"
    assert captured["retrieval_profile"] == "finance_politics"
