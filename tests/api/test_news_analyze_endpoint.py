from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.registry import PackRegistry
from ades.service.app import create_app
from ades.service.models import (
    EntityLink,
    EntityMatch,
    EntityProvenance,
    ImpactCandidate,
    ImpactExpansionResult,
    ImpactSourceEntity,
    TagResponse,
    TopicMatch,
)
from ades.storage.paths import build_storage_layout, ensure_storage_layout
from tests.pack_registry_helpers import create_pack_source


def _install_news_pack(storage_root: Path) -> str:
    layout = ensure_storage_layout(build_storage_layout(storage_root))
    pack_dir = create_pack_source(
        layout.packs_dir,
        pack_id="news-contract-en",
        domain="general",
        labels=("location",),
        aliases=(("Strait of Hormuz", "location"),),
    )
    PackRegistry(storage_root).sync_pack_from_disk(pack_dir.name)
    return pack_dir.name


def _install_named_pack(storage_root: Path, pack_id: str, domain: str = "general") -> str:
    layout = ensure_storage_layout(build_storage_layout(storage_root))
    pack_dir = create_pack_source(
        layout.packs_dir,
        pack_id=pack_id,
        domain=domain,
        labels=("location", "organization", "ticker"),
        aliases=((pack_id, "organization"),),
    )
    PackRegistry(storage_root).sync_pack_from_disk(pack_dir.name)
    return pack_dir.name


def test_news_analyze_endpoint_is_feature_flagged(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/news/analyze",
        json={"text": "Oil shipping risk rose near the Strait of Hormuz."},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "ADES_NEWS_ANALYZE_DISABLED"


def test_news_analyze_endpoint_returns_normalized_contract(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_news_pack(tmp_path)
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text="Iran",
                    label="country",
                    start=text.index("Iran"),
                    end=text.index("Iran") + len("Iran"),
                    confidence=0.91,
                    relevance=0.93,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="general",
                    ),
                    link=EntityLink(
                        entity_id="country:ir",
                        canonical_text="Iran",
                        provider="ades",
                    ),
                ),
                EntityMatch(
                    text="Strait of Hormuz",
                    label="location",
                    start=text.index("Strait"),
                    end=text.index("Strait") + len("Strait of Hormuz"),
                    confidence=0.92,
                    relevance=0.94,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="politics",
                    ),
                    link=EntityLink(
                        entity_id="entity_hormuz",
                        canonical_text="Strait of Hormuz",
                        provider="ades",
                    ),
                ),
                EntityMatch(
                    text="$50",
                    label="currency_amount",
                    start=text.index("$50"),
                    end=text.index("$50") + len("$50"),
                    confidence=0.99,
                    relevance=0.99,
                    provenance=EntityProvenance(
                        match_kind="rule",
                        match_path="rules.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="finance",
                    ),
                ),
            ],
            topics=[TopicMatch(label="politics", score=0.88, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "entity_hormuz" in list(entity_refs)
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-05-12",
            artifact_hash="sha256:test",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="entity_hormuz",
                    name="Strait of Hormuz",
                    entity_type="location",
                    library_id=pack_id,
                    is_graph_seed=True,
                    seed_degree=1,
                    is_tradable=False,
                )
            ],
            candidates=[
                ImpactCandidate(
                    entity_ref="entity_crude_oil",
                    name="Crude oil",
                    entity_type="commodity",
                    evidence_level="shallow",
                    confidence=0.92,
                    source_entity_refs=["entity_hormuz"],
                    relationship_paths=[
                        {
                            "path_depth": 1,
                            "edges": [
                                {
                                    "source_ref": "entity_hormuz",
                                    "target_ref": "entity_crude_oil",
                                    "relation": "chokepoint_affects_energy",
                                    "evidence_level": "direct",
                                    "confidence": 0.92,
                                    "direction_hint": "contextual",
                                    "source_name": "test",
                                    "source_url": "https://example.com",
                                    "source_snapshot": "test",
                                }
                            ],
                        }
                    ],
                )
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Energy shipping risk rises",
            "text": "Iran said oil shipping lanes near the Strait of Hormuz were disrupted while $50 was cited.",
            "hints": {"country": "ir", "topics": ["finance"]},
            "source": {"publisher": "Example", "source_country": "IR"},
            "packs": [pack_id],
            "options": {
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "max_passive_entities": 32,
                "max_terminal_candidates": 8,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "ades.news_analysis.v1"
    assert payload["country_scope"]["entity_ref"] == "country:ir"
    assert payload["country_scope"]["name"] == "Iran"
    assert payload["topic_scope"]["primary"] == "politics"
    assert payload["topic_scope"]["finance_relevant"] is True
    assert payload["topic_scope"]["politics_relevant"] is True
    assert payload["artifact_versions"]["impact_artifact_hash"] == "sha256:test"
    assert [signal["event_type"] for signal in payload["event_signals"]] == [
        "shipping_chokepoint_disruption"
    ]
    assert [candidate["entity_ref"] for candidate in payload["terminal_impact_candidates"]] == [
        "entity_crude_oil"
    ]
    assert payload["terminal_impact_candidates"][0]["compatible_event_types"] == [
        "shipping_chokepoint_disruption"
    ]
    passive_by_ref = {entity["entity_ref"]: entity for entity in payload["passive_entities"]}
    assert passive_by_ref["country:ir"]["role"] == "country_scope"
    assert passive_by_ref["entity_hormuz"]["display_eligible"] is True
    assert any(
        entity["quality"] == "hidden_artifact" and entity["display_eligible"] is False
        for entity in payload["passive_entities"]
    )
    unresolved_by_ref = {
        entity["entity_ref"]: entity for entity in payload["unresolved_entities"]
    }
    assert unresolved_by_ref["country:ir"]["missing_reason"] == (
        "country_scope_without_terminal_candidate"
    )
    assert unresolved_by_ref["country:ir"]["event_types"] == [
        "shipping_chokepoint_disruption"
    ]
    assert unresolved_by_ref["country:ir"]["has_terminal_candidate"] is False
    assert "entity_hormuz" not in unresolved_by_ref
    assert payload["tag_responses"] == []


def test_news_analyze_country_hint_uses_impact_source_display_name(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_news_pack(tmp_path)
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text="Iran",
                    label="location",
                    start=text.index("Iran"),
                    end=text.index("Iran") + len("Iran"),
                    confidence=0.91,
                    relevance=0.93,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="general",
                    ),
                    link=EntityLink(
                        entity_id="wikidata:Q794",
                        canonical_text="Iran",
                        provider="ades",
                    ),
                )
            ],
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "wikidata:Q794" in list(entity_refs)
        return ImpactExpansionResult(
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="wikidata:Q794",
                    name="Iran",
                    same_as_refs=["country:ir"],
                    entity_type="country",
                    is_graph_seed=True,
                    seed_degree=1,
                    is_tradable=False,
                )
            ],
            candidates=[],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Iran shipping risk rises",
            "text": "Iran warned oil shipping routes could be disrupted.",
            "country_hint": "IR",
            "packs": [pack_id],
            "options": {
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["country_scope"]["entity_ref"] == "country:ir"
    assert payload["country_scope"]["name"] == "Iran"
    passive_by_ref = {entity["entity_ref"]: entity for entity in payload["passive_entities"]}
    assert passive_by_ref["country:ir"]["name"] == "Iran"


def test_news_analyze_passive_classifier_hides_artifacts_and_forces_text_country(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_news_pack(tmp_path)
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        def _entity(
            value: str,
            label: str,
            *,
            entity_id: str | None = None,
            confidence: float = 0.91,
        ) -> EntityMatch:
            return EntityMatch(
                text=value,
                label=label,
                start=text.index(value),
                end=text.index(value) + len(value),
                confidence=confidence,
                relevance=confidence,
                provenance=EntityProvenance(
                    match_kind="alias",
                    match_path="aliases.json",
                    match_source="pack",
                    source_pack=pack or pack_id,
                    source_domain="general",
                ),
                link=(
                    EntityLink(
                        entity_id=entity_id,
                        canonical_text=value,
                        provider="ades",
                    )
                    if entity_id
                    else None
                ),
            )

        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                _entity("Reuters", "organization", entity_id="entity_reuters"),
                _entity("10%", "percentage"),
                _entity("https://example.com", "url"),
                _entity("<a>link</a>", "html"),
                _entity(
                    "Federal Reserve",
                    "organization",
                    entity_id="entity_federal_reserve",
                ),
            ],
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Canada inflation pressure rises",
            "text": (
                "Canada inflation rose 10% after Reuters cited "
                "https://example.com and <a>link</a> while the Federal Reserve responded."
            ),
            "packs": [pack_id],
            "options": {
                "include_relationship_paths": False,
                "max_passive_entities": 32,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["country_scope"]["entity_ref"] == "country:ca"
    assert payload["country_scope"]["source"] == "text_country_mention"

    passive_by_name = {entity["name"]: entity for entity in payload["passive_entities"]}
    passive_by_ref = {entity["entity_ref"]: entity for entity in payload["passive_entities"]}
    assert passive_by_ref["country:ca"]["role"] == "country_scope"
    assert passive_by_ref["country:ca"]["display_eligible"] is True
    assert passive_by_name["Federal Reserve"]["role"] == "policy_body"
    assert passive_by_name["Federal Reserve"]["display_eligible"] is True

    assert passive_by_name["Reuters"]["role"] == "source_outlet"
    assert passive_by_name["Reuters"]["quality"] == "hidden_artifact"
    assert passive_by_name["Reuters"]["display_eligible"] is False
    assert "source_outlet_artifact" in passive_by_name["Reuters"]["quality_reasons"]

    hidden_reasons_by_name = {
        entity["name"]: entity["quality_reasons"]
        for entity in payload["passive_entities"]
        if not entity["display_eligible"]
    }
    assert "percentage" in hidden_reasons_by_name["10%"]
    assert "url_or_link" in hidden_reasons_by_name["https://example.com"]
    assert "html_fragment" in hidden_reasons_by_name["<a>link</a>"]


def test_news_analyze_source_outlet_subject_remains_display_eligible(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_news_pack(tmp_path)
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text="Reuters",
                    label="organization",
                    start=text.index("Reuters"),
                    end=text.index("Reuters") + len("Reuters"),
                    confidence=0.91,
                    relevance=0.91,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="business",
                    ),
                    link=EntityLink(
                        entity_id="entity_reuters",
                        canonical_text="Reuters",
                        provider="ades",
                    ),
                )
            ],
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Reuters names new chief executive",
            "text": "Reuters names new chief executive in the United States.",
            "hints": {"country": "us"},
            "packs": [pack_id],
            "options": {"include_relationship_paths": False},
        },
    )

    assert response.status_code == 200
    passive_by_name = {
        entity["name"]: entity for entity in response.json()["passive_entities"]
    }
    assert passive_by_name["Reuters"]["role"] == "source_outlet"
    assert passive_by_name["Reuters"]["quality"] == "strong"
    assert passive_by_name["Reuters"]["display_eligible"] is True


def test_news_analyze_plans_base_and_country_finance_packs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for pack_id, domain in (
        ("general-en", "general"),
        ("business-vector-en", "business"),
        ("economics-vector-en", "economics"),
        ("politics-vector-en", "politics"),
        ("finance-en", "finance"),
        ("finance-tr-en", "finance"),
        ("finance-us-en", "finance"),
        ("finance-fr-en", "finance"),
        ("finance-ca-en", "finance"),
        ("finance-cn-en", "finance"),
        ("finance-jp-en", "finance"),
        ("finance-de-en", "finance"),
        ("finance-uk-en", "finance"),
        ("finance-in-en", "finance"),
        ("finance-br-en", "finance"),
    ):
        _install_named_pack(tmp_path, pack_id, domain=domain)

    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))
    called_packs: list[str] = []

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        called_packs.append(pack or "unknown")
        return TagResponse(
            version="0.1.0",
            pack=pack or "unknown",
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[],
            topics=[TopicMatch(label="finance", score=0.8, evidence_count=1)]
            if pack == "finance-en"
            else [],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    examples = {
        "TR": "finance-tr-en",
        "US": "finance-us-en",
        "FR": "finance-fr-en",
        "CA": "finance-ca-en",
        "CN": "finance-cn-en",
        "JP": "finance-jp-en",
        "DE": "finance-de-en",
        "UK": "finance-uk-en",
        "IN": "finance-in-en",
        "BR": "finance-br-en",
    }
    for country_code, expected_pack in examples.items():
        called_packs.clear()
        response = client.post(
            "/v0/news/analyze",
            json={
                "text": "Central bank policy affected local markets.",
                "source": {"source_country": country_code},
                "options": {
                    "include_relationship_paths": False,
                    "max_country_finance_packs": 1,
                    "max_packs": 24,
                },
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert expected_pack in called_packs
        assert expected_pack in payload["packs_used"]
        assert any(
            decision["pack_id"] == expected_pack
            and decision["selected"] is True
            and decision["reason"] == "source_country"
            for decision in payload["pack_decisions"]
        )


def test_news_analyze_uses_text_country_mentions_for_country_pack(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for pack_id, domain in (
        ("general-en", "general"),
        ("business-vector-en", "business"),
        ("economics-vector-en", "economics"),
        ("politics-vector-en", "politics"),
        ("finance-en", "finance"),
        ("finance-br-en", "finance"),
    ):
        _install_named_pack(tmp_path, pack_id, domain=domain)

    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        return TagResponse(
            version="0.1.0",
            pack=pack or "unknown",
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
        "/v0/news/analyze",
        json={
            "text": "Brazil central bank policy moved bank shares and currency expectations.",
            "options": {
                "include_relationship_paths": False,
                "max_country_finance_packs": 1,
                "max_packs": 24,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert "finance-br-en" in payload["packs_used"]
    assert any(
        decision["pack_id"] == "finance-br-en"
        and decision["reason"] == "text_country_mention"
        for decision in payload["pack_decisions"]
    )


def test_news_analyze_default_pack_budget_allows_full_country_pack_budget(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for pack_id, domain in (
        ("general-en", "general"),
        ("business-vector-en", "business"),
        ("economics-vector-en", "economics"),
        ("politics-vector-en", "politics"),
        ("finance-en", "finance"),
        ("finance-us-en", "finance"),
        ("finance-tr-en", "finance"),
        ("finance-fr-en", "finance"),
        ("finance-ca-en", "finance"),
        ("finance-cn-en", "finance"),
        ("finance-jp-en", "finance"),
    ):
        _install_named_pack(tmp_path, pack_id, domain=domain)

    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        return TagResponse(
            version="0.1.0",
            pack=pack or "unknown",
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
        "/v0/news/analyze",
        json={
            "text": "Turkey, France, Canada, China, and Japan reported market policy updates.",
            "source": {"source_country": "US"},
            "options": {
                "include_relationship_paths": False,
                "max_country_finance_packs": 6,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    planned_country_packs = {
        decision["pack_id"]
        for decision in payload["pack_decisions"]
        if decision.get("country_code") and decision["selected"] is True
    }
    assert planned_country_packs == {
        "finance-us-en",
        "finance-tr-en",
        "finance-fr-en",
        "finance-ca-en",
        "finance-cn-en",
        "finance-jp-en",
    }
    assert planned_country_packs.issubset(set(payload["packs_used"]))
