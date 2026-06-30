from pathlib import Path

from fastapi.testclient import TestClient

from ades.impact.graph_builder import build_market_graph_store
from ades.packs.registry import PackRegistry
from ades.service.app import _terminal_identity_parts, create_app
from ades.service.models import (
    EntityLink,
    EntityMatch,
    EntityProvenance,
    ImpactCandidate,
    ImpactExpansionResult,
    ImpactPassivePath,
    ImpactPathEdge,
    ImpactRelationshipPath,
    ImpactSourceEntity,
    RelatedEntityMatch,
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


def test_news_candidate_path_terminal_identity_supports_legacy_finance_refs() -> None:
    jurisdiction, exchange, ticker, security_ids = _terminal_identity_parts("finance-us:equity:EXM")

    assert jurisdiction == "us"
    assert exchange is None
    assert ticker == "EXM"
    assert security_ids == {
        "ades_ref": "finance-us:equity:EXM",
        "ticker": "EXM",
    }

    jurisdiction, exchange, ticker, security_ids = _terminal_identity_parts(
        "finance-ca-ticker:tsx:key.r"
    )

    assert jurisdiction == "ca"
    assert exchange == "tsx"
    assert ticker == "key.r"
    assert security_ids == {
        "ades_ref": "finance-ca-ticker:tsx:key.r",
        "ticker": "key.r",
        "exchange_ticker": "tsx:key.r",
    }

    jurisdiction, exchange, ticker, security_ids = _terminal_identity_parts("sec-cik:1730168")

    assert jurisdiction is None
    assert exchange is None
    assert ticker is None
    assert security_ids == {
        "ades_ref": "sec-cik:1730168",
        "cik": "1730168",
    }


def test_news_analyze_endpoint_is_feature_flagged(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/news/analyze",
        json={"text": "Oil shipping risk rose near the Strait of Hormuz."},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "ADES_NEWS_ANALYZE_DISABLED"


def test_news_analyze_endpoint_can_enable_hybrid_from_news_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_news_pack(tmp_path)
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_HYBRID_ENABLED", "true")
    monkeypatch.setenv("ADES_HYBRID_ENABLED", "false")
    hybrid_values: list[bool | None] = []

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        hybrid: bool | None = None,
        **_: object,
    ) -> TagResponse:
        hybrid_values.append(hybrid)
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[],
            topics=[],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/news/analyze",
        json={
            "text": "Oil shipping risk rose near the Strait of Hormuz.",
            "packs": [pack_id],
            "options": {
                "include_passive_entities": False,
                "include_terminal_candidates": False,
                "include_impact_paths": False,
            },
        },
    )

    assert response.status_code == 200
    assert hybrid_values == [True]


def test_news_analyze_endpoint_can_limit_news_hybrid_packs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    first_pack_id = _install_news_pack(tmp_path)
    second_pack_id = _install_named_pack(tmp_path, "news-contract-alt-en")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_HYBRID_ENABLED", "true")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_HYBRID_PACK_LIMIT", "1")
    hybrid_values: list[bool | None] = []

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        hybrid: bool | None = None,
        **_: object,
    ) -> TagResponse:
        hybrid_values.append(hybrid)
        return TagResponse(
            version="0.1.0",
            pack=pack or first_pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[],
            topics=[],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/news/analyze",
        json={
            "text": "Oil shipping risk rose near the Strait of Hormuz.",
            "packs": [first_pack_id, second_pack_id],
            "options": {
                "include_passive_entities": False,
                "include_terminal_candidates": False,
                "include_impact_paths": False,
            },
        },
    )

    assert response.status_code == 200
    assert hybrid_values == [True, False]


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
                ),
                ImpactCandidate(
                    entity_ref="ades:impact:rates:policy-rate",
                    name="Policy rate proxy",
                    entity_type="rates_proxy",
                    evidence_level="shallow",
                    confidence=0.81,
                    source_entity_refs=["entity_hormuz"],
                    relationship_paths=[],
                ),
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
    assert payload["artifact_metadata"] == {
        "artifact_id": "sha256:test",
        "artifact_version": "2026-05-12",
        "artifact_hash": "sha256:test",
        "artifact_built_at": None,
        "artifact_deployed_at": None,
        "graph_version": "test-graph",
        "ades_version": payload["version"],
        "source_lane_versions": {
            pack_id: "0.1.0",
            "impact_graph": "test-graph",
            "impact_artifact": "2026-05-12",
        },
    }
    assert [signal["event_type"] for signal in payload["event_signals"]] == [
        "shipping_chokepoint_disruption"
    ]
    assert payload["event_signal"]["event_type"] == "shipping_chokepoint_disruption"
    assert [candidate["entity_ref"] for candidate in payload["terminal_impact_candidates"]] == [
        "entity_crude_oil"
    ]
    assert payload["terminal_candidates"] == payload["terminal_impact_candidates"]
    assert payload["candidate_paths"][0]["terminal_ref"] == "entity_crude_oil"
    assert payload["candidate_paths"][0]["terminal_type"] == "commodity"
    assert payload["candidate_paths"][0]["terminal_name"] == "Crude oil"
    assert payload["candidate_paths"][0]["jurisdiction"] is None
    assert payload["candidate_paths"][0]["exchange"] is None
    assert payload["candidate_paths"][0]["ticker"] is None
    assert payload["candidate_paths"][0]["security_ids"] == {"ades_ref": "entity_crude_oil"}
    assert payload["candidate_paths"][0]["path_confidence"] == 0.92
    assert (
        payload["candidate_paths"][0]["weakest_edge_ref"]
        == "entity_hormuz->chokepoint_affects_energy->entity_crude_oil"
    )
    assert payload["candidate_paths"][0]["weakest_edge"]["confidence"] == 0.92
    assert payload["candidate_paths"][0]["source_tiers"] == ["test_fixture"]
    assert payload["candidate_paths"][0]["effective_from"] is None
    assert payload["candidate_paths"][0]["effective_to"] is None
    assert payload["candidate_paths"][0]["artifact_ref"] == "sha256:test"
    assert (
        payload["candidate_paths"][0]["relationship_path"]["edges"][0]["relation"]
        == "chokepoint_affects_energy"
    )
    assert payload["rejected_candidates"][0]["terminal_ref"] == "ades:impact:rates:policy-rate"
    assert payload["rejected_candidates"][0]["reason_code"] == "event_incompatible"
    assert any(
        diagnostic["code"] == "country_scope_without_terminal_candidate"
        and diagnostic["entity_ref"] == "country:ir"
        for diagnostic in payload["diagnostics"]
    )
    impact_graph_coverage = next(
        lane for lane in payload["source_lane_coverage"] if lane["lane"] == "impact_graph"
    )
    assert impact_graph_coverage["artifact_hash"] == "sha256:test"
    assert impact_graph_coverage["terminal_candidate_count"] == 1
    assert any(
        lane["lane"] == pack_id and lane["version"] == "0.1.0"
        for lane in payload["source_lane_coverage"]
    )
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
    unresolved_by_ref = {entity["entity_ref"]: entity for entity in payload["unresolved_entities"]}
    assert unresolved_by_ref["country:ir"]["missing_reason"] == (
        "country_scope_without_terminal_candidate"
    )
    assert unresolved_by_ref["country:ir"]["reason"] == ("country_scope_without_terminal_candidate")
    assert unresolved_by_ref["country:ir"]["event_types"] == ["shipping_chokepoint_disruption"]
    assert unresolved_by_ref["country:ir"]["has_terminal_candidate"] is False
    assert "entity_hormuz" not in unresolved_by_ref
    assert payload["tag_responses"] == []


def test_news_analyze_hides_weak_alias_passive_entities(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-weak-passive-aliases-en", domain="general")
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
                    text="SIX",
                    label="organization",
                    start=text.index("SIX"),
                    end=text.index("SIX") + len("SIX"),
                    confidence=0.54,
                    relevance=0.45,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="finance",
                        alias_quality="suspect",
                        weak_alias=True,
                        quality_reasons=["ambiguous_acronym"],
                    ),
                    link=EntityLink(
                        entity_id="wikidata:Q681967",
                        canonical_text="SIX Group",
                        provider="wikidata",
                    ),
                ),
                EntityMatch(
                    text="Lee",
                    label="person",
                    start=text.index("Lee"),
                    end=text.index("Lee") + len("Lee"),
                    confidence=0.51,
                    relevance=0.48,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="business",
                        alias_quality="weak",
                        weak_alias=True,
                        quality_reasons=["homograph_alias"],
                    ),
                    link=EntityLink(
                        entity_id="wikidata:Q484597",
                        canonical_text="Lee Enterprises",
                        provider="wikidata",
                    ),
                ),
            ],
            topics=[TopicMatch(label="politics", score=0.7, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Local officials meet",
            "text": "SIX said Lee met with local officials after a council hearing.",
            "packs": [pack_id],
            "options": {
                "include_passive_entities": True,
                "include_terminal_candidates": False,
                "include_impact_paths": False,
                "include_tag_responses": False,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    passive_by_ref = {entity["entity_ref"]: entity for entity in payload["passive_entities"]}
    six = passive_by_ref["wikidata:Q681967"]
    lee = passive_by_ref["wikidata:Q484597"]
    assert six["quality"] == "weak"
    assert six["display_eligible"] is False
    assert {"weak_alias", "suspect_alias", "ambiguous_acronym"} <= set(six["quality_reasons"])
    assert lee["quality"] == "weak"
    assert lee["display_eligible"] is False
    assert {"weak_alias", "homograph_alias"} <= set(lee["quality_reasons"])
    assert payload["unresolved_entities"] == []


def test_news_analyze_hides_contextual_passive_alias_collisions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-passive-alias-collisions-en", domain="general")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        def _collision_entity(
            surface: str,
            label: str,
            entity_id: str,
            canonical_text: str,
            source_domain: str,
        ) -> EntityMatch:
            return EntityMatch(
                text=surface,
                label=label,
                start=text.index(surface),
                end=text.index(surface) + len(surface),
                confidence=0.74,
                relevance=0.68,
                provenance=EntityProvenance(
                    match_kind="alias",
                    match_path="aliases.json",
                    match_source="pack",
                    source_pack=pack or pack_id,
                    source_domain=source_domain,
                ),
                link=EntityLink(
                    entity_id=entity_id,
                    canonical_text=canonical_text,
                    provider="wikidata",
                ),
            )

        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                _collision_entity(
                    "ONU",
                    "organization",
                    "wikidata:Q1065",
                    "United Nations",
                    "politics",
                ),
                _collision_entity(
                    "OAS",
                    "organization",
                    "wikidata:Q123759",
                    "Organization of American States",
                    "politics",
                ),
                _collision_entity(
                    "JMM",
                    "organization",
                    "wikidata:QJMM",
                    "JMM Holdings",
                    "business",
                ),
                _collision_entity(
                    "MBG",
                    "organization",
                    "wikidata:Q27530",
                    "Mercedes-Benz Group",
                    "business",
                ),
                _collision_entity(
                    "SIX",
                    "organization",
                    "wikidata:Q681967",
                    "SIX Group",
                    "finance",
                ),
                _collision_entity(
                    "Lee",
                    "person",
                    "wikidata:Q484597",
                    "Lee Enterprises",
                    "business",
                ),
            ],
            topics=[TopicMatch(label="politics", score=0.7, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Local officials meet",
            "text": (
                "ONU, OAS, JMM, MBG and SIX said Lee met with local officials "
                "after a council hearing."
            ),
            "packs": [pack_id],
            "options": {
                "include_passive_entities": True,
                "include_terminal_candidates": False,
                "include_impact_paths": False,
                "include_tag_responses": False,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    passive_by_ref = {entity["entity_ref"]: entity for entity in payload["passive_entities"]}
    acronym_refs = [
        "wikidata:Q1065",
        "wikidata:Q123759",
        "wikidata:QJMM",
        "wikidata:Q27530",
        "wikidata:Q681967",
    ]
    for entity_ref in acronym_refs:
        entity = passive_by_ref[entity_ref]
        assert entity["quality"] == "weak"
        assert entity["display_eligible"] is False
        assert {"weak_alias", "ambiguous_acronym"} <= set(entity["quality_reasons"])

    lee = passive_by_ref["wikidata:Q484597"]
    assert lee["quality"] == "weak"
    assert lee["display_eligible"] is False
    assert {"weak_alias", "homograph_alias"} <= set(lee["quality_reasons"])


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


def test_news_analyze_promotes_commodity_mentions_to_direct_terminal_seeds(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-commodity-promotion-en", domain="politics")
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
                    text="United States",
                    label="country",
                    start=text.index("United States"),
                    end=text.index("United States") + len("United States"),
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
                        entity_id="country:us",
                        canonical_text="United States",
                        provider="ades",
                    ),
                ),
                EntityMatch(
                    text="Trump",
                    label="person",
                    start=text.index("Trump"),
                    end=text.index("Trump") + len("Trump"),
                    confidence=0.91,
                    relevance=0.9,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="politics",
                    ),
                    link=EntityLink(
                        entity_id="wikidata:Q22686",
                        canonical_text="Donald Trump",
                        provider="wikidata",
                    ),
                ),
            ],
            topics=[TopicMatch(label="politics", score=0.88, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        refs = set(entity_refs)
        assert {
            "ades:impact:commodity:aluminum",
            "ades:impact:commodity:copper",
            "ades:impact:commodity:steel",
        } <= refs
        commodity_refs = [
            "ades:impact:commodity:aluminum",
            "ades:impact:commodity:copper",
            "ades:impact:commodity:steel",
        ]
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-06-02",
            artifact_hash="sha256:test",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref=ref,
                    name=ref.rsplit(":", 1)[-1].replace("-", " ").title(),
                    entity_type="commodity",
                    is_graph_seed=True,
                    seed_degree=1,
                    is_tradable=True,
                )
                for ref in commodity_refs
            ],
            candidates=[
                ImpactCandidate(
                    entity_ref=ref,
                    name=ref.rsplit(":", 1)[-1].replace("-", " ").title(),
                    entity_type="commodity",
                    evidence_level="direct",
                    confidence=0.94,
                    source_entity_refs=[ref],
                    relationship_paths=[],
                )
                for ref in commodity_refs
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Trump tariffs target metals",
            "text": "United States officials said Trump tariffs on steel and imported metals would rise.",
            "packs": [pack_id],
            "categorized_entities": [
                {"text": "aluminum", "entity_type": "industrial_metal"},
                {"name": "Copper", "label": "commodity"},
            ],
            "options": {
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "max_terminal_candidates": 8,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    expected_refs = {
        "ades:impact:commodity:aluminum",
        "ades:impact:commodity:copper",
        "ades:impact:commodity:steel",
    }
    refs = {candidate["entity_ref"] for candidate in payload["terminal_impact_candidates"]}
    assert expected_refs <= refs
    assert all(
        candidate["compatible_event_types"] == ["tariff"]
        for candidate in payload["terminal_impact_candidates"]
        if candidate["entity_ref"] in expected_refs
    )
    passive_refs = {entity["entity_ref"] for entity in payload["passive_entities"]}
    assert "ades:impact:commodity:copper" not in passive_refs


def test_news_analyze_promotes_precious_metal_market_moves(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-gold-promotion-en", domain="finance")
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
            entities=[],
            topics=[TopicMatch(label="finance", score=0.88, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "ades:impact:commodity:gold" in set(entity_refs)
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-06-02",
            artifact_hash="sha256:test",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="ades:impact:commodity:gold",
                    name="Gold",
                    entity_type="commodity",
                    is_graph_seed=True,
                    seed_degree=1,
                    is_tradable=True,
                )
            ],
            candidates=[
                ImpactCandidate(
                    entity_ref="ades:impact:commodity:gold",
                    name="Gold",
                    entity_type="commodity",
                    evidence_level="direct",
                    confidence=0.94,
                    source_entity_refs=["ades:impact:commodity:gold"],
                    relationship_paths=[],
                )
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Gold slips as Middle East tensions keep inflation risks elevated",
            "text": (
                "Gold slips as Middle East tensions keep inflation risks elevated. "
                "Investors said gold prices fell while safe-haven demand stayed in focus."
            ),
            "packs": [pack_id],
            "options": {
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "max_terminal_candidates": 8,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    refs = {candidate["entity_ref"] for candidate in payload["terminal_impact_candidates"]}
    assert "ades:impact:commodity:gold" in refs
    assert all(
        candidate["compatible_event_types"] == ["safe_haven_commodity_move"]
        for candidate in payload["terminal_impact_candidates"]
        if candidate["entity_ref"] == "ades:impact:commodity:gold"
    )


def test_news_analyze_promotes_tradable_source_entities_to_terminals(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-direct-equity-source-en", domain="finance")
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
                    text="Example Corp",
                    label="ticker",
                    start=text.index("Example Corp"),
                    end=text.index("Example Corp") + len("Example Corp"),
                    confidence=0.95,
                    relevance=0.96,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="finance",
                    ),
                    link=EntityLink(
                        entity_id="finance-us-ticker:EXM",
                        canonical_text="Example Corp",
                        provider="ades",
                    ),
                )
            ],
            topics=[TopicMatch(label="finance", score=0.9, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "finance-us-ticker:EXM" in set(entity_refs)
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-06-28",
            artifact_hash="sha256:test",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="finance-us-ticker:EXM",
                    name="Example Corp",
                    entity_type="ticker",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=True,
                ),
                ImpactSourceEntity(
                    entity_ref="wikidata:QEXM",
                    name="Example Corp",
                    same_as_refs=["finance-us-ticker:EXA"],
                    entity_type="organization",
                    is_graph_seed=True,
                    seed_degree=1,
                    is_tradable=False,
                ),
            ],
            candidates=[],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Example Corp reports earnings",
            "text": "Example Corp reported earnings that moved its shares.",
            "packs": [pack_id],
            "options": {
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "max_terminal_candidates": 8,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    candidates = {
        candidate["entity_ref"]: candidate for candidate in payload["terminal_impact_candidates"]
    }
    assert "finance-us-ticker:EXM" in candidates
    assert candidates["finance-us-ticker:EXM"]["evidence_level"] == "direct"
    assert candidates["finance-us-ticker:EXM"]["source_entity_refs"] == ["finance-us-ticker:EXM"]
    assert "finance-us-ticker:EXA" in candidates
    assert candidates["finance-us-ticker:EXA"]["evidence_level"] == "direct"
    assert candidates["finance-us-ticker:EXA"]["source_entity_refs"] == [
        "wikidata:QEXM",
        "finance-us-ticker:EXA",
    ]
    assert "NO_TERMINAL_IMPACT_CANDIDATES" not in payload["quality_flags"]


def test_news_analyze_maps_legal_entity_to_terminal_ticker_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-legal-entity-terminal-en", domain="finance")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))
    article_text = "Example Holdings beat earnings estimates and its shares rallied."

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        start = text.index("Example Holdings")
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text="Example Holdings",
                    label="legal_entity",
                    start=start,
                    end=start + len("Example Holdings"),
                    confidence=0.95,
                    relevance=0.96,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="finance",
                    ),
                    link=EntityLink(
                        entity_id="wikidata:Q123456",
                        canonical_text="Example Holdings PLC",
                        provider="ades",
                    ),
                )
            ],
            topics=[TopicMatch(label="finance", score=0.9, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "wikidata:Q123456" in set(entity_refs)
        relationship_path = ImpactRelationshipPath(
            path_depth=1,
            edges=[
                ImpactPathEdge(
                    source_ref="wikidata:Q123456",
                    target_ref="finance-us-ticker:EXM",
                    relation="issuer_has_listed_ticker",
                    evidence_level="direct",
                    confidence=0.91,
                    direction_hint="issuer_to_ticker",
                    source_name="Example Exchange issuer directory",
                    source_url="https://exchange.example/listings/exm",
                    source_snapshot="2026-06-30",
                    source_year=2026,
                    source_tier="exchange",
                    effective_from="2024-01-01",
                )
            ],
        )
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-06-30",
            artifact_hash="sha256:legal-entity-terminal",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="wikidata:Q123456",
                    name="Example Holdings PLC",
                    entity_type="legal_entity",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=False,
                )
            ],
            candidates=[
                ImpactCandidate(
                    entity_ref="finance-us-ticker:EXM",
                    name="Example Holdings PLC",
                    entity_type="ticker",
                    evidence_level="shallow",
                    confidence=0.88,
                    source_entity_refs=["wikidata:Q123456"],
                    relationship_paths=[relationship_path],
                )
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Example Holdings beat earnings estimates",
            "text": article_text,
            "packs": [pack_id],
            "options": {
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "max_terminal_candidates": 8,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert [candidate["entity_ref"] for candidate in payload["terminal_impact_candidates"]] == [
        "finance-us-ticker:EXM"
    ]
    candidate = payload["terminal_impact_candidates"][0]
    assert candidate["source_entity_refs"] == ["wikidata:Q123456"]
    assert candidate["compatible_event_types"] == ["earnings_beat"]

    candidate_paths = payload["candidate_paths"]
    assert len(candidate_paths) == 1
    path = candidate_paths[0]
    assert path["terminal_ref"] == "finance-us-ticker:EXM"
    assert path["terminal_type"] == "ticker"
    assert path["terminal_name"] == "Example Holdings PLC"
    assert path["jurisdiction"] == "us"
    assert path["ticker"] == "EXM"
    assert path["security_ids"] == {
        "ades_ref": "finance-us-ticker:EXM",
        "ticker": "EXM",
    }
    assert path["source_entity_refs"] == ["wikidata:Q123456"]
    assert path["source_tiers"] == ["exchange"]
    assert path["effective_from"] == "2024-01-01"
    assert path["path_confidence"] == 0.88
    assert path["weakest_edge_ref"] == (
        "wikidata:Q123456->issuer_has_listed_ticker->finance-us-ticker:EXM"
    )
    assert path["weakest_edge_confidence"] == 0.91
    assert path["weakest_edge"]["source_tier"] == "exchange"
    assert path["weakest_edge"]["source_url"] == "https://exchange.example/listings/exm"
    assert path["relationship_path"]["edges"][0]["relation"] == "issuer_has_listed_ticker"
    assert path["artifact_ref"] == "sha256:legal-entity-terminal"
    assert "NO_TERMINAL_IMPACT_CANDIDATES" not in payload["quality_flags"]


def test_news_analyze_maps_product_and_brand_mentions_to_owner_security_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-product-brand-terminal-en", domain="finance")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))
    article_text = (
        "Mekaar and Oxxo beat earnings estimates as the companies expanded lending "
        "and convenience-store sales."
    )

    def _entity(
        surface: str,
        label: str,
        entity_ref: str,
        canonical_text: str,
    ) -> EntityMatch:
        start = article_text.index(surface)
        return EntityMatch(
            text=surface,
            label=label,
            start=start,
            end=start + len(surface),
            confidence=0.94,
            relevance=0.95,
            provenance=EntityProvenance(
                match_kind="alias",
                match_path="aliases.json",
                match_source="pack",
                source_pack=pack_id,
                source_domain="finance",
            ),
            link=EntityLink(
                entity_id=entity_ref,
                canonical_text=canonical_text,
                provider="ades",
            ),
        )

    def _edge(
        source_ref: str,
        target_ref: str,
        relation: str,
        *,
        confidence: float,
        source_name: str,
        source_url: str,
        effective_from: str,
    ) -> ImpactPathEdge:
        return ImpactPathEdge(
            source_ref=source_ref,
            target_ref=target_ref,
            relation=relation,
            evidence_level="direct",
            confidence=confidence,
            direction_hint="source_backed_owner_terminal_path",
            source_name=source_name,
            source_url=source_url,
            source_snapshot="2026-06-30",
            source_year=2026,
            source_tier="issuer_disclosed",
            effective_from=effective_from,
        )

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
                _entity(
                    "Mekaar",
                    "product",
                    "ades:product:id:pnm-mekaar",
                    "PNM Mekaar",
                ),
                _entity(
                    "Oxxo",
                    "brand",
                    "ades:brand:mx:oxxo",
                    "Oxxo",
                ),
            ],
            topics=[TopicMatch(label="finance", score=0.91, evidence_count=2)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert {"ades:product:id:pnm-mekaar", "ades:brand:mx:oxxo"}.issubset(set(entity_refs))
        mekaar_path = ImpactRelationshipPath(
            path_depth=2,
            edges=[
                _edge(
                    "ades:product:id:pnm-mekaar",
                    "finance-id-issuer:bank-rakyat-indonesia",
                    "product_owned_by_org",
                    confidence=0.94,
                    source_name="PNM annual report",
                    source_url="https://example.test/pnm-annual-report",
                    effective_from="2024-01-01",
                ),
                _edge(
                    "finance-id-issuer:bank-rakyat-indonesia",
                    "finance-id-ticker:BBRI",
                    "issuer_has_listed_ticker",
                    confidence=0.97,
                    source_name="Indonesia Stock Exchange issuer directory",
                    source_url="https://example.test/idx-bbri",
                    effective_from="2024-01-01",
                ),
            ],
        )
        oxxo_path = ImpactRelationshipPath(
            path_depth=2,
            edges=[
                _edge(
                    "ades:brand:mx:oxxo",
                    "finance-mx-issuer:FEMSA",
                    "brand_owned_by_org",
                    confidence=0.93,
                    source_name="FEMSA annual report",
                    source_url="https://example.test/femsa-annual-report",
                    effective_from="2025-01-01",
                ),
                _edge(
                    "finance-mx-issuer:FEMSA",
                    "ades:security:mx:bmv:femsa-ubd",
                    "issuer_has_security",
                    confidence=0.96,
                    source_name="BMV issuer security directory",
                    source_url="https://example.test/bmv-femsa-ubd",
                    effective_from="2025-01-01",
                ),
            ],
        )
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-06-30",
            artifact_hash="sha256:product-brand-terminals",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="ades:product:id:pnm-mekaar",
                    name="PNM Mekaar",
                    entity_type="product",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=False,
                ),
                ImpactSourceEntity(
                    entity_ref="ades:brand:mx:oxxo",
                    name="Oxxo",
                    entity_type="brand",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=False,
                ),
            ],
            candidates=[
                ImpactCandidate(
                    entity_ref="finance-id-ticker:BBRI",
                    name="Bank Rakyat Indonesia",
                    entity_type="ticker",
                    evidence_level="direct",
                    confidence=0.92,
                    source_entity_refs=["ades:product:id:pnm-mekaar"],
                    relationship_paths=[mekaar_path],
                    compatible_event_types=["earnings_beat"],
                ),
                ImpactCandidate(
                    entity_ref="ades:security:mx:bmv:femsa-ubd",
                    name="FEMSA UBD share",
                    entity_type="security",
                    evidence_level="direct",
                    confidence=0.91,
                    source_entity_refs=["ades:brand:mx:oxxo"],
                    relationship_paths=[oxxo_path],
                    compatible_event_types=["earnings_beat"],
                ),
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Mekaar and Oxxo beat earnings estimates",
            "text": article_text,
            "packs": [pack_id],
            "options": {
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "max_terminal_candidates": 8,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    candidates_by_ref = {
        candidate["entity_ref"]: candidate for candidate in payload["terminal_impact_candidates"]
    }
    assert set(candidates_by_ref) == {
        "finance-id-ticker:BBRI",
        "ades:security:mx:bmv:femsa-ubd",
    }
    assert candidates_by_ref["finance-id-ticker:BBRI"]["source_entity_refs"] == [
        "ades:product:id:pnm-mekaar"
    ]
    assert candidates_by_ref["ades:security:mx:bmv:femsa-ubd"]["source_entity_refs"] == [
        "ades:brand:mx:oxxo"
    ]

    paths_by_ref = {
        candidate_path["terminal_ref"]: candidate_path
        for candidate_path in payload["candidate_paths"]
    }
    mekaar_path = paths_by_ref["finance-id-ticker:BBRI"]
    assert mekaar_path["jurisdiction"] == "id"
    assert mekaar_path["ticker"] == "BBRI"
    assert mekaar_path["source_entity_refs"] == ["ades:product:id:pnm-mekaar"]
    assert mekaar_path["source_tiers"] == ["issuer_disclosed"]
    assert mekaar_path["path_confidence"] == 0.92
    assert mekaar_path["weakest_edge_ref"] == (
        "ades:product:id:pnm-mekaar->product_owned_by_org->finance-id-issuer:bank-rakyat-indonesia"
    )
    assert [edge["relation"] for edge in mekaar_path["relationship_path"]["edges"]] == [
        "product_owned_by_org",
        "issuer_has_listed_ticker",
    ]
    assert mekaar_path["artifact_ref"] == "sha256:product-brand-terminals"

    oxxo_path = paths_by_ref["ades:security:mx:bmv:femsa-ubd"]
    assert oxxo_path["jurisdiction"] == "mx"
    assert oxxo_path["exchange"] == "bmv"
    assert oxxo_path["security_ids"] == {
        "ades_ref": "ades:security:mx:bmv:femsa-ubd",
        "local_security_id": "femsa-ubd",
    }
    assert oxxo_path["source_entity_refs"] == ["ades:brand:mx:oxxo"]
    assert oxxo_path["path_confidence"] == 0.91
    assert oxxo_path["weakest_edge_ref"] == (
        "ades:brand:mx:oxxo->brand_owned_by_org->finance-mx-issuer:FEMSA"
    )
    assert [edge["relation"] for edge in oxxo_path["relationship_path"]["edges"]] == [
        "brand_owned_by_org",
        "issuer_has_security",
    ]
    assert "NO_TERMINAL_IMPACT_CANDIDATES" not in payload["quality_flags"]


def test_news_analyze_promotes_only_ades_confirmed_direct_tradable_mentions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-direct-tradables-en", domain="finance")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _entity(
        text: str,
        label: str,
        entity_ref: str,
        *,
        canonical_text: str | None = None,
    ) -> EntityMatch:
        start = article_text.index(text)
        return EntityMatch(
            text=text,
            label=label,
            start=start,
            end=start + len(text),
            confidence=0.94,
            relevance=0.95,
            provenance=EntityProvenance(
                match_kind="alias",
                match_path="aliases.json",
                match_source="pack",
                source_pack=pack_id,
                source_domain="finance",
            ),
            link=EntityLink(
                entity_id=entity_ref,
                canonical_text=canonical_text or text,
                provider="ades",
            ),
        )

    article_text = (
        "SPY rose while NVDA common stock rallied and USD/BRL jumped; MISS was also mentioned."
    )

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
                _entity("SPY", "market_index", "finance-us:index:SPY", canonical_text="SPY"),
                _entity(
                    "NVDA common stock",
                    "security",
                    "ades:security:us:nasdaq:us67066g1040-common-stock",
                    canonical_text="NVIDIA common stock",
                ),
                _entity(
                    "USD/BRL",
                    "fx_pair",
                    "ades:impact:currency-pair:usd-brl",
                    canonical_text="USD/BRL",
                ),
                _entity("MISS", "ticker", "finance-us-ticker:MISS", canonical_text="MISS"),
            ],
            topics=[TopicMatch(label="finance", score=0.9, evidence_count=4)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert {
            "finance-us:index:SPY",
            "ades:security:us:nasdaq:us67066g1040-common-stock",
            "ades:impact:currency-pair:usd-brl",
            "finance-us-ticker:MISS",
        }.issubset(set(entity_refs))
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-06-30",
            artifact_hash="sha256:test",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="finance-us:index:SPY",
                    name="SPY",
                    entity_type="market_index",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=True,
                ),
                ImpactSourceEntity(
                    entity_ref="ades:security:us:nasdaq:us67066g1040-common-stock",
                    name="NVIDIA common stock",
                    entity_type="security",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=True,
                ),
                ImpactSourceEntity(
                    entity_ref="ades:impact:currency-pair:usd-brl",
                    name="USD/BRL",
                    entity_type="fx_pair",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=True,
                ),
            ],
            candidates=[],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Direct tradable mentions move",
            "text": article_text,
            "packs": [pack_id],
            "options": {
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "max_terminal_candidates": 8,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    candidates = {
        candidate["entity_ref"]: candidate for candidate in payload["terminal_impact_candidates"]
    }
    assert set(candidates) == {
        "finance-us:index:SPY",
        "ades:security:us:nasdaq:us67066g1040-common-stock",
        "ades:impact:currency-pair:usd-brl",
    }
    assert candidates["finance-us:index:SPY"]["entity_type"] == "market_index"
    assert (
        candidates["ades:security:us:nasdaq:us67066g1040-common-stock"]["entity_type"] == "security"
    )
    assert candidates["ades:impact:currency-pair:usd-brl"]["entity_type"] == "currency_pair"
    assert "finance-us-ticker:MISS" not in candidates
    assert "NO_TERMINAL_IMPACT_CANDIDATES" not in payload["quality_flags"]


def test_news_analyze_keeps_deterministic_rule_entities_in_terminal_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-rule-backed-ticker-en", domain="finance")
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
                    text="EXM",
                    label="ticker",
                    start=text.index("EXM"),
                    end=text.index("EXM") + len("EXM"),
                    confidence=0.9,
                    relevance=0.92,
                    provenance=EntityProvenance(
                        match_kind="rule",
                        match_path="rule.regex",
                        match_source="ticker_symbol",
                        source_pack=pack or pack_id,
                        source_domain="finance",
                        model_name="regex",
                    ),
                    link=EntityLink(
                        entity_id="finance-us-ticker:EXM",
                        canonical_text="Example Corp",
                        provider="rule.regex",
                    ),
                )
            ],
            topics=[TopicMatch(label="finance", score=0.9, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "finance-us-ticker:EXM" in set(entity_refs)
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-06-30",
            artifact_hash="sha256:test",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="finance-us-ticker:EXM",
                    name="Example Corp",
                    entity_type="ticker",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=True,
                )
            ],
            candidates=[],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "EXM reports earnings beat",
            "text": "EXM reported an earnings beat and raised guidance.",
            "packs": [pack_id],
            "options": {
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert [candidate["entity_ref"] for candidate in payload["terminal_impact_candidates"]] == [
        "finance-us-ticker:EXM"
    ]
    assert not any(
        warning.startswith("ADES_NEWS_ANALYZE_LOW_TRUST_ENTITY_CLAIMS_PROPOSAL_ONLY")
        for warning in payload["warnings"]
    )


def test_news_analyze_returns_review_only_relationship_proposals(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-proposal-en", domain="business")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        include_related_entities: bool = False,
        include_graph_support: bool = False,
        **_: object,
    ) -> TagResponse:
        assert include_related_entities is True
        assert include_graph_support is True
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text="Example Minister",
                    label="person",
                    start=text.index("Example Minister"),
                    end=text.index("Example Minister") + len("Example Minister"),
                    confidence=0.91,
                    relevance=0.93,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="politics",
                    ),
                    link=EntityLink(
                        entity_id="wikidata:Qminister",
                        canonical_text="Example Minister",
                        provider="wikidata",
                    ),
                )
            ],
            related_entities=[
                RelatedEntityMatch(
                    entity_id="wikidata:Qissuer",
                    canonical_text="Example Listed Issuer",
                    score=0.84,
                    provider="qdrant.qid_graph",
                    entity_type="organization",
                    seed_entity_ids=["wikidata:Qminister"],
                    shared_seed_count=1,
                )
            ],
            topics=[TopicMatch(label="politics", score=0.88, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "wikidata:Qminister" in list(entity_refs)
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-05-13",
            artifact_hash="sha256:test",
            source_entities=[],
            candidates=[],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Minister signals possible rate cuts",
            "text": "Example Minister said the central bank may cut interest rates next quarter.",
            "hints": {"country": "us", "topics": ["politics"]},
            "packs": [pack_id],
            "options": {
                "include_relationship_proposals": True,
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["relationship_proposals"] == [
        {
            "entity_ref": "wikidata:Qissuer",
            "name": "Example Listed Issuer",
            "entity_type": "organization",
            "score": 0.84,
            "provider": "qdrant.qid_graph",
            "source": "related_entity",
            "seed_entity_refs": ["wikidata:Qminister"],
            "shared_seed_count": 1,
            "publication_allowed": False,
            "promotion_required": "source_backed_edge_or_strict_identity_bridge",
        }
    ]
    unresolved_by_ref = {entity["entity_ref"]: entity for entity in payload["unresolved_entities"]}
    assert unresolved_by_ref["wikidata:Qminister"]["candidate_proposals"] == 1
    assert unresolved_by_ref["wikidata:Qminister"]["missing_reason"] == (
        "passive_relationship_without_terminal_candidate"
    )


def test_news_analyze_keeps_low_trust_entity_claims_out_of_terminal_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-low-trust-claim-en", domain="finance")
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
                    text="FAKE",
                    label="ticker",
                    start=text.index("FAKE"),
                    end=text.index("FAKE") + len("FAKE"),
                    confidence=0.82,
                    relevance=0.9,
                    provenance=EntityProvenance(
                        match_kind="proposal",
                        match_path="llm-digest",
                        match_source="llm",
                        source_pack=pack or pack_id,
                        source_domain="finance",
                        model_name="test-llm",
                    ),
                    link=EntityLink(
                        entity_id="finance-us-ticker:FAKE",
                        canonical_text="Fake Proposed Ticker",
                        provider="llm.proposal",
                    ),
                )
            ],
            related_entities=[],
            topics=[TopicMatch(label="finance", score=0.88, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(*_: object, **__: object) -> ImpactExpansionResult:
        raise AssertionError("low-trust proposal entities must not seed impact expansion")

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "LLM proposes FAKE ticker after earnings beat",
            "text": "Analysts said FAKE reported an earnings beat, but the ticker came from a model claim.",
            "packs": [pack_id],
            "options": {
                "include_relationship_proposals": True,
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["terminal_impact_candidates"] == []
    assert payload["relationship_proposals"] == [
        {
            "entity_ref": "finance-us-ticker:FAKE",
            "name": "Fake Proposed Ticker",
            "entity_type": "ticker",
            "score": 0.9,
            "provider": "llm.proposal",
            "source": "extracted_entity_claim",
            "seed_entity_refs": ["finance-us-ticker:FAKE"],
            "shared_seed_count": 1,
            "publication_allowed": False,
            "promotion_required": "source_backed_edge_or_strict_identity_bridge",
        }
    ]
    assert payload["warnings"] == ["ADES_NEWS_ANALYZE_LOW_TRUST_ENTITY_CLAIMS_PROPOSAL_ONLY:1"]


def test_news_analyze_returns_passive_path_relationship_proposals(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-passive-path-proposal-en", domain="business")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        include_related_entities: bool = False,
        include_graph_support: bool = False,
        **_: object,
    ) -> TagResponse:
        assert include_related_entities is True
        assert include_graph_support is True
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text="Example Minister",
                    label="person",
                    start=text.index("Example Minister"),
                    end=text.index("Example Minister") + len("Example Minister"),
                    confidence=0.91,
                    relevance=0.93,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="politics",
                    ),
                    link=EntityLink(
                        entity_id="wikidata:Qminister",
                        canonical_text="Example Minister",
                        provider="wikidata",
                    ),
                )
            ],
            related_entities=[],
            topics=[TopicMatch(label="politics", score=0.88, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "wikidata:Qminister" in list(entity_refs)
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-05-14",
            artifact_hash="sha256:test",
            source_entities=[],
            candidates=[],
            passive_paths=[
                ImpactPassivePath(
                    entity_ref="finance-us-issuer:0000000001",
                    name="Example Listed Issuer",
                    entity_type="organization",
                    source_entity_refs=["wikidata:Qminister"],
                    relationship_paths=[
                        ImpactRelationshipPath(
                            path_depth=1,
                            edges=[
                                ImpactPathEdge(
                                    source_ref="wikidata:Qminister",
                                    target_ref="finance-us-issuer:0000000001",
                                    relation="person_is_board_member_of_issuer",
                                    evidence_level="direct",
                                    confidence=0.82,
                                    direction_hint="contextual",
                                    source_name="test",
                                    source_url="https://example.com",
                                    source_snapshot="test",
                                )
                            ],
                        )
                    ],
                )
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Minister signals possible rate cuts",
            "text": "Example Minister said the central bank may cut interest rates next quarter.",
            "hints": {"country": "us", "topics": ["politics"]},
            "packs": [pack_id],
            "options": {
                "include_relationship_proposals": True,
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["relationship_proposals"] == [
        {
            "entity_ref": "finance-us-issuer:0000000001",
            "name": "Example Listed Issuer",
            "entity_type": "organization",
            "score": 0.82,
            "provider": "ades.impact.passive_path",
            "source": "impact_passive_path",
            "seed_entity_refs": ["wikidata:Qminister"],
            "shared_seed_count": 1,
            "publication_allowed": False,
            "promotion_required": "source_backed_edge_or_strict_identity_bridge",
        }
    ]
    unresolved_by_ref = {entity["entity_ref"]: entity for entity in payload["unresolved_entities"]}
    assert unresolved_by_ref["wikidata:Qminister"]["candidate_proposals"] == 1


def test_news_analyze_suppresses_relationship_proposals_without_market_event(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(
        tmp_path,
        "news-passive-path-no-event-proposal-en",
        domain="politics",
    )
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        include_related_entities: bool = False,
        include_graph_support: bool = False,
        **_: object,
    ) -> TagResponse:
        assert include_related_entities is True
        assert include_graph_support is True
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text="Example Minister",
                    label="person",
                    start=text.index("Example Minister"),
                    end=text.index("Example Minister") + len("Example Minister"),
                    confidence=0.91,
                    relevance=0.93,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="politics",
                    ),
                    link=EntityLink(
                        entity_id="wikidata:Qminister",
                        canonical_text="Example Minister",
                        provider="wikidata",
                    ),
                )
            ],
            related_entities=[
                RelatedEntityMatch(
                    entity_id="finance-us-issuer:0000000001",
                    canonical_text="Example Listed Issuer",
                    score=0.84,
                    provider="qdrant.qid_graph",
                    entity_type="organization",
                    seed_entity_ids=["wikidata:Qminister"],
                    shared_seed_count=1,
                )
            ],
            topics=[TopicMatch(label="politics", score=0.88, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "wikidata:Qminister" in list(entity_refs)
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-05-14",
            artifact_hash="sha256:test",
            source_entities=[],
            candidates=[],
            passive_paths=[
                ImpactPassivePath(
                    entity_ref="finance-us-issuer:0000000001",
                    name="Example Listed Issuer",
                    entity_type="organization",
                    source_entity_refs=["wikidata:Qminister"],
                    relationship_paths=[
                        ImpactRelationshipPath(
                            path_depth=1,
                            edges=[
                                ImpactPathEdge(
                                    source_ref="wikidata:Qminister",
                                    target_ref="finance-us-issuer:0000000001",
                                    relation="person_is_board_member_of_issuer",
                                    evidence_level="direct",
                                    confidence=0.82,
                                    direction_hint="contextual",
                                    source_name="test",
                                    source_url="https://example.com",
                                    source_snapshot="test",
                                )
                            ],
                        )
                    ],
                )
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Minister wins party leadership vote",
            "text": "Example Minister won the party leadership vote after parliament met.",
            "hints": {"country": "us", "topics": ["politics"]},
            "packs": [pack_id],
            "options": {
                "include_relationship_proposals": True,
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["event_signals"] == []
    assert payload["relationship_proposals"] == []
    unresolved_by_ref = {entity["entity_ref"]: entity for entity in payload["unresolved_entities"]}
    assert unresolved_by_ref["wikidata:Qminister"]["candidate_proposals"] == 0
    assert unresolved_by_ref["wikidata:Qminister"]["missing_reason"] == ("no_market_event_signal")


def test_news_analyze_dedupes_pack_scoped_heuristic_entities(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_one = _install_named_pack(tmp_path, "news-heuristic-one-en", domain="business")
    pack_two = _install_named_pack(tmp_path, "news-heuristic-two-en", domain="politics")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        assert pack is not None
        return TagResponse(
            version="0.1.0",
            pack=pack,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text="US Muslim group",
                    label="organization",
                    start=text.index("US Muslim group"),
                    end=text.index("US Muslim group") + len("US Muslim group"),
                    confidence=0.81,
                    relevance=0.82,
                    provenance=EntityProvenance(
                        match_kind="proposal",
                        match_path="heuristic",
                        match_source="structural",
                        source_pack=pack,
                        source_domain="politics",
                    ),
                    link=EntityLink(
                        entity_id=(
                            "ades:heuristic_structural_organization:"
                            f"{pack}:organization:us-muslim-group"
                        ),
                        canonical_text="US Muslim group",
                        provider="ades",
                    ),
                )
            ],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert len(list(entity_refs)) == 1
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-05-13",
            artifact_hash="sha256:test",
            source_entities=[],
            candidates=[],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "US Muslim group denounces Sharia hoax hearing",
            "text": "US Muslim group denounces Sharia hoax congressional hearing.",
            "hints": {"country": "us", "topics": ["politics"]},
            "packs": [pack_one, pack_two],
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
    passive_group_entities = [
        entity for entity in payload["passive_entities"] if entity["name"] == "US Muslim group"
    ]
    unresolved_group_entities = [
        entity for entity in payload["unresolved_entities"] if entity["name"] == "US Muslim group"
    ]
    assert len(passive_group_entities) == 1
    assert len(unresolved_group_entities) == 1
    assert passive_group_entities[0]["mention_count"] == 2
    assert unresolved_group_entities[0]["mention_count"] == 2


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
    passive_by_name = {entity["name"]: entity for entity in response.json()["passive_entities"]}
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
        decision["pack_id"] == "finance-br-en" and decision["reason"] == "text_country_mention"
        for decision in payload["pack_decisions"]
    )


def test_news_analyze_uses_sector_graph_seeds_for_uninstalled_country_pack(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for pack_id, domain in (
        ("general-en", "general"),
        ("finance-en", "finance"),
    ):
        _install_named_pack(tmp_path, pack_id, domain=domain)
    node_path = tmp_path / "impact_nodes.tsv"
    edge_path = tmp_path / "impact_edges.tsv"
    node_path.write_text(
        "\t".join(
            [
                "entity_ref",
                "canonical_name",
                "entity_type",
                "library_id",
                "is_tradable",
                "is_seed_eligible",
                "identifiers_json",
                "packs",
            ]
        )
        + "\n"
        + "\n".join(
            [
                "finance-uk-issuer:beowulf-mining\tBeowulf Mining PLC\torganization\tfinance-uk-en\t0\t1\t{}\tfinance-uk-en",
                "finance-uk-ticker:bem\tBEM\tticker\tfinance-uk-en\t1\t0\t{}\tfinance-uk-en",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    edge_path.write_text(
        "\t".join(
            [
                "source_ref",
                "target_ref",
                "relation",
                "evidence_level",
                "confidence",
                "direction_hint",
                "source_name",
                "source_url",
                "source_snapshot",
                "source_year",
                "refresh_policy",
                "pack_ids",
                "notes",
                "compatible_event_types",
                "direction_preconditions",
            ]
        )
        + "\n"
        + (
            "finance-uk-issuer:beowulf-mining\tfinance-uk-ticker:bem\t"
            "issuer_has_listed_ticker\tdirect\t0.96\tlisted_equity\t"
            "London Stock Exchange\thttps://www.londonstockexchange.com/\t"
            "2026-06-29\t2026\tannual\tfinance-uk-en\told artifact fixture\t"
            "earnings_beat\tlisted_issuer\n"
        ),
        encoding="utf-8",
    )
    graph = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "graph-artifact",
        artifact_version="2026-06-29Tsector-seed-test",
    )

    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", graph.artifact_path)
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
            "title": "UK parliament passes mining royalty reform",
            "text": (
                "The UK parliament passed a mining royalty reform that analysts said "
                "could affect listed mining companies."
            ),
            "options": {
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "max_country_finance_packs": 1,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert any(
        decision["pack_id"] == "finance-uk-en"
        and decision["selected"] is False
        and decision["country_code"] == "uk"
        for decision in payload["pack_decisions"]
    )
    assert any(
        source["entity_ref"] == "finance-uk-issuer:beowulf-mining"
        for source in payload["source_entities"]
    )
    candidates_by_ref = {
        candidate["entity_ref"]: candidate for candidate in payload["terminal_impact_candidates"]
    }
    assert "finance-uk-ticker:bem" in candidates_by_ref
    assert candidates_by_ref["finance-uk-ticker:bem"]["compatible_event_types"] == [
        "sector_policy_change"
    ]
    assert candidates_by_ref["finance-uk-ticker:bem"]["relationship_paths"]
    assert (
        candidates_by_ref["finance-uk-ticker:bem"]["relationship_paths"][0]["edges"][0]["relation"]
        == "issuer_has_listed_ticker"
    )
    candidate_paths_by_ref = {
        candidate_path["terminal_ref"]: candidate_path
        for candidate_path in payload["candidate_paths"]
    }
    bem_path = candidate_paths_by_ref["finance-uk-ticker:bem"]
    assert bem_path["terminal_type"] == "ticker"
    assert bem_path["terminal_name"] == "BEM"
    assert bem_path["jurisdiction"] == "uk"
    assert bem_path["exchange"] is None
    assert bem_path["ticker"] == "bem"
    assert bem_path["security_ids"] == {
        "ades_ref": "finance-uk-ticker:bem",
        "ticker": "bem",
    }
    assert bem_path["event_compatibility"] == ["sector_policy_change"]
    assert bem_path["path_confidence"] == 0.81216
    assert (
        bem_path["weakest_edge_ref"]
        == "finance-uk-issuer:beowulf-mining->issuer_has_listed_ticker->finance-uk-ticker:bem"
    )
    assert bem_path["weakest_edge_confidence"] == 0.96
    assert bem_path["source_tiers"] == ["exchange"]
    assert bem_path["effective_from"] == "2026-01-01"
    assert bem_path["effective_to"] is None
    assert bem_path["artifact_ref"] == graph.artifact_hash
    assert "NO_TERMINAL_IMPACT_CANDIDATES" not in payload["quality_flags"]


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
