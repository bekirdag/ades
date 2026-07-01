from dataclasses import dataclass
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ades.packs.registry import PackRegistry
from ades.service.app import create_app
from ades.service.models import (
    EntityLink,
    EntityMatch,
    EntityProvenance,
    ImpactCandidate,
    ImpactExpansionResult,
    ImpactPathEdge,
    ImpactRelationshipPath,
    ImpactSourceEntity,
    TagResponse,
    TopicMatch,
)
from ades.storage.paths import build_storage_layout, ensure_storage_layout
from tests.pack_registry_helpers import create_pack_source


@dataclass(frozen=True)
class NewsAnalyzePathFamilySmokeCase:
    family: str
    article: str
    surface: str
    source_label: str
    source_ref: str
    source_type: str
    source_name: str
    terminal_ref: str
    terminal_type: str
    terminal_name: str
    expected_event_type: str
    path_relations: tuple[str, ...] = ()
    bridge_refs: tuple[str, ...] = ()
    direct_tradable: bool = False
    source_tier: str = "issuer_disclosed"


PATH_FAMILY_SMOKE_CASES = (
    NewsAnalyzePathFamilySmokeCase(
        family="direct",
        article="USD/BRL jumped as FX volatility increased.",
        surface="USD/BRL",
        source_label="fx_pair",
        source_ref="ades:impact:currency-pair:usd-brl",
        source_type="fx_pair",
        source_name="USD/BRL",
        terminal_ref="ades:impact:currency-pair:usd-brl",
        terminal_type="currency_pair",
        terminal_name="USD/BRL",
        expected_event_type="currency_market_move",
        direct_tradable=True,
        source_tier="central_bank",
    ),
    NewsAnalyzePathFamilySmokeCase(
        family="issuer",
        article="Example Holdings reported an earnings beat and its shares rallied.",
        surface="Example Holdings",
        source_label="issuer",
        source_ref="finance-us-issuer:example-holdings",
        source_type="issuer",
        source_name="Example Holdings",
        terminal_ref="finance-us-ticker:EXH",
        terminal_type="ticker",
        terminal_name="Example Holdings",
        expected_event_type="earnings_beat",
        path_relations=("issuer_has_listed_ticker",),
        source_tier="exchange",
    ),
    NewsAnalyzePathFamilySmokeCase(
        family="product",
        article="Mekaar beat earnings estimates as lending demand expanded.",
        surface="Mekaar",
        source_label="product",
        source_ref="ades:product:id:pnm-mekaar",
        source_type="product",
        source_name="PNM Mekaar",
        terminal_ref="finance-id-ticker:BBRI",
        terminal_type="ticker",
        terminal_name="Bank Rakyat Indonesia",
        expected_event_type="earnings_beat",
        path_relations=("product_owned_by_org", "issuer_has_listed_ticker"),
        bridge_refs=("finance-id-issuer:bank-rakyat-indonesia",),
    ),
    NewsAnalyzePathFamilySmokeCase(
        family="program",
        article="The Ultra Micro program beat earnings estimates after loan demand rose.",
        surface="Ultra Micro program",
        source_label="program",
        source_ref="ades:program:id:ultra-micro-program",
        source_type="program",
        source_name="Ultra Micro Program",
        terminal_ref="ades:security:id:idx:bbri",
        terminal_type="security",
        terminal_name="Bank Rakyat Indonesia listed share",
        expected_event_type="earnings_beat",
        path_relations=(
            "program_operated_by_org",
            "org_part_of_holding",
            "holding_parent_is_issuer",
            "issuer_has_security",
        ),
        bridge_refs=(
            "finance-id-org:permodalan-nasional-madani",
            "ades:holding:id:ultra-micro-holding",
            "finance-id-issuer:bank-rakyat-indonesia",
        ),
    ),
    NewsAnalyzePathFamilySmokeCase(
        family="sector",
        article="US lawmakers approved a rule that limits banking sector fees.",
        surface="banking sector",
        source_label="sector",
        source_ref="ades:sector:us:banking",
        source_type="sector",
        source_name="US banking sector",
        terminal_ref="ades:impact:index:us-bank-index",
        terminal_type="index",
        terminal_name="US Bank Index",
        expected_event_type="sector_policy_change",
        path_relations=("sector_affects_index",),
        source_tier="government",
    ),
    NewsAnalyzePathFamilySmokeCase(
        family="policy",
        article="US lawmakers passed the CHIPS Act for semiconductor companies.",
        surface="CHIPS Act",
        source_label="law",
        source_ref="ades:us-law:chips-act",
        source_type="law",
        source_name="CHIPS Act",
        terminal_ref="finance-us-ticker:nasdaq:xchp",
        terminal_type="ticker",
        terminal_name="Example Semiconductor",
        expected_event_type="sector_policy_change",
        path_relations=(
            "law_affects_sector",
            "sector_affects_issuer",
            "issuer_has_listed_ticker",
        ),
        bridge_refs=(
            "ades:sector:semiconductors",
            "finance-us-issuer:example-semiconductor",
        ),
        source_tier="government",
    ),
    NewsAnalyzePathFamilySmokeCase(
        family="legal",
        article=(
            "The regulator Federal Trade Commission opened an antitrust "
            "investigation into technology companies."
        ),
        surface="Federal Trade Commission",
        source_label="regulator",
        source_ref="ades:us-regulator:ftc",
        source_type="regulator",
        source_name="Federal Trade Commission",
        terminal_ref="finance-us-ticker:nasdaq:xchp",
        terminal_type="ticker",
        terminal_name="Example Semiconductor",
        expected_event_type="regulatory_enforcement",
        path_relations=(
            "regulator_affects_sector",
            "sector_affects_issuer",
            "issuer_has_listed_ticker",
        ),
        bridge_refs=(
            "ades:sector:semiconductors",
            "finance-us-issuer:example-semiconductor",
        ),
        source_tier="regulator",
    ),
    NewsAnalyzePathFamilySmokeCase(
        family="commodity",
        article="A lithium supply disruption hit the battery supply chain.",
        surface="battery supply chain",
        source_label="supply_chain",
        source_ref="ades:impact:supply-chain:battery-materials",
        source_type="supply_chain",
        source_name="Battery materials supply chain",
        terminal_ref="ades:impact:commodity:lithium",
        terminal_type="commodity",
        terminal_name="Lithium",
        expected_event_type="supply_disruption",
        path_relations=("commodity_flow_affects_commodity",),
        source_tier="industry_association",
    ),
    NewsAnalyzePathFamilySmokeCase(
        family="macro",
        article="The central bank cut interest rates as FX volatility increased.",
        surface="central bank",
        source_label="central_bank",
        source_ref="wikidata:Q53536",
        source_type="central_bank",
        source_name="Federal Reserve",
        terminal_ref="ades:impact:currency:usd",
        terminal_type="currency",
        terminal_name="US dollar",
        expected_event_type="policy_rate_cut",
        path_relations=("central_bank_affects_currency",),
        source_tier="central_bank",
    ),
)


def _install_smoke_pack(storage_root: Path, pack_id: str) -> str:
    layout = ensure_storage_layout(build_storage_layout(storage_root))
    pack_dir = create_pack_source(
        layout.packs_dir,
        pack_id=pack_id,
        domain="finance",
        labels=(
            "ticker",
            "issuer",
            "product",
            "program",
            "sector",
            "law",
            "regulator",
            "supply_chain",
            "central_bank",
        ),
        aliases=((pack_id, "organization"),),
    )
    PackRegistry(storage_root).sync_pack_from_disk(pack_dir.name)
    return pack_dir.name


def _entity_match(
    case: NewsAnalyzePathFamilySmokeCase,
    *,
    text: str,
    pack_id: str,
) -> EntityMatch:
    start = text.index(case.surface)
    return EntityMatch(
        text=case.surface,
        label=case.source_label,
        start=start,
        end=start + len(case.surface),
        confidence=0.95,
        relevance=0.96,
        provenance=EntityProvenance(
            match_kind="alias",
            match_path="aliases.json",
            match_source="pack",
            source_pack=pack_id,
            source_domain="finance",
        ),
        link=EntityLink(
            entity_id=case.source_ref,
            canonical_text=case.source_name,
            provider="ades",
        ),
    )


def _path_edge(
    source_ref: str,
    target_ref: str,
    relation: str,
    *,
    index: int,
    case: NewsAnalyzePathFamilySmokeCase,
) -> ImpactPathEdge:
    return ImpactPathEdge(
        source_ref=source_ref,
        target_ref=target_ref,
        relation=relation,
        evidence_level="direct",
        confidence=0.97 - (index * 0.03),
        direction_hint=f"{case.family}_smoke_path",
        source_name=f"{case.family} smoke source",
        source_url=f"https://example.test/{case.family}-smoke-source",
        source_snapshot="2026-07-01",
        source_year=2026,
        source_tier=case.source_tier,
        effective_from="2026-01-01",
        compatible_event_types=[case.expected_event_type],
        direction_preconditions=["source_backed_relationship_evidence"],
    )


def _relationship_path(case: NewsAnalyzePathFamilySmokeCase) -> ImpactRelationshipPath:
    refs = (case.source_ref, *case.bridge_refs, case.terminal_ref)
    return ImpactRelationshipPath(
        path_depth=len(case.path_relations),
        edges=[
            _path_edge(source_ref, target_ref, relation, index=index, case=case)
            for index, (source_ref, target_ref, relation) in enumerate(
                zip(refs, refs[1:], case.path_relations)
            )
        ],
    )


@pytest.mark.parametrize("case", PATH_FAMILY_SMOKE_CASES, ids=lambda case: case.family)
def test_news_analyze_path_family_smoke_harness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: NewsAnalyzePathFamilySmokeCase,
) -> None:
    pack_id = _install_smoke_pack(tmp_path, f"news-{case.family}-path-smoke-en")
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
            entities=[_entity_match(case, text=text, pack_id=pack or pack_id)],
            topics=[TopicMatch(label="finance", score=0.9, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert case.source_ref in set(entity_refs)
        source_entity = ImpactSourceEntity(
            entity_ref=case.source_ref,
            name=case.source_name,
            entity_type=case.source_type,
            is_graph_seed=True,
            seed_degree=0,
            is_tradable=case.direct_tradable,
        )
        if case.direct_tradable:
            return ImpactExpansionResult(
                graph_version="path-family-smoke",
                artifact_version="2026-07-01",
                artifact_hash=f"sha256:{case.family}-path-smoke",
                source_entities=[source_entity],
                candidates=[
                    ImpactCandidate(
                        entity_ref=case.terminal_ref,
                        name=case.terminal_name,
                        entity_type=case.terminal_type,
                        evidence_level="direct",
                        confidence=0.94,
                        source_entity_refs=[case.source_ref],
                        compatible_event_types=[case.expected_event_type],
                    )
                ],
            )
        return ImpactExpansionResult(
            graph_version="path-family-smoke",
            artifact_version="2026-07-01",
            artifact_hash=f"sha256:{case.family}-path-smoke",
            source_entities=[source_entity],
            candidates=[
                ImpactCandidate(
                    entity_ref=case.terminal_ref,
                    name=case.terminal_name,
                    entity_type=case.terminal_type,
                    evidence_level="direct",
                    confidence=0.91,
                    source_entity_refs=[case.source_ref],
                    relationship_paths=[_relationship_path(case)],
                    compatible_event_types=[case.expected_event_type],
                )
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": f"{case.family} smoke",
            "text": case.article,
            "packs": [pack_id],
            "options": {
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "max_terminal_candidates": 12,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert case.expected_event_type in {signal["event_type"] for signal in payload["event_signals"]}
    candidates_by_ref = {
        candidate["entity_ref"]: candidate for candidate in payload["terminal_impact_candidates"]
    }
    assert case.terminal_ref in candidates_by_ref
    assert candidates_by_ref[case.terminal_ref]["source_entity_refs"]
    assert "NO_TERMINAL_IMPACT_CANDIDATES" not in payload["quality_flags"]
    expected_artifact_hash = f"sha256:{case.family}-path-smoke"
    assert payload["artifact_versions"]["impact_artifact_hash"] == expected_artifact_hash
    assert payload["artifact_metadata"]["artifact_hash"] == expected_artifact_hash

    if case.path_relations:
        paths_by_ref = {
            candidate_path["terminal_ref"]: candidate_path
            for candidate_path in payload["candidate_paths"]
        }
        assert case.terminal_ref in paths_by_ref
        candidate_path = paths_by_ref[case.terminal_ref]
        assert candidate_path["terminal_type"] == case.terminal_type
        assert candidate_path["event_compatibility"] == [case.expected_event_type]
        assert candidate_path["artifact_ref"] == expected_artifact_hash
        assert [edge["relation"] for edge in candidate_path["relationship_path"]["edges"]] == list(
            case.path_relations
        )
