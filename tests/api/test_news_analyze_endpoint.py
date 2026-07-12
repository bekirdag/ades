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


def _build_uk_mining_sector_graph(tmp_path: Path) -> tuple[str, str]:
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
    return graph.artifact_path, graph.artifact_hash


def _build_uk_direct_sector_story_graph(tmp_path: Path) -> tuple[str, str]:
    node_path = tmp_path / "uk_direct_sector_impact_nodes.tsv"
    edge_path = tmp_path / "uk_direct_sector_impact_edges.tsv"
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
                (
                    "finance-uk-sector:mining\tUK mining sector\tsector\tfinance-uk-en\t"
                    '0\t1\t{"jurisdiction":"GB"}\tfinance-uk-en'
                ),
                (
                    "finance-uk-issuer:beowulf-mining\tBeowulf Mining PLC\tissuer\t"
                    'finance-uk-en\t0\t1\t{"jurisdiction":"GB"}\tfinance-uk-en'
                ),
                (
                    "finance-uk-issuer:greatland-gold\tGreatland Gold PLC\tissuer\t"
                    'finance-uk-en\t0\t1\t{"jurisdiction":"GB"}\tfinance-uk-en'
                ),
                (
                    "ades:security:gb:lse:bem-ordinary-share\t"
                    "Beowulf Mining PLC ordinary share\tsecurity\tfinance-uk-en\t1\t0\t"
                    '{"jurisdiction":"GB","exchange":"LSE","isin":"GB0033163287"}\t'
                    "finance-uk-en"
                ),
                (
                    "finance-uk-ticker:bem\tBEM.L\tticker\tfinance-uk-en\t1\t0\t"
                    '{"jurisdiction":"GB","exchange":"LSE","ticker_symbol":"BEM.L"}\t'
                    "finance-uk-en"
                ),
                (
                    "finance-uk-ticker:ggp\tGGP.L\tticker\tfinance-uk-en\t1\t0\t"
                    '{"jurisdiction":"GB","exchange":"LSE","ticker_symbol":"GGP.L"}\t'
                    "finance-uk-en"
                ),
                (
                    "ades:impact:index:uk-mining-index\tUK Mining Index\tindex\t"
                    'finance-uk-en\t1\t0\t{"jurisdiction":"GB"}\tfinance-uk-en'
                ),
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
        + "\n".join(
            [
                (
                    "finance-uk-sector:mining\tfinance-uk-issuer:beowulf-mining\t"
                    "sector_affects_issuer\tdirect\t0.91\tsector_membership\t"
                    "London Stock Exchange issuer sector profile\t"
                    "https://www.londonstockexchange.com/stock/BEM/beowulf-mining-plc/company-page\t"
                    "2026-07-01\t2026\tannual\tfinance-uk-en\t"
                    "reviewed direct sector issuer basket fixture\t"
                    "sector_policy_change\tdirect_issuer_or_sector_membership_evidence"
                ),
                (
                    "finance-uk-sector:mining\tfinance-uk-issuer:greatland-gold\t"
                    "sector_affects_issuer\tdirect\t0.9\tsector_membership\t"
                    "London Stock Exchange issuer sector profile\t"
                    "https://www.londonstockexchange.com/stock/GGP/greatland-gold-plc/company-page\t"
                    "2026-07-01\t2026\tannual\tfinance-uk-en\t"
                    "reviewed direct sector issuer basket fixture\t"
                    "sector_policy_change\tdirect_issuer_or_sector_membership_evidence"
                ),
                (
                    "finance-uk-issuer:beowulf-mining\t"
                    "ades:security:gb:lse:bem-ordinary-share\t"
                    "issuer_has_security\tdirect\t0.96\tlisted_equity\t"
                    "London Stock Exchange Beowulf Mining company page\t"
                    "https://www.londonstockexchange.com/stock/BEM/beowulf-mining-plc/company-page\t"
                    "2026-07-01\t2026\tannual\tfinance-uk-en\t"
                    "reviewed direct sector security fixture\t"
                    "sector_policy_change\tdirect_issuer_or_security_mention"
                ),
                (
                    "finance-uk-issuer:beowulf-mining\tfinance-uk-ticker:bem\t"
                    "issuer_has_listed_ticker\tdirect\t0.96\tlisted_equity\t"
                    "London Stock Exchange Beowulf Mining company page\t"
                    "https://www.londonstockexchange.com/stock/BEM/beowulf-mining-plc/company-page\t"
                    "2026-07-01\t2026\tannual\tfinance-uk-en\t"
                    "reviewed direct sector ticker fixture\t"
                    "sector_policy_change\tlisted_issuer"
                ),
                (
                    "finance-uk-issuer:greatland-gold\tfinance-uk-ticker:ggp\t"
                    "issuer_has_listed_ticker\tdirect\t0.95\tlisted_equity\t"
                    "London Stock Exchange Greatland Gold company page\t"
                    "https://www.londonstockexchange.com/stock/GGP/greatland-gold-plc/company-page\t"
                    "2026-07-01\t2026\tannual\tfinance-uk-en\t"
                    "reviewed direct sector ticker fixture\t"
                    "sector_policy_change\tlisted_issuer"
                ),
                (
                    "finance-uk-sector:mining\tades:impact:index:uk-mining-index\t"
                    "sector_affects_index\tdirect\t0.89\tsector_index\t"
                    "FTSE Russell UK mining index factsheet\t"
                    "https://www.lseg.com/en/ftse-russell/indices\t"
                    "2026-07-01\t2026\tannual\tfinance-uk-en\t"
                    "reviewed direct sector index fixture\t"
                    "sector_policy_change\tdirect_issuer_or_sector_membership_evidence"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    graph = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "uk-direct-sector-graph-artifact",
        artifact_version="2026-07-01Tuk-direct-sector-story-test",
    )
    return graph.artifact_path, graph.artifact_hash


def _build_uk_housebuilder_legal_graph(tmp_path: Path) -> tuple[str, str]:
    node_path = tmp_path / "uk_housebuilder_impact_nodes.tsv"
    edge_path = tmp_path / "uk_housebuilder_impact_edges.tsv"
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
                (
                    "ades:regulator:gb:cma\tCompetition and Markets Authority\t"
                    'regulator\tfinance-uk-en\t0\t1\t{"jurisdiction":"GB"}\tfinance-uk-en'
                ),
                (
                    "ades:sector:gb:homebuilders\tUK homebuilders\tsector\t"
                    'finance-uk-en\t0\t1\t{"jurisdiction":"GB"}\tfinance-uk-en'
                ),
                (
                    "finance-uk-issuer:00296805\tTaylor Wimpey PLC\tissuer\t"
                    'finance-uk-en\t0\t1\t{"jurisdiction":"GB","isin":"GB0008782301"}\t'
                    "finance-uk-en"
                ),
                (
                    "ades:security:gb:lse:tw-ordinary-share\t"
                    "Taylor Wimpey PLC ordinary share\tsecurity\tfinance-uk-en\t1\t0\t"
                    '{"jurisdiction":"GB","exchange":"LSE","isin":"GB0008782301"}\t'
                    "finance-uk-en"
                ),
                (
                    "finance-uk-ticker:tw\tTW.L\tticker\tfinance-uk-en\t1\t0\t"
                    '{"jurisdiction":"GB","exchange":"LSE","ticker_symbol":"TW.L"}\t'
                    "finance-uk-en"
                ),
                (
                    "ades:impact:currency:gbp\tPound sterling\tcurrency\t"
                    'finance-uk-en\t1\t0\t{"jurisdiction":"GB"}\tfinance-uk-en'
                ),
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
        + "\n".join(
            [
                (
                    "ades:regulator:gb:cma\tades:sector:gb:homebuilders\t"
                    "regulator_affects_sector\tdirect\t0.92\tregulatory_action\t"
                    "Competition and Markets Authority housebuilding action\t"
                    "https://www.gov.uk/government/organisations/competition-and-markets-authority\t"
                    "2026-06-30\t2026\tannual\tfinance-uk-en\t"
                    "reviewed UK housebuilder legal-sector fixture\t"
                    "regulatory_enforcement\tjurisdiction_or_regulator_context"
                ),
                (
                    "ades:sector:gb:homebuilders\tfinance-uk-issuer:00296805\t"
                    "sector_affects_issuer\tdirect\t0.91\tlegal_action\t"
                    "Taylor Wimpey annual report\t"
                    "https://www.taylorwimpey.co.uk/corporate/investors/results-reports-and-presentations\t"
                    "2026-06-30\t2026\tannual\tfinance-uk-en\t"
                    "reviewed UK homebuilder sector-to-issuer exposure fixture\t"
                    "regulatory_enforcement\tdirect_issuer_or_sector_membership_evidence"
                ),
                (
                    "finance-uk-issuer:00296805\tades:security:gb:lse:tw-ordinary-share\t"
                    "issuer_has_security\tdirect\t0.96\tlisted_equity\t"
                    "London Stock Exchange Taylor Wimpey company page\t"
                    "https://www.londonstockexchange.com/stock/TW./taylor-wimpey-plc/company-page\t"
                    "2026-06-30\t2026\tannual\tfinance-uk-en\t"
                    "reviewed issuer security fixture\t"
                    "regulatory_enforcement\tdirect_issuer_or_security_mention"
                ),
                (
                    "finance-uk-issuer:00296805\tfinance-uk-ticker:tw\t"
                    "issuer_has_listed_ticker\tdirect\t0.96\tlisted_equity\t"
                    "London Stock Exchange Taylor Wimpey company page\t"
                    "https://www.londonstockexchange.com/stock/TW./taylor-wimpey-plc/company-page\t"
                    "2026-06-30\t2026\tannual\tfinance-uk-en\t"
                    "reviewed issuer ticker fixture\t"
                    "regulatory_enforcement\tlisted_issuer"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    graph = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "uk-housebuilder-graph-artifact",
        artifact_version="2026-06-30Tuk-housebuilder-legal-test",
    )
    return graph.artifact_path, graph.artifact_hash


def _build_us_policy_terminal_graph(tmp_path: Path) -> tuple[str, str]:
    node_path = tmp_path / "policy_impact_nodes.tsv"
    edge_path = tmp_path / "policy_impact_edges.tsv"
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
                (
                    "ades:us-law:chips-act\tCHIPS Act\tlaw\tfinance-us-en\t"
                    '0\t1\t{"jurisdiction":"us"}\tfinance-us-en'
                ),
                (
                    "ades:us-government-body:commerce-department\tUS Commerce Department\t"
                    'government_body\tfinance-us-en\t0\t1\t{"jurisdiction":"us"}\t'
                    "finance-us-en"
                ),
                (
                    "ades:us-ministry:commerce\tUS Commerce Ministry\tministry\t"
                    'finance-us-en\t0\t1\t{"jurisdiction":"us"}\tfinance-us-en'
                ),
                (
                    "ades:us-policy:chip-subsidy-policy\tUS Chip Subsidy Policy\tpolicy\t"
                    'finance-us-en\t0\t1\t{"jurisdiction":"us"}\tfinance-us-en'
                ),
                (
                    "ades:us-regulation:chip-export-rule\tUS Chip Export Rule\t"
                    'regulation\tfinance-us-en\t0\t1\t{"jurisdiction":"us"}\t'
                    "finance-us-en"
                ),
                (
                    "ades:us-regulator:ftc\tFederal Trade Commission\tregulator\t"
                    'finance-us-en\t0\t1\t{"jurisdiction":"us"}\tfinance-us-en'
                ),
                (
                    "ades:us-court:district-court\tUS District Court\tgovernment_body\t"
                    'finance-us-en\t0\t1\t{"jurisdiction":"us"}\tfinance-us-en'
                ),
                (
                    "ades:sector:semiconductors\tSemiconductors\tsector\t"
                    'finance-us-en\t0\t1\t{"jurisdiction":"us"}\tfinance-us-en'
                ),
                (
                    "finance-us-issuer:example-semiconductor\tExample Semiconductor Inc\t"
                    'issuer\tfinance-us-en\t1\t0\t{"jurisdiction":"us"}\tfinance-us-en'
                ),
                (
                    "finance-us-ticker:nasdaq:xchp\tXCHP\tticker\t"
                    "finance-us-en\t1\t0\t{}\tfinance-us-en"
                ),
                (
                    "ades:impact:index:us-semiconductor-index\tUS Semiconductor Index\t"
                    'index\tfinance-us-en\t1\t0\t{"jurisdiction":"us"}\tfinance-us-en'
                ),
                (
                    "ades:impact:currency:usd\tUS dollar\tcurrency\t"
                    'finance-us-en\t1\t0\t{"jurisdiction":"us"}\tfinance-us-en'
                ),
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
        + "\n".join(
            [
                (
                    "ades:us-law:chips-act\tades:sector:semiconductors\t"
                    "law_affects_sector\tdirect\t0.93\tpolicy_risk\t"
                    "Congress CHIPS Act\t"
                    "https://www.congress.gov/bill/117th-congress/house-bill/4346\t"
                    "2026-06-30\t2026\tannual\tfinance-us-en\t"
                    "reviewed law-to-sector policy exposure fixture\t"
                    "sector_policy_change\tsector_policy_event_signal"
                ),
                (
                    "ades:us-government-body:commerce-department\t"
                    "ades:sector:semiconductors\tgovernment_body_affects_sector\t"
                    "direct\t0.92\tpolicy_risk\tUS Commerce Department\t"
                    "https://www.commerce.gov/\t2026-06-30\t2026\tannual\t"
                    "finance-us-en\treviewed government-body sector fixture\t"
                    "sector_policy_change\tsector_policy_event_signal"
                ),
                (
                    "ades:us-ministry:commerce\tades:sector:semiconductors\t"
                    "government_body_affects_sector\tdirect\t0.92\tpolicy_risk\t"
                    "US Commerce Ministry\thttps://www.commerce.gov/\t"
                    "2026-06-30\t2026\tannual\tfinance-us-en\t"
                    "reviewed ministry sector fixture\t"
                    "sector_policy_change\tsector_policy_event_signal"
                ),
                (
                    "ades:us-policy:chip-subsidy-policy\tades:sector:semiconductors\t"
                    "policy_body_affects_sector\tdirect\t0.9\tpolicy_risk\t"
                    "White House industrial policy\t"
                    "https://www.whitehouse.gov/briefing-room/\t2026-06-30\t"
                    "2026\tannual\tfinance-us-en\treviewed policy sector fixture\t"
                    "sector_policy_change\tsector_policy_event_signal"
                ),
                (
                    "ades:us-regulation:chip-export-rule\tades:sector:semiconductors\t"
                    "regulation_affects_sector\tdirect\t0.91\tpolicy_risk\t"
                    "SEC semiconductor disclosure rule\thttps://www.sec.gov/rules\t"
                    "2026-06-30\t2026\tannual\tfinance-us-en\t"
                    "reviewed regulation sector fixture\t"
                    "sector_policy_change\tsector_policy_event_signal"
                ),
                (
                    "ades:us-regulator:ftc\tades:sector:semiconductors\t"
                    "regulator_affects_sector\tdirect\t0.92\tregulatory_action\t"
                    "Federal Trade Commission technology competition action\t"
                    "https://www.ftc.gov/news-events\t2026-06-30\t2026\tannual\t"
                    "finance-us-en\treviewed regulator enforcement sector fixture\t"
                    "regulatory_enforcement\tjurisdiction_or_regulator_context"
                ),
                (
                    "ades:us-court:district-court\tades:sector:semiconductors\t"
                    "government_body_affects_sector\tdirect\t0.9\tlegal_action\t"
                    "United States District Court antitrust docket\t"
                    "https://www.uscourts.gov/\t2026-06-30\t2026\tannual\t"
                    "finance-us-en\treviewed court lawsuit sector fixture\t"
                    "regulatory_enforcement\tjurisdiction_or_regulator_context"
                ),
                (
                    "ades:sector:semiconductors\tfinance-us-issuer:example-semiconductor\t"
                    "sector_affects_issuer\tdirect\t0.91\tpolicy_risk\t"
                    "SEC EDGAR issuer filings\thttps://www.sec.gov/edgar\t"
                    "2026-06-30\t2026\tannual\tfinance-us-en\t"
                    "reviewed sector-to-issuer exposure fixture\t"
                    "sector_policy_change;regulatory_enforcement\t"
                    "direct_issuer_or_sector_membership_evidence"
                ),
                (
                    "finance-us-issuer:example-semiconductor\tfinance-us-ticker:nasdaq:xchp\t"
                    "issuer_has_listed_ticker\tdirect\t0.96\tlisted_equity\t"
                    "Nasdaq listed company directory\thttps://www.nasdaq.com/market-activity/stocks/xchp\t"
                    "2026-06-30\t2026\tannual\tfinance-us-en\t"
                    "reviewed issuer ticker fixture\t"
                    "sector_policy_change;regulatory_enforcement\tlisted_issuer"
                ),
                (
                    "ades:sector:semiconductors\tades:impact:index:us-semiconductor-index\t"
                    "sector_affects_index\tdirect\t0.89\tsector_index\t"
                    "Nasdaq indexes\thttps://www.nasdaq.com/solutions/indexes\t"
                    "2026-06-30\t2026\tannual\tfinance-us-en\t"
                    "reviewed sector index fixture\t"
                    "sector_policy_change;regulatory_enforcement\t"
                    "direct_issuer_or_sector_membership_evidence"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    graph = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "policy-graph-artifact",
        artifact_version="2026-06-30Tpolicy-path-test",
    )
    return graph.artifact_path, graph.artifact_hash


def _build_direct_issuer_security_graph(tmp_path: Path) -> tuple[str, str]:
    node_path = tmp_path / "direct_issuer_security_nodes.tsv"
    edge_path = tmp_path / "direct_issuer_security_edges.tsv"
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
                (
                    "finance-us-issuer:000EXM\tExample Manufacturing Inc\tissuer\t"
                    'finance-us-en\t0\t1\t{"jurisdiction":"us","cik":"000EXM"}\t'
                    "finance-us-en"
                ),
                (
                    "ades:security:us:nasdaq:exm-common-stock\t"
                    "Example Manufacturing Inc common stock\tsecurity\t"
                    "finance-us-en\t1\t0\t"
                    '{"jurisdiction":"us","exchange":"nasdaq","ticker_symbol":"EXM",'
                    '"local_security_id":"exm-common-stock","cik":"000EXM"}\t'
                    "finance-us-en"
                ),
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
            "finance-us-issuer:000EXM\tades:security:us:nasdaq:exm-common-stock\t"
            "issuer_has_security\tdirect\t0.97\tlisted_equity\t"
            "Nasdaq listed company directory\t"
            "https://www.nasdaq.com/market-activity/stocks/exm\t"
            "2026-06-30\t2026\tannual\tfinance-us-en\t"
            "reviewed direct issuer security golden fixture\t"
            "earnings_beat;earnings_miss;guidance_raise;guidance_cut\t"
            "direct_issuer_or_security_mention\n"
        ),
        encoding="utf-8",
    )
    graph = build_market_graph_store(
        node_tsv_paths=[node_path],
        edge_tsv_paths=[edge_path],
        output_dir=tmp_path / "direct-issuer-security-graph-artifact",
        artifact_version="2026-07-01Tdirect-issuer-security-golden",
    )
    return graph.artifact_path, graph.artifact_hash


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
    assert payload["debug"] is None

    debug_text = (
        "Iran said oil shipping lanes near the Strait of Hormuz were disrupted "
        "while $50 was cited " + "by market desk analysts " * 30
    )
    debug_response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Energy shipping risk rises",
            "text": debug_text,
            "hints": {"country": "ir", "topics": ["finance"]},
            "source": {"publisher": "Example", "source_country": "IR"},
            "packs": [pack_id],
            "options": {
                "debug": True,
                "include_passive_entities": True,
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "max_passive_entities": 32,
                "max_terminal_candidates": 8,
            },
        },
    )

    assert debug_response.status_code == 200
    debug_payload = debug_response.json()
    debug = debug_payload["debug"]
    assert debug["enabled"] is True
    assert debug["candidate_path_count"] == len(debug_payload["candidate_paths"])
    assert debug["rejected_candidate_count"] == len(debug_payload["rejected_candidates"])
    assert debug["unresolved_entity_count"] == len(debug_payload["unresolved_entities"])
    assert set(debug["entity_refs"]) == {"country:ir", "entity_hormuz"}
    assert debug["graph_enabled_packs"] == [pack_id]
    assert debug["paths"][0] == {
        "terminal_ref": "entity_crude_oil",
        "terminal_name": "Crude oil",
        "terminal_type": "commodity",
        "source_entity_refs": ["entity_hormuz"],
        "relationship_depth": 1,
        "edge_refs": ["entity_hormuz->chokepoint_affects_energy->entity_crude_oil"],
        "source_tiers": ["test_fixture"],
        "source_names": ["test"],
        "source_urls": ["https://example.com"],
        "source_snapshots": ["test"],
        "artifact_ref": "sha256:test",
    }
    assert debug["rejected_candidates"][0]["terminal_ref"] == ("ades:impact:rates:policy-rate")
    assert debug["rejected_candidates"][0]["reason_code"] == "event_incompatible"
    debug_unresolved_by_ref = {
        entity["entity_ref"]: entity for entity in debug["unresolved_entities"]
    }
    assert debug_unresolved_by_ref["country:ir"]["source_lane_suggestion"] == ("finance-ir-en")
    assert debug_unresolved_by_ref["country:ir"]["replay_key"].startswith("unresolved-entity:")
    assert debug["limits"]["source_evidence_chars"] == 240
    assert all(len(item["text"]) <= 240 for item in debug["source_evidence"])
    assert any(
        item["evidence_type"] == "event_sentence" and item["text"].endswith("...")
        for item in debug["source_evidence"]
    )
    assert any(
        item["evidence_type"] == "path_source" and item["source_url"] == "https://example.com"
        for item in debug["source_evidence"]
    )
    assert debug_payload["tag_responses"] == []


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


def test_news_analyze_drops_unanchored_direct_ticker_entity_refs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(
        tmp_path,
        "news-unanchored-direct-ticker-en",
        domain="finance",
    )
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
                    text="BBBY",
                    label="ticker",
                    start=0,
                    end=4,
                    confidence=0.93,
                    relevance=0.95,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="finance",
                    ),
                    link=EntityLink(
                        entity_id="finance-us-ticker:BBBY",
                        canonical_text="Bed Bath & Beyond",
                        provider="ades",
                    ),
                )
            ],
            topics=[TopicMatch(label="politics", score=0.82, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(*_: object, **__: object) -> ImpactExpansionResult:
        raise AssertionError("unanchored direct ticker must not seed impact expansion")

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Pentagon labor talks continue",
            "text": "Pentagon labor talks continued without a named listed company.",
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
    assert payload["terminal_impact_candidates"] == []
    expected_warning = (
        "ADES_NEWS_ANALYZE_DROPPED_UNANCHORED_DIRECT_TERMINAL:finance-us-ticker:BBBY"
    )
    assert any(warning == expected_warning for warning in payload["warnings"])


def test_news_analyze_drops_direct_ticker_from_photo_credit_only(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(
        tmp_path,
        "news-photo-credit-direct-ticker-en",
        domain="finance",
    )
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        surface = "Getty Images"
        start = text.index(surface)
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text=surface,
                    label="ticker",
                    start=start,
                    end=start + len(surface),
                    confidence=0.93,
                    relevance=0.95,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="finance",
                    ),
                    link=EntityLink(
                        entity_id="finance-us-ticker:GETY",
                        canonical_text="Getty Images Holdings",
                        provider="ades",
                    ),
                )
            ],
            topics=[TopicMatch(label="economy", score=0.82, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(*_: object, **__: object) -> ImpactExpansionResult:
        raise AssertionError("photo-credit direct ticker must not seed impact expansion")

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Italian GDP growth was revised higher",
            "text": "Italian GDP growth was revised higher by statistics officials. Photo: Getty Images.",
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
    assert payload["terminal_impact_candidates"] == []
    expected_warning = (
        "ADES_NEWS_ANALYZE_DROPPED_UNANCHORED_DIRECT_TERMINAL:finance-us-ticker:GETY"
    )
    assert any(warning == expected_warning for warning in payload["warnings"])


def test_news_analyze_drops_photo_credit_same_as_terminal_candidate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(
        tmp_path,
        "news-photo-credit-same-as-terminal-en",
        domain="finance",
    )
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        surface = "Getty Images"
        start = text.index(surface)
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text=surface,
                    label="organization",
                    start=start,
                    end=start + len(surface),
                    confidence=0.93,
                    relevance=0.95,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="finance",
                    ),
                    link=EntityLink(
                        entity_id="wikidata:QGETTY",
                        canonical_text="Getty Images Holdings",
                        provider="ades",
                    ),
                )
            ],
            topics=[TopicMatch(label="economy", score=0.82, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "wikidata:QGETTY" in set(entity_refs)
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-07-10",
            artifact_hash="sha256:getty-same-as",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="wikidata:QGETTY",
                    name="Getty Images Holdings",
                    same_as_refs=["finance-us-ticker:GETY"],
                    entity_type="organization",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=False,
                )
            ],
            candidates=[
                ImpactCandidate(
                    entity_ref="finance-us-ticker:GETY",
                    name="Getty Images Holdings",
                    entity_type="ticker",
                    evidence_level="direct",
                    confidence=0.91,
                    source_entity_refs=["wikidata:QGETTY"],
                    relationship_paths=[],
                )
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Italian GDP growth was revised higher",
            "text": "Italian GDP growth was revised higher by statistics officials. Photo: Getty Images.",
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
    assert payload["terminal_impact_candidates"] == []
    expected_warning = (
        "ADES_NEWS_ANALYZE_DROPPED_UNANCHORED_DIRECT_TERMINAL:finance-us-ticker:GETY"
    )
    assert any(warning == expected_warning for warning in payload["warnings"])


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


def test_news_analyze_direct_issuer_story_reaches_listed_security(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for pack_id, domain in (
        ("general-en", "general"),
        ("finance-en", "finance"),
        ("finance-us-en", "finance"),
    ):
        _install_named_pack(tmp_path, pack_id, domain=domain)
    graph_artifact_path, graph_artifact_hash = _build_direct_issuer_security_graph(tmp_path)

    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", graph_artifact_path)
    client = TestClient(create_app(storage_root=tmp_path))
    issuer_text = "Example Manufacturing"

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        entities = []
        if pack == "finance-us-en":
            start = text.index(issuer_text)
            entities.append(
                EntityMatch(
                    text=issuer_text,
                    label="issuer",
                    start=start,
                    end=start + len(issuer_text),
                    confidence=0.95,
                    relevance=0.96,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack,
                        source_domain="finance",
                    ),
                    link=EntityLink(
                        entity_id="finance-us-issuer:000EXM",
                        canonical_text="Example Manufacturing Inc",
                        provider="ades",
                    ),
                )
            )
        return TagResponse(
            version="0.1.0",
            pack=pack or "unknown",
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=entities,
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Example Manufacturing beats earnings estimates",
            "text": (
                "Example Manufacturing beat earnings estimates and said demand "
                "for its listed shares remained strong."
            ),
            "source": {"source_country": "US"},
            "options": {
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "impact_max_depth": 2,
                "max_country_finance_packs": 1,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["event_signal"]["event_type"] == "earnings_beat"
    assert any(
        source["entity_ref"] == "finance-us-issuer:000EXM"
        and source["is_graph_seed"] is True
        for source in payload["source_entities"]
    )

    terminal_candidates_by_ref = {
        candidate["entity_ref"]: candidate
        for candidate in payload["terminal_impact_candidates"]
    }
    security_ref = "ades:security:us:nasdaq:exm-common-stock"
    assert security_ref in terminal_candidates_by_ref
    security_candidate = terminal_candidates_by_ref[security_ref]
    assert security_candidate["entity_type"] == "security"
    assert security_candidate["compatible_event_types"] == ["earnings_beat"]
    assert security_candidate["source_entity_refs"] == ["finance-us-issuer:000EXM"]
    security_candidate_path = security_candidate["relationship_paths"][0]
    assert [edge["relation"] for edge in security_candidate_path["edges"]] == [
        "issuer_has_security"
    ]
    assert security_candidate_path["edges"][0]["source_tier"] == "exchange"
    assert security_candidate_path["edges"][0]["source_url"] == (
        "https://www.nasdaq.com/market-activity/stocks/exm"
    )

    candidate_paths_by_ref = {
        candidate_path["terminal_ref"]: candidate_path
        for candidate_path in payload["candidate_paths"]
    }
    security_path = candidate_paths_by_ref[security_ref]
    assert security_path["terminal_type"] == "security"
    assert security_path["terminal_name"] == "Example Manufacturing Inc common stock"
    assert security_path["jurisdiction"] == "us"
    assert security_path["exchange"] == "nasdaq"
    assert security_path["ticker"] is None
    assert security_path["security_ids"] == {
        "ades_ref": security_ref,
        "local_security_id": "exm-common-stock",
    }
    assert security_path["source_tiers"] == ["exchange"]
    assert security_path["event_compatibility"] == ["earnings_beat"]
    assert security_path["artifact_ref"] == graph_artifact_hash
    assert security_path["weakest_edge_ref"] == (
        "finance-us-issuer:000EXM->issuer_has_security->"
        "ades:security:us:nasdaq:exm-common-stock"
    )
    assert security_path["weakest_edge_confidence"] == 0.97
    assert [edge["relation"] for edge in security_path["relationship_path"]["edges"]] == [
        "issuer_has_security"
    ]
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


def test_news_analyze_maps_program_mentions_to_operator_holding_security_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-program-terminal-en", domain="finance")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))
    article_text = (
        "The Ultra Micro program beat earnings estimates as it expanded lending "
        "for small businesses."
    )

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        start = article_text.index("Ultra Micro program")
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text="Ultra Micro program",
                    label="program",
                    start=start,
                    end=start + len("Ultra Micro program"),
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
                        entity_id="ades:program:id:ultra-micro-program",
                        canonical_text="Ultra Micro Program",
                        provider="ades",
                    ),
                )
            ],
            topics=[TopicMatch(label="finance", score=0.91, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _edge(
        source_ref: str,
        target_ref: str,
        relation: str,
        *,
        confidence: float,
        source_name: str,
    ) -> ImpactPathEdge:
        return ImpactPathEdge(
            source_ref=source_ref,
            target_ref=target_ref,
            relation=relation,
            evidence_level="direct",
            confidence=confidence,
            direction_hint="source_backed_program_terminal_path",
            source_name=source_name,
            source_url="https://example.test/program-terminal-path",
            source_snapshot="2026-06-30",
            source_year=2026,
            source_tier="issuer_disclosed",
            effective_from="2024-01-01",
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "ades:program:id:ultra-micro-program" in set(entity_refs)
        program_path = ImpactRelationshipPath(
            path_depth=4,
            edges=[
                _edge(
                    "ades:program:id:ultra-micro-program",
                    "finance-id-org:permodalan-nasional-madani",
                    "program_operated_by_org",
                    confidence=0.94,
                    source_name="PNM annual report",
                ),
                _edge(
                    "finance-id-org:permodalan-nasional-madani",
                    "ades:holding:id:ultra-micro-holding",
                    "org_part_of_holding",
                    confidence=0.93,
                    source_name="PNM holding disclosure",
                ),
                _edge(
                    "ades:holding:id:ultra-micro-holding",
                    "finance-id-issuer:bank-rakyat-indonesia",
                    "holding_parent_is_issuer",
                    confidence=0.92,
                    source_name="BRI annual report",
                ),
                _edge(
                    "finance-id-issuer:bank-rakyat-indonesia",
                    "ades:security:id:idx:bbri",
                    "issuer_has_security",
                    confidence=0.97,
                    source_name="Indonesia Stock Exchange issuer directory",
                ),
            ],
        )
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-06-30",
            artifact_hash="sha256:program-terminals",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="ades:program:id:ultra-micro-program",
                    name="Ultra Micro Program",
                    entity_type="program",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=False,
                )
            ],
            candidates=[
                ImpactCandidate(
                    entity_ref="ades:security:id:idx:bbri",
                    name="Bank Rakyat Indonesia listed share",
                    entity_type="security",
                    evidence_level="direct",
                    confidence=0.9,
                    source_entity_refs=["ades:program:id:ultra-micro-program"],
                    relationship_paths=[program_path],
                    compatible_event_types=["earnings_beat"],
                )
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Ultra Micro program beat earnings estimates",
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
        "ades:security:id:idx:bbri"
    ]
    candidate = payload["terminal_impact_candidates"][0]
    assert candidate["source_entity_refs"] == ["ades:program:id:ultra-micro-program"]
    assert candidate["compatible_event_types"] == ["earnings_beat"]

    assert len(payload["candidate_paths"]) == 1
    path = payload["candidate_paths"][0]
    assert path["terminal_ref"] == "ades:security:id:idx:bbri"
    assert path["terminal_type"] == "security"
    assert path["terminal_name"] == "Bank Rakyat Indonesia listed share"
    assert path["jurisdiction"] == "id"
    assert path["exchange"] == "idx"
    assert path["security_ids"] == {
        "ades_ref": "ades:security:id:idx:bbri",
        "local_security_id": "bbri",
    }
    assert path["source_entity_refs"] == ["ades:program:id:ultra-micro-program"]
    assert path["source_tiers"] == ["issuer_disclosed"]
    assert path["effective_from"] == "2024-01-01"
    assert path["path_confidence"] == 0.9
    assert path["weakest_edge_ref"] == (
        "ades:holding:id:ultra-micro-holding"
        "->holding_parent_is_issuer->finance-id-issuer:bank-rakyat-indonesia"
    )
    assert path["weakest_edge_confidence"] == 0.92
    assert [edge["relation"] for edge in path["relationship_path"]["edges"]] == [
        "program_operated_by_org",
        "org_part_of_holding",
        "holding_parent_is_issuer",
        "issuer_has_security",
    ]
    for edge in path["relationship_path"]["edges"]:
        assert edge["evidence_level"] == "direct"
        assert edge["source_tier"] == "issuer_disclosed"
        assert edge["source_url"] == "https://example.test/program-terminal-path"
        assert edge["effective_from"] == "2024-01-01"
    assert path["artifact_ref"] == "sha256:program-terminals"
    assert "NO_TERMINAL_IMPACT_CANDIDATES" not in payload["quality_flags"]


def test_news_analyze_returns_commodity_supply_chain_terminal_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(
        tmp_path,
        "news-commodity-supply-terminal-en",
        domain="finance",
    )
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))
    article_text = (
        "A lithium supply disruption hit the battery supply chain and automakers "
        "warned production could be cut."
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
        source_tier: str,
        source_name: str,
        source_url: str,
    ) -> ImpactPathEdge:
        return ImpactPathEdge(
            source_ref=source_ref,
            target_ref=target_ref,
            relation=relation,
            evidence_level="direct",
            confidence=confidence,
            direction_hint="source_backed_commodity_supply_chain_path",
            source_name=source_name,
            source_url=source_url,
            source_snapshot="2026-06-30",
            source_year=2026,
            source_tier=source_tier,
            effective_from="2025-01-01",
            compatible_event_types=["supply_disruption"],
            direction_preconditions=["source_backed_relationship_evidence"],
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
                    "lithium",
                    "commodity",
                    "ades:impact:commodity:lithium",
                    "Lithium",
                ),
                _entity(
                    "battery supply chain",
                    "supply_chain",
                    "ades:impact:supply-chain:battery-materials",
                    "Battery materials supply chain",
                ),
            ],
            topics=[TopicMatch(label="finance", score=0.92, evidence_count=2)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert {
            "ades:impact:commodity:lithium",
            "ades:impact:supply-chain:battery-materials",
        }.issubset(set(entity_refs))
        supply_to_lithium = _edge(
            "ades:impact:supply-chain:battery-materials",
            "ades:impact:commodity:lithium",
            "commodity_flow_affects_commodity",
            confidence=0.95,
            source_tier="industry_association",
            source_name="International Lithium Association supply-chain review",
            source_url="https://example.test/lithium-supply-review",
        )
        lithium_to_sector = _edge(
            "ades:impact:commodity:lithium",
            "ades:impact:sector:ev-batteries",
            "battery_metal_affects_sector",
            confidence=0.93,
            source_tier="industry_association",
            source_name="Battery Materials Industry Association",
            source_url="https://example.test/battery-materials-sector",
        )
        sector_to_issuer = _edge(
            "ades:impact:sector:ev-batteries",
            "finance-us-issuer:ev-battery-technologies",
            "sector_affects_issuer",
            confidence=0.9,
            source_tier="issuer_disclosed",
            source_name="EV Battery Technologies annual report",
            source_url="https://example.test/evbt-annual-report",
        )
        issuer_to_ticker = _edge(
            "finance-us-issuer:ev-battery-technologies",
            "finance-us-ticker:NASDAQ:EVBT",
            "issuer_has_listed_ticker",
            confidence=0.97,
            source_tier="exchange",
            source_name="NASDAQ issuer directory",
            source_url="https://example.test/nasdaq-evbt",
        )
        commodity_path = ImpactRelationshipPath(
            path_depth=1,
            edges=[supply_to_lithium],
        )
        sector_path = ImpactRelationshipPath(
            path_depth=2,
            edges=[supply_to_lithium, lithium_to_sector],
        )
        issuer_path = ImpactRelationshipPath(
            path_depth=4,
            edges=[
                supply_to_lithium,
                lithium_to_sector,
                sector_to_issuer,
                issuer_to_ticker,
            ],
        )
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-06-30",
            artifact_hash="sha256:commodity-supply-terminals",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="ades:impact:commodity:lithium",
                    name="Lithium",
                    entity_type="commodity",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=False,
                ),
                ImpactSourceEntity(
                    entity_ref="ades:impact:supply-chain:battery-materials",
                    name="Battery materials supply chain",
                    entity_type="supply_chain",
                    is_graph_seed=True,
                    seed_degree=0,
                    is_tradable=False,
                ),
            ],
            candidates=[
                ImpactCandidate(
                    entity_ref="ades:impact:commodity:lithium",
                    name="Lithium",
                    entity_type="commodity",
                    evidence_level="shallow",
                    confidence=0.94,
                    source_entity_refs=["ades:impact:supply-chain:battery-materials"],
                    relationship_paths=[commodity_path],
                ),
                ImpactCandidate(
                    entity_ref="ades:impact:sector:ev-batteries",
                    name="EV battery sector",
                    entity_type="sector",
                    evidence_level="shallow",
                    confidence=0.91,
                    source_entity_refs=[
                        "ades:impact:commodity:lithium",
                        "ades:impact:supply-chain:battery-materials",
                    ],
                    relationship_paths=[sector_path],
                ),
                ImpactCandidate(
                    entity_ref="finance-us-ticker:NASDAQ:EVBT",
                    name="EV Battery Technologies",
                    entity_type="ticker",
                    evidence_level="direct",
                    confidence=0.89,
                    source_entity_refs=[
                        "ades:impact:commodity:lithium",
                        "ades:impact:supply-chain:battery-materials",
                    ],
                    relationship_paths=[issuer_path],
                ),
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Lithium supply disruption hits battery supply chain",
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
    assert "supply_disruption" in {signal["event_type"] for signal in payload["event_signals"]}
    candidates_by_ref = {
        candidate["entity_ref"]: candidate for candidate in payload["terminal_impact_candidates"]
    }
    assert set(candidates_by_ref) == {
        "ades:impact:commodity:lithium",
        "ades:impact:sector:ev-batteries",
        "finance-us-ticker:NASDAQ:EVBT",
    }
    for candidate in candidates_by_ref.values():
        assert candidate["compatible_event_types"] == ["supply_disruption"]

    paths_by_ref = {
        candidate_path["terminal_ref"]: candidate_path
        for candidate_path in payload["candidate_paths"]
    }
    commodity_path = paths_by_ref["ades:impact:commodity:lithium"]
    assert commodity_path["terminal_type"] == "commodity"
    assert set(commodity_path["source_entity_refs"]) == {
        "ades:impact:commodity:lithium",
        "ades:impact:supply-chain:battery-materials",
    }
    assert commodity_path["source_tiers"] == ["industry_association"]
    assert commodity_path["weakest_edge_ref"] == (
        "ades:impact:supply-chain:battery-materials"
        "->commodity_flow_affects_commodity->ades:impact:commodity:lithium"
    )
    assert commodity_path["weakest_edge"]["source_url"] == (
        "https://example.test/lithium-supply-review"
    )

    sector_path = paths_by_ref["ades:impact:sector:ev-batteries"]
    assert sector_path["terminal_type"] == "sector"
    assert sector_path["source_entity_refs"] == [
        "ades:impact:commodity:lithium",
        "ades:impact:supply-chain:battery-materials",
    ]
    assert [edge["relation"] for edge in sector_path["relationship_path"]["edges"]] == [
        "commodity_flow_affects_commodity",
        "battery_metal_affects_sector",
    ]
    assert sector_path["source_tiers"] == ["industry_association"]

    issuer_path = paths_by_ref["finance-us-ticker:NASDAQ:EVBT"]
    assert issuer_path["terminal_type"] == "ticker"
    assert issuer_path["jurisdiction"] == "us"
    assert issuer_path["exchange"] == "NASDAQ"
    assert issuer_path["ticker"] == "EVBT"
    assert issuer_path["security_ids"] == {
        "ades_ref": "finance-us-ticker:NASDAQ:EVBT",
        "exchange_ticker": "NASDAQ:EVBT",
        "ticker": "EVBT",
    }
    assert issuer_path["source_tiers"] == [
        "industry_association",
        "issuer_disclosed",
        "exchange",
    ]
    assert issuer_path["weakest_edge_ref"] == (
        "ades:impact:sector:ev-batteries"
        "->sector_affects_issuer->finance-us-issuer:ev-battery-technologies"
    )
    assert issuer_path["weakest_edge_confidence"] == 0.9
    assert [edge["relation"] for edge in issuer_path["relationship_path"]["edges"]] == [
        "commodity_flow_affects_commodity",
        "battery_metal_affects_sector",
        "sector_affects_issuer",
        "issuer_has_listed_ticker",
    ]
    for edge in issuer_path["relationship_path"]["edges"]:
        assert edge["evidence_level"] == "direct"
        assert edge["compatible_event_types"] == ["supply_disruption"]
        assert edge["direction_preconditions"] == ["source_backed_relationship_evidence"]
    assert issuer_path["artifact_ref"] == "sha256:commodity-supply-terminals"
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
                _entity("None", "organization"),
                _entity("24 Hours", "organization"),
                _entity("AP Photo/Terry Chea", "person"),
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
                "https://example.com and <a>link</a>. None and 24 Hours appeared "
                "near AP Photo/Terry Chea while the Federal Reserve responded."
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
    assert "none_placeholder" in hidden_reasons_by_name["None"]
    assert "duration_or_time_window" in hidden_reasons_by_name["24 Hours"]
    assert "photo_credit" in hidden_reasons_by_name["AP Photo/Terry Chea"]


def test_news_analyze_marks_generic_finance_alias_passive_ineligible(
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
        surface = "The financial"
        start = text.index(surface)
        return TagResponse(
            version="0.1.0",
            pack=pack or pack_id,
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text=surface,
                    label="organization",
                    start=start,
                    end=start + len(surface),
                    confidence=0.88,
                    relevance=0.83,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="finance",
                    ),
                    link=EntityLink(
                        entity_id="finance-us-issuer:FISI",
                        canonical_text="FINANCIAL INSTITUTIONS INC",
                        provider="ades",
                    ),
                )
            ],
            topics=[TopicMatch(label="finance", score=0.82, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Regional lenders brace for rate volatility",
            "text": (
                "The financial sector prepared for a volatile rate decision, "
                "without naming a listed issuer."
            ),
            "packs": [pack_id],
            "options": {
                "include_passive_entities": True,
                "include_relationship_paths": False,
                "max_passive_entities": 32,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    passive_by_ref = {entity["entity_ref"]: entity for entity in payload["passive_entities"]}
    issuer = passive_by_ref["finance-us-issuer:FISI"]
    assert issuer["name"] == "FINANCIAL INSTITUTIONS INC"
    assert issuer["evidence_text"] == "The financial"
    assert issuer["quality"] == "weak"
    assert issuer["display_eligible"] is False
    assert "unanchored_finance_alias" in issuer["quality_reasons"]


def test_news_analyze_prefers_text_country_over_source_country_hint(
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
            entities=[],
            topics=[TopicMatch(label="politics", score=0.84, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "Iran shipping talks affect Gulf routes",
            "text": (
                "Iran said Strait of Hormuz shipping talks with Oman remained active "
                "after regional security warnings."
            ),
            "source": {"publisher": "Example", "source_country": "TR"},
            "packs": [pack_id],
            "options": {"include_relationship_paths": False},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["country_scope"]["entity_ref"] == "country:ir"
    assert payload["country_scope"]["source"] == "text_country_mention"
    assert payload["country_scope"]["entity_ref"] != "country:tr"


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


def test_news_analyze_blocks_sector_graph_seeds_for_uninstalled_country_pack(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for pack_id, domain in (
        ("general-en", "general"),
        ("finance-en", "finance"),
    ):
        _install_named_pack(tmp_path, pack_id, domain=domain)
    graph_artifact_path, _graph_artifact_hash = _build_uk_mining_sector_graph(tmp_path)

    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", graph_artifact_path)
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
    assert payload["source_entities"] == []
    assert payload["terminal_impact_candidates"] == []
    assert payload["candidate_paths"] == []
    assert "NO_TERMINAL_IMPACT_CANDIDATES" in payload["quality_flags"]
    assert "no_entity_match" in payload["no_terminal_reasons"]
    assert "missing_country_pack" in payload["no_terminal_reasons"]
    assert any(
        diagnostic["code"] == "no_terminal:missing_country_pack"
        for diagnostic in payload["diagnostics"]
    )


def test_news_analyze_returns_stable_no_terminal_reason_codes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "news-no-terminal-reasons-en", domain="finance")
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
                    text="United Kingdom",
                    label="country",
                    start=text.index("United Kingdom"),
                    end=text.index("United Kingdom") + len("United Kingdom"),
                    confidence=0.94,
                    relevance=0.95,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack or pack_id,
                        source_domain="general",
                    ),
                    link=EntityLink(
                        entity_id="country:uk",
                        canonical_text="United Kingdom",
                        provider="ades",
                    ),
                ),
                EntityMatch(
                    text="unverified GBP proxy",
                    label="currency",
                    start=text.index("unverified GBP proxy"),
                    end=text.index("unverified GBP proxy") + len("unverified GBP proxy"),
                    confidence=0.52,
                    relevance=0.51,
                    provenance=EntityProvenance(
                        match_kind="proposal",
                        match_path="news_llm_claim",
                        match_source="llm",
                        source_pack=pack or pack_id,
                        source_domain="finance",
                    ),
                    link=EntityLink(
                        entity_id="ades:impact:currency:gbp",
                        canonical_text="British pound",
                        provider="proposal",
                    ),
                ),
            ],
            topics=[TopicMatch(label="politics", score=0.86, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs, **_: object) -> ImpactExpansionResult:
        assert "country:uk" in set(entity_refs)
        return ImpactExpansionResult(
            graph_version="test-graph",
            artifact_version="2026-06-30",
            artifact_hash="sha256:no-terminal",
            warnings=[
                "stale_artifact:expected=sha256:new:observed=sha256:old",
                "active_parent_conflict:issuer:example",
            ],
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="country:uk",
                    name="United Kingdom",
                    entity_type="country",
                    is_graph_seed=True,
                    seed_degree=1,
                )
            ],
            candidates=[
                ImpactCandidate(
                    entity_ref="ades:impact:currency:gbp",
                    name="British pound",
                    entity_type="currency",
                    evidence_level="shallow",
                    confidence=0.72,
                    source_entity_refs=["country:uk"],
                    relationship_paths=[],
                )
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "UK parliament approves mining permit rules",
            "text": (
                "United Kingdom parliament approved new mining permit rules. "
                "Officials dismissed an unverified GBP proxy mentioned by analysts."
            ),
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
    assert payload["terminal_candidates"] == []
    assert payload["candidate_paths"] == []
    assert payload["rejected_candidates"][0]["reason_code"] == "event_incompatible"
    assert set(payload["no_terminal_reasons"]) >= {
        "no_path",
        "event_incompatible",
        "low_tier",
        "conflicted",
        "macro_gated",
        "stale_artifact",
    }
    diagnostic_codes = {diagnostic["code"] for diagnostic in payload["diagnostics"]}
    assert {
        "no_terminal:no_path",
        "no_terminal:event_incompatible",
        "no_terminal:low_tier",
        "no_terminal:conflicted",
        "no_terminal:macro_gated",
        "no_terminal:stale_artifact",
    } <= diagnostic_codes
    unresolved_diagnostic = next(
        diagnostic
        for diagnostic in payload["diagnostics"]
        if diagnostic["code"] == "country_scope_without_terminal_candidate"
    )
    assert unresolved_diagnostic["entity_text"] == "United Kingdom"
    assert unresolved_diagnostic["normalized_ref"] == "country:uk"
    assert unresolved_diagnostic["entity_type"] == "country"
    assert unresolved_diagnostic["jurisdiction"] == "uk"
    assert unresolved_diagnostic["missing_relation"] == "country_to_terminal_candidate"
    assert unresolved_diagnostic["missing_node"] == "terminal_candidate"
    assert unresolved_diagnostic["nearest_known_node"] == "country:uk"
    assert unresolved_diagnostic["source_lane_suggestion"] == "finance-uk-en"
    assert unresolved_diagnostic["review_priority"] == "high"
    assert unresolved_diagnostic["replay_key"].startswith("unresolved-entity:")


def test_news_analyze_uses_sector_graph_seeds_for_installed_country_pack(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for pack_id, domain in (
        ("general-en", "general"),
        ("finance-en", "finance"),
        ("finance-uk-en", "finance"),
    ):
        _install_named_pack(tmp_path, pack_id, domain=domain)
    graph_artifact_path, graph_artifact_hash = _build_uk_mining_sector_graph(tmp_path)

    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", graph_artifact_path)
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
        and decision["selected"] is True
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
    assert bem_path["artifact_ref"] == graph_artifact_hash
    assert "NO_TERMINAL_IMPACT_CANDIDATES" not in payload["quality_flags"]


def test_news_analyze_direct_sector_story_is_country_pack_gated(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ENABLED", "1")

    def _run_direct_sector_case(*, install_country_pack: bool) -> dict[str, object]:
        case_root = tmp_path / ("with-country-pack" if install_country_pack else "no-country-pack")
        for pack_id, domain in (
            ("general-en", "general"),
            ("finance-en", "finance"),
        ):
            _install_named_pack(case_root, pack_id, domain=domain)
        if install_country_pack:
            _install_named_pack(case_root, "finance-uk-en", domain="finance")
        graph_artifact_path, _graph_artifact_hash = _build_uk_direct_sector_story_graph(case_root)
        monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", graph_artifact_path)
        client = TestClient(create_app(storage_root=case_root))

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
                "title": "UK mining sector faces policy reform",
                "text": (
                    "The UK government announced a mining policy reform that analysts said "
                    "could affect miners and mining companies."
                ),
                "source": {"source_country": "GB"},
                "options": {
                    "include_relationship_paths": True,
                    "include_terminal_candidates": True,
                    "include_tag_responses": False,
                    "impact_max_depth": 3,
                    "max_country_finance_packs": 1,
                },
            },
        )

        assert response.status_code == 200
        return response.json()

    unsupported_payload = _run_direct_sector_case(install_country_pack=False)
    assert any(
        decision["pack_id"] == "finance-uk-en"
        and decision["selected"] is False
        and decision["country_code"] == "uk"
        for decision in unsupported_payload["pack_decisions"]
    )
    assert unsupported_payload["source_entities"] == []
    assert unsupported_payload["terminal_impact_candidates"] == []
    assert unsupported_payload["candidate_paths"] == []
    assert "missing_country_pack" in unsupported_payload["no_terminal_reasons"]
    assert any(
        diagnostic["code"] == "no_terminal:missing_country_pack"
        for diagnostic in unsupported_payload["diagnostics"]
    )

    supported_payload = _run_direct_sector_case(install_country_pack=True)
    assert supported_payload["event_signal"]["event_type"] == "sector_policy_change"
    assert any(
        decision["pack_id"] == "finance-uk-en"
        and decision["selected"] is True
        and decision["country_code"] == "uk"
        for decision in supported_payload["pack_decisions"]
    )
    assert any(
        source["entity_ref"] == "finance-uk-sector:mining"
        and source["is_graph_seed"] is True
        for source in supported_payload["source_entities"]
    )
    terminal_candidates_by_ref = {
        candidate["entity_ref"]: candidate
        for candidate in supported_payload["terminal_impact_candidates"]
    }
    assert {
        "ades:impact:index:uk-mining-index",
        "ades:security:gb:lse:bem-ordinary-share",
        "finance-uk-ticker:bem",
        "finance-uk-ticker:ggp",
    } <= set(terminal_candidates_by_ref)
    assert terminal_candidates_by_ref["ades:impact:index:uk-mining-index"][
        "compatible_event_types"
    ] == ["sector_policy_change"]
    assert all(
        candidate["compatible_event_types"] == ["sector_policy_change"]
        for candidate_ref, candidate in terminal_candidates_by_ref.items()
        if candidate_ref
        in {
            "ades:security:gb:lse:bem-ordinary-share",
            "finance-uk-ticker:bem",
            "finance-uk-ticker:ggp",
        }
    )

    candidate_paths_by_ref = {
        candidate_path["terminal_ref"]: candidate_path
        for candidate_path in supported_payload["candidate_paths"]
    }
    index_path = candidate_paths_by_ref["ades:impact:index:uk-mining-index"]
    assert index_path["terminal_type"] == "index"
    assert index_path["security_ids"] == {"ades_ref": "ades:impact:index:uk-mining-index"}
    assert [edge["relation"] for edge in index_path["relationship_path"]["edges"]] == [
        "sector_affects_index"
    ]
    assert index_path["source_tiers"] == ["exchange"]

    security_path = candidate_paths_by_ref["ades:security:gb:lse:bem-ordinary-share"]
    assert security_path["terminal_type"] == "security"
    assert security_path["jurisdiction"] == "gb"
    assert security_path["exchange"] == "lse"
    assert security_path["security_ids"] == {
        "ades_ref": "ades:security:gb:lse:bem-ordinary-share",
        "local_security_id": "bem-ordinary-share",
    }
    assert [edge["relation"] for edge in security_path["relationship_path"]["edges"]] == [
        "sector_affects_issuer",
        "issuer_has_security",
    ]

    ggp_path = candidate_paths_by_ref["finance-uk-ticker:ggp"]
    assert ggp_path["terminal_type"] == "ticker"
    assert ggp_path["ticker"] == "ggp"
    assert [edge["relation"] for edge in ggp_path["relationship_path"]["edges"]] == [
        "sector_affects_issuer",
        "issuer_has_listed_ticker",
    ]
    assert "NO_TERMINAL_IMPACT_CANDIDATES" not in supported_payload["quality_flags"]


def test_news_analyze_returns_government_policy_terminal_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for pack_id, domain in (
        ("general-en", "general"),
        ("finance-en", "finance"),
        ("finance-us-en", "finance"),
    ):
        _install_named_pack(tmp_path, pack_id, domain=domain)
    graph_artifact_path, graph_artifact_hash = _build_us_policy_terminal_graph(tmp_path)

    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", graph_artifact_path)
    client = TestClient(create_app(storage_root=tmp_path))

    source_cases = [
        (
            "CHIPS Act",
            "law",
            "ades:us-law:chips-act",
            "law_affects_sector",
            "government",
        ),
        (
            "US Commerce Department",
            "government_body",
            "ades:us-government-body:commerce-department",
            "government_body_affects_sector",
            "government",
        ),
        (
            "US Commerce Ministry",
            "ministry",
            "ades:us-ministry:commerce",
            "government_body_affects_sector",
            "government",
        ),
        (
            "US Chip Subsidy Policy",
            "policy",
            "ades:us-policy:chip-subsidy-policy",
            "policy_body_affects_sector",
            "government",
        ),
        (
            "US Chip Export Rule",
            "regulation",
            "ades:us-regulation:chip-export-rule",
            "regulation_affects_sector",
            "regulator",
        ),
    ]
    current_source = source_cases[0]

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        source_text, source_label, source_ref, _source_relation, _source_tier = current_source
        entities = []
        if pack == "finance-us-en":
            entities.append(
                EntityMatch(
                    text=source_text,
                    label=source_label,
                    start=text.index(source_text),
                    end=text.index(source_text) + len(source_text),
                    confidence=0.94,
                    relevance=0.95,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack,
                        source_domain="finance",
                    ),
                    link=EntityLink(
                        entity_id=source_ref,
                        canonical_text=source_text,
                        provider="ades",
                    ),
                )
            )
        return TagResponse(
            version="0.1.0",
            pack=pack or "unknown",
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=entities,
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    for source_case in source_cases:
        current_source = source_case
        source_text, _source_label, source_ref, source_relation, source_tier = source_case
        response = client.post(
            "/v0/news/analyze",
            json={
                "title": f"US officials advance semiconductor measure: {source_text}",
                "text": (
                    f"US officials advanced {source_text} for semiconductor "
                    "companies, changing policy support for chipmakers."
                ),
                "source": {"source_country": "US"},
                "options": {
                    "include_relationship_paths": True,
                    "include_terminal_candidates": True,
                    "include_tag_responses": False,
                    "impact_max_depth": 3,
                    "max_country_finance_packs": 1,
                },
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["event_signal"]["event_type"] == "sector_policy_change"
        assert any(
            source["entity_ref"] == source_ref and source["is_graph_seed"] is True
            for source in payload["source_entities"]
        )
        candidates_by_ref = {
            candidate["entity_ref"]: candidate
            for candidate in payload["terminal_impact_candidates"]
        }
        assert "finance-us-issuer:example-semiconductor" in candidates_by_ref
        assert "finance-us-ticker:nasdaq:xchp" in candidates_by_ref
        assert "ades:impact:index:us-semiconductor-index" in candidates_by_ref

        issuer_path = candidates_by_ref["finance-us-issuer:example-semiconductor"][
            "relationship_paths"
        ][0]
        assert [edge["relation"] for edge in issuer_path["edges"]] == [
            source_relation,
            "sector_affects_issuer",
        ]
        assert issuer_path["edges"][0]["source_tier"] == source_tier

        candidate_paths_by_ref = {
            candidate_path["terminal_ref"]: candidate_path
            for candidate_path in payload["candidate_paths"]
        }
        ticker_path = candidate_paths_by_ref["finance-us-ticker:nasdaq:xchp"]
        assert ticker_path["terminal_type"] == "ticker"
        assert ticker_path["jurisdiction"] == "us"
        assert ticker_path["exchange"] == "nasdaq"
        assert ticker_path["ticker"] == "xchp"
        assert ticker_path["security_ids"] == {
            "ades_ref": "finance-us-ticker:nasdaq:xchp",
            "ticker": "xchp",
            "exchange_ticker": "nasdaq:xchp",
        }
        assert ticker_path["event_compatibility"] == ["sector_policy_change"]
        assert ticker_path["artifact_ref"] == graph_artifact_hash
        assert source_tier in ticker_path["source_tiers"]
        assert "exchange" in ticker_path["source_tiers"]
        assert [edge["relation"] for edge in ticker_path["relationship_path"]["edges"]] == [
            source_relation,
            "sector_affects_issuer",
            "issuer_has_listed_ticker",
        ]
        assert (
            candidate_paths_by_ref["ades:impact:index:us-semiconductor-index"]["terminal_type"]
            == "index"
        )
        assert "NO_TERMINAL_IMPACT_CANDIDATES" not in payload["quality_flags"]


def test_news_analyze_government_subsidy_reaches_sector_issuers_not_currency_proxies(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for pack_id, domain in (
        ("general-en", "general"),
        ("finance-en", "finance"),
        ("finance-us-en", "finance"),
    ):
        _install_named_pack(tmp_path, pack_id, domain=domain)
    graph_artifact_path, graph_artifact_hash = _build_us_policy_terminal_graph(tmp_path)

    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", graph_artifact_path)
    client = TestClient(create_app(storage_root=tmp_path))

    source_text = "US Chip Subsidy Policy"
    source_ref = "ades:us-policy:chip-subsidy-policy"

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        entities = []
        if pack == "finance-us-en":
            usd_text = "USD 39 billion"
            entities.extend(
                [
                    EntityMatch(
                        text=source_text,
                        label="policy",
                        start=text.index(source_text),
                        end=text.index(source_text) + len(source_text),
                        confidence=0.94,
                        relevance=0.95,
                        provenance=EntityProvenance(
                            match_kind="alias",
                            match_path="aliases.json",
                            match_source="pack",
                            source_pack=pack,
                            source_domain="finance",
                        ),
                        link=EntityLink(
                            entity_id=source_ref,
                            canonical_text=source_text,
                            provider="ades",
                        ),
                    ),
                    EntityMatch(
                        text=usd_text,
                        label="currency",
                        start=text.index(usd_text),
                        end=text.index(usd_text) + len(usd_text),
                        confidence=0.91,
                        relevance=0.88,
                        provenance=EntityProvenance(
                            match_kind="alias",
                            match_path="aliases.json",
                            match_source="pack",
                            source_pack=pack,
                            source_domain="finance",
                        ),
                        link=EntityLink(
                            entity_id="ades:impact:currency:usd",
                            canonical_text="US dollar",
                            provider="ades",
                        ),
                    ),
                ]
            )
        return TagResponse(
            version="0.1.0",
            pack=pack or "unknown",
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=entities,
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "US expands chip subsidies for semiconductor manufacturers",
            "text": (
                "The US Chip Subsidy Policy expanded USD 39 billion in government subsidy support "
                "for semiconductor companies and chipmakers."
            ),
            "source": {"source_country": "US"},
            "options": {
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "impact_max_depth": 3,
                "max_country_finance_packs": 1,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["event_signal"]["event_type"] == "sector_policy_change"
    assert any(
        source["entity_ref"] == source_ref and source["is_graph_seed"] is True
        for source in payload["source_entities"]
    )

    terminal_candidates_by_ref = {
        candidate["entity_ref"]: candidate
        for candidate in payload["terminal_impact_candidates"]
    }
    candidate_paths_by_ref = {
        candidate_path["terminal_ref"]: candidate_path
        for candidate_path in payload["candidate_paths"]
    }
    expected_sector_terminals = {
        "finance-us-issuer:example-semiconductor",
        "finance-us-ticker:nasdaq:xchp",
        "ades:impact:index:us-semiconductor-index",
    }

    assert expected_sector_terminals.issubset(terminal_candidates_by_ref)
    assert expected_sector_terminals.issubset(candidate_paths_by_ref)
    assert "ades:impact:currency:usd" not in terminal_candidates_by_ref
    assert "ades:impact:currency:usd" not in candidate_paths_by_ref
    assert all(
        candidate_path["terminal_type"] not in {"currency", "rates_proxy"}
        for candidate_path in candidate_paths_by_ref.values()
    )

    issuer_path = terminal_candidates_by_ref["finance-us-issuer:example-semiconductor"][
        "relationship_paths"
    ][0]
    assert [edge["relation"] for edge in issuer_path["edges"]] == [
        "policy_body_affects_sector",
        "sector_affects_issuer",
    ]
    assert issuer_path["edges"][0]["source_tier"] == "government"

    ticker_path = candidate_paths_by_ref["finance-us-ticker:nasdaq:xchp"]
    assert ticker_path["terminal_type"] == "ticker"
    assert ticker_path["jurisdiction"] == "us"
    assert ticker_path["exchange"] == "nasdaq"
    assert ticker_path["ticker"] == "xchp"
    assert ticker_path["security_ids"] == {
        "ades_ref": "finance-us-ticker:nasdaq:xchp",
        "ticker": "xchp",
        "exchange_ticker": "nasdaq:xchp",
    }
    assert ticker_path["event_compatibility"] == ["sector_policy_change"]
    assert ticker_path["artifact_ref"] == graph_artifact_hash
    assert "government" in ticker_path["source_tiers"]
    assert "exchange" in ticker_path["source_tiers"]
    assert [edge["relation"] for edge in ticker_path["relationship_path"]["edges"]] == [
        "policy_body_affects_sector",
        "sector_affects_issuer",
        "issuer_has_listed_ticker",
    ]
    assert (
        candidate_paths_by_ref["ades:impact:index:us-semiconductor-index"]["terminal_type"]
        == "index"
    )
    assert "NO_TERMINAL_IMPACT_CANDIDATES" not in payload["quality_flags"]


def test_news_analyze_returns_legal_regulatory_action_terminal_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for pack_id, domain in (
        ("general-en", "general"),
        ("finance-en", "finance"),
        ("finance-us-en", "finance"),
    ):
        _install_named_pack(tmp_path, pack_id, domain=domain)
    graph_artifact_path, graph_artifact_hash = _build_us_policy_terminal_graph(tmp_path)

    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", graph_artifact_path)
    client = TestClient(create_app(storage_root=tmp_path))

    source_cases = [
        (
            "lawsuit",
            "US District Court",
            "government_body",
            "ades:us-court:district-court",
            "government_body_affects_sector",
            "government",
            (
                "A class action lawsuit in US District Court targeted technology "
                "companies over price conduct claims."
            ),
        ),
        (
            "investigation",
            "Federal Trade Commission",
            "regulator",
            "ades:us-regulator:ftc",
            "regulator_affects_sector",
            "regulator",
            (
                "The regulator Federal Trade Commission opened an antitrust "
                "investigation into technology companies."
            ),
        ),
        (
            "regulator",
            "Federal Trade Commission",
            "regulator",
            "ades:us-regulator:ftc",
            "regulator_affects_sector",
            "regulator",
            (
                "The regulator Federal Trade Commission ordered remedies against "
                "technology companies after an enforcement probe."
            ),
        ),
        (
            "court",
            "US District Court",
            "government_body",
            "ades:us-court:district-court",
            "government_body_affects_sector",
            "government",
            (
                "US District Court ruled against technology companies in an "
                "antitrust case over price conduct claims."
            ),
        ),
    ]
    current_source = source_cases[0]

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        (
            _case_name,
            source_text,
            source_label,
            source_ref,
            _source_relation,
            _source_tier,
            _body_text,
        ) = current_source
        entities = []
        if pack == "finance-us-en":
            entities.append(
                EntityMatch(
                    text=source_text,
                    label=source_label,
                    start=text.index(source_text),
                    end=text.index(source_text) + len(source_text),
                    confidence=0.94,
                    relevance=0.95,
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="aliases.json",
                        match_source="pack",
                        source_pack=pack,
                        source_domain="finance",
                    ),
                    link=EntityLink(
                        entity_id=source_ref,
                        canonical_text=source_text,
                        provider="ades",
                    ),
                )
            )
        return TagResponse(
            version="0.1.0",
            pack=pack or "unknown",
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=entities,
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    for source_case in source_cases:
        current_source = source_case
        (
            case_name,
            source_text,
            _source_label,
            source_ref,
            source_relation,
            source_tier,
            body_text,
        ) = source_case
        response = client.post(
            "/v0/news/analyze",
            json={
                "title": f"Legal regulatory action: {case_name}",
                "text": body_text,
                "source": {"source_country": "US"},
                "options": {
                    "include_relationship_paths": True,
                    "include_terminal_candidates": True,
                    "include_tag_responses": False,
                    "impact_max_depth": 3,
                    "max_country_finance_packs": 1,
                },
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["event_signal"]["event_type"] == "regulatory_enforcement"
        assert any(
            source["entity_ref"] == source_ref and source["is_graph_seed"] is True
            for source in payload["source_entities"]
        )
        candidates_by_ref = {
            candidate["entity_ref"]: candidate
            for candidate in payload["terminal_impact_candidates"]
        }
        assert "finance-us-issuer:example-semiconductor" in candidates_by_ref
        assert "finance-us-ticker:nasdaq:xchp" in candidates_by_ref
        assert "ades:impact:index:us-semiconductor-index" in candidates_by_ref

        issuer_path = candidates_by_ref["finance-us-issuer:example-semiconductor"][
            "relationship_paths"
        ][0]
        assert [edge["relation"] for edge in issuer_path["edges"]] == [
            source_relation,
            "sector_affects_issuer",
        ]
        assert all(edge["source_url"] for edge in issuer_path["edges"])
        assert all(edge["source_tier"] for edge in issuer_path["edges"])
        assert issuer_path["edges"][0]["source_tier"] == source_tier

        candidate_paths_by_ref = {
            candidate_path["terminal_ref"]: candidate_path
            for candidate_path in payload["candidate_paths"]
        }
        ticker_path = candidate_paths_by_ref["finance-us-ticker:nasdaq:xchp"]
        assert ticker_path["terminal_type"] == "ticker"
        assert ticker_path["event_compatibility"] == ["regulatory_enforcement"]
        assert ticker_path["artifact_ref"] == graph_artifact_hash
        assert source_tier in ticker_path["source_tiers"]
        assert "exchange" in ticker_path["source_tiers"]
        assert [edge["relation"] for edge in ticker_path["relationship_path"]["edges"]] == [
            source_relation,
            "sector_affects_issuer",
            "issuer_has_listed_ticker",
        ]
        assert all(
            edge["source_url"] and edge["source_tier"]
            for edge in ticker_path["relationship_path"]["edges"]
        )
        assert (
            candidate_paths_by_ref["ades:impact:index:us-semiconductor-index"]["terminal_type"]
            == "index"
        )
        assert "NO_TERMINAL_IMPACT_CANDIDATES" not in payload["quality_flags"]


def test_news_analyze_uk_housebuilder_legal_golden_reaches_terminal_not_gbp(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for pack_id, domain in (
        ("general-en", "general"),
        ("finance-en", "finance"),
        ("finance-uk-en", "finance"),
    ):
        _install_named_pack(tmp_path, pack_id, domain=domain)
    graph_artifact_path, graph_artifact_hash = _build_uk_housebuilder_legal_graph(tmp_path)

    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ENABLED", "1")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", graph_artifact_path)
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        entities = []
        if pack == "finance-uk-en":
            cma_text = "Competition and Markets Authority"
            gbp_text = "GBP4bn"
            entities.extend(
                [
                    EntityMatch(
                        text=cma_text,
                        label="regulator",
                        start=text.index(cma_text),
                        end=text.index(cma_text) + len(cma_text),
                        confidence=0.94,
                        relevance=0.96,
                        provenance=EntityProvenance(
                            match_kind="alias",
                            match_path="aliases.json",
                            match_source="pack",
                            source_pack=pack,
                            source_domain="finance",
                        ),
                        link=EntityLink(
                            entity_id="ades:regulator:gb:cma",
                            canonical_text=cma_text,
                            provider="ades",
                        ),
                    ),
                    EntityMatch(
                        text=gbp_text,
                        label="currency",
                        start=text.index(gbp_text),
                        end=text.index(gbp_text) + len(gbp_text),
                        confidence=0.91,
                        relevance=0.9,
                        provenance=EntityProvenance(
                            match_kind="alias",
                            match_path="aliases.json",
                            match_source="pack",
                            source_pack=pack,
                            source_domain="finance",
                        ),
                        link=EntityLink(
                            entity_id="ades:impact:currency:gbp",
                            canonical_text="Pound sterling",
                            provider="ades",
                        ),
                    ),
                ]
            )
        return TagResponse(
            version="0.1.0",
            pack=pack or "unknown",
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=entities,
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)

    response = client.post(
        "/v0/news/analyze",
        json={
            "title": "UK housebuilders face legal action",
            "text": (
                "The UK Competition and Markets Authority said a GBP4bn class action "
                "lawsuit over price conduct claims could affect listed housebuilders "
                "including Taylor Wimpey."
            ),
            "source": {"source_country": "GB"},
            "options": {
                "include_relationship_paths": True,
                "include_terminal_candidates": True,
                "include_tag_responses": False,
                "impact_max_depth": 4,
                "max_country_finance_packs": 1,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["event_signal"]["event_type"] == "regulatory_enforcement"
    assert any(
        decision["pack_id"] == "finance-uk-en"
        and decision["selected"] is True
        and decision["country_code"] == "uk"
        for decision in payload["pack_decisions"]
    )
    assert any(
        source["entity_ref"] == "ades:regulator:gb:cma" and source["is_graph_seed"] is True
        for source in payload["source_entities"]
    )

    candidates_by_ref = {
        candidate["entity_ref"]: candidate
        for candidate in payload["terminal_impact_candidates"]
    }
    assert "finance-uk-ticker:tw" in candidates_by_ref
    assert "ades:security:gb:lse:tw-ordinary-share" in candidates_by_ref
    assert "ades:impact:currency:gbp" not in candidates_by_ref

    ticker_path = {
        candidate_path["terminal_ref"]: candidate_path
        for candidate_path in payload["candidate_paths"]
    }["finance-uk-ticker:tw"]
    assert ticker_path["terminal_type"] == "ticker"
    assert ticker_path["jurisdiction"] == "uk"
    assert ticker_path["ticker"] == "tw"
    assert ticker_path["event_compatibility"] == ["regulatory_enforcement"]
    assert ticker_path["artifact_ref"] == graph_artifact_hash
    assert [edge["relation"] for edge in ticker_path["relationship_path"]["edges"]] == [
        "regulator_affects_sector",
        "sector_affects_issuer",
        "issuer_has_listed_ticker",
    ]
    assert "government" in ticker_path["source_tiers"]
    assert "exchange" in ticker_path["source_tiers"]
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


def test_news_analyze_story_quality_routes_topics_and_strips_internal_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "story-quality-en", domain="finance")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    cases = [
        (
            "Bitcoin rallied as crypto ETF flows increased.",
            "Commodities, Energy & Agriculture",
            "Trading & Markets",
        ),
        (
            "A court penalty hit Trump-linked entities after a regulator enforcement action.",
            "Economic & Macro",
            "Regulatory & Legal",
        ),
        (
            "Microsoft expanded AI infrastructure capacity for enterprise software customers.",
            "Commodities, Energy & Agriculture",
            "General / Mixed",
        ),
        (
            "A quantum computing launch expanded cloud security tools for developers.",
            "Commodities, Energy & Agriculture",
            "General / Mixed",
        ),
        (
            "A space launch carried satellite broadband equipment for enterprise networks.",
            "Commodities, Energy & Agriculture",
            "General / Mixed",
        ),
        (
            "Weekly market outlook: equities rallied as bond yields fell and investors rotated.",
            "Commodities, Energy & Agriculture",
            "Trading & Markets",
        ),
        (
            "Sky made a takeover offer for ITV shares.",
            "Regional & International",
            "Trading & Markets",
        ),
    ]

    for story_text, topic_label, expected_primary in cases:

        def _fake_tag(
            text: str,
            *,
            pack: str | None = None,
            content_type: str = "text/plain",
            **_: object,
        ) -> TagResponse:
            assert "Primary signal:" not in text
            assert "Market exposure is concentrated around" not in text
            return TagResponse(
                version="0.1.0",
                pack=pack or "unknown",
                pack_version="0.1.0",
                language="en",
                content_type=content_type,
                entities=[],
                topics=[TopicMatch(label=topic_label, score=0.9, evidence_count=1)],
                warnings=[],
                timing_ms=1,
            )

        monkeypatch.setattr("ades.service.app.tag", _fake_tag)
        response = client.post(
            "/v0/news/analyze",
            json={
                "packs": [pack_id],
                "text": (
                    "Primary signal: Seven & i Holdings internal routing note.\n"
                    "Market exposure is concentrated around stale terminal refs.\n"
                    f"{story_text}"
                ),
                "options": {"include_relationship_paths": False},
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["topic_scope"]["primary"] == expected_primary
        assert "ADES_NEWS_ANALYZE_STRIPPED_INTERNAL_METADATA" in payload["warnings"]


def test_news_analyze_strips_placeholder_titles_and_additional_metadata_labels(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "metadata-hygiene-en", domain="finance")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag(
        text: str,
        *,
        pack: str | None = None,
        content_type: str = "text/plain",
        **_: object,
    ) -> TagResponse:
        assert "Unclassified market-relevant update" not in text
        assert "Primary action:" not in text
        assert "Market exposure:" not in text
        assert "Impact paths:" not in text
        assert "Passive entities:" not in text
        assert "Category:" not in text
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
            "packs": [pack_id],
            "title": "Unclassified market-relevant update",
            "description": (
                "Primary action: raw source text\n"
                "Category: Commodities, Energy & Agriculture"
            ),
            "text": (
                "Market exposure: finance-us-ticker:NFLX\n"
                "Impact paths: short term\n"
                "Passive entities: Anno Domini\n"
                "A bank regulator fined a lender over capital controls."
            ),
            "options": {"include_relationship_paths": False},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert "ADES_NEWS_ANALYZE_STRIPPED_INTERNAL_METADATA" in payload["warnings"]


def test_news_analyze_filters_matched_signal_sports_from_market_impacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "sports-filter-en", domain="general")
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
            entities=[
                EntityMatch(
                    text="FIFA",
                    label="organization",
                    start=0,
                    end=4,
                    confidence=0.91,
                    relevance=0.9,
                    link=EntityLink(
                        entity_id="ades:org:fifa",
                        canonical_text="FIFA",
                        provider="test",
                    ),
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="test",
                        match_source="fixture",
                        source_pack=pack or "unknown",
                        source_domain="general",
                    ),
                )
            ],
            topics=[TopicMatch(label="Sports", score=0.95, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fail_expand(*_: object, **__: object) -> ImpactExpansionResult:
        raise AssertionError("sports stories must not expand market impacts")

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fail_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "packs": [pack_id],
            "text": (
                "matched_signal=sports Morningstar downgrades Barcelona after a "
                "FIFA president investigation."
            ),
            "options": {
                "include_relationship_paths": True,
                "include_impact_paths": True,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["terminal_impact_candidates"] == []
    assert "NON_MARKET_NEWS_DOMAIN" in payload["quality_flags"]
    assert any(
        warning.startswith("ADES_NEWS_ANALYZE_NON_MARKET_DOMAIN:matched_signal_sports")
        for warning in payload["warnings"]
    )


def test_news_analyze_forces_primary_country_actor_passives(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "country-actor-en", domain="general")
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
            topics=[TopicMatch(label="Geopolitics", score=0.9, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    response = client.post(
        "/v0/news/analyze",
        json={
            "packs": [pack_id],
            "text": (
                "The United States and Iran discussed Strait of Hormuz transit "
                "with Oman while BP and Bayer briefed investors."
            ),
            "source": {"source_country": "TR"},
            "options": {"include_relationship_paths": False},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    passive_refs = {entity["entity_ref"] for entity in payload["passive_entities"]}
    assert {"country:us", "country:ir", "country:om", "country:uk", "country:de"}.issubset(
        passive_refs
    )
    assert "country:tr" not in passive_refs


def test_news_analyze_forces_uae_and_cuba_text_country_scopes_without_source_bias(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "country-scope-alias-en", domain="general")
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
            topics=[TopicMatch(label="Geopolitics", score=0.9, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    response = client.post(
        "/v0/news/analyze",
        json={
            "packs": [pack_id],
            "text": (
                "U.S. officials and UAE ministers discussed investment while "
                "Cuba restored power after a nationwide outage."
            ),
            "source": {"source_country": "TR"},
            "options": {"include_relationship_paths": False},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    passive_refs = {entity["entity_ref"] for entity in payload["passive_entities"]}
    assert {"country:us", "country:ae", "country:cu"}.issubset(passive_refs)
    assert "country:tr" not in passive_refs


def test_news_analyze_normalizes_country_alias_passives_and_hides_date_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "passive-country-hygiene-en", domain="general")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    story_text = (
        "Estados Unidos policymakers cited Anno Domini in a malformed source sidebar."
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
            pack=pack or "unknown",
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text="Estados Unidos",
                    label="country",
                    start=text.index("Estados Unidos"),
                    end=text.index("Estados Unidos") + len("Estados Unidos"),
                    confidence=0.94,
                    relevance=0.92,
                    link=EntityLink(
                        entity_id="country:us",
                        canonical_text="Estados Unidos",
                        provider="test",
                    ),
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="test",
                        match_source="fixture",
                        source_pack=pack or "unknown",
                        source_domain="general",
                    ),
                ),
                EntityMatch(
                    text="Anno Domini",
                    label="organization",
                    start=text.index("Anno Domini"),
                    end=text.index("Anno Domini") + len("Anno Domini"),
                    confidence=0.88,
                    relevance=0.7,
                    link=EntityLink(
                        entity_id="ades:artifact:anno-domini",
                        canonical_text="Anno Domini",
                        provider="test",
                    ),
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="test",
                        match_source="fixture",
                        source_pack=pack or "unknown",
                        source_domain="general",
                    ),
                ),
            ],
            topics=[TopicMatch(label="Political & Policy", score=0.9, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    response = client.post(
        "/v0/news/analyze",
        json={
            "packs": [pack_id],
            "text": story_text,
            "options": {"include_relationship_paths": False},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    passive_by_ref = {entity["entity_ref"]: entity for entity in payload["passive_entities"]}
    country = passive_by_ref["country:us"]
    assert country["name"] == "United States"
    assert country["display_eligible"] is True
    artifact = passive_by_ref["ades:artifact:anno-domini"]
    assert artifact["display_eligible"] is False
    assert "date_era_artifact" in artifact["quality_reasons"]


def test_news_analyze_hides_context_mismatched_passive_entities(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "passive-context-filter-en", domain="finance")
    monkeypatch.setenv("ADES_NEWS_ANALYZE_ENABLED", "1")
    client = TestClient(create_app(storage_root=tmp_path))

    story_text = (
        "The Air Force awarded a defense contract after ALAB and VH2 were cited "
        "in a raw market-data sidebar."
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
            pack=pack or "unknown",
            pack_version="0.1.0",
            language="en",
            content_type=content_type,
            entities=[
                EntityMatch(
                    text="Air Force",
                    label="organization",
                    start=text.index("Air Force"),
                    end=text.index("Air Force") + len("Air Force"),
                    confidence=0.92,
                    relevance=0.9,
                    link=EntityLink(
                        entity_id="ades:sports:air-force-falcons-baseball",
                        canonical_text="Air Force Falcons baseball",
                        provider="test",
                    ),
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="test",
                        match_source="fixture",
                        source_pack=pack or "unknown",
                        source_domain="general",
                    ),
                ),
                EntityMatch(
                    text="ALAB",
                    label="organization",
                    start=text.index("ALAB"),
                    end=text.index("ALAB") + len("ALAB"),
                    confidence=0.88,
                    relevance=0.77,
                    aliases=["ALAB"],
                    link=EntityLink(
                        entity_id="finance-us-issuer:astera-labs",
                        canonical_text="Astera Labs, Inc.",
                        provider="test",
                    ),
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="test",
                        match_source="fixture",
                        source_pack=pack or "unknown",
                        source_domain="finance",
                    ),
                ),
                EntityMatch(
                    text="VH2",
                    label="organization",
                    start=text.index("VH2"),
                    end=text.index("VH2") + len("VH2"),
                    confidence=0.86,
                    relevance=0.7,
                    link=EntityLink(
                        entity_id="ades:artifact:vh2",
                        canonical_text="VH2",
                        provider="test",
                    ),
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="test",
                        match_source="fixture",
                        source_pack=pack or "unknown",
                        source_domain="finance",
                    ),
                ),
            ],
            topics=[TopicMatch(label="Trading & Markets", score=0.9, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    response = client.post(
        "/v0/news/analyze",
        json={
            "packs": [pack_id],
            "text": story_text,
            "options": {"include_relationship_paths": False},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    passive_by_ref = {entity["entity_ref"]: entity for entity in payload["passive_entities"]}
    sports_team = passive_by_ref["ades:sports:air-force-falcons-baseball"]
    assert sports_team["display_eligible"] is False
    assert "sports_entity_context_mismatch" in sports_team["quality_reasons"]
    unrelated_company = passive_by_ref["finance-us-issuer:astera-labs"]
    assert unrelated_company["display_eligible"] is False
    assert "raw_ticker_identifier" in unrelated_company["quality_reasons"]
    raw_ticker = passive_by_ref["ades:artifact:vh2"]
    assert raw_ticker["display_eligible"] is False
    assert "raw_ticker_identifier" in raw_ticker["quality_reasons"]


def test_news_analyze_hides_raw_finance_ticker_alias_and_flags_cantarell_operator(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "passive-artifact-en", domain="general")
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
            entities=[
                EntityMatch(
                    text="Finance Us Ticker HERE",
                    label="organization",
                    start=0,
                    end=22,
                    confidence=0.88,
                    relevance=0.8,
                    link=EntityLink(
                        entity_id="ades:artifact:finance-us-ticker-here",
                        canonical_text="Finance Us Ticker HERE",
                        provider="test",
                    ),
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="test",
                        match_source="fixture",
                        source_pack=pack or "unknown",
                        source_domain="general",
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
            "packs": [pack_id],
            "text": "Cantarell field operator Petrobras was cited in an energy story.",
            "options": {"include_relationship_paths": False},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    ticker_alias = next(
        entity
        for entity in payload["passive_entities"]
        if entity["name"] == "Finance Us Ticker HERE"
    )
    assert ticker_alias["display_eligible"] is False
    assert "raw_ticker_identifier" in ticker_alias["quality_reasons"]
    assert (
        "ADES_NEWS_ANALYZE_FACTUAL_CONSISTENCY:CANTARELL_OPERATOR_PEMEX"
        in payload["warnings"]
    )


def test_news_analyze_drops_unanchored_impact_candidates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "impact-context-filter-en", domain="finance")
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
            entities=[
                EntityMatch(
                    text="Reliance Home Finance",
                    label="organization",
                    start=text.index("Reliance Home Finance"),
                    end=text.index("Reliance Home Finance") + len("Reliance Home Finance"),
                    confidence=0.93,
                    relevance=0.92,
                    link=EntityLink(
                        entity_id="finance-in-issuer:rhfl",
                        canonical_text="Reliance Home Finance",
                        provider="test",
                    ),
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="test",
                        match_source="fixture",
                        source_pack=pack or "unknown",
                        source_domain="finance",
                    ),
                )
            ],
            topics=[TopicMatch(label="Regulatory & Legal", score=0.9, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _edge(source_ref: str, target_ref: str) -> ImpactPathEdge:
        return ImpactPathEdge(
            source_ref=source_ref,
            target_ref=target_ref,
            relation="regulatory_action_affects_security",
            evidence_level="direct",
            confidence=0.91,
            direction_hint="negative",
            source_name="Fixture",
            source_url="https://example.test/source",
            source_snapshot="2026-07-11",
            compatible_event_types=["regulatory_enforcement"],
        )

    def _candidate(
        entity_ref: str,
        name: str,
        entity_type: str,
        source_ref: str,
        target_ref: str,
    ) -> ImpactCandidate:
        return ImpactCandidate(
            entity_ref=entity_ref,
            name=name,
            entity_type=entity_type,
            evidence_level="shallow",
            confidence=0.88,
            source_entity_refs=[source_ref],
            relationship_paths=[
                ImpactRelationshipPath(
                    path_depth=1,
                    edges=[_edge(source_ref, target_ref)],
                )
            ],
            compatible_event_types=["regulatory_enforcement"],
        )

    def _fake_expand(entity_refs: list[str], **_: object) -> ImpactExpansionResult:
        assert "finance-in-issuer:rhfl" in entity_refs
        return ImpactExpansionResult(
            graph_version="fixture",
            artifact_version="fixture",
            artifact_hash="sha256:fixture",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="finance-in-issuer:rhfl",
                    name="Reliance Home Finance",
                    entity_type="organization",
                    same_as_refs=["finance-in-ticker:rhfl"],
                    is_graph_seed=True,
                ),
                ImpactSourceEntity(
                    entity_ref="finance-in-issuer:metrobrand",
                    name="METROBRAND",
                    entity_type="organization",
                    same_as_refs=["finance-in-ticker:metrobrand"],
                    is_graph_seed=True,
                ),
            ],
            candidates=[
                _candidate(
                    "finance-in-ticker:rhfl",
                    "RHFL",
                    "ticker",
                    "finance-in-issuer:rhfl",
                    "finance-in-ticker:rhfl",
                ),
                _candidate(
                    "ades:sector:metrobrand-retail",
                    "METROBRAND retail exposure",
                    "sector",
                    "finance-in-issuer:metrobrand",
                    "ades:sector:metrobrand-retail",
                ),
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "packs": [pack_id],
            "text": (
                "Reliance Home Finance faced a regulator enforcement action "
                "after a debt restructuring dispute."
            ),
            "options": {
                "include_relationship_paths": True,
                "include_impact_paths": True,
                "max_terminal_candidates": 8,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    candidate_refs = {
        candidate["entity_ref"] for candidate in payload["terminal_impact_candidates"]
    }
    assert "finance-in-ticker:rhfl" in candidate_refs
    assert "ades:sector:metrobrand-retail" not in candidate_refs
    assert any(
        warning.startswith("ADES_NEWS_ANALYZE_DROPPED_UNANCHORED_IMPACT_CANDIDATE")
        for warning in payload["warnings"]
    )


def test_news_analyze_drops_non_asset_terminal_identifier_candidates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "impact-terminal-hygiene-en", domain="finance")
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
            entities=[
                EntityMatch(
                    text="United Kingdom",
                    label="country",
                    start=text.index("United Kingdom"),
                    end=text.index("United Kingdom") + len("United Kingdom"),
                    confidence=0.94,
                    relevance=0.92,
                    link=EntityLink(
                        entity_id="country:uk",
                        canonical_text="United Kingdom",
                        provider="test",
                    ),
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="test",
                        match_source="fixture",
                        source_pack=pack or "unknown",
                        source_domain="general",
                    ),
                )
            ],
            topics=[TopicMatch(label="Trading & Markets", score=0.9, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _fake_expand(entity_refs: list[str], **_: object) -> ImpactExpansionResult:
        assert "country:uk" in entity_refs
        edge = ImpactPathEdge(
            source_ref="country:uk",
            target_ref="finance-uk-ticker:Gdp",
            relation="macro_indicator_misread_as_ticker",
            evidence_level="direct",
            confidence=0.9,
            direction_hint="macro",
            source_name="Fixture",
            source_url="https://example.test/source",
            source_snapshot="2026-07-12",
            compatible_event_types=["policy_rate_hold"],
        )
        return ImpactExpansionResult(
            graph_version="fixture",
            artifact_version="fixture",
            artifact_hash="sha256:fixture",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="country:uk",
                    name="United Kingdom",
                    entity_type="country",
                    is_graph_seed=True,
                )
            ],
            candidates=[
                ImpactCandidate(
                    entity_ref="finance-uk-ticker:Gdp",
                    name="GDP",
                    entity_type="ticker",
                    evidence_level="shallow",
                    confidence=0.9,
                    source_entity_refs=["country:uk"],
                    relationship_paths=[
                        ImpactRelationshipPath(path_depth=1, edges=[edge])
                    ],
                    compatible_event_types=["policy_rate_hold"],
                )
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)
    response = client.post(
        "/v0/news/analyze",
        json={
            "packs": [pack_id],
            "text": (
                "The United Kingdom central bank held rates steady as GDP data slowed."
            ),
            "options": {
                "include_relationship_paths": True,
                "include_impact_paths": True,
                "max_terminal_candidates": 8,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    candidate_refs = {
        candidate["entity_ref"] for candidate in payload["terminal_impact_candidates"]
    }
    assert "finance-uk-ticker:Gdp" not in candidate_refs
    assert any(
        warning.startswith("ADES_NEWS_ANALYZE_DROPPED_INVALID_TERMINAL")
        for warning in payload["warnings"]
    )


def test_news_analyze_overrides_negative_cost_and_warning_directions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack_id = _install_named_pack(tmp_path, "direction-override-en", domain="finance")
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
            entities=[
                EntityMatch(
                    text="United States",
                    label="country",
                    start=0,
                    end=13,
                    confidence=0.9,
                    relevance=0.8,
                    link=EntityLink(
                        entity_id="country:us",
                        canonical_text="United States",
                        provider="test",
                    ),
                    provenance=EntityProvenance(
                        match_kind="alias",
                        match_path="test",
                        match_source="fixture",
                        source_pack=pack or "unknown",
                        source_domain="finance",
                    ),
                )
            ],
            topics=[TopicMatch(label="Trading & Markets", score=0.9, evidence_count=1)],
            warnings=[],
            timing_ms=1,
        )

    def _edge(target_ref: str, relation: str, event_type: str) -> ImpactPathEdge:
        return ImpactPathEdge(
            source_ref="country:us",
            target_ref=target_ref,
            relation=relation,
            evidence_level="direct",
            confidence=0.9,
            direction_hint="positive",
            source_name="Fixture",
            source_url="https://example.test/source",
            source_snapshot="2026-07-10",
            compatible_event_types=[event_type],
        )

    def _candidate(entity_ref: str, name: str, edge: ImpactPathEdge) -> ImpactCandidate:
        return ImpactCandidate(
            entity_ref=entity_ref,
            name=name,
            entity_type="sector",
            evidence_level="shallow",
            confidence=0.86,
            source_entity_refs=["country:us"],
            relationship_paths=[
                ImpactRelationshipPath(path_depth=1, edges=[edge]),
            ],
            compatible_event_types=edge.compatible_event_types,
        )

    def _fake_expand(*_: object, **__: object) -> ImpactExpansionResult:
        return ImpactExpansionResult(
            graph_version="fixture",
            artifact_version="fixture",
            artifact_hash="sha256:fixture",
            source_entities=[
                ImpactSourceEntity(
                    entity_ref="country:us",
                    name="United States",
                    entity_type="country",
                )
            ],
            candidates=[
                _candidate(
                    "ades:sector:airlines",
                    "Airlines sector",
                    _edge("ades:sector:airlines", "commodity_affects_sector", "commodity_price_move"),
                ),
                _candidate(
                    "ades:sector:semiconductors",
                    "Semiconductors sector",
                    _edge(
                        "ades:sector:semiconductors",
                        "analyst_warning_affects_sector",
                        "guidance_cut",
                    ),
                ),
            ],
        )

    monkeypatch.setattr("ades.service.app.tag", _fake_tag)
    monkeypatch.setattr("ades.service.app.expand_impact_paths", _fake_expand)

    response = client.post(
        "/v0/news/analyze",
        json={
            "packs": [pack_id],
            "text": (
                "United States oil prices rose after a supply disruption, lifting "
                "fuel costs for airlines. Morgan Stanley cut its semiconductor "
                "outlook and warned chipmakers face weaker demand."
            ),
            "options": {
                "include_relationship_paths": True,
                "include_impact_paths": True,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    candidates = {
        candidate["entity_ref"]: candidate
        for candidate in payload["terminal_impact_candidates"]
    }
    assert (
        candidates["ades:sector:airlines"]["relationship_paths"][0]["edges"][0][
            "direction_hint"
        ]
        == "negative_cost_pressure"
    )
    assert (
        candidates["ades:sector:semiconductors"]["relationship_paths"][0]["edges"][0][
            "direction_hint"
        ]
        == "negative_analyst_warning"
    )
