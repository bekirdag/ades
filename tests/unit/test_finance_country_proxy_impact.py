import csv
import json
import sqlite3
from pathlib import Path

from ades.config import Settings
from ades.impact.expansion import expand_impact_paths
from ades.impact.finance_country_proxy import (
    build_finance_country_proxy_source_lane,
    discover_finance_country_pack_dirs,
    load_finance_country_pack_entities,
)


def _write_pack(root: Path, pack_id: str, rows: list[dict[str, object]]) -> Path:
    pack_dir = root / pack_id
    bundle_dir = root / "bundles" / f"{pack_id}-bundle"
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True)
    pack_dir.mkdir()
    entities_path = normalized_dir / "entities.jsonl"
    entities_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    (pack_dir / "aliases.json").write_text('{"aliases":[]}\n', encoding="utf-8")
    (pack_dir / "sources.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": pack_id,
                "entities_path": str(entities_path),
                "sources": [
                    {
                        "name": "unit-test-source",
                        "retrieved_at": "2026-05-05T00:00:00Z",
                    }
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (pack_dir / "build.json").write_text(
        '{"generated_at":"2026-05-05T12:00:00Z"}\n', encoding="utf-8"
    )
    return pack_dir


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _write_alias_pack(root: Path, pack_id: str, rows: list[dict[str, object]]) -> Path:
    pack_dir = root / pack_id
    pack_dir.mkdir(parents=True)
    (pack_dir / "aliases.json").write_text(
        json.dumps({"aliases": rows}, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (pack_dir / "sources.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": pack_id,
                "sources": [
                    {
                        "name": "unit-test-alias-source",
                        "url": "https://example.test/politics",
                        "retrieved_at": "2026-05-05T00:00:00Z",
                    }
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return pack_dir


def test_finance_country_proxy_generator_writes_direct_and_proxy_edges(tmp_path: Path) -> None:
    packs_root = tmp_path / "packs"
    packs_root.mkdir()
    _write_pack(
        packs_root,
        "finance-us-en",
        [
            {
                "entity_id": "finance-us-issuer:0001",
                "entity_type": "organization",
                "canonical_text": "Example Issuer Inc.",
                "metadata": {"country_code": "us", "category": "issuer", "ticker": "ABC"},
            },
            {
                "entity_id": "finance-us-issuer:0002",
                "entity_type": "organization",
                "canonical_text": "Synthetic Ticker Issuer Inc.",
                "metadata": {"country_code": "us", "category": "issuer", "ticker": "DEF"},
            },
            {
                "entity_id": "finance-us-ticker:ABC",
                "entity_type": "ticker",
                "canonical_text": "ABC",
                "metadata": {
                    "country_code": "us",
                    "category": "ticker",
                    "ticker": "ABC",
                    "exchanges": ["NYSE"],
                },
            },
            {
                "entity_id": "finance-person:0001:chief-executive",
                "entity_type": "person",
                "canonical_text": "Example CEO",
                "metadata": {
                    "country_code": "us",
                    "employer_ticker": "ABC",
                    "role_title": "Chief Executive Officer",
                    "source_url": "https://www.sec.gov/example",
                },
            },
            {
                "entity_id": "finance-us:sec",
                "entity_type": "organization",
                "canonical_text": "Securities and Exchange Commission",
                "metadata": {"country_code": "us", "category": "securities_regulator"},
            },
        ],
    )

    result = build_finance_country_proxy_source_lane(
        packs_root=packs_root,
        output_root=tmp_path / "impact_relationships",
        run_id="unit-test",
        build_artifact=True,
        artifact_output_root=tmp_path / "artifacts",
        extra_proxy_pack_ids=(),
    )

    edges = _read_tsv(result.edge_tsv_path)
    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    edge_keys = {(row["source_ref"], row["relation"], row["target_ref"]) for row in edges}
    edges_by_key = {(row["source_ref"], row["relation"], row["target_ref"]): row for row in edges}
    relation_counts = {}
    for row in edges:
        relation_counts[row["relation"]] = relation_counts.get(row["relation"], 0) + 1
    assert result.pack_count == 1
    assert result.entity_count == 5
    assert result.uncovered_entity_count == 0
    assert manifest["relation_counts"] == relation_counts
    assert result.edge_family_counts == manifest["edge_family_counts"]
    assert manifest["edge_family_counts"]["identity_listing"] >= 2
    assert manifest["edge_family_counts"]["person_to_issuer"] >= 2
    assert manifest["edge_family_counts"]["country_macro_policy"] >= 5
    assert (
        "finance-us-issuer:0001",
        "issuer_has_listed_ticker",
        "finance-us-ticker:ABC",
    ) in edge_keys
    assert (
        "finance-us-issuer:0002",
        "issuer_has_listed_ticker",
        "finance-us-ticker:DEF",
    ) in edge_keys
    assert (
        "finance-us-ticker:ABC",
        "ticker_trades_on_exchange",
        "finance-us:nyse",
    ) in edge_keys
    assert (
        "finance-person:0001:chief-executive",
        "person_affects_employer_ticker",
        "finance-us-ticker:ABC",
    ) in edge_keys
    assert (
        "finance-person:0001:chief-executive",
        "person_is_ceo_of_issuer",
        "finance-us-issuer:0001",
    ) in edge_keys
    assert ("country:us", "country_affects_currency_proxy", "ades:impact:currency:usd") in edge_keys
    assert (
        "country:us",
        "country_affects_policy_rate_proxy",
        "ades:impact:rates:us-policy-rate",
    ) in edge_keys
    assert ("finance-us:sec", "entity_affects_dxy_proxy", "ades:impact:indicator:dxy") in edge_keys
    policy_edge = edges_by_key[
        (
            "country:us",
            "country_affects_policy_rate_proxy",
            "ades:impact:rates:us-policy-rate",
        )
    ]
    assert set(policy_edge["compatible_event_types"].split(",")) >= {
        "policy_rate_cut",
        "policy_rate_hike",
        "inflation_shock",
    }
    assert policy_edge["direction_preconditions"] == "rate_inflation_central_bank_or_fiscal_signal"
    risk_edge = edges_by_key[
        (
            "country:us",
            "country_affects_country_risk_proxy",
            "ades:impact:risk:us-country-risk",
        )
    ]
    assert set(risk_edge["compatible_event_types"].split(",")) >= {
        "sanctions",
        "war_escalation",
        "fiscal_expansion",
    }
    assert result.edge_count >= 40
    country_identifiers = json.loads(nodes["country:us"]["identifiers_json"])
    assert country_identifiers["wikidata_qid"] == "Q30"
    assert country_identifiers["geonames_id"] == "6252001"
    assert set(country_identifiers["same_as_refs"]) >= {
        "country:us",
        "wikidata:Q30",
        "geonames:6252001",
    }
    issuer_identifiers = json.loads(nodes["finance-us-issuer:0001"]["identifiers_json"])
    assert issuer_identifiers["ticker_ref"] == "finance-us-ticker:ABC"
    exchange_identifiers = json.loads(nodes["finance-us:nyse"]["identifiers_json"])
    assert exchange_identifiers["country_code"] == "us"
    assert exchange_identifiers["exchange"] == "nyse"
    synthetic_ticker_identifiers = json.loads(nodes["finance-us-ticker:DEF"]["identifiers_json"])
    assert synthetic_ticker_identifiers["issuer_ref"] == "finance-us-issuer:0002"
    assert synthetic_ticker_identifiers["ticker_ref"] == "finance-us-ticker:DEF"
    assert nodes["ades:impact:commodity:copper"]["is_tradable"] == "true"
    assert nodes["ades:impact:commodity:aluminum"]["is_tradable"] == "true"
    assert nodes["ades:impact:commodity:steel"]["is_tradable"] == "true"
    copper_identifiers = json.loads(nodes["ades:impact:commodity:copper"]["identifiers_json"])
    aluminum_identifiers = json.loads(nodes["ades:impact:commodity:aluminum"]["identifiers_json"])
    assert copper_identifiers["futures_symbol"] == "HG"
    assert "COMEX:HG" in copper_identifiers["terminal_proxy_refs"]
    assert aluminum_identifiers["lme_contract_code"] == "AH"
    assert "LME:AH" in aluminum_identifiers["terminal_proxy_refs"]
    assert "CME:ALI" in aluminum_identifiers["terminal_proxy_refs"]

    assert result.artifact_path is not None
    assert result.artifact_edge_count is not None
    assert result.artifact_edge_count > result.edge_count
    assert manifest["include_starter_graph"] is True
    assert manifest["artifact_edge_count"] == result.artifact_edge_count
    connection = sqlite3.connect(str(result.artifact_path))
    try:
        sqlite_edge_count = connection.execute("SELECT COUNT(*) FROM impact_edges").fetchone()[0]
        assert sqlite_edge_count == result.artifact_edge_count
    finally:
        connection.close()

    starter_result = expand_impact_paths(
        ["ades:heuristic_structural_location:finance-en:location:strait-of-hormuz"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=2,
        max_candidates=5,
    )
    assert {candidate.entity_ref for candidate in starter_result.candidates} >= {
        "ades:impact:commodity:crude-oil"
    }

    starter_wikidata_result = expand_impact_paths(
        ["wikidata:Q79883"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=2,
        max_candidates=5,
    )
    assert {candidate.entity_ref for candidate in starter_wikidata_result.candidates} >= {
        "ades:impact:commodity:crude-oil"
    }

    country_result = expand_impact_paths(
        ["country:us"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=1,
        max_candidates=8,
    )
    assert {candidate.entity_ref for candidate in country_result.candidates} >= {
        "ades:impact:currency:usd",
    }
    assert "ades:impact:rates:us-policy-rate" not in {
        candidate.entity_ref for candidate in country_result.candidates
    }
    assert "ades:impact:risk:us-country-risk" not in {
        candidate.entity_ref for candidate in country_result.candidates
    }

    wikidata_country_result = expand_impact_paths(
        ["wikidata:Q30"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=1,
        max_candidates=8,
    )
    assert {candidate.entity_ref for candidate in wikidata_country_result.candidates} >= {
        "ades:impact:currency:usd",
    }
    assert "country:us" in wikidata_country_result.source_entities[0].same_as_refs
    assert "ades:impact:rates:us-policy-rate" not in {
        candidate.entity_ref for candidate in wikidata_country_result.candidates
    }
    assert "ades:impact:risk:us-country-risk" not in {
        candidate.entity_ref for candidate in wikidata_country_result.candidates
    }

    policy_country_result = expand_impact_paths(
        ["wikidata:Q30"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=1,
        max_candidates=8,
        compatible_event_types=["policy_rate_cut"],
    )
    assert "ades:impact:rates:us-policy-rate" in {
        candidate.entity_ref for candidate in policy_country_result.candidates
    }

    sanctions_country_result = expand_impact_paths(
        ["wikidata:Q30"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=1,
        max_candidates=8,
        compatible_event_types=["sanctions"],
    )
    assert {candidate.entity_ref for candidate in sanctions_country_result.candidates} >= {
        "ades:impact:indicator:dxy",
        "ades:impact:risk:us-country-risk",
    }

    issuer_bridge_result = expand_impact_paths(
        ["finance-us-issuer:0002"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=1,
        max_candidates=8,
    )
    assert "finance-us-ticker:DEF" in {
        candidate.entity_ref for candidate in issuer_bridge_result.candidates
    }
    assert "finance-us-ticker:DEF" in issuer_bridge_result.source_entities[0].same_as_refs


def test_finance_country_proxy_generator_builds_generic_role_relationships(
    tmp_path: Path,
) -> None:
    packs_root = tmp_path / "packs"
    packs_root.mkdir()
    _write_pack(
        packs_root,
        "finance-ca-en",
        [
            {
                "entity_id": "finance-ca-issuer:keyera",
                "entity_type": "organization",
                "canonical_text": "Keyera Corp.",
                "metadata": {
                    "country_code": "ca",
                    "category": "issuer",
                    "ticker": "KEY.R",
                    "exchange_code": "TSX",
                    "isin": "CA4932711001",
                    "company_number": "123456",
                },
            },
            {
                "entity_id": "finance-ca-ticker:tsx:key.r",
                "entity_type": "ticker",
                "canonical_text": "KEY.R",
                "metadata": {
                    "country_code": "ca",
                    "category": "ticker",
                    "ticker": "KEY.R",
                    "exchange_code": "TSX",
                    "isin": "CA4932711001",
                    "issuer_name": "Keyera Corp.",
                    "company_number": "123456",
                },
            },
            {
                "entity_id": "finance-ca-person:keyera:ceo",
                "entity_type": "person",
                "canonical_text": "Jane Market",
                "metadata": {
                    "country_code": "ca",
                    "employer_isin": "CA4932711001",
                    "role_title": "Chief Executive Officer",
                    "source_url": "https://example.com/keyera/management",
                },
            },
            {
                "entity_id": "finance-ca-person:keyera:director",
                "entity_type": "person",
                "canonical_text": "Alex Board",
                "metadata": {
                    "country_code": "ca",
                    "employer_company_number": "123456",
                    "role_class": "director",
                    "source_url": "https://example.com/keyera/board",
                },
            },
            {
                "entity_id": "finance-ca-shareholder:keyera:keyera-holdings",
                "entity_type": "organization",
                "canonical_text": "Keyera Holdings Ltd.",
                "metadata": {
                    "country_code": "ca",
                    "category": "shareholder",
                    "employer_ticker": "KEY.R",
                    "exchange_code": "TSX",
                    "source_url": "https://example.com/keyera/shareholders",
                },
            },
            {
                "entity_id": "finance-ca-person:keyera:article-ix",
                "entity_type": "person",
                "canonical_text": "Article IX",
                "metadata": {
                    "country_code": "ca",
                    "employer_ticker": "KEY.R",
                    "exchange_code": "TSX",
                    "role_title": "Executive Officers",
                    "source_url": "https://example.com/keyera/filing",
                },
            },
        ],
    )

    result = build_finance_country_proxy_source_lane(
        packs_root=packs_root,
        output_root=tmp_path / "impact_relationships",
        run_id="generic-role-relationships",
        build_artifact=True,
        artifact_output_root=tmp_path / "artifacts",
        extra_proxy_pack_ids=(),
    )

    edges = _read_tsv(result.edge_tsv_path)
    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    edge_keys = {(row["source_ref"], row["relation"], row["target_ref"]) for row in edges}
    assert (
        "finance-ca-issuer:keyera",
        "issuer_has_listed_ticker",
        "finance-ca-ticker:tsx:key.r",
    ) in edge_keys
    assert (
        "finance-ca-person:keyera:ceo",
        "person_is_ceo_of_issuer",
        "finance-ca-issuer:keyera",
    ) in edge_keys
    assert (
        "finance-ca-person:keyera:ceo",
        "person_affects_employer_ticker",
        "finance-ca-ticker:tsx:key.r",
    ) in edge_keys
    assert (
        "finance-ca-person:keyera:director",
        "person_is_board_member_of_issuer",
        "finance-ca-issuer:keyera",
    ) in edge_keys
    assert (
        "finance-ca-shareholder:keyera:keyera-holdings",
        "holding_company_controls_public_issuer",
        "finance-ca-issuer:keyera",
    ) in edge_keys
    assert not any(
        row["source_ref"] == "finance-ca-person:keyera:article-ix"
        and row["relation"]
        in {
            "person_affects_employer_ticker",
            "person_affects_employer_issuer",
            "person_is_ceo_of_issuer",
        }
        for row in edges
    )
    issuer_identifiers = json.loads(nodes["finance-ca-issuer:keyera"]["identifiers_json"])
    assert issuer_identifiers["ticker_ref"] == "finance-ca-ticker:tsx:key.r"

    assert result.artifact_path is not None
    ceo_result = expand_impact_paths(
        ["finance-ca-person:keyera:ceo"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=2,
        max_candidates=8,
        compatible_event_types=["key_person_ownership_governance"],
    )
    assert "finance-ca-ticker:tsx:key.r" in {
        candidate.entity_ref for candidate in ceo_result.candidates
    }


def test_finance_country_proxy_generator_builds_supply_chain_and_sector_edges(
    tmp_path: Path,
) -> None:
    packs_root = tmp_path / "packs"
    packs_root.mkdir()
    _write_pack(
        packs_root,
        "finance-us-en",
        [
            {
                "entity_id": "finance-us-issuer:aide",
                "entity_type": "organization",
                "canonical_text": "AI Device Maker Inc.",
                "metadata": {
                    "country_code": "us",
                    "category": "issuer",
                    "ticker": "AIDE",
                    "sector": "Technology",
                    "indices": ["S&P 500"],
                    "supplier_ticker": "CHIP",
                    "source_url": "https://example.com/aide/profile",
                },
            },
            {
                "entity_id": "finance-us-ticker:AIDE",
                "entity_type": "ticker",
                "canonical_text": "AIDE",
                "metadata": {
                    "country_code": "us",
                    "category": "ticker",
                    "ticker": "AIDE",
                    "issuer_name": "AI Device Maker Inc.",
                },
            },
            {
                "entity_id": "finance-us-issuer:chipco",
                "entity_type": "organization",
                "canonical_text": "ChipCo Inc.",
                "metadata": {
                    "country_code": "us",
                    "category": "issuer",
                    "ticker": "CHIP",
                    "sector": "Semiconductors",
                },
            },
            {
                "entity_id": "finance-us-ticker:CHIP",
                "entity_type": "ticker",
                "canonical_text": "CHIP",
                "metadata": {
                    "country_code": "us",
                    "category": "ticker",
                    "ticker": "CHIP",
                    "issuer_name": "ChipCo Inc.",
                },
            },
            {
                "entity_id": "finance-us-supplier:aide:chipco",
                "entity_type": "organization",
                "canonical_text": "ChipCo Supply Division",
                "metadata": {
                    "country_code": "us",
                    "category": "supplier",
                    "related_ticker": "AIDE",
                    "relationship": "supplier",
                    "source_url": "https://example.com/aide/suppliers",
                },
            },
        ],
    )

    result = build_finance_country_proxy_source_lane(
        packs_root=packs_root,
        output_root=tmp_path / "impact_relationships",
        run_id="supply-chain-and-sector-relationships",
        build_artifact=True,
        artifact_output_root=tmp_path / "artifacts",
        extra_proxy_pack_ids=(),
    )

    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    edges = _read_tsv(result.edge_tsv_path)
    edge_keys = {(row["source_ref"], row["relation"], row["target_ref"]) for row in edges}
    assert "finance-us-sector:technology" in nodes
    assert "finance-us-index:s-p-500" in nodes
    assert (
        "finance-us-issuer:chipco",
        "issuer_supplier_to_issuer",
        "finance-us-issuer:aide",
    ) in edge_keys
    assert (
        "finance-us-supplier:aide:chipco",
        "issuer_supplier_to_issuer",
        "finance-us-issuer:aide",
    ) in edge_keys
    assert (
        "finance-us-issuer:aide",
        "issuer_in_sector",
        "finance-us-sector:technology",
    ) in edge_keys
    assert (
        "finance-us-sector:technology",
        "sector_affects_index",
        "ades:impact:index:us-market",
    ) in edge_keys
    assert (
        "finance-us-issuer:aide",
        "issuer_in_index",
        "finance-us-index:s-p-500",
    ) in edge_keys
    assert (
        "finance-us-index:s-p-500",
        "index_affects_country_index_proxy",
        "ades:impact:index:us-market",
    ) in edge_keys
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["edge_family_counts"]["sector_exposure"] >= 2
    assert manifest["edge_family_counts"]["index_membership"] >= 2
    assert manifest["edge_family_counts"]["supply_chain_counterparty"] >= 2

    assert result.artifact_path is not None
    supplier_result = expand_impact_paths(
        ["finance-us-issuer:chipco"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=2,
        max_candidates=8,
        compatible_event_types=["supply_disruption"],
    )
    assert "finance-us-ticker:AIDE" in {
        candidate.entity_ref for candidate in supplier_result.candidates
    }

    sector_result = expand_impact_paths(
        ["finance-us-sector:technology"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=1,
        max_candidates=8,
        compatible_event_types=["tariff"],
    )
    assert "ades:impact:index:us-market" in {
        candidate.entity_ref for candidate in sector_result.candidates
    }

    sector_policy_result = expand_impact_paths(
        ["finance-us-sector:technology"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=1,
        max_candidates=8,
        compatible_event_types=["sector_policy_change"],
    )
    assert "ades:impact:index:us-market" in {
        candidate.entity_ref for candidate in sector_policy_result.candidates
    }


def test_finance_country_proxy_generator_does_not_synthesize_role_target_issuer(
    tmp_path: Path,
) -> None:
    packs_root = tmp_path / "packs"
    packs_root.mkdir()
    _write_pack(
        packs_root,
        "finance-cn-en",
        [
            {
                "entity_id": "finance-cn-shareholder-org:visionox:controller",
                "entity_type": "organization",
                "canonical_text": "Visionox Holding Ltd.",
                "metadata": {
                    "country_code": "cn",
                    "category": "shareholder",
                    "employer_ticker": "002387",
                    "exchange_code": "SZSE",
                    "source_url": "https://example.com/visionox/shareholders",
                },
            },
            {
                "entity_id": "finance-cn-issuer:002387",
                "entity_type": "organization",
                "canonical_text": "Visionox Technology Inc.",
                "metadata": {
                    "country_code": "cn",
                    "category": "issuer",
                    "ticker": "002387",
                    "exchange_code": "SZSE",
                },
            },
        ],
    )

    result = build_finance_country_proxy_source_lane(
        packs_root=packs_root,
        output_root=tmp_path / "impact_relationships",
        run_id="role-target-issuer-ordering",
        extra_proxy_pack_ids=(),
    )

    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    edges = _read_tsv(result.edge_tsv_path)
    assert nodes["finance-cn-issuer:002387"]["entity_type"] == "organization"
    assert nodes["finance-cn-issuer:002387"]["is_tradable"] == "false"
    assert nodes["finance-cn-ticker:002387"]["entity_type"] == "ticker"
    assert any(
        row["source_ref"] == "finance-cn-shareholder-org:visionox:controller"
        and row["relation"] == "holding_company_controls_public_issuer"
        and row["target_ref"] == "finance-cn-issuer:002387"
        for row in edges
    )


def test_finance_country_proxy_generator_can_fallback_to_aliases(tmp_path: Path) -> None:
    packs_root = tmp_path / "packs"
    pack_dir = packs_root / "finance-uk-en"
    pack_dir.mkdir(parents=True)
    (pack_dir / "aliases.json").write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "entity_id": "finance-uk:ftse-100",
                        "canonical_text": "FTSE 100",
                        "label": "market_index",
                        "text": "FTSE 100",
                    },
                    {
                        "entity_id": "finance-uk:regulator",
                        "canonical_text": "UK regulator",
                        "label": "organization",
                        "text": "UK regulator",
                    },
                ]
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    assert discover_finance_country_pack_dirs(packs_root) == [pack_dir.resolve()]
    entities = load_finance_country_pack_entities(pack_dir)
    assert {entity.entity_ref for entity in entities} == {
        "finance-uk:ftse-100",
        "finance-uk:regulator",
    }

    result = build_finance_country_proxy_source_lane(
        packs_root=packs_root,
        output_root=tmp_path / "impact_relationships",
        run_id="alias-fallback",
        extra_proxy_pack_ids=(),
    )

    edges = _read_tsv(result.edge_tsv_path)
    assert result.entity_count == 2
    assert result.uncovered_entity_count == 0
    assert any(
        row["source_ref"] == "finance-uk:regulator"
        and row["target_ref"] == "ades:impact:currency:gbp"
        and row["relation"] == "entity_affects_currency_proxy"
        for row in edges
    )


def test_domain_pack_proxy_generator_links_political_entities_to_global_assets(
    tmp_path: Path,
) -> None:
    packs_root = tmp_path / "packs"
    packs_root.mkdir()
    _write_pack(
        packs_root,
        "finance-us-en",
        [
            {
                "entity_id": "finance-us-issuer:0001",
                "entity_type": "organization",
                "canonical_text": "Example Issuer Inc.",
                "metadata": {"country_code": "us", "category": "issuer", "ticker": "ABC"},
            }
        ],
    )
    _write_alias_pack(
        packs_root,
        "politics-vector-en",
        [
            {
                "entity_id": "wikidata:QTEST",
                "label": "person",
                "canonical_text": "Example Minister",
                "text": "Example Minister",
            }
        ],
    )

    result = build_finance_country_proxy_source_lane(
        packs_root=packs_root,
        output_root=tmp_path / "impact_relationships",
        run_id="domain-proxy",
        extra_proxy_pack_ids=("politics-vector-en",),
    )

    edges = _read_tsv(result.edge_tsv_path)
    assert result.extra_proxy_pack_count == 1
    assert result.extra_proxy_entity_count == 1
    assert {
        (row["source_ref"], row["relation"], row["target_ref"])
        for row in edges
        if row["source_ref"] == "wikidata:QTEST"
    } >= {
        ("wikidata:QTEST", "entity_affects_usd_proxy", "ades:impact:currency:usd"),
        ("wikidata:QTEST", "entity_affects_dxy_proxy", "ades:impact:indicator:dxy"),
        (
            "wikidata:QTEST",
            "entity_affects_global_policy_risk_proxy",
            "ades:impact:risk:global-policy-risk",
        ),
    }
