import csv
from pathlib import Path

from ades.impact.graph_store import MarketGraphStore
from ades.impact.issuer_exposure import build_issuer_exposure_source_lane


def _write_tsv(path: Path, columns: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(columns)
        writer.writerows(rows)


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def test_issuer_exposure_builds_commodity_geography_and_supply_chain_paths(
    tmp_path: Path,
) -> None:
    commodity_path = tmp_path / "issuer_commodity.tsv"
    geography_path = tmp_path / "issuer_geography.tsv"
    supply_chain_path = tmp_path / "issuer_supply_chain.tsv"
    common_source_columns = [
        "source_name",
        "source_url",
        "source_snapshot",
        "source_year",
        "refresh_policy",
        "pack_ids",
        "notes",
    ]
    _write_tsv(
        commodity_path,
        [
            "issuer_ref",
            "issuer_name",
            "commodity_ref",
            "commodity_name",
            "exposure_type",
            "ticker_ref",
            "ticker_symbol",
            "jurisdiction",
            *common_source_columns,
        ],
        [
            [
                "finance-au-issuer:newcrest-mining",
                "Newcrest Mining",
                "commodity:XAU",
                "Gold",
                "producer_revenue",
                "ASX:NCM",
                "NCM",
                "AU",
                "Newcrest annual report",
                "https://example.com/newcrest-annual-report",
                "2026-06-28",
                "2026",
                "annual",
                "finance-au-en",
                "gold revenue exposure",
            ],
        ],
    )
    _write_tsv(
        geography_path,
        [
            "issuer_ref",
            "issuer_name",
            "country_ref",
            "country_name",
            "geography_type",
            "ticker_ref",
            "ticker_symbol",
            "jurisdiction",
            *common_source_columns,
        ],
        [
            [
                "finance-ca-issuer:barrick-gold",
                "Barrick Gold",
                "country:MLI",
                "Mali",
                "country",
                "NYSE:GOLD",
                "GOLD",
                "CA",
                "Barrick annual report",
                "https://example.com/barrick-annual-report",
                "2026-06-28",
                "2026",
                "annual",
                "finance-ca-en",
                "country operations exposure",
            ],
        ],
    )
    _write_tsv(
        supply_chain_path,
        [
            "source_issuer_ref",
            "source_issuer_name",
            "target_issuer_ref",
            "target_issuer_name",
            "relationship_type",
            "source_ticker_ref",
            "source_ticker_symbol",
            "target_ticker_ref",
            "target_ticker_symbol",
            "jurisdiction",
            *common_source_columns,
        ],
        [
            [
                "finance-us-issuer:tsmc",
                "TSMC",
                "finance-us-issuer:apple",
                "Apple Inc.",
                "supplier",
                "NYSE:TSM",
                "TSM",
                "NASDAQ:AAPL",
                "AAPL",
                "US",
                "Apple supplier disclosure",
                "https://example.com/apple-suppliers",
                "2026-06-28",
                "2026",
                "annual",
                "finance-us-en",
                "source-backed supplier relationship",
            ],
        ],
    )

    result = build_issuer_exposure_source_lane(
        issuer_commodity_tsv_paths=[commodity_path],
        issuer_geography_tsv_paths=[geography_path],
        issuer_supply_chain_tsv_paths=[supply_chain_path],
        output_root=tmp_path / "impact_relationships",
        run_id="issuer-exposure",
        build_artifact=True,
        include_starter_graph=False,
        artifact_output_root=tmp_path / "artifacts",
    )

    assert result.issuer_commodity_row_count == 1
    assert result.issuer_geography_row_count == 1
    assert result.issuer_supply_chain_row_count == 1
    assert result.node_count == 10
    assert result.edge_count == 8
    assert result.artifact_node_count == 10
    assert result.artifact_edge_count == 8
    assert result.relation_counts == {
        "commodity_affects_issuer_revenue": 1,
        "issuer_exposed_to_commodity": 1,
        "issuer_exposed_to_country": 1,
        "issuer_has_listed_ticker": 4,
        "issuer_supplier_to_issuer": 1,
    }

    edges = _read_tsv(result.edge_tsv_path)
    edge_keys = {(row["source_ref"], row["relation"], row["target_ref"]) for row in edges}
    assert (
        "commodity:XAU",
        "commodity_affects_issuer_revenue",
        "finance-au-issuer:newcrest-mining",
    ) in edge_keys
    assert (
        "country:MLI",
        "issuer_exposed_to_country",
        "finance-ca-issuer:barrick-gold",
    ) in edge_keys
    assert (
        "finance-us-issuer:tsmc",
        "issuer_supplier_to_issuer",
        "finance-us-issuer:apple",
    ) in edge_keys
    commodity_edge = next(
        row for row in edges if row["relation"] == "commodity_affects_issuer_revenue"
    )
    country_edge = next(row for row in edges if row["relation"] == "issuer_exposed_to_country")
    assert "commodity_price_move" in commodity_edge["compatible_event_types"].split(",")
    assert "war_escalation" in country_edge["compatible_event_types"].split(",")
    assert "source_backed_issuer_exposure" in commodity_edge["direction_preconditions"].split(",")

    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    assert nodes["ASX:NCM"]["is_tradable"] == "true"
    assert nodes["commodity:XAU"]["entity_type"] == "commodity"
    assert nodes["country:MLI"]["entity_type"] == "country"

    assert result.artifact_path is not None
    with MarketGraphStore(result.artifact_path) as store:
        commodity_edges = store.outbound_edges_batch(["commodity:XAU"])["commodity:XAU"]
        country_edges = store.outbound_edges_batch(["country:MLI"])["country:MLI"]
        supplier_edges = store.outbound_edges_batch(["finance-us-issuer:tsmc"])[
            "finance-us-issuer:tsmc"
        ]

    assert commodity_edges[0].target_ref == "finance-au-issuer:newcrest-mining"
    assert country_edges[0].target_ref == "finance-ca-issuer:barrick-gold"
    assert any(edge.target_ref == "finance-us-issuer:apple" for edge in supplier_edges)


def test_issuer_exposure_generates_stable_refs_when_missing(tmp_path: Path) -> None:
    commodity_path = tmp_path / "issuer_commodity.tsv"
    _write_tsv(
        commodity_path,
        [
            "issuer_name",
            "commodity_name",
            "ticker_symbol",
            "jurisdiction",
            "source_name",
            "source_url",
            "source_snapshot",
            "pack_ids",
        ],
        [
            [
                "Rare Earths Producer plc",
                "Neodymium",
                "REP",
                "UK",
                "Issuer annual report",
                "https://example.com/rare-earths-annual-report",
                "2026-06-28",
                "finance-uk-en",
            ],
        ],
    )

    result = build_issuer_exposure_source_lane(
        issuer_commodity_tsv_paths=[commodity_path],
        output_root=tmp_path / "impact_relationships",
        run_id="stable-refs",
    )

    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    assert "ades:issuer-exposure:uk:issuer:rare-earths-producer-plc" in nodes
    assert "ades:issuer-exposure:global:commodity:neodymium" in nodes
    assert "ades:issuer-exposure:uk:ticker:rep" in nodes
    edges = _read_tsv(result.edge_tsv_path)
    assert {row["relation"] for row in edges} == {
        "commodity_affects_issuer_revenue",
        "issuer_exposed_to_commodity",
        "issuer_has_listed_ticker",
    }
