import csv
from pathlib import Path

from ades.impact.graph_store import MarketGraphStore
from ades.impact.policy_sector_proxy import build_policy_sector_source_lane


def _write_tsv(path: Path, columns: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(columns)
        writer.writerows(rows)


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def test_policy_sector_proxy_builds_law_sector_issuer_ticker_path(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "policy_sector.tsv"
    issuer_path = tmp_path / "issuer_sector.tsv"
    _write_tsv(
        policy_path,
        [
            "policy_ref",
            "policy_name",
            "policy_type",
            "sector_ref",
            "sector_name",
            "jurisdiction",
            "source_name",
            "source_url",
            "source_snapshot",
            "source_year",
            "refresh_policy",
            "pack_ids",
            "notes",
        ],
        [
            [
                "ades:uk-law:mining-royalties",
                "UK mining royalties bill",
                "law",
                "ades:sector:mining",
                "Mining",
                "UK",
                "UK Parliament",
                "https://bills.parliament.uk/bills/example-mining-royalties",
                "2026-06-28",
                "2026",
                "monthly",
                "finance-uk-en",
                "policy affects listed mining companies",
            ],
        ],
    )
    _write_tsv(
        issuer_path,
        [
            "issuer_ref",
            "issuer_name",
            "sector_ref",
            "sector_name",
            "ticker_ref",
            "ticker_symbol",
            "jurisdiction",
            "source_name",
            "source_url",
            "source_snapshot",
            "source_year",
            "refresh_policy",
            "pack_ids",
            "notes",
        ],
        [
            [
                "finance-uk-issuer:anglo-american",
                "Anglo American plc",
                "ades:sector:mining",
                "Mining",
                "LSE:AAL",
                "AAL",
                "UK",
                "London Stock Exchange",
                "https://www.londonstockexchange.com/stock/AAL/anglo-american-plc/company-page",
                "2026-06-28",
                "2026",
                "monthly",
                "finance-uk-en",
                "issuer sector classification",
            ],
        ],
    )

    result = build_policy_sector_source_lane(
        policy_sector_tsv_paths=[policy_path],
        issuer_sector_tsv_paths=[issuer_path],
        output_root=tmp_path / "impact_relationships",
        run_id="policy-sector",
        build_artifact=True,
        include_starter_graph=False,
        artifact_output_root=tmp_path / "artifacts",
    )

    assert result.policy_sector_row_count == 1
    assert result.issuer_sector_row_count == 1
    assert result.node_count == 4
    assert result.edge_count == 3
    assert result.artifact_node_count == 4
    assert result.artifact_edge_count == 3
    assert result.relation_counts == {
        "issuer_has_listed_ticker": 1,
        "law_affects_sector": 1,
        "sector_affects_issuer": 1,
    }

    edges = _read_tsv(result.edge_tsv_path)
    edge_keys = {(row["source_ref"], row["relation"], row["target_ref"]) for row in edges}
    assert (
        "ades:uk-law:mining-royalties",
        "law_affects_sector",
        "ades:sector:mining",
    ) in edge_keys
    assert (
        "ades:sector:mining",
        "sector_affects_issuer",
        "finance-uk-issuer:anglo-american",
    ) in edge_keys
    assert (
        "finance-uk-issuer:anglo-american",
        "issuer_has_listed_ticker",
        "LSE:AAL",
    ) in edge_keys
    law_edge = next(row for row in edges if row["relation"] == "law_affects_sector")
    assert "sector_policy_change" in law_edge["compatible_event_types"].split(",")
    assert "sector_policy_event_signal" in law_edge["direction_preconditions"].split(",")

    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    assert nodes["LSE:AAL"]["is_tradable"] == "true"
    assert nodes["ades:uk-law:mining-royalties"]["is_seed_eligible"] == "true"

    assert result.artifact_path is not None
    with MarketGraphStore(result.artifact_path) as store:
        policy_edges = store.outbound_edges_batch(["ades:uk-law:mining-royalties"])[
            "ades:uk-law:mining-royalties"
        ]
        sector_edges = store.outbound_edges_batch(["ades:sector:mining"])["ades:sector:mining"]
        issuer_edges = store.outbound_edges_batch(["finance-uk-issuer:anglo-american"])[
            "finance-uk-issuer:anglo-american"
        ]

    assert policy_edges[0].target_ref == "ades:sector:mining"
    assert sector_edges[0].target_ref == "finance-uk-issuer:anglo-american"
    assert issuer_edges[0].target_ref == "LSE:AAL"


def test_policy_sector_proxy_generates_stable_refs_when_missing(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "policy_sector.tsv"
    _write_tsv(
        policy_path,
        [
            "policy_name",
            "policy_type",
            "sector_name",
            "jurisdiction",
            "source_name",
            "source_url",
            "source_snapshot",
            "pack_ids",
        ],
        [
            [
                "Critical minerals act",
                "government_body",
                "Rare earth mining",
                "UK",
                "UK legislation",
                "https://www.legislation.gov.uk/example",
                "2026-06-28",
                "finance-uk-en",
            ],
        ],
    )

    result = build_policy_sector_source_lane(
        policy_sector_tsv_paths=[policy_path],
        output_root=tmp_path / "impact_relationships",
        run_id="stable-refs",
    )

    nodes = {row["entity_ref"]: row for row in _read_tsv(result.node_tsv_path)}
    assert "ades:policy-sector:uk:government_body:critical-minerals-act" in nodes
    assert "ades:policy-sector:uk:sector:rare-earth-mining" in nodes
    edges = _read_tsv(result.edge_tsv_path)
    assert edges[0]["relation"] == "government_body_affects_sector"
