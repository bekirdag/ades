"""Build issuer exposure source lanes for the market impact graph."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Iterable

from .graph_builder import build_market_graph_store
from .policy_sector_proxy import (
    DEFAULT_ARTIFACT_OUTPUT_ROOT,
    DEFAULT_BIG_DATA_ROOT,
    EDGE_COLUMNS,
    NODE_COLUMNS,
    _edge_row,
    _entity_ref,
    _node_row,
    _read_tsv_rows,
    _ticker_ref,
    _write_tsv,
)
from .starter import starter_source_paths

DEFAULT_SOURCE_OUTPUT_ROOT = (
    DEFAULT_BIG_DATA_ROOT / "pack_sources" / "impact_relationships" / "issuer_exposure"
)
DEFAULT_PACK_ID = "issuer-exposure-en"


@dataclass(frozen=True)
class IssuerExposureBuildResult:
    run_id: str
    output_dir: Path
    node_tsv_path: Path
    edge_tsv_path: Path
    manifest_path: Path
    artifact_path: Path | None
    artifact_node_count: int | None
    artifact_edge_count: int | None
    issuer_commodity_row_count: int
    issuer_geography_row_count: int
    issuer_supply_chain_row_count: int
    node_count: int
    edge_count: int
    relation_counts: dict[str, int]

    def to_json_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "output_dir": str(self.output_dir),
            "node_tsv_path": str(self.node_tsv_path),
            "edge_tsv_path": str(self.edge_tsv_path),
            "manifest_path": str(self.manifest_path),
            "artifact_path": str(self.artifact_path) if self.artifact_path else None,
            "artifact_node_count": self.artifact_node_count,
            "artifact_edge_count": self.artifact_edge_count,
            "issuer_commodity_row_count": self.issuer_commodity_row_count,
            "issuer_geography_row_count": self.issuer_geography_row_count,
            "issuer_supply_chain_row_count": self.issuer_supply_chain_row_count,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "relation_counts": self.relation_counts,
        }


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _coalesce(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = _normalize_text(row.get(key))
        if value:
            return value
    return ""


def _parse_list(value: object | None) -> tuple[str, ...]:
    raw = _normalize_text(value)
    if not raw:
        return ()
    return tuple(item.strip() for item in re.split(r"[,|]", raw) if item.strip())


def _pack_ids(row: dict[str, str]) -> tuple[str, ...]:
    return _parse_list(_coalesce(row, "pack_ids", "packs")) or (DEFAULT_PACK_ID,)


def _node_identifiers(
    row: dict[str, str],
    *,
    jurisdiction: str = "",
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    identifiers: dict[str, object] = {}
    for key in ("lei", "isin", "figi", "cik", "exchange", "mic", "ticker_symbol", "symbol"):
        value = _coalesce(row, key)
        if value:
            identifiers[key] = value
    if jurisdiction:
        identifiers["jurisdiction"] = jurisdiction
    if extra:
        identifiers.update({key: value for key, value in extra.items() if value})
    return identifiers


def _issuer_ref(row: dict[str, str], *, namespace: str, jurisdiction: str) -> str:
    issuer_name = _coalesce(
        row,
        "issuer_name",
        "company_name",
        "source_issuer_name",
        "source_company_name",
    )
    if not issuer_name:
        raise ValueError("Issuer exposure rows require issuer_name or company_name.")
    return _entity_ref(
        provided=_coalesce(row, "issuer_ref", "company_ref", "source_issuer_ref"),
        namespace=namespace,
        entity_type="issuer",
        name=issuer_name,
        jurisdiction=jurisdiction,
    )


def _add_issuer_node(
    row: dict[str, str],
    *,
    nodes: dict[str, dict[str, str]],
    namespace: str,
    jurisdiction: str,
    issuer_ref: str | None = None,
    issuer_name_keys: tuple[str, ...] = ("issuer_name", "company_name"),
) -> str:
    pack_ids = _pack_ids(row)
    name = _coalesce(row, *issuer_name_keys)
    if not name:
        raise ValueError("Issuer rows require an issuer name.")
    resolved_ref = issuer_ref or _entity_ref(
        provided=_coalesce(row, "issuer_ref", "company_ref"),
        namespace=namespace,
        entity_type="issuer",
        name=name,
        jurisdiction=jurisdiction,
    )
    nodes.setdefault(
        resolved_ref,
        _node_row(
            entity_ref=resolved_ref,
            canonical_name=name,
            entity_type="issuer",
            pack_ids=pack_ids,
            identifiers=_node_identifiers(row, jurisdiction=jurisdiction),
        ),
    )
    return resolved_ref


def _add_ticker_bridge(
    row: dict[str, str],
    *,
    nodes: dict[str, dict[str, str]],
    edges: dict[tuple[str, str, str, str], dict[str, str]],
    issuer_ref: str,
    namespace: str,
    jurisdiction: str,
    key_prefix: str = "",
) -> None:
    ticker_row = dict(row)
    if key_prefix:
        for source_key, target_key in (
            (f"{key_prefix}_ticker_ref", "ticker_ref"),
            (f"{key_prefix}_asset_ref", "asset_ref"),
            (f"{key_prefix}_terminal_ref", "terminal_ref"),
            (f"{key_prefix}_ticker_symbol", "ticker_symbol"),
            (f"{key_prefix}_symbol", "symbol"),
            (f"{key_prefix}_ticker_name", "ticker_name"),
        ):
            if source_key in row:
                ticker_row[target_key] = row[source_key]
    ticker_ref = _ticker_ref(ticker_row, namespace=namespace, jurisdiction=jurisdiction)
    if not ticker_ref:
        return
    pack_ids = _pack_ids(row)
    ticker_name = _coalesce(ticker_row, "ticker_name", "ticker_symbol", "symbol") or ticker_ref
    nodes.setdefault(
        ticker_ref,
        _node_row(
            entity_ref=ticker_ref,
            canonical_name=ticker_name,
            entity_type="ticker",
            pack_ids=pack_ids,
            identifiers=_node_identifiers(ticker_row, jurisdiction=jurisdiction),
            is_tradable=True,
            is_seed_eligible=False,
        ),
    )
    edge = _edge_row(
        source_ref=issuer_ref,
        target_ref=ticker_ref,
        relation="issuer_has_listed_ticker",
        row=row,
        pack_ids=pack_ids,
        direction_hint="direct_listing",
        notes="issuer exposure row ticker bridge",
    )
    edges[(issuer_ref, ticker_ref, "issuer_has_listed_ticker", edge["source_snapshot"])] = edge


def _commodity_relation(row: dict[str, str]) -> str:
    explicit = _coalesce(row, "impact_relation", "commodity_relation")
    if explicit:
        return explicit
    exposure_type = _coalesce(row, "exposure_type", "commodity_exposure_type").casefold()
    if any(token in exposure_type for token in ("cost", "margin", "input", "consumer")):
        return "commodity_affects_issuer_margin"
    return "commodity_affects_issuer_revenue"


def _add_issuer_commodity_row(
    row: dict[str, str],
    *,
    nodes: dict[str, dict[str, str]],
    edges: dict[tuple[str, str, str, str], dict[str, str]],
    namespace: str,
) -> None:
    pack_ids = _pack_ids(row)
    jurisdiction = _coalesce(row, "jurisdiction", "country", "country_code")
    issuer_ref = _issuer_ref(row, namespace=namespace, jurisdiction=jurisdiction)
    issuer_name = _coalesce(row, "issuer_name", "company_name")
    commodity_name = _coalesce(row, "commodity_name", "commodity", "target_name")
    if not commodity_name:
        raise ValueError("Issuer-commodity rows require commodity_name or commodity.")
    commodity_ref = _entity_ref(
        provided=_coalesce(row, "commodity_ref", "target_ref", "target_entity_ref"),
        namespace=namespace,
        entity_type="commodity",
        name=commodity_name,
        jurisdiction="global",
    )
    nodes.setdefault(
        issuer_ref,
        _node_row(
            entity_ref=issuer_ref,
            canonical_name=issuer_name,
            entity_type="issuer",
            pack_ids=pack_ids,
            identifiers=_node_identifiers(row, jurisdiction=jurisdiction),
        ),
    )
    nodes.setdefault(
        commodity_ref,
        _node_row(
            entity_ref=commodity_ref,
            canonical_name=commodity_name,
            entity_type="commodity",
            pack_ids=pack_ids,
            identifiers=_node_identifiers(row, extra={"commodity": commodity_name}),
        ),
    )
    exposure_edge = _edge_row(
        source_ref=issuer_ref,
        target_ref=commodity_ref,
        relation="issuer_exposed_to_commodity",
        row=row,
        pack_ids=pack_ids,
        direction_hint="issuer_commodity_exposure",
    )
    edges[
        (issuer_ref, commodity_ref, "issuer_exposed_to_commodity", exposure_edge["source_snapshot"])
    ] = exposure_edge
    impact_relation = _commodity_relation(row)
    impact_edge = _edge_row(
        source_ref=commodity_ref,
        target_ref=issuer_ref,
        relation=impact_relation,
        row=row,
        pack_ids=pack_ids,
        direction_hint=_coalesce(row, "direction_hint") or "commodity_exposure",
    )
    edges[(commodity_ref, issuer_ref, impact_relation, impact_edge["source_snapshot"])] = (
        impact_edge
    )
    _add_ticker_bridge(
        row,
        nodes=nodes,
        edges=edges,
        issuer_ref=issuer_ref,
        namespace=namespace,
        jurisdiction=jurisdiction,
    )


def _geography_type(row: dict[str, str]) -> str:
    explicit = _coalesce(row, "geography_type", "geo_type", "entity_type").casefold()
    if explicit in {"region", "country"}:
        return explicit
    if _coalesce(row, "region_name", "region_ref"):
        return "region"
    return "country"


def _add_issuer_geography_row(
    row: dict[str, str],
    *,
    nodes: dict[str, dict[str, str]],
    edges: dict[tuple[str, str, str, str], dict[str, str]],
    namespace: str,
) -> None:
    pack_ids = _pack_ids(row)
    jurisdiction = _coalesce(row, "jurisdiction", "country_code", "country")
    geography_type = _geography_type(row)
    geography_name = _coalesce(
        row,
        f"{geography_type}_name",
        "geography_name",
        "country_name",
        "region_name",
        "target_name",
    )
    if not geography_name:
        raise ValueError("Issuer-geography rows require country/region/geography name.")
    issuer_ref = _issuer_ref(row, namespace=namespace, jurisdiction=jurisdiction)
    issuer_name = _coalesce(row, "issuer_name", "company_name")
    geography_ref = _entity_ref(
        provided=_coalesce(
            row,
            f"{geography_type}_ref",
            "geography_ref",
            "target_ref",
            "target_entity_ref",
        ),
        namespace=namespace,
        entity_type=geography_type,
        name=geography_name,
        jurisdiction=jurisdiction,
    )
    nodes.setdefault(
        issuer_ref,
        _node_row(
            entity_ref=issuer_ref,
            canonical_name=issuer_name,
            entity_type="issuer",
            pack_ids=pack_ids,
            identifiers=_node_identifiers(row, jurisdiction=jurisdiction),
        ),
    )
    nodes.setdefault(
        geography_ref,
        _node_row(
            entity_ref=geography_ref,
            canonical_name=geography_name,
            entity_type=geography_type,
            pack_ids=pack_ids,
            identifiers=_node_identifiers(row, jurisdiction=jurisdiction),
        ),
    )
    relation = _coalesce(row, "relation", "relation_type") or f"issuer_exposed_to_{geography_type}"
    edge = _edge_row(
        source_ref=geography_ref,
        target_ref=issuer_ref,
        relation=relation,
        row=row,
        pack_ids=pack_ids,
        direction_hint=_coalesce(row, "direction_hint") or "geography_exposure",
    )
    edges[(geography_ref, issuer_ref, relation, edge["source_snapshot"])] = edge
    _add_ticker_bridge(
        row,
        nodes=nodes,
        edges=edges,
        issuer_ref=issuer_ref,
        namespace=namespace,
        jurisdiction=jurisdiction,
    )


def _supply_chain_relation(row: dict[str, str]) -> str:
    explicit = _coalesce(row, "relation", "relation_type")
    if explicit:
        return explicit
    relationship_type = _coalesce(row, "relationship_type", "exposure_type").casefold()
    if "customer" in relationship_type:
        return "issuer_customer_of_issuer"
    if "partner" in relationship_type:
        return "issuer_partner_of_issuer"
    return "issuer_supplier_to_issuer"


def _add_issuer_supply_chain_row(
    row: dict[str, str],
    *,
    nodes: dict[str, dict[str, str]],
    edges: dict[tuple[str, str, str, str], dict[str, str]],
    namespace: str,
) -> None:
    pack_ids = _pack_ids(row)
    jurisdiction = _coalesce(row, "jurisdiction", "country", "country_code")
    source_name = _coalesce(
        row,
        "source_issuer_name",
        "supplier_name",
        "issuer_name",
        "company_name",
    )
    target_name = _coalesce(
        row,
        "target_issuer_name",
        "customer_name",
        "partner_name",
        "counterparty_name",
    )
    if not source_name or not target_name:
        raise ValueError("Supply-chain rows require source and target issuer names.")
    source_ref = _entity_ref(
        provided=_coalesce(row, "source_issuer_ref", "source_ref", "supplier_ref"),
        namespace=namespace,
        entity_type="issuer",
        name=source_name,
        jurisdiction=jurisdiction,
    )
    target_ref = _entity_ref(
        provided=_coalesce(row, "target_issuer_ref", "target_ref", "customer_ref", "partner_ref"),
        namespace=namespace,
        entity_type="issuer",
        name=target_name,
        jurisdiction=jurisdiction,
    )
    nodes.setdefault(
        source_ref,
        _node_row(
            entity_ref=source_ref,
            canonical_name=source_name,
            entity_type="issuer",
            pack_ids=pack_ids,
            identifiers=_node_identifiers(row, jurisdiction=jurisdiction),
        ),
    )
    nodes.setdefault(
        target_ref,
        _node_row(
            entity_ref=target_ref,
            canonical_name=target_name,
            entity_type="issuer",
            pack_ids=pack_ids,
            identifiers=_node_identifiers(row, jurisdiction=jurisdiction),
        ),
    )
    relation = _supply_chain_relation(row)
    edge = _edge_row(
        source_ref=source_ref,
        target_ref=target_ref,
        relation=relation,
        row=row,
        pack_ids=pack_ids,
        direction_hint=_coalesce(row, "direction_hint") or "supply_chain_exposure",
    )
    edges[(source_ref, target_ref, relation, edge["source_snapshot"])] = edge
    _add_ticker_bridge(
        row,
        nodes=nodes,
        edges=edges,
        issuer_ref=source_ref,
        namespace=namespace,
        jurisdiction=jurisdiction,
        key_prefix="source",
    )
    _add_ticker_bridge(
        row,
        nodes=nodes,
        edges=edges,
        issuer_ref=target_ref,
        namespace=namespace,
        jurisdiction=jurisdiction,
        key_prefix="target",
    )


def build_issuer_exposure_source_lane(
    *,
    issuer_commodity_tsv_paths: Iterable[str | Path] = (),
    issuer_geography_tsv_paths: Iterable[str | Path] = (),
    issuer_supply_chain_tsv_paths: Iterable[str | Path] = (),
    output_root: str | Path = DEFAULT_SOURCE_OUTPUT_ROOT,
    run_id: str | None = None,
    artifact_output_root: str | Path | None = None,
    build_artifact: bool = False,
    include_starter_graph: bool = True,
    extra_node_tsv_paths: Iterable[str | Path] = (),
    extra_edge_tsv_paths: Iterable[str | Path] = (),
    namespace: str = "issuer-exposure",
) -> IssuerExposureBuildResult:
    resolved_commodity_paths = [
        Path(path).expanduser().resolve() for path in issuer_commodity_tsv_paths
    ]
    resolved_geography_paths = [
        Path(path).expanduser().resolve() for path in issuer_geography_tsv_paths
    ]
    resolved_supply_chain_paths = [
        Path(path).expanduser().resolve() for path in issuer_supply_chain_tsv_paths
    ]
    if not (resolved_commodity_paths or resolved_geography_paths or resolved_supply_chain_paths):
        raise ValueError(
            "At least one issuer commodity, geography, or supply-chain TSV path is required."
        )

    resolved_run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(output_root).expanduser().resolve() / resolved_run_id
    node_tsv_path = output_dir / "impact_nodes.tsv"
    edge_tsv_path = output_dir / "impact_edges.tsv"
    manifest_path = output_dir / "manifest.json"

    nodes: dict[str, dict[str, str]] = {}
    edges: dict[tuple[str, str, str, str], dict[str, str]] = {}
    issuer_commodity_row_count = 0
    issuer_geography_row_count = 0
    issuer_supply_chain_row_count = 0

    for path in resolved_commodity_paths:
        for row in _read_tsv_rows(path):
            issuer_commodity_row_count += 1
            _add_issuer_commodity_row(row, nodes=nodes, edges=edges, namespace=namespace)

    for path in resolved_geography_paths:
        for row in _read_tsv_rows(path):
            issuer_geography_row_count += 1
            _add_issuer_geography_row(row, nodes=nodes, edges=edges, namespace=namespace)

    for path in resolved_supply_chain_paths:
        for row in _read_tsv_rows(path):
            issuer_supply_chain_row_count += 1
            _add_issuer_supply_chain_row(row, nodes=nodes, edges=edges, namespace=namespace)

    node_count = _write_tsv(
        node_tsv_path,
        columns=NODE_COLUMNS,
        rows=(nodes[key] for key in sorted(nodes)),
    )
    edge_count = _write_tsv(
        edge_tsv_path,
        columns=EDGE_COLUMNS,
        rows=(edges[key] for key in sorted(edges)),
    )
    relation_counts: dict[str, int] = {}
    for edge in edges.values():
        relation = edge["relation"]
        relation_counts[relation] = relation_counts.get(relation, 0) + 1

    artifact_path: Path | None = None
    artifact_node_count: int | None = None
    artifact_edge_count: int | None = None
    artifact_warnings: list[str] = []
    if build_artifact:
        artifact_root = (
            Path(artifact_output_root or DEFAULT_ARTIFACT_OUTPUT_ROOT).expanduser().resolve()
        )
        artifact_dir = artifact_root / resolved_run_id
        graph_node_paths = [Path(path).expanduser().resolve() for path in extra_node_tsv_paths]
        graph_edge_paths = [Path(path).expanduser().resolve() for path in extra_edge_tsv_paths]
        graph_node_paths.append(node_tsv_path)
        graph_edge_paths.append(edge_tsv_path)
        if include_starter_graph:
            with starter_source_paths() as (starter_node_path, starter_edge_path):
                response = build_market_graph_store(
                    node_tsv_paths=[starter_node_path, *graph_node_paths],
                    edge_tsv_paths=[starter_edge_path, *graph_edge_paths],
                    output_dir=artifact_dir,
                    artifact_version=resolved_run_id,
                )
        else:
            response = build_market_graph_store(
                node_tsv_paths=graph_node_paths,
                edge_tsv_paths=graph_edge_paths,
                output_dir=artifact_dir,
                artifact_version=resolved_run_id,
            )
        artifact_path = Path(response.artifact_path)
        artifact_node_count = response.node_count
        artifact_edge_count = response.edge_count
        artifact_warnings = list(response.warnings)

    manifest = {
        "schema_version": 1,
        "run_id": resolved_run_id,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "issuer_commodity_tsv_paths": [str(path) for path in resolved_commodity_paths],
        "issuer_geography_tsv_paths": [str(path) for path in resolved_geography_paths],
        "issuer_supply_chain_tsv_paths": [str(path) for path in resolved_supply_chain_paths],
        "output_dir": str(output_dir),
        "node_tsv_path": str(node_tsv_path),
        "edge_tsv_path": str(edge_tsv_path),
        "artifact_path": str(artifact_path) if artifact_path else None,
        "artifact_node_count": artifact_node_count,
        "artifact_edge_count": artifact_edge_count,
        "artifact_warnings": artifact_warnings,
        "issuer_commodity_row_count": issuer_commodity_row_count,
        "issuer_geography_row_count": issuer_geography_row_count,
        "issuer_supply_chain_row_count": issuer_supply_chain_row_count,
        "node_count": node_count,
        "edge_count": edge_count,
        "relation_counts": dict(sorted(relation_counts.items())),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return IssuerExposureBuildResult(
        run_id=resolved_run_id,
        output_dir=output_dir,
        node_tsv_path=node_tsv_path,
        edge_tsv_path=edge_tsv_path,
        manifest_path=manifest_path,
        artifact_path=artifact_path,
        artifact_node_count=artifact_node_count,
        artifact_edge_count=artifact_edge_count,
        issuer_commodity_row_count=issuer_commodity_row_count,
        issuer_geography_row_count=issuer_geography_row_count,
        issuer_supply_chain_row_count=issuer_supply_chain_row_count,
        node_count=node_count,
        edge_count=edge_count,
        relation_counts=dict(sorted(relation_counts.items())),
    )


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issuer-commodity-tsv-path", action="append", default=[])
    parser.add_argument("--issuer-geography-tsv-path", action="append", default=[])
    parser.add_argument("--issuer-supply-chain-tsv-path", action="append", default=[])
    parser.add_argument("--output-root", default=str(DEFAULT_SOURCE_OUTPUT_ROOT))
    parser.add_argument("--run-id")
    parser.add_argument("--build-artifact", action="store_true")
    parser.add_argument("--artifact-output-root", default=str(DEFAULT_ARTIFACT_OUTPUT_ROOT))
    parser.add_argument("--no-starter-graph", action="store_true")
    parser.add_argument("--extra-node-tsv-path", action="append", default=[])
    parser.add_argument("--extra-edge-tsv-path", action="append", default=[])
    parser.add_argument("--namespace", default="issuer-exposure")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    result = build_issuer_exposure_source_lane(
        issuer_commodity_tsv_paths=args.issuer_commodity_tsv_path,
        issuer_geography_tsv_paths=args.issuer_geography_tsv_path,
        issuer_supply_chain_tsv_paths=args.issuer_supply_chain_tsv_path,
        output_root=args.output_root,
        run_id=args.run_id,
        artifact_output_root=args.artifact_output_root,
        build_artifact=args.build_artifact,
        include_starter_graph=not args.no_starter_graph,
        extra_node_tsv_paths=args.extra_node_tsv_path,
        extra_edge_tsv_paths=args.extra_edge_tsv_path,
        namespace=args.namespace,
    )
    print(json.dumps(result.to_json_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
