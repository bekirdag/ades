"""Build policy-sector source lanes for the market impact graph."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Iterable

from .graph_builder import build_market_graph_store
from .relationship_schema import (
    relation_direction_preconditions,
    relation_event_types,
)
from .starter import starter_source_paths

DEFAULT_BIG_DATA_ROOT = Path("/mnt/githubActions/ades_big_data")
DEFAULT_SOURCE_OUTPUT_ROOT = (
    DEFAULT_BIG_DATA_ROOT / "pack_sources" / "impact_relationships" / "policy_sector_proxy"
)
DEFAULT_ARTIFACT_OUTPUT_ROOT = DEFAULT_BIG_DATA_ROOT / "artifacts" / "market-graph"
DEFAULT_PACK_ID = "policy-sector-en"

NODE_COLUMNS = [
    "entity_ref",
    "canonical_name",
    "entity_type",
    "library_id",
    "is_tradable",
    "is_seed_eligible",
    "identifiers_json",
    "pack_ids",
]

EDGE_COLUMNS = [
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

_POLICY_SOURCE_TYPE_TO_RELATION = {
    "government": "government_body_affects_sector",
    "government_body": "government_body_affects_sector",
    "ministry": "government_body_affects_sector",
    "parliament": "government_body_affects_sector",
    "policy_body": "policy_body_affects_sector",
    "regulator": "regulator_affects_sector",
    "regulatory_body": "regulator_affects_sector",
    "law": "law_affects_sector",
    "legislation": "law_affects_sector",
}


@dataclass(frozen=True)
class PolicySectorProxyBuildResult:
    run_id: str
    output_dir: Path
    node_tsv_path: Path
    edge_tsv_path: Path
    manifest_path: Path
    artifact_path: Path | None
    artifact_node_count: int | None
    artifact_edge_count: int | None
    policy_sector_row_count: int
    issuer_sector_row_count: int
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
            "policy_sector_row_count": self.policy_sector_row_count,
            "issuer_sector_row_count": self.issuer_sector_row_count,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "relation_counts": self.relation_counts,
        }


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    return normalized or "unknown"


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


def _entity_ref(
    *,
    provided: str,
    namespace: str,
    entity_type: str,
    name: str,
    jurisdiction: str,
) -> str:
    if provided:
        return provided
    jurisdiction_part = _slug(jurisdiction) if jurisdiction else "global"
    return f"ades:{namespace}:{jurisdiction_part}:{entity_type}:{_slug(name)}"


def _ticker_ref(row: dict[str, str], *, namespace: str, jurisdiction: str) -> str:
    provided = _coalesce(row, "ticker_ref", "asset_ref", "terminal_ref")
    if provided:
        return provided
    symbol = _coalesce(row, "ticker_symbol", "symbol")
    if not symbol:
        return ""
    exchange = _coalesce(row, "exchange", "mic")
    if exchange:
        return f"{exchange.upper()}:{symbol.upper()}"
    jurisdiction_part = _slug(jurisdiction) if jurisdiction else "global"
    return f"ades:{namespace}:{jurisdiction_part}:ticker:{_slug(symbol)}"


def _policy_relation(row: dict[str, str]) -> str:
    explicit = _coalesce(row, "relation", "relation_type")
    if explicit:
        return explicit
    source_type = _coalesce(row, "policy_type", "source_type", "entity_type").casefold()
    return _POLICY_SOURCE_TYPE_TO_RELATION.get(source_type, "policy_body_affects_sector")


def _source_year(row: dict[str, str]) -> str:
    return _coalesce(row, "source_year", "year")


def _confidence(row: dict[str, str], *, default: str) -> str:
    raw = _coalesce(row, "confidence")
    if not raw:
        return default
    parsed = float(raw)
    if parsed < 0 or parsed > 1:
        raise ValueError(f"Invalid confidence: {raw!r}")
    return f"{parsed:.4g}"


def _node_row(
    *,
    entity_ref: str,
    canonical_name: str,
    entity_type: str,
    pack_ids: Iterable[str],
    identifiers: dict[str, object] | None = None,
    is_tradable: bool = False,
    is_seed_eligible: bool = True,
) -> dict[str, str]:
    packs = tuple(dict.fromkeys(pack_ids))
    return {
        "entity_ref": entity_ref,
        "canonical_name": canonical_name,
        "entity_type": entity_type,
        "library_id": packs[0] if packs else DEFAULT_PACK_ID,
        "is_tradable": "true" if is_tradable else "false",
        "is_seed_eligible": "true" if is_seed_eligible else "false",
        "identifiers_json": json.dumps(identifiers or {}, sort_keys=True),
        "pack_ids": ",".join(packs),
    }


def _edge_row(
    *,
    source_ref: str,
    target_ref: str,
    relation: str,
    row: dict[str, str],
    pack_ids: Iterable[str],
    evidence_level: str = "direct",
    confidence: str = "0.85",
    direction_hint: str = "policy_risk",
    notes: str = "",
) -> dict[str, str]:
    return {
        "source_ref": source_ref,
        "target_ref": target_ref,
        "relation": relation,
        "evidence_level": _coalesce(row, "evidence_level") or evidence_level,
        "confidence": _confidence(row, default=confidence),
        "direction_hint": _coalesce(row, "direction_hint") or direction_hint,
        "source_name": _coalesce(row, "source_name"),
        "source_url": _coalesce(row, "source_url"),
        "source_snapshot": _coalesce(row, "source_snapshot", "snapshot"),
        "source_year": _source_year(row),
        "refresh_policy": _coalesce(row, "refresh_policy") or "annual",
        "pack_ids": ",".join(tuple(dict.fromkeys(pack_ids))),
        "notes": _coalesce(row, "notes") or notes,
        "compatible_event_types": ",".join(relation_event_types(relation)),
        "direction_preconditions": ",".join(relation_direction_preconditions(relation)),
    }


def _read_tsv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle, delimiter="\t")]


def _write_tsv(path: Path, *, columns: list[str], rows: Iterable[dict[str, str]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def _add_policy_sector_row(
    row: dict[str, str],
    *,
    nodes: dict[str, dict[str, str]],
    edges: dict[tuple[str, str, str, str], dict[str, str]],
    namespace: str,
) -> None:
    pack_ids = _pack_ids(row)
    jurisdiction = _coalesce(row, "jurisdiction", "country", "country_code")
    policy_name = _coalesce(row, "policy_name", "source_name", "source_entity")
    policy_type = _coalesce(row, "policy_type", "source_type", "entity_type") or "policy_body"
    sector_name = _coalesce(row, "sector_name", "target_name", "target_entity")
    if not policy_name or not sector_name:
        raise ValueError("Policy-sector rows require policy/source and sector names.")
    policy_ref = _entity_ref(
        provided=_coalesce(row, "policy_ref", "source_ref", "source_entity_ref"),
        namespace=namespace,
        entity_type=policy_type,
        name=policy_name,
        jurisdiction=jurisdiction,
    )
    sector_ref = _entity_ref(
        provided=_coalesce(row, "sector_ref", "target_ref", "target_entity_ref"),
        namespace=namespace,
        entity_type="sector",
        name=sector_name,
        jurisdiction=jurisdiction,
    )
    nodes.setdefault(
        policy_ref,
        _node_row(
            entity_ref=policy_ref,
            canonical_name=policy_name,
            entity_type=policy_type,
            pack_ids=pack_ids,
            identifiers={"jurisdiction": jurisdiction} if jurisdiction else {},
        ),
    )
    nodes.setdefault(
        sector_ref,
        _node_row(
            entity_ref=sector_ref,
            canonical_name=sector_name,
            entity_type="sector",
            pack_ids=pack_ids,
            identifiers={"jurisdiction": jurisdiction} if jurisdiction else {},
        ),
    )
    relation = _policy_relation(row)
    edge = _edge_row(
        source_ref=policy_ref,
        target_ref=sector_ref,
        relation=relation,
        row=row,
        pack_ids=pack_ids,
    )
    edges[(policy_ref, sector_ref, relation, edge["source_snapshot"])] = edge


def _add_issuer_sector_row(
    row: dict[str, str],
    *,
    nodes: dict[str, dict[str, str]],
    edges: dict[tuple[str, str, str, str], dict[str, str]],
    namespace: str,
) -> None:
    pack_ids = _pack_ids(row)
    jurisdiction = _coalesce(row, "jurisdiction", "country", "country_code")
    issuer_name = _coalesce(row, "issuer_name", "company_name", "source_name")
    sector_name = _coalesce(row, "sector_name", "target_name", "target_entity")
    if not issuer_name or not sector_name:
        raise ValueError("Issuer-sector rows require issuer and sector names.")
    issuer_ref = _entity_ref(
        provided=_coalesce(row, "issuer_ref", "source_ref", "company_ref"),
        namespace=namespace,
        entity_type="issuer",
        name=issuer_name,
        jurisdiction=jurisdiction,
    )
    sector_ref = _entity_ref(
        provided=_coalesce(row, "sector_ref", "target_ref", "target_entity_ref"),
        namespace=namespace,
        entity_type="sector",
        name=sector_name,
        jurisdiction=jurisdiction,
    )
    nodes.setdefault(
        sector_ref,
        _node_row(
            entity_ref=sector_ref,
            canonical_name=sector_name,
            entity_type="sector",
            pack_ids=pack_ids,
            identifiers={"jurisdiction": jurisdiction} if jurisdiction else {},
        ),
    )
    nodes.setdefault(
        issuer_ref,
        _node_row(
            entity_ref=issuer_ref,
            canonical_name=issuer_name,
            entity_type="issuer",
            pack_ids=pack_ids,
            identifiers={"jurisdiction": jurisdiction} if jurisdiction else {},
        ),
    )
    relation = _coalesce(row, "relation", "relation_type") or "sector_affects_issuer"
    edge = _edge_row(
        source_ref=sector_ref,
        target_ref=issuer_ref,
        relation=relation,
        row=row,
        pack_ids=pack_ids,
        direction_hint="sector_exposure",
    )
    edges[(sector_ref, issuer_ref, relation, edge["source_snapshot"])] = edge

    ticker_ref = _ticker_ref(row, namespace=namespace, jurisdiction=jurisdiction)
    if not ticker_ref:
        return
    ticker_name = _coalesce(row, "ticker_name", "ticker_symbol", "symbol") or ticker_ref
    nodes.setdefault(
        ticker_ref,
        _node_row(
            entity_ref=ticker_ref,
            canonical_name=ticker_name,
            entity_type="ticker",
            pack_ids=pack_ids,
            identifiers={"jurisdiction": jurisdiction} if jurisdiction else {},
            is_tradable=True,
            is_seed_eligible=False,
        ),
    )
    ticker_edge = _edge_row(
        source_ref=issuer_ref,
        target_ref=ticker_ref,
        relation="issuer_has_listed_ticker",
        row=row,
        pack_ids=pack_ids,
        direction_hint="direct_listing",
        notes="issuer-sector row ticker bridge",
    )
    edges[(issuer_ref, ticker_ref, "issuer_has_listed_ticker", ticker_edge["source_snapshot"])] = (
        ticker_edge
    )


def build_policy_sector_source_lane(
    *,
    policy_sector_tsv_paths: Iterable[str | Path],
    issuer_sector_tsv_paths: Iterable[str | Path] = (),
    output_root: str | Path = DEFAULT_SOURCE_OUTPUT_ROOT,
    run_id: str | None = None,
    artifact_output_root: str | Path | None = None,
    build_artifact: bool = False,
    include_starter_graph: bool = True,
    extra_node_tsv_paths: Iterable[str | Path] = (),
    extra_edge_tsv_paths: Iterable[str | Path] = (),
    namespace: str = "policy-sector",
) -> PolicySectorProxyBuildResult:
    resolved_policy_paths = [Path(path).expanduser().resolve() for path in policy_sector_tsv_paths]
    resolved_issuer_paths = [Path(path).expanduser().resolve() for path in issuer_sector_tsv_paths]
    if not resolved_policy_paths and not resolved_issuer_paths:
        raise ValueError("At least one policy-sector or issuer-sector TSV path is required.")

    resolved_run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(output_root).expanduser().resolve() / resolved_run_id
    node_tsv_path = output_dir / "impact_nodes.tsv"
    edge_tsv_path = output_dir / "impact_edges.tsv"
    manifest_path = output_dir / "manifest.json"

    nodes: dict[str, dict[str, str]] = {}
    edges: dict[tuple[str, str, str, str], dict[str, str]] = {}
    policy_sector_row_count = 0
    issuer_sector_row_count = 0

    for path in resolved_policy_paths:
        for row in _read_tsv_rows(path):
            policy_sector_row_count += 1
            _add_policy_sector_row(row, nodes=nodes, edges=edges, namespace=namespace)

    for path in resolved_issuer_paths:
        for row in _read_tsv_rows(path):
            issuer_sector_row_count += 1
            _add_issuer_sector_row(row, nodes=nodes, edges=edges, namespace=namespace)

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
        "policy_sector_tsv_paths": [str(path) for path in resolved_policy_paths],
        "issuer_sector_tsv_paths": [str(path) for path in resolved_issuer_paths],
        "output_dir": str(output_dir),
        "node_tsv_path": str(node_tsv_path),
        "edge_tsv_path": str(edge_tsv_path),
        "artifact_path": str(artifact_path) if artifact_path else None,
        "artifact_node_count": artifact_node_count,
        "artifact_edge_count": artifact_edge_count,
        "artifact_warnings": artifact_warnings,
        "policy_sector_row_count": policy_sector_row_count,
        "issuer_sector_row_count": issuer_sector_row_count,
        "node_count": node_count,
        "edge_count": edge_count,
        "relation_counts": dict(sorted(relation_counts.items())),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return PolicySectorProxyBuildResult(
        run_id=resolved_run_id,
        output_dir=output_dir,
        node_tsv_path=node_tsv_path,
        edge_tsv_path=edge_tsv_path,
        manifest_path=manifest_path,
        artifact_path=artifact_path,
        artifact_node_count=artifact_node_count,
        artifact_edge_count=artifact_edge_count,
        policy_sector_row_count=policy_sector_row_count,
        issuer_sector_row_count=issuer_sector_row_count,
        node_count=node_count,
        edge_count=edge_count,
        relation_counts=dict(sorted(relation_counts.items())),
    )


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy-sector-tsv-path", action="append", default=[])
    parser.add_argument("--issuer-sector-tsv-path", action="append", default=[])
    parser.add_argument("--output-root", default=str(DEFAULT_SOURCE_OUTPUT_ROOT))
    parser.add_argument("--run-id")
    parser.add_argument("--build-artifact", action="store_true")
    parser.add_argument("--artifact-output-root", default=str(DEFAULT_ARTIFACT_OUTPUT_ROOT))
    parser.add_argument("--no-starter-graph", action="store_true")
    parser.add_argument("--extra-node-tsv-path", action="append", default=[])
    parser.add_argument("--extra-edge-tsv-path", action="append", default=[])
    parser.add_argument("--namespace", default="policy-sector")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    result = build_policy_sector_source_lane(
        policy_sector_tsv_paths=args.policy_sector_tsv_path,
        issuer_sector_tsv_paths=args.issuer_sector_tsv_path,
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
