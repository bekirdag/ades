"""Build program/product/organization relationship source lanes."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Iterable

from .graph_builder import build_market_graph_store
from .policy_sector_proxy import (
    DEFAULT_ARTIFACT_OUTPUT_ROOT,
    DEFAULT_BIG_DATA_ROOT,
    EDGE_COLUMNS,
    NODE_COLUMNS,
    _coalesce,
    _edge_row,
    _entity_ref,
    _node_row,
    _parse_list,
    _read_tsv_rows,
    _write_tsv,
)
from .starter import starter_source_paths

DEFAULT_SOURCE_OUTPUT_ROOT = (
    DEFAULT_BIG_DATA_ROOT / "pack_sources" / "impact_relationships" / "program_org_relationship"
)
DEFAULT_PACK_ID = "program-org-relationship-en"

SUPPORTED_NODE_TYPES = {
    "program",
    "vision_program",
    "trade_agreement",
    "policy",
    "law",
    "product",
    "brand",
    "project",
    "giga_project",
    "organization",
    "government_body",
    "central_bank",
    "prudential_authority",
    "state_owned_enterprise",
    "legal_entity",
    "holding_company",
    "subsidiary",
    "issuer",
    "market_index",
    "macro_indicator",
    "regulator",
    "currency",
    "rate",
    "security",
    "depository_receipt",
    "sukuk",
    "reit",
    "sector",
    "country",
    "commodity",
    "commodity_flow",
    "infrastructure_asset",
    "grid",
    "pipeline",
    "port",
    "rail_corridor",
    "mine",
    "mining_right",
    "field",
    "refinery",
    "plant",
    "power_plant",
    "vessel",
    "route",
    "concession",
    "license",
    "payment_network",
    "payment_system",
    "settlement_system",
    "depository",
    "market_infrastructure",
    "sovereign_wealth_fund",
    "pilgrimage_flow",
    "spare_capacity_policy",
    "production_quota",
    "sanctions_authority",
    "sanctions_program",
    "sanctions_designation",
    "trading_restriction",
    "market_access_status",
    "ticker",
    "exchange",
}
TRADABLE_NODE_TYPES = {"commodity", "currency", "market_index", "rate", "security", "ticker"}

PREFIX_IDENTIFIER_SCALAR_KEYS = (
    "asset_ref",
    "country_code",
    "finance_issuer_ref",
    "finance_ticker_ref",
    "geonames_id",
    "issuer_ref",
    "sec_cik",
    "security_ref",
    "ticker_ref",
    "wikidata_id",
    "wikidata_qid",
)
PREFIX_IDENTIFIER_LIST_KEYS = (
    "alias_refs",
    "alternate_refs",
    "entity_refs",
    "equivalent_refs",
    "finance_issuer_refs",
    "finance_ticker_refs",
    "geonames_ids",
    "identity_refs",
    "issuer_refs",
    "same_as_refs",
    "source_refs",
    "ticker_refs",
    "wikidata_ids",
    "wikidata_qids",
)

SUPPORTED_RELATIONS = {
    "program_operated_by_org",
    "program_financed_by_org",
    "program_host_country",
    "program_has_project",
    "program_loan_recipient_org",
    "policy_program_affects_sector",
    "vision_program_affects_sector",
    "vision_program_has_project",
    "pilgrimage_flow_affects_sector",
    "hajj_policy_affects_sector",
    "public_facility_available_to_sector",
    "export_program_affects_sector",
    "product_owned_by_org",
    "brand_owned_by_org",
    "payment_network_operated_by_org",
    "sovereign_fund_sponsors_program",
    "trade_agreement_counterparty_country",
    "trade_agreement_affects_sector",
    "project_operated_by_org",
    "project_owned_by_org",
    "project_developed_by_org",
    "project_contracted_to_org",
    "project_participant_org",
    "project_affects_sector",
    "project_affects_issuer",
    "defense_project_affects_sector",
    "infrastructure_project_affects_sector",
    "org_in_sector",
    "org_part_of_holding",
    "org_subsidiary_of_org",
    "holding_parent_is_issuer",
    "issuer_has_listed_ticker",
    "issuer_has_security",
    "issuer_in_sector",
    "issuer_exposed_to_commodity",
    "exchange_lists_security",
    "security_trades_on_exchange",
    "security_has_trading_status",
    "security_has_market_access_status",
    "issuer_has_trading_restriction",
    "issuer_subject_to_sanctions",
    "security_subject_to_sanctions",
    "sanctions_authority_administers_program",
    "sanctions_program_targets_entity",
    "sanctions_program_targets_sector",
    "sanctions_program_targets_security",
    "sanctions_restriction_affects_sector",
    "sanctions_restriction_affects_commodity",
    "sanctions_restriction_affects_payment_channel",
    "regulator_supervises_exchange",
    "regulator_supervises_depository",
    "regulator_supervises_bank",
    "prudential_authority_supervises_bank",
    "regulator_supervises_issuer",
    "regulator_supervises_insurer",
    "regulator_supervises_telecom",
    "regulator_supervises_electricity",
    "regulator_supervises_competition",
    "regulator_supervises_airline",
    "regulator_supervises_port",
    "depository_serves_security",
    "government_body_affects_sector",
    "government_body_sets_export_policy",
    "government_body_sets_fiscal_policy",
    "government_body_sets_energy_policy",
    "government_body_sets_mining_policy",
    "government_body_sets_tourism_policy",
    "government_body_sets_industrial_policy",
    "state_body_holds_ownership_stake",
    "state_owned_enterprise_operates_infrastructure",
    "org_operates_infrastructure_asset",
    "org_owns_infrastructure_asset",
    "org_operates_grid",
    "org_operates_rail",
    "org_operates_pipeline",
    "org_operates_port",
    "org_operates_field",
    "org_operates_mine",
    "org_operates_refinery",
    "infrastructure_asset_supports_commodity_flow",
    "commodity_flow_affects_commodity",
    "commodity_flow_affects_sector",
    "port_handles_commodity",
    "pipeline_transports_commodity",
    "rail_corridor_moves_commodity",
    "route_affects_commodity_flow",
    "airport_serves_route",
    "issuer_exposed_to_sanctions_program",
    "issuer_exposed_to_project",
    "issuer_exposed_to_oil_policy",
    "country_exports_commodity",
    "country_produces_commodity",
    "policy_affects_commodity",
    "production_policy_affects_commodity",
    "spare_capacity_affects_commodity",
    "oil_revenue_affects_fiscal_balance",
    "central_bank_affects_currency",
    "central_bank_affects_rates",
    "central_bank_affects_credit_sector",
    "central_bank_sets_policy_rate",
    "central_bank_maintains_currency_peg",
    "statistics_body_reports_macro_indicator",
    "regulator_affects_sector",
    "policy_body_affects_sector",
    "law_affects_sector",
    "sector_affects_issuer",
    "sector_affects_index",
    "security_has_identifier",
    "security_listed_on_exchange",
    "ticker_trades_on_exchange",
    "ticker_listed_on_exchange",
    "sukuk_issued_by_issuer",
    "reit_managed_by_org",
}


@dataclass(frozen=True)
class ProgramOrgRelationshipBuildResult:
    run_id: str
    output_dir: Path
    node_tsv_path: Path
    edge_tsv_path: Path
    manifest_path: Path
    artifact_path: Path | None
    artifact_node_count: int | None
    artifact_edge_count: int | None
    relationship_row_count: int
    node_count: int
    edge_count: int
    relation_counts: dict[str, int]
    node_type_counts: dict[str, int]

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
            "relationship_row_count": self.relationship_row_count,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "relation_counts": self.relation_counts,
            "node_type_counts": self.node_type_counts,
        }


def _pack_ids(row: dict[str, str]) -> tuple[str, ...]:
    return _parse_list(_coalesce(row, "pack_ids", "packs")) or (DEFAULT_PACK_ID,)


def _entity_type(row: dict[str, str], prefix: str) -> str:
    value = _coalesce(row, f"{prefix}_entity_type", f"{prefix}_type", "entity_type")
    if not value:
        raise ValueError(f"{prefix}_entity_type is required.")
    normalized = value.strip().casefold()
    if normalized not in SUPPORTED_NODE_TYPES:
        raise ValueError(f"Unsupported {prefix}_entity_type: {value!r}")
    return normalized


def _entity_name(row: dict[str, str], prefix: str) -> str:
    value = _coalesce(row, f"{prefix}_entity_name", f"{prefix}_name")
    if not value:
        raise ValueError(f"{prefix}_entity_name is required.")
    return value


def _entity_provided_ref(row: dict[str, str], prefix: str) -> str:
    return _coalesce(row, f"{prefix}_entity_ref", f"{prefix}_ref")


def _relation(row: dict[str, str]) -> str:
    value = _coalesce(row, "relation", "relation_type")
    if not value:
        raise ValueError("relation is required.")
    normalized = value.strip()
    if normalized not in SUPPORTED_RELATIONS:
        raise ValueError(f"Unsupported relation: {value!r}")
    return normalized


def _source_snapshot(row: dict[str, str]) -> str:
    return _coalesce(
        row,
        "source_snapshot",
        "source_snapshot_path",
        "source_fetched_at",
        "source_published_at",
        "effective_from",
    )


def _source_year(row: dict[str, str]) -> str:
    explicit = _coalesce(row, "source_year", "year")
    if explicit:
        return explicit
    for key in ("effective_from", "source_published_at", "source_fetched_at"):
        value = _coalesce(row, key)
        if len(value) >= 4 and value[:4].isdigit():
            return value[:4]
    return ""


def _notes(row: dict[str, str]) -> str:
    parts: list[str] = []
    notes = _coalesce(row, "notes")
    if notes:
        parts.append(notes)
    evidence = _coalesce(row, "evidence_text", "evidence")
    if evidence:
        parts.append(f"evidence={evidence}")
    effective_from = _coalesce(row, "effective_from")
    if effective_from:
        parts.append(f"effective_from={effective_from}")
    effective_to = _coalesce(row, "effective_to")
    if effective_to:
        parts.append(f"effective_to={effective_to}")
    source_tier = _coalesce(row, "source_tier")
    if source_tier:
        parts.append(f"source_tier={source_tier}")
    return " | ".join(parts)


def _confidence(row: dict[str, str]) -> str:
    raw = _coalesce(row, "confidence")
    if not raw:
        return "0.9"
    parsed = float(raw)
    if parsed < 0 or parsed > 1:
        raise ValueError(f"Invalid confidence: {raw!r}")
    return f"{parsed:.4g}"


def _identifier_list_values(value: object | None) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    if raw.startswith("["):
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            raise ValueError(f"Invalid identifier list: {raw!r}")
        values = [str(item).strip() for item in parsed]
    else:
        values = list(_parse_list(raw))
    return [item for item in values if item]


def _merge_identifier_list(
    identifiers: dict[str, object],
    key: str,
    values: Iterable[str],
) -> None:
    normalized_values = [value for value in values if value]
    if not normalized_values:
        return
    existing = identifiers.get(key)
    merged: list[str] = []
    if isinstance(existing, list):
        merged.extend(str(item).strip() for item in existing if str(item).strip())
    elif existing:
        merged.append(str(existing).strip())
    merged.extend(normalized_values)
    identifiers[key] = list(dict.fromkeys(merged))


def _merge_prefixed_identifier_columns(
    identifiers: dict[str, object],
    *,
    row: dict[str, str],
    prefix: str,
) -> None:
    raw_json = _coalesce(row, f"{prefix}_identifiers_json")
    if raw_json:
        parsed = json.loads(raw_json)
        if not isinstance(parsed, dict):
            raise ValueError(f"Invalid {prefix}_identifiers_json: {raw_json!r}")
        for key, value in parsed.items():
            identifiers[str(key)] = value
    for key in PREFIX_IDENTIFIER_SCALAR_KEYS:
        value = _coalesce(row, f"{prefix}_{key}")
        if value:
            identifiers[key] = value
    for key in PREFIX_IDENTIFIER_LIST_KEYS:
        value = _coalesce(row, f"{prefix}_{key}")
        if value:
            _merge_identifier_list(identifiers, key, _identifier_list_values(value))


def _edge_input_row(row: dict[str, str]) -> dict[str, str]:
    normalized = dict(row)
    if not _coalesce(normalized, "source_name"):
        source_name = _coalesce(
            normalized,
            "source_title",
            "source_publisher",
            "source_document",
        )
        if source_name:
            normalized["source_name"] = source_name
    if not _coalesce(normalized, "source_snapshot", "snapshot"):
        snapshot = _source_snapshot(normalized)
        if snapshot:
            normalized["source_snapshot"] = snapshot
    if not _coalesce(normalized, "source_year", "year"):
        source_year = _source_year(normalized)
        if source_year:
            normalized["source_year"] = source_year
    normalized["notes"] = _notes(normalized)
    return normalized


def _identifiers(
    *,
    entity_ref: str,
    provided_ref: str,
    jurisdiction: str,
    row: dict[str, str],
    prefix: str,
) -> dict[str, object]:
    identifiers: dict[str, object] = {}
    if jurisdiction:
        identifiers["jurisdiction"] = jurisdiction
    if provided_ref and provided_ref != entity_ref:
        identifiers["source_entity_ref"] = provided_ref
    ref = provided_ref or entity_ref
    ref_lower = ref.casefold()
    if ref_lower.startswith("wikidata:"):
        identifiers["wikidata_id"] = ref.split(":", 1)[1]
    elif ref_lower.startswith("lei:"):
        identifiers["lei"] = ref.split(":", 1)[1]
    elif ref_lower.startswith("cik:"):
        identifiers["cik"] = ref.split(":", 1)[1]
    for key in (
        "lei",
        "cik",
        "figi",
        "isin",
        "mic",
        "exchange",
        "ticker_symbol",
        "security_identifier",
    ):
        value = _coalesce(row, f"{prefix}_{key}", key)
        if value:
            identifiers[key] = value
    _merge_prefixed_identifier_columns(identifiers, row=row, prefix=prefix)
    return identifiers


def _add_node(
    *,
    row: dict[str, str],
    prefix: str,
    namespace: str,
    pack_ids: tuple[str, ...],
    nodes: dict[str, dict[str, str]],
) -> tuple[str, str]:
    jurisdiction = _coalesce(row, "jurisdiction", "country", "country_code")
    entity_type = _entity_type(row, prefix)
    entity_name = _entity_name(row, prefix)
    provided_ref = _entity_provided_ref(row, prefix)
    entity_ref = _entity_ref(
        provided=provided_ref,
        namespace=namespace,
        entity_type=entity_type,
        name=entity_name,
        jurisdiction=jurisdiction,
    )
    nodes.setdefault(
        entity_ref,
        _node_row(
            entity_ref=entity_ref,
            canonical_name=entity_name,
            entity_type=entity_type,
            pack_ids=pack_ids,
            identifiers=_identifiers(
                entity_ref=entity_ref,
                provided_ref=provided_ref,
                jurisdiction=jurisdiction,
                row=row,
                prefix=prefix,
            ),
            is_tradable=entity_type in TRADABLE_NODE_TYPES,
            is_seed_eligible=entity_type not in TRADABLE_NODE_TYPES,
        ),
    )
    return entity_ref, entity_type


def _add_relationship_row(
    row: dict[str, str],
    *,
    nodes: dict[str, dict[str, str]],
    edges: dict[tuple[str, str, str, str], dict[str, str]],
    namespace: str,
) -> None:
    if not _coalesce(row, "evidence_text", "evidence"):
        raise ValueError("evidence_text is required.")
    pack_ids = _pack_ids(row)
    source_ref, _source_type = _add_node(
        row=row,
        prefix="source",
        namespace=namespace,
        pack_ids=pack_ids,
        nodes=nodes,
    )
    target_ref, _target_type = _add_node(
        row=row,
        prefix="target",
        namespace=namespace,
        pack_ids=pack_ids,
        nodes=nodes,
    )
    relation = _relation(row)
    edge = _edge_row(
        source_ref=source_ref,
        target_ref=target_ref,
        relation=relation,
        row=_edge_input_row(row),
        pack_ids=pack_ids,
        evidence_level=_coalesce(row, "evidence_level") or "direct",
        confidence=_confidence(row),
        direction_hint=_coalesce(row, "direction_hint") or "relationship_chain",
    )
    edges[(source_ref, target_ref, relation, edge["source_snapshot"])] = edge


def build_program_org_relationship_source_lane(
    *,
    relationship_tsv_paths: Iterable[str | Path],
    output_root: str | Path = DEFAULT_SOURCE_OUTPUT_ROOT,
    run_id: str | None = None,
    artifact_output_root: str | Path | None = None,
    build_artifact: bool = False,
    include_starter_graph: bool = True,
    extra_node_tsv_paths: Iterable[str | Path] = (),
    extra_edge_tsv_paths: Iterable[str | Path] = (),
    namespace: str = "program-org",
) -> ProgramOrgRelationshipBuildResult:
    resolved_relationship_paths = [
        Path(path).expanduser().resolve() for path in relationship_tsv_paths
    ]
    if not resolved_relationship_paths:
        raise ValueError("At least one program/org relationship TSV path is required.")

    resolved_run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(output_root).expanduser().resolve() / resolved_run_id
    node_tsv_path = output_dir / "impact_nodes.tsv"
    edge_tsv_path = output_dir / "impact_edges.tsv"
    manifest_path = output_dir / "manifest.json"

    nodes: dict[str, dict[str, str]] = {}
    edges: dict[tuple[str, str, str, str], dict[str, str]] = {}
    relationship_row_count = 0

    for path in resolved_relationship_paths:
        for row in _read_tsv_rows(path):
            relationship_row_count += 1
            _add_relationship_row(row, nodes=nodes, edges=edges, namespace=namespace)

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
    node_type_counts: dict[str, int] = {}
    for node in nodes.values():
        node_type = node["entity_type"]
        node_type_counts[node_type] = node_type_counts.get(node_type, 0) + 1

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
        "relationship_tsv_paths": [str(path) for path in resolved_relationship_paths],
        "output_dir": str(output_dir),
        "node_tsv_path": str(node_tsv_path),
        "edge_tsv_path": str(edge_tsv_path),
        "artifact_path": str(artifact_path) if artifact_path else None,
        "artifact_node_count": artifact_node_count,
        "artifact_edge_count": artifact_edge_count,
        "artifact_warnings": artifact_warnings,
        "relationship_row_count": relationship_row_count,
        "node_count": node_count,
        "edge_count": edge_count,
        "relation_counts": dict(sorted(relation_counts.items())),
        "node_type_counts": dict(sorted(node_type_counts.items())),
        "supported_relations": sorted(SUPPORTED_RELATIONS),
        "supported_node_types": sorted(SUPPORTED_NODE_TYPES),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return ProgramOrgRelationshipBuildResult(
        run_id=resolved_run_id,
        output_dir=output_dir,
        node_tsv_path=node_tsv_path,
        edge_tsv_path=edge_tsv_path,
        manifest_path=manifest_path,
        artifact_path=artifact_path,
        artifact_node_count=artifact_node_count,
        artifact_edge_count=artifact_edge_count,
        relationship_row_count=relationship_row_count,
        node_count=node_count,
        edge_count=edge_count,
        relation_counts=dict(sorted(relation_counts.items())),
        node_type_counts=dict(sorted(node_type_counts.items())),
    )


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--relationship-tsv-path", action="append", default=[])
    parser.add_argument("--output-root", default=str(DEFAULT_SOURCE_OUTPUT_ROOT))
    parser.add_argument("--run-id")
    parser.add_argument("--build-artifact", action="store_true")
    parser.add_argument("--artifact-output-root", default=str(DEFAULT_ARTIFACT_OUTPUT_ROOT))
    parser.add_argument("--no-starter-graph", action="store_true")
    parser.add_argument("--extra-node-tsv-path", action="append", default=[])
    parser.add_argument("--extra-edge-tsv-path", action="append", default=[])
    parser.add_argument("--namespace", default="program-org")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    result = build_program_org_relationship_source_lane(
        relationship_tsv_paths=args.relationship_tsv_path,
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
