"""Read-only market impact graph store helpers."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
from typing import Iterable

_SQLITE_IN_CLAUSE_BATCH_SIZE = 900
_REQUIRED_TABLES = {
    "build_metadata",
    "impact_nodes",
    "impact_edges",
    "impact_edge_packs",
}


@dataclass(frozen=True)
class MarketGraphNode:
    """Resolved metadata for one market graph node."""

    entity_ref: str
    canonical_name: str
    entity_type: str
    library_id: str | None = None
    is_tradable: bool = False
    is_seed_eligible: bool = False
    seed_degree: int = 0
    identifiers: dict[str, object] | None = None
    packs: tuple[str, ...] = ()


@dataclass(frozen=True)
class MarketGraphEdge:
    """One directed market transmission edge."""

    edge_id: str
    source_ref: str
    target_ref: str
    relation: str
    evidence_level: str
    confidence: float
    direction_hint: str
    source_name: str
    source_url: str
    source_snapshot: str
    source_year: int | None
    refresh_policy: str
    version: str
    packs: tuple[str, ...] = ()


def _normalize_refs(entity_refs: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(ref).strip() for ref in entity_refs if str(ref).strip()))


def _chunked_values(
    values: tuple[str, ...],
    *,
    chunk_size: int = _SQLITE_IN_CLAUSE_BATCH_SIZE,
) -> tuple[tuple[str, ...], ...]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive.")
    return tuple(values[start : start + chunk_size] for start in range(0, len(values), chunk_size))


def _parse_json_dict(raw: str | None) -> dict[str, object]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _parse_json_string_tuple(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return ()
    if not isinstance(value, list):
        return ()
    return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))


class MarketGraphStore:
    """Read-only helper over one market impact graph SQLite artifact."""

    def __init__(self, artifact_path: str | Path) -> None:
        self.artifact_path = Path(artifact_path).expanduser().resolve()
        if not self.artifact_path.exists():
            raise FileNotFoundError(f"Market graph store not found: {self.artifact_path}")
        self._connection = sqlite3.connect(
            f"file:{self.artifact_path}?mode=ro",
            uri=True,
        )
        self._connection.row_factory = sqlite3.Row
        self._node_cache: dict[str, MarketGraphNode] = {}
        self._metadata_cache: dict[str, object] | None = None
        self._validate_schema()

    def _validate_schema(self) -> None:
        rows = self._connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            """
        ).fetchall()
        table_names = {str(row["name"]) for row in rows}
        missing = sorted(_REQUIRED_TABLES - table_names)
        if missing:
            raise ValueError(f"Market graph store is missing required tables: {missing}")
        metadata = self.metadata()
        required_metadata = {
            "graph_version",
            "artifact_version",
            "artifact_hash",
            "builder_version",
            "built_at",
            "source_manifest_hash",
            "node_count",
            "edge_count",
        }
        missing_metadata = sorted(key for key in required_metadata if key not in metadata)
        if missing_metadata:
            raise ValueError(
                f"Market graph store metadata is missing required fields: {missing_metadata}"
            )

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "MarketGraphStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def metadata(self) -> dict[str, object]:
        """Return artifact metadata from the singleton build metadata row."""

        if self._metadata_cache is not None:
            return dict(self._metadata_cache)
        row = self._connection.execute(
            """
            SELECT
                graph_version,
                artifact_version,
                artifact_hash,
                builder_version,
                built_at,
                source_manifest_hash,
                node_count,
                edge_count
            FROM build_metadata
            WHERE id = 1
            """
        ).fetchone()
        if row is None:
            raise ValueError("Market graph store build_metadata row is missing.")
        self._metadata_cache = {
            "graph_version": str(row["graph_version"]),
            "artifact_version": str(row["artifact_version"]),
            "artifact_hash": str(row["artifact_hash"]),
            "builder_version": str(row["builder_version"]),
            "built_at": str(row["built_at"]),
            "source_manifest_hash": str(row["source_manifest_hash"]),
            "node_count": int(row["node_count"]),
            "edge_count": int(row["edge_count"]),
        }
        return dict(self._metadata_cache)

    def node_batch(self, entity_refs: Iterable[str]) -> dict[str, MarketGraphNode]:
        """Return graph nodes for multiple ADES entity refs."""

        normalized_refs = _normalize_refs(entity_refs)
        if not normalized_refs:
            return {}
        missing_refs = [ref for ref in normalized_refs if ref not in self._node_cache]
        if missing_refs:
            rows: list[sqlite3.Row] = []
            for ref_chunk in _chunked_values(tuple(missing_refs)):
                placeholders = ", ".join("?" for _ in ref_chunk)
                rows.extend(
                    self._connection.execute(
                        f"""
                        SELECT
                            entity_ref,
                            canonical_name,
                            entity_type,
                            library_id,
                            is_tradable,
                            is_seed_eligible,
                            seed_degree,
                            identifiers_json,
                            packs_json
                        FROM impact_nodes
                        WHERE entity_ref IN ({placeholders})
                        """,
                        ref_chunk,
                    ).fetchall()
                )
            for row in rows:
                entity_ref = str(row["entity_ref"])
                self._node_cache[entity_ref] = MarketGraphNode(
                    entity_ref=entity_ref,
                    canonical_name=str(row["canonical_name"]),
                    entity_type=str(row["entity_type"]),
                    library_id=(
                        str(row["library_id"]) if row["library_id"] is not None else None
                    ),
                    is_tradable=bool(int(row["is_tradable"])),
                    is_seed_eligible=bool(int(row["is_seed_eligible"])),
                    seed_degree=int(row["seed_degree"]),
                    identifiers=_parse_json_dict(row["identifiers_json"]),
                    packs=_parse_json_string_tuple(row["packs_json"]),
                )
        return {
            ref: self._node_cache[ref]
            for ref in normalized_refs
            if ref in self._node_cache
        }

    def node(self, entity_ref: str) -> MarketGraphNode | None:
        """Return one graph node by ADES entity ref."""

        return self.node_batch([entity_ref]).get(str(entity_ref).strip())

    def outbound_edges_batch(
        self,
        source_refs: Iterable[str],
        *,
        enabled_packs: Iterable[str] | None = None,
        limit_per_source: int = 64,
    ) -> dict[str, list[MarketGraphEdge]]:
        """Return bounded outbound edges for each source ref."""

        if limit_per_source <= 0:
            raise ValueError("limit_per_source must be positive.")
        normalized_sources = _normalize_refs(source_refs)
        normalized_packs = _normalize_refs(enabled_packs or ())
        results: dict[str, list[MarketGraphEdge]] = {source: [] for source in normalized_sources}
        if not normalized_sources:
            return results

        for source_ref in normalized_sources:
            if normalized_packs:
                pack_placeholders = ", ".join("?" for _ in normalized_packs)
                rows = self._connection.execute(
                    f"""
                    SELECT DISTINCT
                        e.edge_id,
                        e.source_ref,
                        e.target_ref,
                        e.relation,
                        e.evidence_level,
                        e.confidence,
                        e.direction_hint,
                        e.source_name,
                        e.source_url,
                        e.source_snapshot,
                        e.source_year,
                        e.refresh_policy,
                        e.version
                    FROM impact_edges AS e
                    JOIN impact_edge_packs AS p ON p.edge_id = e.edge_id
                    WHERE e.source_ref = ?
                      AND p.pack_id IN ({pack_placeholders})
                    ORDER BY e.target_ref ASC, e.relation ASC, e.edge_id ASC
                    LIMIT ?
                    """,
                    (source_ref, *normalized_packs, limit_per_source),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    """
                    SELECT
                        edge_id,
                        source_ref,
                        target_ref,
                        relation,
                        evidence_level,
                        confidence,
                        direction_hint,
                        source_name,
                        source_url,
                        source_snapshot,
                        source_year,
                        refresh_policy,
                        version
                    FROM impact_edges
                    WHERE source_ref = ?
                    ORDER BY target_ref ASC, relation ASC, edge_id ASC
                    LIMIT ?
                    """,
                    (source_ref, limit_per_source),
                ).fetchall()
            edges = [self._edge_from_row(row) for row in rows]
            results[source_ref] = self._attach_pack_memberships(edges)
        return results

    def _edge_from_row(self, row: sqlite3.Row) -> MarketGraphEdge:
        return MarketGraphEdge(
            edge_id=str(row["edge_id"]),
            source_ref=str(row["source_ref"]),
            target_ref=str(row["target_ref"]),
            relation=str(row["relation"]),
            evidence_level=str(row["evidence_level"]),
            confidence=float(row["confidence"]),
            direction_hint=str(row["direction_hint"]),
            source_name=str(row["source_name"]),
            source_url=str(row["source_url"]),
            source_snapshot=str(row["source_snapshot"]),
            source_year=int(row["source_year"]) if row["source_year"] is not None else None,
            refresh_policy=str(row["refresh_policy"]),
            version=str(row["version"]),
        )

    def _attach_pack_memberships(
        self,
        edges: list[MarketGraphEdge],
    ) -> list[MarketGraphEdge]:
        if not edges:
            return []
        edge_ids = tuple(edge.edge_id for edge in edges)
        pack_rows: list[sqlite3.Row] = []
        for edge_id_chunk in _chunked_values(edge_ids):
            placeholders = ", ".join("?" for _ in edge_id_chunk)
            pack_rows.extend(
                self._connection.execute(
                    f"""
                    SELECT edge_id, pack_id
                    FROM impact_edge_packs
                    WHERE edge_id IN ({placeholders})
                    ORDER BY edge_id ASC, pack_id ASC
                    """,
                    edge_id_chunk,
                ).fetchall()
            )
        packs_by_edge_id: dict[str, list[str]] = {}
        for row in pack_rows:
            packs_by_edge_id.setdefault(str(row["edge_id"]), []).append(str(row["pack_id"]))
        return [
            MarketGraphEdge(
                edge_id=edge.edge_id,
                source_ref=edge.source_ref,
                target_ref=edge.target_ref,
                relation=edge.relation,
                evidence_level=edge.evidence_level,
                confidence=edge.confidence,
                direction_hint=edge.direction_hint,
                source_name=edge.source_name,
                source_url=edge.source_url,
                source_snapshot=edge.source_snapshot,
                source_year=edge.source_year,
                refresh_policy=edge.refresh_policy,
                version=edge.version,
                packs=tuple(packs_by_edge_id.get(edge.edge_id, [])),
            )
            for edge in edges
        ]
