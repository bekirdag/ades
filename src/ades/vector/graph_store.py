"""Explicit QID graph store helpers for exact path and bounded batch queries."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Iterable, Literal

GraphDirection = Literal["out", "in", "both"]
_SQLITE_IN_CLAUSE_BATCH_SIZE = 900


@dataclass(frozen=True)
class GraphNodeMetadata:
    """Resolved metadata for one graph node."""

    qid: str
    entity_id: str
    canonical_text: str
    entity_type: str | None = None
    source_name: str | None = None
    popularity: float | None = None
    packs: tuple[str, ...] = ()


@dataclass(frozen=True)
class GraphNodeStats:
    """Precomputed degree statistics for one graph node."""

    qid: str
    out_degree: int = 0
    in_degree: int = 0
    degree_total: int = 0


@dataclass(frozen=True)
class GraphNeighbor:
    """One adjacent graph node plus the traversed relation."""

    qid: str
    entity_id: str
    canonical_text: str
    predicate: str
    direction: Literal["out", "in"]
    entity_type: str | None = None
    source_name: str | None = None
    popularity: float | None = None
    packs: tuple[str, ...] = ()


@dataclass(frozen=True)
class GraphPathStep:
    """One exact graph edge used in a traversed path."""

    src_qid: str
    predicate: str
    dst_qid: str
    traversed_reversed: bool = False


@dataclass(frozen=True)
class GraphPath:
    """Bounded exact path between two QIDs."""

    node_qids: tuple[str, ...]
    steps: tuple[GraphPathStep, ...]


@dataclass(frozen=True)
class SharedGraphNode:
    """One graph node shared across two or more seed QIDs."""

    qid: str
    entity_id: str
    canonical_text: str
    support_count: int
    supporting_qids: tuple[str, ...]
    min_depth: int
    entity_type: str | None = None
    source_name: str | None = None
    popularity: float | None = None
    packs: tuple[str, ...] = ()


def _normalize_predicates(predicates: Iterable[str] | None) -> tuple[str, ...] | None:
    if predicates is None:
        return None
    normalized = tuple(
        sorted(
            {
                str(predicate).strip().upper()
                for predicate in predicates
                if str(predicate).strip()
            }
        )
    )
    return normalized or None


def _normalize_qids(qids: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(qid).strip() for qid in qids if str(qid).strip()))


def _chunked_values(values: tuple[str, ...], *, chunk_size: int = _SQLITE_IN_CLAUSE_BATCH_SIZE) -> tuple[tuple[str, ...], ...]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive.")
    return tuple(
        values[start : start + chunk_size]
        for start in range(0, len(values), chunk_size)
    )


class QidGraphStore:
    """Read-only helper over one explicit QID graph SQLite artifact."""

    def __init__(self, artifact_path: str | Path) -> None:
        self.artifact_path = Path(artifact_path).expanduser().resolve()
        if not self.artifact_path.exists():
            raise FileNotFoundError(f"QID graph store not found: {self.artifact_path}")
        self._connection = sqlite3.connect(str(self.artifact_path))
        self._connection.row_factory = sqlite3.Row
        self._metadata_cache: dict[str, GraphNodeMetadata] = {}
        self._stats_cache: dict[str, GraphNodeStats] = {}
        self._node_columns = {
            str(row["name"])
            for row in self._connection.execute("PRAGMA table_info(nodes)").fetchall()
        }
        self._has_node_stats = (
            self._connection.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table' AND name = 'node_stats'
                """
            ).fetchone()
            is not None
        )

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "QidGraphStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    @staticmethod
    def _default_node_metadata(qid: str) -> GraphNodeMetadata:
        return GraphNodeMetadata(
            qid=qid,
            entity_id=f"wikidata:{qid}",
            canonical_text=qid,
            popularity=None,
        )

    @staticmethod
    def _default_node_stats(qid: str) -> GraphNodeStats:
        return GraphNodeStats(qid=qid)

    def node_metadata(self, qid: str) -> GraphNodeMetadata:
        """Return metadata for one QID."""

        return self.node_metadata_batch([qid]).get(str(qid), self._default_node_metadata(str(qid)))

    def node_metadata_batch(self, qids: Iterable[str]) -> dict[str, GraphNodeMetadata]:
        """Return metadata for multiple QIDs with one node query and one pack query."""

        normalized_qids = _normalize_qids(qids)
        if not normalized_qids:
            return {}
        missing_qids = [qid for qid in normalized_qids if qid not in self._metadata_cache]
        if missing_qids:
            select_columns = [
                "qid",
                "entity_id",
                "canonical_text",
                (
                    "entity_type"
                    if "entity_type" in self._node_columns
                    else "NULL AS entity_type"
                ),
                (
                    "source_name"
                    if "source_name" in self._node_columns
                    else "NULL AS source_name"
                ),
                (
                    "popularity"
                    if "popularity" in self._node_columns
                    else "NULL AS popularity"
                ),
            ]
            rows: list[sqlite3.Row] = []
            pack_rows: list[sqlite3.Row] = []
            for qid_chunk in _chunked_values(tuple(missing_qids)):
                placeholders = ", ".join("?" for _ in qid_chunk)
                rows.extend(
                    self._connection.execute(
                        f"""
                        SELECT {", ".join(select_columns)}
                        FROM nodes
                        WHERE qid IN ({placeholders})
                        """,
                        qid_chunk,
                    ).fetchall()
                )
                pack_rows.extend(
                    self._connection.execute(
                        f"""
                        SELECT qid, pack_id
                        FROM node_packs
                        WHERE qid IN ({placeholders})
                        ORDER BY qid ASC, pack_id ASC
                        """,
                        qid_chunk,
                    ).fetchall()
                )
            packs_by_qid: dict[str, list[str]] = {}
            for row in pack_rows:
                packs_by_qid.setdefault(str(row["qid"]), []).append(str(row["pack_id"]))
            seen_qids: set[str] = set()
            for row in rows:
                qid = str(row["qid"])
                seen_qids.add(qid)
                self._metadata_cache[qid] = GraphNodeMetadata(
                    qid=qid,
                    entity_id=str(row["entity_id"]),
                    canonical_text=str(row["canonical_text"]),
                    entity_type=(
                        str(row["entity_type"]) if row["entity_type"] is not None else None
                    ),
                    source_name=(
                        str(row["source_name"]) if row["source_name"] is not None else None
                    ),
                    popularity=(
                        float(row["popularity"]) if row["popularity"] is not None else None
                    ),
                    packs=tuple(packs_by_qid.get(qid, [])),
                )
            for qid in missing_qids:
                if qid not in seen_qids:
                    self._metadata_cache[qid] = self._default_node_metadata(qid)
        return {qid: self._metadata_cache[qid] for qid in normalized_qids}

    def node_stats(self, qid: str) -> GraphNodeStats:
        """Return precomputed degree statistics for one QID."""

        return self.node_stats_batch([qid]).get(str(qid), self._default_node_stats(str(qid)))

    def node_stats_batch(self, qids: Iterable[str]) -> dict[str, GraphNodeStats]:
        """Return precomputed degree statistics for multiple QIDs."""

        normalized_qids = _normalize_qids(qids)
        if not normalized_qids:
            return {}
        missing_qids = [qid for qid in normalized_qids if qid not in self._stats_cache]
        if missing_qids:
            if self._has_node_stats:
                rows: list[sqlite3.Row] = []
                for qid_chunk in _chunked_values(tuple(missing_qids)):
                    placeholders = ", ".join("?" for _ in qid_chunk)
                    rows.extend(
                        self._connection.execute(
                            f"""
                            SELECT qid, out_degree, in_degree, degree_total
                            FROM node_stats
                            WHERE qid IN ({placeholders})
                            """,
                            qid_chunk,
                        ).fetchall()
                    )
                seen_qids: set[str] = set()
                for row in rows:
                    qid = str(row["qid"])
                    seen_qids.add(qid)
                    self._stats_cache[qid] = GraphNodeStats(
                        qid=qid,
                        out_degree=int(row["out_degree"]),
                        in_degree=int(row["in_degree"]),
                        degree_total=int(row["degree_total"]),
                    )
                for qid in missing_qids:
                    if qid not in seen_qids:
                        self._stats_cache[qid] = self._default_node_stats(qid)
            else:
                for qid in missing_qids:
                    self._stats_cache[qid] = self._default_node_stats(qid)
        return {qid: self._stats_cache[qid] for qid in normalized_qids}

    @staticmethod
    def _sort_neighbors(neighbors: list[GraphNeighbor]) -> list[GraphNeighbor]:
        neighbors.sort(
            key=lambda item: (
                item.predicate,
                item.direction,
                item.canonical_text.casefold(),
                item.qid,
            )
        )
        return neighbors

    def _edge_rows_batch(
        self,
        qids: Iterable[str],
        *,
        direction: GraphDirection,
        predicates: tuple[str, ...] | None,
    ) -> dict[str, list[tuple[Literal["out", "in"], sqlite3.Row]]]:
        normalized_qids = _normalize_qids(qids)
        results = {qid: [] for qid in normalized_qids}
        if not normalized_qids:
            return results
        predicate_clause = ""
        predicate_params: tuple[str, ...] = ()
        if predicates is not None:
            predicate_clause = f" AND predicate IN ({', '.join('?' for _ in predicates)})"
            predicate_params = tuple(predicates)
        for qid_chunk in _chunked_values(normalized_qids):
            qid_placeholders = ", ".join("?" for _ in qid_chunk)
            if direction in {"out", "both"}:
                outbound_rows = self._connection.execute(
                    f"""
                    SELECT src_qid, predicate, dst_qid
                    FROM edges
                    WHERE src_qid IN ({qid_placeholders}){predicate_clause}
                    ORDER BY src_qid ASC, predicate ASC, dst_qid ASC
                    """,
                    (*qid_chunk, *predicate_params),
                ).fetchall()
                for row in outbound_rows:
                    results[str(row["src_qid"])].append(("out", row))
            if direction in {"in", "both"}:
                inbound_rows = self._connection.execute(
                    f"""
                    SELECT src_qid, predicate, dst_qid
                    FROM edges
                    WHERE dst_qid IN ({qid_placeholders}){predicate_clause}
                    ORDER BY dst_qid ASC, predicate ASC, src_qid ASC
                    """,
                    (*qid_chunk, *predicate_params),
                ).fetchall()
                for row in inbound_rows:
                    results[str(row["dst_qid"])].append(("in", row))
        return results

    def _edge_rows(
        self,
        qid: str,
        *,
        direction: GraphDirection,
        predicates: tuple[str, ...] | None,
    ) -> list[tuple[Literal["out", "in"], sqlite3.Row]]:
        return self._edge_rows_batch([qid], direction=direction, predicates=predicates).get(
            str(qid),
            [],
        )

    def _edge_rows_limited(
        self,
        qid: str,
        *,
        direction: GraphDirection,
        predicates: tuple[str, ...] | None,
        limit: int,
    ) -> list[tuple[Literal["out", "in"], sqlite3.Row]]:
        normalized_limit = max(limit, 0)
        if normalized_limit == 0:
            return []
        predicate_clause = ""
        predicate_params: tuple[str, ...] = ()
        if predicates is not None:
            predicate_clause = f" AND predicate IN ({', '.join('?' for _ in predicates)})"
            predicate_params = tuple(predicates)
        rows: list[tuple[Literal["out", "in"], sqlite3.Row]] = []
        if direction in {"out", "both"}:
            outbound_rows = self._connection.execute(
                f"""
                SELECT src_qid, predicate, dst_qid
                FROM edges
                WHERE src_qid = ?{predicate_clause}
                ORDER BY predicate ASC, dst_qid ASC
                LIMIT ?
                """,
                (qid, *predicate_params, normalized_limit),
            ).fetchall()
            rows.extend(("out", row) for row in outbound_rows)
        if direction in {"in", "both"}:
            inbound_rows = self._connection.execute(
                f"""
                SELECT src_qid, predicate, dst_qid
                FROM edges
                WHERE dst_qid = ?{predicate_clause}
                ORDER BY predicate ASC, src_qid ASC
                LIMIT ?
                """,
                (qid, *predicate_params, normalized_limit),
            ).fetchall()
            rows.extend(("in", row) for row in inbound_rows)
        rows.sort(
            key=lambda item: (
                str(item[1]["predicate"]),
                item[0],
                str(item[1]["dst_qid"] if item[0] == "out" else item[1]["src_qid"]),
            )
        )
        return rows[:normalized_limit]

    def neighbors_batch(
        self,
        qids: Iterable[str],
        *,
        direction: GraphDirection = "both",
        predicates: Iterable[str] | None = None,
        limit_per_qid: int | None = None,
    ) -> dict[str, list[GraphNeighbor]]:
        """Return bounded graph neighbors for multiple seed QIDs."""

        normalized_qids = _normalize_qids(qids)
        if not normalized_qids:
            return {}
        normalized_predicates = _normalize_predicates(predicates)
        if limit_per_qid is not None:
            edge_rows_by_qid = {
                qid: self._edge_rows_limited(
                    qid,
                    direction=direction,
                    predicates=normalized_predicates,
                    limit=limit_per_qid,
                )
                for qid in normalized_qids
            }
        else:
            edge_rows_by_qid = self._edge_rows_batch(
                normalized_qids,
                direction=direction,
                predicates=normalized_predicates,
            )
        related_qids: set[str] = set()
        for rows in edge_rows_by_qid.values():
            for edge_direction, row in rows:
                related_qids.add(
                    str(row["dst_qid"]) if edge_direction == "out" else str(row["src_qid"])
                )
        metadata_by_qid = self.node_metadata_batch(sorted(related_qids))
        neighbors_by_qid: dict[str, list[GraphNeighbor]] = {qid: [] for qid in normalized_qids}
        for seed_qid in normalized_qids:
            neighbors = neighbors_by_qid[seed_qid]
            for edge_direction, row in edge_rows_by_qid.get(seed_qid, []):
                neighbor_qid = (
                    str(row["dst_qid"]) if edge_direction == "out" else str(row["src_qid"])
                )
                metadata = metadata_by_qid.get(
                    neighbor_qid,
                    self._default_node_metadata(neighbor_qid),
                )
                neighbors.append(
                    GraphNeighbor(
                        qid=metadata.qid,
                        entity_id=metadata.entity_id,
                        canonical_text=metadata.canonical_text,
                        predicate=str(row["predicate"]),
                        direction=edge_direction,
                        entity_type=metadata.entity_type,
                        source_name=metadata.source_name,
                        popularity=metadata.popularity,
                        packs=metadata.packs,
                    )
                )
            self._sort_neighbors(neighbors)
            if limit_per_qid is not None:
                neighbors_by_qid[seed_qid] = neighbors[: max(limit_per_qid, 0)]
        return neighbors_by_qid

    def neighbors(
        self,
        qid: str,
        *,
        direction: GraphDirection = "both",
        predicates: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> list[GraphNeighbor]:
        """Return the bounded neighboring graph nodes for one QID."""

        return self.neighbors_batch(
            [qid],
            direction=direction,
            predicates=predicates,
            limit_per_qid=limit,
        ).get(str(qid), [])

    def shortest_path(
        self,
        start_qid: str,
        end_qid: str,
        *,
        max_depth: int = 3,
        predicates: Iterable[str] | None = None,
    ) -> GraphPath | None:
        """Return one shortest exact path between two QIDs, if any."""

        if max_depth < 0:
            raise ValueError("max_depth must be non-negative.")
        if start_qid == end_qid:
            return GraphPath(node_qids=(start_qid,), steps=())
        normalized_predicates = _normalize_predicates(predicates)
        queue: deque[tuple[str, list[GraphPathStep]]] = deque([(start_qid, [])])
        visited = {start_qid}
        while queue:
            current_qid, path_steps = queue.popleft()
            if len(path_steps) >= max_depth:
                continue
            for edge_direction, row in self._edge_rows(
                current_qid,
                direction="both",
                predicates=normalized_predicates,
            ):
                if edge_direction == "out":
                    next_qid = str(row["dst_qid"])
                    step = GraphPathStep(
                        src_qid=str(row["src_qid"]),
                        predicate=str(row["predicate"]),
                        dst_qid=str(row["dst_qid"]),
                        traversed_reversed=False,
                    )
                else:
                    next_qid = str(row["src_qid"])
                    step = GraphPathStep(
                        src_qid=str(row["src_qid"]),
                        predicate=str(row["predicate"]),
                        dst_qid=str(row["dst_qid"]),
                        traversed_reversed=True,
                    )
                if next_qid in visited:
                    continue
                next_steps = [*path_steps, step]
                if next_qid == end_qid:
                    return GraphPath(
                        node_qids=self._path_node_qids(start_qid, next_steps),
                        steps=tuple(next_steps),
                    )
                visited.add(next_qid)
                queue.append((next_qid, next_steps))
        return None

    def shared_neighbors(
        self,
        qids: Iterable[str],
        *,
        predicates: Iterable[str] | None = None,
        direction: GraphDirection = "both",
        limit: int = 20,
        limit_per_seed: int | None = None,
    ) -> list[SharedGraphNode]:
        """Return graph nodes directly shared by two or more seed QIDs."""

        seed_qids = _normalize_qids(qids)
        if not seed_qids:
            return []
        support: dict[str, dict[str, int]] = {}
        for seed_qid, neighbors in self.neighbors_batch(
            seed_qids,
            direction=direction,
            predicates=predicates,
            limit_per_qid=limit_per_seed,
        ).items():
            for neighbor in neighbors:
                support.setdefault(neighbor.qid, {})[seed_qid] = 1
        metadata_by_qid = self.node_metadata_batch(support.keys())
        shared: list[SharedGraphNode] = []
        for neighbor_qid, supporting in support.items():
            if len(supporting) < 2:
                continue
            metadata = metadata_by_qid.get(
                neighbor_qid,
                self._default_node_metadata(neighbor_qid),
            )
            shared.append(
                SharedGraphNode(
                    qid=metadata.qid,
                    entity_id=metadata.entity_id,
                    canonical_text=metadata.canonical_text,
                    support_count=len(supporting),
                    supporting_qids=tuple(sorted(supporting)),
                    min_depth=1,
                    entity_type=metadata.entity_type,
                    source_name=metadata.source_name,
                    popularity=metadata.popularity,
                    packs=metadata.packs,
                )
            )
        shared.sort(
            key=lambda item: (-item.support_count, item.min_depth, item.canonical_text.casefold())
        )
        return shared[: max(limit, 0)]

    def shared_ancestors(
        self,
        qids: Iterable[str],
        *,
        max_depth: int = 2,
        predicates: Iterable[str] | None = None,
        limit: int = 20,
        limit_per_hop: int | None = None,
    ) -> list[SharedGraphNode]:
        """Return shared outward ancestors under the selected predicates."""

        if max_depth < 1:
            raise ValueError("max_depth must be at least 1.")
        seed_qids = _normalize_qids(qids)
        if not seed_qids:
            return []
        normalized_predicates = _normalize_predicates(predicates)
        support: dict[str, dict[str, int]] = {}
        for seed_qid in seed_qids:
            queue: deque[tuple[str, int]] = deque([(seed_qid, 0)])
            visited = {seed_qid}
            while queue:
                current_qid, depth = queue.popleft()
                if depth >= max_depth:
                    continue
                for neighbor in self.neighbors(
                    current_qid,
                    direction="out",
                    predicates=normalized_predicates,
                    limit=limit_per_hop,
                ):
                    if neighbor.qid in visited:
                        continue
                    next_depth = depth + 1
                    visited.add(neighbor.qid)
                    existing_depth = support.setdefault(neighbor.qid, {}).get(seed_qid)
                    if existing_depth is None or next_depth < existing_depth:
                        support[neighbor.qid][seed_qid] = next_depth
                    queue.append((neighbor.qid, next_depth))
        metadata_by_qid = self.node_metadata_batch(support.keys())
        shared: list[SharedGraphNode] = []
        for ancestor_qid, supporting in support.items():
            if len(supporting) < 2:
                continue
            metadata = metadata_by_qid.get(
                ancestor_qid,
                self._default_node_metadata(ancestor_qid),
            )
            shared.append(
                SharedGraphNode(
                    qid=metadata.qid,
                    entity_id=metadata.entity_id,
                    canonical_text=metadata.canonical_text,
                    support_count=len(supporting),
                    supporting_qids=tuple(sorted(supporting)),
                    min_depth=min(supporting.values()),
                    entity_type=metadata.entity_type,
                    source_name=metadata.source_name,
                    popularity=metadata.popularity,
                    packs=metadata.packs,
                )
            )
        shared.sort(
            key=lambda item: (-item.support_count, item.min_depth, item.canonical_text.casefold())
        )
        return shared[: max(limit, 0)]

    @staticmethod
    def _path_node_qids(start_qid: str, steps: list[GraphPathStep]) -> tuple[str, ...]:
        node_qids = [start_qid]
        current_qid = start_qid
        for step in steps:
            current_qid = step.src_qid if step.traversed_reversed else step.dst_qid
            node_qids.append(current_qid)
        return tuple(node_qids)


def load_qid_graph_store(artifact_path: str | Path) -> QidGraphStore:
    """Open one explicit QID graph store artifact."""

    return QidGraphStore(artifact_path)
