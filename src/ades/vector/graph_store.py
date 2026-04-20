"""Explicit QID graph store helpers for exact path and ancestor queries."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Iterable, Literal

GraphDirection = Literal["out", "in", "both"]


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


class QidGraphStore:
    """Read-only helper over one explicit QID graph SQLite artifact."""

    def __init__(self, artifact_path: str | Path) -> None:
        self.artifact_path = Path(artifact_path).expanduser().resolve()
        if not self.artifact_path.exists():
            raise FileNotFoundError(f"QID graph store not found: {self.artifact_path}")
        self._connection = sqlite3.connect(str(self.artifact_path))
        self._connection.row_factory = sqlite3.Row
        self._metadata_cache: dict[str, dict[str, object]] = {}

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "QidGraphStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def _node_metadata(self, qid: str) -> dict[str, object]:
        cached = self._metadata_cache.get(qid)
        if cached is not None:
            return cached
        row = self._connection.execute(
            """
            SELECT qid, entity_id, canonical_text, entity_type, source_name
            FROM nodes
            WHERE qid = ?
            """,
            (qid,),
        ).fetchone()
        if row is None:
            metadata = {
                "qid": qid,
                "entity_id": f"wikidata:{qid}",
                "canonical_text": qid,
                "entity_type": None,
                "source_name": None,
                "packs": (),
            }
            self._metadata_cache[qid] = metadata
            return metadata
        packs = tuple(
            row_[0]
            for row_ in self._connection.execute(
                """
                SELECT pack_id
                FROM node_packs
                WHERE qid = ?
                ORDER BY pack_id ASC
                """,
                (qid,),
            )
        )
        metadata = {
            "qid": row["qid"],
            "entity_id": row["entity_id"],
            "canonical_text": row["canonical_text"],
            "entity_type": row["entity_type"],
            "source_name": row["source_name"],
            "packs": packs,
        }
        self._metadata_cache[qid] = metadata
        return metadata

    def _edge_rows(
        self,
        qid: str,
        *,
        direction: GraphDirection,
        predicates: tuple[str, ...] | None,
    ) -> list[tuple[Literal["out", "in"], sqlite3.Row]]:
        rows: list[tuple[Literal["out", "in"], sqlite3.Row]] = []
        if direction in {"out", "both"}:
            if predicates is None:
                result = self._connection.execute(
                    """
                    SELECT src_qid, predicate, dst_qid
                    FROM edges
                    WHERE src_qid = ?
                    ORDER BY predicate ASC, dst_qid ASC
                    """,
                    (qid,),
                )
            else:
                placeholders = ", ".join("?" for _ in predicates)
                result = self._connection.execute(
                    f"""
                    SELECT src_qid, predicate, dst_qid
                    FROM edges
                    WHERE src_qid = ? AND predicate IN ({placeholders})
                    ORDER BY predicate ASC, dst_qid ASC
                    """,
                    (qid, *predicates),
                )
            rows.extend(("out", row) for row in result.fetchall())
        if direction in {"in", "both"}:
            if predicates is None:
                result = self._connection.execute(
                    """
                    SELECT src_qid, predicate, dst_qid
                    FROM edges
                    WHERE dst_qid = ?
                    ORDER BY predicate ASC, src_qid ASC
                    """,
                    (qid,),
                )
            else:
                placeholders = ", ".join("?" for _ in predicates)
                result = self._connection.execute(
                    f"""
                    SELECT src_qid, predicate, dst_qid
                    FROM edges
                    WHERE dst_qid = ? AND predicate IN ({placeholders})
                    ORDER BY predicate ASC, src_qid ASC
                    """,
                    (qid, *predicates),
                )
            rows.extend(("in", row) for row in result.fetchall())
        return rows

    def neighbors(
        self,
        qid: str,
        *,
        direction: GraphDirection = "both",
        predicates: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> list[GraphNeighbor]:
        """Return the bounded neighboring graph nodes for one QID."""

        normalized_predicates = _normalize_predicates(predicates)
        neighbors: list[GraphNeighbor] = []
        for edge_direction, row in self._edge_rows(
            qid,
            direction=direction,
            predicates=normalized_predicates,
        ):
            neighbor_qid = row["dst_qid"] if edge_direction == "out" else row["src_qid"]
            metadata = self._node_metadata(str(neighbor_qid))
            neighbors.append(
                GraphNeighbor(
                    qid=str(metadata["qid"]),
                    entity_id=str(metadata["entity_id"]),
                    canonical_text=str(metadata["canonical_text"]),
                    predicate=str(row["predicate"]),
                    direction=edge_direction,
                    entity_type=(
                        str(metadata["entity_type"])
                        if metadata["entity_type"] is not None
                        else None
                    ),
                    source_name=(
                        str(metadata["source_name"])
                        if metadata["source_name"] is not None
                        else None
                    ),
                    packs=tuple(str(pack_id) for pack_id in metadata["packs"]),
                )
            )
        neighbors.sort(
            key=lambda item: (
                item.predicate,
                item.direction,
                item.canonical_text.casefold(),
                item.qid,
            )
        )
        if limit is None:
            return neighbors
        return neighbors[:limit]

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
    ) -> list[SharedGraphNode]:
        """Return graph nodes directly shared by two or more seed QIDs."""

        seed_qids = tuple(dict.fromkeys(str(qid).strip() for qid in qids if str(qid).strip()))
        if not seed_qids:
            return []
        support: dict[str, dict[str, int]] = {}
        for seed_qid in seed_qids:
            for neighbor in self.neighbors(
                seed_qid,
                direction=direction,
                predicates=predicates,
            ):
                support.setdefault(neighbor.qid, {})[seed_qid] = 1
        shared: list[SharedGraphNode] = []
        for neighbor_qid, supporting in support.items():
            if len(supporting) < 2:
                continue
            metadata = self._node_metadata(neighbor_qid)
            shared.append(
                SharedGraphNode(
                    qid=str(metadata["qid"]),
                    entity_id=str(metadata["entity_id"]),
                    canonical_text=str(metadata["canonical_text"]),
                    support_count=len(supporting),
                    supporting_qids=tuple(sorted(supporting)),
                    min_depth=1,
                    entity_type=(
                        str(metadata["entity_type"])
                        if metadata["entity_type"] is not None
                        else None
                    ),
                    source_name=(
                        str(metadata["source_name"])
                        if metadata["source_name"] is not None
                        else None
                    ),
                    packs=tuple(str(pack_id) for pack_id in metadata["packs"]),
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
    ) -> list[SharedGraphNode]:
        """Return shared outward ancestors under the selected predicates."""

        if max_depth < 1:
            raise ValueError("max_depth must be at least 1.")
        seed_qids = tuple(dict.fromkeys(str(qid).strip() for qid in qids if str(qid).strip()))
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
                ):
                    if neighbor.qid in visited:
                        continue
                    next_depth = depth + 1
                    visited.add(neighbor.qid)
                    existing_depth = support.setdefault(neighbor.qid, {}).get(seed_qid)
                    if existing_depth is None or next_depth < existing_depth:
                        support[neighbor.qid][seed_qid] = next_depth
                    queue.append((neighbor.qid, next_depth))
        shared: list[SharedGraphNode] = []
        for ancestor_qid, supporting in support.items():
            if len(supporting) < 2:
                continue
            metadata = self._node_metadata(ancestor_qid)
            shared.append(
                SharedGraphNode(
                    qid=str(metadata["qid"]),
                    entity_id=str(metadata["entity_id"]),
                    canonical_text=str(metadata["canonical_text"]),
                    support_count=len(supporting),
                    supporting_qids=tuple(sorted(supporting)),
                    min_depth=min(supporting.values()),
                    entity_type=(
                        str(metadata["entity_type"])
                        if metadata["entity_type"] is not None
                        else None
                    ),
                    source_name=(
                        str(metadata["source_name"])
                        if metadata["source_name"] is not None
                        else None
                    ),
                    packs=tuple(str(pack_id) for pack_id in metadata["packs"]),
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
