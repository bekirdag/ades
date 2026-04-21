"""Offline builder for one explicit QID graph store."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sqlite3
from typing import Iterable

from ..service.models import QidGraphStoreBuildResponse
from .builder import (
    DEFAULT_QID_GRAPH_ALLOWED_PREDICATES,
    _TRUTHY_ENTITY_TRIPLE_RE,
    _iter_text_lines,
    _load_targets,
)

_GRAPH_STORE_BUILDER_VERSION = "qid-graph-store-v3"
_NODE_BATCH_SIZE = 4096
_EDGE_BATCH_SIZE = 4096


def _prepare_graph_store(path: Path) -> sqlite3.Connection:
    if path.exists():
        path.unlink()
    connection = sqlite3.connect(str(path))
    connection.execute("PRAGMA journal_mode = OFF")
    connection.execute("PRAGMA synchronous = OFF")
    connection.execute("PRAGMA temp_store = MEMORY")
    connection.executescript(
        """
        CREATE TABLE nodes (
            qid TEXT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            canonical_text TEXT NOT NULL,
            entity_type TEXT,
            source_name TEXT,
            popularity REAL,
            is_target INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE node_packs (
            qid TEXT NOT NULL,
            pack_id TEXT NOT NULL,
            PRIMARY KEY (qid, pack_id)
        );
        CREATE TABLE edges (
            src_qid TEXT NOT NULL,
            predicate TEXT NOT NULL,
            dst_qid TEXT NOT NULL,
            PRIMARY KEY (src_qid, predicate, dst_qid)
        ) WITHOUT ROWID;
        CREATE TABLE node_stats (
            qid TEXT PRIMARY KEY,
            out_degree INTEGER NOT NULL DEFAULT 0,
            in_degree INTEGER NOT NULL DEFAULT 0,
            degree_total INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX idx_edges_src_predicate ON edges (src_qid, predicate, dst_qid);
        CREATE INDEX idx_edges_dst_predicate ON edges (dst_qid, predicate, src_qid);
        """
    )
    return connection


def _insert_target_nodes(connection: sqlite3.Connection, targets: dict[str, object]) -> None:
    node_rows: list[tuple[object, ...]] = []
    pack_rows: list[tuple[str, str]] = []
    for qid in sorted(targets):
        entry = targets[qid]
        node_rows.append(
            (
                qid,
                getattr(entry, "entity_id"),
                getattr(entry, "canonical_text"),
                getattr(entry, "entity_type"),
                getattr(entry, "source_name"),
                getattr(entry, "popularity"),
                1,
            )
        )
        pack_rows.extend((qid, pack_id) for pack_id in sorted(getattr(entry, "packs")))
    connection.executemany(
        """
        INSERT OR REPLACE INTO nodes (
            qid,
            entity_id,
            canonical_text,
            entity_type,
            source_name,
            popularity,
            is_target
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        node_rows,
    )
    connection.executemany(
        """
        INSERT OR IGNORE INTO node_packs (qid, pack_id)
        VALUES (?, ?)
        """,
        pack_rows,
    )


def _flush_pending_rows(
    connection: sqlite3.Connection,
    *,
    pending_nodes: set[str],
    pending_edges: list[tuple[str, str, str]],
) -> tuple[int, int]:
    inserted_nodes = 0
    inserted_edges = 0
    if pending_nodes:
        before = connection.total_changes
        connection.executemany(
            """
            INSERT OR IGNORE INTO nodes (
                qid,
                entity_id,
                canonical_text,
                entity_type,
                source_name,
                popularity,
                is_target
            ) VALUES (?, ?, ?, ?, ?, ?, 0)
            """,
            [
                (
                    qid,
                    f"wikidata:{qid}",
                    qid,
                    None,
                    None,
                    None,
                )
                for qid in sorted(pending_nodes)
            ],
        )
        inserted_nodes = connection.total_changes - before
        pending_nodes.clear()
    if pending_edges:
        before = connection.total_changes
        connection.executemany(
            """
            INSERT OR IGNORE INTO edges (src_qid, predicate, dst_qid)
            VALUES (?, ?, ?)
            """,
            pending_edges,
        )
        inserted_edges = connection.total_changes - before
        pending_edges.clear()
    return inserted_nodes, inserted_edges


def _ingest_truthy_edges(
    connection: sqlite3.Connection,
    *,
    truthy_path: Path,
    target_qids: set[str],
    allowed_predicates: set[str],
) -> tuple[int, int, int]:
    processed_line_count = 0
    matched_statement_count = 0
    stored_edge_count = 0
    known_nodes = set(target_qids)
    pending_nodes: set[str] = set()
    pending_edges: list[tuple[str, str, str]] = []
    for line in _iter_text_lines(truthy_path):
        processed_line_count += 1
        match = _TRUTHY_ENTITY_TRIPLE_RE.match(line)
        if match is None:
            continue
        predicate = match.group("predicate")
        if predicate not in allowed_predicates:
            continue
        subject = match.group("subject")
        obj = match.group("object")
        if subject not in target_qids and obj not in target_qids:
            continue
        matched_statement_count += 1
        if subject not in known_nodes:
            pending_nodes.add(subject)
            known_nodes.add(subject)
        if obj not in known_nodes:
            pending_nodes.add(obj)
            known_nodes.add(obj)
        pending_edges.append((subject, predicate, obj))
        if len(pending_nodes) >= _NODE_BATCH_SIZE or len(pending_edges) >= _EDGE_BATCH_SIZE:
            _, inserted_edges = _flush_pending_rows(
                connection,
                pending_nodes=pending_nodes,
                pending_edges=pending_edges,
            )
            stored_edge_count += inserted_edges
    _, inserted_edges = _flush_pending_rows(
        connection,
        pending_nodes=pending_nodes,
        pending_edges=pending_edges,
    )
    stored_edge_count += inserted_edges
    return processed_line_count, matched_statement_count, stored_edge_count


def _populate_node_stats(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM node_stats")
    connection.executescript(
        """
        INSERT INTO node_stats (qid, out_degree, in_degree, degree_total)
        SELECT
            nodes.qid,
            COALESCE(outbound.out_degree, 0),
            COALESCE(inbound.in_degree, 0),
            COALESCE(outbound.out_degree, 0) + COALESCE(inbound.in_degree, 0)
        FROM nodes
        LEFT JOIN (
            SELECT src_qid AS qid, COUNT(*) AS out_degree
            FROM edges
            GROUP BY src_qid
        ) AS outbound ON outbound.qid = nodes.qid
        LEFT JOIN (
            SELECT dst_qid AS qid, COUNT(*) AS in_degree
            FROM edges
            GROUP BY dst_qid
        ) AS inbound ON inbound.qid = nodes.qid;
        """
    )


def _graph_store_summary(connection: sqlite3.Connection) -> dict[str, object]:
    adjacent_node_count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM nodes
            WHERE is_target = 0
            """
        ).fetchone()[0]
    )
    predicate_histogram = {
        str(row[0]): int(row[1])
        for row in connection.execute(
            """
            SELECT predicate, COUNT(*)
            FROM edges
            GROUP BY predicate
            ORDER BY predicate ASC
            """
        ).fetchall()
    }
    degree_rows = [
        int(row[0])
        for row in connection.execute(
            """
            SELECT degree_total
            FROM node_stats
            ORDER BY degree_total ASC
            """
        ).fetchall()
    ]
    if not degree_rows:
        return {
            "adjacent_node_count": adjacent_node_count,
            "predicate_histogram": predicate_histogram,
            "max_degree_total": 0,
            "mean_degree_total": 0.0,
            "p95_degree_total": 0,
        }
    p95_index = max(0, math.ceil(len(degree_rows) * 0.95) - 1)
    return {
        "adjacent_node_count": adjacent_node_count,
        "predicate_histogram": predicate_histogram,
        "max_degree_total": degree_rows[-1],
        "mean_degree_total": round(sum(degree_rows) / len(degree_rows), 4),
        "p95_degree_total": degree_rows[p95_index],
    }


def build_qid_graph_store(
    bundle_dirs: Iterable[str | Path],
    *,
    truthy_path: str | Path,
    output_dir: str | Path,
    allowed_predicates: Iterable[str] | None = None,
) -> QidGraphStoreBuildResponse:
    """Build one explicit QID graph store artifact for exact graph queries."""

    resolved_bundle_dirs = [Path(path).expanduser().resolve() for path in bundle_dirs]
    if not resolved_bundle_dirs:
        raise ValueError("At least one bundle directory is required.")
    resolved_truthy_path = Path(truthy_path).expanduser().resolve()
    if not resolved_truthy_path.exists():
        raise FileNotFoundError(f"Truthy RDF dump not found: {resolved_truthy_path}")
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    predicate_values = {
        str(predicate).strip().upper()
        for predicate in (
            allowed_predicates if allowed_predicates is not None else DEFAULT_QID_GRAPH_ALLOWED_PREDICATES
        )
        if str(predicate).strip()
    }
    if not predicate_values:
        raise ValueError("At least one allowed predicate is required.")

    targets, pack_ids, warnings = _load_targets(
        resolved_bundle_dirs,
        dimensions=1,
    )
    if not targets:
        raise ValueError("No Wikidata-backed QIDs were found in the provided bundles.")

    artifact_path = resolved_output_dir / "qid_graph_store.sqlite"
    manifest_path = resolved_output_dir / "qid_graph_store_manifest.json"
    connection = _prepare_graph_store(artifact_path)
    try:
        _insert_target_nodes(connection, targets)
        processed_line_count, matched_statement_count, stored_edge_count = _ingest_truthy_edges(
            connection,
            truthy_path=resolved_truthy_path,
            target_qids=set(targets),
            allowed_predicates=predicate_values,
        )
        _populate_node_stats(connection)
        stored_node_count = int(connection.execute("SELECT COUNT(*) FROM nodes").fetchone()[0])
        summary = _graph_store_summary(connection)
        connection.commit()
    finally:
        connection.close()

    response = QidGraphStoreBuildResponse(
        output_dir=str(resolved_output_dir),
        manifest_path=str(manifest_path),
        artifact_path=str(artifact_path),
        target_node_count=len(targets),
        stored_node_count=stored_node_count,
        stored_edge_count=stored_edge_count,
        bundle_count=len(resolved_bundle_dirs),
        bundle_dirs=[str(path) for path in resolved_bundle_dirs],
        pack_ids=pack_ids,
        truthy_path=str(resolved_truthy_path),
        processed_line_count=processed_line_count,
        matched_statement_count=matched_statement_count,
        allowed_predicates=sorted(predicate_values),
        adjacent_node_count=int(summary["adjacent_node_count"]),
        predicate_histogram={
            str(key): int(value) for key, value in dict(summary["predicate_histogram"]).items()
        },
        max_degree_total=int(summary["max_degree_total"]),
        mean_degree_total=float(summary["mean_degree_total"]),
        p95_degree_total=int(summary["p95_degree_total"]),
        warnings=warnings,
    )
    manifest_path.write_text(
        json.dumps(
            {
                **response.model_dump(mode="json"),
                "builder_version": _GRAPH_STORE_BUILDER_VERSION,
                "artifact_scope_version": 3,
                "built_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return response
