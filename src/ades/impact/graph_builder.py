"""Offline builder for the market impact graph store."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Iterable

from ..service.models import MarketGraphStoreBuildResponse

MARKET_GRAPH_STORE_FILENAME = "market_graph_store.sqlite"
MARKET_GRAPH_MANIFEST_FILENAME = "market_graph_store_manifest.json"
MARKET_GRAPH_BUILDER_VERSION = "market-graph-builder-v1"
DEFAULT_MARKET_GRAPH_VERSION = "market-graph-v1"

_REQUIRED_NODE_COLUMNS = {"entity_ref", "canonical_name", "entity_type"}
_REQUIRED_EDGE_COLUMNS = {
    "source_ref",
    "target_ref",
    "relation",
    "evidence_level",
    "confidence",
    "direction_hint",
    "source_name",
    "source_url",
    "source_snapshot",
    "refresh_policy",
    "pack_ids",
}
_ALLOWED_EVIDENCE_LEVELS = {"direct", "shallow", "inferred"}


@dataclass(frozen=True)
class _NodeRecord:
    entity_ref: str
    canonical_name: str
    entity_type: str
    library_id: str | None
    is_tradable: bool
    is_seed_eligible: bool
    identifiers_json: str
    packs: tuple[str, ...]


@dataclass(frozen=True)
class _EdgeRecord:
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
    packs: tuple[str, ...]

    @property
    def dedupe_key(self) -> tuple[str, str, str, str]:
        return (self.source_ref, self.target_ref, self.relation, self.source_snapshot)


def _sha256_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _parse_bool(value: object | None, *, default: bool = False) -> bool:
    raw = _normalize_text(value).casefold()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def _parse_float(value: object | None, *, name: str) -> float:
    try:
        parsed = float(_normalize_text(value))
    except ValueError as exc:
        raise ValueError(f"Invalid {name}: {value!r}") from exc
    if parsed < 0 or parsed > 1:
        raise ValueError(f"Invalid {name}: {value!r}")
    return parsed


def _parse_int_or_none(value: object | None, *, name: str) -> int | None:
    raw = _normalize_text(value)
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid {name}: {value!r}") from exc


def _parse_json_dict_text(value: object | None) -> str:
    raw = _normalize_text(value)
    if not raw:
        return "{}"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid identifiers_json: {raw!r}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"Invalid identifiers_json: {raw!r}")
    return json.dumps(parsed, sort_keys=True, separators=(",", ":"))


def _parse_string_list(value: object | None) -> tuple[str, ...]:
    raw = _normalize_text(value)
    if not raw:
        return ()
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid list value: {raw!r}") from exc
        if not isinstance(parsed, list):
            raise ValueError(f"Invalid list value: {raw!r}")
        values = [str(item).strip() for item in parsed]
    else:
        values = [item.strip() for item in raw.replace(";", ",").split(",")]
    return tuple(sorted({item for item in values if item}))


def _read_tsv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Impact graph TSV not found: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if reader.fieldnames is None:
            raise ValueError(f"Impact graph TSV is missing headers: {path}")
        return [dict(row) for row in reader]


def _parse_node_rows(paths: Iterable[Path]) -> tuple[dict[str, _NodeRecord], list[str]]:
    nodes: dict[str, _NodeRecord] = {}
    warnings: list[str] = []
    for path in paths:
        rows = _read_tsv_rows(path)
        if not rows:
            continue
        missing_columns = sorted(_REQUIRED_NODE_COLUMNS - set(rows[0]))
        if missing_columns:
            raise ValueError(f"Node TSV {path} is missing columns: {missing_columns}")
        for index, row in enumerate(rows, start=2):
            entity_ref = _normalize_text(row.get("entity_ref"))
            canonical_name = _normalize_text(row.get("canonical_name"))
            entity_type = _normalize_text(row.get("entity_type"))
            if not entity_ref or not canonical_name or not entity_type:
                raise ValueError(f"Invalid node TSV row {path}:{index}")
            record = _NodeRecord(
                entity_ref=entity_ref,
                canonical_name=canonical_name,
                entity_type=entity_type,
                library_id=_normalize_text(row.get("library_id")) or None,
                is_tradable=_parse_bool(row.get("is_tradable"), default=False),
                is_seed_eligible=_parse_bool(row.get("is_seed_eligible"), default=False),
                identifiers_json=_parse_json_dict_text(row.get("identifiers_json")),
                packs=_parse_string_list(row.get("packs") or row.get("pack_ids")),
            )
            if entity_ref in nodes:
                warnings.append(f"duplicate_node_replaced:{entity_ref}")
            nodes[entity_ref] = record
    return nodes, warnings


def _edge_id_for(row: dict[str, object]) -> str:
    payload = json.dumps(row, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _parse_edge_rows(
    paths: Iterable[Path],
    *,
    artifact_version: str,
) -> tuple[dict[tuple[str, str, str, str], _EdgeRecord], list[str], int]:
    edges: dict[tuple[str, str, str, str], _EdgeRecord] = {}
    warnings: list[str] = []
    processed_row_count = 0
    for path in paths:
        rows = _read_tsv_rows(path)
        if not rows:
            continue
        missing_columns = sorted(_REQUIRED_EDGE_COLUMNS - set(rows[0]))
        if missing_columns:
            raise ValueError(f"Edge TSV {path} is missing columns: {missing_columns}")
        for index, row in enumerate(rows, start=2):
            processed_row_count += 1
            source_ref = _normalize_text(row.get("source_ref"))
            target_ref = _normalize_text(row.get("target_ref"))
            relation = _normalize_text(row.get("relation"))
            evidence_level = _normalize_text(row.get("evidence_level")).casefold()
            direction_hint = _normalize_text(row.get("direction_hint"))
            source_name = _normalize_text(row.get("source_name"))
            source_url = _normalize_text(row.get("source_url"))
            source_snapshot = _normalize_text(row.get("source_snapshot"))
            refresh_policy = _normalize_text(row.get("refresh_policy"))
            packs = _parse_string_list(row.get("pack_ids"))
            if not (
                source_ref
                and target_ref
                and relation
                and direction_hint
                and source_name
                and source_url
                and source_snapshot
                and refresh_policy
                and packs
            ):
                raise ValueError(f"Invalid edge TSV row {path}:{index}")
            if evidence_level not in _ALLOWED_EVIDENCE_LEVELS:
                raise ValueError(f"Invalid evidence_level at {path}:{index}: {evidence_level!r}")
            confidence = _parse_float(row.get("confidence"), name="confidence")
            source_year = _parse_int_or_none(row.get("source_year"), name="source_year")
            edge_version = _normalize_text(row.get("version")) or artifact_version
            edge_id = _edge_id_for(
                {
                    "source_ref": source_ref,
                    "target_ref": target_ref,
                    "relation": relation,
                    "source_snapshot": source_snapshot,
                }
            )
            record = _EdgeRecord(
                edge_id=edge_id,
                source_ref=source_ref,
                target_ref=target_ref,
                relation=relation,
                evidence_level=evidence_level,
                confidence=confidence,
                direction_hint=direction_hint,
                source_name=source_name,
                source_url=source_url,
                source_snapshot=source_snapshot,
                source_year=source_year,
                refresh_policy=refresh_policy,
                version=edge_version,
                packs=packs,
            )
            existing = edges.get(record.dedupe_key)
            if existing is None:
                edges[record.dedupe_key] = record
                continue
            merged_packs = tuple(sorted(set(existing.packs) | set(record.packs)))
            winner = record if record.confidence > existing.confidence else existing
            edges[record.dedupe_key] = _EdgeRecord(
                edge_id=winner.edge_id,
                source_ref=winner.source_ref,
                target_ref=winner.target_ref,
                relation=winner.relation,
                evidence_level=winner.evidence_level,
                confidence=winner.confidence,
                direction_hint=winner.direction_hint,
                source_name=winner.source_name,
                source_url=winner.source_url,
                source_snapshot=winner.source_snapshot,
                source_year=winner.source_year,
                refresh_policy=winner.refresh_policy,
                version=winner.version,
                packs=merged_packs,
            )
            warnings.append(f"duplicate_edge_merged:{source_ref}:{relation}:{target_ref}")
    return edges, warnings, processed_row_count


def _prepare_store(path: Path) -> sqlite3.Connection:
    if path.exists():
        path.unlink()
    connection = sqlite3.connect(str(path))
    connection.execute("PRAGMA journal_mode = OFF")
    connection.execute("PRAGMA synchronous = OFF")
    connection.execute("PRAGMA temp_store = MEMORY")
    connection.executescript(
        """
        CREATE TABLE build_metadata (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          graph_version TEXT NOT NULL,
          artifact_version TEXT NOT NULL,
          artifact_hash TEXT NOT NULL,
          builder_version TEXT NOT NULL,
          built_at TEXT NOT NULL,
          source_manifest_hash TEXT NOT NULL,
          node_count INTEGER NOT NULL,
          edge_count INTEGER NOT NULL
        );
        CREATE TABLE impact_nodes (
          entity_ref TEXT PRIMARY KEY,
          canonical_name TEXT NOT NULL,
          entity_type TEXT NOT NULL,
          library_id TEXT,
          is_tradable INTEGER NOT NULL DEFAULT 0,
          is_seed_eligible INTEGER NOT NULL DEFAULT 0,
          seed_degree INTEGER NOT NULL DEFAULT 0,
          identifiers_json TEXT NOT NULL DEFAULT '{}',
          packs_json TEXT NOT NULL DEFAULT '[]'
        );
        CREATE TABLE impact_edges (
          edge_id TEXT PRIMARY KEY,
          source_ref TEXT NOT NULL,
          target_ref TEXT NOT NULL,
          relation TEXT NOT NULL,
          evidence_level TEXT NOT NULL,
          confidence REAL NOT NULL,
          direction_hint TEXT NOT NULL,
          source_name TEXT NOT NULL,
          source_url TEXT NOT NULL,
          source_snapshot TEXT NOT NULL,
          source_year INTEGER,
          refresh_policy TEXT NOT NULL,
          version TEXT NOT NULL,
          UNIQUE (source_ref, target_ref, relation, source_snapshot)
        );
        CREATE TABLE impact_edge_packs (
          edge_id TEXT NOT NULL,
          pack_id TEXT NOT NULL,
          PRIMARY KEY (edge_id, pack_id)
        );
        CREATE INDEX idx_impact_edges_source ON impact_edges(source_ref);
        CREATE INDEX idx_impact_edges_target ON impact_edges(target_ref);
        CREATE INDEX idx_impact_edges_relation ON impact_edges(relation);
        CREATE INDEX idx_impact_edge_packs_pack ON impact_edge_packs(pack_id);
        CREATE INDEX idx_impact_nodes_seed_degree ON impact_nodes(seed_degree);
        """
    )
    return connection


def _ensure_edge_nodes(
    nodes: dict[str, _NodeRecord],
    edges: dict[tuple[str, str, str, str], _EdgeRecord],
) -> list[str]:
    warnings: list[str] = []
    for edge in edges.values():
        for entity_ref in (edge.source_ref, edge.target_ref):
            if entity_ref in nodes:
                continue
            nodes[entity_ref] = _NodeRecord(
                entity_ref=entity_ref,
                canonical_name=entity_ref,
                entity_type="unknown",
                library_id=None,
                is_tradable=False,
                is_seed_eligible=True,
                identifiers_json="{}",
                packs=edge.packs,
            )
            warnings.append(f"implicit_node_created:{entity_ref}")
    return warnings


def _source_manifest_hash(
    *,
    nodes: dict[str, _NodeRecord],
    edges: dict[tuple[str, str, str, str], _EdgeRecord],
    graph_version: str,
    artifact_version: str,
) -> str:
    payload = {
        "graph_version": graph_version,
        "artifact_version": artifact_version,
        "nodes": [record.__dict__ for record in sorted(nodes.values(), key=lambda item: item.entity_ref)],
        "edges": [
            record.__dict__ for record in sorted(edges.values(), key=lambda item: item.edge_id)
        ],
    }
    return _sha256_text(json.dumps(payload, sort_keys=True, default=list, separators=(",", ":")))


def build_market_graph_store(
    *,
    edge_tsv_paths: Iterable[str | Path],
    output_dir: str | Path,
    node_tsv_paths: Iterable[str | Path] = (),
    graph_version: str = DEFAULT_MARKET_GRAPH_VERSION,
    artifact_version: str | None = None,
) -> MarketGraphStoreBuildResponse:
    """Build one market impact graph store from normalized TSV source lanes."""

    resolved_edge_paths = [Path(path).expanduser().resolve() for path in edge_tsv_paths]
    if not resolved_edge_paths:
        raise ValueError("At least one edge TSV path is required.")
    resolved_node_paths = [Path(path).expanduser().resolve() for path in node_tsv_paths]
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_graph_version = _normalize_text(graph_version) or DEFAULT_MARKET_GRAPH_VERSION
    resolved_artifact_version = (
        _normalize_text(artifact_version)
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    )

    nodes, node_warnings = _parse_node_rows(resolved_node_paths)
    edges, edge_warnings, processed_edge_row_count = _parse_edge_rows(
        resolved_edge_paths,
        artifact_version=resolved_artifact_version,
    )
    if not edges:
        raise ValueError("No impact graph edge rows were found.")
    implicit_node_warnings = _ensure_edge_nodes(nodes, edges)
    source_manifest_hash = _source_manifest_hash(
        nodes=nodes,
        edges=edges,
        graph_version=resolved_graph_version,
        artifact_version=resolved_artifact_version,
    )

    artifact_path = resolved_output_dir / MARKET_GRAPH_STORE_FILENAME
    manifest_path = resolved_output_dir / MARKET_GRAPH_MANIFEST_FILENAME
    connection = _prepare_store(artifact_path)
    try:
        connection.executemany(
            """
            INSERT INTO impact_nodes (
                entity_ref,
                canonical_name,
                entity_type,
                library_id,
                is_tradable,
                is_seed_eligible,
                identifiers_json,
                packs_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    record.entity_ref,
                    record.canonical_name,
                    record.entity_type,
                    record.library_id,
                    1 if record.is_tradable else 0,
                    1 if record.is_seed_eligible else 0,
                    record.identifiers_json,
                    json.dumps(list(record.packs), sort_keys=True),
                )
                for record in sorted(nodes.values(), key=lambda item: item.entity_ref)
            ],
        )
        connection.executemany(
            """
            INSERT INTO impact_edges (
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    record.edge_id,
                    record.source_ref,
                    record.target_ref,
                    record.relation,
                    record.evidence_level,
                    record.confidence,
                    record.direction_hint,
                    record.source_name,
                    record.source_url,
                    record.source_snapshot,
                    record.source_year,
                    record.refresh_policy,
                    record.version,
                )
                for record in sorted(edges.values(), key=lambda item: item.edge_id)
            ],
        )
        pack_rows = [
            (record.edge_id, pack_id)
            for record in sorted(edges.values(), key=lambda item: item.edge_id)
            for pack_id in record.packs
        ]
        connection.executemany(
            """
            INSERT OR IGNORE INTO impact_edge_packs (edge_id, pack_id)
            VALUES (?, ?)
            """,
            pack_rows,
        )
        connection.executescript(
            """
            UPDATE impact_nodes
            SET seed_degree = (
                SELECT COUNT(*)
                FROM impact_edges
                WHERE impact_edges.source_ref = impact_nodes.entity_ref
            );
            """
        )
        node_count = int(connection.execute("SELECT COUNT(*) FROM impact_nodes").fetchone()[0])
        edge_count = int(connection.execute("SELECT COUNT(*) FROM impact_edges").fetchone()[0])
        pack_ids = [
            str(row[0])
            for row in connection.execute(
                """
                SELECT DISTINCT pack_id
                FROM impact_edge_packs
                ORDER BY pack_id ASC
                """
            ).fetchall()
        ]
        artifact_hash = source_manifest_hash
        built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        connection.execute(
            """
            INSERT INTO build_metadata (
                id,
                graph_version,
                artifact_version,
                artifact_hash,
                builder_version,
                built_at,
                source_manifest_hash,
                node_count,
                edge_count
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                resolved_graph_version,
                resolved_artifact_version,
                artifact_hash,
                MARKET_GRAPH_BUILDER_VERSION,
                built_at,
                source_manifest_hash,
                node_count,
                edge_count,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    response = MarketGraphStoreBuildResponse(
        output_dir=str(resolved_output_dir),
        manifest_path=str(manifest_path),
        artifact_path=str(artifact_path),
        graph_version=resolved_graph_version,
        artifact_version=resolved_artifact_version,
        artifact_hash=artifact_hash,
        builder_version=MARKET_GRAPH_BUILDER_VERSION,
        node_count=node_count,
        edge_count=edge_count,
        source_manifest_hash=source_manifest_hash,
        node_tsv_paths=[str(path) for path in resolved_node_paths],
        edge_tsv_paths=[str(path) for path in resolved_edge_paths],
        pack_ids=pack_ids,
        processed_edge_row_count=processed_edge_row_count,
        warnings=node_warnings + edge_warnings + implicit_node_warnings,
    )
    manifest_path.write_text(
        json.dumps(response.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return response
