#!/usr/bin/env python3
"""Build one filtered QID collection from an existing shared artifact."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
from typing import Any, Iterable

from ades.vector.qdrant import QdrantVectorSearchClient


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Filter one existing qid_graph_points.jsonl.gz artifact down to the "
            "Wikidata-backed ids present in one or more bundle directories, "
            "write a new artifact + manifest, and optionally publish it to Qdrant."
        )
    )
    parser.add_argument(
        "--source-artifact",
        type=Path,
        required=True,
        help="Existing qid_graph_points.jsonl.gz artifact to filter.",
    )
    parser.add_argument(
        "--bundle-dir",
        action="append",
        type=Path,
        required=True,
        help="Bundle directory with normalized/entities.jsonl. Repeat as needed.",
    )
    parser.add_argument(
        "--extra-entity-id",
        action="append",
        default=[],
        help=(
            "Optional explicit entity id or bare QID to keep from the shared artifact "
            "even when it is not present in the bundle dirs. Repeat as needed."
        ),
    )
    parser.add_argument(
        "--extra-qid-file",
        action="append",
        type=Path,
        default=[],
        help=(
            "Optional file containing extra QIDs/entity ids to keep. Files may be "
            "newline-delimited text or a JSON array/object."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory where the filtered artifact + manifest should be written.",
    )
    parser.add_argument(
        "--collection-name",
        required=True,
        help="Immutable collection name to publish into when Qdrant is enabled.",
    )
    parser.add_argument(
        "--exclude-source-name",
        action="append",
        default=[],
        help=(
            "Optional payload source_name values to drop from the filtered artifact. "
            "Repeat as needed."
        ),
    )
    parser.add_argument(
        "--qdrant-url",
        type=str,
        default=None,
        help="Optional Qdrant base URL. When provided, the filtered artifact is published.",
    )
    parser.add_argument(
        "--qdrant-api-key",
        type=str,
        default=None,
        help="Optional Qdrant API key.",
    )
    parser.add_argument(
        "--publish-alias",
        type=str,
        default=None,
        help="Optional alias to atomically move after publish.",
    )
    return parser


def _load_bundle_qids(bundle_dir: Path) -> set[str]:
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    if not entities_path.exists():
        raise FileNotFoundError(f"Bundle entities file not found: {entities_path}")
    qids: set[str] = set()
    with entities_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            payload = json.loads(line)
            candidate_ids = []
            entity_id = payload.get("entity_id")
            if isinstance(entity_id, str):
                candidate_ids.append(entity_id)
            link = payload.get("link")
            if isinstance(link, dict):
                linked_entity_id = link.get("entity_id")
                if isinstance(linked_entity_id, str):
                    candidate_ids.append(linked_entity_id)
            for candidate_id in candidate_ids:
                normalized = candidate_id.strip()
                if normalized.startswith("wikidata:Q"):
                    qids.add(normalized)
    return qids


def _normalize_candidate_entity_id(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.startswith("wikidata:Q"):
        return normalized
    if normalized.startswith("Q") and normalized[1:].isdigit():
        return f"wikidata:{normalized}"
    return None


def _extract_extra_qids_from_json_payload(payload: object) -> set[str]:
    extracted: set[str] = set()
    if isinstance(payload, list):
        for item in payload:
            normalized = _normalize_candidate_entity_id(item)
            if normalized is not None:
                extracted.add(normalized)
    elif isinstance(payload, dict):
        for key in ("entity_ids", "extra_entity_ids", "qids", "extra_qids"):
            values = payload.get(key)
            if isinstance(values, list):
                extracted.update(_extract_extra_qids_from_json_payload(values))
        for key in ("entity_id", "qid"):
            normalized = _normalize_candidate_entity_id(payload.get(key))
            if normalized is not None:
                extracted.add(normalized)
    return extracted


def _load_extra_qids_from_file(path: Path) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(f"Extra QID file not found: {path}")
    content = path.read_text(encoding="utf-8").strip()
    if not content:
        return set()
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        payload = None
    if payload is not None:
        return _extract_extra_qids_from_json_payload(payload)
    qids: set[str] = set()
    for line in content.splitlines():
        normalized = _normalize_candidate_entity_id(line)
        if normalized is not None:
            qids.add(normalized)
    return qids


def _load_extra_qids(
    *,
    extra_entity_ids: Iterable[str],
    extra_qid_files: Iterable[Path],
) -> set[str]:
    qids: set[str] = set()
    for value in extra_entity_ids:
        normalized = _normalize_candidate_entity_id(value)
        if normalized is not None:
            qids.add(normalized)
    for path in extra_qid_files:
        qids.update(_load_extra_qids_from_file(path.expanduser().resolve()))
    return qids


def _iter_dense_points_from_sparse_artifact(
    artifact_path: Path,
    *,
    allowed_ids: set[str],
    dimensions: int,
    excluded_source_names: set[str],
) -> tuple[list[dict[str, Any]], int]:
    filtered_points: list[dict[str, Any]] = []
    scanned_point_count = 0
    with gzip.open(artifact_path, "rt", encoding="utf-8") as handle:
        for line in handle:
            payload = json.loads(line)
            if not isinstance(payload, dict):
                continue
            scanned_point_count += 1
            point_id = payload.get("id")
            if not isinstance(point_id, str) or point_id not in allowed_ids:
                continue
            payload_item = payload.get("payload")
            payload_source_name = None
            if isinstance(payload_item, dict):
                raw_source_name = payload_item.get("source_name")
                if isinstance(raw_source_name, str):
                    payload_source_name = raw_source_name.strip()
            if payload_source_name and payload_source_name in excluded_source_names:
                continue
            vector = [0.0] * dimensions
            sparse_values = payload.get("vector")
            if isinstance(sparse_values, list):
                for item in sparse_values:
                    if (
                        isinstance(item, list)
                        and len(item) == 2
                        and isinstance(item[0], (int, float))
                        and isinstance(item[1], (int, float))
                    ):
                        index = int(item[0])
                        if 0 <= index < dimensions:
                            vector[index] = float(item[1])
            filtered_points.append(
                {
                    "id": point_id,
                    "payload": payload_item,
                    "vector": vector,
                }
            )
    return filtered_points, scanned_point_count


def _write_filtered_artifact(
    artifact_path: Path,
    *,
    points: list[dict[str, Any]],
) -> None:
    with gzip.open(artifact_path, "wt", encoding="utf-8") as handle:
        for point in points:
            dense_vector = point.get("vector") or []
            sparse_vector = [
                [index, value]
                for index, value in enumerate(dense_vector)
                if isinstance(value, (int, float)) and float(value) != 0.0
            ]
            record = {
                "id": point.get("id"),
                "payload": point.get("payload"),
                "vector": sparse_vector,
            }
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")


def main() -> int:
    args = _build_parser().parse_args()
    source_artifact = args.source_artifact.expanduser().resolve()
    if not source_artifact.exists():
        raise SystemExit(f"Source artifact does not exist: {source_artifact}")
    bundle_dirs = [path.expanduser().resolve() for path in args.bundle_dir]
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    bundle_qids: set[str] = set()
    for bundle_dir in bundle_dirs:
        bundle_qids.update(_load_bundle_qids(bundle_dir))
    extra_qids = _load_extra_qids(
        extra_entity_ids=args.extra_entity_id,
        extra_qid_files=args.extra_qid_file,
    )
    allowed_qids = set(bundle_qids)
    allowed_qids.update(extra_qids)
    if not allowed_qids:
        raise SystemExit("No Wikidata-backed ids were found in the provided bundle dirs.")
    excluded_source_names = {
        value.strip() for value in args.exclude_source_name if isinstance(value, str) and value.strip()
    }

    dimensions = 384
    filtered_points, scanned_point_count = _iter_dense_points_from_sparse_artifact(
        source_artifact,
        allowed_ids=allowed_qids,
        dimensions=dimensions,
        excluded_source_names=excluded_source_names,
    )

    artifact_path = output_dir / "qid_graph_points.jsonl.gz"
    _write_filtered_artifact(
        artifact_path,
        points=filtered_points,
    )

    if args.qdrant_url:
        with QdrantVectorSearchClient(
            args.qdrant_url,
            api_key=args.qdrant_api_key,
        ) as client:
            client.ensure_collection(args.collection_name, dimensions=dimensions)
            client.upsert_points(args.collection_name, filtered_points)
            if args.publish_alias:
                client.set_alias(args.publish_alias, args.collection_name)

    manifest = {
        "builder_version": "filtered-artifact-v1",
        "build_strategy": "filtered_artifact",
        "built_at": datetime.now(timezone.utc).isoformat(),
        "source_artifact_path": str(source_artifact),
        "source_scanned_point_count": scanned_point_count,
        "bundle_dirs": [str(path) for path in bundle_dirs],
        "bundle_count": len(bundle_dirs),
        "bundle_qid_count": len(bundle_qids),
        "extra_qid_count": len(extra_qids),
        "allowed_qid_count": len(allowed_qids),
        "extra_qid_files": [str(path.expanduser().resolve()) for path in args.extra_qid_file],
        "excluded_source_names": sorted(excluded_source_names),
        "output_dir": str(output_dir),
        "artifact_path": str(artifact_path),
        "manifest_path": str(output_dir / "qid_graph_index_manifest.json"),
        "collection_name": args.collection_name,
        "alias_name": args.publish_alias,
        "qdrant_url": args.qdrant_url,
        "published": bool(args.qdrant_url),
        "dimensions": dimensions,
        "point_count": len(filtered_points),
        "target_entity_count": len(filtered_points),
    }
    manifest_path = output_dir / "qid_graph_index_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
