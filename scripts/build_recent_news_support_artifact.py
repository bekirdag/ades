#!/usr/bin/env python3
"""Build a recent-news QID co-occurrence support artifact from saved RSS runs."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from itertools import combinations
from pathlib import Path
from typing import Any

from ades.vector.news_context import is_probable_source_outlet_candidate

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SUMMARY_PATH = (
    REPO_ROOT
    / "docs"
    / "planning"
    / "live-rss-vector-reviews"
    / "run-20260421-1000"
    / "summary.json"
)
DEFAULT_OUTPUT_ROOT = Path("/mnt/githubActions/ades_big_data/vector_builds")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a recent-news support artifact from saved live-RSS review bundles."
        )
    )
    parser.add_argument(
        "--summary",
        action="append",
        default=[],
        help="Path to one saved summary.json with report items.",
    )
    parser.add_argument(
        "--manifest",
        action="append",
        default=[],
        help="Path to one saved manifest.jsonl with report items.",
    )
    parser.add_argument(
        "--entity-source",
        default="plain",
        choices=("plain", "flagged"),
        help="Use plain or flagged saved entities from the review bundle.",
    )
    parser.add_argument(
        "--min-document-count",
        type=int,
        default=1,
        help="Keep only QIDs seen in at least this many saved articles.",
    )
    parser.add_argument(
        "--max-neighbors-per-qid",
        type=int,
        default=256,
        help="Maximum stored neighbors per seed QID.",
    )
    parser.add_argument(
        "--output",
        help="Optional output artifact path. Defaults under /mnt/.../vector_builds/.",
    )
    parser.add_argument(
        "--exclude-report-source",
        action="append",
        default=[],
        help=(
            "Exclude saved report records whose top-level source matches one of these "
            "values exactly."
        ),
    )
    parser.add_argument(
        "--keep-probable-source-outlets",
        action="store_true",
        help=(
            "Keep organization entities that match the recent-news source-outlet "
            "noise heuristic. Default behavior filters them out."
        ),
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected one JSON object in {path}.")
    return payload


def _iter_summary_records(path: Path) -> list[dict[str, Any]]:
    payload = _load_json(path)
    for key in ("reports", "cases", "articles"):
        records = payload.get(key)
        if isinstance(records, list):
            return [item for item in records if isinstance(item, dict)]
    raise ValueError(f"Expected one reports, cases, or articles list in {path}.")


def _iter_manifest_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                records.append(payload)
    return records


def _normalize_entity_items(
    record: dict[str, Any],
    *,
    entity_source: str,
) -> list[dict[str, str]]:
    entity_key = "flagged_entities" if entity_source == "flagged" else "plain_entities"
    raw_entities = record.get(entity_key)
    if not isinstance(raw_entities, list):
        nested_key = "hinted" if entity_source == "flagged" else "plain"
        nested_payload = record.get(nested_key)
        if isinstance(nested_payload, dict):
            raw_entities = nested_payload.get("entities")
    if not isinstance(raw_entities, list):
        raw_entities = record.get("linked_entities")
    if not isinstance(raw_entities, list):
        raw_entities = record.get("entities")
    if not isinstance(raw_entities, list):
        return []
    normalized: list[dict[str, str]] = []
    for raw_item in raw_entities:
        if not isinstance(raw_item, dict):
            continue
        entity_id = str(raw_item.get("entity_id") or "").strip()
        if not entity_id.startswith("wikidata:Q"):
            continue
        normalized.append(
            {
                "entity_id": entity_id,
                "canonical_text": str(raw_item.get("canonical_text") or entity_id).strip()
                or entity_id,
                "entity_type": str(
                    raw_item.get("label") or raw_item.get("entity_type") or ""
                ).strip()
                or None,
                "source_name": str(raw_item.get("source_name") or "").strip() or None,
            }
        )
    return normalized


def _default_output_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = DEFAULT_OUTPUT_ROOT / f"recent-news-support-{timestamp}"
    return output_dir / "recent_news_support.json"


def _record_identity(record: dict[str, Any]) -> str | None:
    for key in ("article_url", "url", "news_file_path", "news_path", "text_path", "path"):
        value = record.get(key)
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
    title = record.get("title")
    if isinstance(title, str):
        normalized = title.strip()
        if normalized:
            return normalized
    return None


def _should_filter_probable_source_outlet(entity: dict[str, str]) -> bool:
    canonical_text = str(entity.get("canonical_text") or "").strip()
    entity_type = str(entity.get("entity_type") or "").strip() or None
    if not canonical_text:
        return False
    return is_probable_source_outlet_candidate(
        canonical_text,
        candidate_type=entity_type,
    )


def main() -> int:
    args = _parse_args()
    summary_paths = [Path(value).expanduser().resolve() for value in args.summary]
    manifest_paths = [Path(value).expanduser().resolve() for value in args.manifest]
    excluded_report_sources = {
        value.strip().casefold()
        for value in args.exclude_report_source
        if value and value.strip()
    }
    if not summary_paths and not manifest_paths:
        summary_paths = [DEFAULT_SUMMARY_PATH.resolve()]

    for path in [*summary_paths, *manifest_paths]:
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")

    if args.min_document_count <= 0:
        raise ValueError("--min-document-count must be positive.")
    if args.max_neighbors_per_qid <= 0:
        raise ValueError("--max-neighbors-per-qid must be positive.")

    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else _default_output_path()
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    metadata_by_qid: dict[str, dict[str, str | None]] = {}
    document_count_by_qid: Counter[str] = Counter()
    cooccurrence_counts: dict[str, Counter[str]] = defaultdict(Counter)
    article_count = 0
    success_record_count = 0
    duplicate_record_count = 0
    filtered_probable_source_outlet_count = 0
    seen_record_ids: set[str] = set()

    def _consume_record(record: dict[str, Any]) -> None:
        nonlocal article_count
        nonlocal success_record_count
        nonlocal duplicate_record_count
        nonlocal filtered_probable_source_outlet_count
        status = str(record.get("status") or "").strip().casefold()
        if status and status != "success":
            return
        report_source = str(record.get("source") or "").strip().casefold()
        if report_source and report_source in excluded_report_sources:
            return
        record_id = _record_identity(record)
        if record_id is not None:
            if record_id in seen_record_ids:
                duplicate_record_count += 1
                return
            seen_record_ids.add(record_id)
        success_record_count += 1
        entities = _normalize_entity_items(record, entity_source=args.entity_source)
        if not args.keep_probable_source_outlets:
            filtered_entities: list[dict[str, str]] = []
            for entity in entities:
                if _should_filter_probable_source_outlet(entity):
                    filtered_probable_source_outlet_count += 1
                    continue
                filtered_entities.append(entity)
            entities = filtered_entities
        per_article_qids = list(
            dict.fromkeys(entity["entity_id"] for entity in entities if entity["entity_id"])
        )
        if not per_article_qids:
            return
        article_count += 1
        per_article_entity_by_qid = {
            entity["entity_id"]: entity for entity in entities if entity["entity_id"]
        }
        for entity_id in per_article_qids:
            document_count_by_qid[entity_id] += 1
            metadata = metadata_by_qid.setdefault(
                entity_id,
                {
                    "canonical_text": entity_id,
                    "entity_type": None,
                    "source_name": None,
                },
            )
            entity = per_article_entity_by_qid[entity_id]
            canonical_text = str(entity.get("canonical_text") or "").strip()
            entity_type = str(entity.get("entity_type") or "").strip() or None
            source_name = str(entity.get("source_name") or "").strip() or None
            if canonical_text and metadata["canonical_text"] == entity_id:
                metadata["canonical_text"] = canonical_text
            if entity_type and not metadata["entity_type"]:
                metadata["entity_type"] = entity_type
            if source_name and not metadata["source_name"]:
                metadata["source_name"] = source_name
        for left_qid, right_qid in combinations(sorted(per_article_qids), 2):
            cooccurrence_counts[left_qid][right_qid] += 1
            cooccurrence_counts[right_qid][left_qid] += 1

    for path in summary_paths:
        for record in _iter_summary_records(path):
            _consume_record(record)
    for path in manifest_paths:
        for record in _iter_manifest_records(path):
            _consume_record(record)

    kept_qids = {
        entity_id
        for entity_id, count in document_count_by_qid.items()
        if count >= args.min_document_count
    }
    qid_metadata = {
        entity_id: {
            "canonical_text": str(metadata_by_qid.get(entity_id, {}).get("canonical_text") or entity_id),
            "entity_type": metadata_by_qid.get(entity_id, {}).get("entity_type"),
            "source_name": metadata_by_qid.get(entity_id, {}).get("source_name"),
            "document_count": int(document_count_by_qid[entity_id]),
        }
        for entity_id in sorted(kept_qids)
    }
    cooccurrence = {}
    for entity_id in sorted(kept_qids):
        neighbors = [
            (candidate_id, count)
            for candidate_id, count in cooccurrence_counts.get(entity_id, {}).items()
            if candidate_id in kept_qids and count > 0
        ]
        neighbors.sort(key=lambda item: (-item[1], item[0]))
        limited_neighbors = neighbors[: args.max_neighbors_per_qid]
        if limited_neighbors:
            cooccurrence[entity_id] = {
                candidate_id: count for candidate_id, count in limited_neighbors
            }

    artifact_payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "article_count": article_count,
        "entity_source": args.entity_source,
        "min_document_count": args.min_document_count,
        "max_neighbors_per_qid": args.max_neighbors_per_qid,
        "excluded_report_sources": sorted(excluded_report_sources),
        "keep_probable_source_outlets": bool(args.keep_probable_source_outlets),
        "source_summaries": [str(path) for path in summary_paths],
        "source_manifests": [str(path) for path in manifest_paths],
        "qid_metadata": qid_metadata,
        "cooccurrence": cooccurrence,
    }
    output_path.write_text(
        json.dumps(artifact_payload, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    top_entities = [
        {
            "entity_id": entity_id,
            "canonical_text": qid_metadata[entity_id]["canonical_text"],
            "document_count": int(document_count_by_qid[entity_id]),
        }
        for entity_id, _count in document_count_by_qid.most_common(20)
        if entity_id in kept_qids
    ]
    top_pairs: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for entity_id, neighbors in cooccurrence_counts.items():
        if entity_id not in kept_qids:
            continue
        for candidate_id, count in neighbors.items():
            if candidate_id not in kept_qids:
                continue
            pair = tuple(sorted((entity_id, candidate_id)))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            top_pairs.append(
                {
                    "left_entity_id": pair[0],
                    "right_entity_id": pair[1],
                    "count": int(count),
                }
            )
    top_pairs.sort(key=lambda item: (-item["count"], item["left_entity_id"], item["right_entity_id"]))
    build_summary = {
        "output_path": str(output_path),
        "entity_source": args.entity_source,
        "source_summary_count": len(summary_paths),
        "source_manifest_count": len(manifest_paths),
        "excluded_report_sources": sorted(excluded_report_sources),
        "keep_probable_source_outlets": bool(args.keep_probable_source_outlets),
        "success_record_count": success_record_count,
        "duplicate_record_count": duplicate_record_count,
        "filtered_probable_source_outlet_count": filtered_probable_source_outlet_count,
        "article_count": article_count,
        "kept_qid_count": len(kept_qids),
        "cooccurrence_seed_count": len(cooccurrence),
        "top_entities": top_entities,
        "top_pairs": top_pairs[:20],
    }
    summary_path = output_path.with_name("build_summary.json")
    summary_path.write_text(
        json.dumps(build_summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(build_summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
