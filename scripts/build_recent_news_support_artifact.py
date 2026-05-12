#!/usr/bin/env python3
"""Build a recent-news QID co-occurrence support artifact from saved RSS runs."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from ades.vector.news_context import write_recent_news_support_artifact

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


def _default_output_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = DEFAULT_OUTPUT_ROOT / f"recent-news-support-{timestamp}"
    return output_dir / "recent_news_support.json"


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

    records: list[dict[str, Any]] = []
    for path in summary_paths:
        records.extend(_iter_summary_records(path))
    for path in manifest_paths:
        records.extend(_iter_manifest_records(path))

    _artifact_payload, build_summary = write_recent_news_support_artifact(
        records,
        output_path,
        entity_source=args.entity_source,
        min_document_count=args.min_document_count,
        max_neighbors_per_qid=args.max_neighbors_per_qid,
        excluded_report_sources=excluded_report_sources,
        keep_probable_source_outlets=bool(args.keep_probable_source_outlets),
        source_summaries=[str(path) for path in summary_paths],
        source_manifests=[str(path) for path in manifest_paths],
    )
    print(json.dumps(build_summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
