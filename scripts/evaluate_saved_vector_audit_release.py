#!/usr/bin/env python3
"""Evaluate one saved unseen-RSS vector-audit summary against promotion thresholds."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from ades.vector.evaluation import (
    SavedVectorAuditReleaseThresholds,
    default_saved_vector_audit_release_thresholds,
    evaluate_saved_vector_seed_audit_release,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate one saved unseen-RSS vector-audit summary against the "
            "current local promotion gate."
        )
    )
    parser.add_argument(
        "--summary",
        required=True,
        help="Path to one saved vector-audit summary.json file.",
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Optional output directory. Defaults under "
            "docs/planning/live-rss-vector-reviews/."
        ),
    )
    parser.add_argument("--min-case-count", type=int)
    parser.add_argument("--min-neighbor-seed-hit-rate", type=float)
    parser.add_argument("--min-related-entity-hit-rate", type=float)
    parser.add_argument("--min-cases-with-related-entities", type=int)
    parser.add_argument("--max-warning-case-rate", type=float)
    parser.add_argument("--max-p95-hinted-timing-ms", type=int)
    parser.add_argument("--max-location-seed-query-share", type=float)
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"Expected one JSON object in {path}.")
    return payload


def _safe_slug(value: str) -> str:
    cleaned = "".join(
        character if character.isalnum() or character in {"-", "_"} else "-"
        for character in value.strip().casefold()
    )
    normalized = "-".join(part for part in cleaned.split("-") if part)
    return normalized or "artifact"


def _resolve_output_dir(summary_path: Path, requested: str | None) -> Path:
    if requested:
        return Path(requested).expanduser().resolve()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = _safe_slug(summary_path.parent.name)
    return (
        Path("docs/planning/live-rss-vector-reviews")
        / f"saved-vector-audit-release-{slug}-{timestamp}"
    ).resolve()


def _summary_bucket_share(rows: object, *, value: str) -> float | None:
    if not isinstance(rows, list):
        return None
    total = 0
    matched = 0
    normalized_value = value.casefold()
    for item in rows:
        if not isinstance(item, dict):
            continue
        count = item.get("count")
        item_value = item.get("value")
        try:
            resolved_count = int(count)
        except (TypeError, ValueError):
            continue
        if resolved_count <= 0:
            continue
        total += resolved_count
        if str(item_value or "").strip().casefold() == normalized_value:
            matched += resolved_count
    if total <= 0:
        return None
    return matched / total


def _resolve_thresholds(args: argparse.Namespace) -> SavedVectorAuditReleaseThresholds:
    defaults = default_saved_vector_audit_release_thresholds()
    return SavedVectorAuditReleaseThresholds(
        min_case_count=defaults.min_case_count if args.min_case_count is None else args.min_case_count,
        min_neighbor_seed_hit_rate=(
            defaults.min_neighbor_seed_hit_rate
            if args.min_neighbor_seed_hit_rate is None
            else args.min_neighbor_seed_hit_rate
        ),
        min_related_entity_hit_rate=(
            defaults.min_related_entity_hit_rate
            if args.min_related_entity_hit_rate is None
            else args.min_related_entity_hit_rate
        ),
        min_cases_with_related_entities=(
            defaults.min_cases_with_related_entities
            if args.min_cases_with_related_entities is None
            else args.min_cases_with_related_entities
        ),
        max_warning_case_rate=(
            defaults.max_warning_case_rate
            if args.max_warning_case_rate is None
            else args.max_warning_case_rate
        ),
        max_p95_hinted_timing_ms=(
            defaults.max_p95_hinted_timing_ms
            if args.max_p95_hinted_timing_ms is None
            else args.max_p95_hinted_timing_ms
        ),
        max_location_seed_query_share=(
            defaults.max_location_seed_query_share
            if args.max_location_seed_query_share is None
            else args.max_location_seed_query_share
        ),
    )


def _markdown_summary(report: dict[str, Any]) -> str:
    baseline_metrics = report["baseline_metrics"]
    thresholds = report["thresholds"]
    lines = [
        "# Saved Vector Audit Release Gate",
        "",
        f"- summary: `{report['summary_path']}`",
        f"- generated_at: `{report['generated_at']}`",
        f"- passed: `{report['passed']}`",
        "",
        "## Baseline Metrics",
        "",
        f"- case_count: `{baseline_metrics.get('case_count')}`",
        f"- neighbor_seed_hit_rate: `{baseline_metrics.get('neighbor_seed_hit_rate')}`",
        f"- hinted_related_entity_hit_rate: `{baseline_metrics.get('hinted_related_entity_hit_rate')}`",
        f"- hinted_related_case_count: `{baseline_metrics.get('hinted_related_case_count')}`",
        f"- hinted_warning_case_rate: `{baseline_metrics.get('hinted_warning_case_rate')}`",
        f"- p95_hinted_timing_ms: `{baseline_metrics.get('p95_hinted_timing_ms')}`",
        f"- location_seed_query_share: `{report['location_seed_query_share']}`",
        "",
        "## Thresholds",
        "",
        f"- min_case_count: `{thresholds['min_case_count']}`",
        f"- min_neighbor_seed_hit_rate: `{thresholds['min_neighbor_seed_hit_rate']}`",
        f"- min_related_entity_hit_rate: `{thresholds['min_related_entity_hit_rate']}`",
        f"- min_cases_with_related_entities: `{thresholds['min_cases_with_related_entities']}`",
        f"- max_warning_case_rate: `{thresholds['max_warning_case_rate']}`",
        f"- max_p95_hinted_timing_ms: `{thresholds['max_p95_hinted_timing_ms']}`",
        f"- max_location_seed_query_share: `{thresholds['max_location_seed_query_share']}`",
        "",
        "## Decision",
        "",
    ]
    if report["reasons"]:
        for reason in report["reasons"]:
            lines.append(f"- `{reason}`")
    else:
        lines.append("- no failing reasons")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = _parse_args()
    summary_path = Path(args.summary).expanduser().resolve()
    summary = _load_json(summary_path)
    thresholds = _resolve_thresholds(args)
    decision = evaluate_saved_vector_seed_audit_release(summary, thresholds=thresholds)
    output_dir = _resolve_output_dir(summary_path, args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    baseline_metrics = summary.get("baseline_metrics")
    if not isinstance(baseline_metrics, dict):
        raise SystemExit("Summary does not contain baseline_metrics.")
    seed_shape_analysis = summary.get("seed_shape_analysis")
    if not isinstance(seed_shape_analysis, dict):
        raise SystemExit("Summary does not contain seed_shape_analysis.")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary_path": str(summary_path),
        "passed": decision.passed,
        "reasons": list(decision.reasons),
        "thresholds": asdict(thresholds),
        "baseline_metrics": baseline_metrics,
        "location_seed_query_share": _summary_bucket_share(
            seed_shape_analysis.get("seed_query_label_counts"),
            value="location",
        ),
    }
    summary_output_path = output_dir / "summary.json"
    summary_output_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    index_path = output_dir / "index.md"
    index_path.write_text(_markdown_summary(report), encoding="utf-8")
    print(summary_output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

