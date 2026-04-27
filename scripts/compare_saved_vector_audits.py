#!/usr/bin/env python3
"""Compare two saved vector-audit summaries and emit one delta artifact."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


_METRIC_KEYS = (
    "hinted_related_entity_hit_rate",
    "hinted_warning_case_rate",
    "neighbor_seed_hit_rate",
    "avg_hinted_timing_ms",
    "p95_hinted_timing_ms",
    "seed_count",
    "label_queue_row_count",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare two saved vector-audit summaries and emit one delta artifact."
    )
    parser.add_argument("--baseline-summary", required=True, help="Path to baseline summary.json")
    parser.add_argument("--candidate-summary", required=True, help="Path to candidate summary.json")
    parser.add_argument(
        "--output-dir",
        help=(
            "Optional output directory. Defaults under "
            "docs/planning/live-rss-vector-reviews/vector-audit-compare-<timestamp>/."
        ),
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"Expected a JSON object in {path}.")
    return payload


def _default_output_dir() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (
        Path("docs/planning/live-rss-vector-reviews")
        / f"vector-audit-compare-{timestamp}"
    ).resolve()


def _metric_delta(
    baseline_metrics: dict[str, Any],
    candidate_metrics: dict[str, Any],
) -> dict[str, dict[str, float | int | None]]:
    deltas: dict[str, dict[str, float | int | None]] = {}
    for key in _METRIC_KEYS:
        baseline_value = baseline_metrics.get(key)
        candidate_value = candidate_metrics.get(key)
        delta_value: float | int | None = None
        if isinstance(baseline_value, (int, float)) and isinstance(candidate_value, (int, float)):
            delta_value = round(float(candidate_value) - float(baseline_value), 6)
        deltas[key] = {
            "baseline": baseline_value,
            "candidate": candidate_value,
            "delta": delta_value,
        }
    return deltas


def _alias_stats_map(summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    alias_stats = summary.get("alias_stats")
    if not isinstance(alias_stats, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in alias_stats:
        if not isinstance(item, dict):
            continue
        alias_name = item.get("collection_alias")
        if isinstance(alias_name, str) and alias_name.strip():
            result[alias_name.strip()] = item
    return result


def _case_map(summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    cases = summary.get("cases")
    if not isinstance(cases, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in cases:
        if not isinstance(item, dict):
            continue
        case_id = item.get("case_id")
        if isinstance(case_id, str) and case_id.strip():
            result[case_id.strip()] = item
    return result


def _compare_alias_stats(
    baseline_summary: dict[str, Any],
    candidate_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    baseline_map = _alias_stats_map(baseline_summary)
    candidate_map = _alias_stats_map(candidate_summary)
    all_aliases = sorted(set(baseline_map) | set(candidate_map))
    rows: list[dict[str, Any]] = []
    for alias_name in all_aliases:
        baseline_entry = baseline_map.get(alias_name, {})
        candidate_entry = candidate_map.get(alias_name, {})
        row = {"collection_alias": alias_name}
        for key in (
            "seed_query_count",
            "seed_missing_count",
            "nonempty_neighbor_seed_count",
            "neighbor_count",
        ):
            baseline_value = baseline_entry.get(key)
            candidate_value = candidate_entry.get(key)
            delta_value: float | int | None = None
            if isinstance(baseline_value, (int, float)) and isinstance(candidate_value, (int, float)):
                delta_value = round(float(candidate_value) - float(baseline_value), 6)
            row[key] = {
                "baseline": baseline_value,
                "candidate": candidate_value,
                "delta": delta_value,
            }
        rows.append(row)
    return rows


def _compare_cases(
    baseline_summary: dict[str, Any],
    candidate_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    baseline_map = _case_map(baseline_summary)
    candidate_map = _case_map(candidate_summary)
    case_ids = sorted(set(baseline_map) | set(candidate_map))
    rows: list[dict[str, Any]] = []
    for case_id in case_ids:
        baseline_case = baseline_map.get(case_id, {})
        candidate_case = candidate_map.get(case_id, {})
        row: dict[str, Any] = {
            "case_id": case_id,
            "slug": candidate_case.get("slug") or baseline_case.get("slug"),
            "title": candidate_case.get("title") or baseline_case.get("title"),
        }
        for key in (
            "hinted_related_count",
            "hinted_warning_count",
            "case_warning_count",
            "alias_count",
            "hinted_timing_ms",
        ):
            baseline_value = baseline_case.get(key)
            candidate_value = candidate_case.get(key)
            delta_value: float | int | None = None
            if isinstance(baseline_value, (int, float)) and isinstance(candidate_value, (int, float)):
                delta_value = round(float(candidate_value) - float(baseline_value), 6)
            row[key] = {
                "baseline": baseline_value,
                "candidate": candidate_value,
                "delta": delta_value,
            }
        rows.append(row)
    return rows


def _overall_verdict(metric_deltas: dict[str, dict[str, float | int | None]]) -> dict[str, bool]:
    related_hit_delta = metric_deltas["hinted_related_entity_hit_rate"]["delta"]
    warning_delta = metric_deltas["hinted_warning_case_rate"]["delta"]
    p95_delta = metric_deltas["p95_hinted_timing_ms"]["delta"]
    return {
        "related_entity_hit_rate_non_decreasing": not isinstance(related_hit_delta, (int, float))
        or related_hit_delta >= 0,
        "warning_rate_non_increasing": not isinstance(warning_delta, (int, float))
        or warning_delta <= 0,
        "p95_latency_non_increasing": not isinstance(p95_delta, (int, float)) or p95_delta <= 0,
    }


def _write_index(summary: dict[str, Any], output_dir: Path) -> Path:
    metric_deltas = summary["metric_deltas"]
    lines = [
        "# Vector Audit Comparison",
        "",
        f"- baseline_summary: `{summary['baseline_summary']}`",
        f"- candidate_summary: `{summary['candidate_summary']}`",
        f"- baseline_aliases_seen: `{', '.join(summary['baseline_collection_aliases_seen'])}`",
        f"- candidate_aliases_seen: `{', '.join(summary['candidate_collection_aliases_seen'])}`",
        f"- candidate_alias_overrides: `{summary['candidate_collection_alias_overrides']}`",
        "",
        "## Metric Deltas",
        "",
    ]
    for key in _METRIC_KEYS:
        item = metric_deltas[key]
        lines.append(
            f"- `{key}` | baseline=`{item['baseline']}` | candidate=`{item['candidate']}` | delta=`{item['delta']}`"
        )
    lines.extend(["", "## Verdict", ""])
    for key, value in summary["overall_verdict"].items():
        lines.append(f"- `{key}` = `{value}`")
    lines.extend(["", "## Alias Stats", ""])
    for item in summary["alias_stat_deltas"]:
        lines.append(
            "- "
            f"`{item['collection_alias']}` | "
            f"seed_queries={item['seed_query_count']['baseline']}->{item['seed_query_count']['candidate']} | "
            f"seed_missing={item['seed_missing_count']['baseline']}->{item['seed_missing_count']['candidate']} | "
            f"nonempty_neighbor_seeds={item['nonempty_neighbor_seed_count']['baseline']}->{item['nonempty_neighbor_seed_count']['candidate']} | "
            f"neighbor_count={item['neighbor_count']['baseline']}->{item['neighbor_count']['candidate']}"
        )
    lines.extend(["", "## Case Deltas", ""])
    for item in summary["case_deltas"]:
        lines.append(
            "- "
            f"`{item['case_id']}` | "
            f"related={item['hinted_related_count']['baseline']}->{item['hinted_related_count']['candidate']} | "
            f"hinted_warnings={item['hinted_warning_count']['baseline']}->{item['hinted_warning_count']['candidate']} | "
            f"case_warnings={item['case_warning_count']['baseline']}->{item['case_warning_count']['candidate']} | "
            f"hinted_timing_ms={item['hinted_timing_ms']['baseline']}->{item['hinted_timing_ms']['candidate']}"
        )
    index_path = output_dir / "index.md"
    index_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return index_path


def main() -> int:
    args = _parse_args()
    baseline_summary_path = Path(args.baseline_summary).expanduser().resolve()
    candidate_summary_path = Path(args.candidate_summary).expanduser().resolve()
    output_dir = (
        Path(args.output_dir).expanduser().resolve() if args.output_dir else _default_output_dir()
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    baseline_summary = _load_json(baseline_summary_path)
    candidate_summary = _load_json(candidate_summary_path)
    baseline_metrics = baseline_summary.get("baseline_metrics")
    candidate_metrics = candidate_summary.get("baseline_metrics")
    if not isinstance(baseline_metrics, dict) or not isinstance(candidate_metrics, dict):
        raise SystemExit("Both summaries must contain one baseline_metrics object.")

    comparison_summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_summary": str(baseline_summary_path),
        "candidate_summary": str(candidate_summary_path),
        "baseline_collection_aliases_seen": list(
            baseline_summary.get("collection_aliases_seen", [])
        ),
        "candidate_collection_aliases_seen": list(
            candidate_summary.get("collection_aliases_seen", [])
        ),
        "candidate_collection_alias_overrides": dict(
            candidate_summary.get("collection_alias_overrides", {})
        ),
        "metric_deltas": _metric_delta(baseline_metrics, candidate_metrics),
        "alias_stat_deltas": _compare_alias_stats(baseline_summary, candidate_summary),
        "case_deltas": _compare_cases(baseline_summary, candidate_summary),
    }
    comparison_summary["overall_verdict"] = _overall_verdict(comparison_summary["metric_deltas"])

    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(comparison_summary, indent=2) + "\n", encoding="utf-8")
    index_path = _write_index(comparison_summary, output_dir)

    print(
        json.dumps(
            {
                "summary": str(summary_path),
                "index": str(index_path),
                "verdict": comparison_summary["overall_verdict"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
