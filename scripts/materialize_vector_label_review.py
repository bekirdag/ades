#!/usr/bin/env python3
"""Build a compact manual-review sample from one seed-neighbor label queue."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ReviewRow:
    raw: dict[str, Any]
    neighbor_repeat_count: int
    seed_repeat_count: int


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Select a compact, diverse subset of one vector-neighbor label queue "
            "for manual review."
        ),
    )
    parser.add_argument(
        "--label-queue",
        required=True,
        help="Path to one label-queue.jsonl file emitted by the audit script.",
    )
    parser.add_argument(
        "--output-dir",
        help="Optional output directory. Defaults beside the queue file.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=40,
        help="Maximum sampled rows to emit.",
    )
    parser.add_argument(
        "--max-per-case",
        type=int,
        default=4,
        help="Maximum rows to keep from one case_id.",
    )
    parser.add_argument(
        "--max-per-seed-label",
        type=int,
        default=16,
        help="Maximum rows to keep for one seed_label.",
    )
    parser.add_argument(
        "--max-per-neighbor",
        type=int,
        default=2,
        help="Maximum rows to keep for one neighbor_canonical_text.",
    )
    return parser.parse_args()


def _load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        payload = json.loads(stripped)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _sample_rows(
    rows: list[dict[str, Any]],
    *,
    max_rows: int,
    max_per_case: int,
    max_per_seed_label: int,
    max_per_neighbor: int,
) -> list[ReviewRow]:
    neighbor_counts = Counter(str(row.get("neighbor_canonical_text", "")).strip() for row in rows)
    seed_counts = Counter(str(row.get("seed_canonical_text", "")).strip() for row in rows)

    unique_rows: list[ReviewRow] = []
    seen_triplets: set[tuple[str, str, str]] = set()
    for row in rows:
        case_id = str(row.get("case_id", "")).strip()
        seed_text = str(row.get("seed_canonical_text", "")).strip()
        neighbor_text = str(row.get("neighbor_canonical_text", "")).strip()
        triplet = (case_id, seed_text, neighbor_text)
        if triplet in seen_triplets:
            continue
        seen_triplets.add(triplet)
        unique_rows.append(
            ReviewRow(
                raw=row,
                neighbor_repeat_count=neighbor_counts[neighbor_text],
                seed_repeat_count=seed_counts[seed_text],
            )
        )

    unique_rows.sort(
        key=lambda item: (
            -item.neighbor_repeat_count,
            -item.seed_repeat_count,
            int(item.raw.get("neighbor_rank", 9999)),
            str(item.raw.get("case_id", "")),
            str(item.raw.get("seed_canonical_text", "")),
            str(item.raw.get("neighbor_canonical_text", "")),
        )
    )

    sampled: list[ReviewRow] = []
    per_case_counts: Counter[str] = Counter()
    per_seed_label_counts: Counter[str] = Counter()
    per_neighbor_counts: Counter[str] = Counter()
    for row in unique_rows:
        case_id = str(row.raw.get("case_id", "")).strip()
        seed_label = str(row.raw.get("seed_label", "unknown")).strip() or "unknown"
        neighbor_text = str(row.raw.get("neighbor_canonical_text", "")).strip()
        if per_case_counts[case_id] >= max_per_case:
            continue
        if per_seed_label_counts[seed_label] >= max_per_seed_label:
            continue
        if per_neighbor_counts[neighbor_text] >= max_per_neighbor:
            continue
        sampled.append(row)
        per_case_counts[case_id] += 1
        per_seed_label_counts[seed_label] += 1
        per_neighbor_counts[neighbor_text] += 1
        if len(sampled) >= max_rows:
            break
    return sampled


def _serialize_review_row(row: ReviewRow) -> dict[str, Any]:
    payload = dict(row.raw)
    payload["neighbor_repeat_count"] = row.neighbor_repeat_count
    payload["seed_repeat_count"] = row.seed_repeat_count
    return payload


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows)
        + ("\n" if rows else ""),
        encoding="utf-8",
    )


def _write_summary(
    path: Path,
    *,
    label_queue: Path,
    rows: list[dict[str, Any]],
    max_rows: int,
    max_per_case: int,
    max_per_seed_label: int,
    max_per_neighbor: int,
) -> None:
    seed_label_counts = Counter(str(row.get("seed_label", "unknown")).strip() or "unknown" for row in rows)
    case_counts = Counter(str(row.get("case_id", "")).strip() for row in rows)
    neighbor_counts = Counter(
        str(row.get("neighbor_canonical_text", "")).strip()
        for row in rows
        if str(row.get("neighbor_canonical_text", "")).strip()
    )
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "label_queue": str(label_queue),
        "selected_row_count": len(rows),
        "max_rows": max_rows,
        "max_per_case": max_per_case,
        "max_per_seed_label": max_per_seed_label,
        "max_per_neighbor": max_per_neighbor,
        "seed_label_counts": [
            {"value": value, "count": count}
            for value, count in seed_label_counts.most_common()
        ],
        "case_counts": [
            {"value": value, "count": count}
            for value, count in case_counts.most_common()
        ],
        "neighbor_counts": [
            {"value": value, "count": count}
            for value, count in neighbor_counts.most_common()
        ],
    }
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> None:
    args = _parse_args()
    label_queue = Path(args.label_queue).expanduser().resolve()
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else label_queue.parent
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = _load_rows(label_queue)
    sampled = _sample_rows(
        rows,
        max_rows=args.max_rows,
        max_per_case=args.max_per_case,
        max_per_seed_label=args.max_per_seed_label,
        max_per_neighbor=args.max_per_neighbor,
    )
    serialized = [_serialize_review_row(row) for row in sampled]
    _write_jsonl(output_dir / "label-review-sample.jsonl", serialized)
    _write_summary(
        output_dir / "label-review-sample-summary.json",
        label_queue=label_queue,
        rows=serialized,
        max_rows=args.max_rows,
        max_per_case=args.max_per_case,
        max_per_seed_label=args.max_per_seed_label,
        max_per_neighbor=args.max_per_neighbor,
    )


if __name__ == "__main__":
    main()
