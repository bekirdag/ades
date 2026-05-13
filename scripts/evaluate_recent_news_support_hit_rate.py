#!/usr/bin/env python3
"""Evaluate recent-news support artifact hit-rate against accepted news records."""

from __future__ import annotations

import argparse
from itertools import combinations
import json
from pathlib import Path
from typing import Any

from ades.vector.news_context import (
    load_recent_news_support_artifact,
    normalize_recent_news_entity_items,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure recent-news support pair coverage against accepted BDYA "
            "production news records."
        )
    )
    parser.add_argument("--artifact", required=True, help="recent_news_support.json path.")
    parser.add_argument(
        "--record-jsonl",
        action="append",
        default=[],
        help="Accepted BDYA recent-news support input JSONL. Repeatable.",
    )
    parser.add_argument(
        "--entity-source",
        default="plain",
        choices=("plain", "flagged"),
        help="Entity source lane used by the support artifact builder.",
    )
    parser.add_argument(
        "--min-pair-count",
        type=int,
        default=1,
        help="Minimum artifact pair count treated as supported.",
    )
    parser.add_argument(
        "--min-pair-hit-rate",
        type=float,
        default=0.1,
        help="Minimum supported-pair / eligible-pair ratio for --gate.",
    )
    parser.add_argument(
        "--min-record-hit-rate",
        type=float,
        default=0.1,
        help="Minimum records-with-support / eligible-records ratio for --gate.",
    )
    parser.add_argument("--gate", action="store_true", help="Exit nonzero when below thresholds.")
    return parser.parse_args()


def _iter_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"Expected JSON object in {path}:{line_no}.")
            records.append(payload)
    return records


def _qid_pairs(record: dict[str, Any], *, entity_source: str) -> set[tuple[str, str]]:
    qids = sorted(
        {
            str(entity["entity_id"])
            for entity in normalize_recent_news_entity_items(
                record,
                entity_source=entity_source,
            )
            if str(entity.get("entity_id") or "").startswith("wikidata:Q")
        }
    )
    return {tuple(pair) for pair in combinations(qids, 2)}


def main() -> int:
    args = _parse_args()
    if args.min_pair_count <= 0:
        raise ValueError("--min-pair-count must be positive.")
    artifact = load_recent_news_support_artifact(Path(args.artifact).expanduser())
    records: list[dict[str, Any]] = []
    for raw_path in args.record_jsonl:
        records.extend(_iter_jsonl(Path(raw_path).expanduser()))

    eligible_record_count = 0
    record_hit_count = 0
    eligible_pair_count = 0
    supported_pair_count = 0
    for record in records:
        pairs = _qid_pairs(record, entity_source=args.entity_source)
        if not pairs:
            continue
        eligible_record_count += 1
        eligible_pair_count += len(pairs)
        record_supported = False
        for left_qid, right_qid in pairs:
            left_support = artifact._cooccurrence.get(left_qid, {}).get(right_qid, 0)
            if left_support >= args.min_pair_count:
                supported_pair_count += 1
                record_supported = True
        if record_supported:
            record_hit_count += 1

    pair_hit_rate = (
        supported_pair_count / eligible_pair_count if eligible_pair_count else 0.0
    )
    record_hit_rate = (
        record_hit_count / eligible_record_count if eligible_record_count else 0.0
    )
    gate_failures = []
    if pair_hit_rate < args.min_pair_hit_rate:
        gate_failures.append(
            f"pair_hit_rate_below_threshold:{pair_hit_rate:.4f}<{args.min_pair_hit_rate:.4f}"
        )
    if record_hit_rate < args.min_record_hit_rate:
        gate_failures.append(
            f"record_hit_rate_below_threshold:{record_hit_rate:.4f}<{args.min_record_hit_rate:.4f}"
        )
    summary = {
        "artifact_path": str(Path(args.artifact).expanduser().resolve()),
        "record_jsonl_paths": [
            str(Path(path).expanduser().resolve()) for path in args.record_jsonl
        ],
        "eligible_record_count": eligible_record_count,
        "record_hit_count": record_hit_count,
        "record_hit_rate": record_hit_rate,
        "eligible_pair_count": eligible_pair_count,
        "supported_pair_count": supported_pair_count,
        "pair_hit_rate": pair_hit_rate,
        "min_pair_count": args.min_pair_count,
        "gate_failures": gate_failures,
        "gate_passed": not gate_failures,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 1 if args.gate and gate_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
