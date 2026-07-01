#!/usr/bin/env python3
"""Emit a monthly stale relationship and source-refresh audit report."""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sys
from typing import Iterable

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_ROOT = _REPO_ROOT / "src"
if _SRC_ROOT.exists() and str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

_REFRESH_POLICY_DAYS = {
    "daily": 1,
    "weekly": 7,
    "monthly": 31,
    "quarterly": 92,
    "semiannual": 183,
    "semiannually": 183,
    "annual": 366,
    "annually": 366,
    "yearly": 366,
    "biennial": 731,
    "biennially": 731,
}
_DATE_COLUMNS = (
    "source_snapshot",
    "fetched_at",
    "source_fetched_at",
    "source_published_at",
    "source_year",
)
_DATE_PATTERNS = (
    re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(\d{4}-\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(\d{4})(?!\d)"),
)


def _read_string(row: dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


def _parse_iso_date(value: str) -> date | None:
    normalized = value.strip()
    if not normalized:
        return None
    for pattern in _DATE_PATTERNS:
        match = pattern.search(normalized)
        if not match:
            continue
        matched = match.group(1)
        if len(matched) == 4:
            matched = f"{matched}-01-01"
        elif len(matched) == 7:
            matched = f"{matched}-01"
        try:
            return date.fromisoformat(matched)
        except ValueError:
            continue
    return None


def _policy_key(value: str) -> str:
    return value.strip().casefold().replace("-", "_").replace(" ", "_")


def _refresh_days(refresh_policy: str) -> int | None:
    key = _policy_key(refresh_policy)
    if key.startswith("refresh_on_") or key.startswith("manual"):
        return None
    return _REFRESH_POLICY_DAYS.get(key)


def _source_date(row: dict[str, str]) -> tuple[str, date | None]:
    for key in _DATE_COLUMNS:
        value = _read_string(row, key)
        parsed = _parse_iso_date(value)
        if parsed is not None:
            return key, parsed
    return "", None


def _add_count(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _sorted_counts(counts: dict[str, int]) -> dict[str, int]:
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _source_refresh_sample(
    *,
    path: Path,
    line_number: int,
    row: dict[str, str],
    warning: str,
    source_date_column: str,
    source_date: date | None,
    as_of_date: date,
) -> dict[str, object]:
    return {
        "path": str(path),
        "line": line_number,
        "scope": "source_refresh",
        "warning": warning,
        "source_ref": _read_string(row, "source_ref") or None,
        "target_ref": _read_string(row, "target_ref") or None,
        "relation": _read_string(row, "relation") or None,
        "source_title": (
            _read_string(row, "source_title") or _read_string(row, "source_name") or None
        ),
        "source_url": _read_string(row, "source_url") or None,
        "refresh_policy": _read_string(row, "refresh_policy") or None,
        "source_date_column": source_date_column or None,
        "source_date": source_date.isoformat() if source_date else None,
        "as_of_date": as_of_date.isoformat(),
    }


def _iter_edge_rows(edge_tsv_paths: Iterable[str | Path]) -> Iterable[tuple[Path, int, dict[str, str]]]:
    for edge_tsv_path in edge_tsv_paths:
        path = Path(edge_tsv_path).expanduser().resolve()
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row_index, row in enumerate(reader, start=2):
                yield path, row_index, row


def audit_stale_relationships(
    *,
    edge_tsv_paths: Iterable[str | Path],
    node_tsv_paths: Iterable[str | Path] = (),
    as_of_date: date | None = None,
    sample_limit: int = 50,
) -> dict[str, object]:
    """Return a stable JSON-compatible stale relationship audit payload."""

    from ades.impact.source_lane_validation import validate_market_graph_source_lanes

    resolved_edge_paths = tuple(Path(path).expanduser().resolve() for path in edge_tsv_paths)
    resolved_node_paths = tuple(Path(path).expanduser().resolve() for path in node_tsv_paths)
    as_of = as_of_date or date.today()
    validation = validate_market_graph_source_lanes(
        edge_tsv_paths=resolved_edge_paths,
        node_tsv_paths=resolved_node_paths,
        sample_limit=sample_limit,
    )

    source_refresh_warning_counts: dict[str, int] = {}
    source_refresh_samples: list[dict[str, object]] = []
    for path, row_index, row in _iter_edge_rows(resolved_edge_paths):
        refresh_policy = _read_string(row, "refresh_policy")
        if not refresh_policy:
            continue
        allowed_days = _refresh_days(refresh_policy)
        if allowed_days is None:
            continue
        source_date_column, parsed_source_date = _source_date(row)
        if parsed_source_date is None:
            warning = f"missing_source_refresh_date:{_policy_key(refresh_policy)}"
        elif parsed_source_date < as_of - timedelta(days=allowed_days):
            warning = f"source_refresh_due:{_policy_key(refresh_policy)}"
        else:
            continue

        _add_count(source_refresh_warning_counts, warning)
        if len(source_refresh_samples) < sample_limit:
            source_refresh_samples.append(
                _source_refresh_sample(
                    path=path,
                    line_number=row_index,
                    row=row,
                    warning=warning,
                    source_date_column=source_date_column,
                    source_date=parsed_source_date,
                    as_of_date=as_of,
                )
            )

    stale_counts = validation.stale_relationship_warning_counts
    source_counts = _sorted_counts(source_refresh_warning_counts)
    action_items = _action_items(
        stale_relationship_warning_counts=stale_counts,
        source_refresh_warning_counts=source_counts,
    )
    status = "needs_refresh" if stale_counts or source_counts else "clean"
    return {
        "audit_metadata": {
            "audit_id": "monthly_stale_relationship_audit",
            "as_of_date": as_of.isoformat(),
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "edge_tsv_paths": [str(path) for path in resolved_edge_paths],
            "node_tsv_paths": [str(path) for path in resolved_node_paths],
            "sample_limit": sample_limit,
        },
        "status": status,
        "validation_summary": {
            "row_count": validation.row_count,
            "invalid_row_count": validation.invalid_row_count,
            "promotion_ready_row_count": validation.promotion_ready_row_count,
            "promotion_blocked_row_count": validation.promotion_blocked_row_count,
            "production_promotion_warning_counts": (
                validation.production_promotion_warning_counts
            ),
            "source_warning_counts": validation.source_warning_counts,
            "relation_warning_counts": validation.relation_warning_counts,
        },
        "stale_relationship_warning_counts": stale_counts,
        "stale_relationship_samples": list(validation.stale_relationship_samples),
        "source_refresh_warning_counts": source_counts,
        "source_refresh_samples": source_refresh_samples,
        "action_items": action_items,
    }


def _action_items(
    *,
    stale_relationship_warning_counts: dict[str, int],
    source_refresh_warning_counts: dict[str, int],
) -> list[dict[str, object]]:
    actions: list[dict[str, object]] = []
    for warning, count in sorted(stale_relationship_warning_counts.items()):
        if "ownership" in warning:
            category = "ownership_relationship"
        elif "security" in warning:
            category = "security_relationship"
        elif "issuer" in warning:
            category = "issuer_relationship"
        else:
            category = "relationship"
        actions.append(
            {
                "owner": "Ops/ADES",
                "category": category,
                "warning": warning,
                "count": count,
                "action": "refresh_or_reverify_source_backed_relationship_row",
            }
        )
    for warning, count in sorted(source_refresh_warning_counts.items()):
        actions.append(
            {
                "owner": "Ops/ADES",
                "category": "source_refresh",
                "warning": warning,
                "count": count,
                "action": "refresh_source_snapshot_or_update_refresh_policy",
            }
        )
    return actions


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit stale ownership/security/issuer relationships and source rows."
    )
    parser.add_argument(
        "--edge-tsv-path",
        action="append",
        required=True,
        help="Normalized market graph edge TSV path. Repeat for multiple lanes.",
    )
    parser.add_argument(
        "--node-tsv-path",
        action="append",
        default=[],
        help="Optional normalized market graph node TSV path. Repeat for multiple lanes.",
    )
    parser.add_argument(
        "--as-of-date",
        help="Audit date in YYYY-MM-DD format. Defaults to today's local date.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=50,
        help="Maximum number of relationship and source-refresh samples to include.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    as_of_date = _parse_iso_date(args.as_of_date) if args.as_of_date else None
    if args.as_of_date and as_of_date is None:
        raise SystemExit("--as-of-date must be an ISO date in YYYY-MM-DD format")
    payload = audit_stale_relationships(
        edge_tsv_paths=args.edge_tsv_path,
        node_tsv_paths=args.node_tsv_path,
        as_of_date=as_of_date,
        sample_limit=args.sample_limit,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
