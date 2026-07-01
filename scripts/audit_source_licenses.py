#!/usr/bin/env python3
"""Emit a quarterly source-license audit report for ADES source lanes."""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys
from typing import Iterable

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_ROOT = _REPO_ROOT / "src"
if _SRC_ROOT.exists() and str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

_SOURCE_USE_UNCONFIRMED = "source_use_not_confirmed"
_MISSING_LICENSE_METADATA = "missing_license_metadata"
_MISSING_LICENSE_NOTES = "missing_license_notes"
_LICENSE_STATUS_ALLOWED = "allowed"
_LICENSE_STATUS_UNCLEAR = "unclear"
_LICENSE_STATUS_RESTRICTED = "restricted"
_LICENSE_STATUS_VALUES = {
    _LICENSE_STATUS_ALLOWED,
    _LICENSE_STATUS_UNCLEAR,
    _LICENSE_STATUS_RESTRICTED,
}


def _read_string(row: dict[str, object], key: str) -> str:
    return str(row.get(key) or "").strip()


def _parse_iso_date(value: str) -> date | None:
    normalized = value.strip()
    if not normalized:
        return None
    try:
        return date.fromisoformat(normalized[:10])
    except ValueError:
        return None


def _add_count(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _sorted_counts(counts: dict[str, int]) -> dict[str, int]:
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _source_ref(row: dict[str, object]) -> str | None:
    return (
        _read_string(row, "source_ref")
        or _read_string(row, "source_name")
        or _read_string(row, "name")
        or _read_string(row, "source_url")
        or None
    )


def _source_sample(
    *,
    path: Path,
    line_number: int | None,
    scope: str,
    row: dict[str, object],
    warning: str,
) -> dict[str, object]:
    return {
        "path": str(path),
        "line": line_number,
        "scope": scope,
        "warning": warning,
        "source_ref": _source_ref(row),
        "target_ref": _read_string(row, "target_ref") or None,
        "relation": _read_string(row, "relation") or None,
        "source_title": (
            _read_string(row, "source_title") or _read_string(row, "source_name") or None
        ),
        "source_url": _read_string(row, "source_url") or None,
        "license_status": _read_string(row, "license_status") or None,
        "license_notes": _read_string(row, "license_notes") or None,
        "normalized_output_allowed": row.get("normalized_output_allowed"),
    }


def _has_license_columns(row: dict[str, object]) -> bool:
    return "license_status" in row or "license_notes" in row


def _iter_tsv_rows(
    edge_tsv_paths: Iterable[str | Path],
) -> Iterable[tuple[Path, int, dict[str, object]]]:
    for edge_tsv_path in edge_tsv_paths:
        path = Path(edge_tsv_path).expanduser().resolve()
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row_index, row in enumerate(reader, start=2):
                yield path, row_index, dict(row)


def _iter_manifest_sources(
    source_manifest_paths: Iterable[str | Path],
) -> Iterable[tuple[Path, int | None, dict[str, object]]]:
    for source_manifest_path in source_manifest_paths:
        path = Path(source_manifest_path).expanduser().resolve()
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        sources = payload.get("sources", [])
        if not isinstance(sources, list):
            raise ValueError(f"{path} does not contain a list-valued sources field")
        for source in sources:
            if not isinstance(source, dict):
                raise ValueError(f"{path} contains a non-object source entry")
            yield path, None, dict(source)


def _tsv_license_warnings(row: dict[str, object]) -> tuple[str, ...]:
    warnings: list[str] = []
    license_warning = production_license_warning(row.get("license_status"))
    if license_warning:
        warnings.append(license_warning)
    if not _read_string(row, "license_notes"):
        warnings.append(_MISSING_LICENSE_NOTES)
    return tuple(dict.fromkeys(warnings))


def _manifest_license_warnings(row: dict[str, object]) -> tuple[str, ...]:
    warnings: list[str] = []
    license_status = _read_string(row, "license_status")
    if license_status:
        license_warning = production_license_warning(license_status)
        if license_warning:
            warnings.append(license_warning)
    elif row.get("normalized_output_allowed") is not True:
        warnings.append(_SOURCE_USE_UNCONFIRMED)
    if not _read_string(row, "license_notes"):
        warnings.append(_MISSING_LICENSE_NOTES)
    return tuple(dict.fromkeys(warnings))


def normalize_license_status(value: object | None) -> str:
    return str(value or "").strip().casefold().replace("-", "_").replace(" ", "_")


def production_license_warning(value: object | None) -> str | None:
    license_status = normalize_license_status(value)
    if not license_status or license_status == _LICENSE_STATUS_UNCLEAR:
        return "source_license_unclear"
    if license_status == _LICENSE_STATUS_RESTRICTED:
        return "source_license_restricted"
    if license_status not in _LICENSE_STATUS_VALUES:
        return "source_license_invalid"
    if license_status != _LICENSE_STATUS_ALLOWED:
        return "source_license_not_production_eligible"
    return None


def audit_source_licenses(
    *,
    edge_tsv_paths: Iterable[str | Path] = (),
    source_manifest_paths: Iterable[str | Path] = (),
    as_of_date: date | None = None,
    sample_limit: int = 50,
) -> dict[str, object]:
    """Return a stable JSON-compatible quarterly source-license audit payload."""

    resolved_edge_paths = tuple(Path(path).expanduser().resolve() for path in edge_tsv_paths)
    resolved_manifest_paths = tuple(
        Path(path).expanduser().resolve() for path in source_manifest_paths
    )
    if not resolved_edge_paths and not resolved_manifest_paths:
        raise ValueError("At least one edge TSV path or source manifest path is required.")
    for path in (*resolved_edge_paths, *resolved_manifest_paths):
        if not path.exists():
            raise FileNotFoundError(path)

    as_of = as_of_date or date.today()
    warning_counts: dict[str, int] = {}
    samples: list[dict[str, object]] = []
    tsv_row_count = 0
    legacy_source_row_count = 0
    manifest_source_count = 0
    allowed_source_count = 0
    blocked_source_count = 0

    for path, line_number, row in _iter_tsv_rows(resolved_edge_paths):
        tsv_row_count += 1
        if not _has_license_columns(row):
            legacy_source_row_count += 1
            if not resolved_manifest_paths:
                blocked_source_count += 1
                _add_count(warning_counts, _MISSING_LICENSE_METADATA)
                if len(samples) < sample_limit:
                    samples.append(
                        _source_sample(
                            path=path,
                            line_number=line_number,
                            scope="source_row",
                            row=row,
                            warning=_MISSING_LICENSE_METADATA,
                        )
                    )
            continue
        warnings = _tsv_license_warnings(row)
        if warnings:
            blocked_source_count += 1
        else:
            allowed_source_count += 1
        for warning in warnings:
            _add_count(warning_counts, warning)
            if len(samples) < sample_limit:
                samples.append(
                    _source_sample(
                        path=path,
                        line_number=line_number,
                        scope="source_row",
                        row=row,
                        warning=warning,
                    )
                )

    for path, line_number, row in _iter_manifest_sources(resolved_manifest_paths):
        manifest_source_count += 1
        warnings = _manifest_license_warnings(row)
        if warnings:
            blocked_source_count += 1
        else:
            allowed_source_count += 1
        for warning in warnings:
            _add_count(warning_counts, warning)
            if len(samples) < sample_limit:
                samples.append(
                    _source_sample(
                        path=path,
                        line_number=line_number,
                        scope="source_manifest",
                        row=row,
                        warning=warning,
                    )
                )

    sorted_warning_counts = _sorted_counts(warning_counts)
    return {
        "audit_metadata": {
            "audit_id": "quarterly_source_license_audit",
            "as_of_date": as_of.isoformat(),
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "edge_tsv_paths": [str(path) for path in resolved_edge_paths],
            "source_manifest_paths": [str(path) for path in resolved_manifest_paths],
            "sample_limit": sample_limit,
        },
        "status": "needs_license_review" if sorted_warning_counts else "clean",
        "summary": {
            "source_row_count": tsv_row_count,
            "legacy_source_row_count": legacy_source_row_count,
            "manifest_source_count": manifest_source_count,
            "allowed_source_count": allowed_source_count,
            "blocked_source_count": blocked_source_count,
        },
        "license_warning_counts": sorted_warning_counts,
        "license_warning_samples": samples,
        "action_items": _action_items(sorted_warning_counts),
    }


def _action_items(license_warning_counts: dict[str, int]) -> list[dict[str, object]]:
    actions: list[dict[str, object]] = []
    for warning, count in sorted(license_warning_counts.items()):
        if warning == _MISSING_LICENSE_NOTES:
            action = "add_or_refresh_license_notes_before_promotion"
        elif warning == _MISSING_LICENSE_METADATA:
            action = "supply_source_manifest_or_add_license_metadata_before_promotion"
        elif warning == _SOURCE_USE_UNCONFIRMED:
            action = "confirm_source_use_is_allowed_or_remove_from_production_artifact"
        elif warning == "source_license_restricted":
            action = "quarantine_or_obtain_explicit_license_approval"
        elif warning == "source_license_unclear":
            action = "complete_license_review_before_production_use"
        else:
            action = "resolve_source_license_warning_before_production_use"
        actions.append(
            {
                "owner": "Ops/ADES",
                "category": "source_license",
                "warning": warning,
                "count": count,
                "action": action,
            }
        )
    return actions


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit source-license status and notes for ADES source lanes."
    )
    parser.add_argument(
        "--edge-tsv-path",
        action="append",
        default=[],
        help="Canonical source-row or market graph edge TSV path. Repeat for multiple lanes.",
    )
    parser.add_argument(
        "--source-manifest-path",
        action="append",
        default=[],
        help="Source manifest JSON path with a sources array. Repeat for multiple manifests.",
    )
    parser.add_argument(
        "--as-of-date",
        help="Audit date in YYYY-MM-DD format. Defaults to today's local date.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=50,
        help="Maximum number of source-license samples to include.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    as_of_date = _parse_iso_date(args.as_of_date) if args.as_of_date else None
    if args.as_of_date and as_of_date is None:
        raise SystemExit("--as-of-date must be an ISO date in YYYY-MM-DD format")
    payload = audit_source_licenses(
        edge_tsv_paths=args.edge_tsv_path,
        source_manifest_paths=args.source_manifest_path,
        as_of_date=as_of_date,
        sample_limit=args.sample_limit,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
