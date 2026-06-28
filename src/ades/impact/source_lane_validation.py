"""Validate normalized market impact source-lane TSV files."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable

from .relationship_schema import validate_relation_metadata
from .source_catalog import classify_source_tier, validate_source_attribution


@dataclass(frozen=True)
class ImpactSourceLaneValidationResult:
    edge_tsv_paths: tuple[Path, ...]
    row_count: int
    invalid_row_count: int
    relation_counts: dict[str, int]
    source_tier_counts: dict[str, int]
    source_warning_counts: dict[str, int]
    relation_warning_counts: dict[str, int]
    warning_samples: tuple[dict[str, object], ...]

    def to_json_dict(self) -> dict[str, object]:
        return {
            "edge_tsv_paths": [str(path) for path in self.edge_tsv_paths],
            "row_count": self.row_count,
            "invalid_row_count": self.invalid_row_count,
            "relation_counts": self.relation_counts,
            "source_tier_counts": self.source_tier_counts,
            "source_warning_counts": self.source_warning_counts,
            "relation_warning_counts": self.relation_warning_counts,
            "warning_samples": list(self.warning_samples),
        }


def _read_string(row: dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


def _parse_items(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in re.split(r"[,|]", value) if item.strip())


def _add_count(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _sorted_counts(counts: dict[str, int]) -> dict[str, int]:
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _warning_sample(
    *,
    path: Path,
    line_number: int,
    row: dict[str, str],
    scope: str,
    warning: str,
) -> dict[str, object]:
    return {
        "path": str(path),
        "line": line_number,
        "scope": scope,
        "warning": warning,
        "source_ref": _read_string(row, "source_ref") or None,
        "target_ref": _read_string(row, "target_ref") or None,
        "relation": _read_string(row, "relation") or None,
    }


def validate_market_graph_source_lanes(
    *,
    edge_tsv_paths: Iterable[str | Path],
    sample_limit: int = 50,
) -> ImpactSourceLaneValidationResult:
    resolved_paths = tuple(Path(path).expanduser().resolve() for path in edge_tsv_paths)
    if not resolved_paths:
        raise ValueError("At least one edge TSV path is required.")
    for path in resolved_paths:
        if not path.exists():
            raise FileNotFoundError(path)

    row_count = 0
    invalid_row_count = 0
    relation_counts: dict[str, int] = {}
    source_tier_counts: dict[str, int] = {}
    source_warning_counts: dict[str, int] = {}
    relation_warning_counts: dict[str, int] = {}
    warning_samples: list[dict[str, object]] = []

    for path in resolved_paths:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row_index, row in enumerate(reader, start=2):
                row_count += 1
                source_ref = _read_string(row, "source_ref")
                target_ref = _read_string(row, "target_ref")
                relation = _read_string(row, "relation")
                if not source_ref or not target_ref or not relation:
                    invalid_row_count += 1
                if relation:
                    _add_count(relation_counts, relation)

                source_name = _read_string(row, "source_name")
                source_url = _read_string(row, "source_url")
                source_snapshot = _read_string(row, "source_snapshot")
                source_tier = classify_source_tier(source_name, source_url)
                _add_count(source_tier_counts, source_tier)
                for warning in validate_source_attribution(
                    source_name=source_name or None,
                    source_url=source_url or None,
                    source_snapshot=source_snapshot or None,
                ):
                    _add_count(source_warning_counts, warning)
                    if len(warning_samples) < sample_limit:
                        warning_samples.append(
                            _warning_sample(
                                path=path,
                                line_number=row_index,
                                row=row,
                                scope="source",
                                warning=warning,
                            )
                        )

                for warning in validate_relation_metadata(
                    relation=relation,
                    configured_event_types=_parse_items(
                        _read_string(row, "compatible_event_types")
                    ),
                    configured_preconditions=_parse_items(
                        _read_string(row, "direction_preconditions")
                    ),
                ):
                    _add_count(relation_warning_counts, warning)
                    if len(warning_samples) < sample_limit:
                        warning_samples.append(
                            _warning_sample(
                                path=path,
                                line_number=row_index,
                                row=row,
                                scope="relation",
                                warning=warning,
                            )
                        )

    return ImpactSourceLaneValidationResult(
        edge_tsv_paths=resolved_paths,
        row_count=row_count,
        invalid_row_count=invalid_row_count,
        relation_counts=_sorted_counts(relation_counts),
        source_tier_counts=_sorted_counts(source_tier_counts),
        source_warning_counts=_sorted_counts(source_warning_counts),
        relation_warning_counts=_sorted_counts(relation_warning_counts),
        warning_samples=tuple(warning_samples),
    )
