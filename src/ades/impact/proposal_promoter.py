"""Promote reviewed BDYA relationship proposals into graph edge TSV lanes."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Iterable, Iterator

from .policy_sector_proxy import EDGE_COLUMNS
from .relationship_schema import (
    normalized_relation_direction_preconditions,
    normalized_relation_event_types,
    validate_relation_metadata,
)
from .source_catalog import validate_source_attribution

DEFAULT_BIG_DATA_ROOT = Path("/mnt/githubActions/ades_big_data")
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_BIG_DATA_ROOT / "pack_sources" / "impact_relationships" / "reviewed_proposals"
)
DEFAULT_PACK_ID = "reviewed-proposals-en"
ACCEPTED_REVIEW_STATES = {"accepted", "approved", "reviewed_accepted"}
REJECTED_REVIEW_STATES = {"rejected", "declined", "expired"}


@dataclass(frozen=True)
class ProposalPromotionResult:
    run_id: str
    output_dir: Path
    accepted_edges_path: Path
    rejected_proposals_path: Path
    needs_review_path: Path
    summary_path: Path
    input_paths: tuple[Path, ...]
    accepted_edge_count: int
    rejected_proposal_count: int
    needs_review_count: int
    invalid_line_count: int
    relation_counts: dict[str, int]
    rejection_counts: dict[str, int]
    needs_review_counts: dict[str, int]

    def to_json_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "output_dir": str(self.output_dir),
            "accepted_edges_path": str(self.accepted_edges_path),
            "rejected_proposals_path": str(self.rejected_proposals_path),
            "needs_review_path": str(self.needs_review_path),
            "summary_path": str(self.summary_path),
            "input_paths": [str(path) for path in self.input_paths],
            "accepted_edge_count": self.accepted_edge_count,
            "rejected_proposal_count": self.rejected_proposal_count,
            "needs_review_count": self.needs_review_count,
            "invalid_line_count": self.invalid_line_count,
            "relation_counts": self.relation_counts,
            "rejection_counts": self.rejection_counts,
            "needs_review_counts": self.needs_review_counts,
        }


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _as_record(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _read_string(record: dict[str, object], *keys: str) -> str:
    for key in keys:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value is not None and not isinstance(value, (dict, list)):
            text = str(value).strip()
            if text:
                return text
    return ""


def _read_float_string(record: dict[str, object], *keys: str, default: str) -> str:
    raw = _read_string(record, *keys)
    if not raw:
        return default
    try:
        parsed = float(raw)
    except ValueError:
        return default
    if parsed < 0 or parsed > 1:
        return default
    return f"{parsed:.6g}"


def _read_jsonl_records(
    paths: Iterable[Path],
) -> Iterator[tuple[Path, int, dict[str, object] | None]]:
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    yield path, line_number, None
                    continue
                yield path, line_number, _as_record(parsed) or None


def _parse_items(value: object) -> tuple[str, ...]:
    if isinstance(value, list):
        return tuple(str(item).strip() for item in value if str(item).strip())
    raw = str(value or "").strip()
    if not raw:
        return ()
    return tuple(item.strip() for item in re.split(r"[,|]", raw) if item.strip())


def _proposal_payload(record: dict[str, object]) -> dict[str, object]:
    raw_item = _as_record(record.get("raw_item"))
    accepted_edge = _as_record(record.get("accepted_edge"))
    edge = _as_record(record.get("edge"))
    return accepted_edge or edge or raw_item or record


def _review_state(record: dict[str, object], proposal: dict[str, object]) -> str:
    return (
        _read_string(record, "promotion_state", "review_state", "status")
        or _read_string(proposal, "promotion_state", "review_state", "status")
        or "observed"
    ).casefold()


def _source_year(proposal: dict[str, object]) -> str:
    explicit = _read_string(proposal, "source_year", "year")
    if explicit:
        return explicit
    snapshot = _read_string(proposal, "source_snapshot", "snapshot")
    match = re.search(r"\b(20\d{2}|19\d{2})\b", snapshot)
    return match.group(1) if match else ""


def _notes(record: dict[str, object], proposal: dict[str, object]) -> str:
    notes = _read_string(proposal, "notes", "review_notes", "rationale")
    import_id = _read_string(record, "import_id")
    if notes and import_id:
        return f"{notes}; bdya_import_id={import_id}"
    if import_id:
        return f"bdya_import_id={import_id}"
    return notes


def _edge_row(
    *,
    record: dict[str, object],
    proposal: dict[str, object],
) -> dict[str, str]:
    relation = _read_string(proposal, "relation", "relation_type")
    compatible_event_types = normalized_relation_event_types(
        relation,
        _parse_items(proposal.get("compatible_event_types")),
    )
    direction_preconditions = normalized_relation_direction_preconditions(
        relation,
        _parse_items(proposal.get("direction_preconditions")),
    )
    return {
        "source_ref": _read_string(proposal, "source_ref", "sourceRef"),
        "target_ref": _read_string(proposal, "target_ref", "targetRef"),
        "relation": relation,
        "evidence_level": _read_string(proposal, "evidence_level", "evidenceLevel")
        or "reviewed_proposal",
        "confidence": _read_float_string(proposal, "confidence", default="0.75"),
        "direction_hint": _read_string(
            proposal,
            "direction_hint",
            "directionHint",
            "direction",
        ),
        "source_name": _read_string(proposal, "source_name", "sourceName"),
        "source_url": _read_string(proposal, "source_url", "sourceUrl"),
        "source_snapshot": _read_string(
            proposal,
            "source_snapshot",
            "sourceSnapshot",
            "snapshot",
        ),
        "source_year": _source_year(proposal),
        "refresh_policy": _read_string(proposal, "refresh_policy", "refreshPolicy")
        or "manual_review",
        "pack_ids": ",".join(_parse_items(proposal.get("pack_ids"))) or DEFAULT_PACK_ID,
        "notes": _notes(record, proposal),
        "compatible_event_types": ",".join(compatible_event_types),
        "direction_preconditions": ",".join(direction_preconditions),
    }


def _required_edge_warnings(edge: dict[str, str]) -> list[str]:
    warnings: list[str] = []
    for field in ("source_ref", "target_ref", "relation"):
        if not edge[field]:
            warnings.append(f"missing_{field}")
    return warnings


def _review_record(
    *,
    record: dict[str, object],
    proposal: dict[str, object],
    path: Path,
    line_number: int,
    reason: str,
    warnings: list[str],
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "source_path": str(path),
        "source_line": line_number,
        "reason": reason,
        "warnings": warnings,
        "import_id": _read_string(record, "import_id") or None,
        "news_id": _read_string(record, "news_id") or None,
        "story_id": _read_string(record, "story_id") or None,
        "title": _read_string(record, "title") or None,
        "raw_item": proposal,
    }


def _write_jsonl(handle, row: dict[str, object]) -> None:
    handle.write(_stable_json(row))
    handle.write("\n")


def _add_count(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _sorted_counts(counts: dict[str, int]) -> dict[str, int]:
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def promote_reviewed_relationship_proposals(
    *,
    input_paths: Iterable[str | Path],
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> ProposalPromotionResult:
    resolved_input_paths = tuple(Path(path).expanduser().resolve() for path in input_paths)
    if not resolved_input_paths:
        raise ValueError("At least one reviewed proposal JSONL input path is required.")
    for path in resolved_input_paths:
        if not path.exists():
            raise FileNotFoundError(path)

    resolved_run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(output_root).expanduser().resolve() / resolved_run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    accepted_edges_path = output_dir / "accepted_edges.tsv"
    rejected_proposals_path = output_dir / "rejected_proposals.jsonl"
    needs_review_path = output_dir / "needs_review.jsonl"
    summary_path = output_dir / "summary.json"

    accepted_edge_count = 0
    rejected_proposal_count = 0
    needs_review_count = 0
    invalid_line_count = 0
    relation_counts: dict[str, int] = {}
    rejection_counts: dict[str, int] = {}
    needs_review_counts: dict[str, int] = {}
    accepted_edge_keys: set[tuple[str, str, str, str]] = set()

    with (
        accepted_edges_path.open("w", encoding="utf-8", newline="") as edge_handle,
        rejected_proposals_path.open("w", encoding="utf-8") as rejected_handle,
        needs_review_path.open("w", encoding="utf-8") as needs_review_handle,
    ):
        edge_writer = csv.DictWriter(
            edge_handle,
            delimiter="\t",
            fieldnames=EDGE_COLUMNS,
        )
        edge_writer.writeheader()
        for path, line_number, record in _read_jsonl_records(resolved_input_paths):
            if record is None:
                invalid_line_count += 1
                continue
            proposal = _proposal_payload(record)
            review_state = _review_state(record, proposal)
            if review_state in REJECTED_REVIEW_STATES:
                rejected_proposal_count += 1
                _add_count(rejection_counts, f"review_state:{review_state}")
                _write_jsonl(
                    rejected_handle,
                    _review_record(
                        record=record,
                        proposal=proposal,
                        path=path,
                        line_number=line_number,
                        reason="review_rejected",
                        warnings=[f"review_state:{review_state}"],
                    ),
                )
                continue
            if review_state not in ACCEPTED_REVIEW_STATES:
                needs_review_count += 1
                _add_count(needs_review_counts, f"review_state:{review_state}")
                _write_jsonl(
                    needs_review_handle,
                    _review_record(
                        record=record,
                        proposal=proposal,
                        path=path,
                        line_number=line_number,
                        reason="not_accepted",
                        warnings=[f"review_state:{review_state}"],
                    ),
                )
                continue

            edge = _edge_row(record=record, proposal=proposal)
            required_warnings = _required_edge_warnings(edge)
            source_warnings = validate_source_attribution(
                source_name=edge["source_name"] or None,
                source_url=edge["source_url"] or None,
                source_snapshot=edge["source_snapshot"] or None,
            )
            if required_warnings or source_warnings:
                warnings = [*required_warnings, *source_warnings]
                rejected_proposal_count += 1
                for warning in warnings:
                    _add_count(rejection_counts, warning)
                _write_jsonl(
                    rejected_handle,
                    _review_record(
                        record=record,
                        proposal=proposal,
                        path=path,
                        line_number=line_number,
                        reason="source_or_edge_validation_failed",
                        warnings=warnings,
                    ),
                )
                continue

            relation_warnings = validate_relation_metadata(
                relation=edge["relation"],
                configured_event_types=_parse_items(proposal.get("compatible_event_types")),
                configured_preconditions=_parse_items(proposal.get("direction_preconditions")),
            )
            if relation_warnings:
                needs_review_count += 1
                for warning in relation_warnings:
                    _add_count(needs_review_counts, warning)
                _write_jsonl(
                    needs_review_handle,
                    _review_record(
                        record=record,
                        proposal=proposal,
                        path=path,
                        line_number=line_number,
                        reason="relation_schema_review_required",
                        warnings=relation_warnings,
                    ),
                )
                continue

            edge_key = (
                edge["source_ref"],
                edge["target_ref"],
                edge["relation"],
                edge["source_snapshot"],
            )
            if edge_key in accepted_edge_keys:
                continue
            accepted_edge_keys.add(edge_key)
            edge_writer.writerow(edge)
            accepted_edge_count += 1
            _add_count(relation_counts, edge["relation"])

    result = ProposalPromotionResult(
        run_id=resolved_run_id,
        output_dir=output_dir,
        accepted_edges_path=accepted_edges_path,
        rejected_proposals_path=rejected_proposals_path,
        needs_review_path=needs_review_path,
        summary_path=summary_path,
        input_paths=resolved_input_paths,
        accepted_edge_count=accepted_edge_count,
        rejected_proposal_count=rejected_proposal_count,
        needs_review_count=needs_review_count,
        invalid_line_count=invalid_line_count,
        relation_counts=_sorted_counts(relation_counts),
        rejection_counts=_sorted_counts(rejection_counts),
        needs_review_counts=_sorted_counts(needs_review_counts),
    )
    summary_path.write_text(
        json.dumps(result.to_json_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return result


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", required=True)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    result = promote_reviewed_relationship_proposals(
        input_paths=args.input,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(json.dumps(result.to_json_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
