"""Import BDYA impact-gap backlog exports into ADES review queues."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Iterable, Iterator

DEFAULT_BIG_DATA_ROOT = Path("/mnt/githubActions/ades_big_data")
DEFAULT_OUTPUT_ROOT = DEFAULT_BIG_DATA_ROOT / "feedback" / "bdya_gap_backlog_imports"


@dataclass(frozen=True)
class BdyaGapBacklogImportResult:
    run_id: str
    output_dir: Path
    story_misses_path: Path
    unresolved_entities_path: Path
    relationship_proposals_path: Path
    summary_path: Path
    input_paths: tuple[Path, ...]
    story_count: int
    market_relevant_story_count: int
    unresolved_entity_count: int
    relationship_proposal_count: int
    invalid_line_count: int
    issue_counts: dict[str, int]

    def to_json_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "output_dir": str(self.output_dir),
            "story_misses_path": str(self.story_misses_path),
            "unresolved_entities_path": str(self.unresolved_entities_path),
            "relationship_proposals_path": str(self.relationship_proposals_path),
            "summary_path": str(self.summary_path),
            "input_paths": [str(path) for path in self.input_paths],
            "story_count": self.story_count,
            "market_relevant_story_count": self.market_relevant_story_count,
            "unresolved_entity_count": self.unresolved_entity_count,
            "relationship_proposal_count": self.relationship_proposal_count,
            "invalid_line_count": self.invalid_line_count,
            "issue_counts": self.issue_counts,
        }


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _stable_id(prefix: str, *parts: object) -> str:
    digest = hashlib.sha256(
        "\n".join(_stable_json(part) for part in parts).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:{digest[:24]}"


def _as_record(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _as_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def _as_string_list(value: object) -> list[str]:
    return [item for item in _as_list(value) if isinstance(item, str)]


def _read_string(record: dict[str, object], key: str) -> str | None:
    value = record.get(key)
    return value.strip() if isinstance(value, str) and value.strip() else None


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


def _story_context(
    *,
    record: dict[str, object],
    source_path: Path,
    source_line: int,
) -> dict[str, object]:
    issue_classes = _as_string_list(record.get("issue_classes"))
    return {
        "news_id": _read_string(record, "news_id"),
        "story_id": _read_string(record, "story_id"),
        "published_at": _read_string(record, "published_at"),
        "title": _read_string(record, "title"),
        "summary": _read_string(record, "summary"),
        "market_relevant": bool(record.get("market_relevant")),
        "issue_classes": issue_classes,
        "source_path": str(source_path),
        "source_line": source_line,
    }


def _story_miss_row(
    *,
    record: dict[str, object],
    run_id: str,
    source_path: Path,
    source_line: int,
) -> dict[str, object]:
    context = _story_context(
        record=record,
        source_path=source_path,
        source_line=source_line,
    )
    return {
        "schema_version": 1,
        "record_type": "story_miss",
        "import_id": _stable_id(
            "bdya-story-miss",
            context.get("news_id"),
            context.get("story_id"),
            context.get("issue_classes"),
            source_line,
        ),
        "run_id": run_id,
        "source": "bdya_impact_gap_backlog",
        **context,
        "event_signal_count": len(_as_list(record.get("event_signals"))),
        "source_entity_count": len(_as_list(record.get("source_entities"))),
        "terminal_candidate_count": len(_as_list(record.get("terminal_candidates"))),
        "unresolved_entity_count": len(_as_list(record.get("unresolved_entities"))),
        "relationship_proposal_count": len(_as_list(record.get("relationship_proposals"))),
        "warnings": _as_string_list(record.get("warnings")),
        "promotion_state": "observed",
    }


def _queue_row(
    *,
    raw_item: object,
    item_index: int,
    record: dict[str, object],
    record_type: str,
    run_id: str,
    source_path: Path,
    source_line: int,
) -> dict[str, object]:
    context = _story_context(
        record=record,
        source_path=source_path,
        source_line=source_line,
    )
    return {
        "schema_version": 1,
        "record_type": record_type,
        "import_id": _stable_id(
            f"bdya-{record_type}",
            context.get("news_id"),
            context.get("story_id"),
            item_index,
            raw_item,
        ),
        "run_id": run_id,
        "source": "bdya_impact_gap_backlog",
        **context,
        "raw_item": raw_item,
        "promotion_state": "observed",
        "review_priority": _review_priority(context["issue_classes"]),
    }


def _review_priority(issue_classes: object) -> str:
    issues = set(issue_classes if isinstance(issue_classes, list) else [])
    if "relationship_proposals_present" in issues:
        return "high"
    if "source_entities_without_graph_terminal" in issues:
        return "high"
    if "unresolved_entities_present" in issues:
        return "medium"
    if "broad_proxy_only" in issues:
        return "medium"
    return "normal"


def _write_jsonl(handle, row: dict[str, object]) -> None:
    handle.write(_stable_json(row))
    handle.write("\n")


def _sorted_issue_counts(issue_counts: dict[str, int]) -> dict[str, int]:
    return dict(sorted(issue_counts.items(), key=lambda item: (-item[1], item[0])))


def import_bdya_gap_backlog(
    *,
    input_paths: Iterable[str | Path],
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> BdyaGapBacklogImportResult:
    resolved_input_paths = tuple(Path(path).expanduser().resolve() for path in input_paths)
    if not resolved_input_paths:
        raise ValueError("At least one BDYA impact-gap backlog input path is required.")
    for path in resolved_input_paths:
        if not path.exists():
            raise FileNotFoundError(path)

    resolved_run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(output_root).expanduser().resolve() / resolved_run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    story_misses_path = output_dir / "story_misses.jsonl"
    unresolved_entities_path = output_dir / "unresolved_entities.jsonl"
    relationship_proposals_path = output_dir / "relationship_proposals.jsonl"
    summary_path = output_dir / "summary.json"

    story_count = 0
    market_relevant_story_count = 0
    unresolved_entity_count = 0
    relationship_proposal_count = 0
    invalid_line_count = 0
    issue_counts: dict[str, int] = {}

    with (
        story_misses_path.open("w", encoding="utf-8") as story_handle,
        unresolved_entities_path.open("w", encoding="utf-8") as unresolved_handle,
        relationship_proposals_path.open("w", encoding="utf-8") as proposal_handle,
    ):
        for source_path, source_line, record in _read_jsonl_records(resolved_input_paths):
            if record is None:
                invalid_line_count += 1
                continue
            story_count += 1
            if bool(record.get("market_relevant")):
                market_relevant_story_count += 1
            for issue_class in _as_string_list(record.get("issue_classes")):
                issue_counts[issue_class] = issue_counts.get(issue_class, 0) + 1
            _write_jsonl(
                story_handle,
                _story_miss_row(
                    record=record,
                    run_id=resolved_run_id,
                    source_path=source_path,
                    source_line=source_line,
                ),
            )
            for item_index, raw_item in enumerate(
                _as_list(record.get("unresolved_entities")), start=1
            ):
                unresolved_entity_count += 1
                _write_jsonl(
                    unresolved_handle,
                    _queue_row(
                        raw_item=raw_item,
                        item_index=item_index,
                        record=record,
                        record_type="unresolved_entity",
                        run_id=resolved_run_id,
                        source_path=source_path,
                        source_line=source_line,
                    ),
                )
            for item_index, raw_item in enumerate(
                _as_list(record.get("relationship_proposals")), start=1
            ):
                relationship_proposal_count += 1
                _write_jsonl(
                    proposal_handle,
                    _queue_row(
                        raw_item=raw_item,
                        item_index=item_index,
                        record=record,
                        record_type="relationship_proposal",
                        run_id=resolved_run_id,
                        source_path=source_path,
                        source_line=source_line,
                    ),
                )

    result = BdyaGapBacklogImportResult(
        run_id=resolved_run_id,
        output_dir=output_dir,
        story_misses_path=story_misses_path,
        unresolved_entities_path=unresolved_entities_path,
        relationship_proposals_path=relationship_proposals_path,
        summary_path=summary_path,
        input_paths=resolved_input_paths,
        story_count=story_count,
        market_relevant_story_count=market_relevant_story_count,
        unresolved_entity_count=unresolved_entity_count,
        relationship_proposal_count=relationship_proposal_count,
        invalid_line_count=invalid_line_count,
        issue_counts=_sorted_issue_counts(issue_counts),
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
    result = import_bdya_gap_backlog(
        input_paths=args.input,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(json.dumps(result.to_json_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
