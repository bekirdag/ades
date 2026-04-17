"""Batch review helpers for curating generic retained aliases in general-en."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import shlex
import sqlite3
import subprocess
from typing import Any

from .alias_analysis import (
    DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME,
    DEFAULT_RETAINED_ALIAS_AUDIT_CHUNK_SIZE,
    DEFAULT_RETAINED_ALIAS_AUDIT_TIMEOUT_SECONDS,
    _classify_deterministic_common_english_retained_alias,
    _classify_generic_general_retained_alias,
    RetainedAliasReviewDecision,
    build_retained_alias_review_key,
    build_retained_alias_review_payload,
    iter_retained_aliases_from_db,
)


DEFAULT_GENERAL_ALIAS_REVIEW_OUTPUT_ROOT = Path(
    "/mnt/githubActions/ades_big_data/general_en_alias_review"
)
DEFAULT_GENERAL_ALIAS_REVIEW_DB_FILENAME = "reviews.sqlite"
DEFAULT_GENERAL_ALIAS_REVIEW_JSONL_FILENAME = "reviews.jsonl"
DEFAULT_GENERAL_ALIAS_REVIEW_MARKDOWN_FILENAME = "proposed-exclusions.md"
DEFAULT_GENERAL_ALIAS_REVIEW_BATCH_TIMEOUT_SECONDS = (
    DEFAULT_RETAINED_ALIAS_AUDIT_TIMEOUT_SECONDS
)
GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_ALL = "all"
GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_LIKELY_GENERIC = "likely_generic"
GENERAL_ALIAS_REVIEW_CANDIDATE_MODES = {
    GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_ALL,
    GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_LIKELY_GENERIC,
}
GENERAL_ALIAS_REVIEW_MODE_AI_COMMAND = "ai_command"
GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH = (
    "deterministic_common_english"
)
GENERAL_ALIAS_REVIEW_MODES = {
    GENERAL_ALIAS_REVIEW_MODE_AI_COMMAND,
    GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH,
}


@dataclass(frozen=True)
class GeneralAliasReviewArtifacts:
    output_root: str
    review_db_path: str
    review_jsonl_path: str
    review_markdown_path: str
    drop_jsonl_path: str


@dataclass(frozen=True)
class GeneralAliasReviewResult:
    analysis_db_path: str
    reviewer_name: str
    candidate_mode: str
    candidate_total_count: int
    chunk_size: int
    batches_run: int
    new_reviewed_count: int
    cached_review_count: int
    total_reviewed_count: int
    proposed_drop_count: int
    proposed_keep_count: int
    remaining_unreviewed_count: int
    artifacts: GeneralAliasReviewArtifacts


def default_general_alias_review_output_root(analysis_db_path: str | Path) -> Path:
    resolved = Path(analysis_db_path).expanduser().resolve()
    parent_name = resolved.parent.name or "analysis"
    return DEFAULT_GENERAL_ALIAS_REVIEW_OUTPUT_ROOT / parent_name


def review_general_retained_aliases(
    analysis_db_path: str | Path,
    *,
    reviewer_command: str | None = None,
    review_mode: str = GENERAL_ALIAS_REVIEW_MODE_AI_COMMAND,
    output_root: str | Path | None = None,
    candidate_mode: str = GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_ALL,
    chunk_size: int = DEFAULT_RETAINED_ALIAS_AUDIT_CHUNK_SIZE,
    max_batches: int | None = None,
    timeout_seconds: float = DEFAULT_GENERAL_ALIAS_REVIEW_BATCH_TIMEOUT_SECONDS,
) -> GeneralAliasReviewResult:
    if review_mode not in GENERAL_ALIAS_REVIEW_MODES:
        raise ValueError(
            "review_mode must be one of: " + ", ".join(sorted(GENERAL_ALIAS_REVIEW_MODES))
        )
    if candidate_mode not in GENERAL_ALIAS_REVIEW_CANDIDATE_MODES:
        raise ValueError(
            "candidate_mode must be one of: "
            + ", ".join(sorted(GENERAL_ALIAS_REVIEW_CANDIDATE_MODES))
        )
    if (
        review_mode == GENERAL_ALIAS_REVIEW_MODE_AI_COMMAND
        and not str(reviewer_command or "").strip()
    ):
        raise ValueError("reviewer_command is required when review_mode=ai_command.")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive.")
    if max_batches is not None and max_batches <= 0:
        raise ValueError("max_batches must be positive when provided.")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive.")
    resolved_analysis_db_path = Path(analysis_db_path).expanduser().resolve()
    if not resolved_analysis_db_path.exists():
        raise FileNotFoundError(
            f"Alias analysis database not found: {resolved_analysis_db_path}"
        )
    resolved_output_root = (
        Path(output_root).expanduser().resolve()
        if output_root is not None
        else default_general_alias_review_output_root(resolved_analysis_db_path)
    )
    artifacts = _resolve_artifacts(resolved_output_root)
    existing_keys = _load_existing_review_keys(Path(artifacts.review_db_path))
    cached_review_count = len(existing_keys)
    new_reviewed_count = 0
    batches_run = 0
    pending_batch: list[dict[str, Any]] = []

    for alias in iter_retained_aliases_from_db(
        resolved_analysis_db_path,
        chunk_size=chunk_size,
    ):
        if not _retained_alias_matches_candidate_mode(
            alias,
            candidate_mode=candidate_mode,
        ):
            continue
        review_key = build_retained_alias_review_key(alias)
        if review_key in existing_keys:
            continue
        pending_batch.append(
            {
                "review_key": review_key,
                **build_retained_alias_review_payload(alias),
            }
        )
        if len(pending_batch) < chunk_size:
            continue
        _review_and_store_batch(
            pending_batch,
            review_mode=review_mode,
            reviewer_command=reviewer_command,
            review_db_path=Path(artifacts.review_db_path),
            review_jsonl_path=Path(artifacts.review_jsonl_path),
            batch_index=batches_run + 1,
            timeout_seconds=timeout_seconds,
        )
        existing_keys.update(item["review_key"] for item in pending_batch)
        new_reviewed_count += len(pending_batch)
        batches_run += 1
        pending_batch = []
        if max_batches is not None and batches_run >= max_batches:
            break

    if pending_batch and (max_batches is None or batches_run < max_batches):
        _review_and_store_batch(
            pending_batch,
            review_mode=review_mode,
            reviewer_command=reviewer_command,
            review_db_path=Path(artifacts.review_db_path),
            review_jsonl_path=Path(artifacts.review_jsonl_path),
            batch_index=batches_run + 1,
            timeout_seconds=timeout_seconds,
        )
        existing_keys.update(item["review_key"] for item in pending_batch)
        new_reviewed_count += len(pending_batch)
        batches_run += 1

    render_general_alias_review_markdown(
        Path(artifacts.review_db_path),
        Path(artifacts.review_markdown_path),
    )
    export_general_alias_drop_decisions(
        Path(artifacts.review_db_path),
        Path(artifacts.drop_jsonl_path),
    )
    review_counts = _load_review_counts(Path(artifacts.review_db_path))
    total_alias_count = _count_retained_aliases(
        resolved_analysis_db_path,
        candidate_mode=candidate_mode,
        chunk_size=chunk_size,
    )
    return GeneralAliasReviewResult(
        analysis_db_path=str(resolved_analysis_db_path),
        reviewer_name=_resolve_reviewer_name(
            reviewer_command,
            review_mode=review_mode,
        ),
        candidate_mode=candidate_mode,
        candidate_total_count=total_alias_count,
        chunk_size=chunk_size,
        batches_run=batches_run,
        new_reviewed_count=new_reviewed_count,
        cached_review_count=cached_review_count,
        total_reviewed_count=review_counts["reviewed"],
        proposed_drop_count=review_counts["drop"],
        proposed_keep_count=review_counts["keep"],
        remaining_unreviewed_count=max(total_alias_count - review_counts["reviewed"], 0),
        artifacts=artifacts,
    )


def _retained_alias_matches_candidate_mode(
    alias: dict[str, Any],
    *,
    candidate_mode: str,
) -> bool:
    if candidate_mode == GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_ALL:
        return True
    if candidate_mode == GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_LIKELY_GENERIC:
        return (
            _classify_deterministic_common_english_retained_alias(
                text=str(alias.get("text", "")),
                label=str(alias.get("label", "")),
                canonical_text=str(alias.get("canonical_text", "")),
            )
            is not None
        )
    raise ValueError(f"Unsupported candidate_mode: {candidate_mode!r}")


def render_general_alias_review_markdown(
    review_db_path: str | Path,
    output_path: str | Path,
) -> Path:
    resolved_review_db_path = Path(review_db_path).expanduser().resolve()
    rows = _load_drop_rows(resolved_review_db_path)
    resolved_output_path = Path(output_path).expanduser().resolve()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    counts = _load_review_counts(resolved_review_db_path)
    lines = [
        "# Proposed general-en retained alias exclusions",
        "",
        f"Generated at: `{generated_at}`",
        "",
        f"- Reviewed aliases: `{counts['reviewed']}`",
        f"- Proposed drops: `{counts['drop']}`",
        f"- Proposed keeps: `{counts['keep']}`",
        "",
    ]
    if not rows:
        lines.append("No proposed exclusions yet.")
    else:
        lines.extend(
            [
                "| Alias | Label | Canonical | Entity ID | Source | Reason |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in rows:
            alias = _markdown_cell(row["display_text"])
            label = _markdown_cell(row["label"])
            canonical = _markdown_cell(row["canonical_text"])
            entity_id = _markdown_cell(row["entity_id"])
            source_name = _markdown_cell(row["source_name"])
            reason = _markdown_cell(f"{row['reason_code']}: {row['reason']}")
            lines.append(
                f"| {alias} | {label} | {canonical} | {entity_id} | {source_name} | {reason} |"
            )
    resolved_output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return resolved_output_path


def export_general_alias_drop_decisions(
    review_db_path: str | Path,
    output_path: str | Path,
) -> Path:
    resolved_review_db_path = Path(review_db_path).expanduser().resolve()
    resolved_output_path = Path(output_path).expanduser().resolve()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = _load_drop_rows(resolved_review_db_path)
    with resolved_output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            payload = {
                "review_key": row["review_key"],
                "text": row["display_text"],
                "label": row["label"],
                "canonical_text": row["canonical_text"],
                "entity_id": row["entity_id"],
                "source_name": row["source_name"],
                "decision": row["decision"],
                "reason_code": row["reason_code"],
                "reason": row["reason"],
                "reviewer_name": row["reviewer_name"],
                "reviewed_at": row["reviewed_at"],
            }
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return resolved_output_path


def _resolve_artifacts(output_root: Path) -> GeneralAliasReviewArtifacts:
    output_root.mkdir(parents=True, exist_ok=True)
    return GeneralAliasReviewArtifacts(
        output_root=str(output_root),
        review_db_path=str(output_root / DEFAULT_GENERAL_ALIAS_REVIEW_DB_FILENAME),
        review_jsonl_path=str(output_root / DEFAULT_GENERAL_ALIAS_REVIEW_JSONL_FILENAME),
        review_markdown_path=str(output_root / DEFAULT_GENERAL_ALIAS_REVIEW_MARKDOWN_FILENAME),
        drop_jsonl_path=str(output_root / DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME),
    )


def _open_review_store(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS alias_reviews (
            review_key TEXT PRIMARY KEY,
            display_text TEXT NOT NULL,
            label TEXT NOT NULL,
            canonical_text TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            decision TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            reason TEXT NOT NULL,
            reviewer_name TEXT NOT NULL,
            batch_index INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            response_json TEXT NOT NULL,
            reviewed_at TEXT NOT NULL
        )
        """
    )
    connection.commit()
    return connection


def _load_existing_review_keys(review_db_path: Path) -> set[str]:
    connection = _open_review_store(review_db_path)
    try:
        return {
            str(row[0])
            for row in connection.execute(
                "SELECT review_key FROM alias_reviews ORDER BY review_key ASC"
            )
        }
    finally:
        connection.close()


def _review_and_store_batch(
    batch: list[dict[str, Any]],
    *,
    review_mode: str,
    reviewer_command: str | None,
    review_db_path: Path,
    review_jsonl_path: Path,
    batch_index: int,
    timeout_seconds: float,
) -> None:
    if review_mode == GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH:
        review_results = _run_deterministic_common_english_batch_review(batch)
    elif review_mode == GENERAL_ALIAS_REVIEW_MODE_AI_COMMAND:
        if not reviewer_command:
            raise RuntimeError("reviewer_command is required for ai_command review mode.")
        review_results = _run_general_alias_batch_review_command(
            batch,
            command=reviewer_command,
            timeout_seconds=timeout_seconds,
        )
    else:
        raise RuntimeError(f"Unsupported general alias review mode: {review_mode}")
    reviewer_name = _resolve_reviewer_name(
        reviewer_command,
        review_mode=review_mode,
    )
    reviewed_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    connection = _open_review_store(review_db_path)
    review_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with review_jsonl_path.open("a", encoding="utf-8") as handle:
            for alias in batch:
                review_key = str(alias["review_key"])
                decision = review_results[review_key]
                response_payload = {
                    "review_key": review_key,
                    "decision": decision.decision,
                    "reason_code": decision.reason_code,
                    "reason": decision.reason,
                }
                record = {
                    "review_key": review_key,
                    "display_text": alias["text"],
                    "label": alias["label"],
                    "canonical_text": alias["canonical_text"],
                    "entity_id": alias["entity_id"],
                    "source_name": alias["source_name"],
                    "decision": decision.decision,
                    "reason_code": decision.reason_code,
                    "reason": decision.reason,
                    "reviewer_name": reviewer_name,
                    "batch_index": batch_index,
                    "payload_json": json.dumps(alias, ensure_ascii=False, sort_keys=True),
                    "response_json": json.dumps(
                        response_payload,
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    "reviewed_at": reviewed_at,
                }
                connection.execute(
                    """
                    INSERT OR REPLACE INTO alias_reviews (
                        review_key,
                        display_text,
                        label,
                        canonical_text,
                        entity_id,
                        source_name,
                        decision,
                        reason_code,
                        reason,
                        reviewer_name,
                        batch_index,
                        payload_json,
                        response_json,
                        reviewed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["review_key"],
                        record["display_text"],
                        record["label"],
                        record["canonical_text"],
                        record["entity_id"],
                        record["source_name"],
                        record["decision"],
                        record["reason_code"],
                        record["reason"],
                        record["reviewer_name"],
                        record["batch_index"],
                        record["payload_json"],
                        record["response_json"],
                        record["reviewed_at"],
                    ),
                )
                handle.write(
                    json.dumps(
                        {
                            "review_key": record["review_key"],
                            "text": record["display_text"],
                            "label": record["label"],
                            "canonical_text": record["canonical_text"],
                            "entity_id": record["entity_id"],
                            "source_name": record["source_name"],
                            "decision": record["decision"],
                            "reason_code": record["reason_code"],
                            "reason": record["reason"],
                            "reviewer_name": record["reviewer_name"],
                            "batch_index": record["batch_index"],
                            "reviewed_at": record["reviewed_at"],
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    + "\n"
                )
        connection.commit()
    finally:
        connection.close()


def _run_general_alias_batch_review_command(
    batch: list[dict[str, Any]],
    *,
    command: str,
    timeout_seconds: float,
) -> dict[str, RetainedAliasReviewDecision]:
    payload = {
        "task": "general_retained_alias_batch_review",
        "instructions": (
            "Review every alias one by one. Return one keep/drop verdict for each review_key. "
            "Drop aliases that are too generic for the general-en runtime even if they refer to "
            "real but obscure entities. Prefer keeping specific aliases over broad removals."
        ),
        "required_response_schema": {
            "reviews": [
                {
                    "review_key": "input review_key",
                    "decision": "keep|drop",
                    "reason_code": "short_snake_case_code",
                    "reason": "short_human_readable_reason",
                }
            ]
        },
        "aliases": batch,
    }
    process = subprocess.run(
        shlex.split(command),
        input=json.dumps(payload, ensure_ascii=False),
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if process.returncode != 0:
        error_output = process.stderr.strip() or process.stdout.strip()
        raise RuntimeError(
            "general-en batch alias review command failed: "
            f"{error_output or f'exit code {process.returncode}'}"
        )
    response = _parse_batch_review_response_json(process.stdout)
    if not isinstance(response, dict) or not isinstance(response.get("reviews"), list):
        raise RuntimeError(
            "general-en batch alias review command must return a JSON object with a reviews list."
        )
    expected_keys = {str(item["review_key"]) for item in batch}
    decisions: dict[str, RetainedAliasReviewDecision] = {}
    for raw_review in response["reviews"]:
        if not isinstance(raw_review, dict):
            raise RuntimeError("general-en batch alias review entries must be JSON objects.")
        review_key = str(raw_review.get("review_key", "")).strip()
        if not review_key:
            raise RuntimeError("general-en batch alias reviews must include review_key.")
        if review_key not in expected_keys:
            raise RuntimeError(
                f"general-en batch alias review returned an unexpected review_key: {review_key}"
            )
        if review_key in decisions:
            raise RuntimeError(
                f"general-en batch alias review returned a duplicate review_key: {review_key}"
            )
        decisions[review_key] = _coerce_review_decision(raw_review)
    missing_keys = expected_keys.difference(decisions)
    if missing_keys:
        raise RuntimeError(
            "general-en batch alias review omitted verdicts for review_key values: "
            + ", ".join(sorted(missing_keys))
        )
    return decisions


def _run_deterministic_common_english_batch_review(
    batch: list[dict[str, Any]],
) -> dict[str, RetainedAliasReviewDecision]:
    decisions: dict[str, RetainedAliasReviewDecision] = {}
    for alias in batch:
        review_key = str(alias["review_key"])
        reason = _classify_deterministic_common_english_retained_alias(
            text=str(alias.get("text", "")),
            label=str(alias.get("label", "")),
            canonical_text=str(alias.get("canonical_text", "")),
        )
        if reason is None:
            decisions[review_key] = RetainedAliasReviewDecision(
                decision="keep",
                reason_code="deterministic_common_english_keep",
                reason="Deterministic common-English review kept this retained alias.",
            )
            continue
        decisions[review_key] = RetainedAliasReviewDecision(
            decision="drop",
            reason_code=reason,
            reason=reason,
        )
    return decisions


def _parse_batch_review_response_json(stdout_text: str) -> dict[str, Any]:
    try:
        response = json.loads(stdout_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "general-en batch alias review command did not return JSON."
        ) from exc
    if (
        isinstance(response, dict)
        and isinstance(response.get("reviews"), list)
    ):
        return response
    if not isinstance(response, dict) or not isinstance(response.get("responses"), list):
        return response
    outputs = [
        str(item.get("output", "")).strip()
        for item in response["responses"]
        if isinstance(item, dict) and str(item.get("output", "")).strip()
    ]
    if len(outputs) != 1:
        raise RuntimeError(
            "general-en batch alias review mcoda response must contain exactly one non-empty output payload."
        )
    try:
        nested_response = json.loads(outputs[0])
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "general-en batch alias review mcoda output did not contain JSON."
        ) from exc
    return nested_response


def _coerce_review_decision(raw_review: dict[str, Any]) -> RetainedAliasReviewDecision:
    decision = str(raw_review.get("decision", "")).strip().casefold()
    if decision not in {"keep", "drop"}:
        raise RuntimeError("batch alias review decisions must be keep or drop.")
    reason_code = str(raw_review.get("reason_code", "")).strip()
    if not reason_code:
        reason_code = (
            "reviewed_specific_retained_alias"
            if decision == "keep"
            else "reviewed_generic_retained_alias"
        )
    reason = str(raw_review.get("reason", "")).strip() or reason_code
    return RetainedAliasReviewDecision(
        decision=decision,
        reason_code=reason_code,
        reason=reason,
    )


def _resolve_reviewer_name(
    command: str | None,
    *,
    review_mode: str,
) -> str:
    if review_mode == GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH:
        return GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH
    if not command:
        raise RuntimeError("reviewer command did not resolve to an executable.")
    tokens = shlex.split(command)
    if not tokens:
        raise RuntimeError("reviewer command did not resolve to an executable.")
    if (
        len(tokens) >= 3
        and Path(tokens[0]).name == "mcoda"
        and tokens[1] == "agent-run"
    ):
        return f"mcoda:{tokens[2]}"
    return tokens[0]


def _count_retained_aliases(
    path: Path,
    *,
    candidate_mode: str,
    chunk_size: int,
) -> int:
    if candidate_mode == GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_ALL:
        connection = sqlite3.connect(str(path))
        try:
            return int(connection.execute("SELECT COUNT(*) FROM retained_aliases").fetchone()[0])
        finally:
            connection.close()
    count = 0
    for alias in iter_retained_aliases_from_db(path, chunk_size=chunk_size):
        if _retained_alias_matches_candidate_mode(alias, candidate_mode=candidate_mode):
            count += 1
    return count


def _load_review_counts(review_db_path: Path) -> dict[str, int]:
    connection = _open_review_store(review_db_path)
    try:
        row = connection.execute(
            """
            SELECT
                COUNT(*) AS reviewed_count,
                SUM(CASE WHEN decision = 'drop' THEN 1 ELSE 0 END) AS drop_count,
                SUM(CASE WHEN decision = 'keep' THEN 1 ELSE 0 END) AS keep_count
            FROM alias_reviews
            """
        ).fetchone()
        reviewed_count = int(row[0] or 0)
        drop_count = int(row[1] or 0)
        keep_count = int(row[2] or 0)
        return {
            "reviewed": reviewed_count,
            "drop": drop_count,
            "keep": keep_count,
        }
    finally:
        connection.close()


def _load_drop_rows(review_db_path: Path) -> list[dict[str, str]]:
    connection = _open_review_store(review_db_path)
    try:
        rows = connection.execute(
            """
            SELECT
                review_key,
                display_text,
                label,
                canonical_text,
                entity_id,
                source_name,
                decision,
                reason_code,
                reason,
                reviewer_name,
                reviewed_at
            FROM alias_reviews
            WHERE decision = 'drop'
            ORDER BY lower(display_text) ASC, display_text ASC, lower(label) ASC, label ASC
            """
        ).fetchall()
    finally:
        connection.close()
    return [
        {
            "review_key": str(row[0]),
            "display_text": str(row[1]),
            "label": str(row[2]),
            "canonical_text": str(row[3]),
            "entity_id": str(row[4]),
            "source_name": str(row[5]),
            "decision": str(row[6]),
            "reason_code": str(row[7]),
            "reason": str(row[8]),
            "reviewer_name": str(row[9]),
            "reviewed_at": str(row[10]),
        }
        for row in rows
    ]


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()
