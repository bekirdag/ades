#!/usr/bin/env python3
"""Batch retained general-en aliases through a local reviewer command."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex

from ades.packs.general_alias_review import (
    DEFAULT_GENERAL_ALIAS_REVIEW_BATCH_TIMEOUT_SECONDS,
    DEFAULT_GENERAL_ALIAS_REVIEW_OUTPUT_ROOT,
    GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_ALL,
    GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_LIKELY_GENERIC,
    GENERAL_ALIAS_REVIEW_MODE_AI_COMMAND,
    GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH,
    default_general_alias_review_output_root,
    review_general_retained_aliases,
)


REVIEW_COMMAND_ENV = "ADES_GENERAL_ALIAS_BATCH_REVIEW_COMMAND"
MCODA_AGENT_ENV = "ADES_GENERAL_ALIAS_BATCH_REVIEW_MCODA_AGENT"
MCODA_BIN_ENV = "ADES_GENERAL_ALIAS_BATCH_REVIEW_MCODA_BIN"
DEFAULT_MCODA_BIN = "mcoda"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Review retained aliases from a general-en analysis.sqlite in 1000-item "
            "chunks through either a deterministic common-English filter or a local "
            "reviewer command."
        )
    )
    parser.add_argument("--analysis-db", required=True, help="Path to analysis.sqlite.")
    parser.add_argument(
        "--review-command",
        default=os.getenv(REVIEW_COMMAND_ENV, ""),
        help=(
            "Shell command that receives one batch JSON payload on stdin and returns "
            "batch keep/drop decisions on stdout. Defaults to "
            f"{REVIEW_COMMAND_ENV}."
        ),
    )
    parser.add_argument(
        "--mcoda-agent",
        default=os.getenv(MCODA_AGENT_ENV, ""),
        help=(
            "Optional local mcoda agent slug to run through "
            "`mcoda agent-run <slug> --stdin --json`. Defaults to "
            f"{MCODA_AGENT_ENV}."
        ),
    )
    parser.add_argument(
        "--mcoda-bin",
        default=os.getenv(MCODA_BIN_ENV, DEFAULT_MCODA_BIN),
        help=(
            "mcoda executable to use with --mcoda-agent. Defaults to "
            f"{MCODA_BIN_ENV} or `{DEFAULT_MCODA_BIN}`."
        ),
    )
    parser.add_argument(
        "--deterministic-common-english",
        action="store_true",
        help=(
            "Run the retained alias review deterministically with the built-in "
            "common-English filter powered by wordfreq and the repo's static "
            "generic-word heuristics."
        ),
    )
    parser.add_argument(
        "--candidate-mode",
        choices=[
            GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_ALL,
            GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_LIKELY_GENERIC,
        ],
        default=GENERAL_ALIAS_REVIEW_CANDIDATE_MODE_ALL,
        help=(
            "Which retained aliases to send to AI review. "
            "`all` reviews every retained alias; `likely_generic` only reviews "
            "aliases pre-flagged by the deterministic generic-risk classifier."
        ),
    )
    parser.add_argument(
        "--output-root",
        help=(
            "Directory for the review SQLite/JSONL/markdown artifacts. "
            f"Defaults under {DEFAULT_GENERAL_ALIAS_REVIEW_OUTPUT_ROOT}."
        ),
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Aliases per reviewer batch. Default: 1000.",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Optional cap on how many batches to review in this invocation.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_GENERAL_ALIAS_REVIEW_BATCH_TIMEOUT_SECONDS,
        help="Timeout for each reviewer batch command. Default: 120.",
    )
    return parser


def build_mcoda_review_command(*, mcoda_bin: str, mcoda_agent: str) -> str:
    if not mcoda_bin.strip():
        raise ValueError("mcoda_bin must not be empty.")
    if not mcoda_agent.strip():
        raise ValueError("mcoda_agent must not be empty.")
    return " ".join(
        [
            shlex.quote(mcoda_bin),
            "agent-run",
            shlex.quote(mcoda_agent),
            "--stdin",
            "--json",
        ]
    )


def resolve_review_command(args: argparse.Namespace) -> str:
    if bool(getattr(args, "deterministic_common_english", False)):
        return ""
    raw_review_command = str(args.review_command or "").strip()
    mcoda_agent = str(args.mcoda_agent or "").strip()
    if raw_review_command and mcoda_agent:
        raise ValueError("Pass either --review-command or --mcoda-agent, not both.")
    if raw_review_command:
        return raw_review_command
    if mcoda_agent:
        return build_mcoda_review_command(
            mcoda_bin=str(args.mcoda_bin or DEFAULT_MCODA_BIN).strip(),
            mcoda_agent=mcoda_agent,
        )
    raise ValueError(
        "A local reviewer is required. Pass --review-command, --mcoda-agent, "
        f"or set {REVIEW_COMMAND_ENV}/{MCODA_AGENT_ENV}."
    )


def resolve_review_mode(args: argparse.Namespace) -> str:
    if bool(getattr(args, "deterministic_common_english", False)):
        return GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH
    return GENERAL_ALIAS_REVIEW_MODE_AI_COMMAND


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    review_mode = resolve_review_mode(args)
    try:
        reviewer_command = resolve_review_command(args)
    except ValueError as exc:
        parser.error(str(exc))
    output_root = (
        Path(args.output_root).expanduser().resolve()
        if args.output_root
        else default_general_alias_review_output_root(args.analysis_db)
    )
    result = review_general_retained_aliases(
        args.analysis_db,
        reviewer_command=reviewer_command,
        review_mode=review_mode,
        output_root=output_root,
        candidate_mode=args.candidate_mode,
        chunk_size=args.chunk_size,
        max_batches=args.max_batches,
        timeout_seconds=args.timeout_seconds,
    )
    print(
        json.dumps(
            {
                "analysis_db_path": result.analysis_db_path,
                "reviewer_name": result.reviewer_name,
                "review_mode": review_mode,
                "reviewer_command": reviewer_command or None,
                "candidate_mode": result.candidate_mode,
                "candidate_total_count": result.candidate_total_count,
                "chunk_size": result.chunk_size,
                "batches_run": result.batches_run,
                "new_reviewed_count": result.new_reviewed_count,
                "cached_review_count": result.cached_review_count,
                "total_reviewed_count": result.total_reviewed_count,
                "proposed_drop_count": result.proposed_drop_count,
                "proposed_keep_count": result.proposed_keep_count,
                "remaining_unreviewed_count": result.remaining_unreviewed_count,
                "artifacts": {
                    "output_root": result.artifacts.output_root,
                    "review_db_path": result.artifacts.review_db_path,
                    "review_jsonl_path": result.artifacts.review_jsonl_path,
                    "review_markdown_path": result.artifacts.review_markdown_path,
                    "drop_jsonl_path": result.artifacts.drop_jsonl_path,
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
