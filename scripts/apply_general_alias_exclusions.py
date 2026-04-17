#!/usr/bin/env python3
"""Export curated retained-alias drops into a deterministic exclusion file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ades.packs.alias_analysis import DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME
from ades.packs.general_alias_review import (
    DEFAULT_GENERAL_ALIAS_REVIEW_MARKDOWN_FILENAME,
    export_general_alias_drop_decisions,
    render_general_alias_review_markdown,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export reviewed drop decisions into a JSONL exclusion file that "
            "general-en pack generation can consume deterministically."
        )
    )
    parser.add_argument(
        "--review-db",
        required=True,
        help="Path to the review SQLite produced by review_general_retained_aliases.py.",
    )
    parser.add_argument(
        "--output-path",
        help=(
            "Target JSONL path. Defaults to <bundle-dir>/"
            f"{DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME} when --bundle-dir is "
            "provided, otherwise next to the review DB."
        ),
    )
    parser.add_argument(
        "--bundle-dir",
        help=(
            "Optional source bundle directory. When supplied, the exclusion file is "
            "written there so generate_pack_source can pick it up automatically."
        ),
    )
    parser.add_argument(
        "--review-markdown-path",
        help=(
            "Optional markdown report path. Defaults to next to the review DB as "
            f"{DEFAULT_GENERAL_ALIAS_REVIEW_MARKDOWN_FILENAME}."
        ),
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    review_db_path = Path(args.review_db).expanduser().resolve()
    if not review_db_path.exists():
        parser.error(f"Review DB not found: {review_db_path}")
    if args.output_path:
        output_path = Path(args.output_path).expanduser().resolve()
    elif args.bundle_dir:
        output_path = (
            Path(args.bundle_dir).expanduser().resolve()
            / DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME
        )
    else:
        output_path = review_db_path.with_name(DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME)
    review_markdown_path = (
        Path(args.review_markdown_path).expanduser().resolve()
        if args.review_markdown_path
        else review_db_path.with_name(DEFAULT_GENERAL_ALIAS_REVIEW_MARKDOWN_FILENAME)
    )
    exclusion_path = export_general_alias_drop_decisions(review_db_path, output_path)
    markdown_path = render_general_alias_review_markdown(review_db_path, review_markdown_path)
    print(
        json.dumps(
            {
                "review_db_path": str(review_db_path),
                "exclusion_path": str(exclusion_path),
                "review_markdown_path": str(markdown_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
