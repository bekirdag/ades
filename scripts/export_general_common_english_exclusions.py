#!/usr/bin/env python3
"""Export deterministic general-en exclusions from a common-English word list."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path

from ades.packs.alias_analysis import (
    DEFAULT_GENERAL_COMMON_ENGLISH_WORDLIST_FILENAME,
    DEFAULT_GENERAL_COMMON_ENGLISH_WORDLIST_TOP_N,
    DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME,
    build_retained_alias_review_key,
    classify_exact_common_english_wordlist_alias,
    export_general_common_english_wordlist,
    iter_retained_aliases_from_db,
)

_DEFAULT_OUTPUT_ROOT = Path(
    "/mnt/githubActions/ades_big_data/general_en_alias_review/common-english-wordlist"
)
_DEFAULT_SOURCE_URL = "https://pypi.org/project/wordfreq/"
_DEFAULT_EXPLICIT_WORDS = ("sign", "new", "ai", "exclusive")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a reproducible English common-word list from wordfreq, save it "
            "to disk, and export exact-match general-en alias exclusions."
        )
    )
    parser.add_argument("--analysis-db", required=True, help="Path to analysis.sqlite.")
    parser.add_argument(
        "--output-root",
        default=str(_DEFAULT_OUTPUT_ROOT),
        help="Directory for the saved word list and exported exclusions.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_GENERAL_COMMON_ENGLISH_WORDLIST_TOP_N,
        help="Number of top English words to export from wordfreq.",
    )
    parser.add_argument(
        "--common-word-list-path",
        help=(
            "Optional explicit path for the saved common-English word list. "
            f"Defaults to <output-root>/{DEFAULT_GENERAL_COMMON_ENGLISH_WORDLIST_FILENAME}."
        ),
    )
    parser.add_argument(
        "--metadata-path",
        help="Optional explicit path for word-list metadata JSON.",
    )
    parser.add_argument(
        "--exclusion-path",
        help=(
            "Optional explicit JSONL output path. Defaults to <bundle-dir>/"
            f"{DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME} when --bundle-dir "
            "is provided, otherwise <output-root>/general-retained-alias-exclusions.jsonl."
        ),
    )
    parser.add_argument(
        "--base-exclusion-path",
        help="Optional existing exclusion JSONL to merge into the output before adding new drops.",
    )
    parser.add_argument(
        "--bundle-dir",
        help="Optional bundle dir to receive the exclusion JSONL directly.",
    )
    parser.add_argument(
        "--explicit-word",
        action="append",
        default=[],
        help=(
            "Extra exact words to drop regardless of wordfreq rank. "
            "Can be provided multiple times."
        ),
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Scan chunk size for retained aliases in analysis.sqlite.",
    )
    return parser


def _normalize_word(value: str) -> str:
    return str(value).strip().casefold()


def _load_word_list(path: Path) -> set[str]:
    return {
        normalized
        for normalized in (_normalize_word(line) for line in path.read_text(encoding="utf-8").splitlines())
        if normalized
    }


def _write_metadata(
    path: Path,
    *,
    word_list_path: Path,
    top_n: int,
    explicit_words: set[str],
) -> Path:
    payload = {
        "provider": "wordfreq",
        "language": "en",
        "top_n": top_n,
        "source_url": _DEFAULT_SOURCE_URL,
        "generated_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        "word_list_path": str(word_list_path),
        "explicit_words": sorted(explicit_words),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    analysis_db_path = Path(args.analysis_db).expanduser().resolve()
    if not analysis_db_path.exists():
        parser.error(f"analysis DB not found: {analysis_db_path}")

    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    word_list_path = (
        Path(args.common_word_list_path).expanduser().resolve()
        if args.common_word_list_path
        else output_root / DEFAULT_GENERAL_COMMON_ENGLISH_WORDLIST_FILENAME
    )
    metadata_path = (
        Path(args.metadata_path).expanduser().resolve()
        if args.metadata_path
        else output_root / f"{word_list_path.stem}.metadata.json"
    )
    bundle_word_list_path = (
        Path(args.bundle_dir).expanduser().resolve()
        / DEFAULT_GENERAL_COMMON_ENGLISH_WORDLIST_FILENAME
        if args.bundle_dir
        else None
    )
    if args.exclusion_path:
        exclusion_path = Path(args.exclusion_path).expanduser().resolve()
    elif args.bundle_dir:
        exclusion_path = (
            Path(args.bundle_dir).expanduser().resolve()
            / DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME
        )
    else:
        exclusion_path = output_root / DEFAULT_GENERAL_RETAINED_ALIAS_EXCLUSIONS_FILENAME

    explicit_words = {_normalize_word(word) for word in (_DEFAULT_EXPLICIT_WORDS + tuple(args.explicit_word))}

    export_general_common_english_wordlist(word_list_path, top_n=args.top_n)
    if bundle_word_list_path is not None and bundle_word_list_path != word_list_path:
        bundle_word_list_path.parent.mkdir(parents=True, exist_ok=True)
        bundle_word_list_path.write_text(
            word_list_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    _write_metadata(
        metadata_path,
        word_list_path=word_list_path,
        top_n=args.top_n,
        explicit_words=explicit_words,
    )
    common_words = _load_word_list(word_list_path)

    exclusion_path.parent.mkdir(parents=True, exist_ok=True)
    reviewer_name = f"common_english_word_list_top_{args.top_n}"
    reviewed_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    existing_rows: list[dict[str, object]] = []
    seen_review_keys: set[str] = set()
    if args.base_exclusion_path:
        base_exclusion_path = Path(args.base_exclusion_path).expanduser().resolve()
        if not base_exclusion_path.exists():
            parser.error(f"base exclusion file not found: {base_exclusion_path}")
        with base_exclusion_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                review_key = str(row.get("review_key", "")).strip()
                if not review_key or review_key in seen_review_keys:
                    continue
                existing_rows.append(row)
                seen_review_keys.add(review_key)
    else:
        base_exclusion_path = None
    excluded_count = 0
    with exclusion_path.open("w", encoding="utf-8") as handle:
        for row in existing_rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        for alias in iter_retained_aliases_from_db(
            analysis_db_path,
            chunk_size=max(1, int(args.chunk_size)),
        ):
            reason_code = classify_exact_common_english_wordlist_alias(
                text=str(alias.get("text", "")),
                label=str(alias.get("label", "")),
                common_words=common_words,
                explicit_words=explicit_words,
            )
            if reason_code is None:
                continue
            payload = {
                "canonical_text": str(alias.get("canonical_text", "")),
                "decision": "drop",
                "entity_id": str(alias.get("entity_id", "")),
                "label": str(alias.get("label", "")),
                "reason": reason_code,
                "reason_code": reason_code,
                "review_key": build_retained_alias_review_key(alias),
                "reviewed_at": reviewed_at,
                "reviewer_name": reviewer_name,
                "source_name": str(alias.get("source_name", "")),
                "text": str(alias.get("text", "")),
            }
            if payload["review_key"] in seen_review_keys:
                continue
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
            seen_review_keys.add(str(payload["review_key"]))
            excluded_count += 1

    print(
        json.dumps(
            {
                "analysis_db_path": str(analysis_db_path),
                "word_list_path": str(word_list_path),
                "bundle_word_list_path": (
                    str(bundle_word_list_path) if bundle_word_list_path else None
                ),
                "metadata_path": str(metadata_path),
                "exclusion_path": str(exclusion_path),
                "base_exclusion_path": str(base_exclusion_path) if base_exclusion_path else None,
                "top_n": args.top_n,
                "explicit_words": sorted(explicit_words),
                "existing_exclusion_count": len(existing_rows),
                "excluded_count": excluded_count,
                "total_exclusion_count": len(existing_rows) + excluded_count,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
