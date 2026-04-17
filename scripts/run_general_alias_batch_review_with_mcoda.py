#!/usr/bin/env python3
"""Normalize one retained-alias review batch through a local mcoda agent."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
from typing import Any, Callable

DEFAULT_MCODA_BIN = "mcoda"
DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_MAX_RETRIES = 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read one retained-alias review batch on stdin, send the whole chunk to a "
            "local mcoda agent in one prompt, and emit exact JSON with a reviews array."
        )
    )
    parser.add_argument(
        "--mcoda-agent",
        required=True,
        help="Local mcoda agent slug used for batch retained-alias review.",
    )
    parser.add_argument(
        "--mcoda-bin",
        default=os.getenv("ADES_GENERAL_ALIAS_BATCH_REVIEW_MCODA_BIN", DEFAULT_MCODA_BIN),
        help=f"mcoda executable to use. Default: {DEFAULT_MCODA_BIN}.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Per-batch mcoda timeout. Default: {DEFAULT_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help="Repair retries when the batch output is not valid JSON. Default: 1.",
    )
    return parser


def _load_batch_payload(stdin_text: str) -> dict[str, Any]:
    try:
        payload = json.loads(stdin_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("stdin did not contain JSON.") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("stdin JSON must be an object.")
    aliases = payload.get("aliases")
    if not isinstance(aliases, list) or not aliases:
        raise RuntimeError("stdin JSON must include a non-empty aliases list.")
    return payload


def _batch_alias_payload(alias: dict[str, Any]) -> dict[str, str]:
    return {
        "review_key": str(alias.get("review_key", "")),
        "text": str(alias.get("text", "")),
        "label": str(alias.get("label", "")),
        "canonical_text": str(alias.get("canonical_text", "")),
    }


def _batch_alias_line_text(alias: dict[str, Any]) -> str:
    text = str(alias.get("text", "")).strip()
    canonical_text = str(alias.get("canonical_text", "")).strip()
    value = text or canonical_text
    return re.sub(r"\s+", " ", value).strip()


def build_batch_review_prompt(aliases: list[dict[str, Any]]) -> str:
    entity_lines = [_batch_alias_line_text(alias) for alias in aliases]
    return "\n".join(
        [
            "You are reviewing one chunk of retained aliases for the `general-en` pack.",
            "Find only the aliases that should be removed because they are too generic.",
            "",
            "Policy:",
            "- Drop aliases that are overly generic, common-English, weak, or likely to create false positives in ordinary text.",
            "- Keep aliases that are specific, distinctive, and useful at runtime.",
            "- If uncertain and not clearly generic, omit the alias from the drop list.",
            "",
            "Return exactly one JSON object with this schema and nothing else:",
            '{"drops":[{"line":0,"reason_code":"short_snake_case","reason":"short reason"}]}',
            "",
            "Rules:",
            "- The entity list below is 1-based by line number.",
            "- Include only aliases that should be dropped.",
            "- Do not include kept aliases.",
            "- Do not output markdown, code fences, commentary, or thinking text.",
            "",
            "Entities:",
            "\n".join(entity_lines),
        ]
    )


def build_batch_repair_prompt(
    aliases: list[dict[str, Any]],
    raw_output: str,
) -> str:
    entity_lines = [_batch_alias_line_text(alias) for alias in aliases]
    return "\n".join(
        [
            "Your previous answer was not valid JSON.",
            "Read the chunk again and reply with exactly one JSON object and nothing else.",
            '{"drops":[{"line":0,"reason_code":"short_snake_case","reason":"short reason"}]}',
            "",
            "Return only aliases that should be dropped. Omit all kept aliases.",
            "The entity list below is 1-based by line number.",
            "",
            "Entities:",
            "\n".join(entity_lines),
            "",
            "Previous invalid answer:",
            raw_output,
        ]
    )


def _extract_mcoda_output(stdout_text: str) -> str:
    try:
        response = json.loads(stdout_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("mcoda agent-run did not return JSON.") from exc
    if not isinstance(response, dict) or not isinstance(response.get("responses"), list):
        raise RuntimeError("mcoda agent-run JSON did not include responses.")
    outputs = [
        str(item.get("output", "")).strip()
        for item in response["responses"]
        if isinstance(item, dict) and str(item.get("output", "")).strip()
    ]
    if len(outputs) != 1:
        raise RuntimeError(
            "mcoda agent-run JSON must contain exactly one non-empty output."
        )
    return outputs[0]


def _strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if len(lines) >= 2 and lines[0].startswith("```") and lines[-1].startswith("```"):
        return "\n".join(lines[1:-1]).strip()
    return stripped


def _find_json_objects(text: str) -> list[str]:
    in_string = False
    escaped = False
    depth = 0
    start_index: int | None = None
    objects: list[str] = []
    for index, char in enumerate(text):
        if start_index is None:
            if char == "{":
                start_index = index
                depth = 1
            continue
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char == "{":
            depth += 1
            continue
        if char == "}":
            depth -= 1
            if depth == 0:
                objects.append(text[start_index : index + 1])
                start_index = None
    return objects


def _coerce_drop_review(
    raw_review: dict[str, Any],
    *,
    expected_review_keys: set[str],
    expected_line_numbers: set[int],
    review_key_by_line_number: dict[int, str],
) -> dict[str, str]:
    review_key = str(raw_review.get("review_key", "")).strip()
    if review_key:
        if review_key not in expected_review_keys:
            raise RuntimeError(f"batch review returned unexpected review_key: {review_key}")
    else:
        line_value = raw_review.get("line", raw_review.get("line_number", ""))
        try:
            line_number = int(str(line_value).strip())
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "batch drop entries must include review_key or integer line."
            ) from exc
        if line_number not in expected_line_numbers:
            raise RuntimeError(
                f"batch review returned unexpected line number: {line_number}"
            )
        review_key = review_key_by_line_number[line_number]
    reason_code = str(raw_review.get("reason_code", "")).strip()
    if not reason_code:
        reason_code = "generic_retained_alias_from_batch_review"
    reason = str(raw_review.get("reason", "")).strip() or reason_code
    return {
        "review_key": review_key,
        "decision": "drop",
        "reason_code": reason_code,
        "reason": reason,
    }


def parse_batch_review_text(
    raw_output: str,
    *,
    expected_review_keys: set[str],
    review_key_by_line_number: dict[int, str],
) -> list[dict[str, str]]:
    expected_line_numbers = set(review_key_by_line_number)
    candidates = [
        raw_output.strip(),
        _strip_code_fence(raw_output),
    ]
    candidates.extend(_find_json_objects(raw_output))
    seen: set[str] = set()
    last_error: RuntimeError | None = None
    for candidate in candidates:
        candidate = candidate.strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, dict):
            continue
        if isinstance(parsed.get("drops"), list):
            try:
                seen_keys: set[str] = set()
                drops: list[dict[str, str]] = []
                for raw_review in parsed["drops"]:
                    if not isinstance(raw_review, dict):
                        continue
                    review = _coerce_drop_review(
                        raw_review,
                        expected_review_keys=expected_review_keys,
                        expected_line_numbers=expected_line_numbers,
                        review_key_by_line_number=review_key_by_line_number,
                    )
                    if review["review_key"] in seen_keys:
                        raise RuntimeError(
                            "batch review returned duplicate review_key: "
                            f"{review['review_key']}"
                        )
                    seen_keys.add(review["review_key"])
                    drops.append(review)
                return drops
            except RuntimeError as exc:
                last_error = exc
                continue
        if isinstance(parsed.get("reviews"), list):
            try:
                seen_keys: set[str] = set()
                reviews: list[dict[str, str]] = []
                for raw_review in parsed["reviews"]:
                    if not isinstance(raw_review, dict):
                        continue
                    review_key = str(raw_review.get("review_key", "")).strip()
                    if not review_key:
                        continue
                    if review_key not in expected_review_keys:
                        raise RuntimeError(
                            f"batch review returned unexpected review_key: {review_key}"
                        )
                    if review_key in seen_keys:
                        raise RuntimeError(
                            f"batch review returned duplicate review_key: {review_key}"
                        )
                    decision = str(raw_review.get("decision", "")).strip().casefold()
                    if decision not in {"keep", "drop"}:
                        continue
                    reason_code = str(raw_review.get("reason_code", "")).strip() or (
                        "specific_retained_alias"
                        if decision == "keep"
                        else "generic_retained_alias_from_batch_review"
                    )
                    reason = str(raw_review.get("reason", "")).strip() or reason_code
                    seen_keys.add(review_key)
                    reviews.append(
                        {
                            "review_key": review_key,
                            "decision": decision,
                            "reason_code": reason_code,
                            "reason": reason,
                        }
                    )
                return reviews
            except RuntimeError as exc:
                last_error = exc
                continue
    if last_error is not None:
        raise last_error
    raise RuntimeError(
        "mcoda batch alias review output did not contain a valid batch JSON object."
    )


def run_mcoda_prompt(
    *,
    mcoda_bin: str,
    mcoda_agent: str,
    prompt: str,
    timeout_seconds: float,
) -> str:
    process = subprocess.run(
        [mcoda_bin, "agent-run", mcoda_agent, "--stdin", "--json"],
        input=prompt,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if process.returncode != 0:
        error_output = process.stderr.strip() or process.stdout.strip()
        raise RuntimeError(
            "mcoda alias review command failed: "
            f"{error_output or f'exit code {process.returncode}'}"
        )
    return _extract_mcoda_output(process.stdout)


def review_alias_batch(
    aliases: list[dict[str, Any]],
    *,
    runner: Callable[[str], str],
    max_retries: int,
) -> list[dict[str, str]]:
    review_key_by_line_number = {
        index: str(alias.get("review_key", "")).strip()
        for index, alias in enumerate(aliases, start=1)
        if str(alias.get("review_key", "")).strip()
    }
    expected_review_keys = {
        str(alias.get("review_key", "")).strip()
        for alias in aliases
        if str(alias.get("review_key", "")).strip()
    }
    prompt = build_batch_review_prompt(aliases)
    last_error: Exception | None = None
    explicit_reviews: list[dict[str, str]] | None = None
    for attempt in range(max_retries + 1):
        raw_output = runner(prompt)
        try:
            explicit_reviews = parse_batch_review_text(
                raw_output,
                expected_review_keys=expected_review_keys,
                review_key_by_line_number=review_key_by_line_number,
            )
            break
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            print(
                (
                    "mcoda batch alias review normalization retry "
                    f"{attempt + 1}/{max_retries + 1}: {exc}. "
                    f"Raw output: {raw_output[:1000]!r}"
                ),
                file=sys.stderr,
            )
            if attempt >= max_retries:
                raise RuntimeError(
                    "failed to normalize batch review output: "
                    f"{exc}"
                ) from exc
            prompt = build_batch_repair_prompt(aliases, raw_output)
    if explicit_reviews is None:
        if last_error is not None:
            raise last_error
        raise RuntimeError("batch review did not produce any result.")

    decisions_by_key = {
        review["review_key"]: review for review in explicit_reviews
    }
    reviews: list[dict[str, str]] = []
    for alias in aliases:
        review_key = str(alias.get("review_key", "")).strip()
        if review_key in decisions_by_key:
            reviews.append(decisions_by_key[review_key])
            continue
        reviews.append(
            {
                "review_key": review_key,
                "decision": "keep",
                "reason_code": "implicit_keep_from_batch_review",
                "reason": "Not listed among generic aliases in chunk review.",
            }
        )
    return reviews


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive.")
    if args.max_retries < 0:
        parser.error("--max-retries must be zero or positive.")
    payload = _load_batch_payload(sys.stdin.read())
    aliases = payload["aliases"]

    def _runner(prompt: str) -> str:
        return run_mcoda_prompt(
            mcoda_bin=args.mcoda_bin,
            mcoda_agent=args.mcoda_agent,
            prompt=prompt,
            timeout_seconds=args.timeout_seconds,
        )

    reviews = review_alias_batch(
        aliases,
        runner=_runner,
        max_retries=args.max_retries,
    )
    print(json.dumps({"reviews": reviews}, ensure_ascii=False))


if __name__ == "__main__":
    main()
