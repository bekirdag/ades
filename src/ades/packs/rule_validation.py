"""Validation and compilation helpers for deterministic regex rules."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Pattern

_MAX_RULE_PATTERN_LENGTH = 1_024
_UNSAFE_MARKERS = (
    r"\1",
    r"\2",
    r"\3",
    r"\g<",
    r"(?<=",
    r"(?<!",
    r"(?P=",
    r"(?(",
)


@dataclass(frozen=True)
class ReviewedRule:
    """Validated runtime regex rule."""

    pattern: str
    compiled: Pattern[str]
    engine: str
    reviewed: bool = True


def compile_reviewed_rule(pattern: str) -> ReviewedRule:
    """Validate and compile one deterministic rule pattern."""

    validate_rule_pattern(pattern)
    try:
        import re2  # type: ignore
    except Exception:
        re2 = None

    if re2 is not None and _is_re2_compatible(pattern):
        try:
            return ReviewedRule(
                pattern=pattern,
                compiled=re2.compile(pattern, re.IGNORECASE),
                engine="re2",
            )
        except Exception:
            pass

    return ReviewedRule(
        pattern=pattern,
        compiled=re.compile(pattern, re.IGNORECASE),
        engine="python-re-reviewed",
    )


def validate_rule_pattern(pattern: str) -> None:
    """Raise when a rule pattern is not safe for reviewed runtime use."""

    cleaned = str(pattern or "").strip()
    if not cleaned:
        raise ValueError("Rule pattern must not be empty.")
    if len(cleaned) > _MAX_RULE_PATTERN_LENGTH:
        raise ValueError(
            f"Rule pattern exceeds maximum reviewed length {_MAX_RULE_PATTERN_LENGTH}."
        )
    for marker in _UNSAFE_MARKERS:
        if marker in cleaned:
            raise ValueError(
                f"Rule pattern uses an unsupported high-risk construct: {marker}"
            )
    if _contains_nested_repeat(cleaned):
        raise ValueError("Rule pattern uses nested repeats and is not production-safe.")
    try:
        re.compile(cleaned, re.IGNORECASE)
    except re.error as exc:
        raise ValueError(f"Invalid regex pattern: {exc}") from exc


def validate_rules_payload(patterns: list[dict[str, str]]) -> None:
    """Validate every regex rule record in a pack payload."""

    for item in patterns:
        validate_rule_pattern(str(item.get("pattern", "")))


def _is_re2_compatible(pattern: str) -> bool:
    return not any(marker in pattern for marker in _UNSAFE_MARKERS)


def _contains_nested_repeat(pattern: str) -> bool:
    _, _, found_nested = _scan_sequence(pattern, 0)
    return found_nested


def _scan_sequence(
    pattern: str,
    index: int,
    *,
    terminator: str | None = None,
) -> tuple[int, bool, bool]:
    contains_repeat = False
    found_nested = False
    last_atom_contains_repeat = False
    has_last_atom = False

    while index < len(pattern):
        char = pattern[index]
        if terminator is not None and char == terminator:
            return index + 1, contains_repeat, found_nested
        if char == "\\":
            index += 2
            last_atom_contains_repeat = False
            has_last_atom = True
            continue
        if char == "[":
            index = _skip_character_class(pattern, index + 1)
            last_atom_contains_repeat = False
            has_last_atom = True
            continue
        if char == "(":
            index, group_contains_repeat, group_found_nested = _scan_group(pattern, index)
            contains_repeat = contains_repeat or group_contains_repeat
            found_nested = found_nested or group_found_nested
            last_atom_contains_repeat = group_contains_repeat
            has_last_atom = True
            continue
        if char in "*+?":
            repeats_many = char in {"*", "+"}
            if has_last_atom and repeats_many and last_atom_contains_repeat:
                found_nested = True
            if has_last_atom and repeats_many:
                contains_repeat = True
                last_atom_contains_repeat = True
            index += 1
            continue
        if char == "{":
            if not has_last_atom:
                index += 1
                continue
            quantifier_end, repeats_many = _consume_braced_quantifier(pattern, index)
            if quantifier_end is None:
                index += 1
                last_atom_contains_repeat = False
                has_last_atom = True
                continue
            if repeats_many and last_atom_contains_repeat:
                found_nested = True
            if repeats_many:
                contains_repeat = True
                last_atom_contains_repeat = True
            index = quantifier_end
            continue
        if char == "|":
            index += 1
            last_atom_contains_repeat = False
            has_last_atom = False
            continue
        index += 1
        last_atom_contains_repeat = False
        has_last_atom = True

    return index, contains_repeat, found_nested


def _scan_group(pattern: str, index: int) -> tuple[int, bool, bool]:
    index += 1
    if index < len(pattern) and pattern[index] == "?":
        if pattern.startswith("?:", index):
            index += 2
        elif pattern.startswith("?=", index) or pattern.startswith("?!", index):
            index += 2
        elif pattern.startswith("?P<", index):
            name_end = pattern.find(">", index + 3)
            if name_end == -1:
                return len(pattern), False, False
            index = name_end + 1
        else:
            index += 1
    return _scan_sequence(pattern, index, terminator=")")


def _skip_character_class(pattern: str, index: int) -> int:
    while index < len(pattern):
        char = pattern[index]
        if char == "\\":
            index += 2
            continue
        if char == "]":
            return index + 1
        index += 1
    return index


def _consume_braced_quantifier(pattern: str, index: int) -> tuple[int | None, bool]:
    end = pattern.find("}", index + 1)
    if end == -1:
        return None, False
    body = pattern[index + 1 : end].strip()
    if not body:
        return None, False
    if "," not in body:
        if not body.isdigit():
            return None, False
        return end + 1, int(body) > 1
    minimum, maximum = body.split(",", 1)
    minimum = minimum.strip()
    maximum = maximum.strip()
    if minimum and not minimum.isdigit():
        return None, False
    if maximum and not maximum.isdigit():
        return None, False
    if maximum == "":
        return end + 1, True
    return end + 1, int(maximum) > 1
