"""Pack-aware local entity extraction."""

from __future__ import annotations

from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass
import os
from pathlib import Path
import re
from time import perf_counter

from ..packs.registry import PackRegistry
from ..packs.runtime import PackRuntime, RuleDefinition, load_pack_runtime
from .hybrid import ProposalProvider, ProposalSpan, get_proposal_spans
from ..runtime_matcher import (
    MatcherEntryPayload,
    find_exact_match_candidates,
    load_runtime_matcher,
)
from ..service.models import (
    EntityLink,
    EntityMatch,
    EntityProvenance,
    TagDebug,
    TagMetrics,
    TagResponse,
    TagSpanDecision,
    TopicMatch,
)
from ..text_processing import NormalizedText, normalize_lookup_text, normalize_text, segment_text, token_count
from ..version import __version__

TOKEN_RE = re.compile(r"\b[\w][\w.+&/-]*\b")
LINK_VALUE_RE = re.compile(r"[^a-z0-9]+")
MAX_ALIAS_WINDOW_TOKENS = 8
MAX_INPUT_BYTES = 1_000_000
MAX_SEGMENT_CHARS = 4_000
MAX_SEGMENT_TOKENS = 800
MAX_RULE_MATCHES_PER_RULE = 256
TAG_SCHEMA_VERSION = 2
_CALENDAR_SINGLE_TOKEN_WORDS = {
    "april",
    "august",
    "december",
    "february",
    "friday",
    "january",
    "july",
    "june",
    "march",
    "monday",
    "november",
    "october",
    "saturday",
    "september",
    "sunday",
    "thursday",
    "tuesday",
    "wednesday",
}
_GENERIC_PHRASE_LEADS = {
    "a",
    "an",
    "any",
    "each",
    "every",
    "he",
    "her",
    "his",
    "i",
    "it",
    "its",
    "my",
    "our",
    "she",
    "some",
    "that",
    "the",
    "their",
    "these",
    "they",
    "this",
    "those",
    "we",
    "you",
    "your",
}
_AUXILIARY_LOOKUP_TOKENS = {
    "am",
    "are",
    "be",
    "been",
    "being",
    "can",
    "could",
    "did",
    "do",
    "does",
    "had",
    "has",
    "have",
    "is",
    "may",
    "might",
    "must",
    "shall",
    "should",
    "was",
    "were",
    "will",
    "would",
}
_PERSON_NAME_PARTICLES = {
    "al",
    "ap",
    "ben",
    "bin",
    "da",
    "dal",
    "de",
    "del",
    "della",
    "der",
    "di",
    "do",
    "dos",
    "du",
    "el",
    "ibn",
    "la",
    "le",
    "st",
    "van",
    "von",
}
_TRAILING_FUNCTION_WORDS = _GENERIC_PHRASE_LEADS | {
    "about",
    "at",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "to",
    "with",
}
_GENERIC_SINGLE_TOKEN_HEADS = {
    "bank",
    "city",
    "company",
    "department",
    "founder",
    "fund",
    "market",
    "product",
    "report",
    "stock",
    "university",
    "world",
}
_ACRONYM_CONNECTOR_WORDS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "the",
    "to",
    "with",
}
_ACRONYM_AFTER_ENTITY_RE = re.compile(r"^\s*\((?P<acronym>[A-Z][A-Z0-9.&/-]{1,9})\)")
_ACRONYM_BEFORE_ENTITY_RE = re.compile(r"(?P<acronym>[A-Z][A-Z0-9.&/-]{1,9})\s*\(\s*$")
_MAX_ACRONYM_DEFINITION_WINDOW = 16

_LANE_PRIORITY = {
    "deterministic_rule": 3,
    "deterministic_alias": 2,
    "document_acronym_backfill": 2,
    "proposal_linked": 1,
}


@dataclass(frozen=True)
class ExtractedCandidate:
    """One extracted span candidate before final document resolution."""

    entity: EntityMatch
    domain: str
    lane: str
    normalized_start: int
    normalized_end: int


@dataclass
class DocumentContextSignals:
    """Bounded document-level context gathered from resolved local entities."""

    anchor_counts: Counter[str]
    pack_counts: Counter[str]
    domain_counts: Counter[str]
    label_counts: Counter[str]


@dataclass(frozen=True)
class DocumentAcronymDefinition:
    acronym_text: str
    normalized_acronym: str
    label: str
    start: int
    end: int
    anchor: ExtractedCandidate


def _language_from_pack(pack: str) -> str:
    if "-" not in pack:
        return "unknown"
    return pack.rsplit("-", 1)[-1]


def _iter_token_spans(text: str) -> list[tuple[int, int]]:
    return [(match.start(), match.end()) for match in TOKEN_RE.finditer(text)]


def _iter_candidate_windows(
    text: str,
    *,
    max_tokens: int = MAX_ALIAS_WINDOW_TOKENS,
) -> list[tuple[int, int, str]]:
    spans = _iter_token_spans(text)
    if not spans:
        return []

    windows: list[tuple[int, int, str]] = []
    max_window = min(max_tokens, len(spans))
    for window_size in range(max_window, 0, -1):
        for start_index in range(0, len(spans) - window_size + 1):
            start = spans[start_index][0]
            end = spans[start_index + window_size - 1][1]
            windows.append((start, end, text[start:end]))
    return windows


def _normalize_link_value(value: str) -> str:
    normalized = LINK_VALUE_RE.sub("-", value.casefold()).strip("-")
    return normalized or "value"


def _build_entity_link(
    *,
    provider: str,
    pack_id: str,
    label: str,
    canonical_text: str,
    entity_id: str | None = None,
) -> EntityLink:
    if entity_id:
        return EntityLink(
            entity_id=entity_id,
            canonical_text=canonical_text,
            provider=provider,
        )
    provider_id = provider.replace(".", "_")
    normalized_value = _normalize_link_value(canonical_text)
    return EntityLink(
        entity_id=f"ades:{provider_id}:{pack_id}:{label.casefold()}:{normalized_value}",
        canonical_text=canonical_text,
        provider=provider,
    )


def _build_provenance(
    *,
    match_kind: str,
    match_path: str,
    match_source: str,
    source_pack: str,
    source_domain: str,
    lane: str,
    model_name: str | None = None,
    model_version: str | None = None,
) -> EntityProvenance:
    return EntityProvenance(
        match_kind=match_kind,
        match_path=match_path,
        match_source=match_source,
        source_pack=source_pack,
        source_domain=source_domain,
        lane=lane,
        model_name=model_name,
        model_version=model_version,
    )


def _extract_rule_entities(
    segment_text_value: str,
    *,
    segment_start: int,
    normalized_input: NormalizedText,
    rules: list[RuleDefinition],
    warnings: list[str],
) -> list[ExtractedCandidate]:
    extracted: list[ExtractedCandidate] = []
    warned_limits: set[tuple[str, str]] = set()
    for rule in rules:
        match_count = 0
        for match in rule.compiled_pattern.finditer(segment_text_value):
            match_count += 1
            if match_count > MAX_RULE_MATCHES_PER_RULE:
                warning_key = (rule.source_pack, rule.name)
                if warning_key not in warned_limits:
                    warnings.append(f"rule_match_limit:{rule.source_pack}:{rule.name}")
                    warned_limits.add(warning_key)
                break
            normalized_start = segment_start + match.start()
            normalized_end = segment_start + match.end()
            raw_start, raw_end = normalized_input.raw_span(normalized_start, normalized_end)
            matched_text = segment_text_value[match.start():match.end()]
            extracted.append(
                ExtractedCandidate(
                    entity=EntityMatch(
                        text=matched_text,
                        label=rule.label,
                        start=raw_start,
                        end=raw_end,
                        confidence=0.85,
                        provenance=_build_provenance(
                            match_kind="rule",
                            match_path="rule.regex",
                            match_source=rule.name,
                            source_pack=rule.source_pack,
                            source_domain=rule.source_domain,
                            lane="deterministic_rule",
                            model_name=rule.engine,
                        ),
                        link=_build_entity_link(
                            provider="rule.regex",
                            pack_id=rule.source_pack,
                            label=rule.label,
                            canonical_text=matched_text,
                        ),
                    ),
                    domain=rule.source_domain,
                    lane="deterministic_rule",
                    normalized_start=normalized_start,
                    normalized_end=normalized_end,
                )
            )
    return extracted


def _extract_lookup_alias_entities(
    segment_text_value: str,
    *,
    segment_start: int,
    normalized_input: NormalizedText,
    registry: PackRegistry,
    runtime: PackRuntime,
) -> list[ExtractedCandidate]:
    allowed_pack_ids = set(runtime.pack_chain)
    matcher_pack_ids = {matcher.pack_id for matcher in runtime.matchers}
    lookup_cache: dict[tuple[str, str], list[dict[str, str | float | bool | None]]] = {}
    extracted: list[ExtractedCandidate] = []

    if runtime.matchers:
        extracted.extend(
            _extract_lookup_alias_entities_with_matchers(
                segment_text_value,
                segment_start=segment_start,
                normalized_input=normalized_input,
                registry=registry,
                runtime=runtime,
                lookup_cache=lookup_cache,
            )
        )

    fallback_pack_ids = allowed_pack_ids - matcher_pack_ids
    if fallback_pack_ids:
        extracted.extend(
            _extract_lookup_alias_entities_with_windows(
                segment_text_value,
                segment_start=segment_start,
                normalized_input=normalized_input,
                registry=registry,
                allowed_pack_ids=fallback_pack_ids,
                lookup_cache=lookup_cache,
            )
        )
    return extracted


def _extract_lookup_alias_entities_with_matchers(
    segment_text_value: str,
    *,
    segment_start: int,
    normalized_input: NormalizedText,
    registry: PackRegistry,
    runtime: PackRuntime,
    lookup_cache: dict[tuple[str, str], list[dict[str, str | float | bool | None]]],
) -> list[ExtractedCandidate]:
    extracted: list[ExtractedCandidate] = []
    token_starts, token_ends = _build_token_boundary_maps(segment_text_value)
    for matcher in runtime.matchers:
        compiled_matcher = load_runtime_matcher(
            matcher.artifact_path,
            matcher.entries_path,
        )
        for candidate in find_exact_match_candidates(segment_text_value, compiled_matcher):
            start = candidate.start
            end = candidate.end
            if not _is_token_window_span(start, end, token_starts=token_starts, token_ends=token_ends):
                continue
            candidate_text = segment_text_value[start:end]
            if compiled_matcher.entry_payloads and candidate.entry_indices:
                entry_payloads = [
                    compiled_matcher.entry_payloads[index]
                    for index in candidate.entry_indices
                    if 0 <= index < len(compiled_matcher.entry_payloads)
                ]
                extracted.extend(
                    _build_lookup_alias_entities_from_matcher_entries(
                        entries=entry_payloads,
                        pack_id=matcher.pack_id,
                        runtime_domain=runtime.domain,
                        segment_text_value=segment_text_value,
                        matched_text=candidate_text,
                        start=start,
                        end=end,
                        segment_start=segment_start,
                        normalized_input=normalized_input,
                    )
                )
                continue
            candidates = _lookup_exact_alias_candidates(
                registry=registry,
                pack_id=matcher.pack_id,
                candidate_text=candidate_text,
                lookup_cache=lookup_cache,
            )
            extracted.extend(
                _build_lookup_alias_entities(
                    candidates=candidates,
                    segment_text_value=segment_text_value,
                    matched_text=candidate_text,
                    start=start,
                    end=end,
                    segment_start=segment_start,
                    normalized_input=normalized_input,
                )
            )
    return extracted


def _build_lookup_alias_entities_from_matcher_entries(
    *,
    entries: list[MatcherEntryPayload],
    pack_id: str,
    runtime_domain: str,
    segment_text_value: str,
    matched_text: str,
    start: int,
    end: int,
    segment_start: int,
    normalized_input: NormalizedText,
) -> list[ExtractedCandidate]:
    extracted: list[ExtractedCandidate] = []
    for entry in entries:
        entry_domain = entry.source_domain or runtime_domain
        if _should_skip_single_token_lookup_candidate(
            matched_text=matched_text,
            candidate_value=entry.text,
            canonical_text=entry.canonical_text,
            candidate_label=entry.label,
            candidate_domain=entry_domain,
            segment_text=segment_text_value,
            start=start,
            end=end,
        ):
            continue
        if _should_skip_multi_token_lookup_candidate(
            matched_text=matched_text,
            candidate_value=entry.text,
            canonical_text=entry.canonical_text,
            candidate_label=entry.label,
            candidate_domain=entry_domain,
        ):
            continue
        normalized_start = segment_start + start
        normalized_end = segment_start + end
        raw_start, raw_end = normalized_input.raw_span(normalized_start, normalized_end)
        extracted.append(
            ExtractedCandidate(
                entity=EntityMatch(
                    text=matched_text,
                    label=entry.label,
                    start=raw_start,
                    end=raw_end,
                    confidence=entry.alias_score,
                    provenance=_build_provenance(
                        match_kind="alias",
                        match_path="lookup.alias.exact",
                        match_source=entry.text,
                        source_pack=pack_id,
                        source_domain=entry_domain,
                        lane="deterministic_alias",
                    ),
                    link=_build_entity_link(
                        provider="lookup.alias.exact",
                        pack_id=pack_id,
                        label=entry.label,
                        canonical_text=entry.canonical_text,
                        entity_id=entry.entity_id or None,
                    ),
                ),
                domain=entry_domain,
                lane="deterministic_alias",
                normalized_start=normalized_start,
                normalized_end=normalized_end,
            )
        )
    return extracted


def _extract_lookup_alias_entities_with_windows(
    segment_text_value: str,
    *,
    segment_start: int,
    normalized_input: NormalizedText,
    registry: PackRegistry,
    allowed_pack_ids: set[str],
    lookup_cache: dict[tuple[str, str], list[dict[str, str | float | bool | None]]],
) -> list[ExtractedCandidate]:
    extracted: list[ExtractedCandidate] = []
    pack_id_list = sorted(allowed_pack_ids)
    for start, end, candidate_text in _iter_candidate_windows(segment_text_value):
        if not pack_id_list:
            continue
        if len(pack_id_list) == 1:
            candidates = _lookup_exact_alias_candidates(
                registry=registry,
                pack_id=pack_id_list[0],
                candidate_text=candidate_text,
                lookup_cache=lookup_cache,
            )
        else:
            cache_key = ("", normalize_lookup_text(candidate_text))
            candidates = lookup_cache.get(cache_key)
            if candidates is None:
                candidates = [
                    candidate
                    for candidate in registry.lookup_candidates(
                        candidate_text,
                        exact_alias=True,
                        active_only=True,
                        limit=50,
                    )
                    if candidate["kind"] == "alias" and candidate["pack_id"] in allowed_pack_ids
                ]
                lookup_cache[cache_key] = candidates
        extracted.extend(
            _build_lookup_alias_entities(
                candidates=candidates,
                segment_text_value=segment_text_value,
                matched_text=segment_text_value[start:end],
                start=start,
                end=end,
                segment_start=segment_start,
                normalized_input=normalized_input,
            )
        )
    return extracted


def _build_token_boundary_maps(text: str) -> tuple[set[int], set[int]]:
    token_starts: set[int] = set()
    token_ends: set[int] = set()
    for start, end in _iter_token_spans(text):
        token_starts.add(start)
        token_ends.add(end)
    return token_starts, token_ends


def _is_token_window_span(
    start: int,
    end: int,
    *,
    token_starts: set[int],
    token_ends: set[int],
) -> bool:
    return start in token_starts and end in token_ends and end > start


def _lookup_exact_alias_candidates(
    *,
    registry: PackRegistry,
    pack_id: str,
    candidate_text: str,
    lookup_cache: dict[tuple[str, str], list[dict[str, str | float | bool | None]]],
) -> list[dict[str, str | float | bool | None]]:
    cache_key = (pack_id, normalize_lookup_text(candidate_text))
    candidates = lookup_cache.get(cache_key)
    if candidates is not None:
        return candidates
    candidates = [
        candidate
        for candidate in registry.lookup_candidates(
            candidate_text,
            pack_id=pack_id,
            exact_alias=True,
            active_only=True,
            limit=50,
        )
        if candidate["kind"] == "alias" and candidate["pack_id"] == pack_id
    ]
    lookup_cache[cache_key] = candidates
    return candidates


def _build_lookup_alias_entities(
    *,
    candidates: list[dict[str, str | float | bool | None]],
    segment_text_value: str,
    matched_text: str,
    start: int,
    end: int,
    segment_start: int,
    normalized_input: NormalizedText,
) -> list[ExtractedCandidate]:
    extracted: list[ExtractedCandidate] = []
    for candidate in candidates:
        candidate_value = str(candidate["value"])
        candidate_label = str(candidate["label"])
        candidate_pack_id = str(candidate["pack_id"])
        candidate_domain = str(candidate["domain"])
        canonical_text = str(candidate.get("canonical_text") or candidate_value)
        if _should_skip_single_token_lookup_candidate(
            matched_text=matched_text,
            candidate_value=candidate_value,
            canonical_text=canonical_text,
            candidate_label=candidate_label,
            candidate_domain=candidate_domain,
            segment_text=segment_text_value,
            start=start,
            end=end,
        ):
            continue
        if _should_skip_multi_token_lookup_candidate(
            matched_text=matched_text,
            candidate_value=candidate_value,
            canonical_text=canonical_text,
            candidate_label=candidate_label,
            candidate_domain=candidate_domain,
        ):
            continue
        normalized_start = segment_start + start
        normalized_end = segment_start + end
        raw_start, raw_end = normalized_input.raw_span(normalized_start, normalized_end)
        extracted.append(
            ExtractedCandidate(
                entity=EntityMatch(
                    text=matched_text,
                    label=candidate_label,
                    start=raw_start,
                    end=raw_end,
                    confidence=float(candidate.get("score") or 1.0),
                    provenance=_build_provenance(
                        match_kind="alias",
                        match_path="lookup.alias.exact",
                        match_source=candidate_value,
                        source_pack=candidate_pack_id,
                        source_domain=candidate_domain,
                        lane="deterministic_alias",
                    ),
                    link=_build_entity_link(
                        provider="lookup.alias.exact",
                        pack_id=candidate_pack_id,
                        label=candidate_label,
                        canonical_text=canonical_text,
                        entity_id=str(candidate.get("entity_id") or "") or None,
                    ),
                ),
                domain=candidate_domain,
                lane="deterministic_alias",
                normalized_start=normalized_start,
                normalized_end=normalized_end,
            )
        )
    return extracted


def _should_skip_single_token_lookup_candidate(
    *,
    matched_text: str,
    candidate_value: str,
    canonical_text: str | None = None,
    candidate_label: str,
    candidate_domain: str | None = None,
    segment_text: str | None = None,
    start: int | None = None,
    end: int | None = None,
) -> bool:
    label_key = candidate_label.casefold()
    canonical_text = canonical_text or candidate_value
    normalized_candidate_value = normalize_lookup_text(candidate_value)
    normalized_canonical_text = normalize_lookup_text(canonical_text)
    if label_key not in {"organization", "person", "location"}:
        return False
    if (
        candidate_domain == "general"
        and label_key in {"organization", "person"}
        and matched_text.islower()
        and _is_alpha_phrase(matched_text)
    ):
        return True
    if candidate_domain == "general" and not any(character.isalpha() for character in matched_text):
        return True
    if not _is_single_token_alpha(matched_text):
        return False
    if matched_text.islower():
        return True
    normalized_matched_text = normalize_lookup_text(matched_text)
    generic_single_token_blocklist = (
        _GENERIC_SINGLE_TOKEN_HEADS | _GENERIC_PHRASE_LEADS | _CALENDAR_SINGLE_TOKEN_WORDS
    )
    if (
        normalized_matched_text in generic_single_token_blocklist
        or normalized_candidate_value in generic_single_token_blocklist
        or normalized_canonical_text in generic_single_token_blocklist
    ):
        return True
    if (
        candidate_domain == "general"
        and label_key in {"organization", "person", "location"}
        and segment_text is not None
        and start is not None
        and end is not None
        and not _is_exact_expansion_acronym_alias(
            matched_text=matched_text,
            candidate_value=candidate_value,
            canonical_text=canonical_text,
        )
        and not _is_single_token_all_caps(candidate_value)
        and _has_whitespace_adjacent_titleish_token(segment_text, start=start, end=end)
    ):
        return True
    if (
        candidate_domain == "general"
        and label_key in {"organization", "person"}
        and _is_single_token_all_caps(candidate_value)
        and not _is_single_token_all_caps(matched_text)
    ):
        return True
    if (
        candidate_domain == "general"
        and segment_text is not None
        and start is not None
        and end is not None
        and normalized_candidate_value != normalized_canonical_text
        and not _is_exact_expansion_acronym_alias(
            matched_text=matched_text,
            candidate_value=candidate_value,
            canonical_text=canonical_text,
        )
        and _has_adjacent_titleish_token(segment_text, start=start, end=end)
    ):
        return True
    return False


def _should_skip_multi_token_lookup_candidate(
    *,
    matched_text: str,
    candidate_value: str,
    canonical_text: str | None = None,
    candidate_label: str,
    candidate_domain: str | None = None,
) -> bool:
    if candidate_domain != "general":
        return False
    label_key = candidate_label.casefold()
    if label_key not in {"organization", "person", "location"}:
        return False
    tokens = TOKEN_RE.findall(matched_text)
    if len(tokens) < 2:
        return False
    lowered_tokens = [normalize_lookup_text(token) for token in tokens]
    if label_key == "person":
        if lowered_tokens[0] in _GENERIC_PHRASE_LEADS:
            return True
        if "," in matched_text:
            trailing_segment = matched_text.split(",", 1)[1].lstrip()
            trailing_tokens = TOKEN_RE.findall(trailing_segment)
            if trailing_tokens:
                trailing_first = trailing_tokens[0]
                trailing_lower = normalize_lookup_text(trailing_first)
                if (
                    trailing_first.islower()
                    or trailing_lower in _AUXILIARY_LOOKUP_TOKENS
                    or trailing_lower in _GENERIC_PHRASE_LEADS
                ):
                    return True
        for raw_token, lowered_token in zip(tokens[1:], lowered_tokens[1:]):
            if lowered_token in _AUXILIARY_LOOKUP_TOKENS:
                return True
            if raw_token.islower() and lowered_token not in _PERSON_NAME_PARTICLES:
                return True
        return False
    if "," in matched_text:
        trailing_segment = matched_text.split(",", 1)[1].lstrip()
        trailing_tokens = TOKEN_RE.findall(trailing_segment)
        if trailing_tokens:
            trailing_first = trailing_tokens[0]
            trailing_lower = normalize_lookup_text(trailing_first)
            if (
                trailing_first.islower()
                or trailing_lower in _AUXILIARY_LOOKUP_TOKENS
                or trailing_lower in _GENERIC_PHRASE_LEADS
            ):
                return True
    if lowered_tokens[-1] in _TRAILING_FUNCTION_WORDS:
        return True
    if lowered_tokens[0] in _GENERIC_PHRASE_LEADS and any(
        raw_token.islower() or lowered_token in _AUXILIARY_LOOKUP_TOKENS
        for raw_token, lowered_token in zip(tokens[1:], lowered_tokens[1:])
    ):
        return True
    return False


def _is_single_token_alpha(value: str) -> bool:
    tokens = [token for token in re.split(r"[\s\-_./]+", value.strip()) if token]
    return len(tokens) == 1 and tokens[0].isalpha()


def _is_alpha_phrase(value: str) -> bool:
    tokens = [token for token in re.split(r"[\s\-_./]+", value.strip()) if token]
    return bool(tokens) and all(token.isalpha() for token in tokens)


def _is_single_token_all_caps(value: str) -> bool:
    tokens = [token for token in re.split(r"[\s\-_./]+", value.strip()) if token]
    if len(tokens) != 1:
        return False
    compact = "".join(character for character in tokens[0] if character.isalpha())
    return bool(compact) and compact.isupper()


def _is_exact_expansion_acronym_alias(
    *,
    matched_text: str,
    candidate_value: str,
    canonical_text: str,
) -> bool:
    normalized_matched_text = normalize_lookup_text(matched_text)
    normalized_candidate_value = normalize_lookup_text(candidate_value)
    normalized_canonical_text = normalize_lookup_text(canonical_text)
    if normalized_matched_text != normalized_candidate_value:
        return False
    if normalized_candidate_value == normalized_canonical_text:
        return False
    if not _is_single_token_all_caps(matched_text):
        return False
    if not _is_single_token_all_caps(candidate_value):
        return False
    compact = "".join(character for character in matched_text if character.isalpha())
    if len(compact) < 2 or len(compact) > 8:
        return False
    return _span_token_count(canonical_text) >= 2


def _has_whitespace_adjacent_titleish_token(text: str, *, start: int, end: int) -> bool:
    has_left = start > 0 and text[start - 1].isspace() and _is_titleish_token(
        _previous_token(text, start)
    )
    has_right = end < len(text) and text[end].isspace() and _is_titleish_token(
        _next_token(text, end)
    )
    return has_left or has_right


def _has_adjacent_titleish_token(text: str, *, start: int, end: int) -> bool:
    return _is_titleish_token(_previous_token(text, start)) or _is_titleish_token(
        _next_token(text, end)
    )


def _previous_token(text: str, start: int) -> str:
    match = re.search(r"([\w][\w.+&/-]*)\W*$", text[:start])
    return match.group(1) if match is not None else ""


def _next_token(text: str, end: int) -> str:
    match = re.match(r"\W*([\w][\w.+&/-]*)", text[end:])
    return match.group(1) if match is not None else ""


def _is_titleish_token(token: str) -> bool:
    if not token:
        return False
    compact = "".join(character for character in token if character.isalpha())
    if not compact:
        return False
    return compact.isupper() or compact[:1].isupper()


def _normalize_acronym_text(value: str) -> str:
    return "".join(character for character in value if character.isalnum()).upper()


def _derive_initialism(value: str) -> str:
    letters: list[str] = []
    for token in TOKEN_RE.findall(value):
        normalized = normalize_lookup_text(token)
        if normalized in _ACRONYM_CONNECTOR_WORDS:
            continue
        compact = "".join(character for character in token if character.isalpha())
        if not compact:
            continue
        letters.append(compact[0].upper())
    if len(letters) < 2:
        return ""
    return "".join(letters)


def _find_document_acronym_definition(
    candidate: ExtractedCandidate,
    *,
    text: str,
) -> DocumentAcronymDefinition | None:
    entity = candidate.entity
    label_key = entity.label.casefold()
    if label_key not in {"organization", "location"}:
        return None
    if _span_token_count(entity.text) < 2:
        return None

    candidate_initialisms = {
        initialism
        for initialism in {
            _derive_initialism(entity.text),
            _derive_initialism(entity.link.canonical_text) if entity.link is not None else "",
        }
        if initialism
    }
    if not candidate_initialisms:
        return None

    after_slice = text[
        candidate.normalized_end : candidate.normalized_end + _MAX_ACRONYM_DEFINITION_WINDOW
    ]
    after_match = _ACRONYM_AFTER_ENTITY_RE.match(after_slice)
    if after_match is not None:
        normalized_acronym = _normalize_acronym_text(after_match.group("acronym"))
        if normalized_acronym in candidate_initialisms:
            return DocumentAcronymDefinition(
                acronym_text=after_match.group("acronym"),
                normalized_acronym=normalized_acronym,
                label=entity.label,
                start=candidate.normalized_end + after_match.start("acronym"),
                end=candidate.normalized_end + after_match.end("acronym"),
                anchor=candidate,
            )

    before_window_start = max(0, candidate.normalized_start - _MAX_ACRONYM_DEFINITION_WINDOW)
    before_slice = text[before_window_start : candidate.normalized_start]
    before_match = _ACRONYM_BEFORE_ENTITY_RE.search(before_slice)
    if before_match is not None:
        normalized_acronym = _normalize_acronym_text(before_match.group("acronym"))
        if normalized_acronym in candidate_initialisms:
            return DocumentAcronymDefinition(
                acronym_text=before_match.group("acronym"),
                normalized_acronym=normalized_acronym,
                label=entity.label,
                start=before_window_start + before_match.start("acronym"),
                end=before_window_start + before_match.end("acronym"),
                anchor=candidate,
            )
    return None


def _collect_document_acronym_definitions(
    extracted: list[ExtractedCandidate],
    *,
    text: str,
) -> list[DocumentAcronymDefinition]:
    kept: dict[tuple[str, str], DocumentAcronymDefinition] = {}
    ambiguous_keys: set[tuple[str, str]] = set()
    for candidate in extracted:
        definition = _find_document_acronym_definition(candidate, text=text)
        if definition is None:
            continue
        key = (definition.normalized_acronym, definition.label.casefold())
        current = kept.get(key)
        if current is None:
            kept[key] = definition
            continue
        current_anchor = current.anchor.entity.link.entity_id if current.anchor.entity.link else None
        new_anchor = definition.anchor.entity.link.entity_id if definition.anchor.entity.link else None
        if current_anchor == new_anchor:
            if _candidate_rank_key(definition.anchor) > _candidate_rank_key(current.anchor):
                kept[key] = definition
            continue
        ambiguous_keys.add(key)
    for key in ambiguous_keys:
        kept.pop(key, None)
    return sorted(kept.values(), key=lambda item: (item.start, item.end, item.label.casefold()))


def _backfill_document_acronym_entities(
    extracted: list[ExtractedCandidate],
    *,
    normalized_input: NormalizedText,
) -> list[ExtractedCandidate]:
    definitions = _collect_document_acronym_definitions(extracted, text=normalized_input.text)
    if not definitions:
        return extracted

    existing_spans = {
        (candidate.normalized_start, candidate.normalized_end, candidate.entity.label.casefold())
        for candidate in extracted
    }
    definition_by_key = {
        (definition.normalized_acronym, definition.label.casefold()): definition
        for definition in definitions
    }
    augmented = list(extracted)

    for start, end in _iter_token_spans(normalized_input.text):
        token_text = normalized_input.text[start:end]
        normalized_acronym = _normalize_acronym_text(token_text)
        if len(normalized_acronym) < 2 or not normalized_acronym.isupper():
            continue
        for label_key in ("organization", "location"):
            definition = definition_by_key.get((normalized_acronym, label_key))
            if definition is None or start < definition.start:
                continue
            span_key = (start, end, definition.label.casefold())
            if span_key in existing_spans:
                continue
            anchor_entity = definition.anchor.entity
            if anchor_entity.link is None or anchor_entity.provenance is None:
                continue
            raw_start, raw_end = normalized_input.raw_span(start, end)
            augmented.append(
                ExtractedCandidate(
                    entity=EntityMatch(
                        text=normalized_input.raw_text[raw_start:raw_end],
                        label=anchor_entity.label,
                        start=raw_start,
                        end=raw_end,
                        confidence=_clamp_score(max(anchor_entity.confidence or 0.0, 0.72)),
                        provenance=anchor_entity.provenance.model_copy(
                            update={
                                "match_path": "lookup.alias.document_acronym",
                                "match_source": token_text,
                                "lane": "document_acronym_backfill",
                            }
                        ),
                        link=anchor_entity.link,
                    ),
                    domain=definition.anchor.domain,
                    lane="document_acronym_backfill",
                    normalized_start=start,
                    normalized_end=end,
                )
            )
            existing_spans.add(span_key)
    return augmented


def _normalized_label_set(runtime: PackRuntime) -> set[str]:
    return {label.casefold() for label in runtime.labels}


def _build_document_context_signals(
    extracted: list[ExtractedCandidate],
) -> DocumentContextSignals:
    context = DocumentContextSignals(
        anchor_counts=Counter(),
        pack_counts=Counter(),
        domain_counts=Counter(),
        label_counts=Counter(),
    )
    for candidate in extracted:
        if candidate.entity.link is not None:
            context.anchor_counts[
                _candidate_anchor_key(
                    candidate.entity.link.entity_id,
                    candidate.entity.link.canonical_text,
                )
            ] += 1
        if candidate.entity.provenance is not None and candidate.entity.provenance.source_pack:
            context.pack_counts[candidate.entity.provenance.source_pack] += 1
        if candidate.domain:
            context.domain_counts[candidate.domain] += 1
        if candidate.entity.label:
            context.label_counts[candidate.entity.label.casefold()] += 1
    return context


def _candidate_anchor_key(entity_id: str | None, canonical_text: str) -> str:
    return entity_id or normalize_lookup_text(canonical_text)


def _bounded_context_bonus(count: int, *, step: float) -> float:
    if count <= 0:
        return 0.0
    return round(min(1.0, count * step), 4)


def _surface_similarity(left: str, right: str) -> float:
    normalized_left = normalize_lookup_text(left)
    normalized_right = normalize_lookup_text(right)
    if not normalized_left or not normalized_right:
        return 0.0
    if normalized_left == normalized_right:
        return 1.0
    if normalized_left in normalized_right or normalized_right in normalized_left:
        return 0.9
    left_terms = set(TOKEN_RE.findall(normalized_left))
    right_terms = set(TOKEN_RE.findall(normalized_right))
    if not left_terms or not right_terms:
        return 0.0
    intersection = len(left_terms & right_terms)
    union = len(left_terms | right_terms)
    return round(intersection / union, 4) if union else 0.0


def _score_hybrid_candidate(
    proposal: ProposalSpan,
    candidate: dict[str, str | float | bool | None],
    *,
    document_context: DocumentContextSignals,
) -> float:
    label_match = 1.0 if str(candidate["label"]).casefold() == proposal.label.casefold() else 0.0
    alias_match_score = float(candidate.get("score") or 0.0)
    entity_prior = float(candidate.get("entity_prior") or 0.0)
    source_priority = float(candidate.get("source_priority") or 0.0)
    popularity_weight = float(candidate.get("popularity_weight") or 0.0)
    surface_score = max(
        _surface_similarity(proposal.text, str(candidate["value"])),
        _surface_similarity(proposal.text, str(candidate.get("canonical_text") or candidate["value"])),
    )
    anchor_key = _candidate_anchor_key(
        str(candidate.get("entity_id") or "") or None,
        str(candidate.get("canonical_text") or candidate["value"]),
    )
    anchor_score = _bounded_context_bonus(
        document_context.anchor_counts.get(anchor_key, 0),
        step=0.25,
    )
    pack_score = _bounded_context_bonus(
        document_context.pack_counts.get(str(candidate["pack_id"]), 0),
        step=0.2,
    )
    domain_score = _bounded_context_bonus(
        document_context.domain_counts.get(str(candidate["domain"]), 0),
        step=0.18,
    )
    label_context_score = _bounded_context_bonus(
        document_context.label_counts.get(str(candidate["label"]).casefold(), 0),
        step=0.12,
    )
    context_score = round(
        (pack_score * 0.5) + (domain_score * 0.3) + (label_context_score * 0.2),
        4,
    )
    generated_penalty = 0.04 if bool(candidate.get("generated")) else 0.0
    score = (
        proposal.confidence * 0.24
        + alias_match_score * 0.20
        + entity_prior * 0.12
        + popularity_weight * 0.08
        + label_match * 0.12
        + surface_score * 0.10
        + anchor_score * 0.06
        + context_score * 0.08
        + source_priority * 0.06
        - generated_penalty
    )
    return _clamp_score(score)


def _extract_hybrid_entities(
    segment_text_value: str,
    *,
    segment_start: int,
    normalized_input: NormalizedText,
    registry: PackRegistry,
    runtime: PackRuntime,
    warnings: list[str],
    document_context: DocumentContextSignals,
    hybrid: bool | None,
    proposal_provider: ProposalProvider | None,
) -> list[ExtractedCandidate]:
    allowed_labels = _normalized_label_set(runtime)
    if not allowed_labels:
        return []

    try:
        proposals = get_proposal_spans(
            segment_text_value,
            allowed_labels=allowed_labels,
            enabled_override=hybrid,
            provider_override=proposal_provider,
        )
    except RuntimeError as exc:
        warning = f"hybrid_unavailable:{exc}"
        if warning not in warnings:
            warnings.append(warning)
        return []

    if not proposals:
        return []

    allowed_pack_ids = set(runtime.pack_chain)
    extracted: list[ExtractedCandidate] = []
    for proposal in proposals:
        candidates = [
            candidate
            for candidate in registry.lookup_candidates(
                proposal.text,
                exact_alias=False,
                active_only=True,
                limit=50,
            )
            if candidate["kind"] == "alias"
            and candidate["pack_id"] in allowed_pack_ids
            and str(candidate["label"]).casefold() == proposal.label.casefold()
        ]
        if not candidates:
            continue
        scored_candidates = sorted(
            (
                (
                    _score_hybrid_candidate(
                        proposal,
                        candidate,
                        document_context=document_context,
                    ),
                    candidate,
                )
                for candidate in candidates
            ),
            key=lambda item: (
                -item[0],
                -float(item[1].get("score") or 0.0),
                str(item[1]["pack_id"]),
                str(item[1]["value"]).casefold(),
            ),
        )
        best_score, best_candidate = scored_candidates[0]
        if best_score < 0.62:
            continue
        normalized_start = segment_start + proposal.start
        normalized_end = segment_start + proposal.end
        raw_start, raw_end = normalized_input.raw_span(normalized_start, normalized_end)
        canonical_text = str(best_candidate.get("canonical_text") or best_candidate["value"])
        entity = EntityMatch(
            text=segment_text_value[proposal.start:proposal.end],
            label=str(best_candidate["label"]),
            start=raw_start,
            end=raw_end,
            confidence=best_score,
            provenance=_build_provenance(
                match_kind="proposal",
                match_path="proposal.linked",
                match_source=str(best_candidate["value"]),
                source_pack=str(best_candidate["pack_id"]),
                source_domain=str(best_candidate["domain"]),
                lane="proposal_linked",
                model_name=proposal.model_name,
                model_version=proposal.model_version,
            ),
            link=_build_entity_link(
                provider="proposal.linked",
                pack_id=str(best_candidate["pack_id"]),
                label=str(best_candidate["label"]),
                canonical_text=canonical_text,
                entity_id=str(best_candidate.get("entity_id") or "") or None,
            ),
        )
        extracted.append(
            ExtractedCandidate(
                entity=entity,
                domain=str(best_candidate["domain"]),
                lane="proposal_linked",
                normalized_start=normalized_start,
                normalized_end=normalized_end,
            )
        )
        document_context.anchor_counts[
            _candidate_anchor_key(
                entity.link.entity_id if entity.link is not None else None,
                canonical_text,
            )
        ] += 1
        document_context.pack_counts[str(best_candidate["pack_id"])] += 1
        document_context.domain_counts[str(best_candidate["domain"])] += 1
        document_context.label_counts[str(best_candidate["label"]).casefold()] += 1
    return extracted


def _clamp_score(value: float) -> float:
    return round(max(0.0, min(1.0, value)), 4)


def _count_occurrences(text: str, value: str) -> int:
    if not value:
        return 1
    matches = re.findall(re.escape(value), text, flags=re.IGNORECASE)
    return max(1, len(matches))


def _span_token_count(value: str) -> int:
    return max(1, len(TOKEN_RE.findall(value)))


def _score_entity_relevance(
    candidate: ExtractedCandidate,
    *,
    runtime: PackRuntime,
    text: str,
) -> float:
    entity = candidate.entity
    confidence = entity.confidence or 0.0
    position_score = 1.0 - min(1.0, candidate.normalized_start / max(1, len(text)))
    span_bonus = min(0.08, max(0, _span_token_count(entity.text) - 1) * 0.03)
    canonical_text = entity.link.canonical_text if entity.link is not None else entity.text
    occurrence_bonus = min(0.12, max(0, _count_occurrences(text, canonical_text) - 1) * 0.05)
    lane_bonus = {
        "deterministic_rule": 0.18,
        "deterministic_alias": 0.12,
        "proposal_linked": 0.08,
    }.get(candidate.lane, 0.08)

    if runtime.domain == "general":
        domain_bonus = 0.08 if candidate.domain == "general" else 0.03
    elif candidate.domain == runtime.domain:
        domain_bonus = 0.18
    elif candidate.domain == "general":
        domain_bonus = 0.05
    else:
        domain_bonus = 0.1

    return _clamp_score(
        (confidence * 0.38)
        + (position_score * 0.14)
        + lane_bonus
        + domain_bonus
        + span_bonus
        + occurrence_bonus
    )


def _apply_entity_relevance(
    runtime: PackRuntime,
    *,
    text: str,
    extracted: list[ExtractedCandidate],
) -> list[ExtractedCandidate]:
    scored: list[ExtractedCandidate] = []
    for candidate in extracted:
        scored.append(
            ExtractedCandidate(
                entity=candidate.entity.model_copy(
                    update={"relevance": _score_entity_relevance(candidate, runtime=runtime, text=text)}
                ),
                domain=candidate.domain,
                lane=candidate.lane,
                normalized_start=candidate.normalized_start,
                normalized_end=candidate.normalized_end,
            )
        )
    return scored


def _candidate_rank_key(candidate: ExtractedCandidate) -> tuple[float, int, int, int]:
    entity = candidate.entity
    return (
        entity.relevance if entity.relevance is not None else (entity.confidence or 0.0),
        entity.end - entity.start,
        _LANE_PRIORITY.get(candidate.lane, 0),
        -entity.start,
    )


def _dedupe_exact_candidates(
    extracted: list[ExtractedCandidate],
) -> tuple[list[ExtractedCandidate], list[ExtractedCandidate]]:
    deduped: dict[tuple[int, int, str, str], ExtractedCandidate] = {}
    discarded: list[ExtractedCandidate] = []
    for candidate in extracted:
        entity = candidate.entity
        key = (entity.start, entity.end, entity.label.casefold(), entity.text.casefold())
        current = deduped.get(key)
        if current is None or _candidate_rank_key(candidate) > _candidate_rank_key(current):
            if current is not None:
                discarded.append(current)
            deduped[key] = candidate
            continue
        discarded.append(candidate)
    kept = sorted(
        deduped.values(),
        key=lambda item: (
            item.entity.start,
            item.entity.end,
            item.entity.label.casefold(),
            item.entity.text.casefold(),
        ),
    )
    return kept, discarded


def _apply_repeated_surface_consistency(
    extracted: list[ExtractedCandidate],
) -> list[ExtractedCandidate]:
    grouped: dict[tuple[str, str], list[ExtractedCandidate]] = {}
    for candidate in extracted:
        key = (normalize_lookup_text(candidate.entity.text), candidate.entity.label.casefold())
        grouped.setdefault(key, []).append(candidate)

    rewritten: list[ExtractedCandidate] = []
    for group in grouped.values():
        if len(group) == 1:
            rewritten.extend(group)
            continue
        by_entity = {
            candidate.entity.link.entity_id
            for candidate in group
            if candidate.entity.link is not None
        }
        if len(by_entity) <= 1:
            rewritten.extend(group)
            continue
        anchor = max(group, key=_candidate_rank_key)
        for candidate in group:
            if candidate.entity.link is None or anchor.entity.link is None:
                rewritten.append(candidate)
                continue
            provenance = candidate.entity.provenance
            anchor_provenance = anchor.entity.provenance
            rewritten.append(
                ExtractedCandidate(
                    entity=candidate.entity.model_copy(
                        update={
                            "link": anchor.entity.link,
                            "provenance": (
                                provenance.model_copy(
                                    update={
                                        "match_source": anchor.entity.link.canonical_text,
                                        "source_pack": (
                                            anchor_provenance.source_pack
                                            if anchor_provenance is not None
                                            else provenance.source_pack
                                        ),
                                        "source_domain": (
                                            anchor_provenance.source_domain
                                            if anchor_provenance is not None
                                            else provenance.source_domain
                                        ),
                                    }
                                )
                                if provenance is not None
                                else provenance
                            ),
                        }
                    ),
                    domain=anchor.domain,
                    lane=candidate.lane,
                    normalized_start=candidate.normalized_start,
                    normalized_end=candidate.normalized_end,
                )
            )
    return sorted(rewritten, key=lambda item: (item.entity.start, item.entity.end))


def _interval_weight(candidate: ExtractedCandidate) -> int:
    relevance = candidate.entity.relevance
    confidence = candidate.entity.confidence
    base_score = relevance if relevance is not None else (confidence or 0.0)
    span_length = max(1, candidate.entity.end - candidate.entity.start)
    lane_priority = _LANE_PRIORITY.get(candidate.lane, 0)
    start_bias = max(0, 100_000 - min(candidate.entity.start, 100_000))
    return (
        int(round(base_score * 10_000)) * 1_000_000_000
        + (span_length * 1_000_000)
        + (lane_priority * 1_000)
        + start_bias
    )


def _resolve_overlaps(
    extracted: list[ExtractedCandidate],
    *,
    debug_enabled: bool,
) -> tuple[list[ExtractedCandidate], TagDebug | None, int]:
    if not extracted:
        return [], TagDebug() if debug_enabled else None, 0

    ordered = sorted(
        extracted,
        key=lambda item: (
            item.entity.end,
            item.entity.start,
            -_LANE_PRIORITY.get(item.lane, 0),
            -(item.entity.relevance if item.entity.relevance is not None else (item.entity.confidence or 0.0)),
        ),
    )
    ends = [item.entity.end for item in ordered]
    predecessors: list[int] = []
    for index, candidate in enumerate(ordered):
        predecessor = bisect_right(ends, candidate.entity.start, 0, index) - 1
        predecessors.append(predecessor)

    best_scores = [0] * len(ordered)
    choose_current = [False] * len(ordered)
    for index, candidate in enumerate(ordered):
        include_score = _interval_weight(candidate)
        predecessor = predecessors[index]
        if predecessor >= 0:
            include_score += best_scores[predecessor]
        exclude_score = best_scores[index - 1] if index > 0 else 0
        if include_score >= exclude_score:
            best_scores[index] = include_score
            choose_current[index] = True
        else:
            best_scores[index] = exclude_score

    kept_indexes: set[int] = set()
    pointer = len(ordered) - 1
    while pointer >= 0:
        if choose_current[pointer]:
            kept_indexes.add(pointer)
            pointer = predecessors[pointer]
            continue
        pointer -= 1

    kept = [ordered[index] for index in sorted(kept_indexes)]
    discarded = [ordered[index] for index in range(len(ordered)) if index not in kept_indexes]
    kept = sorted(kept, key=lambda item: (item.entity.start, item.entity.end, item.entity.label.casefold()))

    if not debug_enabled:
        return kept, None, len(discarded)

    span_decisions = [
        TagSpanDecision(
            text=item.entity.text,
            label=item.entity.label,
            start=item.entity.start,
            end=item.entity.end,
            lane=item.lane,
            kept=index in kept_indexes,
            reason=None if index in kept_indexes else "overlap_replaced",
        )
        for index, item in enumerate(ordered)
    ]
    return (
        kept,
        TagDebug(
            kept_span_count=len(kept),
            discarded_span_count=len(discarded),
            span_decisions=span_decisions,
        ),
        len(discarded),
    )


def _topic_score(entities: list[EntityMatch], *, multiplier: float = 1.0) -> float:
    if not entities:
        return _clamp_score(1.0 if multiplier >= 1.0 else 0.4 * multiplier)

    weights = [
        entity.relevance if entity.relevance is not None else (entity.confidence or 0.0)
        for entity in entities
    ]
    average_weight = sum(weights) / len(weights)
    count_bonus = min(0.2, len(entities) * 0.08)
    return _clamp_score(((average_weight * 0.8) + count_bonus) * multiplier)


def _build_topic_match(
    *,
    label: str,
    entities: list[EntityMatch],
    multiplier: float = 1.0,
) -> TopicMatch:
    label_counts = Counter(entity.label for entity in entities)
    return TopicMatch(
        label=label,
        score=_topic_score(entities, multiplier=multiplier),
        evidence_count=len(entities),
        entity_labels=[
            entity_label
            for entity_label, _ in sorted(label_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
    )


def _build_topics(runtime: PackRuntime, extracted: list[ExtractedCandidate]) -> list[TopicMatch]:
    if not extracted:
        return []

    by_domain: dict[str, list[EntityMatch]] = {}
    for candidate in extracted:
        if not candidate.domain or candidate.domain == "general":
            continue
        by_domain.setdefault(candidate.domain, []).append(candidate.entity)

    if by_domain:
        topics = [
            _build_topic_match(label=domain, entities=entities)
            for domain, entities in sorted(by_domain.items())
        ]
        return sorted(topics, key=lambda topic: (-(topic.score or 0.0), -topic.evidence_count, topic.label))

    fallback_entities = [candidate.entity for candidate in extracted]
    multiplier = 1.0 if runtime.domain == "general" else 0.7
    return [
        _build_topic_match(
            label=runtime.domain,
            entities=fallback_entities,
            multiplier=multiplier,
        )
    ]


def _build_metrics(
    *,
    raw_text: str,
    normalized_input: NormalizedText,
    extracted: list[ExtractedCandidate],
    segment_count: int,
    overlap_drop_count: int,
) -> TagMetrics:
    token_total = token_count(normalized_input.text)
    entity_total = len(extracted)
    per_label_counts = Counter(candidate.entity.label for candidate in extracted)
    per_lane_counts = Counter(candidate.lane for candidate in extracted)
    density = round((entity_total * 100 / token_total), 4) if token_total else 0.0
    return TagMetrics(
        input_char_count=len(raw_text),
        normalized_char_count=len(normalized_input.text),
        token_count=token_total,
        segment_count=segment_count,
        chunk_count=segment_count,
        entity_count=entity_total,
        entities_per_100_tokens=density,
        overlap_drop_count=overlap_drop_count,
        per_label_counts=dict(sorted(per_label_counts.items())),
        per_lane_counts=dict(sorted(per_lane_counts.items())),
    )


def _apply_density_warning(
    *,
    runtime: PackRuntime,
    metrics: TagMetrics,
    warnings: list[str],
) -> None:
    threshold = runtime.min_entities_per_100_tokens_warning
    if threshold is None:
        return
    if metrics.token_count < 50:
        return
    if metrics.entities_per_100_tokens >= threshold:
        return
    warnings.append(
        "low_entity_density:"
        f"{runtime.pack_id}:{metrics.entities_per_100_tokens:.4f}<{threshold:.4f}"
    )


def _truncate_input(text: str, warnings: list[str]) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= MAX_INPUT_BYTES:
        return text
    warnings.append(f"input_truncated_to_bytes:{MAX_INPUT_BYTES}")
    return encoded[:MAX_INPUT_BYTES].decode("utf-8", errors="ignore")


def tag_text(
    *,
    text: str,
    pack: str,
    content_type: str,
    storage_root: Path,
    debug: bool = False,
    hybrid: bool | None = None,
    proposal_provider: ProposalProvider | None = None,
    registry: PackRegistry | None = None,
) -> TagResponse:
    """Tag text using installed pack rules and indexed alias lookup."""

    start = perf_counter()
    warnings: list[str] = []
    bounded_text = _truncate_input(text, warnings)
    normalized_input = normalize_text(bounded_text, content_type)
    if not normalized_input.text:
        warnings.append("empty_input")
    if content_type not in {"text/plain", "text/html"}:
        warnings.append(f"unsupported_content_type:{content_type}")

    registry = registry or PackRegistry(storage_root)
    runtime = load_pack_runtime(storage_root, pack, registry=registry)
    if runtime is None:
        warnings.append(f"pack_not_installed:{pack}")
        elapsed_ms = int((perf_counter() - start) * 1000)
        return TagResponse(
            schema_version=TAG_SCHEMA_VERSION,
            version=__version__,
            pack=pack,
            pack_version=None,
            language=_language_from_pack(pack),
            content_type=content_type,
            source_path=None,
            entities=[],
            topics=[],
            warnings=warnings,
            timing_ms=elapsed_ms,
            metrics=_build_metrics(
                raw_text=bounded_text,
                normalized_input=normalized_input,
                extracted=[],
                segment_count=0,
                overlap_drop_count=0,
            ),
        )

    segments = segment_text(
        normalized_input.text,
        language=runtime.language,
        max_chars=MAX_SEGMENT_CHARS,
        max_tokens=MAX_SEGMENT_TOKENS,
    )
    extracted: list[ExtractedCandidate] = []
    for segment in segments:
        segment_extracted = _extract_rule_entities(
            segment.text,
            segment_start=segment.start,
            normalized_input=normalized_input,
            rules=runtime.rules,
            warnings=warnings,
        )
        segment_extracted.extend(
            _extract_lookup_alias_entities(
                segment.text,
                segment_start=segment.start,
                normalized_input=normalized_input,
                registry=registry,
                runtime=runtime,
            )
        )
        document_context = _build_document_context_signals(extracted + segment_extracted)
        segment_extracted.extend(
            _extract_hybrid_entities(
                segment.text,
                segment_start=segment.start,
                normalized_input=normalized_input,
                registry=registry,
                runtime=runtime,
                warnings=warnings,
                document_context=document_context,
                hybrid=hybrid,
                proposal_provider=proposal_provider,
            )
        )
        extracted.extend(segment_extracted)

    deduped, duplicate_discards = _dedupe_exact_candidates(extracted)
    coherent = _apply_repeated_surface_consistency(deduped)
    acronym_backfilled = _backfill_document_acronym_entities(
        coherent,
        normalized_input=normalized_input,
    )
    acronym_deduped, acronym_duplicate_discards = _dedupe_exact_candidates(acronym_backfilled)
    coherent = _apply_repeated_surface_consistency(acronym_deduped)
    scored = _apply_entity_relevance(runtime, text=normalized_input.text, extracted=coherent)
    resolved, debug_payload, overlap_drop_count = _resolve_overlaps(
        scored,
        debug_enabled=debug or os.getenv("ADES_TAG_DEBUG") == "1",
    )

    total_duplicate_discards = duplicate_discards + acronym_duplicate_discards
    if debug_payload is not None and total_duplicate_discards:
        span_decisions = list(debug_payload.span_decisions)
        for item in total_duplicate_discards:
            span_decisions.append(
                TagSpanDecision(
                    text=item.entity.text,
                    label=item.entity.label,
                    start=item.entity.start,
                    end=item.entity.end,
                    lane=item.lane,
                    kept=False,
                    reason="exact_duplicate_replaced",
                )
            )
        debug_payload = TagDebug(
            kept_span_count=debug_payload.kept_span_count,
            discarded_span_count=debug_payload.discarded_span_count + len(total_duplicate_discards),
            span_decisions=span_decisions,
        )

    metrics = _build_metrics(
        raw_text=bounded_text,
        normalized_input=normalized_input,
        extracted=resolved,
        segment_count=len(segments),
        overlap_drop_count=overlap_drop_count + len(total_duplicate_discards),
    )
    _apply_density_warning(runtime=runtime, metrics=metrics, warnings=warnings)

    elapsed_ms = int((perf_counter() - start) * 1000)
    return TagResponse(
        schema_version=TAG_SCHEMA_VERSION,
        version=__version__,
        pack=pack,
        pack_version=runtime.pack_version,
        language=runtime.language,
        content_type=content_type,
        source_path=None,
        entities=[candidate.entity for candidate in resolved],
        topics=_build_topics(runtime, resolved),
        metrics=metrics,
        debug=debug_payload,
        warnings=warnings,
        timing_ms=elapsed_ms,
    )
