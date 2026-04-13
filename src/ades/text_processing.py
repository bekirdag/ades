"""Shared text normalization and segmentation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from html import unescape
import re
from typing import Any
import unicodedata

_WHITESPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"\b[\w][\w.+&/-]*\b")
_SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])\s+|\n{2,}")
_QUOTE_TRANSLATION = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
    }
)
_DASH_TRANSLATION = str.maketrans(
    {
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
    }
)
_MAX_ENTITY_LENGTH = 32
_SPACY_SENTENCIZERS: dict[str, Any] = {}


@dataclass(frozen=True)
class NormalizedText:
    """Normalized text plus a stable normalized-to-raw offset map."""

    raw_text: str
    text: str
    normalized_to_raw: tuple[int, ...]
    normalized_to_raw_end: tuple[int, ...]
    content_type: str

    def raw_span(self, start: int, end: int) -> tuple[int, int]:
        """Translate one normalized span back to raw input offsets."""

        clamped_start = max(0, min(start, len(self.text)))
        clamped_end = max(clamped_start, min(end, len(self.text)))
        raw_start = self.normalized_to_raw[clamped_start]
        raw_end = (
            self.normalized_to_raw_end[clamped_end - 1]
            if clamped_end > clamped_start
            else raw_start
        )
        return raw_start, max(raw_start, raw_end)


@dataclass(frozen=True)
class TextSegment:
    """One stable normalized-text segment."""

    start: int
    end: int
    text: str
    token_count: int


def canonicalize_text(value: str) -> str:
    """Return one shared normalized text form without case folding."""

    normalized = unicodedata.normalize("NFKC", value)
    normalized = normalized.translate(_QUOTE_TRANSLATION)
    normalized = normalized.translate(_DASH_TRANSLATION)
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized


def normalize_lookup_text(value: str) -> str:
    """Return the shared exact-lookup normalization contract."""

    return canonicalize_text(value).casefold()


def normalize_text(value: str, content_type: str) -> NormalizedText:
    """Normalize tag input while preserving a normalized-to-raw offset map."""

    visible_chars: list[tuple[str, int, int]] = []
    in_tag = False
    index = 0
    raw_length = len(value)

    while index < raw_length:
        char = value[index]
        if content_type == "text/html":
            if in_tag:
                if char == ">":
                    in_tag = False
                index += 1
                continue
            if char == "<":
                in_tag = True
                if visible_chars and visible_chars[-1][0] != " ":
                    visible_chars.append((" ", index, index + 1))
                index += 1
                continue

        if content_type == "text/html" and char == "&":
            entity = _decode_html_entity(value, index)
            if entity is not None:
                decoded_text, next_index = entity
                for decoded_char in decoded_text:
                    visible_chars.append((decoded_char, index, next_index))
                index = next_index
                continue

        visible_chars.append((char, index, index + 1))
        index += 1

    output_chars: list[str] = []
    output_raw_offsets: list[int] = []
    output_raw_end_offsets: list[int] = []
    pending_space_raw_index: int | None = None
    pending_space_raw_end: int | None = None
    for char, raw_index, raw_end in visible_chars:
        normalized = unicodedata.normalize("NFKC", char)
        normalized = normalized.translate(_QUOTE_TRANSLATION)
        normalized = normalized.translate(_DASH_TRANSLATION)
        for normalized_char in normalized:
            if normalized_char.isspace():
                if pending_space_raw_index is None:
                    pending_space_raw_index = raw_index
                    pending_space_raw_end = raw_end
                else:
                    pending_space_raw_end = raw_end
                continue
            if pending_space_raw_index is not None and output_chars:
                output_chars.append(" ")
                output_raw_offsets.append(pending_space_raw_index)
                output_raw_end_offsets.append(
                    pending_space_raw_end if pending_space_raw_end is not None else pending_space_raw_index
                )
                pending_space_raw_index = None
                pending_space_raw_end = None
            output_chars.append(normalized_char)
            output_raw_offsets.append(raw_index)
            output_raw_end_offsets.append(raw_end)

    text = "".join(output_chars)
    output_raw_offsets.append(raw_length)
    output_raw_end_offsets.append(raw_length)
    return NormalizedText(
        raw_text=value,
        text=text,
        normalized_to_raw=tuple(output_raw_offsets),
        normalized_to_raw_end=tuple(output_raw_end_offsets),
        content_type=content_type,
    )


def segment_text(
    value: str,
    *,
    language: str,
    max_chars: int,
    max_tokens: int,
) -> list[TextSegment]:
    """Split normalized text into sentence-aware bounded segments."""

    if not value:
        return []

    sentence_spans = _sentence_spans(value, language=language)
    if not sentence_spans:
        token_count = len(_TOKEN_RE.findall(value))
        return [TextSegment(start=0, end=len(value), text=value, token_count=token_count)]

    segments: list[TextSegment] = []
    current_start = sentence_spans[0][0]
    current_end = sentence_spans[0][1]
    current_text = value[current_start:current_end]
    current_tokens = len(_TOKEN_RE.findall(current_text))

    for sentence_start, sentence_end in sentence_spans[1:]:
        sentence_text = value[sentence_start:sentence_end]
        sentence_tokens = len(_TOKEN_RE.findall(sentence_text))
        exceeds_chars = (sentence_end - current_start) > max_chars
        exceeds_tokens = (current_tokens + sentence_tokens) > max_tokens
        if current_text and (exceeds_chars or exceeds_tokens):
            segments.extend(
                _split_oversized_segment(
                    value[current_start:current_end],
                    base_offset=current_start,
                    max_chars=max_chars,
                    max_tokens=max_tokens,
                )
            )
            current_start = sentence_start
            current_end = sentence_end
            current_text = sentence_text
            current_tokens = sentence_tokens
            continue
        current_end = sentence_end
        current_text = value[current_start:current_end]
        current_tokens = len(_TOKEN_RE.findall(current_text))

    segments.extend(
        _split_oversized_segment(
            value[current_start:current_end],
            base_offset=current_start,
            max_chars=max_chars,
            max_tokens=max_tokens,
        )
    )
    return segments


def token_count(value: str) -> int:
    """Return the shared token-count contract used for extraction metrics."""

    return len(_TOKEN_RE.findall(value))


def _decode_html_entity(value: str, index: int) -> tuple[str, int] | None:
    semicolon_index = value.find(";", index + 1, min(len(value), index + _MAX_ENTITY_LENGTH))
    if semicolon_index == -1:
        return None
    candidate = value[index : semicolon_index + 1]
    decoded = unescape(candidate)
    if decoded == candidate:
        return None
    return decoded, semicolon_index + 1


def _sentence_spans(value: str, *, language: str) -> list[tuple[int, int]]:
    sentencizer = _get_spacy_sentencizer(language)
    if sentencizer is not None:
        try:
            doc = sentencizer(value)
            spans = [
                (sentence.start_char, sentence.end_char)
                for sentence in doc.sents
                if value[sentence.start_char:sentence.end_char].strip()
            ]
            if spans:
                return spans
        except Exception:
            pass

    spans: list[tuple[int, int]] = []
    start = 0
    for match in _SENTENCE_BOUNDARY_RE.finditer(value):
        end = match.start()
        if value[start:end].strip():
            spans.append((start, end))
        start = match.end()
    if value[start:].strip():
        spans.append((start, len(value)))
    return spans


def _get_spacy_sentencizer(language: str):
    cache_key = language.casefold()
    if cache_key in _SPACY_SENTENCIZERS:
        return _SPACY_SENTENCIZERS[cache_key]
    try:
        import spacy
    except Exception:
        _SPACY_SENTENCIZERS[cache_key] = None
        return None
    lang_code = cache_key if re.fullmatch(r"[a-z]{2,3}", cache_key) else "xx"
    try:
        nlp = spacy.blank(lang_code)
    except Exception:
        nlp = spacy.blank("xx")
    if "sentencizer" not in nlp.pipe_names:
        nlp.add_pipe("sentencizer")
    _SPACY_SENTENCIZERS[cache_key] = nlp
    return nlp


def _split_oversized_segment(
    value: str,
    *,
    base_offset: int,
    max_chars: int,
    max_tokens: int,
) -> list[TextSegment]:
    if not value:
        return []
    tokens = [(match.start(), match.end()) for match in _TOKEN_RE.finditer(value)]
    if not tokens:
        return [
            TextSegment(
                start=base_offset,
                end=base_offset + len(value),
                text=value,
                token_count=0,
            )
        ]

    segments: list[TextSegment] = []
    window_start = 0
    while window_start < len(tokens):
        window_end = min(window_start + max_tokens, len(tokens))
        segment_end = _segment_window_end(value, tokens, window_end)
        while segment_end - tokens[window_start][0] > max_chars and window_end - window_start > 1:
            window_end -= 1
            segment_end = _segment_window_end(value, tokens, window_end)
        start_char = tokens[window_start][0]
        end_char = segment_end
        while end_char > start_char and value[end_char - 1].isspace():
            end_char -= 1
        segment_text = value[start_char:end_char]
        segments.append(
            TextSegment(
                start=base_offset + start_char,
                end=base_offset + end_char,
                text=segment_text,
                token_count=window_end - window_start,
            )
        )
        window_start = window_end
    return segments


def _segment_window_end(
    value: str,
    tokens: list[tuple[int, int]],
    window_end: int,
) -> int:
    if window_end >= len(tokens):
        return len(value)
    next_token_start = tokens[window_end][0]
    segment_end = next_token_start
    while segment_end > 0 and value[segment_end - 1].isspace():
        segment_end -= 1
    return segment_end
