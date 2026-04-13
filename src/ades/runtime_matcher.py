"""Deterministic exact-match runtime artifact generation."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterator

from .text_processing import normalize_lookup_text


MATCHER_SCHEMA_VERSION = 1
MATCHER_ALGORITHM = "aho_corasick"
MATCHER_NORMALIZATION = "normalize_lookup_text:v1"
_MATCHER_JSON_CHUNK_SIZE = 1024 * 1024
_TOKEN_RE = re.compile(r"\b[\w][\w.+&/-]*\b")


@dataclass(frozen=True)
class MatcherArtifactResult:
    """Summary of one generated matcher artifact set."""

    algorithm: str
    normalization: str
    artifact_path: str
    entries_path: str
    entry_count: int
    state_count: int
    max_alias_length: int
    artifact_sha256: str
    entries_sha256: str


@dataclass(frozen=True)
class LookupTextView:
    """Case-folded lookup text plus a lookup-to-source offset map."""

    source_text: str
    text: str
    lookup_to_source: tuple[int, ...]
    lookup_to_source_end: tuple[int, ...]

    def source_span(self, start: int, end: int) -> tuple[int, int]:
        """Translate one lookup-text span back to source-text offsets."""

        clamped_start = max(0, min(start, len(self.text)))
        clamped_end = max(clamped_start, min(end, len(self.text)))
        source_start = self.lookup_to_source[clamped_start]
        source_end = (
            self.lookup_to_source_end[clamped_end - 1]
            if clamped_end > clamped_start
            else source_start
        )
        return source_start, max(source_start, source_end)


@dataclass(frozen=True)
class RuntimeMatcher:
    """Loaded read-only exact-match runtime artifact."""

    schema_version: int
    algorithm: str
    normalization: str
    entry_count: int
    state_count: int
    max_alias_length: int
    fail: tuple[int, ...]
    outputs: tuple[tuple[int, ...], ...]
    output_lengths: tuple[int, ...]
    transitions: tuple[dict[str, int], ...]


def build_matcher_artifact_from_aliases_json(
    aliases_path: str | Path,
    *,
    output_dir: str | Path,
) -> MatcherArtifactResult:
    """Compile a deterministic exact matcher artifact from aliases.json."""

    resolved_aliases_path = Path(aliases_path).expanduser().resolve()
    if not resolved_aliases_path.exists():
        raise FileNotFoundError(f"Alias payload not found: {resolved_aliases_path}")
    if not resolved_aliases_path.is_file():
        raise FileExistsError(f"Alias payload is not a file: {resolved_aliases_path}")

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = resolved_output_dir / "automaton.json"
    entries_path = resolved_output_dir / "entries.jsonl"

    transitions: list[dict[str, int]] = [{}]
    terminal_outputs: list[list[int]] = [[]]
    entry_count = 0
    max_alias_length = 0

    with entries_path.open("w", encoding="utf-8") as handle:
        for entry_index, alias_payload in enumerate(_iter_alias_payloads_from_aliases_json(resolved_aliases_path)):
            normalized_entry = _normalize_alias_payload(alias_payload)
            handle.write(json.dumps(normalized_entry, sort_keys=True) + "\n")
            entry_count += 1

            normalized_text = normalized_entry["normalized_text"]
            max_alias_length = max(max_alias_length, len(normalized_text))
            state = 0
            for character in normalized_text:
                next_state = transitions[state].get(character)
                if next_state is None:
                    next_state = len(transitions)
                    transitions[state][character] = next_state
                    transitions.append({})
                    terminal_outputs.append([])
                state = next_state
            terminal_outputs[state].append(entry_index)

    fail = [0] * len(transitions)
    outputs = [list(items) for items in terminal_outputs]
    queue: deque[int] = deque()

    for _, child in sorted(transitions[0].items()):
        queue.append(child)
        fail[child] = 0

    while queue:
        state = queue.popleft()
        for character, next_state in sorted(transitions[state].items()):
            queue.append(next_state)
            fallback = fail[state]
            while fallback and character not in transitions[fallback]:
                fallback = fail[fallback]
            fail[next_state] = transitions[fallback].get(character, 0)
            if outputs[fail[next_state]]:
                merged = outputs[next_state] + [
                    entry
                    for entry in outputs[fail[next_state]]
                    if entry not in outputs[next_state]
                ]
                outputs[next_state] = merged

    serialized_states = [
        {
            "fail": fail[state_index],
            "outputs": outputs[state_index],
            "transitions": [
                [character, next_state]
                for character, next_state in sorted(state_transitions.items())
            ],
        }
        for state_index, state_transitions in enumerate(transitions)
    ]

    artifact_payload = {
        "schema_version": MATCHER_SCHEMA_VERSION,
        "algorithm": MATCHER_ALGORITHM,
        "normalization": MATCHER_NORMALIZATION,
        "entry_count": entry_count,
        "state_count": len(serialized_states),
        "max_alias_length": max_alias_length,
        "states": serialized_states,
    }
    artifact_path.write_text(
        json.dumps(artifact_payload, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    return MatcherArtifactResult(
        algorithm=MATCHER_ALGORITHM,
        normalization=MATCHER_NORMALIZATION,
        artifact_path=str(artifact_path),
        entries_path=str(entries_path),
        entry_count=entry_count,
        state_count=len(serialized_states),
        max_alias_length=max_alias_length,
        artifact_sha256=_sha256_file(artifact_path),
        entries_sha256=_sha256_file(entries_path),
    )


def build_lookup_text_view(value: str) -> LookupTextView:
    """Return the shared lookup normalization view for runtime matching."""

    lookup_chars: list[str] = []
    lookup_to_source: list[int] = []
    lookup_to_source_end: list[int] = []
    for index, character in enumerate(value):
        for folded in character.casefold():
            lookup_chars.append(folded)
            lookup_to_source.append(index)
            lookup_to_source_end.append(index + 1)
    lookup_to_source.append(len(value))
    lookup_to_source_end.append(len(value))
    return LookupTextView(
        source_text=value,
        text="".join(lookup_chars),
        lookup_to_source=tuple(lookup_to_source),
        lookup_to_source_end=tuple(lookup_to_source_end),
    )


def find_exact_match_spans(
    text: str,
    matcher: RuntimeMatcher,
) -> list[tuple[int, int]]:
    """Scan text and return exact-match spans in source-text offsets."""

    if not text:
        return []
    view = build_lookup_text_view(text)
    token_starts, token_ends = _token_boundary_sets(view.source_text)
    state = 0
    seen_spans: set[tuple[int, int]] = set()
    matched_spans: list[tuple[int, int]] = []
    for index, character in enumerate(view.text):
        while state and character not in matcher.transitions[state]:
            state = matcher.fail[state]
        state = matcher.transitions[state].get(character, 0)
        for output_index in matcher.outputs[state]:
            if output_index < 0 or output_index >= len(matcher.output_lengths):
                continue
            output_length = matcher.output_lengths[output_index]
            if output_length <= 0:
                continue
            start = index + 1 - output_length
            if start < 0:
                continue
            span = view.source_span(start, index + 1)
            if not _is_token_window_span(span[0], span[1], token_starts=token_starts, token_ends=token_ends):
                continue
            if span in seen_spans:
                continue
            seen_spans.add(span)
            matched_spans.append(span)
    return sorted(matched_spans, key=lambda item: (item[0], -(item[1] - item[0]), item[1]))


def load_runtime_matcher(
    artifact_path: str | Path,
    entries_path: str | Path,
) -> RuntimeMatcher:
    """Load one cached runtime matcher artifact from disk."""

    resolved_artifact_path = Path(artifact_path).expanduser().resolve()
    resolved_entries_path = Path(entries_path).expanduser().resolve()
    return _load_runtime_matcher_cached(
        str(resolved_artifact_path),
        str(resolved_entries_path),
    )


def clear_runtime_matcher_cache() -> None:
    """Clear the process-local runtime matcher cache."""

    _load_runtime_matcher_cached.cache_clear()


def _iter_alias_payloads_from_aliases_json(path: Path) -> Iterator[dict[str, Any]]:
    decoder = json.JSONDecoder()
    buffer = ""
    position = 0
    aliases_started = False
    aliases_finished = False
    seen_aliases_key = False

    def ensure_buffer(*, eof: bool) -> bool:
        nonlocal buffer
        nonlocal position

        if position:
            buffer = buffer[position:]
            position = 0
        if eof:
            return False
        chunk = handle.read(_MATCHER_JSON_CHUNK_SIZE)
        if not chunk:
            return False
        buffer += chunk
        return True

    def skip_whitespace() -> bool:
        nonlocal position
        while True:
            while position < len(buffer) and buffer[position].isspace():
                position += 1
            if position < len(buffer):
                return True
            if not ensure_buffer(eof=False):
                return False

    def parse_value(*, eof: bool) -> Any:
        nonlocal position
        while True:
            if not skip_whitespace():
                if eof:
                    raise ValueError(f"Unexpected end of alias payload in {path}.")
                if not ensure_buffer(eof=False):
                    raise ValueError(f"Unexpected end of alias payload in {path}.")
                continue
            try:
                value, next_position = decoder.raw_decode(buffer, position)
            except json.JSONDecodeError as exc:
                if eof:
                    raise ValueError(f"Invalid alias payload in {path}.") from exc
                if not ensure_buffer(eof=False):
                    raise ValueError(f"Invalid alias payload in {path}.") from exc
                continue
            position = next_position
            return value

    with path.open("r", encoding="utf-8") as handle:
        if not ensure_buffer(eof=False):
            raise ValueError(f"Alias payload in {path} is empty.")
        if not skip_whitespace() or buffer[position] != "{":
            raise ValueError(f"Alias payload in {path} must be a JSON object.")
        position += 1

        while True:
            if not skip_whitespace():
                raise ValueError(f"Alias payload in {path} ended before `aliases`.")
            if not aliases_started and buffer[position] == "}":
                break
            if aliases_started:
                if buffer[position] == "]":
                    position += 1
                    aliases_started = False
                    aliases_finished = True
                    continue
                if buffer[position] == ",":
                    position += 1
                    if not skip_whitespace():
                        raise ValueError(f"Alias payload in {path} ended inside `aliases`.")
                entry = parse_value(eof=False)
                if not isinstance(entry, dict):
                    raise ValueError(f"Alias entries in {path} must be JSON objects.")
                yield entry
                continue

            if buffer[position] == ",":
                position += 1
                continue

            key = parse_value(eof=False)
            if not isinstance(key, str):
                raise ValueError(f"Alias payload in {path} must use string keys.")
            if not skip_whitespace() or buffer[position] != ":":
                raise ValueError(f"Alias payload in {path} is missing `:` after key `{key}`.")
            position += 1
            if key == "aliases":
                seen_aliases_key = True
                if not skip_whitespace() or buffer[position] != "[":
                    raise ValueError(f"Alias payload in {path} must contain a list under `aliases`.")
                position += 1
                aliases_started = True
                continue
            _ = parse_value(eof=False)

        if not seen_aliases_key:
            raise ValueError(f"Alias payload in {path} must contain a list under `aliases`.")
        if not aliases_finished:
            raise ValueError(f"Alias payload in {path} ended before `aliases` closed.")


def _normalize_alias_payload(payload: dict[str, Any]) -> dict[str, Any]:
    alias_text = str(payload["text"]).strip()
    if not alias_text:
        raise ValueError("Alias payload is missing `text`.")
    normalized_text = str(
        payload.get("normalized_text") or normalize_lookup_text(alias_text)
    ).strip()
    if not normalized_text:
        raise ValueError(f"Alias `{alias_text}` does not yield normalized_text.")
    canonical_text = str(payload.get("canonical_text") or alias_text).strip()
    if not canonical_text:
        raise ValueError(f"Alias `{alias_text}` is missing canonical_text.")
    label = str(payload["label"]).strip()
    if not label:
        raise ValueError(f"Alias `{alias_text}` is missing label.")
    return {
        "text": alias_text,
        "label": label,
        "normalized_text": normalized_text,
        "canonical_text": canonical_text,
        "score": round(float(payload.get("score", payload.get("alias_score", 1.0))), 4),
        "alias_score": round(
            float(payload.get("alias_score", payload.get("score", 1.0))), 4
        ),
        "generated": bool(payload.get("generated", False)),
        "source_name": str(payload.get("source_name", "")),
        "entity_id": str(payload.get("entity_id", "")),
        "source_priority": round(float(payload.get("source_priority", 0.6)), 4),
        "popularity_weight": round(float(payload.get("popularity_weight", 0.5)), 4),
        "source_domain": str(payload.get("source_domain", "")),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _token_boundary_sets(text: str) -> tuple[set[int], set[int]]:
    token_starts: set[int] = set()
    token_ends: set[int] = set()
    for match in _TOKEN_RE.finditer(text):
        token_starts.add(match.start())
        token_ends.add(match.end())
    return token_starts, token_ends


def _is_token_window_span(
    start: int,
    end: int,
    *,
    token_starts: set[int],
    token_ends: set[int],
) -> bool:
    return start in token_starts and end in token_ends and end > start


@lru_cache(maxsize=32)
def _load_runtime_matcher_cached(
    artifact_path: str,
    entries_path: str,
) -> RuntimeMatcher:
    resolved_artifact_path = Path(artifact_path)
    resolved_entries_path = Path(entries_path)
    if not resolved_artifact_path.exists():
        raise FileNotFoundError(f"Matcher artifact not found: {resolved_artifact_path}")
    if not resolved_entries_path.exists():
        raise FileNotFoundError(f"Matcher entries not found: {resolved_entries_path}")

    artifact_payload = json.loads(resolved_artifact_path.read_text(encoding="utf-8"))
    if str(artifact_payload.get("algorithm")) != MATCHER_ALGORITHM:
        raise ValueError(
            f"Unsupported matcher algorithm in {resolved_artifact_path}: "
            f"{artifact_payload.get('algorithm')}"
        )
    if str(artifact_payload.get("normalization")) != MATCHER_NORMALIZATION:
        raise ValueError(
            f"Unsupported matcher normalization in {resolved_artifact_path}: "
            f"{artifact_payload.get('normalization')}"
        )

    output_lengths: list[int] = []
    with resolved_entries_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            normalized_text = str(payload.get("normalized_text", "")).strip()
            if not normalized_text:
                raise ValueError(
                    f"Matcher entry {line_number} in {resolved_entries_path} is missing normalized_text."
                )
            output_lengths.append(len(normalized_text))

    entry_count = int(artifact_payload.get("entry_count", 0))
    if entry_count != len(output_lengths):
        raise ValueError(
            f"Matcher entry count mismatch for {resolved_artifact_path}: "
            f"artifact={entry_count}, entries={len(output_lengths)}"
        )

    states_payload = artifact_payload.get("states")
    if not isinstance(states_payload, list):
        raise ValueError(f"Matcher artifact in {resolved_artifact_path} is missing states.")

    fail: list[int] = []
    outputs: list[tuple[int, ...]] = []
    transitions: list[dict[str, int]] = []
    for state_index, state_payload in enumerate(states_payload):
        if not isinstance(state_payload, dict):
            raise ValueError(
                f"Matcher state {state_index} in {resolved_artifact_path} must be an object."
            )
        fail.append(int(state_payload.get("fail", 0)))
        outputs.append(tuple(int(value) for value in state_payload.get("outputs", [])))
        state_transitions: dict[str, int] = {}
        for item in state_payload.get("transitions", []):
            if not isinstance(item, list | tuple) or len(item) != 2:
                raise ValueError(
                    f"Matcher transition in state {state_index} of {resolved_artifact_path} "
                    "must be a [character, next_state] pair."
                )
            character, next_state = item
            state_transitions[str(character)] = int(next_state)
        transitions.append(state_transitions)

    return RuntimeMatcher(
        schema_version=int(artifact_payload.get("schema_version", MATCHER_SCHEMA_VERSION)),
        algorithm=MATCHER_ALGORITHM,
        normalization=MATCHER_NORMALIZATION,
        entry_count=entry_count,
        state_count=int(artifact_payload.get("state_count", len(states_payload))),
        max_alias_length=int(artifact_payload.get("max_alias_length", 0)),
        fail=tuple(fail),
        outputs=tuple(outputs),
        output_lengths=tuple(output_lengths),
        transitions=tuple(transitions),
    )
