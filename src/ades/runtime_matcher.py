"""Deterministic exact-match runtime artifact generation."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from functools import lru_cache
import hashlib
import json
import mmap
from pathlib import Path
import re
import struct
from typing import Any, Iterable, Iterator

from .text_processing import normalize_lookup_text, normalize_text


MATCHER_SCHEMA_VERSION = 1
MATCHER_JSON_AHO_ALGORITHM = "aho_corasick"
MATCHER_TOKEN_TRIE_ALGORITHM = "token_trie_v1"
MATCHER_DEFAULT_ALGORITHM = MATCHER_TOKEN_TRIE_ALGORITHM
MATCHER_NORMALIZATION = "normalize_lookup_text:v1"
_MATCHER_JSON_CHUNK_SIZE = 1024 * 1024
_TOKEN_RE = re.compile(r"\b[\w][\w.+&/-]*\b")
_TOKEN_TRIE_MAGIC = b"ADESTT01"
_TOKEN_TRIE_VERSION = 1
_TOKEN_TRIE_ENTRIES_MAGIC = b"ADESTE01"
_TOKEN_TRIE_ENTRIES_VERSION = 1
_TOKEN_TRIE_INDEX_MAGIC = b"ADESTI01"
_TOKEN_TRIE_INDEX_VERSION = 1
_TOKEN_TRIE_STATE_INDEX_MAGIC = b"ADESTS01"
_TOKEN_TRIE_STATE_INDEX_VERSION = 1


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
class MatcherEntryPayload:
    """Resolved alias metadata stored beside the matcher artifact."""

    text: str
    label: str
    canonical_text: str
    entity_id: str
    score: float
    alias_score: float
    generated: bool
    source_name: str
    source_priority: float
    popularity_weight: float
    source_domain: str


@dataclass(frozen=True)
class ExactMatchCandidate:
    """One exact-match span plus the resolved entry indexes for that span."""

    start: int
    end: int
    entry_indices: tuple[int, ...]


@dataclass(frozen=True)
class TokenTrieEntryPayloadStore:
    """Lazy indexed access to token-trie entry payloads."""

    entries_path: str
    index_path: str
    entry_count: int

    def __len__(self) -> int:
        return self.entry_count

    def __bool__(self) -> bool:
        return self.entry_count > 0

    def __getitem__(self, index: int | slice) -> MatcherEntryPayload | tuple[MatcherEntryPayload, ...]:
        if isinstance(index, slice):
            start, stop, step = index.indices(self.entry_count)
            return tuple(self[position] for position in range(start, stop, step))
        normalized_index = index
        if normalized_index < 0:
            normalized_index += self.entry_count
        if normalized_index < 0 or normalized_index >= self.entry_count:
            raise IndexError(index)
        return _load_token_trie_entry_payload_at(
            self.entries_path,
            self.index_path,
            normalized_index,
            self.entry_count,
        )

    def __iter__(self) -> Iterator[MatcherEntryPayload]:
        for index in range(self.entry_count):
            yield self[index]


@dataclass(frozen=True)
class TokenTrieStateData:
    """One token-trie state resolved from the binary artifact."""

    outputs: tuple[int, ...]
    transitions: dict[int, int]


@dataclass(frozen=True)
class TokenTrieStateStore:
    """Lazy indexed access to token-trie state payloads."""

    artifact_path: str
    index_path: str
    state_count: int

    def __len__(self) -> int:
        return self.state_count

    def __bool__(self) -> bool:
        return self.state_count > 0

    def __getitem__(self, index: int) -> TokenTrieStateData:
        normalized_index = index
        if normalized_index < 0:
            normalized_index += self.state_count
        if normalized_index < 0 or normalized_index >= self.state_count:
            raise IndexError(index)
        return _load_token_trie_state_at(
            self.artifact_path,
            self.index_path,
            normalized_index,
            self.state_count,
        )


@dataclass(frozen=True)
class RuntimeMatcher:
    """Loaded read-only exact-match runtime artifact."""

    schema_version: int
    algorithm: str
    normalization: str
    entry_count: int
    state_count: int
    max_alias_length: int
    fail: tuple[int, ...] = ()
    outputs: tuple[tuple[int, ...], ...] = ()
    output_lengths: tuple[int, ...] = ()
    transitions: tuple[dict[str, int], ...] = ()
    entry_payloads: tuple[MatcherEntryPayload, ...] | TokenTrieEntryPayloadStore = ()
    token_to_id: dict[str, int] = field(default_factory=dict)
    token_outputs: tuple[tuple[int, ...], ...] = ()
    token_transitions: tuple[dict[int, int], ...] = ()
    token_state_store: TokenTrieStateStore | None = None


def build_matcher_artifact_from_alias_payloads(
    alias_payloads: Iterable[dict[str, Any]],
    *,
    output_dir: str | Path,
    algorithm: str = MATCHER_DEFAULT_ALGORITHM,
) -> MatcherArtifactResult:
    """Compile a deterministic exact matcher artifact from normalized alias records."""

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    if algorithm == MATCHER_JSON_AHO_ALGORITHM:
        artifact_path = resolved_output_dir / "automaton.json"
        entries_path = resolved_output_dir / "entries.jsonl"
    elif algorithm == MATCHER_TOKEN_TRIE_ALGORITHM:
        artifact_path = resolved_output_dir / "token-trie.bin"
        entries_path = resolved_output_dir / "entries.bin"
    else:
        raise ValueError(f"Unsupported matcher algorithm: {algorithm}")

    entry_count = 0
    max_alias_length = 0
    json_aho_transitions: list[dict[str, int]] | None = None
    json_aho_terminal_outputs: list[list[int]] | None = None
    token_to_id: dict[str, int] | None = None
    token_pattern_outputs: dict[tuple[int, ...], list[int]] | None = None
    if algorithm == MATCHER_JSON_AHO_ALGORITHM:
        json_aho_transitions = [{}]
        json_aho_terminal_outputs = [[]]
    else:
        token_to_id = {}
        token_pattern_outputs = {}

    if algorithm == MATCHER_JSON_AHO_ALGORITHM:
        with entries_path.open("w", encoding="utf-8") as handle:
            for entry_index, alias_payload in enumerate(alias_payloads):
                normalized_entry = _normalize_alias_payload(alias_payload)
                handle.write(json.dumps(normalized_entry, sort_keys=True) + "\n")
                entry_count += 1

                normalized_text = normalized_entry["normalized_text"]
                max_alias_length = max(max_alias_length, len(normalized_text))
                assert json_aho_transitions is not None
                assert json_aho_terminal_outputs is not None
                state = 0
                for character in normalized_text:
                    next_state = json_aho_transitions[state].get(character)
                    if next_state is None:
                        next_state = len(json_aho_transitions)
                        json_aho_transitions[state][character] = next_state
                        json_aho_transitions.append({})
                        json_aho_terminal_outputs.append([])
                    state = next_state
                json_aho_terminal_outputs[state].append(entry_index)
    else:
        index_path = entries_path.with_suffix(".idx")
        with entries_path.open("wb") as handle:
            handle.write(_TOKEN_TRIE_ENTRIES_MAGIC)
            handle.write(struct.pack(">II", _TOKEN_TRIE_ENTRIES_VERSION, 0))
            with index_path.open("wb") as index_handle:
                index_handle.write(_TOKEN_TRIE_INDEX_MAGIC)
                index_handle.write(struct.pack(">II", _TOKEN_TRIE_INDEX_VERSION, 0))
                for entry_index, alias_payload in enumerate(alias_payloads):
                    normalized_entry = _normalize_alias_payload(alias_payload)
                    index_handle.write(struct.pack(">Q", handle.tell()))
                    _write_token_trie_entry_payload(handle, normalized_entry)
                    entry_count += 1

                    normalized_text = normalized_entry["normalized_text"]
                    max_alias_length = max(max_alias_length, len(normalized_text))
                    assert token_to_id is not None
                    assert token_pattern_outputs is not None
                    token_ids = _token_ids_for_text(normalized_text, token_to_id)
                    if token_ids:
                        token_pattern_outputs.setdefault(token_ids, []).append(entry_index)
                handle.seek(len(_TOKEN_TRIE_ENTRIES_MAGIC))
                handle.write(struct.pack(">II", _TOKEN_TRIE_ENTRIES_VERSION, entry_count))
                index_handle.seek(len(_TOKEN_TRIE_INDEX_MAGIC))
                index_handle.write(struct.pack(">II", _TOKEN_TRIE_INDEX_VERSION, entry_count))

    if algorithm == MATCHER_JSON_AHO_ALGORITHM:
        assert json_aho_transitions is not None
        assert json_aho_terminal_outputs is not None
        state_count = _write_json_aho_artifact(
            artifact_path=artifact_path,
            transitions=json_aho_transitions,
            terminal_outputs=json_aho_terminal_outputs,
            entry_count=entry_count,
            max_alias_length=max_alias_length,
        )
    else:
        assert token_to_id is not None
        assert token_pattern_outputs is not None
        state_count = _write_token_trie_artifact(
            artifact_path=artifact_path,
            token_to_id=token_to_id,
            pattern_outputs=token_pattern_outputs,
            entry_count=entry_count,
            max_alias_length=max_alias_length,
        )

    return MatcherArtifactResult(
        algorithm=algorithm,
        normalization=MATCHER_NORMALIZATION,
        artifact_path=str(artifact_path),
        entries_path=str(entries_path),
        entry_count=entry_count,
        state_count=state_count,
        max_alias_length=max_alias_length,
        artifact_sha256=_sha256_file(artifact_path),
        entries_sha256=_sha256_file(entries_path),
    )


def build_matcher_artifact_from_aliases_json(
    aliases_path: str | Path,
    *,
    output_dir: str | Path,
    algorithm: str = MATCHER_DEFAULT_ALGORITHM,
    include_runtime_tiers: set[str] | None = None,
) -> MatcherArtifactResult:
    """Compile a deterministic exact matcher artifact from aliases.json."""

    resolved_aliases_path = Path(aliases_path).expanduser().resolve()
    if not resolved_aliases_path.exists():
        raise FileNotFoundError(f"Alias payload not found: {resolved_aliases_path}")
    if not resolved_aliases_path.is_file():
        raise FileExistsError(f"Alias payload is not a file: {resolved_aliases_path}")
    return build_matcher_artifact_from_alias_payloads(
        _iter_alias_payloads_from_aliases_json(
            resolved_aliases_path,
            include_runtime_tiers=include_runtime_tiers,
        ),
        output_dir=output_dir,
        algorithm=algorithm,
    )


def build_lookup_text_view(value: str) -> LookupTextView:
    """Return the shared lookup normalization view for runtime matching."""

    normalized = normalize_text(value, "text/plain")
    lookup_chars: list[str] = []
    lookup_to_source: list[int] = []
    lookup_to_source_end: list[int] = []
    for index, character in enumerate(normalized.text):
        raw_start = normalized.normalized_to_raw[index]
        raw_end = normalized.normalized_to_raw_end[index]
        for folded in character.casefold():
            lookup_chars.append(folded)
            lookup_to_source.append(raw_start)
            lookup_to_source_end.append(raw_end)
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

    return [
        (candidate.start, candidate.end)
        for candidate in find_exact_match_candidates(text, matcher)
    ]


def find_exact_match_candidates(
    text: str,
    matcher: RuntimeMatcher,
) -> list[ExactMatchCandidate]:
    """Scan text and return exact-match spans plus resolved matcher entry indexes."""

    if not text:
        return []
    view = build_lookup_text_view(text)
    return find_exact_match_candidates_in_view(view, matcher)


def find_exact_match_candidates_in_view(
    view: LookupTextView,
    matcher: RuntimeMatcher,
) -> list[ExactMatchCandidate]:
    """Scan a prebuilt lookup view and return exact-match candidates."""

    if matcher.algorithm == MATCHER_JSON_AHO_ALGORITHM:
        return _find_json_aho_match_candidates(view, matcher)
    if matcher.algorithm == MATCHER_TOKEN_TRIE_ALGORITHM:
        return _find_token_trie_match_candidates(view, matcher)
    raise ValueError(f"Unsupported runtime matcher algorithm: {matcher.algorithm}")


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


def _iter_alias_payloads_from_aliases_json(
    path: Path,
    *,
    include_runtime_tiers: set[str] | None = None,
) -> Iterator[dict[str, Any]]:
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
                if include_runtime_tiers is not None:
                    runtime_tier = str(
                        entry.get("runtime_tier", "runtime_exact_high_precision")
                    )
                    if runtime_tier not in include_runtime_tiers:
                        continue
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


def _tokenize_lookup_text(value: str) -> tuple[str, ...]:
    return tuple(match.group(0) for match in _TOKEN_RE.finditer(value))


def _token_ids_for_text(
    normalized_text: str,
    token_to_id: dict[str, int],
) -> tuple[int, ...]:
    token_ids: list[int] = []
    for token in _tokenize_lookup_text(normalized_text):
        token_id = token_to_id.get(token)
        if token_id is None:
            token_id = len(token_to_id) + 1
            token_to_id[token] = token_id
        token_ids.append(token_id)
    return tuple(token_ids)


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


def _write_json_aho_artifact(
    *,
    artifact_path: Path,
    transitions: list[dict[str, int]],
    terminal_outputs: list[list[int]],
    entry_count: int,
    max_alias_length: int,
) -> int:
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
        "algorithm": MATCHER_JSON_AHO_ALGORITHM,
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
    return len(serialized_states)


def _write_token_trie_artifact(
    *,
    artifact_path: Path,
    token_to_id: dict[str, int],
    pattern_outputs: dict[tuple[int, ...], list[int]],
    entry_count: int,
    max_alias_length: int,
) -> int:
    trie_transitions: list[dict[int, int]] = [{}]
    terminal_outputs: list[list[int]] = [[]]

    for token_ids in sorted(pattern_outputs):
        state = 0
        for token_id in token_ids:
            next_state = trie_transitions[state].get(token_id)
            if next_state is None:
                next_state = len(trie_transitions)
                trie_transitions[state][token_id] = next_state
                trie_transitions.append({})
                terminal_outputs.append([])
            state = next_state
        terminal_outputs[state].extend(pattern_outputs[token_ids])

    tokens_by_id = [""] * len(token_to_id)
    for token, token_id in token_to_id.items():
        tokens_by_id[token_id - 1] = token

    state_index_path = _token_trie_state_index_path(artifact_path)
    with artifact_path.open("wb") as handle:
        with state_index_path.open("wb") as index_handle:
            index_handle.write(_TOKEN_TRIE_STATE_INDEX_MAGIC)
            index_handle.write(
                struct.pack(">II", _TOKEN_TRIE_STATE_INDEX_VERSION, len(trie_transitions))
            )
            handle.write(_TOKEN_TRIE_MAGIC)
            handle.write(
                struct.pack(
                    ">IIIIII",
                    _TOKEN_TRIE_VERSION,
                    entry_count,
                    len(token_to_id),
                    len(trie_transitions),
                    len(pattern_outputs),
                    max_alias_length,
                )
            )
            for token in tokens_by_id:
                encoded = token.encode("utf-8")
                handle.write(struct.pack(">I", len(encoded)))
                handle.write(encoded)
            for state_index, state_transitions in enumerate(trie_transitions):
                index_handle.write(struct.pack(">Q", handle.tell()))
                handle.write(
                    struct.pack(
                        ">II",
                        len(terminal_outputs[state_index]),
                        len(state_transitions),
                    )
                )
                for output_index in terminal_outputs[state_index]:
                    handle.write(struct.pack(">I", output_index))
                for token_id, child_state in sorted(state_transitions.items()):
                    handle.write(struct.pack(">II", token_id, child_state))

    return len(trie_transitions)


def _find_json_aho_match_candidates(
    view: LookupTextView,
    matcher: RuntimeMatcher,
) -> list[ExactMatchCandidate]:
    token_starts, token_ends = _token_boundary_sets(view.text)
    state = 0
    span_outputs: dict[tuple[int, int], set[int]] = {}
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
            if not _is_token_window_span(
                start,
                index + 1,
                token_starts=token_starts,
                token_ends=token_ends,
            ):
                continue
            span = view.source_span(start, index + 1)
            span_outputs.setdefault(span, set()).add(output_index)
    return _finalize_exact_match_candidates(span_outputs)


def _find_token_trie_match_candidates(
    view: LookupTextView,
    matcher: RuntimeMatcher,
) -> list[ExactMatchCandidate]:
    if not matcher.token_to_id or (matcher.token_state_store is None and not matcher.token_transitions):
        return []
    token_matches = list(_TOKEN_RE.finditer(view.text))
    if not token_matches:
        return []
    token_ids = [matcher.token_to_id.get(match.group(0), 0) for match in token_matches]
    span_outputs: dict[tuple[int, int], set[int]] = {}
    for start_index, start_match in enumerate(token_matches):
        state = 0
        for end_index in range(start_index, len(token_matches)):
            token_id = token_ids[end_index]
            if token_id <= 0:
                break
            current_state = _token_trie_state(matcher, state)
            next_state = current_state.transitions.get(token_id)
            if next_state is None:
                break
            state = next_state
            output_indexes = _token_trie_state(matcher, state).outputs
            if not output_indexes:
                continue
            end_match = token_matches[end_index]
            span = view.source_span(start_match.start(), end_match.end())
            span_outputs.setdefault(span, set()).update(output_indexes)
    return _finalize_exact_match_candidates(span_outputs)


def _finalize_exact_match_candidates(
    span_outputs: dict[tuple[int, int], set[int]],
) -> list[ExactMatchCandidate]:
    return [
        ExactMatchCandidate(
            start=span[0],
            end=span[1],
            entry_indices=tuple(sorted(output_indexes)),
        )
        for span, output_indexes in sorted(
            span_outputs.items(),
            key=lambda item: (item[0][0], -(item[0][1] - item[0][0]), item[0][1]),
        )
    ]


def _token_trie_state(matcher: RuntimeMatcher, state_index: int) -> TokenTrieStateData:
    state_store = matcher.token_state_store
    if state_store is not None:
        return state_store[state_index]
    return TokenTrieStateData(
        outputs=matcher.token_outputs[state_index],
        transitions=matcher.token_transitions[state_index],
    )


def _looks_like_token_trie_artifact(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    with path.open("rb") as handle:
        return handle.read(len(_TOKEN_TRIE_MAGIC)) == _TOKEN_TRIE_MAGIC


def _looks_like_token_trie_entries(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    with path.open("rb") as handle:
        return handle.read(len(_TOKEN_TRIE_ENTRIES_MAGIC)) == _TOKEN_TRIE_ENTRIES_MAGIC


def _looks_like_token_trie_index(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    with path.open("rb") as handle:
        return handle.read(len(_TOKEN_TRIE_INDEX_MAGIC)) == _TOKEN_TRIE_INDEX_MAGIC


def _looks_like_token_trie_state_index(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    with path.open("rb") as handle:
        return handle.read(len(_TOKEN_TRIE_STATE_INDEX_MAGIC)) == _TOKEN_TRIE_STATE_INDEX_MAGIC


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
    if _looks_like_token_trie_artifact(resolved_artifact_path):
        return _load_token_trie_matcher(resolved_artifact_path, resolved_entries_path)
    return _load_json_aho_matcher(resolved_artifact_path, resolved_entries_path)


def _load_json_aho_matcher(
    artifact_path: Path,
    entries_path: Path,
) -> RuntimeMatcher:
    artifact_payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    if str(artifact_payload.get("algorithm")) != MATCHER_JSON_AHO_ALGORITHM:
        raise ValueError(
            f"Unsupported matcher algorithm in {artifact_path}: "
            f"{artifact_payload.get('algorithm')}"
        )
    if str(artifact_payload.get("normalization")) != MATCHER_NORMALIZATION:
        raise ValueError(
            f"Unsupported matcher normalization in {artifact_path}: "
            f"{artifact_payload.get('normalization')}"
        )

    output_lengths: list[int] = []
    entry_payloads: list[MatcherEntryPayload] = []
    with entries_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            entry_payload = _matcher_entry_payload_from_mapping(payload)
            normalized_text = normalize_lookup_text(entry_payload.text)
            if not normalized_text:
                raise ValueError(
                    f"Matcher entry {line_number} in {entries_path} is missing normalized_text."
                )
            output_lengths.append(len(normalized_text))
            entry_payloads.append(entry_payload)

    entry_count = int(artifact_payload.get("entry_count", 0))
    if entry_count != len(output_lengths):
        raise ValueError(
            f"Matcher entry count mismatch for {artifact_path}: "
            f"artifact={entry_count}, entries={len(output_lengths)}"
        )

    states_payload = artifact_payload.get("states")
    if not isinstance(states_payload, list):
        raise ValueError(f"Matcher artifact in {artifact_path} is missing states.")

    fail: list[int] = []
    outputs: list[tuple[int, ...]] = []
    transitions: list[dict[str, int]] = []
    for state_index, state_payload in enumerate(states_payload):
        if not isinstance(state_payload, dict):
            raise ValueError(
                f"Matcher state {state_index} in {artifact_path} must be an object."
            )
        fail.append(int(state_payload.get("fail", 0)))
        outputs.append(tuple(int(value) for value in state_payload.get("outputs", [])))
        state_transitions: dict[str, int] = {}
        for item in state_payload.get("transitions", []):
            if not isinstance(item, list | tuple) or len(item) != 2:
                raise ValueError(
                    f"Matcher transition in state {state_index} of {artifact_path} "
                    "must be a [character, next_state] pair."
                )
            character, next_state = item
            state_transitions[str(character)] = int(next_state)
        transitions.append(state_transitions)

    return RuntimeMatcher(
        schema_version=int(artifact_payload.get("schema_version", MATCHER_SCHEMA_VERSION)),
        algorithm=MATCHER_JSON_AHO_ALGORITHM,
        normalization=MATCHER_NORMALIZATION,
        entry_count=entry_count,
        state_count=int(artifact_payload.get("state_count", len(states_payload))),
        max_alias_length=int(artifact_payload.get("max_alias_length", 0)),
        fail=tuple(fail),
        outputs=tuple(outputs),
        output_lengths=tuple(output_lengths),
        transitions=tuple(transitions),
        entry_payloads=tuple(entry_payloads),
    )


def _load_token_trie_matcher(artifact_path: Path, entries_path: Path) -> RuntimeMatcher:
    index_path = _ensure_token_trie_entry_index(entries_path)
    with artifact_path.open("rb") as handle:
        magic = handle.read(len(_TOKEN_TRIE_MAGIC))
        if magic != _TOKEN_TRIE_MAGIC:
            raise ValueError(f"Unsupported token-trie artifact: {artifact_path}")
        (
            version,
            entry_count,
            token_vocab_count,
            state_count,
            _unique_pattern_count,
            max_alias_length,
        ) = struct.unpack(">IIIIII", handle.read(24))
        if version != _TOKEN_TRIE_VERSION:
            raise ValueError(f"Unsupported token-trie artifact version: {version}")
        id_to_token: list[str] = []
        for _ in range(token_vocab_count):
            (length,) = struct.unpack(">I", handle.read(4))
            id_to_token.append(handle.read(length).decode("utf-8"))
    state_index_path = _ensure_token_trie_state_index(
        artifact_path,
        token_vocab_count=token_vocab_count,
        state_count=state_count,
    )
    token_to_id = {token: index + 1 for index, token in enumerate(id_to_token)}
    return RuntimeMatcher(
        schema_version=MATCHER_SCHEMA_VERSION,
        algorithm=MATCHER_TOKEN_TRIE_ALGORITHM,
        normalization=MATCHER_NORMALIZATION,
        entry_count=entry_count,
        state_count=state_count,
        max_alias_length=max_alias_length,
        entry_payloads=TokenTrieEntryPayloadStore(
            entries_path=str(entries_path),
            index_path=str(index_path),
            entry_count=entry_count,
        ),
        token_to_id=token_to_id,
        token_state_store=TokenTrieStateStore(
            artifact_path=str(artifact_path),
            index_path=str(state_index_path),
            state_count=state_count,
        ),
    )


def _token_trie_entry_index_path(entries_path: Path) -> Path:
    return entries_path.with_suffix(".idx")


def _token_trie_state_index_path(artifact_path: Path) -> Path:
    return artifact_path.with_suffix(".stateidx")


def _ensure_token_trie_entry_index(entries_path: Path) -> Path:
    index_path = _token_trie_entry_index_path(entries_path)
    if _looks_like_token_trie_index(index_path):
        with index_path.open("rb") as handle:
            handle.read(len(_TOKEN_TRIE_INDEX_MAGIC))
            version, entry_count = struct.unpack(">II", handle.read(8))
            if version == _TOKEN_TRIE_INDEX_VERSION and entry_count >= 0:
                return index_path
    _build_token_trie_entry_index(entries_path, index_path)
    return index_path


def _build_token_trie_entry_index(entries_path: Path, index_path: Path) -> None:
    temporary_index_path = index_path.with_suffix(index_path.suffix + ".tmp")
    with entries_path.open("rb") as entries_handle:
        magic = entries_handle.read(len(_TOKEN_TRIE_ENTRIES_MAGIC))
        if magic != _TOKEN_TRIE_ENTRIES_MAGIC:
            raise ValueError(f"Unsupported token-trie entries artifact: {entries_path}")
        version, entry_count = struct.unpack(">II", entries_handle.read(8))
        if version != _TOKEN_TRIE_ENTRIES_VERSION:
            raise ValueError(f"Unsupported token-trie entries version: {version}")
        with temporary_index_path.open("wb") as index_handle:
            index_handle.write(_TOKEN_TRIE_INDEX_MAGIC)
            index_handle.write(struct.pack(">II", _TOKEN_TRIE_INDEX_VERSION, entry_count))
            for _ in range(entry_count):
                index_handle.write(struct.pack(">Q", entries_handle.tell()))
                _skip_token_trie_entry_payload(entries_handle)
    temporary_index_path.replace(index_path)


def _ensure_token_trie_state_index(
    artifact_path: Path,
    *,
    token_vocab_count: int,
    state_count: int,
) -> Path:
    index_path = _token_trie_state_index_path(artifact_path)
    if _looks_like_token_trie_state_index(index_path):
        with index_path.open("rb") as handle:
            handle.read(len(_TOKEN_TRIE_STATE_INDEX_MAGIC))
            version, indexed_state_count = struct.unpack(">II", handle.read(8))
            if version == _TOKEN_TRIE_STATE_INDEX_VERSION and indexed_state_count == state_count:
                return index_path
    _build_token_trie_state_index(
        artifact_path,
        index_path,
        token_vocab_count=token_vocab_count,
        state_count=state_count,
    )
    return index_path


def _build_token_trie_state_index(
    artifact_path: Path,
    index_path: Path,
    *,
    token_vocab_count: int,
    state_count: int,
) -> None:
    temporary_index_path = index_path.with_suffix(index_path.suffix + ".tmp")
    with artifact_path.open("rb") as artifact_handle:
        magic = artifact_handle.read(len(_TOKEN_TRIE_MAGIC))
        if magic != _TOKEN_TRIE_MAGIC:
            raise ValueError(f"Unsupported token-trie artifact: {artifact_path}")
        version, _entry_count, indexed_token_vocab_count, indexed_state_count, _unique_patterns, _max_alias_length = struct.unpack(
            ">IIIIII",
            artifact_handle.read(24),
        )
        if version != _TOKEN_TRIE_VERSION:
            raise ValueError(f"Unsupported token-trie artifact version: {version}")
        if indexed_token_vocab_count != token_vocab_count or indexed_state_count != state_count:
            raise ValueError(
                f"Token-trie artifact header mismatch while indexing {artifact_path}."
            )
        for _ in range(token_vocab_count):
            (length,) = struct.unpack(">I", artifact_handle.read(4))
            artifact_handle.seek(length, 1)
        with temporary_index_path.open("wb") as index_handle:
            index_handle.write(_TOKEN_TRIE_STATE_INDEX_MAGIC)
            index_handle.write(struct.pack(">II", _TOKEN_TRIE_STATE_INDEX_VERSION, state_count))
            for _ in range(state_count):
                index_handle.write(struct.pack(">Q", artifact_handle.tell()))
                _skip_token_trie_state_payload(artifact_handle)
    temporary_index_path.replace(index_path)


def _load_token_trie_entry_payloads(path: Path) -> tuple[MatcherEntryPayload, ...]:
    if not _looks_like_token_trie_entries(path):
        return tuple(_load_matcher_entry_payloads_from_jsonl(path))
    with path.open("rb") as handle:
        magic = handle.read(len(_TOKEN_TRIE_ENTRIES_MAGIC))
        if magic != _TOKEN_TRIE_ENTRIES_MAGIC:
            raise ValueError(f"Unsupported token-trie entries artifact: {path}")
        version, entry_count = struct.unpack(">II", handle.read(8))
        if version != _TOKEN_TRIE_ENTRIES_VERSION:
            raise ValueError(f"Unsupported token-trie entries version: {version}")
        payloads = [
            _read_token_trie_entry_payload(handle)
            for _ in range(entry_count)
        ]
    return tuple(payloads)


@dataclass(frozen=True)
class _MappedBinaryFile:
    """Pinned file descriptor plus mmap for one matcher sidecar."""

    handle: Any
    mapping: mmap.mmap


@lru_cache(maxsize=8)
def _open_mapped_binary_file(path: str) -> _MappedBinaryFile:
    handle = Path(path).open("rb")
    mapping = mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ)
    return _MappedBinaryFile(handle=handle, mapping=mapping)


@lru_cache(maxsize=262144)
def _load_token_trie_entry_payload_at(
    entries_path: str,
    index_path: str,
    entry_index: int,
    entry_count: int,
) -> MatcherEntryPayload:
    if entry_index < 0 or entry_index >= entry_count:
        raise IndexError(entry_index)
    entry_offset = _load_token_trie_entry_offset(index_path, entry_index, entry_count)
    buffer = _open_mapped_binary_file(entries_path).mapping
    return _read_token_trie_entry_payload_from_buffer(buffer, entry_offset)


def _load_token_trie_entry_offset(index_path: str, entry_index: int, entry_count: int) -> int:
    if entry_index < 0 or entry_index >= entry_count:
        raise IndexError(entry_index)
    buffer = _open_mapped_binary_file(index_path).mapping
    header_size = len(_TOKEN_TRIE_INDEX_MAGIC) + 8
    (offset,) = struct.unpack_from(">Q", buffer, header_size + (entry_index * 8))
    return int(offset)


@lru_cache(maxsize=262144)
def _load_token_trie_state_at(
    artifact_path: str,
    index_path: str,
    state_index: int,
    state_count: int,
) -> TokenTrieStateData:
    if state_index < 0 or state_index >= state_count:
        raise IndexError(state_index)
    state_offset = _load_token_trie_state_offset(index_path, state_index, state_count)
    buffer = _open_mapped_binary_file(artifact_path).mapping
    return _read_token_trie_state_from_buffer(buffer, state_offset)


def _load_token_trie_state_offset(index_path: str, state_index: int, state_count: int) -> int:
    if state_index < 0 or state_index >= state_count:
        raise IndexError(state_index)
    buffer = _open_mapped_binary_file(index_path).mapping
    header_size = len(_TOKEN_TRIE_STATE_INDEX_MAGIC) + 8
    (offset,) = struct.unpack_from(">Q", buffer, header_size + (state_index * 8))
    return int(offset)


def _load_matcher_entry_payloads_from_jsonl(path: Path) -> list[MatcherEntryPayload]:
    payloads: list[MatcherEntryPayload] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid matcher entry {line_number} in {path}.") from exc
            payloads.append(_matcher_entry_payload_from_mapping(payload))
    return payloads


def _matcher_entry_payload_from_mapping(payload: dict[str, Any]) -> MatcherEntryPayload:
    return MatcherEntryPayload(
        text=str(payload.get("text", "")).strip(),
        label=str(payload.get("label", "")).strip(),
        canonical_text=str(payload.get("canonical_text", "")).strip(),
        entity_id=str(payload.get("entity_id", "")).strip(),
        score=round(float(payload.get("score", payload.get("alias_score", 1.0))), 4),
        alias_score=round(
            float(payload.get("alias_score", payload.get("score", 1.0))),
            4,
        ),
        generated=bool(payload.get("generated", False)),
        source_name=str(payload.get("source_name", "")).strip(),
        source_priority=round(float(payload.get("source_priority", 0.6)), 4),
        popularity_weight=round(float(payload.get("popularity_weight", 0.5)), 4),
        source_domain=str(payload.get("source_domain", "")).strip(),
    )


def _write_token_trie_entry_payload(handle: Any, payload: dict[str, Any]) -> None:
    handle.write(
        struct.pack(
            ">Bddd",
            1 if bool(payload.get("generated", False)) else 0,
            float(payload["score"]),
            float(payload["source_priority"]),
            float(payload["popularity_weight"]),
        )
    )
    for value in (
        str(payload["text"]),
        str(payload["label"]),
        str(payload["canonical_text"]),
        str(payload["entity_id"]),
        str(payload["source_name"]),
        str(payload["source_domain"]),
    ):
        encoded = value.encode("utf-8")
        handle.write(struct.pack(">I", len(encoded)))
        handle.write(encoded)


def _skip_token_trie_state_payload(handle: Any) -> None:
    output_count, transition_count = struct.unpack(">II", handle.read(8))
    handle.seek((output_count * 4) + (transition_count * 8), 1)


def _read_token_trie_state_from_buffer(
    buffer: mmap.mmap,
    offset: int,
) -> TokenTrieStateData:
    output_count, transition_count = struct.unpack_from(">II", buffer, offset)
    cursor = offset + 8
    outputs = tuple(
        struct.unpack_from(">I", buffer, cursor + (index * 4))[0]
        for index in range(output_count)
    )
    cursor += output_count * 4
    transitions: dict[int, int] = {}
    for _ in range(transition_count):
        token_id, child_state = struct.unpack_from(">II", buffer, cursor)
        transitions[token_id] = child_state
        cursor += 8
    return TokenTrieStateData(outputs=outputs, transitions=transitions)


def _skip_token_trie_entry_payload(handle: Any) -> None:
    handle.seek(25, 1)
    for _ in range(6):
        (length,) = struct.unpack(">I", handle.read(4))
        handle.seek(length, 1)


def _read_token_trie_entry_payload(handle: Any) -> MatcherEntryPayload:
    generated_flag, score, source_priority, popularity_weight = struct.unpack(
        ">Bddd",
        handle.read(25),
    )
    values = [_read_length_prefixed_text(handle) for _ in range(6)]
    return MatcherEntryPayload(
        text=values[0],
        label=values[1],
        canonical_text=values[2],
        entity_id=values[3],
        score=round(float(score), 4),
        alias_score=round(float(score), 4),
        generated=bool(generated_flag),
        source_name=values[4],
        source_priority=round(float(source_priority), 4),
        popularity_weight=round(float(popularity_weight), 4),
        source_domain=values[5],
    )


def _read_token_trie_entry_payload_from_buffer(
    buffer: mmap.mmap,
    offset: int,
) -> MatcherEntryPayload:
    generated_flag, score, source_priority, popularity_weight = struct.unpack_from(
        ">Bddd",
        buffer,
        offset,
    )
    cursor = offset + 25
    values: list[str] = []
    for _ in range(6):
        length = struct.unpack_from(">I", buffer, cursor)[0]
        cursor += 4
        values.append(buffer[cursor : cursor + length].decode("utf-8"))
        cursor += length
    return MatcherEntryPayload(
        text=values[0],
        label=values[1],
        canonical_text=values[2],
        entity_id=values[3],
        score=round(float(score), 4),
        alias_score=round(float(score), 4),
        generated=bool(generated_flag),
        source_name=values[4],
        source_priority=round(float(source_priority), 4),
        popularity_weight=round(float(popularity_weight), 4),
        source_domain=values[5],
    )


def _read_length_prefixed_text(handle: Any) -> str:
    (length,) = struct.unpack(">I", handle.read(4))
    return handle.read(length).decode("utf-8")
