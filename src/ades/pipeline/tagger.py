"""Pack-aware deterministic tagger for the v0.1.0 scaffold."""

from __future__ import annotations

from collections import Counter
from html import unescape
from pathlib import Path
import re
from time import perf_counter

from ..packs.registry import PackRegistry
from ..packs.runtime import PackRuntime, RuleDefinition, load_pack_runtime
from ..service.models import (
    EntityLink,
    EntityMatch,
    EntityProvenance,
    TagResponse,
    TopicMatch,
)
from ..version import __version__

TOKEN_RE = re.compile(r"\b[\w][\w.+&/-]*\b")
LINK_VALUE_RE = re.compile(r"[^a-z0-9]+")
MAX_ALIAS_WINDOW_TOKENS = 8


def _language_from_pack(pack: str) -> str:
    if "-" not in pack:
        return "unknown"
    return pack.rsplit("-", 1)[-1]


def _normalize_text(text: str, content_type: str) -> str:
    normalized = text.strip()
    if content_type == "text/html":
        normalized = re.sub(r"<[^>]+>", " ", normalized)
        normalized = unescape(normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


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
) -> EntityLink:
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
) -> EntityProvenance:
    return EntityProvenance(
        match_kind=match_kind,
        match_path=match_path,
        match_source=match_source,
        source_pack=source_pack,
        source_domain=source_domain,
    )


def _extract_rule_entities(text: str, rules: list[RuleDefinition]) -> list[tuple[EntityMatch, str]]:
    extracted: list[tuple[EntityMatch, str]] = []
    for rule in rules:
        for match in re.finditer(rule.pattern, text, flags=re.IGNORECASE):
            matched_text = match.group(0)
            extracted.append(
                (
                    EntityMatch(
                        text=matched_text,
                        label=rule.label,
                        start=match.start(),
                        end=match.end(),
                        confidence=0.85,
                        provenance=_build_provenance(
                            match_kind="rule",
                            match_path="rule.regex",
                            match_source=rule.name,
                            source_pack=rule.source_pack,
                            source_domain=rule.source_domain,
                        ),
                        link=_build_entity_link(
                            provider="rule.regex",
                            pack_id=rule.source_pack,
                            label=rule.label,
                            canonical_text=matched_text,
                        ),
                    ),
                    rule.source_domain,
                )
            )
    return extracted


def _extract_lookup_alias_entities(
    text: str,
    *,
    registry: PackRegistry,
    runtime: PackRuntime,
) -> list[tuple[EntityMatch, str]]:
    extracted: list[tuple[EntityMatch, str]] = []
    allowed_pack_ids = set(runtime.pack_chain)
    lookup_cache: dict[str, list[dict[str, str | float | bool | None]]] = {}

    for start, end, candidate_text in _iter_candidate_windows(text):
        cache_key = candidate_text.casefold()
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

        for candidate in candidates:
            candidate_value = str(candidate["value"])
            candidate_label = str(candidate["label"])
            candidate_pack_id = str(candidate["pack_id"])
            candidate_domain = str(candidate["domain"])
            extracted.append(
                (
                    EntityMatch(
                        text=text[start:end],
                        label=candidate_label,
                        start=start,
                        end=end,
                        confidence=float(candidate.get("score") or 1.0),
                        provenance=_build_provenance(
                            match_kind="alias",
                            match_path="lookup.alias.exact",
                            match_source=candidate_value,
                            source_pack=candidate_pack_id,
                            source_domain=candidate_domain,
                        ),
                        link=_build_entity_link(
                            provider="lookup.alias.exact",
                            pack_id=candidate_pack_id,
                            label=candidate_label,
                            canonical_text=candidate_value,
                        ),
                    ),
                    candidate_domain,
                )
            )
    return extracted


def _dedupe_entities(
    extracted: list[tuple[EntityMatch, str]],
) -> list[tuple[EntityMatch, str]]:
    deduped: dict[tuple[int, int, str, str], tuple[EntityMatch, str]] = {}
    for entity, domain in extracted:
        key = (entity.start, entity.end, entity.label.lower(), entity.text.lower())
        current = deduped.get(key)
        if current is None or (entity.confidence or 0.0) > (current[0].confidence or 0.0):
            deduped[key] = (entity, domain)
    return sorted(
        deduped.values(),
        key=lambda item: (item[0].start, item[0].end, item[0].label, item[0].text.lower()),
    )


def _build_topics(runtime: PackRuntime, extracted: list[tuple[EntityMatch, str]]) -> list[TopicMatch]:
    if not extracted:
        return []

    domain_counts = Counter(domain for _, domain in extracted if domain and domain != "general")
    topics: list[TopicMatch] = []
    for domain, count in sorted(domain_counts.items()):
        topics.append(TopicMatch(label=domain, score=min(1.0, count / 3.0)))

    if not topics:
        topics.append(TopicMatch(label=runtime.domain, score=1.0 if runtime.domain == "general" else 0.5))
    return topics


def tag_text(*, text: str, pack: str, content_type: str, storage_root: Path) -> TagResponse:
    """Tag text using installed pack rules and indexed alias lookup."""

    start = perf_counter()
    warnings: list[str] = []
    normalized = _normalize_text(text, content_type)
    if not normalized:
        warnings.append("empty_input")
    if content_type not in {"text/plain", "text/html"}:
        warnings.append(f"unsupported_content_type:{content_type}")

    registry = PackRegistry(storage_root)
    runtime = load_pack_runtime(storage_root, pack, registry=registry)
    if runtime is None:
        warnings.append(f"pack_not_installed:{pack}")
        elapsed_ms = int((perf_counter() - start) * 1000)
        return TagResponse(
            version=__version__,
            pack=pack,
            language=_language_from_pack(pack),
            content_type=content_type,
            source_path=None,
            entities=[],
            topics=[],
            warnings=warnings,
            timing_ms=elapsed_ms,
        )

    extracted = _extract_rule_entities(normalized, runtime.rules)
    extracted.extend(_extract_lookup_alias_entities(normalized, registry=registry, runtime=runtime))
    deduped = _dedupe_entities(extracted)

    elapsed_ms = int((perf_counter() - start) * 1000)
    return TagResponse(
        version=__version__,
        pack=pack,
        language=runtime.language,
        content_type=content_type,
        source_path=None,
        entities=[entity for entity, _ in deduped],
        topics=_build_topics(runtime, deduped),
        warnings=warnings,
        timing_ms=elapsed_ms,
    )
