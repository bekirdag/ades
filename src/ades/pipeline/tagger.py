"""Pack-aware local entity extraction."""

from __future__ import annotations

from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass, replace
import os
from pathlib import Path
import re
from time import perf_counter

from ..packs.registry import PackRegistry
from ..packs.runtime import PackRuntime, RuleDefinition, load_pack_runtime
from .hybrid import ProposalProvider, ProposalSpan, get_proposal_spans, hybrid_enabled
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
INITIALISM_TOKEN_RE = re.compile(r"(?:[A-Za-z]\.){2,}|[A-Za-z][A-Za-z'’-]*")
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
_NUMBER_SINGLE_TOKEN_WORDS = {
    "eight",
    "eighteen",
    "eighty",
    "eleven",
    "fifteen",
    "fifty",
    "five",
    "forty",
    "four",
    "fourteen",
    "hundred",
    "nine",
    "nineteen",
    "ninety",
    "one",
    "seven",
    "seventeen",
    "seventy",
    "six",
    "sixteen",
    "sixty",
    "ten",
    "thirteen",
    "thirty",
    "thousand",
    "three",
    "twelve",
    "twenty",
    "two",
    "zero",
}
_GENERIC_PHRASE_LEADS = {
    "a",
    "an",
    "any",
    "but",
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
_PERSON_NAME_SUFFIX_TOKENS = {
    "i",
    "ii",
    "iii",
    "iv",
    "jr",
    "jr.",
    "sr",
    "sr.",
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
    "congress",
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
_RUNTIME_GENERIC_SINGLE_TOKEN_ALIAS_TEXTS = {
    "brits",
    "canadian",
    "fc",
    "linda",
    "many",
    "proof",
    "queen",
    "save",
    "taurasi",
    "unknown",
}
_RUNTIME_GENERIC_ORG_ACRONYM_ALIAS_TEXTS = {"ct", "not", "pm", "uk", "us"}
_RUNTIME_ATTRIBUTION_CONTEXTUAL_ORG_ACRONYM_FOLLOWING_TOKENS = {
    "news",
    "programme",
    "program",
    "radio",
    "sport",
    "that",
    "tv",
}
_RUNTIME_GENERIC_PHRASE_ALIAS_TEXTS = {
    "central command",
    "cut off",
    "high school",
    "high level",
    "john i",
    "let the acm network",
    "many springs",
    "more than",
    "new deal",
    "tamim bin hamad al",
    "u-turn",
    "with us",
}
_ARTICLE_LED_STRUCTURAL_FRAGMENT_TOKENS = {
    "international",
    "local",
    "national",
    "regional",
}
_RUNTIME_ARTICLE_LED_GENERIC_ALIAS_TOKENS = (
    _GENERIC_SINGLE_TOKEN_HEADS
    | _GENERIC_PHRASE_LEADS
    | _ARTICLE_LED_STRUCTURAL_FRAGMENT_TOKENS
    | {"art", "board", "public", "village", "washington"}
)
_RUNTIME_FUNCTION_YEAR_ALIAS_RE = re.compile(
    r"^(?:and|by|for|from|in|of|on|or|the|to|with)\s+(?:19|20)\d{2}$"
)
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
_EXPLICIT_ACRONYM_PAREN_RE = re.compile(r"\(\s*(?P<acronym>[A-Z][A-Z0-9.&/-]{2,9})\s*\)")
_EXPLICIT_ACRONYM_COMMA_RE = re.compile(
    r",\s*(?:or\s+)?(?P<acronym>[A-Z][A-Z0-9.&/-]{2,9})(?=,)"
)
_GENERIC_DURATION_ALIAS_RE = re.compile(
    r"^\d+\s+(?:sec|secs|second|seconds|min|mins|minute|minutes|hr|hrs|hour|hours)$"
)
_DATE_LIKE_ALIAS_RE = re.compile(
    r"^(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2},\s+\d{4}$",
    flags=re.IGNORECASE,
)
_EXPLICIT_ACRONYM_LONGFORM_RE = re.compile(
    r"(?P<long>[A-Z][A-Za-z0-9'’-]*(?:\s+(?:(?:[A-Z][A-Za-z0-9'’-]*)|(?:a|an|and|at|by|for|from|in|of|on|the|to|with|da|de|del|du|la|van|von))){1,11})\s*$"
)
_MAX_EXPLICIT_ACRONYM_LONGFORM_WINDOW = 120
_MATCHER_REGISTRY_SHORT_ALIAS_LENGTH_RANGE = range(2, 7)
_CONTEXTUAL_ORG_ACRONYM_LENGTH_RANGE = range(2, 7)
_CONTEXTUAL_ORG_ACRONYM_REPORTING_CUES = {
    "citing",
    "emailed",
    "interviewed",
    "quoted",
    "reported",
    "reports",
    "reporting",
    "says",
    "said",
    "telling",
    "tells",
    "told",
    "tweeted",
    "tweets",
    "wrote",
    "writes",
}
_CONTEXTUAL_ORG_ACRONYM_PREPOSITION_CUES = {"at", "by", "for", "from", "of", "with"}
_CONTEXTUAL_ORG_ACRONYM_CONTEXT_NOUN_CUES = {
    "agency",
    "association",
    "broadcaster",
    "board",
    "bureau",
    "centre",
    "center",
    "chair",
    "chairman",
    "college",
    "committee",
    "commission",
    "congress",
    "council",
    "director",
    "festival",
    "group",
    "institute",
    "members",
    "member",
    "office",
    "project",
    "poll",
    "school",
    "scholarship",
    "source",
    "survey",
    "team",
    "university",
    "union",
    "unions",
    "website",
}
_CONTEXTUAL_ORG_ACRONYM_FOLLOWING_CUES = _CONTEXTUAL_ORG_ACRONYM_REPORTING_CUES | {
    "adviser",
    "advisor",
    "affiliate",
    "agency",
    "community",
    "official",
    "officials",
    "spokesman",
    "spokesperson",
    "spokeswoman",
}
_HEURISTIC_ACRONYM_LOCATION_SUFFIX_TOKENS = {
    "city",
    "county",
    "islands",
    "kingdom",
    "province",
    "republic",
    "state",
    "states",
    "territory",
}
_HYPHENATED_ALIAS_BACKFILL_SUFFIXES = {
    "backed",
    "based",
    "centered",
    "centred",
    "driven",
    "focused",
    "hit",
    "led",
    "listed",
    "linked",
    "owned",
    "related",
    "run",
    "sourced",
    "targeted",
}
_HYHENATED_LOCATION_PRECEDING_ARTICLES = {"a", "an", "the"}
_HEURISTIC_HYPHENATED_LOCATION_SUFFIXES = {
    "based",
    "centered",
    "centred",
    "listed",
}
_STRUCTURAL_ORG_TRAILING_SUFFIX_TOKENS = {
    "agency",
    "association",
    "bank",
    "bureau",
    "center",
    "capital",
    "centre",
    "committee",
    "company",
    "corp",
    "corporation",
    "council",
    "foundation",
    "group",
    "holding",
    "holdings",
    "hospital",
    "hospitals",
    "hub",
    "inc",
    "institute",
    "intelligence",
    "llc",
    "llp",
    "ltd",
    "media",
    "ministry",
    "network",
    "organization",
    "partners",
    "plc",
    "research",
    "services",
    "society",
    "solutions",
    "studies",
    "systems",
    "technologies",
    "technology",
    "university",
}
_HEURISTIC_STRUCTURAL_ORG_SUFFIX_TOKENS = {
    "agency",
    "association",
    "bank",
    "bureau",
    "center",
    "centre",
    "committee",
    "corporation",
    "council",
    "foundation",
    "group",
    "hospital",
    "hospitals",
    "hub",
    "institute",
    "media",
    "ministry",
    "network",
    "organization",
    "society",
    "solutions",
    "university",
}
_OPTIONAL_EXPLICIT_ACRONYM_TRAILING_SUFFIX_TOKENS = {"hub"}
_STRUCTURAL_ORG_CONNECTOR_HEAD_TOKENS = {
    "agency",
    "association",
    "bank",
    "bureau",
    "center",
    "centre",
    "college",
    "commission",
    "committee",
    "council",
    "court",
    "foundation",
    "group",
    "house",
    "hospital",
    "hospitals",
    "institute",
    "ministry",
    "network",
    "organization",
    "research",
    "society",
    "study",
    "university",
}
_WEAK_HEURISTIC_STRUCTURAL_ORG_SUFFIX_TOKENS = {
    "media",
    "systems",
}
_GENERIC_STRUCTURAL_ORG_QUANTIFIER_LEADS = {
    "all",
    "both",
    "few",
    "many",
    "multiple",
    "several",
    "some",
}
_PARLIAMENTARY_HOUSE_TAIL_TOKENS = {
    "assembly",
    "commons",
    "lords",
    "parliament",
    "representatives",
}
_IRREGULAR_DEMONYM_MODIFIER_TOKENS = {
    "british",
    "dutch",
    "english",
    "french",
    "greek",
    "irish",
    "scottish",
    "welsh",
}
_STRUCTURAL_ORG_CONNECTOR_TOKENS = {"for", "of"}
_MAX_STRUCTURAL_ORG_SUFFIX_TOKENS = 3
_MAX_STRUCTURAL_ORG_CONNECTOR_TAIL_TOKENS = 3
_STRUCTURAL_LOCATION_HEAD_TOKENS = {
    "bay",
    "cape",
    "delta",
    "gulf",
    "island",
    "lake",
    "mainland",
    "mount",
    "mountain",
    "river",
    "sea",
    "strait",
}
_STRUCTURAL_LOCATION_CONNECTOR_TOKENS = {"of"}
_GENERIC_OFFICE_TITLE_TOKENS = {
    "attorney",
    "ceo",
    "chair",
    "commissioner",
    "director",
    "governor",
    "judge",
    "leader",
    "mayor",
    "member",
    "minister",
    "officer",
    "president",
    "premier",
    "prosecutor",
    "secretary",
    "senator",
    "spokesperson",
}
_GENERIC_STRUCTURED_ORG_QUANTIFIER_LEADS = {
    "all",
    "both",
    "few",
    "many",
    "multiple",
    "several",
    "some",
}
_SINGLETON_ORG_CONTEXT_CUE_TOKENS = {
    "at",
    "by",
    "from",
    "in",
    "into",
    "on",
    "under",
    "via",
    "with",
}
_PERSON_SURNAME_COREFERENCE_FOLLOWING_CUES = (
    _CONTEXTUAL_ORG_ACRONYM_REPORTING_CUES
    | _AUXILIARY_LOOKUP_TOKENS
    | {
        "added",
        "adding",
        "believes",
        "believed",
        "called",
        "calls",
        "confirmed",
        "confirms",
        "confirming",
        "continuing",
        "departure",
        "denied",
        "denies",
        "deny",
        "explained",
        "expects",
        "expected",
        "estimated",
        "estimates",
        "leadership",
        "noted",
        "notes",
        "probe",
        "staying",
        "tenure",
        "thinks",
        "warned",
        "warns",
        "who",
        "whose",
    }
)
_PERSON_SURNAME_COREFERENCE_PRECEDING_TITLE_TOKENS = (
    _GENERIC_OFFICE_TITLE_TOKENS | {"dr", "gov", "mr", "mrs", "ms", "rep", "sen"}
)
_PERSON_SURNAME_COREFERENCE_PREPOSITION_CUES = {
    "about",
    "against",
    "around",
    "by",
    "for",
    "from",
    "under",
    "via",
    "with",
}
_PERSON_SURNAME_COREFERENCE_TRAILING_NOUN_CUES = {
    "administration",
    "adviser",
    "advisers",
    "advisor",
    "advisors",
    "allies",
    "backers",
    "campaign",
    "critic",
    "critics",
    "government",
    "office",
    "opponents",
    "premiership",
    "spokesman",
    "spokesperson",
    "spokeswoman",
    "staff",
    "supporters",
    "team",
}
_PERSON_SURNAME_COREFERENCE_FRAGMENT_LEADS = (
    _GENERIC_PHRASE_LEADS | _PERSON_SURNAME_COREFERENCE_PREPOSITION_CUES
)
_HEURISTIC_STRUCTURAL_ORG_SUFFIX_VARIANTS: set[str] = set()
for _suffix in _HEURISTIC_STRUCTURAL_ORG_SUFFIX_TOKENS:
    _HEURISTIC_STRUCTURAL_ORG_SUFFIX_VARIANTS.add(re.escape(_suffix))
    _HEURISTIC_STRUCTURAL_ORG_SUFFIX_VARIANTS.add(re.escape(_suffix.title()))
    _HEURISTIC_STRUCTURAL_ORG_SUFFIX_VARIANTS.add(re.escape(_suffix.upper()))
_STRUCTURAL_ORG_CONNECTOR_HEAD_VARIANTS: set[str] = set()
for _head in _STRUCTURAL_ORG_CONNECTOR_HEAD_TOKENS:
    _STRUCTURAL_ORG_CONNECTOR_HEAD_VARIANTS.add(re.escape(_head))
    _STRUCTURAL_ORG_CONNECTOR_HEAD_VARIANTS.add(re.escape(_head.title()))
    _STRUCTURAL_ORG_CONNECTOR_HEAD_VARIANTS.add(re.escape(_head.upper()))
_STRUCTURAL_LOCATION_HEAD_VARIANTS: set[str] = set()
for _head in _STRUCTURAL_LOCATION_HEAD_TOKENS:
    _STRUCTURAL_LOCATION_HEAD_VARIANTS.add(re.escape(_head))
    _STRUCTURAL_LOCATION_HEAD_VARIANTS.add(re.escape(_head.title()))
    _STRUCTURAL_LOCATION_HEAD_VARIANTS.add(re.escape(_head.upper()))
_HEURISTIC_STRUCTURAL_ORG_RE = re.compile(
    r"\b(?P<org>(?:[A-Z][A-Za-z]*(?:-[A-Z][A-Za-z]*)*)(?:\s+(?:(?:[A-Z][A-Za-z]*(?:-[A-Z][A-Za-z]*)*)|(?:of|for|the))){0,4}\s+(?:"
    + "|".join(sorted(_HEURISTIC_STRUCTURAL_ORG_SUFFIX_VARIANTS))
    + r"))\b"
)
_HEURISTIC_CONNECTOR_STRUCTURAL_ORG_RE = re.compile(
    r"\b(?P<org>(?:[A-Z][A-Za-z]*(?:-[A-Z][A-Za-z]*)*\s+){0,2}(?:"
    + "|".join(sorted(_STRUCTURAL_ORG_CONNECTOR_HEAD_VARIANTS))
    + r")\s+(?:of|for)\s+(?:the\s+)?(?:[A-Z][A-Za-z]*(?:-[A-Z][A-Za-z]*)*|[a-z][A-Za-z-]{4,})(?:\s+(?:(?:of|for|the)|(?:[A-Z][A-Za-z]*(?:-[A-Z][A-Za-z]*)*))){0,3})\b"
)
_HEURISTIC_STRUCTURAL_LOCATION_RE = re.compile(
    r"\b(?P<loc>(?:"
    + "|".join(sorted(_STRUCTURAL_LOCATION_HEAD_VARIANTS))
    + r")\s+of\s+(?:the\s+)?(?:[A-Z][A-Za-z]*(?:-[A-Z][A-Za-z]*)*)(?:\s+[A-Z][A-Za-z]*(?:-[A-Z][A-Za-z]*)*){0,2})\b"
)

_LANE_PRIORITY = {
    "deterministic_rule": 3,
    "deterministic_alias": 2,
    "document_acronym_backfill": 2,
    "heuristic_definition_acronym_backfill": 2,
    "heuristic_structured_org_backfill": 2,
    "heuristic_structured_location_backfill": 2,
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


@dataclass(frozen=True)
class HeuristicAcronymDefinition:
    acronym_text: str
    normalized_acronym: str
    label: str
    long_form: str
    definition_start: int
    definition_end: int
    acronym_start: int
    acronym_end: int


@dataclass(frozen=True)
class HeuristicStructuredOrganization:
    text: str
    start: int
    end: int


@dataclass(frozen=True)
class HeuristicStructuredLocation:
    text: str
    start: int
    end: int


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


def _collapse_whitespace(value: str) -> str:
    return " ".join(value.split())


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


def _build_runtime_generated_entity_link(
    *,
    provider: str,
    runtime: PackRuntime,
    label: str,
    canonical_text: str,
) -> EntityLink:
    return _build_entity_link(
        provider=provider,
        pack_id=runtime.pack_id,
        label=label,
        canonical_text=canonical_text,
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
        extracted.extend(
            _extract_registry_short_alias_entities_for_matcher_packs(
                segment_text_value,
                segment_start=segment_start,
                normalized_input=normalized_input,
                registry=registry,
                allowed_pack_ids=matcher_pack_ids,
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
    if runtime.matchers:
        extracted.extend(
            _extract_hyphenated_lookup_alias_entities_with_matchers(
                segment_text_value,
                segment_start=segment_start,
                normalized_input=normalized_input,
                runtime=runtime,
            )
        )
    if fallback_pack_ids:
        extracted.extend(
            _extract_hyphenated_lookup_alias_entities(
                segment_text_value,
                segment_start=segment_start,
                normalized_input=normalized_input,
                registry=registry,
                allowed_pack_ids=fallback_pack_ids,
                lookup_cache=lookup_cache,
            )
        )
    return extracted


def _extract_hyphenated_lookup_alias_entities(
    segment_text_value: str,
    *,
    segment_start: int,
    normalized_input: NormalizedText,
    registry: PackRegistry,
    allowed_pack_ids: set[str],
    lookup_cache: dict[tuple[str, str], list[dict[str, str | float | bool | None]]],
) -> list[ExtractedCandidate]:
    extracted: list[ExtractedCandidate] = []
    if not allowed_pack_ids:
        return extracted
    pack_id_list = sorted(allowed_pack_ids)
    for start, end, candidate_text in _iter_hyphenated_alias_backfill_windows(segment_text_value):
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


def _extract_registry_short_alias_entities_for_matcher_packs(
    segment_text_value: str,
    *,
    segment_start: int,
    normalized_input: NormalizedText,
    registry: PackRegistry,
    allowed_pack_ids: set[str],
    lookup_cache: dict[tuple[str, str], list[dict[str, str | float | bool | None]]],
) -> list[ExtractedCandidate]:
    extracted: list[ExtractedCandidate] = []
    if not allowed_pack_ids:
        return extracted
    pack_id_list = sorted(allowed_pack_ids)
    for start, end in _iter_token_spans(segment_text_value):
        candidate_text = segment_text_value[start:end]
        normalized_acronym = _normalize_acronym_text(candidate_text)
        if (
            len(normalized_acronym) not in _MATCHER_REGISTRY_SHORT_ALIAS_LENGTH_RANGE
            or not _is_single_token_all_caps(candidate_text)
        ):
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
        if not candidates:
            continue
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


def _extract_hyphenated_lookup_alias_entities_with_matchers(
    segment_text_value: str,
    *,
    segment_start: int,
    normalized_input: NormalizedText,
    runtime: PackRuntime,
) -> list[ExtractedCandidate]:
    extracted: list[ExtractedCandidate] = []
    windows = _iter_hyphenated_alias_backfill_windows(segment_text_value)
    if not windows:
        return extracted
    matcher_entry_cache: dict[tuple[str, str], tuple[MatcherEntryPayload, ...]] = {}
    for matcher in runtime.matchers:
        compiled_matcher = load_runtime_matcher(
            matcher.artifact_path,
            matcher.entries_path,
        )
        for start, end, candidate_text in windows:
            entry_payloads = _lookup_exact_matcher_entry_payloads(
                compiled_matcher=compiled_matcher,
                pack_id=matcher.pack_id,
                candidate_text=candidate_text,
                matcher_entry_cache=matcher_entry_cache,
            )
            if not entry_payloads:
                continue
            extracted.extend(
                _build_lookup_alias_entities_from_matcher_entries(
                    entries=list(entry_payloads),
                    pack_id=matcher.pack_id,
                    runtime_domain=runtime.domain,
                    segment_text_value=segment_text_value,
                    matched_text=segment_text_value[start:end],
                    start=start,
                    end=end,
                    segment_start=segment_start,
                    normalized_input=normalized_input,
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


def _lookup_exact_matcher_entry_payloads(
    *,
    compiled_matcher: RuntimeMatcher,
    pack_id: str,
    candidate_text: str,
    matcher_entry_cache: dict[tuple[str, str], tuple[MatcherEntryPayload, ...]],
) -> tuple[MatcherEntryPayload, ...]:
    cache_key = (pack_id, normalize_lookup_text(candidate_text))
    cached = matcher_entry_cache.get(cache_key)
    if cached is not None:
        return cached

    exact_entries: list[MatcherEntryPayload] = []
    seen_entry_indexes: set[int] = set()
    for candidate in find_exact_match_candidates(candidate_text, compiled_matcher):
        if candidate.start != 0 or candidate.end != len(candidate_text):
            continue
        if not compiled_matcher.entry_payloads or not candidate.entry_indices:
            continue
        for index in candidate.entry_indices:
            if index in seen_entry_indexes:
                continue
            if index < 0 or index >= len(compiled_matcher.entry_payloads):
                continue
            seen_entry_indexes.add(index)
            exact_entries.append(compiled_matcher.entry_payloads[index])

    resolved = tuple(exact_entries)
    matcher_entry_cache[cache_key] = resolved
    return resolved


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


def _next_contiguous_token_span(text: str, offset: int) -> tuple[int, int, str] | None:
    match = TOKEN_RE.search(text, offset)
    if match is None:
        return None
    gap = text[offset : match.start()]
    if gap and not gap.isspace():
        return None
    return match.start(), match.end(), match.group(0)


def _previous_contiguous_token_span(text: str, offset: int) -> tuple[int, int, str] | None:
    if offset <= 0:
        return None
    match: re.Match[str] | None = None
    for candidate in TOKEN_RE.finditer(text, 0, offset):
        match = candidate
    if match is None:
        return None
    gap = text[match.end() : offset]
    if gap and not gap.isspace():
        return None
    return match.start(), match.end(), match.group(0)


def _iter_hyphenated_alias_backfill_windows(
    text: str,
) -> list[tuple[int, int, str]]:
    windows: list[tuple[int, int, str]] = []
    seen: set[tuple[int, int]] = set()
    for start, end in _iter_token_spans(text):
        token_text = text[start:end]
        prefix_length = _hyphenated_alias_prefix_length(token_text)
        if prefix_length is None:
            continue
        window = (start, start + prefix_length)
        if window in seen:
            continue
        seen.add(window)
        windows.append((window[0], window[1], text[window[0] : window[1]]))
    return windows


def _iter_hyphenated_location_backfill_windows(
    text: str,
) -> list[tuple[int, int, str]]:
    windows: list[tuple[int, int, str]] = []
    seen: set[tuple[int, int]] = set()
    for start, end in _iter_token_spans(text):
        token_text = text[start:end]
        prefix_length = _hyphenated_location_prefix_length(token_text)
        if prefix_length is None:
            continue
        prefix_text = token_text[:prefix_length]
        if _looks_like_adjectival_location_tail(prefix_text):
            continue
        window_start = start
        previous_inline = _previous_contiguous_token_span(text, window_start)
        while previous_inline is not None:
            previous_normalized = normalize_lookup_text(previous_inline[2])
            if previous_normalized in _HYHENATED_LOCATION_PRECEDING_ARTICLES:
                break
            if (
                not _is_titleish_token(previous_inline[2])
                or _looks_like_adjectival_location_tail(previous_inline[2])
            ):
                break
            window_start = previous_inline[0]
            previous_inline = _previous_contiguous_token_span(text, window_start)
        window = (window_start, start + prefix_length)
        if window in seen:
            continue
        seen.add(window)
        windows.append((window[0], window[1], text[window[0] : window[1]]))
    return windows


def _hyphenated_alias_prefix_length(token_text: str) -> int | None:
    if "-" not in token_text:
        return None
    parts = token_text.split("-")
    if len(parts) < 2:
        return None
    prefix = parts[0]
    suffix_parts = parts[1:]
    if not prefix or not prefix.isalpha():
        return None
    if len(prefix) < 2:
        return None
    if not prefix[:1].isupper() and not prefix.isupper():
        return None
    if not all(part.isalpha() and part.islower() for part in suffix_parts):
        return None
    if not all(part in _HYPHENATED_ALIAS_BACKFILL_SUFFIXES for part in suffix_parts):
        return None
    return len(prefix)


def _hyphenated_location_prefix_length(token_text: str) -> int | None:
    if "-" not in token_text:
        return None
    parts = token_text.split("-")
    if len(parts) < 2:
        return None
    prefix = parts[0]
    suffix_parts = parts[1:]
    if not prefix or not prefix.isalpha():
        return None
    if len(prefix) < 3:
        return None
    if not prefix[:1].isupper() or not prefix[1:].islower():
        return None
    if not all(part.isalpha() and part.islower() for part in suffix_parts):
        return None
    if not all(part in _HEURISTIC_HYPHENATED_LOCATION_SUFFIXES for part in suffix_parts):
        return None
    return len(prefix)


def _is_token_window_span(
    start: int,
    end: int,
    *,
    token_starts: set[int],
    token_ends: set[int],
) -> bool:
    return start in token_starts and end in token_ends and end > start


def _extend_structural_entity_spans(
    candidates: list[ExtractedCandidate],
    *,
    normalized_input: NormalizedText,
) -> list[ExtractedCandidate]:
    extended: list[ExtractedCandidate] = []
    for candidate in candidates:
        normalized_start = candidate.normalized_start
        normalized_end = candidate.normalized_end
        if (
            candidate.lane
            in {
                "deterministic_alias",
                "heuristic_structured_org_backfill",
                "heuristic_structured_location_backfill",
            }
            and candidate.domain == "general"
        ):
            label_key = candidate.entity.label.casefold()
            if label_key == "organization":
                normalized_end = _extend_structural_organization_end(
                    text=normalized_input.text,
                    start=candidate.normalized_start,
                    end=candidate.normalized_end,
                )
            elif label_key == "location":
                normalized_start = _extend_structural_location_start(
                    text=normalized_input.text,
                    start=candidate.normalized_start,
                    end=candidate.normalized_end,
                )
        if (
            normalized_start >= candidate.normalized_start
            and normalized_end <= candidate.normalized_end
        ):
            extended.append(candidate)
            continue
        raw_start, raw_end = normalized_input.raw_span(normalized_start, normalized_end)
        extended.append(
            ExtractedCandidate(
                entity=EntityMatch(
                    text=normalized_input.raw_text[raw_start:raw_end],
                    label=candidate.entity.label,
                    start=raw_start,
                    end=raw_end,
                    confidence=candidate.entity.confidence,
                    relevance=candidate.entity.relevance,
                    provenance=candidate.entity.provenance,
                    link=candidate.entity.link,
                ),
                domain=candidate.domain,
                lane=candidate.lane,
                normalized_start=normalized_start,
                normalized_end=normalized_end,
            )
        )
    return extended


def _extend_structural_organization_end(text: str, *, start: int, end: int) -> int:
    base_tokens = TOKEN_RE.findall(text[start:end])
    if not base_tokens:
        return end

    normalized_tokens = [normalize_lookup_text(token) for token in base_tokens]
    cursor = end
    extended_end = end
    suffix_count = 0
    while suffix_count < _MAX_STRUCTURAL_ORG_SUFFIX_TOKENS:
        next_token = _next_contiguous_token_span(text, cursor)
        if next_token is None:
            break
        _, next_end, next_text = next_token
        if normalize_lookup_text(next_text) not in _STRUCTURAL_ORG_TRAILING_SUFFIX_TOKENS:
            break
        extended_end = next_end
        cursor = next_end
        suffix_count += 1
    if extended_end != end:
        return extended_end
    if any(token in _STRUCTURAL_ORG_CONNECTOR_TOKENS for token in normalized_tokens):
        coordinated_tail_end = _extend_structural_organization_coordination_tail(
            text,
            cursor=end,
        )
        if coordinated_tail_end is not None:
            return coordinated_tail_end

    supports_connector_extension = (
        normalized_tokens[-1] in _STRUCTURAL_ORG_CONNECTOR_HEAD_TOKENS
        or (
            normalized_tokens[0] in _STRUCTURAL_ORG_CONNECTOR_HEAD_TOKENS
            and any(token in _STRUCTURAL_ORG_CONNECTOR_TOKENS for token in normalized_tokens[1:])
        )
        or _supports_generic_organization_connector_extension(normalized_tokens)
    )
    if not supports_connector_extension:
        return end

    connector = _next_contiguous_token_span(text, cursor)
    if connector is None:
        return end
    _, connector_end, connector_text = connector
    if normalize_lookup_text(connector_text) not in _STRUCTURAL_ORG_CONNECTOR_TOKENS:
        return end
    if len(normalized_tokens) == 1 and normalize_lookup_text(connector_text) != "of":
        return end

    cursor = connector_end
    article = _next_contiguous_token_span(text, cursor)
    if article is not None and normalize_lookup_text(article[2]) == "the":
        cursor = article[1]

    titleish_count = 0
    last_titleish_end: int | None = None
    while titleish_count < _MAX_STRUCTURAL_ORG_CONNECTOR_TAIL_TOKENS:
        next_token = _next_contiguous_token_span(text, cursor)
        if next_token is None:
            break
        _, next_end, next_text = next_token
        if not (
            _is_titleish_token(next_text)
            or (titleish_count == 0 and _is_lowercase_structural_org_tail_token(next_text))
        ):
            break
        last_titleish_end = next_end
        cursor = next_end
        titleish_count += 1
    if last_titleish_end is not None:
        coordinated_tail_end = _extend_structural_organization_coordination_tail(
            text,
            cursor=last_titleish_end,
        )
        if coordinated_tail_end is not None:
            return coordinated_tail_end
    return last_titleish_end or end


def _extend_structural_organization_coordination_tail(
    text: str,
    *,
    cursor: int,
) -> int | None:
    connector = _next_contiguous_token_span(text, cursor)
    if connector is None or normalize_lookup_text(connector[2]) != "and":
        return None
    cursor = connector[1]
    article = _next_contiguous_token_span(text, cursor)
    if article is not None and normalize_lookup_text(article[2]) == "the":
        cursor = article[1]

    consumed = 0
    last_end: int | None = None
    while consumed < _MAX_STRUCTURAL_ORG_CONNECTOR_TAIL_TOKENS:
        next_token = _next_contiguous_token_span(text, cursor)
        if next_token is None:
            break
        _, next_end, next_text = next_token
        if not (
            _is_titleish_token(next_text)
            or (consumed == 0 and _is_lowercase_structural_org_tail_token(next_text))
        ):
            break
        last_end = next_end
        cursor = next_end
        consumed += 1
    return last_end


def _supports_generic_organization_connector_extension(normalized_tokens: list[str]) -> bool:
    if not normalized_tokens:
        return False
    if normalized_tokens[0] in _GENERIC_PHRASE_LEADS:
        return False
    if any(token in _GENERIC_OFFICE_TITLE_TOKENS for token in normalized_tokens):
        return False
    if len(normalized_tokens) == 1:
        return normalized_tokens[0] not in (
            _GENERIC_SINGLE_TOKEN_HEADS | _ARTICLE_LED_STRUCTURAL_FRAGMENT_TOKENS
        )
    return not any(token in _TRAILING_FUNCTION_WORDS for token in normalized_tokens)


def _extend_structural_location_start(text: str, *, start: int, end: int) -> int:
    del end
    connector = _previous_contiguous_token_span(text, start)
    if connector is None:
        return start
    if normalize_lookup_text(connector[2]) not in _STRUCTURAL_LOCATION_CONNECTOR_TOKENS:
        return start
    head = _previous_contiguous_token_span(text, connector[0])
    if head is None:
        return start
    if normalize_lookup_text(head[2]) not in _STRUCTURAL_LOCATION_HEAD_TOKENS:
        return start
    return head[0]


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
    normalized_matched_text = normalize_lookup_text(matched_text)
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
    if candidate_domain == "general" and (
        normalized_matched_text in _RUNTIME_GENERIC_PHRASE_ALIAS_TEXTS
        or normalized_candidate_value in _RUNTIME_GENERIC_PHRASE_ALIAS_TEXTS
        or normalized_canonical_text in _RUNTIME_GENERIC_PHRASE_ALIAS_TEXTS
    ):
        return True
    if (
        candidate_domain == "general"
        and label_key == "organization"
        and _is_single_token_all_caps(matched_text)
        and (
            normalized_matched_text in _RUNTIME_GENERIC_ORG_ACRONYM_ALIAS_TEXTS
            or normalized_candidate_value in _RUNTIME_GENERIC_ORG_ACRONYM_ALIAS_TEXTS
            or normalized_canonical_text in _RUNTIME_GENERIC_ORG_ACRONYM_ALIAS_TEXTS
        )
    ):
        return True
    if not _is_single_token_alpha(matched_text):
        return False
    if matched_text.islower():
        return True
    generic_single_token_blocklist = (
        _GENERIC_SINGLE_TOKEN_HEADS
        | _GENERIC_PHRASE_LEADS
        | _CALENDAR_SINGLE_TOKEN_WORDS
        | _NUMBER_SINGLE_TOKEN_WORDS
        | _RUNTIME_GENERIC_SINGLE_TOKEN_ALIAS_TEXTS
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
        and label_key == "organization"
        and segment_text is not None
        and start is not None
        and end is not None
        and not _is_single_token_all_caps(matched_text)
        and normalized_candidate_value == normalized_canonical_text
    ):
        if _looks_like_locationish_singleton_context(segment_text, start=start, end=end):
            return True
        if _looks_like_sentence_boundary_singleton_org_noise(
            segment_text,
            start=start,
            end=end,
            matched_text=matched_text,
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
    if (
        candidate_domain == "general"
        and segment_text is not None
        and start is not None
        and end is not None
        and _is_single_token_all_caps(matched_text)
        and _looks_like_all_caps_section_heading_noise(segment_text, start=start, end=end)
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
    canonical_text = canonical_text or candidate_value
    if label_key not in {"organization", "person", "location"}:
        return False
    tokens = TOKEN_RE.findall(matched_text)
    if len(tokens) < 2:
        return False
    lowered_tokens = [normalize_lookup_text(token) for token in tokens]
    normalized_values = {
        normalize_lookup_text(matched_text),
        normalize_lookup_text(candidate_value),
        normalize_lookup_text(canonical_text),
    }
    if any(value in _RUNTIME_GENERIC_PHRASE_ALIAS_TEXTS for value in normalized_values):
        return True
    if any(_RUNTIME_FUNCTION_YEAR_ALIAS_RE.fullmatch(value) for value in normalized_values):
        return True
    if (
        len(lowered_tokens) == 2
        and lowered_tokens[0] in _GENERIC_PHRASE_LEADS
        and lowered_tokens[1] in _RUNTIME_ARTICLE_LED_GENERIC_ALIAS_TOKENS
    ):
        return True
    if (
        label_key == "organization"
        and _looks_like_generic_office_title_candidate(tokens, lowered_tokens)
    ):
        return True
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


def _looks_like_generic_office_title_candidate(
    tokens: list[str],
    lowered_tokens: list[str],
) -> bool:
    if not any(token in _GENERIC_OFFICE_TITLE_TOKENS for token in lowered_tokens):
        return False
    if any(token in _STRUCTURAL_ORG_TRAILING_SUFFIX_TOKENS for token in lowered_tokens):
        return False
    return all(
        raw_token.isupper() or raw_token[:1].isupper()
        for raw_token in tokens
    )


def _looks_like_locationish_singleton_context(text: str, *, start: int, end: int) -> bool:
    if re.match(r"^\s*,\s*[A-Z][A-Za-z-]+", text[end:]) is None:
        return False
    previous_token = _previous_token(text, start)
    previous_lower = normalize_lookup_text(previous_token)
    if previous_lower in _SINGLETON_ORG_CONTEXT_CUE_TOKENS:
        return True
    return _is_titleish_token(previous_token)


def _looks_like_sentence_boundary_singleton_org_noise(
    text: str,
    *,
    start: int,
    end: int,
    matched_text: str,
) -> bool:
    if _single_token_occurrence_count(text, matched_text) > 1:
        return False
    if re.match(r'^\s*["\')\]]*[.!?]', text[end:]) is None:
        return False
    if not _is_titleish_token(_next_token(text, end)):
        return False
    previous_lower = normalize_lookup_text(_previous_token(text, start))
    return previous_lower not in _SINGLETON_ORG_CONTEXT_CUE_TOKENS


def _person_surname_key(value: str) -> str | None:
    tokens = [token for token in TOKEN_RE.findall(value) if any(character.isalpha() for character in token)]
    if len(tokens) < 2:
        return None
    if len(tokens) == 2:
        leading_token = normalize_lookup_text(tokens[0])
        if (
            leading_token in _PERSON_SURNAME_COREFERENCE_FRAGMENT_LEADS
            or leading_token in _PERSON_SURNAME_COREFERENCE_PRECEDING_TITLE_TOKENS
        ):
            return None
    for token in reversed(tokens):
        normalized = normalize_lookup_text(token)
        if (
            not normalized
            or normalized in _PERSON_NAME_PARTICLES
            or normalized in _PERSON_NAME_SUFFIX_TOKENS
            or normalized in _GENERIC_OFFICE_TITLE_TOKENS
        ):
            continue
        return normalized
    return None


def _person_surname_coreference_candidate(
    value: str,
) -> tuple[str | None, str | None]:
    normalized_tokens = [
        normalize_lookup_text(token)
        for token in TOKEN_RE.findall(value)
        if any(character.isalpha() for character in token)
    ]
    if len(normalized_tokens) == 2:
        leading_token = normalized_tokens[0]
        if leading_token in _PERSON_SURNAME_COREFERENCE_FRAGMENT_LEADS:
            fragment_kind = (
                "generic_lead_fragment"
                if leading_token in _GENERIC_PHRASE_LEADS
                else "preposition_lead_fragment"
            )
            return normalized_tokens[1], fragment_kind
    if _is_single_token_alpha(value):
        return normalize_lookup_text(value), "single_token"
    return None, None


def _person_surname_coreference_mention_types(
    text: str,
    *,
    start: int,
    end: int,
) -> set[str]:
    mention_types: set[str] = set()
    trailing_text = text[end:]
    if re.match(r"^\s*[’']s\b", trailing_text):
        mention_types.add("possessive_suffix")
    if re.match(
        r"^\s*,\s*(?:who|whose|said|says|told|added|noted|warned|wrote)\b",
        trailing_text,
        flags=re.IGNORECASE,
    ):
        mention_types.add("appositive_clause")
    next_span = _next_contiguous_token_span(text, end)
    next_lower = normalize_lookup_text(next_span[2]) if next_span is not None else ""
    if next_lower in _PERSON_SURNAME_COREFERENCE_FOLLOWING_CUES:
        mention_types.add("following_cue")
    if next_lower in _PERSON_SURNAME_COREFERENCE_TRAILING_NOUN_CUES:
        mention_types.add("institutional_tail")
    previous_span = _previous_contiguous_token_span(text, start)
    if previous_span is None:
        return mention_types
    previous_lower = normalize_lookup_text(previous_span[2])
    if previous_lower in _PERSON_SURNAME_COREFERENCE_PRECEDING_TITLE_TOKENS:
        mention_types.add("title_lead")
    if previous_lower in _PERSON_SURNAME_COREFERENCE_PREPOSITION_CUES:
        mention_types.add("preposition_lead")
        if previous_lower == "about" and next_lower in _PERSON_SURNAME_COREFERENCE_FOLLOWING_CUES:
            mention_types.add("about_discourse")
    if previous_lower != "to":
        return mention_types
    earlier_span = _previous_contiguous_token_span(text, previous_span[0])
    if earlier_span is None:
        return mention_types
    if normalize_lookup_text(earlier_span[2]) == "according":
        mention_types.add("according_to")
    return mention_types


def _looks_like_person_surname_coreference_context(
    text: str,
    *,
    start: int,
    end: int,
) -> bool:
    return bool(
        _person_surname_coreference_mention_types(
            text,
            start=start,
            end=end,
        )
    )


def _suppress_document_person_surname_false_positives(
    candidates: list[ExtractedCandidate],
    *,
    text: str,
    pack_id: str,
) -> tuple[list[ExtractedCandidate], list[ExtractedCandidate]]:
    if pack_id != "general-en":
        return candidates, []

    filtered: list[ExtractedCandidate] = []
    discarded: list[ExtractedCandidate] = []
    seen_person_surname_ends: dict[str, int] = {}
    seen_person_surname_entity_ids: dict[str, set[str]] = {}

    for candidate in sorted(candidates, key=lambda item: (item.entity.start, item.entity.end)):
        entity = candidate.entity
        label_key = entity.label.casefold()
        if label_key == "person":
            surname_key = _person_surname_key(entity.text)
            if surname_key is not None:
                seen_person_surname_ends[surname_key] = max(
                    entity.end,
                    seen_person_surname_ends.get(surname_key, -1),
                )
                if entity.link is not None and entity.link.entity_id:
                    seen_person_surname_entity_ids.setdefault(surname_key, set()).add(
                        entity.link.entity_id
                    )
                filtered.append(candidate)
                continue
        if label_key not in {"organization", "location", "person"}:
            filtered.append(candidate)
            continue
        if candidate.domain != "general":
            filtered.append(candidate)
            continue
        surname_key, candidate_kind = _person_surname_coreference_candidate(entity.text)
        if surname_key is None or candidate_kind is None:
            filtered.append(candidate)
            continue
        previous_person_end = seen_person_surname_ends.get(surname_key)
        if previous_person_end is None or previous_person_end >= entity.start:
            filtered.append(candidate)
            continue
        if label_key == "person":
            candidate_entity_id = entity.link.entity_id if entity.link is not None else None
            if (
                candidate_entity_id is not None
                and candidate_entity_id in seen_person_surname_entity_ids.get(surname_key, set())
            ):
                filtered.append(candidate)
                continue
        if candidate_kind != "single_token":
            discarded.append(candidate)
            continue
        if not _looks_like_person_surname_coreference_context(
            text,
            start=entity.start,
            end=entity.end,
        ):
            filtered.append(candidate)
            continue
        discarded.append(candidate)

    return filtered, discarded


def _single_token_occurrence_count(text: str, matched_text: str) -> int:
    return len(re.findall(rf"\b{re.escape(matched_text)}\b", text))


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


def _looks_like_all_caps_section_heading_noise(text: str, *, start: int, end: int) -> bool:
    trailing = text[end:end + 40]
    if re.match(r"\s*:\s*", trailing) is not None:
        return True
    if re.match(
        r"\s+[A-Z]{2,8}\b(?:\s+[A-Z][A-Za-z'’-]+){0,3}\s*:\s*",
        trailing,
    ) is not None:
        return True
    leading = text[max(0, start - 16):start]
    if re.search(r"\b[A-Z]{2,8}\s*$", leading) is None:
        return False
    return (
        re.match(r"\s+[A-Z][A-Za-z'’-]+(?:\s+[A-Z][A-Za-z'’-]+){0,3}\s*:\s*", trailing)
        is not None
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
    for token in INITIALISM_TOKEN_RE.findall(value):
        normalized = normalize_lookup_text(token)
        if normalized in _ACRONYM_CONNECTOR_WORDS:
            continue
        compact = "".join(character for character in token if character.isalpha())
        if not compact:
            continue
        if "." in token:
            letters.extend(character.upper() for character in compact)
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


def _backfill_heuristic_definition_acronym_entities(
    extracted: list[ExtractedCandidate],
    *,
    normalized_input: NormalizedText,
    runtime: PackRuntime,
) -> list[ExtractedCandidate]:
    definitions = _collect_heuristic_acronym_definitions(
        normalized_input.text,
        allowed_labels=_normalized_label_set(runtime),
    )
    if not definitions:
        return extracted

    existing_spans = {
        (candidate.normalized_start, candidate.normalized_end, candidate.entity.label.casefold())
        for candidate in extracted
    }
    definitions_by_key: dict[tuple[str, str], list[HeuristicAcronymDefinition]] = {}
    for definition in definitions:
        definitions_by_key.setdefault(
            (definition.normalized_acronym, definition.label.casefold()),
            [],
        ).append(definition)
    for group in definitions_by_key.values():
        group.sort(key=lambda item: item.acronym_start)
    augmented = list(extracted)

    for start, end in _iter_token_spans(normalized_input.text):
        token_text = normalized_input.text[start:end]
        normalized_acronym = _normalize_acronym_text(token_text)
        if len(normalized_acronym) < 2 or not normalized_acronym.isupper():
            continue
        for label_key in ("organization", "location"):
            matching_definitions = definitions_by_key.get((normalized_acronym, label_key), [])
            definition = next(
                (
                    item
                    for item in reversed(matching_definitions)
                    if start >= item.acronym_start
                ),
                None,
            )
            if definition is None:
                continue
            span_key = (start, end, label_key)
            if span_key in existing_spans:
                continue
            raw_start, raw_end = normalized_input.raw_span(start, end)
            augmented.append(
                ExtractedCandidate(
                    entity=EntityMatch(
                        text=normalized_input.raw_text[raw_start:raw_end],
                        label=definition.label,
                        start=raw_start,
                        end=raw_end,
                        confidence=0.67,
                    provenance=_build_provenance(
                        match_kind="alias",
                        match_path="heuristic.explicit_definition_acronym",
                        match_source=f"{definition.long_form} ({token_text})",
                        source_pack=runtime.pack_id,
                        source_domain=runtime.domain,
                        lane="heuristic_definition_acronym_backfill",
                    ),
                    link=_build_runtime_generated_entity_link(
                        provider="heuristic.explicit_definition_acronym",
                        runtime=runtime,
                        label=definition.label,
                        canonical_text=definition.long_form,
                    ),
                ),
                    domain=runtime.domain,
                    lane="heuristic_definition_acronym_backfill",
                    normalized_start=start,
                    normalized_end=end,
                )
            )
            existing_spans.add(span_key)
    return augmented


def _backfill_heuristic_structured_organization_entities(
    extracted: list[ExtractedCandidate],
    *,
    normalized_input: NormalizedText,
    runtime: PackRuntime,
) -> list[ExtractedCandidate]:
    if "organization" not in _normalized_label_set(runtime):
        return extracted

    existing_spans = {
        (candidate.normalized_start, candidate.normalized_end, candidate.entity.label.casefold())
        for candidate in extracted
    }
    seen_norms = {
        normalize_lookup_text(candidate.entity.text)
        for candidate in extracted
        if candidate.entity.label.casefold() == "organization"
    }
    augmented = list(extracted)

    for candidate in _collect_heuristic_structured_organizations(normalized_input.text):
        normalized_candidate = normalize_lookup_text(candidate.text)
        span_key = (candidate.start, candidate.end, "organization")
        if span_key in existing_spans or normalized_candidate in seen_norms:
            continue
        raw_start, raw_end = normalized_input.raw_span(candidate.start, candidate.end)
        augmented.append(
            ExtractedCandidate(
                entity=EntityMatch(
                    text=normalized_input.raw_text[raw_start:raw_end],
                    label="organization",
                    start=raw_start,
                    end=raw_end,
                    confidence=0.65,
                    provenance=_build_provenance(
                        match_kind="alias",
                        match_path="heuristic.structural_organization",
                        match_source=candidate.text,
                        source_pack=runtime.pack_id,
                        source_domain=runtime.domain,
                        lane="heuristic_structured_org_backfill",
                    ),
                    link=_build_runtime_generated_entity_link(
                        provider="heuristic.structural_organization",
                        runtime=runtime,
                        label="organization",
                        canonical_text=candidate.text,
                    ),
                ),
                domain=runtime.domain,
                lane="heuristic_structured_org_backfill",
                normalized_start=candidate.start,
                normalized_end=candidate.end,
            )
        )
        existing_spans.add(span_key)
        seen_norms.add(normalized_candidate)
    return augmented


def _backfill_heuristic_structured_location_entities(
    extracted: list[ExtractedCandidate],
    *,
    normalized_input: NormalizedText,
    runtime: PackRuntime,
) -> list[ExtractedCandidate]:
    if "location" not in _normalized_label_set(runtime):
        return extracted

    existing_spans = {
        (candidate.normalized_start, candidate.normalized_end, candidate.entity.label.casefold())
        for candidate in extracted
    }
    seen_norms = {
        normalize_lookup_text(candidate.entity.text)
        for candidate in extracted
        if candidate.entity.label.casefold() == "location"
    }
    augmented = list(extracted)

    for candidate in _collect_heuristic_structured_locations(normalized_input.text):
        normalized_candidate = normalize_lookup_text(candidate.text)
        span_key = (candidate.start, candidate.end, "location")
        if span_key in existing_spans or normalized_candidate in seen_norms:
            continue
        raw_start, raw_end = normalized_input.raw_span(candidate.start, candidate.end)
        augmented.append(
            ExtractedCandidate(
                entity=EntityMatch(
                    text=normalized_input.raw_text[raw_start:raw_end],
                    label="location",
                    start=raw_start,
                    end=raw_end,
                    confidence=0.64,
                    provenance=_build_provenance(
                        match_kind="alias",
                        match_path="heuristic.structural_location",
                        match_source=candidate.text,
                        source_pack=runtime.pack_id,
                        source_domain=runtime.domain,
                        lane="heuristic_structured_location_backfill",
                    ),
                    link=_build_runtime_generated_entity_link(
                        provider="heuristic.structural_location",
                        runtime=runtime,
                        label="location",
                        canonical_text=candidate.text,
                    ),
                ),
                domain=runtime.domain,
                lane="heuristic_structured_location_backfill",
                normalized_start=candidate.start,
                normalized_end=candidate.end,
            )
        )
        existing_spans.add(span_key)
        seen_norms.add(normalized_candidate)
    return augmented


def _backfill_heuristic_hyphenated_location_entities(
    extracted: list[ExtractedCandidate],
    *,
    normalized_input: NormalizedText,
    runtime: PackRuntime,
) -> list[ExtractedCandidate]:
    if "location" not in _normalized_label_set(runtime):
        return extracted

    existing_spans = {
        (candidate.normalized_start, candidate.normalized_end, candidate.entity.label.casefold())
        for candidate in extracted
    }
    seen_norms = {
        normalize_lookup_text(candidate.entity.text)
        for candidate in extracted
        if candidate.entity.label.casefold() == "location"
    }
    augmented = list(extracted)

    for start, end, candidate_text in _iter_hyphenated_location_backfill_windows(
        normalized_input.text
    ):
        normalized_candidate = normalize_lookup_text(candidate_text)
        span_key = (start, end, "location")
        if span_key in existing_spans or normalized_candidate in seen_norms:
            continue
        raw_start, raw_end = normalized_input.raw_span(start, end)
        augmented.append(
            ExtractedCandidate(
                entity=EntityMatch(
                    text=normalized_input.raw_text[raw_start:raw_end],
                    label="location",
                    start=raw_start,
                    end=raw_end,
                    confidence=0.62,
                    provenance=_build_provenance(
                        match_kind="alias",
                        match_path="heuristic.hyphenated_location",
                        match_source=candidate_text,
                        source_pack=runtime.pack_id,
                        source_domain=runtime.domain,
                        lane="heuristic_hyphenated_location_backfill",
                    ),
                    link=_build_runtime_generated_entity_link(
                        provider="heuristic.hyphenated_location",
                        runtime=runtime,
                        label="location",
                        canonical_text=candidate_text,
                    ),
                ),
                domain=runtime.domain,
                lane="heuristic_hyphenated_location_backfill",
                normalized_start=start,
                normalized_end=end,
            )
        )
        existing_spans.add(span_key)
        seen_norms.add(normalized_candidate)
    return augmented


def _collect_heuristic_structured_organizations(
    text: str,
) -> list[HeuristicStructuredOrganization]:
    kept: dict[str, HeuristicStructuredOrganization] = {}

    for pattern in (_HEURISTIC_STRUCTURAL_ORG_RE, _HEURISTIC_CONNECTOR_STRUCTURAL_ORG_RE):
        for match in pattern.finditer(text):
            raw_candidate_text = match.group("org")
            candidate_start = match.start("org")
            candidate_end = match.end("org")
            trimmed_lead = _trim_leading_structural_org_phrase_lead(raw_candidate_text)
            if trimmed_lead is not None:
                trimmed_text, offset = trimmed_lead
                raw_candidate_text = raw_candidate_text[offset:]
                candidate_start += offset
                candidate_end = candidate_start + len(raw_candidate_text)
                candidate_text = trimmed_text
            else:
                candidate_text = _collapse_whitespace(raw_candidate_text)
            trimmed = _trim_leading_structural_org_role_phrase(raw_candidate_text)
            if trimmed is not None:
                trimmed_text, offset = trimmed
                raw_candidate_text = raw_candidate_text[offset:]
                candidate_start += offset
                candidate_end = candidate_start + len(raw_candidate_text)
                candidate_text = trimmed_text
            if not _is_plausible_heuristic_structured_organization_candidate(candidate_text):
                continue
            candidate = HeuristicStructuredOrganization(
                text=candidate_text,
                start=candidate_start,
                end=candidate_end,
            )
            key = normalize_lookup_text(candidate_text)
            current = kept.get(key)
            if current is None or candidate.start < current.start or (
                candidate.start == current.start and candidate.end > current.end
            ):
                kept[key] = candidate
    return sorted(kept.values(), key=lambda item: (item.start, item.end))


def _is_plausible_heuristic_structured_organization_candidate(candidate: str) -> bool:
    tokens = TOKEN_RE.findall(candidate)
    if len(tokens) < 2:
        return False
    normalized_tokens = [normalize_lookup_text(token) for token in tokens]
    if normalized_tokens[0] in _GENERIC_PHRASE_LEADS:
        return False
    if normalized_tokens[0] in _GENERIC_OFFICE_TITLE_TOKENS and "of" in normalized_tokens[1:]:
        return False
    if normalized_tokens[-1] not in _HEURISTIC_STRUCTURAL_ORG_SUFFIX_TOKENS:
        connector_indexes = [
            index
            for index, token in enumerate(normalized_tokens[1:], start=1)
            if token in _STRUCTURAL_ORG_CONNECTOR_TOKENS
        ]
        if not connector_indexes:
            return False
        connector_index = connector_indexes[0]
        head_tokens = normalized_tokens[:connector_index]
        tail_tokens = [
            token
            for token in normalized_tokens[connector_index + 1 :]
            if token not in _STRUCTURAL_ORG_CONNECTOR_TOKENS
        ]
        if not head_tokens or head_tokens[-1] not in _STRUCTURAL_ORG_CONNECTOR_HEAD_TOKENS:
            return False
        if not tail_tokens:
            return False
        if head_tokens[-1] == "house" and tail_tokens[-1] not in _PARLIAMENTARY_HOUSE_TAIL_TOKENS:
            return False
    if (
        len(tokens) == 2
        and normalized_tokens[-1] in _WEAK_HEURISTIC_STRUCTURAL_ORG_SUFFIX_TOKENS
        and (
            normalized_tokens[0] in _GENERIC_STRUCTURED_ORG_QUANTIFIER_LEADS
            or normalized_tokens[0] in _IRREGULAR_DEMONYM_MODIFIER_TOKENS
            or _looks_like_adjectival_location_tail(tokens[0])
        )
    ):
        return False
    if len(tokens) >= 4 and sum(token.isupper() for token in tokens if token.isalpha()) >= 3:
        return False
    return True


def _trim_leading_structural_org_phrase_lead(candidate: str) -> tuple[str, int] | None:
    token_matches = list(TOKEN_RE.finditer(candidate))
    if len(token_matches) < 2:
        return None
    normalized_tokens = [normalize_lookup_text(match.group(0)) for match in token_matches]
    if normalized_tokens[0] not in _GENERIC_PHRASE_LEADS:
        return None
    if normalized_tokens[1] not in (
        _HEURISTIC_STRUCTURAL_ORG_SUFFIX_TOKENS | _STRUCTURAL_ORG_CONNECTOR_HEAD_TOKENS
    ):
        return None
    offset = token_matches[1].start()
    return _collapse_whitespace(candidate[offset:]), offset


def _trim_leading_structural_org_role_phrase(candidate: str) -> tuple[str, int] | None:
    token_matches = list(TOKEN_RE.finditer(candidate))
    if len(token_matches) < 3:
        return None
    normalized_tokens = [normalize_lookup_text(match.group(0)) for match in token_matches]
    if normalized_tokens[0] not in _GENERIC_OFFICE_TITLE_TOKENS:
        return None
    connector_index = next(
        (index for index, token in enumerate(normalized_tokens[1:], start=1) if token in _STRUCTURAL_ORG_CONNECTOR_TOKENS),
        None,
    )
    if connector_index is None or connector_index + 1 >= len(token_matches):
        return None
    offset = token_matches[connector_index + 1].start()
    return _collapse_whitespace(candidate[offset:]), offset


def _collect_heuristic_structured_locations(
    text: str,
) -> list[HeuristicStructuredLocation]:
    kept: dict[str, HeuristicStructuredLocation] = {}

    for match in _HEURISTIC_STRUCTURAL_LOCATION_RE.finditer(text):
        candidate_text = _collapse_whitespace(match.group("loc"))
        if not _is_plausible_heuristic_structured_location_candidate(candidate_text):
            continue
        candidate = HeuristicStructuredLocation(
            text=candidate_text,
            start=match.start("loc"),
            end=match.end("loc"),
        )
        key = normalize_lookup_text(candidate_text)
        current = kept.get(key)
        if current is None or candidate.start < current.start or (
            candidate.start == current.start and candidate.end > current.end
        ):
            kept[key] = candidate
    return sorted(kept.values(), key=lambda item: (item.start, item.end))


def _is_plausible_heuristic_structured_location_candidate(candidate: str) -> bool:
    tokens = TOKEN_RE.findall(candidate)
    if len(tokens) < 3:
        return False
    normalized_tokens = [normalize_lookup_text(token) for token in tokens]
    if normalized_tokens[0] not in _STRUCTURAL_LOCATION_HEAD_TOKENS:
        return False
    if normalized_tokens[1] != "of":
        return False
    if len(tokens) >= 4 and sum(token.isupper() for token in tokens if token.isalpha()) >= 3:
        return False
    return not _looks_like_adjectival_location_tail(tokens[-1])


def _collect_heuristic_acronym_definitions(
    text: str,
    *,
    allowed_labels: set[str],
) -> list[HeuristicAcronymDefinition]:
    kept: dict[tuple[str, str], HeuristicAcronymDefinition] = {}

    for pattern in (_EXPLICIT_ACRONYM_PAREN_RE, _EXPLICIT_ACRONYM_COMMA_RE):
        for match in pattern.finditer(text):
            long_form_match = _match_explicit_acronym_long_form(
                text,
                definition_boundary=match.start(),
            )
            if long_form_match is None:
                continue
            long_form, definition_start = long_form_match
            normalized_acronym = _normalize_acronym_text(match.group("acronym"))
            if not _matches_explicit_acronym_initialism(
                long_form,
                normalized_acronym,
            ):
                continue
            label = _infer_heuristic_acronym_label(long_form)
            if label is None or label.casefold() not in allowed_labels:
                continue
            definition = HeuristicAcronymDefinition(
                acronym_text=match.group("acronym"),
                normalized_acronym=normalized_acronym,
                label=label,
                long_form=long_form,
                definition_start=definition_start,
                definition_end=match.end(),
                acronym_start=match.start("acronym"),
                acronym_end=match.end("acronym"),
            )
            key = (definition.normalized_acronym, definition.label.casefold())
            current = kept.get(key)
            if current is None or definition.definition_start < current.definition_start:
                kept[key] = definition
    return sorted(
        kept.values(),
        key=lambda item: (item.acronym_start, item.acronym_end, item.label.casefold()),
    )


def _match_explicit_acronym_long_form(
    text: str,
    *,
    definition_boundary: int,
) -> tuple[str, int] | None:
    window_start = max(0, definition_boundary - _MAX_EXPLICIT_ACRONYM_LONGFORM_WINDOW)
    prefix = text[window_start:definition_boundary]
    match = _EXPLICIT_ACRONYM_LONGFORM_RE.search(prefix)
    if match is None:
        return None
    long_form = _collapse_whitespace(match.group("long"))
    if _span_token_count(long_form) < 2:
        return None
    return long_form, window_start + match.start("long")


def _matches_explicit_acronym_initialism(
    long_form: str,
    normalized_acronym: str,
) -> bool:
    if _derive_initialism(long_form) == normalized_acronym:
        return True
    tokens = TOKEN_RE.findall(long_form)
    while (
        len(tokens) >= 3
        and normalize_lookup_text(tokens[-1])
        in _OPTIONAL_EXPLICIT_ACRONYM_TRAILING_SUFFIX_TOKENS
    ):
        tokens = tokens[:-1]
        if _derive_initialism(" ".join(tokens)) == normalized_acronym:
            return True
    return False


def _infer_heuristic_acronym_label(long_form: str) -> str | None:
    tokens = [normalize_lookup_text(token) for token in TOKEN_RE.findall(long_form)]
    if len(tokens) < 2:
        return None
    if tokens[0] in _STRUCTURAL_LOCATION_HEAD_TOKENS and "of" in tokens[1:]:
        return "location"
    if tokens[-1] in _HEURISTIC_ACRONYM_LOCATION_SUFFIX_TOKENS:
        return "location"
    if tokens[-1] in _STRUCTURAL_ORG_TRAILING_SUFFIX_TOKENS:
        return "organization"
    if len(tokens) >= 3:
        return "organization"
    return None


def _backfill_contextual_org_acronym_entities(
    extracted: list[ExtractedCandidate],
    *,
    normalized_input: NormalizedText,
    runtime: PackRuntime,
) -> list[ExtractedCandidate]:
    existing_spans = {
        (candidate.normalized_start, candidate.normalized_end) for candidate in extracted
    }
    augmented = list(extracted)

    for start, end in _iter_contextual_acronym_spans(normalized_input.text):
        token_text = normalized_input.text[start:end]
        normalized_acronym = _normalize_acronym_text(token_text)
        if (
            len(normalized_acronym) not in _CONTEXTUAL_ORG_ACRONYM_LENGTH_RANGE
            or not _is_single_token_all_caps(token_text)
        ):
            continue
        span_key = (start, end)
        if span_key in existing_spans:
            continue
        if not _supports_contextual_org_acronym_backfill(
            normalized_input.text,
            start=start,
            end=end,
        ):
            continue
        raw_start, raw_end = normalized_input.raw_span(start, end)
        augmented.append(
            ExtractedCandidate(
                entity=EntityMatch(
                    text=normalized_input.raw_text[raw_start:raw_end],
                    label="organization",
                    start=raw_start,
                    end=raw_end,
                    confidence=0.68,
                    provenance=_build_provenance(
                        match_kind="alias",
                        match_path="heuristic.contextual_acronym",
                        match_source=token_text,
                        source_pack=runtime.pack_id,
                        source_domain=runtime.domain,
                        lane="contextual_org_acronym_backfill",
                    ),
                    link=_build_runtime_generated_entity_link(
                        provider="heuristic.contextual_acronym",
                        runtime=runtime,
                        label="organization",
                        canonical_text=token_text,
                    ),
                ),
                domain=runtime.domain,
                lane="contextual_org_acronym_backfill",
                normalized_start=start,
                normalized_end=end,
            )
        )
        existing_spans.add(span_key)
    return augmented


def _iter_contextual_acronym_spans(text: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for start, end in _iter_token_spans(text):
        token_text = text[start:end]
        candidate_spans = [(start, end)]
        if "/" in token_text:
            candidate_spans = []
            cursor = start
            for part in token_text.split("/"):
                part_end = cursor + len(part)
                if part:
                    candidate_spans.append((cursor, part_end))
                cursor = part_end + 1
        for span in candidate_spans:
            if span in seen:
                continue
            seen.add(span)
            spans.append(span)
    return spans


def _supports_contextual_org_acronym_backfill(
    text: str,
    *,
    start: int,
    end: int,
) -> bool:
    previous_tokens = _previous_normalized_tokens(text, start, limit=4)
    previous_lower = previous_tokens[-1] if previous_tokens else ""
    if previous_lower in _CONTEXTUAL_ORG_ACRONYM_REPORTING_CUES:
        return True
    suffix = text[end : end + 2]
    if suffix in {"'s", "’s"}:
        return True
    if _supports_source_attribution_acronym(text, start=start, end=end, previous_tokens=previous_tokens):
        return True
    if _supports_preceding_context_noun_contextual_org_acronym(
        text,
        start=start,
        end=end,
        previous_tokens=previous_tokens,
    ):
        return True
    if _supports_article_led_contextual_org_acronym(
        text,
        start=start,
        end=end,
        previous_tokens=previous_tokens,
    ):
        return True
    if _supports_slash_paired_contextual_org_acronym(text, start=start, end=end):
        return True
    if _supports_prepositional_contextual_org_acronym(previous_tokens):
        return True
    if _supports_following_contextual_org_acronym(
        text,
        start=start,
        end=end,
        previous_tokens=previous_tokens,
    ):
        return True
    return False


def _supports_preceding_context_noun_contextual_org_acronym(
    text: str,
    *,
    start: int,
    end: int,
    previous_tokens: list[str],
) -> bool:
    if not previous_tokens or previous_tokens[-1] not in _CONTEXTUAL_ORG_ACRONYM_CONTEXT_NOUN_CUES:
        return False
    next_token_raw = _next_token(text, end).rstrip(".,;:!?)]}”’\"'")
    next_token = normalize_lookup_text(next_token_raw)
    if next_token in _CONTEXTUAL_ORG_ACRONYM_FOLLOWING_CUES:
        return True
    if previous_tokens[-1] in {"agency", "association", "bureau", "commission"}:
        return not next_token or bool(re.fullmatch(r"[A-Za-z][A-Za-z'’-]*", next_token_raw))
    return False


def _supports_source_attribution_acronym(
    text: str,
    *,
    start: int,
    end: int,
    previous_tokens: list[str],
) -> bool:
    if not previous_tokens or previous_tokens[-1] != "source":
        return False
    left_slice = text[max(0, start - 4) : start]
    if ":" not in left_slice:
        return False
    return text[end : end + 1] in {"/", ":"} or True


def _supports_article_led_contextual_org_acronym(
    text: str,
    *,
    start: int,
    end: int,
    previous_tokens: list[str],
) -> bool:
    if not previous_tokens or previous_tokens[-1] not in {"a", "an", "the"}:
        return False
    next_token = normalize_lookup_text(_next_token(text, end).rstrip(".,;:!?)]}”’\"'"))
    if not next_token:
        return False
    return next_token in (
        _AUXILIARY_LOOKUP_TOKENS
        | _CONTEXTUAL_ORG_ACRONYM_CONTEXT_NOUN_CUES
        | _CONTEXTUAL_ORG_ACRONYM_FOLLOWING_CUES
    )


def _supports_slash_paired_contextual_org_acronym(text: str, *, start: int, end: int) -> bool:
    paired_token = ""
    if start > 0 and text[start - 1] == "/":
        paired_token = _previous_token(text, start - 1)
    elif end < len(text) and text[end : end + 1] == "/":
        paired_token = _next_token(text, end + 1)
    if not _is_single_token_all_caps(paired_token):
        return False
    next_token = normalize_lookup_text(_next_token(text, end).rstrip(".,;:!?)]}”’\"'"))
    return next_token in (
        _CONTEXTUAL_ORG_ACRONYM_CONTEXT_NOUN_CUES
        | _CONTEXTUAL_ORG_ACRONYM_FOLLOWING_CUES
    )


def _supports_prepositional_contextual_org_acronym(previous_tokens: list[str]) -> bool:
    if len(previous_tokens) < 2:
        return False
    if previous_tokens[-1] in _CONTEXTUAL_ORG_ACRONYM_PREPOSITION_CUES:
        return previous_tokens[-2] in _CONTEXTUAL_ORG_ACRONYM_CONTEXT_NOUN_CUES
    if (
        len(previous_tokens) >= 3
        and previous_tokens[-1] in {"a", "an", "the"}
        and previous_tokens[-2] in _CONTEXTUAL_ORG_ACRONYM_PREPOSITION_CUES
    ):
        return previous_tokens[-3] in _CONTEXTUAL_ORG_ACRONYM_CONTEXT_NOUN_CUES
    return False


def _supports_following_contextual_org_acronym(
    text: str,
    *,
    start: int,
    end: int,
    previous_tokens: list[str],
) -> bool:
    if previous_tokens and previous_tokens[-1] not in {"a", "an", "the"}:
        if not _has_contextual_acronym_clause_boundary(text, start=start):
            return False
    next_token = _next_token(text, end).rstrip(".,;:!?)]}”’\"'")
    return normalize_lookup_text(next_token) in _CONTEXTUAL_ORG_ACRONYM_FOLLOWING_CUES


def _has_contextual_acronym_clause_boundary(text: str, *, start: int) -> bool:
    boundary_slice = text[max(0, start - 8) : start]
    return bool(re.search(r"[,:;.!?\-–—\"“”'’)\]]", boundary_slice))


def _looks_like_adjectival_location_tail(token_text: str) -> bool:
    normalized = normalize_lookup_text(token_text)
    if len(normalized) <= 3:
        return False
    return normalized.endswith(("ese", "ian", "ish", "ist", "ite")) or (
        normalized.endswith("i") and len(normalized) > 4
    )


def _is_lowercase_structural_org_tail_token(token_text: str) -> bool:
    if not token_text.isalpha() or not token_text.islower():
        return False
    normalized = normalize_lookup_text(token_text)
    if len(normalized) < 5:
        return False
    if normalized in _GENERIC_PHRASE_LEADS or normalized in _STRUCTURAL_ORG_CONNECTOR_TOKENS:
        return False
    return True


def _previous_normalized_tokens(text: str, start: int, *, limit: int) -> list[str]:
    tokens = [normalize_lookup_text(match.group(0)) for match in TOKEN_RE.finditer(text[:start])]
    if not tokens:
        return []
    return tokens[-limit:]


def _structural_fragment_penalty(candidate: ExtractedCandidate, *, text: str) -> float:
    if candidate.domain != "general" or candidate.lane != "deterministic_alias":
        return 0.0
    label_key = candidate.entity.label.casefold()
    if _looks_like_article_led_structural_org_fragment(
        text,
        start=candidate.normalized_start,
        end=candidate.normalized_end,
    ):
        return 0.42 if label_key == "organization" else 0.56
    if label_key == "organization":
        if _looks_like_trailing_structural_org_fragment(
            text,
            start=candidate.normalized_start,
            end=candidate.normalized_end,
        ) or _looks_like_leading_structural_org_fragment(
            text,
            start=candidate.normalized_start,
            end=candidate.normalized_end,
        ):
            return 0.28
    if label_key == "location" and _looks_like_leading_structural_org_fragment(
        text,
        start=candidate.normalized_start,
        end=candidate.normalized_end,
    ):
        return 0.42
    return 0.0


def _structural_location_label_penalty(candidate: ExtractedCandidate) -> float:
    if candidate.domain != "general" or candidate.lane != "deterministic_alias":
        return 0.0
    if candidate.entity.label.casefold() != "location":
        return 0.0
    if not _is_plausible_heuristic_structured_organization_candidate(candidate.entity.text):
        return 0.0
    return 0.18


def _looks_like_trailing_structural_org_fragment(text: str, *, start: int, end: int) -> bool:
    connector = _previous_contiguous_token_span(text, start)
    if connector is not None and normalize_lookup_text(connector[2]) in _STRUCTURAL_ORG_CONNECTOR_TOKENS:
        previous = _previous_contiguous_token_span(text, connector[0])
        if previous is not None and _is_titleish_token(previous[2]):
            return True
    connector = _next_contiguous_token_span(text, end)
    if connector is None or normalize_lookup_text(connector[2]) not in _STRUCTURAL_ORG_CONNECTOR_TOKENS:
        return False
    following = _next_contiguous_token_span(text, connector[1])
    if following is None:
        return False
    if normalize_lookup_text(following[2]) == "the":
        following = _next_contiguous_token_span(text, following[1])
    return following is not None and _is_titleish_token(following[2])


def _looks_like_leading_structural_org_fragment(text: str, *, start: int, end: int) -> bool:
    span_tokens = TOKEN_RE.findall(text[start:end])
    normalized_tokens = [normalize_lookup_text(token) for token in span_tokens]
    if not normalized_tokens:
        return False
    previous = _previous_contiguous_token_span(text, start)
    if (
        previous is not None
        and _is_titleish_token(previous[2])
        and normalized_tokens[-1] in _HEURISTIC_STRUCTURAL_ORG_SUFFIX_TOKENS
    ):
        return True
    next_token = _next_contiguous_token_span(text, end)
    if next_token is not None and normalize_lookup_text(next_token[2]) in _HEURISTIC_STRUCTURAL_ORG_SUFFIX_TOKENS:
        return True
    if next_token is not None and (
        _is_titleish_token(next_token[2]) or _is_lowercase_structural_org_tail_token(next_token[2])
    ):
        next_next = _next_contiguous_token_span(text, next_token[1])
        if next_next is not None and normalize_lookup_text(next_next[2]) in (
            _HEURISTIC_STRUCTURAL_ORG_SUFFIX_TOKENS | _STRUCTURAL_ORG_CONNECTOR_HEAD_TOKENS
        ):
            return True
    if len(normalized_tokens) == 1 and normalized_tokens[0] in _HEURISTIC_STRUCTURAL_ORG_SUFFIX_TOKENS:
        return previous is not None and _is_titleish_token(previous[2])
    return False


def _looks_like_article_led_structural_org_fragment(
    text: str,
    *,
    start: int,
    end: int,
) -> bool:
    span_tokens = TOKEN_RE.findall(text[start:end])
    normalized_tokens = [normalize_lookup_text(token) for token in span_tokens]
    if len(normalized_tokens) != 2 or normalized_tokens[0] not in _GENERIC_PHRASE_LEADS:
        return False
    if normalized_tokens[1] not in (
        _GENERIC_SINGLE_TOKEN_HEADS | _ARTICLE_LED_STRUCTURAL_FRAGMENT_TOKENS
    ):
        return False
    current = _next_contiguous_token_span(text, end)
    if current is None:
        return False
    if normalize_lookup_text(current[2]) in _STRUCTURAL_ORG_CONNECTOR_TOKENS:
        current = _next_contiguous_token_span(text, current[1])
        if current is not None and normalize_lookup_text(current[2]) == "the":
            current = _next_contiguous_token_span(text, current[1])
    if current is None or not _is_titleish_token(current[2]):
        return False
    for _ in range(3):
        following = _next_contiguous_token_span(text, current[1])
        if following is None:
            return False
        normalized_following = normalize_lookup_text(following[2])
        if normalized_following in (
            _HEURISTIC_STRUCTURAL_ORG_SUFFIX_TOKENS | _STRUCTURAL_ORG_CONNECTOR_HEAD_TOKENS
        ):
            return True
        if normalized_following in _STRUCTURAL_ORG_CONNECTOR_TOKENS:
            current = _next_contiguous_token_span(text, following[1])
            if current is None:
                return False
            if normalize_lookup_text(current[2]) == "the":
                current = _next_contiguous_token_span(text, current[1])
                if current is None:
                    return False
            if not _is_titleish_token(current[2]):
                return False
            continue
        if not (
            _is_titleish_token(following[2]) or _is_lowercase_structural_org_tail_token(following[2])
        ):
            return False
        current = following
    return False


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


def _relevance_occurrence_text(candidate: ExtractedCandidate) -> str:
    canonical_text = (
        candidate.entity.link.canonical_text
        if candidate.entity.link is not None
        else candidate.entity.text
    )
    if (
        candidate.lane == "deterministic_alias"
        and _is_exact_expansion_acronym_alias(
            matched_text=candidate.entity.text,
            candidate_value=candidate.entity.text,
            canonical_text=canonical_text,
        )
    ):
        return candidate.entity.text
    return canonical_text


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
    occurrence_text = _relevance_occurrence_text(candidate)
    occurrence_bonus = min(0.18, max(0, _count_occurrences(text, occurrence_text) - 1) * 0.06)
    lane_bonus = {
        "deterministic_rule": 0.18,
        "deterministic_alias": 0.12,
        "heuristic_structured_org_backfill": 0.14,
        "heuristic_structured_location_backfill": 0.14,
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

    fragment_penalty = _structural_fragment_penalty(candidate, text=text)
    label_conflict_penalty = _structural_location_label_penalty(candidate)

    return _clamp_score(
        (confidence * 0.38)
        + (position_score * 0.14)
        + lane_bonus
        + domain_bonus
        + span_bonus
        + occurrence_bonus
        - fragment_penalty
        - label_conflict_penalty
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


def _is_runtime_generic_duration_alias(candidate: ExtractedCandidate) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    for raw_value in (
        candidate.entity.text,
        provenance.match_source,
        candidate.entity.link.canonical_text if candidate.entity.link is not None else None,
    ):
        if not raw_value:
            continue
        normalized = " ".join(str(raw_value).casefold().split())
        if _GENERIC_DURATION_ALIAS_RE.fullmatch(normalized):
            return True
    return False


def _runtime_normalized_alias_values(candidate: ExtractedCandidate) -> set[str]:
    provenance = candidate.entity.provenance
    values: set[str] = set()
    for raw_value in (
        candidate.entity.text,
        provenance.match_source if provenance is not None else None,
        candidate.entity.link.canonical_text if candidate.entity.link is not None else None,
    ):
        if not raw_value:
            continue
        normalized = " ".join(str(raw_value).casefold().split())
        if normalized:
            values.add(normalized)
    return values


def _contains_apostropheish_suffix(value: str) -> bool:
    return bool(
        re.search(
            r"(?:['’ʻ`]|[\u2000-\u200b\u202f\u205f\u3000])s\b",
            value,
            flags=re.IGNORECASE,
        )
    )


def _normalize_apostropheish_alias_text(value: str) -> str:
    return " ".join(
        re.sub(r"[\'’ʻ`]+", " ", value.casefold()).split()
    )


def _is_runtime_possessive_alias_artifact(candidate: ExtractedCandidate) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    if candidate.lane != "deterministic_alias":
        return False
    if candidate.entity.label.casefold() not in {"organization", "location"}:
        return False
    surface = candidate.entity.text
    if not _contains_apostropheish_suffix(surface):
        return False
    source_values = [
        provenance.match_source or "",
        candidate.entity.link.canonical_text if candidate.entity.link is not None else "",
    ]
    if any(_contains_apostropheish_suffix(value) for value in source_values if value):
        return False
    normalized_surface = _normalize_apostropheish_alias_text(surface)
    return any(
        value
        and _normalize_apostropheish_alias_text(value) == normalized_surface
        and value.casefold() != surface.casefold()
        for value in source_values
    )


def _is_runtime_leading_apostrophe_fragment_alias(candidate: ExtractedCandidate) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    if candidate.entity.label.casefold() not in {"organization", "person", "location"}:
        return False
    if re.match(r"^[sdt]\s+[A-Z]", candidate.entity.text) is None:
        return False
    return True


def _is_runtime_generic_single_token_alias(candidate: ExtractedCandidate) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    if candidate.lane != "deterministic_alias":
        return False
    if not _is_single_token_alpha(candidate.entity.text):
        return False
    blocklist = (
        _GENERIC_SINGLE_TOKEN_HEADS
        | _GENERIC_PHRASE_LEADS
        | _CALENDAR_SINGLE_TOKEN_WORDS
        | _NUMBER_SINGLE_TOKEN_WORDS
        | _RUNTIME_GENERIC_SINGLE_TOKEN_ALIAS_TEXTS
    )
    normalized_values = _runtime_normalized_alias_values(candidate)
    if (
        candidate.entity.label.casefold() == "organization"
        and _is_single_token_all_caps(candidate.entity.text)
        and any(value in _RUNTIME_GENERIC_ORG_ACRONYM_ALIAS_TEXTS for value in normalized_values)
    ):
        return True
    return any(value in blocklist for value in normalized_values)


def _looks_like_reporting_attribution_contextual_org_acronym(
    candidate: ExtractedCandidate,
    *,
    text: str,
) -> bool:
    if _single_token_occurrence_count(text, candidate.entity.text) != 1:
        return False
    previous_tokens = _previous_normalized_tokens(text, candidate.entity.start, limit=4)
    if not previous_tokens:
        return False
    reporting_context = previous_tokens[-1] in _CONTEXTUAL_ORG_ACRONYM_REPORTING_CUES or (
        len(previous_tokens) >= 2
        and previous_tokens[-1] in {"a", "an", "the"}
        and previous_tokens[-2] in _CONTEXTUAL_ORG_ACRONYM_REPORTING_CUES
    )
    if not reporting_context:
        return False
    next_token = normalize_lookup_text(_next_token(text, candidate.entity.end).rstrip(".,;:!?)]}”’\\\"'"))
    return next_token in _RUNTIME_ATTRIBUTION_CONTEXTUAL_ORG_ACRONYM_FOLLOWING_TOKENS


def _is_runtime_contextual_org_acronym_noise(
    candidate: ExtractedCandidate,
    *,
    text: str | None = None,
) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    if candidate.lane != "contextual_org_acronym_backfill":
        return False
    if candidate.entity.label.casefold() != "organization":
        return False
    if not _is_single_token_all_caps(candidate.entity.text):
        return False
    normalized_values = _runtime_normalized_alias_values(candidate)
    if any(value in _RUNTIME_GENERIC_ORG_ACRONYM_ALIAS_TEXTS for value in normalized_values):
        return True
    if text is None:
        return False
    return _looks_like_reporting_attribution_contextual_org_acronym(candidate, text=text)


def _is_runtime_generic_phrase_fragment_alias(candidate: ExtractedCandidate) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    normalized_values = _runtime_normalized_alias_values(candidate)
    return any(value in _RUNTIME_GENERIC_PHRASE_ALIAS_TEXTS for value in normalized_values)


def _is_runtime_function_year_alias(candidate: ExtractedCandidate) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    return any(
        _RUNTIME_FUNCTION_YEAR_ALIAS_RE.fullmatch(value)
        for value in _runtime_normalized_alias_values(candidate)
    )


def _runtime_alias_tokens(value: str) -> list[str]:
    return [token.casefold() for token in re.findall(r"[A-Za-z]+|&", value)]


def _is_runtime_truncated_connector_alias_artifact(candidate: ExtractedCandidate) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    if candidate.lane != "deterministic_alias":
        return False
    if candidate.entity.label.casefold() != "organization":
        return False
    surface_tokens = _runtime_alias_tokens(candidate.entity.text)
    if not surface_tokens:
        return False
    for raw_value in (
        provenance.match_source or "",
        candidate.entity.link.canonical_text if candidate.entity.link is not None else "",
    ):
        source_tokens = _runtime_alias_tokens(raw_value)
        if len(source_tokens) <= len(surface_tokens):
            continue
        if source_tokens[: len(surface_tokens)] != surface_tokens:
            continue
        if source_tokens[len(surface_tokens)] in {"&", "and"}:
            return True
    return False


def _is_runtime_date_like_alias(candidate: ExtractedCandidate) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    return any(_DATE_LIKE_ALIAS_RE.fullmatch(value) for value in _runtime_normalized_alias_values(candidate))


def _is_runtime_quoted_fragment_alias(candidate: ExtractedCandidate) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    surface = candidate.entity.text
    if '"' not in surface and "“" not in surface and "”" not in surface:
        return False
    normalized = " ".join(surface.casefold().split())
    if '"' in surface and surface.count('"') % 2 == 1:
        return True
    return normalized.startswith("the ")


def _is_runtime_article_led_generic_alias(candidate: ExtractedCandidate) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    if candidate.entity.label.casefold() not in {"organization", "location"}:
        return False
    normalized_tokens = [
        normalize_lookup_text(token)
        for token in TOKEN_RE.findall(candidate.entity.text)
        if any(character.isalpha() for character in token)
    ]
    if len(normalized_tokens) != 2 or normalized_tokens[0] not in _GENERIC_PHRASE_LEADS:
        return False
    return normalized_tokens[1] in _RUNTIME_ARTICLE_LED_GENERIC_ALIAS_TOKENS


def _is_runtime_article_led_generic_org_alias(candidate: ExtractedCandidate) -> bool:
    return _is_runtime_article_led_generic_alias(candidate)


def _is_runtime_single_token_titleish_fragment_alias(
    candidate: ExtractedCandidate,
    *,
    text: str,
) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    if candidate.lane != "deterministic_alias":
        return False
    if candidate.entity.label.casefold() not in {"organization", "location"}:
        return False
    surface = candidate.entity.text.strip()
    if not surface.isalpha() or surface.isupper():
        return False
    normalized = normalize_lookup_text(surface)
    if normalized in (
        _GENERIC_SINGLE_TOKEN_HEADS
        | _GENERIC_PHRASE_LEADS
        | _CALENDAR_SINGLE_TOKEN_WORDS
        | _NUMBER_SINGLE_TOKEN_WORDS
        | _RUNTIME_GENERIC_SINGLE_TOKEN_ALIAS_TEXTS
    ):
        return False
    previous_span = _previous_contiguous_token_span(text, candidate.entity.start)
    next_span = _next_contiguous_token_span(text, candidate.entity.end)
    return _is_titleish_token(previous_span[2] if previous_span is not None else "") or _is_titleish_token(
        next_span[2] if next_span is not None else ""
    )


def _is_runtime_partial_person_fragment_alias(candidate: ExtractedCandidate) -> bool:
    provenance = candidate.entity.provenance
    if provenance is None or provenance.source_pack != "general-en":
        return False
    if candidate.entity.label.casefold() != "person":
        return False
    tokens = [
        token
        for token in TOKEN_RE.findall(candidate.entity.text)
        if any(character.isalpha() for character in token)
    ]
    if len(tokens) < 2:
        return False
    last_normalized = normalize_lookup_text(tokens[-1])
    canonical_values = _runtime_normalized_alias_values(candidate) - {
        " ".join(candidate.entity.text.casefold().split())
    }
    if not canonical_values:
        return False
    return (
        last_normalized in _PERSON_NAME_PARTICLES
        or last_normalized in _PERSON_NAME_SUFFIX_TOKENS
        or len(last_normalized) == 1
    )


def _runtime_general_en_discard_reason(
    candidate: ExtractedCandidate,
    *,
    text: str,
) -> str | None:
    if _is_runtime_generic_duration_alias(candidate):
        return "runtime_generic_duration_alias"
    if _is_runtime_contextual_org_acronym_noise(candidate, text=text):
        return "runtime_contextual_org_acronym_noise"
    if _is_runtime_possessive_alias_artifact(candidate):
        return "runtime_possessive_alias_artifact"
    if _is_runtime_truncated_connector_alias_artifact(candidate):
        return "runtime_truncated_connector_alias_artifact"
    if _is_runtime_leading_apostrophe_fragment_alias(candidate):
        return "runtime_leading_apostrophe_fragment_alias"
    if _is_runtime_date_like_alias(candidate):
        return "runtime_date_like_alias"
    if _is_runtime_function_year_alias(candidate):
        return "runtime_function_year_alias"
    if _is_runtime_quoted_fragment_alias(candidate):
        return "runtime_quoted_fragment_alias"
    if _is_runtime_generic_phrase_fragment_alias(candidate):
        return "runtime_generic_phrase_fragment_alias"
    if _is_runtime_article_led_generic_alias(candidate):
        return "runtime_article_led_generic_alias"
    if _is_runtime_generic_single_token_alias(candidate):
        return "runtime_generic_single_token_alias"
    if _is_runtime_single_token_titleish_fragment_alias(candidate, text=text):
        return "runtime_single_token_titleish_fragment_alias"
    if _is_runtime_partial_person_fragment_alias(candidate):
        return "runtime_partial_person_fragment_alias"
    return None


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


def _merge_linked_entity_candidates(
    candidates: list[ExtractedCandidate],
) -> tuple[list[ExtractedCandidate], list[ExtractedCandidate]]:
    merged: list[ExtractedCandidate] = []
    merged_discards: list[ExtractedCandidate] = []
    groups: dict[str, list[ExtractedCandidate]] = {}
    ordered_keys: list[str] = []

    for candidate in candidates:
        link = candidate.entity.link
        entity_id = link.entity_id.strip() if link is not None else ""
        if not entity_id:
            merged.append(candidate)
            continue
        if entity_id not in groups:
            ordered_keys.append(entity_id)
            groups[entity_id] = []
        groups[entity_id].append(candidate)

    for entity_id in ordered_keys:
        group = groups[entity_id]
        representative = min(
            group,
            key=lambda item: (
                item.entity.start,
                item.entity.end,
                -(item.entity.relevance if item.entity.relevance is not None else (item.entity.confidence or 0.0)),
                item.entity.label.casefold(),
                item.entity.text.casefold(),
            ),
        )
        if len(group) == 1:
            merged.append(representative)
            continue
        merged_discards.extend(item for item in group if item is not representative)
        alias_values: list[str] = []
        seen_aliases: set[str] = set()
        for item in group:
            values = item.entity.aliases or [item.entity.text]
            for value in values:
                normalized = value.casefold()
                if not value or normalized in seen_aliases:
                    continue
                seen_aliases.add(normalized)
                alias_values.append(value)
        confidence_values = [
            item.entity.confidence for item in group if item.entity.confidence is not None
        ]
        relevance_values = [
            item.entity.relevance for item in group if item.entity.relevance is not None
        ]
        mention_count = sum(max(item.entity.mention_count, 1) for item in group)
        merged_entity = representative.entity.model_copy(
            update={
                "aliases": alias_values,
                "mention_count": mention_count,
                "confidence": max(confidence_values) if confidence_values else None,
                "relevance": max(relevance_values) if relevance_values else None,
            }
        )
        merged.append(replace(representative, entity=merged_entity))

    return merged, merged_discards


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


def _elapsed_ms(start: float) -> int:
    return int((perf_counter() - start) * 1000)


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
    runtime: PackRuntime | None = None,
) -> TagResponse:
    """Tag text using installed pack rules and indexed alias lookup."""

    start = perf_counter()
    warnings: list[str] = []
    phase_start = perf_counter()
    bounded_text = _truncate_input(text, warnings)
    normalized_input = normalize_text(bounded_text, content_type)
    if not normalized_input.text:
        warnings.append("empty_input")
    if content_type not in {"text/plain", "text/html"}:
        warnings.append(f"unsupported_content_type:{content_type}")
    normalization_ms = _elapsed_ms(phase_start)

    registry = registry or PackRegistry(storage_root)
    runtime = (
        runtime
        if runtime is not None and runtime.pack_id == pack
        else load_pack_runtime(storage_root, pack, registry=registry)
    )
    if runtime is None:
        warnings.append(f"pack_not_installed:{pack}")
        elapsed_ms = _elapsed_ms(start)
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
            total_time_ms=elapsed_ms,
            timing_breakdown_ms={
                "normalization_ms": normalization_ms,
                "segmentation_ms": 0,
                "lookup_rule_extraction_ms": 0,
                "overlap_resolution_ms": 0,
                "final_metric_assembly_ms": max(0, elapsed_ms - normalization_ms),
            },
            timing_ms=elapsed_ms,
            metrics=_build_metrics(
                raw_text=bounded_text,
                normalized_input=normalized_input,
                extracted=[],
                segment_count=0,
                overlap_drop_count=0,
            ),
        )

    phase_start = perf_counter()
    segments = segment_text(
        normalized_input.text,
        language=runtime.language,
        max_chars=MAX_SEGMENT_CHARS,
        max_tokens=MAX_SEGMENT_TOKENS,
    )
    segmentation_ms = _elapsed_ms(phase_start)
    phase_start = perf_counter()
    extracted: list[ExtractedCandidate] = []
    hybrid_requested = hybrid_enabled(hybrid)
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
        if hybrid_requested:
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
    lookup_rule_extraction_ms = _elapsed_ms(phase_start)

    phase_start = perf_counter()
    deduped, duplicate_discards = _dedupe_exact_candidates(extracted)
    coherent = _apply_repeated_surface_consistency(deduped)
    heuristic_structured_org_backfilled = _backfill_heuristic_structured_organization_entities(
        coherent,
        normalized_input=normalized_input,
        runtime=runtime,
    )
    heuristic_structured_org_deduped, heuristic_structured_org_duplicate_discards = (
        _dedupe_exact_candidates(heuristic_structured_org_backfilled)
    )
    coherent = _apply_repeated_surface_consistency(heuristic_structured_org_deduped)
    heuristic_structured_location_backfilled = _backfill_heuristic_structured_location_entities(
        coherent,
        normalized_input=normalized_input,
        runtime=runtime,
    )
    heuristic_structured_location_deduped, heuristic_structured_location_duplicate_discards = (
        _dedupe_exact_candidates(heuristic_structured_location_backfilled)
    )
    coherent = _apply_repeated_surface_consistency(heuristic_structured_location_deduped)
    heuristic_hyphenated_location_backfilled = _backfill_heuristic_hyphenated_location_entities(
        coherent,
        normalized_input=normalized_input,
        runtime=runtime,
    )
    heuristic_hyphenated_location_deduped, heuristic_hyphenated_location_duplicate_discards = (
        _dedupe_exact_candidates(heuristic_hyphenated_location_backfilled)
    )
    coherent = _apply_repeated_surface_consistency(heuristic_hyphenated_location_deduped)
    extended = _extend_structural_entity_spans(
        coherent,
        normalized_input=normalized_input,
    )
    extended_deduped, extension_duplicate_discards = _dedupe_exact_candidates(extended)
    coherent = _apply_repeated_surface_consistency(extended_deduped)
    heuristic_definition_backfilled = _backfill_heuristic_definition_acronym_entities(
        coherent,
        normalized_input=normalized_input,
        runtime=runtime,
    )
    heuristic_definition_deduped, heuristic_definition_duplicate_discards = _dedupe_exact_candidates(
        heuristic_definition_backfilled
    )
    coherent = _apply_repeated_surface_consistency(heuristic_definition_deduped)
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
    resolved = _backfill_contextual_org_acronym_entities(
        resolved,
        normalized_input=normalized_input,
        runtime=runtime,
    )
    resolved, merged_entity_discards = _merge_linked_entity_candidates(resolved)
    resolved, surname_coreference_discards = _suppress_document_person_surname_false_positives(
        resolved,
        text=bounded_text,
        pack_id=runtime.pack_id,
    )

    total_duplicate_discards = (
        duplicate_discards
        + heuristic_structured_org_duplicate_discards
        + heuristic_structured_location_duplicate_discards
        + heuristic_hyphenated_location_duplicate_discards
        + extension_duplicate_discards
        + heuristic_definition_duplicate_discards
        + acronym_duplicate_discards
    )
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
    if debug_payload is not None and merged_entity_discards:
        span_decisions = list(debug_payload.span_decisions)
        for item in merged_entity_discards:
            span_decisions.append(
                TagSpanDecision(
                    text=item.entity.text,
                    label=item.entity.label,
                    start=item.entity.start,
                    end=item.entity.end,
                    lane=item.lane,
                    kept=False,
                    reason="entity_link_merged",
                )
            )
        debug_payload = TagDebug(
            kept_span_count=debug_payload.kept_span_count,
            discarded_span_count=debug_payload.discarded_span_count + len(merged_entity_discards),
            span_decisions=span_decisions,
        )
    if debug_payload is not None and surname_coreference_discards:
        span_decisions = list(debug_payload.span_decisions)
        for item in surname_coreference_discards:
            span_decisions.append(
                TagSpanDecision(
                    text=item.entity.text,
                    label=item.entity.label,
                    start=item.entity.start,
                    end=item.entity.end,
                    lane=item.lane,
                    kept=False,
                    reason="surname_coreference_false_positive",
                )
            )
        debug_payload = TagDebug(
            kept_span_count=debug_payload.kept_span_count,
            discarded_span_count=debug_payload.discarded_span_count
            + len(surname_coreference_discards),
            span_decisions=span_decisions,
        )

    runtime_generic_discards: list[tuple[ExtractedCandidate, str]] = []
    if runtime.pack_id == "general-en":
        filtered_resolved: list[ExtractedCandidate] = []
        for candidate in resolved:
            discard_reason = _runtime_general_en_discard_reason(candidate, text=bounded_text)
            if discard_reason is not None:
                runtime_generic_discards.append((candidate, discard_reason))
                continue
            filtered_resolved.append(candidate)
        resolved = filtered_resolved
        if debug_payload is not None and runtime_generic_discards:
            span_decisions = list(debug_payload.span_decisions)
            for item, reason in runtime_generic_discards:
                span_decisions.append(
                    TagSpanDecision(
                        text=item.entity.text,
                        label=item.entity.label,
                        start=item.entity.start,
                        end=item.entity.end,
                        lane=item.lane,
                        kept=False,
                        reason=reason,
                    )
                )
            debug_payload = TagDebug(
                kept_span_count=debug_payload.kept_span_count,
                discarded_span_count=debug_payload.discarded_span_count
                + len(runtime_generic_discards),
                span_decisions=span_decisions,
            )
    overlap_resolution_ms = _elapsed_ms(phase_start)

    metrics = _build_metrics(
        raw_text=bounded_text,
        normalized_input=normalized_input,
        extracted=resolved,
        segment_count=len(segments),
        overlap_drop_count=overlap_drop_count + len(total_duplicate_discards),
    )
    _apply_density_warning(runtime=runtime, metrics=metrics, warnings=warnings)

    elapsed_ms = _elapsed_ms(start)
    final_metric_assembly_ms = max(
        0,
        elapsed_ms
        - normalization_ms
        - segmentation_ms
        - lookup_rule_extraction_ms
        - overlap_resolution_ms,
    )
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
        total_time_ms=elapsed_ms,
        timing_breakdown_ms={
            "normalization_ms": normalization_ms,
            "segmentation_ms": segmentation_ms,
            "lookup_rule_extraction_ms": lookup_rule_extraction_ms,
            "overlap_resolution_ms": overlap_resolution_ms,
            "final_metric_assembly_ms": final_metric_assembly_ms,
        },
        timing_ms=elapsed_ms,
    )
