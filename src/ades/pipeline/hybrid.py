"""Optional hybrid span-proposal lane for entity extraction."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Protocol

_TRUE_VALUES = {"1", "true", "yes", "y", "on"}
_DEFAULT_MODEL_ROOT = Path("/mnt/githubActions/ades_big_data/models/ades")
_DEFAULT_SPACY_CACHE_SIZE = 128
_SPACY_DEFAULT_DISABLED_PIPES = ("tagger", "parser", "attribute_ruler", "lemmatizer")
_CACHED_PROVIDER = None
_SPACY_LABEL_MAP = {
    "PERSON": "person",
    "ORG": "organization",
    "GPE": "location",
    "LOC": "location",
    "FAC": "location",
    "PRODUCT": "product",
    "NORP": "organization",
}


@dataclass(frozen=True)
class ProposalSpan:
    """One proposed entity span from a learned or model-backed lane."""

    start: int
    end: int
    text: str
    label: str
    confidence: float
    model_name: str
    model_version: str


class ProposalProvider(Protocol):
    """Protocol for local proposal providers."""

    def propose(self, text: str, *, allowed_labels: set[str]) -> list[ProposalSpan]: ...


class SpacyProposalProvider:
    """Local spaCy-backed proposal provider."""

    def __init__(self, *, model_path: str | None = None) -> None:
        try:
            import spacy
        except Exception as exc:  # pragma: no cover - exercised when spaCy is absent.
            raise RuntimeError("spaCy is not installed for hybrid span proposal.") from exc

        resolved_model_path = (
            Path(model_path).expanduser().resolve()
            if model_path is not None
            else self._default_model_path()
        )
        disabled_pipes = _spacy_disabled_pipes()
        if resolved_model_path.exists():
            self._nlp = spacy.load(str(resolved_model_path), disable=disabled_pipes)
            self._model_name = resolved_model_path.name
        else:
            self._nlp = spacy.load("en_core_web_sm", disable=disabled_pipes)
            self._model_name = "en_core_web_sm"
        self._proposal_cache_size = _spacy_cache_size()
        self._proposal_cache: OrderedDict[str, tuple[ProposalSpan, ...]] = OrderedDict()

    def propose(self, text: str, *, allowed_labels: set[str]) -> list[ProposalSpan]:
        if not allowed_labels:
            return []
        proposals = self._propose_all(text)
        return [proposal for proposal in proposals if proposal.label in allowed_labels]

    def _propose_all(self, text: str) -> tuple[ProposalSpan, ...]:
        if self._proposal_cache_size > 0:
            cached = self._proposal_cache.get(text)
            if cached is not None:
                self._proposal_cache.move_to_end(text)
                return cached

        doc = self._nlp(text)
        proposals: list[ProposalSpan] = []
        for entity in doc.ents:
            mapped_label = _SPACY_LABEL_MAP.get(entity.label_)
            if mapped_label is None:
                continue
            proposals.append(
                ProposalSpan(
                    start=entity.start_char,
                    end=entity.end_char,
                    text=text[entity.start_char : entity.end_char],
                    label=mapped_label,
                    confidence=0.7,
                    model_name=self._model_name,
                    model_version="local",
                )
            )
        cached_proposals = tuple(proposals)
        if self._proposal_cache_size > 0:
            self._proposal_cache[text] = cached_proposals
            self._proposal_cache.move_to_end(text)
            while len(self._proposal_cache) > self._proposal_cache_size:
                self._proposal_cache.popitem(last=False)
        return cached_proposals

    @staticmethod
    def _default_model_path() -> Path:
        override = os.getenv("ADES_HYBRID_SPACY_MODEL")
        if override:
            return Path(override).expanduser().resolve()
        return (_DEFAULT_MODEL_ROOT / "spacy" / "en_core_web_sm").resolve()


def hybrid_enabled(enabled_override: bool | None = None) -> bool:
    """Return whether the hybrid proposal lane is enabled."""

    if enabled_override is not None:
        return enabled_override
    return os.getenv("ADES_HYBRID_ENABLED", "").strip().lower() in _TRUE_VALUES


def _spacy_cache_size() -> int:
    raw_value = os.getenv("ADES_HYBRID_SPACY_CACHE_SIZE")
    if raw_value is None or not raw_value.strip():
        return _DEFAULT_SPACY_CACHE_SIZE
    try:
        return max(0, int(raw_value))
    except ValueError:
        return _DEFAULT_SPACY_CACHE_SIZE


def _spacy_disabled_pipes() -> tuple[str, ...]:
    raw_value = os.getenv("ADES_HYBRID_SPACY_DISABLE_PIPES")
    if raw_value is None:
        return _SPACY_DEFAULT_DISABLED_PIPES
    return tuple(pipe.strip() for pipe in raw_value.split(",") if pipe.strip())


def get_proposal_spans(
    text: str,
    *,
    allowed_labels: set[str],
    enabled_override: bool | None = None,
    provider_override: ProposalProvider | None = None,
) -> list[ProposalSpan]:
    """Return learned proposal spans when the hybrid lane is enabled."""

    if not hybrid_enabled(enabled_override):
        return []
    provider = provider_override or _load_provider()
    return provider.propose(text, allowed_labels=allowed_labels)


def reset_cached_provider() -> None:
    """Clear the cached learned span provider."""

    global _CACHED_PROVIDER
    _CACHED_PROVIDER = None


def _load_provider() -> ProposalProvider:
    global _CACHED_PROVIDER
    if _CACHED_PROVIDER is not None:
        return _CACHED_PROVIDER
    provider_name = os.getenv("ADES_HYBRID_PROVIDER", "spacy").strip().lower() or "spacy"
    if provider_name != "spacy":
        raise RuntimeError(f"Unsupported hybrid proposal provider: {provider_name}")
    provider = SpacyProposalProvider(model_path=os.getenv("ADES_HYBRID_SPACY_MODEL"))
    _CACHED_PROVIDER = provider
    return provider
