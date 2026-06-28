from __future__ import annotations

import sys
from types import SimpleNamespace

from ades.pipeline.hybrid import SpacyProposalProvider


class _FakeEntity:
    def __init__(self, start: int, end: int, label: str) -> None:
        self.start_char = start
        self.end_char = end
        self.label_ = label


class _FakeNlp:
    def __init__(self, entities_by_text: dict[str, list[_FakeEntity]]) -> None:
        self.calls = 0
        self._entities_by_text = entities_by_text

    def __call__(self, text: str) -> SimpleNamespace:
        self.calls += 1
        return SimpleNamespace(ents=self._entities_by_text.get(text, []))


class _FakeSpacy:
    def __init__(self, fake_nlp: _FakeNlp) -> None:
        self.fake_nlp = fake_nlp
        self.load_calls: list[tuple[str, tuple[str, ...]]] = []

    def load(self, model: str, *, disable: tuple[str, ...] = ()) -> _FakeNlp:
        self.load_calls.append((model, disable))
        return self.fake_nlp


def _install_fake_spacy(monkeypatch, fake_nlp: _FakeNlp) -> _FakeSpacy:
    fake_spacy = _FakeSpacy(fake_nlp)
    monkeypatch.setitem(sys.modules, "spacy", fake_spacy)
    return fake_spacy


def test_spacy_provider_caches_text_before_allowed_label_filter(monkeypatch) -> None:
    monkeypatch.delenv("ADES_HYBRID_SPACY_CACHE_SIZE", raising=False)
    text = "OpenAI opened San Francisco office"
    fake_nlp = _FakeNlp(
        {
            text: [
                _FakeEntity(0, 6, "ORG"),
                _FakeEntity(14, 27, "GPE"),
            ]
        }
    )
    fake_spacy = _install_fake_spacy(monkeypatch, fake_nlp)

    provider = SpacyProposalProvider(model_path="missing-model")
    organizations = provider.propose(text, allowed_labels={"organization"})
    locations = provider.propose(text, allowed_labels={"location"})

    assert fake_nlp.calls == 1
    assert fake_spacy.load_calls == [
        ("en_core_web_sm", ("tagger", "parser", "attribute_ruler", "lemmatizer"))
    ]
    assert [proposal.text for proposal in organizations] == ["OpenAI"]
    assert [proposal.label for proposal in organizations] == ["organization"]
    assert [proposal.text for proposal in locations] == ["San Francisco"]
    assert [proposal.label for proposal in locations] == ["location"]


def test_spacy_provider_cache_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("ADES_HYBRID_SPACY_CACHE_SIZE", "0")
    text = "OpenAI opened San Francisco office"
    fake_nlp = _FakeNlp({text: [_FakeEntity(0, 6, "ORG")]})
    _install_fake_spacy(monkeypatch, fake_nlp)

    provider = SpacyProposalProvider(model_path="missing-model")
    first = provider.propose(text, allowed_labels={"organization"})
    second = provider.propose(text, allowed_labels={"organization"})

    assert fake_nlp.calls == 2
    assert [proposal.text for proposal in first] == ["OpenAI"]
    assert [proposal.text for proposal in second] == ["OpenAI"]
