from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path
import sys

from ades.service.models import (
    EntityLink,
    EntityMatch,
    EntityProvenance,
    GraphSupport,
    TagResponse,
)


def _load_rerun_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "rerun_saved_unseen_service.py"
    spec = spec_from_file_location("rerun_saved_unseen_service", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _entity(*, text: str, entity_id: str) -> EntityMatch:
    return EntityMatch(
        text=text,
        label="organization",
        start=0,
        end=len(text),
        aliases=[],
        confidence=0.8,
        relevance=0.8,
        provenance=EntityProvenance(
            match_kind="alias",
            match_path="lookup.alias.exact",
            match_source=text,
            source_pack="general-en",
            source_domain="general",
            lane="deterministic_alias",
        ),
        link=EntityLink(
            entity_id=entity_id,
            canonical_text=text,
            provider="lookup.alias.exact",
        ),
    )


def test_response_summary_preserves_graph_blocker_details() -> None:
    module = _load_rerun_module()
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[_entity(text="OpenAI", entity_id="wikidata:Q123")],
        related_entities=[],
        graph_support=GraphSupport(
            requested=True,
            applied=True,
            provider="sqlite.qid_graph_context",
            seed_entity_ids=["wikidata:Q123"],
            coherence_score=0.031,
            related_entity_count=0,
        ),
        refinement_applied=False,
        refinement_strategy=None,
        refinement_score=0.031,
        refinement_debug={
            "strategy": "qid_graph_context_v1",
            "related_debug": {
                "reason": "low_document_coherence",
                "coherence_score": 0.031,
            },
        },
        topics=[],
        warnings=[],
        timing_ms=11,
    )

    summary = module._response_summary(response)

    assert summary["graph_support"]["coherence_score"] == 0.031
    assert summary["refinement_applied"] is False
    assert summary["refinement_score"] == 0.031
    assert summary["related_debug_reason"] == "low_document_coherence"
    assert summary["refinement_debug"]["related_debug"]["reason"] == "low_document_coherence"


def test_load_cases_accepts_direct_article_paths(tmp_path: Path) -> None:
    module = _load_rerun_module()
    article_path = tmp_path / "article.txt"
    article_path.write_text("Test article body.", encoding="utf-8")
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "case_id": "saved-repro",
                        "path": str(article_path),
                        "title": "Saved repro title",
                        "pack": "general-en",
                        "domain_hint": "politics",
                        "country_hint": "uk",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(summary_path, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "saved-repro"
    assert case.title == "Saved repro title"
    assert case.pack_id == "general-en"
    assert case.domain_hint == "politics"
    assert case.country_hint == "uk"
    assert case.article_path == article_path.resolve()
    assert case.baseline_plain == {}
    assert case.baseline_hinted == {}


def test_load_cases_accepts_direct_news_paths(tmp_path: Path) -> None:
    module = _load_rerun_module()
    article_path = tmp_path / "news-article.txt"
    article_path.write_text("News article body.", encoding="utf-8")
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "case_id": "saved-news-path",
                        "news_path": str(article_path),
                        "title": "Saved news-path title",
                        "article_url": "https://example.test/news-path",
                        "pack": "general-en",
                        "domain_hint": "business",
                        "country_hint": "uk",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(summary_path, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "saved-news-path"
    assert case.title == "Saved news-path title"
    assert case.url == "https://example.test/news-path"
    assert case.pack_id == "general-en"
    assert case.domain_hint == "business"
    assert case.country_hint == "uk"
    assert case.article_path == article_path.resolve()
    assert case.baseline_plain == {}
    assert case.baseline_hinted == {}


def test_load_cases_accepts_articles_summary_shape(tmp_path: Path) -> None:
    module = _load_rerun_module()
    news_dir = tmp_path / "news"
    news_dir.mkdir()
    article_path = news_dir / "sample-stem.txt"
    article_path.write_text("News article body.", encoding="utf-8")
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "articles": [
                    {
                        "stem": "sample-stem",
                        "title": "Sample stem title",
                        "source": "abcnews",
                        "article_url": "https://example.test/article",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(summary_path, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "sample-stem"
    assert case.title == "Sample stem title"
    assert case.source == "abcnews"
    assert case.url == "https://example.test/article"
    assert case.pack_id == "general-en"
    assert case.article_path == article_path.resolve()


def test_load_cases_accepts_articles_summary_with_previous_run_news_fallback(
    tmp_path: Path,
) -> None:
    module = _load_rerun_module()
    previous_run_dir = tmp_path / "previous-run"
    news_dir = previous_run_dir / "news"
    news_dir.mkdir(parents=True)
    article_path = news_dir / "sample-stem.txt"
    article_path.write_text("News article body.", encoding="utf-8")
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "previous_run": str(previous_run_dir),
                "articles": [
                    {
                        "stem": "sample-stem",
                        "title": "Sample stem title",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(summary_path, default_pack="general-en")

    assert len(cases) == 1
    assert cases[0].article_path == article_path.resolve()


def test_json_object_argument_normalizes_mapping() -> None:
    module = _load_rerun_module()

    payload = module._json_object_argument(
        '{"business-vector-en":"ades-qids-business-from-shared-v1"," ": "skip"}',
        name="--pack-collection-aliases-json",
    )

    assert payload == {
        "business-vector-en": "ades-qids-business-from-shared-v1",
    }


def test_run_tag_request_in_process_applies_env_overrides(monkeypatch) -> None:
    module = _load_rerun_module()
    captured: dict[str, object] = {}
    environment = __import__("os").environ
    original_values = {
        key: environment.get(key)
        for key in (
            "ADES_VECTOR_SEARCH_COLLECTION_ALIAS",
            "ADES_VECTOR_SEARCH_PACK_COLLECTION_ALIASES",
            "ADES_GRAPH_CONTEXT_ENABLED",
            "ADES_GRAPH_CONTEXT_ARTIFACT_PATH",
            "ADES_NEWS_CONTEXT_ARTIFACT_PATH",
        )
    }

    def _fake_tag(text: str, **kwargs):
        captured["text"] = text
        captured["kwargs"] = kwargs
        captured["collection_alias"] = environment.get(
            "ADES_VECTOR_SEARCH_COLLECTION_ALIAS"
        )
        captured["pack_aliases"] = environment.get(
            "ADES_VECTOR_SEARCH_PACK_COLLECTION_ALIASES"
        )
        captured["graph_context_enabled"] = environment.get(
            "ADES_GRAPH_CONTEXT_ENABLED"
        )
        captured["graph_context_artifact_path"] = environment.get(
            "ADES_GRAPH_CONTEXT_ARTIFACT_PATH"
        )
        captured["news_context_artifact_path"] = environment.get(
            "ADES_NEWS_CONTEXT_ARTIFACT_PATH"
        )
        return TagResponse(
            version="0.1.0",
            pack="general-en",
            pack_version="0.2.0",
            language="en",
            content_type="text/plain",
            entities=[],
            related_entities=[],
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.api.tag", _fake_tag)
    saved_case = module._SavedResponseCase(
        slug="case-slug",
        title="Case Title",
        source="source",
        url="https://example.test/article",
        pack_id="general-en",
        domain_hint="business",
        country_hint="uk",
        response_file_path=Path("/tmp/case.responses.json"),
        article_path=Path("/tmp/case.article.txt"),
        baseline_plain={},
        baseline_hinted={},
    )

    response = module._run_tag_request(
        "Article body.",
        saved_case=saved_case,
        use_hints=True,
        mode="in-process",
        refinement_depth="deep",
        collection_alias="ades-qids-shared-general-plus-domain-v1",
        pack_collection_aliases={
            "business-vector-en": "ades-qids-business-from-shared-v1",
        },
        graph_context_artifact_path=Path("/tmp/graph.sqlite"),
        news_context_artifact_path=Path("/tmp/recent-news-support.json"),
    )

    assert response.pack == "general-en"
    assert captured["collection_alias"] == "ades-qids-shared-general-plus-domain-v1"
    assert captured["pack_aliases"] == json.dumps(
        {"business-vector-en": "ades-qids-business-from-shared-v1"},
        sort_keys=True,
    )
    assert captured["graph_context_enabled"] == "true"
    assert captured["graph_context_artifact_path"] == "/tmp/graph.sqlite"
    assert (
        captured["news_context_artifact_path"]
        == "/tmp/recent-news-support.json"
    )
    kwargs = captured["kwargs"]
    assert kwargs["domain_hint"] == "business"
    assert kwargs["country_hint"] == "uk"
    assert kwargs["content_type"] == "text/plain"
    assert environment.get("ADES_VECTOR_SEARCH_COLLECTION_ALIAS") == original_values[
        "ADES_VECTOR_SEARCH_COLLECTION_ALIAS"
    ]
    assert environment.get(
        "ADES_VECTOR_SEARCH_PACK_COLLECTION_ALIASES"
    ) == original_values["ADES_VECTOR_SEARCH_PACK_COLLECTION_ALIASES"]
    assert environment.get("ADES_GRAPH_CONTEXT_ENABLED") == original_values[
        "ADES_GRAPH_CONTEXT_ENABLED"
    ]
    assert environment.get("ADES_GRAPH_CONTEXT_ARTIFACT_PATH") == original_values[
        "ADES_GRAPH_CONTEXT_ARTIFACT_PATH"
    ]
    assert environment.get("ADES_NEWS_CONTEXT_ARTIFACT_PATH") == original_values[
        "ADES_NEWS_CONTEXT_ARTIFACT_PATH"
    ]


def test_run_tag_request_service_mode_keeps_hint_toggle(monkeypatch) -> None:
    module = _load_rerun_module()
    captured: dict[str, object] = {}

    def _fake_service(text: str, **kwargs):
        captured["text"] = text
        captured["kwargs"] = kwargs
        return TagResponse(
            version="0.1.0",
            pack="general-en",
            pack_version="0.2.0",
            language="en",
            content_type="text/plain",
            entities=[],
            related_entities=[],
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr(module, "tag_via_local_service", _fake_service)
    saved_case = module._SavedResponseCase(
        slug="case-slug",
        title="Case Title",
        source="source",
        url="https://example.test/article",
        pack_id="general-en",
        domain_hint="economics",
        country_hint="de",
        response_file_path=Path("/tmp/case.responses.json"),
        article_path=Path("/tmp/case.article.txt"),
        baseline_plain={},
        baseline_hinted={},
    )

    module._run_tag_request(
        "Article body.",
        saved_case=saved_case,
        use_hints=False,
        mode="service",
        refinement_depth="deep",
        collection_alias=None,
        pack_collection_aliases={},
        graph_context_artifact_path=None,
        news_context_artifact_path=None,
    )

    kwargs = captured["kwargs"]
    assert "domain_hint" not in kwargs
    assert "country_hint" not in kwargs


def test_load_cases_propagates_summary_runtime_artifact_paths(tmp_path: Path) -> None:
    module = _load_rerun_module()
    article_path = tmp_path / "article.txt"
    article_path.write_text("Article body.", encoding="utf-8")
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "graph_context_artifact_path": "/tmp/graph.sqlite",
                "news_context_artifact_path": "/tmp/recent-news-support.json",
                "cases": [
                    {
                        "case_id": "artifact-case",
                        "path": str(article_path),
                        "title": "Artifact title",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(summary_path, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.graph_context_artifact_path == Path("/tmp/graph.sqlite")
    assert case.news_context_artifact_path == Path(
        "/tmp/recent-news-support.json"
    )


def test_load_cases_selector_inherits_source_summary_runtime_artifact_paths(
    tmp_path: Path,
) -> None:
    module = _load_rerun_module()
    source_root = tmp_path / "source-run"
    news_dir = source_root / "news"
    news_dir.mkdir(parents=True)
    article_path = news_dir / "selector-case.txt"
    article_path.write_text("Article body.", encoding="utf-8")
    source_summary = source_root / "summary.json"
    source_summary.write_text(
        json.dumps(
            {
                "graph_context_artifact_path": "/tmp/source-graph.sqlite",
                "news_context_artifact_path": "/tmp/source-news.json",
                "cases": [
                    {
                        "slug": "selector-case",
                        "title": "Selector title",
                        "news_path": str(article_path),
                        "plain": {
                            "entities": [],
                            "related_entities": [],
                            "warnings": [],
                            "timing_ms": 1,
                        },
                        "hinted": {
                            "entities": [],
                            "related_entities": [],
                            "warnings": [],
                            "timing_ms": 2,
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    benchmark_summary = tmp_path / "benchmark-summary.json"
    benchmark_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "summary_path": str(source_summary),
                        "selector_kind": "case_slug",
                        "selector_value": "selector-case",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(benchmark_summary, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.graph_context_artifact_path == Path("/tmp/source-graph.sqlite")
    assert case.news_context_artifact_path == Path("/tmp/source-news.json")


def test_default_output_dir_falls_back_to_live_review_root_for_external_summary() -> None:
    module = _load_rerun_module()
    summary_path = Path("/tmp/ades-related-entity-candidate-benchmark.json")

    output_dir = module._default_output_dir(summary_path)

    assert str(output_dir).startswith(str(module.LIVE_REVIEW_ROOT))
    assert output_dir.name.startswith("rerun-saved-ades-related-entity-candidate-benchmark-")


def test_load_cases_accepts_benchmark_case_slug_selector(tmp_path: Path) -> None:
    module = _load_rerun_module()
    article_path = tmp_path / "source.article.txt"
    article_path.write_text("Article body.", encoding="utf-8")
    response_path = tmp_path / "source.responses.json"
    response_path.write_text(
        json.dumps(
            {
                "case": {
                    "slug": "source-case",
                    "title": "Source case title",
                    "source": "dw",
                    "url": "https://example.test/source",
                    "pack": "general-en",
                    "domain_hint": "economics",
                    "country_hint": "de",
                },
                "article_path": str(article_path),
                "plain": {"entities": [], "related_entities": [], "warnings": [], "timing_ms": 1},
                "hinted": {"entities": [], "related_entities": [], "warnings": [], "timing_ms": 2},
            }
        ),
        encoding="utf-8",
    )
    source_summary = tmp_path / "source-summary.json"
    source_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "slug": "source-case",
                        "response_file_path": str(response_path),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    benchmark_summary = tmp_path / "benchmark-summary.json"
    benchmark_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "benchmark_id": "de-case",
                        "summary_path": str(source_summary),
                        "selector_kind": "case_slug",
                        "selector_value": "source-case",
                        "domain_hint": "economics",
                        "country_hint": "de",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(benchmark_summary, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "source-case"
    assert case.title == "Source case title"
    assert case.article_path == article_path.resolve()
    assert case.domain_hint == "economics"
    assert case.country_hint == "de"


def test_load_cases_accepts_benchmark_case_slug_selector_with_inline_case_payload(
    tmp_path: Path,
) -> None:
    module = _load_rerun_module()
    source_root = tmp_path / "source-run"
    news_dir = source_root / "news"
    news_dir.mkdir(parents=True)
    article_path = news_dir / "inline-case.txt"
    article_path.write_text("Inline article body.", encoding="utf-8")
    source_summary = source_root / "summary.json"
    source_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "slug": "inline-case",
                        "title": "Inline case title",
                        "source": "cityam",
                        "url": "https://example.test/inline",
                        "plain": {"entities": [], "related_entities": [], "warnings": [], "timing_ms": 1},
                        "hinted": {"entities": [], "related_entities": [], "warnings": [], "timing_ms": 2},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    benchmark_summary = tmp_path / "benchmark-summary-inline.json"
    benchmark_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "benchmark_id": "uk-inline",
                        "summary_path": str(source_summary),
                        "selector_kind": "case_slug",
                        "selector_value": "inline-case",
                        "domain_hint": "business",
                        "country_hint": "uk",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(benchmark_summary, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "inline-case"
    assert case.title == "Inline case title"
    assert case.article_path == article_path.resolve()
    assert case.baseline_plain["timing_ms"] == 1
    assert case.baseline_hinted["timing_ms"] == 2


def test_load_cases_accepts_benchmark_case_slug_selector_with_inline_news_path_case_payload(
    tmp_path: Path,
) -> None:
    module = _load_rerun_module()
    source_root = tmp_path / "source-run"
    news_dir = source_root / "news"
    news_dir.mkdir(parents=True)
    article_path = news_dir / "inline-news-path.txt"
    article_path.write_text("Inline article body.", encoding="utf-8")
    source_summary = source_root / "summary.json"
    source_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "slug": "inline-news-path",
                        "title": "Inline news-path title",
                        "source": "cityam",
                        "article_url": "https://example.test/inline-news-path",
                        "news_path": str(article_path),
                        "plain": {
                            "entities": [],
                            "related_entities": [],
                            "warnings": [],
                            "timing_ms": 1,
                        },
                        "hinted": {
                            "entities": [],
                            "related_entities": [],
                            "warnings": [],
                            "timing_ms": 2,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    benchmark_summary = tmp_path / "benchmark-summary-inline-news-path.json"
    benchmark_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "benchmark_id": "uk-inline-news-path",
                        "summary_path": str(source_summary),
                        "selector_kind": "case_slug",
                        "selector_value": "inline-news-path",
                        "domain_hint": "business",
                        "country_hint": "uk",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(benchmark_summary, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "inline-news-path"
    assert case.title == "Inline news-path title"
    assert case.url == "https://example.test/inline-news-path"
    assert case.article_path == article_path.resolve()
    assert case.baseline_plain["timing_ms"] == 1
    assert case.baseline_hinted["timing_ms"] == 2


def test_load_cases_accepts_benchmark_case_slug_selector_with_sibling_response_files(
    tmp_path: Path,
) -> None:
    module = _load_rerun_module()
    source_root = tmp_path / "source-run"
    source_root.mkdir()
    article_path = source_root / "sibling-case.article.txt"
    article_path.write_text("Sibling article body.", encoding="utf-8")
    response_path = source_root / "sibling-case.responses.json"
    response_path.write_text(
        json.dumps(
            {
                "case": {
                    "slug": "sibling-case",
                    "title": "Sibling case title",
                    "source": "dw",
                    "url": "https://example.test/sibling",
                    "pack": "general-en",
                },
                "plain": {"entities": [], "related_entities": [], "warnings": [], "timing_ms": 3},
                "hinted": {"entities": [], "related_entities": [], "warnings": [], "timing_ms": 4},
            }
        ),
        encoding="utf-8",
    )
    source_summary = source_root / "summary.json"
    source_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "slug": "sibling-case",
                        "title": "Sibling case title",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    benchmark_summary = tmp_path / "benchmark-summary-sibling.json"
    benchmark_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "benchmark_id": "sibling-case",
                        "summary_path": str(source_summary),
                        "selector_kind": "case_slug",
                        "selector_value": "sibling-case",
                        "domain_hint": "economics",
                        "country_hint": "de",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(benchmark_summary, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "sibling-case"
    assert case.article_path == article_path.resolve()
    assert case.baseline_plain["timing_ms"] == 3
    assert case.baseline_hinted["timing_ms"] == 4


def test_load_cases_accepts_reports_summary_shape(tmp_path: Path) -> None:
    module = _load_rerun_module()
    news_dir = tmp_path / "news"
    news_dir.mkdir()
    article_path = news_dir / "report-article.txt"
    article_path.write_text("Report article body.", encoding="utf-8")
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "reports": [
                    {
                        "index": 1,
                        "source": "abcnews",
                        "title": "Report title",
                        "article_url": "https://example.test/report",
                        "news_file_path": str(article_path),
                        "plain_entity_count": 1,
                        "plain_entities": [
                            {
                                "text": "OpenAI",
                                "label": "organization",
                                "entity_id": "wikidata:Q24283660",
                                "canonical_text": "OpenAI",
                            }
                        ],
                        "plain_timing_ms": 11,
                        "flagged_entity_count": 2,
                        "flagged_entities": [
                            {
                                "text": "OpenAI",
                                "label": "organization",
                                "entity_id": "wikidata:Q24283660",
                                "canonical_text": "OpenAI",
                            }
                        ],
                        "related_entity_count": 1,
                        "related_entities": [
                            {
                                "entity_id": "wikidata:Q11661",
                                "canonical_text": "Artificial intelligence",
                                "entity_type": "topic",
                                "score": 0.7,
                            }
                        ],
                        "flagged_timing_ms": 7,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(summary_path, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "report-article"
    assert case.title == "Report title"
    assert case.source == "abcnews"
    assert case.url == "https://example.test/report"
    assert case.article_path == article_path.resolve()
    assert case.baseline_plain["timing_ms"] == 11
    assert case.baseline_plain["entity_count"] == 1
    assert case.baseline_plain["entities"][0]["entity_id"] == "wikidata:Q24283660"
    assert case.baseline_hinted["timing_ms"] == 7
    assert case.baseline_hinted["entity_count"] == 2
    assert case.baseline_hinted["related_entity_count"] == 1
    assert case.baseline_hinted["related_entities"][0]["entity_id"] == "wikidata:Q11661"


def test_load_cases_accepts_hybrid_compare_reports_summary_shape(tmp_path: Path) -> None:
    module = _load_rerun_module()
    news_dir = tmp_path / "news"
    news_dir.mkdir()
    article_path = news_dir / "hybrid-report-article.txt"
    article_path.write_text("Hybrid compare article body.", encoding="utf-8")
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "reports": [
                    {
                        "index": 2,
                        "source": "dw",
                        "title": "Hybrid compare title",
                        "article_url": "https://example.test/hybrid-report",
                        "news_file_path": str(article_path),
                        "plain_entity_count": 9,
                        "plain_timing_ms": 30,
                        "hybrid_entity_count": 5,
                        "hybrid_related_count": 2,
                        "hybrid_timing_ms": 12,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(summary_path, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "hybrid-report-article"
    assert case.title == "Hybrid compare title"
    assert case.url == "https://example.test/hybrid-report"
    assert case.article_path == article_path.resolve()
    assert case.baseline_plain["entity_count"] == 9
    assert case.baseline_plain["timing_ms"] == 30
    assert case.baseline_hinted["entity_count"] == 5
    assert case.baseline_hinted["related_entity_count"] == 2
    assert case.baseline_hinted["timing_ms"] == 12


def test_load_cases_accepts_benchmark_report_index_selector(tmp_path: Path) -> None:
    module = _load_rerun_module()
    source_root = tmp_path / "source-run"
    news_dir = source_root / "news"
    news_dir.mkdir(parents=True)
    article_path = news_dir / "report-selector-article.txt"
    article_path.write_text("Report selector article body.", encoding="utf-8")
    source_summary = source_root / "summary.json"
    source_summary.write_text(
        json.dumps(
            {
                "reports": [
                    {
                        "index": 988,
                        "source": "odt",
                        "title": "Selector report title",
                        "article_url": "https://example.test/report-selector",
                        "news_file_path": str(article_path),
                        "plain_entity_count": 3,
                        "plain_timing_ms": 21,
                        "flagged_entity_count": 2,
                        "flagged_timing_ms": 8,
                        "related_entity_count": 1,
                        "related_entities": [
                            {
                                "entity_id": "wikidata:Q45256",
                                "canonical_text": "Australasia",
                                "entity_type": "location",
                                "score": 0.4,
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    benchmark_summary = tmp_path / "benchmark-summary-report-index.json"
    benchmark_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "benchmark_id": "report-index-case",
                        "summary_path": str(source_summary),
                        "selector_kind": "report_index",
                        "selector_value": 988,
                        "domain_hint": "economics",
                        "country_hint": "nz",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(benchmark_summary, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "report-selector-article"
    assert case.title == "Selector report title"
    assert case.source == "odt"
    assert case.url == "https://example.test/report-selector"
    assert case.domain_hint == "economics"
    assert case.country_hint == "nz"
    assert case.article_path == article_path.resolve()
    assert case.baseline_plain["entity_count"] == 3
    assert case.baseline_plain["timing_ms"] == 21
    assert case.baseline_hinted["entity_count"] == 2
    assert case.baseline_hinted["related_entity_count"] == 1
    assert case.baseline_hinted["related_entities"][0]["entity_id"] == "wikidata:Q45256"


def test_load_cases_accepts_benchmark_article_index_selector(tmp_path: Path) -> None:
    module = _load_rerun_module()
    news_dir = tmp_path / "source-run" / "news"
    news_dir.mkdir(parents=True)
    article_path = news_dir / "korea-stem.txt"
    article_path.write_text("Korea article body.", encoding="utf-8")
    source_summary = tmp_path / "source-run" / "summary.json"
    source_summary.write_text(
        json.dumps(
            {
                "articles": [
                    {
                        "index": 2,
                        "stem": "korea-stem",
                        "title": "Korea title",
                        "source": "korea_times",
                        "url": "https://example.test/kr",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    benchmark_summary = tmp_path / "benchmark-summary.json"
    benchmark_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "benchmark_id": "kr-article",
                        "summary_path": str(source_summary),
                        "selector_kind": "article_index",
                        "selector_value": 2,
                        "domain_hint": "economics",
                        "country_hint": "kr",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(benchmark_summary, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "korea-stem"
    assert case.title == "Korea title"
    assert case.article_path == article_path.resolve()
    assert case.domain_hint == "economics"
    assert case.country_hint == "kr"


def test_load_cases_accepts_benchmark_article_index_selector_with_text_path(
    tmp_path: Path,
) -> None:
    module = _load_rerun_module()
    article_path = tmp_path / "korea-article.txt"
    article_path.write_text("Korea article body.", encoding="utf-8")
    source_summary = tmp_path / "source-summary.json"
    source_summary.write_text(
        json.dumps(
            {
                "articles": [
                    {
                        "index": 2,
                        "title": "Korea title",
                        "url": "https://example.test/kr",
                        "text_path": str(article_path),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    benchmark_summary = tmp_path / "benchmark-summary.json"
    benchmark_summary.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "benchmark_id": "kr-article",
                        "summary_path": str(source_summary),
                        "selector_kind": "article_index",
                        "selector_value": 2,
                        "domain_hint": "economics",
                        "country_hint": "kr",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_cases(benchmark_summary, default_pack="general-en")

    assert len(cases) == 1
    case = cases[0]
    assert case.slug == "korea-article"
    assert case.title == "Korea title"
    assert case.article_path == article_path.resolve()
