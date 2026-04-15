import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ades.news_feedback import (
    DEFAULT_LIVE_NEWS_FEEDS,
    LiveNewsArticleResult,
    LiveNewsDigestionRunReport,
    LiveNewsEntity,
    LiveNewsFeedItem,
    LiveNewsFeedSpec,
    LiveNewsFeedbackReport,
    LiveNewsFixSuggestion,
    LiveNewsIssue,
    _detect_news_feedback_issues,
    _filter_recent_feed_items,
    _find_missing_structured_organization_candidates,
    evaluate_live_news_feedback,
    live_news_feedback_cluster_suggestions_path,
    live_news_feedback_merged_suggestions_path,
    live_news_digestion_cluster_report_path,
    live_news_digestion_run_report_path,
    live_news_processed_articles_path,
    run_live_news_digestion_clusters,
)
from ades.service.models import EntityMatch, TagResponse


def test_detect_live_news_feedback_issues_flags_generic_missing_and_partial_spans() -> None:
    text = (
        "Four analysts in Shanghai-based Northwind Solutions said the Strait of "
        "Hormuz disrupted the US market."
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=17,
        entities=[
            EntityMatch(
                text="Four",
                label="location",
                start=text.index("Four"),
                end=text.index("Four") + len("Four"),
            ),
            EntityMatch(
                text="Hormuz",
                label="location",
                start=text.index("Hormuz"),
                end=text.index("Hormuz") + len("Hormuz"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    issue_types = {item.issue_type for item in issues}
    candidates = {item.candidate_text for item in issues if item.candidate_text}

    assert "generic_single_token_entity" in issue_types
    assert "partial_span_candidate" in issue_types
    assert "missing_acronym_candidate" in issue_types
    assert "missing_hyphenated_prefix_candidate" in issue_types
    assert "missing_structured_organization_candidate" in issue_types
    assert "Strait of Hormuz" in candidates
    assert "US" in candidates
    assert "Shanghai" in candidates
    assert "Northwind Solutions" in candidates


def test_structured_organization_candidates_skip_determiners_and_generic_phrases() -> None:
    text = (
        "This group met after Social media reports mentioned Northwind Solutions "
        "and Refugee Council."
    )

    candidates = _find_missing_structured_organization_candidates(text, set())

    assert "Northwind Solutions" in candidates
    assert "Refugee Council" in candidates
    assert "This group" not in candidates
    assert "Social media" not in candidates


def test_evaluate_live_news_feedback_collects_round_robin_articles(
    monkeypatch,
    tmp_path: Path,
) -> None:
    feeds = (
        LiveNewsFeedSpec(source="bbc", feed_url="https://example.com/bbc.xml"),
        LiveNewsFeedSpec(source="cnn", feed_url="https://example.com/cnn.xml"),
    )

    def fake_load_feed_items(client, feed, *, per_feed_limit):
        del client, per_feed_limit
        return [
            LiveNewsFeedItem(
                source=feed.source,
                feed_url=feed.feed_url,
                title=f"{feed.source}-title",
                article_url=f"https://example.com/{feed.source}/article",
            )
        ]

    def fake_evaluate_item(client, item, *, pack_id, storage_root, registry):
        del client, pack_id, storage_root, registry
        return LiveNewsArticleResult(
            source=item.source,
            title=item.title,
            article_url=item.article_url,
            published_at=None,
            text_source="paragraphs",
            word_count=200,
            entity_count=3,
            timing_ms=25 if item.source == "bbc" else 40,
            issue_types=["missing_acronym_candidate"],
            issues=[
                LiveNewsIssue(
                    issue_type="missing_acronym_candidate",
                    message="all-caps acronym appears in text but was not extracted",
                    candidate_text="US",
                )
            ],
            entities=[
                LiveNewsEntity(text="Iran", label="location", start=0, end=4),
            ],
        )

    monkeypatch.setattr("ades.news_feedback._load_feed_items", fake_load_feed_items)
    monkeypatch.setattr("ades.news_feedback._evaluate_live_news_item", fake_evaluate_item)

    report = evaluate_live_news_feedback(
        "general-en",
        storage_root=tmp_path / "storage",
        article_limit=2,
        per_feed_limit=2,
        feeds=feeds,
    )

    assert report.pack_id == "general-en"
    assert report.collected_article_count == 2
    assert report.successful_feed_count == 2
    assert report.per_source_article_counts == {"bbc": 1, "cnn": 1}
    assert report.per_issue_counts == {"missing_acronym_candidate": 2}
    assert "strengthen_acronym_retention" in report.suggested_fix_classes


def test_detect_live_news_feedback_issues_ignores_coordination_noise_and_valid_org_tokens() -> None:
    text = (
        "Google said Pakistan and Bangladesh would join the forum after Voice of "
        "America aired the segment."
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[
            EntityMatch(
                text="Google",
                label="organization",
                start=text.index("Google"),
                end=text.index("Google") + len("Google"),
            ),
            EntityMatch(
                text="Pakistan",
                label="location",
                start=text.index("Pakistan"),
                end=text.index("Pakistan") + len("Pakistan"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)

    assert all(issue.entity_text != "Google" for issue in issues)
    assert all(issue.candidate_text != "Pakistan and Bangladesh" for issue in issues)


def test_detect_live_news_feedback_issues_suppresses_boilerplate_and_weak_structural_noise() -> None:
    text = (
        "CLICK HERE TO DOWNLOAD THE FOX NEWS APP. CBS News reported that the Strait of Hormuz "
        "remains open. Chinese Ministry officials said an Islamist group moved near sea of Israeli waters."
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[
            EntityMatch(
                text="CBS",
                label="organization",
                start=text.index("CBS"),
                end=text.index("CBS") + len("CBS"),
            ),
            EntityMatch(
                text="Hormuz",
                label="location",
                start=text.index("Hormuz"),
                end=text.index("Hormuz") + len("Hormuz"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response, source="foxnews")
    acronym_candidates = {issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"}
    partial_candidates = {issue.candidate_text for issue in issues if issue.issue_type == "partial_span_candidate"}
    structured_org_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_structured_organization_candidate"
    }
    structured_location_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_structured_location_candidate"
    }

    assert {"CLICK", "HERE", "TO", "THE", "FOX", "APP", "NEWS"}.isdisjoint(acronym_candidates)
    assert "Strait of Hormuz" in partial_candidates
    assert "CBS News" not in partial_candidates
    assert "Chinese Ministry" not in structured_org_candidates
    assert "Islamist group" not in structured_org_candidates
    assert "sea of Israeli" not in structured_location_candidates


def test_filter_recent_feed_items_rejects_stale_feed_batches() -> None:
    items = [
        LiveNewsFeedItem(
            source="cnn",
            feed_url="https://example.com/rss.xml",
            title="Old item",
            article_url="https://example.com/story",
            published_at="Fri, 14 Apr 2023 20:00:28 GMT",
        )
    ]

    with pytest.raises(ValueError, match="stale_feed_items"):
        _filter_recent_feed_items(
            items,
            max_age_days=14,
            now=datetime(2026, 4, 15, tzinfo=timezone.utc),
        )


def test_filter_recent_feed_items_keeps_stale_items_when_guardrail_disabled() -> None:
    items = [
        LiveNewsFeedItem(
            source="cnn",
            feed_url="https://example.com/rss.xml",
            title="Old item",
            article_url="https://example.com/story",
            published_at="Fri, 14 Apr 2023 20:00:28 GMT",
        )
    ]

    filtered = _filter_recent_feed_items(
        items,
        max_age_days=0,
        now=datetime(2026, 4, 15, tzinfo=timezone.utc),
    )

    assert filtered == items


def test_default_live_news_feeds_exclude_known_failing_sources() -> None:
    feed_names = {name for name, _ in DEFAULT_LIVE_NEWS_FEEDS}

    assert {"aljazeera", "reuters", "npr", "nytimes", "skynews", "washpost"}.isdisjoint(feed_names)
    assert len(feed_names) >= 50
    assert {
        "bbc",
        "cnn",
        "euronews",
        "guardian",
        "abcnews",
        "cbsnews",
        "foxnews",
        "nbcnews",
        "latimes",
        "politico",
        "telegraph",
        "the_hindu",
        "france24",
        "dw",
    }.issubset(feed_names)


def test_run_live_news_digestion_clusters_consumes_unique_urls_and_writes_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    feeds = (
        LiveNewsFeedSpec(source="bbc", feed_url="https://example.com/bbc.xml"),
        LiveNewsFeedSpec(source="guardian", feed_url="https://example.com/guardian.xml"),
    )
    items_by_source = {
        "bbc": [
            LiveNewsFeedItem(
                source="bbc",
                feed_url="https://example.com/bbc.xml",
                title="BBC A",
                article_url="https://example.com/a",
            ),
            LiveNewsFeedItem(
                source="bbc",
                feed_url="https://example.com/bbc.xml",
                title="BBC C",
                article_url="https://example.com/c",
            ),
            LiveNewsFeedItem(
                source="bbc",
                feed_url="https://example.com/bbc.xml",
                title="BBC E",
                article_url="https://example.com/e",
            ),
        ],
        "guardian": [
            LiveNewsFeedItem(
                source="guardian",
                feed_url="https://example.com/guardian.xml",
                title="Guardian A duplicate",
                article_url="https://example.com/a",
            ),
            LiveNewsFeedItem(
                source="guardian",
                feed_url="https://example.com/guardian.xml",
                title="Guardian D",
                article_url="https://example.com/d",
            ),
            LiveNewsFeedItem(
                source="guardian",
                feed_url="https://example.com/guardian.xml",
                title="Guardian F",
                article_url="https://example.com/f",
            ),
        ],
    }

    def fake_load_feed_items(client, feed, *, per_feed_limit):
        del client, per_feed_limit
        return list(items_by_source[feed.source])

    seen_urls: list[str] = []

    def fake_evaluate_item(client, item, *, pack_id, storage_root, registry):
        del client, pack_id, storage_root, registry
        seen_urls.append(item.article_url)
        return LiveNewsArticleResult(
            source=item.source,
            title=item.title,
            article_url=item.article_url,
            published_at=None,
            text_source="paragraphs",
            word_count=250,
            entity_count=1,
            timing_ms=20,
            warnings=[],
            issue_types=["missing_acronym_candidate"],
            issues=[
                LiveNewsIssue(
                    issue_type="missing_acronym_candidate",
                    message="all-caps acronym appears in text but was not extracted",
                    candidate_text="US",
                )
            ],
            entities=[LiveNewsEntity(text="Iran", label="location", start=0, end=4)],
        )

    monkeypatch.setattr("ades.news_feedback._load_feed_items", fake_load_feed_items)
    monkeypatch.setattr("ades.news_feedback._evaluate_live_news_item", fake_evaluate_item)

    report = run_live_news_digestion_clusters(
        "general-en",
        storage_root=tmp_path / "storage",
        cluster_count=2,
        cluster_size=2,
        per_feed_limit=3,
        feeds=feeds,
        artifact_root=tmp_path / "artifacts",
        processed_root=tmp_path / "state",
        write_artifacts=True,
    )

    assert isinstance(report, LiveNewsDigestionRunReport)
    assert report.completed_cluster_count == 2
    assert report.collected_article_count == 4
    assert report.previously_processed_article_count == 0
    assert report.newly_processed_article_count == 4
    assert report.known_processed_article_count == 4
    assert seen_urls == [
        "https://example.com/a",
        "https://example.com/d",
        "https://example.com/c",
        "https://example.com/f",
    ]
    assert report.merged_suggestions == [
        LiveNewsFixSuggestion(
            issue_type="missing_acronym_candidate",
            fix_class="strengthen_acronym_retention",
            issue_count=4,
            recommendation="Retain all-caps acronyms when surrounding context supports geopolitical or organizational usage.",
            cluster_indexes=[1, 2],
            sample_titles=["BBC A", "Guardian D", "BBC C", "Guardian F"],
            sample_urls=[
                "https://example.com/a",
                "https://example.com/d",
                "https://example.com/c",
                "https://example.com/f",
            ],
            sample_candidate_texts=["US"],
            sample_entity_texts=[],
        )
    ]
    assert live_news_digestion_cluster_report_path("general-en", 1, root=tmp_path / "artifacts").exists()
    assert live_news_digestion_cluster_report_path("general-en", 2, root=tmp_path / "artifacts").exists()
    assert live_news_feedback_cluster_suggestions_path("general-en", 1, root=tmp_path / "artifacts").exists()
    assert live_news_feedback_cluster_suggestions_path("general-en", 2, root=tmp_path / "artifacts").exists()
    assert live_news_feedback_merged_suggestions_path("general-en", root=tmp_path / "artifacts").exists()
    assert live_news_digestion_run_report_path("general-en", root=tmp_path / "artifacts").exists()
    processed_store = live_news_processed_articles_path("general-en", root=tmp_path / "state")
    assert processed_store.exists()
    processed_payload = json.loads(processed_store.read_text(encoding="utf-8"))
    assert processed_payload["article_count"] == 4


def test_run_live_news_digestion_clusters_skips_historical_processed_urls(
    monkeypatch,
    tmp_path: Path,
) -> None:
    historical_report = (
        tmp_path
        / "state"
        / "historical-run"
        / "reports"
        / "live-news-digestion"
        / "general-en"
        / "cluster-01.json"
    )
    historical_report.parent.mkdir(parents=True, exist_ok=True)
    historical_report.write_text(
        json.dumps(
            {
                "pack_id": "general-en",
                "generated_at": "2026-04-15T09:00:00+00:00",
                "articles": [
                    {
                        "source": "bbc",
                        "title": "Older A",
                        "article_url": "https://example.com/a",
                        "published_at": "2026-04-10T09:00:00+00:00",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    feeds = (
        LiveNewsFeedSpec(source="bbc", feed_url="https://example.com/bbc.xml"),
        LiveNewsFeedSpec(source="guardian", feed_url="https://example.com/guardian.xml"),
    )
    items_by_source = {
        "bbc": [
            LiveNewsFeedItem(
                source="bbc",
                feed_url="https://example.com/bbc.xml",
                title="BBC A",
                article_url="https://example.com/a",
            ),
            LiveNewsFeedItem(
                source="bbc",
                feed_url="https://example.com/bbc.xml",
                title="BBC C",
                article_url="https://example.com/c",
            ),
            LiveNewsFeedItem(
                source="bbc",
                feed_url="https://example.com/bbc.xml",
                title="BBC E",
                article_url="https://example.com/e",
            ),
        ],
        "guardian": [
            LiveNewsFeedItem(
                source="guardian",
                feed_url="https://example.com/guardian.xml",
                title="Guardian A duplicate",
                article_url="https://example.com/a",
            ),
            LiveNewsFeedItem(
                source="guardian",
                feed_url="https://example.com/guardian.xml",
                title="Guardian D",
                article_url="https://example.com/d",
            ),
            LiveNewsFeedItem(
                source="guardian",
                feed_url="https://example.com/guardian.xml",
                title="Guardian F",
                article_url="https://example.com/f",
            ),
        ],
    }

    def fake_load_feed_items(client, feed, *, per_feed_limit):
        del client, per_feed_limit
        return list(items_by_source[feed.source])

    seen_urls: list[str] = []

    def fake_evaluate_item(client, item, *, pack_id, storage_root, registry):
        del client, pack_id, storage_root, registry
        seen_urls.append(item.article_url)
        return LiveNewsArticleResult(
            source=item.source,
            title=item.title,
            article_url=item.article_url,
            published_at=None,
            text_source="paragraphs",
            word_count=250,
            entity_count=1,
            timing_ms=20,
            warnings=[],
            issue_types=["missing_acronym_candidate"],
            issues=[
                LiveNewsIssue(
                    issue_type="missing_acronym_candidate",
                    message="all-caps acronym appears in text but was not extracted",
                    candidate_text="US",
                )
            ],
            entities=[LiveNewsEntity(text="Iran", label="location", start=0, end=4)],
        )

    monkeypatch.setattr("ades.news_feedback._load_feed_items", fake_load_feed_items)
    monkeypatch.setattr("ades.news_feedback._evaluate_live_news_item", fake_evaluate_item)

    report = run_live_news_digestion_clusters(
        "general-en",
        storage_root=tmp_path / "storage",
        cluster_count=2,
        cluster_size=2,
        per_feed_limit=3,
        feeds=feeds,
        artifact_root=tmp_path / "artifacts",
        processed_root=tmp_path / "state",
        write_artifacts=True,
    )

    assert report.previously_processed_article_count == 1
    assert report.newly_processed_article_count == 4
    assert report.known_processed_article_count == 5
    assert seen_urls == [
        "https://example.com/c",
        "https://example.com/d",
        "https://example.com/e",
        "https://example.com/f",
    ]
    processed_store = live_news_processed_articles_path("general-en", root=tmp_path / "state")
    processed_payload = json.loads(processed_store.read_text(encoding="utf-8"))
    assert processed_payload["article_count"] == 5
