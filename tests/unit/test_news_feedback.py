import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from ades.config import Settings
from ades.news_feedback import (
    DEFAULT_LIVE_NEWS_FEEDS,
    HistoricalNewsRecord,
    HistoricalNewsSnapshotRecord,
    HistoricalNewsSourceSpec,
    LiveNewsArticleResult,
    LiveNewsDigestionRunReport,
    LiveNewsEntity,
    LiveNewsFeedItem,
    LiveNewsFeedSpec,
    LiveNewsFeedbackReport,
    LiveNewsFixSuggestion,
    LiveNewsIssue,
    _detect_news_feedback_issues,
    _download_historical_huggingface_rows_snapshot,
    _filter_historical_records_for_pack,
    _filter_recent_feed_items,
    _find_missing_structured_organization_candidates,
    _historical_snapshot_record_from_row,
    _pack_registry_for_storage_root,
    _resolve_historical_news_sources,
    download_historical_news_source_snapshots,
    evaluate_live_news_feedback,
    historical_news_digestion_cluster_report_path,
    historical_news_digestion_run_report_path,
    historical_news_feedback_cluster_suggestions_path,
    historical_news_feedback_merged_suggestions_path,
    historical_news_processed_articles_path,
    historical_news_source_metadata_path,
    historical_news_source_records_path,
    live_news_feedback_cluster_suggestions_path,
    live_news_feedback_merged_suggestions_path,
    live_news_digestion_cluster_report_path,
    live_news_digestion_run_report_path,
    live_news_processed_articles_path,
    run_historical_news_digestion_clusters,
    run_live_news_digestion_clusters,
)
from ades.service.models import EntityMatch, TagResponse
from ades.storage.backend import MetadataBackend, RuntimeTarget


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


def test_pack_registry_for_storage_root_uses_runtime_settings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    class DummyRegistry:
        def __init__(
            self,
            storage_root: Path,
            *,
            runtime_target: RuntimeTarget,
            metadata_backend: MetadataBackend,
            database_url: str | None,
        ) -> None:
            captured["storage_root"] = storage_root
            captured["runtime_target"] = runtime_target
            captured["metadata_backend"] = metadata_backend
            captured["database_url"] = database_url

    monkeypatch.setattr(
        "ades.news_feedback.get_settings",
        lambda: Settings(
            storage_root=Path("/mnt/githubActions/ades_big_data"),
            runtime_target=RuntimeTarget.PRODUCTION_SERVER,
            metadata_backend=MetadataBackend.POSTGRESQL,
            database_url="postgresql://example",
        ),
    )
    monkeypatch.setattr("ades.news_feedback.PackRegistry", DummyRegistry)

    registry = _pack_registry_for_storage_root(tmp_path / "runtime")

    assert isinstance(registry, DummyRegistry)
    assert captured == {
        "storage_root": tmp_path / "runtime",
        "runtime_target": RuntimeTarget.PRODUCTION_SERVER,
        "metadata_backend": MetadataBackend.POSTGRESQL,
        "database_url": "postgresql://example",
    }


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


def test_detect_live_news_feedback_issues_skips_candidates_contained_in_longer_extracted_spans() -> None:
    text = "Chair of the Marr Area Committee, Moira Innes, welcomed the discovery."
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[
            EntityMatch(
                text="Chair of the Marr Area Committee",
                label="organization",
                start=0,
                end=len("Chair of the Marr Area Committee"),
            )
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    candidates = {item.candidate_text for item in issues if item.candidate_text}

    assert "Marr Area Committee" not in candidates


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


def test_detect_live_news_feedback_issues_skips_person_and_office_title_partial_noise() -> None:
    text = (
        "Greig Laidlaw of Scotland spoke after the US District Judge Derrick Watson ruling."
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
                text="Greig Laidlaw",
                label="person",
                start=text.index("Greig Laidlaw"),
                end=text.index("Greig Laidlaw") + len("Greig Laidlaw"),
            ),
            EntityMatch(
                text="US",
                label="location",
                start=text.index("US"),
                end=text.index("US") + len("US"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    partial_candidates = {issue.candidate_text for issue in issues if issue.issue_type == "partial_span_candidate"}

    assert "Greig Laidlaw of Scotland" not in partial_candidates
    assert "US District Judge Derrick Watson" not in partial_candidates


def test_detect_live_news_feedback_issues_skips_structured_prefixes_already_covered_by_longer_org_spans() -> None:
    text = "Researchers at Massachusetts Institute of Technology issued a report."
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[
            EntityMatch(
                text="Massachusetts Institute of Technology",
                label="organization",
                start=text.index("Massachusetts Institute of Technology"),
                end=text.index("Massachusetts Institute of Technology")
                + len("Massachusetts Institute of Technology"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    structured_org_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_structured_organization_candidate"
    }

    assert "Massachusetts Institute" not in structured_org_candidates


def test_detect_live_news_feedback_issues_ignores_sentence_boundary_singletons() -> None:
    text = "I posted on Nimbus. Nimbus users agreed."
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[
            EntityMatch(
                text="Nimbus",
                label="organization",
                start=text.index("Nimbus"),
                end=text.index("Nimbus") + len("Nimbus"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)

    assert all(issue.issue_type != "single_token_name_fragment" for issue in issues)


def test_detect_live_news_feedback_issues_suppresses_read_more_acronym_noise() -> None:
    text = "READ MORE: analysts said the market stayed calm."
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }

    assert "READ" not in acronym_candidates
    assert "MORE" not in acronym_candidates


def test_detect_live_news_feedback_issues_suppresses_generic_acronym_noise_but_keeps_org_like_cases() -> None:
    text = (
        "Julian Knight MP praised the TV show. "
        "We support DACA, the act that helps students. "
        "LGBT (lesbian, gay, bisexual and transgender) refugees arrived. "
        "He told CNBC that WWF's centre had expanded."
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }

    assert "MP" not in acronym_candidates
    assert "TV" not in acronym_candidates
    assert "DACA" not in acronym_candidates
    assert "LGBT" not in acronym_candidates
    assert "CNBC" in acronym_candidates
    assert "WWF" in acronym_candidates


def test_detect_live_news_feedback_issues_suppresses_dateline_acronym_noise() -> None:
    text = "LONDON (Reuters) - British police said a bomb was used during an explosion."
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }

    assert "LONDON" not in acronym_candidates


def test_detect_live_news_feedback_issues_suppresses_opening_dateline_reference_and_wire_noise() -> None:
    text = (
        "OKLAHOMA CITY (KFOR) - At 8 AM lawmakers discussed HB 1005x. "
        "That should NEVER have happened! Dallas. AFP"
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }

    assert {"CITY", "KFOR", "AM", "HB", "NEVER", "AFP"}.isdisjoint(acronym_candidates)


def test_detect_live_news_feedback_issues_suppresses_roman_numeral_title_suffix_and_generic_tech_acronyms() -> None:
    text = (
        "Judge Michael Topolski QC ruled after player XI arrived. "
        "CCTV showed him entering after its IT system crashed."
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }

    assert {"QC", "XI", "CCTV", "IT"}.isdisjoint(acronym_candidates)


def test_detect_live_news_feedback_issues_suppresses_schedule_day_and_military_time_acronyms() -> None:
    text = "Watch live coverage from 1930 GMT on BBC One. Fixtures continue on TUE 5 SEP."
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }

    assert {"GMT", "TUE"}.isdisjoint(acronym_candidates)


def test_detect_live_news_feedback_issues_suppresses_section_heading_and_ap_dateline_noise() -> None:
    text = (
        "ANAHEIM, Calif. (AP) — Brayden Point scored in overtime. "
        "NOTES: Pittsburgh closed the month strongly. "
        "UP NEXT Lightning: At the Los Angeles Kings on Thursday night. "
        "___ More AP NHL: https://apnews.com/NHL"
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }

    assert {"AP", "NOTES", "UP", "NEXT"}.isdisjoint(acronym_candidates)


def test_detect_live_news_feedback_issues_suppresses_contraction_and_hyphen_fragment_acronym_noise() -> None:
    text = "I'VE NEVER BEEN to ABS-CBN studios."
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }

    assert {"VE", "BEEN", "ABS", "CBN"}.isdisjoint(acronym_candidates)


def test_detect_live_news_feedback_issues_suppresses_adjectival_hyphen_prefix_noise() -> None:
    text = (
        "Pakistan said the Pakistani-backed group issued threats while free Islamic-based "
        "education continued in the district."
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
                text="Pakistan",
                label="location",
                start=text.index("Pakistan"),
                end=text.index("Pakistan") + len("Pakistan"),
            )
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    hyphen_candidates = {
        issue.candidate_text
        for issue in issues
        if issue.issue_type == "missing_hyphenated_prefix_candidate"
    }

    assert {"Pakistani", "Islamic"}.isdisjoint(hyphen_candidates)


def test_detect_live_news_feedback_issues_skips_role_title_and_locationish_partial_noise() -> None:
    text = (
        "Marco Biagi, the SNP MSP for Edinburgh Central, wrote to his Member of Parliament. "
        "Nasser al Ansari is CEO of Qatari Diar. "
        "Since reunification with the Mainland of China in 1997, trade has expanded."
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
                text="SNP",
                label="organization",
                start=text.index("SNP"),
                end=text.index("SNP") + len("SNP"),
            ),
            EntityMatch(
                text="Parliament",
                label="organization",
                start=text.index("Parliament"),
                end=text.index("Parliament") + len("Parliament"),
            ),
            EntityMatch(
                text="CEO",
                label="organization",
                start=text.index("CEO"),
                end=text.index("CEO") + len("CEO"),
            ),
            EntityMatch(
                text="China",
                label="organization",
                start=text.index("China"),
                end=text.index("China") + len("China"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    partial_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "partial_span_candidate"
    }

    assert "SNP MSP for Edinburgh Central" not in partial_candidates
    assert "Member of Parliament" not in partial_candidates
    assert "CEO of Qatari Diar" not in partial_candidates
    assert "Mainland of China" not in partial_candidates


def test_detect_live_news_feedback_issues_suppresses_locative_person_singleton_noise() -> None:
    text = "Mr Hutchins, from Ilfracombe in Devon, said the harbour was quiet."
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=12,
        entities=[
            EntityMatch(
                text="Devon",
                label="person",
                start=text.index("Devon"),
                end=text.index("Devon") + len("Devon"),
            )
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    fragment_candidates = {
        issue.entity_text for issue in issues if issue.issue_type == "single_token_name_fragment"
    }

    assert "Devon" not in fragment_candidates


def test_detect_live_news_feedback_issues_suppresses_role_and_quantifier_noise() -> None:
    text = (
        "Several media outlets filed requests. "
        "He would like to become an Engineer for the DOT. "
        "Dr SK Gupta, Distinguished Professor & Head Clinical Research, spoke later."
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
                text="DOT",
                label="organization",
                start=text.index("DOT"),
                end=text.index("DOT") + len("DOT"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    partial_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "partial_span_candidate"
    }
    structured_org_candidates = {
        issue.candidate_text
        for issue in issues
        if issue.issue_type == "missing_structured_organization_candidate"
    }

    assert "Engineer for the DOT" not in partial_candidates
    assert "Several media" not in structured_org_candidates
    assert "Head Clinical Research" not in structured_org_candidates


def test_detect_live_news_feedback_issues_suppresses_person_initial_titled_work_and_route_noise() -> None:
    text = (
        "The names, JRR Tolkien and CS Lewis, were discussed. "
        "Game of Thrones actress Lena Headey appeared on stage. "
        "Steven Spielberg's film Bridge of Spies won attention. "
        "Rebecca Lowe set off from the UK for Iran by bicycle."
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
                text="Thrones",
                label="organization",
                start=text.index("Thrones"),
                end=text.index("Thrones") + len("Thrones"),
            ),
            EntityMatch(
                text="Spies",
                label="organization",
                start=text.index("Spies"),
                end=text.index("Spies") + len("Spies"),
            ),
            EntityMatch(
                text="Iran",
                label="organization",
                start=text.index("Iran"),
                end=text.index("Iran") + len("Iran"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }
    partial_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "partial_span_candidate"
    }

    assert {"JRR", "CS"}.isdisjoint(acronym_candidates)
    assert "Game of Thrones" not in partial_candidates
    assert "Bridge of Spies" not in partial_candidates
    assert "UK for Iran" not in partial_candidates


def test_detect_live_news_feedback_issues_suppresses_titled_work_acronym_and_truncated_comma_noise() -> None:
    text = (
        "The star of NOTLD returned for a screening. "
        "Wales XV lost the friendly. "
        "The Department for Culture, Media and Sport Committee met later."
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
                text="Culture",
                label="organization",
                start=text.index("Culture"),
                end=text.index("Culture") + len("Culture"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }
    partial_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "partial_span_candidate"
    }

    assert {"NOTLD", "XV"}.isdisjoint(acronym_candidates)
    assert "Department for Culture" not in partial_candidates


def test_detect_live_news_feedback_issues_suppresses_organiser_insp_and_person_affiliation_partial_noise() -> None:
    text = (
        "Organisers of the Essen meeting issued a statement. "
        "Det Ch Insp Richard Ocone of Avon and Somerset Police spoke later. "
        "John Degenkolb of Trek-Segafredo led the sprint."
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
                text="Essen",
                label="organization",
                start=text.index("Essen"),
                end=text.index("Essen") + len("Essen"),
            ),
            EntityMatch(
                text="Avon",
                label="organization",
                start=text.index("Avon"),
                end=text.index("Avon") + len("Avon"),
            ),
            EntityMatch(
                text="Trek-Segafredo",
                label="organization",
                start=text.index("Trek-Segafredo"),
                end=text.index("Trek-Segafredo") + len("Trek-Segafredo"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    partial_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "partial_span_candidate"
    }

    assert "Organisers of the Essen" not in partial_candidates
    assert "Ch Insp Richard Ocone of Avon" not in partial_candidates
    assert "John Degenkolb of Trek-Segafredo" not in partial_candidates


def test_detect_live_news_feedback_issues_suppresses_known_as_exam_resume_and_team_affiliation_noise() -> None:
    text = (
        "John Terry, known as JT, said his CV mattered less than his IQ and GCSE results. "
        "Later, Valtteri Bottas agreed to race for Mercedes while Kevin Pietersen skipped the "
        "IPL for Royal Challengers Bangalore. "
        "Separate parts of Bombardier were sold as Ryanair reviewed Irish-based crew planning."
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
                text="Mercedes",
                label="organization",
                start=text.index("Mercedes"),
                end=text.index("Mercedes") + len("Mercedes"),
            ),
            EntityMatch(
                text="IPL",
                label="organization",
                start=text.index("IPL"),
                end=text.index("IPL") + len("IPL"),
            ),
            EntityMatch(
                text="Bombardier",
                label="organization",
                start=text.index("Bombardier"),
                end=text.index("Bombardier") + len("Bombardier"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }
    partial_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "partial_span_candidate"
    }
    hyphen_candidates = {
        issue.candidate_text
        for issue in issues
        if issue.issue_type == "missing_hyphenated_prefix_candidate"
    }

    assert {"JT", "CV", "IQ", "GCSE"}.isdisjoint(acronym_candidates)
    assert "Mercedes for Sauber" not in partial_candidates
    assert "IPL for Royal Challengers Bangalore" not in partial_candidates
    assert "Parts of Bombardier" not in partial_candidates
    assert "Irish" not in hyphen_candidates


def test_detect_live_news_feedback_issues_suppresses_competition_index_and_common_noun_acronym_noise() -> None:
    text = (
        "The FA Cup tie resumed after his illustrious CV was mentioned. "
        "Britain's rider was granted a TUE to take medicine before the race. "
        "The broadcaster's shares rose on the FTSE 100 while the passenger boarded with no ID. "
        "Some media outlets speculated as NATRE worked with the RE Council of England and Wales. "
        "A London-based regulator and a Vermont-based group later issued statements."
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
                text="London",
                label="location",
                start=text.index("London"),
                end=text.index("London") + len("London"),
            ),
            EntityMatch(
                text="Vermont",
                label="location",
                start=text.index("Vermont"),
                end=text.index("Vermont") + len("Vermont"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }
    structured_org_candidates = {
        issue.candidate_text
        for issue in issues
        if issue.issue_type == "missing_structured_organization_candidate"
    }
    hyphen_candidates = {
        issue.candidate_text
        for issue in issues
        if issue.issue_type == "missing_hyphenated_prefix_candidate"
    }

    assert {"FA", "CV", "TUE", "FTSE", "ID"}.isdisjoint(acronym_candidates)
    assert "Some media" not in structured_org_candidates
    assert "RE Council" not in structured_org_candidates
    assert {"London", "Vermont"}.isdisjoint(hyphen_candidates)


def test_detect_live_news_feedback_issues_suppresses_archive_tail_noise_and_composite_spans() -> None:
    text = (
        "Michael Grade and Gilbert Bradley spoke with Stephanie Prowse outside Fortnum and Mason. "
        "The US National Hurricane Center said Hurricane Maria was strengthening. "
        "A director of Royal College of Nursing Wales later spoke on radio. "
        "The Hong Kong-based team then tested a GPS-guided device over a virtual private network (VPN) "
        "during an 8th Millennium BC exhibition while FA Cups were displayed."
    )
    response = TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.9",
        language="en",
        content_type="text/plain",
        timing_ms=9,
        entities=[
            EntityMatch(
                text="Michael",
                label="person",
                start=text.index("Michael"),
                end=text.index("Michael") + len("Michael"),
            ),
            EntityMatch(
                text="Gilbert",
                label="person",
                start=text.index("Gilbert"),
                end=text.index("Gilbert") + len("Gilbert"),
            ),
            EntityMatch(
                text="Stephanie",
                label="person",
                start=text.index("Stephanie"),
                end=text.index("Stephanie") + len("Stephanie"),
            ),
            EntityMatch(
                text="Fortnum",
                label="person",
                start=text.index("Fortnum"),
                end=text.index("Fortnum") + len("Fortnum"),
            ),
            EntityMatch(
                text="US",
                label="location",
                start=text.index("US"),
                end=text.index("US") + len("US"),
            ),
            EntityMatch(
                text="National Hurricane Center",
                label="organization",
                start=text.index("National Hurricane Center"),
                end=text.index("National Hurricane Center") + len("National Hurricane Center"),
            ),
            EntityMatch(
                text="Royal College of Nursing",
                label="organization",
                start=text.index("Royal College of Nursing"),
                end=text.index("Royal College of Nursing") + len("Royal College of Nursing"),
            ),
            EntityMatch(
                text="Wales",
                label="location",
                start=text.index("Wales"),
                end=text.index("Wales") + len("Wales"),
            ),
            EntityMatch(
                text="Hong Kong",
                label="location",
                start=text.index("Hong Kong"),
                end=text.index("Hong Kong") + len("Hong Kong"),
            ),
        ],
    )

    issues = _detect_news_feedback_issues(text, response)
    acronym_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "missing_acronym_candidate"
    }
    partial_candidates = {
        issue.candidate_text for issue in issues if issue.issue_type == "partial_span_candidate"
    }
    hyphen_candidates = {
        issue.candidate_text
        for issue in issues
        if issue.issue_type == "missing_hyphenated_prefix_candidate"
    }
    structured_org_candidates = {
        issue.candidate_text
        for issue in issues
        if issue.issue_type == "missing_structured_organization_candidate"
    }
    fragment_entities = {
        issue.entity_text
        for issue in issues
        if issue.issue_type == "single_token_name_fragment"
    }

    assert {"GPS", "VPN", "BC", "FA"}.isdisjoint(acronym_candidates)
    assert "Royal College of Nursing Wales" not in partial_candidates
    assert "Kong" not in hyphen_candidates
    assert "US National Hurricane Center" not in structured_org_candidates
    assert {"Michael", "Gilbert", "Stephanie", "Fortnum"}.isdisjoint(fragment_entities)


def test_find_missing_structured_organization_candidates_skips_generic_heads_but_keeps_real_solutions() -> None:
    text = (
        "The June media release said Two Systems had failed. "
        "Later, Dave Ramsey said Ramsey Solutions would respond."
    )

    candidates = _find_missing_structured_organization_candidates(text, extracted_norms=set())

    assert "June media" not in candidates
    assert "Two Systems" not in candidates
    assert "Ramsey Solutions" in candidates


def test_find_missing_structured_organization_candidates_skips_generic_leads_but_keeps_real_media_and_centre_spans() -> None:
    text = (
        "Every Centre reopened after the storm. "
        "Multiple Media outlets filed requests. "
        "Clickon Media hired editors. "
        "La Pradera International Health Centre opened a new ward."
    )

    candidates = _find_missing_structured_organization_candidates(text, extracted_norms=set())

    assert "Every Centre" not in candidates
    assert "Multiple Media" not in candidates
    assert "Clickon Media" in candidates
    assert "La Pradera International Health Centre" in candidates


def test_find_missing_structured_organization_candidates_skips_weak_all_caps_and_directional_noise() -> None:
    text = (
        "Recent research from IT consultancy Capgemini was cited. "
        "US media followed the briefing while East German intelligence watched HR systems. "
        "Later, the Insurance Council and the European Free Trade Association responded."
    )

    candidates = _find_missing_structured_organization_candidates(text, extracted_norms=set())

    assert "IT consultancy" not in candidates
    assert "US media" not in candidates
    assert "East German intelligence" not in candidates
    assert "HR systems" not in candidates
    assert "Insurance Council" in candidates
    assert "European Free Trade Association" in candidates


def test_filter_historical_records_for_pack_keeps_only_english_like_rows_for_general_en() -> None:
    english = HistoricalNewsRecord(
        source="ccnews_2019",
        record_id="https://example.com/en",
        title="Markets steady after talks",
        article_url="https://example.com/en",
        published_at="2019-05-01",
        text=(
            "The government said the talks were held in London and the markets were calm "
            "after the meeting with investors."
        ),
        text_source="plain_text",
    )
    spanish = HistoricalNewsRecord(
        source="ccnews_2019",
        record_id="https://example.com/es",
        title="Mercados tras las conversaciones",
        article_url="https://example.com/es",
        published_at="2019-05-01",
        text=(
            "Los mercados y el gobierno dijeron que las conversaciones fueron en Madrid "
            "despues de una reunion con inversores internacionales."
        ),
        text_source="plain_text",
    )

    filtered = _filter_historical_records_for_pack([english, spanish], pack_id="general-en")

    assert filtered == [english]


def test_filter_historical_records_for_pack_rejects_non_latin_dominated_rows_for_general_en() -> None:
    english = HistoricalNewsRecord(
        source="ccnews_2019",
        record_id="https://example.com/en-latin",
        title="Markets steady after talks",
        article_url="https://example.com/en-latin",
        published_at="2019-05-01",
        text=(
            "The government said the talks were held in London and the markets were calm "
            "after the meeting with investors."
        ),
        text_source="plain_text",
    )
    non_latin = HistoricalNewsRecord(
        source="ccnews_2019",
        record_id="https://example.com/non-latin",
        title="നടൻ പ്രകാശ് രാജ് രാഷ്ട്രീയത്തിലേക്ക്",
        article_url="https://example.com/non-latin",
        published_at="2019-05-01",
        text=(
            "നടൻ പ്രകാശ് രാജ് രാഷ്ട്രീയത്തിലേക്ക് ലോക്സഭാ തിരഞ്ഞെടുപ്പില് മത്സരിക്കും HAPPY NEW YEAR "
            "UR SARKAR"
        ),
        text_source="plain_text",
    )

    filtered = _filter_historical_records_for_pack([english, non_latin], pack_id="general-en")

    assert filtered == [english]


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


def test_download_historical_news_source_snapshots_writes_jsonl_and_metadata(
    monkeypatch,
    tmp_path: Path,
) -> None:
    source = HistoricalNewsSourceSpec(
        source="bbc_archive_2017_01",
        kind="huggingface_rows",
        dataset_id="RealTimeData/bbc_news_alltime",
        config="2017-01",
        split="train",
        title_field="title",
        body_fields=("content",),
        summary_fields=("description",),
        url_field="link",
        published_at_field="published_date",
    )

    def fake_download_snapshot(client, spec, *, record_limit):
        del client, record_limit
        assert spec == source
        return [
            HistoricalNewsSnapshotRecord(
                source=spec.source,
                kind=spec.kind,
                dataset_id=spec.dataset_id,
                config=spec.config,
                split=spec.split,
                row_idx=0,
                record_id="https://example.com/old-a",
                title="Older A",
                article_url="https://example.com/old-a",
                published_at="2017-01-21",
                text=" ".join(["alpha"] * 80),
                text_source="content",
                metadata={"section": "World"},
            )
        ]

    monkeypatch.setattr(
        "ades.news_feedback._download_historical_huggingface_rows_snapshot",
        fake_download_snapshot,
    )

    active_sources, failures, snapshot_paths = download_historical_news_source_snapshots(
        sources=(source,),
        data_root=tmp_path / "datasets",
        overwrite=True,
    )

    assert active_sources == (source,)
    assert failures == []
    snapshot_path = historical_news_source_records_path(source.source, root=tmp_path / "datasets")
    assert snapshot_paths == {source.source: snapshot_path}
    assert snapshot_path.exists()
    lines = snapshot_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["title"] == "Older A"
    assert payload["article_url"] == "https://example.com/old-a"
    metadata_path = historical_news_source_metadata_path(source.source, root=tmp_path / "datasets")
    assert metadata_path.exists()
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["record_count"] == 1
    assert metadata["dataset_id"] == "RealTimeData/bbc_news_alltime"


def test_download_historical_huggingface_rows_snapshot_retries_retryable_statuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = HistoricalNewsSourceSpec(
        source="bbc_archive_2017_01",
        kind="huggingface_rows",
        dataset_id="RealTimeData/bbc_news_alltime",
        config="2017-01",
        split="train",
        title_field="title",
        body_fields=("content",),
        summary_fields=("description",),
        url_field="link",
        published_at_field="published_date",
    )
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(
                429,
                headers={"retry-after": "0"},
                request=request,
            )
        return httpx.Response(
            200,
            json={
                "rows": [
                    {
                        "row_idx": 0,
                        "row": {
                            "title": "Older A",
                            "content": " ".join(["alpha"] * 80),
                            "description": "Archive row",
                            "link": "https://example.com/old-a",
                            "published_date": "2017-01-21",
                        },
                    }
                ]
            },
            request=request,
        )

    monkeypatch.setattr("ades.news_feedback.time.sleep", lambda _seconds: None)
    client = httpx.Client(transport=httpx.MockTransport(handler))

    try:
        records = _download_historical_huggingface_rows_snapshot(
            client,
            source,
            record_limit=1,
        )
    finally:
        client.close()

    assert calls["count"] == 2
    assert len(records) == 1
    assert records[0].article_url == "https://example.com/old-a"


def test_resolve_historical_news_sources_includes_expanded_dataset_mappings() -> None:
    active_sources = {source.source: source for source in _resolve_historical_news_sources(None)}

    cnn = active_sources["cnn_dailymail_v3"]
    assert cnn.dataset_id == "abisee/cnn_dailymail"
    assert cnn.body_fields == ("article",)
    assert cnn.summary_fields == ("highlights",)
    assert cnn.url_field is None

    xsum = active_sources["xsum"]
    assert xsum.dataset_id == "EdinburghNLP/xsum"
    assert xsum.body_fields == ("document",)
    assert xsum.summary_fields == ("summary",)

    ccnews = active_sources["ccnews_2018"]
    assert ccnews.dataset_id == "stanford-oval/ccnews"
    assert ccnews.config == "2018"
    assert ccnews.body_fields == ("plain_text",)
    assert ccnews.url_field == "requested_url"
    assert ccnews.published_at_field == "published_date"


def test_historical_snapshot_record_uses_summary_or_text_title_fallbacks() -> None:
    xsum_spec = HistoricalNewsSourceSpec(
        source="xsum",
        kind="huggingface_rows",
        dataset_id="EdinburghNLP/xsum",
        config="default",
        split="train",
        body_fields=("document",),
        summary_fields=("summary",),
        url_field=None,
        published_at_field=None,
    )
    xsum_record = _historical_snapshot_record_from_row(
        xsum_spec,
        {
            "row_idx": 5,
            "row": {
                "document": "Storm Frank caused severe flooding across the region. " * 10,
                "summary": "Flood clean-up continues across Scotland after Storm Frank.",
            },
        },
    )

    assert xsum_record is not None
    assert xsum_record.title == "Flood clean-up continues across Scotland after Storm Frank."
    assert xsum_record.article_url == "hf://EdinburghNLP/xsum/default/train/5"

    reuters_spec = HistoricalNewsSourceSpec(
        source="reuters_news_summary",
        kind="huggingface_rows",
        dataset_id="argilla/news-summary",
        config="default",
        split="train",
        body_fields=("text",),
        summary_fields=(),
        url_field=None,
        published_at_field=None,
    )
    reuters_record = _historical_snapshot_record_from_row(
        reuters_spec,
        {
            "row_idx": 7,
            "row": {
                "text": (
                    "PHNOM PENH (Reuters) Sweden said it was halting some new aid for Cambodia "
                    "after an opposition crackdown, while education and research support would continue. "
                )
                * 4,
            },
        },
    )

    assert reuters_record is not None
    assert reuters_record.title.startswith("PHNOM PENH (Reuters) Sweden said it was halting some new aid")
    assert reuters_record.title.endswith("...")


def test_run_historical_news_digestion_clusters_consumes_disk_records_and_writes_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "datasets" / "records.jsonl"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "source": "bbc_archive_2017_01",
            "record_id": "https://example.com/a",
            "title": "Older A",
            "article_url": "https://example.com/a",
            "published_at": "2017-01-21",
            "text": "United States officials said London markets remained calm. " * 8,
            "text_source": "content",
        },
        {
            "source": "bbc_archive_2017_02",
            "record_id": "https://example.com/b",
            "title": "Older B",
            "article_url": "https://example.com/b",
            "published_at": "2017-02-21",
            "text": "European Union leaders said Paris traders expected steady demand. " * 8,
            "text_source": "content",
        },
        {
            "source": "bbc_archive_2017_01",
            "record_id": "https://example.com/c",
            "title": "Older C",
            "article_url": "https://example.com/c",
            "published_at": "2017-03-21",
            "text": "United Nations delegates said Brussels exporters remained active. " * 8,
            "text_source": "content",
        },
        {
            "source": "bbc_archive_2017_02",
            "record_id": "https://example.com/d",
            "title": "Older D",
            "article_url": "https://example.com/d",
            "published_at": "2017-04-21",
            "text": "Bank officials in Berlin said traders expected stable policy signals. " * 8,
            "text_source": "content",
        },
    ]
    snapshot_path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    sources = (
        HistoricalNewsSourceSpec(
            source="bbc_archive_2017_01",
            kind="local_jsonl",
            snapshot_path=str(snapshot_path),
        ),
        HistoricalNewsSourceSpec(
            source="bbc_archive_2017_02",
            kind="local_jsonl",
            snapshot_path=str(snapshot_path),
        ),
    )

    def fake_tag_article_text(text, *, pack_id, storage_root, registry):
        del pack_id, storage_root, registry
        candidate = "US" if "United States" in text else "EU"
        return TagResponse(
            version="0.1.0",
            pack="general-en",
            pack_version="0.3.2",
            language="en",
            content_type="text/plain",
            timing_ms=18,
            entities=[
                EntityMatch(
                    text="London",
                    label="location",
                    start=text.index("London") if "London" in text else 0,
                    end=(text.index("London") + len("London")) if "London" in text else 6,
                )
            ],
        )

    def fake_detect_issues(text, response, *, source=None):
        del text, response, source
        return [
            LiveNewsIssue(
                issue_type="missing_acronym_candidate",
                message="all-caps acronym appears in text but was not extracted",
                candidate_text="US",
            )
        ]

    monkeypatch.setattr("ades.news_feedback._tag_article_text", fake_tag_article_text)
    monkeypatch.setattr("ades.news_feedback._detect_news_feedback_issues", fake_detect_issues)

    report = run_historical_news_digestion_clusters(
        "general-en",
        storage_root=tmp_path / "storage",
        cluster_count=2,
        cluster_size=2,
        per_source_limit=4,
        sources=sources,
        data_root=tmp_path / "datasets-root",
        artifact_root=tmp_path / "artifacts",
        processed_root=tmp_path / "state",
        write_artifacts=True,
    )

    assert isinstance(report, LiveNewsDigestionRunReport)
    assert report.completed_cluster_count == 2
    assert report.collected_article_count == 4
    assert report.feed_count == 2
    assert report.successful_feed_count == 2
    assert report.per_issue_counts == {"missing_acronym_candidate": 4}
    assert historical_news_digestion_cluster_report_path("general-en", 1, root=tmp_path / "artifacts").exists()
    assert historical_news_digestion_cluster_report_path("general-en", 2, root=tmp_path / "artifacts").exists()
    assert historical_news_feedback_cluster_suggestions_path("general-en", 1, root=tmp_path / "artifacts").exists()
    assert historical_news_feedback_cluster_suggestions_path("general-en", 2, root=tmp_path / "artifacts").exists()
    assert historical_news_feedback_merged_suggestions_path("general-en", root=tmp_path / "artifacts").exists()
    assert historical_news_digestion_run_report_path("general-en", root=tmp_path / "artifacts").exists()
    processed_store = historical_news_processed_articles_path("general-en", root=tmp_path / "state")
    assert processed_store.exists()
    processed_payload = json.loads(processed_store.read_text(encoding="utf-8"))
    assert processed_payload["article_count"] == 4


def test_run_historical_news_digestion_clusters_skips_historical_processed_records(
    monkeypatch,
    tmp_path: Path,
) -> None:
    historical_report = (
        tmp_path
        / "state"
        / "historical-run"
        / "reports"
        / "historical-news-digestion"
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
                        "source": "bbc_archive_2017_01",
                        "title": "Older A",
                        "article_url": "https://example.com/a",
                        "published_at": "2017-01-21",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    snapshot_path = tmp_path / "datasets" / "records.jsonl"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "source": "bbc_archive_2017_01",
            "record_id": "https://example.com/a",
            "title": "Older A",
            "article_url": "https://example.com/a",
            "published_at": "2017-01-21",
            "text": "United States officials said London markets remained calm. " * 8,
            "text_source": "content",
        },
        {
            "source": "bbc_archive_2017_01",
            "record_id": "https://example.com/c",
            "title": "Older C",
            "article_url": "https://example.com/c",
            "published_at": "2017-03-21",
            "text": "United Nations delegates said Brussels exporters remained active. " * 8,
            "text_source": "content",
        },
    ]
    snapshot_path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    sources = (
        HistoricalNewsSourceSpec(
            source="bbc_archive_2017_01",
            kind="local_jsonl",
            snapshot_path=str(snapshot_path),
        ),
    )

    def fake_tag_article_text(text, *, pack_id, storage_root, registry):
        del text, pack_id, storage_root, registry
        return TagResponse(
            version="0.1.0",
            pack="general-en",
            pack_version="0.3.2",
            language="en",
            content_type="text/plain",
            timing_ms=18,
            entities=[],
        )

    monkeypatch.setattr("ades.news_feedback._tag_article_text", fake_tag_article_text)
    monkeypatch.setattr("ades.news_feedback._detect_news_feedback_issues", lambda *args, **kwargs: [])

    report = run_historical_news_digestion_clusters(
        "general-en",
        storage_root=tmp_path / "storage",
        cluster_count=1,
        cluster_size=1,
        per_source_limit=2,
        sources=sources,
        data_root=tmp_path / "datasets-root",
        artifact_root=tmp_path / "artifacts",
        processed_root=tmp_path / "state",
        write_artifacts=True,
    )

    assert report.previously_processed_article_count == 1
    assert report.newly_processed_article_count == 1
    assert report.known_processed_article_count == 2
    processed_store = historical_news_processed_articles_path("general-en", root=tmp_path / "state")
    processed_payload = json.loads(processed_store.read_text(encoding="utf-8"))
    assert processed_payload["article_count"] == 2
