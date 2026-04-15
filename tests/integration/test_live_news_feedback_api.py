from pathlib import Path

from ades import evaluate_live_news_feedback
from ades.news_feedback import LiveNewsFeedbackReport


def test_public_api_can_evaluate_live_news_feedback(tmp_path: Path, monkeypatch) -> None:
    def fake_report(pack_id, *, storage_root, article_limit, per_feed_limit):
        del storage_root, article_limit, per_feed_limit
        return LiveNewsFeedbackReport(
            pack_id=pack_id,
            generated_at="2026-04-15T00:00:00+00:00",
            requested_article_count=10,
            collected_article_count=2,
            feed_count=4,
            successful_feed_count=3,
            p50_latency_ms=110,
            p95_latency_ms=180,
            per_source_article_counts={"bbc": 1, "cnn": 1},
            per_issue_counts={"missing_acronym_candidate": 1},
            suggested_fix_classes=["strengthen_acronym_retention"],
        )

    monkeypatch.setattr("ades.api.run_evaluate_live_news_feedback", fake_report)

    report_path = tmp_path / "reports" / "live-news.json"
    response = evaluate_live_news_feedback(
        "general-en",
        storage_root=tmp_path / "storage",
        article_limit=10,
        per_feed_limit=5,
        report_path=report_path,
    )

    assert response.pack_id == "general-en"
    assert response.collected_article_count == 2
    assert response.report_path == str(report_path.resolve())
    assert report_path.exists()
