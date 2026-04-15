import json

from typer.testing import CliRunner

from ades.cli import app
from ades.service.models import RegistryEvaluateLiveNewsFeedbackResponse


def test_cli_can_evaluate_live_news_feedback(monkeypatch) -> None:
    def fake_response(*args, **kwargs):
        del args, kwargs
        return RegistryEvaluateLiveNewsFeedbackResponse(
            pack_id="general-en",
            generated_at="2026-04-15T00:00:00+00:00",
            requested_article_count=10,
            collected_article_count=3,
            feed_count=4,
            successful_feed_count=3,
            p50_latency_ms=120,
            p95_latency_ms=190,
            per_source_article_counts={"bbc": 1, "cnn": 1, "euronews": 1},
            per_issue_counts={"missing_acronym_candidate": 2},
            suggested_fix_classes=["strengthen_acronym_retention"],
            report_path=None,
        )

    monkeypatch.setattr("ades.cli.api_evaluate_live_news_feedback", fake_response)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "registry",
            "evaluate-live-news",
            "general-en",
            "--article-limit",
            "10",
            "--per-feed-limit",
            "5",
            "--no-write-report",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["pack_id"] == "general-en"
    assert payload["collected_article_count"] == 3
    assert payload["per_issue_counts"]["missing_acronym_candidate"] == 2
