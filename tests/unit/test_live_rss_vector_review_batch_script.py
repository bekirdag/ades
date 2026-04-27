from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path
import sys
from types import SimpleNamespace


def _load_module():
    module_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "planning"
        / "live_rss_vector_review_batch.py"
    )
    spec = spec_from_file_location("live_rss_vector_review_batch_script", module_path)
    assert spec is not None
    module = module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_persist_processed_result_records_success_and_failure(tmp_path: Path) -> None:
    module = _load_module()
    records: dict[str, object] = {}
    processed_root = tmp_path / "state"

    module._persist_processed_result(
        pack_id="general-en",
        root=processed_root,
        records=records,
        result={
            "article_url": "https://example.com/a",
            "source": "bbc",
            "title": "Alpha",
            "published_at": "2026-04-26T00:00:00Z",
            "status": "success",
        },
        processed_at="2026-04-26T10:00:00Z",
    )

    ledger_path = module.live_news_processed_articles_path(
        "general-en",
        root=processed_root,
    )
    payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert payload["article_count"] == 1
    assert payload["articles"][0]["article_url"] == "https://example.com/a"
    assert payload["articles"][0]["processed_count"] == 1
    assert payload["articles"][0]["last_status"] == "success"

    module._persist_processed_result(
        pack_id="general-en",
        root=processed_root,
        records=records,
        result={
            "article_url": "https://example.com/a",
            "source": "bbc",
            "title": "Alpha",
            "published_at": "2026-04-26T00:00:00Z",
            "status": "article_extract",
            "message": "insufficient_text:42",
        },
        processed_at="2026-04-26T10:05:00Z",
    )

    payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert payload["article_count"] == 1
    assert payload["articles"][0]["processed_count"] == 2
    assert payload["articles"][0]["first_processed_at"] == "2026-04-26T10:00:00Z"
    assert payload["articles"][0]["last_processed_at"] == "2026-04-26T10:05:00Z"
    assert payload["articles"][0]["last_status"] == "article_extract"
    assert payload["articles"][0]["last_message"] == "insufficient_text:42"


def test_persist_processed_result_ignores_missing_article_url(tmp_path: Path) -> None:
    module = _load_module()
    records: dict[str, object] = {}
    processed_root = tmp_path / "state"

    module._persist_processed_result(
        pack_id="general-en",
        root=processed_root,
        records=records,
        result={"status": "worker_error", "message": "boom"},
        processed_at="2026-04-26T10:00:00Z",
    )

    ledger_path = module.live_news_processed_articles_path(
        "general-en",
        root=processed_root,
    )
    assert not ledger_path.exists()
    assert records == {}


def test_low_value_video_candidate_detection_is_narrow() -> None:
    module = _load_module()

    assert (
        module._looks_like_low_value_video_candidate(
            article_url="https://abcnews.go.com/video/132339935/",
            title="Police release statement",
        )
        is True
    )
    assert (
        module._looks_like_low_value_video_candidate(
            article_url="https://example.com/news/video-game-industry-reports-growth",
            title="Video game industry reports growth",
        )
        is False
    )


def test_load_candidates_skips_low_value_video_urls(monkeypatch) -> None:
    module = _load_module()
    feed = SimpleNamespace(
        source="abcnews",
        feed_url="https://abcnews.go.com/abcnews/topstories",
    )

    class _FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

    monkeypatch.setattr(module.httpx, "Client", lambda **kwargs: _FakeClient())
    monkeypatch.setattr(module, "_resolve_live_news_feeds", lambda _specs: [feed])
    monkeypatch.setattr(module, "load_live_news_processed_articles", lambda pack_id, root: {})

    monkeypatch.setattr(
        module,
        "_load_feed_items",
        lambda client, feed, *, per_feed_limit: [
            SimpleNamespace(
                source=feed.source,
                feed_url=feed.feed_url,
                title="Watch: suspect video",
                article_url="https://abcnews.go.com/video/132339935/",
                published_at="2026-04-26T00:00:00Z",
            ),
            SimpleNamespace(
                source=feed.source,
                feed_url=feed.feed_url,
                title="Normal article",
                article_url="https://abcnews.go.com/US/story?id=132393780",
                published_at="2026-04-26T00:01:00Z",
            ),
        ],
    )

    candidates, feed_failures = module._load_candidates(
        pack_id="general-en",
        per_feed_limit=4,
        include_seen=False,
        limit=None,
    )

    assert feed_failures == []
    assert [candidate["article_url"] for candidate in candidates] == [
        "https://abcnews.go.com/US/story?id=132393780"
    ]


def test_detect_news_context_artifact_prefers_explicit_path(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    explicit = tmp_path / "explicit.json"
    explicit.write_text("{}", encoding="utf-8")
    env_path = tmp_path / "env.json"
    env_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("ADES_NEWS_CONTEXT_ARTIFACT_PATH", str(env_path))

    detected = module._detect_news_context_artifact(str(explicit))

    assert detected == explicit.resolve()


def test_detect_news_context_artifact_uses_env_when_present(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    env_path = tmp_path / "env.json"
    env_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("ADES_NEWS_CONTEXT_ARTIFACT_PATH", str(env_path))

    detected = module._detect_news_context_artifact(None)

    assert detected == env_path.resolve()


def test_worker_init_sets_news_context_env_when_provided(monkeypatch, tmp_path: Path) -> None:
    module = _load_module()

    class _FakeClient:
        def close(self) -> None:
            return None

    class _FakeRegistry:
        def __init__(self, root: Path) -> None:
            self.root = root

    monkeypatch.setattr(module.httpx, "Client", lambda **kwargs: _FakeClient())
    monkeypatch.setattr(module, "PackRegistry", _FakeRegistry)
    monkeypatch.setattr(module, "load_pack_runtime", lambda *args, **kwargs: object())

    class _FakeGraphStore:
        def __init__(self, path: Path) -> None:
            self.path = path

        def close(self) -> None:
            return None

    monkeypatch.setattr(module, "QidGraphStore", _FakeGraphStore)

    graph_path = tmp_path / "graph.sqlite"
    graph_path.write_text("graph", encoding="utf-8")
    news_path = tmp_path / "recent-news-support.json"
    news_path.write_text("{}", encoding="utf-8")

    module._worker_init(
        "general-en",
        str(tmp_path),
        str(graph_path),
        str(news_path),
    )
    try:
        assert module.os.environ["ADES_GRAPH_CONTEXT_ARTIFACT_PATH"] == str(graph_path)
        assert module.os.environ["ADES_NEWS_CONTEXT_ARTIFACT_PATH"] == str(news_path)
    finally:
        module._worker_close()


def test_write_summary_records_news_context_artifact(tmp_path: Path) -> None:
    module = _load_module()
    summary_path = tmp_path / "summary.json"
    run_root = tmp_path / "run"
    run_root.mkdir()
    graph_path = tmp_path / "graph.sqlite"
    graph_path.write_text("graph", encoding="utf-8")
    news_path = tmp_path / "recent-news-support.json"
    news_path.write_text("{}", encoding="utf-8")

    module._write_summary(
        path=summary_path,
        run_root=run_root,
        graph_artifact_path=graph_path,
        news_context_artifact_path=news_path,
        target_count=1,
        feeds=2,
        per_feed_limit=3,
        candidate_count=4,
        selected_count=2,
        success_records=[],
        failure_counts={},
        feed_failures=[],
    )

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["graph_artifact_path"] == str(graph_path)
    assert payload["news_context_artifact_path"] == str(news_path)
