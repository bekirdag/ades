import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ades.vector.news_context import (
    build_recent_news_support_payload,
    load_recent_news_support_artifact,
    write_recent_news_support_artifact,
)


def _artifact_path(tmp_path: Path) -> Path:
    path = tmp_path / "recent-news-support.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-26T00:00:00Z",
                "article_count": 12,
                "qid_metadata": {
                    "wikidata:Q1": {
                        "canonical_text": "OpenAI",
                        "entity_type": "organization",
                        "source_name": "saved-news",
                        "document_count": 4,
                    },
                    "wikidata:Q2": {
                        "canonical_text": "Microsoft",
                        "entity_type": "organization",
                        "source_name": "saved-news",
                        "document_count": 5,
                    },
                    "wikidata:Q12": {
                        "canonical_text": "Enterprise AI",
                        "entity_type": "organization",
                        "source_name": "saved-news",
                        "document_count": 3,
                    },
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q12": 3, "wikidata:Q2": 2},
                    "wikidata:Q2": {"wikidata:Q12": 2, "wikidata:Q1": 2},
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def test_load_recent_news_support_artifact_returns_cached_instance(tmp_path: Path) -> None:
    path = _artifact_path(tmp_path)

    first = load_recent_news_support_artifact(path)
    second = load_recent_news_support_artifact(path)

    assert first is second
    assert first.article_count == 12
    assert first.metadata_for("wikidata:Q12") is not None
    assert first.metadata_for("wikidata:Q12").canonical_text == "Enterprise AI"


def test_recent_news_support_requires_multiple_seed_support(tmp_path: Path) -> None:
    artifact = load_recent_news_support_artifact(_artifact_path(tmp_path))

    support = artifact.support_for_candidate(
        "wikidata:Q12",
        ["wikidata:Q1", "wikidata:Q2", "wikidata:Q1"],
        min_supporting_seeds=2,
        min_pair_count=2,
    )

    assert support is not None
    assert support.supporting_seed_ids == ("wikidata:Q1", "wikidata:Q2")
    assert support.shared_seed_count == 2
    assert support.total_support_count == 5
    assert support.max_pair_count == 3


def test_recent_news_support_rejects_single_seed_support(tmp_path: Path) -> None:
    artifact = load_recent_news_support_artifact(_artifact_path(tmp_path))

    support = artifact.support_for_candidate(
        "wikidata:Q12",
        ["wikidata:Q1"],
        min_supporting_seeds=2,
        min_pair_count=1,
    )

    assert support is None


def test_recent_news_support_lists_ranked_candidate_supports(tmp_path: Path) -> None:
    artifact = load_recent_news_support_artifact(_artifact_path(tmp_path))

    supports = artifact.candidate_supports_for_seeds(
        ["wikidata:Q1", "wikidata:Q2", "wikidata:Q1"],
        min_supporting_seeds=2,
        min_pair_count=2,
    )

    assert [item.entity_id for item in supports] == ["wikidata:Q12"]
    assert supports[0].supporting_seed_ids == ("wikidata:Q1", "wikidata:Q2")
    assert supports[0].shared_seed_count == 2
    assert supports[0].total_support_count == 5


def _news_record(
    *,
    published_at: datetime,
    topics: list[str],
    entities: list[dict[str, object]],
    url: str,
    source_reliability: str = "medium",
) -> dict[str, object]:
    return {
        "status": "accepted",
        "article_url": url,
        "published_at": published_at.isoformat(),
        "topics": topics,
        "source_reliability": source_reliability,
        "entities": entities,
    }


def test_build_recent_news_support_payload_keeps_rolling_windows() -> None:
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    records = [
        _news_record(
            published_at=now - timedelta(hours=2),
            topics=["finance", "markets"],
            source_reliability="high",
            entities=[
                {"entity_id": "wikidata:Q1", "canonical_text": "Crude oil", "label": "commodity"},
                {"entity_id": "wikidata:Q2", "canonical_text": "Iran", "label": "location"},
            ],
            url="https://example.com/recent",
        ),
        _news_record(
            published_at=now - timedelta(days=2),
            topics=["economy"],
            entities=[
                {"entity_id": "wikidata:Q1", "canonical_text": "Crude oil", "label": "commodity"},
                {"entity_id": "wikidata:Q3", "canonical_text": "OPEC", "label": "organization"},
            ],
            url="https://example.com/week",
        ),
        _news_record(
            published_at=now - timedelta(days=10),
            topics=["politics"],
            entities=[
                {"entity_id": "wikidata:Q1", "canonical_text": "Crude oil", "label": "commodity"},
                {"entity_id": "wikidata:Q4", "canonical_text": "Sanctions", "label": "policy"},
            ],
            url="https://example.com/month",
        ),
        _news_record(
            published_at=now - timedelta(days=31),
            topics=["finance"],
            entities=[
                {"entity_id": "wikidata:Q1", "canonical_text": "Crude oil", "label": "commodity"},
                {"entity_id": "wikidata:Q5", "canonical_text": "Out of window", "label": "event"},
            ],
            url="https://example.com/old",
        ),
    ]

    payload, summary = build_recent_news_support_payload(records, generated_at=now)

    assert payload["schema_version"] == 2
    assert payload["article_count"] == 3
    assert payload["windows"]["24h"]["article_count"] == 1
    assert payload["windows"]["7d"]["article_count"] == 2
    assert payload["windows"]["30d"]["article_count"] == 3
    assert "wikidata:Q5" not in payload["qid_metadata"]
    assert payload["cooccurrence"]["wikidata:Q1"]["wikidata:Q2"] >= 1
    assert payload["cooccurrence"]["wikidata:Q1"]["wikidata:Q4"] >= 1
    assert summary["window_article_counts"] == {"24h": 1, "7d": 2, "30d": 3}


def test_build_recent_news_support_payload_filters_soft_news_and_source_outlets() -> None:
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    records = [
        _news_record(
            published_at=now - timedelta(hours=1),
            topics=["sports"],
            entities=[
                {"entity_id": "wikidata:Q1", "canonical_text": "Alpha", "label": "organization"},
                {"entity_id": "wikidata:Q2", "canonical_text": "Beta", "label": "person"},
            ],
            url="https://example.com/sports",
        ),
        _news_record(
            published_at=now - timedelta(hours=1),
            topics=["finance"],
            entities=[
                {
                    "entity_id": "wikidata:Q3",
                    "canonical_text": "Associated Press",
                    "label": "organization",
                },
                {"entity_id": "wikidata:Q4", "canonical_text": "Copper", "label": "commodity"},
                {"entity_id": "wikidata:Q5", "canonical_text": "Chile", "label": "location"},
            ],
            url="https://example.com/finance",
        ),
    ]

    payload, summary = build_recent_news_support_payload(records, generated_at=now)

    assert summary["filtered_topic_count"] == 1
    assert summary["filtered_probable_source_outlet_count"] == 1
    assert sorted(payload["qid_metadata"]) == ["wikidata:Q4", "wikidata:Q5"]
    assert payload["cooccurrence"]["wikidata:Q4"]["wikidata:Q5"] >= 1


def test_write_recent_news_support_artifact_preserves_legacy_runtime_shape(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    output_path = tmp_path / "recent_news_support.json"

    write_recent_news_support_artifact(
        [
            _news_record(
                published_at=now - timedelta(hours=1),
                topics=["markets"],
                entities=[
                    {"entity_id": "wikidata:Q1", "canonical_text": "Dollar", "label": "currency"},
                    {"entity_id": "wikidata:Q2", "canonical_text": "Gold", "label": "commodity"},
                ],
                url="https://example.com/markets",
            )
        ],
        output_path,
        generated_at=now,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    artifact = load_recent_news_support_artifact(output_path)

    assert payload["windows"]["30d"]["cooccurrence"] == payload["cooccurrence"]
    assert artifact.article_count == 1
    assert artifact.support_for_candidate(
        "wikidata:Q2",
        ["wikidata:Q1", "wikidata:Q3"],
        min_supporting_seeds=1,
        min_pair_count=1,
    ) is not None
