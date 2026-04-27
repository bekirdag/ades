import json
from pathlib import Path

from ades.vector.news_context import load_recent_news_support_artifact


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
