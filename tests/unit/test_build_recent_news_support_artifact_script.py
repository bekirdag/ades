import json
import subprocess
import sys
from pathlib import Path


SCRIPT_PATH = Path("scripts/build_recent_news_support_artifact.py")


def _summary_path(tmp_path: Path, reports: list[dict[str, object]]) -> Path:
    path = tmp_path / "summary.json"
    path.write_text(json.dumps({"reports": reports}), encoding="utf-8")
    return path


def _cases_summary_path(tmp_path: Path, cases: list[dict[str, object]]) -> Path:
    path = tmp_path / "cases-summary.json"
    path.write_text(json.dumps({"cases": cases}), encoding="utf-8")
    return path


def _articles_summary_path(tmp_path: Path, articles: list[dict[str, object]]) -> Path:
    path = tmp_path / "articles-summary.json"
    path.write_text(json.dumps({"articles": articles}), encoding="utf-8")
    return path


def _entity(entity_id: str, canonical_text: str, label: str) -> dict[str, object]:
    return {
        "entity_id": entity_id,
        "canonical_text": canonical_text,
        "label": label,
        "source_name": "wikidata-general-entities",
    }


def test_recent_news_support_builder_can_exclude_report_sources(tmp_path: Path) -> None:
    summary_path = _summary_path(
        tmp_path,
        [
            {
                "status": "success",
                "source": "evening_standard",
                "title": "Noisy article",
                "article_url": "https://example.com/noisy",
                "plain_entities": [
                    _entity("wikidata:Q1", "Alpha", "organization"),
                    _entity("wikidata:Q2", "Beta", "person"),
                ],
            },
            {
                "status": "success",
                "source": "bbc",
                "title": "Clean article",
                "article_url": "https://example.com/clean",
                "plain_entities": [
                    _entity("wikidata:Q1", "Alpha", "organization"),
                    _entity("wikidata:Q3", "Gamma", "person"),
                ],
            },
        ],
    )
    output_path = tmp_path / "recent_news_support.json"

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--summary",
            str(summary_path),
            "--entity-source",
            "plain",
            "--exclude-report-source",
            "evening_standard",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    summary = json.loads(output_path.with_name("build_summary.json").read_text(encoding="utf-8"))

    assert artifact["excluded_report_sources"] == ["evening_standard"]
    assert summary["excluded_report_sources"] == ["evening_standard"]
    assert summary["success_record_count"] == 1
    assert summary["article_count"] == 1
    assert sorted(artifact["qid_metadata"]) == ["wikidata:Q1", "wikidata:Q3"]
    assert artifact["cooccurrence"] == {"wikidata:Q1": {"wikidata:Q3": 1}, "wikidata:Q3": {"wikidata:Q1": 1}}


def test_recent_news_support_builder_reads_cases_plain_and_hinted_entities(tmp_path: Path) -> None:
    summary_path = _cases_summary_path(
        tmp_path,
        [
            {
                "article_url": "https://example.com/plain-and-hinted",
                "source": "bbc",
                "plain": {
                    "entities": [
                        _entity("wikidata:Q1", "Alpha", "organization"),
                        _entity("wikidata:Q2", "Beta", "person"),
                    ]
                },
                "hinted": {
                    "entities": [
                        _entity("wikidata:Q1", "Alpha", "organization"),
                        _entity("wikidata:Q3", "Gamma", "organization"),
                    ]
                },
            }
        ],
    )
    plain_output = tmp_path / "recent_news_support_plain.json"
    flagged_output = tmp_path / "recent_news_support_flagged.json"

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--summary",
            str(summary_path),
            "--entity-source",
            "plain",
            "--output",
            str(plain_output),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--summary",
            str(summary_path),
            "--entity-source",
            "flagged",
            "--output",
            str(flagged_output),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    plain_artifact = json.loads(plain_output.read_text(encoding="utf-8"))
    flagged_artifact = json.loads(flagged_output.read_text(encoding="utf-8"))

    assert sorted(plain_artifact["qid_metadata"]) == ["wikidata:Q1", "wikidata:Q2"]
    assert sorted(flagged_artifact["qid_metadata"]) == ["wikidata:Q1", "wikidata:Q3"]
    assert plain_artifact["cooccurrence"] == {
        "wikidata:Q1": {"wikidata:Q2": 1},
        "wikidata:Q2": {"wikidata:Q1": 1},
    }
    assert flagged_artifact["cooccurrence"] == {
        "wikidata:Q1": {"wikidata:Q3": 1},
        "wikidata:Q3": {"wikidata:Q1": 1},
    }


def test_recent_news_support_builder_reads_cases_linked_entities(tmp_path: Path) -> None:
    summary_path = _cases_summary_path(
        tmp_path,
        [
            {
                "article_url": "https://example.com/linked-entities",
                "source": "dw",
                "linked_entities": [
                    {
                        "entity_id": "wikidata:Q8",
                        "canonical_text": "Delta",
                        "label": "organization",
                    },
                    {
                        "entity_id": "wikidata:Q9",
                        "canonical_text": "Epsilon",
                        "label": "location",
                    },
                ],
            }
        ],
    )
    output_path = tmp_path / "recent_news_support.json"

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--summary",
            str(summary_path),
            "--entity-source",
            "plain",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    summary = json.loads(output_path.with_name("build_summary.json").read_text(encoding="utf-8"))

    assert sorted(artifact["qid_metadata"]) == ["wikidata:Q8", "wikidata:Q9"]
    assert summary["article_count"] == 1
    assert artifact["cooccurrence"] == {
        "wikidata:Q8": {"wikidata:Q9": 1},
        "wikidata:Q9": {"wikidata:Q8": 1},
    }


def test_recent_news_support_builder_reads_articles_records(tmp_path: Path) -> None:
    summary_path = _articles_summary_path(
        tmp_path,
        [
            {
                "article_url": "https://example.com/articles-layout",
                "source": "nbcnews",
                "entities": [
                    _entity("wikidata:Q10", "Gamma", "organization"),
                    _entity("wikidata:Q11", "Delta", "location"),
                ],
            }
        ],
    )
    output_path = tmp_path / "recent_news_support_articles.json"

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--summary",
            str(summary_path),
            "--entity-source",
            "plain",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))

    assert sorted(artifact["qid_metadata"]) == ["wikidata:Q10", "wikidata:Q11"]
    assert artifact["cooccurrence"] == {
        "wikidata:Q10": {"wikidata:Q11": 1},
        "wikidata:Q11": {"wikidata:Q10": 1},
    }


def test_recent_news_support_builder_filters_probable_source_outlets_by_default(
    tmp_path: Path,
) -> None:
    summary_path = _summary_path(
        tmp_path,
        [
            {
                "status": "success",
                "source": "abc",
                "article_url": "https://example.com/abc-1",
                "plain_entities": [
                    _entity("wikidata:Q1", "MailOnline", "organization"),
                    _entity("wikidata:Q5", "Associated Press", "organization"),
                    _entity("wikidata:Q2", "Alpha", "organization"),
                    _entity("wikidata:Q3", "Beta", "person"),
                ],
            },
            {
                "status": "success",
                "source": "guardian",
                "article_url": "https://example.com/guardian-1",
                "plain_entities": [
                    _entity("wikidata:Q4", "The Guardian", "organization"),
                    _entity("wikidata:Q2", "Alpha", "organization"),
                    _entity("wikidata:Q3", "Beta", "person"),
                ],
            },
        ],
    )
    output_path = tmp_path / "recent_news_support_filtered.json"

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--summary",
            str(summary_path),
            "--entity-source",
            "plain",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    summary = json.loads(output_path.with_name("build_summary.json").read_text(encoding="utf-8"))

    assert artifact["keep_probable_source_outlets"] is False
    assert summary["keep_probable_source_outlets"] is False
    assert summary["filtered_probable_source_outlet_count"] == 3
    assert sorted(artifact["qid_metadata"]) == ["wikidata:Q2", "wikidata:Q3"]
    assert artifact["cooccurrence"] == {
        "wikidata:Q2": {"wikidata:Q3": 2},
        "wikidata:Q3": {"wikidata:Q2": 2},
    }


def test_recent_news_support_builder_can_keep_probable_source_outlets_when_requested(
    tmp_path: Path,
) -> None:
    summary_path = _summary_path(
        tmp_path,
        [
            {
                "status": "success",
                "source": "abc",
                "article_url": "https://example.com/abc-keep",
                "plain_entities": [
                    _entity("wikidata:Q1", "ABC News", "organization"),
                    _entity("wikidata:Q2", "Alpha", "organization"),
                ],
            }
        ],
    )
    output_path = tmp_path / "recent_news_support_keep.json"

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--summary",
            str(summary_path),
            "--entity-source",
            "plain",
            "--keep-probable-source-outlets",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    summary = json.loads(output_path.with_name("build_summary.json").read_text(encoding="utf-8"))

    assert artifact["keep_probable_source_outlets"] is True
    assert summary["keep_probable_source_outlets"] is True
    assert summary["filtered_probable_source_outlet_count"] == 0
    assert sorted(artifact["qid_metadata"]) == ["wikidata:Q1", "wikidata:Q2"]
