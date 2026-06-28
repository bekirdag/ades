import json
from pathlib import Path

from ades.impact.bdya_gap_backlog import import_bdya_gap_backlog


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def test_import_bdya_gap_backlog_writes_review_queue_files(tmp_path: Path) -> None:
    input_path = tmp_path / "bdya_gap.jsonl"
    input_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "news_id": "news-1",
                        "story_id": "story-1",
                        "published_at": "2026-06-28T12:00:00Z",
                        "title": "UK parliament passes mining royalties bill",
                        "summary": "A mining policy story.",
                        "market_relevant": True,
                        "issue_classes": [
                            "relationship_proposals_present",
                            "source_entities_without_graph_terminal",
                        ],
                        "event_signals": [{"event_type": "sector_policy_change"}],
                        "source_entities": [{"entity_ref": "wikidata:Q123"}],
                        "terminal_candidates": [],
                        "unresolved_entities": [
                            {
                                "entity_ref": "wikidata:Qmining-bill",
                                "missing_reason": "no_graph_path",
                            }
                        ],
                        "relationship_proposals": [
                            {
                                "source_ref": "wikidata:Qparliament",
                                "target_ref": "ades:sector:mining",
                                "relation": "government_body_affects_sector",
                            }
                        ],
                        "warnings": ["event_signal_gate_filtered:test"],
                    }
                ),
                "{not-json",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = import_bdya_gap_backlog(
        input_paths=[input_path],
        output_root=tmp_path / "imports",
        run_id="run-1",
    )

    assert result.story_count == 1
    assert result.market_relevant_story_count == 1
    assert result.unresolved_entity_count == 1
    assert result.relationship_proposal_count == 1
    assert result.invalid_line_count == 1
    assert result.issue_counts == {
        "relationship_proposals_present": 1,
        "source_entities_without_graph_terminal": 1,
    }

    story_rows = _read_jsonl(result.story_misses_path)
    assert story_rows[0]["record_type"] == "story_miss"
    assert story_rows[0]["promotion_state"] == "observed"
    assert story_rows[0]["relationship_proposal_count"] == 1
    assert story_rows[0]["unresolved_entity_count"] == 1

    unresolved_rows = _read_jsonl(result.unresolved_entities_path)
    assert unresolved_rows[0]["record_type"] == "unresolved_entity"
    assert unresolved_rows[0]["review_priority"] == "high"
    assert unresolved_rows[0]["raw_item"] == {
        "entity_ref": "wikidata:Qmining-bill",
        "missing_reason": "no_graph_path",
    }

    proposal_rows = _read_jsonl(result.relationship_proposals_path)
    assert proposal_rows[0]["record_type"] == "relationship_proposal"
    assert proposal_rows[0]["review_priority"] == "high"
    assert proposal_rows[0]["raw_item"] == {
        "source_ref": "wikidata:Qparliament",
        "target_ref": "ades:sector:mining",
        "relation": "government_body_affects_sector",
    }

    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert summary["run_id"] == "run-1"
    assert summary["invalid_line_count"] == 1


def test_import_bdya_gap_backlog_uses_stable_ids(tmp_path: Path) -> None:
    input_path = tmp_path / "bdya_gap.jsonl"
    payload = {
        "news_id": "news-1",
        "story_id": "story-1",
        "market_relevant": True,
        "issue_classes": ["unresolved_entities_present"],
        "unresolved_entities": [{"entity_ref": "wikidata:Q1"}],
        "relationship_proposals": [],
    }
    input_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    first = import_bdya_gap_backlog(
        input_paths=[input_path],
        output_root=tmp_path / "first",
        run_id="run-1",
    )
    second = import_bdya_gap_backlog(
        input_paths=[input_path],
        output_root=tmp_path / "second",
        run_id="run-1",
    )

    first_row = _read_jsonl(first.unresolved_entities_path)[0]
    second_row = _read_jsonl(second.unresolved_entities_path)[0]
    assert first_row["import_id"] == second_row["import_id"]
