import csv
import json
from pathlib import Path

from ades.impact.proposal_promoter import promote_reviewed_relationship_proposals


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def test_promote_reviewed_relationship_proposals_writes_accepted_edges(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "relationship_proposals.jsonl"
    input_path.write_text(
        json.dumps(
            {
                "import_id": "bdya-relationship-proposal:test",
                "news_id": "news-1",
                "story_id": "story-1",
                "promotion_state": "accepted",
                "raw_item": {
                    "source_ref": "ades:uk-law:mining-royalties",
                    "target_ref": "ades:sector:mining",
                    "relation": "law_affects_sector",
                    "confidence": 0.91,
                    "source_name": "UK Parliament",
                    "source_url": "https://bills.parliament.uk/bills/example",
                    "source_snapshot": "2026-06-28",
                    "pack_ids": ["finance-uk-en"],
                    "notes": "reviewed official bill source",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = promote_reviewed_relationship_proposals(
        input_paths=[input_path],
        output_root=tmp_path / "promoted",
        run_id="run-1",
    )

    assert result.accepted_edge_count == 1
    assert result.rejected_proposal_count == 0
    assert result.needs_review_count == 0
    edges = _read_tsv(result.accepted_edges_path)
    assert edges[0]["source_ref"] == "ades:uk-law:mining-royalties"
    assert edges[0]["target_ref"] == "ades:sector:mining"
    assert edges[0]["relation"] == "law_affects_sector"
    assert edges[0]["confidence"] == "0.91"
    assert (
        edges[0]["compatible_event_types"]
        == "sector_policy_change,regulatory_enforcement,fiscal_expansion,fiscal_austerity,tariff,export_control,sanctions"
    )
    assert edges[0]["direction_preconditions"] == (
        "sector_policy_event_signal,jurisdiction_or_regulator_context"
    )
    assert "bdya_import_id=bdya-relationship-proposal:test" in edges[0]["notes"]


def test_promote_reviewed_relationship_proposals_rejects_missing_source(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "relationship_proposals.jsonl"
    input_path.write_text(
        json.dumps(
            {
                "promotion_state": "accepted",
                "raw_item": {
                    "source_ref": "ades:uk-law:mining-royalties",
                    "target_ref": "ades:sector:mining",
                    "relation": "law_affects_sector",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = promote_reviewed_relationship_proposals(
        input_paths=[input_path],
        output_root=tmp_path / "promoted",
        run_id="run-1",
    )

    assert result.accepted_edge_count == 0
    assert result.rejected_proposal_count == 1
    rejected = _read_jsonl(result.rejected_proposals_path)
    assert rejected[0]["reason"] == "source_or_edge_validation_failed"
    assert rejected[0]["warnings"] == [
        "missing_source_name",
        "missing_source_url",
        "missing_source_snapshot",
    ]


def test_promote_reviewed_relationship_proposals_keeps_observed_items_for_review(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "relationship_proposals.jsonl"
    input_path.write_text(
        json.dumps(
            {
                "promotion_state": "observed",
                "raw_item": {
                    "source_ref": "ades:uk-law:mining-royalties",
                    "target_ref": "ades:sector:mining",
                    "relation": "law_affects_sector",
                    "source_name": "UK Parliament",
                    "source_url": "https://bills.parliament.uk/bills/example",
                    "source_snapshot": "2026-06-28",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = promote_reviewed_relationship_proposals(
        input_paths=[input_path],
        output_root=tmp_path / "promoted",
        run_id="run-1",
    )

    assert result.accepted_edge_count == 0
    assert result.needs_review_count == 1
    needs_review = _read_jsonl(result.needs_review_path)
    assert needs_review[0]["reason"] == "not_accepted"
    assert needs_review[0]["warnings"] == ["review_state:observed"]


def test_promote_reviewed_relationship_proposals_routes_unknown_relation_to_review(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "relationship_proposals.jsonl"
    input_path.write_text(
        json.dumps(
            {
                "promotion_state": "accepted",
                "raw_item": {
                    "source_ref": "ades:test:source",
                    "target_ref": "ades:test:target",
                    "relation": "unknown_relation",
                    "source_name": "UK Parliament",
                    "source_url": "https://bills.parliament.uk/bills/example",
                    "source_snapshot": "2026-06-28",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = promote_reviewed_relationship_proposals(
        input_paths=[input_path],
        output_root=tmp_path / "promoted",
        run_id="run-1",
    )

    assert result.accepted_edge_count == 0
    assert result.needs_review_count == 1
    assert result.needs_review_counts == {"unknown_relation_schema": 1}


def test_promote_reviewed_relationship_proposals_rejects_low_trust_proposal_only_source(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "relationship_proposals.jsonl"
    input_path.write_text(
        json.dumps(
            {
                "promotion_state": "accepted",
                "raw_item": {
                    "source_ref": "ades:uk-law:mining-royalties",
                    "target_ref": "ades:sector:mining",
                    "relation": "law_affects_sector",
                    "source_name": "Reviewed Proposal Source",
                    "source_url": "https://review-queue.internal/proposals/program.jsonl",
                    "source_snapshot": "2026-06-28",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = promote_reviewed_relationship_proposals(
        input_paths=[input_path],
        output_root=tmp_path / "promoted",
        run_id="run-1",
    )

    assert result.accepted_edge_count == 0
    assert result.rejected_proposal_count == 1
    assert result.rejection_counts == {"low_trust_source_proposal_only": 1}
    rejected = _read_jsonl(result.rejected_proposals_path)
    assert rejected[0]["reason"] == "source_or_edge_validation_failed"
    assert rejected[0]["warnings"] == ["low_trust_source_proposal_only"]
