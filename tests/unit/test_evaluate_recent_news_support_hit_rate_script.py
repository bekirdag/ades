import json
import subprocess
import sys
from pathlib import Path


SCRIPT_PATH = Path("scripts/evaluate_recent_news_support_hit_rate.py")


def test_recent_news_support_hit_rate_script_reports_pair_coverage(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent_news_support.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "generated_at": "2026-05-13T00:00:00Z",
                "article_count": 1,
                "qid_metadata": {
                    "wikidata:Q1": {"canonical_text": "Turkey"},
                    "wikidata:Q2": {"canonical_text": "Lira"},
                },
                "cooccurrence": {
                    "wikidata:Q1": {"wikidata:Q2": 2},
                    "wikidata:Q2": {"wikidata:Q1": 2},
                },
            }
        ),
        encoding="utf-8",
    )
    record_path = tmp_path / "records.jsonl"
    record_path.write_text(
        json.dumps(
            {
                "status": "accepted",
                "topics": ["finance"],
                "entities": [
                    {"entity_id": "wikidata:Q1", "canonical_text": "Turkey"},
                    {"entity_id": "wikidata:Q2", "canonical_text": "Lira"},
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--artifact",
            str(artifact_path),
            "--record-jsonl",
            str(record_path),
            "--min-pair-count",
            "2",
            "--gate",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    summary = json.loads(result.stdout)

    assert summary["eligible_record_count"] == 1
    assert summary["supported_pair_count"] == 1
    assert summary["pair_hit_rate"] == 1
    assert summary["gate_passed"] is True
