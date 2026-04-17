import argparse
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys

from ades.packs.alias_analysis import analyze_alias_candidates, build_alias_candidate
from ades.packs.general_alias_review import (
    GENERAL_ALIAS_REVIEW_MODE_AI_COMMAND,
    GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH,
    _resolve_reviewer_name,
    review_general_retained_aliases,
)


def _create_analysis_db_with_generic_aliases(root: Path) -> Path:
    analysis_db_path = root / "analysis.sqlite"
    analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="cars",
                display_text="Cars",
                label="organization",
                canonical_text="Cars",
                record={
                    "entity_id": "organization:cars",
                    "source_name": "curated-general",
                },
            ),
            build_alias_candidate(
                alias_key="four ways",
                display_text="Four Ways",
                label="location",
                canonical_text="Four Ways",
                record={
                    "entity_id": "location:four-ways",
                    "source_name": "curated-general",
                },
            ),
            build_alias_candidate(
                alias_key="north harbor",
                display_text="North Harbor",
                label="location",
                canonical_text="North Harbor",
                record={
                    "entity_id": "location:north-harbor",
                    "source_name": "curated-general",
                },
            ),
        ],
        allowed_ambiguous_aliases=set(),
        analysis_db_path=analysis_db_path,
        materialize_retained_aliases=False,
    )
    return analysis_db_path


def _create_retained_alias_db_with_mixed_candidates(root: Path) -> Path:
    analysis_db_path = root / "mixed-analysis.sqlite"
    connection = sqlite3.connect(str(analysis_db_path))
    try:
        connection.execute(
            """
            CREATE TABLE retained_aliases (
                display_text TEXT NOT NULL,
                label TEXT NOT NULL,
                display_sort_key TEXT NOT NULL,
                label_sort_key TEXT NOT NULL,
                generated INTEGER NOT NULL DEFAULT 0,
                score REAL NOT NULL DEFAULT 1.0,
                canonical_text TEXT NOT NULL DEFAULT '',
                source_name TEXT NOT NULL DEFAULT '',
                entity_id TEXT NOT NULL DEFAULT '',
                source_priority REAL NOT NULL DEFAULT 0.6,
                popularity_weight REAL NOT NULL DEFAULT 0.5
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO retained_aliases (
                display_text,
                label,
                display_sort_key,
                label_sort_key,
                generated,
                score,
                canonical_text,
                source_name,
                entity_id,
                source_priority,
                popularity_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("Cars", "organization", "cars", "organization", 0, 1.0, "Cars", "curated-general", "organization:cars", 0.6, 0.5),
                ("Four Ways", "location", "four ways", "location", 0, 1.0, "Four Ways", "curated-general", "location:four-ways", 0.6, 0.5),
                ("K7 Music", "organization", "k7 music", "organization", 0, 1.0, "K7 Music", "curated-general", "organization:k7-music", 0.6, 0.5),
            ],
        )
        connection.commit()
    finally:
        connection.close()
    return analysis_db_path


def _create_retained_alias_db_with_deterministic_candidates(root: Path) -> Path:
    analysis_db_path = root / "deterministic-analysis.sqlite"
    connection = sqlite3.connect(str(analysis_db_path))
    try:
        connection.execute(
            """
            CREATE TABLE retained_aliases (
                display_text TEXT NOT NULL,
                label TEXT NOT NULL,
                display_sort_key TEXT NOT NULL,
                label_sort_key TEXT NOT NULL,
                generated INTEGER NOT NULL DEFAULT 0,
                score REAL NOT NULL DEFAULT 1.0,
                canonical_text TEXT NOT NULL DEFAULT '',
                source_name TEXT NOT NULL DEFAULT '',
                entity_id TEXT NOT NULL DEFAULT '',
                source_priority REAL NOT NULL DEFAULT 0.6,
                popularity_weight REAL NOT NULL DEFAULT 0.5
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO retained_aliases (
                display_text,
                label,
                display_sort_key,
                label_sort_key,
                generated,
                score,
                canonical_text,
                source_name,
                entity_id,
                source_priority,
                popularity_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("Cars", "organization", "cars", "organization", 0, 1.0, "Cars", "curated-general", "organization:cars", 0.6, 0.5),
                ("March", "location", "march", "location", 0, 1.0, "March", "curated-general", "location:march", 0.6, 0.5),
                ("North Harbor", "location", "north harbor", "location", 0, 1.0, "North Harbor", "curated-general", "location:north-harbor", 0.6, 0.5),
                ("1+1 Media Group", "organization", "1+1 media group", "organization", 0, 1.0, "1+1 Media Group", "curated-general", "organization:1plus1-media-group", 0.6, 0.5),
            ],
        )
        connection.commit()
    finally:
        connection.close()
    return analysis_db_path


def _create_batch_reviewer_script(root: Path) -> Path:
    reviewer_path = root / "reviewer.py"
    reviewer_path.write_text(
        "\n".join(
            [
                "import json",
                "import sys",
                "",
                "payload = json.loads(sys.stdin.read())",
                "reviews = []",
                "for alias in payload['aliases']:",
                "    if alias['text'] == 'Four Ways':",
                "        reviews.append({",
                "            'review_key': alias['review_key'],",
                "            'decision': 'drop',",
                "            'reason_code': 'generic_retained_alias_from_batch_review',",
                "            'reason': 'too generic for general-en',",
                "        })",
                "    else:",
                "        reviews.append({",
                "            'review_key': alias['review_key'],",
                "            'decision': 'keep',",
                "            'reason_code': 'specific_retained_alias',",
                "            'reason': 'specific enough to keep',",
                "        })",
                "print(json.dumps({'reviews': reviews}))",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return reviewer_path


def _create_mcoda_style_batch_reviewer_script(root: Path) -> Path:
    reviewer_path = root / "mcoda_reviewer.py"
    reviewer_path.write_text(
        "\n".join(
            [
                "import json",
                "import sys",
                "",
                "payload = json.loads(sys.stdin.read())",
                "reviews = []",
                "for alias in payload['aliases']:",
                "    reviews.append({",
                "        'review_key': alias['review_key'],",
                "        'decision': 'drop' if alias['text'] == 'Four Ways' else 'keep',",
                "        'reason_code': 'generic_retained_alias_from_batch_review' if alias['text'] == 'Four Ways' else 'specific_retained_alias',",
                "        'reason': 'too generic for general-en' if alias['text'] == 'Four Ways' else 'specific enough to keep',",
                "    })",
                "response = {",
                "    'agent': {'slug': 'qwen-3.5-27b'},",
                "    'responses': [",
                "        {",
                "            'output': json.dumps({'reviews': reviews}),",
                "            'adapter': 'ollama-cli',",
                "            'model': 'qwen3.5:27b',",
                "            'metadata': {},",
                "        }",
                "    ],",
                "}",
                "print(json.dumps(response))",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return reviewer_path


def _load_review_script_module() -> object:
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "review_general_retained_aliases.py"
    spec = importlib.util.spec_from_file_location(
        "review_general_retained_aliases_script",
        script_path,
    )
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_review_general_retained_aliases_batches_and_reuses_cache(tmp_path: Path) -> None:
    analysis_db_path = _create_analysis_db_with_generic_aliases(tmp_path)
    reviewer_path = _create_batch_reviewer_script(tmp_path)
    output_root = tmp_path / "review-output"
    command = f"{sys.executable} {reviewer_path}"

    first = review_general_retained_aliases(
        analysis_db_path,
        reviewer_command=command,
        output_root=output_root,
        chunk_size=2,
    )

    assert first.batches_run == 1
    assert first.new_reviewed_count == 2
    assert first.cached_review_count == 0
    assert first.total_reviewed_count == 2
    assert first.proposed_drop_count == 1
    assert first.proposed_keep_count == 1
    assert first.remaining_unreviewed_count == 0
    assert Path(first.artifacts.review_db_path).exists()
    assert Path(first.artifacts.review_jsonl_path).exists()
    assert Path(first.artifacts.review_markdown_path).exists()
    assert Path(first.artifacts.drop_jsonl_path).exists()

    markdown = Path(first.artifacts.review_markdown_path).read_text(encoding="utf-8")
    assert "Four Ways" in markdown
    assert "North Harbor" not in markdown

    drop_rows = [
        json.loads(line)
        for line in Path(first.artifacts.drop_jsonl_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {(row["text"], row["label"]) for row in drop_rows} == {("Four Ways", "location")}

    second = review_general_retained_aliases(
        analysis_db_path,
        reviewer_command=command,
        output_root=output_root,
        chunk_size=2,
    )

    assert second.batches_run == 0
    assert second.new_reviewed_count == 0
    assert second.cached_review_count == 2
    assert second.total_reviewed_count == 2
    assert second.proposed_drop_count == 1
    assert second.proposed_keep_count == 1


def test_review_general_retained_aliases_accepts_mcoda_json_envelope(
    tmp_path: Path,
) -> None:
    analysis_db_path = _create_analysis_db_with_generic_aliases(tmp_path)
    reviewer_path = _create_mcoda_style_batch_reviewer_script(tmp_path)
    output_root = tmp_path / "review-output"
    command = f"{sys.executable} {reviewer_path}"

    result = review_general_retained_aliases(
        analysis_db_path,
        reviewer_command=command,
        output_root=output_root,
        chunk_size=3,
    )

    assert result.total_reviewed_count == 2
    assert result.proposed_drop_count == 1
    assert result.proposed_keep_count == 1

    drop_rows = [
        json.loads(line)
        for line in Path(result.artifacts.drop_jsonl_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {(row["text"], row["label"]) for row in drop_rows} == {("Four Ways", "location")}


def test_resolve_reviewer_name_formats_mcoda_agent_run() -> None:
    assert (
        _resolve_reviewer_name(
            "mcoda agent-run qwen-3.5-27b --stdin --json",
            review_mode=GENERAL_ALIAS_REVIEW_MODE_AI_COMMAND,
        )
        == "mcoda:qwen-3.5-27b"
    )


def test_resolve_reviewer_name_uses_deterministic_mode_name() -> None:
    assert (
        _resolve_reviewer_name(
            None,
            review_mode=GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH,
        )
        == GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH
    )


def test_review_script_resolves_mcoda_agent_command() -> None:
    module = _load_review_script_module()
    args = argparse.Namespace(
        review_command="",
        mcoda_agent="qwen-3.5-27b",
        mcoda_bin="mcoda",
    )

    reviewer_command = module.resolve_review_command(args)

    assert reviewer_command == "mcoda agent-run qwen-3.5-27b --stdin --json"


def test_review_general_retained_aliases_candidate_mode_likely_generic(
    tmp_path: Path,
) -> None:
    analysis_db_path = _create_retained_alias_db_with_mixed_candidates(tmp_path)
    reviewer_path = _create_batch_reviewer_script(tmp_path)
    output_root = tmp_path / "review-output"
    command = f"{sys.executable} {reviewer_path}"

    result = review_general_retained_aliases(
        analysis_db_path,
        reviewer_command=command,
        output_root=output_root,
        candidate_mode="likely_generic",
        chunk_size=10,
    )

    assert result.candidate_mode == "likely_generic"
    assert result.candidate_total_count == 2
    assert result.total_reviewed_count == 2
    assert result.remaining_unreviewed_count == 0

    review_rows = [
        json.loads(line)
        for line in Path(result.artifacts.review_jsonl_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {(row["text"], row["label"]) for row in review_rows} == {
        ("Cars", "organization"),
        ("Four Ways", "location"),
    }


def test_review_general_retained_aliases_deterministic_common_english(
    tmp_path: Path,
) -> None:
    analysis_db_path = _create_retained_alias_db_with_deterministic_candidates(tmp_path)
    output_root = tmp_path / "deterministic-review-output"

    result = review_general_retained_aliases(
        analysis_db_path,
        review_mode=GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH,
        output_root=output_root,
        candidate_mode="all",
        chunk_size=10,
    )

    assert result.reviewer_name == GENERAL_ALIAS_REVIEW_MODE_DETERMINISTIC_COMMON_ENGLISH
    assert result.candidate_total_count == 4
    assert result.total_reviewed_count == 4
    assert result.proposed_drop_count == 2
    assert result.proposed_keep_count == 2

    drop_rows = [
        json.loads(line)
        for line in Path(result.artifacts.drop_jsonl_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {(row["text"], row["label"]) for row in drop_rows} == {
        ("Cars", "organization"),
        ("March", "location"),
    }
