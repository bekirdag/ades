import json
from pathlib import Path

from ades.runtime_matcher import (
    build_matcher_artifact_from_aliases_json,
    find_exact_match_spans,
    load_runtime_matcher,
)


def test_matcher_artifact_generation_is_deterministic(tmp_path: Path) -> None:
    aliases_path = tmp_path / "aliases.json"
    aliases_path.write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "text": "Org Beta",
                        "label": "organization",
                        "normalized_text": "org beta",
                        "canonical_text": "Org Beta",
                        "entity_id": "org:beta",
                        "source_domain": "finance",
                    },
                    {
                        "text": "TICKA",
                        "label": "ticker",
                        "normalized_text": "ticka",
                        "canonical_text": "TICKA",
                        "entity_id": "ticker:ticka",
                        "source_domain": "finance",
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    first = build_matcher_artifact_from_aliases_json(
        aliases_path,
        output_dir=tmp_path / "matcher-a",
    )
    second = build_matcher_artifact_from_aliases_json(
        aliases_path,
        output_dir=tmp_path / "matcher-b",
    )

    assert first.algorithm == "aho_corasick"
    assert first.normalization == "normalize_lookup_text:v1"
    assert first.entry_count == 2
    assert first.state_count >= 3
    assert first.max_alias_length == len("org beta")
    assert first.artifact_sha256 == second.artifact_sha256
    assert first.entries_sha256 == second.entries_sha256


def test_matcher_artifact_generation_writes_entry_payloads(tmp_path: Path) -> None:
    aliases_path = tmp_path / "aliases.json"
    aliases_path.write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "text": "Jordan Vale",
                        "label": "person",
                        "canonical_text": "Jordan Elliott Vale",
                        "entity_id": "person:jordan-vale",
                        "source_domain": "general",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_matcher_artifact_from_aliases_json(
        aliases_path,
        output_dir=tmp_path / "matcher",
    )

    entries = [
        json.loads(line)
        for line in Path(result.entries_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    automaton = json.loads(Path(result.artifact_path).read_text(encoding="utf-8"))

    assert entries == [
        {
            "alias_score": 1.0,
            "canonical_text": "Jordan Elliott Vale",
            "entity_id": "person:jordan-vale",
            "generated": False,
            "label": "person",
            "normalized_text": "jordan vale",
            "popularity_weight": 0.5,
            "score": 1.0,
            "source_domain": "general",
            "source_name": "",
            "source_priority": 0.6,
            "text": "Jordan Vale",
        }
    ]
    assert automaton["algorithm"] == "aho_corasick"
    assert automaton["entry_count"] == 1
    assert automaton["state_count"] >= 2


def test_runtime_matcher_loads_and_finds_case_insensitive_boundary_matches(
    tmp_path: Path,
) -> None:
    aliases_path = tmp_path / "aliases.json"
    aliases_path.write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "text": "Org Beta",
                        "label": "organization",
                        "canonical_text": "Org Beta",
                        "entity_id": "org:beta",
                        "source_domain": "finance",
                    },
                    {
                        "text": "TICKA",
                        "label": "ticker",
                        "canonical_text": "TICKA",
                        "entity_id": "ticker:ticka",
                        "source_domain": "finance",
                    },
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_matcher_artifact_from_aliases_json(
        aliases_path,
        output_dir=tmp_path / "matcher",
    )
    matcher = load_runtime_matcher(result.artifact_path, result.entries_path)

    spans = find_exact_match_spans(
        "org beta met TICKA but not TICKAX.",
        matcher,
    )

    assert spans == [(0, 8), (13, 18)]
