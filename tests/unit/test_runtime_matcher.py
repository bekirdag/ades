import json
from pathlib import Path

from ades.runtime_matcher import (
    MATCHER_JSON_AHO_ALGORITHM,
    MatcherEntryPayload,
    build_matcher_artifact_from_aliases_json,
    find_exact_match_candidates,
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

    assert first.algorithm == "token_trie_v1"
    assert first.normalization == "normalize_lookup_text:v1"
    assert first.entry_count == 2
    assert first.state_count >= 3
    assert first.max_alias_length == len("org beta")
    assert Path(first.artifact_path).name == "token-trie.bin"
    assert Path(first.entries_path).name == "entries.bin"
    assert Path(first.entries_path).with_suffix(".idx").exists()
    assert Path(first.artifact_path).with_suffix(".stateidx").exists()
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

    matcher = load_runtime_matcher(result.artifact_path, result.entries_path)

    assert Path(result.entries_path).name == "entries.bin"
    assert Path(result.entries_path).with_suffix(".idx").exists()
    assert Path(result.artifact_path).with_suffix(".stateidx").exists()
    assert matcher.algorithm == "token_trie_v1"
    assert matcher.entry_count == 1
    assert matcher.state_count >= 2
    assert len(matcher.token_to_id) == 2
    assert tuple(matcher.entry_payloads) == (
        MatcherEntryPayload(
            text="Jordan Vale",
            label="person",
            canonical_text="Jordan Elliott Vale",
            entity_id="person:jordan-vale",
            score=1.0,
            alias_score=1.0,
            generated=False,
            source_name="",
            source_priority=0.6,
            popularity_weight=0.5,
            source_domain="general",
        ),
    )
    assert matcher.entry_payloads[0] == MatcherEntryPayload(
        text="Jordan Vale",
        label="person",
        canonical_text="Jordan Elliott Vale",
        entity_id="person:jordan-vale",
        score=1.0,
        alias_score=1.0,
        generated=False,
        source_name="",
        source_priority=0.6,
        popularity_weight=0.5,
        source_domain="general",
    )


def test_runtime_matcher_rebuilds_missing_token_trie_index(tmp_path: Path) -> None:
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
    index_path = Path(result.entries_path).with_suffix(".idx")
    assert index_path.exists()
    index_path.unlink()

    matcher = load_runtime_matcher(result.artifact_path, result.entries_path)

    assert index_path.exists()
    assert matcher.entry_payloads[0].text == "Jordan Vale"


def test_runtime_matcher_rebuilds_missing_token_trie_state_index(tmp_path: Path) -> None:
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
    state_index_path = Path(result.artifact_path).with_suffix(".stateidx")
    assert state_index_path.exists()
    state_index_path.unlink()

    matcher = load_runtime_matcher(result.artifact_path, result.entries_path)

    assert state_index_path.exists()
    assert find_exact_match_spans("Jordan Vale spoke.", matcher) == [(0, 11)]


def test_matcher_artifact_generation_filters_search_only_runtime_tiers(tmp_path: Path) -> None:
    aliases_path = tmp_path / "aliases.json"
    aliases_path.write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "text": "Jordan Vale",
                        "label": "person",
                        "canonical_text": "Jordan Vale",
                        "entity_id": "person:jordan-vale",
                        "source_domain": "general",
                        "runtime_tier": "runtime_exact_high_precision",
                    },
                    {
                        "text": "The Company",
                        "label": "organization",
                        "canonical_text": "The Company",
                        "entity_id": "organization:the-company",
                        "source_domain": "general",
                        "runtime_tier": "search_only",
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
        include_runtime_tiers={"runtime_exact_high_precision"},
    )
    matcher = load_runtime_matcher(result.artifact_path, result.entries_path)

    assert matcher.entry_count == 1
    assert find_exact_match_spans("Jordan Vale spoke for The Company.", matcher) == [(0, 11)]


def test_matcher_artifact_generation_includes_additional_exact_runtime_tiers(
    tmp_path: Path,
) -> None:
    aliases_path = tmp_path / "aliases.json"
    aliases_path.write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "text": "AQL",
                        "label": "organization",
                        "canonical_text": "AQL",
                        "entity_id": "organization:aql",
                        "source_domain": "general",
                        "runtime_tier": "runtime_exact_acronym_high_precision",
                    },
                    {
                        "text": "Luma",
                        "label": "location",
                        "canonical_text": "Luma",
                        "entity_id": "location:luma",
                        "source_domain": "general",
                        "runtime_tier": "runtime_exact_geopolitical_high_precision",
                    },
                    {
                        "text": "The Company",
                        "label": "organization",
                        "canonical_text": "The Company",
                        "entity_id": "organization:the-company",
                        "source_domain": "general",
                        "runtime_tier": "search_only",
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
        include_runtime_tiers={
            "runtime_exact_acronym_high_precision",
            "runtime_exact_geopolitical_high_precision",
            "runtime_exact_high_precision",
        },
    )
    matcher = load_runtime_matcher(result.artifact_path, result.entries_path)

    assert matcher.entry_count == 2
    assert find_exact_match_spans("AQL moved to Luma for The Company.", matcher) == [
        (0, 3),
        (13, 17),
    ]


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
    candidates = find_exact_match_candidates(
        "org beta met TICKA but not TICKAX.",
        matcher,
    )

    assert spans == [(0, 8), (13, 18)]
    assert [(candidate.start, candidate.end) for candidate in candidates] == spans
    assert [candidate.entry_indices for candidate in candidates] == [(0,), (1,)]


def test_runtime_matcher_still_loads_legacy_json_aho_artifacts(tmp_path: Path) -> None:
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
        algorithm=MATCHER_JSON_AHO_ALGORITHM,
    )
    matcher = load_runtime_matcher(result.artifact_path, result.entries_path)

    assert matcher.algorithm == MATCHER_JSON_AHO_ALGORITHM
    assert Path(result.artifact_path).name == "automaton.json"
    assert find_exact_match_spans("Org Beta closed higher.", matcher) == [(0, 8)]
