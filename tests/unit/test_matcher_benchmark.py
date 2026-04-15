import json
from pathlib import Path

from ades.matcher_benchmark import (
    MatcherBenchmarkCandidate,
    _recommend_backend,
    benchmark_matcher_backends,
    write_matcher_benchmark_spike_report,
)
from ades.packs.alias_analysis import analyze_alias_candidates, build_alias_candidate


def test_benchmark_matcher_backends_compares_candidates(tmp_path: Path) -> None:
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
                        "score": 0.92,
                        "source_priority": 0.8,
                    },
                    {
                        "text": "Jordan Vale",
                        "label": "person",
                        "canonical_text": "Jordan Vale",
                        "entity_id": "person:jordan-vale",
                        "score": 0.91,
                        "source_priority": 0.8,
                    },
                    {
                        "text": "the company",
                        "label": "organization",
                        "canonical_text": "The Company",
                        "entity_id": "org:company",
                        "score": 0.4,
                        "source_priority": 0.2,
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report = benchmark_matcher_backends(
        pack_id="general-en",
        profile="unit",
        aliases_path=aliases_path,
        query_texts=[
            "Org Beta met Jordan Vale.",
            "Jordan Vale returned to Org Beta.",
        ],
        output_dir=tmp_path / "benchmark",
        alias_limit=10,
        scan_limit=10,
        min_alias_score=0.8,
        exact_tier_min_token_count=2,
        query_runs=2,
    )

    assert report.pack_id == "general-en"
    assert report.scanned_alias_count == 3
    assert report.retained_alias_count == 2
    assert report.unique_pattern_count == 2
    assert report.query_count == 2
    assert report.query_runs == 2
    assert {candidate.backend for candidate in report.candidates} == {
        "current_json_aho",
        "token_trie_v1",
    }
    assert all(candidate.artifact_bytes > 0 for candidate in report.candidates)
    assert report.recommended_backend in {"current_json_aho", "token_trie_v1"}


def test_write_matcher_benchmark_spike_report_round_trip_shape(tmp_path: Path) -> None:
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
                        "score": 0.92,
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    report = benchmark_matcher_backends(
        pack_id="general-en",
        profile="unit",
        aliases_path=aliases_path,
        query_texts=["Org Beta closed higher."],
        output_dir=tmp_path / "benchmark",
        alias_limit=10,
        scan_limit=10,
        min_alias_score=0.6,
        exact_tier_min_token_count=2,
        query_runs=1,
    )

    destination = write_matcher_benchmark_spike_report(
        tmp_path / "reports" / "matcher-backends.json",
        report,
    )
    payload = json.loads(destination.read_text(encoding="utf-8"))

    assert payload["pack_id"] == "general-en"
    assert payload["profile"] == "unit"
    assert payload["recommended_backend"] in {"current_json_aho", "token_trie_v1"}
    assert len(payload["candidates"]) == 2


def test_benchmark_matcher_backends_prefers_analysis_db_when_available(
    tmp_path: Path,
) -> None:
    aliases_path = tmp_path / "aliases.json"
    aliases_path.write_text("not valid json", encoding="utf-8")
    analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="org beta",
                display_text="Org Beta",
                label="organization",
                canonical_text="Org Beta",
                record={
                    "entity_id": "org:beta",
                    "source_name": "synthetic-general",
                    "source_priority": 0.8,
                },
            ),
            build_alias_candidate(
                alias_key="jordan vale",
                display_text="Jordan Vale",
                label="person",
                canonical_text="Jordan Vale",
                record={
                    "entity_id": "person:jordan-vale",
                    "source_name": "synthetic-general",
                    "source_priority": 0.8,
                },
            ),
        ],
        allowed_ambiguous_aliases=set(),
        analysis_db_path=tmp_path / "analysis.sqlite",
        materialize_retained_aliases=False,
    )

    report = benchmark_matcher_backends(
        pack_id="general-en",
        profile="unit",
        aliases_path=aliases_path,
        query_texts=["Org Beta met Jordan Vale."],
        output_dir=tmp_path / "benchmark",
        alias_limit=10,
        scan_limit=10,
        min_alias_score=0.6,
        exact_tier_min_token_count=2,
        query_runs=1,
    )

    assert report.scanned_alias_count == 2
    assert report.retained_alias_count == 2
    assert {candidate.backend for candidate in report.candidates} == {
        "current_json_aho",
        "token_trie_v1",
    }


def test_recommend_backend_treats_sub_ms_query_noise_as_tie() -> None:
    recommended = _recommend_backend(
        [
            MatcherBenchmarkCandidate(
                backend="current_json_aho",
                available=True,
                alias_count=50000,
                unique_pattern_count=50000,
                build_ms=4634,
                cold_load_ms=2363,
                warm_query_p50_ms=0,
                warm_query_p95_ms=0,
                artifact_bytes=54925571,
                state_count=680754,
            ),
            MatcherBenchmarkCandidate(
                backend="token_trie_v1",
                available=True,
                alias_count=50000,
                unique_pattern_count=50000,
                build_ms=278,
                cold_load_ms=59,
                warm_query_p50_ms=0,
                warm_query_p95_ms=1,
                artifact_bytes=1923998,
                state_count=110123,
                token_vocab_count=44025,
            ),
        ]
    )

    assert recommended == "token_trie_v1"
