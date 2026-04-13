from pathlib import Path

from ades.packs.alias_analysis import (
    analyze_alias_candidates,
    build_alias_candidate,
    iter_retained_aliases_from_db,
)


def test_analyze_alias_candidates_persists_file_backed_retained_aliases(
    tmp_path: Path,
) -> None:
    analysis_db_path = tmp_path / "analysis.sqlite"
    candidates = [
        build_alias_candidate(
            alias_key="jordan vale",
            display_text="Jordan Vale",
            label="person",
            canonical_text="Jordan Vale",
            record={
                "entity_id": "synthetic:person:jordan-vale",
                "source_name": "synthetic-general",
                "source_priority": 0.8,
            },
            generated=True,
        ),
        build_alias_candidate(
            alias_key="north harbor",
            display_text="North Harbor",
            label="location",
            canonical_text="North Harbor",
            record={
                "entity_id": "synthetic:location:north-harbor",
                "source_name": "synthetic-general",
                "population": 250000,
            },
        ),
    ]

    result = analyze_alias_candidates(
        candidates,
        allowed_ambiguous_aliases=set(),
        analysis_db_path=analysis_db_path,
        materialize_retained_aliases=False,
    )

    assert result.database_path == str(analysis_db_path)
    assert result.retained_aliases_materialized is False
    assert analysis_db_path.exists()
    assert not analysis_db_path.with_name(f"{analysis_db_path.name}-journal").exists()

    retained = list(iter_retained_aliases_from_db(analysis_db_path))
    assert ("Jordan Vale", "person") in {
        (item["text"], item["label"]) for item in retained
    }
    assert ("North Harbor", "location") in {
        (item["text"], item["label"]) for item in retained
    }


def test_alias_analysis_blocks_low_information_single_label_alias() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="and",
                display_text="AND",
                label="organization",
                canonical_text="Geo Example",
                record={
                    "entity_id": "wikidata:Q1",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 5,
                },
                generated=True,
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 0
    assert result.blocked_reason_counts["low_information_single_label_alias"] == 1


def test_alias_analysis_keeps_dominant_location_alias() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="new york city",
                display_text="New York City",
                label="location",
                canonical_text="New York City",
                record={
                    "entity_id": "wikidata:Q60",
                    "source_name": "geonames-general-places",
                    "source_priority": 0.82,
                    "population": 8_000_000,
                },
            ),
            build_alias_candidate(
                alias_key="new york city",
                display_text="New York City",
                label="organization",
                canonical_text="New York City",
                record={
                    "entity_id": "wikidata:Q999",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity_weight": 0.8,
                },
            ),
        ],
        allowed_ambiguous_aliases=set(),
    )

    retained_pairs = {(item["text"], item["label"]) for item in result.retained_aliases}
    assert ("New York City", "location") in retained_pairs
    assert result.blocked_reason_counts["dominant_location_alias"] == 1
