from pathlib import Path

from ades.packs.alias_analysis import (
    analyze_alias_candidates,
    analyze_alias_candidates_from_db,
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
    assert all(item["runtime_tier"] == "runtime_exact_high_precision" for item in retained)


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


def test_analyze_alias_candidates_from_db_reuses_candidate_store(
    tmp_path: Path,
) -> None:
    source_db_path = tmp_path / "source.sqlite"
    refreshed_db_path = tmp_path / "refreshed.sqlite"
    source_result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="aql",
                display_text="AQL",
                label="organization",
                canonical_text="AQL",
                record={
                    "entity_id": "synthetic:organization:aql",
                    "source_name": "curated-general",
                    "source_priority": 0.95,
                    "preferred_name_weight": 1.0,
                    "popularity_weight": 0.9,
                },
            ),
            build_alias_candidate(
                alias_key="north harbor",
                display_text="North Harbor",
                label="location",
                canonical_text="North Harbor",
                record={
                    "entity_id": "synthetic:location:north-harbor",
                    "source_name": "geonames-general-places",
                    "source_priority": 0.82,
                    "population": 250000,
                },
            ),
        ],
        allowed_ambiguous_aliases=set(),
        analysis_db_path=source_db_path,
        materialize_retained_aliases=False,
    )

    refreshed_result = analyze_alias_candidates_from_db(
        source_db_path,
        allowed_ambiguous_aliases=set(),
        analysis_db_path=refreshed_db_path,
        materialize_retained_aliases=False,
    )

    assert source_result.candidate_alias_count == refreshed_result.candidate_alias_count
    assert source_result.retained_alias_count == refreshed_result.retained_alias_count
    assert list(iter_retained_aliases_from_db(source_db_path)) == list(
        iter_retained_aliases_from_db(refreshed_db_path)
    )


def test_alias_analysis_blocks_single_token_person_aliases() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="jory",
                display_text="Jory",
                label="person",
                canonical_text="Jory",
                record={
                    "entity_id": "synthetic:person:jory",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 1.0,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 0
    assert result.blocked_reason_counts["low_information_single_label_alias"] == 1


def test_alias_analysis_blocks_low_support_single_token_location_aliases() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="fernridge",
                display_text="Fernridge",
                label="location",
                canonical_text="Fernridge",
                record={
                    "entity_id": "synthetic:location:fernridge",
                    "source_name": "geonames-places",
                    "source_priority": 0.82,
                    "population": 1,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 0
    assert result.blocked_reason_counts["low_information_single_label_alias"] == 1


def test_alias_analysis_blocks_longer_noncanonical_single_token_org_fragments() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="capital",
                display_text="Capital",
                label="organization",
                canonical_text="Capital Group",
                record={
                    "entity_id": "synthetic:organization:capital-group",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 1.0,
                },
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


def test_alias_analysis_promotes_supported_location_over_generated_competitors() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="harborview",
                display_text="Harborview",
                label="location",
                canonical_text="Harborview",
                record={
                    "entity_id": "synthetic:location:harborview-primary",
                    "source_name": "geonames-general-places",
                    "source_priority": 0.82,
                    "population": 8_800_000,
                },
            ),
            build_alias_candidate(
                alias_key="harborview",
                display_text="Harborview",
                label="location",
                canonical_text="Harborview",
                record={
                    "entity_id": "synthetic:location:harborview-secondary",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "population": 8_900_000,
                },
            ),
            build_alias_candidate(
                alias_key="harborview",
                display_text="Harborview",
                label="organization",
                canonical_text="Harborview Capital",
                record={
                    "entity_id": "synthetic:organization:harborview-capital",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.9,
                },
                generated=True,
            ),
            build_alias_candidate(
                alias_key="harborview",
                display_text="Harborview",
                label="organization",
                canonical_text="Harborview Records",
                record={
                    "entity_id": "synthetic:organization:harborview-records",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.86,
                },
                generated=True,
            ),
        ],
        allowed_ambiguous_aliases=set(),
    )

    retained_pairs = {(item["text"], item["label"]) for item in result.retained_aliases}
    assert ("Harborview", "location") in retained_pairs
    assert ("Harborview", "organization") not in retained_pairs
    assert result.blocked_reason_counts["supported_location_over_generated_competitors"] == 1


def test_alias_analysis_demotes_article_led_org_phrase_to_search_only() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="the company",
                display_text="The Company",
                label="organization",
                canonical_text="The Company",
                record={
                    "entity_id": "synthetic:organization:the-company",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.95,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["runtime_tier"] == "search_only"


def test_alias_analysis_demotes_article_led_person_phrase_to_search_only() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="the founder",
                display_text="The Founder",
                label="person",
                canonical_text="The Founder",
                record={
                    "entity_id": "synthetic:person:the-founder",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.95,
                },
                generated=True,
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["runtime_tier"] == "search_only"


def test_alias_analysis_demotes_auxiliary_led_org_phrase_to_search_only() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="we are",
                display_text="We Are",
                label="organization",
                canonical_text="We Are",
                record={
                    "entity_id": "synthetic:organization:we-are",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.95,
                },
                generated=True,
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["runtime_tier"] == "search_only"


def test_alias_analysis_demotes_generic_low_support_location_phrase_to_search_only() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="southern city",
                display_text="Southern City",
                label="location",
                canonical_text="Southern City",
                record={
                    "entity_id": "synthetic:location:southern-city",
                    "source_name": "geonames-places",
                    "source_priority": 0.82,
                    "population": 20,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["runtime_tier"] == "search_only"


def test_alias_analysis_blocks_generic_single_token_location_head() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="university",
                display_text="University",
                label="location",
                canonical_text="University",
                record={
                    "entity_id": "synthetic:location:university",
                    "source_name": "geonames-places",
                    "source_priority": 0.82,
                    "population": 5_000_000,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 0
    assert result.blocked_reason_counts["low_information_single_label_alias"] == 1


def test_alias_analysis_blocks_demonstrative_single_token_location_alias() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="this",
                display_text="This",
                label="location",
                canonical_text="This",
                record={
                    "entity_id": "synthetic:location:this",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.95,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 0
    assert result.blocked_reason_counts["low_information_single_label_alias"] == 1


def test_alias_analysis_blocks_calendar_single_token_location_alias() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="march",
                display_text="March",
                label="location",
                canonical_text="March",
                record={
                    "entity_id": "synthetic:location:march",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.95,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 0
    assert result.blocked_reason_counts["low_information_single_label_alias"] == 1


def test_alias_analysis_blocks_generic_stock_single_token_alias() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="stock",
                display_text="Stock",
                label="location",
                canonical_text="Stock",
                record={
                    "entity_id": "synthetic:location:stock",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.95,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 0
    assert result.blocked_reason_counts["low_information_single_label_alias"] == 1


def test_alias_analysis_keeps_strong_exact_single_token_org_in_runtime_tier() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="meridia",
                display_text="Meridia",
                label="organization",
                canonical_text="Meridia",
                record={
                    "entity_id": "synthetic:organization:meridia",
                    "source_name": "curated-general",
                    "source_priority": 0.9,
                    "popularity": 0.88,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["runtime_tier"] == "runtime_exact_high_precision"


def test_alias_analysis_keeps_strong_curated_single_token_org_fragment() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="beacon",
                display_text="Beacon",
                label="organization",
                canonical_text="Beacon Group",
                record={
                    "entity_id": "synthetic:organization:beacon-group",
                    "source_name": "curated-general",
                    "source_priority": 0.9,
                    "popularity": 0.95,
                },
                generated=True,
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["runtime_tier"] == "runtime_exact_high_precision"


def test_alias_analysis_keeps_strong_all_caps_acronym_org_in_runtime_tier() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="aql",
                display_text="AQL",
                label="organization",
                canonical_text="AQL",
                record={
                    "entity_id": "synthetic:organization:aql",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 1.0,
                },
            ),
            build_alias_candidate(
                alias_key="aql",
                display_text="AQL",
                label="organization",
                canonical_text="Aquila Labs",
                record={
                    "entity_id": "synthetic:organization:aquila-labs",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.6,
                },
                generated=True,
            ),
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert (
        result.retained_aliases[0]["runtime_tier"]
        == "runtime_exact_acronym_high_precision"
    )


def test_alias_analysis_prefers_strong_same_label_acronym_expansion() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="gbc",
                display_text="GBC",
                label="organization",
                canonical_text="Global Broadcast Corporation",
                record={
                    "entity_id": "synthetic:organization:global-broadcast-corporation",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                },
                generated=True,
            ),
            build_alias_candidate(
                alias_key="gbc",
                display_text="GBC",
                label="organization",
                canonical_text="Green Bay club",
                record={
                    "entity_id": "synthetic:organization:green-bay-club",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                },
                generated=True,
            ),
            build_alias_candidate(
                alias_key="gbc",
                display_text="GBC",
                label="organization",
                canonical_text="Granite State University",
                record={
                    "entity_id": "synthetic:organization:granite-state-university",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                },
                generated=True,
            ),
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["canonical_text"] == "Global Broadcast Corporation"
    assert (
        result.retained_aliases[0]["runtime_tier"]
        == "runtime_exact_acronym_high_precision"
    )


def test_alias_analysis_prefers_supported_acronym_expansion_over_weak_exact_short_org() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="iea",
                display_text="IEA",
                label="organization",
                canonical_text="IEA",
                record={
                    "entity_id": "synthetic:organization:iea-short",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.5,
                },
            ),
            build_alias_candidate(
                alias_key="iea",
                display_text="IEA",
                label="organization",
                canonical_text="International Energy Agency",
                record={
                    "entity_id": "synthetic:organization:international-energy-agency",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 1.0,
                },
                generated=True,
            ),
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert (
        result.retained_aliases[0]["canonical_text"]
        == "International Energy Agency"
    )
    assert (
        result.retained_aliases[0]["runtime_tier"]
        == "runtime_exact_acronym_high_precision"
    )


def test_alias_analysis_blocks_pronoun_like_caps_acronym_org() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="we",
                display_text="WE",
                label="organization",
                canonical_text="WE",
                record={
                    "entity_id": "synthetic:organization:we",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 1.0,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 0
    assert result.blocked_reason_counts["low_information_single_label_alias"] == 1


def test_alias_analysis_keeps_strong_short_location_in_geopolitical_runtime_tier() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="luma",
                display_text="Luma",
                label="location",
                canonical_text="Luma",
                record={
                    "entity_id": "synthetic:location:luma",
                    "source_name": "geonames-places",
                    "source_priority": 0.82,
                    "population": 5_500_000,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert (
        result.retained_aliases[0]["runtime_tier"]
        == "runtime_exact_geopolitical_high_precision"
    )


def test_alias_analysis_keeps_supported_short_location_over_cross_label_conflict() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="nara",
                display_text="Nara",
                label="location",
                canonical_text="Nara",
                record={
                    "entity_id": "synthetic:location:nara",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 1.0,
                },
            ),
            build_alias_candidate(
                alias_key="nara",
                display_text="Nara",
                label="organization",
                canonical_text="Nara",
                record={
                    "entity_id": "synthetic:organization:nara",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.5,
                },
            ),
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["label"] == "location"
    assert (
        result.retained_aliases[0]["runtime_tier"]
        == "runtime_exact_geopolitical_high_precision"
    )
    assert result.blocked_reason_counts["strong_short_geopolitical_alias"] == 1


def test_alias_analysis_keeps_strong_geopolitical_acronym_over_short_org_conflict() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="us",
                display_text="US",
                label="location",
                canonical_text="United States",
                record={
                    "entity_id": "synthetic:location:united-states",
                    "source_name": "geonames-places",
                    "source_priority": 0.82,
                    "popularity": 0.8,
                },
                generated=True,
            ),
            build_alias_candidate(
                alias_key="us",
                display_text="US",
                label="organization",
                canonical_text="US",
                record={
                    "entity_id": "synthetic:organization:us",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 1.0,
                },
            ),
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["label"] == "location"
    assert (
        result.retained_aliases[0]["runtime_tier"]
        == "runtime_exact_geopolitical_high_precision"
    )
    assert result.blocked_reason_counts["strong_short_geopolitical_alias"] == 1


def test_alias_analysis_prefers_strong_location_family_over_org_conflict() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="europe",
                display_text="Europe",
                label="location",
                canonical_text="Europe",
                record={
                    "entity_id": "synthetic:location:europe",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "population": 740_000_000,
                },
            ),
            build_alias_candidate(
                alias_key="europe",
                display_text="Europe",
                label="organization",
                canonical_text="Europe",
                record={
                    "entity_id": "synthetic:organization:europe",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.82,
                },
            ),
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["label"] == "location"
    assert result.blocked_reason_counts["family_preferred_location_alias"] == 1
