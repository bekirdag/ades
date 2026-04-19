from pathlib import Path
import sqlite3

from ades.packs.alias_analysis import (
    AliasAnalysisResult,
    apply_general_exact_alias_exclusions,
    analyze_alias_candidates,
    analyze_alias_candidates_from_db,
    audit_general_retained_aliases,
    build_alias_candidate,
    classify_exact_common_english_wordlist_alias,
    export_general_common_english_wordlist,
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


def test_iter_retained_aliases_from_db_supports_legacy_schema_without_runtime_tier(
    tmp_path: Path,
) -> None:
    analysis_db_path = tmp_path / "legacy-analysis.sqlite"
    connection = sqlite3.connect(str(analysis_db_path))
    try:
        connection.execute(
            """
            CREATE TABLE retained_aliases (
                display_text TEXT NOT NULL,
                label TEXT NOT NULL,
                display_sort_key TEXT NOT NULL,
                label_sort_key TEXT NOT NULL,
                generated INTEGER NOT NULL,
                score REAL NOT NULL,
                canonical_text TEXT NOT NULL,
                source_name TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                source_priority REAL NOT NULL,
                popularity_weight REAL NOT NULL
            )
            """
        )
        connection.execute(
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
            (
                "Four Ways",
                "location",
                "four ways",
                "location",
                0,
                0.91,
                "Four Ways",
                "legacy-general",
                "location:four-ways",
                0.9,
                0.95,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    retained = list(iter_retained_aliases_from_db(analysis_db_path))

    assert retained == [
        {
            "text": "Four Ways",
            "label": "location",
            "generated": False,
            "score": 0.91,
            "canonical_text": "Four Ways",
            "source_name": "legacy-general",
            "entity_id": "location:four-ways",
            "source_priority": 0.9,
            "popularity_weight": 0.95,
            "runtime_tier": "runtime_exact_high_precision",
        }
    ]


def test_audit_general_retained_aliases_removes_generic_words_and_phrases() -> None:
    result = AliasAnalysisResult(
        backend="sqlite",
        database_path=None,
        candidate_alias_count=3,
        retained_alias_count=3,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=0,
        natural_language_alias_count=3,
        retained_label_counts={"location": 2, "organization": 1},
        retained_aliases_materialized=True,
        retained_aliases=[
            {
                "text": "Cars",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Cars",
                "source_name": "curated-general",
                "entity_id": "organization:cars",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Four Ways",
                "label": "location",
                "generated": False,
                "score": 0.91,
                "canonical_text": "Four Ways",
                "source_name": "curated-general",
                "entity_id": "location:four-ways",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "North Harbor",
                "label": "location",
                "generated": False,
                "score": 0.93,
                "canonical_text": "North Harbor",
                "source_name": "curated-general",
                "entity_id": "location:north-harbor",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
        ],
    )

    result = audit_general_retained_aliases(result, chunk_size=2)

    assert {(item["text"], item["label"]) for item in result.retained_aliases} == {
        ("North Harbor", "location")
    }
    assert result.retained_alias_count == 1
    assert result.retained_label_counts == {"location": 1}
    assert result.retained_alias_audit_scanned_alias_count == 3
    assert result.retained_alias_audit_removed_alias_count == 2
    assert result.retained_alias_audit_chunk_size == 2
    assert result.retained_alias_audit_reason_counts == {
        "generic_retained_phrase_alias": 1,
        "generic_retained_single_token_alias": 1,
    }


def test_audit_general_retained_aliases_deterministic_common_english() -> None:
    result = AliasAnalysisResult(
        backend="sqlite",
        database_path=None,
        candidate_alias_count=5,
        retained_alias_count=5,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=0,
        natural_language_alias_count=5,
        retained_label_counts={"location": 3, "organization": 2},
        retained_aliases_materialized=True,
        retained_aliases=[
            {
                "text": "Cars",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Cars",
                "source_name": "curated-general",
                "entity_id": "organization:cars",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "March",
                "label": "location",
                "generated": False,
                "score": 0.91,
                "canonical_text": "March",
                "source_name": "curated-general",
                "entity_id": "location:march",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "North Harbor",
                "label": "location",
                "generated": False,
                "score": 0.93,
                "canonical_text": "North Harbor",
                "source_name": "curated-general",
                "entity_id": "location:north-harbor",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "1+1 Media Group",
                "label": "organization",
                "generated": False,
                "score": 0.93,
                "canonical_text": "1+1 Media Group",
                "source_name": "curated-general",
                "entity_id": "organization:1plus1-media-group",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Beacon Group",
                "label": "organization",
                "generated": False,
                "score": 0.93,
                "canonical_text": "Beacon Group",
                "source_name": "curated-general",
                "entity_id": "organization:beacon-group",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
        ],
    )

    result = audit_general_retained_aliases(
        result,
        chunk_size=5,
        review_mode="deterministic_common_english",
    )

    assert {(item["text"], item["label"]) for item in result.retained_aliases} == {
        ("1+1 Media Group", "organization"),
        ("Beacon Group", "organization"),
        ("North Harbor", "location"),
    }
    assert result.retained_alias_count == 3
    assert result.retained_label_counts == {"location": 1, "organization": 2}
    assert result.retained_alias_audit_mode == "deterministic_common_english"
    assert result.retained_alias_audit_reason_counts == {
        "generic_retained_single_token_alias": 2,
    }


def test_classify_exact_common_english_wordlist_alias() -> None:
    common_words = {"exclusive", "new", "sign"}
    explicit_words = {"ai", "ct", "linda", "many", "not", "queen", "uk"}

    assert (
        classify_exact_common_english_wordlist_alias(
            text="Exclusive",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "common_english_word_list_match"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="NEW",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "common_english_word_list_match"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="AI",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "explicit_generic_alias_override"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="CT",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "explicit_generic_alias_override"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="UK",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "explicit_generic_alias_override"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="Linda",
            label="location",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "explicit_generic_alias_override"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="NOT",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "explicit_generic_alias_override"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="Five",
            label="location",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "common_english_word_list_match"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="Europe",
            label="location",
            common_words={"europe"},
            explicit_words=explicit_words,
        )
        is None
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="5 min",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "duration_like_alias_match"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="15 Minutes",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "duration_like_alias_match"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="Central Command",
            label="location",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "explicit_generic_alias_override"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="New Deal",
            label="location",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "explicit_generic_alias_override"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="U-turn",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "explicit_generic_alias_override"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="The Art",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "explicit_generic_alias_override"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="O'Connor &",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "trailing_connector_alias_match"
    )
    assert (
        classify_exact_common_english_wordlist_alias(
            text="and 2016",
            label="organization",
            common_words=common_words,
            explicit_words=explicit_words,
        )
        == "function_year_alias_match"
    )


def test_apply_general_exact_alias_exclusions_drops_duration_aliases(
    tmp_path: Path,
) -> None:
    word_list_path = tmp_path / "common-english.txt"
    word_list_path.write_text("exclusive\nnew\nsign\n", encoding="utf-8")
    result = AliasAnalysisResult(
        backend="sqlite",
        database_path=None,
        candidate_alias_count=2,
        retained_alias_count=2,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=0,
        natural_language_alias_count=2,
        retained_label_counts={"organization": 2},
        retained_aliases_materialized=True,
        retained_aliases=[
            {
                "text": "5 min",
                "label": "organization",
                "canonical_text": "5 min",
                "entity_id": "org:five-min",
                "source_name": "synthetic",
                "generated": False,
                "score": 0.8,
                "source_priority": 0.8,
                "popularity_weight": 0.7,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "World Bank",
                "label": "organization",
                "canonical_text": "World Bank",
                "entity_id": "org:world-bank",
                "source_name": "synthetic",
                "generated": False,
                "score": 0.9,
                "source_priority": 0.9,
                "popularity_weight": 0.8,
                "runtime_tier": "runtime_exact_high_precision",
            },
        ],
    )

    filtered = apply_general_exact_alias_exclusions(
        result,
        common_word_list_path=word_list_path,
    )

    assert [(item["text"], item["label"]) for item in filtered.retained_aliases] == [
        ("World Bank", "organization")
    ]
    assert filtered.retained_alias_count == 1
    assert filtered.exact_common_english_alias_removed_alias_count == 1


def test_export_general_common_english_wordlist(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from ades.packs import alias_analysis as module

    module._wordfreq_top_n_words_en.cache_clear()
    monkeypatch.setattr(
        module,
        "_wordfreq_top_n_list",
        lambda lang, n: ["the", "and", "new"][:n],
    )

    output_path = tmp_path / "wordfreq-en-top3.txt"
    export_general_common_english_wordlist(output_path, top_n=3)

    assert output_path.read_text(encoding="utf-8").splitlines() == ["the", "and", "new"]
    module._wordfreq_top_n_words_en.cache_clear()


def test_apply_general_exact_alias_exclusions_materialized(tmp_path: Path) -> None:
    word_list_path = tmp_path / "wordfreq-en-top5.txt"
    word_list_path.write_text("exclusive\nnew\ncare\n", encoding="utf-8")
    result = AliasAnalysisResult(
        backend="sqlite",
        database_path=None,
        candidate_alias_count=5,
        retained_alias_count=5,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=0,
        natural_language_alias_count=5,
        retained_label_counts={"location": 1, "organization": 4},
        retained_aliases_materialized=True,
        retained_aliases=[
            {
                "text": "Exclusive",
                "label": "organization",
                "canonical_text": "Exclusive",
                "entity_id": "org:exclusive",
                "source_name": "synthetic",
                "generated": False,
                "score": 0.8,
                "source_priority": 0.8,
                "popularity_weight": 0.5,
                "runtime_tier": "search_only",
            },
            {
                "text": "NEW",
                "label": "organization",
                "canonical_text": "Next Entertainment World",
                "entity_id": "org:new",
                "source_name": "synthetic",
                "generated": True,
                "score": 0.8,
                "source_priority": 0.8,
                "popularity_weight": 0.5,
                "runtime_tier": "runtime_exact_acronym_high_precision",
            },
            {
                "text": "AI",
                "label": "organization",
                "canonical_text": "Applied Imaging",
                "entity_id": "org:ai",
                "source_name": "synthetic",
                "generated": True,
                "score": 0.8,
                "source_priority": 0.8,
                "popularity_weight": 0.5,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "World Bank",
                "label": "organization",
                "canonical_text": "World Bank",
                "entity_id": "org:world-bank",
                "source_name": "synthetic",
                "generated": False,
                "score": 0.9,
                "source_priority": 0.9,
                "popularity_weight": 0.7,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Chicago",
                "label": "location",
                "canonical_text": "Chicago",
                "entity_id": "loc:chicago",
                "source_name": "synthetic",
                "generated": False,
                "score": 0.9,
                "source_priority": 0.9,
                "popularity_weight": 0.7,
                "runtime_tier": "runtime_exact_high_precision",
            },
        ],
    )

    filtered = apply_general_exact_alias_exclusions(
        result,
        common_word_list_path=word_list_path,
    )

    assert {(item["text"], item["label"]) for item in filtered.retained_aliases} == {
        ("World Bank", "organization"),
        ("Chicago", "location"),
    }
    assert filtered.retained_alias_count == 2
    assert filtered.exact_common_english_alias_removed_alias_count == 3
    assert filtered.exact_common_english_alias_source_path == str(
        word_list_path.resolve()
    )


def test_audit_general_retained_aliases_generic_phrase_rule() -> None:
    result = AliasAnalysisResult(
        backend="sqlite",
        database_path=None,
        candidate_alias_count=3,
        retained_alias_count=3,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=0,
        natural_language_alias_count=3,
        retained_label_counts={"location": 2, "organization": 1},
        retained_aliases_materialized=True,
        retained_aliases=[
            {
                "text": "Four Ways",
                "label": "location",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Four Ways",
                "source_name": "curated-general",
                "entity_id": "location:four-ways",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "North Harbor",
                "label": "location",
                "generated": False,
                "score": 0.93,
                "canonical_text": "North Harbor",
                "source_name": "curated-general",
                "entity_id": "location:north-harbor",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "1+1 Media Group",
                "label": "organization",
                "generated": False,
                "score": 0.93,
                "canonical_text": "1+1 Media Group",
                "source_name": "curated-general",
                "entity_id": "organization:1plus1-media-group",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
        ],
    )

    result = audit_general_retained_aliases(
        result,
        chunk_size=3,
        review_mode="deterministic_common_english",
    )

    assert {(item["text"], item["label"]) for item in result.retained_aliases} == {
        ("1+1 Media Group", "organization"),
        ("North Harbor", "location"),
    }
    assert result.retained_alias_audit_reason_counts == {
        "generic_retained_phrase_alias": 1,
    }


def test_audit_general_retained_aliases_keeps_dotted_initial_aliases() -> None:
    result = AliasAnalysisResult(
        backend="sqlite",
        database_path=None,
        candidate_alias_count=4,
        retained_alias_count=4,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=0,
        natural_language_alias_count=4,
        retained_label_counts={"organization": 2, "location": 2},
        retained_aliases_materialized=True,
        retained_aliases=[
            {
                "text": "A. T. Cross",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "A. T. Cross",
                "source_name": "curated-general",
                "entity_id": "organization:at-cross",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "A. V. Club",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "A. V. Club",
                "source_name": "curated-general",
                "entity_id": "organization:av-club",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "A. Point",
                "label": "location",
                "generated": False,
                "score": 0.94,
                "canonical_text": "A. Point",
                "source_name": "curated-general",
                "entity_id": "location:a-point",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Four Ways",
                "label": "location",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Four Ways",
                "source_name": "curated-general",
                "entity_id": "location:four-ways",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
        ],
    )

    result = audit_general_retained_aliases(
        result,
        chunk_size=4,
        review_mode="deterministic_common_english",
    )

    assert {(item["text"], item["label"]) for item in result.retained_aliases} == {
        ("A. Point", "location"),
        ("A. T. Cross", "organization"),
        ("A. V. Club", "organization"),
    }
    assert result.retained_alias_audit_reason_counts == {
        "generic_retained_phrase_alias": 1,
    }


def test_audit_general_retained_aliases_keeps_article_led_and_parenthetical_aliases() -> None:
    result = AliasAnalysisResult(
        backend="sqlite",
        database_path=None,
        candidate_alias_count=4,
        retained_alias_count=4,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=0,
        natural_language_alias_count=4,
        retained_label_counts={"organization": 2, "person": 1, "location": 1},
        retained_aliases_materialized=True,
        retained_aliases=[
            {
                "text": "The Truth",
                "label": "person",
                "generated": False,
                "score": 0.94,
                "canonical_text": "The Truth",
                "source_name": "curated-general",
                "entity_id": "person:the-truth",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "The American University",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "The American University",
                "source_name": "curated-general",
                "entity_id": "organization:the-american-university",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Can (band)",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Can (band)",
                "source_name": "curated-general",
                "entity_id": "organization:can-band",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "A Day in the Life",
                "label": "location",
                "generated": False,
                "score": 0.94,
                "canonical_text": "A Day in the Life",
                "source_name": "curated-general",
                "entity_id": "location:a-day-in-the-life",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
        ],
    )

    result = audit_general_retained_aliases(
        result,
        chunk_size=4,
        review_mode="deterministic_common_english",
    )

    assert {(item["text"], item["label"]) for item in result.retained_aliases} == {
        ("A Day in the Life", "location"),
        ("Can (band)", "organization"),
        ("The American University", "organization"),
        ("The Truth", "person"),
    }
    assert result.retained_alias_audit_reason_counts == {}


def test_audit_general_retained_aliases_keeps_location_singletons_outside_static_blocklist() -> None:
    result = AliasAnalysisResult(
        backend="sqlite",
        database_path=None,
        candidate_alias_count=2,
        retained_alias_count=2,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=0,
        natural_language_alias_count=2,
        retained_label_counts={"location": 2},
        retained_aliases_materialized=True,
        retained_aliases=[
            {
                "text": "Europe",
                "label": "location",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Europe",
                "source_name": "curated-general",
                "entity_id": "location:europe",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Five",
                "label": "location",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Five",
                "source_name": "curated-general",
                "entity_id": "location:five",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
        ],
    )

    result = audit_general_retained_aliases(
        result,
        chunk_size=2,
        review_mode="deterministic_common_english",
    )

    assert {(item["text"], item["label"]) for item in result.retained_aliases} == {
        ("Europe", "location"),
    }
    assert result.retained_alias_audit_reason_counts == {
        "generic_retained_single_token_alias": 1,
    }


def test_audit_general_retained_aliases_keeps_non_ascii_alpha_aliases() -> None:
    result = AliasAnalysisResult(
        backend="sqlite",
        database_path=None,
        candidate_alias_count=2,
        retained_alias_count=2,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=0,
        natural_language_alias_count=2,
        retained_label_counts={"location": 1, "organization": 1},
        retained_aliases_materialized=True,
        retained_aliases=[
            {
                "text": "Āhim",
                "label": "location",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Āhim",
                "source_name": "curated-general",
                "entity_id": "location:ahim",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Cars",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Cars",
                "source_name": "curated-general",
                "entity_id": "organization:cars",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
        ],
    )

    result = audit_general_retained_aliases(
        result,
        chunk_size=2,
        review_mode="deterministic_common_english",
    )

    assert {(item["text"], item["label"]) for item in result.retained_aliases} == {
        ("Āhim", "location"),
    }
    assert result.retained_alias_audit_reason_counts == {
        "generic_retained_single_token_alias": 1,
    }


def test_audit_general_retained_aliases_keeps_branded_and_month_led_aliases() -> None:
    result = AliasAnalysisResult(
        backend="sqlite",
        database_path=None,
        candidate_alias_count=11,
        retained_alias_count=11,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=0,
        natural_language_alias_count=11,
        retained_label_counts={"location": 4, "organization": 7},
        retained_aliases_materialized=True,
        retained_aliases=[
            {
                "text": "World Bank",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "World Bank",
                "source_name": "curated-general",
                "entity_id": "organization:world-bank",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Capital One",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Capital One",
                "source_name": "curated-general",
                "entity_id": "organization:capital-one",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Twitter",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Twitter",
                "source_name": "curated-general",
                "entity_id": "organization:twitter",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Chicago",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Chicago",
                "source_name": "curated-general",
                "entity_id": "organization:chicago",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "April",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "April",
                "source_name": "curated-general",
                "entity_id": "organization:april",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "May City",
                "label": "location",
                "generated": False,
                "score": 0.94,
                "canonical_text": "May City",
                "source_name": "curated-general",
                "entity_id": "location:may-city",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "May Day",
                "label": "location",
                "generated": False,
                "score": 0.94,
                "canonical_text": "May Day",
                "source_name": "curated-general",
                "entity_id": "location:may-day",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Seven Eleven",
                "label": "location",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Seven Eleven",
                "source_name": "curated-general",
                "entity_id": "location:seven-eleven",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Four Ways",
                "label": "location",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Four Ways",
                "source_name": "curated-general",
                "entity_id": "location:four-ways",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Cars",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Cars",
                "source_name": "curated-general",
                "entity_id": "organization:cars",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "CEO",
                "label": "organization",
                "generated": False,
                "score": 0.94,
                "canonical_text": "Corporate Europe Observatory",
                "source_name": "wikidata-general-entities",
                "entity_id": "wikidata:Q2997673",
                "source_priority": 0.9,
                "popularity_weight": 0.95,
                "runtime_tier": "runtime_exact_high_precision",
            },
        ],
    )

    result = audit_general_retained_aliases(
        result,
        chunk_size=10,
        review_mode="deterministic_common_english",
    )

    assert {(item["text"], item["label"]) for item in result.retained_aliases} == {
        ("April", "organization"),
        ("Capital One", "organization"),
        ("Chicago", "organization"),
        ("May City", "location"),
        ("May Day", "location"),
        ("Seven Eleven", "location"),
        ("Twitter", "organization"),
        ("World Bank", "organization"),
    }
    assert result.retained_alias_audit_reason_counts == {
        "generic_retained_phrase_alias": 1,
        "generic_retained_single_token_alias": 2,
    }


def test_audit_general_retained_aliases_callable_reviewer_scans_each_alias_once(
    tmp_path: Path,
) -> None:
    result = AliasAnalysisResult(
        backend="sqlite",
        database_path=None,
        candidate_alias_count=3,
        retained_alias_count=3,
        blocked_alias_count=0,
        ambiguous_alias_count=0,
        exact_identifier_alias_count=0,
        natural_language_alias_count=3,
        retained_label_counts={"location": 2, "organization": 1},
        retained_aliases_materialized=True,
        retained_aliases=[
            {
                "text": "Cars",
                "label": "organization",
                "canonical_text": "Cars",
                "entity_id": "organization:cars",
                "source_name": "curated-general",
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "Four Ways",
                "label": "location",
                "canonical_text": "Four Ways",
                "entity_id": "location:four-ways",
                "source_name": "curated-general",
                "runtime_tier": "runtime_exact_high_precision",
            },
            {
                "text": "North Harbor",
                "label": "location",
                "canonical_text": "North Harbor",
                "entity_id": "location:north-harbor",
                "source_name": "curated-general",
                "runtime_tier": "runtime_exact_high_precision",
            },
        ],
    )
    review_cache_path = tmp_path / "retained-alias-review.sqlite"
    calls: list[str] = []
    fail_on_call = False

    def reviewer(alias: dict[str, object]) -> dict[str, str]:
        if fail_on_call:
            raise AssertionError(f"unexpected cache miss for {alias['text']}")
        calls.append(str(alias["text"]))
        if alias["text"] in {"Cars", "Four Ways"}:
            return {
                "decision": "drop",
                "reason_code": "generic_retained_alias_from_ai_review",
                "reason": "too generic for general-en runtime",
            }
        return {
            "decision": "keep",
            "reason_code": "specific_retained_alias",
            "reason": "specific enough to keep",
        }

    first = audit_general_retained_aliases(
        result,
        reviewer=reviewer,
        review_cache_path=review_cache_path,
    )

    assert calls == ["Cars", "Four Ways", "North Harbor"]
    assert {(item["text"], item["label"]) for item in first.retained_aliases} == {
        ("North Harbor", "location")
    }
    assert first.retained_alias_audit_mode == "callable"
    assert first.retained_alias_audit_invoked_review_count == 3
    assert first.retained_alias_audit_cached_review_count == 0
    assert review_cache_path.exists()

    fail_on_call = True

    second = audit_general_retained_aliases(
        result,
        reviewer=reviewer,
        review_cache_path=review_cache_path,
    )

    assert {(item["text"], item["label"]) for item in second.retained_aliases} == {
        ("North Harbor", "location")
    }
    assert second.retained_alias_audit_mode == "callable"
    assert second.retained_alias_audit_invoked_review_count == 0
    assert second.retained_alias_audit_cached_review_count == 3
    assert second.retained_alias_audit_review_cache_path == str(review_cache_path)


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


def test_alias_analysis_blocks_number_word_single_token_aliases() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="four",
                display_text="Four",
                label="location",
                canonical_text="Four",
                record={
                    "entity_id": "synthetic:location:four",
                    "source_name": "geonames-places",
                    "source_priority": 0.82,
                    "population": 9_900_000,
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


def test_alias_analysis_matches_acronym_expansions_with_connector_words() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="daera",
                display_text="DAERA",
                label="organization",
                canonical_text="DAERA",
                record={
                    "entity_id": "synthetic:organization:daera-short",
                    "source_name": "wikidata-general-entities",
                    "source_priority": 0.8,
                    "popularity": 0.4,
                },
            ),
            build_alias_candidate(
                alias_key="daera",
                display_text="DAERA",
                label="organization",
                canonical_text="Department of Agriculture, Environment and Rural Affairs",
                record={
                    "entity_id": "synthetic:organization:daera-expansion",
                    "source_name": "curated-general",
                    "source_priority": 0.9,
                    "popularity": 0.92,
                },
                generated=True,
            ),
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert (
        result.retained_aliases[0]["canonical_text"]
        == "Department of Agriculture, Environment and Rural Affairs"
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


def test_alias_analysis_keeps_finance_market_index_short_alias_in_runtime_tier() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="vix",
                display_text="VIX",
                label="market_index",
                canonical_text="Cboe Volatility Index",
                record={
                    "entity_id": "curated:market_index:cboe-volatility-index",
                    "source_name": "curated-finance-entities",
                    "source_priority": 0.9,
                },
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["text"] == "VIX"
    assert result.retained_aliases[0]["runtime_tier"] == "runtime_exact_high_precision"


def test_alias_analysis_keeps_finance_exchange_initialism_in_runtime_tier() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="lse",
                display_text="LSE",
                label="exchange",
                canonical_text="London Stock Exchange",
                record={
                    "entity_id": "curated:exchange:london-stock-exchange",
                    "source_name": "curated-finance-entities",
                    "source_priority": 0.9,
                },
                generated=True,
            )
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["text"] == "LSE"
    assert result.retained_aliases[0]["runtime_tier"] == "runtime_exact_high_precision"


def test_alias_analysis_keeps_finance_exchange_initialism_over_ticker_conflict() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="lse",
                display_text="LSE",
                label="exchange",
                canonical_text="London Stock Exchange",
                record={
                    "entity_id": "curated:exchange:london-stock-exchange",
                    "source_name": "curated-finance-entities",
                    "source_priority": 0.9,
                },
                generated=True,
            ),
            build_alias_candidate(
                alias_key="lse",
                display_text="LSE",
                label="ticker",
                canonical_text="LSE",
                record={
                    "entity_id": "ticker:LSE",
                    "source_name": "symbol-directory",
                    "source_priority": 0.65,
                },
            ),
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["label"] == "exchange"
    assert result.retained_aliases[0]["text"] == "LSE"
    assert result.blocked_reason_counts["strong_short_finance_alias"] == 1


def test_alias_analysis_keeps_authoritative_finance_exchange_alias_over_generated_org() -> None:
    result = analyze_alias_candidates(
        [
            build_alias_candidate(
                alias_key="chicago mercantile exchange",
                display_text="Chicago Mercantile Exchange",
                label="exchange",
                canonical_text="CME",
                record={
                    "entity_id": "curated:exchange:cme",
                    "source_name": "curated-finance-entities",
                    "source_priority": 0.9,
                },
            ),
            build_alias_candidate(
                alias_key="chicago mercantile exchange",
                display_text="CHICAGO MERCANTILE EXCHANGE",
                label="organization",
                canonical_text="CME GROUP INC.",
                record={
                    "entity_id": "sec-cik:0001156375",
                    "source_name": "sec-company-tickers",
                    "source_priority": 0.65,
                },
                generated=True,
            ),
        ],
        allowed_ambiguous_aliases=set(),
    )

    assert result.retained_alias_count == 1
    assert result.retained_aliases[0]["label"] == "exchange"
    assert result.retained_aliases[0]["text"] == "Chicago Mercantile Exchange"
    assert result.blocked_reason_counts["authoritative_finance_alias"] == 1


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
