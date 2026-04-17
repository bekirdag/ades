from ades.pipeline.tagger import (
    _iter_candidate_windows,
    _iter_hyphenated_alias_backfill_windows,
)


def test_candidate_windows_are_generated_longest_first() -> None:
    windows = _iter_candidate_windows("Person Alpha met Org Beta", max_tokens=2)

    values = [window[2] for window in windows]

    assert values[:3] == ["Person Alpha", "Alpha met", "met Org"]
    assert values[-4:] == ["Alpha", "met", "Org", "Beta"]


def test_hyphenated_alias_backfill_windows_expose_titleish_prefixes() -> None:
    windows = _iter_hyphenated_alias_backfill_windows(
        "Shanghai-based Northwind Solutions and data-driven systems"
    )

    assert windows == [(0, 8, "Shanghai")]


def test_hyphenated_alias_backfill_windows_support_listed_and_hit_suffixes() -> None:
    listed_windows = _iter_hyphenated_alias_backfill_windows(
        "Shanghai-listed issuers and lower-case noise"
    )
    hit_windows = _iter_hyphenated_alias_backfill_windows(
        "COVID-hit hospitals and lower-case noise"
    )

    assert listed_windows == [(0, 8, "Shanghai")]
    assert hit_windows == [(0, 5, "COVID")]
