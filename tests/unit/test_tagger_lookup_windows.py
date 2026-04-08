from ades.pipeline.tagger import _iter_candidate_windows


def test_candidate_windows_are_generated_longest_first() -> None:
    windows = _iter_candidate_windows("Tim Cook met Apple", max_tokens=2)

    values = [window[2] for window in windows]

    assert values[:3] == ["Tim Cook", "Cook met", "met Apple"]
    assert values[-4:] == ["Tim", "Cook", "met", "Apple"]
