from ades.pipeline.tagger import _iter_candidate_windows


def test_candidate_windows_are_generated_longest_first() -> None:
    windows = _iter_candidate_windows("Person Alpha met Org Beta", max_tokens=2)

    values = [window[2] for window in windows]

    assert values[:3] == ["Person Alpha", "Alpha met", "met Org"]
    assert values[-4:] == ["Alpha", "met", "Org", "Beta"]
