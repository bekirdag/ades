from ades.text_processing import normalize_lookup_text, normalize_text, segment_text, token_count


def test_normalize_lookup_text_uses_shared_contract() -> None:
    assert normalize_lookup_text("  Entity\u2014Alpha  ") == "entity-alpha"
    assert normalize_lookup_text("“Entity”   Alpha") == '"entity" alpha'


def test_normalize_text_preserves_html_offsets() -> None:
    raw = "<p>Entity&nbsp;Alpha</p>"
    normalized = normalize_text(raw, "text/html")

    assert normalized.text == "Entity Alpha"
    alpha_start = normalized.text.index("Alpha")
    raw_start, raw_end = normalized.raw_span(alpha_start, alpha_start + len("Alpha"))

    assert raw[raw_start:raw_end] == "Alpha"


def test_segment_text_splits_on_sentence_boundaries_and_preserves_offsets() -> None:
    value = "Entity Alpha moved. Entity Beta followed.\n\nEntity Gamma stayed."

    segments = segment_text(value, language="en", max_chars=32, max_tokens=6)

    assert [segment.text for segment in segments] == [
        "Entity Alpha moved.",
        "Entity Beta followed.",
        "Entity Gamma stayed.",
    ]
    assert [segment.start for segment in segments] == [0, 20, 43]
    assert token_count(value) == 9
