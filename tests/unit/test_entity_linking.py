from ades.pipeline.tagger import _build_entity_link, _normalize_link_value


def test_entity_link_ids_are_stable_and_normalized() -> None:
    assert _normalize_link_value("USD 12.5") == "usd-12-5"

    link = _build_entity_link(
        provider="lookup.alias.exact",
        pack_id="finance-en",
        label="Ticker",
        canonical_text="AAPL",
    )

    assert link.entity_id == "ades:lookup_alias_exact:finance-en:ticker:aapl"
    assert link.canonical_text == "AAPL"
    assert link.provider == "lookup.alias.exact"
