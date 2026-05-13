from ades.pipeline import tagger


def test_runtime_finance_country_short_alias_blocks_news_artifact_terms() -> None:
    for surface in ("Source", "Landmark", "Exchange", "Fort", "Coast", "London"):
        assert not tagger._runtime_finance_country_short_alias_surface_allowed(surface)


def test_runtime_finance_country_short_alias_blocks_geographic_surfaces() -> None:
    assert not tagger._runtime_finance_country_short_alias_surface_allowed(
        "Silicon Valley"
    )
    assert not tagger._runtime_finance_country_short_alias_surface_allowed(
        "Lost Coast"
    )


def test_runtime_finance_country_short_alias_keeps_company_surfaces() -> None:
    assert tagger._runtime_finance_country_short_alias_surface_allowed("Intertek")
    assert tagger._runtime_finance_country_short_alias_surface_allowed("Akcansa")
    assert tagger._runtime_finance_country_short_alias_surface_allowed("Sabanci Holding")
