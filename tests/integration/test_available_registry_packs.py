from ades import list_available_packs


def test_available_registry_packs_expose_descriptions_and_tags() -> None:
    packs = list_available_packs()

    finance_pack = next(pack for pack in packs if pack.pack_id == "finance-en")
    assert finance_pack.description is not None
    assert "finance" in finance_pack.tags
