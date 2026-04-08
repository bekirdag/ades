from pathlib import Path

from ades import pull_pack, tag


def test_public_api_tag_exposes_dependency_provenance_and_links(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    response = tag(
        "Apple said AAPL traded on NASDAQ after USD 12.5 guidance.",
        pack="finance-en",
        storage_root=tmp_path,
    )

    entities = {(entity.text, entity.label): entity for entity in response.entities}

    apple = entities[("Apple", "organization")]
    assert apple.provenance is not None
    assert apple.provenance.source_pack == "general-en"
    assert apple.provenance.match_kind == "alias"
    assert apple.link is not None
    assert apple.link.provider == "lookup.alias.exact"

    currency = entities[("USD 12.5", "currency_amount")]
    assert currency.provenance is not None
    assert currency.provenance.source_pack == "finance-en"
    assert currency.provenance.match_kind == "rule"
    assert currency.provenance.match_source == "currency_amount"
    assert currency.link is not None
    assert currency.link.provider == "rule.regex"
