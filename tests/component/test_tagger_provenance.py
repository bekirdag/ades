from pathlib import Path

from ades.packs.installer import PackInstaller
from ades.pipeline.tagger import tag_text


def test_tagger_returns_alias_and_rule_provenance(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")

    response = tag_text(
        text="Apple said AAPL traded on NASDAQ after USD 12.5 guidance.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    entities = {(entity.text, entity.label): entity for entity in response.entities}

    apple = entities[("Apple", "organization")]
    assert apple.provenance is not None
    assert apple.provenance.match_kind == "alias"
    assert apple.provenance.match_path == "lookup.alias.exact"
    assert apple.provenance.source_pack == "general-en"
    assert apple.provenance.source_domain == "general"
    assert apple.link is not None
    assert apple.link.entity_id == "ades:lookup_alias_exact:general-en:organization:apple"
    assert apple.link.canonical_text == "Apple"

    currency = entities[("USD 12.5", "currency_amount")]
    assert currency.provenance is not None
    assert currency.provenance.match_kind == "rule"
    assert currency.provenance.match_path == "rule.regex"
    assert currency.provenance.match_source == "currency_amount"
    assert currency.provenance.source_pack == "finance-en"
    assert currency.provenance.source_domain == "finance"
    assert currency.link is not None
    assert currency.link.entity_id == "ades:rule_regex:finance-en:currency_amount:usd-12-5"
    assert currency.link.canonical_text == "USD 12.5"
