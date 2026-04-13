from pathlib import Path

from ades.packs.installer import PackInstaller
from ades.pipeline.tagger import tag_text


def test_tagger_returns_alias_and_rule_provenance(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")

    response = tag_text(
        text="Org Beta said TICKA traded on EXCHX after USD 12.5 guidance.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    entities = {(entity.text, entity.label): entity for entity in response.entities}

    organization_entity = entities[("Org Beta", "organization")]
    assert organization_entity.provenance is not None
    assert organization_entity.provenance.match_kind == "alias"
    assert organization_entity.provenance.match_path == "lookup.alias.exact"
    assert organization_entity.provenance.source_pack == "general-en"
    assert organization_entity.provenance.source_domain == "general"
    assert organization_entity.link is not None
    assert organization_entity.link.entity_id == "ades:lookup_alias_exact:general-en:organization:org-beta"
    assert organization_entity.link.canonical_text == "Org Beta"

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
