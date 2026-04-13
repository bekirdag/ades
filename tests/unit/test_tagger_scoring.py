from pathlib import Path

from ades.packs.installer import PackInstaller
from ades.pipeline.tagger import tag_text


def test_entity_relevance_prefers_pack_domain_matches(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")

    response = tag_text(
        text="Org Beta said TICKA traded on EXCHX after USD 12.5 guidance.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    entities = {(entity.text, entity.label): entity for entity in response.entities}

    organization_entity = entities[("Org Beta", "organization")]
    ticker = entities[("TICKA", "ticker")]
    currency = entities[("USD 12.5", "currency_amount")]

    assert all(entity.relevance is not None for entity in response.entities)
    assert all(0.0 <= (entity.relevance or 0.0) <= 1.0 for entity in response.entities)
    assert ticker.relevance is not None
    assert currency.relevance is not None
    assert organization_entity.relevance is not None
    assert ticker.relevance > organization_entity.relevance
    assert currency.relevance > organization_entity.relevance
