from pathlib import Path

from ades import pull_pack, tag


def test_public_api_tag_returns_entity_relevance_and_topic_evidence(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    response = tag(
        "Org Beta said TICKA traded on EXCHX after USD 12.5 guidance.",
        pack="finance-en",
        storage_root=tmp_path,
    )

    entities = {(entity.text, entity.label): entity for entity in response.entities}
    finance_topic = next(topic for topic in response.topics if topic.label == "finance")

    assert entities[("TICKA", "ticker")].relevance is not None
    assert entities[("USD 12.5", "currency_amount")].relevance is not None
    assert entities[("Org Beta", "organization")].relevance is not None
    assert finance_topic.score is not None
    assert finance_topic.evidence_count == 3
    assert set(finance_topic.entity_labels) == {"currency_amount", "exchange", "ticker"}
