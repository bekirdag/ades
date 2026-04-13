from pathlib import Path

from ades.packs.installer import PackInstaller
from ades.pipeline.tagger import tag_text


def test_tagger_returns_topic_evidence_for_non_general_domains(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")

    response = tag_text(
        text="Org Beta said TICKA traded on EXCHX after USD 12.5 guidance.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    topics = {topic.label: topic for topic in response.topics}

    assert "finance" in topics
    assert "general" not in topics

    finance_topic = topics["finance"]
    assert finance_topic.score is not None
    assert finance_topic.score > 0.0
    assert finance_topic.evidence_count == 3
    assert set(finance_topic.entity_labels) == {"currency_amount", "exchange", "ticker"}


def test_tagger_falls_back_to_runtime_domain_topic_when_only_general_matches(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")

    response = tag_text(
        text="Org Beta released a statement.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    assert len(response.topics) == 1

    finance_topic = response.topics[0]
    assert finance_topic.label == "finance"
    assert finance_topic.score is not None
    assert finance_topic.evidence_count == 1
    assert finance_topic.entity_labels == ["organization"]
