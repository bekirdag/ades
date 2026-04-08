from pathlib import Path

from ades.packs.installer import PackInstaller
from ades.pipeline.tagger import tag_text


def test_finance_pack_tags_aliases_and_rules(tmp_path: Path) -> None:
    installer = PackInstaller(tmp_path)
    installer.install("finance-en")

    response = tag_text(
        text="AAPL rallied on NASDAQ after USD 12.5 earnings guidance.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    labels = {entity.label for entity in response.entities}
    topics = {topic.label for topic in response.topics}

    assert "ticker" in labels
    assert "exchange" in labels
    assert "currency_amount" in labels
    assert "finance" in topics


def test_missing_pack_warns(tmp_path: Path) -> None:
    response = tag_text(
        text="anything",
        pack="missing-pack",
        content_type="text/plain",
        storage_root=tmp_path,
    )
    assert "pack_not_installed:missing-pack" in response.warnings
