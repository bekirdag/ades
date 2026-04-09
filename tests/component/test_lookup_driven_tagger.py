from pathlib import Path

from ades.packs.installer import PackInstaller
from ades.pipeline.tagger import tag_text
from tests.pack_registry_helpers import delete_installed_pack_metadata


def test_tagger_uses_lookup_for_multi_token_general_aliases(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("general-en")

    response = tag_text(
        text="Tim Cook spoke about Apple.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Tim Cook", "person") in pairs
    assert ("Apple", "organization") in pairs


def test_tagger_recovers_finance_alias_lookup_after_metadata_row_deletion(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")

    delete_installed_pack_metadata(tmp_path, "finance-en")

    response = tag_text(
        text="AAPL traded on NASDAQ.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("AAPL", "ticker") in pairs
    assert ("NASDAQ", "exchange") in pairs
