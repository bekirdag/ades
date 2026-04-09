from pathlib import Path

from ades import pull_pack, tag
from tests.pack_registry_helpers import delete_installed_pack_metadata


def test_finance_tagging_includes_dependency_aliases_via_lookup(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    response = tag(
        "Apple said AAPL traded on NASDAQ.",
        pack="finance-en",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Apple", "organization") in pairs
    assert ("AAPL", "ticker") in pairs
    assert ("NASDAQ", "exchange") in pairs


def test_finance_tagging_recovers_after_metadata_row_deletion(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    delete_installed_pack_metadata(tmp_path, "finance-en")

    response = tag(
        "AAPL traded on NASDAQ.",
        pack="finance-en",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("AAPL", "ticker") in pairs
    assert ("NASDAQ", "exchange") in pairs
