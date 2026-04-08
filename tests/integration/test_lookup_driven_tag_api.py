from pathlib import Path

from ades import pull_pack, tag


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
