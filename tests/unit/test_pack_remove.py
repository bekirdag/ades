from pathlib import Path

import pytest

from ades import build_registry, list_packs, lookup_candidates, remove_pack
from ades.packs.installer import PackInstaller
from tests.pack_registry_helpers import create_finance_registry_sources


def test_remove_pack_deletes_requested_pack_and_metadata(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    PackInstaller(install_root, registry_url=registry.index_url).install("finance-en")

    removed = remove_pack("finance-en", storage_root=install_root)

    assert removed is not None
    assert removed.pack_id == "finance-en"
    assert (install_root / "packs" / "finance-en").exists() is False
    assert (install_root / "packs" / "general-en").exists() is True
    assert {pack.pack_id for pack in list_packs(storage_root=install_root)} == {"general-en"}
    assert lookup_candidates(
        "AAPL",
        storage_root=install_root,
        exact_alias=True,
        active_only=False,
    ).candidates == []


def test_remove_pack_blocks_installed_dependency_chain(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    PackInstaller(install_root, registry_url=registry.index_url).install("finance-en")

    with pytest.raises(
        ValueError,
        match="Cannot remove pack general-en while installed dependent packs exist: finance-en",
    ):
        remove_pack("general-en", storage_root=install_root)

    assert {pack.pack_id for pack in list_packs(storage_root=install_root)} == {
        "finance-en",
        "general-en",
    }


def test_remove_pack_returns_none_after_pack_is_already_removed(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    PackInstaller(install_root, registry_url=registry.index_url).install("finance-en")

    assert remove_pack("finance-en", storage_root=install_root) is not None
    assert remove_pack("finance-en", storage_root=install_root) is None
