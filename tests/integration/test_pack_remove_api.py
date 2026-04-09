from pathlib import Path

import pytest

from ades import build_registry, list_packs, remove_pack
from ades.packs.installer import PackInstaller
from tests.pack_registry_helpers import create_finance_registry_sources


def test_public_api_remove_deletes_requested_pack(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    PackInstaller(install_root, registry_url=registry.index_url).install("finance-en")

    removed = remove_pack("finance-en", storage_root=install_root)

    assert removed is not None
    assert removed.pack_id == "finance-en"
    assert {pack.pack_id for pack in list_packs(storage_root=install_root)} == {"general-en"}


def test_public_api_blocks_dependency_removal_with_installed_dependents(
    tmp_path: Path,
) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    PackInstaller(install_root, registry_url=registry.index_url).install("finance-en")

    with pytest.raises(
        ValueError,
        match="Cannot remove pack general-en while installed dependent packs exist: finance-en",
    ):
        remove_pack("general-en", storage_root=install_root)


def test_public_api_remove_missing_pack_returns_none(tmp_path: Path) -> None:
    assert remove_pack("finance-en", storage_root=tmp_path) is None
