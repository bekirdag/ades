from pathlib import Path

import pytest

from ades import build_registry, get_pack, list_packs, pull_pack
from tests.pack_registry_helpers import create_finance_registry_sources


def test_public_api_failed_update_preserves_existing_pack(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources-initial")
    initial_registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry-initial")

    install_root = tmp_path / "install"
    pull_pack(
        "finance-en",
        storage_root=install_root,
        registry_url=initial_registry.index_url,
    )

    updated_general_dir, updated_finance_dir = create_finance_registry_sources(
        tmp_path / "sources-update",
        general_version="0.1.0",
        finance_version="0.2.0",
    )
    updated_registry = build_registry(
        [updated_general_dir, updated_finance_dir],
        output_dir=tmp_path / "registry-update",
    )
    (tmp_path / "registry-update" / "artifacts" / "finance-en-0.2.0.tar.zst").write_bytes(
        b"corrupted-artifact"
    )

    with pytest.raises(ValueError, match="checksum mismatch"):
        pull_pack(
            "finance-en",
            storage_root=install_root,
            registry_url=updated_registry.index_url,
        )

    finance_pack = get_pack("finance-en", storage_root=install_root)
    assert finance_pack is not None
    assert finance_pack.version == "0.1.0"
    assert sorted(pack.pack_id for pack in list_packs(storage_root=install_root)) == [
        "finance-en",
        "general-en",
    ]
