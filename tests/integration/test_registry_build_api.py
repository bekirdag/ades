from pathlib import Path

from ades import build_registry, list_packs, pull_pack
from tests.pack_registry_helpers import create_finance_registry_sources


def test_public_api_can_build_registry_and_install_from_it(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry(
        [general_dir, finance_dir],
        output_dir=tmp_path / "registry",
    )

    install_root = tmp_path / "install"
    result = pull_pack(
        "finance-en",
        storage_root=install_root,
        registry_url=registry.index_url,
    )

    assert registry.pack_count == 2
    assert result.installed == ["general-en", "finance-en"]
    assert sorted(pack.pack_id for pack in list_packs(storage_root=install_root)) == [
        "finance-en",
        "general-en",
    ]
