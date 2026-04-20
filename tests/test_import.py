from importlib.metadata import version as installed_version

from ades import __version__
from ades.packs.installer import PackInstaller
from ades.packs.registry import load_registry_index
from ades.storage.paths import build_storage_layout


def test_version_is_set() -> None:
    assert __version__
    assert installed_version("ades-tool") == __version__


def test_storage_layout_uses_expected_folders(tmp_path) -> None:
    layout = build_storage_layout(tmp_path)
    assert layout.packs_dir == tmp_path / "packs"
    assert layout.registry_db == tmp_path / "registry" / "ades.db"


def test_bundled_registry_lists_expected_packs() -> None:
    index = load_registry_index()
    assert sorted(index.packs) == ["finance-en", "general-en", "medical-en"]


def test_pack_installer_installs_dependencies(tmp_path) -> None:
    installer = PackInstaller(tmp_path)
    result = installer.install("finance-en")

    assert "general-en" in result.installed
    assert "finance-en" in result.installed
    assert (tmp_path / "packs" / "general-en" / "manifest.json").exists()
    assert (tmp_path / "packs" / "finance-en" / "manifest.json").exists()
