from pathlib import Path

import pytest

from ades import build_registry
from ades.packs.installer import PackInstaller
from ades.storage.paths import build_storage_layout
from tests.pack_registry_helpers import create_finance_registry_sources


def _hidden_pack_workdirs(packs_dir: Path, pack_id: str) -> list[str]:
    return sorted(path.name for path in packs_dir.glob(f".{pack_id}.*"))


def test_failed_fresh_install_cleans_up_partial_pack_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")
    installer = PackInstaller(tmp_path / "install", registry_url=registry.index_url)

    def boom(payload: bytes, destination: Path) -> None:
        raise RuntimeError("extract failed")

    monkeypatch.setattr(PackInstaller, "_extract_tar_zst", staticmethod(boom))

    with pytest.raises(RuntimeError, match="extract failed"):
        installer.install("general-en")

    layout = build_storage_layout(tmp_path / "install")
    assert not (layout.packs_dir / "general-en").exists()
    assert installer.registry.get_pack("general-en") is None
    assert _hidden_pack_workdirs(layout.packs_dir, "general-en") == []


def test_failed_update_restores_existing_pack_after_replacement_starts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources-initial")
    initial_registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry-initial")

    install_root = tmp_path / "install"
    PackInstaller(install_root, registry_url=initial_registry.index_url).install("finance-en")

    updated_general_dir, updated_finance_dir = create_finance_registry_sources(
        tmp_path / "sources-update",
        general_version="0.1.0",
        finance_version="0.2.0",
    )
    updated_registry = build_registry(
        [updated_general_dir, updated_finance_dir],
        output_dir=tmp_path / "registry-update",
    )
    updater = PackInstaller(install_root, registry_url=updated_registry.index_url)
    original_sync = updater.registry.store.sync_pack_from_dir
    sync_calls = {"finance": 0}

    def fail_once(pack_dir: Path, *, active: bool | None = None) -> object:
        if Path(pack_dir).name == "finance-en" and sync_calls["finance"] == 0:
            sync_calls["finance"] += 1
            raise RuntimeError("sync failed")
        return original_sync(pack_dir, active=active)

    monkeypatch.setattr(updater.registry.store, "sync_pack_from_dir", fail_once)

    with pytest.raises(RuntimeError, match="sync failed"):
        updater.install("finance-en")

    restored = updater.registry.get_pack("finance-en")
    assert restored is not None
    assert restored.version == "0.1.0"

    layout = build_storage_layout(install_root)
    assert (layout.packs_dir / "finance-en" / "manifest.json").exists()
    assert _hidden_pack_workdirs(layout.packs_dir, "finance-en") == []
