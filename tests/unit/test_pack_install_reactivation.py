from pathlib import Path

import pytest

from ades import activate_pack, build_registry, deactivate_pack, list_packs, lookup_candidates, tag
from ades.packs.installer import InstallResult, PackInstaller
from ades.storage.backend import MetadataBackend
from tests.pack_registry_helpers import (
    append_pack_alias,
    create_finance_registry_sources,
    delete_installed_pack_metadata,
)


def test_repull_reactivates_inactive_dependency_same_version(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    installer = PackInstaller(install_root, registry_url=registry.index_url)
    installer.install("finance-en")
    installer.registry.set_pack_active("general-en", False)

    result = installer.install("finance-en")

    general_pack = installer.registry.get_pack("general-en")
    assert general_pack is not None
    assert general_pack.active is True
    assert result.installed == ["general-en"]
    assert result.skipped == ["finance-en"]

    entities = {(entity.text, entity.label) for entity in tag(
        "Contact us at team@example.com and watch TICKA on EXCHX.",
        pack="finance-en",
        storage_root=install_root,
    ).entities}
    assert ("team@example.com", "email_address") in entities


def test_repull_reactivates_inactive_requested_pack_same_version(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    installer = PackInstaller(install_root, registry_url=registry.index_url)
    installer.install("finance-en")
    installer.registry.set_pack_active("finance-en", False)

    result = installer.install("finance-en")

    finance_pack = installer.registry.get_pack("finance-en")
    assert finance_pack is not None
    assert finance_pack.active is True
    assert result.installed == ["finance-en"]
    assert result.skipped == ["general-en"]


def test_repull_repairs_stale_alias_metadata_before_skipping_same_version(
    tmp_path: Path,
) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    installer = PackInstaller(install_root, registry_url=registry.index_url)
    installer.install("finance-en")

    append_pack_alias(
        install_root / "packs" / "general-en",
        text="ceo@example.com",
        label="email_address",
    )
    assert installer.registry.lookup_candidates("ceo@example.com", exact_alias=True) == []

    result = installer.install("finance-en")

    assert result.installed == []
    assert result.skipped == ["general-en", "finance-en"]
    assert any(
        candidate["kind"] == "alias"
        and candidate["pack_id"] == "general-en"
        and candidate["value"] == "ceo@example.com"
        and candidate["label"] == "email_address"
        for candidate in installer.registry.lookup_candidates("ceo@example.com", exact_alias=True)
    )


def test_postgresql_same_version_pull_skips_without_full_disk_resync() -> None:
    class _FakeEntry:
        manifest_url = "https://example.invalid/packs/general-en/manifest.json"

    class _FakeIndex:
        def get(self, pack_id: str):
            assert pack_id == "general-en"
            return _FakeEntry()

    class _FakeManifest:
        pack_id = "general-en"
        version = "0.2.0"
        dependencies: list[str] = []

    class _FakePack:
        pack_id = "general-en"
        version = "0.2.0"
        active = True

    class _FakeStore:
        def get_pack(self, pack_id: str, *, active_only: bool = False):
            return _FakePack()

    class _FakeRegistry:
        metadata_backend = MetadataBackend.POSTGRESQL
        store = _FakeStore()

        def get_pack(self, pack_id: str):
            return _FakePack()

        def sync_pack_from_disk(self, pack_id: str, *, active: bool | None = None) -> bool:
            raise AssertionError("same-version PostgreSQL pulls must not re-sync pack aliases")

        def set_pack_active(self, pack_id: str, active: bool) -> bool:
            return True

    installer = object.__new__(PackInstaller)
    installer.registry = _FakeRegistry()
    installer.registry_url = "https://example.invalid/index.json"
    installer._load_index = lambda: _FakeIndex()  # type: ignore[method-assign]
    installer._load_manifest = lambda manifest_url: _FakeManifest()  # type: ignore[method-assign]

    result = InstallResult(requested_pack="general-en", registry_url=installer.registry_url)

    installer._install_recursive("general-en", result=result, visiting=set())

    assert result.installed == []
    assert result.skipped == ["general-en"]


def test_lookup_repairs_missing_metadata_rows_from_filesystem(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    installer = PackInstaller(install_root, registry_url=registry.index_url)
    installer.install("finance-en")

    delete_installed_pack_metadata(install_root, "finance-en")
    repaired_lookup = installer.registry.lookup_candidates("TICKA", exact_alias=True)

    assert any(
        candidate["kind"] == "alias"
        and candidate["pack_id"] == "finance-en"
        and candidate["value"] == "TICKA"
        and candidate["label"] == "ticker"
        for candidate in repaired_lookup
    )
    assert {
        pack.pack_id for pack in installer.registry.list_installed_packs()
    } == {"finance-en", "general-en"}
    assert any(
        candidate.kind == "alias"
        and candidate.pack_id == "finance-en"
        and candidate.value == "TICKA"
        and candidate.label == "ticker"
        for candidate in lookup_candidates(
            "TICKA",
            storage_root=install_root,
            exact_alias=True,
        ).candidates
    )


def test_deactivating_dependency_pack_with_active_dependents_is_blocked(
    tmp_path: Path,
) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    PackInstaller(install_root, registry_url=registry.index_url).install("finance-en")

    with pytest.raises(
        ValueError,
        match="Cannot deactivate pack general-en while active dependent packs exist: finance-en",
    ):
        deactivate_pack("general-en", storage_root=install_root)

    assert {
        pack.pack_id for pack in list_packs(storage_root=install_root, active_only=True)
    } == {"finance-en", "general-en"}


def test_activating_pack_reactivates_inactive_dependency_chain(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    PackInstaller(install_root, registry_url=registry.index_url).install("finance-en")

    deactivate_pack("finance-en", storage_root=install_root)
    deactivate_pack("general-en", storage_root=install_root)
    assert list_packs(storage_root=install_root, active_only=True) == []

    reactivated = activate_pack("finance-en", storage_root=install_root)

    assert reactivated is not None
    assert reactivated.pack_id == "finance-en"
    assert reactivated.active is True
    assert {
        pack.pack_id for pack in list_packs(storage_root=install_root, active_only=True)
    } == {"finance-en", "general-en"}
