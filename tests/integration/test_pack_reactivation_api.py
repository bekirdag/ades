from pathlib import Path

import pytest

from ades import (
    activate_pack,
    build_registry,
    deactivate_pack,
    list_packs,
    lookup_candidates,
    pull_pack,
    tag,
)
from tests.pack_registry_helpers import append_pack_alias, create_finance_registry_sources


def test_public_api_repull_reactivates_inactive_dependency(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    pull_pack("finance-en", storage_root=install_root, registry_url=registry.index_url)
    finance_down = deactivate_pack("finance-en", storage_root=install_root)
    assert finance_down is not None
    assert finance_down.active is False
    deactivated = deactivate_pack("general-en", storage_root=install_root)
    assert deactivated is not None
    assert deactivated.active is False

    result = pull_pack("finance-en", storage_root=install_root, registry_url=registry.index_url)

    assert result.installed == ["general-en", "finance-en"]
    assert result.skipped == []
    assert {pack.pack_id for pack in list_packs(storage_root=install_root, active_only=True)} == {
        "finance-en",
        "general-en",
    }

    entities = {(entity.text, entity.label) for entity in tag(
        "Contact us at team@example.com and watch AAPL on NASDAQ.",
        pack="finance-en",
        storage_root=install_root,
    ).entities}
    assert ("team@example.com", "email_address") in entities


def test_public_api_repull_repairs_stale_pack_alias_metadata(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    pull_pack("finance-en", storage_root=install_root, registry_url=registry.index_url)

    append_pack_alias(
        install_root / "packs" / "general-en",
        text="ceo@example.com",
        label="email_address",
    )
    assert lookup_candidates(
        "ceo@example.com",
        storage_root=install_root,
        exact_alias=True,
    ).candidates == []

    repull = pull_pack("finance-en", storage_root=install_root, registry_url=registry.index_url)

    assert repull.installed == []
    assert repull.skipped == ["general-en", "finance-en"]
    assert any(
        candidate.kind == "alias"
        and candidate.pack_id == "general-en"
        and candidate.value == "ceo@example.com"
        and candidate.label == "email_address"
        for candidate in lookup_candidates(
            "ceo@example.com",
            storage_root=install_root,
            exact_alias=True,
        ).candidates
    )


def test_public_api_blocks_dependency_deactivation_with_active_dependents(
    tmp_path: Path,
) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    pull_pack("finance-en", storage_root=install_root, registry_url=registry.index_url)

    with pytest.raises(
        ValueError,
        match="Cannot deactivate pack general-en while active dependent packs exist: finance-en",
    ):
        deactivate_pack("general-en", storage_root=install_root)

    assert {pack.pack_id for pack in list_packs(storage_root=install_root, active_only=True)} == {
        "finance-en",
        "general-en",
    }


def test_public_api_activate_reactivates_inactive_dependencies(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    pull_pack("finance-en", storage_root=install_root, registry_url=registry.index_url)

    deactivate_pack("finance-en", storage_root=install_root)
    deactivate_pack("general-en", storage_root=install_root)

    reactivated = activate_pack("finance-en", storage_root=install_root)

    assert reactivated is not None
    assert reactivated.pack_id == "finance-en"
    assert reactivated.active is True
    assert {pack.pack_id for pack in list_packs(storage_root=install_root, active_only=True)} == {
        "finance-en",
        "general-en",
    }
