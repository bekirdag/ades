from pathlib import Path

from ades import build_registry, deactivate_pack, list_packs, pull_pack, tag
from tests.pack_registry_helpers import create_finance_registry_sources


def test_public_api_repull_reactivates_inactive_dependency(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry = build_registry([general_dir, finance_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    pull_pack("finance-en", storage_root=install_root, registry_url=registry.index_url)
    deactivated = deactivate_pack("general-en", storage_root=install_root)
    assert deactivated is not None
    assert deactivated.active is False

    result = pull_pack("finance-en", storage_root=install_root, registry_url=registry.index_url)

    assert result.installed == ["general-en"]
    assert result.skipped == ["finance-en"]
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
