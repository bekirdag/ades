import sqlite3
from pathlib import Path

from ades.packs.installer import PackInstaller
from ades.packs.registry import PackRegistry
from ades.pipeline.tagger import tag_text
from ades.storage.paths import build_storage_layout


def test_pack_metadata_is_synced_to_sqlite(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    layout = build_storage_layout(tmp_path)

    assert layout.registry_db.exists()

    connection = sqlite3.connect(layout.registry_db)
    try:
        packs = {
            row[0]: row[1]
            for row in connection.execute(
                "SELECT pack_id, active FROM installed_packs ORDER BY pack_id ASC"
            ).fetchall()
        }
        dependencies = connection.execute(
            """
            SELECT dependency_pack_id
            FROM pack_dependencies
            WHERE pack_id = 'finance-en'
            """
        ).fetchall()
        aliases = connection.execute(
            """
            SELECT alias_text, label
            FROM pack_aliases
            WHERE pack_id = 'finance-en'
            ORDER BY position ASC
            """
        ).fetchall()
        rules = connection.execute(
            """
            SELECT rule_name, label
            FROM pack_rules
            WHERE pack_id = 'finance-en'
            ORDER BY position ASC
            """
        ).fetchall()
    finally:
        connection.close()

    assert packs == {"finance-en": 1, "general-en": 1}
    assert dependencies == [("general-en",)]
    assert ("AAPL", "ticker") in aliases
    assert ("currency_amount", "currency_amount") in rules


def test_inactive_pack_is_not_used_by_runtime(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    registry = PackRegistry(tmp_path)

    finance_pack = registry.get_pack("finance-en")
    assert finance_pack is not None
    assert finance_pack.active is True

    assert registry.set_pack_active("finance-en", False) is True

    inactive_pack = registry.get_pack("finance-en")
    assert inactive_pack is not None
    assert inactive_pack.active is False
    assert registry.get_pack("finance-en", active_only=True) is None

    response = tag_text(
        text="AAPL rallied on NASDAQ after USD 12.5 guidance.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )
    assert "pack_not_installed:finance-en" in response.warnings


def test_registry_lookup_candidates_respects_active_state(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    registry = PackRegistry(tmp_path)

    exact_alias = registry.lookup_candidates("AAPL", exact_alias=True)
    assert exact_alias
    assert exact_alias[0]["kind"] == "alias"
    assert exact_alias[0]["value"] == "AAPL"

    rules = registry.lookup_candidates("currency_amount")
    assert any(candidate["kind"] == "rule" for candidate in rules)

    registry.set_pack_active("finance-en", False)
    hidden = registry.lookup_candidates("AAPL", exact_alias=True)
    assert hidden == []
    visible = registry.lookup_candidates("AAPL", exact_alias=True, active_only=False)
    assert visible
