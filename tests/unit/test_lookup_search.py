from pathlib import Path

from ades.packs.installer import PackInstaller
from ades.packs.registry import PackRegistry


def test_registry_lookup_supports_multi_term_cross_field_alias_search(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    registry = PackRegistry(tmp_path)

    candidates = registry.lookup_candidates("apple organization", limit=10)

    assert candidates
    assert candidates[0]["kind"] == "alias"
    assert candidates[0]["value"] == "Apple"
    assert candidates[0]["label"] == "organization"


def test_registry_lookup_supports_multi_term_cross_field_rule_search(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    registry = PackRegistry(tmp_path)

    candidates = registry.lookup_candidates("currency finance", limit=10)

    assert any(
        candidate["kind"] == "rule" and candidate["value"] == "currency_amount"
        for candidate in candidates
    )
