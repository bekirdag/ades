from pathlib import Path

from ades import (
    activate_pack,
    deactivate_pack,
    get_pack,
    list_available_packs,
    list_packs,
    lookup_candidates,
    pull_pack,
    status,
    tag,
)


def test_public_api_can_install_and_list_packs(tmp_path: Path) -> None:
    result = pull_pack("finance-en", storage_root=tmp_path)

    assert "general-en" in result.installed
    assert "finance-en" in result.installed

    packs = list_packs(storage_root=tmp_path)
    assert {pack.pack_id for pack in packs} == {"finance-en", "general-en"}
    assert all(pack.active is True for pack in packs)

    finance_pack = get_pack("finance-en", storage_root=tmp_path)
    assert finance_pack is not None
    assert finance_pack.domain == "finance"
    assert finance_pack.active is True


def test_public_api_status_and_tag(tmp_path: Path, monkeypatch) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    monkeypatch.setenv("ADES_REGISTRY_URL", "https://example.invalid/index.json")

    runtime_status = status(storage_root=tmp_path)
    assert runtime_status.storage_root == str(tmp_path)
    assert "finance-en" in runtime_status.installed_packs
    assert runtime_status.registry_url == "https://example.invalid/index.json"

    response = tag(
        "AAPL rallied on NASDAQ after USD 12.5 guidance from Apple.",
        pack="finance-en",
        storage_root=tmp_path,
    )
    labels = {entity.label for entity in response.entities}

    assert "ticker" in labels
    assert "exchange" in labels
    assert "currency_amount" in labels


def test_public_api_activation_and_lookup(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    active_packs = list_packs(storage_root=tmp_path, active_only=True)
    assert {pack.pack_id for pack in active_packs} == {"finance-en", "general-en"}

    deactivated = deactivate_pack("finance-en", storage_root=tmp_path)
    assert deactivated is not None
    assert deactivated.active is False

    active_after = list_packs(storage_root=tmp_path, active_only=True)
    assert {pack.pack_id for pack in active_after} == {"general-en"}

    inactive_lookup = lookup_candidates("AAPL", storage_root=tmp_path, exact_alias=True)
    assert inactive_lookup.candidates == []

    reactivated = activate_pack("finance-en", storage_root=tmp_path)
    assert reactivated is not None
    assert reactivated.active is True

    lookup = lookup_candidates("AAPL", storage_root=tmp_path, exact_alias=True)
    assert lookup.candidates
    assert lookup.candidates[0].kind == "alias"
    assert lookup.candidates[0].value == "AAPL"


def test_public_api_lists_available_pack_metadata() -> None:
    available_packs = list_available_packs()

    pack_ids = {pack.pack_id for pack in available_packs}
    assert {"general-en", "finance-en", "medical-en"} <= pack_ids

    finance_pack = next(pack for pack in available_packs if pack.pack_id == "finance-en")
    assert finance_pack.description is not None
    assert "finance" in finance_pack.tags
