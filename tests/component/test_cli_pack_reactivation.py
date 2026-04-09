import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.pack_registry_helpers import append_pack_alias, create_finance_registry_sources


def test_cli_repull_reactivates_inactive_dependency(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry_dir = tmp_path / "registry"
    install_root = tmp_path / "install"
    runner = CliRunner()

    build_result = runner.invoke(
        app,
        [
            "registry",
            "build",
            str(general_dir),
            str(finance_dir),
            "--output-dir",
            str(registry_dir),
        ],
    )
    assert build_result.exit_code == 0
    registry_index = json.loads(build_result.stdout)["index_url"]

    first_pull = runner.invoke(
        app,
        ["pull", "finance-en", "--registry-url", registry_index],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert first_pull.exit_code == 0

    deactivate = runner.invoke(
        app,
        ["packs", "deactivate", "general-en"],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert deactivate.exit_code == 0
    assert json.loads(deactivate.stdout)["active"] is False

    repull = runner.invoke(
        app,
        ["pull", "finance-en", "--registry-url", registry_index],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert repull.exit_code == 0
    repull_payload = json.loads(repull.stdout)
    assert repull_payload["installed"] == ["general-en"]
    assert repull_payload["skipped"] == ["finance-en"]

    active_packs = runner.invoke(
        app,
        ["packs", "list", "--active-only"],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert active_packs.exit_code == 0
    assert {pack["pack_id"] for pack in json.loads(active_packs.stdout)["packs"]} == {
        "finance-en",
        "general-en",
    }


def test_cli_repull_repairs_stale_pack_alias_metadata(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry_dir = tmp_path / "registry"
    install_root = tmp_path / "install"
    runner = CliRunner()

    build_result = runner.invoke(
        app,
        [
            "registry",
            "build",
            str(general_dir),
            str(finance_dir),
            "--output-dir",
            str(registry_dir),
        ],
    )
    assert build_result.exit_code == 0
    registry_index = json.loads(build_result.stdout)["index_url"]

    first_pull = runner.invoke(
        app,
        ["pull", "finance-en", "--registry-url", registry_index],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert first_pull.exit_code == 0

    append_pack_alias(
        install_root / "packs" / "general-en",
        text="ceo@example.com",
        label="email_address",
    )

    lookup_before = runner.invoke(
        app,
        ["packs", "lookup", "ceo@example.com", "--exact-alias"],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert lookup_before.exit_code == 0
    assert json.loads(lookup_before.stdout)["candidates"] == []

    repull = runner.invoke(
        app,
        ["pull", "finance-en", "--registry-url", registry_index],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert repull.exit_code == 0
    repull_payload = json.loads(repull.stdout)
    assert repull_payload["installed"] == []
    assert repull_payload["skipped"] == ["general-en", "finance-en"]

    lookup_after = runner.invoke(
        app,
        ["packs", "lookup", "ceo@example.com", "--exact-alias"],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert lookup_after.exit_code == 0
    assert any(
        candidate["kind"] == "alias"
        and candidate["pack_id"] == "general-en"
        and candidate["value"] == "ceo@example.com"
        and candidate["label"] == "email_address"
        for candidate in json.loads(lookup_after.stdout)["candidates"]
    )
