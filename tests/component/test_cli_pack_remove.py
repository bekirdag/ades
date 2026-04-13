import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.pack_registry_helpers import create_finance_registry_sources


def test_cli_remove_deletes_requested_pack_and_keeps_dependencies(tmp_path: Path) -> None:
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

    pull_result = runner.invoke(
        app,
        ["pull", "finance-en", "--json", "--registry-url", registry_index],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert pull_result.exit_code == 0

    remove_result = runner.invoke(
        app,
        ["packs", "remove", "finance-en"],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert remove_result.exit_code == 0
    assert json.loads(remove_result.stdout)["pack_id"] == "finance-en"
    assert (install_root / "packs" / "finance-en").exists() is False

    listed = runner.invoke(
        app,
        ["packs", "list", "--json"],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert listed.exit_code == 0
    assert {pack["pack_id"] for pack in json.loads(listed.stdout)["packs"]} == {"general-en"}


def test_cli_blocks_dependency_removal_with_installed_dependents(tmp_path: Path) -> None:
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

    pull_result = runner.invoke(
        app,
        ["pull", "finance-en", "--json", "--registry-url", registry_index],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert pull_result.exit_code == 0

    remove_result = runner.invoke(
        app,
        ["packs", "remove", "general-en"],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert remove_result.exit_code == 1
    assert (
        "Cannot remove pack general-en while installed dependent packs exist: finance-en"
        in remove_result.stderr
    )
