import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller
from tests.pack_registry_helpers import create_finance_registry_sources


def test_cli_list_packs_defaults_to_human_readable_table(tmp_path: Path) -> None:
    general_dir, finance_dir = create_finance_registry_sources(tmp_path / "sources")
    registry_dir = tmp_path / "registry"
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

    result = runner.invoke(
        app,
        [
            "list",
            "packs",
            "--registry-url",
            registry_index,
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path / "install")},
    )

    assert result.exit_code == 0
    assert "Available packs (2)" in result.stdout
    assert "PACK ID" in result.stdout
    assert "finance-en" in result.stdout
    assert "general-en" in result.stdout
    assert '"mode"' not in result.stdout


def test_cli_list_installed_defaults_to_human_readable_table(tmp_path: Path) -> None:
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
    PackInstaller(install_root, registry_url=registry_index).install("finance-en")

    result = runner.invoke(
        app,
        ["list", "installed"],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )

    assert result.exit_code == 0
    assert "Installed packs (2)" in result.stdout
    assert "ACTIVE" in result.stdout
    assert "finance-en" in result.stdout
    assert "general-en" in result.stdout
    assert '"mode"' not in result.stdout


def test_cli_pull_defaults_to_human_readable_summary(tmp_path: Path) -> None:
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

    result = runner.invoke(
        app,
        [
            "pull",
            "finance-en",
            "--registry-url",
            registry_index,
        ],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )

    assert result.exit_code == 0
    assert "Pull complete" in result.stdout
    assert "Requested pack: finance-en" in result.stdout
    assert "Installed (2):" in result.stdout
    assert "general-en" in result.stdout
    assert "finance-en" in result.stdout
    assert '"requested_pack"' not in result.stdout


def test_cli_tag_requires_installed_pack_guidance(tmp_path: Path) -> None:
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["tag", "Org Beta said TICKA traded on EXCHX after USD 12.5 guidance."],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 1
    assert "No packs are installed yet." in result.stderr
    assert "ades pull general-en" in result.stderr


def test_cli_help_command_prints_root_help() -> None:
    runner = CliRunner()

    result = runner.invoke(app, ["help"])

    assert result.exit_code == 0
    assert "Usage:" in result.stdout
    assert "ades local semantic enrichment CLI" in result.stdout
    assert "list" in result.stdout
    assert "packs" in result.stdout
    assert "pull" in result.stdout
    assert "help" in result.stdout


def test_cli_help_command_supports_nested_command_paths() -> None:
    runner = CliRunner()

    result = runner.invoke(app, ["help", "list", "packs"])

    assert result.exit_code == 0
    assert "Usage:" in result.stdout
    assert "ades list packs [OPTIONS]" in result.stdout
    assert "ades ades" not in result.stdout
    assert "List packs with available registry packs as the default view." in result.stdout
    assert "--json" in result.stdout
