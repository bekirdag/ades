import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.pack_registry_helpers import create_finance_registry_sources


def test_cli_list_packs_alias_defaults_to_available_registry_packs(tmp_path: Path) -> None:
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
    build_payload = json.loads(build_result.stdout)

    available_result = runner.invoke(
        app,
        [
            "list",
            "packs",
            "--json",
            "--registry-url",
            build_payload["index_url"],
        ],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )

    assert available_result.exit_code == 0
    available_payload = json.loads(available_result.stdout)
    assert available_payload["mode"] == "available"
    assert available_payload["pack_ids"] == ["finance-en", "general-en"]

    pull_result = runner.invoke(
        app,
        [
            "pull",
            "finance-en",
            "--json",
            "--registry-url",
            build_payload["index_url"],
        ],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )

    assert pull_result.exit_code == 0

    installed_result = runner.invoke(
        app,
        ["list", "installed", "--json"],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )

    assert installed_result.exit_code == 0
    installed_payload = json.loads(installed_result.stdout)
    assert installed_payload["mode"] == "installed"
    assert sorted(installed_payload["pack_ids"]) == ["finance-en", "general-en"]
