import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.pack_registry_helpers import create_finance_registry_sources


def test_cli_can_build_registry_and_pull_from_registry_url(tmp_path: Path) -> None:
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
    build_payload = json.loads(build_result.stdout)
    assert build_payload["pack_count"] == 2

    available_result = runner.invoke(
        app,
        [
            "packs",
            "list",
            "--available",
            "--json",
            "--registry-url",
            build_payload["index_url"],
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path / "install")},
    )

    assert available_result.exit_code == 0
    available_payload = json.loads(available_result.stdout)
    assert available_payload["mode"] == "available"
    assert available_payload["pack_ids"] == ["finance-en", "general-en"]
    finance_pack = next(pack for pack in available_payload["packs"] if pack["pack_id"] == "finance-en")
    assert "finance" in finance_pack["tags"]

    pull_result = runner.invoke(
        app,
        [
            "pull",
            "finance-en",
            "--json",
            "--registry-url",
            build_payload["index_url"],
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path / "install")},
    )

    assert pull_result.exit_code == 0
    pull_payload = json.loads(pull_result.stdout)
    assert pull_payload["registry_url"] == build_payload["index_url"]
    assert pull_payload["installed"] == ["general-en", "finance-en"]
