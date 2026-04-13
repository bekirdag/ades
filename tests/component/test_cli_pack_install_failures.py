import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.registry import PackRegistry
from ades.storage.paths import build_storage_layout
from tests.pack_registry_helpers import create_finance_registry_sources


def test_cli_pull_failure_does_not_leave_partial_pack_install(tmp_path: Path) -> None:
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

    corrupted_artifact = registry_dir / "artifacts" / "general-en-0.1.0.tar.zst"
    corrupted_artifact.write_bytes(b"corrupted-artifact")

    pull_result = runner.invoke(
        app,
        [
            "pull",
            "general-en",
            "--registry-url",
            build_payload["index_url"],
        ],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )

    assert pull_result.exit_code != 0
    assert pull_result.exception is not None
    assert "checksum mismatch" in str(pull_result.exception).lower()

    packs_result = runner.invoke(
        app,
        ["packs", "list", "--json"],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )
    assert packs_result.exit_code == 0
    packs_payload = json.loads(packs_result.stdout)
    assert packs_payload["mode"] == "installed"
    assert packs_payload["pack_ids"] == []
    assert packs_payload["packs"] == []

    layout = build_storage_layout(install_root)
    assert not (layout.packs_dir / "general-en").exists()
    assert sorted(path.name for path in layout.packs_dir.glob(".general-en.*")) == []
    assert PackRegistry(install_root).list_installed_packs() == []
