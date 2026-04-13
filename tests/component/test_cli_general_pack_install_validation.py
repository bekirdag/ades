import json

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.registry import PackRegistry
from ades.storage.paths import build_storage_layout
from tests.pack_registry_helpers import create_pack_source, write_general_pack_build


def test_cli_pull_rejects_pathological_general_pack_artifact(tmp_path) -> None:
    general_dir = create_pack_source(tmp_path / "sources", pack_id="general-en", domain="general")
    write_general_pack_build(
        general_dir,
        person_count=26,
        organization_count=14,
        location_count=1_417_881,
    )
    registry_dir = tmp_path / "registry"
    install_root = tmp_path / "install"
    runner = CliRunner()

    build_result = runner.invoke(
        app,
        [
            "registry",
            "build",
            str(general_dir),
            "--output-dir",
            str(registry_dir),
        ],
    )

    assert build_result.exit_code == 0
    build_payload = json.loads(build_result.stdout)

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
    assert "General-en install verification failed" in str(pull_result.exception)

    layout = build_storage_layout(install_root)
    assert not (layout.packs_dir / "general-en").exists()
    assert sorted(path.name for path in layout.packs_dir.glob(".general-en.*")) == []
    assert PackRegistry(install_root).list_installed_packs() == []
