import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.pack_generation_helpers import create_finance_generation_bundle


def test_cli_can_generate_pack_then_build_and_pull_from_it(tmp_path: Path) -> None:
    bundle_dir = create_finance_generation_bundle(tmp_path / "bundle")
    generated_root = tmp_path / "generated-packs"
    registry_dir = tmp_path / "registry"
    install_root = tmp_path / "install"
    runner = CliRunner()

    generate_result = runner.invoke(
        app,
        [
            "registry",
            "generate-pack",
            str(bundle_dir),
            "--output-dir",
            str(generated_root),
        ],
    )

    assert generate_result.exit_code == 0
    generate_payload = json.loads(generate_result.stdout)
    assert generate_payload["pack_id"] == "finance-en"
    assert Path(generate_payload["pack_dir"]).exists()
    assert generate_payload["publishable_sources_only"] is False
    assert generate_payload["source_license_classes"] == {"build-only": 1, "ship-now": 1}

    build_result = runner.invoke(
        app,
        [
            "registry",
            "build",
            generate_payload["pack_dir"],
            "--output-dir",
            str(registry_dir),
        ],
    )

    assert build_result.exit_code == 0
    build_payload = json.loads(build_result.stdout)
    assert build_payload["pack_count"] == 1

    pull_result = runner.invoke(
        app,
        [
            "pull",
            "finance-en",
            "--registry-url",
            build_payload["index_url"],
        ],
        env={"ADES_STORAGE_ROOT": str(install_root)},
    )

    assert pull_result.exit_code == 0
    pull_payload = json.loads(pull_result.stdout)
    assert pull_payload["installed"] == ["finance-en"]
