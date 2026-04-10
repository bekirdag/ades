import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.object_storage_publish_helpers import (
    create_reviewed_registry_dir,
    install_object_storage_publish_stub,
)


def test_cli_can_publish_generated_registry_release(
    tmp_path: Path,
    monkeypatch,
) -> None:
    registry_dir = create_reviewed_registry_dir(tmp_path)
    install_object_storage_publish_stub(
        monkeypatch,
        registry_dir=registry_dir,
        prefix="generated-pack-releases/finance-general-medical-2026-04-10",
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "registry",
            "publish-generated-release",
            str(registry_dir),
            "--prefix",
            "generated-pack-releases/finance-general-medical-2026-04-10",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["bucket"] == "ades-test"
    assert payload["object_count"] == 3
    assert payload["index_storage_uri"].endswith(
        "/generated-pack-releases/finance-general-medical-2026-04-10/index.json"
    )
