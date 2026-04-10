import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.published_registry_smoke_helpers import (
    create_working_published_registry_dir,
    serve_published_registry_dir,
)


def test_cli_can_smoke_published_registry_release(tmp_path: Path) -> None:
    registry_dir = create_working_published_registry_dir(tmp_path)
    runner = CliRunner()

    with serve_published_registry_dir(registry_dir) as registry_url:
        result = runner.invoke(
            app,
            [
                "registry",
                "smoke-published-release",
                registry_url,
                "--pack-id",
                "finance-en",
            ],
        )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["passed"] is True
    assert payload["pack_count"] == 1
    assert payload["cases"][0]["pack_id"] == "finance-en"
    assert payload["cases"][0]["installed_pack_ids"] == ["finance-en"]
    assert "AAPL" in payload["cases"][0]["matched_entity_texts"]
