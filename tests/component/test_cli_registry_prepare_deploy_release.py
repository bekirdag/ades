import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.published_registry_smoke_helpers import (
    create_working_published_registry_dir,
    serve_published_registry_dir,
    write_registry_promotion_spec,
)


def test_cli_can_prepare_deploy_release_from_promoted_registry(
    tmp_path: Path,
) -> None:
    registry_dir = create_working_published_registry_dir(tmp_path)
    runner = CliRunner()

    with serve_published_registry_dir(registry_dir) as registry_url:
        promotion_spec_path = write_registry_promotion_spec(
            tmp_path,
            registry_url=registry_url,
        )
        result = runner.invoke(
            app,
            [
                "registry",
                "prepare-deploy-release",
                "--output-dir",
                str(tmp_path / "deploy-registry"),
                "--promotion-spec",
                str(promotion_spec_path),
            ],
        )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["mode"] == "promoted"
    assert payload["promoted_registry_url"] == registry_url
    assert payload["consumer_smoke"]["passed"] is True
    assert payload["pack_count"] == 3
