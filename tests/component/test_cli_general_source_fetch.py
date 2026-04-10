import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.general_bundle_helpers import create_general_remote_sources


def test_cli_can_fetch_general_source_snapshot(tmp_path: Path) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "registry",
            "fetch-general-sources",
            "--output-dir",
            str(tmp_path / "raw" / "general-en"),
            "--snapshot",
            "2026-04-10",
            "--wikidata-url",
            remote_sources["wikidata_url"],
            "--geonames-url",
            remote_sources["geonames_places_url"],
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["pack_id"] == "general-en"
    assert payload["snapshot"] == "2026-04-10"
    assert payload["geonames_location_count"] == 2
    assert Path(payload["source_manifest_path"]).exists()
    assert Path(payload["wikidata_entities_path"]).exists()
