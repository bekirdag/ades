import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller
from ades.storage.registry_db import PackMetadataStore


def test_cli_lookup_supports_multi_term_cross_field_search(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["packs", "lookup", "org beta organization"],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["candidates"]
    assert payload["candidates"][0]["kind"] == "alias"
    assert payload["candidates"][0]["value"] == "Org Beta"
    assert payload["candidates"][0]["label"] == "organization"


def test_cli_exact_lookup_does_not_sync_global_search_index(
    tmp_path: Path,
    monkeypatch,
) -> None:
    PackInstaller(tmp_path).install("finance-en")
    runner = CliRunner()

    def fail_sync(self: PackMetadataStore, connection: object) -> None:
        raise AssertionError("exact lookup must not rebuild the global search index")

    monkeypatch.setattr(PackMetadataStore, "_sync_search_index", fail_sync)

    result = runner.invoke(
        app,
        ["packs", "lookup", "TICKA", "--exact-alias"],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0


def test_cli_lookup_supports_explicit_fuzzy_mode(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["packs", "lookup", "org beta", "--fuzzy"],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["fuzzy"] is True
    assert payload["candidates"]
