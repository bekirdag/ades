import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.storage.registry_db import PackMetadataStore


def test_cli_status_reports_missing_explicit_config(tmp_path: Path) -> None:
    runner = CliRunner()
    missing_config = tmp_path / "missing.toml"

    result = runner.invoke(
        app,
        ["status"],
        env={"ADES_CONFIG_FILE": str(missing_config)},
    )

    assert result.exit_code == 1
    assert "ades config file not found" in getattr(result, "stderr", result.output)


def test_cli_status_reports_invalid_runtime_target(tmp_path: Path) -> None:
    runner = CliRunner()
    config_path = tmp_path / "ades.toml"
    config_path.write_text("", encoding="utf-8")

    result = runner.invoke(
        app,
        ["status"],
        env={
            "ADES_CONFIG_FILE": str(config_path),
            "ADES_RUNTIME_TARGET": "broken-runtime",
        },
    )

    assert result.exit_code == 1
    assert "Unsupported ades runtime target" in getattr(result, "stderr", result.output)


def test_cli_status_does_not_sync_global_search_index(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()

    def fail_sync(self: PackMetadataStore, connection: object) -> None:
        raise AssertionError("status must not rebuild the global search index")

    monkeypatch.setattr(PackMetadataStore, "_sync_search_index", fail_sync)

    result = runner.invoke(
        app,
        ["status"],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["exact_extraction_backend"] == "compiled_matcher"
    assert payload["operator_lookup_backend"] == "sqlite_search_index"


def test_cli_status_reports_production_postgresql_mode(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()

    class _FakePack:
        pack_id = "general-en"

    class _FakePlan:
        metadata_persistence_backend = type("_Value", (), {"value": "postgresql"})()
        exact_extraction_backend = type("_Value", (), {"value": "compiled_matcher"})()
        operator_lookup_backend = type("_Value", (), {"value": "postgresql_search"})()

    class _FakeRegistry:
        def __init__(self, *args, **kwargs) -> None:
            self.backend_plan = _FakePlan()

        def list_installed_packs(self):
            return [_FakePack()]

    monkeypatch.setattr("ades.api.PackRegistry", _FakeRegistry)

    result = runner.invoke(
        app,
        ["status"],
        env={
            "ADES_STORAGE_ROOT": str(tmp_path),
            "ADES_RUNTIME_TARGET": "production_server",
            "ADES_METADATA_BACKEND": "postgresql",
            "ADES_DATABASE_URL": "postgresql://db.example/ades",
        },
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["runtime_target"] == "production_server"
    assert payload["metadata_backend"] == "postgresql"
    assert payload["metadata_persistence_backend"] == "postgresql"
    assert payload["exact_extraction_backend"] == "compiled_matcher"
    assert payload["operator_lookup_backend"] == "postgresql_search"
    assert payload["installed_packs"] == ["general-en"]
