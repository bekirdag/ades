from pathlib import Path

import pytest

from ades.config import InvalidConfigurationError, Settings
from ades.storage import (
    MetadataBackend,
    RuntimeTarget,
    UnsupportedRuntimeConfigurationError,
)


def test_settings_can_load_values_from_toml_config(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "ades.toml"
    config_path.write_text(
        "\n".join(
            [
                "[ades]",
                'host = "0.0.0.0"',
                "port = 9843",
                f'storage_root = "{tmp_path / "storage"}"',
                'default_pack = "finance-en"',
                'registry_url = "https://repo.adestool.com/index.json"',
                'runtime_target = "production_server"',
                'metadata_backend = "postgresql"',
                'database_url = "postgresql://ades:secret@127.0.0.1:5432/ades"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))
    monkeypatch.delenv("ADES_HOST", raising=False)
    monkeypatch.delenv("ADES_PORT", raising=False)
    monkeypatch.delenv("ADES_STORAGE_ROOT", raising=False)
    monkeypatch.delenv("ADES_DEFAULT_PACK", raising=False)
    monkeypatch.delenv("ADES_REGISTRY_URL", raising=False)
    monkeypatch.delenv("ADES_RUNTIME_TARGET", raising=False)
    monkeypatch.delenv("ADES_METADATA_BACKEND", raising=False)
    monkeypatch.delenv("ADES_DATABASE_URL", raising=False)

    settings = Settings.from_env()

    assert settings.host == "0.0.0.0"
    assert settings.port == 9843
    assert settings.storage_root == tmp_path / "storage"
    assert settings.default_pack == "finance-en"
    assert settings.registry_url == "https://repo.adestool.com/index.json"
    assert settings.runtime_target is RuntimeTarget.PRODUCTION_SERVER
    assert settings.metadata_backend is MetadataBackend.POSTGRESQL
    assert settings.database_url == "postgresql://ades:secret@127.0.0.1:5432/ades"
    assert settings.config_path == config_path


def test_settings_reject_missing_explicit_config_file(
    tmp_path: Path, monkeypatch
) -> None:
    missing_config = tmp_path / "missing.toml"
    monkeypatch.setenv("ADES_CONFIG_FILE", str(missing_config))

    with pytest.raises(
        FileNotFoundError,
        match=f"ades config file not found: {missing_config}",
    ):
        Settings.from_env()


def test_settings_reject_invalid_runtime_target(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "ades.toml"
    config_path.write_text("", encoding="utf-8")
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("ADES_RUNTIME_TARGET", "broken-runtime")

    with pytest.raises(
        UnsupportedRuntimeConfigurationError,
        match="Unsupported ades runtime target",
    ):
        Settings.from_env()


def test_settings_reject_invalid_explicit_config_syntax(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "ades.toml"
    config_path.write_text("[ades\n", encoding="utf-8")
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))

    with pytest.raises(
        InvalidConfigurationError,
        match="Invalid ades config file",
    ):
        Settings.from_env()


def test_settings_reject_invalid_port_value(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "ades.toml"
    config_path.write_text("port = 'not-a-port'\n", encoding="utf-8")
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))

    with pytest.raises(
        InvalidConfigurationError,
        match="Invalid ades port",
    ):
        Settings.from_env()
