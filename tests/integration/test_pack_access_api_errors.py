from pathlib import Path

import pytest

from ades import get_pack, pull_pack
from ades.config import InvalidConfigurationError
from ades.storage import UnsupportedRuntimeConfigurationError


def test_public_get_pack_rejects_missing_explicit_config(
    tmp_path: Path, monkeypatch
) -> None:
    missing_config = tmp_path / "missing.toml"
    monkeypatch.setenv("ADES_CONFIG_FILE", str(missing_config))

    with pytest.raises(
        FileNotFoundError,
        match=f"ades config file not found: {missing_config}",
    ):
        get_pack("general-en", storage_root=tmp_path)


def test_public_get_pack_rejects_invalid_runtime_target(
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
        get_pack("general-en", storage_root=tmp_path)


def test_public_pull_rejects_missing_explicit_config(
    tmp_path: Path, monkeypatch
) -> None:
    missing_config = tmp_path / "missing.toml"
    monkeypatch.setenv("ADES_CONFIG_FILE", str(missing_config))

    with pytest.raises(
        FileNotFoundError,
        match=f"ades config file not found: {missing_config}",
    ):
        pull_pack("general-en", storage_root=tmp_path)


def test_public_pull_rejects_invalid_runtime_target(
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
        pull_pack("general-en", storage_root=tmp_path)


def test_public_get_pack_rejects_invalid_port_value(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "ades.toml"
    config_path.write_text("port = 'not-a-port'\n", encoding="utf-8")
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))

    with pytest.raises(
        InvalidConfigurationError,
        match="Invalid ades port",
    ):
        get_pack("general-en", storage_root=tmp_path)


def test_public_pull_rejects_invalid_port_value(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "ades.toml"
    config_path.write_text("port = 'not-a-port'\n", encoding="utf-8")
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))

    with pytest.raises(
        InvalidConfigurationError,
        match="Invalid ades port",
    ):
        pull_pack("general-en", storage_root=tmp_path)
