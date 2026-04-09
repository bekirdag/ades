from pathlib import Path

import pytest

from ades import pull_pack, status
from ades.storage import UnsupportedRuntimeConfigurationError


def test_local_runtime_status_reports_local_sqlite_mode(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    runtime_status = status(storage_root=tmp_path)

    assert runtime_status.runtime_target == "local"
    assert runtime_status.metadata_backend == "sqlite"
    assert "finance-en" in runtime_status.installed_packs


def test_local_runtime_status_rejects_missing_explicit_config(
    tmp_path: Path, monkeypatch
) -> None:
    missing_config = tmp_path / "missing.toml"
    monkeypatch.setenv("ADES_CONFIG_FILE", str(missing_config))

    with pytest.raises(
        FileNotFoundError,
        match=f"ades config file not found: {missing_config}",
    ):
        status(storage_root=tmp_path)


def test_local_runtime_status_rejects_invalid_runtime_target(
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
        status(storage_root=tmp_path)
