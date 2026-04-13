from pathlib import Path

import pytest

from ades.api import lookup_candidates
from ades.storage import UnsupportedRuntimeConfigurationError


def test_api_lookup_rejects_missing_explicit_config(
    tmp_path: Path, monkeypatch
) -> None:
    missing_config = tmp_path / "missing.toml"
    monkeypatch.setenv("ADES_CONFIG_FILE", str(missing_config))

    with pytest.raises(
        FileNotFoundError,
        match=f"ades config file not found: {missing_config}",
    ):
        lookup_candidates("org beta", storage_root=tmp_path)


def test_api_lookup_rejects_invalid_runtime_target(
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
        lookup_candidates("org beta", storage_root=tmp_path)
