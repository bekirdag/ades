from pathlib import Path

import pytest

from ades.packs.registry import default_registry_url


@pytest.fixture(autouse=True)
def _isolate_runtime_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "ades-test-config.toml"
    config_path.write_text("[ades]\n", encoding="utf-8")
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("ADES_REGISTRY_URL", default_registry_url())
    monkeypatch.setenv("ADES_STORAGE_ROOT", str(tmp_path / "ades-storage"))
