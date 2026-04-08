import os
from pathlib import Path

import pytest

from ades.config import Settings
from ades.service.production import create_production_app
from ades.storage import MetadataBackend, RuntimeTarget


def test_settings_from_env_resolve_local_runtime_defaults(monkeypatch) -> None:
    monkeypatch.setenv("ADES_STORAGE_ROOT", "/tmp/ades-unit-test")
    monkeypatch.setenv("ADES_RUNTIME_TARGET", "local")
    monkeypatch.setenv("ADES_METADATA_BACKEND", "sqlite")

    settings = Settings.from_env()

    assert settings.storage_root == Path("/tmp/ades-unit-test")
    assert settings.runtime_target is RuntimeTarget.LOCAL
    assert settings.metadata_backend is MetadataBackend.SQLITE


def test_production_service_requires_explicit_runtime(monkeypatch) -> None:
    monkeypatch.delenv("ADES_RUNTIME_TARGET", raising=False)
    monkeypatch.delenv("ADES_METADATA_BACKEND", raising=False)
    monkeypatch.delenv("ADES_DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError):
        create_production_app(storage_root=os.getcwd())
