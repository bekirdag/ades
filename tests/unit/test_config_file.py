from pathlib import Path

from ades.config import Settings
from ades.storage import MetadataBackend, RuntimeTarget


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
