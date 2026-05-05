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


def test_settings_can_load_vector_routing_maps_from_toml_config(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "ades.toml"
    config_path.write_text(
        "\n".join(
            [
                "[ades]",
                '[ades.vector_search_domain_pack_routes]',
                'business = "business-custom-en"',
                '[ades.vector_search_pack_collection_aliases]',
                'business-custom-en = "ades-qids-business-custom-current"',
                '[ades.vector_search_country_aliases]',
                'great-britain = "uk"',
                'south-korea = "kr"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))

    settings = Settings.from_env()

    assert settings.vector_search_domain_pack_routes == {
        "business": "business-custom-en",
    }
    assert settings.vector_search_pack_collection_aliases == {
        "business-custom-en": "ades-qids-business-custom-current",
    }
    assert settings.vector_search_country_aliases == {
        "great-britain": "uk",
        "south-korea": "kr",
    }


def test_settings_can_load_impact_expansion_from_env(tmp_path: Path, monkeypatch) -> None:
    artifact_path = tmp_path / "market_graph_store.sqlite"
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ENABLED", "true")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_ARTIFACT_PATH", str(artifact_path))
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_MAX_DEPTH", "2")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_SEED_LIMIT", "12")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_MAX_CANDIDATES", "30")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_MAX_EDGES_PER_SEED", "40")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_MAX_PATHS_PER_CANDIDATE", "2")
    monkeypatch.setenv("ADES_IMPACT_EXPANSION_VECTOR_PROPOSALS_ENABLED", "false")

    settings = Settings.from_env()

    assert settings.impact_expansion_enabled is True
    assert settings.impact_expansion_artifact_path == artifact_path
    assert settings.impact_expansion_max_depth == 2
    assert settings.impact_expansion_seed_limit == 12
    assert settings.impact_expansion_max_candidates == 30
    assert settings.impact_expansion_max_edges_per_seed == 40
    assert settings.impact_expansion_max_paths_per_candidate == 2
    assert settings.impact_expansion_vector_proposals_enabled is False


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


def test_settings_reject_invalid_vector_country_aliases_json(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "ades.toml"
    config_path.write_text("", encoding="utf-8")
    monkeypatch.setenv("ADES_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("ADES_VECTOR_SEARCH_COUNTRY_ALIASES", "not-json")

    with pytest.raises(
        InvalidConfigurationError,
        match="Invalid ades vector search country aliases",
    ):
        Settings.from_env()
