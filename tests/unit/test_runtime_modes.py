import os
from pathlib import Path

from fastapi import FastAPI
import pytest

from ades.api import _resolve_settings
from ades.config import Settings
from ades.service.production import create_production_app
from ades.storage import MetadataBackend, RuntimeTarget


def test_settings_from_env_resolve_local_runtime_defaults(monkeypatch) -> None:
    monkeypatch.setenv("ADES_STORAGE_ROOT", "/tmp/ades-unit-test")
    monkeypatch.setenv("ADES_RUNTIME_TARGET", "local")
    monkeypatch.setenv("ADES_METADATA_BACKEND", "sqlite")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_ENABLED", "true")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_URL", "http://qdrant.local:6333")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_RELATED_LIMIT", "8")
    monkeypatch.setenv(
        "ADES_VECTOR_SEARCH_DOMAIN_PACK_ROUTES",
        '{"business":"business-custom-en"}',
    )
    monkeypatch.setenv(
        "ADES_VECTOR_SEARCH_PACK_COLLECTION_ALIASES",
        '{"business-custom-en":"ades-qids-business-custom-current"}',
    )
    monkeypatch.setenv(
        "ADES_VECTOR_SEARCH_COUNTRY_ALIASES",
        '{"great-britain":"uk"}',
    )
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_ENABLED", "true")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_ARTIFACT_PATH", "/tmp/qid-graph-store.sqlite")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_RELATED_LIMIT", "6")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_SEED_NEIGHBOR_LIMIT", "12")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_VECTOR_PROPOSALS_ENABLED", "true")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_VECTOR_PROPOSAL_LIMIT", "9")
    monkeypatch.setenv("ADES_NEWS_CONTEXT_ARTIFACT_PATH", "/tmp/recent-news-support.json")
    monkeypatch.setenv("ADES_NEWS_CONTEXT_MIN_SUPPORTING_SEEDS", "3")
    monkeypatch.setenv("ADES_NEWS_CONTEXT_MIN_PAIR_COUNT", "2")

    settings = Settings.from_env()

    assert settings.storage_root == Path("/tmp/ades-unit-test")
    assert settings.runtime_target is RuntimeTarget.LOCAL
    assert settings.metadata_backend is MetadataBackend.SQLITE
    assert settings.vector_search_enabled is True
    assert settings.vector_search_url == "http://qdrant.local:6333"
    assert settings.vector_search_related_limit == 8
    assert settings.vector_search_domain_pack_routes == {
        "business": "business-custom-en",
    }
    assert settings.vector_search_pack_collection_aliases == {
        "business-custom-en": "ades-qids-business-custom-current",
    }
    assert settings.vector_search_country_aliases == {
        "great-britain": "uk",
    }
    assert settings.graph_context_enabled is True
    assert settings.graph_context_artifact_path == Path("/tmp/qid-graph-store.sqlite")
    assert settings.graph_context_related_limit == 6
    assert settings.graph_context_seed_neighbor_limit == 12
    assert settings.graph_context_vector_proposals_enabled is True
    assert settings.graph_context_vector_proposal_limit == 9
    assert settings.news_context_artifact_path == Path("/tmp/recent-news-support.json")
    assert settings.news_context_min_supporting_seeds == 3
    assert settings.news_context_min_pair_count == 2


def test_production_service_requires_explicit_runtime(monkeypatch) -> None:
    monkeypatch.delenv("ADES_RUNTIME_TARGET", raising=False)
    monkeypatch.delenv("ADES_METADATA_BACKEND", raising=False)
    monkeypatch.delenv("ADES_DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError):
        create_production_app(storage_root=os.getcwd())


def test_public_api_settings_resolution_preserves_hybrid_graph_context_flags(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "ADES_VECTOR_SEARCH_DOMAIN_PACK_ROUTES",
        '{"politics":"politics-custom-en"}',
    )
    monkeypatch.setenv(
        "ADES_VECTOR_SEARCH_PACK_COLLECTION_ALIASES",
        '{"politics-custom-en":"ades-qids-politics-custom-current"}',
    )
    monkeypatch.setenv(
        "ADES_VECTOR_SEARCH_COUNTRY_ALIASES",
        '{"united-kingdom":"uk"}',
    )
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_VECTOR_PROPOSALS_ENABLED", "true")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_VECTOR_PROPOSAL_LIMIT", "11")
    monkeypatch.setenv("ADES_NEWS_CONTEXT_ARTIFACT_PATH", "/tmp/recent-news-support.json")
    monkeypatch.setenv("ADES_NEWS_CONTEXT_MIN_SUPPORTING_SEEDS", "4")
    monkeypatch.setenv("ADES_NEWS_CONTEXT_MIN_PAIR_COUNT", "2")

    settings = _resolve_settings(storage_root="/tmp/ades-unit-test")

    assert settings.vector_search_domain_pack_routes == {
        "politics": "politics-custom-en",
    }
    assert settings.vector_search_pack_collection_aliases == {
        "politics-custom-en": "ades-qids-politics-custom-current",
    }
    assert settings.vector_search_country_aliases == {
        "united-kingdom": "uk",
    }
    assert settings.graph_context_vector_proposals_enabled is True
    assert settings.graph_context_vector_proposal_limit == 11
    assert settings.news_context_artifact_path == Path("/tmp/recent-news-support.json")
    assert settings.news_context_min_supporting_seeds == 4
    assert settings.news_context_min_pair_count == 2


def test_production_service_accepts_postgresql_runtime(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def _fake_create_app(*, storage_root):
        captured["storage_root"] = storage_root
        return FastAPI()

    monkeypatch.setenv("ADES_RUNTIME_TARGET", "production_server")
    monkeypatch.setenv("ADES_METADATA_BACKEND", "postgresql")
    monkeypatch.setenv("ADES_DATABASE_URL", "postgresql://db.example/ades")
    monkeypatch.setattr("ades.service.production.create_app", _fake_create_app)

    app = create_production_app(storage_root=tmp_path)

    assert isinstance(app, FastAPI)
    assert captured["storage_root"] == tmp_path
