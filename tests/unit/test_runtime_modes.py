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
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_ENABLED", "true")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_ARTIFACT_PATH", "/tmp/qid-graph-store.sqlite")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_RELATED_LIMIT", "6")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_SEED_NEIGHBOR_LIMIT", "12")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_VECTOR_PROPOSALS_ENABLED", "true")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_VECTOR_PROPOSAL_LIMIT", "9")

    settings = Settings.from_env()

    assert settings.storage_root == Path("/tmp/ades-unit-test")
    assert settings.runtime_target is RuntimeTarget.LOCAL
    assert settings.metadata_backend is MetadataBackend.SQLITE
    assert settings.vector_search_enabled is True
    assert settings.vector_search_url == "http://qdrant.local:6333"
    assert settings.vector_search_related_limit == 8
    assert settings.graph_context_enabled is True
    assert settings.graph_context_artifact_path == Path("/tmp/qid-graph-store.sqlite")
    assert settings.graph_context_related_limit == 6
    assert settings.graph_context_seed_neighbor_limit == 12
    assert settings.graph_context_vector_proposals_enabled is True
    assert settings.graph_context_vector_proposal_limit == 9


def test_production_service_requires_explicit_runtime(monkeypatch) -> None:
    monkeypatch.delenv("ADES_RUNTIME_TARGET", raising=False)
    monkeypatch.delenv("ADES_METADATA_BACKEND", raising=False)
    monkeypatch.delenv("ADES_DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError):
        create_production_app(storage_root=os.getcwd())


def test_public_api_settings_resolution_preserves_hybrid_graph_context_flags(
    monkeypatch,
) -> None:
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_VECTOR_PROPOSALS_ENABLED", "true")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_VECTOR_PROPOSAL_LIMIT", "11")

    settings = _resolve_settings(storage_root="/tmp/ades-unit-test")

    assert settings.graph_context_vector_proposals_enabled is True
    assert settings.graph_context_vector_proposal_limit == 11


def test_settings_apply_finance_domain_profile(monkeypatch) -> None:
    monkeypatch.setenv("ADES_STORAGE_ROOT", "/tmp/ades-unit-test")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_ENABLED", "true")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_URL", "http://qdrant.local:6333")
    monkeypatch.setenv("ADES_GRAPH_CONTEXT_ENABLED", "true")
    monkeypatch.setenv(
        "ADES_GRAPH_CONTEXT_ARTIFACT_PATH",
        "/tmp/qid-graph-store.sqlite",
    )

    settings = _resolve_settings(
        storage_root="/tmp/ades-unit-test",
        domain_hint="finance",
        pack="general-en",
    )

    assert settings.retrieval_profile_name == "finance"
    assert settings.vector_search_collection_alias == "ades-qids-finance-current"
    assert "finance-en" in settings.retrieval_profile_pack_ids
    assert "general-en" in settings.retrieval_profile_pack_ids


def test_settings_apply_explicit_retrieval_profile(monkeypatch) -> None:
    monkeypatch.setenv("ADES_STORAGE_ROOT", "/tmp/ades-unit-test")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_ENABLED", "true")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_URL", "http://qdrant.local:6333")

    settings = _resolve_settings(
        storage_root="/tmp/ades-unit-test",
        retrieval_profile="politics",
        pack="general-en",
    )

    assert settings.retrieval_profile_name == "politics"
    assert settings.default_pack == "politics-vector-en"
    assert settings.vector_search_collection_alias == "ades-qids-politics-current"
    assert settings.retrieval_profile_pack_ids == ("politics-vector-en",)


def test_settings_apply_finance_politics_retrieval_profile(monkeypatch) -> None:
    monkeypatch.setenv("ADES_STORAGE_ROOT", "/tmp/ades-unit-test")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_ENABLED", "true")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_URL", "http://qdrant.local:6333")

    settings = _resolve_settings(
        storage_root="/tmp/ades-unit-test",
        retrieval_profile="finance_politics",
        pack="general-en",
    )

    assert settings.retrieval_profile_name == "finance_politics"
    assert settings.vector_search_collection_alias == "ades-qids-finance-politics-current"
    assert "finance-en" in settings.retrieval_profile_pack_ids
    assert "politics-vector-en" in settings.retrieval_profile_pack_ids


def test_settings_apply_business_domain_profile(monkeypatch) -> None:
    monkeypatch.setenv("ADES_STORAGE_ROOT", "/tmp/ades-unit-test")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_ENABLED", "true")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_URL", "http://qdrant.local:6333")

    settings = _resolve_settings(
        storage_root="/tmp/ades-unit-test",
        domain_hint="business",
        pack="general-en",
    )

    assert settings.retrieval_profile_name == "business"
    assert settings.default_pack == "general-en"
    assert settings.vector_search_collection_alias == "ades-qids-business-current"
    assert settings.retrieval_profile_pack_ids == ("business-vector-en",)


def test_settings_apply_economics_domain_profile(monkeypatch) -> None:
    monkeypatch.setenv("ADES_STORAGE_ROOT", "/tmp/ades-unit-test")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_ENABLED", "true")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_URL", "http://qdrant.local:6333")

    settings = _resolve_settings(
        storage_root="/tmp/ades-unit-test",
        domain_hint="economics",
        pack="general-en",
    )

    assert settings.retrieval_profile_name == "economics"
    assert settings.default_pack == "general-en"
    assert settings.vector_search_collection_alias == "ades-qids-economics-current"
    assert settings.retrieval_profile_pack_ids == ("economics-vector-en",)


def test_settings_infer_business_profile_from_pack(monkeypatch) -> None:
    monkeypatch.setenv("ADES_STORAGE_ROOT", "/tmp/ades-unit-test")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_ENABLED", "true")
    monkeypatch.setenv("ADES_VECTOR_SEARCH_URL", "http://qdrant.local:6333")

    settings = _resolve_settings(
        storage_root="/tmp/ades-unit-test",
        pack="business-vector-en",
    )

    assert settings.retrieval_profile_name == "business"
    assert settings.default_pack == "general-en"
    assert settings.vector_search_collection_alias == "ades-qids-business-current"


def test_settings_reject_unknown_domain_hint(monkeypatch) -> None:
    monkeypatch.setenv("ADES_STORAGE_ROOT", "/tmp/ades-unit-test")

    with pytest.raises(ValueError):
        _resolve_settings(
            storage_root="/tmp/ades-unit-test",
            domain_hint="sports",
            pack="general-en",
        )


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
