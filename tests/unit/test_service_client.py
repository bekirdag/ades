import io
import json
from pathlib import Path
import urllib.error

import pytest

from ades.config import DEFAULT_STORAGE_ROOT, Settings
from ades.service import client as client_module
from ades.service.client import (
    SERVICE_MODE_ENV,
    LocalServiceUnavailableError,
    should_use_local_service,
    tag_file_via_local_service,
    tag_via_local_service,
)
from ades.storage.backend import MetadataBackend, RuntimeTarget


class _DummyResponse:
    def __init__(self, payload: object, *, status: int = 200) -> None:
        self._payload = payload
        self.status = status

    def __enter__(self) -> "_DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def _production_settings(storage_root: Path) -> Settings:
    return Settings(
        host="127.0.0.1",
        port=8734,
        storage_root=storage_root,
        default_pack="general-en",
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
    )


def test_should_use_local_service_respects_runtime_target_and_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local_settings = Settings(storage_root=DEFAULT_STORAGE_ROOT)
    production_settings = _production_settings(DEFAULT_STORAGE_ROOT)
    custom_root_settings = Settings(storage_root=tmp_path)

    monkeypatch.delenv(SERVICE_MODE_ENV, raising=False)
    assert should_use_local_service(local_settings) is True
    assert should_use_local_service(production_settings) is True
    assert should_use_local_service(custom_root_settings) is False

    monkeypatch.setenv(SERVICE_MODE_ENV, "in_process")
    assert should_use_local_service(local_settings) is False
    assert should_use_local_service(production_settings) is False
    assert should_use_local_service(custom_root_settings) is False

    monkeypatch.setenv(SERVICE_MODE_ENV, "service")
    assert should_use_local_service(local_settings) is True
    assert should_use_local_service(custom_root_settings) is True


def test_tag_via_local_service_serializes_request_and_parses_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _production_settings(tmp_path)
    captured: dict[str, object] = {}

    monkeypatch.setattr(client_module, "ensure_local_service", lambda *args, **kwargs: None)

    def fake_urlopen(request, timeout: float):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        return _DummyResponse(
            {
                "version": "0.1.0",
                "pack": "finance-en",
                "pack_version": "1.2.3",
                "language": "en",
                "content_type": "text/plain",
                "entities": [],
                "topics": [],
                "warnings": [],
                "timing_ms": 12,
            }
        )

    monkeypatch.setattr(client_module.urllib.request, "urlopen", fake_urlopen)

    response = tag_via_local_service(
        "Org Beta said TICKA rallied.",
        pack="finance-en",
        content_type="text/plain",
        output_dir=tmp_path / "outputs",
        pretty_output=False,
        settings=settings,
        debug=True,
    )

    assert response.pack == "finance-en"
    assert response.pack_version == "1.2.3"
    assert captured["url"] == "http://127.0.0.1:8734/v0/tag"
    assert captured["timeout"] == 60.0
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["options"] == {"debug": True}
    assert payload["output"] == {
        "directory": str((tmp_path / "outputs").expanduser()),
        "pretty": False,
        "write_manifest": False,
    }


def test_tag_via_local_service_maps_missing_resource_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _production_settings(tmp_path)
    missing_message = "File not found: /tmp/missing.html"

    monkeypatch.setattr(client_module, "ensure_local_service", lambda *args, **kwargs: None)

    def fake_urlopen(request, timeout: float):
        raise urllib.error.HTTPError(
            request.full_url,
            404,
            "Not Found",
            hdrs=None,
            fp=io.BytesIO(json.dumps({"detail": missing_message}).encode("utf-8")),
        )

    monkeypatch.setattr(client_module.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(FileNotFoundError, match=missing_message):
        tag_via_local_service(
            "Org Beta said TICKA rallied.",
            pack="finance-en",
            settings=settings,
        )


def test_spawn_local_service_uses_package_module_entrypoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _production_settings(tmp_path)
    captured: dict[str, object] = {}

    class _DummyProcess:
        pid = 12345

    def fake_popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return _DummyProcess()

    monkeypatch.setattr(client_module.subprocess, "Popen", fake_popen)

    process = client_module._spawn_local_service(settings)

    assert process.pid == 12345
    assert captured["command"] == [
        client_module.sys.executable,
        "-m",
        "ades",
        "serve",
        "--host",
        "127.0.0.1",
        "--port",
        "8734",
    ]


def test_tag_via_local_service_falls_back_to_in_process_in_auto_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _production_settings(tmp_path)
    fallback_response = {
        "version": "0.1.0",
        "pack": "general-en",
        "pack_version": "0.2.0",
        "language": "en",
        "content_type": "text/plain",
        "entities": [],
        "topics": [],
        "warnings": [],
        "timing_ms": 7,
    }
    captured: dict[str, object] = {}

    monkeypatch.delenv(SERVICE_MODE_ENV, raising=False)
    monkeypatch.setattr(client_module, "ensure_local_service", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        client_module,
        "_request_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(LocalServiceUnavailableError("timed out")),
    )

    def fake_api_tag(text: str, **kwargs):
        captured["text"] = text
        captured["kwargs"] = kwargs
        return client_module.TagResponse.model_validate(fallback_response)

    monkeypatch.setattr("ades.api.tag", fake_api_tag)

    response = tag_via_local_service(
        "Orbán stepped down after the election.",
        pack="general-en",
        settings=settings,
        hybrid=True,
        include_related_entities=True,
        include_graph_support=True,
        domain_hint="politics",
    )

    assert response.timing_ms == 7
    assert captured["text"] == "Orbán stepped down after the election."
    assert captured["kwargs"] == {
        "pack": "general-en",
        "content_type": "text/plain",
        "output_path": None,
        "output_dir": None,
        "pretty_output": True,
        "debug": False,
        "hybrid": True,
        "include_related_entities": True,
        "include_graph_support": True,
        "refine_links": False,
        "refinement_depth": "light",
        "domain_hint": "politics",
        "country_hint": None,
    }


def test_tag_via_local_service_does_not_fallback_in_explicit_service_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _production_settings(tmp_path)
    monkeypatch.setenv(SERVICE_MODE_ENV, "service")
    monkeypatch.setattr(client_module, "ensure_local_service", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        client_module,
        "_request_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(LocalServiceUnavailableError("timed out")),
    )

    with pytest.raises(LocalServiceUnavailableError, match="timed out"):
        tag_via_local_service("service only", pack="general-en", settings=settings)


def test_tag_file_via_local_service_falls_back_to_in_process_in_auto_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _production_settings(tmp_path)
    article_path = tmp_path / "article.txt"
    article_path.write_text("Orbán stepped down after the election.", encoding="utf-8")
    fallback_response = {
        "version": "0.1.0",
        "pack": "general-en",
        "pack_version": "0.2.0",
        "language": "en",
        "content_type": "text/plain",
        "entities": [],
        "topics": [],
        "warnings": [],
        "timing_ms": 9,
    }
    captured: dict[str, object] = {}

    monkeypatch.delenv(SERVICE_MODE_ENV, raising=False)
    monkeypatch.setattr(client_module, "ensure_local_service", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        client_module,
        "_request_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(LocalServiceUnavailableError("timed out")),
    )

    def fake_api_tag_file(path: Path, **kwargs):
        captured["path"] = path
        captured["kwargs"] = kwargs
        return client_module.TagResponse.model_validate(fallback_response)

    monkeypatch.setattr("ades.api.tag_file", fake_api_tag_file)

    response = tag_file_via_local_service(
        article_path,
        pack="general-en",
        settings=settings,
        include_graph_support=True,
    )

    assert response.timing_ms == 9
    assert captured["path"] == article_path
    assert captured["kwargs"] == {
        "pack": "general-en",
        "content_type": None,
        "output_path": None,
        "output_dir": None,
        "pretty_output": True,
        "debug": False,
        "hybrid": None,
        "include_related_entities": False,
        "include_graph_support": True,
        "refine_links": False,
        "refinement_depth": "light",
        "domain_hint": None,
        "country_hint": None,
    }
