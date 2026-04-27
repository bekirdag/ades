from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from ades.service.models import TagResponse


def test_tag_file_endpoint_tags_local_documents(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    input_path = tmp_path / "report.html"
    input_path.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/file",
        json={
            "path": str(input_path),
            "pack": "finance-en",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    pairs = {(entity["text"], entity["label"]) for entity in payload["entities"]}

    assert payload["source_path"] == str(input_path.resolve())
    assert payload["content_type"] == "text/html"
    assert ("Org Beta", "organization") in pairs
    assert ("TICKA", "ticker") in pairs
    assert ("EXCHX", "exchange") in pairs


def test_tag_file_endpoint_forwards_domain_and_country_hints(
    tmp_path: Path, monkeypatch
) -> None:
    input_path = tmp_path / "report.txt"
    input_path.write_text("Entity Alpha Holdings moved.", encoding="utf-8")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag_file(
        path,
        *,
        pack=None,
        content_type=None,
        domain_hint=None,
        country_hint=None,
        **kwargs,
    ):
        assert Path(path) == input_path
        assert pack == "finance-en"
        assert content_type is None
        assert domain_hint == "business"
        assert country_hint == "uk"
        assert "registry" in kwargs
        return TagResponse(
            version="0.1.0",
            pack="finance-en",
            pack_version="0.2.0",
            language="en",
            content_type="text/plain",
            entities=[],
            topics=[],
            warnings=[],
            timing_ms=1,
        )

    monkeypatch.setattr("ades.service.app.tag_file", _fake_tag_file)

    response = client.post(
        "/v0/tag/file",
        json={
            "path": str(input_path),
            "pack": "finance-en",
            "options": {
                "domain_hint": "business",
                "country_hint": "uk",
            },
        },
    )

    assert response.status_code == 200
