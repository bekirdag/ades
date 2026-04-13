from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.registry import PackRegistry
from ades.pipeline.hybrid import ProposalSpan
from ades.service.app import create_app
from ades.storage.paths import build_storage_layout, ensure_storage_layout
from tests.pack_registry_helpers import create_pack_source


def _install_endpoint_pack(storage_root: Path) -> str:
    layout = ensure_storage_layout(build_storage_layout(storage_root))
    pack_dir = create_pack_source(
        layout.packs_dir,
        pack_id="hybrid-en",
        domain="general",
        labels=("organization",),
        aliases=(("Entity Alpha Holdings", "organization"),),
        rules=(("entity_alpha_holdings", "organization", r"Entity Alpha Holdings"),),
    )
    PackRegistry(storage_root).sync_pack_from_disk(pack_dir.name)
    return pack_dir.name


def test_tag_endpoint_exposes_metrics_and_debug_options(tmp_path: Path) -> None:
    pack_id = _install_endpoint_pack(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "Entity Alpha Holdings moved.",
            "pack": pack_id,
            "content_type": "text/plain",
            "options": {"debug": True},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == 2
    assert payload["pack_version"] == "0.1.0"
    assert payload["metrics"]["entity_count"] == 1
    assert payload["debug"]["discarded_span_count"] >= 1


def test_tag_endpoint_can_enable_hybrid_linking(tmp_path: Path, monkeypatch) -> None:
    pack_id = _install_endpoint_pack(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path))

    def _proposal_spans(text: str, **_: object) -> list[ProposalSpan]:
        start = text.index("Entity Alpha")
        end = start + len("Entity Alpha")
        return [
            ProposalSpan(
                start=start,
                end=end,
                text=text[start:end],
                label="organization",
                confidence=0.84,
                model_name="patched-proposer",
                model_version="test",
            )
        ]

    monkeypatch.setattr("ades.pipeline.tagger.get_proposal_spans", _proposal_spans)

    response = client.post(
        "/v0/tag",
        json={
            "text": "Entity Alpha moved.",
            "pack": pack_id,
            "content_type": "text/plain",
            "options": {"hybrid": True},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["entities"][0]["text"] == "Entity Alpha"
    assert payload["entities"][0]["provenance"]["match_kind"] == "proposal"


def test_tag_endpoint_rejects_invalid_boolean_options(tmp_path: Path) -> None:
    pack_id = _install_endpoint_pack(tmp_path)
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "Entity Alpha Holdings moved.",
            "pack": pack_id,
            "content_type": "text/plain",
            "options": {"debug": "sometimes"},
        },
    )

    assert response.status_code == 400
    assert "Invalid boolean option for debug." in response.json()["detail"]
