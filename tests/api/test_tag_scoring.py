from pathlib import Path
import shutil

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_generation_helpers import (
    create_bundle_backed_general_pack_source,
    create_bundle_backed_general_pack_source_with_alias_collision,
)


def test_tag_endpoint_returns_entity_relevance_and_topic_evidence(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "Org Beta said TICKA traded on EXCHX after USD 12.5 guidance.",
            "pack": "finance-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    entities = {(entity["text"], entity["label"]): entity for entity in payload["entities"]}
    topics = {topic["label"]: topic for topic in payload["topics"]}

    assert 0.0 <= entities[("TICKA", "ticker")]["relevance"] <= 1.0
    assert 0.0 <= entities[("USD 12.5", "currency_amount")]["relevance"] <= 1.0
    assert 0.0 <= entities[("Org Beta", "organization")]["relevance"] <= 1.0
    assert topics["finance"] == {
        "label": "finance",
        "score": topics["finance"]["score"],
        "evidence_count": 3,
        "entity_labels": ["currency_amount", "exchange", "ticker"],
    }


def test_tag_endpoint_can_use_bundle_backed_general_pack_source(tmp_path: Path) -> None:
    pack_dir = create_bundle_backed_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": "Jordan Vale from North Harbor said Beacon expanded.",
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    entities = {(entity["text"], entity["label"]) for entity in payload["entities"]}
    assert ("Jordan Vale", "person") in entities
    assert ("North Harbor", "location") in entities
    assert ("Beacon", "organization") in entities


def test_tag_endpoint_bundle_backed_pack_resolves_alias_collisions(tmp_path: Path) -> None:
    pack_dir = create_bundle_backed_general_pack_source_with_alias_collision(
        tmp_path / "bundle-pack"
    )
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": "Jordan Vale arrived in North Harbor.",
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    entity = next(
        item for item in payload["entities"] if item["text"] == "Jordan Vale" and item["label"] == "person"
    )
    assert entity["link"]["entity_id"] == "person:jordan-elliott-vale-primary"
    assert entity["link"]["canonical_text"] == "Jordan Elliott Vale Prime"
