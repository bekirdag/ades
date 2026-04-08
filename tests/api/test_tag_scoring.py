from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_endpoint_returns_entity_relevance_and_topic_evidence(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "Apple said AAPL traded on NASDAQ after USD 12.5 guidance.",
            "pack": "finance-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    entities = {(entity["text"], entity["label"]): entity for entity in payload["entities"]}
    topics = {topic["label"]: topic for topic in payload["topics"]}

    assert 0.0 <= entities[("AAPL", "ticker")]["relevance"] <= 1.0
    assert 0.0 <= entities[("USD 12.5", "currency_amount")]["relevance"] <= 1.0
    assert 0.0 <= entities[("Apple", "organization")]["relevance"] <= 1.0
    assert topics["finance"] == {
        "label": "finance",
        "score": topics["finance"]["score"],
        "evidence_count": 3,
        "entity_labels": ["currency_amount", "exchange", "ticker"],
    }
