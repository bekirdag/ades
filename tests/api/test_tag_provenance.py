from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_endpoint_returns_provenance_and_link_metadata(tmp_path: Path) -> None:
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

    apple = entities[("Apple", "organization")]
    assert apple["provenance"] == {
        "match_kind": "alias",
        "match_path": "lookup.alias.exact",
        "match_source": "Apple",
        "source_pack": "general-en",
        "source_domain": "general",
    }
    assert apple["link"] == {
        "entity_id": "ades:lookup_alias_exact:general-en:organization:apple",
        "canonical_text": "Apple",
        "provider": "lookup.alias.exact",
    }

    currency = entities[("USD 12.5", "currency_amount")]
    assert currency["provenance"] == {
        "match_kind": "rule",
        "match_path": "rule.regex",
        "match_source": "currency_amount",
        "source_pack": "finance-en",
        "source_domain": "finance",
    }
    assert currency["link"] == {
        "entity_id": "ades:rule_regex:finance-en:currency_amount:usd-12-5",
        "canonical_text": "USD 12.5",
        "provider": "rule.regex",
    }
