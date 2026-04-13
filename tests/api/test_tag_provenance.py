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
            "text": "Org Beta said TICKA traded on EXCHX after USD 12.5 guidance.",
            "pack": "finance-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    entities = {(entity["text"], entity["label"]): entity for entity in payload["entities"]}

    organization_entity = entities[("Org Beta", "organization")]
    assert organization_entity["provenance"] == {
        "lane": "deterministic_alias",
        "match_kind": "alias",
        "match_path": "lookup.alias.exact",
        "match_source": "Org Beta",
        "model_name": None,
        "model_version": None,
        "source_pack": "general-en",
        "source_domain": "general",
    }
    assert organization_entity["link"] == {
        "entity_id": "ades:lookup_alias_exact:general-en:organization:org-beta",
        "canonical_text": "Org Beta",
        "provider": "lookup.alias.exact",
    }

    currency = entities[("USD 12.5", "currency_amount")]
    assert currency["provenance"] == {
        "lane": "deterministic_rule",
        "match_kind": "rule",
        "match_path": "rule.regex",
        "match_source": "currency_amount",
        "model_name": "python-re-reviewed",
        "model_version": None,
        "source_pack": "finance-en",
        "source_domain": "finance",
    }
    assert currency["link"] == {
        "entity_id": "ades:rule_regex:finance-en:currency_amount:usd-12-5",
        "canonical_text": "USD 12.5",
        "provider": "rule.regex",
    }
