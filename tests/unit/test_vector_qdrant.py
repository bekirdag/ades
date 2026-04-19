from ades.vector.qdrant import QdrantVectorSearchClient


def test_query_similar_by_id_includes_payload_filter() -> None:
    captured: dict[str, object] = {}
    client = object.__new__(QdrantVectorSearchClient)

    def _fake_request(method: str, path: str, *, json_body=None, params=None):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = json_body
        return {"result": []}

    client._request = _fake_request  # type: ignore[method-assign]

    points = client.query_similar_by_id(
        "ades-qids-current",
        point_id="wikidata:Q1",
        limit=12,
        filter_payload={
            "packs": ["general-en", "finance-en"],
            "entity_type": "organization",
        },
    )

    assert points == []
    assert captured["method"] == "POST"
    assert captured["path"] == "/collections/ades-qids-current/points/query"
    assert captured["json_body"] == {
        "query": "wikidata:Q1",
        "limit": 12,
        "with_payload": True,
        "filter": {
            "must": [
                {"key": "packs", "match": {"any": ["general-en", "finance-en"]}},
                {"key": "entity_type", "match": {"value": "organization"}},
            ]
        },
    }
