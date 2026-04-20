from ades.vector.qdrant import QdrantVectorSearchClient, _qdrant_point_id


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
        "query": _qdrant_point_id("wikidata:Q1"),
        "limit": 12,
        "with_payload": True,
        "filter": {
            "must": [
                {"key": "packs", "match": {"any": ["general-en", "finance-en"]}},
                {"key": "entity_type", "match": {"value": "organization"}},
            ]
        },
    }


def test_upsert_points_normalizes_string_point_ids() -> None:
    captured: dict[str, object] = {}
    client = object.__new__(QdrantVectorSearchClient)

    def _fake_request(method: str, path: str, *, json_body=None, params=None):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = json_body
        captured["params"] = params
        return {"result": []}

    client._request = _fake_request  # type: ignore[method-assign]

    client.upsert_points(
        "ades-qids-current",
        [
            {
                "id": "wikidata:Q1",
                "vector": [0.1, 0.2],
                "payload": {
                    "canonical_text": "Anthropic",
                    "packs": ["general-en"],
                },
            }
        ],
        batch_size=8,
    )

    assert captured["method"] == "PUT"
    assert captured["path"] == "/collections/ades-qids-current/points"
    assert captured["params"] == {"wait": "true"}
    assert captured["json_body"] == {
        "points": [
            {
                "id": _qdrant_point_id("wikidata:Q1"),
                "vector": [0.1, 0.2],
                "payload": {
                    "entity_id": "wikidata:Q1",
                    "canonical_text": "Anthropic",
                    "packs": ["general-en"],
                },
            }
        ]
    }


def test_query_similar_by_id_prefers_payload_entity_id() -> None:
    client = object.__new__(QdrantVectorSearchClient)

    def _fake_request(method: str, path: str, *, json_body=None, params=None):
        return {
            "result": {
                "points": [
                    {
                        "id": _qdrant_point_id("wikidata:Q1"),
                        "score": 0.91,
                        "payload": {
                            "entity_id": "wikidata:Q1",
                            "canonical_text": "Anthropic",
                        },
                    }
                ]
            }
        }

    client._request = _fake_request  # type: ignore[method-assign]

    points = client.query_similar_by_id(
        "ades-qids-current",
        point_id="wikidata:Q1",
        limit=4,
    )

    assert len(points) == 1
    assert points[0].point_id == "wikidata:Q1"
    assert points[0].payload == {
        "entity_id": "wikidata:Q1",
        "canonical_text": "Anthropic",
    }
