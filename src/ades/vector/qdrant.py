"""Minimal Qdrant REST transport for hosted QID enrichment."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
from typing import Any, Iterable, Iterator

import httpx


class QdrantVectorSearchError(RuntimeError):
    """Raised when one Qdrant request fails."""


@dataclass(frozen=True)
class QdrantNearestPoint:
    """One Qdrant nearest-neighbor result."""

    point_id: str
    score: float
    payload: dict[str, Any]


def _chunked(
    values: Iterable[dict[str, Any]],
    *,
    size: int,
) -> Iterator[list[dict[str, Any]]]:
    iterator = iter(values)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            return
        yield chunk


def _match_condition(key: str, value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [
            item
            for item in value
            if isinstance(item, (str, int, float, bool)) and str(item).strip()
        ]
        if not items:
            return None
        return {
            "key": key,
            "match": {
                "any": list(items),
            },
        }
    if isinstance(value, (str, int, float, bool)):
        normalized = str(value).strip() if isinstance(value, str) else value
        if normalized == "":
            return None
        return {
            "key": key,
            "match": {
                "value": normalized,
            },
        }
    return None


def _payload_filter_json(filter_payload: dict[str, object] | None) -> dict[str, Any] | None:
    if not filter_payload:
        return None
    must = [
        condition
        for key, value in filter_payload.items()
        if (condition := _match_condition(key, value)) is not None
    ]
    if not must:
        return None
    return {"must": must}


class QdrantVectorSearchClient:
    """Small sync REST client for the Qdrant operations ades needs."""

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        headers: dict[str, str] = {}
        if api_key:
            headers["api-key"] = api_key
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers=headers,
            timeout=timeout_seconds,
        )

    def __enter__(self) -> "QdrantVectorSearchClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def close(self) -> None:
        self._client.close()

    def _raise_for_status(self, response: httpx.Response) -> None:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:  # pragma: no cover - exercised in tests via wrapper
            detail = ""
            try:
                payload = response.json()
            except ValueError:
                detail = response.text.strip()
            else:
                if isinstance(payload, dict):
                    status = payload.get("status")
                    result = payload.get("result")
                    detail = str(result or status or "").strip()
            if not detail:
                detail = response.text.strip() or str(exc)
            raise QdrantVectorSearchError(
                f"Qdrant request failed ({response.status_code}): {detail}"
            ) from exc

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = self._client.request(method, path, json=json_body, params=params)
        self._raise_for_status(response)
        payload = response.json()
        if not isinstance(payload, dict):
            raise QdrantVectorSearchError("Qdrant returned one non-object response.")
        return payload

    def ensure_collection(
        self,
        collection_name: str,
        *,
        dimensions: int,
    ) -> None:
        response = self._client.get(f"/collections/{collection_name}")
        if response.status_code == 200:
            return
        if response.status_code != 404:
            self._raise_for_status(response)
            return
        self._request(
            "PUT",
            f"/collections/{collection_name}",
            json_body={
                "vectors": {
                    "size": dimensions,
                    "distance": "Cosine",
                },
                "on_disk_payload": True,
            },
        )

    def upsert_points(
        self,
        collection_name: str,
        points: Iterable[dict[str, Any]],
        *,
        batch_size: int = 256,
    ) -> None:
        for chunk in _chunked(points, size=batch_size):
            self._request(
                "PUT",
                f"/collections/{collection_name}/points",
                params={"wait": "true"},
                json_body={"points": chunk},
            )

    def list_aliases(self) -> dict[str, str]:
        payload = self._request("GET", "/aliases")
        result = payload.get("result")
        aliases = result.get("aliases") if isinstance(result, dict) else None
        mapping: dict[str, str] = {}
        if not isinstance(aliases, list):
            return mapping
        for item in aliases:
            if not isinstance(item, dict):
                continue
            alias_name = item.get("alias_name")
            collection_name = item.get("collection_name")
            if isinstance(alias_name, str) and isinstance(collection_name, str):
                mapping[alias_name] = collection_name
        return mapping

    def set_alias(self, alias_name: str, collection_name: str) -> None:
        existing = self.list_aliases().get(alias_name)
        actions: list[dict[str, Any]] = []
        if existing == collection_name:
            return
        if existing is not None:
            actions.append({"delete_alias": {"alias_name": alias_name}})
        actions.append(
            {
                "create_alias": {
                    "collection_name": collection_name,
                    "alias_name": alias_name,
                }
            }
        )
        self._request("POST", "/collections/aliases", json_body={"actions": actions})

    def query_similar_by_id(
        self,
        collection_name: str,
        *,
        point_id: str,
        limit: int,
        filter_payload: dict[str, object] | None = None,
    ) -> list[QdrantNearestPoint]:
        request_body: dict[str, Any] = {
            "query": point_id,
            "limit": limit,
            "with_payload": True,
        }
        payload_filter = _payload_filter_json(filter_payload)
        if payload_filter is not None:
            request_body["filter"] = payload_filter
        payload = self._request(
            "POST",
            f"/collections/{collection_name}/points/query",
            json_body=request_body,
        )
        result = payload.get("result")
        raw_points = result.get("points") if isinstance(result, dict) else result
        if not isinstance(raw_points, list):
            return []
        points: list[QdrantNearestPoint] = []
        for item in raw_points:
            if not isinstance(item, dict):
                continue
            raw_id = item.get("id")
            if not isinstance(raw_id, (str, int)):
                continue
            payload_item = item.get("payload")
            score = item.get("score")
            points.append(
                QdrantNearestPoint(
                    point_id=str(raw_id),
                    score=float(score or 0.0),
                    payload=payload_item if isinstance(payload_item, dict) else {},
                )
            )
        return points
