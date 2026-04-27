"""Recent-news support artifact helpers for hybrid related-entity gating."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from threading import Lock
from typing import Any

_RECENT_NEWS_SOURCE_OUTLET_EXACT = {
    "associated press",
    "bbc",
    "guardian",
    "mailonline",
    "radio 4",
    "wikimedia commons",
}
_RECENT_NEWS_SOURCE_OUTLET_SUFFIXES = (
    " online",
    " times",
    " telegraph",
    " standard",
    " news",
)


def is_probable_source_outlet_candidate(
    canonical_text: str,
    *,
    candidate_type: str | None,
) -> bool:
    """Return True when recent-news support should treat the entity as outlet noise."""

    if str(candidate_type or "").strip().casefold() != "organization":
        return False
    normalized = re.sub(r"\s+", " ", canonical_text.casefold()).strip()
    if not normalized:
        return False
    normalized_variants = {normalized}
    if normalized.startswith("the "):
        stripped = normalized.removeprefix("the ").strip()
        if stripped:
            normalized_variants.add(stripped)
    if any(value in _RECENT_NEWS_SOURCE_OUTLET_EXACT for value in normalized_variants):
        return True
    return any(
        value.endswith(suffix)
        for value in normalized_variants
        for suffix in _RECENT_NEWS_SOURCE_OUTLET_SUFFIXES
    )


@dataclass(frozen=True)
class RecentNewsEntityMetadata:
    """Compact per-QID metadata stored in the recent-news support artifact."""

    entity_id: str
    canonical_text: str
    entity_type: str | None = None
    source_name: str | None = None
    document_count: int = 0


@dataclass(frozen=True)
class RecentNewsCandidateSupport:
    """Support evidence for one candidate against the current strong seeds."""

    entity_id: str
    supporting_seed_ids: tuple[str, ...]
    pair_support_counts: dict[str, int]
    shared_seed_count: int
    total_support_count: int
    max_pair_count: int


@dataclass(frozen=True)
class _ArtifactCacheKey:
    path: str
    modified_time_ns: int


class RecentNewsSupportArtifact:
    """In-memory view over one recent-news co-occurrence artifact."""

    def __init__(
        self,
        *,
        schema_version: int,
        generated_at: str | None,
        article_count: int,
        qid_metadata: dict[str, RecentNewsEntityMetadata],
        cooccurrence: dict[str, dict[str, int]],
    ) -> None:
        self.schema_version = int(schema_version)
        self.generated_at = generated_at
        self.article_count = max(int(article_count), 0)
        self._qid_metadata = qid_metadata
        self._cooccurrence = cooccurrence

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "RecentNewsSupportArtifact":
        raw_metadata = payload.get("qid_metadata")
        metadata: dict[str, RecentNewsEntityMetadata] = {}
        if isinstance(raw_metadata, dict):
            for raw_entity_id, raw_item in raw_metadata.items():
                entity_id = str(raw_entity_id or "").strip()
                if not entity_id.startswith("wikidata:Q") or not isinstance(raw_item, dict):
                    continue
                metadata[entity_id] = RecentNewsEntityMetadata(
                    entity_id=entity_id,
                    canonical_text=str(raw_item.get("canonical_text") or entity_id).strip()
                    or entity_id,
                    entity_type=(
                        str(raw_item.get("entity_type")).strip()
                        if raw_item.get("entity_type") is not None
                        else None
                    ),
                    source_name=(
                        str(raw_item.get("source_name")).strip()
                        if raw_item.get("source_name") is not None
                        else None
                    ),
                    document_count=max(int(raw_item.get("document_count") or 0), 0),
                )

        raw_cooccurrence = payload.get("cooccurrence")
        cooccurrence: dict[str, dict[str, int]] = {}
        if isinstance(raw_cooccurrence, dict):
            for raw_seed_id, raw_neighbors in raw_cooccurrence.items():
                seed_id = str(raw_seed_id or "").strip()
                if not seed_id.startswith("wikidata:Q") or not isinstance(raw_neighbors, dict):
                    continue
                normalized_neighbors: dict[str, int] = {}
                for raw_candidate_id, raw_count in raw_neighbors.items():
                    candidate_id = str(raw_candidate_id or "").strip()
                    if not candidate_id.startswith("wikidata:Q") or candidate_id == seed_id:
                        continue
                    try:
                        count = int(raw_count)
                    except (TypeError, ValueError):
                        continue
                    if count <= 0:
                        continue
                    normalized_neighbors[candidate_id] = count
                if normalized_neighbors:
                    cooccurrence[seed_id] = normalized_neighbors

        return cls(
            schema_version=int(payload.get("schema_version") or 1),
            generated_at=(
                str(payload.get("generated_at")).strip()
                if payload.get("generated_at") is not None
                else None
            ),
            article_count=max(int(payload.get("article_count") or 0), 0),
            qid_metadata=metadata,
            cooccurrence=cooccurrence,
        )

    def metadata_for(self, entity_id: str) -> RecentNewsEntityMetadata | None:
        return self._qid_metadata.get(entity_id)

    def support_for_candidate(
        self,
        candidate_entity_id: str,
        strong_seed_entity_ids: list[str] | tuple[str, ...],
        *,
        min_supporting_seeds: int = 2,
        min_pair_count: int = 1,
    ) -> RecentNewsCandidateSupport | None:
        candidate_id = str(candidate_entity_id or "").strip()
        if not candidate_id.startswith("wikidata:Q"):
            return None
        unique_seed_ids = tuple(
            dict.fromkeys(
                seed_entity_id
                for seed_entity_id in strong_seed_entity_ids
                if isinstance(seed_entity_id, str)
                and seed_entity_id.startswith("wikidata:Q")
                and seed_entity_id != candidate_id
            )
        )
        if len(unique_seed_ids) < min_supporting_seeds:
            return None
        pair_support_counts: dict[str, int] = {}
        for seed_entity_id in unique_seed_ids:
            count = int(
                self._cooccurrence.get(seed_entity_id, {}).get(candidate_id, 0)
            )
            if count < min_pair_count:
                continue
            pair_support_counts[seed_entity_id] = count
        if len(pair_support_counts) < min_supporting_seeds:
            return None
        return RecentNewsCandidateSupport(
            entity_id=candidate_id,
            supporting_seed_ids=tuple(
                seed_entity_id
                for seed_entity_id in unique_seed_ids
                if seed_entity_id in pair_support_counts
            ),
            pair_support_counts=pair_support_counts,
            shared_seed_count=len(pair_support_counts),
            total_support_count=sum(pair_support_counts.values()),
            max_pair_count=max(pair_support_counts.values()),
        )

    def candidate_supports_for_seeds(
        self,
        strong_seed_entity_ids: list[str] | tuple[str, ...],
        *,
        min_supporting_seeds: int = 2,
        min_pair_count: int = 1,
        limit: int | None = None,
    ) -> list[RecentNewsCandidateSupport]:
        unique_seed_ids = tuple(
            dict.fromkeys(
                seed_entity_id
                for seed_entity_id in strong_seed_entity_ids
                if isinstance(seed_entity_id, str) and seed_entity_id.startswith("wikidata:Q")
            )
        )
        if len(unique_seed_ids) < min_supporting_seeds:
            return []
        candidate_pair_counts: dict[str, dict[str, int]] = {}
        for seed_entity_id in unique_seed_ids:
            for candidate_id, raw_count in self._cooccurrence.get(seed_entity_id, {}).items():
                if candidate_id == seed_entity_id:
                    continue
                count = int(raw_count)
                if count < min_pair_count:
                    continue
                candidate_pair_counts.setdefault(candidate_id, {})[seed_entity_id] = count
        supports: list[RecentNewsCandidateSupport] = []
        for candidate_id, pair_support_counts in candidate_pair_counts.items():
            if len(pair_support_counts) < min_supporting_seeds:
                continue
            supporting_seed_ids = tuple(
                seed_entity_id
                for seed_entity_id in unique_seed_ids
                if seed_entity_id in pair_support_counts
            )
            supports.append(
                RecentNewsCandidateSupport(
                    entity_id=candidate_id,
                    supporting_seed_ids=supporting_seed_ids,
                    pair_support_counts=dict(pair_support_counts),
                    shared_seed_count=len(pair_support_counts),
                    total_support_count=sum(pair_support_counts.values()),
                    max_pair_count=max(pair_support_counts.values()),
                )
            )
        supports.sort(
            key=lambda support: (
                -support.shared_seed_count,
                -support.total_support_count,
                -support.max_pair_count,
                -self.metadata_for(support.entity_id).document_count
                if self.metadata_for(support.entity_id) is not None
                else 0,
                support.entity_id,
            )
        )
        if limit is not None and limit >= 0:
            return supports[:limit]
        return supports


_ARTIFACT_CACHE: dict[_ArtifactCacheKey, RecentNewsSupportArtifact] = {}
_ARTIFACT_CACHE_LOCK = Lock()


def load_recent_news_support_artifact(
    path: str | Path,
) -> RecentNewsSupportArtifact:
    """Load one recent-news support artifact with a small mtime-based cache."""

    resolved_path = Path(path).expanduser().resolve()
    stat = resolved_path.stat()
    cache_key = _ArtifactCacheKey(
        path=str(resolved_path),
        modified_time_ns=stat.st_mtime_ns,
    )
    with _ARTIFACT_CACHE_LOCK:
        cached = _ARTIFACT_CACHE.get(cache_key)
        if cached is not None:
            return cached
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected one JSON object in {resolved_path}.")
    artifact = RecentNewsSupportArtifact.from_payload(payload)
    with _ARTIFACT_CACHE_LOCK:
        stale_keys = [
            key for key in _ARTIFACT_CACHE if key.path == str(resolved_path)
        ]
        for stale_key in stale_keys:
            del _ARTIFACT_CACHE[stale_key]
        _ARTIFACT_CACHE[cache_key] = artifact
    return artifact
