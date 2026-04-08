"""API models for the local service."""

from __future__ import annotations

from typing import Literal
from typing import Any

from pydantic import BaseModel, Field, model_validator


class PackSummary(BaseModel):
    """Installed pack metadata exposed by the service."""

    pack_id: str
    version: str
    language: str
    domain: str
    tier: str
    active: bool = True


class StatusResponse(BaseModel):
    """Basic runtime status."""

    service: str
    version: str
    runtime_target: str
    metadata_backend: str
    storage_root: str
    host: str
    port: int
    installed_packs: list[str] = Field(default_factory=list)


class EntityProvenance(BaseModel):
    """Deterministic provenance for a single entity match."""

    match_kind: Literal["alias", "rule"]
    match_path: str
    match_source: str
    source_pack: str
    source_domain: str


class EntityLink(BaseModel):
    """Lightweight local link metadata for a matched entity."""

    entity_id: str
    canonical_text: str
    provider: str


class EntityMatch(BaseModel):
    """Single extracted entity."""

    text: str
    label: str
    start: int
    end: int
    confidence: float | None = None
    provenance: EntityProvenance | None = None
    link: EntityLink | None = None


class TopicMatch(BaseModel):
    """Single assigned topic."""

    label: str
    score: float | None = None


class TagOutputOptions(BaseModel):
    """Optional local JSON persistence settings for tag responses."""

    path: str | None = None
    directory: str | None = None
    pretty: bool = True


class TagRequest(BaseModel):
    """Request body for tagging."""

    text: str
    pack: str
    content_type: str = "text/plain"
    output: TagOutputOptions | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class FileTagRequest(BaseModel):
    """Request body for local file tagging."""

    path: str
    pack: str
    content_type: str | None = None
    output: TagOutputOptions | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class BatchFileTagRequest(BaseModel):
    """Request body for local batch file tagging."""

    paths: list[str] = Field(default_factory=list)
    directories: list[str] = Field(default_factory=list)
    glob_patterns: list[str] = Field(default_factory=list)
    include_patterns: list[str] = Field(default_factory=list)
    exclude_patterns: list[str] = Field(default_factory=list)
    recursive: bool = True
    pack: str
    content_type: str | None = None
    output: TagOutputOptions | None = None
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_sources(self) -> "BatchFileTagRequest":
        if self.paths or self.directories or self.glob_patterns:
            return self
        raise ValueError("At least one of paths, directories, or glob_patterns is required.")


class TagResponse(BaseModel):
    """Response body for tagging."""

    version: str
    pack: str
    language: str
    content_type: str
    source_path: str | None = None
    saved_output_path: str | None = None
    entities: list[EntityMatch] = Field(default_factory=list)
    topics: list[TopicMatch] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    timing_ms: int


class BatchSourceSummary(BaseModel):
    """Discovery and filtering summary for a local corpus tagging run."""

    explicit_path_count: int
    directory_match_count: int
    glob_match_count: int
    discovered_count: int
    included_count: int
    excluded_count: int
    duplicate_count: int
    generated_output_skipped_count: int
    recursive: bool
    include_patterns: list[str] = Field(default_factory=list)
    exclude_patterns: list[str] = Field(default_factory=list)


class BatchTagResponse(BaseModel):
    """Response body for local batch tagging."""

    pack: str
    item_count: int
    summary: BatchSourceSummary
    items: list[TagResponse] = Field(default_factory=list)


class LookupCandidate(BaseModel):
    """Single deterministic candidate returned from metadata lookup."""

    kind: Literal["alias", "rule"]
    pack_id: str
    label: str
    value: str
    pattern: str | None = None
    domain: str
    active: bool = True
    score: float | None = None


class LookupResponse(BaseModel):
    """Response body for metadata lookup."""

    query: str
    pack_id: str | None = None
    exact_alias: bool = False
    active_only: bool = True
    candidates: list[LookupCandidate] = Field(default_factory=list)
