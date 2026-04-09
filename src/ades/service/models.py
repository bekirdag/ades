"""API models for the local service."""

from __future__ import annotations

from datetime import datetime
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
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    active: bool = True


class AvailablePackSummary(BaseModel):
    """Installable pack metadata exposed from a registry."""

    pack_id: str
    version: str
    language: str | None = None
    domain: str | None = None
    tier: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    min_ades_version: str = "0.1.0"
    manifest_url: str


class StatusResponse(BaseModel):
    """Basic runtime status."""

    service: str
    version: str
    runtime_target: str
    metadata_backend: str
    storage_root: str
    host: str
    port: int
    registry_url: str | None = None
    installed_packs: list[str] = Field(default_factory=list)


class RegistryBuildPackSummary(BaseModel):
    """Published output for one pack in a static registry build."""

    pack_id: str
    version: str
    source_path: str
    manifest_path: str
    artifact_path: str
    artifact_sha256: str
    artifact_size_bytes: int


class RegistryBuildRequest(BaseModel):
    """Request body for building a static file-based pack registry."""

    pack_dirs: list[str] = Field(default_factory=list)
    output_dir: str

    @model_validator(mode="after")
    def validate_pack_dirs(self) -> "RegistryBuildRequest":
        if not self.pack_dirs:
            raise ValueError("At least one pack directory is required.")
        return self


class RegistryBuildResponse(BaseModel):
    """Response body for a static registry build."""

    output_dir: str
    index_path: str
    index_url: str
    generated_at: datetime
    pack_count: int
    packs: list[RegistryBuildPackSummary] = Field(default_factory=list)


class NpmInstallerInfo(BaseModel):
    """Canonical npm-wrapper bootstrap metadata."""

    npm_package: str
    command: str
    wrapper_version: str
    python_package_spec: str
    runtime_dir: str
    python_bin_env: str
    runtime_dir_env: str
    package_spec_env: str
    pip_install_args_env: str


class ReleaseArtifactSummary(BaseModel):
    """Verified metadata for one built release artifact."""

    path: str
    file_name: str
    size_bytes: int
    sha256: str


class ReleaseVersionState(BaseModel):
    """Current version alignment across the coordinated release files."""

    project_root: str
    version_file_path: str
    pyproject_path: str
    npm_package_json_path: str
    version_file_version: str
    pyproject_version: str
    npm_version: str
    synchronized: bool


class ReleaseVersionSyncRequest(BaseModel):
    """Request body for coordinated version synchronization."""

    version: str


class ReleaseVersionSyncResponse(BaseModel):
    """Response body for coordinated version synchronization."""

    target_version: str
    updated_files: list[str] = Field(default_factory=list)
    version_state: ReleaseVersionState


class ReleaseVerificationRequest(BaseModel):
    """Request body for local release artifact verification."""

    output_dir: str
    clean: bool = True
    smoke_install: bool = True


class ReleaseVerificationResponse(BaseModel):
    """Built and verified metadata for one local release set."""

    project_root: str
    output_dir: str
    python_package: str
    python_version: str
    npm_package: str
    npm_version: str
    version_state: ReleaseVersionState
    versions_match: bool
    smoke_install: bool = True
    overall_success: bool
    warnings: list[str] = Field(default_factory=list)
    wheel: ReleaseArtifactSummary
    sdist: ReleaseArtifactSummary
    npm_tarball: ReleaseArtifactSummary
    python_install_smoke: "ReleaseInstallSmokeResult | None" = None
    npm_install_smoke: "ReleaseInstallSmokeResult | None" = None


class ReleaseCommandResult(BaseModel):
    """Captured output for one release-validation shell command."""

    command: list[str] = Field(default_factory=list)
    exit_code: int
    passed: bool
    stdout: str = ""
    stderr: str = ""


class ReleaseInstallSmokeResult(BaseModel):
    """Captured smoke-install result for one release artifact."""

    artifact_kind: Literal["python_wheel", "npm_tarball"]
    working_dir: str
    environment_dir: str
    storage_root: str
    install: ReleaseCommandResult
    invoke: ReleaseCommandResult
    pull: ReleaseCommandResult | None = None
    tag: ReleaseCommandResult | None = None
    recovery_status: ReleaseCommandResult | None = None
    serve: ReleaseCommandResult | None = None
    serve_healthz: ReleaseCommandResult | None = None
    serve_status: ReleaseCommandResult | None = None
    serve_tag: ReleaseCommandResult | None = None
    serve_tag_file: ReleaseCommandResult | None = None
    passed: bool
    reported_version: str | None = None
    pulled_pack_ids: list[str] = Field(default_factory=list)
    tagged_labels: list[str] = Field(default_factory=list)
    recovered_pack_ids: list[str] = Field(default_factory=list)
    served_pack_ids: list[str] = Field(default_factory=list)
    serve_tagged_labels: list[str] = Field(default_factory=list)
    serve_tag_file_labels: list[str] = Field(default_factory=list)


class ReleaseValidationSummary(BaseModel):
    """Validation metadata persisted into a release manifest."""

    validated_at: datetime
    tests_command: list[str] = Field(default_factory=list)
    tests_exit_code: int
    tests_passed: bool


class ReleaseManifestRequest(BaseModel):
    """Request body for persisted release-manifest generation."""

    output_dir: str
    manifest_path: str | None = None
    version: str | None = None
    clean: bool = True
    smoke_install: bool = True


class ReleaseManifestResponse(BaseModel):
    """Persisted manifest for one coordinated local release build."""

    manifest_path: str
    generated_at: datetime
    release_version: str
    version_state: ReleaseVersionState
    version_sync: ReleaseVersionSyncResponse | None = None
    validation: ReleaseValidationSummary | None = None
    verification: ReleaseVerificationResponse


class ReleaseValidationRequest(BaseModel):
    """Request body for one-command local release validation."""

    output_dir: str
    manifest_path: str | None = None
    version: str | None = None
    clean: bool = True
    smoke_install: bool = True
    tests_command: list[str] = Field(default_factory=list)


class ReleaseValidationResponse(BaseModel):
    """Structured result for a local test-plus-release validation run."""

    project_root: str
    output_dir: str
    tests: ReleaseCommandResult
    manifest: ReleaseManifestResponse | None = None
    manifest_path: str | None = None
    overall_success: bool
    warnings: list[str] = Field(default_factory=list)


class ReleasePublishRequest(BaseModel):
    """Request body for coordinated release publication."""

    manifest_path: str
    dry_run: bool = False


class ReleasePublishTargetResult(BaseModel):
    """Structured publish result for one distribution target."""

    artifact_paths: list[str] = Field(default_factory=list)
    registry: str | None = None
    command: ReleaseCommandResult
    executed: bool
    passed: bool


class ReleasePublishResponse(BaseModel):
    """Structured result for one coordinated release publication run."""

    manifest_path: str
    release_version: str
    dry_run: bool
    validated_manifest: bool
    python_publish: ReleasePublishTargetResult
    npm_publish: ReleasePublishTargetResult
    overall_success: bool
    warnings: list[str] = Field(default_factory=list)


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
    relevance: float | None = None
    provenance: EntityProvenance | None = None
    link: EntityLink | None = None


class SourceFingerprint(BaseModel):
    """Stable local fingerprint for a source file."""

    size_bytes: int
    modified_time_ns: int
    sha256: str


class TopicMatch(BaseModel):
    """Single assigned topic."""

    label: str
    score: float | None = None
    evidence_count: int = 0
    entity_labels: list[str] = Field(default_factory=list)


class TagOutputOptions(BaseModel):
    """Optional local JSON persistence settings for tag responses."""

    path: str | None = None
    directory: str | None = None
    manifest_path: str | None = None
    write_manifest: bool = False
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
    manifest_input_path: str | None = None
    manifest_replay_mode: Literal["resume", "processed", "all"] = "resume"
    skip_unchanged: bool = False
    reuse_unchanged_outputs: bool = False
    repair_missing_reused_outputs: bool = False
    include_patterns: list[str] = Field(default_factory=list)
    exclude_patterns: list[str] = Field(default_factory=list)
    recursive: bool = True
    max_files: int | None = Field(default=None, ge=0)
    max_input_bytes: int | None = Field(default=None, ge=0)
    pack: str | None = None
    content_type: str | None = None
    output: TagOutputOptions | None = None
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_sources(self) -> "BatchFileTagRequest":
        if self.paths or self.directories or self.glob_patterns or self.manifest_input_path:
            if self.skip_unchanged and self.manifest_input_path is None:
                raise ValueError("skip_unchanged requires manifest_input_path.")
            if self.reuse_unchanged_outputs and self.manifest_input_path is None:
                raise ValueError("reuse_unchanged_outputs requires manifest_input_path.")
            if self.reuse_unchanged_outputs and not self.skip_unchanged:
                raise ValueError("reuse_unchanged_outputs requires skip_unchanged.")
            if self.repair_missing_reused_outputs and self.manifest_input_path is None:
                raise ValueError("repair_missing_reused_outputs requires manifest_input_path.")
            if self.repair_missing_reused_outputs and not self.reuse_unchanged_outputs:
                raise ValueError("repair_missing_reused_outputs requires reuse_unchanged_outputs.")
            return self
        raise ValueError(
            "At least one of paths, directories, glob_patterns, or manifest_input_path is required."
        )


class TagResponse(BaseModel):
    """Response body for tagging."""

    version: str
    pack: str
    language: str
    content_type: str
    source_path: str | None = None
    input_size_bytes: int | None = None
    source_fingerprint: SourceFingerprint | None = None
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
    processed_count: int
    excluded_count: int
    skipped_count: int
    rejected_count: int
    limit_skipped_count: int = 0
    unchanged_skipped_count: int = 0
    unchanged_reused_count: int = 0
    reused_output_missing_count: int = 0
    repaired_reused_output_count: int = 0
    duplicate_count: int
    generated_output_skipped_count: int
    discovered_input_bytes: int
    included_input_bytes: int
    processed_input_bytes: int
    max_files: int | None = None
    max_input_bytes: int | None = None
    manifest_input_path: str | None = None
    manifest_replay_mode: Literal["resume", "processed", "all"] | None = None
    manifest_candidate_count: int = 0
    manifest_selected_count: int = 0
    recursive: bool
    include_patterns: list[str] = Field(default_factory=list)
    exclude_patterns: list[str] = Field(default_factory=list)


class BatchSkippedInput(BaseModel):
    """One skipped corpus input with a deterministic reason."""

    reference: str
    reason: str
    source_kind: str
    size_bytes: int | None = None


class BatchRejectedInput(BaseModel):
    """One rejected corpus input that could not be accepted for processing."""

    reference: str
    reason: str
    source_kind: str


class BatchRerunDiff(BaseModel):
    """Categorized rerun delta against a prior batch manifest."""

    manifest_input_path: str
    changed: list[str] = Field(default_factory=list)
    newly_processed: list[str] = Field(default_factory=list)
    reused: list[str] = Field(default_factory=list)
    repaired: list[str] = Field(default_factory=list)
    skipped: list[BatchSkippedInput] = Field(default_factory=list)


class BatchRunLineage(BaseModel):
    """Stable lineage metadata for one batch run."""

    run_id: str
    root_run_id: str
    parent_run_id: str | None = None
    source_manifest_path: str | None = None
    created_at: datetime


class BatchManifestItem(BaseModel):
    """Compact run-manifest entry for one persisted or processed batch item."""

    source_path: str | None = None
    saved_output_path: str | None = None
    saved_output_exists: bool | None = None
    content_type: str
    input_size_bytes: int | None = None
    source_fingerprint: SourceFingerprint | None = None
    warning_count: int
    warnings: list[str] = Field(default_factory=list)
    entity_count: int
    topic_count: int
    timing_ms: int


class BatchTagResponse(BaseModel):
    """Response body for local batch tagging."""

    pack: str
    item_count: int
    summary: BatchSourceSummary
    warnings: list[str] = Field(default_factory=list)
    saved_manifest_path: str | None = None
    lineage: BatchRunLineage | None = None
    rerun_diff: BatchRerunDiff | None = None
    skipped: list[BatchSkippedInput] = Field(default_factory=list)
    rejected: list[BatchRejectedInput] = Field(default_factory=list)
    reused_items: list[BatchManifestItem] = Field(default_factory=list)
    items: list[TagResponse] = Field(default_factory=list)


class BatchManifest(BaseModel):
    """Stable run-level audit artifact for a batch tagging execution."""

    version: str
    pack: str
    item_count: int
    summary: BatchSourceSummary
    warnings: list[str] = Field(default_factory=list)
    lineage: BatchRunLineage | None = None
    rerun_diff: BatchRerunDiff | None = None
    skipped: list[BatchSkippedInput] = Field(default_factory=list)
    rejected: list[BatchRejectedInput] = Field(default_factory=list)
    reused_items: list[BatchManifestItem] = Field(default_factory=list)
    items: list[BatchManifestItem] = Field(default_factory=list)


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
