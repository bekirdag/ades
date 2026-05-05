"""Public library helpers for ades v0.1.0."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from .config import Settings, get_settings
from .distribution import (
    NPM_COMMAND_NAME,
    NPM_PACKAGE_NAME,
    NPM_PIP_INSTALL_ARGS_ENV,
    NPM_PYTHON_BIN_ENV,
    NPM_PYTHON_PACKAGE_SPEC_ENV,
    NPM_RUNTIME_DIR_ENV,
    build_npm_python_package_spec,
    resolve_npm_runtime_dir,
)
from .packs.installer import InstallResult, PackInstaller
from .packs.generation import generate_pack_source as run_generate_pack_source
from .packs.finance_bundle import build_finance_source_bundle as run_build_finance_source_bundle
from .packs.finance_country import (
    DEFAULT_FINANCE_COUNTRY_BUNDLE_OUTPUT_ROOT,
    DEFAULT_FINANCE_COUNTRY_SOURCE_OUTPUT_ROOT,
    DEFAULT_COUNTRY_SOURCE_FETCH_USER_AGENT,
    build_finance_country_source_bundles as run_build_finance_country_source_bundles,
    fetch_finance_country_source_snapshots as run_fetch_finance_country_source_snapshots,
)
from .packs.finance_sources import (
    DEFAULT_FINANCE_SOURCE_OUTPUT_ROOT,
    DEFAULT_SEC_ARCHIVES_BASE_URL,
    DEFAULT_SEC_COMPANIES_URL,
    DEFAULT_SEC_COMPANYFACTS_URL,
    DEFAULT_SEC_SUBMISSIONS_URL,
    DEFAULT_SOURCE_FETCH_USER_AGENT,
    DEFAULT_OTHER_LISTED_URL,
    DEFAULT_SYMBOL_DIRECTORY_URL,
    fetch_finance_source_snapshot as run_fetch_finance_source_snapshot,
)
from .packs.finance_quality import validate_finance_pack_quality as run_validate_finance_pack_quality
from .packs.general_bundle import build_general_source_bundle as run_build_general_source_bundle
from .packs.general_sources import (
    DEFAULT_GENERAL_SOURCE_OUTPUT_ROOT,
    DEFAULT_GEONAMES_ALTERNATE_NAMES_URL,
    DEFAULT_GEONAMES_PLACES_URL,
    DEFAULT_SOURCE_FETCH_USER_AGENT as DEFAULT_GENERAL_SOURCE_FETCH_USER_AGENT,
    DEFAULT_WIKIDATA_ENTITIES_URL,
    DEFAULT_WIKIDATA_SEED_SNAPSHOT_URL,
    DEFAULT_WIKIDATA_TRUTHY_URL,
    fetch_general_source_snapshot as run_fetch_general_source_snapshot,
)
from .packs.general_quality import (
    DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES,
    validate_general_pack_quality as run_validate_general_pack_quality,
)
from .packs.medical_bundle import build_medical_source_bundle as run_build_medical_source_bundle
from .packs.medical_quality import validate_medical_pack_quality as run_validate_medical_pack_quality
from .packs.medical_sources import (
    DEFAULT_CLINICAL_TRIALS_MAX_RECORDS,
    DEFAULT_CLINICAL_TRIALS_URL,
    DEFAULT_DISEASE_ONTOLOGY_URL,
    DEFAULT_HGNC_GENES_URL,
    DEFAULT_MEDICAL_SOURCE_OUTPUT_ROOT,
    DEFAULT_ORANGE_BOOK_URL,
    DEFAULT_SOURCE_FETCH_USER_AGENT as DEFAULT_MEDICAL_SOURCE_FETCH_USER_AGENT,
    DEFAULT_UNIPROT_MAX_RECORDS,
    DEFAULT_UNIPROT_PROTEINS_URL,
    fetch_medical_source_snapshot as run_fetch_medical_source_snapshot,
)
from .extraction_quality import (
    benchmark_report_path as default_benchmark_report_path,
    benchmark_matcher_backends_for_pack as run_benchmark_matcher_backends_for_pack,
    benchmark_runtime_pack as run_benchmark_runtime_pack,
    compare_quality_reports,
    evaluate_golden_set,
    get_pack_health_summary,
    golden_set_path as default_golden_set_path,
    load_golden_set,
    load_quality_report,
    matcher_benchmark_spike_report_path as default_matcher_benchmark_spike_report_path,
    record_tag_observation,
    quality_report_path as default_quality_report_path,
    write_matcher_benchmark_spike_report,
    write_runtime_benchmark_report,
    write_quality_report,
)
from .news_feedback import (
    evaluate_live_news_feedback as run_evaluate_live_news_feedback,
    live_news_feedback_report_path as default_live_news_feedback_report_path,
    write_live_news_feedback_report,
)
from .packs.refresh import refresh_generated_pack_registry as run_refresh_generated_pack_registry
from .packs.reporting import report_generated_pack as run_report_generated_pack
from .packs.publish import (
    build_static_registry,
    prepare_registry_deploy_payload as run_prepare_registry_deploy_payload,
    publish_registry_to_object_storage as run_publish_registry_to_object_storage,
    smoke_test_published_registry as run_smoke_test_published_registry,
)
from .packs.registry import PackRegistry, load_registry_index
from .packs.runtime import PackRuntime, load_pack_runtime
from .packs.versioning import (
    default_release_thresholds,
    diff_pack_directories,
    evaluate_release_thresholds,
    ReleaseThresholds,
)
from .pipeline.files import TagFileSkippedEntry, discover_tag_file_sources, load_tag_file
from .pipeline.tagger import tag_text
from .release import release_versions as read_release_versions
from .release import publish_release_manifest as run_publish_release_manifest
from .release import sync_release_version as run_sync_release_version
from .release import validate_release_workflow
from .release import verify_release_artifacts
from .release import write_release_manifest as persist_release_manifest
from .service.models import (
    AvailablePackSummary,
    BatchManifestItem,
    BatchRerunDiff,
    BatchSourceSummary,
    BatchTagResponse,
    ImpactExpansionResult,
    LookupCandidate,
    LookupResponse,
    MarketGraphStoreBuildResponse,
    ReleaseManifestResponse,
    ReleasePublishResponse,
    ReleaseValidationResponse,
    ReleaseVersionState,
    ReleaseVersionSyncResponse,
    NpmInstallerInfo,
    PackSummary,
    PackHealthResponse,
    QidGraphStoreBuildResponse,
    ReleaseVerificationResponse,
    RegistryBuildPackSummary,
    RegistryBuildResponse,
    RegistryBuildFinanceBundleResponse,
    RegistryBuildFinanceCountryBundlesResponse,
    RegistryCompareExtractionQualityResponse,
    RegistryEvaluateExtractionQualityResponse,
    RegistryEvaluateReleaseThresholdsResponse,
    RegistryEvaluateVectorQualityResponse,
    RegistryEvaluateVectorReleaseThresholdsResponse,
    RegistryPackDiffResponse,
    RegistryReleaseThresholdsResponse,
    RegistryVectorReleaseThresholdsResponse,
    RegistryPrepareDeployReleaseResponse,
    RegistryFetchFinanceCountrySourcesResponse,
    RegistryFetchGeneralSourcesResponse,
    RegistryFetchMedicalSourcesResponse,
    RegistryFetchFinanceSourcesResponse,
    RegistryBuildGeneralBundleResponse,
    RegistryBuildMedicalBundleResponse,
    ExtractionQualityCaseResultResponse,
    ExtractionQualityDeltaResponse,
    ExtractionQualityReportResponse,
    LabelQualityDeltaResponse,
    MatcherBackendCandidateResponse,
    MatcherBenchmarkSpikeReportResponse,  # noqa: F401
    LiveNewsArticleResultResponse,
    LiveNewsEntityResponse,
    LiveNewsFailureResponse,
    LiveNewsIssueResponse,
    RegistryBenchmarkMatcherBackendsResponse,
    RegistryBenchmarkMatcherBackendsRequest,  # noqa: F401
    RegistryBenchmarkRuntimeResponse,
    RegistryEvaluateLiveNewsFeedbackResponse,
    RuntimeDiskBenchmarkResponse,
    RuntimeLatencyBenchmarkResponse,
    RegistryPackQualityCaseResult,
    RegistryPackQualityResponse,
    RegistryPublishedObject,
    RegistryPublishGeneratedReleaseResponse,
    RegistryPublishedReleaseSmokeCaseResponse,
    RegistryRefreshGeneratedPacksResponse,
    RegistryRefreshPackResponse,
    RegistrySmokePublishedReleaseResponse,
    RegistryValidateFinanceQualityResponse,
    RegistryGeneratePackResponse,
    RegistryReportPackResponse,
    SourceFingerprint,
    StatusResponse,
    TagResponse,
    VectorCaseResultResponse,
    VectorIndexBuildResponse,
    VectorQualityReportResponse,  # noqa: F401
)
from .storage.paths import build_storage_layout, ensure_storage_layout
from .storage.results import (
    aggregate_batch_warnings,
    build_batch_manifest_item_from_tag_response,
    build_batch_manifest_replay_plan,
    build_batch_run_lineage,
    persist_batch_tag_manifest_json,
    persist_tag_response_json,
    persist_tag_responses_json,
    verify_reused_output_items,
)
from .vector.builder import (
    DEFAULT_QID_GRAPH_ALLOWED_PREDICATES,
    DEFAULT_QID_GRAPH_DIMENSIONS,
    build_qid_graph_index as run_build_qid_graph_index,
    build_qid_graph_index_from_store as run_build_qid_graph_index_from_store,
)
from .vector.graph_builder import build_qid_graph_store as run_build_qid_graph_store
from .vector.evaluation import (
    default_vector_release_thresholds,
    evaluate_vector_golden_set,
    evaluate_vector_release_thresholds,
    load_vector_golden_set,
    load_vector_quality_report,
    vector_golden_set_path as default_vector_golden_set_path,
    vector_quality_report_path as default_vector_quality_report_path,
    write_vector_quality_report,
    VectorReleaseThresholds,
)
from .vector.service import enrich_tag_response_with_related_entities
from .impact.expansion import (
    enrich_tag_response_with_impact_paths,
    expand_impact_paths as run_expand_impact_paths,
)
from .impact.graph_builder import build_market_graph_store as run_build_market_graph_store

if TYPE_CHECKING:
    from .packs.quality_common import PackQualityResult
    from .packs.reporting import GeneratedPackReport


def _resolve_settings(
    *,
    storage_root: str | Path | None = None,
    host: str | None = None,
    port: int | None = None,
    default_pack: str | None = None,
    registry_url: str | None = None,
    database_url: str | None = None,
) -> Settings:
    base = get_settings()
    return Settings(
        host=host or base.host,
        port=port if port is not None else base.port,
        storage_root=Path(storage_root).expanduser() if storage_root is not None else base.storage_root,
        default_pack=default_pack or base.default_pack,
        registry_url=registry_url if registry_url is not None else base.registry_url,
        runtime_target=base.runtime_target,
        metadata_backend=base.metadata_backend,
        database_url=database_url if database_url is not None else base.database_url,
        vector_search_enabled=base.vector_search_enabled,
        vector_search_url=base.vector_search_url,
        vector_search_api_key=base.vector_search_api_key,
        vector_search_collection_alias=base.vector_search_collection_alias,
        vector_search_related_limit=base.vector_search_related_limit,
        vector_search_score_threshold=base.vector_search_score_threshold,
        vector_search_domain_pack_routes=dict(base.vector_search_domain_pack_routes),
        vector_search_pack_collection_aliases=dict(
            base.vector_search_pack_collection_aliases
        ),
        vector_search_country_aliases=dict(base.vector_search_country_aliases),
        graph_context_enabled=base.graph_context_enabled,
        graph_context_artifact_path=base.graph_context_artifact_path,
        graph_context_max_depth=base.graph_context_max_depth,
        graph_context_related_limit=base.graph_context_related_limit,
        graph_context_seed_neighbor_limit=base.graph_context_seed_neighbor_limit,
        graph_context_candidate_limit=base.graph_context_candidate_limit,
        graph_context_min_supporting_seeds=base.graph_context_min_supporting_seeds,
        graph_context_vector_proposals_enabled=(
            base.graph_context_vector_proposals_enabled
        ),
        graph_context_vector_proposal_limit=base.graph_context_vector_proposal_limit,
        graph_context_genericity_penalty_enabled=base.graph_context_genericity_penalty_enabled,
        news_context_artifact_path=base.news_context_artifact_path,
        news_context_min_supporting_seeds=base.news_context_min_supporting_seeds,
        news_context_min_pair_count=base.news_context_min_pair_count,
        impact_expansion_enabled=base.impact_expansion_enabled,
        impact_expansion_artifact_path=base.impact_expansion_artifact_path,
        impact_expansion_max_depth=base.impact_expansion_max_depth,
        impact_expansion_seed_limit=base.impact_expansion_seed_limit,
        impact_expansion_max_candidates=base.impact_expansion_max_candidates,
        impact_expansion_max_edges_per_seed=base.impact_expansion_max_edges_per_seed,
        impact_expansion_max_paths_per_candidate=(
            base.impact_expansion_max_paths_per_candidate
        ),
        impact_expansion_vector_proposals_enabled=(
            base.impact_expansion_vector_proposals_enabled
        ),
        service_prewarm_enabled=base.service_prewarm_enabled,
        config_path=base.config_path,
    )


def status(
    *,
    storage_root: str | Path | None = None,
    host: str | None = None,
    port: int | None = None,
) -> StatusResponse:
    """Return the current local ades runtime status."""

    settings = _resolve_settings(storage_root=storage_root, host=host, port=port)
    layout = ensure_storage_layout(build_storage_layout(settings.storage_root))
    registry = PackRegistry(
        layout.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )
    return StatusResponse(
        service="ades",
        version=settings_version(),
        runtime_target=settings.runtime_target.value,
        metadata_backend=settings.metadata_backend.value,
        metadata_persistence_backend=registry.backend_plan.metadata_persistence_backend.value,
        exact_extraction_backend=registry.backend_plan.exact_extraction_backend.value,
        operator_lookup_backend=registry.backend_plan.operator_lookup_backend.value,
        storage_root=str(layout.storage_root),
        host=settings.host,
        port=settings.port,
        registry_url=settings.registry_url,
        installed_packs=[pack.pack_id for pack in registry.list_installed_packs()],
    )


def list_packs(
    *,
    storage_root: str | Path | None = None,
    active_only: bool = False,
) -> list[PackSummary]:
    """List installed packs for a storage root."""

    settings = _resolve_settings(storage_root=storage_root)
    registry = PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )
    return [
        PackSummary(**pack.to_summary())
        for pack in registry.list_installed_packs(active_only=active_only)
    ]


def list_available_packs(
    *,
    registry_url: str | None = None,
) -> list[AvailablePackSummary]:
    """List installable packs from the configured registry."""

    settings = _resolve_settings(registry_url=registry_url)
    index = load_registry_index(settings.registry_url)
    return [
        AvailablePackSummary(**pack.to_summary())
        for pack in index.list_packs()
    ]


def get_pack(
    pack_id: str,
    *,
    storage_root: str | Path | None = None,
    active_only: bool = False,
) -> PackSummary | None:
    """Return a single installed pack summary."""

    settings = _resolve_settings(storage_root=storage_root)
    registry = PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )
    manifest = registry.get_pack(pack_id, active_only=active_only)
    if manifest is None:
        return None
    return PackSummary(**manifest.to_summary())


def get_pack_health(
    pack_id: str,
    *,
    storage_root: str | Path | None = None,
    limit: int = 100,
) -> PackHealthResponse:
    """Return aggregated recent extraction-health telemetry for one pack."""

    settings = _resolve_settings(storage_root=storage_root)
    summary = get_pack_health_summary(
        pack_id,
        storage_root=settings.storage_root,
        limit=limit,
    )
    return PackHealthResponse(**summary)


def pull_pack(
    pack_id: str,
    *,
    storage_root: str | Path | None = None,
    registry_url: str | None = None,
) -> InstallResult:
    """Install a pack and its dependencies."""

    settings = _resolve_settings(storage_root=storage_root, registry_url=registry_url)
    installer = PackInstaller(
        settings.storage_root,
        registry_url=settings.registry_url,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )
    return installer.install(pack_id)


def build_registry(
    pack_dirs: Iterable[str | Path],
    *,
    output_dir: str | Path,
) -> RegistryBuildResponse:
    """Build a static file-based pack registry from local pack directories."""

    result = build_static_registry(list(pack_dirs), output_dir=output_dir)
    return RegistryBuildResponse(
        output_dir=result.output_dir,
        index_path=result.index_path,
        index_url=result.index_url,
        generated_at=result.generated_at,
        pack_count=len(result.packs),
        packs=[
            RegistryBuildPackSummary(
                pack_id=pack.pack_id,
                version=pack.version,
                source_path=pack.source_path,
                manifest_path=pack.manifest_path,
                artifact_path=pack.artifact_path,
                artifact_sha256=pack.artifact_sha256,
                artifact_size_bytes=pack.artifact_size_bytes,
            )
            for pack in result.packs
        ],
    )


def generate_pack_source(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    version: str | None = None,
    include_build_metadata: bool = True,
    include_build_only: bool = False,
) -> RegistryGeneratePackResponse:
    """Generate one runtime-compatible pack directory from a normalized source bundle."""

    result = run_generate_pack_source(
        bundle_dir,
        output_dir=output_dir,
        version=version,
        include_build_metadata=include_build_metadata,
        include_build_only=include_build_only,
    )
    return RegistryGeneratePackResponse(
        pack_id=result.pack_id,
        version=result.version,
        bundle_dir=result.bundle_dir,
        output_dir=result.output_dir,
        pack_dir=result.pack_dir,
        manifest_path=result.manifest_path,
        labels_path=result.labels_path,
        aliases_path=result.aliases_path,
        rules_path=result.rules_path,
        matcher_artifact_path=result.matcher_artifact_path,
        matcher_entries_path=result.matcher_entries_path,
        matcher_algorithm=result.matcher_algorithm,
        matcher_entry_count=result.matcher_entry_count,
        matcher_state_count=result.matcher_state_count,
        matcher_max_alias_length=result.matcher_max_alias_length,
        matcher_artifact_sha256=result.matcher_artifact_sha256,
        matcher_entries_sha256=result.matcher_entries_sha256,
        sources_path=result.sources_path,
        build_path=result.build_path,
        generated_at=result.generated_at,
        label_count=result.label_count,
        alias_count=result.alias_count,
        rule_count=result.rule_count,
        source_count=result.source_count,
        publishable_source_count=result.publishable_source_count,
        restricted_source_count=result.restricted_source_count,
        publishable_sources_only=result.publishable_sources_only,
        source_license_classes=result.source_license_classes,
        included_entity_count=result.included_entity_count,
        included_rule_count=result.included_rule_count,
        dropped_record_count=result.dropped_record_count,
        dropped_alias_count=result.dropped_alias_count,
        ambiguous_alias_count=result.ambiguous_alias_count,
        warnings=result.warnings,
    )


def report_generated_pack(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    version: str | None = None,
    include_build_metadata: bool = True,
    include_build_only: bool = False,
) -> RegistryReportPackResponse:
    """Generate one runtime-compatible pack directory and report stable statistics."""

    result = run_report_generated_pack(
        bundle_dir,
        output_dir=output_dir,
        version=version,
        include_build_metadata=include_build_metadata,
        include_build_only=include_build_only,
    )
    return RegistryReportPackResponse(
        pack_id=result.pack_id,
        version=result.version,
        bundle_dir=result.bundle_dir,
        output_dir=result.output_dir,
        pack_dir=result.pack_dir,
        manifest_path=result.manifest_path,
        labels_path=result.labels_path,
        aliases_path=result.aliases_path,
        rules_path=result.rules_path,
        matcher_artifact_path=result.matcher_artifact_path,
        matcher_entries_path=result.matcher_entries_path,
        matcher_algorithm=result.matcher_algorithm,
        matcher_entry_count=result.matcher_entry_count,
        matcher_state_count=result.matcher_state_count,
        matcher_max_alias_length=result.matcher_max_alias_length,
        matcher_artifact_sha256=result.matcher_artifact_sha256,
        matcher_entries_sha256=result.matcher_entries_sha256,
        sources_path=result.sources_path,
        build_path=result.build_path,
        generated_at=result.generated_at,
        source_count=result.source_count,
        publishable_source_count=result.publishable_source_count,
        restricted_source_count=result.restricted_source_count,
        publishable_sources_only=result.publishable_sources_only,
        source_license_classes=result.source_license_classes,
        label_count=result.label_count,
        alias_count=result.alias_count,
        unique_canonical_count=result.unique_canonical_count,
        rule_count=result.rule_count,
        included_entity_count=result.included_entity_count,
        included_rule_count=result.included_rule_count,
        dropped_record_count=result.dropped_record_count,
        dropped_alias_count=result.dropped_alias_count,
        ambiguous_alias_count=result.ambiguous_alias_count,
        dropped_alias_ratio=result.dropped_alias_ratio,
        label_distribution=result.label_distribution,
        warnings=result.warnings,
    )


def refresh_generated_packs(
    bundle_dirs: Iterable[str | Path],
    *,
    output_dir: str | Path,
    general_bundle_dir: str | Path | None = None,
    materialize_registry: bool = False,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int | None = None,
    max_dropped_alias_ratio: float | None = None,
) -> RegistryRefreshGeneratedPacksResponse:
    """Refresh one or more generated packs into a quality-gated registry release."""

    result = run_refresh_generated_pack_registry(
        list(bundle_dirs),
        output_dir=output_dir,
        general_bundle_dir=general_bundle_dir,
        materialize_registry=materialize_registry,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )
    registry_response = (
        RegistryBuildResponse(
            output_dir=result.registry.output_dir,
            index_path=result.registry.index_path,
            index_url=result.registry.index_url,
            generated_at=result.registry.generated_at,
            pack_count=len(result.registry.packs),
            packs=[
                RegistryBuildPackSummary(
                    pack_id=pack.pack_id,
                    version=pack.version,
                    source_path=pack.source_path,
                    manifest_path=pack.manifest_path,
                    artifact_path=pack.artifact_path,
                    artifact_sha256=pack.artifact_sha256,
                    artifact_size_bytes=pack.artifact_size_bytes,
                )
                for pack in result.registry.packs
            ],
        )
        if result.registry is not None
        else None
    )
    return RegistryRefreshGeneratedPacksResponse(
        output_dir=result.output_dir,
        candidate_dir=result.candidate_dir,
        report_dir=result.report_dir,
        quality_dir=result.quality_dir,
        generated_at=result.generated_at,
        pack_count=result.pack_count,
        passed=result.passed,
        materialize_registry=result.materialize_registry,
        registry_materialized=result.registry_materialized,
        registry=registry_response,
        warnings=result.warnings,
        packs=[
            RegistryRefreshPackResponse(
                pack_id=item.pack_id,
                bundle_dir=item.bundle_dir,
                report=_report_response_from_result(item.report),
                quality=_quality_response_from_result(item.quality),
            )
            for item in result.packs
        ],
    )


def publish_generated_registry_release(
    registry_dir: str | Path,
    *,
    prefix: str,
    bucket: str | None = None,
    endpoint: str | None = None,
    region: str = "us-east-1",
    delete: bool = True,
) -> RegistryPublishGeneratedReleaseResponse:
    """Publish one reviewed generated registry directory to object storage."""

    result = run_publish_registry_to_object_storage(
        registry_dir,
        prefix=prefix,
        bucket=bucket,
        endpoint=endpoint,
        region=region,
        delete=delete,
    )
    return RegistryPublishGeneratedReleaseResponse(
        registry_dir=result.registry_dir,
        bucket=result.bucket,
        endpoint=result.endpoint,
        endpoint_url=result.endpoint_url,
        prefix=result.prefix,
        storage_uri=result.storage_uri,
        index_storage_uri=result.index_storage_uri,
        registry_url_candidates=result.registry_url_candidates,
        published_at=result.published_at,
        delete=result.delete,
        object_count=result.object_count,
        objects=[
            RegistryPublishedObject(
                key=item.key,
                size_bytes=item.size_bytes,
            )
            for item in result.objects
        ],
    )


def prepare_registry_deploy_release(
    *,
    output_dir: str | Path,
    pack_dirs: Iterable[str | Path] = (),
    promotion_spec_path: str | Path | None = None,
) -> RegistryPrepareDeployReleaseResponse:
    """Prepare the deploy-owned registry payload from bundled packs or a promoted release."""

    result = run_prepare_registry_deploy_payload(
        list(pack_dirs),
        output_dir=output_dir,
        promotion_spec_path=promotion_spec_path,
    )
    consumer_smoke = None
    if result.consumer_smoke is not None:
        consumer_smoke = RegistrySmokePublishedReleaseResponse(
            registry_url=result.consumer_smoke.registry_url,
            checked_at=result.consumer_smoke.checked_at,
            fixture_profile=result.consumer_smoke.fixture_profile,
            pack_count=result.consumer_smoke.pack_count,
            passed_case_count=result.consumer_smoke.passed_case_count,
            failed_case_count=result.consumer_smoke.failed_case_count,
            passed=result.consumer_smoke.passed,
            failures=result.consumer_smoke.failures,
            cases=[
                RegistryPublishedReleaseSmokeCaseResponse(
                    pack_id=case.pack_id,
                    text=case.text,
                    expected_installed_pack_ids=case.expected_installed_pack_ids,
                    pulled_pack_ids=case.pulled_pack_ids,
                    installed_pack_ids=case.installed_pack_ids,
                    missing_installed_pack_ids=case.missing_installed_pack_ids,
                    expected_entity_texts=case.expected_entity_texts,
                    matched_entity_texts=case.matched_entity_texts,
                    missing_entity_texts=case.missing_entity_texts,
                    tagged_entity_texts=case.tagged_entity_texts,
                    expected_labels=case.expected_labels,
                    matched_labels=case.matched_labels,
                    missing_labels=case.missing_labels,
                    tagged_labels=case.tagged_labels,
                    passed=case.passed,
                    failure=case.failure,
                )
                for case in result.consumer_smoke.cases
            ],
        )
    return RegistryPrepareDeployReleaseResponse(
        mode=result.mode,
        output_dir=result.output_dir,
        index_path=result.index_path,
        index_url=result.index_url,
        generated_at=result.generated_at,
        pack_count=result.pack_count,
        packs=[
            RegistryBuildPackSummary(
                pack_id=pack.pack_id,
                version=pack.version,
                source_path=pack.source_path,
                manifest_path=pack.manifest_path,
                artifact_path=pack.artifact_path,
                artifact_sha256=pack.artifact_sha256,
                artifact_size_bytes=pack.artifact_size_bytes,
            )
            for pack in result.packs
        ],
        promotion_spec_path=result.promotion_spec_path,
        promoted_registry_url=result.promoted_registry_url,
        consumer_smoke=consumer_smoke,
    )


def smoke_test_published_generated_registry(
    registry_url: str | Path,
    *,
    pack_ids: Iterable[str] = (),
) -> RegistrySmokePublishedReleaseResponse:
    """Validate clean consumer behavior for one published generated registry URL."""

    result = run_smoke_test_published_registry(
        registry_url,
        pack_ids=list(pack_ids),
    )
    return RegistrySmokePublishedReleaseResponse(
        registry_url=result.registry_url,
        checked_at=result.checked_at,
        fixture_profile=result.fixture_profile,
        pack_count=result.pack_count,
        passed_case_count=result.passed_case_count,
        failed_case_count=result.failed_case_count,
        passed=result.passed,
        failures=result.failures,
        cases=[
            RegistryPublishedReleaseSmokeCaseResponse(
                pack_id=case.pack_id,
                text=case.text,
                expected_installed_pack_ids=case.expected_installed_pack_ids,
                pulled_pack_ids=case.pulled_pack_ids,
                installed_pack_ids=case.installed_pack_ids,
                missing_installed_pack_ids=case.missing_installed_pack_ids,
                expected_entity_texts=case.expected_entity_texts,
                matched_entity_texts=case.matched_entity_texts,
                missing_entity_texts=case.missing_entity_texts,
                tagged_entity_texts=case.tagged_entity_texts,
                expected_labels=case.expected_labels,
                matched_labels=case.matched_labels,
                missing_labels=case.missing_labels,
                tagged_labels=case.tagged_labels,
                passed=case.passed,
                failure=case.failure,
            )
            for case in result.cases
        ],
    )


def build_finance_source_bundle(
    *,
    sec_companies_path: str | Path,
    sec_submissions_path: str | Path | None = None,
    sec_companyfacts_path: str | Path | None = None,
    symbol_directory_path: str | Path,
    other_listed_path: str | Path | None = None,
    finance_people_path: str | Path | None = None,
    curated_entities_path: str | Path,
    output_dir: str | Path,
    version: str = "0.2.0",
) -> RegistryBuildFinanceBundleResponse:
    """Build one normalized `finance-en` source bundle from raw snapshot files."""

    result = run_build_finance_source_bundle(
        sec_companies_path=sec_companies_path,
        sec_submissions_path=sec_submissions_path,
        sec_companyfacts_path=sec_companyfacts_path,
        symbol_directory_path=symbol_directory_path,
        other_listed_path=other_listed_path,
        finance_people_path=finance_people_path,
        curated_entities_path=curated_entities_path,
        output_dir=output_dir,
        version=version,
    )
    return RegistryBuildFinanceBundleResponse(
        pack_id=result.pack_id,
        version=result.version,
        output_dir=result.output_dir,
        bundle_dir=result.bundle_dir,
        bundle_manifest_path=result.bundle_manifest_path,
        sources_lock_path=result.sources_lock_path,
        entities_path=result.entities_path,
        rules_path=result.rules_path,
        generated_at=result.generated_at,
        source_count=result.source_count,
        entity_record_count=result.entity_record_count,
        rule_record_count=result.rule_record_count,
        sec_issuer_count=result.sec_issuer_count,
        symbol_count=result.symbol_count,
        person_count=result.person_count,
        company_equity_ticker_count=result.company_equity_ticker_count,
        company_name_enriched_issuer_count=result.company_name_enriched_issuer_count,
        curated_entity_count=result.curated_entity_count,
        warnings=result.warnings,
    )


def fetch_finance_source_snapshot(
    *,
    output_dir: str | Path = DEFAULT_FINANCE_SOURCE_OUTPUT_ROOT,
    snapshot: str | None = None,
    sec_companies_url: str = DEFAULT_SEC_COMPANIES_URL,
    sec_submissions_url: str = DEFAULT_SEC_SUBMISSIONS_URL,
    sec_companyfacts_url: str = DEFAULT_SEC_COMPANYFACTS_URL,
    symbol_directory_url: str = DEFAULT_SYMBOL_DIRECTORY_URL,
    other_listed_url: str = DEFAULT_OTHER_LISTED_URL,
    finance_people_url: str | None = None,
    derive_finance_people_from_sec: bool = False,
    finance_people_archive_base_url: str = DEFAULT_SEC_ARCHIVES_BASE_URL,
    finance_people_max_companies: int | None = None,
    user_agent: str = DEFAULT_SOURCE_FETCH_USER_AGENT,
) -> RegistryFetchFinanceSourcesResponse:
    """Download one real finance source snapshot set into the big-data root."""

    result = run_fetch_finance_source_snapshot(
        output_dir=output_dir,
        snapshot=snapshot,
        sec_companies_url=sec_companies_url,
        sec_submissions_url=sec_submissions_url,
        sec_companyfacts_url=sec_companyfacts_url,
        symbol_directory_url=symbol_directory_url,
        other_listed_url=other_listed_url,
        finance_people_url=finance_people_url,
        derive_finance_people_from_sec=derive_finance_people_from_sec,
        finance_people_archive_base_url=finance_people_archive_base_url,
        finance_people_max_companies=finance_people_max_companies,
        user_agent=user_agent,
    )
    return RegistryFetchFinanceSourcesResponse(
        pack_id=result.pack_id,
        output_dir=result.output_dir,
        snapshot=result.snapshot,
        snapshot_dir=result.snapshot_dir,
        sec_companies_url=result.sec_companies_url,
        sec_submissions_url=result.sec_submissions_url,
        sec_companyfacts_url=result.sec_companyfacts_url,
        symbol_directory_url=result.symbol_directory_url,
        other_listed_url=result.other_listed_url,
        finance_people_url=result.finance_people_url,
        source_manifest_path=result.source_manifest_path,
        sec_companies_path=result.sec_companies_path,
        sec_submissions_path=result.sec_submissions_path,
        sec_companyfacts_path=result.sec_companyfacts_path,
        symbol_directory_path=result.symbol_directory_path,
        other_listed_path=result.other_listed_path,
        finance_people_path=result.finance_people_path,
        curated_entities_path=result.curated_entities_path,
        generated_at=result.generated_at,
        source_count=result.source_count,
        curated_entity_count=result.curated_entity_count,
        sec_companies_sha256=result.sec_companies_sha256,
        sec_submissions_sha256=result.sec_submissions_sha256,
        sec_companyfacts_sha256=result.sec_companyfacts_sha256,
        symbol_directory_sha256=result.symbol_directory_sha256,
        other_listed_sha256=result.other_listed_sha256,
        finance_people_sha256=result.finance_people_sha256,
        curated_entities_sha256=result.curated_entities_sha256,
        warnings=result.warnings,
    )


def fetch_finance_country_source_snapshots(
    *,
    output_dir: str | Path = DEFAULT_FINANCE_COUNTRY_SOURCE_OUTPUT_ROOT,
    snapshot: str | None = None,
    country_codes: list[str] | tuple[str, ...] | None = None,
    user_agent: str = DEFAULT_COUNTRY_SOURCE_FETCH_USER_AGENT,
) -> RegistryFetchFinanceCountrySourcesResponse:
    """Download official source landing pages for country-scoped finance packs."""

    result = run_fetch_finance_country_source_snapshots(
        output_dir=output_dir,
        snapshot=snapshot,
        country_codes=list(country_codes or []),
        user_agent=user_agent,
    )
    return RegistryFetchFinanceCountrySourcesResponse(
        output_dir=result.output_dir,
        snapshot=result.snapshot,
        snapshot_dir=result.snapshot_dir,
        generated_at=result.generated_at,
        country_count=result.country_count,
        countries=[
            {
                "country_code": item.country_code,
                "country_name": item.country_name,
                "pack_id": item.pack_id,
                "snapshot_dir": item.snapshot_dir,
                "profile_path": item.profile_path,
                "source_manifest_path": item.source_manifest_path,
                "curated_entities_path": item.curated_entities_path,
                "source_count": item.source_count,
                "curated_entity_count": item.curated_entity_count,
                "warnings": item.warnings,
            }
            for item in result.countries
        ],
        warnings=result.warnings,
    )


def build_finance_country_source_bundles(
    *,
    snapshot_dir: str | Path,
    output_dir: str | Path = DEFAULT_FINANCE_COUNTRY_BUNDLE_OUTPUT_ROOT,
    country_codes: list[str] | tuple[str, ...] | None = None,
    version: str = "0.2.0",
) -> RegistryBuildFinanceCountryBundlesResponse:
    """Build country-scoped finance bundles from downloaded snapshots."""

    result = run_build_finance_country_source_bundles(
        snapshot_dir=snapshot_dir,
        output_dir=output_dir,
        country_codes=list(country_codes or []),
        version=version,
    )
    return RegistryBuildFinanceCountryBundlesResponse(
        output_dir=result.output_dir,
        generated_at=result.generated_at,
        country_count=result.country_count,
        bundles=[
            {
                "country_code": item.country_code,
                "country_name": item.country_name,
                "pack_id": item.pack_id,
                "version": item.version,
                "bundle_dir": item.bundle_dir,
                "bundle_manifest_path": item.bundle_manifest_path,
                "sources_lock_path": item.sources_lock_path,
                "entities_path": item.entities_path,
                "rules_path": item.rules_path,
                "generated_at": item.generated_at,
                "source_count": item.source_count,
                "entity_record_count": item.entity_record_count,
                "rule_record_count": item.rule_record_count,
                "organization_count": item.organization_count,
                "exchange_count": item.exchange_count,
                "market_index_count": item.market_index_count,
                "warnings": item.warnings,
            }
            for item in result.bundles
        ],
        warnings=result.warnings,
    )


def fetch_general_source_snapshot(
    *,
    output_dir: str | Path = DEFAULT_GENERAL_SOURCE_OUTPUT_ROOT,
    snapshot: str | None = None,
    wikidata_url: str | None = None,
    wikidata_truthy_url: str | None = DEFAULT_WIKIDATA_TRUTHY_URL,
    wikidata_entities_url: str | None = DEFAULT_WIKIDATA_ENTITIES_URL,
    wikidata_seed_url: str | None = DEFAULT_WIKIDATA_SEED_SNAPSHOT_URL,
    geonames_places_url: str = DEFAULT_GEONAMES_PLACES_URL,
    geonames_alternate_names_url: str | None = DEFAULT_GEONAMES_ALTERNATE_NAMES_URL,
    geonames_modifications_url: str | None = None,
    geonames_deletes_url: str | None = None,
    geonames_alternate_modifications_url: str | None = None,
    geonames_alternate_deletes_url: str | None = None,
    user_agent: str = DEFAULT_GENERAL_SOURCE_FETCH_USER_AGENT,
) -> RegistryFetchGeneralSourcesResponse:
    """Download one real general source snapshot set into the big-data root."""

    result = run_fetch_general_source_snapshot(
        output_dir=output_dir,
        snapshot=snapshot,
        wikidata_url=wikidata_url,
        wikidata_truthy_url=wikidata_truthy_url,
        wikidata_entities_url=wikidata_entities_url,
        wikidata_seed_url=wikidata_seed_url,
        geonames_places_url=geonames_places_url,
        geonames_alternate_names_url=geonames_alternate_names_url,
        geonames_modifications_url=geonames_modifications_url,
        geonames_deletes_url=geonames_deletes_url,
        geonames_alternate_modifications_url=geonames_alternate_modifications_url,
        geonames_alternate_deletes_url=geonames_alternate_deletes_url,
        user_agent=user_agent,
    )
    return RegistryFetchGeneralSourcesResponse(
        pack_id=result.pack_id,
        output_dir=result.output_dir,
        snapshot=result.snapshot,
        snapshot_dir=result.snapshot_dir,
        wikidata_url=result.wikidata_url,
        geonames_places_url=result.geonames_places_url,
        source_manifest_path=result.source_manifest_path,
        wikidata_entities_path=result.wikidata_entities_path,
        geonames_places_path=result.geonames_places_path,
        curated_entities_path=result.curated_entities_path,
        generated_at=result.generated_at,
        source_count=result.source_count,
        wikidata_entity_count=result.wikidata_entity_count,
        geonames_location_count=result.geonames_location_count,
        curated_entity_count=result.curated_entity_count,
        wikidata_entities_sha256=result.wikidata_entities_sha256,
        geonames_places_sha256=result.geonames_places_sha256,
        curated_entities_sha256=result.curated_entities_sha256,
        warnings=result.warnings,
    )


def fetch_medical_source_snapshot(
    *,
    output_dir: str | Path = DEFAULT_MEDICAL_SOURCE_OUTPUT_ROOT,
    snapshot: str | None = None,
    disease_ontology_url: str = DEFAULT_DISEASE_ONTOLOGY_URL,
    hgnc_genes_url: str = DEFAULT_HGNC_GENES_URL,
    uniprot_proteins_url: str = DEFAULT_UNIPROT_PROTEINS_URL,
    clinical_trials_url: str = DEFAULT_CLINICAL_TRIALS_URL,
    orange_book_url: str = DEFAULT_ORANGE_BOOK_URL,
    user_agent: str = DEFAULT_MEDICAL_SOURCE_FETCH_USER_AGENT,
    uniprot_max_records: int = DEFAULT_UNIPROT_MAX_RECORDS,
    clinical_trials_max_records: int = DEFAULT_CLINICAL_TRIALS_MAX_RECORDS,
) -> RegistryFetchMedicalSourcesResponse:
    """Download one real medical source snapshot set into the big-data root."""

    result = run_fetch_medical_source_snapshot(
        output_dir=output_dir,
        snapshot=snapshot,
        disease_ontology_url=disease_ontology_url,
        hgnc_genes_url=hgnc_genes_url,
        uniprot_proteins_url=uniprot_proteins_url,
        clinical_trials_url=clinical_trials_url,
        orange_book_url=orange_book_url,
        user_agent=user_agent,
        uniprot_max_records=uniprot_max_records,
        clinical_trials_max_records=clinical_trials_max_records,
    )
    return RegistryFetchMedicalSourcesResponse(
        pack_id=result.pack_id,
        output_dir=result.output_dir,
        snapshot=result.snapshot,
        snapshot_dir=result.snapshot_dir,
        disease_ontology_url=result.disease_ontology_url,
        hgnc_genes_url=result.hgnc_genes_url,
        uniprot_proteins_url=result.uniprot_proteins_url,
        clinical_trials_url=result.clinical_trials_url,
        orange_book_url=result.orange_book_url,
        source_manifest_path=result.source_manifest_path,
        disease_ontology_path=result.disease_ontology_path,
        hgnc_genes_path=result.hgnc_genes_path,
        uniprot_proteins_path=result.uniprot_proteins_path,
        clinical_trials_path=result.clinical_trials_path,
        orange_book_path=result.orange_book_path,
        curated_entities_path=result.curated_entities_path,
        generated_at=result.generated_at,
        source_count=result.source_count,
        disease_count=result.disease_count,
        gene_count=result.gene_count,
        protein_count=result.protein_count,
        clinical_trial_count=result.clinical_trial_count,
        orange_book_product_count=result.orange_book_product_count,
        curated_entity_count=result.curated_entity_count,
        disease_ontology_sha256=result.disease_ontology_sha256,
        hgnc_genes_sha256=result.hgnc_genes_sha256,
        uniprot_proteins_sha256=result.uniprot_proteins_sha256,
        clinical_trials_sha256=result.clinical_trials_sha256,
        orange_book_sha256=result.orange_book_sha256,
        curated_entities_sha256=result.curated_entities_sha256,
        warnings=result.warnings,
    )


def build_general_source_bundle(
    *,
    wikidata_entities_path: str | Path,
    geonames_places_path: str | Path,
    curated_entities_path: str | Path,
    output_dir: str | Path,
    version: str = "0.2.0",
) -> RegistryBuildGeneralBundleResponse:
    """Build one normalized `general-en` source bundle from raw snapshot files."""

    result = run_build_general_source_bundle(
        wikidata_entities_path=wikidata_entities_path,
        geonames_places_path=geonames_places_path,
        curated_entities_path=curated_entities_path,
        output_dir=output_dir,
        version=version,
    )
    return RegistryBuildGeneralBundleResponse(
        pack_id=result.pack_id,
        version=result.version,
        output_dir=result.output_dir,
        bundle_dir=result.bundle_dir,
        bundle_manifest_path=result.bundle_manifest_path,
        sources_lock_path=result.sources_lock_path,
        entities_path=result.entities_path,
        rules_path=result.rules_path,
        generated_at=result.generated_at,
        source_count=result.source_count,
        entity_record_count=result.entity_record_count,
        rule_record_count=result.rule_record_count,
        wikidata_entity_count=result.wikidata_entity_count,
        geonames_location_count=result.geonames_location_count,
        curated_entity_count=result.curated_entity_count,
        warnings=result.warnings,
    )


def build_medical_source_bundle(
    *,
    disease_ontology_path: str | Path,
    hgnc_genes_path: str | Path,
    uniprot_proteins_path: str | Path,
    clinical_trials_path: str | Path,
    orange_book_products_path: str | Path | None = None,
    curated_entities_path: str | Path,
    output_dir: str | Path,
    version: str = "0.2.0",
) -> RegistryBuildMedicalBundleResponse:
    """Build one normalized `medical-en` source bundle from raw snapshot files."""

    result = run_build_medical_source_bundle(
        disease_ontology_path=disease_ontology_path,
        hgnc_genes_path=hgnc_genes_path,
        uniprot_proteins_path=uniprot_proteins_path,
        clinical_trials_path=clinical_trials_path,
        orange_book_products_path=orange_book_products_path,
        curated_entities_path=curated_entities_path,
        output_dir=output_dir,
        version=version,
    )
    return RegistryBuildMedicalBundleResponse(
        pack_id=result.pack_id,
        version=result.version,
        output_dir=result.output_dir,
        bundle_dir=result.bundle_dir,
        bundle_manifest_path=result.bundle_manifest_path,
        sources_lock_path=result.sources_lock_path,
        entities_path=result.entities_path,
        rules_path=result.rules_path,
        generated_at=result.generated_at,
        source_count=result.source_count,
        entity_record_count=result.entity_record_count,
        rule_record_count=result.rule_record_count,
        disease_count=result.disease_count,
        gene_count=result.gene_count,
        protein_count=result.protein_count,
        clinical_trial_count=result.clinical_trial_count,
        drug_count=result.drug_count,
        curated_entity_count=result.curated_entity_count,
        warnings=result.warnings,
    )


def validate_general_pack_quality(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    fixture_profile: str = "benchmark",
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES,
    max_dropped_alias_ratio: float = 0.5,
) -> RegistryPackQualityResponse:
    """Build, install, and evaluate one generated `general-en` pack bundle."""

    result = run_validate_general_pack_quality(
        str(bundle_dir),
        output_dir=str(output_dir),
        fixture_profile=fixture_profile,
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )
    return RegistryPackQualityResponse(
        pack_id=result.pack_id,
        version=result.version,
        bundle_dir=result.bundle_dir,
        output_dir=result.output_dir,
        generated_pack_dir=result.generated_pack_dir,
        registry_dir=result.registry_dir,
        registry_index_path=result.registry_index_path,
        registry_index_url=result.registry_index_url,
        storage_root=result.storage_root,
        evaluated_at=result.evaluated_at,
        fixture_profile=result.fixture_profile,
        fixture_count=result.fixture_count,
        passed_case_count=result.passed_case_count,
        failed_case_count=result.failed_case_count,
        expected_entity_count=result.expected_entity_count,
        actual_entity_count=result.actual_entity_count,
        matched_expected_entity_count=result.matched_expected_entity_count,
        missing_expected_entity_count=result.missing_expected_entity_count,
        unexpected_entity_count=result.unexpected_entity_count,
        expected_recall=result.expected_recall,
        precision=result.precision,
        alias_count=result.alias_count,
        unique_canonical_count=result.unique_canonical_count,
        rule_count=result.rule_count,
        dropped_alias_count=result.dropped_alias_count,
        ambiguous_alias_count=result.ambiguous_alias_count,
        dropped_alias_ratio=result.dropped_alias_ratio,
        passed=result.passed,
        failures=result.failures,
        warnings=result.warnings,
        cases=[
            RegistryPackQualityCaseResult(
                name=case.name,
                text=case.text,
                expected_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.expected_entities
                ],
                actual_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.actual_entities
                ],
                matched_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.matched_entities
                ],
                missing_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.missing_entities
                ],
                unexpected_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.unexpected_entities
                ],
                passed=case.passed,
            )
            for case in result.cases
        ],
    )


def validate_medical_pack_quality(
    bundle_dir: str | Path,
    *,
    general_bundle_dir: str | Path,
    output_dir: str | Path,
    fixture_profile: str = "benchmark",
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = 25,
    max_dropped_alias_ratio: float = 0.5,
) -> RegistryPackQualityResponse:
    """Build, install, and evaluate one generated `medical-en` pack bundle."""

    result = run_validate_medical_pack_quality(
        str(bundle_dir),
        general_bundle_dir=str(general_bundle_dir),
        output_dir=str(output_dir),
        fixture_profile=fixture_profile,
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )
    return RegistryPackQualityResponse(
        pack_id=result.pack_id,
        version=result.version,
        bundle_dir=result.bundle_dir,
        output_dir=result.output_dir,
        generated_pack_dir=result.generated_pack_dir,
        registry_dir=result.registry_dir,
        registry_index_path=result.registry_index_path,
        registry_index_url=result.registry_index_url,
        storage_root=result.storage_root,
        evaluated_at=result.evaluated_at,
        fixture_profile=result.fixture_profile,
        fixture_count=result.fixture_count,
        passed_case_count=result.passed_case_count,
        failed_case_count=result.failed_case_count,
        expected_entity_count=result.expected_entity_count,
        actual_entity_count=result.actual_entity_count,
        matched_expected_entity_count=result.matched_expected_entity_count,
        missing_expected_entity_count=result.missing_expected_entity_count,
        unexpected_entity_count=result.unexpected_entity_count,
        expected_recall=result.expected_recall,
        precision=result.precision,
        alias_count=result.alias_count,
        unique_canonical_count=result.unique_canonical_count,
        rule_count=result.rule_count,
        dropped_alias_count=result.dropped_alias_count,
        ambiguous_alias_count=result.ambiguous_alias_count,
        dropped_alias_ratio=result.dropped_alias_ratio,
        passed=result.passed,
        failures=result.failures,
        warnings=result.warnings,
        cases=[
            RegistryPackQualityCaseResult(
                name=case.name,
                text=case.text,
                expected_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.expected_entities
                ],
                actual_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.actual_entities
                ],
                matched_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.matched_entities
                ],
                missing_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.missing_entities
                ],
                unexpected_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.unexpected_entities
                ],
                passed=case.passed,
            )
            for case in result.cases
        ],
    )


def validate_finance_pack_quality(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    fixture_profile: str = "benchmark",
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = 300,
    max_dropped_alias_ratio: float = 0.5,
) -> RegistryValidateFinanceQualityResponse:
    """Build, install, and evaluate one generated `finance-en` pack bundle."""

    result = run_validate_finance_pack_quality(
        bundle_dir,
        output_dir=output_dir,
        fixture_profile=fixture_profile,
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )
    return RegistryValidateFinanceQualityResponse(
        pack_id=result.pack_id,
        version=result.version,
        bundle_dir=result.bundle_dir,
        output_dir=result.output_dir,
        generated_pack_dir=result.generated_pack_dir,
        registry_dir=result.registry_dir,
        registry_index_path=result.registry_index_path,
        registry_index_url=result.registry_index_url,
        storage_root=result.storage_root,
        evaluated_at=result.evaluated_at,
        fixture_profile=result.fixture_profile,
        fixture_count=result.fixture_count,
        passed_case_count=result.passed_case_count,
        failed_case_count=result.failed_case_count,
        expected_entity_count=result.expected_entity_count,
        actual_entity_count=result.actual_entity_count,
        matched_expected_entity_count=result.matched_expected_entity_count,
        missing_expected_entity_count=result.missing_expected_entity_count,
        unexpected_entity_count=result.unexpected_entity_count,
        expected_recall=result.expected_recall,
        precision=result.precision,
        alias_count=result.alias_count,
        unique_canonical_count=result.unique_canonical_count,
        rule_count=result.rule_count,
        dropped_alias_count=result.dropped_alias_count,
        ambiguous_alias_count=result.ambiguous_alias_count,
        dropped_alias_ratio=result.dropped_alias_ratio,
        passed=result.passed,
        failures=result.failures,
        warnings=result.warnings,
        cases=[
            {
                "name": case.name,
                "text": case.text,
                "expected_entities": [
                    {"text": entity.text, "label": entity.label}
                    for entity in case.expected_entities
                ],
                "actual_entities": [
                    {"text": entity.text, "label": entity.label}
                    for entity in case.actual_entities
                ],
                "matched_entities": [
                    {"text": entity.text, "label": entity.label}
                    for entity in case.matched_entities
                ],
                "missing_entities": [
                    {"text": entity.text, "label": entity.label}
                    for entity in case.missing_entities
                ],
                "unexpected_entities": [
                    {"text": entity.text, "label": entity.label}
                    for entity in case.unexpected_entities
                ],
                "passed": case.passed,
            }
            for case in result.cases
        ],
    )


def _report_response_from_result(
    result: "GeneratedPackReport",
) -> RegistryReportPackResponse:
    return RegistryReportPackResponse(
        pack_id=result.pack_id,
        version=result.version,
        bundle_dir=result.bundle_dir,
        output_dir=result.output_dir,
        pack_dir=result.pack_dir,
        manifest_path=result.manifest_path,
        labels_path=result.labels_path,
        aliases_path=result.aliases_path,
        rules_path=result.rules_path,
        matcher_artifact_path=result.matcher_artifact_path,
        matcher_entries_path=result.matcher_entries_path,
        matcher_algorithm=result.matcher_algorithm,
        matcher_entry_count=result.matcher_entry_count,
        matcher_state_count=result.matcher_state_count,
        matcher_max_alias_length=result.matcher_max_alias_length,
        matcher_artifact_sha256=result.matcher_artifact_sha256,
        matcher_entries_sha256=result.matcher_entries_sha256,
        sources_path=result.sources_path,
        build_path=result.build_path,
        generated_at=result.generated_at,
        source_count=result.source_count,
        publishable_source_count=result.publishable_source_count,
        restricted_source_count=result.restricted_source_count,
        publishable_sources_only=result.publishable_sources_only,
        source_license_classes=result.source_license_classes,
        label_count=result.label_count,
        alias_count=result.alias_count,
        unique_canonical_count=result.unique_canonical_count,
        rule_count=result.rule_count,
        included_entity_count=result.included_entity_count,
        included_rule_count=result.included_rule_count,
        dropped_record_count=result.dropped_record_count,
        dropped_alias_count=result.dropped_alias_count,
        ambiguous_alias_count=result.ambiguous_alias_count,
        dropped_alias_ratio=result.dropped_alias_ratio,
        label_distribution=result.label_distribution,
        warnings=result.warnings,
    )


def _quality_response_from_result(
    result: "PackQualityResult",
) -> RegistryPackQualityResponse:
    return RegistryPackQualityResponse(
        pack_id=result.pack_id,
        version=result.version,
        bundle_dir=result.bundle_dir,
        output_dir=result.output_dir,
        generated_pack_dir=result.generated_pack_dir,
        registry_dir=result.registry_dir,
        registry_index_path=result.registry_index_path,
        registry_index_url=result.registry_index_url,
        storage_root=result.storage_root,
        evaluated_at=result.evaluated_at,
        fixture_profile=result.fixture_profile,
        fixture_count=result.fixture_count,
        passed_case_count=result.passed_case_count,
        failed_case_count=result.failed_case_count,
        expected_entity_count=result.expected_entity_count,
        actual_entity_count=result.actual_entity_count,
        matched_expected_entity_count=result.matched_expected_entity_count,
        missing_expected_entity_count=result.missing_expected_entity_count,
        unexpected_entity_count=result.unexpected_entity_count,
        expected_recall=result.expected_recall,
        precision=result.precision,
        alias_count=result.alias_count,
        unique_canonical_count=result.unique_canonical_count,
        rule_count=result.rule_count,
        dropped_alias_count=result.dropped_alias_count,
        ambiguous_alias_count=result.ambiguous_alias_count,
        dropped_alias_ratio=result.dropped_alias_ratio,
        passed=result.passed,
        failures=result.failures,
        warnings=result.warnings,
        cases=[
            RegistryPackQualityCaseResult(
                name=case.name,
                text=case.text,
                expected_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.expected_entities
                ],
                actual_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.actual_entities
                ],
                matched_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.matched_entities
                ],
                missing_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.missing_entities
                ],
                unexpected_entities=[
                    {"text": entity.text, "label": entity.label}
                    for entity in case.unexpected_entities
                ],
                passed=case.passed,
            )
            for case in result.cases
        ],
    )


def _registry_quality_entity(entity: object) -> dict[str, str]:
    return {
        "text": str(getattr(entity, "text")),
        "label": str(getattr(entity, "label")),
    }


def _extraction_case_response(case: object) -> ExtractionQualityCaseResultResponse:
    return ExtractionQualityCaseResultResponse(
        name=str(getattr(case, "name")),
        expected_entities=[
            _registry_quality_entity(entity)
            for entity in getattr(case, "expected_entities")
        ],
        actual_entities=[
            _registry_quality_entity(entity)
            for entity in getattr(case, "actual_entities")
        ],
        matched_entities=[
            _registry_quality_entity(entity)
            for entity in getattr(case, "matched_entities")
        ],
        missing_entities=[
            _registry_quality_entity(entity)
            for entity in getattr(case, "missing_entities")
        ],
        unexpected_entities=[
            _registry_quality_entity(entity)
            for entity in getattr(case, "unexpected_entities")
        ],
        warnings=[str(item) for item in getattr(case, "warnings")],
        token_count=int(getattr(case, "token_count")),
        entity_count=int(getattr(case, "entity_count")),
        entities_per_100_tokens=float(getattr(case, "entities_per_100_tokens")),
        overlap_drop_count=int(getattr(case, "overlap_drop_count")),
        chunk_count=int(getattr(case, "chunk_count")),
        timing_ms=int(getattr(case, "timing_ms")),
        passed=bool(getattr(case, "passed")),
    )


def _extraction_report_response(
    report: object,
    *,
    report_path: str | None = None,
) -> RegistryEvaluateExtractionQualityResponse:
    return RegistryEvaluateExtractionQualityResponse(
        report_path=report_path,
        pack_id=str(getattr(report, "pack_id")),
        profile=str(getattr(report, "profile")),
        hybrid=bool(getattr(report, "hybrid")),
        document_count=int(getattr(report, "document_count")),
        expected_entity_count=int(getattr(report, "expected_entity_count")),
        actual_entity_count=int(getattr(report, "actual_entity_count")),
        matched_entity_count=int(getattr(report, "matched_entity_count")),
        missing_entity_count=int(getattr(report, "missing_entity_count")),
        unexpected_entity_count=int(getattr(report, "unexpected_entity_count")),
        recall=float(getattr(report, "recall")),
        precision=float(getattr(report, "precision")),
        entities_per_100_tokens=float(getattr(report, "entities_per_100_tokens")),
        overlap_drop_count=int(getattr(report, "overlap_drop_count")),
        chunk_count=int(getattr(report, "chunk_count")),
        low_density_warning_count=int(getattr(report, "low_density_warning_count")),
        p50_latency_ms=int(getattr(report, "p50_latency_ms", 0)),
        p95_latency_ms=int(getattr(report, "p95_latency_ms")),
        per_label_expected=dict(getattr(report, "per_label_expected")),
        per_label_actual=dict(getattr(report, "per_label_actual")),
        per_label_matched=dict(getattr(report, "per_label_matched")),
        per_label_recall=dict(getattr(report, "per_label_recall")),
        per_lane_counts=dict(getattr(report, "per_lane_counts")),
        warnings=[str(item) for item in getattr(report, "warnings")],
        cases=[
            _extraction_case_response(case)
            for case in getattr(report, "cases")
        ],
    )


def _vector_case_response(case: object) -> VectorCaseResultResponse:
    return VectorCaseResultResponse(
        name=str(getattr(case, "name")),
        case_kind=str(getattr(case, "case_kind")),
        seed_entity_ids=[str(item) for item in getattr(case, "seed_entity_ids")],
        expected_related_entity_ids=[
            str(item) for item in getattr(case, "expected_related_entity_ids")
        ],
        actual_related_entity_ids=[
            str(item) for item in getattr(case, "actual_related_entity_ids")
        ],
        matched_related_entity_ids=[
            str(item) for item in getattr(case, "matched_related_entity_ids")
        ],
        missing_related_entity_ids=[
            str(item) for item in getattr(case, "missing_related_entity_ids")
        ],
        unexpected_related_entity_ids=[
            str(item) for item in getattr(case, "unexpected_related_entity_ids")
        ],
        expected_boosted_entity_ids=[
            str(item) for item in getattr(case, "expected_boosted_entity_ids")
        ],
        actual_boosted_entity_ids=[
            str(item) for item in getattr(case, "actual_boosted_entity_ids")
        ],
        expected_downgraded_entity_ids=[
            str(item) for item in getattr(case, "expected_downgraded_entity_ids")
        ],
        actual_downgraded_entity_ids=[
            str(item) for item in getattr(case, "actual_downgraded_entity_ids")
        ],
        expected_suppressed_entity_ids=[
            str(item) for item in getattr(case, "expected_suppressed_entity_ids")
        ],
        actual_suppressed_entity_ids=[
            str(item) for item in getattr(case, "actual_suppressed_entity_ids")
        ],
        expected_refinement_applied=bool(getattr(case, "expected_refinement_applied")),
        actual_refinement_applied=bool(getattr(case, "actual_refinement_applied")),
        graph_support_applied=bool(getattr(case, "graph_support_applied")),
        fallback_used=bool(getattr(case, "fallback_used")),
        refinement_score=(
            float(getattr(case, "refinement_score"))
            if getattr(case, "refinement_score") is not None
            else None
        ),
        related_precision_at_k=float(getattr(case, "related_precision_at_k")),
        related_recall_at_k=float(getattr(case, "related_recall_at_k")),
        reciprocal_rank=float(getattr(case, "reciprocal_rank")),
        timing_ms=int(getattr(case, "timing_ms")),
        warnings=[str(item) for item in getattr(case, "warnings")],
        passed=bool(getattr(case, "passed")),
    )


def _vector_quality_report_response(
    report: object,
    *,
    report_path: str | None = None,
) -> RegistryEvaluateVectorQualityResponse:
    return RegistryEvaluateVectorQualityResponse(
        report_path=report_path,
        pack_id=str(getattr(report, "pack_id")),
        profile=str(getattr(report, "profile")),
        refinement_depth=str(getattr(report, "refinement_depth")),
        collection_alias=(
            str(getattr(report, "collection_alias"))
            if getattr(report, "collection_alias") is not None
            else None
        ),
        provider=(
            str(getattr(report, "provider"))
            if getattr(report, "provider") is not None
            else None
        ),
        case_count=int(getattr(report, "case_count")),
        top_k=int(getattr(report, "top_k")),
        related_precision_at_k=float(getattr(report, "related_precision_at_k")),
        related_recall_at_k=float(getattr(report, "related_recall_at_k")),
        related_mrr=float(getattr(report, "related_mrr")),
        suppression_alignment_rate=float(getattr(report, "suppression_alignment_rate")),
        refinement_alignment_rate=float(getattr(report, "refinement_alignment_rate")),
        easy_case_pass_rate=float(getattr(report, "easy_case_pass_rate")),
        graph_support_rate=float(getattr(report, "graph_support_rate")),
        refinement_trigger_rate=float(getattr(report, "refinement_trigger_rate")),
        fallback_rate=float(getattr(report, "fallback_rate")),
        p50_latency_ms=int(getattr(report, "p50_latency_ms")),
        p95_latency_ms=int(getattr(report, "p95_latency_ms")),
        warnings=[str(item) for item in getattr(report, "warnings")],
        cases=[
            _vector_case_response(case)
            for case in getattr(report, "cases")
        ],
    )


def _live_news_issue_response(issue: object) -> LiveNewsIssueResponse:
    return LiveNewsIssueResponse(
        issue_type=str(getattr(issue, "issue_type")),
        message=str(getattr(issue, "message")),
        entity_text=(
            str(getattr(issue, "entity_text"))
            if getattr(issue, "entity_text") is not None
            else None
        ),
        label=(
            str(getattr(issue, "label"))
            if getattr(issue, "label") is not None
            else None
        ),
        candidate_text=(
            str(getattr(issue, "candidate_text"))
            if getattr(issue, "candidate_text") is not None
            else None
        ),
    )


def _live_news_entity_response(entity: object) -> LiveNewsEntityResponse:
    return LiveNewsEntityResponse(
        text=str(getattr(entity, "text")),
        label=str(getattr(entity, "label")),
        start=int(getattr(entity, "start")),
        end=int(getattr(entity, "end")),
    )


def _live_news_failure_response(failure: object) -> LiveNewsFailureResponse:
    return LiveNewsFailureResponse(
        source=str(getattr(failure, "source")),
        stage=str(getattr(failure, "stage")),
        url=str(getattr(failure, "url")),
        message=str(getattr(failure, "message")),
        title=(
            str(getattr(failure, "title"))
            if getattr(failure, "title") is not None
            else None
        ),
    )


def _live_news_article_response(article: object) -> LiveNewsArticleResultResponse:
    return LiveNewsArticleResultResponse(
        source=str(getattr(article, "source")),
        title=str(getattr(article, "title")),
        article_url=str(getattr(article, "article_url")),
        published_at=(
            str(getattr(article, "published_at"))
            if getattr(article, "published_at") is not None
            else None
        ),
        text_source=str(getattr(article, "text_source")),
        word_count=int(getattr(article, "word_count")),
        entity_count=int(getattr(article, "entity_count")),
        timing_ms=int(getattr(article, "timing_ms")),
        warnings=[str(item) for item in getattr(article, "warnings")],
        issue_types=[str(item) for item in getattr(article, "issue_types")],
        issues=[
            _live_news_issue_response(issue)
            for issue in getattr(article, "issues")
        ],
        entities=[
            _live_news_entity_response(entity)
            for entity in getattr(article, "entities")
        ],
    )


def _live_news_feedback_response(
    report: object,
    *,
    report_path: str | None = None,
) -> RegistryEvaluateLiveNewsFeedbackResponse:
    return RegistryEvaluateLiveNewsFeedbackResponse(
        report_path=report_path,
        pack_id=str(getattr(report, "pack_id")),
        generated_at=str(getattr(report, "generated_at")),
        requested_article_count=int(getattr(report, "requested_article_count")),
        collected_article_count=int(getattr(report, "collected_article_count")),
        feed_count=int(getattr(report, "feed_count")),
        successful_feed_count=int(getattr(report, "successful_feed_count")),
        p50_latency_ms=int(getattr(report, "p50_latency_ms")),
        p95_latency_ms=int(getattr(report, "p95_latency_ms")),
        per_source_article_counts=dict(getattr(report, "per_source_article_counts")),
        per_issue_counts=dict(getattr(report, "per_issue_counts")),
        suggested_fix_classes=[
            str(item) for item in getattr(report, "suggested_fix_classes")
        ],
        warnings=[str(item) for item in getattr(report, "warnings")],
        feed_failures=[
            _live_news_failure_response(item)
            for item in getattr(report, "feed_failures")
        ],
        article_failures=[
            _live_news_failure_response(item)
            for item in getattr(report, "article_failures")
        ],
        articles=[
            _live_news_article_response(item)
            for item in getattr(report, "articles")
        ],
    )


def _runtime_latency_response(summary: object) -> RuntimeLatencyBenchmarkResponse:
    return RuntimeLatencyBenchmarkResponse(
        sample_count=int(getattr(summary, "sample_count")),
        p50_latency_ms=int(getattr(summary, "p50_latency_ms")),
        p95_latency_ms=int(getattr(summary, "p95_latency_ms")),
    )


def _runtime_disk_response(summary: object) -> RuntimeDiskBenchmarkResponse:
    return RuntimeDiskBenchmarkResponse(
        runtime_pack_bytes=int(getattr(summary, "runtime_pack_bytes")),
        matcher_artifact_bytes=int(getattr(summary, "matcher_artifact_bytes")),
        matcher_entries_bytes=int(getattr(summary, "matcher_entries_bytes")),
        matcher_total_bytes=int(getattr(summary, "matcher_total_bytes")),
        metadata_store_bytes=(
            int(getattr(summary, "metadata_store_bytes"))
            if getattr(summary, "metadata_store_bytes") is not None
            else None
        ),
    )


def _runtime_benchmark_response(
    report: object,
    *,
    report_path: str | None = None,
) -> RegistryBenchmarkRuntimeResponse:
    return RegistryBenchmarkRuntimeResponse(
        report_path=report_path,
        pack_id=str(getattr(report, "pack_id")),
        profile=str(getattr(report, "profile")),
        hybrid=bool(getattr(report, "hybrid")),
        metadata_backend=str(getattr(report, "metadata_backend")),
        exact_extraction_backend=str(getattr(report, "exact_extraction_backend")),
        operator_lookup_backend=str(getattr(report, "operator_lookup_backend")),
        matcher_entry_count=int(getattr(report, "matcher_entry_count")),
        matcher_state_count=int(getattr(report, "matcher_state_count")),
        matcher_cold_load_ms=int(getattr(report, "matcher_cold_load_ms")),
        lookup_query_count=int(getattr(report, "lookup_query_count")),
        warm_tagging=_runtime_latency_response(getattr(report, "warm_tagging")),
        operator_lookup=_runtime_latency_response(getattr(report, "operator_lookup")),
        disk_usage=_runtime_disk_response(getattr(report, "disk_usage")),
        quality_report=ExtractionQualityReportResponse(
            **_extraction_report_response(getattr(report, "quality_report"))
            .model_dump(mode="json", exclude={"report_path"})
        ),
        warnings=[str(item) for item in getattr(report, "warnings")],
    )


def _matcher_backend_candidate_response(candidate: object) -> MatcherBackendCandidateResponse:
    return MatcherBackendCandidateResponse(
        backend=str(getattr(candidate, "backend")),
        available=bool(getattr(candidate, "available")),
        alias_count=int(getattr(candidate, "alias_count")),
        unique_pattern_count=int(getattr(candidate, "unique_pattern_count")),
        build_ms=int(getattr(candidate, "build_ms")),
        cold_load_ms=int(getattr(candidate, "cold_load_ms")),
        warm_query_p50_ms=int(getattr(candidate, "warm_query_p50_ms")),
        warm_query_p95_ms=int(getattr(candidate, "warm_query_p95_ms")),
        artifact_bytes=int(getattr(candidate, "artifact_bytes")),
        state_count=int(getattr(candidate, "state_count")),
        token_vocab_count=int(getattr(candidate, "token_vocab_count")),
        notes=[str(item) for item in getattr(candidate, "notes")],
    )


def _matcher_benchmark_spike_response(
    report: object,
    *,
    report_path: str | None = None,
) -> RegistryBenchmarkMatcherBackendsResponse:
    return RegistryBenchmarkMatcherBackendsResponse(
        report_path=report_path,
        pack_id=str(getattr(report, "pack_id")),
        profile=str(getattr(report, "profile")),
        aliases_path=str(getattr(report, "aliases_path")),
        scanned_alias_count=int(getattr(report, "scanned_alias_count")),
        retained_alias_count=int(getattr(report, "retained_alias_count")),
        unique_pattern_count=int(getattr(report, "unique_pattern_count")),
        query_count=int(getattr(report, "query_count")),
        query_runs=int(getattr(report, "query_runs")),
        min_alias_score=float(getattr(report, "min_alias_score")),
        alias_limit=int(getattr(report, "alias_limit")),
        scan_limit=int(getattr(report, "scan_limit")),
        exact_tier_min_token_count=int(getattr(report, "exact_tier_min_token_count")),
        candidates=[
            _matcher_backend_candidate_response(candidate)
            for candidate in getattr(report, "candidates")
        ],
        recommended_backend=(
            str(getattr(report, "recommended_backend"))
            if getattr(report, "recommended_backend") is not None
            else None
        ),
        warnings=[str(item) for item in getattr(report, "warnings")],
    )


def _quality_delta_response(delta: object) -> ExtractionQualityDeltaResponse:
    return ExtractionQualityDeltaResponse(
        pack_id=str(getattr(delta, "pack_id")),
        baseline_mode=str(getattr(delta, "baseline_mode")),
        candidate_mode=str(getattr(delta, "candidate_mode")),
        recall_delta=float(getattr(delta, "recall_delta")),
        precision_delta=float(getattr(delta, "precision_delta")),
        p95_latency_delta_ms=int(getattr(delta, "p95_latency_delta_ms")),
        per_label_deltas=[
            LabelQualityDeltaResponse(
                label=str(getattr(item, "label")),
                baseline_recall=float(getattr(item, "baseline_recall")),
                candidate_recall=float(getattr(item, "candidate_recall")),
                recall_delta=float(getattr(item, "recall_delta")),
            )
            for item in getattr(delta, "per_label_deltas")
        ],
    )


def _pack_diff_response(diff: object) -> RegistryPackDiffResponse:
    return RegistryPackDiffResponse(
        pack_id=str(getattr(diff, "pack_id")),
        from_version=getattr(diff, "from_version"),
        to_version=getattr(diff, "to_version"),
        severity=str(getattr(diff, "severity")),
        requires_retag=bool(getattr(diff, "requires_retag")),
        added_labels=[str(item) for item in getattr(diff, "added_labels")],
        removed_labels=[str(item) for item in getattr(diff, "removed_labels")],
        added_rules=[str(item) for item in getattr(diff, "added_rules")],
        removed_rules=[str(item) for item in getattr(diff, "removed_rules")],
        added_aliases=[str(item) for item in getattr(diff, "added_aliases")],
        removed_aliases=[str(item) for item in getattr(diff, "removed_aliases")],
        changed_canonical_aliases=[
            str(item) for item in getattr(diff, "changed_canonical_aliases")
        ],
        changed_metadata_fields=[
            str(item) for item in getattr(diff, "changed_metadata_fields")
        ],
    )


def evaluate_extraction_quality(
    pack_id: str,
    *,
    storage_root: str | Path | None = None,
    golden_set_path: str | Path | None = None,
    profile: str = "default",
    hybrid: bool = False,
    write_report: bool = True,
    report_path: str | Path | None = None,
) -> RegistryEvaluateExtractionQualityResponse:
    """Evaluate one installed pack against one golden set and optionally persist the report."""

    settings = _resolve_settings(storage_root=storage_root)
    if not write_report and report_path is not None:
        raise ValueError("report_path requires write_report=True.")
    resolved_golden_set_path = (
        Path(golden_set_path).expanduser().resolve()
        if golden_set_path is not None
        else default_golden_set_path(pack_id, profile=profile)
    )
    golden_set = load_golden_set(resolved_golden_set_path)
    if golden_set.pack_id != pack_id:
        raise ValueError(
            "Golden set pack_id does not match requested pack_id: "
            f"{golden_set.pack_id} != {pack_id}"
        )
    report = evaluate_golden_set(
        golden_set,
        storage_root=settings.storage_root,
        hybrid=hybrid,
    )
    persisted_report_path: str | None = None
    if write_report:
        destination = (
            Path(report_path).expanduser().resolve()
            if report_path is not None
            else default_quality_report_path(
                pack_id,
                profile=golden_set.profile,
                hybrid=hybrid,
            )
        )
        persisted_report_path = str(write_quality_report(destination, report))
    return _extraction_report_response(report, report_path=persisted_report_path)


def evaluate_live_news_feedback(
    pack_id: str,
    *,
    storage_root: str | Path | None = None,
    article_limit: int = 10,
    per_feed_limit: int = 10,
    write_report: bool = True,
    report_path: str | Path | None = None,
) -> RegistryEvaluateLiveNewsFeedbackResponse:
    """Fetch live RSS articles, run tagging, and aggregate generic issue classes."""

    settings = _resolve_settings(storage_root=storage_root)
    if not write_report and report_path is not None:
        raise ValueError("report_path requires write_report=True.")
    report = run_evaluate_live_news_feedback(
        pack_id,
        storage_root=settings.storage_root,
        article_limit=article_limit,
        per_feed_limit=per_feed_limit,
    )
    persisted_report_path: str | None = None
    if write_report:
        destination = (
            Path(report_path).expanduser().resolve()
            if report_path is not None
            else default_live_news_feedback_report_path(pack_id)
        )
        persisted_report_path = str(
            write_live_news_feedback_report(destination, report)
        )
    return _live_news_feedback_response(report, report_path=persisted_report_path)


def benchmark_runtime(
    pack_id: str,
    *,
    storage_root: str | Path | None = None,
    golden_set_path: str | Path | None = None,
    profile: str = "default",
    hybrid: bool = False,
    warm_runs: int = 3,
    lookup_limit: int = 20,
    write_report: bool = True,
    report_path: str | Path | None = None,
) -> RegistryBenchmarkRuntimeResponse:
    """Benchmark one installed pack across matcher, tagging, and lookup lanes."""

    settings = _resolve_settings(storage_root=storage_root)
    if not write_report and report_path is not None:
        raise ValueError("report_path requires write_report=True.")
    resolved_golden_set_path = (
        Path(golden_set_path).expanduser().resolve()
        if golden_set_path is not None
        else default_golden_set_path(pack_id, profile=profile)
    )
    golden_set = load_golden_set(resolved_golden_set_path)
    if golden_set.pack_id != pack_id:
        raise ValueError(
            "Golden set pack_id does not match requested pack_id: "
            f"{golden_set.pack_id} != {pack_id}"
        )
    benchmark = run_benchmark_runtime_pack(
        golden_set,
        storage_root=settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
        hybrid=hybrid,
        warm_runs=warm_runs,
        lookup_limit=lookup_limit,
    )
    persisted_report_path: str | None = None
    if write_report:
        destination = (
            Path(report_path).expanduser().resolve()
            if report_path is not None
            else default_benchmark_report_path(
                pack_id,
                profile=golden_set.profile,
                hybrid=hybrid,
            )
        )
        persisted_report_path = str(write_runtime_benchmark_report(destination, benchmark))
    return _runtime_benchmark_response(benchmark, report_path=persisted_report_path)


def benchmark_matcher_backends(
    pack_id: str,
    *,
    storage_root: str | Path | None = None,
    golden_set_path: str | Path | None = None,
    profile: str = "default",
    alias_limit: int = 10000,
    scan_limit: int = 50000,
    min_alias_score: float = 0.8,
    exact_tier_min_token_count: int = 2,
    query_runs: int = 3,
    write_report: bool = True,
    report_path: str | Path | None = None,
    output_root: str | Path | None = None,
) -> RegistryBenchmarkMatcherBackendsResponse:
    """Run the Phase-0 matcher backend benchmark spike on one installed pack."""

    settings = _resolve_settings(storage_root=storage_root)
    if not write_report and report_path is not None:
        raise ValueError("report_path requires write_report=True.")
    resolved_golden_set_path = (
        Path(golden_set_path).expanduser().resolve()
        if golden_set_path is not None
        else default_golden_set_path(pack_id, profile=profile)
    )
    golden_set = load_golden_set(resolved_golden_set_path)
    if golden_set.pack_id != pack_id:
        raise ValueError(
            "Golden set pack_id does not match requested pack_id: "
            f"{golden_set.pack_id} != {pack_id}"
        )
    benchmark = run_benchmark_matcher_backends_for_pack(
        pack_id,
        storage_root=settings.storage_root,
        golden_set=golden_set,
        alias_limit=alias_limit,
        scan_limit=scan_limit,
        min_alias_score=min_alias_score,
        exact_tier_min_token_count=exact_tier_min_token_count,
        query_runs=query_runs,
        output_root=output_root,
    )
    persisted_report_path: str | None = None
    if write_report:
        destination = (
            Path(report_path).expanduser().resolve()
            if report_path is not None
            else default_matcher_benchmark_spike_report_path(
                pack_id,
                profile=golden_set.profile,
            )
        )
        persisted_report_path = str(
            write_matcher_benchmark_spike_report(destination, benchmark)
        )
    return _matcher_benchmark_spike_response(
        benchmark,
        report_path=persisted_report_path,
    )


def compare_extraction_quality_reports(
    *,
    baseline_report_path: str | Path,
    candidate_report_path: str | Path,
) -> RegistryCompareExtractionQualityResponse:
    """Compare two stored extraction-quality reports."""

    baseline_path = Path(baseline_report_path).expanduser().resolve()
    candidate_path = Path(candidate_report_path).expanduser().resolve()
    baseline = load_quality_report(baseline_path)
    candidate = load_quality_report(candidate_path)
    if baseline.pack_id != candidate.pack_id:
        raise ValueError(
            "Extraction-quality reports must belong to the same pack: "
            f"{baseline.pack_id} != {candidate.pack_id}"
        )
    delta = compare_quality_reports(baseline, candidate)
    return RegistryCompareExtractionQualityResponse(
        baseline_report_path=str(baseline_path),
        candidate_report_path=str(candidate_path),
        **_quality_delta_response(delta).model_dump(mode="json"),
    )


def diff_pack_versions(
    old_pack_dir: str | Path,
    new_pack_dir: str | Path,
) -> RegistryPackDiffResponse:
    """Return the semantic diff between two runtime pack directories."""

    return _pack_diff_response(diff_pack_directories(old_pack_dir, new_pack_dir))


def evaluate_extraction_release_thresholds(
    *,
    report_path: str | Path,
    mode: str,
    baseline_report_path: str | Path | None = None,
    min_recall: float | None = None,
    min_precision: float | None = None,
    max_label_recall_drop: float | None = None,
    min_recall_lift: float | None = None,
    max_precision_drop: float | None = None,
    max_p95_latency_ms: int | None = None,
    max_model_artifact_bytes: int | None = None,
    max_peak_memory_mb: int | None = None,
    model_artifact_path: str | Path | None = None,
    peak_memory_mb: int | None = None,
) -> RegistryEvaluateReleaseThresholdsResponse:
    """Evaluate one stored extraction-quality report against release thresholds."""

    if mode not in {"deterministic", "hybrid"}:
        raise ValueError("mode must be deterministic or hybrid.")
    resolved_report_path = Path(report_path).expanduser().resolve()
    report = load_quality_report(resolved_report_path)
    resolved_baseline_path: Path | None = None
    baseline_report = None
    if baseline_report_path is not None:
        resolved_baseline_path = Path(baseline_report_path).expanduser().resolve()
        baseline_report = load_quality_report(resolved_baseline_path)
        if baseline_report.pack_id != report.pack_id:
            raise ValueError(
                "Baseline and candidate reports must belong to the same pack: "
                f"{baseline_report.pack_id} != {report.pack_id}"
            )
    resolved_model_artifact_bytes: int | None = None
    if model_artifact_path is not None:
        resolved_model_artifact_bytes = (
            Path(model_artifact_path).expanduser().resolve().stat().st_size
        )
    defaults = default_release_thresholds(mode)
    thresholds = ReleaseThresholds(
        min_recall=defaults.min_recall if min_recall is None else min_recall,
        min_precision=defaults.min_precision if min_precision is None else min_precision,
        max_label_recall_drop=(
            defaults.max_label_recall_drop
            if max_label_recall_drop is None
            else max_label_recall_drop
        ),
        min_recall_lift=(
            defaults.min_recall_lift
            if min_recall_lift is None
            else min_recall_lift
        ),
        max_precision_drop=(
            defaults.max_precision_drop
            if max_precision_drop is None
            else max_precision_drop
        ),
        max_p95_latency_ms=(
            defaults.max_p95_latency_ms
            if max_p95_latency_ms is None
            else max_p95_latency_ms
        ),
        max_model_artifact_bytes=(
            defaults.max_model_artifact_bytes
            if max_model_artifact_bytes is None
            else max_model_artifact_bytes
        ),
        max_peak_memory_mb=(
            defaults.max_peak_memory_mb
            if max_peak_memory_mb is None
            else max_peak_memory_mb
        ),
    )
    decision = evaluate_release_thresholds(
        report,
        mode=mode,
        thresholds=thresholds,
        baseline_report=baseline_report,
        model_artifact_bytes=resolved_model_artifact_bytes,
        peak_memory_mb=peak_memory_mb,
    )
    return RegistryEvaluateReleaseThresholdsResponse(
        report_path=str(resolved_report_path),
        baseline_report_path=str(resolved_baseline_path) if resolved_baseline_path else None,
        mode=str(mode),
        thresholds=RegistryReleaseThresholdsResponse(
            min_recall=thresholds.min_recall,
            min_precision=thresholds.min_precision,
            max_label_recall_drop=thresholds.max_label_recall_drop,
            min_recall_lift=thresholds.min_recall_lift,
            max_precision_drop=thresholds.max_precision_drop,
            max_p95_latency_ms=thresholds.max_p95_latency_ms,
            max_model_artifact_bytes=thresholds.max_model_artifact_bytes,
            max_peak_memory_mb=thresholds.max_peak_memory_mb,
        ),
        model_artifact_bytes=resolved_model_artifact_bytes,
        peak_memory_mb=peak_memory_mb,
        passed=decision.passed,
        reasons=[str(item) for item in decision.reasons],
        quality_delta=(
            _quality_delta_response(decision.quality_delta)
            if decision.quality_delta is not None
            else None
        ),
    )


def evaluate_vector_quality(
    pack_id: str,
    *,
    storage_root: str | Path | None = None,
    golden_set_path: str | Path | None = None,
    profile: str = "default",
    refinement_depth: str = "light",
    top_k: int | None = None,
    write_report: bool = True,
    report_path: str | Path | None = None,
) -> RegistryEvaluateVectorQualityResponse:
    """Evaluate the hosted vector refinement lane against one vector golden set."""

    settings = _resolve_settings(storage_root=storage_root)
    if not write_report and report_path is not None:
        raise ValueError("report_path requires write_report=True.")
    resolved_golden_set_path = (
        Path(golden_set_path).expanduser().resolve()
        if golden_set_path is not None
        else default_vector_golden_set_path(pack_id, profile=profile)
    )
    golden_set = load_vector_golden_set(resolved_golden_set_path)
    if golden_set.pack_id != pack_id:
        raise ValueError(
            "Vector golden set pack_id does not match requested pack_id: "
            f"{golden_set.pack_id} != {pack_id}"
        )
    report = evaluate_vector_golden_set(
        golden_set,
        settings=settings,
        refinement_depth=refinement_depth,
        top_k=top_k,
    )
    persisted_report_path: str | None = None
    if write_report:
        destination = (
            Path(report_path).expanduser().resolve()
            if report_path is not None
            else default_vector_quality_report_path(
                pack_id,
                profile=golden_set.profile,
            )
        )
        persisted_report_path = str(write_vector_quality_report(destination, report))
    return _vector_quality_report_response(report, report_path=persisted_report_path)


def evaluate_vector_release_thresholds_from_report(
    *,
    report_path: str | Path,
    min_related_precision_at_k: float | None = None,
    min_related_recall_at_k: float | None = None,
    min_related_mrr: float | None = None,
    min_suppression_alignment_rate: float | None = None,
    min_refinement_alignment_rate: float | None = None,
    min_easy_case_pass_rate: float | None = None,
    max_fallback_rate: float | None = None,
    max_p95_latency_ms: int | None = None,
) -> RegistryEvaluateVectorReleaseThresholdsResponse:
    """Evaluate one stored vector-quality report against hosted release thresholds."""

    resolved_report_path = Path(report_path).expanduser().resolve()
    report = load_vector_quality_report(resolved_report_path)
    defaults = default_vector_release_thresholds()
    thresholds = VectorReleaseThresholds(
        min_related_precision_at_k=(
            defaults.min_related_precision_at_k
            if min_related_precision_at_k is None
            else min_related_precision_at_k
        ),
        min_related_recall_at_k=(
            defaults.min_related_recall_at_k
            if min_related_recall_at_k is None
            else min_related_recall_at_k
        ),
        min_related_mrr=(
            defaults.min_related_mrr
            if min_related_mrr is None
            else min_related_mrr
        ),
        min_suppression_alignment_rate=(
            defaults.min_suppression_alignment_rate
            if min_suppression_alignment_rate is None
            else min_suppression_alignment_rate
        ),
        min_refinement_alignment_rate=(
            defaults.min_refinement_alignment_rate
            if min_refinement_alignment_rate is None
            else min_refinement_alignment_rate
        ),
        min_easy_case_pass_rate=(
            defaults.min_easy_case_pass_rate
            if min_easy_case_pass_rate is None
            else min_easy_case_pass_rate
        ),
        max_fallback_rate=(
            defaults.max_fallback_rate
            if max_fallback_rate is None
            else max_fallback_rate
        ),
        max_p95_latency_ms=(
            defaults.max_p95_latency_ms
            if max_p95_latency_ms is None
            else max_p95_latency_ms
        ),
    )
    decision = evaluate_vector_release_thresholds(
        report,
        thresholds=thresholds,
    )
    return RegistryEvaluateVectorReleaseThresholdsResponse(
        report_path=str(resolved_report_path),
        thresholds=RegistryVectorReleaseThresholdsResponse(
            min_related_precision_at_k=thresholds.min_related_precision_at_k,
            min_related_recall_at_k=thresholds.min_related_recall_at_k,
            min_related_mrr=thresholds.min_related_mrr,
            min_suppression_alignment_rate=thresholds.min_suppression_alignment_rate,
            min_refinement_alignment_rate=thresholds.min_refinement_alignment_rate,
            min_easy_case_pass_rate=thresholds.min_easy_case_pass_rate,
            max_fallback_rate=thresholds.max_fallback_rate,
            max_p95_latency_ms=thresholds.max_p95_latency_ms,
        ),
        passed=decision.passed,
        reasons=[str(item) for item in decision.reasons],
    )


def npm_installer_info() -> NpmInstallerInfo:
    """Return the canonical npm-wrapper bootstrap metadata."""

    return NpmInstallerInfo(
        npm_package=NPM_PACKAGE_NAME,
        command=NPM_COMMAND_NAME,
        wrapper_version=settings_version(),
        python_package_spec=build_npm_python_package_spec(),
        runtime_dir=str(resolve_npm_runtime_dir().resolve()),
        python_bin_env=NPM_PYTHON_BIN_ENV,
        runtime_dir_env=NPM_RUNTIME_DIR_ENV,
        package_spec_env=NPM_PYTHON_PACKAGE_SPEC_ENV,
        pip_install_args_env=NPM_PIP_INSTALL_ARGS_ENV,
    )


def verify_release(
    *,
    output_dir: str | Path,
    clean: bool = True,
    smoke_install: bool = True,
) -> ReleaseVerificationResponse:
    """Build and verify the local Python and npm release artifacts."""

    return verify_release_artifacts(
        output_dir=output_dir,
        clean=clean,
        smoke_install=smoke_install,
    )


def release_versions() -> ReleaseVersionState:
    """Return the current coordinated release version state."""

    return read_release_versions()


def sync_release_version(version: str) -> ReleaseVersionSyncResponse:
    """Synchronize the coordinated release version files."""

    return run_sync_release_version(version)


def write_release_manifest(
    *,
    output_dir: str | Path,
    manifest_path: str | Path | None = None,
    version: str | None = None,
    clean: bool = True,
    smoke_install: bool = True,
) -> ReleaseManifestResponse:
    """Build and persist the coordinated release manifest."""

    return persist_release_manifest(
        output_dir=output_dir,
        manifest_path=manifest_path,
        version=version,
        clean=clean,
        smoke_install=smoke_install,
    )


def publish_release(
    *,
    manifest_path: str | Path,
    dry_run: bool = False,
) -> ReleasePublishResponse:
    """Publish one validated release manifest to Python and npm registries."""

    return run_publish_release_manifest(
        manifest_path=manifest_path,
        dry_run=dry_run,
    )


def validate_release(
    *,
    output_dir: str | Path,
    manifest_path: str | Path | None = None,
    version: str | None = None,
    clean: bool = True,
    smoke_install: bool = True,
    tests_command: Iterable[str] | None = None,
) -> ReleaseValidationResponse:
    """Run the local test suite and persist one coordinated release manifest."""

    return validate_release_workflow(
        output_dir=output_dir,
        manifest_path=manifest_path,
        version=version,
        clean=clean,
        smoke_install=smoke_install,
        tests_command=list(tests_command) if tests_command is not None else None,
    )


def tag(
    text: str,
    *,
    pack: str | None = None,
    content_type: str = "text/plain",
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    pretty_output: bool = True,
    storage_root: str | Path | None = None,
    debug: bool = False,
    hybrid: bool | None = None,
    include_related_entities: bool = False,
    include_graph_support: bool = False,
    refine_links: bool = False,
    refinement_depth: str = "light",
    include_impact_paths: bool = False,
    impact_max_depth: int | None = None,
    impact_seed_limit: int | None = None,
    impact_max_candidates: int | None = None,
    domain_hint: str | None = None,
    retrieval_profile: str | None = None,
    country_hint: str | None = None,
    registry: PackRegistry | None = None,
    runtime: PackRuntime | None = None,
) -> TagResponse:
    """Run in-process tagging through the installed local runtime."""

    settings = _resolve_settings(storage_root=storage_root)
    resolved_pack = pack or settings.default_pack
    registry = registry or PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )
    runtime = (
        runtime
        if runtime is not None and runtime.pack_id == resolved_pack
        else load_pack_runtime(settings.storage_root, resolved_pack, registry=registry)
    )
    response = tag_text(
        text=text,
        pack=resolved_pack,
        content_type=content_type,
        storage_root=settings.storage_root,
        debug=debug,
        hybrid=hybrid,
        registry=registry,
        runtime=runtime,
    )
    response = enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=include_related_entities,
        include_graph_support=include_graph_support,
        refine_links=refine_links,
        refinement_depth=refinement_depth,
        domain_hint=domain_hint,
        country_hint=country_hint,
    )
    response = enrich_tag_response_with_impact_paths(
        response,
        settings=settings,
        include_impact_paths=include_impact_paths,
        max_depth=impact_max_depth,
        impact_expansion_seed_limit=impact_seed_limit,
        max_candidates=impact_max_candidates,
    )
    if output_path is not None or output_dir is not None:
        response = persist_tag_response_json(
            response,
            pack_id=resolved_pack,
            output_path=output_path,
            output_dir=output_dir,
            pretty=pretty_output,
        )
    record_tag_observation(settings.storage_root, response)
    return response


def tag_file(
    path: str | Path,
    *,
    pack: str | None = None,
    content_type: str | None = None,
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    pretty_output: bool = True,
    storage_root: str | Path | None = None,
    debug: bool = False,
    hybrid: bool | None = None,
    include_related_entities: bool = False,
    include_graph_support: bool = False,
    refine_links: bool = False,
    refinement_depth: str = "light",
    include_impact_paths: bool = False,
    impact_max_depth: int | None = None,
    impact_seed_limit: int | None = None,
    impact_max_candidates: int | None = None,
    domain_hint: str | None = None,
    retrieval_profile: str | None = None,
    country_hint: str | None = None,
    registry: PackRegistry | None = None,
    runtime: PackRuntime | None = None,
) -> TagResponse:
    """Run in-process tagging for a local file path."""

    settings = _resolve_settings(storage_root=storage_root)
    resolved_pack = pack or settings.default_pack
    registry = registry or PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )
    runtime = (
        runtime
        if runtime is not None and runtime.pack_id == resolved_pack
        else load_pack_runtime(settings.storage_root, resolved_pack, registry=registry)
    )
    resolved_path, text, resolved_content_type, input_size_bytes, source_fingerprint = load_tag_file(
        path,
        content_type=content_type,
    )
    response = tag_text(
        text=text,
        pack=resolved_pack,
        content_type=resolved_content_type,
        storage_root=settings.storage_root,
        debug=debug,
        hybrid=hybrid,
        registry=registry,
        runtime=runtime,
    )
    response = response.model_copy(
        update={
            "content_type": resolved_content_type,
            "source_path": str(resolved_path),
            "input_size_bytes": input_size_bytes,
            "source_fingerprint": SourceFingerprint(**source_fingerprint.to_dict()),
        }
    )
    response = enrich_tag_response_with_related_entities(
        response,
        settings=settings,
        include_related_entities=include_related_entities,
        include_graph_support=include_graph_support,
        refine_links=refine_links,
        refinement_depth=refinement_depth,
        domain_hint=domain_hint,
        country_hint=country_hint,
    )
    response = enrich_tag_response_with_impact_paths(
        response,
        settings=settings,
        include_impact_paths=include_impact_paths,
        max_depth=impact_max_depth,
        impact_expansion_seed_limit=impact_seed_limit,
        max_candidates=impact_max_candidates,
    )
    if output_path is not None or output_dir is not None:
        response = persist_tag_response_json(
            response,
            pack_id=resolved_pack,
            output_path=output_path,
            output_dir=output_dir,
            pretty=pretty_output,
        )
    record_tag_observation(settings.storage_root, response)
    return response


def tag_files(
    paths: Iterable[str | Path] = (),
    *,
    pack: str | None = None,
    content_type: str | None = None,
    output_dir: str | Path | None = None,
    pretty_output: bool = True,
    storage_root: str | Path | None = None,
    directories: Iterable[str | Path] = (),
    glob_patterns: Iterable[str] = (),
    manifest_input_path: str | Path | None = None,
    manifest_replay_mode: str = "resume",
    skip_unchanged: bool = False,
    reuse_unchanged_outputs: bool = False,
    repair_missing_reused_outputs: bool = False,
    recursive: bool = True,
    include_patterns: Iterable[str] = (),
    exclude_patterns: Iterable[str] = (),
    max_files: int | None = None,
    max_input_bytes: int | None = None,
    write_manifest: bool = False,
    manifest_output_path: str | Path | None = None,
    debug: bool = False,
    hybrid: bool | None = None,
    include_related_entities: bool = False,
    include_graph_support: bool = False,
    refine_links: bool = False,
    refinement_depth: str = "light",
    include_impact_paths: bool = False,
    impact_max_depth: int | None = None,
    impact_seed_limit: int | None = None,
    impact_max_candidates: int | None = None,
    domain_hint: str | None = None,
    retrieval_profile: str | None = None,
    country_hint: str | None = None,
    registry: PackRegistry | None = None,
    runtime: PackRuntime | None = None,
) -> BatchTagResponse:
    """Run in-process tagging for multiple local file paths."""

    settings = _resolve_settings(storage_root=storage_root)
    registry = registry or PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )
    explicit_paths = [Path(path) for path in paths]
    resolved_directories = [Path(path) for path in directories]
    resolved_glob_patterns = list(glob_patterns)
    has_discovery_sources = bool(explicit_paths or resolved_directories or resolved_glob_patterns)
    replay_plan = (
        build_batch_manifest_replay_plan(
            manifest_input_path,
            mode=manifest_replay_mode,
        )
        if manifest_input_path is not None
        else None
    )
    if skip_unchanged and replay_plan is None:
        raise ValueError("skip_unchanged requires manifest_input_path.")
    if reuse_unchanged_outputs and replay_plan is None:
        raise ValueError("reuse_unchanged_outputs requires manifest_input_path.")
    if reuse_unchanged_outputs and not skip_unchanged:
        raise ValueError("reuse_unchanged_outputs requires skip_unchanged.")
    if repair_missing_reused_outputs and replay_plan is None:
        raise ValueError("repair_missing_reused_outputs requires manifest_input_path.")
    if repair_missing_reused_outputs and not reuse_unchanged_outputs:
        raise ValueError("repair_missing_reused_outputs requires reuse_unchanged_outputs.")
    resolved_pack = pack or (replay_plan.manifest.pack if replay_plan is not None else None) or settings.default_pack
    runtime = (
        runtime
        if runtime is not None and runtime.pack_id == resolved_pack
        else load_pack_runtime(settings.storage_root, resolved_pack, registry=registry)
    )
    if (write_manifest or manifest_output_path is not None) and output_dir is None:
        raise ValueError("Batch manifest export requires output_dir.")
    discovery = discover_tag_file_sources(
        paths=[
            *explicit_paths,
            *(replay_plan.paths if replay_plan is not None and not has_discovery_sources else []),
        ],
        directories=resolved_directories,
        glob_patterns=resolved_glob_patterns,
        recursive=recursive,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
        max_files=max_files,
        max_input_bytes=max_input_bytes,
    )
    items: list[TagResponse] = []
    reused_items: list[BatchManifestItem] = []
    unchanged_skipped_count = 0
    repaired_reused_output_count = 0
    rerun_changed: list[str] = []
    rerun_newly_processed: list[str] = []
    rerun_reused: list[str] = []
    rerun_repaired: list[str] = []
    for path in discovery.paths:
        manifest_content_type = (
            replay_plan.content_type_overrides.get(path)
            if replay_plan is not None
            else None
        )
        resolved_path, text, resolved_content_type, input_size_bytes, source_fingerprint = load_tag_file(
            path,
            content_type=content_type or manifest_content_type,
        )
        baseline_item = replay_plan.item_index.get(resolved_path) if replay_plan is not None else None
        if (
            skip_unchanged
            and baseline_item is not None
            and baseline_item.source_fingerprint is not None
            and baseline_item.source_fingerprint.model_dump(mode="python") == source_fingerprint.to_dict()
        ):
            unchanged_skipped_count += 1
            discovery.skipped.append(
                TagFileSkippedEntry(
                    reference=str(resolved_path),
                    reason="unchanged_since_manifest",
                    source_kind="fingerprint",
                    size_bytes=input_size_bytes,
                )
            )
            if reuse_unchanged_outputs:
                reused_item = baseline_item.model_copy(
                    update={
                        "source_path": str(resolved_path),
                        "input_size_bytes": input_size_bytes,
                        "source_fingerprint": SourceFingerprint(**source_fingerprint.to_dict()),
                    }
                )
                if repair_missing_reused_outputs:
                    has_saved_output_path = reused_item.saved_output_path is not None
                    saved_output_exists = (
                        Path(reused_item.saved_output_path).expanduser().resolve().exists()
                        if has_saved_output_path
                        else False
                    )
                    if (not has_saved_output_path and output_dir is not None) or (
                        has_saved_output_path and not saved_output_exists
                    ):
                        repaired_response = tag_text(
                            text=text,
                            pack=resolved_pack,
                            content_type=resolved_content_type,
                            storage_root=settings.storage_root,
                            debug=debug,
                            hybrid=hybrid,
                            registry=registry,
                            runtime=runtime,
                        ).model_copy(
                            update={
                                "content_type": resolved_content_type,
                                "source_path": str(resolved_path),
                                "input_size_bytes": input_size_bytes,
                                "source_fingerprint": SourceFingerprint(**source_fingerprint.to_dict()),
                            }
                        )
                        repaired_response = enrich_tag_response_with_related_entities(
                            repaired_response,
                            settings=settings,
                            include_related_entities=include_related_entities,
                            include_graph_support=include_graph_support,
                            refine_links=refine_links,
                            refinement_depth=refinement_depth,
                            domain_hint=domain_hint,
                            country_hint=country_hint,
                        )
                        repaired_response = enrich_tag_response_with_impact_paths(
                            repaired_response,
                            settings=settings,
                            include_impact_paths=include_impact_paths,
                            max_depth=impact_max_depth,
                            impact_expansion_seed_limit=impact_seed_limit,
                            max_candidates=impact_max_candidates,
                        )
                        repaired_response = persist_tag_response_json(
                            repaired_response,
                            pack_id=resolved_pack,
                            output_path=reused_item.saved_output_path if has_saved_output_path else None,
                            output_dir=output_dir if not has_saved_output_path else None,
                            pretty=pretty_output,
                        )
                        record_tag_observation(settings.storage_root, repaired_response)
                        repaired_reused_output_count += 1
                        rerun_repaired.append(str(resolved_path))
                        reused_item = build_batch_manifest_item_from_tag_response(
                            repaired_response,
                            extra_warnings=[
                                f"repaired_reused_output:{repaired_response.saved_output_path}"
                            ],
                        )
                rerun_reused.append(str(resolved_path))
                reused_items.append(reused_item)
            continue
        response = tag_text(
            text=text,
            pack=resolved_pack,
            content_type=resolved_content_type,
            storage_root=settings.storage_root,
            debug=debug,
            hybrid=hybrid,
            registry=registry,
            runtime=runtime,
        )
        response = response.model_copy(
            update={
                "content_type": resolved_content_type,
                "source_path": str(resolved_path),
                "input_size_bytes": input_size_bytes,
                "source_fingerprint": SourceFingerprint(**source_fingerprint.to_dict()),
            }
        )
        response = enrich_tag_response_with_related_entities(
            response,
            settings=settings,
            include_related_entities=include_related_entities,
            include_graph_support=include_graph_support,
            refine_links=refine_links,
            refinement_depth=refinement_depth,
            domain_hint=domain_hint,
            country_hint=country_hint,
        )
        response = enrich_tag_response_with_impact_paths(
            response,
            settings=settings,
            include_impact_paths=include_impact_paths,
            max_depth=impact_max_depth,
            impact_expansion_seed_limit=impact_seed_limit,
            max_candidates=impact_max_candidates,
        )
        record_tag_observation(settings.storage_root, response)
        items.append(response)
        if replay_plan is not None:
            if baseline_item is None:
                rerun_newly_processed.append(str(resolved_path))
            else:
                rerun_changed.append(str(resolved_path))
    if output_dir is not None:
        items = persist_tag_responses_json(
            items,
            pack_id=resolved_pack,
            output_dir=output_dir,
            pretty=pretty_output,
        )
    reused_items, reused_output_missing_count = verify_reused_output_items(reused_items)
    summary_payload = discovery.summary.to_dict()
    summary_payload["processed_count"] = len(items)
    summary_payload["processed_input_bytes"] = sum(item.input_size_bytes or 0 for item in items)
    summary_payload["skipped_count"] = len(discovery.skipped)
    summary_payload["unchanged_skipped_count"] = unchanged_skipped_count
    summary_payload["unchanged_reused_count"] = len(reused_items)
    summary_payload["reused_output_missing_count"] = reused_output_missing_count
    summary_payload["repaired_reused_output_count"] = repaired_reused_output_count
    summary_payload["manifest_input_path"] = (
        str(Path(manifest_input_path).expanduser().resolve())
        if manifest_input_path is not None
        else None
    )
    summary_payload["manifest_replay_mode"] = manifest_replay_mode if replay_plan is not None else None
    summary_payload["manifest_candidate_count"] = (
        replay_plan.candidate_count
        if replay_plan is not None and not has_discovery_sources
        else 0
    )
    summary_payload["manifest_selected_count"] = (
        replay_plan.selected_count
        if replay_plan is not None and not has_discovery_sources
        else 0
    )
    summary = BatchSourceSummary(**summary_payload)
    warnings = aggregate_batch_warnings(items, reused_items=reused_items)
    if replay_plan is not None and not has_discovery_sources and replay_plan.selected_count == 0:
        warnings = sorted({*warnings, "manifest_replay_empty"})
    if replay_plan is not None and pack is not None and pack != replay_plan.manifest.pack:
        warnings = sorted({*warnings, f"manifest_pack_override:{replay_plan.manifest.pack}->{pack}"})
    if not items and not reused_items:
        warnings = sorted({*warnings, "no_files_processed"})
    rerun_diff = None
    if replay_plan is not None:
        rerun_diff = BatchRerunDiff(
            manifest_input_path=str(Path(manifest_input_path).expanduser().resolve()),
            changed=rerun_changed,
            newly_processed=rerun_newly_processed,
            reused=rerun_reused,
            repaired=rerun_repaired,
            skipped=[
                entry.to_dict()
                for entry in discovery.skipped
                if entry.reference not in rerun_reused
            ],
        )
    lineage = build_batch_run_lineage(
        parent_manifest=replay_plan.manifest if replay_plan is not None else None,
        source_manifest_path=manifest_input_path,
    )
    response = BatchTagResponse(
        pack=resolved_pack,
        item_count=len(items),
        summary=summary,
        warnings=warnings,
        lineage=lineage,
        rerun_diff=rerun_diff,
        skipped=[entry.to_dict() for entry in discovery.skipped],
        rejected=[entry.to_dict() for entry in discovery.rejected],
        reused_items=reused_items,
        items=items,
    )
    if write_manifest or manifest_output_path is not None:
        return persist_batch_tag_manifest_json(
            response,
            pack_id=resolved_pack,
            output_dir=output_dir,
            output_path=manifest_output_path,
            pretty=pretty_output,
        )
    return response


def set_pack_active(
    pack_id: str,
    active: bool,
    *,
    storage_root: str | Path | None = None,
) -> PackSummary | None:
    """Set the active state for an installed pack and return its summary."""

    settings = _resolve_settings(storage_root=storage_root)
    registry = PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )
    manifest = registry.get_pack(pack_id, active_only=False)
    if manifest is None:
        return None
    if active:
        _activate_pack_dependencies(registry, pack_id, visiting=set())
    else:
        active_dependents = _active_dependent_pack_ids(registry, pack_id)
        if active_dependents:
            dependents = ", ".join(active_dependents)
            raise ValueError(
                f"Cannot deactivate pack {pack_id} while active dependent packs exist: "
                f"{dependents}"
            )
        registry.set_pack_active(pack_id, False)
    manifest = registry.get_pack(pack_id, active_only=False)
    if manifest is None:
        return None
    return PackSummary(**manifest.to_summary())


def activate_pack(pack_id: str, *, storage_root: str | Path | None = None) -> PackSummary | None:
    """Activate an installed pack."""

    return set_pack_active(pack_id, True, storage_root=storage_root)


def deactivate_pack(pack_id: str, *, storage_root: str | Path | None = None) -> PackSummary | None:
    """Deactivate an installed pack."""

    return set_pack_active(pack_id, False, storage_root=storage_root)


def remove_pack(pack_id: str, *, storage_root: str | Path | None = None) -> PackSummary | None:
    """Remove an installed pack and return the removed summary snapshot."""

    settings = _resolve_settings(storage_root=storage_root)
    registry = PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )
    manifest = registry.get_pack(pack_id, active_only=False)
    if manifest is None:
        return None
    installed_dependents = _installed_dependent_pack_ids(registry, pack_id)
    if installed_dependents:
        dependents = ", ".join(installed_dependents)
        raise ValueError(
            f"Cannot remove pack {pack_id} while installed dependent packs exist: "
            f"{dependents}"
        )
    removed = registry.remove_pack(pack_id)
    if not removed:
        return None
    return PackSummary(**manifest.to_summary())


def _activate_pack_dependencies(
    registry: PackRegistry,
    pack_id: str,
    *,
    visiting: set[str],
) -> None:
    if pack_id in visiting:
        raise ValueError(f"Cyclic installed-pack dependency detected at {pack_id}")

    manifest = registry.get_pack(pack_id, active_only=False)
    if manifest is None:
        raise ValueError(f"Pack not found: {pack_id}")

    visiting.add(pack_id)
    try:
        for dependency in manifest.dependencies:
            dependency_manifest = registry.get_pack(dependency, active_only=False)
            if dependency_manifest is None:
                raise ValueError(
                    f"Cannot activate pack {pack_id}: missing installed dependency {dependency}"
                )
            _activate_pack_dependencies(registry, dependency, visiting=visiting)
        registry.set_pack_active(pack_id, True)
    finally:
        visiting.remove(pack_id)


def _active_dependent_pack_ids(registry: PackRegistry, pack_id: str) -> list[str]:
    return _dependent_pack_ids(registry, pack_id, active_only=True)


def _installed_dependent_pack_ids(registry: PackRegistry, pack_id: str) -> list[str]:
    return _dependent_pack_ids(registry, pack_id, active_only=False)


def _dependent_pack_ids(
    registry: PackRegistry,
    pack_id: str,
    *,
    active_only: bool,
) -> list[str]:
    dependents = {
        manifest.pack_id
        for manifest in registry.list_installed_packs(active_only=active_only)
        if manifest.pack_id != pack_id and pack_id in manifest.dependencies
    }
    return sorted(dependents)


def lookup_candidates(
    query: str,
    *,
    storage_root: str | Path | None = None,
    pack_id: str | None = None,
    exact_alias: bool = False,
    fuzzy: bool = False,
    active_only: bool = True,
    limit: int = 20,
) -> LookupResponse:
    """Search deterministic metadata candidates from the configured local store."""

    if exact_alias and fuzzy:
        raise ValueError("Lookup cannot request both exact_alias and fuzzy mode.")

    settings = _resolve_settings(storage_root=storage_root)
    registry = PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )
    bounded_limit = max(1, min(limit, 100))
    candidates = [
        LookupCandidate(**item)
        for item in registry.lookup_candidates(
            query,
            pack_id=pack_id,
            exact_alias=exact_alias,
            fuzzy=fuzzy,
            active_only=active_only,
            limit=bounded_limit,
        )
    ]
    return LookupResponse(
        query=query,
        pack_id=pack_id,
        exact_alias=exact_alias,
        fuzzy=fuzzy,
        active_only=active_only,
        candidates=candidates,
    )


def build_qid_graph_index(
    bundle_dirs: Iterable[str | Path],
    *,
    truthy_path: str | Path,
    output_dir: str | Path,
    storage_root: str | Path | None = None,
    dimensions: int = DEFAULT_QID_GRAPH_DIMENSIONS,
    allowed_predicates: Iterable[str] | None = None,
    qdrant_url: str | None = None,
    qdrant_api_key: str | None = None,
    collection_name: str | None = None,
    publish_alias: str | None = None,
) -> VectorIndexBuildResponse:
    """Build one hosted QID graph artifact and optionally publish it to Qdrant."""

    settings = _resolve_settings(storage_root=storage_root)
    resolved_qdrant_url = qdrant_url if qdrant_url is not None else settings.vector_search_url
    resolved_qdrant_api_key = (
        qdrant_api_key if qdrant_api_key is not None else settings.vector_search_api_key
    )
    resolved_publish_alias = (
        publish_alias
        if publish_alias is not None
        else (
            settings.vector_search_collection_alias
            if resolved_qdrant_url and settings.vector_search_enabled
            else None
        )
    )
    response = run_build_qid_graph_index(
        bundle_dirs,
        truthy_path=truthy_path,
        output_dir=output_dir,
        dimensions=dimensions,
        allowed_predicates=(
            list(allowed_predicates)
            if allowed_predicates is not None
            else list(DEFAULT_QID_GRAPH_ALLOWED_PREDICATES)
        ),
        qdrant_url=resolved_qdrant_url,
        qdrant_api_key=resolved_qdrant_api_key,
        collection_name=collection_name,
        publish_alias=resolved_publish_alias,
    )
    _record_vector_build_state_if_supported(settings, response)
    return response


def build_qid_graph_index_from_store(
    bundle_dirs: Iterable[str | Path],
    *,
    graph_store_path: str | Path,
    output_dir: str | Path,
    storage_root: str | Path | None = None,
    dimensions: int = DEFAULT_QID_GRAPH_DIMENSIONS,
    allowed_predicates: Iterable[str] | None = None,
    neighbor_limit_per_qid: int = 128,
    qdrant_url: str | None = None,
    qdrant_api_key: str | None = None,
    collection_name: str | None = None,
    publish_alias: str | None = None,
) -> VectorIndexBuildResponse:
    """Build one hosted QID graph artifact from an existing explicit graph store."""

    settings = _resolve_settings(storage_root=storage_root)
    resolved_qdrant_url = qdrant_url if qdrant_url is not None else settings.vector_search_url
    resolved_qdrant_api_key = (
        qdrant_api_key if qdrant_api_key is not None else settings.vector_search_api_key
    )
    resolved_publish_alias = (
        publish_alias
        if publish_alias is not None
        else (
            settings.vector_search_collection_alias
            if resolved_qdrant_url and settings.vector_search_enabled
            else None
        )
    )
    response = run_build_qid_graph_index_from_store(
        bundle_dirs,
        graph_store_path=graph_store_path,
        output_dir=output_dir,
        dimensions=dimensions,
        allowed_predicates=(
            list(allowed_predicates)
            if allowed_predicates is not None
            else list(DEFAULT_QID_GRAPH_ALLOWED_PREDICATES)
        ),
        neighbor_limit_per_qid=neighbor_limit_per_qid,
        qdrant_url=resolved_qdrant_url,
        qdrant_api_key=resolved_qdrant_api_key,
        collection_name=collection_name,
        publish_alias=resolved_publish_alias,
    )
    _record_vector_build_state_if_supported(settings, response)
    return response


def build_qid_graph_store(
    bundle_dirs: Iterable[str | Path],
    *,
    truthy_path: str | Path,
    output_dir: str | Path,
    allowed_predicates: Iterable[str] | None = None,
) -> QidGraphStoreBuildResponse:
    """Build one explicit QID graph store artifact for exact graph queries."""

    return run_build_qid_graph_store(
        bundle_dirs,
        truthy_path=truthy_path,
        output_dir=output_dir,
        allowed_predicates=allowed_predicates,
    )


def build_market_graph_store(
    *,
    edge_tsv_paths: Iterable[str | Path],
    output_dir: str | Path,
    node_tsv_paths: Iterable[str | Path] = (),
    graph_version: str = "market-graph-v1",
    artifact_version: str | None = None,
) -> MarketGraphStoreBuildResponse:
    """Build one market impact graph store artifact from normalized TSV lanes."""

    return run_build_market_graph_store(
        edge_tsv_paths=edge_tsv_paths,
        output_dir=output_dir,
        node_tsv_paths=node_tsv_paths,
        graph_version=graph_version,
        artifact_version=artifact_version,
    )


def expand_impact_paths(
    entity_refs: Iterable[str],
    *,
    language: str = "en",
    enabled_packs: Iterable[str] | None = None,
    max_depth: int | None = None,
    impact_expansion_seed_limit: int | None = None,
    max_candidates: int | None = None,
    include_passive_paths: bool = True,
    vector_proposals_enabled: bool | None = None,
    storage_root: str | Path | None = None,
    artifact_path: str | Path | None = None,
) -> ImpactExpansionResult:
    """Expand ADES entity refs into deterministic market impact paths."""

    settings = _resolve_settings(storage_root=storage_root)
    return run_expand_impact_paths(
        entity_refs,
        language=language,
        enabled_packs=enabled_packs,
        max_depth=max_depth,
        impact_expansion_seed_limit=impact_expansion_seed_limit,
        max_candidates=max_candidates,
        include_passive_paths=include_passive_paths,
        vector_proposals_enabled=vector_proposals_enabled,
        settings=settings,
        artifact_path=artifact_path,
    )


def _record_vector_build_state_if_supported(
    settings: Settings,
    response: VectorIndexBuildResponse,
) -> None:
    if settings.runtime_target.value != "production_server":
        return
    if settings.metadata_backend.value != "postgresql":
        return
    if not settings.database_url:
        return
    registry = PackRegistry(
        settings.storage_root,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )
    recorder = getattr(registry.store, "record_vector_build", None)
    if callable(recorder):
        recorder(response)


def create_service_app(*, storage_root: str | Path | None = None):
    """Create the FastAPI service app for direct embedding."""

    settings = _resolve_settings(storage_root=storage_root)
    if settings.runtime_target.value == "production_server":
        from .service.production import create_production_app

        return create_production_app(
            storage_root=settings.storage_root,
            database_url=settings.database_url,
        )

    from .service.app import create_app

    return create_app(storage_root=settings.storage_root)


def settings_version() -> str:
    """Return the current package version without widening public imports."""

    from .version import __version__

    return __version__
