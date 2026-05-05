"""Local FastAPI application for ades."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from ..api import (
    _resolve_settings,
    activate_pack,
    benchmark_runtime,
    benchmark_matcher_backends,
    build_finance_source_bundle,
    build_finance_country_source_bundles,
    fetch_finance_source_snapshot,
    fetch_finance_country_source_snapshots,
    build_general_source_bundle,
    fetch_general_source_snapshot,
    build_medical_source_bundle,
    fetch_medical_source_snapshot,
    build_market_graph_store,
    build_qid_graph_store,
    build_registry,
    compare_extraction_quality_reports,
    deactivate_pack,
    diff_pack_versions,
    evaluate_extraction_quality,
    evaluate_extraction_release_thresholds,
    evaluate_vector_quality,
    evaluate_vector_release_thresholds_from_report,
    expand_impact_paths,
    generate_pack_source,
    get_pack,
    get_pack_health,
    list_available_packs,
    list_packs,
    lookup_candidates,
    npm_installer_info,
    prepare_registry_deploy_release,
    publish_generated_registry_release,
    publish_release,
    report_generated_pack,
    release_versions,
    refresh_generated_packs,
    remove_pack,
    smoke_test_published_generated_registry,
    status,
    sync_release_version,
    tag,
    tag_file,
    tag_files,
    validate_general_pack_quality,
    validate_finance_pack_quality,
    validate_medical_pack_quality,
    validate_release,
    verify_release,
    write_release_manifest,
)
from ..config import Settings
from ..packs.registry import PackRegistry
from ..packs.runtime import clear_pack_runtime_cache, load_pack_runtime
from ..runtime_matcher import clear_runtime_matcher_cache, load_runtime_matcher
from ..storage import UnsupportedRuntimeConfigurationError
from ..version import __version__
from .models import (
    AvailablePackSummary,
    BatchFileTagRequest,
    BatchTagResponse,
    FileTagRequest,
    ImpactExpansionRequest,
    ImpactExpansionResult,
    LookupResponse,
    MarketGraphStoreBuildRequest,
    MarketGraphStoreBuildResponse,
    NpmInstallerInfo,
    PackSummary,
    PackHealthResponse,
    QidGraphStoreBuildRequest,
    QidGraphStoreBuildResponse,
    ReleaseManifestRequest,
    ReleaseManifestResponse,
    ReleasePublishRequest,
    ReleasePublishResponse,
    ReleaseValidationRequest,
    ReleaseValidationResponse,
    ReleaseVersionState,
    ReleaseVersionSyncRequest,
    ReleaseVersionSyncResponse,
    ReleaseVerificationRequest,
    ReleaseVerificationResponse,
    RegistryBuildFinanceBundleRequest,
    RegistryBuildFinanceBundleResponse,
    RegistryBuildFinanceCountryBundlesRequest,
    RegistryBuildFinanceCountryBundlesResponse,
    RegistryFetchFinanceSourcesRequest,
    RegistryFetchFinanceSourcesResponse,
    RegistryFetchFinanceCountrySourcesRequest,
    RegistryFetchFinanceCountrySourcesResponse,
    RegistryFetchGeneralSourcesRequest,
    RegistryFetchGeneralSourcesResponse,
    RegistryFetchMedicalSourcesRequest,
    RegistryFetchMedicalSourcesResponse,
    RegistryBuildGeneralBundleRequest,
    RegistryBuildGeneralBundleResponse,
    RegistryBuildMedicalBundleRequest,
    RegistryBuildMedicalBundleResponse,
    RegistryBuildRequest,
    RegistryBuildResponse,
    RegistryBenchmarkRuntimeRequest,
    RegistryBenchmarkRuntimeResponse,
    RegistryBenchmarkMatcherBackendsRequest,
    RegistryBenchmarkMatcherBackendsResponse,
    RegistryCompareExtractionQualityRequest,
    RegistryCompareExtractionQualityResponse,
    RegistryDiffPackRequest,
    RegistryEvaluateExtractionQualityRequest,
    RegistryEvaluateExtractionQualityResponse,
    RegistryEvaluateReleaseThresholdsRequest,
    RegistryEvaluateReleaseThresholdsResponse,
    RegistryEvaluateVectorQualityRequest,
    RegistryEvaluateVectorQualityResponse,
    RegistryEvaluateVectorReleaseThresholdsRequest,
    RegistryEvaluateVectorReleaseThresholdsResponse,
    RegistryPackQualityResponse,
    RegistryPackDiffResponse,
    RegistryPublishGeneratedReleaseRequest,
    RegistryPublishGeneratedReleaseResponse,
    RegistryPrepareDeployReleaseRequest,
    RegistryPrepareDeployReleaseResponse,
    RegistrySmokePublishedReleaseRequest,
    RegistrySmokePublishedReleaseResponse,
    RegistryRefreshGeneratedPacksRequest,
    RegistryRefreshGeneratedPacksResponse,
    RegistryValidateFinanceQualityRequest,
    RegistryValidateFinanceQualityResponse,
    RegistryValidateGeneralQualityRequest,
    RegistryValidateMedicalQualityRequest,
    RegistryGeneratePackRequest,
    RegistryGeneratePackResponse,
    RegistryReportPackRequest,
    RegistryReportPackResponse,
    StatusResponse,
    TagRequest,
    TagResponse,
)


def _bool_option(options: dict[str, object] | None, key: str) -> bool | None:
    if not options or key not in options:
        return None
    value = options[key]
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    raise ValueError(f"Invalid boolean option for {key}.")


def _refinement_depth_option(options: dict[str, object] | None) -> str:
    if not options or "refinement_depth" not in options:
        return "light"
    value = options["refinement_depth"]
    normalized = str(value).strip().casefold()
    if normalized in {"light", "deep"}:
        return normalized
    raise ValueError("Invalid refinement_depth option.")


def _string_option(options: dict[str, object] | None, key: str) -> str | None:
    if not options or key not in options:
        return None
    value = options[key]
    if not isinstance(value, str):
        raise ValueError(f"Invalid string option for {key}.")
    normalized = value.strip()
    return normalized or None


def _int_option(options: dict[str, object] | None, key: str) -> int | None:
    if not options or key not in options:
        return None
    value = options[key]
    if isinstance(value, bool):
        raise ValueError(f"Invalid integer option for {key}.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer option for {key}.") from exc
    return parsed


def _list_string_option(options: dict[str, object] | None, key: str) -> list[str] | None:
    if not options or key not in options:
        return None
    value = options[key]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        result = [str(item).strip() for item in value if str(item).strip()]
        return result
    raise ValueError(f"Invalid list option for {key}.")


def _request_string_option(
    request_value: object | None,
    options: dict[str, object] | None,
    key: str,
) -> str | None:
    if request_value is not None:
        value = request_value
    elif options and key in options:
        value = options[key]
    else:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Invalid string option for {key}.")
    normalized = value.strip()
    return normalized or None


def _raise_configuration_http_exception(exc: Exception) -> None:
    """Map one configuration/runtime failure onto the public HTTP contract."""

    if isinstance(exc, FileNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    raise HTTPException(status_code=400, detail=str(exc)) from exc


@dataclass
class _ServiceRuntimeState:
    """Resident settings, registry, and warm-pack state for the local service."""

    storage_root_override: str | Path | None
    settings: Settings | None = None
    registry: PackRegistry | None = None
    prewarmed_packs: set[str] = field(default_factory=set)

    def ensure_registry(self) -> PackRegistry:
        """Return one resident registry bound to the current service settings."""

        if self.registry is not None:
            return self.registry
        settings = _resolve_settings(storage_root=self.storage_root_override)
        self.settings = settings
        self.registry = PackRegistry(
            settings.storage_root,
            runtime_target=settings.runtime_target,
            metadata_backend=settings.metadata_backend,
            database_url=settings.database_url,
        )
        return self.registry

    def current_settings(self) -> Settings:
        """Return the resolved service settings, loading them lazily."""

        if self.settings is None:
            self.ensure_registry()
        assert self.settings is not None
        return self.settings

    def prewarm_pack(self, pack_id: str) -> None:
        """Load one runtime and warm its matcher sidecars."""

        registry = self.ensure_registry()
        settings = self.current_settings()
        runtime = load_pack_runtime(settings.storage_root, pack_id, registry=registry)
        if runtime is None:
            self.prewarmed_packs.discard(pack_id)
            return
        for matcher in runtime.matchers:
            load_runtime_matcher(matcher.artifact_path, matcher.entries_path)
        self.prewarmed_packs.add(pack_id)

    def prewarm_active_packs(self) -> None:
        """Warm every active installed pack."""

        registry = self.ensure_registry()
        for pack in registry.list_installed_packs(active_only=True):
            self.prewarm_pack(pack.pack_id)

    def invalidate(self) -> None:
        """Drop warm caches after a pack mutation."""

        clear_pack_runtime_cache()
        clear_runtime_matcher_cache()
        self.prewarmed_packs.clear()


def create_app(*, storage_root: str | Path | None = None) -> FastAPI:
    """Create a FastAPI application bound to a storage root."""

    runtime_state = _ServiceRuntimeState(storage_root)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        settings = runtime_state.current_settings()
        if settings.service_prewarm_enabled:
            try:
                runtime_state.prewarm_active_packs()
            except (FileNotFoundError, UnsupportedRuntimeConfigurationError, ValueError):
                # Preserve request-time config errors on the public endpoints.
                pass
        yield

    app = FastAPI(title="ades", version=__version__, lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        """Simple health endpoint."""

        return {"status": "ok", "version": __version__}

    @app.get("/v0/status", response_model=StatusResponse)
    def runtime_status() -> StatusResponse:
        """Report local runtime status."""

        try:
            response = status(storage_root=storage_root)
            return response.model_copy(
                update={"prewarmed_packs": sorted(runtime_state.prewarmed_packs)}
            )
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)

    @app.get("/v0/installers/npm", response_model=NpmInstallerInfo)
    def runtime_npm_installer_info() -> NpmInstallerInfo:
        """Return canonical npm-wrapper bootstrap metadata."""

        return npm_installer_info()

    @app.get("/v0/release/versions", response_model=ReleaseVersionState)
    def runtime_release_versions() -> ReleaseVersionState:
        """Return the current coordinated release version state."""

        try:
            return release_versions()
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/release/sync-version", response_model=ReleaseVersionSyncResponse)
    def runtime_sync_release_version(
        request: ReleaseVersionSyncRequest,
    ) -> ReleaseVersionSyncResponse:
        """Synchronize release versions across Python and npm metadata."""

        try:
            return sync_release_version(request.version)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/release/verify", response_model=ReleaseVerificationResponse)
    def runtime_verify_release(request: ReleaseVerificationRequest) -> ReleaseVerificationResponse:
        """Build and verify the local Python and npm release artifacts."""

        try:
            return verify_release(
                output_dir=request.output_dir,
                clean=request.clean,
                smoke_install=request.smoke_install,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/release/manifest", response_model=ReleaseManifestResponse)
    def runtime_write_release_manifest(
        request: ReleaseManifestRequest,
    ) -> ReleaseManifestResponse:
        """Build and persist the coordinated release manifest."""

        try:
            return write_release_manifest(
                output_dir=request.output_dir,
                manifest_path=request.manifest_path,
                version=request.version,
                clean=request.clean,
                smoke_install=request.smoke_install,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/release/validate", response_model=ReleaseValidationResponse)
    def runtime_validate_release(
        request: ReleaseValidationRequest,
    ) -> ReleaseValidationResponse:
        """Run tests, then build and persist one coordinated release manifest."""

        try:
            return validate_release(
                output_dir=request.output_dir,
                manifest_path=request.manifest_path,
                version=request.version,
                clean=request.clean,
                smoke_install=request.smoke_install,
                tests_command=request.tests_command or None,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/release/publish", response_model=ReleasePublishResponse)
    def runtime_publish_release(
        request: ReleasePublishRequest,
    ) -> ReleasePublishResponse:
        """Publish one validated release manifest to Python and npm registries."""

        try:
            return publish_release(
                manifest_path=request.manifest_path,
                dry_run=request.dry_run,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v0/packs", response_model=list[PackSummary])
    def runtime_list_packs(active_only: bool = Query(False)) -> list[PackSummary]:
        """List locally installed packs."""

        try:
            return list_packs(storage_root=storage_root, active_only=active_only)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)

    @app.get("/v0/packs/available", response_model=list[AvailablePackSummary])
    def runtime_list_available_packs(
        registry_url: str | None = Query(None),
    ) -> list[AvailablePackSummary]:
        """List installable packs from the configured registry."""

        try:
            return list_available_packs(registry_url=registry_url)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)

    @app.get("/v0/packs/{pack_id}", response_model=PackSummary)
    def runtime_get_pack(pack_id: str) -> PackSummary:
        """Get a single installed pack."""

        try:
            pack = get_pack(pack_id, storage_root=storage_root)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)
        if pack is None:
            raise HTTPException(status_code=404, detail=f"Pack not found: {pack_id}")
        return pack

    @app.get("/v0/packs/{pack_id}/health", response_model=PackHealthResponse)
    def runtime_get_pack_health(
        pack_id: str,
        limit: int = Query(100, ge=1, le=1000),
    ) -> PackHealthResponse:
        """Return aggregated recent extraction-health telemetry for one pack."""

        try:
            return get_pack_health(pack_id, storage_root=storage_root, limit=limit)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)

    @app.post("/v0/packs/{pack_id}/activate", response_model=PackSummary)
    def runtime_activate_pack(pack_id: str) -> PackSummary:
        """Activate a single installed pack."""

        try:
            pack = activate_pack(pack_id, storage_root=storage_root)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)
        if pack is None:
            raise HTTPException(status_code=404, detail=f"Pack not found: {pack_id}")
        runtime_state.invalidate()
        try:
            runtime_state.prewarm_active_packs()
        except (FileNotFoundError, UnsupportedRuntimeConfigurationError, ValueError):
            pass
        return pack

    @app.post("/v0/packs/{pack_id}/deactivate", response_model=PackSummary)
    def runtime_deactivate_pack(pack_id: str) -> PackSummary:
        """Deactivate a single installed pack."""

        try:
            pack = deactivate_pack(pack_id, storage_root=storage_root)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)
        if pack is None:
            raise HTTPException(status_code=404, detail=f"Pack not found: {pack_id}")
        runtime_state.invalidate()
        try:
            runtime_state.prewarm_active_packs()
        except (FileNotFoundError, UnsupportedRuntimeConfigurationError, ValueError):
            pass
        return pack

    @app.delete("/v0/packs/{pack_id}", response_model=PackSummary)
    def runtime_remove_pack(pack_id: str) -> PackSummary:
        """Remove a single installed pack."""

        try:
            pack = remove_pack(pack_id, storage_root=storage_root)
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)
        if pack is None:
            raise HTTPException(status_code=404, detail=f"Pack not found: {pack_id}")
        runtime_state.invalidate()
        try:
            runtime_state.prewarm_active_packs()
        except (FileNotFoundError, UnsupportedRuntimeConfigurationError, ValueError):
            pass
        return pack

    @app.post("/v0/registry/build", response_model=RegistryBuildResponse)
    def runtime_build_registry(request: RegistryBuildRequest) -> RegistryBuildResponse:
        """Build a static file-based registry from local pack directories."""

        try:
            return build_registry(
                request.pack_dirs,
                output_dir=request.output_dir,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-qid-graph-store",
        response_model=QidGraphStoreBuildResponse,
    )
    def runtime_build_qid_graph_store(
        request: QidGraphStoreBuildRequest,
    ) -> QidGraphStoreBuildResponse:
        """Build one explicit QID graph store from local bundle directories."""

        try:
            return build_qid_graph_store(
                request.bundle_dirs,
                truthy_path=request.truthy_path,
                output_dir=request.output_dir,
                allowed_predicates=request.predicate or None,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-market-graph-store",
        response_model=MarketGraphStoreBuildResponse,
    )
    def runtime_build_market_graph_store(
        request: MarketGraphStoreBuildRequest,
    ) -> MarketGraphStoreBuildResponse:
        """Build one market impact graph store from normalized TSV source lanes."""

        try:
            return build_market_graph_store(
                edge_tsv_paths=request.edge_tsv_paths,
                output_dir=request.output_dir,
                node_tsv_paths=request.node_tsv_paths,
                graph_version=request.graph_version,
                artifact_version=request.artifact_version,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/fetch-finance-sources",
        response_model=RegistryFetchFinanceSourcesResponse,
    )
    def runtime_fetch_finance_sources(
        request: RegistryFetchFinanceSourcesRequest,
    ) -> RegistryFetchFinanceSourcesResponse:
        """Download one real `finance-en` source snapshot set."""

        try:
            return fetch_finance_source_snapshot(
                output_dir=request.output_dir,
                snapshot=request.snapshot,
                sec_companies_url=request.sec_companies_url,
                sec_submissions_url=request.sec_submissions_url,
                sec_companyfacts_url=request.sec_companyfacts_url,
                symbol_directory_url=request.symbol_directory_url,
                other_listed_url=request.other_listed_url,
                finance_people_url=request.finance_people_url,
                derive_finance_people_from_sec=request.derive_finance_people_from_sec,
                finance_people_archive_base_url=request.finance_people_archive_base_url,
                finance_people_max_companies=request.finance_people_max_companies,
                user_agent=request.user_agent,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-finance-bundle",
        response_model=RegistryBuildFinanceBundleResponse,
    )
    def runtime_build_finance_bundle(
        request: RegistryBuildFinanceBundleRequest,
    ) -> RegistryBuildFinanceBundleResponse:
        """Build one normalized `finance-en` source bundle from raw snapshot files."""

        try:
            return build_finance_source_bundle(
                sec_companies_path=request.sec_companies_path,
                sec_submissions_path=request.sec_submissions_path,
                sec_companyfacts_path=request.sec_companyfacts_path,
                symbol_directory_path=request.symbol_directory_path,
                other_listed_path=request.other_listed_path,
                finance_people_path=request.finance_people_path,
                curated_entities_path=request.curated_entities_path,
                output_dir=request.output_dir,
                version=request.version,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/fetch-finance-country-sources",
        response_model=RegistryFetchFinanceCountrySourcesResponse,
    )
    def runtime_fetch_finance_country_sources(
        request: RegistryFetchFinanceCountrySourcesRequest,
    ) -> RegistryFetchFinanceCountrySourcesResponse:
        """Download official source landing pages for country-scoped finance packs."""

        try:
            return fetch_finance_country_source_snapshots(
                output_dir=request.output_dir,
                snapshot=request.snapshot,
                country_codes=request.country_codes,
                user_agent=request.user_agent,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-finance-country-bundles",
        response_model=RegistryBuildFinanceCountryBundlesResponse,
    )
    def runtime_build_finance_country_bundles(
        request: RegistryBuildFinanceCountryBundlesRequest,
    ) -> RegistryBuildFinanceCountryBundlesResponse:
        """Build country-scoped finance bundles from downloaded source snapshots."""

        try:
            return build_finance_country_source_bundles(
                snapshot_dir=request.snapshot_dir,
                output_dir=request.output_dir,
                country_codes=request.country_codes,
                version=request.version,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-general-bundle",
        response_model=RegistryBuildGeneralBundleResponse,
    )
    def runtime_build_general_bundle(
        request: RegistryBuildGeneralBundleRequest,
    ) -> RegistryBuildGeneralBundleResponse:
        """Build one normalized `general-en` source bundle from raw snapshot files."""

        try:
            return build_general_source_bundle(
                wikidata_entities_path=request.wikidata_entities_path,
                geonames_places_path=request.geonames_places_path,
                curated_entities_path=request.curated_entities_path,
                output_dir=request.output_dir,
                version=request.version,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/fetch-general-sources",
        response_model=RegistryFetchGeneralSourcesResponse,
    )
    def runtime_fetch_general_sources(
        request: RegistryFetchGeneralSourcesRequest,
    ) -> RegistryFetchGeneralSourcesResponse:
        """Download one real `general-en` source snapshot set."""

        try:
            return fetch_general_source_snapshot(
                output_dir=request.output_dir,
                snapshot=request.snapshot,
                wikidata_url=request.wikidata_url,
                wikidata_truthy_url=request.wikidata_truthy_url,
                wikidata_entities_url=request.wikidata_entities_url,
                geonames_places_url=request.geonames_places_url,
                geonames_alternate_names_url=request.geonames_alternate_names_url,
                geonames_modifications_url=request.geonames_modifications_url,
                geonames_deletes_url=request.geonames_deletes_url,
                geonames_alternate_modifications_url=request.geonames_alternate_modifications_url,
                geonames_alternate_deletes_url=request.geonames_alternate_deletes_url,
                user_agent=request.user_agent,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/fetch-medical-sources",
        response_model=RegistryFetchMedicalSourcesResponse,
    )
    def runtime_fetch_medical_sources(
        request: RegistryFetchMedicalSourcesRequest,
    ) -> RegistryFetchMedicalSourcesResponse:
        """Download one real `medical-en` source snapshot set."""

        try:
            return fetch_medical_source_snapshot(
                output_dir=request.output_dir,
                snapshot=request.snapshot,
                disease_ontology_url=request.disease_ontology_url,
                hgnc_genes_url=request.hgnc_genes_url,
                uniprot_proteins_url=request.uniprot_proteins_url,
                clinical_trials_url=request.clinical_trials_url,
                orange_book_url=request.orange_book_url,
                user_agent=request.user_agent,
                uniprot_max_records=request.uniprot_max_records,
                clinical_trials_max_records=request.clinical_trials_max_records,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/build-medical-bundle",
        response_model=RegistryBuildMedicalBundleResponse,
    )
    def runtime_build_medical_bundle(
        request: RegistryBuildMedicalBundleRequest,
    ) -> RegistryBuildMedicalBundleResponse:
        """Build one normalized `medical-en` source bundle from raw snapshot files."""

        try:
            return build_medical_source_bundle(
                disease_ontology_path=request.disease_ontology_path,
                hgnc_genes_path=request.hgnc_genes_path,
                uniprot_proteins_path=request.uniprot_proteins_path,
                clinical_trials_path=request.clinical_trials_path,
                orange_book_products_path=request.orange_book_products_path,
                curated_entities_path=request.curated_entities_path,
                output_dir=request.output_dir,
                version=request.version,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/validate-finance-quality",
        response_model=RegistryValidateFinanceQualityResponse,
    )
    def runtime_validate_finance_quality(
        request: RegistryValidateFinanceQualityRequest,
    ) -> RegistryValidateFinanceQualityResponse:
        """Build, install, and evaluate one generated `finance-en` pack bundle."""

        try:
            return validate_finance_pack_quality(
                request.bundle_dir,
                output_dir=request.output_dir,
                fixture_profile=request.fixture_profile,
                version=request.version,
                min_expected_recall=request.min_expected_recall,
                max_unexpected_hits=request.max_unexpected_hits,
                max_ambiguous_aliases=request.max_ambiguous_aliases,
                max_dropped_alias_ratio=request.max_dropped_alias_ratio,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/validate-general-quality",
        response_model=RegistryPackQualityResponse,
    )
    def runtime_validate_general_quality(
        request: RegistryValidateGeneralQualityRequest,
    ) -> RegistryPackQualityResponse:
        """Build, install, and evaluate one generated `general-en` pack bundle."""

        try:
            return validate_general_pack_quality(
                request.bundle_dir,
                output_dir=request.output_dir,
                fixture_profile=request.fixture_profile,
                version=request.version,
                min_expected_recall=request.min_expected_recall,
                max_unexpected_hits=request.max_unexpected_hits,
                max_ambiguous_aliases=request.max_ambiguous_aliases,
                max_dropped_alias_ratio=request.max_dropped_alias_ratio,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/validate-medical-quality",
        response_model=RegistryPackQualityResponse,
    )
    def runtime_validate_medical_quality(
        request: RegistryValidateMedicalQualityRequest,
    ) -> RegistryPackQualityResponse:
        """Build, install, and evaluate one generated `medical-en` pack bundle."""

        try:
            return validate_medical_pack_quality(
                request.bundle_dir,
                general_bundle_dir=request.general_bundle_dir,
                output_dir=request.output_dir,
                fixture_profile=request.fixture_profile,
                version=request.version,
                min_expected_recall=request.min_expected_recall,
                max_unexpected_hits=request.max_unexpected_hits,
                max_ambiguous_aliases=request.max_ambiguous_aliases,
                max_dropped_alias_ratio=request.max_dropped_alias_ratio,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/evaluate-extraction-quality",
        response_model=RegistryEvaluateExtractionQualityResponse,
    )
    def runtime_evaluate_extraction_quality(
        request: RegistryEvaluateExtractionQualityRequest,
    ) -> RegistryEvaluateExtractionQualityResponse:
        """Evaluate one installed pack against one golden set."""

        try:
            return evaluate_extraction_quality(
                request.pack_id,
                storage_root=storage_root,
                golden_set_path=request.golden_set_path,
                profile=request.profile,
                hybrid=request.hybrid,
                write_report=request.write_report,
                report_path=request.report_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/evaluate-vector-quality",
        response_model=RegistryEvaluateVectorQualityResponse,
    )
    def runtime_evaluate_vector_quality(
        request: RegistryEvaluateVectorQualityRequest,
    ) -> RegistryEvaluateVectorQualityResponse:
        """Evaluate the hosted vector-quality lane against one golden set."""

        try:
            return evaluate_vector_quality(
                request.pack_id,
                storage_root=storage_root,
                golden_set_path=request.golden_set_path,
                profile=request.profile,
                refinement_depth=request.refinement_depth,
                top_k=request.top_k,
                write_report=request.write_report,
                report_path=request.report_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/benchmark-runtime",
        response_model=RegistryBenchmarkRuntimeResponse,
    )
    def runtime_benchmark_runtime(
        request: RegistryBenchmarkRuntimeRequest,
    ) -> RegistryBenchmarkRuntimeResponse:
        """Benchmark matcher load, warm tagging, operator lookup, and disk usage."""

        try:
            return benchmark_runtime(
                request.pack_id,
                storage_root=storage_root,
                golden_set_path=request.golden_set_path,
                profile=request.profile,
                hybrid=request.hybrid,
                warm_runs=request.warm_runs,
                lookup_limit=request.lookup_limit,
                write_report=request.write_report,
                report_path=request.report_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/benchmark-matcher-backends",
        response_model=RegistryBenchmarkMatcherBackendsResponse,
    )
    def runtime_benchmark_matcher_backends(
        request: RegistryBenchmarkMatcherBackendsRequest,
    ) -> RegistryBenchmarkMatcherBackendsResponse:
        """Benchmark candidate matcher backends on a bounded installed-pack alias slice."""

        try:
            return benchmark_matcher_backends(
                request.pack_id,
                storage_root=storage_root,
                golden_set_path=request.golden_set_path,
                profile=request.profile,
                alias_limit=request.alias_limit,
                scan_limit=request.scan_limit,
                min_alias_score=request.min_alias_score,
                exact_tier_min_token_count=request.exact_tier_min_token_count,
                query_runs=request.query_runs,
                write_report=request.write_report,
                report_path=request.report_path,
                output_root=request.output_root,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/compare-extraction-quality",
        response_model=RegistryCompareExtractionQualityResponse,
    )
    def runtime_compare_extraction_quality(
        request: RegistryCompareExtractionQualityRequest,
    ) -> RegistryCompareExtractionQualityResponse:
        """Compare two stored extraction-quality reports."""

        try:
            return compare_extraction_quality_reports(
                baseline_report_path=request.baseline_report_path,
                candidate_report_path=request.candidate_report_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/diff-pack",
        response_model=RegistryPackDiffResponse,
    )
    def runtime_diff_pack(
        request: RegistryDiffPackRequest,
    ) -> RegistryPackDiffResponse:
        """Diff two runtime pack directories."""

        try:
            return diff_pack_versions(
                request.old_pack_dir,
                request.new_pack_dir,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/evaluate-release-thresholds",
        response_model=RegistryEvaluateReleaseThresholdsResponse,
    )
    def runtime_evaluate_release_thresholds(
        request: RegistryEvaluateReleaseThresholdsRequest,
    ) -> RegistryEvaluateReleaseThresholdsResponse:
        """Evaluate one stored extraction-quality report against release thresholds."""

        try:
            return evaluate_extraction_release_thresholds(
                report_path=request.report_path,
                mode=request.mode,
                baseline_report_path=request.baseline_report_path,
                min_recall=request.min_recall,
                min_precision=request.min_precision,
                max_label_recall_drop=request.max_label_recall_drop,
                min_recall_lift=request.min_recall_lift,
                max_precision_drop=request.max_precision_drop,
                max_p95_latency_ms=request.max_p95_latency_ms,
                max_model_artifact_bytes=request.max_model_artifact_bytes,
                max_peak_memory_mb=request.max_peak_memory_mb,
                model_artifact_path=request.model_artifact_path,
                peak_memory_mb=request.peak_memory_mb,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/evaluate-vector-release-thresholds",
        response_model=RegistryEvaluateVectorReleaseThresholdsResponse,
    )
    def runtime_evaluate_vector_release_thresholds(
        request: RegistryEvaluateVectorReleaseThresholdsRequest,
    ) -> RegistryEvaluateVectorReleaseThresholdsResponse:
        """Evaluate one stored vector-quality report against release thresholds."""

        try:
            return evaluate_vector_release_thresholds_from_report(
                report_path=request.report_path,
                min_related_precision_at_k=request.min_related_precision_at_k,
                min_related_recall_at_k=request.min_related_recall_at_k,
                min_related_mrr=request.min_related_mrr,
                min_suppression_alignment_rate=request.min_suppression_alignment_rate,
                min_refinement_alignment_rate=request.min_refinement_alignment_rate,
                min_easy_case_pass_rate=request.min_easy_case_pass_rate,
                max_fallback_rate=request.max_fallback_rate,
                max_p95_latency_ms=request.max_p95_latency_ms,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/registry/generate-pack", response_model=RegistryGeneratePackResponse)
    def runtime_generate_pack(
        request: RegistryGeneratePackRequest,
    ) -> RegistryGeneratePackResponse:
        """Generate one runtime-compatible pack directory from a normalized bundle."""

        try:
            return generate_pack_source(
                request.bundle_dir,
                output_dir=request.output_dir,
                version=request.version,
                include_build_metadata=request.include_build_metadata,
                include_build_only=request.include_build_only,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/registry/report-pack", response_model=RegistryReportPackResponse)
    def runtime_report_pack(
        request: RegistryReportPackRequest,
    ) -> RegistryReportPackResponse:
        """Generate one runtime-compatible pack directory and report stable statistics."""

        try:
            return report_generated_pack(
                request.bundle_dir,
                output_dir=request.output_dir,
                version=request.version,
                include_build_metadata=request.include_build_metadata,
                include_build_only=request.include_build_only,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/refresh-generated-packs",
        response_model=RegistryRefreshGeneratedPacksResponse,
    )
    def runtime_refresh_generated_packs(
        request: RegistryRefreshGeneratedPacksRequest,
    ) -> RegistryRefreshGeneratedPacksResponse:
        """Refresh one or more generated bundles into a quality-gated registry release."""

        try:
            return refresh_generated_packs(
                request.bundle_dirs,
                output_dir=request.output_dir,
                general_bundle_dir=request.general_bundle_dir,
                materialize_registry=request.materialize_registry,
                min_expected_recall=request.min_expected_recall,
                max_unexpected_hits=request.max_unexpected_hits,
                max_ambiguous_aliases=request.max_ambiguous_aliases,
                max_dropped_alias_ratio=request.max_dropped_alias_ratio,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/publish-generated-release",
        response_model=RegistryPublishGeneratedReleaseResponse,
    )
    def runtime_publish_generated_release(
        request: RegistryPublishGeneratedReleaseRequest,
    ) -> RegistryPublishGeneratedReleaseResponse:
        """Publish one reviewed generated registry directory to object storage."""

        try:
            return publish_generated_registry_release(
                request.registry_dir,
                prefix=request.prefix,
                bucket=request.bucket,
                endpoint=request.endpoint,
                region=request.region,
                delete=request.delete,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/prepare-deploy-release",
        response_model=RegistryPrepareDeployReleaseResponse,
    )
    def runtime_prepare_deploy_release(
        request: RegistryPrepareDeployReleaseRequest,
    ) -> RegistryPrepareDeployReleaseResponse:
        """Prepare the deploy-owned registry payload from bundled packs or a promoted release."""

        try:
            return prepare_registry_deploy_release(
                output_dir=request.output_dir,
                pack_dirs=request.pack_dirs,
                promotion_spec_path=request.promotion_spec_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v0/registry/smoke-published-release",
        response_model=RegistrySmokePublishedReleaseResponse,
    )
    def runtime_smoke_published_release(
        request: RegistrySmokePublishedReleaseRequest,
    ) -> RegistrySmokePublishedReleaseResponse:
        """Run clean pull/install/tag smoke against one published registry URL."""

        try:
            return smoke_test_published_generated_registry(
                request.registry_url,
                pack_ids=request.pack_ids,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/impact/expand", response_model=ImpactExpansionResult)
    def runtime_expand_impact_paths(
        request: ImpactExpansionRequest,
    ) -> ImpactExpansionResult:
        """Expand ADES entity refs into deterministic market impact paths."""

        try:
            return expand_impact_paths(
                request.entity_refs,
                language=request.language,
                enabled_packs=request.enabled_packs,
                max_depth=request.max_depth,
                impact_expansion_seed_limit=request.impact_expansion_seed_limit,
                max_candidates=request.max_candidates,
                include_passive_paths=request.include_passive_paths,
                vector_proposals_enabled=request.vector_proposals_enabled,
                storage_root=storage_root,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/tag", response_model=TagResponse)
    def runtime_tag(request: TagRequest) -> TagResponse:
        """Tag text through the local in-process pipeline."""

        try:
            resolved_pack = request.pack or runtime_state.current_settings().default_pack
            runtime_state.prewarm_pack(resolved_pack)
            return tag(
                request.text,
                pack=resolved_pack,
                content_type=request.content_type,
                output_path=request.output.path if request.output else None,
                output_dir=request.output.directory if request.output else None,
                pretty_output=request.output.pretty if request.output else True,
                storage_root=storage_root,
                debug=bool(_bool_option(request.options, "debug")),
                hybrid=_bool_option(request.options, "hybrid"),
                include_related_entities=bool(
                    _bool_option(request.options, "include_related_entities")
                ),
                include_graph_support=bool(
                    _bool_option(request.options, "include_graph_support")
                ),
                refine_links=bool(_bool_option(request.options, "refine_links")),
                refinement_depth=_refinement_depth_option(request.options),
                include_impact_paths=bool(
                    _bool_option(request.options, "include_impact_paths")
                ),
                impact_max_depth=_int_option(request.options, "impact_max_depth"),
                impact_seed_limit=_int_option(request.options, "impact_seed_limit"),
                impact_max_candidates=_int_option(
                    request.options,
                    "impact_max_candidates",
                ),
                domain_hint=_request_string_option(
                    request.domain_hint,
                    request.options,
                    "domain_hint",
                ),
                retrieval_profile=_request_string_option(
                    request.retrieval_profile,
                    request.options,
                    "retrieval_profile",
                ),
                country_hint=_string_option(request.options, "country_hint"),
                registry=runtime_state.ensure_registry(),
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/tag/file", response_model=TagResponse)
    def runtime_tag_file(request: FileTagRequest) -> TagResponse:
        """Tag a local file through the in-process pipeline."""

        try:
            resolved_pack = request.pack or runtime_state.current_settings().default_pack
            runtime_state.prewarm_pack(resolved_pack)
            return tag_file(
                request.path,
                pack=resolved_pack,
                content_type=request.content_type,
                output_path=request.output.path if request.output else None,
                output_dir=request.output.directory if request.output else None,
                pretty_output=request.output.pretty if request.output else True,
                storage_root=storage_root,
                debug=bool(_bool_option(request.options, "debug")),
                hybrid=_bool_option(request.options, "hybrid"),
                include_related_entities=bool(
                    _bool_option(request.options, "include_related_entities")
                ),
                include_graph_support=bool(
                    _bool_option(request.options, "include_graph_support")
                ),
                refine_links=bool(_bool_option(request.options, "refine_links")),
                refinement_depth=_refinement_depth_option(request.options),
                include_impact_paths=bool(
                    _bool_option(request.options, "include_impact_paths")
                ),
                impact_max_depth=_int_option(request.options, "impact_max_depth"),
                impact_seed_limit=_int_option(request.options, "impact_seed_limit"),
                impact_max_candidates=_int_option(
                    request.options,
                    "impact_max_candidates",
                ),
                domain_hint=_request_string_option(
                    request.domain_hint,
                    request.options,
                    "domain_hint",
                ),
                retrieval_profile=_request_string_option(
                    request.retrieval_profile,
                    request.options,
                    "retrieval_profile",
                ),
                country_hint=_string_option(request.options, "country_hint"),
                registry=runtime_state.ensure_registry(),
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/tag/files", response_model=BatchTagResponse)
    def runtime_tag_files(request: BatchFileTagRequest) -> BatchTagResponse:
        """Tag multiple local files through the in-process pipeline."""

        if request.output and request.output.path is not None:
            raise HTTPException(
                status_code=400,
                detail="Batch file tagging supports output.directory only.",
            )
        if (
            request.output
            and (request.output.write_manifest or request.output.manifest_path is not None)
            and request.output.directory is None
        ):
            raise HTTPException(
                status_code=400,
                detail="Batch manifest export requires output.directory.",
            )
        try:
            if request.pack is not None:
                runtime_state.prewarm_pack(request.pack)
            return tag_files(
                request.paths,
                pack=request.pack,
                content_type=request.content_type,
                output_dir=request.output.directory if request.output else None,
                pretty_output=request.output.pretty if request.output else True,
                storage_root=storage_root,
                directories=request.directories,
                glob_patterns=request.glob_patterns,
                manifest_input_path=request.manifest_input_path,
                manifest_replay_mode=request.manifest_replay_mode,
                skip_unchanged=request.skip_unchanged,
                reuse_unchanged_outputs=request.reuse_unchanged_outputs,
                repair_missing_reused_outputs=request.repair_missing_reused_outputs,
                recursive=request.recursive,
                include_patterns=request.include_patterns,
                exclude_patterns=request.exclude_patterns,
                max_files=request.max_files,
                max_input_bytes=request.max_input_bytes,
                write_manifest=request.output.write_manifest if request.output else False,
                manifest_output_path=request.output.manifest_path if request.output else None,
                debug=bool(_bool_option(request.options, "debug")),
                hybrid=_bool_option(request.options, "hybrid"),
                include_related_entities=bool(
                    _bool_option(request.options, "include_related_entities")
                ),
                include_graph_support=bool(
                    _bool_option(request.options, "include_graph_support")
                ),
                refine_links=bool(_bool_option(request.options, "refine_links")),
                refinement_depth=_refinement_depth_option(request.options),
                include_impact_paths=bool(
                    _bool_option(request.options, "include_impact_paths")
                ),
                impact_max_depth=_int_option(request.options, "impact_max_depth"),
                impact_seed_limit=_int_option(request.options, "impact_seed_limit"),
                impact_max_candidates=_int_option(
                    request.options,
                    "impact_max_candidates",
                ),
                domain_hint=_request_string_option(
                    request.domain_hint,
                    request.options,
                    "domain_hint",
                ),
                retrieval_profile=_request_string_option(
                    request.retrieval_profile,
                    request.options,
                    "retrieval_profile",
                ),
                country_hint=_string_option(request.options, "country_hint"),
                registry=runtime_state.ensure_registry(),
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v0/lookup", response_model=LookupResponse)
    def runtime_lookup(
        q: str = Query(..., min_length=1),
        pack_id: str | None = Query(None),
        exact_alias: bool = Query(False),
        fuzzy: bool = Query(False),
        active_only: bool = Query(True),
        limit: int = Query(20, ge=1, le=100),
    ) -> LookupResponse:
        """Search deterministic alias and rule metadata candidates."""

        try:
            return lookup_candidates(
                q,
                storage_root=storage_root,
                pack_id=pack_id,
                exact_alias=exact_alias,
                fuzzy=fuzzy,
                active_only=active_only,
                limit=limit,
            )
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)

    return app


app = create_app()
