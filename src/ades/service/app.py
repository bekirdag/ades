"""Local FastAPI application for ades."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from ..api import (
    activate_pack,
    benchmark_runtime,
    build_finance_source_bundle,
    fetch_finance_source_snapshot,
    build_general_source_bundle,
    fetch_general_source_snapshot,
    build_medical_source_bundle,
    fetch_medical_source_snapshot,
    build_registry,
    compare_extraction_quality_reports,
    deactivate_pack,
    diff_pack_versions,
    evaluate_extraction_quality,
    evaluate_extraction_release_thresholds,
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
from ..storage import UnsupportedRuntimeConfigurationError
from ..version import __version__
from .models import (
    AvailablePackSummary,
    BatchFileTagRequest,
    BatchTagResponse,
    FileTagRequest,
    LookupResponse,
    NpmInstallerInfo,
    PackSummary,
    PackHealthResponse,
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
    RegistryFetchFinanceSourcesRequest,
    RegistryFetchFinanceSourcesResponse,
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
    RegistryCompareExtractionQualityRequest,
    RegistryCompareExtractionQualityResponse,
    RegistryDiffPackRequest,
    RegistryEvaluateExtractionQualityRequest,
    RegistryEvaluateExtractionQualityResponse,
    RegistryEvaluateReleaseThresholdsRequest,
    RegistryEvaluateReleaseThresholdsResponse,
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


def _raise_configuration_http_exception(exc: Exception) -> None:
    """Map one configuration/runtime failure onto the public HTTP contract."""

    if isinstance(exc, FileNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    raise HTTPException(status_code=400, detail=str(exc)) from exc


def create_app(*, storage_root: str | Path | None = None) -> FastAPI:
    """Create a FastAPI application bound to a storage root."""

    app = FastAPI(title="ades", version=__version__)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        """Simple health endpoint."""

        return {"status": "ok", "version": __version__}

    @app.get("/v0/status", response_model=StatusResponse)
    def runtime_status() -> StatusResponse:
        """Report local runtime status."""

        try:
            return status(storage_root=storage_root)
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

    @app.post("/v0/tag", response_model=TagResponse)
    def runtime_tag(request: TagRequest) -> TagResponse:
        """Tag text through the local in-process pipeline."""

        try:
            return tag(
                request.text,
                pack=request.pack,
                content_type=request.content_type,
                output_path=request.output.path if request.output else None,
                output_dir=request.output.directory if request.output else None,
                pretty_output=request.output.pretty if request.output else True,
                storage_root=storage_root,
                debug=bool(_bool_option(request.options, "debug")),
                hybrid=_bool_option(request.options, "hybrid"),
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
            return tag_file(
                request.path,
                pack=request.pack,
                content_type=request.content_type,
                output_path=request.output.path if request.output else None,
                output_dir=request.output.directory if request.output else None,
                pretty_output=request.output.pretty if request.output else True,
                storage_root=storage_root,
                debug=bool(_bool_option(request.options, "debug")),
                hybrid=_bool_option(request.options, "hybrid"),
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
