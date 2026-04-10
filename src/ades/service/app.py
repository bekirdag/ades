"""Local FastAPI application for ades."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from ..api import (
    activate_pack,
    build_finance_source_bundle,
    build_general_source_bundle,
    build_medical_source_bundle,
    build_registry,
    deactivate_pack,
    generate_pack_source,
    get_pack,
    list_available_packs,
    list_packs,
    lookup_candidates,
    npm_installer_info,
    publish_release,
    report_generated_pack,
    release_versions,
    refresh_generated_packs,
    remove_pack,
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
    RegistryBuildGeneralBundleRequest,
    RegistryBuildGeneralBundleResponse,
    RegistryBuildMedicalBundleRequest,
    RegistryBuildMedicalBundleResponse,
    RegistryBuildRequest,
    RegistryBuildResponse,
    RegistryPackQualityResponse,
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
                symbol_directory_path=request.symbol_directory_path,
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
                active_only=active_only,
                limit=limit,
            )
        except FileNotFoundError as exc:
            _raise_configuration_http_exception(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _raise_configuration_http_exception(exc)

    return app


app = create_app()
