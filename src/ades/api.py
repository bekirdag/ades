"""Public library helpers for ades v0.1.0."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

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
from .packs.finance_quality import validate_finance_pack_quality as run_validate_finance_pack_quality
from .packs.general_bundle import build_general_source_bundle as run_build_general_source_bundle
from .packs.general_quality import validate_general_pack_quality as run_validate_general_pack_quality
from .packs.medical_bundle import build_medical_source_bundle as run_build_medical_source_bundle
from .packs.medical_quality import validate_medical_pack_quality as run_validate_medical_pack_quality
from .packs.refresh import refresh_generated_pack_registry as run_refresh_generated_pack_registry
from .packs.reporting import report_generated_pack as run_report_generated_pack
from .packs.publish import build_static_registry
from .packs.registry import PackRegistry, load_registry_index
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
    LookupCandidate,
    LookupResponse,
    ReleaseManifestResponse,
    ReleasePublishResponse,
    ReleaseValidationResponse,
    ReleaseVersionState,
    ReleaseVersionSyncResponse,
    NpmInstallerInfo,
    PackSummary,
    ReleaseVerificationResponse,
    RegistryBuildPackSummary,
    RegistryBuildResponse,
    RegistryBuildFinanceBundleResponse,
    RegistryBuildGeneralBundleResponse,
    RegistryBuildMedicalBundleResponse,
    RegistryPackQualityCaseResult,
    RegistryPackQualityResponse,
    RegistryRefreshGeneratedPacksResponse,
    RegistryRefreshPackResponse,
    RegistryValidateFinanceQualityResponse,
    RegistryGeneratePackResponse,
    RegistryReportPackResponse,
    SourceFingerprint,
    StatusResponse,
    TagResponse,
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
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = 0,
    max_dropped_alias_ratio: float = 0.5,
) -> RegistryRefreshGeneratedPacksResponse:
    """Refresh one or more generated packs into a quality-gated registry release."""

    result = run_refresh_generated_pack_registry(
        list(bundle_dirs),
        output_dir=output_dir,
        general_bundle_dir=general_bundle_dir,
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
        report_dir=result.report_dir,
        quality_dir=result.quality_dir,
        generated_at=result.generated_at,
        pack_count=result.pack_count,
        passed=result.passed,
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


def build_finance_source_bundle(
    *,
    sec_companies_path: str | Path,
    symbol_directory_path: str | Path,
    curated_entities_path: str | Path,
    output_dir: str | Path,
    version: str = "0.2.0",
) -> RegistryBuildFinanceBundleResponse:
    """Build one normalized `finance-en` source bundle from raw snapshot files."""

    result = run_build_finance_source_bundle(
        sec_companies_path=sec_companies_path,
        symbol_directory_path=symbol_directory_path,
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
        entities_path=result.entities_path,
        rules_path=result.rules_path,
        generated_at=result.generated_at,
        source_count=result.source_count,
        entity_record_count=result.entity_record_count,
        rule_record_count=result.rule_record_count,
        sec_issuer_count=result.sec_issuer_count,
        symbol_count=result.symbol_count,
        curated_entity_count=result.curated_entity_count,
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
        curated_entity_count=result.curated_entity_count,
        warnings=result.warnings,
    )


def validate_general_pack_quality(
    bundle_dir: str | Path,
    *,
    output_dir: str | Path,
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = 0,
    max_dropped_alias_ratio: float = 0.5,
) -> RegistryPackQualityResponse:
    """Build, install, and evaluate one generated `general-en` pack bundle."""

    result = run_validate_general_pack_quality(
        str(bundle_dir),
        output_dir=str(output_dir),
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
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = 0,
    max_dropped_alias_ratio: float = 0.5,
) -> RegistryPackQualityResponse:
    """Build, install, and evaluate one generated `medical-en` pack bundle."""

    result = run_validate_medical_pack_quality(
        str(bundle_dir),
        general_bundle_dir=str(general_bundle_dir),
        output_dir=str(output_dir),
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
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = 0,
    max_dropped_alias_ratio: float = 0.5,
) -> RegistryValidateFinanceQualityResponse:
    """Build, install, and evaluate one generated `finance-en` pack bundle."""

    result = run_validate_finance_pack_quality(
        bundle_dir,
        output_dir=output_dir,
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
) -> TagResponse:
    """Run in-process tagging through the installed local runtime."""

    settings = _resolve_settings(storage_root=storage_root)
    resolved_pack = pack or settings.default_pack
    response = tag_text(
        text=text,
        pack=resolved_pack,
        content_type=content_type,
        storage_root=settings.storage_root,
    )
    if output_path is not None or output_dir is not None:
        return persist_tag_response_json(
            response,
            pack_id=resolved_pack,
            output_path=output_path,
            output_dir=output_dir,
            pretty=pretty_output,
        )
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
) -> TagResponse:
    """Run in-process tagging for a local file path."""

    settings = _resolve_settings(storage_root=storage_root)
    resolved_pack = pack or settings.default_pack
    resolved_path, text, resolved_content_type, input_size_bytes, source_fingerprint = load_tag_file(
        path,
        content_type=content_type,
    )
    response = tag_text(
        text=text,
        pack=resolved_pack,
        content_type=resolved_content_type,
        storage_root=settings.storage_root,
    )
    response = response.model_copy(
        update={
            "content_type": resolved_content_type,
            "source_path": str(resolved_path),
            "input_size_bytes": input_size_bytes,
            "source_fingerprint": SourceFingerprint(**source_fingerprint.to_dict()),
        }
    )
    if output_path is not None or output_dir is not None:
        return persist_tag_response_json(
            response,
            pack_id=resolved_pack,
            output_path=output_path,
            output_dir=output_dir,
            pretty=pretty_output,
        )
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
    ) -> BatchTagResponse:
    """Run in-process tagging for multiple local file paths."""

    settings = _resolve_settings(storage_root=storage_root)
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
                        ).model_copy(
                            update={
                                "content_type": resolved_content_type,
                                "source_path": str(resolved_path),
                                "input_size_bytes": input_size_bytes,
                                "source_fingerprint": SourceFingerprint(**source_fingerprint.to_dict()),
                            }
                        )
                        repaired_response = persist_tag_response_json(
                            repaired_response,
                            pack_id=resolved_pack,
                            output_path=reused_item.saved_output_path if has_saved_output_path else None,
                            output_dir=output_dir if not has_saved_output_path else None,
                            pretty=pretty_output,
                        )
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
        )
        items.append(
            response.model_copy(
                update={
                    "content_type": resolved_content_type,
                    "source_path": str(resolved_path),
                    "input_size_bytes": input_size_bytes,
                    "source_fingerprint": SourceFingerprint(**source_fingerprint.to_dict()),
                }
            )
        )
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
    active_only: bool = True,
    limit: int = 20,
) -> LookupResponse:
    """Search deterministic metadata candidates from the configured local store."""

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
            active_only=active_only,
            limit=bounded_limit,
        )
    ]
    return LookupResponse(
        query=query,
        pack_id=pack_id,
        exact_alias=exact_alias,
        active_only=active_only,
        candidates=candidates,
    )


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
