"""API models for the local service."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Literal
from typing import Any

from pydantic import BaseModel, Field, model_validator

from ..packs.quality_defaults import DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES

try:
    from wordfreq import zipf_frequency as _wordfreq_zipf_frequency
except ImportError:  # pragma: no cover - packaging/tests install wordfreq
    _wordfreq_zipf_frequency = None

_GENERAL_EXECUTIVE_TITLE_ACRONYMS = {
    "cao",
    "cbo",
    "cco",
    "cdo",
    "ceo",
    "cfo",
    "cho",
    "cio",
    "cmo",
    "coo",
    "cpo",
    "cro",
    "cso",
    "cto",
}
_GENERAL_OUTPUT_ARTICLE_LEADS = {"the"}
_GENERAL_OUTPUT_SINGLE_TOKEN_GENERIC_ZIPF_MIN = 5.25
_GENERAL_OUTPUT_ARTICLE_TAIL_GENERIC_ZIPF_MIN = 4.7
_GENERAL_OUTPUT_EXACT_NOISE_TEXTS = {"canadian", "cut off", "unknown"}


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
    matcher_ready: bool = False
    matcher_algorithm: str | None = None
    matcher_entry_count: int | None = None


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
    metadata_persistence_backend: str
    exact_extraction_backend: str
    operator_lookup_backend: str
    storage_root: str
    host: str
    port: int
    registry_url: str | None = None
    installed_packs: list[str] = Field(default_factory=list)
    prewarmed_packs: list[str] = Field(default_factory=list)


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


class RegistryBuildFinanceBundleRequest(BaseModel):
    """Request body for building one normalized `finance-en` source bundle."""

    sec_companies_path: str
    sec_submissions_path: str | None = None
    sec_companyfacts_path: str | None = None
    symbol_directory_path: str
    other_listed_path: str | None = None
    finance_people_path: str | None = None
    curated_entities_path: str
    output_dir: str
    version: str = "0.2.0"


class RegistryBuildFinanceBundleResponse(BaseModel):
    """Response body for one built normalized `finance-en` source bundle."""

    pack_id: str
    version: str
    output_dir: str
    bundle_dir: str
    bundle_manifest_path: str
    sources_lock_path: str
    entities_path: str
    rules_path: str
    generated_at: datetime
    source_count: int
    entity_record_count: int
    rule_record_count: int
    sec_issuer_count: int
    symbol_count: int
    person_count: int
    company_equity_ticker_count: int
    company_name_enriched_issuer_count: int
    curated_entity_count: int
    warnings: list[str] = Field(default_factory=list)


class RegistryFetchFinanceSourcesRequest(BaseModel):
    """Request body for downloading one real `finance-en` source snapshot set."""

    output_dir: str = "/mnt/githubActions/ades_big_data/pack_sources/raw/finance-en"
    snapshot: str | None = None
    sec_companies_url: str = "https://www.sec.gov/files/company_tickers.json"
    sec_submissions_url: str = (
        "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
    )
    sec_companyfacts_url: str = (
        "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
    )
    symbol_directory_url: str = (
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    )
    other_listed_url: str = (
        "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    )
    finance_people_url: str | None = None
    derive_finance_people_from_sec: bool = False
    finance_people_archive_base_url: str = (
        "https://www.sec.gov/Archives/edgar/data"
    )
    finance_people_max_companies: int | None = None
    user_agent: str = "ades/0.1.0 (ops@adestool.com)"


class RegistryFetchFinanceSourcesResponse(BaseModel):
    """Response body for one downloaded `finance-en` source snapshot set."""

    pack_id: str
    output_dir: str
    snapshot: str
    snapshot_dir: str
    sec_companies_url: str
    sec_submissions_url: str
    sec_companyfacts_url: str
    symbol_directory_url: str
    other_listed_url: str
    finance_people_url: str
    source_manifest_path: str
    sec_companies_path: str
    sec_submissions_path: str
    sec_companyfacts_path: str
    symbol_directory_path: str
    other_listed_path: str
    finance_people_path: str
    curated_entities_path: str
    generated_at: datetime
    source_count: int
    curated_entity_count: int
    sec_companies_sha256: str
    sec_submissions_sha256: str
    sec_companyfacts_sha256: str
    symbol_directory_sha256: str
    other_listed_sha256: str
    finance_people_sha256: str
    curated_entities_sha256: str
    warnings: list[str] = Field(default_factory=list)


class RegistryFetchFinanceCountrySourcesRequest(BaseModel):
    """Request body for downloading country-scoped finance source snapshots."""

    output_dir: str = "/mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en"
    snapshot: str | None = None
    country_codes: list[str] = Field(default_factory=list)
    user_agent: str = "ades/0.1.0 (ops@adestool.com)"


class RegistryFinanceCountrySourceItemResponse(BaseModel):
    """One downloaded country-scoped finance source snapshot set."""

    country_code: str
    country_name: str
    pack_id: str
    snapshot_dir: str
    profile_path: str
    source_manifest_path: str
    curated_entities_path: str
    source_count: int
    curated_entity_count: int
    warnings: list[str] = Field(default_factory=list)


class RegistryFetchFinanceCountrySourcesResponse(BaseModel):
    """Response body for downloaded country-scoped finance source snapshots."""

    output_dir: str
    snapshot: str
    snapshot_dir: str
    generated_at: datetime
    country_count: int
    countries: list[RegistryFinanceCountrySourceItemResponse] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class RegistryBuildFinanceCountryBundlesRequest(BaseModel):
    """Request body for building country-scoped finance bundles."""

    snapshot_dir: str
    output_dir: str = "/mnt/githubActions/ades_big_data/pack_sources/bundles/finance-country-en"
    country_codes: list[str] = Field(default_factory=list)
    version: str = "0.2.0"


class RegistryFinanceCountryBundleItemResponse(BaseModel):
    """One built country-scoped finance bundle."""

    country_code: str
    country_name: str
    pack_id: str
    version: str
    bundle_dir: str
    bundle_manifest_path: str
    sources_lock_path: str
    entities_path: str
    rules_path: str
    generated_at: datetime
    source_count: int
    entity_record_count: int
    rule_record_count: int
    organization_count: int
    exchange_count: int
    market_index_count: int
    warnings: list[str] = Field(default_factory=list)


class RegistryBuildFinanceCountryBundlesResponse(BaseModel):
    """Response body for built country-scoped finance bundles."""

    output_dir: str
    generated_at: datetime
    country_count: int
    bundles: list[RegistryFinanceCountryBundleItemResponse] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class RegistryFetchGeneralSourcesRequest(BaseModel):
    """Request body for downloading one real `general-en` source snapshot set."""

    output_dir: str = "/mnt/githubActions/ades_big_data/pack_sources/raw/general-en"
    snapshot: str | None = None
    wikidata_url: str | None = None
    wikidata_truthy_url: str | None = (
        "https://dumps.wikimedia.org/wikidatawiki/entities/latest-truthy.nt.gz"
    )
    wikidata_entities_url: str | None = (
        "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz"
    )
    geonames_places_url: str = "https://download.geonames.org/export/dump/allCountries.zip"
    geonames_alternate_names_url: str | None = (
        "https://download.geonames.org/export/dump/alternateNamesV2.zip"
    )
    geonames_modifications_url: str | None = None
    geonames_deletes_url: str | None = None
    geonames_alternate_modifications_url: str | None = None
    geonames_alternate_deletes_url: str | None = None
    user_agent: str = "ades/0.1.0 (ops@adestool.com)"


class RegistryFetchGeneralSourcesResponse(BaseModel):
    """Response body for one downloaded `general-en` source snapshot set."""

    pack_id: str
    output_dir: str
    snapshot: str
    snapshot_dir: str
    wikidata_url: str
    geonames_places_url: str
    source_manifest_path: str
    wikidata_entities_path: str
    geonames_places_path: str
    curated_entities_path: str
    generated_at: datetime
    source_count: int
    wikidata_entity_count: int
    geonames_location_count: int
    curated_entity_count: int
    wikidata_entities_sha256: str
    geonames_places_sha256: str
    curated_entities_sha256: str
    warnings: list[str] = Field(default_factory=list)


class RegistryFetchMedicalSourcesRequest(BaseModel):
    """Request body for downloading one real `medical-en` source snapshot set."""

    output_dir: str = "/mnt/githubActions/ades_big_data/pack_sources/raw/medical-en"
    snapshot: str | None = None
    disease_ontology_url: str = (
        "https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/"
        "main/src/ontology/doid.obo"
    )
    hgnc_genes_url: str = (
        "https://storage.googleapis.com/public-download-files/hgnc/json/json/"
        "hgnc_complete_set.json"
    )
    uniprot_proteins_url: str = (
        "https://rest.uniprot.org/uniprotkb/search?"
        "query=%28reviewed%3Atrue%20AND%20organism_id%3A9606%29&"
        "format=json&fields=accession%2Cprotein_name%2Cgene_names&size=500"
    )
    clinical_trials_url: str = "https://clinicaltrials.gov/api/v2/studies?format=json"
    orange_book_url: str = "https://www.fda.gov/media/76860/download?attachment"
    user_agent: str = "ades/0.1.0 (ops@adestool.com)"
    uniprot_max_records: int = Field(0, ge=0)
    clinical_trials_max_records: int = Field(0, ge=0)


class RegistryFetchMedicalSourcesResponse(BaseModel):
    """Response body for one downloaded `medical-en` source snapshot set."""

    pack_id: str
    output_dir: str
    snapshot: str
    snapshot_dir: str
    disease_ontology_url: str
    hgnc_genes_url: str
    uniprot_proteins_url: str
    clinical_trials_url: str
    orange_book_url: str
    source_manifest_path: str
    disease_ontology_path: str
    hgnc_genes_path: str
    uniprot_proteins_path: str
    clinical_trials_path: str
    orange_book_path: str
    curated_entities_path: str
    generated_at: datetime
    source_count: int
    disease_count: int
    gene_count: int
    protein_count: int
    clinical_trial_count: int
    orange_book_product_count: int
    curated_entity_count: int
    disease_ontology_sha256: str
    hgnc_genes_sha256: str
    uniprot_proteins_sha256: str
    clinical_trials_sha256: str
    orange_book_sha256: str
    curated_entities_sha256: str
    warnings: list[str] = Field(default_factory=list)


class RegistryBuildGeneralBundleRequest(BaseModel):
    """Request body for building one normalized `general-en` source bundle."""

    wikidata_entities_path: str
    geonames_places_path: str
    curated_entities_path: str
    output_dir: str
    version: str = "0.2.0"


class RegistryBuildGeneralBundleResponse(BaseModel):
    """Response body for one built normalized `general-en` source bundle."""

    pack_id: str
    version: str
    output_dir: str
    bundle_dir: str
    bundle_manifest_path: str
    sources_lock_path: str
    entities_path: str
    rules_path: str
    generated_at: datetime
    source_count: int
    entity_record_count: int
    rule_record_count: int
    wikidata_entity_count: int
    geonames_location_count: int
    curated_entity_count: int
    warnings: list[str] = Field(default_factory=list)


class RegistryBuildMedicalBundleRequest(BaseModel):
    """Request body for building one normalized `medical-en` source bundle."""

    disease_ontology_path: str
    hgnc_genes_path: str
    uniprot_proteins_path: str
    clinical_trials_path: str
    orange_book_products_path: str | None = None
    curated_entities_path: str
    output_dir: str
    version: str = "0.2.0"


class RegistryBuildMedicalBundleResponse(BaseModel):
    """Response body for one built normalized `medical-en` source bundle."""

    pack_id: str
    version: str
    output_dir: str
    bundle_dir: str
    bundle_manifest_path: str
    sources_lock_path: str
    entities_path: str
    rules_path: str
    generated_at: datetime
    source_count: int
    entity_record_count: int
    rule_record_count: int
    disease_count: int
    gene_count: int
    protein_count: int
    clinical_trial_count: int
    drug_count: int
    curated_entity_count: int
    warnings: list[str] = Field(default_factory=list)


class RegistryValidateFinanceQualityRequest(BaseModel):
    """Request body for validating one generated `finance-en` pack bundle."""

    bundle_dir: str
    output_dir: str
    fixture_profile: str = "benchmark"
    version: str | None = None
    min_expected_recall: float = Field(1.0, ge=0.0, le=1.0)
    max_unexpected_hits: int = Field(0, ge=0)
    max_ambiguous_aliases: int = Field(300, ge=0)
    max_dropped_alias_ratio: float = Field(0.5, ge=0.0, le=1.0)


class RegistryQualityEntity(BaseModel):
    """Tagged or expected entity reported by offline pack-quality validation."""

    text: str
    label: str


class RegistryPackQualityCaseResult(BaseModel):
    """Outcome for one generated-pack quality fixture case."""

    name: str
    text: str
    expected_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    actual_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    matched_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    missing_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    unexpected_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    passed: bool


class RegistryFinanceQualityCaseResult(BaseModel):
    """Outcome for one finance pack quality fixture case."""

    name: str
    text: str
    expected_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    actual_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    matched_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    missing_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    unexpected_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    passed: bool


class RegistryValidateFinanceQualityResponse(BaseModel):
    """Response body for one validated generated `finance-en` pack bundle."""

    pack_id: str
    version: str
    bundle_dir: str
    output_dir: str
    generated_pack_dir: str
    registry_dir: str
    registry_index_path: str
    registry_index_url: str
    storage_root: str
    evaluated_at: datetime
    fixture_profile: str
    fixture_count: int
    passed_case_count: int
    failed_case_count: int
    expected_entity_count: int
    actual_entity_count: int
    matched_expected_entity_count: int
    missing_expected_entity_count: int
    unexpected_entity_count: int
    expected_recall: float
    precision: float
    alias_count: int
    unique_canonical_count: int
    rule_count: int
    dropped_alias_count: int
    ambiguous_alias_count: int
    dropped_alias_ratio: float
    passed: bool
    failures: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    cases: list[RegistryFinanceQualityCaseResult] = Field(default_factory=list)


class RegistryValidateGeneralQualityRequest(BaseModel):
    """Request body for validating one generated `general-en` pack bundle."""

    bundle_dir: str
    output_dir: str
    fixture_profile: str = "benchmark"
    version: str | None = None
    min_expected_recall: float = Field(1.0, ge=0.0, le=1.0)
    max_unexpected_hits: int = Field(0, ge=0)
    max_ambiguous_aliases: int = Field(
        DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES, ge=0
    )
    max_dropped_alias_ratio: float = Field(0.5, ge=0.0, le=1.0)


class RegistryValidateMedicalQualityRequest(BaseModel):
    """Request body for validating one generated `medical-en` pack bundle."""

    bundle_dir: str
    general_bundle_dir: str
    output_dir: str
    fixture_profile: str = "benchmark"
    version: str | None = None
    min_expected_recall: float = Field(1.0, ge=0.0, le=1.0)
    max_unexpected_hits: int = Field(0, ge=0)
    max_ambiguous_aliases: int = Field(25, ge=0)
    max_dropped_alias_ratio: float = Field(0.5, ge=0.0, le=1.0)


class RegistryPackQualityResponse(BaseModel):
    """Response body for one validated generated pack bundle."""

    pack_id: str
    version: str
    bundle_dir: str
    output_dir: str
    generated_pack_dir: str
    registry_dir: str
    registry_index_path: str
    registry_index_url: str
    storage_root: str
    evaluated_at: datetime
    fixture_profile: str
    fixture_count: int
    passed_case_count: int
    failed_case_count: int
    expected_entity_count: int
    actual_entity_count: int
    matched_expected_entity_count: int
    missing_expected_entity_count: int
    unexpected_entity_count: int
    expected_recall: float
    precision: float
    alias_count: int
    unique_canonical_count: int
    rule_count: int
    dropped_alias_count: int
    ambiguous_alias_count: int
    dropped_alias_ratio: float
    passed: bool
    failures: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    cases: list[RegistryPackQualityCaseResult] = Field(default_factory=list)


class ExtractionQualityCaseResultResponse(BaseModel):
    """Outcome for one evaluated extraction-quality document."""

    name: str
    expected_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    actual_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    matched_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    missing_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    unexpected_entities: list[RegistryQualityEntity] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    token_count: int
    entity_count: int
    entities_per_100_tokens: float
    overlap_drop_count: int
    chunk_count: int
    timing_ms: int
    passed: bool


class ExtractionQualityReportResponse(BaseModel):
    """Extraction-quality report for one installed pack and one golden set."""

    pack_id: str
    profile: str
    hybrid: bool
    document_count: int
    expected_entity_count: int
    actual_entity_count: int
    matched_entity_count: int
    missing_entity_count: int
    unexpected_entity_count: int
    recall: float
    precision: float
    entities_per_100_tokens: float
    overlap_drop_count: int
    chunk_count: int
    low_density_warning_count: int
    p50_latency_ms: int = 0
    p95_latency_ms: int
    per_label_expected: dict[str, int] = Field(default_factory=dict)
    per_label_actual: dict[str, int] = Field(default_factory=dict)
    per_label_matched: dict[str, int] = Field(default_factory=dict)
    per_label_recall: dict[str, float] = Field(default_factory=dict)
    per_lane_counts: dict[str, int] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    cases: list[ExtractionQualityCaseResultResponse] = Field(default_factory=list)


class RegistryEvaluateExtractionQualityRequest(BaseModel):
    """Request body for evaluating one installed pack against a golden set."""

    pack_id: str
    golden_set_path: str | None = None
    profile: str = "default"
    hybrid: bool = False
    write_report: bool = True
    report_path: str | None = None


class RegistryEvaluateExtractionQualityResponse(ExtractionQualityReportResponse):
    """Extraction-quality report plus optional persisted report path."""

    report_path: str | None = None


class LiveNewsIssueResponse(BaseModel):
    """One heuristic issue discovered during live-news evaluation."""

    issue_type: str
    message: str
    entity_text: str | None = None
    label: str | None = None
    candidate_text: str | None = None


class LiveNewsEntityResponse(BaseModel):
    """Condensed extracted entity summary for one live article."""

    text: str
    label: str
    start: int
    end: int


class LiveNewsFailureResponse(BaseModel):
    """One feed/article failure during live-news evaluation."""

    source: str
    stage: str
    url: str
    message: str
    title: str | None = None


class LiveNewsArticleResultResponse(BaseModel):
    """One evaluated live article result."""

    source: str
    title: str
    article_url: str
    published_at: str | None = None
    text_source: str
    word_count: int
    entity_count: int
    timing_ms: int
    warnings: list[str] = Field(default_factory=list)
    issue_types: list[str] = Field(default_factory=list)
    issues: list[LiveNewsIssueResponse] = Field(default_factory=list)
    entities: list[LiveNewsEntityResponse] = Field(default_factory=list)


class RegistryEvaluateLiveNewsFeedbackResponse(BaseModel):
    """Aggregated live-news feedback report plus optional persisted report path."""

    pack_id: str
    generated_at: str
    requested_article_count: int
    collected_article_count: int
    feed_count: int
    successful_feed_count: int
    p50_latency_ms: int
    p95_latency_ms: int
    per_source_article_counts: dict[str, int] = Field(default_factory=dict)
    per_issue_counts: dict[str, int] = Field(default_factory=dict)
    suggested_fix_classes: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    feed_failures: list[LiveNewsFailureResponse] = Field(default_factory=list)
    article_failures: list[LiveNewsFailureResponse] = Field(default_factory=list)
    articles: list[LiveNewsArticleResultResponse] = Field(default_factory=list)
    report_path: str | None = None


class RuntimeLatencyBenchmarkResponse(BaseModel):
    """Latency summary for one runtime benchmark lane."""

    sample_count: int
    p50_latency_ms: int
    p95_latency_ms: int


class RuntimeDiskBenchmarkResponse(BaseModel):
    """Disk-usage summary for runtime benchmark reporting."""

    runtime_pack_bytes: int
    matcher_artifact_bytes: int
    matcher_entries_bytes: int
    matcher_total_bytes: int
    metadata_store_bytes: int | None = None


class RuntimeBenchmarkReportResponse(BaseModel):
    """Consolidated runtime benchmark report for one installed pack."""

    pack_id: str
    profile: str
    hybrid: bool
    metadata_backend: str
    exact_extraction_backend: str
    operator_lookup_backend: str
    matcher_entry_count: int
    matcher_state_count: int
    matcher_cold_load_ms: int
    lookup_query_count: int
    warm_tagging: RuntimeLatencyBenchmarkResponse
    operator_lookup: RuntimeLatencyBenchmarkResponse
    disk_usage: RuntimeDiskBenchmarkResponse
    quality_report: ExtractionQualityReportResponse
    warnings: list[str] = Field(default_factory=list)


class RegistryBenchmarkRuntimeRequest(BaseModel):
    """Request body for benchmarking runtime matcher and metadata surfaces."""

    pack_id: str
    golden_set_path: str | None = None
    profile: str = "default"
    hybrid: bool = False
    warm_runs: int = Field(3, ge=1, le=25)
    lookup_limit: int = Field(20, ge=1, le=100)
    write_report: bool = True
    report_path: str | None = None


class RegistryBenchmarkRuntimeResponse(RuntimeBenchmarkReportResponse):
    """Runtime benchmark report plus optional persisted report path."""

    report_path: str | None = None


class MatcherBackendCandidateResponse(BaseModel):
    """One candidate matcher backend benchmark result."""

    backend: str
    available: bool
    alias_count: int
    unique_pattern_count: int
    build_ms: int
    cold_load_ms: int
    warm_query_p50_ms: int
    warm_query_p95_ms: int
    artifact_bytes: int
    state_count: int = 0
    token_vocab_count: int = 0
    notes: list[str] = Field(default_factory=list)


class MatcherBenchmarkSpikeReportResponse(BaseModel):
    """Candidate matcher benchmark spike report."""

    pack_id: str
    profile: str
    aliases_path: str
    scanned_alias_count: int
    retained_alias_count: int
    unique_pattern_count: int
    query_count: int
    query_runs: int
    min_alias_score: float
    alias_limit: int
    scan_limit: int
    exact_tier_min_token_count: int
    candidates: list[MatcherBackendCandidateResponse] = Field(default_factory=list)
    recommended_backend: str | None = None
    warnings: list[str] = Field(default_factory=list)


class RegistryBenchmarkMatcherBackendsRequest(BaseModel):
    """Request body for the Phase-0 matcher backend benchmark spike."""

    pack_id: str
    golden_set_path: str | None = None
    profile: str = "default"
    alias_limit: int = Field(10000, ge=1, le=500000)
    scan_limit: int = Field(50000, ge=1, le=1000000)
    min_alias_score: float = Field(0.8, ge=0.0, le=1.0)
    exact_tier_min_token_count: int = Field(2, ge=1, le=16)
    query_runs: int = Field(3, ge=1, le=25)
    write_report: bool = True
    report_path: str | None = None
    output_root: str | None = None


class RegistryBenchmarkMatcherBackendsResponse(MatcherBenchmarkSpikeReportResponse):
    """Matcher benchmark spike report plus optional persisted report path."""

    report_path: str | None = None


class LabelQualityDeltaResponse(BaseModel):
    """Per-label recall delta between two quality reports."""

    label: str
    baseline_recall: float
    candidate_recall: float
    recall_delta: float


class ExtractionQualityDeltaResponse(BaseModel):
    """Aggregate delta between two extraction-quality reports."""

    pack_id: str
    baseline_mode: str
    candidate_mode: str
    recall_delta: float
    precision_delta: float
    p95_latency_delta_ms: int
    per_label_deltas: list[LabelQualityDeltaResponse] = Field(default_factory=list)


class RegistryCompareExtractionQualityRequest(BaseModel):
    """Request body for comparing two stored extraction-quality reports."""

    baseline_report_path: str
    candidate_report_path: str


class RegistryCompareExtractionQualityResponse(ExtractionQualityDeltaResponse):
    """Quality delta plus the source report paths used for comparison."""

    baseline_report_path: str
    candidate_report_path: str


class PackHealthResponse(BaseModel):
    """Aggregated recent extraction-health view for one installed pack."""

    pack_id: str
    requested_window: int
    observation_count: int
    latest_pack_version: str | None = None
    latest_observed_at: str | None = None
    average_entity_count: float = 0.0
    average_entities_per_100_tokens: float = 0.0
    zero_entity_rate: float = 0.0
    warning_rate: float = 0.0
    low_density_warning_rate: float = 0.0
    p95_timing_ms: int = 0
    per_label_counts: dict[str, int] = Field(default_factory=dict)
    per_lane_counts: dict[str, int] = Field(default_factory=dict)
    warning_counts: dict[str, int] = Field(default_factory=dict)


class RegistryDiffPackRequest(BaseModel):
    """Request body for diffing two runtime pack directories."""

    old_pack_dir: str
    new_pack_dir: str


class RegistryPackDiffResponse(BaseModel):
    """Machine-readable semantic diff between two runtime pack directories."""

    pack_id: str
    from_version: str | None = None
    to_version: str | None = None
    severity: str
    requires_retag: bool
    added_labels: list[str] = Field(default_factory=list)
    removed_labels: list[str] = Field(default_factory=list)
    added_rules: list[str] = Field(default_factory=list)
    removed_rules: list[str] = Field(default_factory=list)
    added_aliases: list[str] = Field(default_factory=list)
    removed_aliases: list[str] = Field(default_factory=list)
    changed_canonical_aliases: list[str] = Field(default_factory=list)
    changed_metadata_fields: list[str] = Field(default_factory=list)


class RegistryReleaseThresholdsResponse(BaseModel):
    """Structured threshold configuration for one release decision."""

    min_recall: float
    min_precision: float
    max_label_recall_drop: float
    min_recall_lift: float
    max_precision_drop: float
    max_p95_latency_ms: int | None = None
    max_model_artifact_bytes: int | None = None
    max_peak_memory_mb: int | None = None


class RegistryEvaluateReleaseThresholdsRequest(BaseModel):
    """Request body for evaluating deterministic or hybrid release thresholds."""

    report_path: str
    mode: Literal["deterministic", "hybrid"] = "deterministic"
    baseline_report_path: str | None = None
    min_recall: float | None = Field(None, ge=0.0, le=1.0)
    min_precision: float | None = Field(None, ge=0.0, le=1.0)
    max_label_recall_drop: float | None = Field(None, ge=0.0, le=1.0)
    min_recall_lift: float | None = Field(None, ge=0.0, le=1.0)
    max_precision_drop: float | None = Field(None, ge=0.0, le=1.0)
    max_p95_latency_ms: int | None = Field(None, ge=0)
    max_model_artifact_bytes: int | None = Field(None, ge=0)
    max_peak_memory_mb: int | None = Field(None, ge=0)
    model_artifact_path: str | None = None
    peak_memory_mb: int | None = Field(None, ge=0)


class RegistryEvaluateReleaseThresholdsResponse(BaseModel):
    """Structured release-threshold decision for one stored quality report."""

    report_path: str
    baseline_report_path: str | None = None
    mode: Literal["deterministic", "hybrid"]
    thresholds: RegistryReleaseThresholdsResponse
    model_artifact_bytes: int | None = None
    peak_memory_mb: int | None = None
    passed: bool
    reasons: list[str] = Field(default_factory=list)
    quality_delta: ExtractionQualityDeltaResponse | None = None


class VectorCaseResultResponse(BaseModel):
    """Outcome for one evaluated hosted vector-quality case."""

    name: str
    case_kind: str
    seed_entity_ids: list[str] = Field(default_factory=list)
    expected_related_entity_ids: list[str] = Field(default_factory=list)
    actual_related_entity_ids: list[str] = Field(default_factory=list)
    matched_related_entity_ids: list[str] = Field(default_factory=list)
    missing_related_entity_ids: list[str] = Field(default_factory=list)
    unexpected_related_entity_ids: list[str] = Field(default_factory=list)
    expected_boosted_entity_ids: list[str] = Field(default_factory=list)
    actual_boosted_entity_ids: list[str] = Field(default_factory=list)
    expected_downgraded_entity_ids: list[str] = Field(default_factory=list)
    actual_downgraded_entity_ids: list[str] = Field(default_factory=list)
    expected_suppressed_entity_ids: list[str] = Field(default_factory=list)
    actual_suppressed_entity_ids: list[str] = Field(default_factory=list)
    expected_refinement_applied: bool = False
    actual_refinement_applied: bool = False
    graph_support_applied: bool = False
    fallback_used: bool = False
    refinement_score: float | None = None
    related_precision_at_k: float
    related_recall_at_k: float
    reciprocal_rank: float
    timing_ms: int
    warnings: list[str] = Field(default_factory=list)
    passed: bool


class VectorQualityReportResponse(BaseModel):
    """Aggregate report for hosted vector-quality evaluation."""

    pack_id: str
    profile: str
    refinement_depth: Literal["light", "deep"]
    collection_alias: str | None = None
    provider: str | None = None
    case_count: int
    top_k: int
    related_precision_at_k: float
    related_recall_at_k: float
    related_mrr: float
    suppression_alignment_rate: float
    refinement_alignment_rate: float
    easy_case_pass_rate: float
    graph_support_rate: float
    refinement_trigger_rate: float
    fallback_rate: float
    p50_latency_ms: int = 0
    p95_latency_ms: int = 0
    warnings: list[str] = Field(default_factory=list)
    cases: list[VectorCaseResultResponse] = Field(default_factory=list)


class RegistryEvaluateVectorQualityRequest(BaseModel):
    """Request body for evaluating one hosted vector-quality golden set."""

    pack_id: str
    golden_set_path: str | None = None
    profile: str = "default"
    refinement_depth: Literal["light", "deep"] = "light"
    top_k: int | None = Field(None, ge=1, le=100)
    write_report: bool = True
    report_path: str | None = None


class RegistryEvaluateVectorQualityResponse(VectorQualityReportResponse):
    """Hosted vector-quality report plus optional persisted report path."""

    report_path: str | None = None


class RegistryVectorReleaseThresholdsResponse(BaseModel):
    """Structured threshold configuration for one vector-quality release decision."""

    min_related_precision_at_k: float
    min_related_recall_at_k: float
    min_related_mrr: float
    min_suppression_alignment_rate: float
    min_refinement_alignment_rate: float
    min_easy_case_pass_rate: float
    max_fallback_rate: float
    max_p95_latency_ms: int | None = None


class RegistryEvaluateVectorReleaseThresholdsRequest(BaseModel):
    """Request body for evaluating one stored vector-quality report."""

    report_path: str
    min_related_precision_at_k: float | None = Field(None, ge=0.0, le=1.0)
    min_related_recall_at_k: float | None = Field(None, ge=0.0, le=1.0)
    min_related_mrr: float | None = Field(None, ge=0.0, le=1.0)
    min_suppression_alignment_rate: float | None = Field(None, ge=0.0, le=1.0)
    min_refinement_alignment_rate: float | None = Field(None, ge=0.0, le=1.0)
    min_easy_case_pass_rate: float | None = Field(None, ge=0.0, le=1.0)
    max_fallback_rate: float | None = Field(None, ge=0.0, le=1.0)
    max_p95_latency_ms: int | None = Field(None, ge=0)


class RegistryEvaluateVectorReleaseThresholdsResponse(BaseModel):
    """Structured release-threshold decision for one stored vector-quality report."""

    report_path: str
    thresholds: RegistryVectorReleaseThresholdsResponse
    passed: bool
    reasons: list[str] = Field(default_factory=list)


class RegistryGeneratePackRequest(BaseModel):
    """Request body for generating one pack directory from a normalized bundle."""

    bundle_dir: str
    output_dir: str
    version: str | None = None
    include_build_metadata: bool = True
    include_build_only: bool = False


class RegistryGeneratePackResponse(BaseModel):
    """Response body for one generated runtime-compatible pack directory."""

    pack_id: str
    version: str
    bundle_dir: str
    output_dir: str
    pack_dir: str
    manifest_path: str
    labels_path: str
    aliases_path: str
    rules_path: str
    matcher_artifact_path: str
    matcher_entries_path: str
    matcher_algorithm: str
    matcher_entry_count: int
    matcher_state_count: int
    matcher_max_alias_length: int
    matcher_artifact_sha256: str
    matcher_entries_sha256: str
    sources_path: str | None = None
    build_path: str | None = None
    generated_at: datetime
    label_count: int
    alias_count: int
    rule_count: int
    source_count: int
    publishable_source_count: int
    restricted_source_count: int
    publishable_sources_only: bool
    source_license_classes: dict[str, int] = Field(default_factory=dict)
    included_entity_count: int
    included_rule_count: int
    dropped_record_count: int
    dropped_alias_count: int
    ambiguous_alias_count: int
    warnings: list[str] = Field(default_factory=list)


class RegistryReportPackRequest(BaseModel):
    """Request body for generating one pack report from a normalized bundle."""

    bundle_dir: str
    output_dir: str
    version: str | None = None
    include_build_metadata: bool = True
    include_build_only: bool = False


class RegistryReportPackResponse(BaseModel):
    """Response body for one generated runtime-compatible pack report."""

    pack_id: str
    version: str
    bundle_dir: str
    output_dir: str
    pack_dir: str
    manifest_path: str
    labels_path: str
    aliases_path: str
    rules_path: str
    matcher_artifact_path: str
    matcher_entries_path: str
    matcher_algorithm: str
    matcher_entry_count: int
    matcher_state_count: int
    matcher_max_alias_length: int
    matcher_artifact_sha256: str
    matcher_entries_sha256: str
    sources_path: str | None = None
    build_path: str | None = None
    generated_at: datetime
    source_count: int
    publishable_source_count: int
    restricted_source_count: int
    publishable_sources_only: bool
    source_license_classes: dict[str, int] = Field(default_factory=dict)
    label_count: int
    alias_count: int
    unique_canonical_count: int
    rule_count: int
    included_entity_count: int
    included_rule_count: int
    dropped_record_count: int
    dropped_alias_count: int
    ambiguous_alias_count: int
    dropped_alias_ratio: float
    label_distribution: dict[str, int] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


class RegistryRefreshGeneratedPacksRequest(BaseModel):
    """Request body for building a quality-gated registry release from bundle dirs."""

    bundle_dirs: list[str] = Field(default_factory=list)
    output_dir: str
    general_bundle_dir: str | None = None
    materialize_registry: bool = False
    min_expected_recall: float = Field(1.0, ge=0.0, le=1.0)
    max_unexpected_hits: int = Field(0, ge=0)
    max_ambiguous_aliases: int | None = Field(None, ge=0)
    max_dropped_alias_ratio: float = Field(0.5, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def validate_bundle_dirs(self) -> "RegistryRefreshGeneratedPacksRequest":
        if not self.bundle_dirs:
            raise ValueError("At least one bundle directory is required.")
        return self


class RegistryRefreshPackResponse(BaseModel):
    """Reported and validated output for one bundle in a refresh run."""

    pack_id: str
    bundle_dir: str
    report: RegistryReportPackResponse
    quality: RegistryPackQualityResponse


class RegistryRefreshGeneratedPacksResponse(BaseModel):
    """Response body for one generated-pack refresh run."""

    output_dir: str
    candidate_dir: str
    report_dir: str
    quality_dir: str
    generated_at: datetime
    pack_count: int
    passed: bool
    materialize_registry: bool
    registry_materialized: bool
    registry: RegistryBuildResponse | None = None
    warnings: list[str] = Field(default_factory=list)
    packs: list[RegistryRefreshPackResponse] = Field(default_factory=list)


class RegistryPublishedObject(BaseModel):
    """One object written under a published object-storage prefix."""

    key: str
    size_bytes: int


class RegistryPublishGeneratedReleaseRequest(BaseModel):
    """Request body for publishing a reviewed generated registry to object storage."""

    registry_dir: str
    prefix: str
    bucket: str | None = None
    endpoint: str | None = None
    region: str = "us-east-1"
    delete: bool = True


class RegistryPrepareDeployReleaseRequest(BaseModel):
    """Request body for preparing the deploy-owned registry payload."""

    output_dir: str
    pack_dirs: list[str] = Field(default_factory=list)
    promotion_spec_path: str | None = None


class RegistryPublishGeneratedReleaseResponse(BaseModel):
    """Response body for publishing a reviewed generated registry to object storage."""

    registry_dir: str
    bucket: str
    endpoint: str
    endpoint_url: str
    prefix: str
    storage_uri: str
    index_storage_uri: str
    registry_url_candidates: list[str] = Field(default_factory=list)
    published_at: datetime
    delete: bool
    object_count: int
    objects: list[RegistryPublishedObject] = Field(default_factory=list)


class RegistrySmokePublishedReleaseRequest(BaseModel):
    """Request body for smoking a published generated registry URL."""

    registry_url: str
    pack_ids: list[str] = Field(default_factory=list)


class RegistryPublishedReleaseSmokeCaseResponse(BaseModel):
    """One clean-consumer smoke case against a published registry URL."""

    pack_id: str
    text: str
    expected_installed_pack_ids: list[str] = Field(default_factory=list)
    pulled_pack_ids: list[str] = Field(default_factory=list)
    installed_pack_ids: list[str] = Field(default_factory=list)
    missing_installed_pack_ids: list[str] = Field(default_factory=list)
    expected_entity_texts: list[str] = Field(default_factory=list)
    matched_entity_texts: list[str] = Field(default_factory=list)
    missing_entity_texts: list[str] = Field(default_factory=list)
    tagged_entity_texts: list[str] = Field(default_factory=list)
    expected_labels: list[str] = Field(default_factory=list)
    matched_labels: list[str] = Field(default_factory=list)
    missing_labels: list[str] = Field(default_factory=list)
    tagged_labels: list[str] = Field(default_factory=list)
    passed: bool
    failure: str | None = None


class RegistrySmokePublishedReleaseResponse(BaseModel):
    """Response body for clean consumer smoke against a published registry URL."""

    registry_url: str
    checked_at: datetime
    fixture_profile: str
    pack_count: int
    passed_case_count: int
    failed_case_count: int
    passed: bool
    failures: list[str] = Field(default_factory=list)
    cases: list[RegistryPublishedReleaseSmokeCaseResponse] = Field(default_factory=list)


class RegistryPrepareDeployReleaseResponse(BaseModel):
    """Response body for preparing the deploy-owned registry payload."""

    mode: Literal["bundled", "promoted"]
    output_dir: str
    index_path: str
    index_url: str
    generated_at: datetime
    pack_count: int
    packs: list[RegistryBuildPackSummary] = Field(default_factory=list)
    promotion_spec_path: str | None = None
    promoted_registry_url: str | None = None
    consumer_smoke: RegistrySmokePublishedReleaseResponse | None = None


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
    serve_tag_files: ReleaseCommandResult | None = None
    serve_tag_files_replay: ReleaseCommandResult | None = None
    remove_guardrail: ReleaseCommandResult | None = None
    remove: ReleaseCommandResult | None = None
    remove_status: ReleaseCommandResult | None = None
    passed: bool
    reported_version: str | None = None
    pulled_pack_ids: list[str] = Field(default_factory=list)
    tagged_labels: list[str] = Field(default_factory=list)
    recovered_pack_ids: list[str] = Field(default_factory=list)
    served_pack_ids: list[str] = Field(default_factory=list)
    remaining_pack_ids: list[str] = Field(default_factory=list)
    serve_tagged_labels: list[str] = Field(default_factory=list)
    serve_tag_file_labels: list[str] = Field(default_factory=list)
    serve_tag_files_labels: list[str] = Field(default_factory=list)
    serve_tag_files_replay_labels: list[str] = Field(default_factory=list)


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

    match_kind: Literal["alias", "rule", "proposal"]
    match_path: str
    match_source: str
    source_pack: str
    source_domain: str
    lane: str | None = None
    model_name: str | None = None
    model_version: str | None = None


class EntityLink(BaseModel):
    """Lightweight local link metadata for a matched entity."""

    entity_id: str
    canonical_text: str
    provider: str


class RelatedEntityMatch(BaseModel):
    """Related entity returned by the hosted QID graph enrichment lane."""

    entity_id: str
    canonical_text: str
    score: float
    provider: str
    entity_type: str | None = None
    source_name: str | None = None
    packs: list[str] = Field(default_factory=list)
    seed_entity_ids: list[str] = Field(default_factory=list)
    shared_seed_count: int = 0


class GraphSupport(BaseModel):
    """Metadata describing one hosted graph-enrichment attempt."""

    requested: bool = False
    applied: bool = False
    provider: str | None = None
    collection_alias: str | None = None
    requested_related_entities: bool = False
    requested_refinement: bool = False
    refinement_depth: Literal["light", "deep"] | None = None
    seed_entity_ids: list[str] = Field(default_factory=list)
    coherence_score: float | None = None
    related_entity_count: int = 0
    refined_entity_count: int = 0
    boosted_entity_ids: list[str] = Field(default_factory=list)
    downgraded_entity_ids: list[str] = Field(default_factory=list)
    suppressed_entity_ids: list[str] = Field(default_factory=list)
    suppressed_entity_count: int = 0
    warnings: list[str] = Field(default_factory=list)


class EntityMatch(BaseModel):
    """Single extracted entity."""

    text: str
    label: str
    start: int
    end: int
    aliases: list[str] = Field(default_factory=list)
    mention_count: int = Field(default=1, ge=1)
    confidence: float | None = None
    relevance: float | None = None
    provenance: EntityProvenance | None = None
    link: EntityLink | None = None


def _merge_duplicate_linked_entities(entities: list[EntityMatch]) -> tuple[list[EntityMatch], int]:
    ordered_entity_ids: list[str] = []
    grouped: dict[str, list[EntityMatch]] = {}
    passthrough: list[EntityMatch] = []

    for entity in entities:
        link = entity.link
        entity_id = link.entity_id.strip() if link is not None else ""
        if not entity_id:
            passthrough.append(entity)
            continue
        if entity_id not in grouped:
            ordered_entity_ids.append(entity_id)
            grouped[entity_id] = []
        grouped[entity_id].append(entity)

    merged: list[EntityMatch] = list(passthrough)
    merged_count = 0
    for entity_id in ordered_entity_ids:
        group = grouped[entity_id]
        if len(group) == 1:
            merged.append(group[0])
            continue
        merged_count += len(group) - 1
        representative = min(
            group,
            key=lambda item: (
                item.start,
                item.end,
                -(item.relevance if item.relevance is not None else (item.confidence or 0.0)),
                item.label.casefold(),
                item.text.casefold(),
            ),
        )
        alias_values: list[str] = []
        seen_aliases: set[str] = set()
        for item in group:
            values = item.aliases or [item.text]
            for value in values:
                normalized = value.casefold()
                if not value or normalized in seen_aliases:
                    continue
                seen_aliases.add(normalized)
                alias_values.append(value)
        confidence_values = [item.confidence for item in group if item.confidence is not None]
        relevance_values = [item.relevance for item in group if item.relevance is not None]
        mention_count = sum(max(item.mention_count, 1) for item in group)
        merged.append(
            representative.model_copy(
                update={
                    "aliases": alias_values,
                    "mention_count": mention_count,
                    "confidence": max(confidence_values) if confidence_values else None,
                    "relevance": max(relevance_values) if relevance_values else None,
                }
            )
        )

    merged.sort(key=lambda item: (item.start, item.end, item.label.casefold(), item.text.casefold()))
    return merged, merged_count


def _is_generic_general_org_title_alias(text: str) -> bool:
    normalized = "".join(character for character in text.casefold() if character.isalpha())
    return normalized in _GENERAL_EXECUTIVE_TITLE_ACRONYMS


def _wordfreq_zipf_en(text: str) -> float:
    if _wordfreq_zipf_frequency is None:
        return 0.0
    return float(_wordfreq_zipf_frequency(text.casefold(), "en"))


def _is_common_english_general_org_alias(entity: EntityMatch) -> bool:
    if entity.label != "organization":
        return False
    if entity.link is None:
        return False
    if entity.link.canonical_text.casefold() != entity.text.casefold():
        return False
    text = entity.text.strip()
    if not text.isascii():
        return False
    tokens = [token.casefold() for token in text.split() if token.isalpha()]
    if not tokens:
        return False
    if len(tokens) == 1:
        if not text.isalpha():
            return False
        return _wordfreq_zipf_en(tokens[0]) >= _GENERAL_OUTPUT_SINGLE_TOKEN_GENERIC_ZIPF_MIN
    if len(tokens) == 2 and tokens[0] in _GENERAL_OUTPUT_ARTICLE_LEADS:
        return _wordfreq_zipf_en(tokens[1]) >= _GENERAL_OUTPUT_ARTICLE_TAIL_GENERIC_ZIPF_MIN
    return False


def _is_exact_general_output_noise_alias(entity: EntityMatch) -> bool:
    normalized_values = {" ".join(entity.text.casefold().split())}
    if entity.link is not None:
        normalized_values.add(" ".join(entity.link.canonical_text.casefold().split()))
    return any(value in _GENERAL_OUTPUT_EXACT_NOISE_TEXTS for value in normalized_values)


def _filter_final_output_entities(pack: str, entities: list[EntityMatch]) -> tuple[list[EntityMatch], int]:
    if pack != "general-en":
        return entities, 0
    kept: list[EntityMatch] = []
    removed = 0
    for entity in entities:
        if _is_exact_general_output_noise_alias(entity):
            removed += 1
            continue
        if entity.label == "organization" and (
            _is_generic_general_org_title_alias(entity.text)
            or _is_common_english_general_org_alias(entity)
        ):
            removed += 1
            continue
        kept.append(entity)
    return kept, removed


class TagMetrics(BaseModel):
    """Structured tagging metrics used for quality loops and warnings."""

    input_char_count: int
    normalized_char_count: int
    token_count: int
    segment_count: int = 0
    chunk_count: int = 0
    entity_count: int = 0
    entities_per_100_tokens: float = 0.0
    overlap_drop_count: int = 0
    per_label_counts: dict[str, int] = Field(default_factory=dict)
    per_lane_counts: dict[str, int] = Field(default_factory=dict)


class TagSpanDecision(BaseModel):
    """Optional debug record for overlap-resolution decisions."""

    text: str
    label: str
    start: int
    end: int
    lane: str
    kept: bool
    reason: str | None = None


class TagDebug(BaseModel):
    """Optional debug payload for extraction decisions."""

    kept_span_count: int = 0
    discarded_span_count: int = 0
    span_decisions: list[TagSpanDecision] = Field(default_factory=list)


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

    schema_version: int = 2
    version: str
    pack: str
    pack_version: str | None = None
    language: str
    content_type: str
    source_path: str | None = None
    input_size_bytes: int | None = None
    source_fingerprint: SourceFingerprint | None = None
    saved_output_path: str | None = None
    entities: list[EntityMatch] = Field(default_factory=list)
    related_entities: list[RelatedEntityMatch] = Field(default_factory=list)
    refinement_applied: bool = False
    refinement_strategy: str | None = None
    refinement_score: float | None = None
    refinement_debug: dict[str, Any] | None = None
    graph_support: GraphSupport | None = None
    topics: list[TopicMatch] = Field(default_factory=list)
    metrics: TagMetrics | None = None
    debug: TagDebug | None = None
    warnings: list[str] = Field(default_factory=list)
    total_time_ms: int | None = None
    timing_breakdown_ms: dict[str, int] | None = None
    timing_ms: int

    @model_validator(mode="after")
    def dedupe_linked_entities(self) -> "TagResponse":
        if self.total_time_ms is None:
            self.total_time_ms = self.timing_ms
        filtered_entities, filtered_count = _filter_final_output_entities(self.pack, self.entities)
        merged_entities, merged_count = _merge_duplicate_linked_entities(filtered_entities)
        if filtered_count == 0 and merged_count == 0:
            return self
        self.entities = merged_entities
        if self.metrics is not None:
            entity_count = len(merged_entities)
            per_label_counts = Counter(entity.label for entity in merged_entities)
            per_lane_counts = Counter(
                entity.provenance.lane
                for entity in merged_entities
                if entity.provenance is not None and entity.provenance.lane
            )
            self.metrics.entity_count = entity_count
            self.metrics.per_label_counts = dict(sorted(per_label_counts.items()))
            self.metrics.per_lane_counts = dict(sorted(per_lane_counts.items()))
            self.metrics.entities_per_100_tokens = (
                round((entity_count * 100 / self.metrics.token_count), 4)
                if self.metrics.token_count
                else 0.0
            )
        if len(self.topics) == 1:
            label_counts = Counter(entity.label for entity in merged_entities)
            self.topics[0].evidence_count = len(merged_entities)
            self.topics[0].entity_labels = [
                label
                for label, _ in sorted(label_counts.items(), key=lambda item: (-item[1], item[0]))
            ]
        return self


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
    canonical_text: str | None = None
    generated: bool | None = None
    source_name: str | None = None
    entity_id: str | None = None
    entity_prior: float | None = None
    source_priority: float | None = None
    popularity_weight: float | None = None
    score: float | None = None


class LookupResponse(BaseModel):
    """Response body for metadata lookup."""

    query: str
    pack_id: str | None = None
    exact_alias: bool = False
    fuzzy: bool = False
    active_only: bool = True
    candidates: list[LookupCandidate] = Field(default_factory=list)


class VectorIndexBuildResponse(BaseModel):
    """Stable result payload for one offline QID graph index build."""

    output_dir: str
    manifest_path: str
    artifact_path: str
    collection_name: str | None = None
    alias_name: str | None = None
    qdrant_url: str | None = None
    published: bool = False
    dimensions: int
    point_count: int
    target_entity_count: int
    bundle_count: int
    bundle_dirs: list[str] = Field(default_factory=list)
    pack_ids: list[str] = Field(default_factory=list)
    truthy_path: str
    processed_line_count: int
    matched_statement_count: int
    allowed_predicates: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class QidGraphStoreBuildRequest(BaseModel):
    """Request body for building one explicit QID graph store."""

    bundle_dirs: list[str] = Field(default_factory=list)
    truthy_path: str
    output_dir: str
    predicate: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_bundle_dirs(self) -> "QidGraphStoreBuildRequest":
        if not self.bundle_dirs:
            raise ValueError("At least one bundle directory is required.")
        return self


class QidGraphStoreBuildResponse(BaseModel):
    """Stable result payload for one offline explicit QID graph store build."""

    output_dir: str
    manifest_path: str
    artifact_path: str
    target_node_count: int
    stored_node_count: int
    stored_edge_count: int
    bundle_count: int
    bundle_dirs: list[str] = Field(default_factory=list)
    pack_ids: list[str] = Field(default_factory=list)
    truthy_path: str
    processed_line_count: int
    matched_statement_count: int
    allowed_predicates: list[str] = Field(default_factory=list)
    adjacent_node_count: int = 0
    predicate_histogram: dict[str, int] = Field(default_factory=dict)
    max_degree_total: int = 0
    mean_degree_total: float = 0.0
    p95_degree_total: int = 0
    warnings: list[str] = Field(default_factory=list)
