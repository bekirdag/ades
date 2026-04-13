"""Medical-source snapshot fetchers for real-content pack generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from hashlib import sha256
import io
import json
from pathlib import Path
import re
import shutil
import urllib.request
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
import zipfile

from .fetch import normalize_source_url


DEFAULT_MEDICAL_SOURCE_OUTPUT_ROOT = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/raw/medical-en"
)
DEFAULT_DISEASE_ONTOLOGY_URL = (
    "https://raw.githubusercontent.com/DiseaseOntology/"
    "HumanDiseaseOntology/main/src/ontology/doid.obo"
)
DEFAULT_HGNC_GENES_URL = (
    "https://storage.googleapis.com/public-download-files/hgnc/json/json/"
    "hgnc_complete_set.json"
)
DEFAULT_UNIPROT_PROTEINS_URL = (
    "https://rest.uniprot.org/uniprotkb/search?"
    "query=%28reviewed%3Atrue%20AND%20organism_id%3A9606%29&"
    "format=json&fields=accession%2Cprotein_name%2Cgene_names&size=500"
)
DEFAULT_CLINICAL_TRIALS_URL = (
    "https://clinicaltrials.gov/api/v2/studies?format=json"
)
DEFAULT_ORANGE_BOOK_URL = "https://www.fda.gov/media/76860/download?attachment"
DEFAULT_SOURCE_FETCH_USER_AGENT = "ades/0.1.0 (ops@adestool.com)"
DEFAULT_CLINICAL_TRIALS_FIELDS: tuple[str, ...] = (
    "protocolSection.identificationModule",
    "protocolSection.conditionsModule",
    "protocolSection.armsInterventionsModule",
)
DEFAULT_UNIPROT_MAX_RECORDS = 0
DEFAULT_CLINICAL_TRIALS_MAX_RECORDS = 0
DEFAULT_CURATED_MEDICAL_ENTITIES: tuple[dict[str, object], ...] = ()
_DO_TERM_START_RE = re.compile(r"^\[Term\]\s*$")
_DO_KEY_VALUE_RE = re.compile(r"^([^:]+):\s*(.*)$")
_DO_SYNONYM_RE = re.compile(r'^"([^"]+)"')
_NEXT_LINK_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


@dataclass(frozen=True)
class MedicalSourceFetchResult:
    """Summary of one downloaded medical source snapshot set."""

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
    generated_at: str
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
    warnings: list[str] = field(default_factory=list)


def fetch_medical_source_snapshot(
    *,
    output_dir: str | Path = DEFAULT_MEDICAL_SOURCE_OUTPUT_ROOT,
    snapshot: str | None = None,
    disease_ontology_url: str = DEFAULT_DISEASE_ONTOLOGY_URL,
    hgnc_genes_url: str = DEFAULT_HGNC_GENES_URL,
    uniprot_proteins_url: str = DEFAULT_UNIPROT_PROTEINS_URL,
    clinical_trials_url: str = DEFAULT_CLINICAL_TRIALS_URL,
    orange_book_url: str = DEFAULT_ORANGE_BOOK_URL,
    user_agent: str = DEFAULT_SOURCE_FETCH_USER_AGENT,
    uniprot_max_records: int = DEFAULT_UNIPROT_MAX_RECORDS,
    clinical_trials_max_records: int = DEFAULT_CLINICAL_TRIALS_MAX_RECORDS,
) -> MedicalSourceFetchResult:
    """Download one immutable medical source snapshot set into the big-data root."""

    if uniprot_max_records < 0:
        raise ValueError("uniprot_max_records must be greater than or equal to zero.")
    if clinical_trials_max_records < 0:
        raise ValueError(
            "clinical_trials_max_records must be greater than or equal to zero."
        )

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_snapshot = _resolve_snapshot(snapshot)
    snapshot_dir = resolved_output_dir / resolved_snapshot
    if snapshot_dir.exists():
        raise FileExistsError(
            f"Medical source snapshot directory already exists: {snapshot_dir}"
        )
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    generated_at = _utc_timestamp()
    warnings: list[str] = []
    try:
        disease_url, disease_raw_path, disease_terms_path, disease_count = (
            _download_disease_ontology_snapshot(
                disease_ontology_url,
                snapshot_dir=snapshot_dir,
                user_agent=user_agent,
            )
        )
        hgnc_url, hgnc_raw_path, hgnc_genes_path, gene_count = (
            _download_hgnc_snapshot(
                hgnc_genes_url,
                snapshot_dir=snapshot_dir,
                user_agent=user_agent,
            )
        )
        uniprot_url, uniprot_raw_path, uniprot_proteins_path, protein_count = (
            _download_uniprot_snapshot(
                uniprot_proteins_url,
                snapshot_dir=snapshot_dir,
                user_agent=user_agent,
                max_records=uniprot_max_records,
            )
        )
        (
            trials_url,
            trials_raw_path,
            clinical_trials_path,
            clinical_trial_count,
        ) = _download_clinical_trials_snapshot(
            clinical_trials_url,
            snapshot_dir=snapshot_dir,
            user_agent=user_agent,
            max_records=clinical_trials_max_records,
        )
        (
            resolved_orange_book_url,
            orange_book_raw_path,
            orange_book_products_path,
            orange_book_product_count,
        ) = _download_orange_book_snapshot(
            orange_book_url,
            snapshot_dir=snapshot_dir,
            user_agent=user_agent,
        )
        curated_entities_path = snapshot_dir / "curated_medical_entities.json"
        _write_curated_entities(curated_entities_path)
        _write_source_manifest(
            path=snapshot_dir / "sources.fetch.json",
            snapshot=resolved_snapshot,
            generated_at=generated_at,
            user_agent=user_agent,
            disease_ontology_url=disease_url,
            hgnc_genes_url=hgnc_url,
            uniprot_proteins_url=uniprot_url,
            clinical_trials_url=trials_url,
            orange_book_url=resolved_orange_book_url,
            disease_raw_path=disease_raw_path,
            hgnc_raw_path=hgnc_raw_path,
            uniprot_raw_path=uniprot_raw_path,
            clinical_trials_raw_path=trials_raw_path,
            orange_book_raw_path=orange_book_raw_path,
            disease_terms_path=disease_terms_path,
            hgnc_genes_path=hgnc_genes_path,
            uniprot_proteins_path=uniprot_proteins_path,
            clinical_trials_path=clinical_trials_path,
            orange_book_products_path=orange_book_products_path,
            curated_entities_path=curated_entities_path,
            disease_count=disease_count,
            gene_count=gene_count,
            protein_count=protein_count,
            clinical_trial_count=clinical_trial_count,
            orange_book_product_count=orange_book_product_count,
        )
    except Exception:
        shutil.rmtree(snapshot_dir, ignore_errors=True)
        raise

    manifest_path = snapshot_dir / "sources.fetch.json"
    return MedicalSourceFetchResult(
        pack_id="medical-en",
        output_dir=str(resolved_output_dir),
        snapshot=resolved_snapshot,
        snapshot_dir=str(snapshot_dir),
        disease_ontology_url=disease_url,
        hgnc_genes_url=hgnc_url,
        uniprot_proteins_url=uniprot_url,
        clinical_trials_url=trials_url,
        orange_book_url=resolved_orange_book_url,
        source_manifest_path=str(manifest_path),
        disease_ontology_path=str(disease_terms_path),
        hgnc_genes_path=str(hgnc_genes_path),
        uniprot_proteins_path=str(uniprot_proteins_path),
        clinical_trials_path=str(clinical_trials_path),
        orange_book_path=str(orange_book_products_path),
        curated_entities_path=str(curated_entities_path),
        generated_at=generated_at,
        source_count=6,
        disease_count=disease_count,
        gene_count=gene_count,
        protein_count=protein_count,
        clinical_trial_count=clinical_trial_count,
        orange_book_product_count=orange_book_product_count,
        curated_entity_count=len(DEFAULT_CURATED_MEDICAL_ENTITIES),
        disease_ontology_sha256=_sha256_file(disease_terms_path),
        hgnc_genes_sha256=_sha256_file(hgnc_genes_path),
        uniprot_proteins_sha256=_sha256_file(uniprot_proteins_path),
        clinical_trials_sha256=_sha256_file(clinical_trials_path),
        orange_book_sha256=_sha256_file(orange_book_products_path),
        curated_entities_sha256=_sha256_file(curated_entities_path),
        warnings=warnings,
    )


def _download_disease_ontology_snapshot(
    source_url: str | Path,
    *,
    snapshot_dir: Path,
    user_agent: str,
) -> tuple[str, Path, Path, int]:
    normalized_source_url = normalize_source_url(source_url)
    raw_bytes = _read_bytes(normalized_source_url, user_agent=user_agent)
    raw_path = snapshot_dir / "disease_ontology.source.obo"
    raw_path.write_bytes(raw_bytes)
    terms = _parse_disease_ontology_obo(raw_bytes.decode("utf-8"))
    formatted_path = snapshot_dir / "disease_ontology_terms.json"
    formatted_path.write_text(
        json.dumps({"terms": terms}, indent=2) + "\n",
        encoding="utf-8",
    )
    return normalized_source_url, raw_path, formatted_path, len(terms)


def _download_hgnc_snapshot(
    source_url: str | Path,
    *,
    snapshot_dir: Path,
    user_agent: str,
) -> tuple[str, Path, Path, int]:
    normalized_source_url = normalize_source_url(source_url)
    raw_bytes = _read_bytes(normalized_source_url, user_agent=user_agent)
    raw_path = snapshot_dir / "hgnc_complete_set.source.json"
    raw_path.write_bytes(raw_bytes)
    payload = json.loads(raw_bytes.decode("utf-8"))
    docs = payload.get("response", {}).get("docs", [])
    genes: list[dict[str, object]] = []
    for item in docs:
        if not isinstance(item, dict):
            continue
        if _clean_text(item.get("status")) not in {"Approved", ""}:
            continue
        symbol = _clean_text(item.get("symbol"))
        if not symbol:
            continue
        aliases = [
            alias
            for alias in (
                _coerce_alias_list(item.get("alias_symbol"))
                + _coerce_alias_list(item.get("prev_symbol"))
            )
            if alias
        ]
        genes.append(
            {
                "hgnc_id": _clean_text(item.get("hgnc_id")) or symbol,
                "symbol": symbol,
                "aliases": _dedupe_preserving_order(aliases),
            }
        )
    formatted_path = snapshot_dir / "hgnc_genes.json"
    formatted_path.write_text(
        json.dumps({"genes": genes}, indent=2) + "\n",
        encoding="utf-8",
    )
    return normalized_source_url, raw_path, formatted_path, len(genes)


def _download_uniprot_snapshot(
    source_url: str | Path,
    *,
    snapshot_dir: Path,
    user_agent: str,
    max_records: int,
) -> tuple[str, Path, Path, int]:
    normalized_source_url = normalize_source_url(source_url)
    next_url = _ensure_uniprot_page_size(normalized_source_url)
    collected_results: list[dict[str, object]] = []
    visited_urls: list[str] = []
    while next_url and not _record_limit_reached(
        collected_count=len(collected_results),
        max_records=max_records,
    ):
        visited_urls.append(next_url)
        payload, response_headers = _read_json_response(next_url, user_agent=user_agent)
        page_results = payload.get("results", [])
        if not isinstance(page_results, list):
            raise ValueError("UniProt payload does not contain a results list.")
        remaining = None if max_records == 0 else max_records - len(collected_results)
        collected_results.extend(page_results if remaining is None else page_results[:remaining])
        next_url = _extract_next_link(response_headers.get("Link"))

    raw_path = snapshot_dir / "uniprot_proteins.source.json"
    raw_path.write_text(
        json.dumps({"results": collected_results, "fetched_urls": visited_urls}, indent=2)
        + "\n",
        encoding="utf-8",
    )

    proteins: list[dict[str, object]] = []
    for item in collected_results:
        if not isinstance(item, dict):
            continue
        accession = _clean_text(item.get("primaryAccession"))
        recommended_name = _extract_uniprot_recommended_name(item)
        if not accession or not recommended_name:
            continue
        aliases = _extract_uniprot_aliases(item)
        proteins.append(
            {
                "accession": accession,
                "recommended_name": recommended_name,
                "aliases": _dedupe_preserving_order(aliases),
            }
        )

    formatted_path = snapshot_dir / "uniprot_proteins.json"
    formatted_path.write_text(
        json.dumps({"proteins": proteins}, indent=2) + "\n",
        encoding="utf-8",
    )
    return normalized_source_url, raw_path, formatted_path, len(proteins)


def _download_clinical_trials_snapshot(
    source_url: str | Path,
    *,
    snapshot_dir: Path,
    user_agent: str,
    max_records: int,
) -> tuple[str, Path, Path, int]:
    normalized_source_url = normalize_source_url(source_url)
    next_url = _ensure_clinical_trials_page_size(
        _ensure_clinical_trials_fields(normalized_source_url)
    )
    studies: list[dict[str, object]] = []
    page_token: str | None = None
    fetched_urls: list[str] = []

    while next_url and not _record_limit_reached(
        collected_count=len(studies),
        max_records=max_records,
    ):
        paged_url = _append_query_params(next_url, {"pageToken": page_token} if page_token else {})
        fetched_urls.append(paged_url)
        payload, _headers = _read_json_response(paged_url, user_agent=user_agent)
        page_studies = payload.get("studies", [])
        if not isinstance(page_studies, list):
            raise ValueError("ClinicalTrials.gov payload does not contain a studies list.")
        remaining = None if max_records == 0 else max_records - len(studies)
        studies.extend(page_studies if remaining is None else page_studies[:remaining])
        page_token = payload.get("nextPageToken")
        if not page_token or _record_limit_reached(
            collected_count=len(studies),
            max_records=max_records,
        ):
            break

    raw_path = snapshot_dir / "clinical_trials.source.json"
    raw_path.write_text(
        json.dumps({"studies": studies, "fetched_urls": fetched_urls}, indent=2) + "\n",
        encoding="utf-8",
    )

    normalized_studies: list[dict[str, object]] = []
    for item in studies:
        if not isinstance(item, dict):
            continue
        protocol = item.get("protocolSection") or {}
        identification = protocol.get("identificationModule") or {}
        conditions_module = protocol.get("conditionsModule") or {}
        arms_module = protocol.get("armsInterventionsModule") or {}
        nct_id = _clean_text(identification.get("nctId"))
        brief_title = _clean_text(identification.get("briefTitle"))
        if not nct_id or not brief_title:
            continue
        interventions: list[str] = []
        for intervention in arms_module.get("interventions", []) or []:
            if not isinstance(intervention, dict):
                continue
            name = _clean_text(intervention.get("name"))
            if name:
                interventions.append(name)
        normalized_studies.append(
            {
                "nct_id": nct_id,
                "brief_title": brief_title,
                "conditions": _coerce_alias_list(conditions_module.get("conditions")),
                "interventions": _dedupe_preserving_order(interventions),
            }
        )

    formatted_path = snapshot_dir / "clinical_trials.json"
    formatted_path.write_text(
        json.dumps({"studies": normalized_studies}, indent=2) + "\n",
        encoding="utf-8",
    )
    return normalized_source_url, raw_path, formatted_path, len(normalized_studies)


def _download_orange_book_snapshot(
    source_url: str | Path,
    *,
    snapshot_dir: Path,
    user_agent: str,
) -> tuple[str, Path, Path, int]:
    normalized_source_url = normalize_source_url(source_url)
    raw_bytes = _read_bytes(normalized_source_url, user_agent=user_agent)
    raw_suffix = ".zip" if zipfile.is_zipfile(io.BytesIO(raw_bytes)) else ".txt"
    raw_path = snapshot_dir / f"orange_book.source{raw_suffix}"
    raw_path.write_bytes(raw_bytes)
    products_text = _extract_orange_book_products_text(raw_bytes)
    products = _parse_orange_book_products(products_text)
    formatted_path = snapshot_dir / "orange_book_products.json"
    formatted_path.write_text(
        json.dumps({"products": products}, indent=2) + "\n",
        encoding="utf-8",
    )
    return normalized_source_url, raw_path, formatted_path, len(products)


def _write_curated_entities(path: Path) -> None:
    path.write_text(
        json.dumps({"entities": list(DEFAULT_CURATED_MEDICAL_ENTITIES)}, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_source_manifest(
    *,
    path: Path,
    snapshot: str,
    generated_at: str,
    user_agent: str,
    disease_ontology_url: str,
    hgnc_genes_url: str,
    uniprot_proteins_url: str,
    clinical_trials_url: str,
    orange_book_url: str,
    disease_raw_path: Path,
    hgnc_raw_path: Path,
    uniprot_raw_path: Path,
    clinical_trials_raw_path: Path,
    orange_book_raw_path: Path,
    disease_terms_path: Path,
    hgnc_genes_path: Path,
    uniprot_proteins_path: Path,
    clinical_trials_path: Path,
    orange_book_products_path: Path,
    curated_entities_path: Path,
    disease_count: int,
    gene_count: int,
    protein_count: int,
    clinical_trial_count: int,
    orange_book_product_count: int,
) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": "medical-en",
                "snapshot": snapshot,
                "generated_at": generated_at,
                "user_agent": user_agent,
                "sources": [
                    _source_manifest_entry(
                        name="disease-ontology",
                        source_url=disease_ontology_url,
                        raw_path=disease_raw_path,
                        formatted_path=disease_terms_path,
                        record_count=disease_count,
                    ),
                    _source_manifest_entry(
                        name="hgnc-genes",
                        source_url=hgnc_genes_url,
                        raw_path=hgnc_raw_path,
                        formatted_path=hgnc_genes_path,
                        record_count=gene_count,
                    ),
                    _source_manifest_entry(
                        name="uniprot-proteins",
                        source_url=uniprot_proteins_url,
                        raw_path=uniprot_raw_path,
                        formatted_path=uniprot_proteins_path,
                        record_count=protein_count,
                    ),
                    _source_manifest_entry(
                        name="clinical-trials",
                        source_url=clinical_trials_url,
                        raw_path=clinical_trials_raw_path,
                        formatted_path=clinical_trials_path,
                        record_count=clinical_trial_count,
                    ),
                    _source_manifest_entry(
                        name="orange-book",
                        source_url=orange_book_url,
                        raw_path=orange_book_raw_path,
                        formatted_path=orange_book_products_path,
                        record_count=orange_book_product_count,
                    ),
                    {
                        "name": "curated-medical-entities",
                        "source_url": "operator://ades/curated-medical-entities",
                        "path": curated_entities_path.name,
                        "sha256": _sha256_file(curated_entities_path),
                        "size_bytes": curated_entities_path.stat().st_size,
                        "record_count": len(DEFAULT_CURATED_MEDICAL_ENTITIES),
                    },
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _source_manifest_entry(
    *,
    name: str,
    source_url: str,
    raw_path: Path,
    formatted_path: Path,
    record_count: int,
) -> dict[str, object]:
    return {
        "name": name,
        "source_url": source_url,
        "path": formatted_path.name,
        "sha256": _sha256_file(formatted_path),
        "size_bytes": formatted_path.stat().st_size,
        "record_count": record_count,
        "raw_path": raw_path.name,
        "raw_sha256": _sha256_file(raw_path),
        "raw_size_bytes": raw_path.stat().st_size,
    }


def _parse_disease_ontology_obo(text: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if _DO_TERM_START_RE.match(stripped):
            if current is not None:
                _append_disease_term(records, current)
            current = {"synonyms": []}
            continue
        if current is None or not stripped:
            continue
        match = _DO_KEY_VALUE_RE.match(stripped)
        if not match:
            continue
        key, value = match.group(1), match.group(2)
        if key == "synonym":
            synonym_match = _DO_SYNONYM_RE.match(value)
            if synonym_match:
                current.setdefault("synonyms", []).append(synonym_match.group(1))
            continue
        current[key] = value
    if current is not None:
        _append_disease_term(records, current)
    return records


def _append_disease_term(records: list[dict[str, object]], term: dict[str, object]) -> None:
    if _clean_text(term.get("is_obsolete")).casefold() == "true":
        return
    source_id = _clean_text(term.get("id"))
    name = _clean_text(term.get("name"))
    if not source_id or not name or not source_id.startswith("DOID:"):
        return
    records.append(
        {
            "id": source_id,
            "name": name,
            "synonyms": _dedupe_preserving_order(_coerce_alias_list(term.get("synonyms"))),
        }
    )


def _extract_orange_book_products_text(raw_bytes: bytes) -> str:
    archive_buffer = io.BytesIO(raw_bytes)
    if zipfile.is_zipfile(archive_buffer):
        archive_buffer.seek(0)
        with zipfile.ZipFile(archive_buffer) as archive:
            for name in archive.namelist():
                if Path(name).name.casefold() != "products.txt":
                    continue
                with archive.open(name) as handle:
                    return handle.read().decode("utf-8-sig")
        raise ValueError("Orange Book archive does not contain products.txt.")
    return raw_bytes.decode("utf-8-sig")


def _parse_orange_book_products(text: str) -> list[dict[str, object]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return []
    header = [_normalize_orange_book_header(value) for value in lines[0].split("~")]
    products: list[dict[str, object]] = []
    for line in lines[1:]:
        columns = line.split("~")
        if len(columns) < len(header):
            columns.extend([""] * (len(header) - len(columns)))
        row = {
            header[index]: _clean_text(columns[index]) if index < len(columns) else ""
            for index in range(len(header))
        }
        trade_name = row.get("trade_name", "")
        ingredient = row.get("ingredient", "")
        canonical_text = trade_name or ingredient
        if not canonical_text:
            continue
        application_number = row.get("appl_no", "")
        product_number = row.get("product_no", "")
        application_type = row.get("appl_type", "")
        source_id = _build_orange_book_source_id(
            application_type=application_type,
            application_number=application_number,
            product_number=product_number,
            fallback_name=canonical_text,
        )
        ingredient_aliases = [
            _clean_text(part)
            for part in ingredient.split(";")
            if _clean_text(part)
        ]
        aliases = _dedupe_preserving_order(
            [
                alias
                for alias in ingredient_aliases
                if alias and alias.casefold() != canonical_text.casefold()
            ]
        )
        products.append(
            {
                "entity_id": source_id,
                "trade_name": trade_name,
                "ingredient": ingredient,
                "aliases": aliases,
                "application_type": application_type,
                "application_number": application_number,
                "product_number": product_number,
                "strength": row.get("strength", ""),
                "applicant": row.get("applicant_full_name") or row.get("applicant", ""),
                "approval_date": row.get("approval_date", ""),
            }
        )
    return products


def _normalize_orange_book_header(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", value.strip()).strip("_").casefold()
    return normalized


def _build_orange_book_source_id(
    *,
    application_type: str,
    application_number: str,
    product_number: str,
    fallback_name: str,
) -> str:
    components = [
        component
        for component in (application_type, application_number, product_number)
        if component
    ]
    if components:
        return ":".join(components)
    return fallback_name.casefold()


def _extract_uniprot_recommended_name(item: dict[str, object]) -> str:
    description = item.get("proteinDescription")
    if not isinstance(description, dict):
        return ""
    recommended = description.get("recommendedName")
    if not isinstance(recommended, dict):
        return ""
    full_name = recommended.get("fullName")
    if not isinstance(full_name, dict):
        return ""
    return _clean_text(full_name.get("value"))


def _extract_uniprot_aliases(item: dict[str, object]) -> list[str]:
    aliases: list[str] = []
    description = item.get("proteinDescription")
    if isinstance(description, dict):
        recommended = description.get("recommendedName")
        if isinstance(recommended, dict):
            short_names = recommended.get("shortNames") or []
            for short_name in short_names:
                if isinstance(short_name, dict):
                    aliases.append(_clean_text(short_name.get("value")))
        for alt_name in description.get("alternativeNames", []) or []:
            if not isinstance(alt_name, dict):
                continue
            full_name = alt_name.get("fullName")
            if isinstance(full_name, dict):
                aliases.append(_clean_text(full_name.get("value")))
            for short_name in alt_name.get("shortNames", []) or []:
                if isinstance(short_name, dict):
                    aliases.append(_clean_text(short_name.get("value")))
    for gene in item.get("genes", []) or []:
        if not isinstance(gene, dict):
            continue
        gene_name = gene.get("geneName")
        if isinstance(gene_name, dict):
            aliases.append(_clean_text(gene_name.get("value")))
        for synonym in gene.get("synonyms", []) or []:
            if isinstance(synonym, dict):
                aliases.append(_clean_text(synonym.get("value")))
    return [alias for alias in aliases if alias]


def _ensure_uniprot_page_size(source_url: str) -> str:
    return _append_query_params(source_url, {"size": "500"})


def _ensure_clinical_trials_fields(source_url: str) -> str:
    return _append_query_params(
        source_url,
        {"fields": ",".join(DEFAULT_CLINICAL_TRIALS_FIELDS)},
    )


def _ensure_clinical_trials_page_size(source_url: str) -> str:
    return _append_query_params(source_url, {"pageSize": "200", "format": "json"})


def _append_query_params(source_url: str, params: dict[str, str]) -> str:
    parsed = urlparse(source_url)
    if parsed.scheme not in {"http", "https"}:
        return source_url
    query_items = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in params.items():
        if key not in query_items:
            query_items[key] = value
    return urlunparse(parsed._replace(query=urlencode(query_items)))


def _record_limit_reached(*, collected_count: int, max_records: int) -> bool:
    return max_records > 0 and collected_count >= max_records


def _read_json_response(
    source_url: str,
    *,
    user_agent: str,
) -> tuple[dict[str, object], dict[str, str]]:
    payload_bytes, headers = _read_response(source_url, user_agent=user_agent)
    payload = json.loads(payload_bytes.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object from source: {source_url}")
    return payload, headers


def _extract_next_link(link_header: str | None) -> str | None:
    if not link_header:
        return None
    match = _NEXT_LINK_RE.search(link_header)
    if not match:
        return None
    return match.group(1)


def _read_bytes(source_url: str, *, user_agent: str) -> bytes:
    payload, _headers = _read_response(source_url, user_agent=user_agent)
    return payload


def _read_response(
    source_url: str,
    *,
    user_agent: str,
) -> tuple[bytes, dict[str, str]]:
    parsed = urlparse(source_url)
    request: str | urllib.request.Request
    if parsed.scheme in {"http", "https"}:
        request = urllib.request.Request(
            source_url,
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json,text/plain;q=0.9,*/*;q=0.1",
            },
        )
    else:
        request = source_url
    with urllib.request.urlopen(request, timeout=120.0) as response:
        return response.read(), dict(response.headers.items())


def _coerce_alias_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = [value]
    aliases: list[str] = []
    for item in raw_items:
        cleaned = _clean_text(item)
        if cleaned:
            aliases.append(cleaned)
    return aliases


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


def _resolve_snapshot(snapshot: str | None) -> str:
    if snapshot is None:
        return date.today().isoformat()
    return date.fromisoformat(snapshot).isoformat()


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
