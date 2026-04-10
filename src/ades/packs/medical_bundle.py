"""Medical-domain source-bundle builders for real-content pack generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
from pathlib import Path
import shutil
from typing import Any


@dataclass(frozen=True)
class MedicalSourceBundleResult:
    """Summary of one generated `medical-en` normalized source bundle."""

    pack_id: str
    version: str
    output_dir: str
    bundle_dir: str
    bundle_manifest_path: str
    entities_path: str
    rules_path: str
    generated_at: str
    source_count: int
    entity_record_count: int
    rule_record_count: int
    disease_count: int
    gene_count: int
    protein_count: int
    clinical_trial_count: int
    curated_entity_count: int
    warnings: list[str] = field(default_factory=list)


def build_medical_source_bundle(
    *,
    disease_ontology_path: str | Path,
    hgnc_genes_path: str | Path,
    uniprot_proteins_path: str | Path,
    clinical_trials_path: str | Path,
    curated_entities_path: str | Path,
    output_dir: str | Path,
    version: str = "0.2.0",
) -> MedicalSourceBundleResult:
    """Build a normalized `medical-en` source bundle from raw snapshot files."""

    disease_path = _resolve_input_file(
        disease_ontology_path,
        label="Disease ontology snapshot",
    )
    hgnc_path = _resolve_input_file(
        hgnc_genes_path,
        label="HGNC gene snapshot",
    )
    uniprot_path = _resolve_input_file(
        uniprot_proteins_path,
        label="UniProt protein snapshot",
    )
    trials_path = _resolve_input_file(
        clinical_trials_path,
        label="ClinicalTrials.gov snapshot",
    )
    curated_path = _resolve_input_file(
        curated_entities_path,
        label="curated medical entities snapshot",
    )
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    bundle_dir = resolved_output_dir / "medical-en-bundle"
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    diseases = _load_disease_ontology_entities(disease_path)
    genes = _load_hgnc_gene_entities(hgnc_path)
    proteins = _load_uniprot_protein_entities(uniprot_path)
    clinical_trials = _load_clinical_trial_entities(trials_path)
    curated_entities = _load_curated_medical_entities(curated_path)
    rules = _build_medical_rule_records()

    entities = sorted(
        [*clinical_trials, *curated_entities, *diseases, *genes, *proteins],
        key=lambda item: (
            str(item["entity_type"]).casefold(),
            str(item["canonical_text"]).casefold(),
            str(item["entity_id"]).casefold(),
        ),
    )

    entities_path = normalized_dir / "entities.jsonl"
    rules_path = normalized_dir / "rules.jsonl"
    bundle_manifest_path = bundle_dir / "bundle.json"
    generated_at = _utc_timestamp()

    _write_jsonl(entities_path, entities)
    _write_jsonl(rules_path, rules)

    sources = [
        {
            "name": "disease-ontology",
            "snapshot_uri": disease_path.as_uri(),
            "license_class": "ship-now",
            "license": "operator-supplied",
            "retrieved_at": _path_timestamp(disease_path),
            "record_count": len(diseases),
            "notes": "adapter=disease_ontology_terms",
        },
        {
            "name": "hgnc-genes",
            "snapshot_uri": hgnc_path.as_uri(),
            "license_class": "ship-now",
            "license": "operator-supplied",
            "retrieved_at": _path_timestamp(hgnc_path),
            "record_count": len(genes),
            "notes": "adapter=hgnc_complete_set",
        },
        {
            "name": "uniprot-proteins",
            "snapshot_uri": uniprot_path.as_uri(),
            "license_class": "ship-now",
            "license": "operator-supplied",
            "retrieved_at": _path_timestamp(uniprot_path),
            "record_count": len(proteins),
            "notes": "adapter=uniprot_reviewed_proteins",
        },
        {
            "name": "clinical-trials",
            "snapshot_uri": trials_path.as_uri(),
            "license_class": "ship-now",
            "license": "operator-supplied",
            "retrieved_at": _path_timestamp(trials_path),
            "record_count": len(clinical_trials),
            "notes": "adapter=clinical_trials_json",
        },
        {
            "name": "curated-medical-entities",
            "snapshot_uri": curated_path.as_uri(),
            "license_class": "ship-now",
            "license": "operator-supplied",
            "retrieved_at": _path_timestamp(curated_path),
            "record_count": len(curated_entities),
            "notes": "adapter=curated_medical_entities",
        },
    ]
    bundle_manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": "medical-en",
                "version": version,
                "language": "en",
                "domain": "medical",
                "tier": "domain",
                "description": "Generated English medical entities and deterministic identifier rules.",
                "tags": ["medical", "generated", "english"],
                "dependencies": ["general-en"],
                "entities_path": "normalized/entities.jsonl",
                "rules_path": "normalized/rules.jsonl",
                "label_mappings": {
                    "clinical_trial": "clinical_trial",
                    "disease": "disease",
                    "drug": "drug",
                    "gene": "gene",
                    "protein": "protein",
                    "dosage": "dosage",
                },
                "stoplisted_aliases": ["N/A"],
                "allowed_ambiguous_aliases": [],
                "sources": sources,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    warnings: list[str] = []
    if not diseases:
        warnings.append("Disease ontology snapshot produced no disease entities.")
    if not genes:
        warnings.append("HGNC snapshot produced no gene entities.")
    if not proteins:
        warnings.append("UniProt snapshot produced no protein entities.")
    if not clinical_trials:
        warnings.append("Clinical trial snapshot produced no clinical trial entities.")
    if not curated_entities:
        warnings.append("Curated entity snapshot produced no medical entities.")

    return MedicalSourceBundleResult(
        pack_id="medical-en",
        version=version,
        output_dir=str(resolved_output_dir),
        bundle_dir=str(bundle_dir),
        bundle_manifest_path=str(bundle_manifest_path),
        entities_path=str(entities_path),
        rules_path=str(rules_path),
        generated_at=generated_at,
        source_count=len(sources),
        entity_record_count=len(entities),
        rule_record_count=len(rules),
        disease_count=len(diseases),
        gene_count=len(genes),
        protein_count=len(proteins),
        clinical_trial_count=len(clinical_trials),
        curated_entity_count=len(curated_entities),
        warnings=warnings,
    )


def _load_disease_ontology_entities(path: Path) -> list[dict[str, Any]]:
    records = _read_json_records(path)
    normalized: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        canonical_text = _clean_text(
            item.get("canonical_text") or item.get("name") or item.get("label")
        )
        if not canonical_text:
            continue
        aliases = _coerce_alias_list(item.get("aliases") or item.get("synonyms"))
        source_id = (
            _clean_text(item.get("entity_id") or item.get("id") or item.get("doid"))
            or canonical_text.casefold()
        )
        normalized.append(
            {
                "entity_id": f"disease-ontology:{source_id}",
                "entity_type": "disease",
                "canonical_text": canonical_text,
                "aliases": aliases,
                "source_name": "disease-ontology",
                "source_id": source_id,
            }
        )
    return normalized


def _load_hgnc_gene_entities(path: Path) -> list[dict[str, Any]]:
    records = _read_json_records(path)
    normalized: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        canonical_text = _clean_text(
            item.get("canonical_text") or item.get("symbol") or item.get("name")
        )
        if not canonical_text:
            continue
        aliases = _coerce_alias_list(item.get("aliases") or item.get("alias_symbol"))
        source_id = (
            _clean_text(item.get("entity_id") or item.get("hgnc_id") or item.get("id"))
            or canonical_text
        )
        normalized.append(
            {
                "entity_id": f"hgnc:{source_id}",
                "entity_type": "gene",
                "canonical_text": canonical_text,
                "aliases": aliases,
                "source_name": "hgnc-genes",
                "source_id": source_id,
            }
        )
    return normalized


def _load_uniprot_protein_entities(path: Path) -> list[dict[str, Any]]:
    records = _read_json_records(path)
    normalized: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        canonical_text = _clean_text(
            item.get("canonical_text")
            or item.get("recommended_name")
            or item.get("name")
        )
        if not canonical_text:
            continue
        aliases = _coerce_alias_list(item.get("aliases") or item.get("synonyms"))
        source_id = (
            _clean_text(item.get("entity_id") or item.get("accession") or item.get("id"))
            or canonical_text.casefold()
        )
        normalized.append(
            {
                "entity_id": f"uniprot:{source_id}",
                "entity_type": "protein",
                "canonical_text": canonical_text,
                "aliases": aliases,
                "source_name": "uniprot-proteins",
                "source_id": source_id,
            }
        )
    return normalized


def _load_clinical_trial_entities(path: Path) -> list[dict[str, Any]]:
    records = _read_json_records(path)
    normalized: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        trial_id = _clean_text(item.get("nct_id") or item.get("id"))
        title = _clean_text(item.get("brief_title") or item.get("title") or trial_id)
        if not trial_id or not title:
            continue
        aliases = [trial_id]
        normalized.append(
            {
                "entity_id": f"clinical-trial:{trial_id}",
                "entity_type": "clinical_trial",
                "canonical_text": title,
                "aliases": aliases,
                "source_name": "clinical-trials",
                "source_id": trial_id,
                "metadata": {
                    "conditions": _coerce_alias_list(item.get("conditions")),
                    "interventions": _coerce_alias_list(item.get("interventions")),
                },
            }
        )
    return normalized


def _load_curated_medical_entities(path: Path) -> list[dict[str, Any]]:
    records = _read_json_records(path)
    normalized: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        canonical_text = _clean_text(item.get("canonical_text") or item.get("name"))
        entity_type = _clean_text(item.get("entity_type") or item.get("type"))
        if not canonical_text or not entity_type:
            continue
        normalized.append(
            {
                "entity_id": str(
                    item.get("entity_id")
                    or f"curated-medical:{entity_type.casefold()}:{_slugify(canonical_text)}"
                ),
                "entity_type": entity_type,
                "canonical_text": canonical_text,
                "aliases": _coerce_alias_list(item.get("aliases")),
                "source_name": str(item.get("source_name") or "curated-medical-entities"),
                "source_id": str(item.get("source_id") or canonical_text),
            }
        )
    return normalized


def _build_medical_rule_records() -> list[dict[str, str]]:
    return [
        {
            "name": "clinical_trial_id",
            "label": "clinical_trial",
            "kind": "regex",
            "pattern": r"NCT[0-9]{8}",
        },
        {
            "name": "dosage",
            "label": "dosage",
            "kind": "regex",
            "pattern": r"[0-9]+\s?(mg|ml|mcg)",
        },
    ]


def _read_json_records(path: Path) -> list[Any]:
    if path.suffix.casefold() == ".jsonl":
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        for key in ("entities", "terms", "genes", "proteins", "studies"):
            if key in payload and isinstance(payload[key], list):
                return payload[key]
        return list(payload.values())
    if isinstance(payload, list):
        return payload
    raise ValueError(f"Unsupported medical snapshot structure: {path}")


def _coerce_alias_list(value: Any) -> list[str]:
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


def _resolve_input_file(path: str | Path, *, label: str) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"{label} not found: {resolved}")
    if not resolved.is_file():
        raise IsADirectoryError(f"{label} is not a file: {resolved}")
    return resolved


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


def _slugify(value: str) -> str:
    return "".join(character.lower() if character.isalnum() else "-" for character in value).strip("-")


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _path_timestamp(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat().replace("+00:00", "Z")
