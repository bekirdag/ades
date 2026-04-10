import json
from pathlib import Path


def create_medical_raw_snapshots(root: Path) -> dict[str, Path]:
    raw_dir = root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    disease_path = raw_dir / "disease_ontology_terms.json"
    disease_path.write_text(
        json.dumps(
            {
                "terms": [
                    {
                        "id": "DOID:9352",
                        "name": "diabetes",
                        "synonyms": ["diabetes mellitus"],
                    },
                    {
                        "id": "DOID:8469",
                        "name": "influenza",
                        "synonyms": ["flu"],
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    hgnc_path = raw_dir / "hgnc_genes.json"
    hgnc_path.write_text(
        json.dumps(
            {
                "genes": [
                    {
                        "hgnc_id": "HGNC:1100",
                        "symbol": "BRCA1",
                        "aliases": ["RNF53"],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    uniprot_path = raw_dir / "uniprot_proteins.json"
    uniprot_path.write_text(
        json.dumps(
            {
                "proteins": [
                    {
                        "accession": "P04637",
                        "recommended_name": "p53 protein",
                        "aliases": ["cellular tumor antigen p53"],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    trials_path = raw_dir / "clinical_trials.json"
    trials_path.write_text(
        json.dumps(
            {
                "studies": [
                    {
                        "nct_id": "NCT04280705",
                        "brief_title": "Diabetes Aspirin Trial",
                        "conditions": ["diabetes"],
                        "interventions": ["Aspirin"],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    curated_path = raw_dir / "curated_medical_entities.json"
    curated_path.write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "entity_type": "drug",
                        "canonical_text": "Aspirin",
                        "aliases": ["acetylsalicylic acid"],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "disease_ontology": disease_path,
        "hgnc_genes": hgnc_path,
        "uniprot_proteins": uniprot_path,
        "clinical_trials": trials_path,
        "curated_entities": curated_path,
    }
