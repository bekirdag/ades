import json
from pathlib import Path
import zipfile


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
                        "name": "disease alpha",
                        "synonyms": ["disease alpha syndrome"],
                    },
                    {
                        "id": "DOID:8469",
                        "name": "disease beta",
                        "synonyms": ["disease beta alias"],
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
                        "symbol": "GENEA1",
                        "aliases": ["GENEA1ALT"],
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
                        "recommended_name": "Protein Alpha",
                        "aliases": ["Protein Alpha Long Form"],
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
                        "nct_id": "NCT00000001",
                        "brief_title": "Study Alpha Compound Trial",
                        "conditions": ["disease alpha"],
                        "interventions": ["Compound Alpha"],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    orange_book_path = raw_dir / "orange_book_products.json"
    orange_book_path.write_text(
        json.dumps(
            {
                "products": [
                    {
                        "entity_id": "NDA:020603:001",
                        "trade_name": "Brand Alpha",
                        "ingredient": "Ingredient Alpha",
                        "aliases": ["Ingredient Alpha"],
                        "application_type": "NDA",
                        "application_number": "020603",
                        "product_number": "001",
                        "strength": "500MG",
                        "applicant": "Sponsor Alpha Labs",
                        "approval_date": "1995-03-03",
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
                        "canonical_text": "Compound Alpha",
                        "aliases": ["Compound Alpha Acid"],
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
        "orange_book_products": orange_book_path,
        "curated_entities": curated_path,
    }


def create_medical_remote_sources(root: Path) -> dict[str, str]:
    remote_dir = root / "remote"
    remote_dir.mkdir(parents=True, exist_ok=True)

    disease_path = remote_dir / "disease_ontology.source.obo"
    disease_path.write_text(
        "\n".join(
            [
                "format-version: 1.2",
                "",
                "[Term]",
                "id: DOID:9352",
                "name: disease alpha",
                'synonym: "disease alpha syndrome" EXACT []',
                "",
                "[Term]",
                "id: DOID:8469",
                "name: disease beta",
                'synonym: "disease beta alias" EXACT []',
                "",
            ]
        ),
        encoding="utf-8",
    )

    hgnc_path = remote_dir / "hgnc_complete_set.source.json"
    hgnc_path.write_text(
        json.dumps(
            {
                "response": {
                    "docs": [
                        {
                            "status": "Approved",
                            "hgnc_id": "HGNC:1100",
                            "symbol": "GENEA1",
                            "alias_symbol": ["GENEA1ALT"],
                        }
                    ]
                }
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    uniprot_path = remote_dir / "uniprot_proteins.source.json"
    uniprot_path.write_text(
        json.dumps(
            {
                "results": [
                    {
                        "primaryAccession": "P04637",
                        "proteinDescription": {
                            "recommendedName": {
                                "fullName": {"value": "Protein Alpha"}
                            },
                            "alternativeNames": [
                                {
                                    "fullName": {
                                        "value": "Protein Alpha Long Form"
                                    }
                                }
                            ],
                        },
                        "genes": [
                            {
                                "geneName": {"value": "TP53"},
                            }
                        ],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    trials_path = remote_dir / "clinical_trials.source.json"
    trials_path.write_text(
        json.dumps(
            {
                "studies": [
                    {
                        "protocolSection": {
                            "identificationModule": {
                                "nctId": "NCT00000001",
                                "briefTitle": "Study Alpha Compound Trial",
                            },
                            "conditionsModule": {
                                "conditions": ["disease alpha"]
                            },
                            "armsInterventionsModule": {
                                "interventions": [{"name": "Compound Alpha"}]
                            },
                        }
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    orange_book_path = remote_dir / "orange_book.source.zip"
    with zipfile.ZipFile(orange_book_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "products.txt",
            "\n".join(
                [
                    "Ingredient~DF;Route~Trade_Name~Applicant~Strength~Appl_Type~Appl_No~Product_No~TE_Code~Approval_Date~RLD~RS~Type~Applicant_Full_Name",
                    "Ingredient Alpha~TABLET;ORAL~Brand Alpha~BMS~500MG~NDA~020603~001~~1995-03-03~Yes~Yes~RX~Sponsor Alpha Labs",
                ]
            )
            + "\n",
        )

    return {
        "disease_ontology_url": disease_path.as_uri(),
        "hgnc_genes_url": hgnc_path.as_uri(),
        "uniprot_proteins_url": uniprot_path.as_uri(),
        "clinical_trials_url": trials_path.as_uri(),
        "orange_book_url": orange_book_path.as_uri(),
    }
