import json
from pathlib import Path
import zipfile


def create_general_raw_snapshots(root: Path) -> dict[str, Path]:
    raw_dir = root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    wikidata_path = raw_dir / "wikidata_general_entities.json"
    wikidata_path.write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "id": "Q1",
                        "entity_type": "person",
                        "label": "Person Alpha",
                        "aliases": ["Person Alpha Variant"],
                    },
                    {
                        "id": "Q2",
                        "entity_type": "organization",
                        "label": "Org Alpha",
                        "aliases": ["Org Alpha Variant"],
                    },
                    {
                        "id": "Q3",
                        "entity_type": "person",
                        "label": "Person Beta",
                        "aliases": [],
                    },
                    {
                        "id": "Q4",
                        "entity_type": "person",
                        "label": "Person Gamma",
                        "aliases": [],
                    },
                    {
                        "id": "Q5",
                        "entity_type": "organization",
                        "label": "Org Delta",
                        "aliases": [],
                    },
                    {
                        "id": "Q6",
                        "entity_type": "organization",
                        "label": "Org Epsilon",
                        "aliases": [],
                    },
                    {
                        "id": "Q7",
                        "entity_type": "person",
                        "label": "Person Delta",
                        "aliases": [],
                    },
                    {
                        "id": "Q8",
                        "entity_type": "organization",
                        "label": "Org Zeta",
                        "aliases": ["Org Zeta Variant"],
                    },
                    {
                        "id": "Q9",
                        "entity_type": "person",
                        "label": "Person Epsilon",
                        "aliases": [
                            "Person Epsilon Variant One",
                            "Person Epsilon Variant Two",
                            "EpsilonSurname",
                            "Person Epsilon Variant Three",
                        ],
                    },
                    {
                        "id": "Q10",
                        "entity_type": "person",
                        "label": "Person Zeta",
                        "aliases": ["ZetaSurname", "Person Zeta Honorific"],
                    },
                    {
                        "id": "Q11",
                        "entity_type": "person",
                        "label": "Person Eta",
                        "aliases": ["Person Eta Variant"],
                    },
                    {
                        "id": "Q12",
                        "entity_type": "person",
                        "label": "Person Theta",
                        "aliases": ["Person Theta Variant"],
                    },
                    {
                        "id": "Q13",
                        "entity_type": "organization",
                        "label": "International Energy Agency",
                        "aliases": [],
                    },
                    {
                        "id": "Q14",
                        "entity_type": "location",
                        "label": "United States",
                        "aliases": ["the US", "U.S."],
                        "sitelinks": 1000,
                    },
                    {
                        "id": "Q15",
                        "entity_type": "location",
                        "label": "Europe",
                        "aliases": [],
                        "sitelinks": 1000,
                    },
                    {
                        "id": "Q16",
                        "entity_type": "organization",
                        "label": "Europe",
                        "aliases": [],
                        "sitelinks": 1,
                    },
                    {
                        "id": "Q17",
                        "entity_type": "location",
                        "label": "This",
                        "aliases": [],
                    },
                    {
                        "id": "Q18",
                        "entity_type": "location",
                        "label": "March",
                        "aliases": [],
                    },
                    {
                        "id": "Q19",
                        "entity_type": "location",
                        "label": "Stock",
                        "aliases": [],
                    },
                    {
                        "id": "Q20",
                        "entity_type": "location",
                        "label": "Talora",
                        "aliases": [],
                        "sitelinks": 120,
                    },
                    {
                        "id": "Q21",
                        "entity_type": "location",
                        "label": "Strait of Talora",
                        "aliases": [],
                        "sitelinks": 120,
                    },
                    {
                        "id": "Q22",
                        "entity_type": "organization",
                        "label": "Bank of Talora",
                        "aliases": [],
                    },
                    {
                        "id": "Q23",
                        "entity_type": "organization",
                        "label": "National Bank of Talora",
                        "aliases": [],
                    },
                    {
                        "id": "Q24",
                        "entity_type": "location",
                        "label": "China",
                        "aliases": [],
                        "sitelinks": 1200,
                        "source_features": {
                            "has_enwiki": False,
                            "sitelink_count": 1200,
                            "alias_count": 0,
                            "identifier_count": 0,
                            "statement_count": 40,
                        },
                    },
                    {
                        "id": "Q25",
                        "entity_type": "location",
                        "label": "Israel",
                        "aliases": [],
                        "sitelinks": 900,
                        "source_features": {
                            "has_enwiki": False,
                            "sitelink_count": 900,
                            "alias_count": 0,
                            "identifier_count": 0,
                            "statement_count": 30,
                        },
                    },
                    {
                        "id": "Q26",
                        "entity_type": "location",
                        "label": "Shanghai",
                        "aliases": [],
                        "sitelinks": 850,
                        "source_features": {
                            "has_enwiki": False,
                            "sitelink_count": 850,
                            "alias_count": 0,
                            "identifier_count": 0,
                            "statement_count": 25,
                        },
                    },
                    {
                        "id": "Q27",
                        "entity_type": "organization",
                        "label": "Northwind Solutions",
                        "aliases": [],
                        "source_features": {
                            "has_enwiki": False,
                            "sitelink_count": 0,
                            "alias_count": 0,
                            "identifier_count": 0,
                            "statement_count": 0,
                        },
                    },
                    {
                        "id": "Q28",
                        "entity_type": "organization",
                        "label": "Harbor China Information",
                        "aliases": ["Harbor"],
                        "source_features": {
                            "has_enwiki": False,
                            "sitelink_count": 0,
                            "alias_count": 1,
                            "identifier_count": 0,
                            "statement_count": 0,
                        },
                    },
                    {
                        "id": "Q29",
                        "entity_type": "location",
                        "label": "Four",
                        "aliases": [],
                        "sitelinks": 500,
                        "source_features": {
                            "has_enwiki": False,
                            "sitelink_count": 500,
                            "alias_count": 0,
                            "identifier_count": 0,
                            "statement_count": 12,
                        },
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    geonames_path = raw_dir / "geonames_places.txt"
    geonames_path.write_text(
        "\n".join(
            [
                "geonameid|name|alternatenames|feature_class|feature_code|country_code",
                "745044|Metro Alpha|Metro Alpha Historic,Byzantium|P|PPLA|TR",
                "2643743|Metro Beta|Metro Beta Alt|P|PPLC|GB",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    curated_path = raw_dir / "curated_general_entities.json"
    curated_path.write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "entity_type": "organization",
                        "canonical_text": "Org Beta",
                        "aliases": [],
                    },
                    {
                        "entity_type": "location",
                        "canonical_text": "Metro Gamma",
                        "aliases": ["Metro Gamma Variant"],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "wikidata_entities": wikidata_path,
        "geonames_places": geonames_path,
        "curated_entities": curated_path,
    }


def create_general_remote_sources(root: Path) -> dict[str, str]:
    remote_dir = root / "remote"
    remote_dir.mkdir(parents=True, exist_ok=True)

    wikidata_path = remote_dir / "wikidata_general_entities.source.json"
    wikidata_path.write_text(
        json.dumps(
            {
                "entities": {
                    "Q265852": {
                        "id": "Q265852",
                        "entity_type": "person",
                        "labels": {"en": {"value": "Person Alpha"}},
                        "aliases": {
                            "en": [
                                {"value": "Person Alpha Variant"},
                                {"value": "Person Alpha Alias"},
                            ]
                        },
                    },
                    "Q2283": {
                        "id": "Q2283",
                        "entity_type": "organization",
                        "labels": {"en": {"value": "Org Gamma"}},
                        "aliases": {
                            "en": [
                                {"value": "Issuer Beta Group"},
                            ]
                        },
                    },
                    "Q7426870": {
                        "id": "Q7426870",
                        "entity_type": "person",
                        "labels": {"en": {"value": "Person Beta"}},
                        "aliases": {
                            "en": [
                                {"value": "Person Beta Variant"},
                            ]
                        },
                    },
                    "Q305177": {
                        "id": "Q305177",
                        "entity_type": "person",
                        "labels": {"en": {"value": "Person Gamma"}},
                        "aliases": {
                            "en": [
                                {"value": "Person Gamma Variant"},
                            ]
                        },
                    },
                    "Q182477": {
                        "id": "Q182477",
                        "entity_type": "organization",
                        "labels": {"en": {"value": "Org Delta"}},
                        "aliases": {
                            "en": [
                                {"value": "Org Delta Variant"},
                            ]
                        },
                    },
                    "Q116758847": {
                        "id": "Q116758847",
                        "entity_type": "organization",
                        "labels": {"en": {"value": "Org Epsilon"}},
                        "aliases": {
                            "en": [
                                {"value": "Org Epsilon Variant"},
                            ]
                        },
                    },
                    "Q7407093": {
                        "id": "Q7407093",
                        "entity_type": "person",
                        "labels": {"en": {"value": "Person Delta"}},
                        "aliases": {
                            "en": [
                                {"value": "Person Delta Variant"},
                                {"value": "Person Delta Variant Two"},
                                {"value": "Person Delta Variant Three"},
                            ]
                        },
                    },
                    "Q15733006": {
                        "id": "Q15733006",
                        "entity_type": "organization",
                        "labels": {"en": {"value": "Org Zeta"}},
                        "aliases": {
                            "en": [
                                {"value": "Org Zeta Variant Limited"},
                                {"value": "Org Zeta Variant Limited (Metro Beta)"},
                                {"value": "Org Zeta Variant"},
                                {"value": "orgzeta.example.test"},
                                {"value": "Org Zeta Variant Split"},
                            ]
                        },
                    },
                    "Q3503829": {
                        "id": "Q3503829",
                        "entity_type": "person",
                        "labels": {"en": {"value": "Person Epsilon"}},
                        "aliases": {
                            "en": [
                                {"value": "Person Epsilon Variant One"},
                                {"value": "Person Epsilon Variant Two"},
                                {"value": "EpsilonSurname"},
                                {"value": "Person Epsilon Variant Three"},
                            ]
                        },
                    },
                    "Q3022141": {
                        "id": "Q3022141",
                        "entity_type": "person",
                        "labels": {"en": {"value": "Person Zeta"}},
                        "aliases": {
                            "en": [
                                {"value": "ZetaSurname"},
                                {"value": "Person Zeta Honorific"},
                            ]
                        },
                    },
                    "Q116706551": {
                        "id": "Q116706551",
                        "entity_type": "person",
                        "labels": {"en": {"value": "Person Eta"}},
                        "aliases": {
                            "en": [
                                {"value": "Person Eta Variant"},
                            ]
                        },
                    },
                    "Q21712134": {
                        "id": "Q21712134",
                        "entity_type": "person",
                        "labels": {"en": {"value": "Person Theta"}},
                        "aliases": {
                            "en": [
                                {"value": "Person Theta Variant"},
                            ]
                        },
                    },
                }
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    geonames_zip_path = remote_dir / "geonames_places.source.zip"
    with zipfile.ZipFile(geonames_zip_path, "w") as archive:
        archive.writestr(
            "cities15000.txt",
            "\n".join(
                [
                    "745044\tMetro Alpha\tMetro Alpha\tMetro Alpha Historic,Byzantium\t41.01384\t28.94966\tP\tPPLA\tTR\t\t34\t\t\t\t15462452\t\t39\tEurope/Metro Alpha\t2024-06-20",
                    "2643743\tMetro Beta\tMetro Beta\tMetro Beta Alt\t51.50853\t-0.12574\tP\tPPLC\tGB\t\tH9\t\t\t\t8961989\t\t25\tEurope/Metro Beta\t2024-06-20",
                ]
            )
            + "\n",
        )

    return {
        "wikidata_url": wikidata_path.resolve().as_uri(),
        "geonames_places_url": geonames_zip_path.resolve().as_uri(),
    }
