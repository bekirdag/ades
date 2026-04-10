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
                        "label": "Tim Cook",
                        "aliases": ["Timothy Cook"],
                    },
                    {
                        "id": "Q2",
                        "entity_type": "organization",
                        "label": "OpenAI",
                        "aliases": ["Open AI"],
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
                "745044|Istanbul|Constantinople,Byzantium|P|PPLA|TR",
                "2643743|London|Londres|P|PPLC|GB",
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
                        "canonical_text": "Apple",
                        "aliases": [],
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
                        "labels": {"en": {"value": "Tim Cook"}},
                        "aliases": {
                            "en": [
                                {"value": "Timothy Cook"},
                                {"value": "Tim Apple"},
                            ]
                        },
                    },
                    "Q2283": {
                        "id": "Q2283",
                        "labels": {"en": {"value": "Microsoft"}},
                        "aliases": {
                            "en": [
                                {"value": "Microsoft Corporation"},
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
                    "745044\tIstanbul\tIstanbul\tConstantinople,Byzantium\t41.01384\t28.94966\tP\tPPLA\tTR\t\t34\t\t\t\t15462452\t\t39\tEurope/Istanbul\t2024-06-20",
                    "2643743\tLondon\tLondon\tLondres\t51.50853\t-0.12574\tP\tPPLC\tGB\t\tH9\t\t\t\t8961989\t\t25\tEurope/London\t2024-06-20",
                ]
            )
            + "\n",
        )

    return {
        "wikidata_url": wikidata_path.resolve().as_uri(),
        "geonames_places_url": geonames_zip_path.resolve().as_uri(),
    }
